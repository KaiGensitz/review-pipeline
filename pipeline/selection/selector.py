from __future__ import annotations

import csv
import json
import os
import typing
from collections import OrderedDict
from dataclasses import dataclass

import numpy as np
from dotenv import load_dotenv
from openai import OpenAI

from config.user_orchestrator import EMBEDDING_SETTINGS, require_setting


gpustack_embedding_model = require_setting(EMBEDDING_SETTINGS, "gpustack_embedding_model", "EMBEDDING_SETTINGS")
gpustack_base_url = require_setting(EMBEDDING_SETTINGS, "gpustack_base_url", "EMBEDDING_SETTINGS")
embedding_cache_size = require_setting(EMBEDDING_SETTINGS, "embedding_cache_size", "EMBEDDING_SETTINGS")

LabeledExample = typing.TypedDict("LabeledExample", {"label": str, "text": str})


def load_labeled_examples(path: str) -> list[LabeledExample]:
	"""Load POS/NEG training examples used to score relevance."""

	ext = os.path.splitext(path)[1].lower()
	if ext not in {".csv", ".jsonl"}:
		raise ValueError("Knowledge base must be .csv or .jsonl with fields 'label' and 'text'.")

	examples: list[LabeledExample] = []
	with open(path, "r", encoding="utf-8") as handle:
		if ext == ".csv":
			reader = csv.DictReader(handle)
			for row in reader:
				label = (row.get("label") or row.get("Label") or "").strip().upper()
				text = (row.get("text") or row.get("sentence") or "").strip()
				if not label or not text:
					continue
				if label not in {"POS", "NEG"}:
					continue
				examples.append({"label": label, "text": text})
		else:
			for line in handle:
				if not line.strip():
					continue
				payload = json.loads(line)
				label = (payload.get("label") or "").strip().upper()
				text = (payload.get("text") or payload.get("sentence") or "").strip()
				if not label or not text:
					continue
				if label not in {"POS", "NEG"}:
					continue
				examples.append({"label": label, "text": text})

	if not examples:
		raise ValueError("No labeled examples found. Provide POS/NEG sentences in the knowledge base file.")
	return examples


def _normalize(vec: np.ndarray) -> np.ndarray:
	"""Normalize a vector to unit length."""

	norm = np.linalg.norm(vec)
	return vec if norm == 0 else vec / norm


@dataclass
class EmbeddingBackend:
	"""Fetch embeddings from the API and cache them for reuse."""

	batch_size: int = 32
	cache: "OrderedDict[str, np.ndarray]" | None = None
	cache_size: int | None = None

	def __post_init__(self) -> None:
		load_dotenv()
		self.cache = OrderedDict() if self.cache is None else self.cache
		self.cache_size = embedding_cache_size if self.cache_size is None else self.cache_size
		self._client = OpenAI(api_key=os.environ.get("LLM_API_KEY"), base_url=gpustack_base_url)

	def embed_texts(self, texts: list[str]) -> tuple[list[np.ndarray], dict | None]:
		"""Return embeddings for texts plus usage metadata when the API provides it."""

		results: list[np.ndarray] = [None] * len(texts)
		missing: list[str] = []
		missing_idx: list[int] = []
		usage_totals: dict | None = None

		for idx, text in enumerate(texts):
			cached = self.cache.get(text)
			if cached is not None:
				results[idx] = cached
				self.cache.move_to_end(text)
			else:
				missing.append(text)
				missing_idx.append(idx)

		if missing:
			new_embeddings, usage_totals = self._embed_in_batches(missing)
			normalized_batches: list[np.ndarray] = []
			for text, embedding in zip(missing, new_embeddings):
				normalized = _normalize(np.asarray(embedding, dtype=float))
				self.cache[text] = normalized
				self._maybe_evict_cache()
				normalized_batches.append(normalized)
			for idx, embedding in zip(missing_idx, normalized_batches):
				results[idx] = embedding

		return results, usage_totals

	def _maybe_evict_cache(self) -> None:
		"""Evict oldest cached embeddings to cap memory usage."""

		if not self.cache_size:
			return
		while len(self.cache) > max(self.cache_size, 0):
			self.cache.popitem(last=False)

	def _embed_in_batches(self, texts: list[str]) -> tuple[list[list[float]], dict | None]:
		"""Request embeddings in batches and accumulate usage if available."""

		embeddings: list[list[float]] = []
		usage_totals: dict | None = None

		for start in range(0, len(texts), self.batch_size):
			batch = texts[start : start + self.batch_size]
			response = self._client.embeddings.create(model=gpustack_embedding_model, input=batch)
			batch_embeddings = [item.embedding for item in response.data]
			embeddings.extend(batch_embeddings)

			usage = getattr(response, "usage", None)
			if usage is not None:
				usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else dict(usage)
				if usage_totals is None:
					usage_totals = {k: usage_dict.get(k, 0) for k in usage_dict}
				else:
					for key, value in usage_dict.items():
						usage_totals[key] = usage_totals.get(key, 0) + value

		return embeddings, usage_totals


class RelevanceSelector:
	"""Score chunks against POS/NEG examples and keep the most relevant ones."""

	def __init__(
		self,
		embedder: EmbeddingBackend,
		examples: list[LabeledExample],
		always_include_kinds: typing.Iterable[str] = ("title",),
	) -> None:
		self.embedder = embedder
		self.always_include_kinds = {kind for kind in always_include_kinds}

		positives = [ex["text"] for ex in examples if ex["label"] == "POS"]
		negatives = [ex["text"] for ex in examples if ex["label"] == "NEG"]

		if not positives:
			raise ValueError("Knowledge base requires at least one POS example.")

		pos_vectors, _ = self.embedder.embed_texts(positives)
		pos_embeddings = [_normalize(vec) for vec in pos_vectors]
		self.pos_centroid = _normalize(np.mean(pos_embeddings, axis=0))

		self.neg_centroid = None
		if negatives:
			neg_vectors, _ = self.embedder.embed_texts(negatives)
			neg_embeddings = [_normalize(vec) for vec in neg_vectors]
			self.neg_centroid = _normalize(np.mean(neg_embeddings, axis=0))

	def _score_vectors(self, vectors: list[np.ndarray]) -> list[float]:
		"""Compute relevance scores for each vector."""

		scores: list[float] = []
		for vec in vectors:
			vec_norm = _normalize(np.asarray(vec, dtype=float))
			pos_score = float(np.dot(vec_norm, self.pos_centroid))
			neg_score = float(np.dot(vec_norm, self.neg_centroid)) if self.neg_centroid is not None else 0.0
			scores.append(pos_score - neg_score)
		return scores

	def select(
		self,
		chunks: list[dict],
		top_k: int | None,
		score_threshold: float | None = None,
	) -> tuple[list[dict], list[float], dict | None]:
		"""Return selected chunks and their scores based on relevance."""

		texts = [chunk["text"] for chunk in chunks]
		embeddings, usage = self.embedder.embed_texts(texts)
		scores = self._score_vectors(embeddings)

		enriched = []
		for chunk, score in zip(chunks, scores):
			item = dict(chunk)
			item["score"] = score
			enriched.append(item)

		always = [c for c in enriched if c.get("kind") in self.always_include_kinds]
		remaining = [c for c in enriched if c not in always]
		remaining.sort(key=lambda item: item["score"], reverse=True)

		if score_threshold is not None:
			remaining = [c for c in remaining if c["score"] >= score_threshold]
		if top_k is not None:
			remaining = remaining[: max(0, top_k)]

		merged: dict[str, dict] = {c["chunk_id"]: c for c in always}
		for candidate in remaining:
			if candidate["chunk_id"] not in merged:
				merged[candidate["chunk_id"]] = candidate

		selected = list(merged.values())
		selected.sort(
			key=lambda item: (
				item.get("kind") != "title",
				-item.get("score", 0.0),
				item.get("page_start") or 0,
				item.get("line_start") or 0,
				item.get("chunk_id", ""),
			)
		)

		return selected, scores, usage