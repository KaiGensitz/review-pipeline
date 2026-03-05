from __future__ import annotations

import typing

from pipeline.integrations.embedding_utils import split_text_into_sentences
from config.user_orchestrator import EMBEDDING_SETTINGS, require_setting

chunk_size = int(str(require_setting(EMBEDDING_SETTINGS, "chunk_size", "EMBEDDING_SETTINGS")))
overlap_size = int(str(require_setting(EMBEDDING_SETTINGS, "overlap_size", "EMBEDDING_SETTINGS")))

Chunk = typing.Dict[str, typing.Any]


def _clean_text(value: str) -> str:
	"""Trim whitespace from text fields safely."""

	return value.strip() if value else ""


def chunk_paper_sentences(paper_id: str, title: str, abstract: str, language: str) -> list[Chunk]:
	"""Split title and abstract into sentence chunks (title sentences are always kept)."""

	title_text = _clean_text(title)
	abstract_text = _clean_text(abstract)

	title_sentences = [s.strip() for s in split_text_into_sentences(title_text, language) if s.strip()]
	abstract_sentences = [s.strip() for s in split_text_into_sentences(abstract_text, language) if s.strip()]

	chunks: list[Chunk] = []
	for idx, sentence in enumerate(title_sentences):
		chunks.append(
			{
				"paper_id": paper_id,
				"chunk_id": f"{paper_id}::title::{idx:04d}",
				"text": sentence,
				"kind": "title",
				"page_start": None,
				"page_end": None,
				"line_start": None,
				"line_end": None,
			}
		)

	for idx, sentence in enumerate(abstract_sentences):
		chunks.append(
			{
				"paper_id": paper_id,
				"chunk_id": f"{paper_id}::abstract::{idx:04d}",
				"text": sentence,
				"kind": "abstract",
				"page_start": None,
				"page_end": None,
				"line_start": None,
				"line_end": None,
			}
		)

	return chunks


def _chunk_sentence_entries(entries: list[dict], chunk_size: int, overlap_size: int) -> list[Chunk]:
	"""Group sentence entries into overlapping chunks with page/line spans."""

	if overlap_size >= chunk_size:
		raise ValueError("overlap_size must be smaller than chunk_size.")

	chunks: list[Chunk] = []
	step = chunk_size - overlap_size
	for i in range(0, len(entries), step):
		window = entries[i : i + chunk_size]
		if not window:
			continue
		text = " ".join(item["text"] for item in window)
		first = window[0]
		last = window[-1]
		chunks.append(
			{
				"text": text,
				"page_start": first.get("page"),
				"page_end": last.get("page"),
				"line_start": first.get("line"),
				"line_end": last.get("line"),
			}
		)
	return chunks


def chunk_fulltext_sentences(
	paper_id: str,
	title: str,
	full_text: str,
	language: str,
	page_texts: list[str] | None = None,
) -> list[Chunk]:
	"""Split full-text into overlapping blocks to stay within context limits."""

	cleaned_title = _clean_text(title)
	cleaned_full = _clean_text(full_text)

	entries: list[dict] = []
	if page_texts:
		for page_idx, page_text in enumerate(page_texts, start=1):
			if not page_text:
				continue
			lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
			for line_idx, line in enumerate(lines, start=1):
				for sentence in split_text_into_sentences(line, language):
					sentence = sentence.strip()
					if not sentence:
						continue
					entries.append({"text": sentence, "page": page_idx, "line": line_idx})
	else:
		sentences = [s.strip() for s in split_text_into_sentences(cleaned_full, language) if s.strip()]
		entries = [{"text": s, "page": None, "line": None} for s in sentences]

	sentence_blocks = _chunk_sentence_entries(entries, chunk_size=chunk_size, overlap_size=overlap_size)
	chunks: list[Chunk] = []

	if cleaned_title:
		chunks.append(
			{
				"paper_id": paper_id,
				"chunk_id": f"{paper_id}::title::0000",
				"text": cleaned_title,
				"kind": "title",
				"page_start": None,
				"page_end": None,
				"line_start": None,
				"line_end": None,
			}
		)

	for idx, block in enumerate(sentence_blocks):
		chunks.append(
			{
				"paper_id": paper_id,
				"chunk_id": f"{paper_id}::fulltext::{idx:04d}",
				"text": block["text"],
				"kind": "full_text",
				"page_start": block.get("page_start"),
				"page_end": block.get("page_end"),
				"line_start": block.get("line_start"),
				"line_end": block.get("line_end"),
			}
		)

	return chunks