from __future__ import annotations

from collections import Counter
import re
import typing

from pipeline.integrations.embedding_utils import split_text_into_sentences
from config.user_orchestrator import EMBEDDING_SETTINGS, require_setting
from pipeline.selection.prompt_signals import retrieval_pattern, retrieval_section_patterns

chunk_size = int(str(require_setting(EMBEDDING_SETTINGS, "chunk_size", "EMBEDDING_SETTINGS")))
overlap_size = int(str(require_setting(EMBEDDING_SETTINGS, "overlap_size", "EMBEDDING_SETTINGS")))

Chunk = typing.Dict[str, typing.Any]


class ChunkBuilder:
	"""human readable hint: one class that groups all chunk-building methods for title/abstract and full-text."""

	SUBSTANTIVE_SENTENCE_PATTERN = retrieval_pattern("substantive_sentence_terms")

	SECTION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = retrieval_section_patterns()

	SECTION_INLINE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = retrieval_section_patterns(inline=True)

	@staticmethod
	def clean_text(value: str) -> str:
		"""Trim whitespace from text fields safely."""

		return value.strip() if value else ""

	@staticmethod
	def _normalize_line_for_chunking(line: str) -> str:
		"""Normalize noisy extracted line text before heading detection and sentence splitting."""

		value = (line or "").replace("\u00ad", "")
		value = re.sub(r"(\w)-\s+(\w)", r"\1\2", value)
		value = re.sub(r"\s+", " ", value)
		return value.strip()

	@staticmethod
	def _looks_substantive_sentence(sentence: str) -> bool:
		"""Keep citation-heavy lines when they still contain substantive method/result narrative."""

		value = (sentence or "").strip()
		if not value:
			return False

		alpha_tokens = re.findall(r"[A-Za-z]+", value)
		if len(alpha_tokens) < 10:
			return False

		long_token_count = sum(1 for token in alpha_tokens if len(token) >= 5)
		if long_token_count < 5:
			return False

		return bool(ChunkBuilder.SUBSTANTIVE_SENTENCE_PATTERN.search(value))

	@staticmethod
	def _is_low_information_sentence(sentence: str) -> bool:
		"""human readable hint: discard sentence fragments that are mostly tables/citations/noise."""

		value = (sentence or "").strip()
		if not value:
			return True

		if re.fullmatch(r"(?:[\W_]|\s)+", value):
			return True

		alpha_tokens = re.findall(r"[A-Za-z]+", value)
		if len(alpha_tokens) < 3:
			return True

		if len(value) < 25:
			return True

		digit_count = sum(1 for ch in value if ch.isdigit())
		digit_ratio = digit_count / max(len(value), 1)
		if digit_ratio > 0.35:
			return True

		citation_hits = len(re.findall(r"\[[0-9,\s\-]+\]|\([12][0-9]{3}\)", value))
		if citation_hits >= 2:
			if ChunkBuilder._looks_substantive_sentence(value):
				return False
			return True

		return False

	@staticmethod
	def _extract_section_heading(line: str) -> tuple[str | None, str]:
		"""Return detected section label plus remaining line content for sentence extraction."""

		candidate = ChunkBuilder._normalize_line_for_chunking(line)
		if not candidate:
			return None, ""
		for pattern, label in ChunkBuilder.SECTION_PATTERNS:
			if pattern.match(candidate):
				return label, ""
		for pattern, label in ChunkBuilder.SECTION_INLINE_PATTERNS:
			match = pattern.match(candidate)
			if match:
				return label, (match.group(1) or "").strip()
		return None, candidate

	@staticmethod
	def chunk_sentence_entries(entries: list[dict], chunk_size: int, overlap_size: int) -> list[Chunk]:
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
			section_votes = [str(item.get("section") or "").strip() for item in window]
			section_votes = [vote for vote in section_votes if vote]
			dominant_section = Counter(section_votes).most_common(1)[0][0] if section_votes else None
			chunks.append(
				{
					"text": text,
					"sentence_count": len(window),
					"word_count": len(text.split()),
					"page_start": first.get("page"),
					"page_end": last.get("page"),
					"line_start": first.get("line"),
					"line_end": last.get("line"),
					"section": dominant_section,
				}
			)
		return chunks

	@staticmethod
	def chunk_paper_sentences(paper_id: str, title: str, abstract: str, language: str) -> list[Chunk]:
		"""Split title and abstract into sentence chunks (title sentences are always kept)."""

		title_text = ChunkBuilder.clean_text(title)
		abstract_text = ChunkBuilder.clean_text(abstract)

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

	@staticmethod
	def chunk_fulltext_sentences(
		paper_id: str,
		title: str,
		full_text: str,
		language: str,
		page_texts: list[str] | None = None,
	) -> list[Chunk]:
		"""Split full-text into overlapping blocks; title is kept as metadata outside chunk evidence."""

		_ = title
		cleaned_full = ChunkBuilder.clean_text(full_text)

		entries: list[dict] = []
		if page_texts:
			current_section: str | None = None
			for page_idx, page_text in enumerate(page_texts, start=1):
				if not page_text:
					continue
				lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
				for line_idx, line in enumerate(lines, start=1):
					heading, content = ChunkBuilder._extract_section_heading(line)
					if heading:
						current_section = heading
					if not content:
						continue
					for sentence in split_text_into_sentences(content, language):
						sentence = sentence.strip()
						if not sentence or ChunkBuilder._is_low_information_sentence(sentence):
							continue
						entries.append(
							{
								"text": sentence,
								"page": page_idx,
								"line": line_idx,
								"section": current_section,
							}
						)
		else:
			current_section = None
			for line_idx, line in enumerate(cleaned_full.splitlines(), start=1):
				normalized_line = ChunkBuilder._normalize_line_for_chunking(line)
				if not normalized_line:
					continue
				heading, content = ChunkBuilder._extract_section_heading(normalized_line)
				if heading:
					current_section = heading
				if not content:
					continue
				for sentence in split_text_into_sentences(content, language):
					sentence = sentence.strip()
					if not sentence or ChunkBuilder._is_low_information_sentence(sentence):
						continue
					entries.append({"text": sentence, "page": None, "line": line_idx, "section": current_section})

		if not entries:
			sentences = [
				s.strip()
				for s in split_text_into_sentences(cleaned_full, language)
				if s.strip() and not ChunkBuilder._is_low_information_sentence(s)
			]
			entries = [{"text": s, "page": None, "line": None, "section": None} for s in sentences]

		sentence_blocks = ChunkBuilder.chunk_sentence_entries(entries, chunk_size=chunk_size, overlap_size=overlap_size)
		chunks: list[Chunk] = []

		for idx, block in enumerate(sentence_blocks):
			chunks.append(
				{
					"paper_id": paper_id,
					"chunk_id": f"{paper_id}::fulltext::{idx:04d}",
					"text": block["text"],
					"kind": "full_text",
					"sentence_count": int(block.get("sentence_count") or 0),
					"word_count": int(block.get("word_count") or 0),
					"page_start": block.get("page_start"),
					"page_end": block.get("page_end"),
					"line_start": block.get("line_start"),
					"line_end": block.get("line_end"),
					"section": block.get("section"),
				}
			)

		return chunks


def chunk_paper_sentences(paper_id: str, title: str, abstract: str, language: str) -> list[Chunk]:
	"""Split title and abstract into sentence chunks (title sentences are always kept)."""
	return ChunkBuilder.chunk_paper_sentences(paper_id, title, abstract, language)


def chunk_fulltext_sentences(
	paper_id: str,
	title: str,
	full_text: str,
	language: str,
	page_texts: list[str] | None = None,
) -> list[Chunk]:
	"""Split full-text into overlapping blocks to stay within context limits."""
	return ChunkBuilder.chunk_fulltext_sentences(
		paper_id,
		title,
		full_text,
		language,
		page_texts=page_texts,
	)
