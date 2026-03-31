from __future__ import annotations

from collections import Counter
import re
import typing

from pipeline.integrations.embedding_utils import split_text_into_sentences
from config.user_orchestrator import EMBEDDING_SETTINGS, require_setting

chunk_size = int(str(require_setting(EMBEDDING_SETTINGS, "chunk_size", "EMBEDDING_SETTINGS")))
overlap_size = int(str(require_setting(EMBEDDING_SETTINGS, "overlap_size", "EMBEDDING_SETTINGS")))

Chunk = typing.Dict[str, typing.Any]


class ChunkBuilder:
	"""human readable hint: one class that groups all chunk-building methods for title/abstract and full-text."""

	SECTION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
		(re.compile(r"^\s*(?:\d+(?:\.\d+)*)?\s*(?:introduction|background)\s*$", re.IGNORECASE), "introduction"),
		(re.compile(r"^\s*(?:\d+(?:\.\d+)*)?\s*(?:methods?|materials?\s+and\s+methods?|methodology|study\s+design)\s*$", re.IGNORECASE), "method"),
		(re.compile(r"^\s*(?:\d+(?:\.\d+)*)?\s*(?:results?|findings)\s*$", re.IGNORECASE), "results"),
		(re.compile(r"^\s*(?:\d+(?:\.\d+)*)?\s*(?:discussion)\s*$", re.IGNORECASE), "discussion"),
		(re.compile(r"^\s*(?:\d+(?:\.\d+)*)?\s*(?:conclusions?|summary)\s*$", re.IGNORECASE), "conclusion"),
	)

	@staticmethod
	def clean_text(value: str) -> str:
		"""Trim whitespace from text fields safely."""

		return value.strip() if value else ""

	@staticmethod
	def _detect_section_heading(line: str) -> str | None:
		"""Map standalone section heading lines to canonical IMRaD-style labels."""

		candidate = (line or "").strip()
		if not candidate:
			return None
		for pattern, label in ChunkBuilder.SECTION_PATTERNS:
			if pattern.match(candidate):
				return label
		return None

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
		"""Split full-text into overlapping blocks to stay within context limits."""

		cleaned_title = ChunkBuilder.clean_text(title)
		cleaned_full = ChunkBuilder.clean_text(full_text)

		entries: list[dict] = []
		if page_texts:
			current_section: str | None = None
			for page_idx, page_text in enumerate(page_texts, start=1):
				if not page_text:
					continue
				lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
				for line_idx, line in enumerate(lines, start=1):
					heading = ChunkBuilder._detect_section_heading(line)
					if heading:
						current_section = heading
						continue
					for sentence in split_text_into_sentences(line, language):
						sentence = sentence.strip()
						if not sentence:
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
				line = line.strip()
				if not line:
					continue
				heading = ChunkBuilder._detect_section_heading(line)
				if heading:
					current_section = heading
					continue
				for sentence in split_text_into_sentences(line, language):
					sentence = sentence.strip()
					if not sentence:
						continue
					entries.append({"text": sentence, "page": None, "line": line_idx, "section": current_section})

		if not entries:
			sentences = [s.strip() for s in split_text_into_sentences(cleaned_full, language) if s.strip()]
			entries = [{"text": s, "page": None, "line": None, "section": None} for s in sentences]

		sentence_blocks = ChunkBuilder.chunk_sentence_entries(entries, chunk_size=chunk_size, overlap_size=overlap_size)
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
					"section": None,
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
					"section": block.get("section"),
				}
			)

		return chunks


def _clean_text(value: str) -> str:
	"""Trim whitespace from text fields safely."""
	return ChunkBuilder.clean_text(value)


def chunk_paper_sentences(paper_id: str, title: str, abstract: str, language: str) -> list[Chunk]:
	"""Split title and abstract into sentence chunks (title sentences are always kept)."""
	return ChunkBuilder.chunk_paper_sentences(paper_id, title, abstract, language)


def _chunk_sentence_entries(entries: list[dict], chunk_size: int, overlap_size: int) -> list[Chunk]:
	"""Group sentence entries into overlapping chunks with page/line spans."""
	return ChunkBuilder.chunk_sentence_entries(entries, chunk_size=chunk_size, overlap_size=overlap_size)


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