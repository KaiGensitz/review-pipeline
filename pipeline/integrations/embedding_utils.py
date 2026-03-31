"""Helpers for reading PDFs and splitting text into sentences (moved from embedding/utils.py).

Notes for setup: requires NLTK sentence tokenizer data (auto-downloaded on first run) and
only distinguishes English vs German via simple stopword counts; other languages default to English splitting.
"""

from __future__ import annotations

import re

import nltk

try:
	import pdfplumber
except Exception:  # pragma: no cover - optional dependency at import time
	pdfplumber = None

EN_STOPWORDS = {
	"the",
	"and",
	"of",
	"to",
	"in",
	"for",
	"with",
	"on",
	"is",
	"are",
	"was",
	"were",
	"this",
	"that",
}

DE_STOPWORDS = {
	"der",
	"die",
	"das",
	"und",
	"zu",
	"in",
	"mit",
	"auf",
	"ist",
	"sind",
	"war",
	"waren",
	"dies",
	"diese",
}


class TextPdfUtils:
	"""human readable hint: one utility class for language detection, PDF reading, and sentence splitting."""

	@staticmethod
	def normalize_extracted_text(text: str) -> str:
		"""human readable hint: apply conservative cleanup to extracted PDF text before sentence splitting."""

		value = text or ""
		if not value:
			return ""

		# Remove invisible soft hyphen artifacts from PDF extraction.
		value = value.replace("\u00ad", "")
		# Join explicit line-break hyphenations (e.g., "micro-\nrandomized" -> "microrandomized").
		value = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", value)
		# Normalize line breaks/tabs to spaces.
		value = re.sub(r"[\r\n\t]+", " ", value)
		# Add missing space after sentence punctuation where extraction collapsed boundaries.
		value = re.sub(r"([\.!\?;,:])(\w)", r"\1 \2", value)
		# Add spacing between alpha-numeric boundaries for readability (e.g., "10degrees" / "Figure2").
		value = re.sub(r"([A-Za-z])([0-9])", r"\1 \2", value)
		value = re.sub(r"([0-9])([A-Za-z])", r"\1 \2", value)
		# Add spacing at lower->Upper transitions (e.g., "degreesCelsius").
		value = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
		# Collapse repeated whitespace after all repairs.
		value = re.sub(r"\s+", " ", value).strip()
		return value

	@staticmethod
	def detect_language(text: str) -> str:
		"""Detect whether text is English or German using stopword counts."""

		tokens = re.findall(r"[a-zA-ZäöüÄÖÜß]+", (text or "").lower())
		if not tokens:
			print("[warning] Language detection saw no text; defaulting to English sentence splitting.")
			return "english"

		en_hits = sum(1 for token in tokens if token in EN_STOPWORDS)
		de_hits = sum(1 for token in tokens if token in DE_STOPWORDS)

		if en_hits == de_hits:
			print("[warning] Language detection was ambiguous; defaulting to English sentence splitting.")
			return "english"
		return "german" if de_hits > en_hits else "english"

	@staticmethod
	def read_pdf_file(file_path: str, max_pages: int | None = None) -> str:
		"""Read PDF text and return a single combined string (optionally capped by max_pages)."""

		if pdfplumber is None:
			raise RuntimeError(
				"pdfplumber is required for PDF reading but is not installed. "
				"Install dependencies with: python -m pip install -r requirement.txt"
			)

		text_content = []
		with pdfplumber.open(file_path) as pdf:
			pages_iter = pdf.pages if max_pages is None else pdf.pages[:max_pages]
			for page in pages_iter:
				page_text = page.extract_text()
				if page_text:
					text_content.append(page_text)
		return "\n".join(text_content)

	@staticmethod
	def read_pdf_pages(file_path: str, max_pages: int | None = None) -> list[str]:
		"""Read PDF text and return a list of page-level strings (optionally capped)."""

		if pdfplumber is None:
			raise RuntimeError(
				"pdfplumber is required for PDF reading but is not installed. "
				"Install dependencies with: python -m pip install -r requirement.txt"
			)

		pages: list[str] = []
		with pdfplumber.open(file_path) as pdf:
			pages_iter = pdf.pages if max_pages is None else pdf.pages[:max_pages]
			for page in pages_iter:
				page_text = page.extract_text() or ""
				pages.append(page_text)
		return pages

	@staticmethod
	def split_text_into_sentences(text: str, language: str) -> list[str]:
		"""Split text into sentences using NLTK."""

		normalized_text = TextPdfUtils.normalize_extracted_text(text)
		if not normalized_text:
			return []
		selected_language = TextPdfUtils.detect_language(normalized_text) if language == "auto" else language
		sentences = nltk.sent_tokenize(normalized_text, language=selected_language)
		return sentences


def detect_language(text: str) -> str:
	"""Detect whether text is English or German using stopword counts."""
	return TextPdfUtils.detect_language(text)


def read_pdf_file(file_path: str, max_pages: int | None = None) -> str:
	"""Read PDF text and return a single combined string (optionally capped by max_pages)."""
	return TextPdfUtils.read_pdf_file(file_path, max_pages=max_pages)


def read_pdf_pages(file_path: str, max_pages: int | None = None) -> list[str]:
	"""Read PDF text and return a list of page-level strings (optionally capped)."""
	return TextPdfUtils.read_pdf_pages(file_path, max_pages=max_pages)


def split_text_into_sentences(text: str, language: str) -> list[str]:
	"""Split text into sentences using NLTK."""
	return TextPdfUtils.split_text_into_sentences(text, language)


def normalize_extracted_text(text: str) -> str:
	"""Apply conservative cleanup to extracted PDF text before downstream processing."""
	return TextPdfUtils.normalize_extracted_text(text)
