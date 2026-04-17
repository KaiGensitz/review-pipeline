"""Helpers for reading PDFs and splitting text into sentences (moved from embedding/utils.py).

Notes for setup: requires NLTK sentence tokenizer data (auto-downloaded on first run) and
only distinguishes English vs German via simple stopword counts; other languages default to English splitting.
"""

from __future__ import annotations

from collections import Counter
import re

import nltk

try:
	import pdfplumber
except Exception:  # pragma: no cover - optional dependency at import time
	pdfplumber = None

try:
	from PyPDF2 import PdfReader
except Exception:  # pragma: no cover - optional dependency at import time
	PdfReader = None

try:
	from langdetect import DetectorFactory, LangDetectException, detect as _langdetect_detect
except Exception:  # pragma: no cover - optional dependency at import time
	DetectorFactory = None
	LangDetectException = Exception
	_langdetect_detect = None

if DetectorFactory is not None:
	DetectorFactory.seed = 42

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
	def detect_language_code(text: str) -> str | None:
		"""human readable hint: detect the ISO-like language code used for policy checks (e.g., en/de/fr)."""

		normalized_text = TextPdfUtils.normalize_extracted_text(text)
		if not normalized_text:
			return None

		sample = normalized_text[:10000]
		if _langdetect_detect is not None:
			try:
				code = (_langdetect_detect(sample) or "").strip().lower()
				if code:
					return code
			except LangDetectException:
				pass
			except Exception:
				pass

		tokens = re.findall(r"[a-zA-ZäöüÄÖÜß]+", normalized_text.lower())
		if not tokens:
			return None

		en_hits = sum(1 for token in tokens if token in EN_STOPWORDS)
		de_hits = sum(1 for token in tokens if token in DE_STOPWORDS)
		if max(en_hits, de_hits) < 2 or en_hits == de_hits:
			return None
		return "de" if de_hits > en_hits else "en"

	@staticmethod
	def detect_language(text: str) -> str:
		"""Detect whether text is English or German using stopword counts."""

		code = TextPdfUtils.detect_language_code(text)
		if code == "de":
			return "german"
		return "english"

	@staticmethod
	def _normalize_margin_line(line: str) -> str:
		"""Normalize page-margin lines so repeated headers/footers can be detected robustly."""

		value = re.sub(r"\d+", " ", (line or "").strip().lower())
		value = re.sub(r"\s+", " ", value)
		return value.strip()

	@staticmethod
	def _remove_repeated_margin_lines(raw_pages: list[str]) -> list[str]:
		"""human readable hint: remove repetitive page headers/footers that pollute retrieval quality."""

		if len(raw_pages) < 3:
			return raw_pages

		top_counter: Counter[str] = Counter()
		bottom_counter: Counter[str] = Counter()
		pages_lines: list[list[str]] = []

		for page in raw_pages:
			lines = [ln.strip() for ln in (page or "").splitlines() if ln and ln.strip()]
			pages_lines.append(lines)
			if not lines:
				continue
			for candidate in lines[:2]:
				norm = TextPdfUtils._normalize_margin_line(candidate)
				if norm and len(norm) >= 6:
					top_counter[norm] += 1
			for candidate in lines[-2:]:
				norm = TextPdfUtils._normalize_margin_line(candidate)
				if norm and len(norm) >= 6:
					bottom_counter[norm] += 1

		threshold = max(3, int(round(len(raw_pages) * 0.5)))
		drop_top = {line for line, count in top_counter.items() if count >= threshold}
		drop_bottom = {line for line, count in bottom_counter.items() if count >= threshold}

		cleaned_pages: list[str] = []
		for lines in pages_lines:
			if not lines:
				cleaned_pages.append("")
				continue

			filtered: list[str] = []
			for idx, line in enumerate(lines):
				norm = TextPdfUtils._normalize_margin_line(line)
				is_top_margin = idx < 2 and norm in drop_top
				is_bottom_margin = idx >= max(0, len(lines) - 2) and norm in drop_bottom
				if is_top_margin or is_bottom_margin:
					continue
				filtered.append(line)

			cleaned_pages.append("\n".join(filtered))

		return cleaned_pages

	@staticmethod
	def _read_pypdf_pages(file_path: str, max_pages: int | None = None) -> list[str]:
		"""Read page-level text via PyPDF fallback when available."""

		if PdfReader is None:
			return []

		try:
			reader = PdfReader(file_path)
		except Exception:
			return []

		pages: list[str] = []
		total = len(reader.pages)
		limit = total if max_pages is None else min(max_pages, total)
		for idx in range(limit):
			try:
				text = reader.pages[idx].extract_text() or ""
			except Exception:
				text = ""
			pages.append(text)
		return pages

	@staticmethod
	def read_pdf_file(file_path: str, max_pages: int | None = None) -> str:
		"""Read PDF text and return a single combined string (optionally capped by max_pages)."""

		pages = TextPdfUtils.read_pdf_pages(file_path, max_pages=max_pages)
		joined = "\n".join(page for page in pages if page)
		return TextPdfUtils.normalize_extracted_text(joined)

	@staticmethod
	def read_pdf_pages(file_path: str, max_pages: int | None = None) -> list[str]:
		"""Read PDF text and return a list of page-level strings (optionally capped)."""

		if pdfplumber is None and PdfReader is None:
			raise RuntimeError(
				"No PDF text backend is available. Install dependencies with: python -m pip install -r requirement.txt"
			)

		plumber_pages: list[str] = []
		if pdfplumber is not None:
			try:
				with pdfplumber.open(file_path) as pdf:
					pages_iter = pdf.pages if max_pages is None else pdf.pages[:max_pages]
					for page in pages_iter:
						try:
							page_text = page.extract_text(layout=True, x_tolerance=2, y_tolerance=3) or ""
						except TypeError:
							page_text = page.extract_text() or ""
						except Exception:
							page_text = ""
						plumber_pages.append(page_text)
			except Exception:
				plumber_pages = []

		fallback_pages = TextPdfUtils._read_pypdf_pages(file_path, max_pages=max_pages)

		if not plumber_pages and not fallback_pages:
			return []

		max_len = max(len(plumber_pages), len(fallback_pages))
		raw_pages: list[str] = []
		for idx in range(max_len):
			primary = plumber_pages[idx] if idx < len(plumber_pages) else ""
			fallback = fallback_pages[idx] if idx < len(fallback_pages) else ""
			primary_clean = primary.strip()
			fallback_clean = fallback.strip()

			if not primary_clean and fallback_clean:
				chosen = fallback_clean
			elif len(primary_clean) < 80 and len(fallback_clean) > len(primary_clean):
				chosen = fallback_clean
			else:
				chosen = primary_clean or fallback_clean

			raw_pages.append(chosen)

		cleaned_pages = TextPdfUtils._remove_repeated_margin_lines(raw_pages)
		return [TextPdfUtils.normalize_extracted_text(page) for page in cleaned_pages]

	@staticmethod
	def split_text_into_sentences(text: str, language: str) -> list[str]:
		"""Split text into sentences using NLTK."""

		normalized_text = TextPdfUtils.normalize_extracted_text(text)
		if not normalized_text:
			return []
		selected_language = TextPdfUtils.detect_language(normalized_text) if language == "auto" else language
		if selected_language not in {"english", "german"}:
			selected_language = "english"
		sentences = nltk.sent_tokenize(normalized_text, language=selected_language)
		return sentences


def detect_language(text: str) -> str:
	"""Detect whether text is English or German using stopword counts."""
	return TextPdfUtils.detect_language(text)


def detect_language_code(text: str) -> str | None:
	"""Detect language code for deterministic language policy checks."""
	return TextPdfUtils.detect_language_code(text)


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
