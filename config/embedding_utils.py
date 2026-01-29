"""Helpers for reading PDFs and splitting text into sentences (moved from embedding/utils.py)."""

from __future__ import annotations

import re

import nltk
import pdfplumber

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


def read_pdf_file(file_path: str) -> str:
	"""Read PDF text and return a single combined string."""

	text_content = []
	with pdfplumber.open(file_path) as pdf:
		for page in pdf.pages:
			page_text = page.extract_text()
			if page_text:
				text_content.append(page_text)
	return "\n".join(text_content)


def read_pdf_pages(file_path: str) -> list[str]:
	"""Read PDF text and return a list of page-level strings."""

	pages: list[str] = []
	with pdfplumber.open(file_path) as pdf:
		for page in pdf.pages:
			page_text = page.extract_text() or ""
			pages.append(page_text)
	return pages


def split_text_into_sentences(text: str, language: str) -> list[str]:
	"""Split text into sentences using NLTK."""

	selected_language = detect_language(text) if language == "auto" else language
	sentences = nltk.sent_tokenize(text, language=selected_language)
	return sentences