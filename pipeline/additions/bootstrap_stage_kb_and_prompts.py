from __future__ import annotations

import argparse
from collections import Counter
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
from typing import Callable
import unicodedata

from config.user_orchestrator import STUDY_TAGS_INCLUDE
from pipeline.integrations.embedding_utils import detect_language_code, read_pdf_pages
from pipeline.selection.chunking import chunk_fulltext_sentences


STOPWORDS = {
	"a",
	"about",
	"after",
	"all",
	"also",
	"an",
	"and",
	"any",
	"are",
	"as",
	"at",
	"be",
	"because",
	"been",
	"before",
	"between",
	"both",
	"but",
	"by",
	"can",
	"could",
	"did",
	"do",
	"does",
	"during",
	"each",
	"for",
	"from",
	"had",
	"has",
	"have",
	"how",
	"if",
	"in",
	"into",
	"is",
	"it",
	"its",
	"may",
	"more",
	"most",
	"no",
	"not",
	"of",
	"on",
	"or",
	"our",
	"out",
	"over",
	"paper",
	"participants",
	"results",
	"screening",
	"stage",
	"study",
	"such",
	"that",
	"the",
	"their",
	"these",
	"this",
	"those",
	"to",
	"used",
	"using",
	"was",
	"were",
	"what",
	"when",
	"which",
	"with",
	"within",
	"without",
}


SECTION_BONUS = {
	"introduction": 0.2,
	"method": 1.2,
	"results": 1.0,
	"discussion": 0.5,
	"conclusion": 0.3,
	"reference": -6.0,
}


REFERENCE_PATTERN = re.compile(
	r"\breferences?\b|\bbibliography\b|\bcopyright\b|\ball rights reserved\b|\bdoi\b",
	re.IGNORECASE,
)


TITLE_YEAR_PATTERN = re.compile(r"^(?P<author>.+?)\s*-\s*(?P<year>(?:19|20)\d{2})\s*-\s*(?P<title>.+)$")


TOKEN_PATTERN = re.compile(r"[a-z][a-z0-9\-]{2,}")


@dataclass(slots=True)
class PaperRecord:
	label: str
	source: str
	title: str
	pdf_path: Path
	pages: list[str]
	full_text: str
	page_count: int
	language: str
	chunks: list[dict]


@dataclass(frozen=True, slots=True)
class BootstrapSignals:
	"""human readable hint: data-derived cue terms learned from the local POS/NEG example PDFs."""

	include_terms: tuple[str, ...]
	exclude_terms: tuple[str, ...]
	extraction_terms: tuple[str, ...]


def _normalize_ascii(value: str) -> str:
	normalized = unicodedata.normalize("NFKD", value)
	return normalized.encode("ascii", "ignore").decode("ascii")


def _slugify(value: str) -> str:
	ascii_value = _normalize_ascii(value).lower()
	slug = re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_")
	return slug or "paper"


def _list_pdfs(folder: Path) -> list[Path]:
	if not folder.exists():
		return []
	return sorted([item for item in folder.iterdir() if item.is_file() and item.suffix.lower() == ".pdf"])


def _parse_filename_metadata(filename: str) -> tuple[str, str]:
	stem = Path(filename).stem.strip()
	match = TITLE_YEAR_PATTERN.match(stem)
	if not match:
		return stem, stem
	author = re.sub(r"\s+", " ", match.group("author")).strip(" -")
	year = match.group("year").strip()
	title = re.sub(r"\s+", " ", match.group("title")).strip(" -")
	source = f"{author} ({year})"
	return source, title


def _keyword_hits(lower_text: str, phrases: set[str]) -> int:
	hits = 0
	for phrase in phrases:
		if phrase in lower_text:
			hits += 1
	return hits


def _looks_reference_like(text: str, section: str | None = None) -> bool:
	lower_text = text.lower()
	if section and section.lower() == "reference":
		return True
	if REFERENCE_PATTERN.search(lower_text):
		return True

	citation_hits = len(re.findall(r"\[[0-9,\s\-]+\]|\([12][0-9]{3}[a-z]?\)", text))
	if citation_hits >= 4:
		return True

	if len(re.findall(r"\bet\s+al\.\b", lower_text)) >= 3:
		return True

	return False


def _truncate_words(text: str, max_words: int) -> str:
	words = text.split()
	if len(words) <= max_words:
		return text.strip()
	return " ".join(words[:max_words]).strip() + " ..."


def _score_fulltext_chunk(chunk: dict, paper: PaperRecord, signals: BootstrapSignals) -> float:
	"""human readable hint: rank chunks using terms learned from the local POS/NEG example PDFs."""

	text = str(chunk.get("text") or "").strip()
	if not text:
		return -1000.0

	word_count = len(text.split())
	if word_count < 20:
		return -200.0

	lower_text = text.lower()
	section = str(chunk.get("section") or "").strip().lower()

	score = 0.0
	score += _keyword_hits(lower_text, set(signals.include_terms)) * (1.4 if paper.label == "POS" else 0.4)
	score += _keyword_hits(lower_text, set(signals.exclude_terms)) * (1.4 if paper.label == "NEG" else -0.4)
	score += SECTION_BONUS.get(section, 0.0)

	if _looks_reference_like(text, section=section):
		score -= 8.0

	page_start = chunk.get("page_start")
	if isinstance(page_start, int) and paper.page_count > 0:
		ratio = page_start / max(paper.page_count, 1)
		if ratio > 0.9:
			score -= 0.8

	score += min(word_count, 200) / 200.0
	return score


def _score_data_extraction_chunk(chunk: dict, paper: PaperRecord, signals: BootstrapSignals) -> float:
	"""human readable hint: rank extraction examples using terms learned from included example PDFs."""

	text = str(chunk.get("text") or "").strip()
	if not text:
		return -1000.0

	lower_text = text.lower()
	section = str(chunk.get("section") or "").strip().lower()
	word_count = len(text.split())

	score = 0.0
	score += _keyword_hits(lower_text, set(signals.extraction_terms)) * 1.6
	score += _keyword_hits(lower_text, set(signals.include_terms)) * 0.6
	score += SECTION_BONUS.get(section, 0.0)
	score += min(word_count, 260) / 260.0

	if _looks_reference_like(text, section=section):
		score -= 6.0

	return score


def _select_diverse_chunks(
	chunks: list[dict],
	paper: PaperRecord,
	score_fn: Callable[[dict, PaperRecord], float],
	max_items: int,
) -> list[dict]:
	if not chunks or max_items <= 0:
		return []

	scored = []
	for chunk in chunks:
		score = score_fn(chunk, paper)
		scored.append((score, chunk))

	scored.sort(key=lambda item: item[0], reverse=True)
	selected: list[dict] = []
	selected_ids: set[str] = set()
	used_pages: set[int] = set()

	for score, chunk in scored:
		if len(selected) >= max_items:
			break
		chunk_id = str(chunk.get("chunk_id") or "")
		if chunk_id in selected_ids:
			continue
		if score < -30.0:
			continue
		page = chunk.get("page_start")
		if isinstance(page, int) and page in used_pages and len(scored) > (max_items * 4):
			continue

		selected.append(chunk)
		if chunk_id:
			selected_ids.add(chunk_id)
		if isinstance(page, int):
			used_pages.add(page)

	if len(selected) < max_items:
		for _, chunk in scored:
			if len(selected) >= max_items:
				break
			chunk_id = str(chunk.get("chunk_id") or "")
			if chunk_id in selected_ids:
				continue
			selected.append(chunk)
			if chunk_id:
				selected_ids.add(chunk_id)

	return selected


def _page_tag(chunk: dict) -> str:
	page_start = chunk.get("page_start")
	page_end = chunk.get("page_end")
	if isinstance(page_start, int) and isinstance(page_end, int):
		if page_start == page_end:
			return f"p{page_start}"
		return f"p{page_start}-{page_end}"
	if isinstance(page_start, int):
		return f"p{page_start}"
	return "pNA"


def _infer_negative_reason(title: str, snippet: str, signals: BootstrapSignals) -> str:
	"""human readable hint: explain NEG examples using exclusion-like terms learned from local PDFs."""

	lower_title = title.lower()
	lower_text = snippet.lower()
	joined = lower_title + " " + lower_text
	matched = [term for term in signals.exclude_terms[:12] if term in joined]
	if matched:
		return "Negative example because the provided evidence matches local NEG-example terms: " + ", ".join(matched[:4]) + "."

	if "review" in joined or "scoping" in joined:
		return "Negative example because this paper is a review-style publication rather than direct primary evidence."
	if "agent-based" in joined or "simulation" in joined:
		return "Negative example because this paper focuses on simulation/modeling rather than direct participant-level evidence."
	if "children" in joined or "adolescent" in joined:
		return "Negative example because the target population is non-adult."

	return "Negative example from the provided neg_examples set to represent non-eligible or weak-fit screening patterns."


def _extract_signals_for_reasoning(text: str, signals: BootstrapSignals) -> list[str]:
	"""human readable hint: summarize why a POS example matched using local data-derived cue terms."""

	lower_text = text.lower()
	matched = [term for term in signals.include_terms[:12] if term in lower_text]
	return matched[:4]


def _infer_extraction_tags(text: str, signals: BootstrapSignals) -> list[str]:
	"""human readable hint: label extraction snippets with matched local cue terms rather than fixed domains."""

	lower_text = text.lower()
	tags = [term for term in signals.extraction_terms[:12] if term in lower_text]
	if not tags:
		tags.append("general")
	return tags[:5]


def _tokenize(text: str) -> list[str]:
	lower = _normalize_ascii(text.lower())
	tokens = TOKEN_PATTERN.findall(lower)
	return [token for token in tokens if token not in STOPWORDS]


def _top_discriminative_terms(primary: Counter[str], contrast: Counter[str], top_n: int) -> list[str]:
	scored: list[tuple[float, str]] = []
	for term, count in primary.items():
		if count < 4:
			continue
		contrast_count = contrast.get(term, 0)
		ratio = (count + 1.0) / (contrast_count + 1.0)
		score = ratio * math.log(count + 1.0)
		scored.append((score, term))
	scored.sort(reverse=True)
	return [term for _, term in scored[:top_n]]


def _top_common_terms(counter: Counter[str], top_n: int) -> list[str]:
	"""human readable hint: choose frequent local terms as fallback extraction cues when POS/NEG contrast is sparse."""

	return [term for term, count in counter.most_common() if count >= 3][:top_n]


def _build_bootstrap_signals(papers: list[PaperRecord]) -> BootstrapSignals:
	"""human readable hint: learn prompt and chunk-ranking cue terms from the current POS/NEG PDF examples."""

	pos_counter: Counter[str] = Counter()
	neg_counter: Counter[str] = Counter()
	all_counter: Counter[str] = Counter()

	for paper in papers:
		token_counter = Counter(_tokenize(paper.full_text))
		all_counter.update(token_counter)
		if paper.label == "POS":
			pos_counter.update(token_counter)
		else:
			neg_counter.update(token_counter)

	include_terms = tuple(_top_discriminative_terms(pos_counter, neg_counter, top_n=30))
	exclude_terms = tuple(_top_discriminative_terms(neg_counter, pos_counter, top_n=30))
	extraction_terms = tuple(include_terms[:20] or _top_common_terms(all_counter, top_n=20))
	return BootstrapSignals(
		include_terms=include_terms,
		exclude_terms=exclude_terms,
		extraction_terms=extraction_terms,
	)


def _write_csv(path: Path, rows: list[dict]) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	with path.open("w", encoding="utf-8", newline="") as handle:
		writer = csv.DictWriter(handle, fieldnames=["label", "source", "text"])
		writer.writeheader()
		for row in rows:
			writer.writerow({
				"label": row.get("label", ""),
				"source": row.get("source", ""),
				"text": row.get("text", ""),
			})


def _build_title_abstract_rows(papers: list[PaperRecord], signals: BootstrapSignals) -> list[dict]:
	rows: list[dict] = []
	for paper in papers:
		selected = _select_diverse_chunks(
			chunks=paper.chunks,
			paper=paper,
			score_fn=lambda chunk, paper_arg: _score_fulltext_chunk(chunk, paper_arg, signals),
			max_items=1,
		)
		chunk = selected[0] if selected else None
		snippet = _truncate_words(str(chunk.get("text") or paper.full_text), max_words=170)

		if paper.label == "POS":
			reason_signals = _extract_signals_for_reasoning(snippet, signals)
			signal_text = ", ".join(reason_signals) if reason_signals else "local positive-example alignment"
			reasoning = (
				"Positive example because the provided evidence shows "
				+ signal_text
				+ ", matching the target screening focus."
			)
		else:
			reasoning = _infer_negative_reason(paper.title, snippet, signals)

		rows.append(
			{
				"label": paper.label,
				"source": f"{paper.source} | {paper.pdf_path.name}",
				"text": f"REASONING: {reasoning} CONTENT: {paper.title}. {snippet}",
			}
		)

	return rows


def _build_full_text_rows(papers: list[PaperRecord], chunks_per_paper: int, signals: BootstrapSignals) -> list[dict]:
	rows: list[dict] = []
	for paper in papers:
		selected = _select_diverse_chunks(
			chunks=paper.chunks,
			paper=paper,
			score_fn=lambda chunk, paper_arg: _score_fulltext_chunk(chunk, paper_arg, signals),
			max_items=chunks_per_paper,
		)
		for rank, chunk in enumerate(selected, start=1):
			rows.append(
				{
					"label": paper.label,
					"source": f"{paper.source} | {paper.pdf_path.name} | {_page_tag(chunk)} | chunk_rank_{rank}",
					"text": _truncate_words(str(chunk.get("text") or "").strip(), max_words=260),
				}
			)
	return rows


def _build_data_extraction_rows(papers: list[PaperRecord], chunks_per_paper: int, signals: BootstrapSignals) -> list[dict]:
	rows: list[dict] = []
	for paper in papers:
		selected = _select_diverse_chunks(
			chunks=paper.chunks,
			paper=paper,
			score_fn=lambda chunk, paper_arg: _score_data_extraction_chunk(chunk, paper_arg, signals),
			max_items=chunks_per_paper,
		)
		for rank, chunk in enumerate(selected, start=1):
			chunk_text = str(chunk.get("text") or "").strip()
			tags = ", ".join(_infer_extraction_tags(chunk_text, signals))
			rows.append(
				{
					"label": paper.label,
					"source": f"{paper.source} | {paper.pdf_path.name} | {_page_tag(chunk)} | extraction_rank_{rank}",
					"text": f"EXTRACTION_SIGNAL: {tags}. CONTENT: {_truncate_words(chunk_text, max_words=260)}",
				}
			)
	return rows


def _study_tag_key(tag: str) -> str:
	"""human readable hint: convert configured study tags into suggested JSON exclusion keys."""

	key = re.sub(r"[^a-z0-9]+", "_", str(tag).strip().lower()).strip("_")
	return key


def _configured_exclusion_key_lines() -> str:
	"""human readable hint: render user-configured exclusion tags as prompt key suggestions."""

	keys = [_study_tag_key(tag) for tag in STUDY_TAGS_INCLUDE if _study_tag_key(tag)]
	if not keys:
		keys = ["wrong_publication_type", "insufficient_context"]
	lines = [f"- {key} (boolean)" for key in keys]
	for key in ["wrong_publication_type", "insufficient_context"]:
		if key not in keys:
			lines.append(f"- {key} (boolean)")
	return "\n".join(lines)


def _render_prompt_title_abstract(include_terms: list[str], exclude_terms: list[str]) -> str:
	include_text = ", ".join(include_terms[:18]) if include_terms else "positive-example terms from your local PDFs"
	exclude_text = ", ".join(exclude_terms[:18]) if exclude_terms else "review, simulation, non-adult"
	exclusion_key_lines = _configured_exclusion_key_lines()

	return f"""SCREENING PROMPT (Title/Abstract - Suggested Bootstrap Version)

# ROLE
You are an expert reviewer for the review topic defined by the local examples, knowledge base, and user_orchestrator.py.

# DOMAIN SIGNALS DERIVED FROM LOCAL EXAMPLE PDFS
- Include-leaning signals: {include_text}
- Exclude-leaning signals: {exclude_text}

# TASK
Review the title/abstract evidence and return a single flat JSON object.
Apply strict exclusion only for explicit evidence. If information is missing but the record is plausibly relevant, return \"NEUTRAL\" for uncertain context flags and for is_eligible.

# REQUIRED JSON KEYS
- step_by_step_deliberation (string)
{exclusion_key_lines}
- justification (string)
- is_eligible (boolean or \"NEUTRAL\")
- confidence_score (number between 0.0 and 1.0)
- exclusion_reason_category (string or null)

# OUTPUT RULES
- Output JSON only.
- Use double quotes for all keys and string values.
- Do not wrap output in Markdown.

# DATA
{{data}}
"""


def _render_prompt_full_text(include_terms: list[str], exclude_terms: list[str]) -> str:
	include_text = ", ".join(include_terms[:20]) if include_terms else "positive-example terms from your local PDFs"
	exclude_text = ", ".join(exclude_terms[:20]) if exclude_terms else "review, simulation, conceptual"
	exclusion_key_lines = _configured_exclusion_key_lines()

	return f"""SCREENING PROMPT (Full-Text - Suggested Bootstrap Version)

# ROLE
You are an expert reviewer for the review topic defined by the local examples, knowledge base, and user_orchestrator.py.

# DOMAIN SIGNALS DERIVED FROM LOCAL EXAMPLE PDFS
- Include-leaning signals: {include_text}
- Exclude-leaning signals: {exclude_text}

# TASK
Use full-text evidence to produce one strict JSON decision.
For full_text stage, is_eligible must be boolean (true/false), never neutral.

# REQUIRED JSON KEYS
- step_by_step_deliberation (string)
{exclusion_key_lines}
- seed_references (boolean or null)
- justification (string)
- is_eligible (boolean)
- confidence_score (number between 0.0 and 1.0)
- exclusion_reason_category (string or null)

# DECISION RULE
- Return false if any exclusion flag is true.
- Return true only when the evidence matches the inclusion logic defined by your prompt, tags, and knowledge base.

# OUTPUT RULES
- Output JSON only.
- Use double quotes for all keys and string values.
- Do not wrap output in Markdown.

# DATA
{{data}}
"""


def _render_prompt_data_extraction(include_terms: list[str], exclude_terms: list[str]) -> str:
	include_text = ", ".join(include_terms[:20]) if include_terms else "positive-example terms from your local PDFs"
	exclude_text = ", ".join(exclude_terms[:20]) if exclude_terms else "negative-example terms from your local PDFs"

	return f"""DATA EXTRACTION PROMPT (Suggested Bootstrap Version)

# ROLE
You are an expert reviewer extracting variables defined by the external extraction schema CSV.

# TERM HINTS FROM LOCAL EXAMPLE PDFS
- Frequent extraction-positive terms: {include_text}
- Frequent extraction-negative terms: {exclude_text}

# KB-DRIVEN RESPONSE SCHEMA
{{extraction_schema_instructions}}

# TASK
Extract only what is explicitly present in the text evidence.
If a value is not explicit, use the missing-value conventions from the KB-driven schema block.
Return one valid JSON object matching the schema exactly.

# OUTPUT RULES
- Output JSON only.
- Do not add Markdown fences.

# DATA
{{data}}
"""


def _build_prompt_suggestions(signals: BootstrapSignals) -> dict[str, str]:
	"""human readable hint: render suggested prompts from local data-derived cue terms."""

	include_terms = list(signals.include_terms)
	exclude_terms = list(signals.exclude_terms)
	return {
		"title_abstract": _render_prompt_title_abstract(include_terms=include_terms, exclude_terms=exclude_terms),
		"full_text": _render_prompt_full_text(include_terms=include_terms, exclude_terms=exclude_terms),
		"data_extraction": _render_prompt_data_extraction(include_terms=include_terms, exclude_terms=exclude_terms),
	}


def _load_papers(label: str, folder: Path, max_pages: int | None) -> list[PaperRecord]:
	papers: list[PaperRecord] = []
	pdf_paths = _list_pdfs(folder)

	for index, pdf_path in enumerate(pdf_paths, start=1):
		source, title = _parse_filename_metadata(pdf_path.name)
		pages = read_pdf_pages(str(pdf_path), max_pages=max_pages)
		full_text = "\n".join(page for page in pages if page).strip()
		if not full_text:
			print(f"[warn] Skipping empty PDF extraction: {pdf_path}")
			continue

		lang_code = detect_language_code(full_text[:12000]) or "en"
		language = "german" if lang_code.startswith("de") else "english"
		paper_id = f"{label.lower()}_{index:04d}_{_slugify(source)[:50]}"

		chunks = chunk_fulltext_sentences(
			paper_id=paper_id,
			title=title,
			full_text=full_text,
			language=language,
			page_texts=pages,
		)
		if not chunks:
			print(f"[warn] No chunks generated for: {pdf_path}")
			continue

		papers.append(
			PaperRecord(
				label=label,
				source=source,
				title=title,
				pdf_path=pdf_path,
				pages=pages,
				full_text=full_text,
				page_count=len(pages),
				language=language,
				chunks=chunks,
			)
		)

	return papers


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description=(
			"Build stage-specific knowledge-base CSVs and first-pass prompt suggestions "
			"from POS/NEG example PDFs."
		)
	)
	repo_default = Path(__file__).resolve().parents[2]

	parser.add_argument("--repo-root", type=Path, default=repo_default)
	parser.add_argument("--pos-dir", type=Path, default=Path("papers/pos_examples"))
	parser.add_argument("--neg-dir", type=Path, default=Path("papers/neg_examples"))
	parser.add_argument("--kb-dir", type=Path, default=Path("knowledge-base"))
	parser.add_argument("--prompt-dir", type=Path, default=Path("config"))
	parser.add_argument("--fulltext-chunks-per-paper", type=int, default=3)
	parser.add_argument("--dataextraction-chunks-per-paper", type=int, default=2)
	parser.add_argument("--max-pages", type=int, default=None)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	repo_root = args.repo_root.resolve()
	pos_dir = (repo_root / args.pos_dir).resolve()
	neg_dir = (repo_root / args.neg_dir).resolve()
	kb_dir = (repo_root / args.kb_dir).resolve()
	prompt_dir = (repo_root / args.prompt_dir).resolve()

	print("[info] Loading POS example PDFs from:", pos_dir)
	pos_papers = _load_papers(label="POS", folder=pos_dir, max_pages=args.max_pages)
	print(f"[info] Loaded POS papers: {len(pos_papers)}")

	print("[info] Loading NEG example PDFs from:", neg_dir)
	neg_papers = _load_papers(label="NEG", folder=neg_dir, max_pages=args.max_pages)
	print(f"[info] Loaded NEG papers: {len(neg_papers)}")

	if not pos_papers or not neg_papers:
		raise RuntimeError(
			"At least one POS and one NEG PDF with extractable text are required to bootstrap stage KBs."
		)

	all_papers = pos_papers + neg_papers
	signals = _build_bootstrap_signals(all_papers)

	title_rows = _build_title_abstract_rows(all_papers, signals)
	full_rows = _build_full_text_rows(all_papers, chunks_per_paper=max(1, args.fulltext_chunks_per_paper), signals=signals)
	data_rows = _build_data_extraction_rows(all_papers, chunks_per_paper=max(1, args.dataextraction_chunks_per_paper), signals=signals)

	kb_dir.mkdir(parents=True, exist_ok=True)
	prompt_dir.mkdir(parents=True, exist_ok=True)

	title_kb_path = kb_dir / "title_abstract_pos-neg_examples.csv"
	full_kb_path = kb_dir / "full_text_pos-neg_examples.csv"
	data_kb_path = kb_dir / "data_extraction_pos-neg_examples.csv"

	_write_csv(title_kb_path, title_rows)
	_write_csv(full_kb_path, full_rows)
	_write_csv(data_kb_path, data_rows)

	prompts = _build_prompt_suggestions(signals)
	prompt_paths = {
		"title_abstract": prompt_dir / "prompt_script_title_abstract_suggested.txt",
		"full_text": prompt_dir / "prompt_script_full_text_suggested.txt",
		"data_extraction": prompt_dir / "prompt_script_data_extraction_suggested.txt",
	}
	for stage, prompt_text in prompts.items():
		prompt_paths[stage].write_text(prompt_text, encoding="utf-8")

	summary = {
		"generated_at_utc": datetime.now(timezone.utc).isoformat(),
		"input": {
			"pos_dir": str(pos_dir),
			"neg_dir": str(neg_dir),
			"pos_papers_loaded": len(pos_papers),
			"neg_papers_loaded": len(neg_papers),
			"max_pages": args.max_pages,
		},
		"output": {
			"title_abstract_kb": str(title_kb_path),
			"title_abstract_rows": len(title_rows),
			"full_text_kb": str(full_kb_path),
			"full_text_rows": len(full_rows),
			"data_extraction_kb": str(data_kb_path),
			"data_extraction_rows": len(data_rows),
			"prompt_script_title_abstract_suggested": str(prompt_paths["title_abstract"]),
			"prompt_script_full_text_suggested": str(prompt_paths["full_text"]),
			"prompt_script_data_extraction_suggested": str(prompt_paths["data_extraction"]),
		},
		"data_derived_signals": {
			"include_terms": list(signals.include_terms),
			"exclude_terms": list(signals.exclude_terms),
			"extraction_terms": list(signals.extraction_terms),
		},
	}
	summary_path = kb_dir / "kb_bootstrap_summary.json"
	summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

	print("[done] Wrote knowledge-base CSVs:")
	print("        ", title_kb_path)
	print("        ", full_kb_path)
	print("        ", data_kb_path)
	print("[done] Wrote suggested prompt scripts:")
	print("        ", prompt_paths["title_abstract"])
	print("        ", prompt_paths["full_text"])
	print("        ", prompt_paths["data_extraction"])
	print("[done] Wrote summary:", summary_path)


if __name__ == "__main__":
	main()
