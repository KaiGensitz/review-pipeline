"""Direct run: python -m pipeline.additions.input_trace

Reconstruct and verify the exact per-paper LLM input text using stored SHA-256 hashes.

Usage examples:
- python -m pipeline.additions.input_trace --paper-id 697294 --stage title_abstract
- python -m pipeline.additions.input_trace --input-hash <sha256> --stage full_text --show-full-prompt
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from config.user_orchestrator import (
    CURRENT_STAGE,
    DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES,
    DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_LOW_PRIORITY_PATTERNS,
    LLM_SETTINGS,
    PATH_SETTINGS,
    PROMPT_FILES,
)
from pipeline.integrations.embedding_utils import (
    normalize_extracted_text,
    normalize_extracted_text_for_llm,
)
from pipeline.core.metadata_aliases import read_metadata_value
from pipeline.core.extraction_io import PerPaperFileIndex, SupplementalCitedEvidenceLoader
from pipeline.core.extraction_schema import (
    DynamicExtractionSchema,
    ExtractionVariable,
    MISSING_TEXT_VALUES,
    MISSING_TEXT_VALUE,
    SchemaEvidenceHintBuilder,
    SchemaEvidenceHintConfig,
)

ELIGIBILITY_CRITERIA_PLACEHOLDER = "{eligibility_criteria}"
DEFAULT_OUTPUT_ROOT = Path(PATH_SETTINGS.get("output_root", "output"))
DATA_EXTRACTION_TRACE_STOPWORDS = {
    "about",
    "above",
    "after",
    "also",
    "abstract",
    "available",
    "baseline-characteristics",
    "column",
    "compute",
    "consensus",
    "criteria",
    "data",
    "domain",
    "exactly",
    "evidence",
    "explicit",
    "explicitly",
    "extract",
    "field",
    "from",
    "given",
    "group",
    "groups",
    "information",
    "label",
    "missing",
    "not",
    "overall",
    "paper",
    "participant",
    "participants",
    "population",
    "reported",
    "results",
    "return",
    "summary",
    "summaries",
    "prefer",
    "schema",
    "source",
    "stated",
    "study",
    "table",
    "tables",
    "text",
    "that",
    "this",
    "value",
    "values",
    "variable",
    "when",
    "with",
    "preserving",
    "measure",
}


def _sha256_text(value: str) -> str:
    """human readable hint: compute a stable fingerprint of any text."""

    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _candidate_stage_dirs(stage: str) -> list[Path]:
    """human readable hint: find plausible output folders for a stage across naming variants."""

    root = DEFAULT_OUTPUT_ROOT
    preferred: list[Path] = []
    primary = root / stage
    legacy = root / f"{stage}_"
    if primary.exists() and primary.is_dir():
        preferred.append(primary)
    if legacy.exists() and legacy.is_dir() and legacy not in preferred:
        preferred.append(legacy)

    discovered: list[Path] = []
    if root.exists():
        for child in sorted(root.iterdir()):
            if not child.is_dir():
                continue
            if child.name.startswith(stage) and child not in preferred and child not in discovered:
                discovered.append(child)

    if preferred or discovered:
        return preferred + discovered
    return [primary]


def _iter_eligibility_files(stage: str, stage_dirs: list[Path]) -> list[Path]:
    """human readable hint: collect all non-split eligibility files from candidate stage folders."""

    files: list[Path] = []
    for stage_dir in stage_dirs:
        candidates = list(stage_dir.glob(f"{stage}_*_eligibility_*.jsonl"))
        for path in candidates:
            name = path.name
            if (
                "eligibility_select" in name
                or "eligibility_irrelevant" in name
                or "eligibility_included" in name
                or "eligibility_excluded" in name
            ):
                continue
            files.append(path)
    return files


def _latest_eligibility_file(stage: str) -> Path:
    """human readable hint: pick the latest eligibility file (excluding split files)."""

    stage_dirs = _candidate_stage_dirs(stage)
    candidates = _iter_eligibility_files(stage, stage_dirs)
    if not candidates:
        searched = ", ".join(str(p) for p in stage_dirs)
        raise FileNotFoundError(f"No eligibility JSONL found for stage '{stage}'. Searched: {searched}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _find_record(eligibility_file: Path, paper_id: str | None, input_hash: str | None) -> dict:
    """human readable hint: find one paper in eligibility output by paper_id or stored input hash."""

    target_id = str(paper_id or "").strip()
    target_id_normalized = target_id.lstrip("#")

    with eligibility_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict) or payload.get("meta"):
                continue

            pid = str(payload.get("paper_id", "")).strip()
            diagnostics = payload.get("diagnostics", {}) if isinstance(payload.get("diagnostics"), dict) else {}
            stored_hash = str(diagnostics.get("llm_input_sha256", "")).strip().lower()

            if target_id and (pid == target_id or pid.lstrip("#") == target_id_normalized):
                return payload
            if input_hash and stored_hash and stored_hash == input_hash.lower().strip():
                return payload

    target = f"paper_id='{paper_id}'" if paper_id else f"input_hash='{input_hash}'"
    raise ValueError(f"No eligibility record matched {target} in {eligibility_file}.")


def _strip_author_mentions(text: str, authors: str) -> str:
    """human readable hint: mirror screening redaction logic for exact reproducibility."""

    value = (text or "").strip()
    author_block = (authors or "").strip()
    if not value or not author_block:
        return value

    patterns: list[str] = [author_block]
    for candidate in re.split(r"[;\n|]", author_block):
        c = candidate.strip()
        if c:
            patterns.append(c)

    redacted = value
    for candidate in patterns:
        redacted = re.sub(re.escape(candidate), " ", redacted, flags=re.IGNORECASE)

    return re.sub(r"\s+", " ", redacted).strip()


def _normalize_section_label(value: str | None) -> str | None:
    """Approximate runtime section normalization for prompt prefix reproduction."""

    label = (value or "").strip().lower()
    if not label:
        return None
    if label in {"introduction", "method", "results", "discussion", "conclusion"}:
        return label
    if label in {"background"}:
        return "introduction"
    if label in {"methods", "methodology", "materials and methods", "study design"}:
        return "method"
    if label in {"findings"}:
        return "results"
    if label in {"conclusions", "summary"}:
        return "conclusion"
    if label in {"references", "reference", "bibliography", "acknowledgements", "acknowledgments"}:
        return "reference"
    return None


def _infer_chunk_section_label(chunk: dict) -> str | None:
    """Infer section label using explicit chunk metadata and heading cues."""

    explicit = _normalize_section_label(str(chunk.get("section") or ""))
    if explicit:
        return explicit

    text_prefix = " ".join(str(chunk.get("text") or "").strip().split()[:20])
    patterns = {
        "introduction": r"\b(introduction|background)\b",
        "method": r"\b(methods?|methodology|materials?\s+and\s+methods?|study\s+design)\b",
        "results": r"\b(results?|findings)\b",
        "discussion": r"\bdiscussion\b",
        "conclusion": r"\b(conclusion|conclusions|summary)\b",
        "reference": r"\b(references?|bibliography|acknowledg(e)?ments?)\b",
    }
    for label, pattern in patterns.items():
        if re.search(pattern, text_prefix, flags=re.IGNORECASE):
            return label
    return None


def _format_chunks_for_prompt(
    stage: str,
    paper_id: str,
    title: str,
    authors: str,
    chunks: list[dict],
    detected_language_code: str | None = None,
) -> str:
    """human readable hint: rebuild the same context text format sent to the model."""

    if not chunks:
        return ""

    title_text = (title or "").strip()
    if stage in {"title_abstract", "full_text"}:
        title_text = _strip_author_mentions(title_text, authors)

    parts: list[str] = [f"Paper ID: {paper_id}", f"Title: {title_text}".strip()]

    if stage == "full_text" and detected_language_code:
        parts.append(f"Detected full-text language code (auto): {detected_language_code}")

    prompt_chunks = [
        chunk for chunk in chunks if str(chunk.get("kind") or "") != "title"
    ]

    for idx, chunk in enumerate(prompt_chunks, start=1):
        text = str(chunk.get("text", "")).strip()
        if stage in {"title_abstract", "full_text"}:
            text = _strip_author_mentions(text, authors)
        if stage == "full_text":
            text = normalize_extracted_text(text)

        prefix_parts = [f"Chunk {idx}"]
        if stage in {"full_text", "data_extraction"}:
            section = _infer_chunk_section_label(chunk)
            if section:
                prefix_parts.append(f"section {section}")
        page_start = chunk.get("page_start")
        page_end = chunk.get("page_end")
        if isinstance(page_start, int) and isinstance(page_end, int):
            if page_start == page_end:
                prefix_parts.append(f"page {page_start}")
            else:
                prefix_parts.append(f"pages {page_start}-{page_end}")
        sentence_count = chunk.get("sentence_count")
        if isinstance(sentence_count, int) and sentence_count > 0:
            prefix_parts.append(f"sentences {sentence_count}")
        prefix = "[" + ", ".join(prefix_parts) + "]"
        parts.append(f"{prefix}\n{text}")
    return "\n\n".join(parts)


def _load_data_extraction_full_text_context(folder: Path, paper_id: str, title: str) -> str:
    """human readable hint: rebuild the full normalized text evidence block used by data extraction."""

    file_index = PerPaperFileIndex(folder, paper_id=paper_id)
    candidates: list[Path] = []
    for name in ("full_text_normalized.txt", "data_extraction_normalized.txt"):
        candidates.extend(file_index.candidates(name, paper_id=paper_id))
    for candidate in candidates:
        if not candidate.exists():
            continue
        raw_text = candidate.read_text(encoding="utf-8")
        marker = "=== normalized_full_text ==="
        marker_index = raw_text.find(marker)
        if marker_index >= 0:
            raw_text = raw_text[marker_index + len(marker):]
        # human readable hint: mirror the extraction-time normalization for full-text evidence.
        normalized_text = normalize_extracted_text_for_llm(raw_text).strip()
        if not normalized_text:
            continue
        max_words = int(LLM_SETTINGS.get("data_extraction_full_text_max_words", 0) or 0)
        if max_words > 0:
            words = normalized_text.split()
            if len(words) > max_words:
                normalized_text = " ".join(words[:max_words])
        parts = [f"Paper ID: {paper_id}"]
        if title:
            parts.append(f"Title: {title.strip()}")
        evidence_hints = _build_data_extraction_schema_evidence_hints(normalized_text)
        if evidence_hints:
            parts.append(evidence_hints)
        parts.append("[Full Normalized Text]\n" + normalized_text)
        supplemental_cited_evidence = SupplementalCitedEvidenceLoader.from_user_config().load_for_folder(folder)
        if supplemental_cited_evidence:
            parts.append(supplemental_cited_evidence)
        return "\n\n".join(parts)
    return ""


def _build_data_extraction_schema_evidence_hints(normalized_text: str) -> str:
    """human readable hint: mirror the runtime schema-guided evidence map in trace outputs."""

    if not bool(LLM_SETTINGS.get("data_extraction_schema_evidence_hints", True)):
        return ""
    try:
        schema = DynamicExtractionSchema.from_kb()
    except Exception:
        return ""
    config = SchemaEvidenceHintConfig(
        enabled=True,
        snippets_per_variable=max(0, int(LLM_SETTINGS.get("data_extraction_evidence_hints_per_variable", 2) or 0)),
        max_snippet_chars=max(120, int(LLM_SETTINGS.get("data_extraction_evidence_hint_max_chars", 420) or 420)),
        max_total_chars=max(1000, int(LLM_SETTINGS.get("data_extraction_evidence_hints_max_total_chars", 18000) or 18000)),
        context_lines=max(0, int(LLM_SETTINGS.get("data_extraction_evidence_hint_context_lines", 1) or 0)),
        alias_map=DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES,
        low_priority_patterns=tuple(DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_LOW_PRIORITY_PATTERNS),
    )
    # human readable hint: use the same schema-owned builder as runtime extraction input assembly.
    return SchemaEvidenceHintBuilder(schema.variables, config).build(normalized_text)


def _iter_selected_chunk_candidates(eligibility_file: Path, stage: str) -> list[Path]:
    """human readable hint: prefer selected_chunks files that match the eligibility run tag."""

    stage_dir = eligibility_file.parent
    names: list[str] = []

    for token in (
        "eligibility_select_",
        "eligibility_irrelevant_",
        "eligibility_included_",
        "eligibility_excluded_",
        "eligibility_",
    ):
        if token in eligibility_file.name:
            names.append(eligibility_file.name.replace(token, "selected_chunks_", 1))

    stem = eligibility_file.stem
    suffix_match = re.search(
        r"eligibility(?:_(?:select|irrelevant|included|excluded))?_(.+)$",
        stem,
    )
    if suffix_match:
        suffix = suffix_match.group(1)
        names.append(f"{stage}_selected_chunks_{suffix}.jsonl")

    candidates: list[Path] = []
    seen: set[Path] = set()

    for name in names:
        candidate = stage_dir / name
        if candidate.exists() and candidate not in seen:
            candidates.append(candidate)
            seen.add(candidate)

    # Conservative fallback: newest stage selected-chunks outputs in this stage folder.
    for candidate in sorted(
        stage_dir.glob(f"{stage}_*_selected_chunks_*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    ):
        if candidate not in seen:
            candidates.append(candidate)
            seen.add(candidate)

    return candidates


def _load_selected_chunks_from_stage_output(
    eligibility_file: Path,
    stage: str,
    paper_id: str,
) -> list[dict] | None:
    """human readable hint: read selected chunks from stage output first to avoid per-folder drift."""

    target = str(paper_id or "").strip()
    target_normalized = target.lstrip("#")

    for chunks_file in _iter_selected_chunk_candidates(eligibility_file, stage):
        try:
            with chunks_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    if not isinstance(payload, dict) or payload.get("meta"):
                        continue
                    pid = str(payload.get("paper_id", "")).strip()
                    if not pid:
                        continue
                    if pid != target and pid.lstrip("#") != target_normalized:
                        continue
                    selected = payload.get("selected_chunks")
                    if isinstance(selected, list):
                        return selected
        except Exception:
            continue

    return None


def _detected_language_hint(diagnostics: dict) -> str | None:
    """human readable hint: mirror runtime language hint line added to full_text prompts."""

    trace = diagnostics.get("selection_trace")
    if not isinstance(trace, dict):
        return None
    raw = trace.get("detected_language_code")
    if not isinstance(raw, str):
        return None
    value = raw.strip().lower()
    return value or None


def _title_abstract_context(stage: str, paper_id: str) -> tuple[str, list[dict]]:
    """human readable hint: title_abstract stores the full model context in selected_chunks output."""

    files: list[Path] = []
    for stage_root in _candidate_stage_dirs(stage):
        files.extend(stage_root.glob(f"{stage}_*_selected_chunks_*.jsonl"))
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)

    for file in files:
        with file.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict) or payload.get("meta"):
                    continue
                if str(payload.get("paper_id", "")).strip() != paper_id:
                    continue
                selected = payload.get("selected_chunks") or []
                if not selected:
                    continue
                context_text = str(selected[0].get("text", ""))
                return context_text, selected

    raise ValueError(f"Could not reconstruct title_abstract input for paper_id='{paper_id}'.")


def _load_folder_metadata(folder: Path) -> dict:
    file_index = PerPaperFileIndex(folder)
    artifact_candidates: list[Path] = []
    for stage_name in ("full_text", "data_extraction"):
        artifact_candidates.extend(file_index.artifact_candidates(stage_name))
    for artifact_path in artifact_candidates:
        if not artifact_path.exists():
            continue
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                metadata = payload.get("metadata")
                if isinstance(metadata, dict):
                    return metadata
                # Backward compatibility for non-nested payload variants.
                return payload
        except Exception:
            continue

    return {}


def _extract_paper_id(row: dict) -> str:
    """human readable hint: read the paper ID through user-configured metadata aliases."""

    return read_metadata_value(row, "paper_id").lstrip("#")


def _find_paper_folder(stage: str, paper_id: str, csv_root: Path) -> Path:
    """human readable hint: locate the per-paper folder by matching configured paper IDs in metadata."""

    bases: list[Path] = []
    if stage == "full_text":
        bases.append(csv_root / "per_paper_full_text")
    elif stage == "data_extraction":
        bases.append(csv_root / "per_paper_data_extraction")
    else:
        raise ValueError(f"Folder-based lookup is only valid for full_text/data_extraction, got '{stage}'.")

    for base in bases:
        if not base.exists():
            continue
        for folder in sorted(base.iterdir()):
            if not folder.is_dir():
                continue
            metadata = _load_folder_metadata(folder)
            folder_paper_id = _extract_paper_id(metadata)
            if folder_paper_id and folder_paper_id == paper_id.lstrip("#"):
                return folder

    raise FileNotFoundError(f"Could not find folder for paper_id='{paper_id}' in per-paper inputs.")


def _load_selected_chunks(folder: Path, stage: str, paper_id: str) -> list[dict]:
    file_index = PerPaperFileIndex(folder, paper_id=paper_id)
    chunks_candidates = file_index.selected_chunk_candidates(stage, paper_id=paper_id)

    for chunks_path in chunks_candidates:
        if not chunks_path.exists():
            continue
        with chunks_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict) or payload.get("meta"):
                    continue
                if str(payload.get("paper_id", "")).strip() == paper_id:
                    selected = payload.get("selected_chunks")
                    if isinstance(selected, list):
                        return selected

    artifact_candidates: list[Path] = []
    for stage_name in (stage, "full_text"):
        artifact_candidates.extend(file_index.artifact_candidates(stage_name, paper_id=paper_id))
    for artifact_path in artifact_candidates:
        if not artifact_path.exists():
            continue
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        selected = payload.get("selected_chunks")
        if isinstance(selected, list):
            return selected

    if not any(path.exists() for path in chunks_candidates):
        raise FileNotFoundError(
            f"Missing selected chunks sources for stage='{stage}', paper_id='{paper_id}' and compact artifacts in {folder}"
        )
    raise ValueError(f"No selected chunks found for paper_id='{paper_id}' in selected chunk sidecars under {folder}.")


def _folder_stage_context(
    stage: str,
    paper_id: str,
    csv_root: Path,
    diagnostics: dict | None = None,
    eligibility_file: Path | None = None,
    fallback_metadata: dict | None = None,
) -> tuple[str, list[dict]]:
    """human readable hint: rebuild full_text/data_extraction model context from metadata + selected chunks."""

    folder = _find_paper_folder(stage, paper_id, csv_root)
    metadata = _load_folder_metadata(folder)
    merged_metadata: dict = {}
    if isinstance(fallback_metadata, dict):
        merged_metadata.update(fallback_metadata)
    if isinstance(metadata, dict):
        merged_metadata.update(metadata)

    title = read_metadata_value(merged_metadata, "title")
    authors = read_metadata_value(merged_metadata, "authors")

    chunks = None
    if eligibility_file is not None:
        chunks = _load_selected_chunks_from_stage_output(eligibility_file, stage, paper_id)
    if chunks is None:
        chunks = _load_selected_chunks(folder, stage, paper_id)

    if stage == "data_extraction":
        evidence_mode = str(LLM_SETTINGS.get("data_extraction_evidence_mode", "full_text") or "full_text").strip().lower()
        if evidence_mode == "full_text":
            full_text_context = _load_data_extraction_full_text_context(folder, paper_id, title)
            if full_text_context:
                return full_text_context, chunks

    language_hint = _detected_language_hint(diagnostics or {}) if stage == "full_text" else None
    context_text = _format_chunks_for_prompt(
        stage,
        paper_id,
        title,
        authors,
        chunks,
        detected_language_code=language_hint,
    )
    return context_text, chunks


def _reconstruct_context(
    stage: str,
    paper_id: str,
    csv_root: Path,
    diagnostics: dict | None = None,
    eligibility_file: Path | None = None,
    fallback_metadata: dict | None = None,
) -> tuple[str, list[dict]]:
    """human readable hint: stage-aware reconstruction of exact model context with selected chunks."""

    if stage == "title_abstract":
        return _title_abstract_context(stage, paper_id)
    return _folder_stage_context(
        stage,
        paper_id,
        csv_root,
        diagnostics=diagnostics,
        eligibility_file=eligibility_file,
        fallback_metadata=fallback_metadata,
    )


def _format_metric_value(value: object, digits: int = 6) -> str:
    """human readable hint: normalize metric values for compact trace display."""

    if value is None:
        return "NA"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    text = str(value).strip()
    return text if text else "NA"


def _selection_diagnostics_lines(diagnostics: dict) -> list[str]:
    """human readable hint: summarize retrieval diagnostics used for chunk selection."""

    lines = [
        "=== Selection Diagnostics ===",
        f"total_chunks: {diagnostics.get('total_chunks', 'NA')}",
        f"selected_count: {diagnostics.get('selected_count', 'NA')}",
        f"top_k: {diagnostics.get('top_k', 'NA')}",
        f"score_threshold: {diagnostics.get('score_threshold', 'NA')}",
    ]

    selected_score_stats = diagnostics.get("selected_score_stats")
    if isinstance(selected_score_stats, dict):
        lines.append(
            "selected_score_stats: "
            + json.dumps(selected_score_stats, ensure_ascii=False, sort_keys=True)
        )

    selected_page_coverage = diagnostics.get("selected_page_coverage")
    if isinstance(selected_page_coverage, dict):
        lines.append(
            "selected_page_coverage: "
            + json.dumps(selected_page_coverage, ensure_ascii=False, sort_keys=True)
        )

    selection_trace = diagnostics.get("selection_trace")
    if isinstance(selection_trace, dict):
        lines.append(
            "selection_trace: "
            + json.dumps(selection_trace, ensure_ascii=False, sort_keys=True)
        )

    return lines


def _selected_chunk_confidence_lines(chunks: list[dict]) -> list[str]:
    """human readable hint: show per-chunk ranking and certainty used by retrieval."""

    lines = ["=== Selected Chunk Confidence Trace ==="]
    if not chunks:
        lines.append("No selected chunks found for this paper.")
        return lines

    for idx, chunk in enumerate(chunks, start=1):
        chunk_id = str(chunk.get("chunk_id") or "").strip() or "NA"
        kind = str(chunk.get("kind") or "").strip() or "NA"
        rank = _format_metric_value(chunk.get("retrieval_rank"), digits=0)
        score = _format_metric_value(chunk.get("relevance_score", chunk.get("score")), digits=6)
        pos_score = _format_metric_value(
            chunk.get("positive_alignment_score", chunk.get("pos_score")), digits=6
        )
        neg_score = _format_metric_value(
            chunk.get("negative_alignment_score", chunk.get("neg_score")), digits=6
        )
        certainty_label = str(chunk.get("certainty_label") or "NA")
        certainty_pct = _format_metric_value(chunk.get("certainty_percentile"), digits=4)
        hybrid_score = _format_metric_value(chunk.get("hybrid_score"), digits=6)
        sentence_count = _format_metric_value(chunk.get("sentence_count"), digits=0)
        word_count = _format_metric_value(chunk.get("word_count"), digits=0)
        readability = _format_metric_value(chunk.get("readability_score"), digits=4)

        page_start = chunk.get("page_start")
        page_end = chunk.get("page_end")
        if isinstance(page_start, int) and isinstance(page_end, int):
            page_span = str(page_start) if page_start == page_end else f"{page_start}-{page_end}"
        else:
            page_span = "NA"

        lines.append(
            f"Chunk {idx}: id={chunk_id} kind={kind} pages={page_span} rank={rank} "
            f"score={score} certainty={certainty_label} pct={certainty_pct}"
        )
        lines.append(
            f"  pos_score={pos_score} neg_score={neg_score} hybrid_score={hybrid_score} "
            f"sentence_count={sentence_count} word_count={word_count} readability={readability}"
        )

        sources = chunk.get("selection_sources")
        if isinstance(sources, list) and sources:
            rendered_sources = ", ".join(str(item) for item in sources)
            lines.append(f"  selection_sources: {rendered_sources}")

    return lines


def _resolve_prompt_snapshot(stage: str, campaign_id: str, stage_dirs: list[Path]) -> Path | None:
    """human readable hint: locate the persisted prompt snapshot for a campaign when available."""

    if not campaign_id:
        return None
    modern_pattern = f"{stage}_prompt_template_*_{campaign_id}.txt"
    legacy_name = f"{stage}_prompt_template_{campaign_id}.txt"
    modern_candidates: list[Path] = []
    for folder in stage_dirs:
        modern_candidates.extend(folder.glob(modern_pattern))

    if modern_candidates:
        return max(modern_candidates, key=lambda path: path.stat().st_mtime)

    for folder in stage_dirs:
        candidate = folder / legacy_name
        if candidate.exists():
            return candidate
    return None


def _load_prompt_template(stage: str, campaign_id: str = "") -> tuple[str, str]:
    """human readable hint: mirror runtime prompt assembly with optional eligibility criteria injection."""

    stage_dirs = _candidate_stage_dirs(stage)
    snapshot = _resolve_prompt_snapshot(stage, campaign_id, stage_dirs)
    template_path = snapshot if snapshot else PROMPT_FILES[stage]
    prompt_template = template_path.read_text(encoding="utf-8")
    source_label = str(template_path)

    # Snapshot files are written from runtime's post-injection prompt and should be used as-is.
    if snapshot:
        return prompt_template.strip(), source_label

    if ELIGIBILITY_CRITERIA_PLACEHOLDER not in prompt_template:
        return prompt_template.strip(), source_label

    configured_path = PATH_SETTINGS.get("eligibility_criteria_file")
    if not configured_path:
        return prompt_template.replace(ELIGIBILITY_CRITERIA_PLACEHOLDER, "").strip(), source_label

    criteria_path = Path(configured_path)
    if not criteria_path.exists():
        return prompt_template.replace(ELIGIBILITY_CRITERIA_PLACEHOLDER, "").strip(), source_label

    criteria_text = criteria_path.read_text(encoding="utf-8").strip()
    return prompt_template.replace(ELIGIBILITY_CRITERIA_PLACEHOLDER, criteria_text).strip(), source_label


def _load_jsonl_payload(path: Path) -> dict[str, Any] | None:
    """human readable hint: read the first non-meta JSONL payload from a stage output file."""

    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict) and not payload.get("meta"):
                return payload
    return None


def _is_missing_extraction_value(value: Any, variable: ExtractionVariable) -> bool:
    """human readable hint: classify missing extraction values using the same type defaults as the schema."""

    if value is None:
        return True
    if variable.variable_type == "boolean":
        if isinstance(value, bool):
            return value is False
        return str(value).strip().casefold() in MISSING_TEXT_VALUES | {"false", "0", "no", "n"}
    if variable.variable_type == "list":
        if isinstance(value, list):
            return len([item for item in value if str(item).strip()]) == 0
        return str(value).strip().casefold() in MISSING_TEXT_VALUES
    return str(value).strip().casefold() in MISSING_TEXT_VALUES


def _value_for_variable(extracted: dict[str, Any], variable: ExtractionVariable) -> Any:
    """human readable hint: fetch one schema variable value from nested extraction JSON."""

    domain_payload = extracted.get(variable.domain)
    if not isinstance(domain_payload, dict):
        return None
    return domain_payload.get(variable.value_key)


def _quote_for_variable(extracted: dict[str, Any], variable: ExtractionVariable) -> str:
    """human readable hint: fetch one schema variable quote from nested extraction JSON."""

    domain_payload = extracted.get(variable.domain)
    if not isinstance(domain_payload, dict):
        return ""
    quote = domain_payload.get(variable.quote_key)
    return str(quote or "").strip()


def _trace_search_terms_for_variable(variable: ExtractionVariable) -> list[str]:
    """human readable hint: derive lightweight evidence-search terms from schema text without study-specific code."""

    # human readable hint: input traces should audit with the same generic schema terms used in runtime evidence hints.
    config = SchemaEvidenceHintConfig(alias_map=DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES)
    return SchemaEvidenceHintBuilder([variable], config).terms_for_variable(variable)[:10]


def _line_hits_for_terms(text: str, terms: list[str], max_hits: int = 5) -> list[tuple[int, str]]:
    """human readable hint: find short normalized-text snippets that might explain a missing field."""

    if not text.strip() or not terms:
        return []
    pattern = re.compile("|".join(r"(?<![A-Za-z0-9])" + re.escape(term) + r"(?![A-Za-z0-9])" for term in terms), re.I)
    hits: list[tuple[int, str]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        cleaned = re.sub(r"\s+", " ", line).strip()
        if not cleaned or not pattern.search(cleaned):
            continue
        hits.append((line_number, cleaned[:500]))
        if len(hits) >= max_hits:
            break
    return hits


def _read_normalized_text(folder: Path) -> tuple[Path | None, str]:
    """human readable hint: read the normalized full text file that extraction used as evidence."""

    file_index = PerPaperFileIndex(folder)
    candidates: list[Path] = []
    for name in ("full_text_normalized.txt", "data_extraction_normalized.txt"):
        candidates.extend(file_index.candidates(name))
    for candidate in candidates:
        if candidate.exists():
            return candidate, candidate.read_text(encoding="utf-8")
    return None, ""


def _iter_data_extraction_output_dirs(stage_root: Path) -> list[Path]:
    """human readable hint: list per-paper data-extraction output folders that contain JSONL results."""

    if not stage_root.exists():
        return []
    return [
        folder
        for folder in sorted(stage_root.iterdir(), key=lambda path: path.name)
        if folder.is_dir() and (folder / "data_extraction_results.jsonl").exists()
    ]


def _diagnose_mismatch(
    context_ok: bool,
    prompt_template_ok: bool,
    full_prompt_ok: bool,
    stored_full_prompt_hash: str,
    full_prompt: str,
) -> str:
    """human readable hint: classify the likely cause of hash mismatches for operator debugging."""

    if context_ok and prompt_template_ok and full_prompt_ok:
        return "none"
    if not context_ok:
        return "context_drift"
    if not prompt_template_ok:
        return "prompt_template_drift"

    normalized_hash = _sha256_text(full_prompt.strip())
    if stored_full_prompt_hash and stored_full_prompt_hash == normalized_hash and not full_prompt_ok:
        return "normalization_only_drift"
    return "full_prompt_assembly_drift"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct and verify per-paper model input text by hash.")
    parser.add_argument("--stage", default=CURRENT_STAGE, help="Pipeline stage (title_abstract | full_text | data_extraction)")
    parser.add_argument("--paper-id", help="Paper ID from the configured metadata aliases")
    parser.add_argument("--input-hash", help="Stored llm_input_sha256 to search for")
    parser.add_argument("--eligibility-file", help="Optional explicit eligibility JSONL path")
    parser.add_argument(
        "--all-data-extraction-output",
        action="store_true",
        help="Create one trace per paper from output/data_extraction results without requiring eligibility JSONL.",
    )
    parser.add_argument(
        "--stage-output-dir",
        help="Optional stage output folder to inspect, e.g. output/data_extraction_v7.",
    )
    parser.add_argument("--show-full-prompt", action="store_true", help="Also output merged prompt (template + evidence)")
    parser.add_argument("--output", help="Optional explicit output .txt path")
    args = parser.parse_args()

    if args.all_data_extraction_output:
        return args

    if not args.paper_id and not args.input_hash:
        parser.error("Provide either --paper-id or --input-hash.")
    return args


class InputTraceRunner:
    """human readable hint: one-class trace utility that reconstructs one paper input and verifies its hashes."""

    def __init__(self, stage: str = CURRENT_STAGE) -> None:
        """human readable hint: __init__ stores the default stage used when CLI arguments omit --stage."""

        self.stage = stage

    def run(self, args: argparse.Namespace | None = None) -> None:
        """human readable hint: execute the full trace workflow from eligibility record lookup to report writing."""

        args = args or _parse_args()
        stage = str(args.stage).strip() if getattr(args, "stage", None) else self.stage

        if bool(getattr(args, "all_data_extraction_output", False)):
            self.run_all_data_extraction_output(args)
            return

        if stage not in {"title_abstract", "full_text", "data_extraction"}:
            raise ValueError(f"Unsupported stage '{stage}'.")

        eligibility_file = Path(args.eligibility_file) if args.eligibility_file else _latest_eligibility_file(stage)
        if not eligibility_file.exists():
            raise FileNotFoundError(f"Eligibility file not found: {eligibility_file}")

        record = _find_record(eligibility_file, args.paper_id, args.input_hash)
        paper_id = str(record.get("paper_id", "")).strip()
        diagnostics = record.get("diagnostics", {}) if isinstance(record.get("diagnostics"), dict) else {}

        stored_context_hash = str(diagnostics.get("llm_input_sha256", "")).strip().lower()
        stored_prompt_template_hash = str(diagnostics.get("prompt_template_sha256", "")).strip().lower()
        stored_full_prompt_hash = str(diagnostics.get("full_prompt_sha256", "")).strip().lower()
        stored_prompt_campaign_id = str(diagnostics.get("prompt_campaign_id", "")).strip()
        csv_root = Path(PATH_SETTINGS.get("csv_dir", "input"))

        record_metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        context_text, selected_chunks = _reconstruct_context(
            stage,
            paper_id,
            csv_root,
            diagnostics=diagnostics,
            eligibility_file=eligibility_file,
            fallback_metadata=record_metadata,
        )
        recomputed_context_hash = _sha256_text(context_text)

        prompt_template, prompt_template_source = _load_prompt_template(stage, stored_prompt_campaign_id)
        recomputed_prompt_template_hash = _sha256_text(prompt_template)
        full_prompt = prompt_template.replace("{data}", context_text)
        recomputed_full_prompt_hash = _sha256_text(full_prompt)

        context_ok = bool(stored_context_hash) and stored_context_hash == recomputed_context_hash
        prompt_template_ok = bool(stored_prompt_template_hash) and stored_prompt_template_hash == recomputed_prompt_template_hash
        full_prompt_ok = bool(stored_full_prompt_hash) and stored_full_prompt_hash == recomputed_full_prompt_hash
        mismatch_cause = _diagnose_mismatch(
            context_ok=context_ok,
            prompt_template_ok=prompt_template_ok,
            full_prompt_ok=full_prompt_ok,
            stored_full_prompt_hash=stored_full_prompt_hash,
            full_prompt=full_prompt,
        )

        diagnostics_lines = _selection_diagnostics_lines(diagnostics)
        chunk_confidence_lines = _selected_chunk_confidence_lines(selected_chunks)

        stage_root = eligibility_file.parent
        ts = datetime.now().strftime("%Y%m%d_%H-%M-%S")
        default_name = f"{stage}_{paper_id}_input_trace_{ts}.txt"
        output_path = Path(args.output) if args.output else (stage_root / default_name)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if args.eligibility_file:
            eligibility_resolution = "explicit"
        else:
            eligibility_resolution = "auto_latest"

        lines: list[str] = [
            "INPUT TRACE REPORT",
            f"stage: {stage}",
            f"paper_id: {paper_id}",
            f"eligibility_file: {eligibility_file}",
            f"eligibility_resolution: {eligibility_resolution}",
            f"stored_llm_input_sha256: {stored_context_hash or 'NA'}",
            f"recomputed_llm_input_sha256: {recomputed_context_hash}",
            f"context_hash_match: {context_ok}",
            f"stored_prompt_template_sha256: {stored_prompt_template_hash or 'NA'}",
            f"recomputed_prompt_template_sha256: {recomputed_prompt_template_hash}",
            f"prompt_template_hash_match: {prompt_template_ok}",
            f"stored_prompt_campaign_id: {stored_prompt_campaign_id or 'NA'}",
            f"prompt_template_source: {prompt_template_source}",
            f"stored_full_prompt_sha256: {stored_full_prompt_hash or 'NA'}",
            f"recomputed_full_prompt_sha256: {recomputed_full_prompt_hash}",
            f"full_prompt_hash_match: {full_prompt_ok}",
            f"mismatch_cause: {mismatch_cause}",
            "",
            *diagnostics_lines,
            "",
            *chunk_confidence_lines,
            "",
            "=== Reconstructed LLM Input Context ===",
            context_text,
        ]

        if args.show_full_prompt:
            lines.extend(["", "=== Reconstructed Full Prompt ===", full_prompt])

        output_path.write_text("\n".join(lines), encoding="utf-8")

        print("[trace] input_trace status=completed")
        print(f"[output] trace_report path={output_path}")
        print(f"[trace] context_hash_match={context_ok}")
        print(f"[trace] prompt_template_hash_match={prompt_template_ok}")
        print(f"[trace] full_prompt_hash_match={full_prompt_ok}")
        if mismatch_cause != "none":
            print(f"[trace] mismatch_cause={mismatch_cause}")

    def run_all_data_extraction_output(self, args: argparse.Namespace | None = None) -> None:
        """human readable hint: create audit traces from data_extraction outputs and normalized full texts."""

        args = args or argparse.Namespace(stage="data_extraction", output=None, show_full_prompt=False)
        stage = str(getattr(args, "stage", "data_extraction") or "data_extraction").strip()
        if stage != "data_extraction":
            raise ValueError("--all-data-extraction-output is only supported for stage='data_extraction'.")

        configured_stage_output_dir = getattr(args, "stage_output_dir", None)
        stage_root = Path(configured_stage_output_dir) if configured_stage_output_dir else _candidate_stage_dirs(stage)[0]
        if not stage_root.exists():
            raise FileNotFoundError(f"Stage output directory not found: {stage_root}")
        output_root = Path(getattr(args, "output", "") or "") if getattr(args, "output", None) else stage_root / "input_traces"
        output_root.mkdir(parents=True, exist_ok=True)
        csv_root = Path(PATH_SETTINGS.get("csv_dir", "input"))
        schema = DynamicExtractionSchema.from_kb()
        prompt_template, prompt_template_source = _load_prompt_template(stage)
        prompt_template_hash = _sha256_text(prompt_template)
        rows: list[dict[str, str]] = []
        result_candidates: dict[str, tuple[int, float, Path, dict[str, Any]]] = {}

        # human readable hint: retries can leave stale per-paper result folders; trace only the newest most-complete payload per paper.
        for result_dir in _iter_data_extraction_output_dirs(stage_root):
            results_path = result_dir / "data_extraction_results.jsonl"
            result_payload = _load_jsonl_payload(results_path)
            if not isinstance(result_payload, dict):
                continue
            paper_id = str(result_payload.get("paper_id") or result_dir.name.split("_", 1)[0]).strip()
            clean_paper_id = paper_id.lstrip("#")
            extracted = result_payload.get("extracted_data")
            if not isinstance(extracted, dict):
                extracted = {}
            present_count = sum(
                0 if _is_missing_extraction_value(_value_for_variable(extracted, variable), variable) else 1
                for variable in schema.variables
            )
            try:
                mtime = results_path.stat().st_mtime
            except OSError:
                mtime = 0.0
            previous = result_candidates.get(clean_paper_id)
            if previous is None or (present_count, mtime) >= (previous[0], previous[1]):
                result_candidates[clean_paper_id] = (present_count, mtime, result_dir, result_payload)

        for _clean_paper_id, (_present_count, _mtime, result_dir, result_payload) in result_candidates.items():
            results_path = result_dir / "data_extraction_results.jsonl"
            paper_id = str(result_payload.get("paper_id") or result_dir.name.split("_", 1)[0]).strip()
            extracted = result_payload.get("extracted_data")
            if not isinstance(extracted, dict):
                extracted = {}
            try:
                input_folder = _find_paper_folder(stage, paper_id, csv_root)
            except Exception:
                input_folder = csv_root / "per_paper_data_extraction" / result_dir.name

            metadata = _load_folder_metadata(input_folder)
            title = read_metadata_value(metadata, "title")
            selected_chunks = _load_selected_chunks(input_folder, stage, paper_id) if input_folder.exists() else []
            context_text, selected_chunks = _reconstruct_context(
                stage,
                paper_id,
                csv_root,
                fallback_metadata=metadata,
            )
            normalized_path, normalized_raw = _read_normalized_text(input_folder)
            normalized_for_search = normalize_extracted_text_for_llm(normalized_raw).strip()
            context_hash = _sha256_text(context_text)
            full_prompt = prompt_template.replace("{data}", context_text)
            full_prompt_hash = _sha256_text(full_prompt)

            present_lines: list[str] = []
            missing_lines: list[str] = []
            missing_fields: list[str] = []
            evidence_hint_lines: list[str] = ["=== Evidence Hint Search For Missing Fields ==="]

            for variable in schema.variables:
                value = _value_for_variable(extracted, variable)
                quote = _quote_for_variable(extracted, variable)
                field_path = f"{variable.domain}.{variable.variable_name}"
                if _is_missing_extraction_value(value, variable):
                    missing_fields.append(field_path)
                    missing_lines.append(
                        f"- {field_path} -> {MISSING_TEXT_VALUE}; consensus_column={variable.covidence_column_name}"
                    )
                    terms = _trace_search_terms_for_variable(variable)
                    hits = _line_hits_for_terms(normalized_for_search, terms)
                    evidence_hint_lines.append("")
                    evidence_hint_lines.append(f"[{field_path}] search_terms={', '.join(terms) or 'NA'}")
                    if hits:
                        for line_number, snippet in hits:
                            evidence_hint_lines.append(f"  L{line_number}: {snippet}")
                    else:
                        evidence_hint_lines.append("  no nearby evidence hits found by schema-derived terms")
                else:
                    rendered_value = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
                    present_lines.append(
                        f"- {field_path} = {rendered_value}; quote={quote or 'NA'}"
                    )

            trace_paper_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", paper_id.lstrip("#") or paper_id).strip("_")
            trace_name = f"data_extraction_{trace_paper_id}_input_trace.txt"
            trace_path = output_root / trace_name
            normalized_word_count = len(normalized_for_search.split())
            lines: list[str] = [
                "DATA EXTRACTION INPUT TRACE REPORT",
                f"stage: {stage}",
                f"paper_id: {paper_id}",
                f"title: {title or 'NA'}",
                f"output_dir: {result_dir}",
                f"results_path: {results_path}",
                f"input_folder: {input_folder}",
                f"normalized_text_path: {normalized_path or 'NA'}",
                f"normalized_text_sha256: {_sha256_text(normalized_raw) if normalized_raw else 'NA'}",
                f"normalized_word_count: {normalized_word_count}",
                f"selected_chunks_count: {len(selected_chunks)}",
                f"prompt_template_source: {prompt_template_source}",
                f"prompt_template_sha256: {prompt_template_hash}",
                f"reconstructed_context_sha256: {context_hash}",
                f"reconstructed_full_prompt_sha256: {full_prompt_hash}",
                f"present_field_count: {len(present_lines)}",
                f"missing_field_count: {len(missing_fields)}",
                "",
                "=== Present Extracted Fields ===",
                *(present_lines or ["No present extracted fields found."]),
                "",
                "=== Missing Extracted Fields ===",
                *(missing_lines or ["No missing extracted fields found."]),
                "",
                *evidence_hint_lines,
                "",
                *(_selected_chunk_confidence_lines(selected_chunks)),
                "",
                "=== Reconstructed LLM Input Context ===",
                context_text,
            ]
            if bool(getattr(args, "show_full_prompt", False)):
                lines.extend(["", "=== Reconstructed Full Prompt ===", full_prompt])
            trace_path.write_text("\n".join(lines), encoding="utf-8")

            rows.append(
                {
                    "paper_id": paper_id,
                    "title": title,
                    "trace_path": str(trace_path),
                    "input_folder": str(input_folder),
                    "normalized_text_path": str(normalized_path or ""),
                    "normalized_word_count": str(normalized_word_count),
                    "selected_chunks_count": str(len(selected_chunks)),
                    "present_field_count": str(len(present_lines)),
                    "missing_field_count": str(len(missing_fields)),
                    "missing_fields": "; ".join(missing_fields),
                }
            )

        summary_path = output_root / "data_extraction_input_trace_summary.csv"
        with summary_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = [
                "paper_id",
                "title",
                "trace_path",
                "input_folder",
                "normalized_text_path",
                "normalized_word_count",
                "selected_chunks_count",
                "present_field_count",
                "missing_field_count",
                "missing_fields",
            ]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print("[trace] data_extraction_input_traces status=completed")
        print(f"[trace] papers_traced={len(rows)}")
        print(f"[output] trace_dir path={output_root}")
        print(f"[output] trace_summary path={summary_path}")


def run_trace() -> None:
    """Compatibility wrapper for direct module execution."""

    InputTraceRunner().run()


if __name__ == "__main__":
    run_trace()
