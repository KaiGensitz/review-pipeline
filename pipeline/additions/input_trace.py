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
from typing import Optional

from config.user_orchestrator import CURRENT_STAGE, PATH_SETTINGS, PROMPT_FILES


def _sha256_text(value: str) -> str:
    """human readable hint: compute a stable fingerprint of any text."""

    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _latest_eligibility_file(stage: str) -> Path:
    """human readable hint: pick the latest eligibility file (excluding split files)."""

    stage_root = Path(PATH_SETTINGS.get("output_root", "output")) / stage
    candidates = list(stage_root.glob(f"{stage}_*_eligibility_*.jsonl"))
    candidates = [
        p
        for p in candidates
        if "eligibility_select" not in p.name
        and "eligibility_irrelevant" not in p.name
        and "eligibility_included" not in p.name
        and "eligibility_excluded" not in p.name
    ]
    if not candidates:
        raise FileNotFoundError(f"No eligibility JSONL found in {stage_root} for stage '{stage}'.")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _find_record(eligibility_file: Path, paper_id: str | None, input_hash: str | None) -> dict:
    """human readable hint: find one paper in eligibility output by paper_id or stored input hash."""

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

            if paper_id and pid == paper_id:
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


def _format_chunks_for_prompt(stage: str, paper_id: str, title: str, authors: str, chunks: list[dict]) -> str:
    """human readable hint: rebuild the same context text format sent to the model."""

    title_text = (title or "").strip()
    if stage in {"title_abstract", "full_text"}:
        title_text = _strip_author_mentions(title_text, authors)

    parts: list[str] = [f"Paper ID: {paper_id}", f"Title: {title_text}".strip()]
    for idx, chunk in enumerate(chunks, start=1):
        text = str(chunk.get("text", "")).strip()
        if stage in {"title_abstract", "full_text"}:
            text = _strip_author_mentions(text, authors)
        page = chunk.get("page")
        prefix = f"[Chunk {idx}" + (f", page {page}]" if page is not None else "]")
        parts.append(f"{prefix}\n{text}")
    return "\n\n".join(parts)


def _title_abstract_context(stage: str, paper_id: str) -> str:
    """human readable hint: title_abstract stores the full model context in selected_chunks output."""

    stage_root = Path(PATH_SETTINGS.get("output_root", "output")) / stage
    files = sorted(stage_root.glob(f"{stage}_*_selected_chunks_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)

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
                return str(selected[0].get("text", ""))

    raise ValueError(f"Could not reconstruct title_abstract input for paper_id='{paper_id}'.")


def _load_folder_metadata(folder: Path) -> dict:
    meta_path = folder / "metadata.json"
    if not meta_path.exists():
        return {}
    try:
        with meta_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _extract_covidence_id(row: dict) -> str:
    return str(
        row.get("Covidence #")
        or row.get("Covidence#")
        or row.get("paper_id")
        or row.get("id")
        or row.get("ID")
        or ""
    ).strip().lstrip("#")


def _find_paper_folder(stage: str, paper_id: str, csv_root: Path) -> Path:
    """human readable hint: locate the per-paper folder by matching Covidence/paper ID in metadata."""

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
            cov_id = _extract_covidence_id(metadata)
            if cov_id and cov_id == paper_id.lstrip("#"):
                return folder

    raise FileNotFoundError(f"Could not find folder for paper_id='{paper_id}' in per-paper inputs.")


def _load_selected_chunks(folder: Path, stage: str, paper_id: str) -> list[dict]:
    chunks_path = folder / f"{stage}_selected_chunks.jsonl"
    if not chunks_path.exists():
        raise FileNotFoundError(f"Missing selected chunks file: {chunks_path}")

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
    raise ValueError(f"No selected chunks found for paper_id='{paper_id}' in {chunks_path}.")


def _folder_stage_context(stage: str, paper_id: str, csv_root: Path) -> str:
    """human readable hint: rebuild full_text/data_extraction model context from metadata + selected chunks."""

    folder = _find_paper_folder(stage, paper_id, csv_root)
    metadata = _load_folder_metadata(folder)
    title = str(metadata.get("Title") or metadata.get("title") or "")
    authors = str(metadata.get("Authors") or metadata.get("authors") or "")
    chunks = _load_selected_chunks(folder, stage, paper_id)
    return _format_chunks_for_prompt(stage, paper_id, title, authors, chunks)


def _reconstruct_context(stage: str, paper_id: str, csv_root: Path) -> str:
    """human readable hint: stage-aware reconstruction of exact model context."""

    if stage == "title_abstract":
        return _title_abstract_context(stage, paper_id)
    return _folder_stage_context(stage, paper_id, csv_root)


def _load_prompt_template(stage: str) -> str:
    """human readable hint: mirror runtime prompt assembly, including all-stage external criteria injection."""

    template = PROMPT_FILES[stage].read_text(encoding="utf-8")

    criteria_map = PATH_SETTINGS.get("eligibility_criteria_files", {})
    criteria_path_raw = criteria_map.get(stage) or PATH_SETTINGS.get("eligibility_criteria_file")
    criteria_path = Path(criteria_path_raw) if criteria_path_raw else None

    if not criteria_path:
        if "{eligibility_criteria}" in template:
            raise ValueError(
                f"Prompt for stage '{stage}' contains '{{eligibility_criteria}}' but no criteria file is configured."
            )
        return template

    if not criteria_path.exists():
        raise FileNotFoundError(
            "Missing external eligibility criteria file for CURRENT_STAGE. "
            f"Expected file at: {criteria_path}."
        )

    criteria_text = criteria_path.read_text(encoding="utf-8").strip()
    if not criteria_text:
        raise ValueError(f"Eligibility criteria file is empty: {criteria_path}")

    if "{eligibility_criteria}" in template:
        return template.replace("{eligibility_criteria}", criteria_text)

    fallback_lines = [
        (f"CRITERION {line}" if line.strip() else "")
        for line in criteria_text.splitlines()
    ]
    fallback_text = "\n".join(fallback_lines).strip()

    return (
        template
        + "\n\nEXTERNAL ELIGIBILITY CRITERIA (injected at runtime):\n"
        + fallback_text
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct and verify per-paper model input text by hash.")
    parser.add_argument("--stage", default=CURRENT_STAGE, help="Pipeline stage (title_abstract | full_text | data_extraction)")
    parser.add_argument("--paper-id", help="Paper ID (Covidence or paper_id)")
    parser.add_argument("--input-hash", help="Stored llm_input_sha256 to search for")
    parser.add_argument("--eligibility-file", help="Optional explicit eligibility JSONL path")
    parser.add_argument("--show-full-prompt", action="store_true", help="Also output merged prompt (template + evidence)")
    parser.add_argument("--output", help="Optional explicit output .txt path")
    args = parser.parse_args()

    if not args.paper_id and not args.input_hash:
        parser.error("Provide either --paper-id or --input-hash.")
    return args


def run_trace() -> None:
    """human readable hint: recover human-readable input text and verify exact-hash identity."""

    args = _parse_args()
    stage = str(args.stage).strip()

    if stage not in {"title_abstract", "full_text", "data_extraction"}:
        raise ValueError(f"Unsupported stage '{stage}'.")

    eligibility_file = Path(args.eligibility_file) if args.eligibility_file else _latest_eligibility_file(stage)
    if not eligibility_file.exists():
        raise FileNotFoundError(f"Eligibility file not found: {eligibility_file}")

    record = _find_record(eligibility_file, args.paper_id, args.input_hash)
    paper_id = str(record.get("paper_id", "")).strip()
    diagnostics = record.get("diagnostics", {}) if isinstance(record.get("diagnostics"), dict) else {}

    stored_context_hash = str(diagnostics.get("llm_input_sha256", "")).strip().lower()
    stored_full_prompt_hash = str(diagnostics.get("full_prompt_sha256", "")).strip().lower()
    csv_root = Path(PATH_SETTINGS.get("csv_dir", "input"))

    context_text = _reconstruct_context(stage, paper_id, csv_root)
    recomputed_context_hash = _sha256_text(context_text)

    prompt_template = _load_prompt_template(stage)
    full_prompt = prompt_template.replace("{data}", context_text)
    recomputed_full_prompt_hash = _sha256_text(full_prompt)

    context_ok = bool(stored_context_hash) and stored_context_hash == recomputed_context_hash
    full_prompt_ok = bool(stored_full_prompt_hash) and stored_full_prompt_hash == recomputed_full_prompt_hash

    stage_root = Path(PATH_SETTINGS.get("output_root", "output")) / stage
    ts = datetime.now().strftime("%Y%m%d_%H-%M-%S")
    default_name = f"{stage}_{paper_id}_input_trace_{ts}.txt"
    output_path = Path(args.output) if args.output else (stage_root / default_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [
        "INPUT TRACE REPORT",
        f"stage: {stage}",
        f"paper_id: {paper_id}",
        f"eligibility_file: {eligibility_file}",
        f"stored_llm_input_sha256: {stored_context_hash or 'NA'}",
        f"recomputed_llm_input_sha256: {recomputed_context_hash}",
        f"context_hash_match: {context_ok}",
        f"stored_full_prompt_sha256: {stored_full_prompt_hash or 'NA'}",
        f"recomputed_full_prompt_sha256: {recomputed_full_prompt_hash}",
        f"full_prompt_hash_match: {full_prompt_ok}",
        "",
        "=== Reconstructed LLM Input Context ===",
        context_text,
    ]

    if args.show_full_prompt:
        lines.extend(["", "=== Reconstructed Full Prompt ===", full_prompt])

    output_path.write_text("\n".join(lines), encoding="utf-8")

    print("Input trace completed.")
    print(f"- report: {output_path}")
    print(f"- context hash match: {context_ok}")
    print(f"- full prompt hash match: {full_prompt_ok}")


if __name__ == "__main__":
    run_trace()
