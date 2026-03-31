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

ELIGIBILITY_CRITERIA_PLACEHOLDER = "{eligibility_criteria}"
DEFAULT_OUTPUT_ROOT = Path(PATH_SETTINGS.get("output_root", "output"))


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
    parser.add_argument("--paper-id", help="Paper ID (Covidence or paper_id)")
    parser.add_argument("--input-hash", help="Stored llm_input_sha256 to search for")
    parser.add_argument("--eligibility-file", help="Optional explicit eligibility JSONL path")
    parser.add_argument("--show-full-prompt", action="store_true", help="Also output merged prompt (template + evidence)")
    parser.add_argument("--output", help="Optional explicit output .txt path")
    args = parser.parse_args()

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

        context_text = _reconstruct_context(stage, paper_id, csv_root)
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
            "=== Reconstructed LLM Input Context ===",
            context_text,
        ]

        if args.show_full_prompt:
            lines.extend(["", "=== Reconstructed Full Prompt ===", full_prompt])

        output_path.write_text("\n".join(lines), encoding="utf-8")

        print("Input trace completed.")
        print(f"- report: {output_path}")
        print(f"- context hash match: {context_ok}")
        print(f"- prompt template hash match: {prompt_template_ok}")
        print(f"- full prompt hash match: {full_prompt_ok}")
        if mismatch_cause != "none":
            print(f"- mismatch cause: {mismatch_cause}")


def run_trace() -> None:
    """Compatibility wrapper for direct module execution."""

    InputTraceRunner().run()


if __name__ == "__main__":
    run_trace()
