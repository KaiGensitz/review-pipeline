"""Retry CSV, retry artifact, and retry decision helpers for main.py."""

from __future__ import annotations

import csv
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from config.user_orchestrator import PATH_SETTINGS, STAGE_RULES
from pipeline.additions.run_index import _latest_base_outputs
from pipeline.core.metadata_aliases import metadata_aliases, read_metadata_value

def _parse_is_eligible(decision: object, stage: str) -> bool | None:
    """Best-effort extraction of is_eligible from an LLM decision payload (stage-aware)."""
    payload = decision
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None
    if isinstance(payload, dict):
        val = payload.get("is_eligible")
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            low = val.lower()
            if stage == "title_abstract" and low in {"true", "yes", "eligible", "neutral", "maybe"}:
                return True
            if low in {"false", "no", "ineligible", "exclude"}:
                return False
    return None

def _parse_exclusion_reason(decision: object) -> str | None:
    """human readable hint: extract exclusion_reason_category if present."""

    payload = decision
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None
    if isinstance(payload, dict):
        for key in ("exclusion_reason_category", "exclusion_reason", "reason"):
            val = payload.get(key)
            if val:
                return str(val)
    return None

def _collect_missing_is_eligible(error_log_path: Path, eligibility_path: Path, stage: str) -> set[str]:
    """Find paper_ids that have errors AND no is_eligible in eligibility output."""
    error_ids: set[str] = set()
    if error_log_path.exists():
        try:
            with error_log_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                        pid = str(obj.get("paper_id", ""))
                        if pid:
                            error_ids.add(pid)
                    except Exception:
                        continue
        except Exception:
            pass

    if not error_ids or not eligibility_path.exists():
        return set()

    missing: set[str] = set()
    try:
        with eligibility_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                if '"meta": "eligibility_records"' in line or '"meta": "summary"' in line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                pid = str(payload.get("paper_id", ""))
                if pid not in error_ids:
                    continue
                decision = payload.get("llm_decision")
                has_is = _parse_is_eligible(decision, stage)
                if has_is is None:
                    missing.add(pid)
    except Exception:
        return set()

    return missing

def _unique_retry_path(path: Path) -> Path:
    """human readable hint: ensure retry artifacts never overwrite an existing file with the same timestamped name."""

    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    for idx in range(1, 10000):
        candidate = path.with_name(f"{stem}_{idx}{suffix}")
        if not candidate.exists():
            return candidate

    fallback = datetime.now().strftime("%Y%m%d_%H-%M-%S-%f")
    return path.with_name(f"{stem}_{fallback}{suffix}")

def _write_retry_csv(source_csv: Path, target_dir: Path, paper_ids: set[str], stage: str, run_label: str) -> Path | None:
    """Create a stage-valid retry CSV using run_label and stage-specific token (screen/select)."""

    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H-%M-%S-%f")
    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
    token = (
        "screen"
        if stage == "title_abstract"
        else "select"
        if stage == "full_text"
        else "included"
        if stage == "data_extraction"
        else "screen"
    )
    filename = f"{stage}_{sample_tag}_sample_{token}_csv_retry_{timestamp}.csv"
    target_path = _unique_retry_path(target_dir / filename)

    if not source_csv.exists():
        print(f"[retry] source CSV missing: {source_csv}")
        return None

    id_keys = metadata_aliases("paper_id")
    rows_written = 0
    written_ids: set[str] = set()
    try:
        with source_csv.open("r", encoding="utf-8") as src:
            reader = csv.DictReader(src)
            fieldnames = reader.fieldnames or []
            with target_path.open("w", encoding="utf-8", newline="") as dst:
                writer = csv.DictWriter(dst, fieldnames=fieldnames)
                writer.writeheader()
                for row in reader:
                    pid = ""
                    for key in id_keys:
                        if key in row and row[key]:
                            pid = str(row[key]).strip()
                            break
                    if pid in paper_ids and pid not in written_ids:
                        writer.writerow(row)
                        rows_written += 1
                        written_ids.add(pid)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[retry] failed to write retry CSV: {exc}")
        return None

    if rows_written == 0:
        print("[retry] no matching rows were written; retry CSV will not be used.")
        try:
            target_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None

    return target_path

def _prepare_isolated_retry_run_dir(stage: str, retry_csv: Path) -> Path | None:
    """human readable hint: run retries from a clean one-file folder to avoid processing old retry CSVs."""

    if not retry_csv.exists():
        return None

    retry_root = Path(PATH_SETTINGS["csv_dir"]) / "retry_runs"
    isolated_dir = retry_root / f"_active_retry_{stage}"
    isolated_dir.mkdir(parents=True, exist_ok=True)

    try:
        for stale_csv in isolated_dir.glob("*.csv"):
            stale_csv.unlink(missing_ok=True)

        rule = STAGE_RULES.get(stage, {}) if isinstance(STAGE_RULES, dict) else {}
        pdf_dir = rule.get("pdf_dir")
        if pdf_dir:
            stale_paper_root = isolated_dir / str(pdf_dir)
            if stale_paper_root.exists() and stale_paper_root.is_dir():
                shutil.rmtree(stale_paper_root, ignore_errors=True)

        target_csv = isolated_dir / retry_csv.name
        shutil.copy2(retry_csv, target_csv)
        return isolated_dir
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[retry] failed to prepare isolated retry run folder: {exc}")
        return None

def _retry_pdf_root(stage: str) -> str | None:
    """human readable hint: retries should still resolve PDFs from the main input folder tree."""

    rule = STAGE_RULES.get(stage, {}) if isinstance(STAGE_RULES, dict) else {}
    pdf_dir = rule.get("pdf_dir")
    if not pdf_dir:
        return None
    return str(Path(PATH_SETTINGS["csv_dir"]) / str(pdf_dir))

def _retry_output_paths(stage: str, run_label: str, attempt_index: int) -> dict:
    """human readable hint: retry outputs stay separate using stage_runlabel_retry_attempt_output_timestamp order."""

    timestamp = datetime.now().strftime("%Y%m%d_%H-%M-%S-%f")
    stage_root = Path(PATH_SETTINGS["output_root"]) / stage
    stage_root.mkdir(parents=True, exist_ok=True)
    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
    base_prefix = f"{stage}_{sample_tag}_sample_retry_{attempt_index}"
    return {
        "eligibility": _unique_retry_path(stage_root / f"{base_prefix}_eligibility_{timestamp}.jsonl"),
        "text": _unique_retry_path(stage_root / f"{base_prefix}_screening_results_readable_{timestamp}.txt"),
        "chunks": _unique_retry_path(stage_root / f"{base_prefix}_selected_chunks_{timestamp}.jsonl"),
        "error": _unique_retry_path(stage_root / f"{base_prefix}_error_log_{timestamp}.txt"),
        "resource": _unique_retry_path(stage_root / f"{base_prefix}_resource_usage_{timestamp}.log"),
    }

def _require_base_outputs(stage: str, run_label: str) -> dict[str, Path | None]:
    """Ensure base outputs exist before running a retry; avoid orphan retry files."""

    base = _latest_base_outputs(stage, run_label)
    if stage == "data_extraction":
        # human readable hint: data extraction writes per-paper extraction files
        # and resource logs, not a screening-style eligibility JSONL. A retry
        # can therefore be launched from the source CSV once the failed run has
        # produced any base extraction artifact.
        if not (base.get("resource") or base.get("emissions")):
            print(
                f"[retry] Cannot run retry: missing base data-extraction output for stage '{stage}' "
                f"(run_label='{run_label}'). Run the sample once before retrying failed papers."
            )
            return {}
        return base

    if not base.get("eligibility"):
        print(
            f"[retry] Cannot run retry: missing base eligibility output for stage '{stage}' (run_label='{run_label}'). "
            "Run the main screening for this sample first so we can append to it."
        )
        return {}

    if not base.get("emissions"):
        print(
            f"[retry] Base CodeCarbon file is missing for stage '{stage}' (run_label='{run_label}'). "
            "Retry will continue; emissions merge/update will be skipped for this attempt."
        )

    return base

def _infer_run_label_from_retry_csv(path: Path, stage: str) -> str | None:
    """human readable hint: infer run_label from retry CSV name or existing base files."""

    name = path.name.lower()
    base_qc = _latest_base_outputs(stage, "qc_sample").get("eligibility")
    base_rem = _latest_base_outputs(stage, "remaining_sample").get("eligibility")

    if "qc_sample" in name:
        return "qc_sample"
    if "remaining_sample" in name:
        return "remaining_sample"
    if base_rem:
        return "remaining_sample"
    if base_qc:
        return "qc_sample"
    return None

def _first_available_run_label(stage: str, preferred: str | None) -> str | None:
    """human readable hint: pick a run_label that has base outputs (eligibility + emissions)."""

    candidates = [preferred] if preferred else []
    if "qc_sample" not in candidates:
        candidates.append("qc_sample")
    if "remaining_sample" not in candidates:
        candidates.append("remaining_sample")

    for label in candidates:
        if not label:
            continue
        base = _latest_base_outputs(stage, label)
        if stage == "data_extraction" and (base.get("resource") or base.get("emissions")):
            return label
        elig = base.get("eligibility")
        emissions = base.get("emissions")
        if elig and emissions:
            return label
    return None

def _record_retry_manifest(
    retry_artifact: dict | None,
    stage: str,
    attempt_map: dict[str, int] | None = None,
    source_csv: Path | None = None,
    emissions_info: dict[str, object] | None = None,
) -> None:
    """Keep retry artifacts separate and append a manifest entry listing files and paper_ids."""

    if not retry_artifact or not isinstance(retry_artifact, dict):
        return

    run_label = retry_artifact.get("run_label") or "remaining_sample"
    attempt_lookup = attempt_map or {}
    attempt_default = max(attempt_lookup.values()) if attempt_lookup else 1

    stage_root = Path(PATH_SETTINGS["output_root"]) / stage
    stage_root.mkdir(parents=True, exist_ok=True)
    manifest_path = stage_root / f"{stage}_retry_manifest.jsonl"

    def _to_path(val: object) -> Path | None:
        return Path(val) if isinstance(val, (str, os.PathLike)) else None

    def _ensure_retry_name(path: Path | None) -> Path | None:
        if not path:
            return None
        if "_sample_retry_" in path.name or "_sample_main_" in path.name:
            return path
        sample_tag_local = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
        token = f"{sample_tag_local}_sample_"
        replacement = f"{sample_tag_local}_sample_retry_{attempt_default}_"
        if token in path.name:
            new_name = path.name.replace(token, replacement, 1)
        else:
            new_name = f"{path.stem}_retry_{attempt_default}{path.suffix}"
        renamed = path.with_name(new_name)
        try:
            path.rename(renamed)
            return renamed
        except Exception:
            return path

    retry_split = retry_artifact.get("split_paths", {}) if isinstance(retry_artifact.get("split_paths"), dict) else {}

    eligibility_path = _ensure_retry_name(_to_path(retry_artifact.get("eligibility_path") or retry_artifact.get("eligibility")))
    select_path = _ensure_retry_name(
        _to_path(retry_split.get(True) or retry_split.get("select") or retry_split.get("included"))
    )
    exclude_path = _ensure_retry_name(
        _to_path(retry_split.get(False) or retry_split.get("irrelevant") or retry_split.get("excluded"))
    )
    chunks_path = _ensure_retry_name(_to_path(retry_artifact.get("chunks_path") or retry_artifact.get("chunks")))
    text_path = _ensure_retry_name(_to_path(retry_artifact.get("text_path") or retry_artifact.get("text")))
    resource_path = _ensure_retry_name(_to_path(retry_artifact.get("resource_log_path") or retry_artifact.get("resource")))
    emissions_path = _to_path(retry_artifact.get("emissions_path") or retry_artifact.get("emissions"))
    emissions_rows: list[int] = []
    if emissions_info and isinstance(emissions_info, dict):
        path_obj = emissions_info.get("emissions_path")
        if isinstance(path_obj, (str, os.PathLike, Path)):
            emissions_path = Path(path_obj)
        rows_obj = emissions_info.get("emissions_rows")
        if isinstance(rows_obj, list):
            emissions_rows = [int(r) for r in rows_obj if isinstance(r, (int, float))]

    paper_ids: set[str] = set()

    def _collect_ids_from_jsonl(path: Path | None) -> None:
        if not path or not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if not isinstance(obj, dict) or obj.get("meta"):
                        continue
                    pid = obj.get("paper_id")
                    if pid is None:
                        continue
                    paper_ids.add(str(pid))
        except Exception:
            return

    def _collect_ids_from_resource(path: Path | None) -> None:
        if not path or not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if not isinstance(obj, dict):
                        continue
                    if str(obj.get("paper_id")) == "TOTAL":
                        continue
                    pid = obj.get("paper_id")
                    if pid is None:
                        continue
                    paper_ids.add(str(pid))
        except Exception:
            return

    _collect_ids_from_jsonl(eligibility_path)
    _collect_ids_from_jsonl(select_path)
    _collect_ids_from_jsonl(exclude_path)
    _collect_ids_from_jsonl(chunks_path)
    _collect_ids_from_resource(resource_path)

    manifest_entry = {
        "stage": stage,
        "run_label": run_label,
        "run_id": retry_artifact.get("run_id"),
        "attempt_index": attempt_default,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_csv": str(source_csv) if source_csv else None,
        "paper_count": len(paper_ids),
        "paper_ids": sorted(paper_ids),
        "artifact_paths": {
            "eligibility": str(eligibility_path) if eligibility_path else None,
            "select": str(select_path) if select_path else None,
            "exclude": str(exclude_path) if exclude_path else None,
            "chunks": str(chunks_path) if chunks_path else None,
            "text": str(text_path) if text_path else None,
            "resource": str(resource_path) if resource_path else None,
            "emissions": str(emissions_path) if emissions_path else None,
            "emissions_rows": emissions_rows,
        },
    }

    try:
        with manifest_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(manifest_entry, ensure_ascii=False) + "\n")
    except Exception:
        return

def _next_retry_attempt(stage: str, run_label: str) -> int:
    """human readable hint: derive the next retry attempt index from the manifest (per run_label)."""

    stage_root = Path(PATH_SETTINGS.get("output_root", "output")) / stage
    manifest_path = stage_root / f"{stage}_retry_manifest.jsonl"
    max_attempt = 0
    if not manifest_path.exists():
        return 1
    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if obj.get("run_label") != run_label:
                    continue
                try:
                    attempt_val = int(obj.get("attempt_index", 0))
                    if attempt_val > max_attempt:
                        max_attempt = attempt_val
                except Exception:
                    continue
    except Exception:
        return 1
    return max_attempt + 1

def _latest_eligibility_map(stage: str) -> dict[str, object]:
    """human readable hint: load the most recent eligibility JSONL into a paper_id->decision map."""

    stage_root = Path(PATH_SETTINGS.get("output_root", "output")) / stage
    pattern = f"{stage}_*_eligibility_*.jsonl"
    split_tokens = (
        "eligibility_select_",
        "eligibility_irrelevant_",
        "eligibility_included_",
        "eligibility_excluded_",
    )
    candidates = sorted(
        [
            path
            for path in stage_root.glob(pattern)
            if not any(token in path.name for token in split_tokens)
        ],
        key=lambda path: path.stat().st_mtime,
    )
    if not candidates:
        return {}

    records: dict[str, object] = {}
    for path in candidates:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip() or '"meta": "' in line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    pid = str(obj.get("paper_id", ""))
                    if pid:
                        # Newer files overwrite older decisions paper-by-paper.
                        records[pid] = obj.get("llm_decision")
        except Exception:
            continue
    return records

def _decision_is_complete(decision: object, stage: str) -> bool:
    """human readable hint: validate presence of is_eligible and required justification/reason."""

    elig = _parse_is_eligible(decision, stage)
    if elig is None:
        return False
    payload = decision
    if isinstance(decision, str):
        try:
            payload = json.loads(decision)
        except Exception:
            return False
    if not isinstance(payload, dict):
        return False
    conf = payload.get("confidence_score")
    if conf is None:
        return False
    just = payload.get("justification")
    if not isinstance(just, str) or not just.strip():
        return False
    if elig is False:
        reason = _parse_exclusion_reason(payload)
        if not isinstance(reason, str) or not reason.strip():
            return False
    return True

def _retry_csv_needed(retry_csv: Path, stage: str) -> set[str]:
    """human readable hint: return paper_ids in retry_csv that still lack complete decisions."""

    if not retry_csv.exists():
        return set()
    latest = _latest_eligibility_map(stage)
    needed: set[str] = set()
    try:
        with retry_csv.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                pid = read_metadata_value(row, "paper_id")
                if not pid:
                    continue
                decision = latest.get(pid)
                if not _decision_is_complete(decision, stage):
                    needed.add(pid)
    except Exception:
        return set()
    return needed

def _archive_retry_csv(retry_csv: Path) -> None:
    """human readable hint: archive a fully resolved retry CSV to processed/."""

    if not retry_csv.exists():
        return
    processed_dir = retry_csv.parent / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H-%M-%S")
    target = processed_dir / f"{retry_csv.stem}_archived_{ts}{retry_csv.suffix}"
    try:
        shutil.move(str(retry_csv), str(target))
    except Exception:
        return

def _latest_retry_csv(stage: str) -> Path | None:
    """Locate the most recent retry CSV under input/retry_runs for this stage (any sample, screen/select)."""

    retry_dir = Path(PATH_SETTINGS["csv_dir"]) / "retry_runs"
    patterns = [
        f"{stage}_*_sample_*_csv_retry_*.csv",  # new naming
        f"{stage}_screen_csv_retry_*.csv",      # legacy naming
    ]
    candidates: list[Path] = []
    for pat in patterns:
        candidates.extend(retry_dir.glob(pat))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)

def _error_ids_by_type(error_log_path: Path, blocked_types: set[str]) -> set[str]:
    """human readable hint: collect paper_ids with deterministic errors that should not trigger auto-retry."""

    if not error_log_path.exists() or not blocked_types:
        return set()

    blocked_ids: set[str] = set()
    try:
        with error_log_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if not isinstance(obj, dict) or obj.get("meta"):
                    continue
                err_type = str(obj.get("error_type") or "").strip()
                if err_type not in blocked_types:
                    continue
                pid = str(obj.get("paper_id") or "").strip()
                if pid:
                    blocked_ids.add(pid)
    except Exception:
        return set()
    return blocked_ids
