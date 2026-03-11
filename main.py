import csv
import json
import os
import sys
import subprocess
import shutil
from datetime import datetime
from pathlib import Path

import nltk

from config.user_orchestrator import (
    CURRENT_STAGE,
    PATH_SETTINGS,
    LLM_API_KEY,
    LLM_MODEL,
    QC_ENABLED,
    QC_SAMPLE_RATE,
    STAGE_RULES,
)
from pipeline.core.run_screening import run_pipeline
from pipeline.additions.resource_usage import backfill_time_savings

# Track whether every interactive prompt in this run received a "yes" response.
_PROMPT_STATE: dict[str, object] = {
    "all_yes": True,
    "last_artifact": None,
    "validation_ran": False,
    "time_savings_ok": False,  # set when the user confirmed reviewer minutes are provided
}


def _last_artifact_dict() -> dict[str, object] | None:
    """Return last_artifact only when it is a dict; otherwise None for type safety."""

    value = _PROMPT_STATE.get("last_artifact")
    return value if isinstance(value, dict) else None


def _qc_screened_already(stage: str) -> bool:
    """Detect whether a QC sample for this stage was already screened."""

    stage_root = Path(PATH_SETTINGS.get("output_root", Path("output"))) / stage
    patterns = [
        f"{stage}_qc_sample_main_eligibility_*.jsonl",  # new naming (preferred)
        f"{stage}_qc_sample_eligibility_*.jsonl",       # fallback naming
        f"{stage}_eligibility_qc_sample_*.jsonl",       # legacy naming
    ]

    candidates: list[Path] = []
    for pat in patterns:
        matches = [p for p in stage_root.glob(pat) if "_retry_" not in p.name]
        candidates.extend(matches)

    if not candidates:
        return False

    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    try:
        return latest.exists() and latest.stat().st_size > 0
    except Exception:
        return False


def _run_pipeline_guarded(*, mark_failure: bool = True, **kwargs) -> bool:
    """Run the pipeline and store artifacts; mark prompts as not-all-yes on failure."""
    if "enable_time_savings" not in kwargs:
        # Enable time-savings from the start so resource_usage captures human-rate fields before prompts.
        kwargs["enable_time_savings"] = True

    result = run_pipeline(**kwargs)

    if isinstance(result, dict):
        success = bool(result.get("success", False))
        _PROMPT_STATE["last_artifact"] = result
    else:
        success = bool(result)
        _PROMPT_STATE["last_artifact"] = {"success": success}

    if mark_failure and not success:
        _PROMPT_STATE["all_yes"] = False
    return success


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


def _write_retry_csv(source_csv: Path, target_dir: Path, paper_ids: set[str], stage: str, run_label: str) -> Path | None:
    """Create a stage-valid retry CSV using run_label and stage-specific token (screen/select)."""

    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H-%M")
    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
    token = "screen" if stage == "title_abstract" else "select" if stage == "full_text" else "screen"
    filename = f"{stage}_{sample_tag}_sample_{token}_csv_retry_{timestamp}.csv"
    target_path = target_dir / filename

    if not source_csv.exists():
        print(f"[retry] source CSV missing: {source_csv}")
        return None

    id_keys = ["paper_id", "Covidence #", "Covidence#", "Ref", "Study", "ID", "id"]
    rows_written = 0
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
                    if pid in paper_ids:
                        writer.writerow(row)
                        rows_written += 1
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


def _retry_output_paths(stage: str, run_label: str, attempt_index: int) -> dict:
    """human readable hint: retry outputs stay separate using stage_runlabel_retry_attempt_output_timestamp order."""

    timestamp = datetime.now().strftime("%Y%m%d_%H-%M")
    stage_root = Path(PATH_SETTINGS["output_root"]) / stage
    stage_root.mkdir(parents=True, exist_ok=True)
    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
    base_prefix = f"{stage}_{sample_tag}_sample_retry_{attempt_index}"
    return {
        "eligibility": stage_root / f"{base_prefix}_eligibility_{timestamp}.jsonl",
        "text": stage_root / f"{base_prefix}_screening_results_readable_{timestamp}.txt",
        "chunks": stage_root / f"{base_prefix}_selected_chunks_{timestamp}.jsonl",
        "error": stage_root / f"{base_prefix}_error_log_{timestamp}.txt",
        "resource": stage_root / f"{base_prefix}_resource_usage_{timestamp}.log",
    }


def _latest_base_outputs(stage: str, run_label: str) -> dict[str, Path | None]:
    """human readable hint: locate the most recent outputs for a stage+run_label."""

    stage_root = Path(PATH_SETTINGS["output_root"]) / stage
    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label

    suffix_candidates: list[str] = []
    if run_label:
        suffix_candidates.append(f"{run_label}_main")
        suffix_candidates.append(run_label)
    if sample_tag:
        suffix_candidates.append(f"{sample_tag}_sample_main")
        suffix_candidates.append(f"{sample_tag}_sample")
        suffix_candidates.append(sample_tag)

    seen: set[str] = set()
    suffix_candidates = [s for s in suffix_candidates if s and not (s in seen or seen.add(s))]

    def _latest(pattern: str, *, prefer_all: bool = False) -> Path | None:
        matches = [p for p in stage_root.glob(pattern) if "_retry_" not in p.name]
        if prefer_all:
            matches = [
                p
                for p in matches
                if not any(tag in p.name for tag in ("eligibility_select_", "eligibility_irrelevant_", "eligibility_included_", "eligibility_excluded_"))
            ]
        return max(matches, key=lambda p: p.stat().st_mtime) if matches else None

    def _latest_for(token: str, ext: str) -> Path | None:
        for suffix in suffix_candidates:
            path = _latest(f"{stage}_{suffix}_{token}_*.{ext}", prefer_all=(token == "eligibility"))
            if path:
                return path
        return None

    return {
        "eligibility": _latest_for("eligibility", "jsonl"),
        "split_select": _latest_for("eligibility_select", "jsonl"),
        "split_exclude": _latest_for("eligibility_irrelevant", "jsonl"),
        "chunks": _latest_for("selected_chunks", "jsonl"),
        "text": _latest_for("screening_results_readable", "txt"),
        "resource": _latest_for("resource_usage", "log"),
        "emissions": _latest_for("codecarbon_emissions", "csv"),
    }


def _require_base_outputs(stage: str, run_label: str) -> dict[str, Path | None]:
    """Ensure base outputs exist before running a retry; avoid orphan retry files."""

    base = _latest_base_outputs(stage, run_label)
    if not base.get("eligibility"):
        print(
            f"[retry] Cannot run retry: missing base eligibility output for stage '{stage}' (run_label='{run_label}'). "
            "Run the main screening for this sample first so we can append to it."
        )
        return {}

    if not base.get("emissions"):
        print(
            f"[retry] Cannot run retry: missing base CodeCarbon file for stage '{stage}' (run_label='{run_label}'). "
            "Run the main screening for this sample with sustainability tracking enabled before retrying."
        )
        return {}

    return base


def _infer_run_label_from_retry_csv(path: Path, stage: str) -> str | None:
    """human readable hint: infer run_label from retry CSV name or existing base files."""

    name = path.name.lower()
    stage_root = Path(PATH_SETTINGS["output_root"]) / stage
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
        "attempt_index": attempt_default,
        "timestamp": datetime.utcnow().isoformat(),
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


def _merge_emissions_with_run_column(stage: str, run_label: str, attempt_index: int) -> dict[str, object] | None:
    """human readable hint: keep one CodeCarbon CSV per run_label; append new rows with run=main/retry_N and report row numbers."""

    stage_root = Path(PATH_SETTINGS["output_root"]) / stage
    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
    run_key = f"{sample_tag}_sample"
    pattern = f"{stage}_{run_key}_codecarbon_emissions_*.csv"
    files = sorted(stage_root.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not files:
        return None

    base_file = files[0]
    latest_file = files[-1]

    # If only one file exists (e.g., CodeCarbon reused the same file) and this is a retry,
    # assign the first data row to main and the rest to the retry attempt so runs stay distinguishable.
    if len(files) == 1 and attempt_index not in {None, 0}:
        try:
            with base_file.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()
        except Exception:
            return None
        if not lines:
            return None
        headers = lines[0].strip().split(",")
        if "run" not in headers:
            proj_idx = headers.index("project_name") if "project_name" in headers else 0
            headers.insert(proj_idx + 1, "run")
        run_idx = headers.index("run")
        rewritten = [",".join(headers) + "\n"]
        for idx, row in enumerate(lines[1:]):
            if not row.strip():
                continue
            cols = row.rstrip("\n").split(",")
            run_value = "main" if idx == 0 else f"retry_{attempt_index}"
            current = cols[run_idx] if len(cols) > run_idx else ""
            if len(cols) <= run_idx:
                cols.append(run_value)
            elif idx == 0:
                # Keep existing labels on the first row; only fill if empty.
                if not current:
                    cols[run_idx] = run_value
            else:
                # For retry rows, overwrite "main"/empty with the retry attempt but preserve any prior retry labels.
                if not current or current == "main":
                    cols[run_idx] = run_value
            rewritten.append(",".join(cols) + "\n")
        try:
            with base_file.open("w", encoding="utf-8") as handle:
                handle.writelines(rewritten)
        except Exception:
            return None
        rows = [idx + 1 for idx, row in enumerate(rewritten[1:]) if row.strip() and row.split(",")[run_idx] == f"retry_{attempt_index}"]
        return {"emissions_path": base_file, "emissions_rows": rows}

    def _rewrite_with_run(path: Path, run_value: str, *, override_existing: bool) -> list[str]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()
        except Exception:
            return []
        if not lines:
            return []
        headers = lines[0].strip().split(",")
        run_idx = None
        if "run" in headers:
            run_idx = headers.index("run")
        else:
            proj_idx = headers.index("project_name") if "project_name" in headers else 0
            headers.insert(proj_idx + 1, "run")
            run_idx = proj_idx + 1

        rewritten: list[str] = [",".join(headers) + "\n"]
        for row in lines[1:]:
            if not row.strip():
                continue
            cols = row.rstrip("\n").split(",")
            current = cols[run_idx] if len(cols) > run_idx else ""
            if len(cols) <= run_idx:
                cols.append(run_value)
            elif override_existing or not current:
                cols[run_idx] = run_value
            rewritten.append(",".join(cols) + "\n")

        try:
            with path.open("w", encoding="utf-8") as handle:
                handle.writelines(rewritten)
        except Exception:
            return []
        return rewritten

    def _ensure_base_run_labels(path: Path, run_value: str) -> None:
        """human readable hint: make sure every existing row carries a run label before merges."""

        if not path.exists():
            return
        _rewrite_with_run(path, run_value, override_existing=False)

    _ensure_base_run_labels(base_file, "main")

    _rewrite_with_run(base_file, "main", override_existing=False)
    if latest_file == base_file:
        return {"emissions_path": base_file, "emissions_rows": []}

    run_value = "main" if attempt_index in {None, 0} else f"retry_{attempt_index}"
    latest_lines = _rewrite_with_run(latest_file, run_value, override_existing=True)
    if not latest_lines:
        return None

    appended_rows: list[int] = []
    try:
        existing_lines = []
        if base_file.exists():
            with base_file.open("r", encoding="utf-8") as existing:
                existing_lines = existing.readlines()
        start_row = max(len(existing_lines) - 1, 0)
        with base_file.open("a", encoding="utf-8") as out:
            for idx, row in enumerate(latest_lines[1:], start=1):
                out.write(row if row.endswith("\n") else row + "\n")
                if row.strip():
                    appended_rows.append(start_row + idx)
        latest_file.unlink(missing_ok=True)
    except Exception:
        return None

    return {"emissions_path": base_file, "emissions_rows": appended_rows}


def _extract_summary_stats(path: Path) -> tuple[int, float, float, float, float]:
    """human readable hint: derive counts and summary percentiles from eligibility JSONL."""

    count = 0
    percent = 0.0
    p50 = 0.0
    p95 = 0.0
    pmax = 0.0
    if not path or not path.exists():
        return count, percent, p50, p95, pmax
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict) and obj.get("meta") == "summary":
                    percent = obj.get("percent_of_stage", 0.0) or 0.0
                    stats = obj.get("response_time_seconds", {}) or {}
                    p50 = stats.get("p50", 0.0) or 0.0
                    p95 = stats.get("p95", 0.0) or 0.0
                    pmax = stats.get("max", 0.0) or 0.0
                    continue
                if isinstance(obj, dict) and obj.get("meta"):
                    continue
                count += 1
    except Exception:
        return count, percent, p50, p95, pmax
    return count, percent, p50, p95, pmax


def _run_tag_for_path(path: Path, stage: str, output_token: str) -> str:
    """human readable hint: derive run tag (sample + timestamp + retry) from filename."""

    stem = path.stem
    prefix = f"{stage}_"
    if stem.startswith(prefix):
        stem = stem[len(prefix) :]
    marker = f"{output_token}_"
    if marker in stem:
        stem = stem.replace(marker, "", 1)
    return stem


def _append_index_row(
    idx_path: Path,
    sample_selection: str,
    stage: str,
    decision_split: str,
    path: Path,
    stats: tuple[int, float, float, float, float],
    total_paper_count: int | None = None,
) -> None:
    """human readable hint: write/update one row in eligibility index for a decision split."""

    count, percent, p50, p95, pmax = stats
    percent_of_input = (count / total_paper_count * 100.0) if total_paper_count else 0.0
    fieldnames = [
        "sample_selection",
        "stage",
        "decision_split",
        "paper_count",
        "percent_of_stage",
        "p50_seconds",
        "p95_seconds",
        "max_seconds",
        "timestamp",
        "file_path",
        "total_paper_count",
        "percent_of_input_file",
    ]

    rows: list[dict[str, object]] = []
    if idx_path.exists() and idx_path.stat().st_size > 0:
        try:
            with idx_path.open("r", newline="", encoding="utf-8") as existing:
                reader = csv.DictReader(existing)
                for row in reader:
                    if row:
                        rows.append(dict(row))
        except Exception:
            rows = []

    new_row = {
        "sample_selection": sample_selection,
        "stage": stage,
        "decision_split": decision_split,
        "paper_count": count,
        "percent_of_stage": percent,
        "p50_seconds": p50,
        "p95_seconds": p95,
        "max_seconds": pmax,
        "timestamp": datetime.utcnow().isoformat(),
        "file_path": str(path),
        "total_paper_count": total_paper_count or 0,
        "percent_of_input_file": percent_of_input,
    }

    rows = [r for r in rows if r.get("sample_selection") != new_row["sample_selection"]]
    rows.append(new_row)

    try:
        with idx_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    except Exception:
        return


def _update_index_from_artifact(stage: str, artifact: dict | None, attempt_index: int) -> None:
    """human readable hint: append index rows for all eligibility splits from a run (base or retry)."""

    if not artifact or not isinstance(artifact, dict):
        return

    run_label = artifact.get("run_label") or "remaining_sample"
    stage_root = Path(PATH_SETTINGS.get("output_root", "output")) / stage
    idx_path = stage_root / f"{stage}_eligibility_index.csv"

    def _to_path(val: object) -> Path | None:
        return Path(val) if isinstance(val, (str, os.PathLike)) else None

    def _count_records(path: Path | None) -> int:
        if not path or not path.exists():
            return 0
        total = 0
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip() or '"meta":' in line:
                        continue
                    total += 1
        except Exception:
            return 0
        return total

    def _count_input_rows(paths: list[Path]) -> int:
        total = 0
        seen: set[Path] = set()
        for p in paths:
            if not p or not p.exists() or p in seen:
                continue
            seen.add(p)
            try:
                with p.open("r", encoding="utf-8") as handle:
                    reader = csv.reader(handle)
                    # subtract header row if present
                    row_count = sum(1 for _ in reader)
                    if row_count > 0:
                        row_count -= 1
                    if row_count > 0:
                        total += row_count
            except Exception:
                continue
        return total

    elig_path = _to_path(artifact.get("eligibility_path") or artifact.get("eligibility"))
    split_paths_raw = artifact.get("split_paths") if isinstance(artifact.get("split_paths"), dict) else {}
    split_paths: dict[str, object] = dict(split_paths_raw) if isinstance(split_paths_raw, dict) else {}

    stage_csvs = [Path(p) for p in artifact.get("stage_csv_files", []) if p]
    qc_csv = _to_path(artifact.get("qc_sample_path"))
    input_paths = stage_csvs if stage_csvs else ([qc_csv] if qc_csv else [])
    total_input_rows = _count_input_rows(input_paths)

    base_outputs = _latest_base_outputs(stage, run_label)
    baseline_total = _count_records(base_outputs.get("eligibility"))

    entries: list[tuple[str, Path | None, str]] = []
    entries.append(("all", elig_path, "eligibility"))
    if stage == "title_abstract":
        entries.append(("select", _to_path(split_paths.get("select")), "eligibility_select"))
        entries.append(("irrelevant", _to_path(split_paths.get("irrelevant")), "eligibility_irrelevant"))
    elif stage == "full_text":
        entries.append(("included", _to_path(split_paths.get("included")), "eligibility_included"))
        entries.append(("excluded", _to_path(split_paths.get("excluded")), "eligibility_excluded"))

    for decision_label, path_obj, token in entries:
        if not path_obj or not path_obj.exists():
            continue
        stats = _extract_summary_stats(path_obj)
        count, percent, p50, p95, pmax = stats
        if baseline_total > 0:
            percent = round((count / baseline_total) * 100, 6)
        stats = (count, percent, p50, p95, pmax)
        run_tag = _run_tag_for_path(path_obj, stage, token)
        sample_selection = f"{stage}_{run_tag}_{decision_label}"
        _append_index_row(
            idx_path,
            sample_selection,
            stage,
            decision_label,
            path_obj,
            stats,
            total_paper_count=total_input_rows,
        )


def _post_run_updates(stage: str, artifact: dict | None, attempt_index: int) -> dict[str, object] | None:
    """human readable hint: after any run, merge emissions and refresh eligibility index."""

    if not artifact or not isinstance(artifact, dict):
        return None
    if not artifact.get("success", True):
        return None
    run_label = artifact.get("run_label") or "remaining_sample"
    emissions_info = _merge_emissions_with_run_column(stage, run_label, attempt_index)
    _update_index_from_artifact(stage, artifact, attempt_index)
    return emissions_info


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
    candidates = sorted(stage_root.glob(pattern))
    if not candidates:
        return {}
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    records: dict[str, object] = {}
    try:
        with latest.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip() or '"meta": "' in line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                pid = str(obj.get("paper_id", ""))
                if pid:
                    records[pid] = obj.get("llm_decision")
    except Exception:
        return {}
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
                pid = str(row.get("paper_id") or row.get("Covidence #") or row.get("Covidence#") or "").strip()
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


def _prompt_retry_if_needed(stage: str, artifact: dict | None) -> None:
    """Prompt for re-screening when errors are present for this stage."""
    if not artifact:
        return

    err_path = artifact.get("error_log_path")
    if not err_path:
        return

    error_path = Path(err_path)
    if not error_path.exists() or error_path.stat().st_size == 0:
        return

    candidates: set[str] = set()
    if stage in {"title_abstract", "full_text"}:
        elig_path = artifact.get("eligibility_path")
        if not elig_path:
            return
        eligibility_path = Path(elig_path)
        if not eligibility_path.exists():
            return
        candidates = _collect_missing_is_eligible(error_path, eligibility_path, stage)
        if not candidates:
            print("[retry] All error cases contain is_eligible; no retry suggested.")
            return

        blocked_ids = _error_ids_by_type(error_path, {"llm_output_token_limit", "context_overflow"})
        blocked_candidates = {pid for pid in candidates if pid in blocked_ids}
        if blocked_candidates:
            print(
                "[retry] Some papers were not queued for auto-retry because they failed deterministically "
                "(token-limit/context-overflow). Adjust max_tokens or reduce prompt payload first:"
            )
            for pid in sorted(blocked_candidates):
                print(f"  - {pid}")
            candidates = {pid for pid in candidates if pid not in blocked_candidates}
            if not candidates:
                print("[retry] No remaining candidates for automatic retry after deterministic-failure filtering.")
                return

        print("[retry] Papers with missing is_eligible after screening:")
    elif stage == "data_extraction":
        candidates = set(artifact.get("error_ids", []) or [])
        if not candidates:
            print("[retry] Errors logged but no paper IDs were captured; no retry suggested.")
            return
        print("[retry] Papers with extraction errors or truncated outputs:")
    else:
        return

    for pid in sorted(candidates):
        print(f"  - {pid}")

    if not _prompt_yes_no("[retry] Re-screen these papers now? [y/n]: "):
        return

    run_label = str(artifact.get("run_label", "remaining_sample"))
    next_attempt = _next_retry_attempt(stage, run_label)
    attempt_map: dict[str, int] = {pid: next_attempt for pid in candidates}
    stage_csv_files = [Path(p) for p in artifact.get("stage_csv_files", []) if p]
    qc_sample_path = artifact.get("qc_sample_path")

    source_csv = None
    if run_label == "qc_sample" and qc_sample_path:
        candidate = Path(qc_sample_path)
        if candidate.exists():
            source_csv = candidate
    if source_csv is None:
        for candidate in stage_csv_files:
            if candidate.exists():
                source_csv = candidate
                break

    if source_csv is None:
        print("[retry] Could not locate a source CSV for retry. Aborting retry step.")
        return

    base_outputs = _require_base_outputs(stage, run_label)
    if not base_outputs:
        return

    target_dir = Path(PATH_SETTINGS["csv_dir"]) / "retry_runs"
    retry_csv = _write_retry_csv(source_csv, target_dir, candidates, stage, run_label)
    if not retry_csv:
        return

    attempt_for_run = max(attempt_map.values()) if attempt_map else 1
    out_paths = _retry_output_paths(stage, run_label, attempt_for_run)
    print(f"[retry] Created retry CSV at {retry_csv}. Running re-screen (QC disabled for retry)...")
    retry_artifact = run_pipeline(
        stage=stage,
        csv_dir=str(target_dir),
        qc_enabled=False,
        confirm_sampling=False,
        quiet=False,
        eligibility_output=out_paths.get("eligibility"),
        chunks_output=out_paths.get("chunks"),
        text_output=out_paths.get("text"),
        error_log=out_paths.get("error"),
        resource_log=out_paths.get("resource"),
        sustainability_tracking=True,
        run_label_override=run_label,
    )

    if isinstance(retry_artifact, dict):
        _PROMPT_STATE["last_artifact"] = retry_artifact
        success = bool(retry_artifact.get("success", False))
    else:
        success = bool(retry_artifact)

    if success:
        retry_artifact_dict = retry_artifact if isinstance(retry_artifact, dict) else _last_artifact_dict()
        if not isinstance(retry_artifact_dict, dict):
            print("[retry] No retry artifact available; manifest not updated.")
            return
        emissions_info = _post_run_updates(stage, retry_artifact_dict, attempt_for_run)

        stage_root = Path(PATH_SETTINGS.get("output_root", "output")) / stage
        manifest_path = stage_root / f"{stage}_retry_manifest.jsonl"
        _record_retry_manifest(retry_artifact_dict, stage, attempt_map, retry_csv, emissions_info)
        print(f"[retry] Re-screen completed. Outputs kept separate; manifest updated at {manifest_path}.")

        # human readable hint: if the retry still has errors, offer another retry prompt instead of stopping silently.
        if retry_artifact_dict.get("error_log_path"):
            _prompt_retry_if_needed(stage, retry_artifact_dict)
    else:
        print("[retry] Re-screen failed. Check the retry error log for details.")


def _ensure_csv_inputs(csv_dir: Path) -> bool:
    """Check that the input folder exists and has at least one CSV file.

    Args:
        csv_dir: Path to the input/ folder containing Covidence exports.

    Returns:
        True if at least one CSV exists; False otherwise.

    Note: this prevents running the pipeline with missing exports.
    """
    if not csv_dir.exists():
        print(f"[setup] Create the folder at {csv_dir} and drop your Covidence/CSV exports there.")
        return False
    csvs = sorted(csv_dir.glob("*.csv"))
    if not csvs:
        print(f"[setup] No CSV files found in {csv_dir}. Place your exported CSV files there and rerun.")
        return False
    return True


def _require_pattern(csv_dir: Path, pattern: str, description: str, stage: str | None = None) -> list[Path]:
    """Ensure required CSVs exist for the current stage (pick latest when multiple).

    Args:
        csv_dir: Path to the input/ folder.
        pattern: Glob pattern for required CSV files.
        description: Human-readable description of the required export.
        stage: Optional stage label for extra sanity checks.

    Returns:
        A list containing the latest matching CSV path (empty if none found).

    Note: deterministic choice avoids ambiguity when several exports are present.
    """
    matches = sorted(csv_dir.glob(pattern))
    if not matches:
        print(f"[setup] Missing {description}. Expected a file matching '{pattern}' in {csv_dir}.")
        return []

    chosen = max(matches, key=lambda p: p.stat().st_mtime)
    if stage and stage not in chosen.name:
        print(f"[warning] Using {chosen.name} for stage '{stage}'. Confirm this is intentional.")
    if len(matches) > 1:
        print(f"[info] Multiple matches for {description}; using most recent: {chosen.name}")
    return [chosen]


def _missing_pdf_folders(base_dir: Path) -> list[str]:
    """List per-paper folders that still have no PDF file.

    Args:
        base_dir: Path to the per-paper folder root (e.g., input/per_paper_full_text/).

    Returns:
        A list of folder names missing a PDF file.

    Note: missing PDFs are skipped in full_text/data_extraction.
    """
    if not base_dir.exists():
        return []
    missing: list[str] = []
    for folder in sorted(base_dir.iterdir()):
        if folder.is_dir() and not any(folder.glob("*.pdf")):
            missing.append(folder.name)
    return missing


def _ensure_nltk_tokenizers() -> None:
    """Download NLTK sentence tokenizers once so sentence splitting works.

    Note: required for consistent sentence chunking.
    """
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)
    try:
        nltk.data.find("tokenizers/punkt_tab")
    except LookupError:
        nltk.download("punkt_tab", quiet=True)


def _prompt_yes_no(message: str) -> bool:
    """Ask a yes/no question in the terminal and return True for yes.

    Args:
        message: Prompt text displayed to the user.

    Returns:
        True for yes, False for no (or non-interactive terminal).

    Note: keeps QC decisions explicit and auditable.
    """
    if not sys.stdin.isatty():
        print("[error] This workflow requires an interactive terminal.")
        _PROMPT_STATE["all_yes"] = False
        return False
    while True:
        resp = input(message).strip().lower()
        if resp in {"y", "yes"}:
            # Keep the all-yes flag true when the user affirms.
            _PROMPT_STATE["all_yes"] = _PROMPT_STATE.get("all_yes", True)
            return True
        if resp in {"n", "no"}:
            _PROMPT_STATE["all_yes"] = False
            return False
        print("Please answer 'y' or 'n'.")


def _run_validation() -> bool:
    """human readable hint: run validation and return True on success."""

    has_times = _prompt_yes_no(
        f"[qc] Have estimated reviewer times (minutes) been inserted for human reviewers at CURRENT_STAGE='{CURRENT_STAGE}'? [y/n]: "
    )
    _PROMPT_STATE["time_savings_ok"] = bool(has_times)
    if not has_times:
        print("[qc] Please add estimated reviewer times before running validation.")
        _PROMPT_STATE["all_yes"] = False
        _PROMPT_STATE["validation_ran"] = False
        return False

    # After the user confirms minutes, backfill the latest resource_usage log with time-savings fields.
    artifact = _last_artifact_dict()
    if artifact and isinstance(artifact, dict):
        res_path = artifact.get("resource_log_path") or artifact.get("resource")
        qc_path = artifact.get("qc_sample_path")

        res_path_obj: Path | None
        if isinstance(res_path, Path):
            res_path_obj = res_path
        elif isinstance(res_path, (str, os.PathLike)):
            res_path_obj = Path(res_path)
        else:
            res_path_obj = None

        qc_path_obj: Path | None
        if isinstance(qc_path, Path):
            qc_path_obj = qc_path
        elif isinstance(qc_path, (str, os.PathLike)):
            qc_path_obj = Path(qc_path)
        else:
            qc_path_obj = None

        if res_path_obj:
            backfill_time_savings(res_path_obj, CURRENT_STAGE, qc_path_obj)

    if not _prompt_yes_no("[qc] Run validation now? [y/n]: "):
        _PROMPT_STATE["validation_ran"] = False
        return False

    print("[qc] Running validation using auto-detected CSVs in input/. If files are missing, a warning will appear.")
    result = subprocess.run([sys.executable, "-m", "pipeline.additions.stats_engine"], check=False)
    if result.returncode != 0:
        _PROMPT_STATE["all_yes"] = False
        _PROMPT_STATE["validation_ran"] = False
        return False

    _PROMPT_STATE["validation_ran"] = True
    return True
def _run_qc_loop(stage: str, sample_rate: float, quiet: bool = False) -> bool:
    """Run QC-only screening, validation prompt, and decision loop.

    Returns True if the user approves validation and wants full screening.

    Args:
        stage: Current pipeline stage (title_abstract/full_text/data_extraction).
        sample_rate: Fraction of planned papers to include in QC.
        quiet: If True, suppress most console output.

    Returns:
        True if user approves validation and proceeds to full screening; False otherwise.
    """
    force_new_qc = False
    while True:
        if not force_new_qc and _qc_screened_already(stage):
            print("[qc] Existing QC screening found; skipping re-screen of QC sample.")
            ran = True
            _PROMPT_STATE["last_artifact"] = {"success": True}
        else:
            ran = _run_pipeline_guarded(
                stage=stage,
                confirm_sampling=False,
                sample_rate=sample_rate,
                qc_only=True,
                qc_enabled=True,
                force_new_qc=force_new_qc,
                quiet=quiet,
            )
            _prompt_retry_if_needed(stage, _last_artifact_dict())
            _post_run_updates(stage, _last_artifact_dict(), 0)
        if not ran:
            return False
        force_new_qc = False
        print("[qc] QC-only screening complete.")

        if not _run_validation():
            print("[qc] Validation skipped or failed. Rerun main.py to continue.")
            _PROMPT_STATE["all_yes"] = False
            return False

        if _prompt_yes_no("[qc] Are you satisfied with validation results and do you want to continue with screening of the remaining papers? [y/n]: "):
            return True

        # If not satisfied, stop and let the operator refine prompts/config before rerun.
        _PROMPT_STATE["all_yes"] = False
        return False


class MainWorkflow:
    """human readable hint: one-class orchestrator for terminal flow, retries, QC gating, and stage execution."""

    def __init__(self) -> None:
        """human readable hint: __init__ keeps the key runtime attributes visible in one place."""

        self.stage = CURRENT_STAGE
        self.csv_dir = Path(PATH_SETTINGS["csv_dir"])
        self.sample_rate = QC_SAMPLE_RATE

    def run(self) -> None:
        """Run the pipeline for the selected stage with safety checks."""

        if not LLM_API_KEY:
            print("[error] LLM_API_KEY is empty. Set it in .env or config/user_orchestrator.py before running.")
            return

        stage = self.stage
        csv_dir = self.csv_dir
        sample_rate = self.sample_rate

        print(f"[main] Stage: {stage} | Model: {LLM_MODEL}")

        if stage not in STAGE_RULES:
            print(f"[error] Unknown CURRENT_STAGE='{stage}'. Choose from {sorted(STAGE_RULES)}.")
            return

        if not _ensure_csv_inputs(csv_dir):
            return

        _ensure_nltk_tokenizers()

        if not sys.stdin.isatty():
            print("[error] QC confirmation requires an interactive terminal. Rerun in an interactive session.")
            return

        if not _prompt_yes_no("[qc] Are study tags the same since the last run? [y/n]: "):
            print("[qc] Update STUDY_TAGS_INCLUDE/STUDY_TAGS_IGNORE in config/user_orchestrator.py.")
            return

        retry_csv = _latest_retry_csv(stage)
        if retry_csv:
            pending_ids = _retry_csv_needed(retry_csv, stage)
            if not pending_ids:
                print(f"[retry] Retry CSV {retry_csv.name} already has complete decisions; skipping retry prompt.")
            else:
                hinted = _infer_run_label_from_retry_csv(retry_csv, stage)
                run_label = _first_available_run_label(stage, hinted)
                if not run_label:
                    print("[retry] Base outputs missing for both qc_sample and remaining_sample; run a base screening first.")
                else:
                    print(f"[retry] Detected retry CSV at {retry_csv} with {len(pending_ids)} pending paper(s). Re-screen before the new run?")
                    if _prompt_yes_no("[retry] Run pending retry CSV first? [y/n]: "):
                        attempt_map: dict[str, int] = {}
                        attempt_for_run = _next_retry_attempt(stage, run_label)
                        for pid in pending_ids:
                            attempt_map[pid] = attempt_for_run
                        filtered_retry_csv = _write_retry_csv(retry_csv, retry_csv.parent, pending_ids, stage, run_label)
                        if not filtered_retry_csv:
                            print("[retry] Could not build a filtered retry CSV; aborting retry step.")
                            return
                        retry_csv = filtered_retry_csv
                        retry_out = _retry_output_paths(stage, run_label, attempt_for_run)
                        _run_pipeline_guarded(
                            stage=stage,
                            csv_dir=str(retry_csv.parent),
                            qc_enabled=False,
                            confirm_sampling=False,
                            quiet=False,
                            eligibility_output=retry_out.get("eligibility"),
                            chunks_output=retry_out.get("chunks"),
                            text_output=retry_out.get("text"),
                            error_log=retry_out.get("error"),
                            resource_log=retry_out.get("resource"),
                            run_label_override=run_label,
                        )
                        artifact = _last_artifact_dict()
                        emissions_info = _post_run_updates(stage, artifact, attempt_for_run)
                        _record_retry_manifest(artifact, stage, attempt_map, retry_csv, emissions_info)
                        manifest_path = Path(PATH_SETTINGS.get("output_root", "output")) / stage / f"{stage}_retry_manifest.jsonl"
                        print(f"[retry] Outputs kept separate; manifest updated at {manifest_path}.")
                        _prompt_retry_if_needed(stage, artifact)

                        if not _retry_csv_needed(retry_csv, stage):
                            _archive_retry_csv(retry_csv)

        rule = STAGE_RULES[stage]
        for pattern in rule["screen_patterns"]:
            if not _require_pattern(csv_dir, pattern, f"{stage} required CSV export", stage=stage):
                return

        if stage == "title_abstract":
            if QC_ENABLED:
                if _run_qc_loop(stage, sample_rate, quiet=False):
                    _run_pipeline_guarded(stage=stage, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
                    _post_run_updates(stage, _last_artifact_dict(), 0)
                    _prompt_retry_if_needed(stage, _last_artifact_dict())
                return
            _run_pipeline_guarded(stage=stage, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
            _post_run_updates(stage, _last_artifact_dict(), 0)
            _prompt_retry_if_needed(stage, _last_artifact_dict())
            return

        if QC_ENABLED:
            print(f"[main] Preparing per-paper folders for {stage} (no screening in this step)...")
            _run_pipeline_guarded(stage=stage, split_only=True, quiet=True, mark_failure=False)

            if stage == "data_extraction":
                full_text_dir = csv_dir / "per_paper_full_text"
                if not full_text_dir.exists():
                    print(
                        f"[warning] per_paper_full_text missing at {full_text_dir}. "
                        "Run the full_text stage first (or rerun after creating full_text folders)."
                    )
                    return

            paper_dir = csv_dir / rule["pdf_dir"]
            if not paper_dir.exists():
                print(f"[setup] Expected per-paper folders at {paper_dir}. Rerun after generating CSV exports.")
                return

            missing = _missing_pdf_folders(paper_dir)
            if missing:
                print(
                    f"[setup] PDFs missing for {len(missing)} folder(s) in {rule['pdf_dir']}."
                    " Screening will proceed; missing folders will be skipped and logged."
                )
                for name in missing:
                    print(f"  - {name}")

            if _run_qc_loop(stage, sample_rate, quiet=False):
                _run_pipeline_guarded(stage=stage, quiet=False, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
                _post_run_updates(stage, _last_artifact_dict(), 0)
                _prompt_retry_if_needed(stage, _last_artifact_dict())
            return

        _run_pipeline_guarded(stage=stage, quiet=False, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
        _post_run_updates(stage, _last_artifact_dict(), 0)
        _prompt_retry_if_needed(stage, _last_artifact_dict())
        return


def main() -> None:
    """Compatibility entrypoint that runs the class-based main workflow."""

    MainWorkflow().run()


if __name__ == "__main__":
    main()
    # Offer to trigger backup after pipeline run only if every prompt was accepted.
    try:
        if _PROMPT_STATE.get("all_yes", False):
            resp = input("\nDo you want to back up your changes to GitHub now? (y/n): ").strip().lower()
            if resp == "y":
                import subprocess
                import sys
                subprocess.run([sys.executable, "backup_to_github.py"])
    except Exception:
        print("[warning] Could not trigger backup script. Please run backup_to_github.py manually if needed.")
