"""Run-output indexing, summary, and tracking-file helpers for main.py."""

from __future__ import annotations

import csv
import json
import math
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from config.user_orchestrator import CITATION_SEARCHING_SCREENING, CITATION_SEARCHING_STAGE_RULES, PATH_SETTINGS
from pipeline.additions.emissions_merge import (
    merge_emissions_with_run_column as _merge_emissions_with_run_column_csvsafe,
)

def _stage_root(stage: str) -> Path:
    """human readable hint: keep citation-searching index files beside citation-searching outputs."""

    output_root = Path(PATH_SETTINGS.get("output_root", "output"))
    if CITATION_SEARCHING_SCREENING:
        citation_rule = CITATION_SEARCHING_STAGE_RULES.get(stage, {})
        output_dir = citation_rule.get("output_dir")
        if output_dir:
            return output_root / str(output_dir)
    return output_root / stage


def _latest_base_outputs(stage: str, run_label: str) -> dict[str, Path | None]:
    """human readable hint: locate the most recent outputs for a stage+run_label."""

    stage_root = _stage_root(stage)
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
        "split_included": _latest_for("eligibility_included", "jsonl"),
        "split_excluded": _latest_for("eligibility_excluded", "jsonl"),
        "chunks": _latest_for("selected_chunks", "jsonl"),
        "text": _latest_for("screening_results_readable", "txt"),
        "resource": _latest_for("resource_usage", "log"),
        "emissions": _latest_for("codecarbon_emissions", "csv"),
    }

def _latest_qc_sample_csv(stage: str) -> Path | None:
    """human readable hint: find the newest QC sample CSV for a stage, if present."""

    stage_root = _stage_root(stage)
    matches = sorted(stage_root.glob(f"{stage}_qc_sample_batch_*.csv"))
    return max(matches, key=lambda p: p.stat().st_mtime) if matches else None

def _artifact_from_latest_base_outputs(stage: str, run_label: str) -> dict[str, object] | None:
    """human readable hint: synthesize a minimal run artifact from latest persisted base outputs."""

    base = _latest_base_outputs(stage, run_label)
    eligibility = base.get("eligibility")
    if not eligibility or not eligibility.exists():
        return None

    split_paths: dict[str, str] = {}
    if stage == "title_abstract":
        select_path = base.get("split_select")
        irrelevant_path = base.get("split_exclude")
        if select_path and select_path.exists():
            split_paths["select"] = str(select_path)
        if irrelevant_path and irrelevant_path.exists():
            split_paths["irrelevant"] = str(irrelevant_path)
    elif stage == "full_text":
        included_path = base.get("split_included")
        excluded_path = base.get("split_excluded")
        if included_path and included_path.exists():
            split_paths["included"] = str(included_path)
        if excluded_path and excluded_path.exists():
            split_paths["excluded"] = str(excluded_path)

    artifact: dict[str, object] = {
        "success": True,
        "run_label": run_label,
        "stage": stage,
        "eligibility_path": str(eligibility),
        "split_paths": split_paths,
    }

    chunks_path = base.get("chunks")
    if isinstance(chunks_path, Path) and chunks_path.exists():
        artifact["chunks_path"] = str(chunks_path)

    text_path = base.get("text")
    if isinstance(text_path, Path) and text_path.exists():
        artifact["text_path"] = str(text_path)

    resource_path = base.get("resource")
    if isinstance(resource_path, Path) and resource_path.exists():
        artifact["resource_log_path"] = str(resource_path)

    if run_label == "qc_sample":
        qc_csv = _latest_qc_sample_csv(stage)
        if qc_csv and qc_csv.exists():
            artifact["qc_sample_path"] = str(qc_csv)

    return artifact

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

def _extract_total_paper_count_from_summary(path: Path | None) -> int | None:
    """human readable hint: read total_paper_count from the summary meta row when present."""

    if not path or not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if not isinstance(obj, dict) or obj.get("meta") != "summary":
                    continue
                raw_total = obj.get("total_paper_count")
                parsed_total: int | None = None
                if isinstance(raw_total, int) and raw_total > 0:
                    parsed_total = raw_total
                elif isinstance(raw_total, float) and raw_total > 0:
                    parsed_total = int(raw_total)
                elif isinstance(raw_total, str):
                    try:
                        parsed = int(float(raw_total.strip()))
                        if parsed > 0:
                            parsed_total = parsed
                    except Exception:
                        parsed_total = None

                raw_count = obj.get("paper_count")
                parsed_count: int | None = None
                if isinstance(raw_count, int) and raw_count > 0:
                    parsed_count = raw_count
                elif isinstance(raw_count, float) and raw_count > 0:
                    parsed_count = int(raw_count)
                elif isinstance(raw_count, str):
                    try:
                        parsed = int(float(raw_count.strip()))
                        if parsed > 0:
                            parsed_count = parsed
                    except Exception:
                        parsed_count = None

                raw_percent = obj.get("percent_of_input_file")
                parsed_percent: float | None = None
                if isinstance(raw_percent, (int, float)):
                    parsed_percent = float(raw_percent)
                elif isinstance(raw_percent, str):
                    try:
                        parsed_percent = float(raw_percent.strip())
                    except Exception:
                        parsed_percent = None

                inferred_total: int | None = None
                if isinstance(parsed_count, int) and isinstance(parsed_percent, float) and parsed_percent > 0:
                    inferred = int(round((parsed_count * 100.0) / parsed_percent))
                    if inferred > 0:
                        inferred_total = inferred

                if isinstance(parsed_total, int) and parsed_total > 0:
                    if isinstance(inferred_total, int) and inferred_total > parsed_total and parsed_percent is not None and parsed_percent < 100.0:
                        return inferred_total
                    return parsed_total
                if isinstance(inferred_total, int) and inferred_total > 0:
                    return inferred_total
                return None
    except Exception:
        return None
    return None

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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "file_path": str(path),
        "total_paper_count": total_paper_count or 0,
        "percent_of_input_file": percent_of_input,
    }

    rows = [r for r in rows if r.get("sample_selection") != new_row["sample_selection"]]
    rows.append(new_row)

    try:
        with idx_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in fieldnames})
    except Exception:
        return

def _update_index_from_artifact(stage: str, artifact: dict | None, attempt_index: int) -> None:
    """human readable hint: append index rows for all eligibility splits from a run (base or retry)."""

    if not artifact or not isinstance(artifact, dict):
        return

    run_label = artifact.get("run_label") or "remaining_sample"
    stage_root = _stage_root(stage)
    idx_path = stage_root / f"{stage}_eligibility_index.csv"

    def _to_path(val: object) -> Path | None:
        return Path(val) if isinstance(val, (str, os.PathLike)) else None

    def _existing_or_fallback(primary: Path | None, fallback: Path | None) -> Path | None:
        if isinstance(primary, Path) and primary.exists():
            return primary
        if isinstance(fallback, Path) and fallback.exists():
            return fallback
        return None

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

    base_outputs = _latest_base_outputs(stage, run_label)

    elig_path_raw = _to_path(artifact.get("eligibility_path") or artifact.get("eligibility"))
    elig_path = _existing_or_fallback(elig_path_raw, base_outputs.get("eligibility"))
    split_paths_raw = artifact.get("split_paths") if isinstance(artifact.get("split_paths"), dict) else {}
    split_paths: dict[str, object] = dict(split_paths_raw) if isinstance(split_paths_raw, dict) else {}

    stage_csvs = [Path(p) for p in artifact.get("stage_csv_files", []) if p]
    qc_csv = _to_path(artifact.get("qc_sample_path"))
    input_paths = stage_csvs if stage_csvs else ([qc_csv] if qc_csv else [])
    total_input_rows = _count_input_rows(input_paths)
    if total_input_rows <= 0:
        summary_total = _extract_total_paper_count_from_summary(elig_path)
        if isinstance(summary_total, int) and summary_total > 0:
            total_input_rows = summary_total
    if total_input_rows <= 0 and isinstance(elig_path, Path):
        total_input_rows = _count_records(elig_path)

    baseline_total = _count_records(base_outputs.get("eligibility"))
    if baseline_total <= 0 and isinstance(elig_path, Path):
        baseline_total = _count_records(elig_path)

    entries: list[tuple[str, Path | None, str]] = []
    entries.append(("all", elig_path, "eligibility"))
    if stage == "title_abstract":
        select_path = _existing_or_fallback(_to_path(split_paths.get("select")), base_outputs.get("split_select"))
        irrelevant_path = _existing_or_fallback(_to_path(split_paths.get("irrelevant")), base_outputs.get("split_exclude"))
        entries.append(("select", select_path, "eligibility_select"))
        entries.append(("irrelevant", irrelevant_path, "eligibility_irrelevant"))
    elif stage == "full_text":
        included_path = _existing_or_fallback(_to_path(split_paths.get("included")), base_outputs.get("split_included"))
        excluded_path = _existing_or_fallback(_to_path(split_paths.get("excluded")), base_outputs.get("split_excluded"))
        entries.append(("included", included_path, "eligibility_included"))
        entries.append(("excluded", excluded_path, "eligibility_excluded"))

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

def _ensure_qc_rows_in_index(stage: str) -> None:
    """human readable hint: guarantee QC sample rows are present in the eligibility index."""

    qc_artifact = _artifact_from_latest_base_outputs(stage, "qc_sample")
    if qc_artifact:
        _update_index_from_artifact(stage, qc_artifact, 0)

def _latest_total_resource_entry(path: Path | None) -> dict[str, object] | None:
    """human readable hint: read the last TOTAL row from a resource_usage JSONL file."""

    if not path or not path.exists():
        return None
    latest: dict[str, object] | None = None
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict) and str(payload.get("paper_id")) == "TOTAL":
                    latest = payload
    except Exception:
        return None
    return latest

def _safe_float(value: object) -> float:
    """human readable hint: normalize numeric-like values for robust summary math."""

    try:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return 0.0
            return float(text)
        return float(str(value))
    except Exception:
        return 0.0

def _summarize_codecarbon_csv(path: Path | None) -> dict[str, float]:
    """human readable hint: sum key CodeCarbon numeric columns across all rows."""

    totals = {
        "row_count": 0.0,
        "duration": 0.0,
        "emissions": 0.0,
        "energy_consumed": 0.0,
        "cpu_energy": 0.0,
        "gpu_energy": 0.0,
        "ram_energy": 0.0,
    }
    if not path or not path.exists():
        return totals
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not row:
                    continue
                totals["row_count"] += 1.0
                totals["duration"] += _safe_float(row.get("duration"))
                totals["emissions"] += _safe_float(row.get("emissions"))
                totals["energy_consumed"] += _safe_float(row.get("energy_consumed"))
                totals["cpu_energy"] += _safe_float(row.get("cpu_energy"))
                totals["gpu_energy"] += _safe_float(row.get("gpu_energy"))
                totals["ram_energy"] += _safe_float(row.get("ram_energy"))
    except Exception:
        return totals
    return totals

def _write_combined_qc_remaining_totals(stage: str) -> None:
    """human readable hint: write explicit qc+remaining total files for resource usage and CodeCarbon."""

    stage_root = _stage_root(stage)
    stage_root.mkdir(parents=True, exist_ok=True)

    qc_base = _latest_base_outputs(stage, "qc_sample")
    rem_base = _latest_base_outputs(stage, "remaining_sample")

    qc_resource = qc_base.get("resource")
    rem_resource = rem_base.get("resource")
    qc_emissions = qc_base.get("emissions")
    rem_emissions = rem_base.get("emissions")

    if not qc_resource or not rem_resource:
        print("[summary] Skipping qc+remaining total files: missing qc or remaining resource_usage log.")
        return

    qc_total = _latest_total_resource_entry(qc_resource)
    rem_total = _latest_total_resource_entry(rem_resource)
    if not qc_total or not rem_total:
        print("[summary] Skipping qc+remaining resource total file: TOTAL row not found in one of the logs.")
        return

    qc_run_id = str(qc_total.get("run_id") or "")
    rem_run_id = str(rem_total.get("run_id") or "")

    timestamp = datetime.now().strftime("%Y%m%d_%H-%M-%S")

    numeric_fields = [
        "tokens_total",
        "prompt_tokens",
        "response_tokens",
        "embedding_tokens",
        "pdf_text_tokens",
        "pdf_visual_tokens",
        "total_runtime_seconds",
        "paper_count",
        "paper_seconds_total",
        "human_minutes_estimate",
        "time_saved_minutes",
        "codecarbon_emissions_kg",
        "codecarbon_energy_kwh",
    ]
    combined_numeric: dict[str, float] = {}
    for field in numeric_fields:
        combined_numeric[field] = _safe_float(qc_total.get(field)) + _safe_float(rem_total.get(field))

    paper_count = int(combined_numeric.get("paper_count", 0.0))
    total_runtime_seconds = combined_numeric.get("total_runtime_seconds", 0.0)
    paper_seconds_total = combined_numeric.get("paper_seconds_total", 0.0)
    human_minutes_estimate = combined_numeric.get("human_minutes_estimate", 0.0)

    total_runtime_avg_seconds_per_paper = (total_runtime_seconds / paper_count) if paper_count else 0.0
    llm_avg_seconds_per_paper = (paper_seconds_total / paper_count) if paper_count else 0.0
    human_rate_min_per_paper = (human_minutes_estimate / paper_count) if paper_count else None
    time_saved_percent = None
    if human_minutes_estimate > 0:
        time_saved_percent = 1.0 - ((total_runtime_seconds / 60.0) / human_minutes_estimate)

    combined_resource_payload: dict[str, object] = {
        "paper_id": "TOTAL_QC_PLUS_REMAINING",
        "stage": stage,
        "run_label": "qc_plus_remaining",
        "run_id": f"{stage}_qc_plus_remaining_{timestamp}",
        "tokens_total": int(combined_numeric.get("tokens_total", 0.0)),
        "prompt_tokens": int(combined_numeric.get("prompt_tokens", 0.0)),
        "response_tokens": int(combined_numeric.get("response_tokens", 0.0)),
        "embedding_tokens": int(combined_numeric.get("embedding_tokens", 0.0)),
        "pdf_text_tokens": int(combined_numeric.get("pdf_text_tokens", 0.0)),
        "pdf_visual_tokens": int(combined_numeric.get("pdf_visual_tokens", 0.0)),
        "codecarbon_emissions_kg": combined_numeric.get("codecarbon_emissions_kg", 0.0),
        "codecarbon_energy_kwh": combined_numeric.get("codecarbon_energy_kwh", 0.0),
        "total_runtime_seconds": total_runtime_seconds,
        "total_runtime_avg_seconds_per_paper": total_runtime_avg_seconds_per_paper,
        "paper_count": paper_count,
        "paper_seconds_total": paper_seconds_total,
        "llm_decision_avg_seconds_per_paper": llm_avg_seconds_per_paper,
        "human_rate_min_per_paper": human_rate_min_per_paper,
        "human_minutes_estimate": human_minutes_estimate,
        "time_saved_minutes": combined_numeric.get("time_saved_minutes", 0.0),
        "time_saved_percent": time_saved_percent,
        "source_resource_logs": {
            "qc_sample": str(qc_resource),
            "remaining_sample": str(rem_resource),
        },
        "source_run_ids": {
            "qc_sample": qc_run_id,
            "remaining_sample": rem_run_id,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    combined_resource_path = stage_root / f"{stage}_qc_plus_remaining_total_resource_usage_{timestamp}.log"
    try:
        with combined_resource_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(combined_resource_payload, ensure_ascii=False) + "\n")
        print(f"[summary] Combined resource totals written: {combined_resource_path}")
    except Exception as exc:
        print(f"[summary] Could not write combined resource totals: {exc}")

    if not qc_emissions or not rem_emissions:
        print("[summary] Skipping qc+remaining CodeCarbon total file: missing qc or remaining emissions CSV.")
        return

    qc_em_totals = _summarize_codecarbon_csv(qc_emissions)
    rem_em_totals = _summarize_codecarbon_csv(rem_emissions)
    total_emissions_kg = qc_em_totals["emissions"] + rem_em_totals["emissions"]
    total_energy_kwh = qc_em_totals["energy_consumed"] + rem_em_totals["energy_consumed"]
    total_duration_s = qc_em_totals["duration"] + rem_em_totals["duration"]
    total_intensity_g_per_kwh = (total_emissions_kg * 1000.0 / total_energy_kwh) if total_energy_kwh > 0 else None

    combined_emissions_path = stage_root / f"{stage}_qc_plus_remaining_total_codecarbon_emissions_{timestamp}.csv"
    try:
        with combined_emissions_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "stage",
                    "run_scope",
                    "qc_run_id",
                    "remaining_run_id",
                    "qc_emissions_path",
                    "remaining_emissions_path",
                    "qc_rows",
                    "remaining_rows",
                    "total_rows",
                    "qc_duration_seconds",
                    "remaining_duration_seconds",
                    "total_duration_seconds",
                    "qc_emissions_kg",
                    "remaining_emissions_kg",
                    "total_emissions_kg",
                    "qc_energy_kwh",
                    "remaining_energy_kwh",
                    "total_energy_kwh",
                    "total_carbon_intensity_g_per_kwh",
                    "timestamp",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "stage": stage,
                    "run_scope": "qc_plus_remaining",
                    "qc_run_id": qc_run_id,
                    "remaining_run_id": rem_run_id,
                    "qc_emissions_path": str(qc_emissions),
                    "remaining_emissions_path": str(rem_emissions),
                    "qc_rows": int(qc_em_totals["row_count"]),
                    "remaining_rows": int(rem_em_totals["row_count"]),
                    "total_rows": int(qc_em_totals["row_count"] + rem_em_totals["row_count"]),
                    "qc_duration_seconds": qc_em_totals["duration"],
                    "remaining_duration_seconds": rem_em_totals["duration"],
                    "total_duration_seconds": total_duration_s,
                    "qc_emissions_kg": qc_em_totals["emissions"],
                    "remaining_emissions_kg": rem_em_totals["emissions"],
                    "total_emissions_kg": total_emissions_kg,
                    "qc_energy_kwh": qc_em_totals["energy_consumed"],
                    "remaining_energy_kwh": rem_em_totals["energy_consumed"],
                    "total_energy_kwh": total_energy_kwh,
                    "total_carbon_intensity_g_per_kwh": total_intensity_g_per_kwh,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
        print(f"[summary] Combined CodeCarbon totals written: {combined_emissions_path}")
    except Exception as exc:
        print(f"[summary] Could not write combined CodeCarbon totals: {exc}")

def _minute_stamp_from_remaining_resource_name(stage: str, path: Path) -> str | None:
    """human readable hint: extract YYYYMMDD_HH-MM stamp from remaining resource filename."""

    prefix = f"{stage}_remaining_sample_main_resource_usage_"
    name = path.name
    if not name.startswith(prefix):
        return None
    tail = name[len(prefix) :]
    parts = tail.split("_")
    if len(parts) < 2:
        return None
    date_part = parts[0]
    time_part = parts[1]
    if len(date_part) != 8 or len(time_part) < 5:
        return None
    return f"{date_part}_{time_part[:5]}"

def _minute_stamp_from_remaining_emissions_name(stage: str, path: Path) -> str | None:
    """human readable hint: extract YYYYMMDD_HH-MM stamp from remaining emissions filename."""

    prefix = f"{stage}_remaining_sample_codecarbon_emissions_"
    name = path.name
    if not name.startswith(prefix):
        return None
    tail = name[len(prefix) :]
    if len(tail) < 14:
        return None
    stamp = tail[:14]
    if len(stamp.split("_")) != 2:
        return None
    return stamp

def _cleanup_stale_remaining_tracking_files(stage: str) -> None:
    """human readable hint: delete stale zero-token remaining_sample tracking artifacts from earlier runs."""

    stage_root = _stage_root(stage)
    if not stage_root.exists():
        return

    resource_logs = sorted(stage_root.glob(f"{stage}_remaining_sample_main_resource_usage_*.log"))
    if not resource_logs:
        return

    stale_logs: list[Path] = []
    stale_minutes: set[str] = set()
    nonstale_minutes: set[str] = set()

    for log_path in resource_logs:
        total = _latest_total_resource_entry(log_path)
        if not total:
            continue
        tokens_total = _safe_float(total.get("tokens_total"))
        paper_count = _safe_float(total.get("paper_count"))
        minute_stamp = _minute_stamp_from_remaining_resource_name(stage, log_path)
        if tokens_total <= 0.0 and paper_count <= 0.0:
            stale_logs.append(log_path)
            if minute_stamp:
                stale_minutes.add(minute_stamp)
        elif minute_stamp:
            nonstale_minutes.add(minute_stamp)

    removed_logs = 0
    for log_path in stale_logs:
        try:
            log_path.unlink(missing_ok=True)
            removed_logs += 1
        except Exception:
            continue

    removed_emissions = 0
    if stale_minutes:
        emissions_files = sorted(stage_root.glob(f"{stage}_remaining_sample_codecarbon_emissions_*.csv"))
        for emissions_path in emissions_files:
            minute_stamp = _minute_stamp_from_remaining_emissions_name(stage, emissions_path)
            if not minute_stamp:
                continue
            # Keep emissions files if a non-stale remaining run exists for the same minute stamp.
            if minute_stamp in stale_minutes and minute_stamp not in nonstale_minutes:
                try:
                    emissions_path.unlink(missing_ok=True)
                    removed_emissions += 1
                except Exception:
                    continue

    if removed_logs or removed_emissions:
        print(
            f"[cleanup] Removed stale remaining_sample tracking files: "
            f"resource_logs={removed_logs}, emissions_csv={removed_emissions}."
        )

def _auto_generate_qc_mismatch_csv(stage: str, artifact: dict[str, object] | None) -> None:
    """human readable hint: automatically build an explicit mismatch CSV after QC validation completes."""

    stage_root = _stage_root(stage)
    alignments = sorted(
        stage_root.glob(f"{stage}_qc_sample_validation_alignment*.csv"),
        key=lambda path: path.stat().st_mtime,
    )
    if not alignments:
        print("[qc] Validation alignment CSV was not found; mismatch CSV was not generated.")
        return

    alignment_path = alignments[-1]
    eligibility_path: Path | None = None

    if artifact and str(artifact.get("run_label") or "") == "qc_sample":
        candidate = artifact.get("eligibility_path")
        if isinstance(candidate, (str, os.PathLike)):
            maybe_path = Path(candidate)
            if maybe_path.exists():
                eligibility_path = maybe_path

    if eligibility_path is None:
        fallback = _latest_base_outputs(stage, "qc_sample").get("eligibility")
        if isinstance(fallback, Path) and fallback.exists():
            eligibility_path = fallback

    from pipeline.additions.qc_mismatch_report import build_mismatch_sheet

    timestamp = datetime.now().strftime("%Y%m%d_%H-%M")
    output_path = stage_root / f"{stage}_qc_sample_validation_mismatch_{timestamp}.csv"
    mismatch_count, out_path = build_mismatch_sheet(
        alignment_path,
        output_path,
        eligibility_path=eligibility_path,
    )
    print(f"[qc] Explicit mismatch CSV written: {out_path} (rows={mismatch_count}).")


def _qc_screened_already(stage: str, run_label: str = "qc_sample") -> bool:
    """Detect whether a QC sample for this stage was already screened."""

    scoped = _latest_base_outputs(stage, run_label).get("eligibility")
    if scoped and scoped.exists():
        try:
            return scoped.stat().st_size > 0
        except Exception:
            return False

    if run_label != "qc_sample":
        return False

    stage_root = _stage_root(stage)
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

def _merge_emissions_with_run_column(
    stage: str,
    run_label: str,
    attempt_index: int,
    run_id: str | None,
) -> dict[str, object] | None:
    """human readable hint: delegate CodeCarbon merge to the dedicated CSV-safe emissions helper module."""

    stage_root = _stage_root(stage)
    return _merge_emissions_with_run_column_csvsafe(
        stage_root=stage_root,
        stage=stage,
        run_label=run_label,
        attempt_index=attempt_index,
        run_id=run_id,
    )

def _post_run_updates(stage: str, artifact: dict | None, attempt_index: int) -> dict[str, object] | None:
    """human readable hint: after any run, merge emissions and refresh eligibility index."""

    if not artifact or not isinstance(artifact, dict):
        return None
    if not artifact.get("success", True):
        return None
    run_label = artifact.get("run_label") or "remaining_sample"
    run_id = str(artifact.get("run_id") or "").strip() or None
    emissions_info = _merge_emissions_with_run_column(stage, run_label, attempt_index, run_id)
    _update_index_from_artifact(stage, artifact, attempt_index)
    if str(run_label) == "remaining_sample":
        _ensure_qc_rows_in_index(stage)
    # human readable hint: after a successful remaining run, write explicit qc+remaining total summaries.
    if str(run_label) == "remaining_sample" and int(attempt_index or 0) == 0:
        _write_combined_qc_remaining_totals(stage)
    return emissions_info
