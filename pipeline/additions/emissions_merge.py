"""CSV-safe CodeCarbon merge helpers for run and retry labeling.

This module centralizes CodeCarbon row merge behavior so orchestration code in main.py
stays focused on workflow control while all CSV manipulation remains standards-compliant.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def _pattern_for_stage_run(stage: str, run_label: str) -> str:
    """human readable hint: derive the stage/run-specific emissions filename glob."""

    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
    run_key = f"{sample_tag}_sample"
    return f"{stage}_{run_key}_codecarbon_emissions_*.csv"


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]] | None:
    """human readable hint: load rows with csv.DictReader to preserve quoted comma-safe parsing."""

    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or [])
            rows = [dict(row) for row in reader]
    except Exception:
        return None

    if not fieldnames:
        return None
    return fieldnames, rows


def _ensure_run_column(fieldnames: list[str]) -> list[str]:
    """human readable hint: ensure the emissions schema always includes a run column."""

    if "run" in fieldnames:
        return fieldnames
    updated = list(fieldnames)
    if "project_name" in updated:
        project_idx = updated.index("project_name")
        updated.insert(project_idx + 1, "run")
    else:
        updated.append("run")
    return updated


def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> bool:
    """human readable hint: write rows with csv.DictWriter for safe quoting and stable headers."""

    try:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                safe_row = {name: row.get(name, "") for name in fieldnames}
                writer.writerow(safe_row)
    except Exception:
        return False
    return True


def _fill_run_values(
    rows: list[dict[str, str]],
    run_value: str,
    *,
    override_existing: bool,
) -> list[dict[str, str]]:
    """human readable hint: set run labels row-by-row with optional overwrite semantics."""

    updated: list[dict[str, str]] = []
    for row in rows:
        normalized = dict(row)
        current = str(normalized.get("run", "") or "")
        if override_existing or not current:
            normalized["run"] = run_value
        updated.append(normalized)
    return updated


def _label_single_file_retry_rows(rows: list[dict[str, str]], attempt_index: int) -> list[dict[str, str]]:
    """human readable hint: when only one emissions file exists, row 1 is main and later rows become retry_N."""

    retry_tag = f"retry_{attempt_index}"
    labeled: list[dict[str, str]] = []
    for idx, row in enumerate(rows):
        normalized = dict(row)
        current = str(normalized.get("run", "") or "")
        if idx == 0:
            if not current:
                normalized["run"] = "main"
        else:
            if not current or current == "main":
                normalized["run"] = retry_tag
        labeled.append(normalized)
    return labeled


def merge_emissions_with_run_column(
    *,
    stage_root: Path,
    stage: str,
    run_label: str,
    attempt_index: int | None,
) -> dict[str, Any] | None:
    """human readable hint: keep one per-run emissions CSV and append retry rows with explicit run labels."""

    pattern = _pattern_for_stage_run(stage, run_label)
    files = sorted(stage_root.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not files:
        return None

    base_file = files[0]
    latest_file = files[-1]

    # Single-file retry case: CodeCarbon may have reused one file for both main and retry.
    if len(files) == 1 and attempt_index not in {None, 0}:
        loaded = _read_csv_rows(base_file)
        if loaded is None:
            return None
        fieldnames, rows = loaded
        fieldnames = _ensure_run_column(fieldnames)
        if not rows:
            return None

        rewritten = _label_single_file_retry_rows(rows, int(attempt_index))
        if not _write_csv_rows(base_file, fieldnames, rewritten):
            return None

        retry_tag = f"retry_{int(attempt_index)}"
        retry_rows = [idx + 1 for idx, row in enumerate(rewritten) if str(row.get("run", "")) == retry_tag]
        return {"emissions_path": base_file, "emissions_rows": retry_rows}

    base_loaded = _read_csv_rows(base_file)
    if base_loaded is None:
        return None
    base_fieldnames, base_rows = base_loaded
    base_fieldnames = _ensure_run_column(base_fieldnames)
    base_rows = _fill_run_values(base_rows, "main", override_existing=False)
    if not _write_csv_rows(base_file, base_fieldnames, base_rows):
        return None

    if latest_file == base_file:
        return {"emissions_path": base_file, "emissions_rows": []}

    latest_loaded = _read_csv_rows(latest_file)
    if latest_loaded is None:
        return None
    latest_fieldnames, latest_rows = latest_loaded
    latest_fieldnames = _ensure_run_column(latest_fieldnames)

    run_value = "main" if attempt_index in {None, 0} else f"retry_{int(attempt_index)}"
    latest_rows = _fill_run_values(latest_rows, run_value, override_existing=True)

    final_fieldnames = list(base_fieldnames)
    for name in latest_fieldnames:
        if name not in final_fieldnames:
            final_fieldnames.append(name)

    combined_rows = [{name: row.get(name, "") for name in final_fieldnames} for row in base_rows]
    appended_rows_start = len(combined_rows)
    combined_rows.extend({name: row.get(name, "") for name in final_fieldnames} for row in latest_rows)

    if not _write_csv_rows(base_file, final_fieldnames, combined_rows):
        return None

    try:
        latest_file.unlink(missing_ok=True)
    except Exception:
        return None

    appended_row_numbers = [appended_rows_start + idx + 1 for idx, row in enumerate(latest_rows) if str(row.get("run", ""))]
    return {"emissions_path": base_file, "emissions_rows": appended_row_numbers}


__all__ = ["merge_emissions_with_run_column"]
