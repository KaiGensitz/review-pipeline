"""Startup and input-readiness checks for the interactive workflow."""

from __future__ import annotations

import os
from pathlib import Path

import nltk

from config.user_orchestrator import PATH_SETTINGS


def active_prompt_and_kb(stage: str) -> tuple[str, str]:
    """human readable hint: resolve the active prompt and stage KB paths shown before a run starts."""

    prompt_path = PATH_SETTINGS.get("prompt_file") or "<unconfigured>"
    kb_path = PATH_SETTINGS.get("knowledge_base_file")

    kb_by_stage = PATH_SETTINGS.get("knowledge_base_files")
    if isinstance(kb_by_stage, dict):
        stage_kb = kb_by_stage.get(stage)
        if stage_kb:
            kb_path = stage_kb

    if not kb_path:
        kb_path = "<unconfigured>"

    return str(prompt_path), str(kb_path)


def ensure_csv_inputs(csv_dir: Path) -> bool:
    """human readable hint: confirm that the input folder exists and contains exported CSV files."""

    if not csv_dir.exists():
        print(f"[setup] Create the folder at {csv_dir} and drop your exported CSV files there.")
        return False
    csvs = sorted(csv_dir.glob("*.csv"))
    if not csvs:
        print(f"[setup] No CSV files found in {csv_dir}. Place your exported CSV files there and rerun.")
        return False
    return True


def require_pattern(csv_dir: Path, pattern: str, description: str, stage: str | None = None) -> list[Path]:
    """human readable hint: find the newest required stage CSV and warn if naming looks unusual."""

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


def missing_pdf_folders(base_dir: Path) -> list[str]:
    """human readable hint: list per-paper folders that do not yet contain a PDF."""

    if not base_dir.exists():
        return []
    missing: list[str] = []
    for folder in sorted(base_dir.iterdir()):
        if folder.is_dir() and not any(folder.glob("*.pdf")):
            missing.append(folder.name)
    return missing


def enforce_no_runtime_model_downloads() -> None:
    """human readable hint: force offline model behavior unless the operator explicitly opts into downloads."""

    allow_runtime_downloads = str(os.getenv("ALLOW_RUNTIME_MODEL_DOWNLOADS", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if allow_runtime_downloads:
        return

    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"


def ensure_nltk_tokenizers() -> bool:
    """human readable hint: verify sentence tokenizer assets are preloaded before screening starts."""

    required_resources = (
        ("tokenizers/punkt", "punkt"),
        ("tokenizers/punkt_tab", "punkt_tab"),
    )
    missing_packages: list[str] = []

    for resource_path, package_name in required_resources:
        try:
            nltk.data.find(resource_path)
        except LookupError:
            missing_packages.append(package_name)

    if not missing_packages:
        return True

    missing_text = ", ".join(missing_packages)
    print(f"[setup] Missing NLTK tokenizer data: {missing_text}")
    print("[setup] Runtime downloads are disabled during screening runs.")
    print(
        "[next] Preload runtime assets once, then rerun: "
        ".venv\\Scripts\\python -m pipeline.additions.preload_runtime_assets"
    )
    return False
