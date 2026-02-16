import sys
import json
from pathlib import Path
from datetime import datetime

# Keep imports simple: this module just wires defaults and exposes run_pipeline for main.py.
# Note: this file mainly sets default output names and wires the stage settings.

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.core.pipeline import PaperScreeningPipeline
from pipeline.selection.selector import load_labeled_examples
from config.user_orchestrator import (
    PATH_SETTINGS,
    SCREENING_DEFAULTS,
    CURRENT_STAGE,
    STAGE_RULES,
    LLM_MODEL,
    require_setting,
)


DEFAULT_STAGE_ROOT = Path(PATH_SETTINGS.get("output_root", REPO_ROOT / "output"))

STAGE_KB_DEFAULTS = {
    "title_abstract": REPO_ROOT / "knowledge-base" / "title_abstract_pos-neg_examples.csv",
    "full_text": REPO_ROOT / "knowledge-base" / "full_text_pos-neg_examples.csv",
    "data_extraction": REPO_ROOT / "knowledge-base" / "data_extraction_pos-neg_examples.csv",
}

def _timestamp_label() -> str:
    """Create a timestamp string for output filenames.

    Returns:
        Timestamp string formatted as YYYYMMDD_HH-MM.

    Note: timestamps prevent overwriting prior runs.
    """
    return datetime.now().strftime("%Y%m%d_%H-%M")


def _stage_root(stage: str) -> Path:
    """Return the output folder for a given stage.

    Args:
        stage: Current stage name (title_abstract/full_text/data_extraction).

    Returns:
        Path to output/<stage>/.

    Note: each stage writes into output/<stage>/.
    """
    return DEFAULT_STAGE_ROOT / stage


def _existing_qc_files(stage_root: Path, stage_prefix: str) -> tuple[Path | None, Path | None]:
    """Reuse the latest QC sample if present so the list stays stable across runs.

    Args:
        stage_root: Output directory for the stage.
        stage_prefix: Prefix for stage files (e.g., "title_abstract_").

    Returns:
        Tuple of (qc_sample_csv_path, qc_sample_readable_path), or (None, None).

    Note: QC sample reuse ensures the same list is validated.
    """
    matches = sorted(stage_root.glob(f"{stage_prefix}qc_sample_batch_*.csv"))
    if not matches:
        return None, None
    chosen = max(matches, key=lambda p: p.stat().st_mtime)
    date_tag = chosen.stem.replace(f"{stage_prefix}qc_sample_batch_", "")
    readable = stage_root / f"{stage_prefix}qc_sample_batch_readable_{date_tag}.txt"
    return chosen, readable


def _stage_prefixed(path: Path, target_stage: str) -> Path:
    """Ensure a file path is placed under output/<stage>/ for consistency.

    Args:
        path: Desired file path (possibly outside output/<stage>/).
        target_stage: Stage name for output placement.

    Returns:
        Path under output/<stage>/ with the same filename.

    Note: keeps all outputs stage-scoped.
    """
    # If a path is already under output/<stage>/ keep it; otherwise, place it there.
    if path.parent.name == target_stage:
        return path
    return DEFAULT_STAGE_ROOT / target_stage / path.name


def _extract_text(row: dict, keys: list[str]) -> str:
    """Read a text field from a CSV row using a list of possible column names.

    Args:
        row: A CSV row as a dict.
        keys: Candidate column names to search for.

    Returns:
        The first non-empty matching value, or empty string.

    Note: handles minor column-name variations in exports.
    """
    for key in keys:
        if key in row and row[key]:
            return str(row[key]).strip()
        lower = key.lower()
        for rk, rv in row.items():
            if rv and rk.lower() == lower:
                return str(rv).strip()
    return ""


def _load_negative_examples_from_csvs(csv_dir: Path, patterns: list[str]) -> list[dict]:
    """Load extra negative examples from CSVs to enrich the knowledge base.

    Args:
        csv_dir: Directory containing Covidence exports.
        patterns: List of glob patterns for negative-example CSVs.

    Returns:
        List of NEG example dicts with label/text.

    Note: these negatives improve evidence filtering precision.
    """
    import csv

    negatives: list[dict] = []
    csv_files: list[Path] = []
    for pattern in patterns:
        csv_files.extend(sorted(csv_dir.glob(pattern)))

    if not csv_files:
        print(
            f"[warning] No negative-example CSVs found for patterns {patterns} in {csv_dir}. "
            "Proceeding without extra NEG knowledge base examples."
        )

    for csv_file in csv_files:
        with open(csv_file, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                abstract = _extract_text(row, ["Abstract", "abstract"])
                title = _extract_text(row, ["Title", "title"])
                text = abstract or title
                if not text:
                    continue
                negatives.append({"label": "NEG", "text": text})

    return negatives


def _safe_int(val, default=None):
    if val is None:
        return default
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        try:
            return int(val)
        except Exception:
            pass
    raise ValueError(f"Cannot convert {val!r} to int")


def _safe_float(val, default=None):
    if val is None:
        return default
    if isinstance(val, float):
        return val
    if isinstance(val, int):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except Exception:
            pass
    raise ValueError(f"Cannot convert {val!r} to float")


def _safe_bool(val, default=None):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes", "on")
    raise ValueError(f"Cannot convert {val!r} to bool")


def _append_qc_records_to_remaining(stage_root: Path, stage_prefix: str, remaining_path: Path) -> None:
    """Append QC sample eligibility records to the remaining-sample output."""
    qc_files = sorted(stage_root.glob(f"{stage_prefix}eligibility_qc_sample_*.jsonl"))
    if not qc_files:
        return
    qc_path = max(qc_files, key=lambda p: p.stat().st_mtime)
    lines_to_append: list[str] = []
    try:
        with open(qc_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict) and obj.get("meta") in {"eligibility_records", "summary"}:
                    continue
                lines_to_append.append(json.dumps(obj))
    except Exception:
        return

    if not lines_to_append:
        return

    try:
        with open(remaining_path, "a", encoding="utf-8") as out:
            for line in lines_to_append:
                out.write(line + "\n")
    except Exception:
        return


def run_pipeline(
    stage: str = CURRENT_STAGE,
    split_only: bool = False,
    csv_dir: str | None = None,
    kb_file: str | None = None,
    eligibility_output: Path | None = None,
    chunks_output: Path | None = None,
    text_output: Path | None = None,
    error_log: Path | None = None,
    resource_log: Path | None = None,
    top_k: int | None = None,
    score_threshold: float | None = None,
    sample_size: int | None = None,
    sample_seed: int | None = None,
    batch_size: int | None = None,
    sustainability_tracking: bool | None = None,
    pdf_root: str | None = None,
    quiet: bool = False,
    confirm_sampling: bool = False,
    sample_rate: float = 0.10,
    qc_only: bool = False,
    qc_enabled: bool = True,
    force_new_qc: bool = False,
    enable_time_savings: bool | None = None,
    run_label_override: str | None = None,
) -> object:
    """Run one pipeline stage with stage-specific defaults and outputs.

    Args:
        stage: Stage name (title_abstract/full_text/data_extraction).
        split_only: If True, only prepare folders and exit.
        csv_dir: Override input/ folder path.
        kb_file: Override KB file path for this run.
        eligibility_output: Override eligibility JSONL output path.
        chunks_output: Override selected-chunks JSONL output path.
        text_output: Override readable summary output path.
        error_log: Override error log path.
        top_k: Max number of evidence chunks per paper.
        score_threshold: Minimum relevance score threshold.
        sample_size: Optional fixed number of papers to sample.
        sample_seed: Random seed for sampling.
        batch_size: Embedding batch size.
        sustainability_tracking: If True, write resource logs.
        pdf_root: Optional PDF root path override.
        quiet: If True, suppress most console output.
        confirm_sampling: If True, skip QC prompt (already confirmed).
        sample_rate: QC sample fraction (0–1).
        qc_only: If True, screen QC sample only.
        qc_enabled: If False, skip QC sampling entirely.
        force_new_qc: If True, generate a new QC sample even if one exists.

    Returns:
        True if screening executed; False if the run exited early.

    Note: this is the core launcher used by main.py.
    """
    timestamp_label = _timestamp_label()
    date_label = datetime.now().strftime("%Y%m%d_%H-%M")
    stage_prefix = f"{stage}_"
    stage_root = _stage_root(stage)

    if stage not in STAGE_KB_DEFAULTS:
        raise ValueError(f"Unknown stage '{stage}'. Expected one of {sorted(STAGE_KB_DEFAULTS)}.")
    stage_kb_default = STAGE_KB_DEFAULTS[stage]
    if kb_file is None:
        kb_file = str(stage_kb_default)
        if not Path(kb_file).exists():
            raise FileNotFoundError(
                f"Missing stage-specific knowledge base for '{stage}'. Expected file at {stage_kb_default}."
            )
    else:
        if not Path(kb_file).exists():
            raise FileNotFoundError(f"Missing knowledge base override at {kb_file} for stage '{stage}'.")

    csv_dir = csv_dir or PATH_SETTINGS.get("csv_dir")
    csv_dir_path = Path(csv_dir) if csv_dir else REPO_ROOT / "input"
    run_label = run_label_override or ("qc_sample" if qc_only else "remaining_sample")
    sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
    base_prefix = f"{stage_prefix}{sample_tag}_sample_main"
    eligibility_output = eligibility_output or stage_root / f"{base_prefix}_eligibility_{timestamp_label}.jsonl"
    chunks_output = chunks_output or stage_root / f"{base_prefix}_selected_chunks_{timestamp_label}.jsonl"
    text_output = text_output or stage_root / f"{base_prefix}_screening_results_readable_{timestamp_label}.txt"
    error_log = error_log or stage_root / f"{base_prefix}_error_log_{timestamp_label}.txt"
    resource_log_path = _stage_prefixed(
        Path(resource_log)
        if resource_log
        else Path(stage_root / f"{base_prefix}_resource_usage_{timestamp_label}.log"),
        stage,
    )
    existing_qc_path, existing_qc_readable = _existing_qc_files(stage_root, stage_prefix) if not force_new_qc else (None, None)
    qc_suffix = date_label if not existing_qc_path else existing_qc_path.stem.replace(f"{stage_prefix}qc_sample_batch_", "")
    qc_sample_path = existing_qc_path or _stage_prefixed(
        stage_root / f"{stage_prefix}qc_sample_batch_{qc_suffix}.csv", stage
    )
    qc_sample_readable_path = existing_qc_readable or _stage_prefixed(
        stage_root / f"{stage_prefix}qc_sample_batch_readable_{qc_suffix}.txt", stage
    )
    overflow_log_path = _stage_prefixed(
        Path(PATH_SETTINGS.get("overflow_log", stage_root / f"{stage_prefix}overflow_log_{timestamp_label}.txt")), stage
    )

    top_k = top_k if top_k is not None else _safe_int(require_setting(SCREENING_DEFAULTS, "top_k", "SCREENING_DEFAULTS"))
    score_threshold = score_threshold if score_threshold is not None else _safe_float(require_setting(SCREENING_DEFAULTS, "score_threshold", "SCREENING_DEFAULTS"))
    sample_size = sample_size if sample_size is not None else _safe_int(require_setting(SCREENING_DEFAULTS, "sample_size", "SCREENING_DEFAULTS"))
    sample_seed = sample_seed if sample_seed is not None else _safe_int(require_setting(SCREENING_DEFAULTS, "sample_seed", "SCREENING_DEFAULTS"))
    batch_size = batch_size if batch_size is not None else _safe_int(require_setting(SCREENING_DEFAULTS, "batch_size", "SCREENING_DEFAULTS"))
    sustainability_tracking = (
        _safe_bool(require_setting(SCREENING_DEFAULTS, "sustainability_tracking", "SCREENING_DEFAULTS"))
        if sustainability_tracking is None
        else sustainability_tracking
    )
    default_time_savings = _safe_bool(SCREENING_DEFAULTS.get("enable_time_savings"), False)
    enable_time_savings = bool(enable_time_savings) if enable_time_savings is not None else bool(default_time_savings)
    codecarbon_enabled = True
    pdf_root = pdf_root or PATH_SETTINGS.get("pdf_root")

    examples = load_labeled_examples(kb_file)
    neg_patterns = STAGE_RULES.get(stage, {}).get("neg_patterns", [])
    if neg_patterns:
        neg_examples = _load_negative_examples_from_csvs(csv_dir_path, neg_patterns)
        # Ensure all negatives are dicts with 'label' and 'text' keys (LabeledExample structure)
        for neg in neg_examples:
            if "label" in neg and "text" in neg:
                examples.append({"label": str(neg["label"]), "text": str(neg["text"])})

    # Ensure csv_dir is always a str (never None)
    if csv_dir is None:
        raise ValueError("csv_dir must not be None")

    # Cast examples to list[dict] for type safety
    from typing import cast

    # Load QC sample IDs even when qc_enabled is False so we can skip them later.
    qc_sample_ids: set[str] = set()
    if confirm_sampling and qc_sample_path and Path(qc_sample_path).exists():
        import csv

        try:
            with open(qc_sample_path, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    pid = row.get("paper_id") or row.get("Covidence #") or row.get("Covidence#")
                    if pid:
                        qc_sample_ids.add(str(pid))
        except Exception:
            qc_sample_ids = set()

    pipeline = PaperScreeningPipeline(
        csv_dir=csv_dir,
        knowledge_base_path=kb_file,
        eligibility_output_path=str(eligibility_output),
        chunks_output_path=str(chunks_output),
        text_output_path=str(text_output),
        run_label=run_label,
        error_log_path=str(error_log),
        resource_log_path=str(resource_log_path),
        codecarbon_enabled=codecarbon_enabled,
        qc_sample_path=str(qc_sample_path),
        qc_sample_readable_path=str(qc_sample_readable_path),
        confirm_sampling=confirm_sampling,
        sample_rate=sample_rate,
        qc_only=qc_only,
        qc_enabled=qc_enabled,
        force_new_qc=force_new_qc,
        overflow_log_path=str(overflow_log_path),
        top_k=top_k,
        score_threshold=score_threshold,
        sample_size=sample_size,
        sample_seed=sample_seed,
        batch_size=batch_size,
        sustainability_tracking=sustainability_tracking,
        enable_time_savings=enable_time_savings,
        stage=stage,
        pdf_root=pdf_root,
        split_only=split_only,
        quiet=quiet,
        summary_to_console=False,
        examples=cast(list[dict], examples),
    )
    stage_csvs = [str(p) for p in pipeline._stage_csv_files()]
    if qc_sample_ids:
        pipeline._qc_sample_ids = qc_sample_ids  # reuse prior QC sample IDs to skip in remaining run
    ran = pipeline.run()

    # Split-only: folder prep only; do not print screening output statuses
    if split_only:
        return ran

    if not ran:
        return False

    elig_path = eligibility_output.resolve()
    if qc_sample_ids and confirm_sampling and not qc_only:
        # human readable hint: QC records are appended once into the remaining file to avoid re-screening the sample.
        _append_qc_records_to_remaining(stage_root, stage_prefix, elig_path)
    chunks_path = chunks_output.resolve()
    text_path = text_output.resolve()
    resource_log_resolved = resource_log_path.resolve()
    error_log_path = error_log.resolve()

    if stage in {"title_abstract", "full_text"}:
        print("Eligibility results:", "successfully done" if elig_path.exists() else "see error log", elig_path)
        print("Readable summary:", "successfully done" if text_path.exists() else "see error log", text_path)
    if stage == "title_abstract":
        print("Chunk records:", "successfully done" if chunks_path.exists() else "see error log", chunks_path)
    print("Resource summary:", "successfully done" if resource_log_resolved.exists() else "see error log", resource_log_resolved)

    error_ids: set[str] = set()
    if error_log_path.exists() and error_log_path.stat().st_size > 0:
        print("Errors occurred; see log:", error_log_path)
        try:
            with open(error_log_path, "r", encoding="utf-8") as handle:
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
            error_ids = set()
    else:
        print("No errors recorded")

    # Derive split eligibility paths for downstream merging
    split_paths: dict[str, str] = {}
    if stage in {"title_abstract", "full_text"}:
        base = str(elig_path.name)
        if stage == "title_abstract":
            split_paths["select"] = str(eligibility_output.with_name(base.replace("eligibility_", "eligibility_select_")))
            split_paths["irrelevant"] = str(eligibility_output.with_name(base.replace("eligibility_", "eligibility_irrelevant_")))
        else:
            split_paths["included"] = str(eligibility_output.with_name(base.replace("eligibility_", "eligibility_included_")))
            split_paths["excluded"] = str(eligibility_output.with_name(base.replace("eligibility_", "eligibility_excluded_")))

    artifact = {
        "success": bool(ran),
        "error_log_path": str(error_log_path),
        "eligibility_path": str(elig_path),
        "chunks_path": str(chunks_path),
        "text_path": str(text_path),
        "resource_log_path": str(resource_log_resolved),
        "run_label": run_label,
        "stage": stage,
        "qc_sample_path": str(qc_sample_path) if qc_sample_path else None,
        "stage_csv_files": stage_csvs,
        "error_ids": sorted(error_ids),
        "split_paths": split_paths,
    }

    return artifact


if __name__ == "__main__":
    run_pipeline()
