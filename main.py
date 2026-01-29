import sys
import subprocess
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


def _require_pattern(csv_dir: Path, pattern: str, description: str) -> list[Path]:
    """Ensure required CSVs exist for the current stage.

    Args:
        csv_dir: Path to the input/ folder.
        pattern: Glob pattern for required CSV files.
        description: Human-readable description of the required export.

    Returns:
        A list of matching CSV paths (empty if none found).

    Note: each stage needs a specific Covidence export.
    """
    matches = sorted(csv_dir.glob(pattern))
    if matches:
        return matches
    print(f"[setup] Missing {description}. Expected a file matching '{pattern}' in {csv_dir}.")
    return []


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
        return False
    while True:
        resp = input(message).strip().lower()
        if resp in {"y", "yes"}:
            return True
        if resp in {"n", "no"}:
            return False
        print("Please answer 'y' or 'n'.")


def _run_validation() -> bool:
    """Run validation interactively and return True if it executed.

    Returns:
        True if validation ran successfully; False if skipped or failed.

    Note: compares AI decisions to human QC exports.
    """
    if not _prompt_yes_no("[qc] Run validation now? [y/n]: "):
        return False

    print("[qc] Running validation using auto-detected CSVs in input/. If files are missing, a warning will appear.")
    result = subprocess.run([sys.executable, "-m", "pipeline.additions.stats_engine"], check=False)
    return result.returncode == 0


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
        ran = run_pipeline(
            stage=stage,
            confirm_sampling=False,
            sample_rate=sample_rate,
            qc_only=True,
            qc_enabled=True,
            force_new_qc=force_new_qc,
            quiet=quiet,
        )
        if not ran:
            return False
        force_new_qc = False
        print("[qc] QC-only screening complete.")

        if not _run_validation():
            print("[qc] Validation skipped or failed. Rerun main.py to continue.")
            return False

        if _prompt_yes_no("[qc] Are you satisfied with validation results and do you want to continue with screening of the remaining papers? [y/n]: "):
            return True

        if not _prompt_yes_no("[qc] Start a new QC round with a fresh sample? [y/n]: "):
            return False

        # Start a new QC round without deleting prior QC files.
        force_new_qc = True


def main() -> None:
    """Run the pipeline for the selected stage with safety checks.

    Note: this is the main entry point; it handles QC → validation → full run.
    """
    # Note: QC is always required for screening; this script guides the QC → validation → full run loop.
    stage = CURRENT_STAGE
    csv_dir = Path(PATH_SETTINGS["csv_dir"])
    sample_rate = QC_SAMPLE_RATE

    print(f"[main] Stage: {stage} | Model: {LLM_MODEL}")
    if not LLM_API_KEY:
        print("[warning] LLM_API_KEY is empty. Set it in config/user_orchestrator.py or as an environment variable.")

    if stage not in STAGE_RULES:
        print(f"[error] Unknown CURRENT_STAGE='{stage}'. Choose from {sorted(STAGE_RULES)}.")
        return

    if not _ensure_csv_inputs(csv_dir):
        return

    _ensure_nltk_tokenizers()

    if not sys.stdin.isatty():
        print("[error] QC confirmation requires an interactive terminal. Rerun in an interactive session.")
        return

    rule = STAGE_RULES[stage]
    for pattern in rule["screen_patterns"]:
        if not _require_pattern(csv_dir, pattern, f"{stage} required CSV export"):
            return

    if stage == "title_abstract":
        if QC_ENABLED:
            if _run_qc_loop(stage, sample_rate, quiet=False):
                run_pipeline(stage=stage, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
            return
        run_pipeline(stage=stage, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
        return

    if QC_ENABLED:
        print(f"[main] Preparing per-paper folders for {stage} (no screening in this step)...")
        run_pipeline(stage=stage, split_only=True, quiet=True)

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
            run_pipeline(stage=stage, quiet=False, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
        return

    # QC disabled: single screening pass, no pre-run split_only call
    run_pipeline(stage=stage, quiet=False, confirm_sampling=True, sample_rate=sample_rate, qc_only=False, qc_enabled=False)
    return


if __name__ == "__main__":
    main()
    # Offer to trigger backup after pipeline run
    try:
        resp = input("\nDo you want to back up your changes to GitHub now? (y/n): ").strip().lower()
        if resp == "y":
            import subprocess
            import sys
            subprocess.run([sys.executable, "backup_to_github.py"])
    except Exception:
        print("[warning] Could not trigger backup script. Please run backup_to_github.py manually if needed.")
