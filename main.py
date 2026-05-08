import argparse
import os
import sys
import subprocess
from pathlib import Path

from config.user_orchestrator import (
    CURRENT_STAGE,
    DATA_EXTRACTION_SCHEMA_FILE,
    PATH_SETTINGS,
    LLM_API_KEY,
    LLM_MODEL,
    EMBED_MODEL,
    QC_ENABLED,
    QC_SAMPLE_RATE,
    STAGE_RULES,
    CITATION_SEARCHING_SCREENING,
    CITATION_SEARCHING_STAGE_RULES,
)
from pipeline.core.run_screening import run_pipeline
from pipeline.core.citation_io import CitationCsvParser
from pipeline.additions.resource_usage import backfill_time_savings
from pipeline.additions.retry_flow import (
    _archive_retry_csv,
    _collect_missing_is_eligible,
    _error_ids_by_type,
    _first_available_run_label,
    _infer_run_label_from_retry_csv,
    _latest_retry_csv,
    _next_retry_attempt,
    _prepare_isolated_retry_run_dir,
    _record_retry_manifest,
    _require_base_outputs,
    _retry_csv_needed,
    _retry_output_paths,
    _retry_pdf_root,
    _write_retry_csv,
)
from pipeline.additions.run_index import (
    _artifact_from_latest_base_outputs,
    _auto_generate_qc_mismatch_csv,
    _cleanup_stale_remaining_tracking_files,
    _post_run_updates,
    _qc_screened_already,
    _update_index_from_artifact,
)
from pipeline.additions.startup_checks import (
    active_prompt_and_kb as _active_prompt_and_kb,
    enforce_no_runtime_model_downloads as _enforce_no_runtime_model_downloads,
    ensure_csv_inputs as _ensure_csv_inputs,
    ensure_nltk_tokenizers as _ensure_nltk_tokenizers,
    missing_pdf_folders as _missing_pdf_folders,
    require_pattern as _require_pattern,
)

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




def _run_pipeline_guarded(*, mark_failure: bool = True, **kwargs) -> bool:
    """Run the pipeline and store artifacts; mark prompts as not-all-yes on failure."""
    if "enable_time_savings" not in kwargs:
        # Enable time-savings from the start so resource_usage captures human-rate fields before prompts.
        kwargs["enable_time_savings"] = True

    if bool(kwargs.get("split_only")) and "run_label_override" not in kwargs:
        # human readable hint: split-only preflight runs should not create remaining_sample artifacts.
        kwargs["run_label_override"] = "preflight"

    if bool(kwargs.get("split_only")) and "sustainability_tracking" not in kwargs:
        # human readable hint: skip tracking during folder-prep preflight to reduce empty/stale artifacts.
        kwargs["sustainability_tracking"] = False

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














































































def _execute_retry_run(
    *,
    stage: str,
    run_label: str,
    retry_csv: Path,
    attempt_map: dict[str, int],
) -> dict[str, object] | None:
    """human readable hint: run one retry attempt with consistent outputs, manifest writing, and post-run updates."""

    if not retry_csv.exists():
        print(f"[retry] Retry CSV missing: {retry_csv}. Aborting retry step.")
        return None

    base_outputs = _require_base_outputs(stage, run_label)
    if not base_outputs:
        return None

    retry_run_dir = _prepare_isolated_retry_run_dir(stage, retry_csv)
    if not retry_run_dir:
        print("[retry] Could not prepare isolated retry run folder. Aborting retry step.")
        return None

    attempt_for_run = max(attempt_map.values()) if attempt_map else _next_retry_attempt(stage, run_label)
    retry_out = _retry_output_paths(stage, run_label, attempt_for_run)
    print(f"[retry] Created retry CSV at {retry_csv}. Running re-screen (QC disabled for retry)...")

    _run_pipeline_guarded(
        stage=stage,
        csv_dir=str(retry_run_dir),
        pdf_root=_retry_pdf_root(stage),
        qc_enabled=False,
        confirm_sampling=False,
        quiet=False,
        eligibility_output=retry_out.get("eligibility"),
        chunks_output=retry_out.get("chunks"),
        text_output=retry_out.get("text"),
        error_log=retry_out.get("error"),
        resource_log=retry_out.get("resource"),
        run_label_override=run_label,
        mark_failure=False,
    )

    artifact = _last_artifact_dict()
    if not isinstance(artifact, dict) or not artifact.get("success", False):
        print("[retry] Re-screen failed. Check the retry error log for details.")
        return None

    emissions_info = _post_run_updates(stage, artifact, attempt_for_run)
    _record_retry_manifest(artifact, stage, attempt_map, retry_csv, emissions_info)

    manifest_path = Path(PATH_SETTINGS.get("output_root", "output")) / stage / f"{stage}_retry_manifest.jsonl"
    print(f"[retry] Re-screen completed. Outputs kept separate; manifest updated at {manifest_path}.")
    return artifact


def _prompt_retry_if_needed(stage: str, artifact: dict | None, depth: int = 0) -> None:
    """Prompt for re-screening when errors are present for this stage."""
    if depth >= 2:
        print("[retry] Maximum automatic retry depth reached for this run. Stop and inspect the error log before retrying again.")
        return
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

        blocked_ids = _error_ids_by_type(
            error_path,
            {"llm_output_token_limit", "context_overflow", "pdf_missing", "no_chunks"},
        )
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

    target_dir = Path(PATH_SETTINGS["csv_dir"]) / "retry_runs"
    retry_csv = _write_retry_csv(source_csv, target_dir, candidates, stage, run_label)
    if not retry_csv:
        return
    retry_artifact_dict = _execute_retry_run(
        stage=stage,
        run_label=run_label,
        retry_csv=retry_csv,
        attempt_map=attempt_map,
    )
    if not isinstance(retry_artifact_dict, dict):
        return

    # human readable hint: if the retry still has errors, offer another retry prompt instead of stopping silently.
    if retry_artifact_dict.get("error_log_path"):
        _prompt_retry_if_needed(stage, retry_artifact_dict, depth=depth + 1)


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
        print("[input] Please answer 'y' or 'n'.")


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

    artifact_after_validation = _last_artifact_dict()
    _auto_generate_qc_mismatch_csv(CURRENT_STAGE, artifact_after_validation)

    _PROMPT_STATE["validation_ran"] = True
    return True
def _run_qc_loop(
    stage: str,
    sample_rate: float,
    quiet: bool = False,
    input_files: list[str] | None = None,
    qc_run_label: str = "qc_sample",
) -> bool:
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
        if not force_new_qc and _qc_screened_already(stage, qc_run_label):
            print("[qc] Existing QC screening found; skipping re-screen of QC sample.")
            ran = True
            qc_artifact = _artifact_from_latest_base_outputs(stage, qc_run_label)
            if qc_artifact:
                _PROMPT_STATE["last_artifact"] = qc_artifact
                _update_index_from_artifact(stage, qc_artifact, 0)
            else:
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
                input_files=input_files,
                run_label_override=qc_run_label,
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

    def __init__(
        self,
        stage: str | None = None,
        input_files: list[str] | None = None,
        run_scope: str | None = None,
    ) -> None:
        """human readable hint: __init__ keeps the key runtime attributes visible in one place."""

        self.stage = stage or CURRENT_STAGE
        self.csv_dir = Path(PATH_SETTINGS["csv_dir"])
        self.sample_rate = QC_SAMPLE_RATE
        self.citation_searching_mode = bool(CITATION_SEARCHING_SCREENING)
        self.input_files = input_files or []
        self.run_scope = _sanitize_run_scope(run_scope) or (
            "citation_searching" if self.citation_searching_mode else ""
        )
        self.qc_run_label = f"{self.run_scope}_qc_sample" if self.run_scope else "qc_sample"
        self.remaining_run_label = (
            f"{self.run_scope}_remaining_sample" if self.run_scope else "remaining_sample"
        )

    def run(self) -> None:
        """Run the pipeline for the selected stage with safety checks."""

        if not LLM_API_KEY:
            print("[error] LLM_API_KEY is empty. Set it in .env or config/user_orchestrator.py before running.")
            return

        stage = self.stage
        csv_dir = self.csv_dir
        sample_rate = self.sample_rate
        input_files = self.input_files

        print(f"[main] Stage: {stage} | LLM: {LLM_MODEL} | Embedding: {EMBED_MODEL}")
        if self.run_scope:
            print(f"[main] Run scope: {self.run_scope}")
        if self.citation_searching_mode:
            print("[main] Citation-searching mode: ON (QC sampling disabled).")
        if input_files:
            print("[main] Explicit input file(s):")
            for path in input_files:
                print(f"  - {path}")

        if stage not in STAGE_RULES:
            print(f"[error] Unknown CURRENT_STAGE='{stage}'. Choose from {sorted(STAGE_RULES)}.")
            return
        if self.citation_searching_mode and stage not in CITATION_SEARCHING_STAGE_RULES:
            print("[error] CITATION_SEARCHING_SCREENING=True has no file pattern for this stage.")
            return

        if not _ensure_csv_inputs(csv_dir):
            return

        _cleanup_stale_remaining_tracking_files(stage)

        _enforce_no_runtime_model_downloads()

        if not _ensure_nltk_tokenizers():
            return

        if not sys.stdin.isatty():
            print("[error] QC confirmation requires an interactive terminal. Rerun in an interactive session.")
            return

        active_prompt_path, active_kb_path = _active_prompt_and_kb(stage)
        print(f"[config] Active prompt file: {active_prompt_path}")
        print(f"[config] Active knowledge-base file: {active_kb_path}")
        if stage == "data_extraction":
            print(f"[config] Active data-extraction schema file: {DATA_EXTRACTION_SCHEMA_FILE}")

        if not _prompt_yes_no("[qc] Are the prompt, knowledge base, and study tags identical to previous runs? [y/n]: "):
            print("[qc] Please confirm or update prompt, knowledge-base, and study-tag settings before running.")
            print(f"[qc] Prompt file in use: {active_prompt_path}")
            print(f"[qc] Knowledge-base file in use: {active_kb_path}")
            print("[qc] Study-tag settings: STUDY_TAGS_INCLUDE and STUDY_TAGS_IGNORE in config/user_orchestrator.py.")
            return

        if self.citation_searching_mode and not input_files:
            try:
                input_files = _prepare_citation_searching_delta(csv_dir, stage)
            except (FileNotFoundError, ValueError) as exc:
                print(f"[citation][error] {exc}")
                return
            self.input_files = input_files
            if input_files:
                print("[main] Citation-searching novel-record input file(s):")
                for path in input_files:
                    print(f"  - {path}")

        retry_csv = None if (self.run_scope or input_files) else _latest_retry_csv(stage)
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
                        artifact = _execute_retry_run(
                            stage=stage,
                            run_label=run_label,
                            retry_csv=retry_csv,
                            attempt_map=attempt_map,
                        )
                        if not isinstance(artifact, dict):
                            return
                        _prompt_retry_if_needed(stage, artifact)

                        if not _retry_csv_needed(retry_csv, stage):
                            _archive_retry_csv(retry_csv)

        rule = _active_stage_rule(stage, citation_searching=self.citation_searching_mode)
        if input_files:
            missing_inputs: list[str] = []
            for raw_path in input_files:
                path = Path(raw_path)
                exists = path.exists() if path.is_absolute() else (path.exists() or (csv_dir / path).exists())
                if not exists:
                    missing_inputs.append(raw_path)
            if missing_inputs:
                print("[error] Explicit input file(s) not found:")
                for path in missing_inputs:
                    print(f"  - {path}")
                return
        else:
            active_patterns = _active_screen_patterns(stage, citation_searching=self.citation_searching_mode)
            for pattern in active_patterns:
                if not _require_pattern(csv_dir, pattern, f"{stage} required CSV export", stage=stage):
                    return

        qc_enabled_effective = bool(QC_ENABLED and not self.citation_searching_mode)

        if stage == "full_text":
            paper_dir = csv_dir / str(rule["pdf_dir"])
            first_prep_run = not paper_dir.exists()

            print("[setup] Preparing per-paper folders for full_text (setup preflight)...")
            _run_pipeline_guarded(
                stage=stage,
                split_only=True,
                quiet=True,
                mark_failure=False,
                input_files=input_files,
                run_label_override=self.remaining_run_label,
            )

            if not paper_dir.exists():
                print(f"[setup] Expected per-paper folders at {paper_dir}. Rerun after generating CSV exports.")
                return

            if first_prep_run:
                print(
                    "[setup] First full_text run completed folder creation only. "
                    "Upload one PDF per folder, then rerun main.py to start screening."
                )
                print("[next] After uploading PDFs, rerun: .venv\\Scripts\\python main.py")
                return

            missing = _missing_pdf_folders(paper_dir)
            if missing:
                print(
                    f"[setup] PDFs missing for {len(missing)} folder(s) in {rule['pdf_dir']}. "
                    "Upload all PDFs before screening can start."
                )
                for name in missing:
                    print(f"  - {name}")
                print("[next] After uploading PDFs, rerun: .venv\\Scripts\\python main.py")
                return

            print("[setup] All per-paper folders contain PDFs. Proceeding to screening flow.")

        if stage == "title_abstract":
            if qc_enabled_effective:
                if _run_qc_loop(
                    stage,
                    sample_rate,
                    quiet=False,
                    input_files=input_files,
                    qc_run_label=self.qc_run_label,
                ):
                    _run_pipeline_guarded(
                        stage=stage,
                        confirm_sampling=True,
                        sample_rate=sample_rate,
                        qc_only=False,
                        qc_enabled=False,
                        input_files=input_files,
                        run_label_override=self.remaining_run_label,
                    )
                    _post_run_updates(stage, _last_artifact_dict(), 0)
                    _prompt_retry_if_needed(stage, _last_artifact_dict())
                return
            _run_pipeline_guarded(
                stage=stage,
                confirm_sampling=True,
                sample_rate=sample_rate,
                qc_only=False,
                qc_enabled=False,
                input_files=input_files,
                run_label_override=self.remaining_run_label,
            )
            _post_run_updates(stage, _last_artifact_dict(), 0)
            _prompt_retry_if_needed(stage, _last_artifact_dict())
            return

        if qc_enabled_effective:
            if stage != "full_text":
                print(f"[setup] Preparing per-paper folders for {stage} (setup preflight; no screening in this step)...")
                _run_pipeline_guarded(
                    stage=stage,
                    split_only=True,
                    quiet=True,
                    mark_failure=False,
                    input_files=input_files,
                    run_label_override=self.remaining_run_label,
                )

                if stage == "data_extraction":
                    full_text_rule = _active_stage_rule("full_text", citation_searching=self.citation_searching_mode)
                    full_text_pdf_dir = str(full_text_rule.get("pdf_dir") or "per_paper_full_text")
                    full_text_dir = csv_dir / full_text_pdf_dir
                    if not full_text_dir.exists():
                        print(
                            f"[warning] {full_text_pdf_dir} missing at {full_text_dir}. "
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

            if _run_qc_loop(
                stage,
                sample_rate,
                quiet=False,
                input_files=input_files,
                qc_run_label=self.qc_run_label,
            ):
                _run_pipeline_guarded(
                    stage=stage,
                    quiet=False,
                    confirm_sampling=True,
                    sample_rate=sample_rate,
                    qc_only=False,
                    qc_enabled=False,
                    input_files=input_files,
                    run_label_override=self.remaining_run_label,
                )
                _post_run_updates(stage, _last_artifact_dict(), 0)
                _prompt_retry_if_needed(stage, _last_artifact_dict())
            return

        _run_pipeline_guarded(
            stage=stage,
            quiet=False,
            confirm_sampling=True,
            sample_rate=sample_rate,
            qc_only=False,
            qc_enabled=False,
            input_files=input_files,
            run_label_override=self.remaining_run_label,
        )
        _post_run_updates(stage, _last_artifact_dict(), 0)
        _prompt_retry_if_needed(stage, _last_artifact_dict())
        return


def _sanitize_run_scope(value: str | None) -> str:
    """human readable hint: keep optional terminal run scope safe for output filenames."""

    cleaned = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in str(value or "").strip())
    return cleaned.strip("_-")


def _active_screen_patterns(stage: str, *, citation_searching: bool = False) -> list[str]:
    """human readable hint: choose normal or citation-search CSV patterns from user config."""

    if citation_searching:
        configured = CITATION_SEARCHING_STAGE_RULES.get(stage, {})
        patterns = configured.get("screen_patterns", [])
        return [str(pattern) for pattern in patterns]
    return [str(pattern) for pattern in STAGE_RULES.get(stage, {}).get("screen_patterns", [])]


def _active_stage_rule(stage: str, *, citation_searching: bool = False) -> dict:
    """human readable hint: merge citation-search stage overrides onto the normal stage rule."""

    rule = dict(STAGE_RULES.get(stage, {}))
    if citation_searching:
        rule.update(CITATION_SEARCHING_STAGE_RULES.get(stage, {}))
    return rule


def _prepare_citation_searching_delta(csv_dir: Path, stage: str) -> list[str]:
    """human readable hint: diff citation-search exports before LLM screening."""

    parser = CitationCsvParser()
    targets = parser.find_target_files(str(csv_dir), stage)
    parser.ingest_and_diff(
        current_export_path=targets["citation"],
        previous_export_path=targets["baseline"],
        stage=stage,
    )
    output_dir = csv_dir / "citation_searching_delta"
    exported = parser.export_for_screening(str(output_dir))
    print(
        "[citation] Delta extraction complete: "
        f"baseline={parser.audit.baseline_total_records} "
        f"citation_export={parser.audit.citation_total_records} "
        f"filtered_old={parser.audit.old_records_filtered_out} "
        f"novel={parser.audit.novel_records_for_screening}"
    )
    print(f"[citation] Audit log: {exported['log']}")
    return [exported["csv"]]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """human readable hint: expose run-scoped input selection without changing user_orchestrator.py."""

    parser = argparse.ArgumentParser(description="Run one review-pipeline stage.")
    parser.add_argument(
        "--stage",
        choices=sorted(STAGE_RULES),
        default=None,
        help="Override CURRENT_STAGE for this terminal run.",
    )
    parser.add_argument(
        "--input-file",
        action="append",
        default=[],
        help="Exact CSV input file for this run. Repeat for multiple files. Relative paths resolve from input/.",
    )
    parser.add_argument(
        "--run-scope",
        default=None,
        help="Optional output/QC namespace such as citation_searching to separate this run from normal screening.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Compatibility entrypoint that runs the class-based main workflow."""

    args = _parse_args(argv)
    MainWorkflow(
        stage=args.stage,
        input_files=list(args.input_file or []),
        run_scope=args.run_scope,
    ).run()


if __name__ == "__main__":
    main()
    # Offer to trigger backup after pipeline run only if every prompt was accepted.
    try:
        if _PROMPT_STATE.get("all_yes", False):
            resp = input("\nDo you want to back up your changes to GitHub now? (y/n): ").strip().lower()
            if resp == "y":
                result = subprocess.run([sys.executable, "backup_to_github.py"], check=False)
                if result.returncode != 0:
                    print(
                        f"[warning] Backup script exited with code {result.returncode}. "
                        "Run backup_to_github.py manually after checking git status."
                    )
    except Exception:
        print("[warning] Could not trigger backup script. Please run backup_to_github.py manually if needed.")
