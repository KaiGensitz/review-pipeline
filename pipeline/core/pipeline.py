"""Screening and extraction pipeline.

This module materializes per-paper folders (for full text and data extraction),
selects evidence via embeddings, calls the LLM, writes JSONL/CSV outputs, and
tracks sustainability/resource usage. It retains prior behavior with clearer
structure and logging.
"""

from __future__ import annotations

import csv
import json
import math
import os
import random
import re
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator, Iterable, Tuple, TextIO
from statistics import median

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency
    tqdm = None

from dotenv import load_dotenv

from config.embedding_utils import read_pdf_file, read_pdf_pages, detect_language
from config.llm_client import OpenAIResponder
from config.user_orchestrator import (
    CURRENT_STAGE,
    EMBEDDING_SETTINGS,
    LLM_SETTINGS,
    PATH_SETTINGS,
    PROMPT_FILE,
    SCREENING_DEFAULTS,
    STAGE_RULES,
    require_setting,
)
from pipeline.additions.resource_usage import ResourceUsageConfig, ResourceUsageTracker
from pipeline.selection.chunking import chunk_fulltext_sentences, chunk_paper_sentences
from pipeline.selection.selector import EmbeddingBackend, RelevanceSelector, load_labeled_examples

# Load environment variables once per process.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Configuration defaults pulled from user_orchestrator.
data_language = require_setting(EMBEDDING_SETTINGS, "data_language", "EMBEDDING_SETTINGS")
prompt = PROMPT_FILE.read_text(encoding="utf-8").strip()
use_api = require_setting(LLM_SETTINGS, "use_api", "LLM_SETTINGS")
gpustack_model = require_setting(LLM_SETTINGS, "screening_model", "LLM_SETTINGS")
gpustack_base_url = require_setting(LLM_SETTINGS, "gpustack_base_url", "LLM_SETTINGS")

BATCH_SIZE_DEFAULT = require_setting(SCREENING_DEFAULTS, "batch_size", "SCREENING_DEFAULTS")
SAMPLE_SIZE_DEFAULT = require_setting(SCREENING_DEFAULTS, "sample_size", "SCREENING_DEFAULTS")
CODECARBON_ENABLED_DEFAULT = True
SUSTAINABILITY_DEFAULT = require_setting(
    SCREENING_DEFAULTS, "sustainability_tracking", "SCREENING_DEFAULTS"
)
TOP_K_DEFAULT = require_setting(SCREENING_DEFAULTS, "top_k", "SCREENING_DEFAULTS")
SCORE_THRESHOLD_DEFAULT = require_setting(SCREENING_DEFAULTS, "score_threshold", "SCREENING_DEFAULTS")
SAMPLE_SEED_DEFAULT = require_setting(SCREENING_DEFAULTS, "sample_seed", "SCREENING_DEFAULTS")

TOKENS_PER_WORD = 1.3
TOKENS_PER_PAGE_IMAGE = 258
CONTEXT_WINDOW = 78_000
TITLE_TRUNC = 50  # short folder names to avoid Windows path limits
PAPER_PDF_NAME = "paper.pdf"


@dataclass
class PaperRecord:
    paper_id: str
    title: str
    abstract: str
    metadata: dict


CANONICAL_FIELDS = [
    "Title",
    "Authors",
    "Abstract",
    "Published Year",
    "Published Month",
    "Journal",
    "Volume",
    "Issue",
    "Pages",
    "Accession Number",
    "DOI",
    "Ref",
    "Covidence #",
    "Study",
    "Notes",
    "Tags",
]


class PaperScreeningPipeline:
    def __init__(
        self,
        csv_dir: str,
        knowledge_base_path: str,
        eligibility_output_path: str,
        chunks_output_path: str,
        text_output_path: str,
        top_k: int | None = None,
        score_threshold: float | None = None,
        batch_size: int | None = None,
        embedder: EmbeddingBackend | None = None,
        examples: list[dict] | None = None,
        sample_size: int | None = None,
        sample_seed: int | None = None,
        sustainability_tracking: bool | None = None,
        resource_log_path: str | None = None,
        enable_time_savings: bool = False,
        run_label: str = "run",
        codecarbon_enabled: bool | None = None,
        qc_sample_path: str | None = None,
        qc_sample_readable_path: str | None = None,
        confirm_sampling: bool = False,
        sample_rate: float | None = None,
        qc_only: bool = False,
        qc_enabled: bool | None = None,
        force_new_qc: bool = False,
        error_log_path: str | None = None,
        stage: str | None = None,
        pdf_root: str | None = None,
        overflow_log_path: str | None = None,
        split_only: bool = False,
        quiet: bool = False,
        summary_to_console: bool = True,
    ) -> None:
        """
        Initialize the screening/extraction pipeline with configuration.
        All arguments are strictly typed and have clear defaults for robust, reproducible runs.
        Non-coders: Each parameter controls a key aspect of the workflow (see README for details).
        """

        self.csv_dir = Path(csv_dir)
        self.knowledge_base_path = Path(knowledge_base_path)
        self.eligibility_output_path = Path(eligibility_output_path)
        self.chunks_output_path = Path(chunks_output_path)
        self.text_output_path = Path(text_output_path)
        self.stage_output_dir = self.chunks_output_path.parent

        # Import dynamic config from user_orchestrator
        from config.user_orchestrator import (
            SCREENING_DEFAULTS, QC_ENABLED, QC_SAMPLE_RATE, CURRENT_STAGE
        )

        self.top_k = top_k if top_k is not None else SCREENING_DEFAULTS.get("top_k", 10)
        self.score_threshold = score_threshold if score_threshold is not None else SCREENING_DEFAULTS.get("score_threshold", 0.005)
        self.batch_size = batch_size if batch_size is not None else SCREENING_DEFAULTS.get("batch_size", 32)
        self.sample_size = sample_size if sample_size is not None else SCREENING_DEFAULTS.get("sample_size", None)
        self.sample_seed = sample_seed if sample_seed is not None else SCREENING_DEFAULTS.get("sample_seed", None)
        self.sustainability_tracking = sustainability_tracking if sustainability_tracking is not None else SCREENING_DEFAULTS.get("sustainability_tracking", True)
        self.resource_log_path = Path(resource_log_path) if resource_log_path else Path("output/resource_usage.log")
        self.run_label = run_label
        self.qc_sample_path = Path(qc_sample_path) if qc_sample_path else Path("output/qc_sample_batch.csv")
        self.qc_sample_readable_path = (
            Path(qc_sample_readable_path)
            if qc_sample_readable_path
            else Path("output/qc_sample_batch_readable.txt")
        )
        self.confirm_sampling = confirm_sampling
        self.sample_rate = max(0.0, min(sample_rate if sample_rate is not None else QC_SAMPLE_RATE, 1.0))
        self.qc_only = qc_only
        self.qc_enabled = qc_enabled if qc_enabled is not None else QC_ENABLED
        self.force_new_qc = force_new_qc
        self.error_log_path = Path(error_log_path) if error_log_path else Path("output/error_log.txt")
        self.stage = stage if stage is not None else CURRENT_STAGE
        self.pdf_root = Path(pdf_root) if pdf_root else None
        self.overflow_log_path = Path(overflow_log_path) if overflow_log_path else Path("output/overflow_log.txt")
        self.split_only = split_only
        self.quiet = quiet
        self.summary_to_console = summary_to_console
        self.language_setting = str(EMBEDDING_SETTINGS.get("data_language", "en")) or "en"
        self._detect_language = detect_language
        # human readable hint: tracking response times to surface p50/p95 for operators.
        self._paper_times: list[float] = []
        # human readable hint: keep paper_ids that hit an error so outputs can be flagged inline.
        self._error_ids: set[str] = set()

        self._row_counter = 0
        self._paper_folders: list[Path] = []
        self._qc_sample_ids: set[str] = set()
        self._extraction_criteria = self._extract_criteria_from_prompt(prompt)

        # Resource usage tracker logs tokens and energy for each run.
        self.resource_tracker = ResourceUsageTracker(
            ResourceUsageConfig(
                resource_log_path=self.resource_log_path,
                enable_tracking=self.sustainability_tracking,
                enable_codecarbon=codecarbon_enabled if codecarbon_enabled is not None else False,
                stage=self.stage,
                qc_sample_path=self.qc_sample_path,
                run_label=self.run_label,
                enable_time_savings=enable_time_savings,
            )
        )

        self._total_runtime_seconds = 0.0
        self._paper_count = 0

        # Warn if the prompt template is missing the {data} placeholder.
        if not self.split_only and "{data}" not in prompt:
            print(
                "[warning] prompt is missing {data} placeholder; LLM may not see evidence",
                file=sys.stderr,
            )

        # Embedding backend for chunk selection; loads examples if not provided.
        resolved_batch_size = batch_size if batch_size is not None else SCREENING_DEFAULTS.get("batch_size", 32)
        self.embedder = embedder or EmbeddingBackend(batch_size=resolved_batch_size)
        def to_labeled_examples(examples_list):
            # Accepts list[dict] or list[LabeledExample], returns list[LabeledExample]
            labeled = []
            for ex in examples_list:
                if isinstance(ex, dict) and "label" in ex and "text" in ex:
                    labeled.append({"label": ex["label"], "text": ex["text"]})
                else:
                    raise ValueError("Each example must be a dict with 'label' and 'text' keys.")
            return labeled

        if examples is not None:
            kb_examples = to_labeled_examples(examples)
        else:
            kb_examples = load_labeled_examples(str(self.knowledge_base_path))
        self.selector = RelevanceSelector(self.embedder, kb_examples)

    def run(self) -> bool:
        """Main pipeline: prep folders (if needed), QC sample, then screen papers."""

        self.eligibility_output_path.parent.mkdir(parents=True, exist_ok=True)
        self.chunks_output_path.parent.mkdir(parents=True, exist_ok=True)
        self.text_output_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.quiet:
            if self.split_only:
                print(f"[prep] Creating per-paper folders from: {self.csv_dir.resolve()}")
            else:
                print(f"[pipeline] Screening from CSV dir: {self.csv_dir.resolve()}")

        if self.stage == "full_text":
            self._materialize_paper_folders_full_text()
            if self.split_only:
                missing = self._find_missing_pdfs(self.csv_dir / "per_paper_full_text")
                if missing and not self.quiet:
                    print(
                        f"[prep] PDFs missing for {len(missing)} folder(s) in per_paper_full_text. Add one PDF per folder:"
                    )
                    for name in missing:
                        print(f"  - {name}")
                elif not missing and not self.quiet:
                    print("[prep] Done. PDFs found in all folders. Rerun main.py to start screening.")
                return False
        elif self.stage == "data_extraction":
            self._materialize_data_extraction_subset()
            if self.split_only:
                missing = self._find_missing_pdfs(self.csv_dir / "per_paper_data_extraction")
                if missing and not self.quiet:
                    print(
                        f"[prep] PDFs missing for {len(missing)} folder(s) in per_paper_data_extraction. Add one PDF per folder:"
                    )
                    for name in missing:
                        print(f"  - {name}")
                elif not missing and not self.quiet:
                    print("[prep] Done. PDFs found in all folders. Rerun main.py to start screening.")
                return False

        planned_papers = self._collect_planned_papers()
        if not planned_papers:
            if not self.quiet:
                print("[progress] No papers to process")
            return False

        total_input_rows = len(planned_papers)
        total_planned = len(planned_papers)

        if not self.split_only and self.qc_enabled:
            created_sample = self._ensure_qc_sample(planned_papers, force_new=self.force_new_qc)
            if hasattr(self, "resource_tracker") and self.resource_tracker:
                self.resource_tracker.set_qc_count(len(self._qc_sample_ids))
            if self.qc_only:
                planned_papers = [paper for paper in planned_papers if paper.paper_id in self._qc_sample_ids]
                if not planned_papers:
                    if not self.quiet:
                        print("[qc] QC-only mode selected but no QC papers found; aborting run.")
                    return False
                total_planned = len(planned_papers)
            if not self.confirm_sampling:
                proceed = self._prompt_sampling_confirmation(created_sample)
                if not proceed:
                    if not self.quiet:
                        print(
                            "[qc] QC screening pending. Review the QC files and rerun main.py to continue."
                        )
                    return False
                self.confirm_sampling = True

            # Remaining run: skip QC papers to avoid re-screening the sample twice.
            if self.confirm_sampling and not self.qc_only and self._qc_sample_ids:
                planned_papers = [p for p in planned_papers if p.paper_id not in self._qc_sample_ids]
                total_planned = len(planned_papers)
                if not planned_papers:
                    if not self.quiet:
                        print("[qc] Remaining run has no papers after removing QC sample; nothing to do.")
                    return False

        # When QC is disabled for the remaining run, still skip any known QC sample IDs.
        if not self.qc_enabled and self.confirm_sampling and self._qc_sample_ids:
            planned_papers = [p for p in planned_papers if p.paper_id not in self._qc_sample_ids]
            total_planned = len(planned_papers)
            if not planned_papers:
                if not self.quiet:
                    print("[qc] Remaining run has no papers after removing QC sample; nothing to do.")
                return False

        if self.sustainability_tracking:
            self.resource_tracker.start_run()

        papers_iter = iter(planned_papers)
        progress_total = total_planned if total_planned is not None else (self.sample_size or None)
        progress = (
            tqdm(total=progress_total, desc="screening", ncols=100)
            if (tqdm and progress_total and not self.quiet)
            else None
        )

        start_time = time.time()
        elig_writer = None
        text_writer = None
        chunk_writer = None
        extra_decision_writers: dict[bool, TextIO] = {}
        writer_paths: dict[TextIO, Path] = {}
        elig_main_count = 0
        extra_counts: dict[bool, int] = {True: 0, False: 0}
        reason_counts_main: dict[str, int] = {}
        reason_counts_split: dict[bool, dict[str, int]] = {True: {}, False: {}}
        index_entries: list[dict[str, object]] = []

        if self.stage in {"title_abstract", "full_text"}:
            self.eligibility_output_path.parent.mkdir(parents=True, exist_ok=True)
            self.text_output_path.parent.mkdir(parents=True, exist_ok=True)
            elig_writer = open(self.eligibility_output_path, "w", encoding="utf-8")
            writer_paths[elig_writer] = self.eligibility_output_path
            text_writer = open(self.text_output_path, "w", encoding="utf-8")
            meta_line = json.dumps(
                {
                    "meta": "eligibility_records",
                    "description": f"Per-paper LLM decisions for stage '{self.stage}' (JSONL).",
                }
            )
            elig_writer.write(meta_line + "\n")

            # Stage-specific split outputs for eligibility decisions
            def _variant_path(tag: str) -> Path:
                return self.eligibility_output_path.with_name(
                    self.eligibility_output_path.name.replace("eligibility_", f"eligibility_{tag}_", 1)
                )

            if self.stage in {"title_abstract", "full_text"}:
                select_tag = "select" if self.stage == "title_abstract" else "included"
                exclude_tag = "irrelevant" if self.stage == "title_abstract" else "excluded"
                extra_paths = {
                    True: _variant_path(select_tag),
                    False: _variant_path(exclude_tag),
                }
                for truthy, path in extra_paths.items():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    writer = open(path, "w", encoding="utf-8")
                    writer.write(meta_line + "\n")
                    extra_decision_writers[truthy] = writer
                    writer_paths[writer] = path
            text_writer.write(
                f"SCREENING RESULTS (stage: {self.stage})\n"
                "This file summarizes per-paper selections and decisions for manual review.\n\n"
            )

        if self.stage == "title_abstract":
            self.chunks_output_path.parent.mkdir(parents=True, exist_ok=True)
            chunk_writer = open(self.chunks_output_path, "w", encoding="utf-8")
            chunk_writer.write(
                json.dumps(
                    {
                        "meta": "selected_chunks",
                        "description": f"Chunks retained for LLM input for stage '{self.stage}' (JSONL).",
                    }
                )
                + "\n"
            )

        times_by_decision: dict[bool, list[float]] = {True: [], False: []}

        try:

            for idx, paper in enumerate(papers_iter, start=1):
                self._paper_count = idx

                if progress is not None:
                    progress.update(1)
                    progress.set_postfix({"paper": paper.paper_id})
                elif not self.quiet:
                    denom = total_planned if total_planned is not None else (
                        self.sample_size if self.sample_size else idx
                    )
                    percent = idx / max(denom, 1)
                    bar_len = 30
                    filled = int(bar_len * min(percent, 1.0))
                    bar = "#" * filled + "-" * (bar_len - filled)
                    print(
                        f"\r[progress] {idx}/{denom} papers [{bar}] {percent*100:5.1f}% {paper.paper_id}",
                        end="",
                        flush=True,
                    )

                paper_start_ts = time.time()
                record, token_stats, extraction_payload = self._process_paper(paper)
                paper_elapsed = time.time() - paper_start_ts
                self._paper_times.append(paper_elapsed)

                error_flag = paper.paper_id in self._error_ids

                if elig_writer:
                    payload = {
                        "paper_id": record["paper_id"],
                        "error_flag": error_flag,
                        "llm_decision": record["llm_decision"],
                        "diagnostics": record["diagnostics"],
                        "metadata": record["metadata"],
                        "stage": self.stage,
                    }
                    elig_writer.write(json.dumps(payload) + "\n")
                    elig_main_count += 1

                    is_eligible_val = self._parse_is_eligible(record["llm_decision"])
                    reason_val = self._parse_exclusion_reason(record["llm_decision"])
                    if reason_val:
                        reason_counts_main[reason_val] = reason_counts_main.get(reason_val, 0) + 1
                    if isinstance(is_eligible_val, bool):
                        extra_writer = extra_decision_writers.get(is_eligible_val)
                        if extra_writer is not None:
                            extra_writer.write(json.dumps(payload) + "\n")
                            extra_counts[is_eligible_val] += 1
                            times_by_decision[is_eligible_val].append(paper_elapsed)
                            if reason_val:
                                reason_counts_split[is_eligible_val][reason_val] = (
                                    reason_counts_split[is_eligible_val].get(reason_val, 0) + 1
                                )

                if chunk_writer:
                    chunk_writer.write(
                        json.dumps(
                            {
                                "paper_id": record["paper_id"],
                                "error_flag": error_flag,
                                "selected_chunks": record["selected_chunks"],
                                "stage": self.stage,
                            }
                        )
                        + "\n"
                    )
                elif self.stage in {"full_text", "data_extraction"}:
                    self._write_selected_chunks_to_input(paper, record["selected_chunks"])

                if text_writer:
                    self._write_plain_text_summary(text_writer, record)

                if self.stage == "data_extraction" and extraction_payload is not None:
                    self._write_data_extraction_outputs(paper, extraction_payload)

                if self.sustainability_tracking:
                    self.resource_tracker.log_paper(
                        paper_id=paper.paper_id,
                        prompt_tokens=token_stats.get("prompt_tokens", 0),
                        response_tokens=token_stats.get("response_tokens", 0),
                        pdf_text_tokens=token_stats.get("pdf_text_tokens", 0),
                        pdf_visual_tokens=token_stats.get("pdf_visual_tokens", 0),
                        embedding_tokens=token_stats.get("embedding_tokens", 0),
                        prompt_tokens_source=token_stats.get("prompt_tokens_source", "estimate"),
                        response_tokens_source=token_stats.get("response_tokens_source", "estimate"),
                        embedding_tokens_source=token_stats.get("embedding_tokens_source", "estimate"),
                        paper_seconds=paper_elapsed,
                    )

            if progress:
                progress.close()
            elif not self.quiet:
                print()
        finally:
            def _reverse_lookup_writer(writer_obj):
                for decision_val, w in extra_decision_writers.items():
                    if w is writer_obj:
                        return decision_val
                return None

            def _decision_label(writer_obj):
                if writer_obj is elig_writer:
                    return "all"
                flag = _reverse_lookup_writer(writer_obj)
                if flag is True:
                    return "select" if self.stage == "title_abstract" else "included"
                if flag is False:
                    return "irrelevant" if self.stage == "title_abstract" else "excluded"
                return "unknown"

            def _reason_lookup(writer_obj):
                key = _reverse_lookup_writer(writer_obj)
                if writer_obj is elig_writer:
                    return reason_counts_main
                if isinstance(key, bool):
                    return reason_counts_split.get(key, {})
                return {}

            def _run_tag_from_path(path: Path | None, decision_label: str) -> str:
                """human readable hint: derive a run tag that includes retry/main and timestamp."""

                if path is None:
                    return self.run_label or "run"
                stem = path.stem
                prefix = f"{self.stage}_"
                if stem.startswith(prefix):
                    stem = stem[len(prefix) :]
                marker = f"eligibility_{decision_label}_"
                if marker in stem:
                    stem = stem.replace(marker, "", 1)
                elif "eligibility_" in stem:
                    stem = stem.replace("eligibility_", "", 1)
                return stem

            def _write_summary(writer, count: int) -> None:
                """human readable hint: append per-file totals with share and timing percentiles."""

                percent = (count / total_planned * 100.0) if total_planned else 0.0
                percent_of_input_file = (count / total_input_rows * 100.0) if total_input_rows else 0.0
                decision_key = _reverse_lookup_writer(writer)
                decision_times = times_by_decision.get(decision_key, []) if isinstance(decision_key, bool) else []
                time_stats = self._percentiles(self._paper_times if writer is elig_writer else decision_times)
                summary: dict[str, object] = {
                    "meta": "summary",
                    "paper_count": count,
                    "percent_of_stage": percent,
                    "total_paper_count": total_input_rows,
                    "percent_of_input_file": percent_of_input_file,
                    "response_time_seconds": time_stats,
                }
                reason_payload = _reason_lookup(writer)
                if reason_payload:
                    summary["exclusion_reasons"] = reason_payload
                writer.write(json.dumps(summary) + "\n")
                decision_label = _decision_label(writer)
                run_tag = _run_tag_from_path(writer_paths.get(writer), decision_label)
                sample_selection = f"{self.stage}_{run_tag}_{decision_label}"
                index_entries.append(
                    {
                        "sample_selection": sample_selection,
                        "stage": self.stage,
                        "decision_split": decision_label,
                        "paper_count": count,
                        "percent_of_stage": percent,
                        "total_paper_count": total_input_rows,
                        "percent_of_input_file": percent_of_input_file,
                        "p50_seconds": time_stats.get("p50", 0.0),
                        "p95_seconds": time_stats.get("p95", 0.0),
                        "max_seconds": time_stats.get("max", 0.0),
                        "timestamp": datetime.utcnow().isoformat(),
                        "file_path": str(writer_paths.get(writer, "")),
                    }
                )

            if elig_writer:
                _write_summary(elig_writer, elig_main_count)
                elig_writer.close()
            for truthy, writer in extra_decision_writers.items():
                _write_summary(writer, extra_counts.get(truthy, 0))
                writer.close()

            # human readable hint: index CSV makes it easy for non-coders to find the right eligibility file.
            if index_entries:
                index_path = self.stage_output_dir / f"{self.stage}_eligibility_index.csv"
                fieldnames = [
                    "sample_selection",
                    "stage",
                    "decision_split",
                    "paper_count",
                    "percent_of_stage",
                    "total_paper_count",
                    "percent_of_input_file",
                    "p50_seconds",
                    "p95_seconds",
                    "max_seconds",
                    "timestamp",
                    "file_path",
                ]
                existing_rows: list[dict[str, object]] = []
                if index_path.exists() and index_path.stat().st_size > 0:
                    try:
                        with open(index_path, "r", newline="", encoding="utf-8") as handle:
                            reader = csv.DictReader(handle)
                            for row in reader:
                                if row:
                                    existing_rows.append(dict(row))
                    except Exception:
                        existing_rows = []

                # Drop any rows that collide with the new sample selections to avoid duplicates.
                replace_keys = {e["sample_selection"] for e in index_entries}
                existing_rows = [r for r in existing_rows if r.get("sample_selection") not in replace_keys]
                existing_rows.extend(index_entries)

                with open(index_path, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in existing_rows:
                        writer.writerow(row)
            if text_writer:
                text_writer.close()
            if chunk_writer:
                chunk_writer.close()

        self._total_runtime_seconds = time.time() - start_time

        if not self.quiet and self.summary_to_console:
            print("[pipeline] screening run completed successfully")

        if self.sustainability_tracking:
            self.resource_tracker.stop_run(self._total_runtime_seconds, self._paper_count)

        def _append_error_summary(total_planned: int) -> None:
            """human readable hint: append a run-level summary row into the error log."""

            if not self.error_log_path.exists() or self.error_log_path.stat().st_size == 0:
                return
            unique_ids: set[str] = set()
            try:
                with open(self.error_log_path, "r", encoding="utf-8") as handle:
                    for line in handle:
                        if not line.strip():
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        if isinstance(obj, dict) and not obj.get("meta"):
                            pid = obj.get("paper_id")
                            if pid:
                                unique_ids.add(str(pid))
            except Exception:
                return

            summary_payload = {
                "meta": "error_summary",
                "stage": self.stage,
                "run_label": self.run_label,
                "paper_errors": len(unique_ids),
                "paper_total": total_planned,
                "error_rate_percent": ((len(unique_ids) / total_planned) * 100.0) if total_planned else 0.0,
                "timestamp": datetime.utcnow().isoformat(),
            }
            try:
                with open(self.error_log_path, "a", encoding="utf-8") as logf:
                    logf.write(json.dumps(summary_payload) + "\n")
            except Exception:
                return

        if self.error_log_path.exists() and self.error_log_path.stat().st_size > 0 and (
            not self.quiet and self.summary_to_console
        ):
            print(f"[pipeline] errors occurred; see {self.error_log_path.resolve()}")

        _append_error_summary(total_planned)

        return True

    def _iter_papers(self) -> Iterable[PaperRecord]:
        """Yield papers sequentially; sample only if requested."""

        if self.stage in {"full_text", "data_extraction"} and self._paper_folders:
            folders = self._paper_folders
            if self.sample_size is not None:
                rng = random.Random(self.sample_seed)
                folders = rng.sample(folders, min(self.sample_size, len(folders)))

            for folder in folders:
                meta_path = folder / "metadata.json"
                if not meta_path.exists():
                    continue
                with open(meta_path, "r", encoding="utf-8") as handle:
                    row = json.load(handle)
                title = row.get("Title") or row.get("title", "")
                abstract = row.get("Abstract") or row.get("abstract", "")
                paper_id = (
                    row.get("Covidence #")
                    or row.get("Covidence#")
                    or row.get("paper_id")
                    or folder.name
                )
                metadata = dict(row)
                metadata["folder_path"] = str(folder)
                yield PaperRecord(paper_id=str(paper_id), title=title, abstract=abstract, metadata=metadata)
            return

        csv_files = self._stage_csv_files()
        if self.sample_size is None:
            for csv_file in csv_files:
                yield from self._iter_file_rows(csv_file)
        else:
            all_records: list[PaperRecord] = []
            for csv_file in csv_files:
                all_records.extend(list(self._iter_file_rows(csv_file)))
            if not all_records:
                return []
            rng = random.Random(self.sample_seed)
            sample_n = min(self.sample_size, len(all_records))
            for record in rng.sample(all_records, sample_n):
                yield record

    def _collect_planned_papers(self) -> list[PaperRecord]:
        """Materialize the papers to be screened so sampling and progress are deterministic."""

        return list(self._iter_papers())

    def _ensure_qc_sample(self, planned_papers: list[PaperRecord], force_new: bool = False) -> bool:
        """Create (or load) a QC sample and record its paper_ids."""

        self.qc_sample_path.parent.mkdir(parents=True, exist_ok=True)
        self.qc_sample_readable_path.parent.mkdir(parents=True, exist_ok=True)

        if self.qc_sample_path.exists() and not force_new:
            try:
                with open(self.qc_sample_path, "r", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    rows = [row for row in reader]
                    self._qc_sample_ids = {row.get("paper_id", "") for row in rows if row.get("paper_id")}

                if not self.qc_sample_readable_path.exists():
                    with open(self.qc_sample_readable_path, "w", encoding="utf-8") as qc_txt:
                        qc_txt.write("QC SAMPLE (pre-screen verification)\n")
                        qc_txt.write(f"stage: {self.stage}\n")
                        qc_txt.write(f"timestamp: {datetime.utcnow().isoformat()}Z\n")
                        qc_txt.write(
                            f"records: {len(rows)} of {len(planned_papers)} planned ({self.sample_rate*100:.1f}% target)\n\n"
                        )
                        for idx, row in enumerate(rows, start=1):
                            qc_txt.write(f"[{idx}] paper_id: {row.get('paper_id', '')}\n")
                            qc_txt.write("Title: " + (row.get("title") or "") + "\n")
                            qc_txt.write("Abstract: " + (row.get("abstract") or "")[:1000] + "\n\n")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[qc] Failed to read existing QC sample; regenerating. Error: {exc}")
            else:
                return False

        if not planned_papers:
            return False

        sample_n = max(1, math.ceil(len(planned_papers) * self.sample_rate))
        rng = random.Random(self.sample_seed)
        selected = rng.sample(planned_papers, min(sample_n, len(planned_papers)))
        self._qc_sample_ids = {paper.paper_id for paper in selected}

        with open(self.qc_sample_path, "w", encoding="utf-8", newline="") as qc_file:
            fieldnames = ["paper_id", "title", "abstract", "stage"]
            writer = csv.DictWriter(qc_file, fieldnames=fieldnames)
            writer.writeheader()
            for paper in selected:
                writer.writerow(
                    {
                        "paper_id": paper.paper_id,
                        "title": paper.title,
                        "abstract": paper.abstract,
                        "stage": self.stage,
                    }
                )

        with open(self.qc_sample_readable_path, "w", encoding="utf-8") as qc_txt:
            qc_txt.write("QC SAMPLE (pre-screen verification)\n")
            qc_txt.write(f"stage: {self.stage}\n")
            qc_txt.write(f"timestamp: {datetime.utcnow().isoformat()}Z\n")
            qc_txt.write(
                f"records: {len(selected)} of {len(planned_papers)} planned ({self.sample_rate*100:.1f}% target)\n\n"
            )
            for idx, paper in enumerate(selected, start=1):
                qc_txt.write(f"[{idx}] paper_id: {paper.paper_id}\n")
                qc_txt.write("Title: " + paper.title + "\n")
                qc_txt.write("Abstract: " + paper.abstract[:1000] + "\n\n")

        return True

    def _prompt_sampling_confirmation(self, created_sample: bool) -> bool:
        """Ask the user on the CLI whether to proceed after QC sample is ready."""

        if self.quiet:
            return False

        sample_msg = "created" if created_sample else "found existing"
        print(
            f"[qc] QC sample screening {sample_msg}. Review these files and confirm to proceed:",
            f"\n       CSV: {self.qc_sample_path.resolve()}",
            f"\n  readable: {self.qc_sample_readable_path.resolve()}",
        )

        if not sys.stdin.isatty():
            print("[qc] Non-interactive session detected; rerun with CONFIRM_QC_SAMPLE=1 after review.")
            return False

        while True:
            resp = input("Proceed with QC screening? [y/n]: ").strip().lower()
            if resp in {"y", "yes"}:
                return True
            if resp in {"n", "no"}:
                return False
            print("Please answer 'y' or 'n'.")

    def _normalize_row(self, row: dict, default_id: str = "") -> dict:
        """Normalize a raw CSV row into standard fields."""

        normalized = {k.strip(): (v or "") for k, v in row.items()}

        def fetch(names: list[str]) -> str:
            for name in names:
                value = self._match_row_value(normalized, name)
                if value:
                    return value
            return ""

        def ensure(key: str, aliases: list[str]) -> str:
            if normalized.get(key):
                return str(normalized[key])
            val = fetch(aliases)
            normalized[key] = val or ""
            return normalized[key]

        cov_id = fetch(["Covidence #", "Covidence#", "covidence id", "covidence_id", "covidence number"])
        paper_id = cov_id or fetch(["paper_id", "id", "ID", "Ref", "Study"]) or default_id

        normalized["Covidence #"] = normalized.get("Covidence #") or cov_id
        normalized["Covidence#"] = normalized.get("Covidence#") or cov_id
        normalized["paper_id"] = normalized.get("paper_id") or paper_id

        title = ensure("Title", ["title", "Title"])
        normalized["title"] = normalized.get("title") or title

        authors = ensure("Authors", ["Authors", "authors", "author", "Author"])
        normalized["authors"] = normalized.get("authors") or authors

        abstract = ensure("Abstract", ["abstract", "Abstract"])
        normalized["abstract"] = normalized.get("abstract") or abstract

        year_val = self._extract_year(normalized)
        normalized["year"] = normalized.get("year") or year_val
        normalized["Year"] = normalized.get("Year") or year_val
        normalized["Published Year"] = normalized.get("Published Year") or year_val

        ensure("Published Month", ["Published Month", "month", "Month", "Published month"])
        ensure("Journal", ["Journal", "journal", "Source", "Source Title", "Publication Title"])
        ensure("Volume", ["Volume", "volume"])
        ensure("Issue", ["Issue", "issue"])
        ensure("Pages", ["Pages", "pages", "Page", "page", "Page range", "Page Range"])
        ensure("Accession Number", ["Accession Number", "AccessionNumber", "Accession", "WOS Accession Number"])
        ensure("DOI", ["DOI", "doi", "Doi"])
        ensure("Ref", ["Ref", "Reference", "reference"])
        ensure("Study", ["Study", "study"])
        ensure("Notes", ["Notes", "notes"])
        ensure("Tags", ["Tags", "tags", "Keywords", "keywords", "label", "labels"])

        return normalized

    def _canonicalize_row(self, row: dict) -> dict:
        """Map normalized fields into the canonical metadata schema."""

        normalized = self._normalize_row(row, default_id="")

        def pick(*keys: str) -> str:
            for key in keys:
                val = normalized.get(key)
                if val:
                    return str(val)
            return ""

        canonical = {
            "Title": pick("Title", "title"),
            "Authors": pick("Authors", "authors", "author", "Author"),
            "Abstract": pick("Abstract", "abstract"),
            "Published Year": self._extract_year(normalized),
            "Published Month": pick("Published Month", "month", "Month"),
            "Journal": pick("Journal", "journal", "Source", "Source Title", "Publication Title"),
            "Volume": pick("Volume", "volume"),
            "Issue": pick("Issue", "issue"),
            "Pages": pick("Pages", "pages", "Page", "page", "Page range", "Page Range"),
            "Accession Number": pick("Accession Number", "AccessionNumber", "Accession", "WOS Accession Number"),
            "DOI": pick("DOI", "doi", "Doi"),
            "Ref": pick("Ref", "Reference", "reference"),
            "Covidence #": pick("Covidence #", "Covidence#", "paper_id", "covidence id"),
            "Study": pick("Study", "study"),
            "Notes": pick("Notes", "notes"),
            "Tags": pick("Tags", "tags", "Keywords", "keywords", "label", "labels"),
        }
        return canonical

    def _iter_file_rows(self, csv_file: Path) -> Generator[PaperRecord, None, None]:
        """Yield PaperRecord items from a CSV file."""

        with open(csv_file, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                self._row_counter += 1
                normalized = self._normalize_row(row, default_id=f"row-{self._row_counter:05d}")
                if not normalized.get("title") and not normalized.get("abstract"):
                    continue
                paper_id = normalized.get("paper_id") or f"row-{self._row_counter:05d}"
                metadata = self._canonicalize_row(normalized)
                yield PaperRecord(
                    paper_id=str(paper_id).strip(),
                    title=str(normalized.get("title", "")),
                    abstract=str(normalized.get("abstract", "")),
                    metadata=metadata,
                )

    def _collect_csv_rows(self, select_only: bool = False) -> list[dict]:
        """Collect raw CSV rows into a list (used for folder creation)."""

        rows: list[dict] = []
        csv_files = self._stage_csv_files(select_only=select_only)
        counter = 0
        for csv_file in csv_files:
            with open(csv_file, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    counter += 1
                    normalized = self._normalize_row(row, default_id=f"row-{counter:05d}")
                    rows.append(dict(normalized))
        return rows

    def _process_paper(self, paper: PaperRecord) -> tuple[dict, dict, dict | None]:
        """Select evidence, call the LLM, and build outputs for one paper."""

        llm_decision = None
        selected: list[dict] = []
        llm_input = ""
        extraction_payload = None
        language_used = str(EMBEDDING_SETTINGS.get("data_language", "en"))

        def _needs_retry(decision: str) -> bool:
            """Detect obviously incomplete JSON-like LLM outputs."""

            if not isinstance(decision, str):
                return False
            if decision.startswith("LLM error"):
                return False
            text = decision.strip()
            if not text:
                return False
            if not text.endswith("}"):
                return True
            if text.startswith("{"):
                try:
                    json.loads(text)
                except Exception:
                    return True
            return False

        preselected = False
        chunks: list[dict] = []
        pdf_text_tokens = 0
        pdf_visual_tokens = 0
        estimated_input_tokens = 0
        prompt_tokens = 0
        response_tokens = 0
        embed_tokens = 0
        embed_usage = None
        prompt_tokens_source = "estimate"
        response_tokens_source = "estimate"
        embed_tokens_source = "estimate"
        llm_decision_incomplete = False
        attempt = 0
        failure_reason: str | None = None
        failure_type: str | None = None
        failure_attempt = 0

        if self.stage == "data_extraction":
            preselected_chunks = self._load_selected_chunks_from_input(paper)
            if preselected_chunks:
                selected = preselected_chunks
                preselected = True
                llm_input = self._format_chunks_for_prompt(paper, selected)
                prompt_tokens = len((llm_input or "").split())
                estimated_input_tokens = prompt_tokens

        if not preselected:
            chunks, pdf_text_tokens, pdf_visual_tokens, language_used = self._prepare_chunks(paper)
            if self.stage in {"full_text", "data_extraction"} and not chunks:
                llm_decision = "LLM skipped: PDF missing or unreadable; see error log."
                estimated_input_tokens = pdf_text_tokens + pdf_visual_tokens
                selected = []
                self._log_error(
                    paper.paper_id,
                    f"LLM skipped: no chunks produced (PDF missing or unreadable). folder={paper.metadata.get('folder_path')}",
                    error_type="no_chunks",
                    pdf_text_tokens=pdf_text_tokens,
                    pdf_visual_tokens=pdf_visual_tokens,
                    total_estimated_tokens=estimated_input_tokens,
                )
            else:
                if not chunks:
                    llm_decision = "LLM skipped: no evidence chunks available."
                    self._log_error(
                        paper.paper_id,
                        "LLM skipped: no evidence chunks available after preprocessing.",
                        error_type="no_chunks",
                        pdf_text_tokens=pdf_text_tokens,
                        pdf_visual_tokens=pdf_visual_tokens,
                        total_estimated_tokens=estimated_input_tokens,
                    )
                    selected = []
                else:
                    selected, scores, embed_usage = self.selector.select(
                        chunks, top_k=self.top_k, score_threshold=self.score_threshold
                    )
                if embed_usage:
                    embed_tokens = int(
                        embed_usage.get("prompt_tokens")
                        or embed_usage.get("input_tokens")
                        or embed_usage.get("total_tokens")
                        or 0
                    )
                    embed_tokens_source = "api"
                else:
                    embed_tokens = int(sum(len((c.get("text") or "").split()) for c in chunks) * TOKENS_PER_WORD)

                llm_input = self._format_chunks_for_prompt(paper, selected)
                prompt_tokens = len((llm_input or "").split())
                estimated_input_tokens = prompt_tokens + pdf_text_tokens + pdf_visual_tokens

        if llm_decision is None and not selected:
            llm_decision = "LLM skipped: no evidence available after selection."
            self._log_error(
                paper.paper_id,
                "LLM skipped: empty evidence set (title/abstract or PDF missing).",
                error_type="no_evidence",
                total_estimated_tokens=estimated_input_tokens,
            )

        if llm_decision is None:
            if estimated_input_tokens > CONTEXT_WINDOW:
                llm_decision = (
                    f"LLM skipped: estimated input {estimated_input_tokens} tokens exceeds context window {CONTEXT_WINDOW}."
                )
                self._log_overflow(paper.paper_id, estimated_input_tokens)
            else:
                attempt = 1
                llm_decision, llm_usage = self._call_llm(llm_input)
                if llm_usage:
                    prompt_tokens = int(
                        llm_usage.get("prompt_tokens")
                        or llm_usage.get("input_tokens")
                        or llm_usage.get("total_tokens")
                        or prompt_tokens
                    )
                    response_tokens = int(
                        llm_usage.get("completion_tokens")
                        or llm_usage.get("output_tokens")
                        or llm_usage.get("response_tokens")
                        or 0
                    )
                    prompt_tokens_source = "api"
                    response_tokens_source = "api"
                elif llm_decision:
                    prompt_tokens = len((llm_input or "").split())
                    response_tokens = len((llm_decision or "").split())
                    prompt_tokens_source = "estimate"
                    response_tokens_source = "estimate"

                if llm_decision and _needs_retry(llm_decision):
                    llm_decision_incomplete = True
                    attempt = 2
                    retry_decision, retry_usage = self._call_llm(llm_input)
                    if retry_decision:
                        llm_decision = retry_decision
                    if retry_usage:
                        prompt_tokens = int(
                            retry_usage.get("prompt_tokens")
                            or retry_usage.get("input_tokens")
                            or retry_usage.get("total_tokens")
                            or prompt_tokens
                        )
                        response_tokens = int(
                            retry_usage.get("completion_tokens")
                            or retry_usage.get("output_tokens")
                            or retry_usage.get("response_tokens")
                            or response_tokens
                        )
                        prompt_tokens_source = "api"
                        response_tokens_source = "api"
                    else:
                        if response_tokens == 0:
                            response_tokens = len((llm_decision or "").split())
                        if prompt_tokens == 0:
                            prompt_tokens = len((llm_input or "").split())

                    if _needs_retry(llm_decision):
                        llm_decision_incomplete = True
                        failure_reason = "LLM decision incomplete after retry; output may be truncated."
                        failure_type = "llm_incomplete"
                        failure_attempt = attempt
                        model_name = getattr(getattr(self, "llm_client", None), "model", None) or gpustack_model
                        print(
                            f"[error] chat attempt {attempt}/2 failed for model='{model_name}': output incomplete; logged and paper flagged for later re-screen",
                            file=sys.stderr,
                        )
                    else:
                        llm_decision_incomplete = False

                if failure_type is None and isinstance(llm_decision, str) and llm_decision.startswith("LLM error"):
                    llm_decision_incomplete = True
                    failure_reason = llm_decision
                    failure_type = "llm_error"
                    failure_attempt = max(attempt, 2)
                    model_name = getattr(getattr(self, "llm_client", None), "model", None) or gpustack_model
                    print(
                        f"[error] chat attempt {failure_attempt}/2 failed for model='{model_name}': error is logged and paper flagged for later re-screen",
                        file=sys.stderr,
                    )

        if llm_decision and response_tokens == 0:
            response_tokens = len((llm_decision or "").split())

        if failure_type is None and self._decision_missing_fields(llm_decision):
            llm_decision_incomplete = True
            failure_reason = (
                "LLM decision missing justification or exclusion_reason_category after retries; flagged for re-screen."
            )
            failure_type = "llm_missing_fields"
            failure_attempt = attempt or 2

        if failure_type:
            self._log_error(
                paper.paper_id,
                failure_reason or "LLM decision incomplete after retries.",
                error_type=failure_type,
                attempt=failure_attempt or None,
                prompt_tokens=prompt_tokens,
                response_tokens=response_tokens,
                embedding_tokens=embed_tokens,
                pdf_text_tokens=pdf_text_tokens,
                pdf_visual_tokens=pdf_visual_tokens,
                total_estimated_tokens=estimated_input_tokens,
            )

        total_chunks = len(chunks) if chunks else len(selected)
        record = {
            "paper_id": paper.paper_id,
            "selected_chunks": selected,
            "llm_decision": llm_decision,
            "diagnostics": {
                "total_chunks": total_chunks,
                "selected_count": len(selected),
                "top_k": self.top_k,
                "score_threshold": self.score_threshold,
                "preselected_chunks": preselected,
                "stage": self.stage,
                "llm_decision_incomplete": llm_decision_incomplete,
                "language_used": language_used,
            },
            "metadata": paper.metadata,
        }

        if self.stage == "data_extraction":
            extraction_payload = self._build_extraction_payload(paper, llm_decision)
            self._write_data_extraction_metadata(paper, selected, llm_decision, extraction_payload)

        token_stats = {
            "prompt_tokens": prompt_tokens,
            "prompt_tokens_source": prompt_tokens_source,
            "response_tokens": response_tokens,
            "response_tokens_source": response_tokens_source,
            "embedding_tokens": embed_tokens,
            "embedding_tokens_source": embed_tokens_source,
            "pdf_text_tokens": pdf_text_tokens,
            "pdf_visual_tokens": pdf_visual_tokens,
        }

        return record, token_stats, extraction_payload

    def _prepare_chunks(self, paper: PaperRecord) -> Tuple[list[dict], int, int, str]:
        """Create evidence chunks, token counts, and resolved language for one paper."""

        language_setting = self.language_setting
        resolved_language = language_setting or "en"

        if self.stage == "title_abstract":
            if language_setting in {"auto_first", "auto-first"}:
                sample_text = f"{paper.title}\n{paper.abstract}"
                resolved_language = self._detect_language(sample_text)
            elif language_setting == "auto":
                resolved_language = "auto"
            chunks = chunk_paper_sentences(paper.paper_id, paper.title, paper.abstract, resolved_language)
            return chunks, 0, 0, resolved_language

        if self.stage in {"full_text", "data_extraction"}:
            resolved_path = self._resolve_pdf_path(paper)
            pdf_text, page_count, used_path = self._load_pdf_text(paper, resolved_path)
            if not pdf_text:
                return [], 0, 0, resolved_language
            if language_setting in {"auto_first", "auto-first"}:
                sample_text = f"{paper.title}\n{pdf_text[:4000]}" if pdf_text else f"{paper.title}\n{paper.abstract}"
                resolved_language = self._detect_language(sample_text)
            elif language_setting == "auto":
                resolved_language = "auto"
            page_texts = self._load_pdf_pages(used_path)
            chunks = chunk_fulltext_sentences(
                paper.paper_id,
                paper.title,
                pdf_text,
                resolved_language,
                page_texts=page_texts,
            )
            pdf_text_tokens = self._estimate_text_tokens(pdf_text)
            pdf_visual_tokens = page_count * TOKENS_PER_PAGE_IMAGE
            return chunks, pdf_text_tokens, pdf_visual_tokens, resolved_language

        chunks = chunk_paper_sentences(paper.paper_id, paper.title, paper.abstract, resolved_language)
        return chunks, 0, 0, resolved_language

    def _materialize_paper_folders_full_text(self) -> None:
        """Split select CSV rows into per-paper folders under csv_dir/per_paper_full_text."""

        csv_rows = self._collect_csv_rows(select_only=True)
        if not csv_rows:
            print("[warning] no select CSV rows found for materialization")
            return

        base_dir = self.csv_dir / "per_paper_full_text"
        base_dir.mkdir(parents=True, exist_ok=True)
        folders: list[Path] = []

        for row in csv_rows:
            folder_name = self._build_paper_folder_name(row)
            folder_path = base_dir / folder_name
            folder_path.mkdir(parents=True, exist_ok=True)

            canonical = self._canonicalize_row(row)
            metadata_path = folder_path / "metadata.json"
            with open(metadata_path, "w", encoding="utf-8") as handle:
                json.dump(canonical, handle, ensure_ascii=False, indent=2)

            csv_path = folder_path / "metadata.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=CANONICAL_FIELDS)
                writer.writeheader()
                writer.writerow(canonical)

            folders.append(folder_path)

        self._paper_folders = folders

    def _materialize_data_extraction_subset(self) -> None:
        """Create per-paper data_extraction folders from included IDs."""

        source_dir = self.csv_dir / "per_paper_full_text"
        if not source_dir.exists():
            print(
                f"[warning] full_text folders not found at {source_dir}; run split-only at full_text stage first"
            )
            self._paper_folders = []
            return

        included_csv = self._find_included_csv()
        if not included_csv:
            print("[warning] no *_included_csv_* file found; data_extraction subset not prepared")
            self._paper_folders = []
            return

        included_ids = self._load_included_ids(included_csv)
        if not included_ids:
            print("[warning] no included IDs extracted; data_extraction subset not prepared")
            self._paper_folders = []
            return

        target_dir = self.csv_dir / "per_paper_data_extraction"
        target_dir.mkdir(parents=True, exist_ok=True)

        copied: list[Path] = []
        for folder in sorted(source_dir.iterdir()):
            if not folder.is_dir():
                continue
            meta_path = folder / "metadata.json"
            if not meta_path.exists():
                continue
            try:
                with open(meta_path, "r", encoding="utf-8") as handle:
                    row = json.load(handle)
            except Exception:
                continue

            cov_id = self._extract_covidence_id(row)
            if cov_id not in included_ids:
                continue

            dest = target_dir / folder.name
            try:
                dest.mkdir(parents=True, exist_ok=True)

                pdfs = sorted(folder.glob("*.pdf"))
                if pdfs:
                    shutil.copy2(pdfs[0], dest / pdfs[0].name)

                full_text_chunks = folder / "full_text_selected_chunks.jsonl"
                if full_text_chunks.exists():
                    shutil.copy2(full_text_chunks, dest / "data_extraction_selected_chunks.jsonl")

                row["folder_path"] = str(dest)
                metadata_path = dest / "metadata.json"
                with open(metadata_path, "w", encoding="utf-8") as handle:
                    json.dump(row, handle, ensure_ascii=False, indent=2)

                csv_path = dest / "metadata.csv"
                with open(csv_path, "w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=CANONICAL_FIELDS)
                    writer.writeheader()
                    writer.writerow({field: row.get(field, "") for field in CANONICAL_FIELDS})

                copied.append(dest)
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    str(cov_id),
                    f"data_extraction folder copy failed: {exc}",
                    error_type="data_extraction_copy_failed",
                )

        self._paper_folders = copied

    @staticmethod
    def _find_missing_pdfs(base_dir: Path) -> list[str]:
        """List folders that do not contain any PDF."""

        if not base_dir.exists():
            return []

        missing: list[str] = []
        for folder in sorted(base_dir.iterdir()):
            if folder.is_dir() and not any(folder.glob("*.pdf")):
                missing.append(folder.name)
        return missing

    def _find_included_csv(self) -> Path | None:
        """Find the most recent included CSV used for data_extraction."""

        candidates = [p for p in self.csv_dir.glob("*_included_csv_*.csv")]
        return max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None

    def _stage_csv_files(self, select_only: bool = False) -> list[Path]:
        """Return stage-appropriate CSV files."""

        if select_only:
            return sorted(self.csv_dir.glob("*_select_csv_*.csv"))

        patterns = STAGE_RULES.get(self.stage, {}).get("screen_patterns", [])
        if not patterns:
            return sorted(self.csv_dir.glob("*.csv"))

        files: list[Path] = []
        for pattern in patterns:
            files.extend(sorted(self.csv_dir.glob(pattern)))
        if self.csv_dir.name == "retry_runs":
            return [max(files, key=lambda p: p.stat().st_mtime)] if files else []
        return sorted(files)

    def _load_included_ids(self, csv_path: Path) -> set[str]:
        """Read included IDs from a Covidence CSV."""

        ids: set[str] = set()
        with open(csv_path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                cid = self._extract_covidence_id(row)
                if cid:
                    ids.add(cid)
        return ids

    def _extract_covidence_id(self, row: dict) -> str:
        """Extract the best available Covidence/paper ID."""

        return str(
            row.get("Covidence #")
            or row.get("Covidence#")
            or row.get("paper_id")
            or row.get("id")
            or row.get("ID")
            or ""
        ).strip()

    def _extract_year(self, row: dict) -> str:
        """Try to find a publication year from many possible columns."""

        candidates = [
            "year",
            "Year",
            "year of publication",
            "Year of publication",
            "Year of Publication",
            "Publication Year",
            "publication year",
            "Published Year",
            "PublicationYear",
            "PublishedYear",
            "PubYear",
            "PY",
            "date",
            "Date",
            "publication date",
            "Publication date",
            "Publication Date",
            "Date Published",
            "Published",
        ]
        raw = ""
        for candidate in candidates:
            raw = self._match_row_value(row, candidate)
            if raw:
                break
        if not raw:
            return ""
        match = re.search(r"(19|20)\d{2}", raw)
        return match.group(0) if match else str(raw).strip()

    @staticmethod
    def _match_row_value(row: dict, key: str) -> str:
        """Find a value in a row using exact, case-insensitive, or compact keys."""

        if key in row and row[key]:
            return str(row[key])

        lower = key.lower()
        for rk, rv in row.items():
            if rv and rk.lower() == lower:
                return str(rv)

        compact = key.replace(" ", "").lower()
        for rk, rv in row.items():
            if rv and rk.replace(" ", "").lower() == compact:
                return str(rv)
        return ""

    def _build_paper_folder_name(self, row: dict) -> str:
        """Create a safe per-paper folder name using ID/author/year/title."""

        def norm(val: str) -> str:
            return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in (val or "")).strip("_")

        cov_id = row.get("Covidence #") or row.get("Covidence#") or row.get("paper_id") or "unknown"
        authors = row.get("authors") or row.get("Authors") or ""
        first_author = authors.split(",")[0].strip()
        first_author = first_author.split(" ")[0] if first_author else ""
        year = self._extract_year(row)
        title = row.get("title") or row.get("Title") or ""

        parts = [norm(str(cov_id)), norm(str(first_author)), norm(str(year)), norm(str(title))[:TITLE_TRUNC]]
        name = "_".join([p for p in parts if p])
        while "__" in name:
            name = name.replace("__", "_")
        return name or "paper"

    def _load_pdf_text(self, paper: PaperRecord, resolved_path: Path | None = None) -> tuple[str, int, Path | None]:
        """Read PDF text and count pages; returns the path used."""

        path = resolved_path or self._resolve_pdf_path(paper)
        if not path or not path.exists():
            self._log_error(
                paper.paper_id,
                f"PDF not found or unreadable path for stage {self.stage}: {path}",
                error_type="pdf_missing",
            )
            return "", 0, None

        try:
            text = read_pdf_file(str(path))
            if not text or not text.strip():
                self._log_error(
                    paper.paper_id,
                    f"PDF has no extractable text (likely scanned or empty). OCR is disabled; skipping: {path}",
                    error_type="pdf_unreadable",
                )
                return "", 0, None
            return text, self._count_pdf_pages(path), path
        except Exception as exc:  # pylint: disable=broad-except
            self._log_error(paper.paper_id, f"PDF read failed at {path}: {exc}", error_type="pdf_read_error")
            return "", 0, None

    def _load_pdf_pages(self, path: Path | None) -> list[str]:
        """Read PDF text page-by-page for approximate provenance."""

        if not path or not path.exists():
            return []
        try:
            return read_pdf_pages(str(path))
        except Exception:
            return []

    def _resolve_pdf_path(self, paper: PaperRecord) -> Path | None:
        """Find the PDF inside the per-paper folder and normalize its filename."""

        folder = paper.metadata.get("folder_path")
        if not folder:
            self._log_error(paper.paper_id, "PDF folder path missing in metadata", error_type="pdf_folder_missing")
            return None

        folder_path = Path(folder)
        if not folder_path.exists():
            self._log_error(
                paper.paper_id,
                f"PDF folder does not exist: {folder_path}",
                error_type="pdf_folder_not_found",
            )
            return None

        canonical = folder_path / PAPER_PDF_NAME
        pdfs = sorted(folder_path.glob("*.pdf"))
        if canonical.exists():
            pdfs = [canonical] + [p for p in pdfs if p != canonical]

        if not pdfs:
            self._log_error(
                paper.paper_id,
                f"PDF not found in folder: {folder_path}",
                error_type="pdf_missing",
            )
            return None

        pdf_path = pdfs[0]
        cov_id = str(paper.metadata.get("Covidence #") or paper.paper_id).strip().lstrip("#") or "paper"
        target = folder_path / f"{cov_id}.pdf"
        rename_error: Exception | None = None

        if pdf_path != target:
            try:
                if target.exists():
                    pdf_path = target
                else:
                    if sys.platform == "win32":
                        src_long = Path("\\\\?\\" + str(pdf_path))
                        dst_long = Path("\\\\?\\" + str(target))
                        src_long.replace(dst_long)
                    else:
                        pdf_path.replace(target)
                    pdf_path = target
            except Exception as exc:  # pylint: disable=broad-except
                rename_error = exc

        if not pdf_path.exists():
            detail = f"; rename_error={rename_error}" if rename_error else ""
            self._log_error(
                paper.paper_id,
                f"PDF missing after rename attempt in {folder_path}{detail}",
                error_type="pdf_missing_after_rename",
            )
            return None

        return pdf_path

    def _count_pdf_pages(self, path: Path) -> int:
        """Count pages in a PDF (used for approximate input sizing)."""

        try:
            import pdfplumber

            with pdfplumber.open(str(path)) as pdf:
                return len(pdf.pages)
        except Exception:
            return 0

    def _estimate_text_tokens(self, text: str) -> int:
        """Estimate tokens from text length (rough approximation)."""

        words = len(text.split())
        return int(words * TOKENS_PER_WORD)

    def _write_plain_text_summary(self, handle, record: dict) -> None:
        """Write a human-readable summary for each paper (screening stages only)."""

        handle.write(f"Paper {record['paper_id']}\n")
        handle.write(
            "Title: "
            + next((c["text"] for c in record["selected_chunks"] if c.get("kind") == "title"), "")
            + "\n"
        )
        handle.write("Key snippets:\n")
        for idx, chunk in enumerate(record["selected_chunks"], start=1):
            handle.write(f"  {idx}. {chunk.get('text','')[:500]} (score {chunk.get('score',0):.3f})\n")
        decision_val = record.get("llm_decision")
        decision_text = json.dumps(decision_val, ensure_ascii=False) if isinstance(decision_val, dict) else str(decision_val)
        handle.write("Model decision: " + decision_text + "\n")
        handle.write(
            "Diagnostics: "
            f"total_chunks={record['diagnostics']['total_chunks']}, "
            f"selected={record['diagnostics']['selected_count']}, "
            f"top_k={record['diagnostics']['top_k']}, "
            f"threshold={record['diagnostics']['score_threshold']}\n"
        )
        handle.write("Metadata: " + json.dumps(record["metadata"]) + "\n\n")

    def _format_chunks_for_prompt(self, paper: PaperRecord, selected: list[dict]) -> str:
        """Format selected chunks into the LLM prompt body."""

        lines = [f"Paper ID: {paper.paper_id}"]
        lines.append(f"Title: {paper.title.strip()}")
        lines.append("Selected evidence:")
        for chunk in selected:
            lines.append(
                f"- {chunk['chunk_id']} ({chunk.get('kind','?')}, score={chunk.get('score', 0):.3f}): {chunk['text']}"
            )
        return "\n".join(lines)

    def _call_llm(self, context: str) -> tuple[str | None, dict | None]:
        """Call the LLM and return both text and usage (if provided by the API)."""

        try:
            if use_api:
                # Import dynamic config for model and base_url
                from config.user_orchestrator import LLM_SETTINGS, require_setting
                model = str(require_setting(LLM_SETTINGS, "screening_model", "LLM_SETTINGS"))
                base_url_val = LLM_SETTINGS.get("gpustack_base_url")
                base_url = str(base_url_val) if base_url_val is not None else None
                responder = OpenAIResponder(
                    data=context,
                    model=model,
                    prompt_template=prompt,
                    client=self._get_openai_client(base_url=base_url),
                )
                return responder.generate_response()
        except Exception as exc:  # pylint: disable=broad-except
            return f"LLM error: {exc}", None
        return None, None

    def _get_openai_client(self, base_url: str | None = None):
        """Create a configured OpenAI API client."""

        from openai import OpenAI

        return OpenAI(api_key=os.environ.get("LLM_API_KEY"), base_url=base_url)

    @staticmethod
    def _percentiles(values: list[float]) -> dict:
        """human readable hint: provide quick p50/p95/max without heavy deps."""

        if not values:
            return {"p50": 0.0, "p95": 0.0, "max": 0.0}
        vals = sorted(values)
        n = len(vals)
        p50 = median(vals)
        p95_idx = max(0, int(0.95 * n) - 1)
        p95 = vals[p95_idx]
        return {"p50": float(p50), "p95": float(p95), "max": float(vals[-1])}

    def _parse_is_eligible(self, decision: object) -> bool | None:
        """human readable hint: stage-aware extraction of is_eligible from the LLM decision payload."""

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
                val_low = val.lower()
                allow_neutral = self.stage == "title_abstract"
                if val_low in {"true", "yes", "eligible", "include"}:
                    return True
                if allow_neutral and val_low in {"maybe", "neutral"}:
                    return True
                if val_low in {"false", "no", "ineligible", "exclude"}:
                    return False
        return None

    @staticmethod
    def _parse_exclusion_reason(decision: object) -> str | None:
        """human readable hint: derive exclusion_reason_category if present in LLM output."""

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

    def _decision_missing_fields(self, decision: object) -> bool:
        """human readable hint: detect missing justification or exclusion_reason_category without altering the decision."""

        if decision is None:
            return True

        payload = decision
        if isinstance(decision, str):
            try:
                payload = json.loads(decision)
            except Exception:
                return True

        if not isinstance(payload, dict):
            return True

        justification = payload.get("justification")
        reason_val = self._parse_exclusion_reason(payload)
        missing_just = not isinstance(justification, str) or not justification.strip()

        is_eligible_val = self._parse_is_eligible(payload)
        if is_eligible_val is False:
            missing_reason = not isinstance(reason_val, str) or not reason_val.strip()
        elif is_eligible_val is None:
            missing_reason = True
        else:
            missing_reason = False

        return bool(missing_just or missing_reason or is_eligible_val is None)

    def _log_error(
        self,
        paper_id: str,
        message: str,
        context: dict | None = None,
        error_type: str | None = None,
        attempt: int | None = None,
        prompt_tokens: int | None = None,
        response_tokens: int | None = None,
        embedding_tokens: int | None = None,
        pdf_text_tokens: int | None = None,
        pdf_visual_tokens: int | None = None,
        total_estimated_tokens: int | None = None,
        **extra: object,
    ) -> None:
        """Append errors to the error log with detailed context for transparency."""

        llm_model = getattr(getattr(self, "llm_client", None), "model", None) or gpustack_model

        payload = {
            "paper_id": paper_id,
            "error": message,
            "stage": getattr(self, "stage", None),
            "run_label": getattr(self, "run_label", None),
            "llm_model": llm_model,
            "timestamp": datetime.utcnow().isoformat(),
        }

        if error_type:
            payload["error_type"] = error_type
        if attempt is not None:
            payload["attempt"] = attempt
        if prompt_tokens is not None:
            payload["prompt_tokens"] = prompt_tokens
        if response_tokens is not None:
            payload["response_tokens"] = response_tokens
        if embedding_tokens is not None:
            payload["embedding_tokens"] = embedding_tokens
        if pdf_text_tokens is not None:
            payload["pdf_text_tokens"] = pdf_text_tokens
        if pdf_visual_tokens is not None:
            payload["pdf_visual_tokens"] = pdf_visual_tokens
        if total_estimated_tokens is not None:
            payload["total_estimated_tokens"] = total_estimated_tokens

        if context:
            payload.update(context)
        if extra:
            payload.update(extra)

        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.error_log_path, "a", encoding="utf-8") as logf:
            logf.write(json.dumps(payload) + "\n")
        try:
            if paper_id:
                self._error_ids.add(str(paper_id))
        except Exception:
            pass

    def _log_overflow(self, paper_id: str, estimated_tokens: int) -> None:
        """Record a context-window overflow event."""

        msg = {
            "paper_id": paper_id,
            "estimated_tokens": estimated_tokens,
            "context_window": CONTEXT_WINDOW,
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.overflow_log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.overflow_log_path, "a", encoding="utf-8") as logf:
            logf.write(json.dumps(msg) + "\n")
        self._log_error(
            paper_id,
            f"context overflow: {estimated_tokens} > {CONTEXT_WINDOW}",
            error_type="context_overflow",
            total_estimated_tokens=estimated_tokens,
        )

    def _write_data_extraction_metadata(
        self,
        paper: PaperRecord,
        selected: list[dict],
        decision: str | None,
        extraction_payload: dict | None,
    ) -> None:
        """Write evidence.json linking extracted fields to selected chunks."""

        output_dir = self._data_extraction_output_dir(paper)
        output_dir.mkdir(parents=True, exist_ok=True)
        meta_path = output_dir / f"{self.stage}_evidence.json"

        payload = {
            "paper_id": paper.paper_id,
            "llm_decision": decision,
            "selected_chunks": selected,
            "extracted_data": extraction_payload.get("extracted_data") if extraction_payload else None,
            "field_provenance": extraction_payload.get("field_provenance") if extraction_payload else None,
            "criteria": self._extraction_criteria,
            "timestamp": datetime.utcnow().isoformat(),
        }

        try:
            with open(meta_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
        except Exception as exc:  # pylint: disable=broad-except
            self._log_error(paper.paper_id, f"failed to write evidence metadata: {exc}", error_type="evidence_write_failed")

    def _data_extraction_output_dir(self, paper: PaperRecord) -> Path:
        """Return the output folder for this paper in data_extraction."""

        folder = paper.metadata.get("folder_path")
        if folder:
            return self.stage_output_dir / Path(folder).name
        return self.stage_output_dir / str(paper.paper_id)

    def _write_selected_chunks_to_input(self, paper: PaperRecord, selected: list[dict]) -> None:
        """Save selected chunks inside the input per-paper folder."""

        folder = paper.metadata.get("folder_path")
        if not folder:
            return

        folder_path = Path(folder)
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
            chunks_path = folder_path / f"{self.stage}_selected_chunks.jsonl"
            with open(chunks_path, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "meta": "selected_chunks",
                            "description": f"Chunks retained for LLM input for stage '{self.stage}' (JSONL).",
                        }
                    )
                    + "\n"
                )
                handle.write(
                    json.dumps(
                        {
                            "paper_id": paper.paper_id,
                            "error_flag": paper.paper_id in self._error_ids,
                            "selected_chunks": selected,
                            "stage": self.stage,
                        }
                    )
                    + "\n"
                )
        except Exception as exc:  # pylint: disable=broad-except
            self._log_error(
                paper.paper_id,
                f"failed to write selected chunks: {exc}",
                error_type="selected_chunks_write_failed",
            )

    def _load_selected_chunks_from_input(self, paper: PaperRecord) -> list[dict]:
        """Load preselected chunks from the input folder, if present."""

        folder = paper.metadata.get("folder_path")
        if not folder:
            return []

        chunks_path = Path(folder) / f"{self.stage}_selected_chunks.jsonl"
        if not chunks_path.exists():
            return []

        try:
            with open(chunks_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    if payload.get("paper_id") == paper.paper_id and "selected_chunks" in payload:
                        return payload.get("selected_chunks") or []
        except Exception as exc:  # pylint: disable=broad-except
            self._log_error(
                paper.paper_id,
                f"failed to read selected chunks: {exc}",
                error_type="selected_chunks_read_failed",
            )
        return []

    def _write_data_extraction_outputs(self, paper: PaperRecord, extraction_payload: dict) -> None:
        """Write per-paper extraction outputs (JSONL + CSV)."""

        output_dir = self._data_extraction_output_dir(paper)
        output_dir.mkdir(parents=True, exist_ok=True)

        extraction_jsonl_path = output_dir / f"{self.stage}_extraction_results.jsonl"
        with open(extraction_jsonl_path, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "meta": "extraction_results",
                        "description": "Per-paper extracted fields (JSONL).",
                        "criteria": self._extraction_criteria,
                    }
                )
                + "\n"
            )
            handle.write(json.dumps(extraction_payload) + "\n")

        extraction_csv_path = output_dir / f"{self.stage}_extraction_results.csv"
        fieldnames = ["paper_id"]
        if self._extraction_criteria:
            fieldnames.extend(self._extraction_criteria)
        else:
            fieldnames.append("extracted_data")

        with open(extraction_csv_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            row = {"paper_id": extraction_payload.get("paper_id", "")}
            extracted = extraction_payload.get("extracted_data") or {}
            if self._extraction_criteria:
                for key in self._extraction_criteria:
                    value = extracted.get(key, "")
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, ensure_ascii=False)
                    row[key] = value
            else:
                row["extracted_data"] = json.dumps(extracted, ensure_ascii=False)
            writer.writerow(row)

    @staticmethod
    def _normalize_criterion(text: str) -> str:
        """Normalize a prompt bullet line into a clean field name."""

        cleaned = text.strip().strip("- ").strip()
        for sep in (":", "â€”", "-", "â€“"):
            if sep in cleaned:
                cleaned = cleaned.split(sep, 1)[0].strip()
                break
        return cleaned

    @classmethod
    def _extract_criteria_from_prompt(cls, prompt_text: str) -> list[str]:
        """Infer extraction fields from bullet lines in the prompt."""

        criteria: list[str] = []
        seen = set()
        for line in prompt_text.splitlines():
            stripped = line.strip()
            if stripped.startswith(("- ", "* ")):
                item = stripped[2:].strip()
                key = cls._normalize_criterion(item)
                if key and key.lower() not in seen:
                    criteria.append(key)
                    seen.add(key.lower())
        return criteria

    def _build_extraction_payload(self, paper: PaperRecord, llm_decision: str | None) -> dict | None:
        """Parse the LLM output into structured extraction data."""

        if not llm_decision:
            return None

        extracted_data = None
        field_provenance = {}
        raw_text = llm_decision

        try:
            extracted_data = json.loads(raw_text)
        except Exception:
            match = re.search(r"\{.*\}", raw_text, re.DOTALL)
            if match:
                try:
                    extracted_data = json.loads(match.group(0))
                except Exception:
                    extracted_data = None

        if isinstance(extracted_data, dict):
            if "field_provenance" in extracted_data and isinstance(extracted_data["field_provenance"], dict):
                field_provenance = extracted_data.pop("field_provenance")
            if self._extraction_criteria:
                criteria_set = {c.lower() for c in self._extraction_criteria}
                filtered = {k: v for k, v in extracted_data.items() if k.lower() in criteria_set}
            else:
                filtered = extracted_data
        else:
            filtered = {}

        return {
            "paper_id": paper.paper_id,
            "extracted_data": filtered,
            "field_provenance": field_provenance,
            "raw_output": raw_text,
        }
