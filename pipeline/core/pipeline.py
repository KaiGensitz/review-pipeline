"""Screening and extraction pipeline.

This module materializes per-paper folders (for full text and data extraction),
selects evidence via embeddings, calls the LLM, writes JSONL/CSV outputs, and
tracks sustainability/resource usage. It retains prior behavior with clearer
structure and logging.
"""

from __future__ import annotations

import asyncio
import csv
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from queue import Queue
import random
import re
import shutil
import sys
from threading import Thread
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Iterable, Tuple, TextIO
from typing import Any, Literal
from statistics import mean, median, pstdev

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency
    tqdm = None

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from pipeline.integrations.embedding_utils import (
    read_pdf_file,
    read_pdf_pages,
    detect_language,
    normalize_extracted_text,
)
from pipeline.integrations.llm_client import OpenAIResponder
from config.user_orchestrator import (
    CURRENT_STAGE,
    EMBEDDING_SETTINGS,
    LLM_SETTINGS,
    PATH_SETTINGS,
    PROMPT_FILES,
    SCREENING_DEFAULTS,
    STAGE_RULES,
    require_setting,
)
from pipeline.additions.resource_usage import ResourceUsageEngine
from pipeline.selection.chunking import chunk_fulltext_sentences, chunk_paper_sentences
from pipeline.selection.selector import EmbeddingBackend, SelectionEngine, load_labeled_examples

# Load environment variables once per process.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Configuration defaults pulled from user_orchestrator.
data_language = require_setting(EMBEDDING_SETTINGS, "data_language", "EMBEDDING_SETTINGS")
use_api = require_setting(LLM_SETTINGS, "use_api", "LLM_SETTINGS")
gpustack_model = require_setting(LLM_SETTINGS, "screening_model", "LLM_SETTINGS")
gpustack_base_url = require_setting(LLM_SETTINGS, "gpustack_base_url", "LLM_SETTINGS")
llm_max_tokens = require_setting(LLM_SETTINGS, "max_tokens", "LLM_SETTINGS", int)

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
ELIGIBILITY_CRITERIA_PLACEHOLDER = "{eligibility_criteria}"
RETRIEVAL_FALLBACK_TOP_K = 20
RETRIEVAL_WEAK_MIN_NON_TITLE = 3
RETRIEVAL_WEAK_MIN_WORDS = 280
RETRIEVAL_FRAGMENTED_MAX_SHARE = 0.40
FULLTEXT_BORDERLINE_CONFIDENCE_MIN = 0.45
FULLTEXT_BORDERLINE_CONFIDENCE_MAX = 0.75
SECTION_RESCUE_KEYWORDS = (
    "introduction",
    "background",
    "method",
    "methods",
    "methodology",
    "materials and methods",
    "participant",
    "participants",
    "intervention",
    "procedure",
    "outcome",
    "results",
    "discussion",
    "conclusion",
    "conclusions",
    "trial",
    "protocol",
    "urban",
    "city",
    "smartphone",
    "mobile app",
    "machine learning",
    "artificial intelligence",
)
SECTION_PRIORITY = ("introduction", "method", "results", "discussion", "conclusion")
SECTION_INFERENCE_PATTERNS: dict[str, re.Pattern[str]] = {
    "introduction": re.compile(r"\b(introduction|background)\b", re.IGNORECASE),
    "method": re.compile(r"\b(methods?|methodology|materials?\s+and\s+methods?|study\s+design)\b", re.IGNORECASE),
    "results": re.compile(r"\b(results?|findings)\b", re.IGNORECASE),
    "discussion": re.compile(r"\bdiscussion\b", re.IGNORECASE),
    "conclusion": re.compile(r"\b(conclusion|conclusions|summary)\b", re.IGNORECASE),
}
PUBLISHER_BOILERPLATE_STRONG_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"authorized licensed use",
        r"downloaded on",
        r"ieee xplore",
        r"all rights reserved",
        r"creative commons attribution",
        r"distributed under the terms",
        r"open-access",
        r"copyright",
        r"\bissn\b",
        r"\bisbn\b",
    )
]
PUBLISHER_BOILERPLATE_WEAK_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bdoi\b",
        r"\bcitation\b",
        r"published",
        r"accepted",
        r"received",
        r"front\.",
        r"journal",
        r"license",
    )
]


def _load_optional_eligibility_criteria_text() -> str:
    """human readable hint: load shared eligibility criteria text when configured and available."""

    configured_path = PATH_SETTINGS.get("eligibility_criteria_file")
    if not configured_path:
        return ""

    criteria_path = Path(configured_path)
    if not criteria_path.exists():
        print(
            f"[warning] eligibility criteria file not found at: {criteria_path}. "
            "Continuing without criteria injection.",
            file=sys.stderr,
        )
        return ""

    return criteria_path.read_text(encoding="utf-8").strip()


def _load_stage_prompt_template(stage: str) -> str:
    """human readable hint: load stage prompt and inject shared criteria only when placeholder is present."""

    prompt_path = PROMPT_FILES.get(stage)
    if not prompt_path:
        raise ValueError(f"Missing prompt mapping for stage '{stage}'.")

    prompt_template = prompt_path.read_text(encoding="utf-8")
    if ELIGIBILITY_CRITERIA_PLACEHOLDER not in prompt_template:
        return prompt_template.strip()

    criteria_text = _load_optional_eligibility_criteria_text()
    if not criteria_text:
        print(
            "[warning] prompt contains {eligibility_criteria} but no criteria text was loaded; "
            "continuing with an empty replacement.",
            file=sys.stderr,
        )

    return prompt_template.replace(ELIGIBILITY_CRITERIA_PLACEHOLDER, criteria_text).strip()


@dataclass
class PaperRecord:
    paper_id: str
    title: str
    abstract: str
    metadata: dict


class _ScreeningDecisionBaseModel(BaseModel):
    """human readable hint: shared schema for screening decisions returned by the LLM."""

    model_config = ConfigDict(extra="allow")

    step_by_step_deliberation: str
    not_adult_population: bool
    no_smartphone_technology: bool
    no_artificial_intelligence: bool
    no_physical_activity: bool
    not_urban_context: bool | Literal["NEUTRAL"]
    wrong_publication_type: bool
    insufficient_context: bool
    confidence_score: float = Field(ge=0.0, le=1.0)
    justification: str = Field(min_length=1)
    exclusion_reason_category: str | None

    @model_validator(mode="after")
    def _check_reason_for_exclusion(self):
        """human readable hint: exclusion decisions must carry an explicit exclusion reason."""

        if getattr(self, "is_eligible", None) is False and not self.exclusion_reason_category:
            raise ValueError("exclusion_reason_category is required when is_eligible is false")
        return self


class TitleAbstractScreeningDecisionModel(_ScreeningDecisionBaseModel):
    """human readable hint: title_abstract allows a NEUTRAL eligibility outcome."""

    is_eligible: bool | Literal["NEUTRAL"]


class FullTextScreeningDecisionModel(_ScreeningDecisionBaseModel):
    """human readable hint: full_text requires a strict boolean eligibility outcome."""

    seed_references: bool
    is_eligible: bool

    @model_validator(mode="after")
    def _check_seed_references_threshold(self):
        """human readable hint: seed_references can be true only when confidence_score is strictly greater than 0.98."""

        if self.seed_references and not (self.confidence_score > 0.98):
            raise ValueError(
                "seed_references can be true only when confidence_score is strictly greater than 0.98"
            )
        return self


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
        embedder: EmbeddingBackend | SelectionEngine | None = None,
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
        self._llm_model = str(gpustack_model)
        self._llm_base_url = str(gpustack_base_url) if gpustack_base_url is not None else None
        self._openai_client = None
        self._openai_client_base_url: str | None = None
        self._async_openai_client = None
        self._async_openai_client_base_url: str | None = None
        self._async_max_concurrency = max(1, int(LLM_SETTINGS.get("async_max_concurrency", 12) or 12))
        self._async_max_retries = max(0, int(LLM_SETTINGS.get("async_max_retries", 3) or 3))
        self._async_backoff_base = float(LLM_SETTINGS.get("async_backoff_base_seconds", 0.5) or 0.5)
        self._async_backoff_max = float(LLM_SETTINGS.get("async_backoff_max_seconds", 8.0) or 8.0)
        self._async_jitter = float(LLM_SETTINGS.get("async_jitter_seconds", 0.2) or 0.2)
        self._async_heartbeat_seconds = max(5.0, float(LLM_SETTINGS.get("async_heartbeat_seconds", 30) or 30))
        self._async_enable_full_text = bool(LLM_SETTINGS.get("async_enable_full_text", True))
        self._async_enable_data_extraction = bool(LLM_SETTINGS.get("async_enable_data_extraction", True))
        self._validation_max_retries = 3
        # human readable hint: tracking response times to surface p50/p95 for operators.
        self._paper_times: list[float] = []
        # human readable hint: keep paper_ids that hit an error so outputs can be flagged inline.
        self._error_ids: set[str] = set()

        self._row_counter = 0
        self._paper_folders: list[Path] = []
        self._qc_sample_ids: set[str] = set()
        self._stage_csv_cache: dict[bool, list[Path]] = {}
        self.prompt_template = _load_stage_prompt_template(self.stage)
        self._prompt_template_hash = self._sha256_text(self.prompt_template)
        self._prompt_campaign_id = self._prompt_template_hash[:12]
        self._prompt_snapshot_path: Path | None = None
        self._prompt_required_json_fields = self._extract_required_json_fields_from_prompt(self.prompt_template)
        self._extraction_criteria = self._extract_criteria_from_prompt(self.prompt_template)

        # Resource usage tracker logs tokens and energy for each run.
        self.resource_tracker = ResourceUsageEngine(
            resource_log_path=self.resource_log_path,
            enable_tracking=self.sustainability_tracking,
            enable_codecarbon=codecarbon_enabled if codecarbon_enabled is not None else False,
            stage=self.stage,
            qc_sample_path=self.qc_sample_path,
            run_label=self.run_label,
            enable_time_savings=enable_time_savings,
        )

        self._total_runtime_seconds = 0.0
        self._paper_count = 0

        # Warn if the prompt template is missing the {data} placeholder.
        if not self.split_only and "{data}" not in self.prompt_template:
            print(
                "[warning] prompt is missing {data} placeholder; LLM may not see evidence",
                file=sys.stderr,
            )

        # Embedding backend for chunk selection; loads examples if not provided.
        self.embedder = embedder if isinstance(embedder, EmbeddingBackend) else None
        self.selection_engine = embedder if isinstance(embedder, SelectionEngine) else None
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

        if self.selection_engine is None:
            self.selection_engine = SelectionEngine(
                examples=kb_examples,
                batch_size=self.batch_size,
                embedder=self.embedder,
            )

        self.selector = self.selection_engine

    @staticmethod
    def _sha256_text(value: str) -> str:
        """human readable hint: stable fingerprint to verify whether two input texts are exactly identical."""

        return hashlib.sha256((value or "").encode("utf-8")).hexdigest()

    def _warn_prompt_hash_drift_in_stage_outputs(self) -> None:
        """human readable hint: warn when stage outputs were produced with a different prompt hash."""

        if self.stage not in {"title_abstract", "full_text"}:
            return

        stage_dir = self.stage_output_dir
        if not stage_dir.exists():
            return

        seen_hashes: set[str] = set()
        paths = sorted(
            stage_dir.glob(f"{self.stage}_*_eligibility_*.jsonl"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in paths:
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    for line in handle:
                        payload = json.loads(line)
                        if not isinstance(payload, dict) or "paper_id" not in payload:
                            continue
                        diagnostics = payload.get("diagnostics") or {}
                        prompt_hash = diagnostics.get("prompt_template_sha256")
                        if isinstance(prompt_hash, str) and prompt_hash:
                            seen_hashes.add(prompt_hash)
                        break
            except Exception:
                continue

        if seen_hashes and any(value != self._prompt_template_hash for value in seen_hashes):
            print(
                (
                    f"[warning] prompt campaign drift detected in output/{self.stage}: "
                    f"current={self._prompt_campaign_id}, seen={sorted({value[:12] for value in seen_hashes})}. "
                    "Comparisons across these runs may not reflect code-only changes."
                ),
                file=sys.stderr,
            )

    def _persist_prompt_template_snapshot(self) -> None:
        """human readable hint: write one immutable prompt template snapshot per campaign for exact replay."""

        try:
            self.stage_output_dir.mkdir(parents=True, exist_ok=True)
            stem = self.chunks_output_path.stem
            stamp_match = re.search(
                rf"_(\d{{8}}_\d{{2}}-\d{{2}})_{re.escape(self._prompt_campaign_id)}$",
                stem,
            )
            run_stamp = stamp_match.group(1) if stamp_match else datetime.now().strftime("%Y%m%d_%H-%M")
            snapshot_path = self.stage_output_dir / f"{self.stage}_prompt_template_{run_stamp}_{self._prompt_campaign_id}.txt"
            if snapshot_path.exists():
                existing = snapshot_path.read_text(encoding="utf-8")
                if existing == self.prompt_template:
                    self._prompt_snapshot_path = snapshot_path
                    return
            snapshot_path.write_text(self.prompt_template, encoding="utf-8")
            self._prompt_snapshot_path = snapshot_path
        except Exception as exc:
            self._prompt_snapshot_path = None
            print(
                f"[warning] could not persist prompt snapshot for campaign {self._prompt_campaign_id}: {exc}",
                file=sys.stderr,
            )

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

        self._persist_prompt_template_snapshot()

        self._warn_prompt_hash_drift_in_stage_outputs()

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
        jsonl_flush_every = 64
        elig_buffer: list[str] = []
        split_buffers: dict[bool, list[str]] = {True: [], False: []}
        chunk_buffer: list[str] = []
        index_entries: list[dict[str, object]] = []
        executor: ThreadPoolExecutor | None = None

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

        if self.stage in {"title_abstract", "full_text"}:
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

        def _process_with_timing(paper_item: PaperRecord) -> tuple[PaperRecord, dict, dict, dict | None, float]:
            """human readable hint: keep per-paper runtime while allowing optional concurrent processing."""

            paper_start_ts = time.time()
            record_obj, token_stats_obj, extraction_obj = self._process_paper(paper_item)
            return paper_item, record_obj, token_stats_obj, extraction_obj, (time.time() - paper_start_ts)

        if self.stage == "title_abstract":
            # human readable hint: title_abstract is processed via asyncio to maximize API concurrency safely.
            processed_stream = self._process_title_abstract_batch(planned_papers)
        elif self._use_async_stage_processing():
            # human readable hint: full_text/data_extraction can also use async LLM calls with bounded concurrency.
            processed_stream = self._process_non_title_async_batch(planned_papers)
        else:
            processed_stream = (_process_with_timing(paper) for paper in planned_papers)

        try:

            for idx, processed in enumerate(processed_stream, start=1):
                self._paper_count = idx
                paper, record, token_stats, extraction_payload, paper_elapsed = processed

                if progress is not None:
                    progress.update(1)
                elif not self.quiet:
                    denom = total_planned if total_planned is not None else (
                        self.sample_size if self.sample_size else idx
                    )
                    percent = idx / max(denom, 1)
                    bar_len = 30
                    filled = int(bar_len * min(percent, 1.0))
                    bar = "#" * filled + "-" * (bar_len - filled)
                    print(
                        f"\r[progress] {idx}/{denom} papers [{bar}] {percent*100:5.1f}%",
                        end="",
                        flush=True,
                    )

                self._paper_times.append(paper_elapsed)

                error_flag = paper.paper_id in self._error_ids
                if error_flag and not self.quiet:
                    print(
                        f"\n[warn] LLM could not finalize a decision for paper {paper.paper_id}; see {self.error_log_path}",
                        flush=True,
                    )

                if elig_writer:
                    payload = {
                        "paper_id": record["paper_id"],
                        "error_flag": error_flag,
                        "llm_decision": record["llm_decision"],
                        "diagnostics": record["diagnostics"],
                        "metadata": record["metadata"],
                        "stage": self.stage,
                    }
                    payload_line = json.dumps(payload) + "\n"
                    elig_buffer.append(payload_line)
                    if len(elig_buffer) >= jsonl_flush_every:
                        elig_writer.write("".join(elig_buffer))
                        elig_buffer.clear()
                    elig_main_count += 1

                    decision_payload = self._decision_payload(record["llm_decision"])
                    is_eligible_val = self._parse_is_eligible(decision_payload)
                    reason_val = self._parse_exclusion_reason(decision_payload)
                    if reason_val:
                        reason_counts_main[reason_val] = reason_counts_main.get(reason_val, 0) + 1
                    if isinstance(is_eligible_val, bool):
                        extra_writer = extra_decision_writers.get(is_eligible_val)
                        if extra_writer is not None:
                            split_buffer = split_buffers[is_eligible_val]
                            split_buffer.append(payload_line)
                            if len(split_buffer) >= jsonl_flush_every:
                                extra_writer.write("".join(split_buffer))
                                split_buffer.clear()
                            extra_counts[is_eligible_val] += 1
                            times_by_decision[is_eligible_val].append(paper_elapsed)
                            if reason_val:
                                reason_counts_split[is_eligible_val][reason_val] = (
                                    reason_counts_split[is_eligible_val].get(reason_val, 0) + 1
                                )

                if chunk_writer:
                    chunk_buffer.append(
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
                    if len(chunk_buffer) >= jsonl_flush_every:
                        chunk_writer.write("".join(chunk_buffer))
                        chunk_buffer.clear()

                if self.stage in {"full_text", "data_extraction"}:
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
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "file_path": str(writer_paths.get(writer, "")),
                    }
                )

            if elig_writer:
                if elig_buffer:
                    elig_writer.write("".join(elig_buffer))
                    elig_buffer.clear()
                _write_summary(elig_writer, elig_main_count)
                elig_writer.close()
            for truthy, writer in extra_decision_writers.items():
                split_buffer = split_buffers.get(truthy)
                if split_buffer:
                    writer.write("".join(split_buffer))
                    split_buffer.clear()
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
                if chunk_buffer:
                    chunk_writer.write("".join(chunk_buffer))
                    chunk_buffer.clear()
                chunk_writer.close()
            if executor is not None:
                executor.shutdown(wait=True)

        self._total_runtime_seconds = time.time() - start_time

        if not self.quiet and self.summary_to_console:
            print("[pipeline] screening run completed successfully")

        if self.sustainability_tracking:
            self.resource_tracker.stop_run(self._total_runtime_seconds, self._paper_count)

        def _append_error_summary(total_planned: int) -> None:
            """human readable hint: append a run-level summary row into the error log."""

            if not self.error_log_path.exists() or self.error_log_path.stat().st_size == 0:
                return

            paper_error_count = len(self._error_ids)

            summary_payload = {
                "meta": "error_summary",
                "stage": self.stage,
                "run_label": self.run_label,
                "paper_errors": paper_error_count,
                "paper_total": total_planned,
                "error_rate_percent": ((paper_error_count / total_planned) * 100.0) if total_planned else 0.0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
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
                        qc_txt.write(f"timestamp: {datetime.now(timezone.utc).isoformat()}\n")
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
            qc_txt.write(f"timestamp: {datetime.now(timezone.utc).isoformat()}\n")
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

    def _process_title_abstract_batch(
        self,
        planned_papers: list[PaperRecord],
    ) -> Generator[tuple[PaperRecord, dict, dict, dict | None, float], None, None]:
        """human readable hint: stream title_abstract completions paper-by-paper as async calls finish."""

        yield from self._stream_async_batch(
            planned_papers,
            self._process_title_abstract_paper_async,
            stage_label="title_abstract",
        )

    def _use_async_stage_processing(self) -> bool:
        """human readable hint: allow stage-specific opt-in async processing beyond title_abstract."""

        if self.stage == "full_text":
            return bool(self._async_enable_full_text and bool(use_api))
        if self.stage == "data_extraction":
            return bool(self._async_enable_data_extraction and bool(use_api))
        return False

    def _process_non_title_async_batch(
        self,
        planned_papers: list[PaperRecord],
    ) -> Generator[tuple[PaperRecord, dict, dict, dict | None, float], None, None]:
        """human readable hint: stream full_text/data_extraction completions paper-by-paper."""

        yield from self._stream_async_batch(
            planned_papers,
            self._process_paper_async,
            stage_label=str(self.stage),
        )

    def _stream_async_batch(
        self,
        planned_papers: list[PaperRecord],
        processor,
        *,
        stage_label: str,
    ) -> Generator[tuple[PaperRecord, dict, dict, dict | None, float], None, None]:
        """human readable hint: bridge async processing to sync caller while emitting per-paper completion updates."""

        queue: Queue = Queue()

        async def _runner() -> None:
            semaphore = asyncio.Semaphore(max(1, self._async_max_concurrency))
            total = len(planned_papers)
            completed = 0
            warn_count = 0
            last_completion_ts = time.time()

            async def _heartbeat() -> None:
                """human readable hint: emit periodic progress so operators can see async work is alive."""

                while True:
                    await asyncio.sleep(self._async_heartbeat_seconds)
                    if self.quiet:
                        continue
                    remaining = max(total - completed, 0)
                    seconds_since_last = int(max(0.0, time.time() - last_completion_ts))
                    print(
                        f"[async][heartbeat] stage={stage_label} done={completed}/{total} warn={warn_count} remaining={remaining} no_completion_for={seconds_since_last}s",
                        flush=True,
                    )

            async def _run_one(paper_item: PaperRecord) -> tuple[PaperRecord, dict, dict, dict | None, float]:
                paper_start_ts = time.time()
                async with semaphore:
                    record_obj, token_stats_obj, extraction_obj = await processor(paper_item)
                return paper_item, record_obj, token_stats_obj, extraction_obj, (time.time() - paper_start_ts)

            tasks = [asyncio.create_task(_run_one(paper)) for paper in planned_papers]
            heartbeat_task = asyncio.create_task(_heartbeat())

            try:
                for finished in asyncio.as_completed(tasks):
                    result = await finished
                    completed += 1
                    last_completion_ts = time.time()
                    paper_item, record_obj, _, _, _ = result
                    decision_obj = record_obj.get("llm_decision")
                    diagnostics_obj = record_obj.get("diagnostics", {}) or {}
                    decision_incomplete = bool(diagnostics_obj.get("llm_decision_incomplete"))
                    llm_error = isinstance(decision_obj, str) and decision_obj.startswith("LLM error")
                    if decision_incomplete or llm_error:
                        warn_count += 1
                    if not self.quiet:
                        status = "WARN" if (decision_incomplete or llm_error) else "OK"
                        print(
                            f"[async] stage={stage_label} completed {completed}/{total} paper={paper_item.paper_id} status={status}",
                            flush=True,
                        )
                        if decision_incomplete or llm_error:
                            print(
                                f"[async][warn] paper={paper_item.paper_id} returned incomplete/invalid LLM output; check {self.error_log_path}",
                                flush=True,
                            )
                    queue.put(("result", result))
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
                queue.put(("done", None))

        def _thread_target() -> None:
            try:
                asyncio.run(_runner())
            except Exception as exc:  # pylint: disable=broad-except
                queue.put(("error", exc))
                queue.put(("done", None))

        worker = Thread(target=_thread_target, daemon=True)
        worker.start()

        while True:
            kind, payload = queue.get()
            if kind == "result":
                yield payload
                continue
            if kind == "error":
                raise RuntimeError(f"Async batch processing failed for stage '{stage_label}': {payload}")
            if kind == "done":
                break

    async def _process_title_abstract_paper_async(self, paper: PaperRecord) -> tuple[dict, dict, dict | None]:
        """human readable hint: async title_abstract screening with strict schema validation and retry policy."""

        llm_input = self._title_abstract_full_input(paper)
        selected = [
            {
                "paper_id": paper.paper_id,
                "chunk_id": f"{paper.paper_id}::full_input::0000",
                "text": llm_input,
                "kind": "full_input",
                "page_start": None,
                "page_end": None,
                "line_start": None,
                "line_end": None,
            }
        ]
        selected, selected_score_stats = self._attach_chunk_certainty_metrics(selected)
        selected_coverage = self._build_selected_coverage_metrics(selected, page_count=None)

        prompt_tokens = len((llm_input or "").split())
        response_tokens = 0
        prompt_tokens_source = "estimate"
        response_tokens_source = "estimate"
        llm_seed = LLM_SETTINGS.get("seed")
        llm_top_p = float(LLM_SETTINGS.get("top_p", 1.0) or 1.0)
        llm_decision_incomplete = False
        failure_type: str | None = None
        failure_reason: str | None = None

        if not use_api:
            llm_decision = "LLM disabled: use_api=False; no API call made."
        else:
            llm_decision = None
            max_attempts = self._validation_max_retries
            for attempt in range(1, max_attempts + 1):
                current_decision, llm_usage = await self._call_llm_async(llm_input)

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

                if not current_decision:
                    failure_type = "llm_no_response"
                    failure_reason = "LLM returned no decision after retries."
                    llm_decision_incomplete = True
                    continue

                if isinstance(current_decision, str) and current_decision.startswith("LLM error"):
                    failure_type = "llm_error"
                    failure_reason = current_decision
                    llm_decision_incomplete = True
                    continue

                sanitized = self._sanitize_screening_decision(current_decision, paper)

                try:
                    validated_payload = self._validate_screening_decision(sanitized or "")
                    llm_decision = json.dumps(validated_payload, ensure_ascii=False)
                    llm_decision_incomplete = False
                    failure_type = None
                    failure_reason = None
                    break
                except ValidationError as exc:
                    llm_decision_incomplete = True
                    failure_type = "llm_validation_error"
                    failure_reason = f"Schema validation failed: {exc}"
                    if attempt == max_attempts:
                        llm_decision = sanitized

        if llm_decision and response_tokens == 0:
            response_tokens = len((llm_decision or "").split())

        if failure_type:
            self._log_error(
                paper.paper_id,
                failure_reason or "LLM decision failed validation.",
                error_type=failure_type,
                prompt_tokens=prompt_tokens,
                response_tokens=response_tokens,
                embedding_tokens=0,
                pdf_text_tokens=0,
                pdf_visual_tokens=0,
                total_estimated_tokens=prompt_tokens,
            )

        context_input_hash = self._sha256_text(llm_input)
        prompt_template_hash = self._sha256_text(self.prompt_template)
        full_prompt_hash = self._sha256_text(self.prompt_template.replace("{data}", llm_input or ""))

        output_metadata = self._metadata_without_authors(paper.metadata)
        record = {
            "paper_id": paper.paper_id,
            "selected_chunks": selected,
            "llm_decision": llm_decision,
            "diagnostics": {
                "total_chunks": 1,
                "selected_count": 1,
                "top_k": self.top_k,
                "score_threshold": self.score_threshold,
                "preselected_chunks": True,
                "stage": self.stage,
                "llm_decision_incomplete": llm_decision_incomplete,
                "language_used": str(EMBEDDING_SETTINGS.get("data_language", "en")),
                "llm_input_sha256": context_input_hash,
                "prompt_template_sha256": prompt_template_hash,
                "full_prompt_sha256": full_prompt_hash,
                "prompt_campaign_id": self._prompt_campaign_id,
                "prompt_template_snapshot_path": str(self._prompt_snapshot_path) if self._prompt_snapshot_path else None,
                "llm_seed": llm_seed,
                "llm_top_p": llm_top_p,
                "selected_score_stats": selected_score_stats,
                "selected_page_coverage": selected_coverage,
                "selection_trace": {
                    "fallback_triggered": False,
                    "effective_top_k": self.top_k,
                    "notes": "title_abstract uses full input block by design",
                },
            },
            "metadata": output_metadata,
        }

        token_stats = {
            "prompt_tokens": prompt_tokens,
            "prompt_tokens_source": prompt_tokens_source,
            "response_tokens": response_tokens,
            "response_tokens_source": response_tokens_source,
            "embedding_tokens": 0,
            "embedding_tokens_source": "estimate",
            "pdf_text_tokens": 0,
            "pdf_visual_tokens": 0,
        }

        return record, token_stats, None

    async def _process_paper_async(self, paper: PaperRecord) -> tuple[dict, dict, dict | None]:
        """human readable hint: shared async paper processor used by both async and sync execution paths."""

        if self.stage == "title_abstract":
            return await self._process_title_abstract_paper_async(paper)

        llm_decision = None
        selected: list[dict] = []
        llm_input = ""
        extraction_payload = None
        language_used = str(EMBEDDING_SETTINGS.get("data_language", "en"))

        def _needs_retry(decision: str) -> bool:
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
        api_disabled = not use_api
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
        raw_chunk_count = 0
        dropped_low_quality_chunks = 0
        page_count: int | None = None
        selected_score_stats: dict = {
            "non_title_count": 0,
            "score_min": 0.0,
            "score_max": 0.0,
            "score_mean": 0.0,
            "score_std": 0.0,
        }
        selected_page_coverage: dict = {
            "pdf_page_count": None,
            "selected_unique_pages": 0,
            "selected_page_min": None,
            "selected_page_max": None,
            "selected_page_coverage_ratio": None,
        }
        selection_trace: dict = {
            "fallback_triggered": False,
            "effective_top_k": self.top_k,
        }
        publication_prefilter: dict[str, Any] = {
            "likely_non_empirical_publication": False,
            "strong_matches": [],
            "weak_matches": [],
        }
        decision_guardrails: dict[str, Any] = {
            "adjudication_triggered": False,
            "adjudication_reasons": [],
            "decision_contradictions": [],
        }
        llm_decision_incomplete = False
        attempt = 0
        failure_reason: str | None = None
        failure_type: str | None = None
        failure_attempt = 0
        llm_seed = LLM_SETTINGS.get("seed")
        llm_top_p = float(LLM_SETTINGS.get("top_p", 1.0) or 1.0)

        if self.stage == "data_extraction":
            preselected_chunks = self._load_selected_chunks_from_input(paper)
            if preselected_chunks:
                selected = preselected_chunks
                selected, selected_score_stats = self._attach_chunk_certainty_metrics(selected)
                selected_page_coverage = self._build_selected_coverage_metrics(selected, page_count=None)
                preselected = True
                llm_input = self._format_chunks_for_prompt(paper, selected)
                prompt_tokens = len((llm_input or "").split())
                estimated_input_tokens = prompt_tokens
                selection_trace = {
                    "fallback_triggered": False,
                    "effective_top_k": self.top_k,
                    "source": "preselected_chunks",
                }

        if not preselected:
            if self.stage == "full_text":
                publication_prefilter = self._publication_type_prefilter(paper.metadata)
            chunks, pdf_text_tokens, pdf_visual_tokens, language_used = await asyncio.to_thread(
                self._prepare_chunks,
                paper,
            )
            raw_chunk_count = len(chunks)
            if pdf_visual_tokens:
                page_count = max(int(pdf_visual_tokens / TOKENS_PER_PAGE_IMAGE), 0)
            if self.stage in {"full_text", "data_extraction"} and chunks:
                chunks, dropped_low_quality_chunks = self._filter_low_quality_chunks(chunks)
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
            elif not chunks:
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
                selected, embed_usage, selection_trace = await asyncio.to_thread(
                    self._select_chunks_with_rescue,
                    chunks,
                )
                selected, selected_score_stats = self._attach_chunk_certainty_metrics(selected)
                selected_page_coverage = self._build_selected_coverage_metrics(selected, page_count=page_count)

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
            elif api_disabled:
                llm_decision = "LLM disabled: use_api=False; no API call made."
                prompt_tokens = len((llm_input or "").split())
                response_tokens = 0
                prompt_tokens_source = "estimate"
                response_tokens_source = "estimate"
                llm_decision_incomplete = False
            else:
                screening_stage = self.stage in {"title_abstract", "full_text"}
                max_attempts = self._validation_max_retries if screening_stage else 2
                model_name = getattr(getattr(self, "llm_client", None), "model", None) or gpustack_model
                attempt_context = llm_input
                for attempt in range(1, max_attempts + 1):
                    current_decision, llm_usage = await self._call_llm_async(attempt_context)

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

                    if not current_decision:
                        if attempt == max_attempts:
                            failure_reason = "LLM returned no decision after retries."
                            failure_type = "llm_no_response"
                            failure_attempt = attempt
                        continue

                    llm_decision = current_decision

                    if isinstance(llm_decision, str) and llm_decision.startswith("LLM error"):
                        llm_decision_incomplete = True
                        failure_reason = llm_decision
                        failure_type = "llm_error"
                        failure_attempt = attempt
                        print(
                            f"[error] async chat attempt {attempt}/{max_attempts} failed for model='{model_name}': {llm_decision}",
                            file=sys.stderr,
                        )
                        if attempt == max_attempts:
                            break
                        llm_decision = None
                        continue

                    if screening_stage:
                        llm_decision = self._sanitize_screening_decision(llm_decision, paper)
                        try:
                            validated_payload = self._validate_screening_decision(llm_decision or "")

                            contradictions = self._detect_decision_contradictions(validated_payload)
                            borderline = self._assess_borderline_decision(
                                validated_payload,
                                contradictions,
                                publication_prefilter,
                            )

                            decision_guardrails = {
                                "adjudication_triggered": bool(borderline.get("needs_adjudication")),
                                "adjudication_reasons": list(borderline.get("reasons") or []),
                                "decision_contradictions": contradictions,
                            }

                            if (
                                self.stage == "full_text"
                                and bool(borderline.get("needs_adjudication"))
                                and attempt < max_attempts
                            ):
                                attempt_context = self._build_adjudication_context(
                                    llm_input,
                                    validated_payload,
                                    list(borderline.get("reasons") or []),
                                    publication_prefilter,
                                )
                                llm_decision_incomplete = True
                                llm_decision = None
                                continue

                            if self.stage == "full_text" and bool(borderline.get("needs_adjudication")):
                                raise ValueError(
                                    "Full-text decision remained contradictory/borderline after adjudication: "
                                    + ", ".join(list(borderline.get("reasons") or []))
                                )

                            llm_decision = json.dumps(validated_payload, ensure_ascii=False)
                            llm_decision_incomplete = False
                            failure_type = None
                            failure_reason = None
                            failure_attempt = attempt
                            break
                        except Exception as exc:
                            llm_decision_incomplete = True
                            failure_reason = f"Decision validation failed: {exc}"
                            failure_type = "llm_validation_error"
                            failure_attempt = attempt
                            if attempt == max_attempts:
                                break
                            llm_decision = None
                            continue

                    if _needs_retry(llm_decision):
                        llm_decision_incomplete = True
                        if attempt < max_attempts:
                            if not self.quiet:
                                print(
                                    f"[warn] async chat attempt {attempt}/{max_attempts} incomplete; retrying for paper {paper.paper_id}"
                                )
                            llm_decision = None
                            continue
                        near_token_limit = bool(response_tokens and response_tokens >= int(0.9 * llm_max_tokens))
                        if near_token_limit:
                            failure_reason = (
                                f"LLM decision likely truncated by max_tokens={llm_max_tokens}; reduce prompt payload or increase max_tokens."
                            )
                            failure_type = "llm_output_token_limit"
                        else:
                            failure_reason = "LLM decision incomplete after retry; output may be truncated."
                            failure_type = "llm_incomplete"
                        failure_attempt = attempt
                        print(
                            f"[error] async chat attempt {attempt}/{max_attempts} failed for model='{model_name}': output incomplete; logged for re-screen",
                            file=sys.stderr,
                        )
                    else:
                        llm_decision_incomplete = False
                        failure_type = None
                        failure_reason = None
                        failure_attempt = attempt
                        break

                if failure_type is None and llm_decision and response_tokens == 0:
                    response_tokens = len((llm_decision or "").split())
                if failure_type is None and llm_decision and prompt_tokens == 0:
                    prompt_tokens = len((llm_input or "").split())

        if llm_decision and response_tokens == 0:
            response_tokens = len((llm_decision or "").split())

        if self.stage in {"title_abstract", "full_text"} and not isinstance(llm_decision, str):
            llm_decision = None

        if self.stage in {"title_abstract", "full_text"} and llm_decision is not None:
            llm_decision = self._sanitize_screening_decision(llm_decision, paper)

        context_input_hash = self._sha256_text(llm_input)
        prompt_template_hash = self._sha256_text(self.prompt_template)
        full_prompt_hash = self._sha256_text(self.prompt_template.replace("{data}", llm_input or ""))

        if failure_type is None and not api_disabled and self._decision_missing_fields(llm_decision):
            llm_decision_incomplete = True
            near_token_limit = bool(response_tokens and response_tokens >= int(0.9 * llm_max_tokens))
            if near_token_limit:
                failure_reason = (
                    f"LLM decision missing required fields likely due to output truncation at max_tokens={llm_max_tokens}."
                )
                failure_type = "llm_output_token_limit"
            else:
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
        output_metadata = paper.metadata
        if self.stage in {"title_abstract", "full_text"}:
            output_metadata = self._metadata_without_authors(output_metadata)

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
                "llm_input_sha256": context_input_hash,
                "prompt_template_sha256": prompt_template_hash,
                "full_prompt_sha256": full_prompt_hash,
                "prompt_campaign_id": self._prompt_campaign_id,
                "prompt_template_snapshot_path": str(self._prompt_snapshot_path) if self._prompt_snapshot_path else None,
                "llm_seed": llm_seed,
                "llm_top_p": llm_top_p,
                "raw_chunk_count": raw_chunk_count,
                "dropped_low_quality_chunks": dropped_low_quality_chunks,
                "selected_score_stats": selected_score_stats,
                "selected_page_coverage": selected_page_coverage,
                "selection_trace": selection_trace,
                "publication_type_prefilter": publication_prefilter,
                "decision_guardrails": decision_guardrails,
            },
            "metadata": output_metadata,
        }

        if self.stage == "data_extraction":
            extraction_payload = await asyncio.to_thread(self._build_extraction_payload, paper, llm_decision)
            await asyncio.to_thread(self._write_data_extraction_metadata, paper, selected, llm_decision, extraction_payload)

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

    def _process_paper(self, paper: PaperRecord) -> tuple[dict, dict, dict | None]:
        """human readable hint: sync mode reuses the async processing core to avoid duplicate decision logic."""

        return asyncio.run(self._process_paper_async(paper))

    def _format_chunks_for_prompt(self, paper: PaperRecord, chunks: list[dict]) -> str:
        """Format selected chunks into a readable prompt section."""

        if not chunks:
            return ""

        authors = self._authors_for_paper(paper) if self.stage in {"title_abstract", "full_text"} else ""

        title_text = (paper.title or "").strip()
        if self.stage in {"title_abstract", "full_text"}:
            title_text = self._strip_author_mentions(title_text, authors)

        parts: list[str] = [f"Paper ID: {paper.paper_id}", f"Title: {title_text}".strip()]
        for idx, chunk in enumerate(chunks, start=1):
            text = str(chunk.get("text", "")).strip()
            if self.stage in {"title_abstract", "full_text"}:
                text = self._strip_author_mentions(text, authors)
            if self.stage == "full_text":
                # human readable hint: final cleanup right before prompt assembly catches residual PDF artifacts.
                text = normalize_extracted_text(text)

            prefix_parts = [f"Chunk {idx}"]
            if self.stage in {"full_text", "data_extraction"}:
                section = self._infer_chunk_section_label(chunk)
                if section:
                    prefix_parts.append(f"section {section}")
            page_start = chunk.get("page_start")
            page_end = chunk.get("page_end")
            if isinstance(page_start, int) and isinstance(page_end, int):
                if page_start == page_end:
                    prefix_parts.append(f"page {page_start}")
                else:
                    prefix_parts.append(f"pages {page_start}-{page_end}")
            prefix = "[" + ", ".join(prefix_parts) + "]"
            parts.append(f"{prefix}\n{text}")
        return "\n\n".join(parts)

    def _title_abstract_full_input(self, paper: PaperRecord) -> str:
        """Build one full context block for title_abstract (no chunking/retrieval)."""

        title_text = (paper.title or "").strip()
        abstract_text = (paper.abstract or "").strip()
        authors = self._authors_for_paper(paper)
        title_text = self._strip_author_mentions(title_text, authors)
        abstract_text = self._strip_author_mentions(abstract_text, authors)
        parts = [f"Paper ID: {paper.paper_id}", f"Title: {title_text}"]
        if abstract_text:
            parts.append("Abstract:\n" + abstract_text)
        return "\n\n".join(parts)

    @staticmethod
    def _metadata_without_authors(metadata: dict) -> dict:
        """Remove author fields from screening outputs."""

        cleaned = dict(metadata or {})
        cleaned.pop("Authors", None)
        cleaned.pop("authors", None)
        cleaned.pop("Author", None)
        cleaned.pop("author", None)
        return cleaned

    @staticmethod
    def _authors_for_paper(paper: PaperRecord) -> str:
        """Get author string for redaction matching."""

        metadata = paper.metadata or {}
        return str(metadata.get("Authors") or metadata.get("authors") or "").strip()

    @staticmethod
    def _strip_author_mentions(text: str, authors: str) -> str:
        """Redact exact author names/blocks from text to avoid author-based screening."""

        value = (text or "").strip()
        author_block = (authors or "").strip()
        if not value or not author_block:
            return value

        patterns: list[str] = [author_block]
        split_candidates = re.split(r"[;\n|]", author_block)
        for candidate in split_candidates:
            c = candidate.strip()
            if c:
                patterns.append(c)

        redacted = value
        for candidate in patterns:
            escaped = re.escape(candidate)
            redacted = re.sub(escaped, " ", redacted, flags=re.IGNORECASE)

        redacted = re.sub(r"\s+", " ", redacted).strip()
        return redacted

    def _sanitize_screening_decision(self, decision: str | None, paper: PaperRecord) -> str | None:
        """Remove author mentions from LLM output for screening stages."""

        if decision is None:
            return None
        return self._strip_author_mentions(decision, self._authors_for_paper(paper))

    def _write_plain_text_summary(self, writer: TextIO, record: dict) -> None:
        """Write a simple per-paper summary for manual review text files."""

        meta = record.get("metadata", {}) or {}
        title = meta.get("Title") or meta.get("title") or ""
        abstract = meta.get("Abstract") or meta.get("abstract") or ""
        decision = record.get("llm_decision", "") or ""

        writer.write(f"Paper ID: {record.get('paper_id', '')}\n")
        if title:
            writer.write(f"Title: {title}\n")
        if abstract:
            writer.write(f"Abstract: {abstract[:1000]}\n")
        writer.write(f"LLM Decision: {decision}\n\n")

    @staticmethod
    def _estimate_text_tokens(text: str) -> int:
        """Estimate token count using a simple whitespace split heuristic."""

        if not text:
            return 0
        return len(text.split())

    @staticmethod
    def _is_low_quality_evidence_text(text: str) -> bool:
        """Detect extraction noise chunks that should not enter retrieval."""

        value = (text or "").strip()
        if not value:
            return True
        if re.fullmatch(r"(?:[\W_]|\s)+", value):
            return True
        if PaperScreeningPipeline._is_publisher_boilerplate_text(value):
            return True

        metrics = PaperScreeningPipeline._chunk_readability_metrics(value)

        if metrics["alnum_count"] < 12:
            return True
        if metrics["alnum_ratio"] < 0.45:
            return True
        if metrics["word_count"] < 5:
            return True
        if metrics["unique_alpha_word_count"] < 4:
            return True
        if metrics["single_char_token_ratio"] > 0.35:
            return True
        if metrics["avg_alpha_word_length"] < 3.0:
            return True
        if metrics["character_spaced_pattern"]:
            return True

        return False

    @staticmethod
    def _is_publisher_boilerplate_text(text: str) -> bool:
        """Detect licensing/publisher boilerplate that should never be used as evidence chunks."""

        value = (text or "").strip().lower()
        if not value:
            return False

        strong_hits = sum(1 for pattern in PUBLISHER_BOILERPLATE_STRONG_PATTERNS if pattern.search(value))
        if strong_hits >= 1:
            return True

        weak_hits = sum(1 for pattern in PUBLISHER_BOILERPLATE_WEAK_PATTERNS if pattern.search(value))
        if weak_hits >= 3:
            return True

        return False

    @staticmethod
    def _chunk_readability_metrics(text: str) -> dict[str, float | int | bool]:
        """Compute readability signals for chunk-level denoising and audit fields."""

        value = (text or "").strip()
        token_like = re.findall(r"\S+", value)
        alpha_tokens = re.findall(r"[A-Za-z]+", value)
        alpha_tokens_ge2 = [w for w in alpha_tokens if len(w) >= 2]

        alnum_count = sum(1 for ch in value if ch.isalnum())
        alnum_ratio = alnum_count / max(len(value), 1)
        word_count = len(token_like)
        single_char_count = sum(1 for t in token_like if len(re.sub(r"\W+", "", t)) == 1)
        single_char_ratio = single_char_count / max(word_count, 1)
        avg_alpha_len = (
            sum(len(w) for w in alpha_tokens_ge2) / max(len(alpha_tokens_ge2), 1)
            if alpha_tokens_ge2
            else 0.0
        )
        unique_alpha_word_count = len(set(w.lower() for w in alpha_tokens_ge2))
        spaced_pattern = bool(re.search(r"(?:\b[A-Za-z]\b[\s\W]*){10,}", value))

        readability = (
            0.40 * min(1.0, alnum_ratio)
            + 0.30 * min(1.0, avg_alpha_len / 5.0)
            + 0.20 * (1.0 - min(1.0, single_char_ratio))
            + 0.10 * min(1.0, unique_alpha_word_count / 20.0)
        )
        if spaced_pattern:
            readability *= 0.6

        return {
            "alnum_count": int(alnum_count),
            "alnum_ratio": float(alnum_ratio),
            "word_count": int(word_count),
            "single_char_token_ratio": float(single_char_ratio),
            "avg_alpha_word_length": float(avg_alpha_len),
            "unique_alpha_word_count": int(unique_alpha_word_count),
            "character_spaced_pattern": bool(spaced_pattern),
            "readability_score": float(readability),
        }

    def _filter_low_quality_chunks(self, chunks: list[dict]) -> tuple[list[dict], int]:
        """Drop low-information full-text chunks while always preserving title chunks."""

        filtered: list[dict] = []
        dropped = 0
        for chunk in chunks:
            kind = str(chunk.get("kind") or "")
            if kind == "title":
                filtered.append(chunk)
                continue
            if self._is_low_quality_evidence_text(str(chunk.get("text") or "")):
                dropped += 1
                continue
            filtered.append(chunk)
        return filtered, dropped

    @staticmethod
    def _merge_usage_dicts(left: dict | None, right: dict | None) -> dict | None:
        """Merge token-usage dicts returned by one or more embedding calls."""

        if not left and not right:
            return None
        merged: dict[str, float] = {}
        for payload in (left or {}, right or {}):
            for key, value in payload.items():
                merged[key] = float(merged.get(key, 0.0)) + float(value or 0.0)
        return merged

    def _select_chunks_with_rescue(self, chunks: list[dict]) -> tuple[list[dict], dict | None, dict]:
        """Select chunks with adaptive fallback to improve recall on long/noisy manuscripts."""

        selected_primary, _, usage_primary = self.selector.select(
            chunks,
            self.top_k,
            self.score_threshold,
        )

        non_title_primary = [c for c in selected_primary if c.get("kind") != "title"]
        primary_words = sum(len(str(c.get("text") or "").split()) for c in non_title_primary)
        primary_max_score = max([float(c.get("score", 0.0) or 0.0) for c in non_title_primary], default=0.0)
        primary_fragmented = sum(
            1
            for c in non_title_primary
            if self._is_low_quality_evidence_text(str(c.get("text") or ""))
        )
        primary_fragmented_share = primary_fragmented / max(len(non_title_primary), 1)
        weak_evidence = (
            len(non_title_primary) < RETRIEVAL_WEAK_MIN_NON_TITLE
            or primary_words < RETRIEVAL_WEAK_MIN_WORDS
            or primary_max_score < 0.02
            or primary_fragmented_share > RETRIEVAL_FRAGMENTED_MAX_SHARE
        )

        sources_by_chunk: dict[str, set[str]] = {}

        def _mark_sources(selected_rows: list[dict], source_name: str) -> None:
            for row in selected_rows:
                cid = str(row.get("chunk_id") or "")
                if not cid:
                    continue
                sources_by_chunk.setdefault(cid, set()).add(source_name)

        _mark_sources(selected_primary, "primary")

        def _post_selection_denoise(selected_rows: list[dict]) -> tuple[list[dict], int]:
            """Remove fragmented rows from final selected chunks unless rescue evidence is strong."""

            cleaned: list[dict] = []
            dropped = 0
            for row in selected_rows:
                if row.get("kind") == "title":
                    cleaned.append(row)
                    continue

                text = str(row.get("text") or "")
                metrics = self._chunk_readability_metrics(text)
                keep = not self._is_low_quality_evidence_text(text)

                sources = sources_by_chunk.get(str(row.get("chunk_id") or ""), set())
                if not keep and "section_rescue" in sources:
                    # Keep rescue evidence only if it remains reasonably readable and informative.
                    keep = bool(
                        metrics["readability_score"] >= 0.45
                        and metrics["word_count"] >= 20
                        and metrics["single_char_token_ratio"] <= 0.25
                    )

                if keep:
                    cleaned.append(row)
                else:
                    dropped += 1

            return cleaned, dropped

        if not weak_evidence:
            final_selected, dropped_after_merge = _post_selection_denoise([dict(item) for item in selected_primary])
            for item in final_selected:
                cid = str(item.get("chunk_id") or "")
                item["selection_sources"] = sorted(sources_by_chunk.get(cid, {"primary"}))
            trace = {
                "fallback_triggered": False,
                "primary_selected_count": len(selected_primary),
                "primary_non_title_count": len(non_title_primary),
                "primary_non_title_word_count": primary_words,
                "primary_max_score": primary_max_score,
                "primary_fragmented_share": round(float(primary_fragmented_share), 6),
                "post_selection_dropped_fragments": dropped_after_merge,
                "final_selected_count": len(final_selected),
                "effective_top_k": self.top_k,
            }
            return final_selected, usage_primary, trace

        fallback_top_k = max(int(self.top_k or 0), RETRIEVAL_FALLBACK_TOP_K)
        fallback_threshold = 0.0 if self.score_threshold is None else min(float(self.score_threshold), 0.0)
        selected_fallback, _, usage_fallback = self.selector.select(
            chunks,
            fallback_top_k,
            fallback_threshold,
        )
        _mark_sources(selected_fallback, "fallback")

        keyword_pattern = re.compile("|".join(re.escape(k) for k in SECTION_RESCUE_KEYWORDS), re.IGNORECASE)
        for row in selected_fallback:
            if row.get("kind") == "title":
                continue
            cid = str(row.get("chunk_id") or "")
            if not cid:
                continue
            section_label = self._infer_chunk_section_label(row)
            if keyword_pattern.search(str(row.get("text") or "")) or section_label in SECTION_PRIORITY:
                sources_by_chunk.setdefault(cid, set()).add("section_rescue")
            if section_label:
                sources_by_chunk.setdefault(cid, set()).add(f"section:{section_label}")

        merged_map: dict[str, dict] = {}
        for row in selected_primary + selected_fallback:
            cid = str(row.get("chunk_id") or "")
            if not cid:
                continue
            existing = merged_map.get(cid)
            if existing is None or float(row.get("score", 0.0) or 0.0) > float(existing.get("score", 0.0) or 0.0):
                merged_map[cid] = dict(row)

        titles = [row for row in merged_map.values() if row.get("kind") == "title"]
        non_titles = [row for row in merged_map.values() if row.get("kind") != "title"]
        non_titles.sort(
            key=lambda item: (
                "section_rescue" not in sources_by_chunk.get(str(item.get("chunk_id") or ""), set()),
                -float(item.get("score", 0.0) or 0.0),
                item.get("page_start") or 0,
                item.get("line_start") or 0,
                item.get("chunk_id", ""),
            )
        )

        # Keep at least one strong chunk for each core paper section when available.
        section_best: dict[str, dict] = {}
        for item in non_titles:
            section_label = self._infer_chunk_section_label(item)
            if section_label not in SECTION_PRIORITY:
                continue
            if section_label in section_best:
                continue
            if self._is_low_quality_evidence_text(str(item.get("text") or "")):
                continue
            section_best[section_label] = item

        balanced_non_titles: list[dict] = []
        seen_ids: set[str] = set()
        for section_label in SECTION_PRIORITY:
            candidate = section_best.get(section_label)
            if not candidate:
                continue
            cid = str(candidate.get("chunk_id") or "")
            if not cid or cid in seen_ids:
                continue
            balanced_non_titles.append(candidate)
            seen_ids.add(cid)

        for item in non_titles:
            if len(balanced_non_titles) >= fallback_top_k:
                break
            cid = str(item.get("chunk_id") or "")
            if not cid or cid in seen_ids:
                continue
            balanced_non_titles.append(item)
            seen_ids.add(cid)

        non_titles = balanced_non_titles[:fallback_top_k]

        final_selected = titles + non_titles
        final_selected, dropped_after_merge = _post_selection_denoise(final_selected)

        # Keep at least a small non-title context to avoid over-pruning into title-only prompts.
        non_title_after = [row for row in final_selected if row.get("kind") != "title"]
        if not non_title_after:
            rescue_candidates = [row for row in non_titles if not self._is_low_quality_evidence_text(str(row.get("text") or ""))]
            final_selected.extend(rescue_candidates[: min(3, len(rescue_candidates))])
        final_selected.sort(
            key=lambda item: (
                item.get("kind") != "title",
                -float(item.get("score", 0.0) or 0.0),
                item.get("page_start") or 0,
                item.get("line_start") or 0,
                item.get("chunk_id", ""),
            )
        )

        for item in final_selected:
            cid = str(item.get("chunk_id") or "")
            item["selection_sources"] = sorted(sources_by_chunk.get(cid, {"fallback"}))

        trace = {
            "fallback_triggered": True,
            "primary_selected_count": len(selected_primary),
            "primary_non_title_count": len(non_title_primary),
            "primary_non_title_word_count": primary_words,
            "primary_max_score": primary_max_score,
            "primary_fragmented_share": round(float(primary_fragmented_share), 6),
            "fallback_selected_count": len(selected_fallback),
            "fallback_top_k": fallback_top_k,
            "fallback_threshold": fallback_threshold,
            "section_rescue_hits": sum(
                1 for row in final_selected if "section_rescue" in set(row.get("selection_sources") or [])
            ),
            "section_balance_hits": [
                section
                for section in SECTION_PRIORITY
                if any(
                    self._infer_chunk_section_label(row) == section
                    for row in final_selected
                    if row.get("kind") != "title"
                )
            ],
            "post_selection_dropped_fragments": dropped_after_merge,
            "final_selected_count": len(final_selected),
            "effective_top_k": fallback_top_k,
        }

        merged_usage = self._merge_usage_dicts(usage_primary, usage_fallback)
        return final_selected, merged_usage, trace

    @staticmethod
    def _attach_chunk_certainty_metrics(selected: list[dict]) -> tuple[list[dict], dict]:
        """Attach human-readable certainty metrics to each selected chunk and aggregate score stats."""

        if not selected:
            return [], {
                "non_title_count": 0,
                "score_min": 0.0,
                "score_max": 0.0,
                "score_mean": 0.0,
                "score_std": 0.0,
            }

        non_title = [c for c in selected if c.get("kind") != "title"]
        scores = [float(c.get("score", 0.0) or 0.0) for c in non_title]
        score_min = min(scores) if scores else 0.0
        score_max = max(scores) if scores else 0.0
        score_mean = mean(scores) if scores else 0.0
        score_std = pstdev(scores) if len(scores) > 1 else 0.0
        denom = (score_max - score_min) if score_max != score_min else 0.0

        ranked_ids: list[str] = [
            str(c.get("chunk_id") or "")
            for c in sorted(non_title, key=lambda x: -float(x.get("score", 0.0) or 0.0))
        ]
        rank_map = {cid: idx + 1 for idx, cid in enumerate(ranked_ids)}
        non_title_count = len(non_title)

        enriched: list[dict] = []
        for chunk in selected:
            item = dict(chunk)
            cid = str(item.get("chunk_id") or "")
            score = float(item.get("score", 0.0) or 0.0)
            readability = PaperScreeningPipeline._chunk_readability_metrics(str(item.get("text") or ""))
            item["relevance_score"] = score
            item["relevance_margin"] = score
            item["positive_alignment_score"] = float(item.get("pos_score", 0.0) or 0.0)
            item["negative_alignment_score"] = float(item.get("neg_score", 0.0) or 0.0)
            item["readability_score"] = round(float(readability["readability_score"]), 4)
            item["single_char_token_ratio"] = round(float(readability["single_char_token_ratio"]), 4)
            item["avg_alpha_word_length"] = round(float(readability["avg_alpha_word_length"]), 4)

            if item.get("kind") == "title":
                # human readable hint: title chunks are forced-in context and are intentionally unscored.
                item["score"] = None
                item["pos_score"] = None
                item["neg_score"] = None
                item["relevance_score"] = None
                item["relevance_margin"] = None
                item["positive_alignment_score"] = None
                item["negative_alignment_score"] = None
                item["retrieval_rank"] = 0
                item["certainty_percentile"] = 1.0
                item["certainty_label"] = "always_included_title"
            else:
                rank = int(rank_map.get(cid, non_title_count or 1))
                if denom > 0:
                    percentile = (score - score_min) / denom
                else:
                    percentile = 1.0 if non_title_count <= 1 else 0.5
                if percentile >= 0.8:
                    certainty_label = "high"
                elif percentile >= 0.5:
                    certainty_label = "medium"
                else:
                    certainty_label = "low"
                item["retrieval_rank"] = rank
                item["certainty_percentile"] = round(float(percentile), 4)
                item["certainty_label"] = certainty_label

            enriched.append(item)

        stats = {
            "non_title_count": non_title_count,
            "score_min": round(float(score_min), 6),
            "score_max": round(float(score_max), 6),
            "score_mean": round(float(score_mean), 6),
            "score_std": round(float(score_std), 6),
        }
        return enriched, stats

    @staticmethod
    def _build_selected_coverage_metrics(selected: list[dict], page_count: int | None) -> dict:
        """Summarize page coverage of selected chunks for human traceability."""

        pages: set[int] = set()
        for chunk in selected:
            if chunk.get("kind") == "title":
                continue
            start = chunk.get("page_start")
            end = chunk.get("page_end")
            if isinstance(start, int) and isinstance(end, int):
                lo, hi = min(start, end), max(start, end)
                for page in range(lo, hi + 1):
                    if page > 0:
                        pages.add(page)

        unique_pages = len(pages)
        observed_min = min(pages) if pages else None
        observed_max = max(pages) if pages else None
        ratio = None
        if page_count and page_count > 0:
            ratio = round(unique_pages / page_count, 6)

        return {
            "pdf_page_count": page_count,
            "selected_unique_pages": unique_pages,
            "selected_page_min": observed_min,
            "selected_page_max": observed_max,
            "selected_page_coverage_ratio": ratio,
        }

    @staticmethod
    def _normalize_section_label(value: str | None) -> str | None:
        """Map section strings to canonical labels used for section-aware retrieval."""

        label = (value or "").strip().lower()
        if not label:
            return None
        if label in SECTION_PRIORITY:
            return label
        if label in {"background"}:
            return "introduction"
        if label in {"methods", "methodology", "materials and methods", "study design"}:
            return "method"
        if label in {"findings"}:
            return "results"
        if label in {"conclusions", "summary"}:
            return "conclusion"
        return None

    @staticmethod
    def _infer_chunk_section_label(chunk: dict) -> str | None:
        """Infer chunk section from explicit metadata first, then heading-like text cues."""

        explicit = PaperScreeningPipeline._normalize_section_label(str(chunk.get("section") or ""))
        if explicit:
            return explicit

        text = str(chunk.get("text") or "")
        text_prefix = " ".join(text.strip().split()[:20])
        for label, pattern in SECTION_INFERENCE_PATTERNS.items():
            if pattern.search(text_prefix):
                return label
        return None

    @staticmethod
    def _count_pdf_pages(pdf_path: Path | str) -> int:
        """Return number of pages in a PDF; fall back to 0 on failure."""

        try:
            from PyPDF2 import PdfReader

            reader = PdfReader(str(pdf_path))
            return len(reader.pages)
        except Exception:
            return 0

    def _prepare_chunks(self, paper: PaperRecord) -> Tuple[list[dict], int, int, str]:
        """Create evidence chunks, token counts, and resolved language for one paper."""

        language_setting = self.language_setting
        resolved_language = language_setting or "en"

        if self.stage in {"full_text", "data_extraction"}:
            resolved_path = self._resolve_pdf_path(paper)
            pdf_text, page_count, used_path, page_texts = self._load_pdf_text(paper, resolved_path, include_pages=True)
            if not pdf_text:
                return [], 0, 0, resolved_language
            if language_setting in {"auto_first", "auto-first"}:
                sample_text = f"{paper.title}\n{pdf_text[:4000]}" if pdf_text else f"{paper.title}\n{paper.abstract}"
                resolved_language = self._detect_language(sample_text)
            elif language_setting == "auto":
                resolved_language = "auto"
            chunks = chunk_fulltext_sentences(
                paper.paper_id,
                paper.title,
                pdf_text,
                resolved_language,
                page_texts=page_texts,
            )
            if self.stage == "full_text":
                # human readable hint: do not let authors be part of screening evidence.
                authors = self._authors_for_paper(paper)
                for chunk in chunks:
                    chunk_text = str(chunk.get("text", ""))
                    chunk["text"] = self._strip_author_mentions(chunk_text, authors)
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

        if select_only in self._stage_csv_cache:
            return list(self._stage_csv_cache[select_only])

        if select_only:
            files = sorted(self.csv_dir.glob("*_select_csv_*.csv"))
            self._stage_csv_cache[select_only] = files
            return list(files)

        patterns = STAGE_RULES.get(self.stage, {}).get("screen_patterns", [])
        if not patterns:
            files = sorted(self.csv_dir.glob("*.csv"))
            self._stage_csv_cache[select_only] = files
            return list(files)

        files: list[Path] = []
        for pattern in patterns:
            files.extend(sorted(self.csv_dir.glob(pattern)))
        if self.csv_dir.name == "retry_runs":
            resolved = [max(files, key=lambda p: p.stat().st_mtime)] if files else []
            self._stage_csv_cache[select_only] = resolved
            return list(resolved)
        resolved = sorted(files)
        self._stage_csv_cache[select_only] = resolved
        return list(resolved)

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

    def _load_pdf_text(
        self,
        paper: PaperRecord,
        resolved_path: Path | None = None,
        *,
        include_pages: bool = False,
    ) -> tuple[str, int, Path | None, list[str]]:
        """Read PDF text once (optionally page-level) and count pages; returns the path used."""

        path = resolved_path or self._resolve_pdf_path(paper)
        if not path or not path.exists():
            self._log_error(
                paper.paper_id,
                f"PDF not found or unreadable path for stage {self.stage}: {path}",
                error_type="pdf_missing",
            )
            return "", 0, None, []

        try:
            if include_pages:
                pages = read_pdf_pages(str(path))
                text = "\n".join(pages)
            else:
                pages = []
                text = read_pdf_file(str(path))

            if not text or not text.strip():
                self._log_error(
                    paper.paper_id,
                    f"PDF has no extractable text (likely scanned or empty). OCR is disabled; skipping: {path}",
                    error_type="pdf_unreadable",
                )
                return "", 0, None, []

            if include_pages and not pages:
                pages = [text]

            page_count = len(pages) if include_pages else self._count_pdf_pages(path)
            return text, page_count, path, pages
        except Exception as exc:  # pylint: disable=broad-except
            self._log_error(paper.paper_id, f"PDF read failed at {path}: {exc}", error_type="pdf_read_error")
            return "", 0, None, []

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

    def _call_llm(self, context: str) -> tuple[str | None, dict | None]:
        """Call the LLM and return both text and usage (if provided by the API)."""

        try:
            if use_api:
                responder = OpenAIResponder(
                    data=context,
                    model=self._llm_model,
                    prompt_template=self.prompt_template,
                    client=self._get_openai_client(base_url=self._llm_base_url),
                )
                return responder.generate_response()
        except Exception as exc:  # pylint: disable=broad-except
            return f"LLM error: {exc}", None
        return None, None

    async def _call_llm_async(self, context: str) -> tuple[str | None, dict | None]:
        """Call the LLM asynchronously and return text plus usage metadata."""

        try:
            if use_api:
                responder = OpenAIResponder(
                    data=context,
                    model=self._llm_model,
                    prompt_template=self.prompt_template,
                    client=self._get_async_openai_client(base_url=self._llm_base_url),
                )
                return await responder.generate_response_async(
                    max_retries=self._async_max_retries,
                    backoff_base_seconds=self._async_backoff_base,
                    backoff_max_seconds=self._async_backoff_max,
                    jitter_seconds=self._async_jitter,
                )
        except Exception as exc:  # pylint: disable=broad-except
            return f"LLM error: {exc}", None
        return None, None

    def _get_openai_client(self, base_url: str | None = None):
        """Create a configured OpenAI API client."""

        from openai import OpenAI

        if self._openai_client is not None and self._openai_client_base_url == base_url:
            return self._openai_client

        self._openai_client = OpenAI(api_key=os.environ.get("LLM_API_KEY"), base_url=base_url)
        self._openai_client_base_url = base_url
        return self._openai_client

    def _get_async_openai_client(self, base_url: str | None = None):
        """Create a configured async OpenAI API client."""

        from openai import AsyncOpenAI

        if self._async_openai_client is not None and self._async_openai_client_base_url == base_url:
            return self._async_openai_client

        self._async_openai_client = AsyncOpenAI(api_key=os.environ.get("LLM_API_KEY"), base_url=base_url)
        self._async_openai_client_base_url = base_url
        return self._async_openai_client

    def _validate_screening_decision(self, decision_text: str) -> dict[str, Any]:
        """human readable hint: validate screening JSON and enforce prompt-demanded keys for this stage."""

        model_cls = (
            TitleAbstractScreeningDecisionModel
            if self.stage == "title_abstract"
            else FullTextScreeningDecisionModel
        )
        # human readable hint: coerce frequent near-valid outputs into the expected flat schema before strict validation.
        normalized_input: Any = decision_text
        try:
            parsed_input = json.loads(decision_text)
            if isinstance(parsed_input, dict):
                normalized_input = self._coerce_screening_payload(parsed_input)
        except Exception:
            normalized_input = decision_text

        validated = (
            model_cls.model_validate(normalized_input)
            if isinstance(normalized_input, dict)
            else model_cls.model_validate_json(decision_text)
        )
        payload = validated.model_dump()

        required_fields = getattr(self, "_prompt_required_json_fields", set())
        if required_fields:
            missing_fields = sorted(field for field in required_fields if field not in payload)
            if missing_fields:
                raise ValueError(
                    "LLM decision is missing prompt-required JSON field(s): " + ", ".join(missing_fields)
                )

        return payload

    @staticmethod
    def _coerce_screening_payload(payload: dict[str, Any]) -> dict[str, Any]:
        """human readable hint: normalize known model-output shape drift while preserving strict field semantics."""

        normalized = dict(payload)

        deliberation = normalized.get("step_by_step_deliberation")
        if isinstance(deliberation, dict):
            logic_text = deliberation.get("Logic") or deliberation.get("logic")
            if isinstance(logic_text, str) and logic_text.strip():
                normalized["step_by_step_deliberation"] = logic_text.strip()
            else:
                flattened = " ".join(
                    str(value).strip()
                    for value in deliberation.values()
                    if isinstance(value, str) and value.strip()
                )
                normalized["step_by_step_deliberation"] = flattened or json.dumps(deliberation, ensure_ascii=False)
        elif deliberation is not None and not isinstance(deliberation, str):
            normalized["step_by_step_deliberation"] = str(deliberation)

        context_flag = normalized.get("not_urban_context")
        if isinstance(context_flag, str):
            lowered = context_flag.strip().lower()
            if lowered == "neutral":
                normalized["not_urban_context"] = "NEUTRAL"
            elif lowered in {"true", "false"}:
                normalized["not_urban_context"] = lowered == "true"

        return normalized

    @staticmethod
    def _extract_required_json_fields_from_prompt(prompt_template: str) -> set[str]:
        """human readable hint: detect field names declared in the prompt schema section."""

        if not prompt_template:
            return set()

        candidates = re.findall(r"[\"`']([a-zA-Z_][a-zA-Z0-9_]*)[\"`']\s*:\s*", prompt_template)
        fields = {field.strip() for field in candidates if field and field.strip()}

        # Keep only meaningful key-like tokens (ignore long prose fragments if any prompt format changes).
        return {field for field in fields if 1 <= len(field) <= 64}

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

        payload = self._decision_payload(decision)

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
    def _decision_payload(decision: object) -> object:
        """human readable hint: parse JSON text decisions once so downstream checks can reuse the payload."""

        if isinstance(decision, str):
            try:
                return json.loads(decision)
            except Exception:
                return decision
        return decision

    @staticmethod
    def _parse_exclusion_reason(decision: object) -> str | None:
        """human readable hint: derive exclusion_reason_category if present in LLM output."""

        payload = PaperScreeningPipeline._decision_payload(decision)
        if isinstance(payload, dict):
            for key in ("exclusion_reason_category", "exclusion_reason", "reason"):
                val = payload.get(key)
                if val:
                    return str(val)
        return None

    @staticmethod
    def _publication_type_prefilter(metadata: dict | None) -> dict[str, Any]:
        """human readable hint: deterministic metadata prefilter to flag likely non-empirical publication types."""

        info = metadata or {}
        fields = {
            "title": str(info.get("Title") or ""),
            "journal": str(info.get("Journal") or ""),
            "tags": str(info.get("Tags") or ""),
            "notes": str(info.get("Notes") or ""),
        }

        strong_rules = {
            "dissertation abstracts": "journal",
            "dissertation": "title_or_journal",
            "doctoral thesis": "title_or_journal",
            "master thesis": "title_or_journal",
            "white paper": "title_or_journal",
            "technical report": "title_or_journal",
        }
        weak_rules = {
            "editorial": "title_or_journal",
            "letter to the editor": "title_or_journal",
            "commentary": "title_or_journal",
            "opinion": "title_or_journal",
            "report": "title_or_journal",
        }

        title_lower = fields["title"].lower()
        journal_lower = fields["journal"].lower()
        tags_lower = fields["tags"].lower()
        notes_lower = fields["notes"].lower()

        strong_matches: list[str] = []
        weak_matches: list[str] = []
        for token in strong_rules:
            if token in title_lower or token in journal_lower or token in tags_lower or token in notes_lower:
                strong_matches.append(token)
        for token in weak_rules:
            if token in title_lower or token in journal_lower or token in tags_lower or token in notes_lower:
                weak_matches.append(token)

        return {
            "likely_non_empirical_publication": bool(strong_matches),
            "strong_matches": strong_matches,
            "weak_matches": weak_matches,
        }

    @staticmethod
    def _parse_bool_like(value: Any) -> bool | None:
        """human readable hint: parse relaxed bool-like values while preserving NEUTRAL as None."""

        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "yes", "1", "include", "eligible"}:
                return True
            if lowered in {"false", "no", "0", "exclude", "ineligible"}:
                return False
        return None

    @staticmethod
    def _collect_exclusion_flag_values(payload: dict[str, Any]) -> dict[str, bool | None]:
        """human readable hint: map exclusion flag values from a screening payload."""

        return {
            "not_adult_population": PaperScreeningPipeline._parse_bool_like(payload.get("not_adult_population")),
            "no_smartphone_technology": PaperScreeningPipeline._parse_bool_like(payload.get("no_smartphone_technology")),
            "no_artificial_intelligence": PaperScreeningPipeline._parse_bool_like(payload.get("no_artificial_intelligence")),
            "no_physical_activity": PaperScreeningPipeline._parse_bool_like(payload.get("no_physical_activity")),
            "not_urban_context": PaperScreeningPipeline._parse_bool_like(payload.get("not_urban_context")),
            "wrong_publication_type": PaperScreeningPipeline._parse_bool_like(payload.get("wrong_publication_type")),
            "insufficient_context": PaperScreeningPipeline._parse_bool_like(payload.get("insufficient_context")),
        }

    def _detect_decision_contradictions(self, payload: dict[str, Any]) -> list[str]:
        """human readable hint: identify logical contradictions in the LLM screening output."""

        contradictions: list[str] = []
        is_eligible = self._parse_is_eligible(payload)
        reason = (self._parse_exclusion_reason(payload) or "").strip()
        flag_values = self._collect_exclusion_flag_values(payload)
        active_flags = sorted([name for name, value in flag_values.items() if value is True])

        if is_eligible is False and not active_flags:
            contradictions.append("is_eligible_false_without_true_exclusion_flag")
        if is_eligible is True and active_flags:
            contradictions.append("is_eligible_true_with_true_exclusion_flag")
        if is_eligible is False and reason == "not_urban_context" and flag_values.get("not_urban_context") is not True:
            contradictions.append("context_reason_without_context_exclusion")

        return contradictions

    def _assess_borderline_decision(
        self,
        payload: dict[str, Any],
        contradictions: list[str],
        publication_prefilter: dict[str, Any],
    ) -> dict[str, Any]:
        """human readable hint: classify borderline outputs that merit one adjudication pass."""

        reasons: list[str] = []
        reason = (self._parse_exclusion_reason(payload) or "").strip()
        is_eligible = self._parse_is_eligible(payload)
        confidence_raw = payload.get("confidence_score")

        confidence = None
        if isinstance(confidence_raw, (float, int)):
            confidence = float(confidence_raw)

        if contradictions:
            reasons.extend(contradictions)

        if confidence is not None and FULLTEXT_BORDERLINE_CONFIDENCE_MIN <= confidence <= FULLTEXT_BORDERLINE_CONFIDENCE_MAX:
            reasons.append("mid_confidence_band")

        if is_eligible is False and reason == "not_urban_context":
            reasons.append("context_only_exclusion")

        if publication_prefilter.get("likely_non_empirical_publication") and is_eligible is True:
            reasons.append("publication_type_prefilter_conflict")

        deduped = sorted(set(reasons))
        return {
            "needs_adjudication": bool(deduped),
            "reasons": deduped,
            "confidence": confidence,
        }

    @staticmethod
    def _build_adjudication_context(
        base_context: str,
        current_payload: dict[str, Any],
        reasons: list[str],
        publication_prefilter: dict[str, Any],
    ) -> str:
        """human readable hint: append a deterministic adjudication note for one final consistency pass."""

        reason_text = ", ".join(reasons) if reasons else "none"
        prefilter_text = json.dumps(publication_prefilter, ensure_ascii=False)
        prior = json.dumps(current_payload, ensure_ascii=False)

        note = (
            "\n\n[ADJUDICATION NOTE]\n"
            "Your prior output was flagged as borderline or inconsistent."
            f" Reasons: {reason_text}.\n"
            f" Deterministic publication prefilter: {prefilter_text}.\n"
            "Re-evaluate using only the provided criteria."
            " Ensure is_eligible is logically consistent with exclusion flags and exclusion_reason_category."
            " If not_urban_context is NEUTRAL, do not use not_urban_context as exclusion_reason_category."
            " Return one flat JSON object only.\n"
            f"Prior output: {prior}\n"
        )
        return f"{base_context}{note}"

    def _decision_missing_fields(self, decision: object) -> bool:
        """human readable hint: detect missing justification or exclusion_reason_category without altering the decision."""

        if decision is None:
            return True

        payload = self._decision_payload(decision)
        if isinstance(payload, str):
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
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
        """Infer extraction fields from the "Fields to extract" section only."""

        criteria: list[str] = []
        seen = set()
        in_fields_section = False

        for line in prompt_text.splitlines():
            stripped = line.strip()

            lower = stripped.lower()
            if "fields to extract" in lower:
                in_fields_section = True
                continue
            if in_fields_section and (
                lower.startswith("formatting rules")
                or lower.startswith("response shape")
                or lower.startswith("evidence block")
                or lower.startswith("external eligibility criteria")
            ):
                break

            if not in_fields_section:
                continue

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
