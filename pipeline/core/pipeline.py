"""Screening and extraction pipeline.

This module materializes per-paper folders (for full text and data extraction),
selects evidence via embeddings, calls the LLM, writes JSONL/CSV outputs, and
tracks sustainability/resource usage. It retains prior behavior with clearer
structure and logging.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
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
from typing import Generator, Iterable, Tuple, TextIO, cast
from typing import Any
from statistics import mean, median, pstdev

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency
    tqdm = None

from dotenv import load_dotenv
from pydantic import ValidationError

from pipeline.integrations.embedding_utils import (
    read_pdf_file,
    read_pdf_pages,
    detect_language,
    detect_language_code,
    normalize_extracted_text,
)
from pipeline.selection.pdf_parser import (
    extract_markdown_from_pdf_with_level,
    PARSER_LEVEL_DOCLING_SUCCESS,
    PARSER_LEVEL_PYMUPDF_FALLBACK,
    PARSER_LEVEL_OCR_SUCCESS,
    PARSER_LEVEL_LOW_DENSITY,
)
from pipeline.integrations.llm_client import OpenAIResponder
from pipeline.core.extraction_schema import (
    DynamicExtractionSchema,
    domain_groups_for_schema,
    flatten_extracted_data,
    parse_and_validate,
)
from pipeline.core.prompt_context import load_stage_prompt_template
from pipeline.core.screening_schema import (
    FullTextScreeningDecisionModel,
    TitleAbstractScreeningDecisionModel,
)
from pipeline.core.metadata_aliases import (
    extract_year_from_metadata,
    metadata_aliases,
    normalize_metadata_row,
    read_metadata_value,
)
from config.user_orchestrator import (
    CURRENT_STAGE,
    EMBEDDING_SETTINGS,
    LLM_SETTINGS,
    PATH_SETTINGS,
    SCREENING_DEFAULTS,
    STUDY_TAGS_INCLUDE,
    STAGE_RULES,
    require_setting,
)
from pipeline.additions.resource_usage import ResourceUsageEngine
from pipeline.additions.export_extraction_tables import ExtractionAggregateWriter
from pipeline.selection.chunking import chunk_fulltext_sentences, chunk_paper_sentences
from pipeline.selection.prompt_signals import (
    CORE_SCREENING_SCHEMA_FIELDS,
    LEGACY_DEFAULT_EXCLUSION_KEYS,
    NEVER_MATCH_PATTERN,
    SECTION_RESCUE_KEYWORDS,
    build_monitoring_signal_config,
    build_prompt_signal_config,
    build_study_tag_field_keys,
    looks_like_exclusion_field,
    normalize_schema_key,
    select_topic_absence_reason_key,
)
from pipeline.selection.retrieval_config import (
    FULLTEXT_SENTENCE_TARGET,
    RETRIEVAL_ASSUMED_CHUNK_TOKENS,
    RETRIEVAL_CHUNK_PROMPT_OVERHEAD_TOKENS,
    RETRIEVAL_COUNTEREVIDENCE_MAX_PAIRS,
    RETRIEVAL_COUNTEREVIDENCE_MAX_SCORE,
    RETRIEVAL_COUNTEREVIDENCE_MIN_NEG_SCORE,
    RETRIEVAL_COUNTEREVIDENCE_MIN_NON_TITLE_CAP,
    RETRIEVAL_COUNTEREVIDENCE_MIN_PRIMARY_SCORE,
    RETRIEVAL_COUNTEREVIDENCE_PAGE_DISTANCE,
    RETRIEVAL_DATA_PROMPT_BUDGET_MIN_TOKENS,
    RETRIEVAL_DATA_PROMPT_BUDGET_RATIO,
    RETRIEVAL_DIVERSITY_NEAR_DUPLICATE_JACCARD,
    RETRIEVAL_DIVERSITY_PAGE_WINDOW_SIZE,
    RETRIEVAL_DIVERSITY_PAGE_WINDOW_SOFT_CAP,
    RETRIEVAL_DIVERSITY_SECTION_SOFT_CAP,
    RETRIEVAL_FALLBACK_TOP_K,
    RETRIEVAL_FRAGMENTED_MAX_SHARE,
    RETRIEVAL_MAX_NON_TITLE_CHUNKS,
    RETRIEVAL_MIN_METHOD_TARGET,
    RETRIEVAL_MIN_NON_TITLE_TARGET,
    RETRIEVAL_MIN_SENTENCE_FLOOR,
    RETRIEVAL_MONITORING_ONLY_PENALTY,
    RETRIEVAL_PRECISION_MEDIUM_NON_TITLE_CAP,
    RETRIEVAL_WEAK_MIN_NON_TITLE,
    RETRIEVAL_WEAK_MIN_WORDS,
)
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
llm_context_window_total_tokens = require_setting(
    LLM_SETTINGS,
    "context_window_total_tokens",
    "LLM_SETTINGS",
    int,
)

if llm_max_tokens <= 0:
    raise ValueError("LLM_SETTINGS['max_tokens'] must be a positive integer.")
if llm_context_window_total_tokens <= 0:
    raise ValueError("LLM_SETTINGS['context_window_total_tokens'] must be a positive integer.")
if llm_max_tokens >= llm_context_window_total_tokens:
    raise ValueError(
        "LLM_SETTINGS['max_tokens'] must be smaller than "
        "LLM_SETTINGS['context_window_total_tokens'] so prompt tokens can fit in context."
    )

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
CONTEXT_WINDOW = int(llm_context_window_total_tokens)
PROMPT_TOKEN_BUDGET = max(1, CONTEXT_WINDOW - int(llm_max_tokens))
TITLE_TRUNC = 50  # short folder names to avoid Windows path limits
PAPER_PDF_NAME = "paper.pdf"
SUPPORTED_FULLTEXT_LANGUAGE_CODES = {"en", "de"}
FULLTEXT_BORDERLINE_CONFIDENCE_MIN = 0.45
FULLTEXT_BORDERLINE_CONFIDENCE_MAX = 0.75
SECTION_PRIORITY = ("introduction", "method", "results", "discussion", "conclusion")
SECTION_INFERENCE_PATTERNS: dict[str, re.Pattern[str]] = {
    "introduction": re.compile(r"\b(introduction|background)\b", re.IGNORECASE),
    "method": re.compile(
        r"\b(methods?|methodology|materials?\s+and\s+methods?|study\s+design|participants?|intervention|procedure|protocol|randomi[sz]ed)\b",
        re.IGNORECASE,
    ),
    "results": re.compile(r"\b(results?|findings)\b", re.IGNORECASE),
    "discussion": re.compile(r"\bdiscussion\b", re.IGNORECASE),
    "conclusion": re.compile(r"\b(conclusion|conclusions|summary)\b", re.IGNORECASE),
    "reference": re.compile(r"\b(references?|bibliography|acknowledg(?:e)?ments?)\b", re.IGNORECASE),
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
ALWAYS_INCLUDED_CHUNK_KINDS = frozenset({"title"})
REFERENCE_HEAVY_PATTERN = re.compile(r"\b(references|bibliography|acknowledg(?:e)?ments?)\b", re.IGNORECASE)
INLINE_CITATION_PATTERN = re.compile(r"\[[0-9,\s\-]+\]|\([12][0-9]{3}\)")
MISSING_ABSTRACT_MARKERS = {"", "na", "n/a", "not available", "none", "null", "nan"}
METHOD_EVIDENCE_PATTERN = re.compile(
    r"\b(methods?|methodology|materials?\s+and\s+methods?|study\s+design|participants?|recruit(?:ed|ment)?|intervention|procedure|protocol|randomi[sz]ed|outcome\s+measure|baseline|follow[- ]?up)\b",
    re.IGNORECASE,
)
MONITORING_OR_PROTOCOL_PATTERN = re.compile(
    r"\b(monitor(?:ing)?|assessment|evaluation|feasibility|usability|acceptability|observational|classification|prediction|detection|framework|protocol|pilot)\b",
    re.IGNORECASE,
)
SUBSTANTIVE_MAIN_TEXT_PATTERN = re.compile(
    r"\b(results?|findings|analysis|effect|improv(?:ed|ement)|increase|decrease|significant|comparison|group|sample|participants?|outcome|baseline|follow[- ]?up)\b",
    re.IGNORECASE,
)
@dataclass
class PaperRecord:
    paper_id: str
    title: str
    abstract: str
    metadata: dict


CANONICAL_FIELDS = [
    "title",
    "authors",
    "abstract",
    "publication_year",
    "publication_month",
    "journal",
    "volume",
    "issue",
    "pages",
    "accession_number",
    "doi",
    "reference",
    "paper_id",
    "study_id",
    "notes",
    "tags",
]


class _SplitOnlySelectionEngineStub:
    """Guard object used when split_only prep mode intentionally skips embedding setup."""

    def select(
        self,
        chunks: list[dict],
        top_k: int | None,
        score_threshold: float | None = None,
    ) -> tuple[list[dict], list[float], dict | None]:
        raise RuntimeError("Selection engine is unavailable in split_only preparation mode.")


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
        run_id: str | None = None,
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
        artifact_mode: str | None = None,
        use_advanced_pdf_parser: bool | None = None,
        input_files: list[str | Path] | None = None,
    ) -> None:
        """
        Initialize the screening/extraction pipeline with configuration.
        All arguments are strictly typed and have clear defaults for robust, reproducible runs.
        Non-coders: Each parameter controls a key aspect of the workflow (see README for details).
        """

        self.csv_dir = Path(csv_dir)
        self.input_files = [Path(path) for path in (input_files or [])]
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
        mode_raw = str(artifact_mode if artifact_mode is not None else SCREENING_DEFAULTS.get("artifact_mode", "full"))
        mode_normalized = mode_raw.strip().lower()
        self.artifact_mode = mode_normalized if mode_normalized in {"full", "compact"} else "full"
        self.compact_keep_legacy_selected_chunks = bool(
            SCREENING_DEFAULTS.get("compact_keep_legacy_selected_chunks", False)
        )
        if use_advanced_pdf_parser is None:
            self.use_advanced_pdf_parser = str(os.getenv("USE_ADVANCED_PDF_PARSER", "0")).strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        else:
            self.use_advanced_pdf_parser = bool(use_advanced_pdf_parser)
        preparse_default = bool(SCREENING_DEFAULTS.get("fulltext_preparse_before_screening", True))
        preparse_env = os.getenv("FULLTEXT_PREPARSE_BEFORE_SCREENING")
        if preparse_env is None:
            preparse_enabled = preparse_default
        else:
            preparse_enabled = preparse_env.strip().lower() in {"1", "true", "yes", "on"}
        self._fulltext_preparse_enabled = self.stage == "full_text" and preparse_enabled

        preparse_log_default = bool(SCREENING_DEFAULTS.get("fulltext_preparse_log_each_paper", True))
        preparse_log_env = os.getenv("FULLTEXT_PREPARSE_LOG_EACH_PAPER")
        if preparse_log_env is None:
            self._fulltext_preparse_log_each_paper = preparse_log_default
        else:
            self._fulltext_preparse_log_each_paper = preparse_log_env.strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        self.language_setting = str(EMBEDDING_SETTINGS.get("data_language", "en")) or "en"
        self._detect_language = detect_language
        self._detect_language_code = detect_language_code
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
        self._always_include_kinds = tuple(sorted(ALWAYS_INCLUDED_CHUNK_KINDS))
        self._validation_max_retries = 3
        # human readable hint: tracking response times to surface p50/p95 for operators.
        self._paper_times: list[float] = []
        # human readable hint: keep paper_ids that hit an error so outputs can be flagged inline.
        self._error_ids: set[str] = set()

        self._row_counter = 0
        self._paper_folders: list[Path] = []
        self._qc_sample_ids: set[str] = set()
        self._stage_csv_cache: dict[bool, list[Path]] = {}
        self._extraction_schema: DynamicExtractionSchema | None = None
        self._extraction_aggregate_writer: ExtractionAggregateWriter | None = None
        self._base_prompt_template = load_stage_prompt_template(self.stage)
        self.prompt_template = self._base_prompt_template
        if self.stage == "data_extraction":
            # human readable hint: data extraction fields, JSON schema, and prompt instructions come from the CSV KB.
            self._extraction_schema = DynamicExtractionSchema.from_kb()
            # human readable hint: when extraction runs domain-by-domain, the
            # saved prompt-template snapshot should stay close to the
            # scientist-edited prompt. Exact domain-specific runtime prompts
            # are assembled later and can be audited with input traces.
            if not bool(LLM_SETTINGS.get("data_extraction_split_by_domain", True)):
                self.prompt_template = self._extraction_schema.inject_into_prompt(self._base_prompt_template)
        topic_signal_config = build_prompt_signal_config(self.prompt_template)
        self._topic_signal_source = str(topic_signal_config.get("source") or "default_signals")
        self._intervention_signal_pattern = topic_signal_config["intervention_pattern"]
        self._topic_primary_signal_pattern = topic_signal_config["primary_pattern"]
        self._topic_secondary_signal_pattern = topic_signal_config["secondary_pattern"]
        self._topic_intervention_terms = tuple(topic_signal_config.get("intervention_terms") or ())
        self._topic_primary_terms = tuple(topic_signal_config.get("primary_terms") or ())
        self._topic_secondary_terms = tuple(topic_signal_config.get("secondary_terms") or ())
        self._section_rescue_keywords = tuple(topic_signal_config.get("section_rescue_keywords") or SECTION_RESCUE_KEYWORDS)
        self._monitoring_deprioritization_enabled = False
        self._monitoring_signal_source = "disabled_not_configured"
        self._monitoring_signal_pattern = NEVER_MATCH_PATTERN
        self._intervention_action_pattern = self._intervention_signal_pattern
        self._monitoring_signal_terms: tuple[str, ...] = tuple()
        self._intervention_action_terms: tuple[str, ...] = tuple()
        self._monitoring_kb_pos_count = 0
        self._monitoring_kb_neg_count = 0
        self._prompt_template_hash = self._sha256_text(self.prompt_template)
        self._prompt_campaign_id = self._prompt_template_hash[:12]
        self.run_id = str(run_id).strip() if run_id else f"{self.stage}_{self.run_label}_{self._prompt_campaign_id}"
        self._prompt_snapshot_path: Path | None = None
        self._prompt_required_json_fields = self._extract_required_json_fields_from_prompt(self.prompt_template)
        self._configure_dynamic_screening_schema()
        self._print_dynamic_schema_summary()
        self._extraction_criteria = (
            [variable.value_path for variable in self._extraction_schema.variables]
            if self._extraction_schema is not None
            else []
        )

        # Resource usage tracker logs tokens and energy for each run.
        self.resource_tracker = ResourceUsageEngine(
            resource_log_path=self.resource_log_path,
            enable_tracking=self.sustainability_tracking,
            enable_codecarbon=codecarbon_enabled if codecarbon_enabled is not None else False,
            stage=self.stage,
            qc_sample_path=self.qc_sample_path,
            run_label=self.run_label,
            run_id=self.run_id,
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

        monitoring_signal_config = build_monitoring_signal_config(
            prompt_template=self.prompt_template,
            topic_signal_config=topic_signal_config,
            kb_examples=kb_examples,
        )
        self._monitoring_deprioritization_enabled = bool(monitoring_signal_config.get("enabled"))
        self._monitoring_signal_source = str(
            monitoring_signal_config.get("source") or "disabled_not_configured"
        )
        self._monitoring_signal_pattern = monitoring_signal_config.get("monitoring_pattern") or NEVER_MATCH_PATTERN
        self._intervention_action_pattern = (
            monitoring_signal_config.get("intervention_action_pattern")
            or self._intervention_signal_pattern
        )
        self._monitoring_signal_terms = tuple(monitoring_signal_config.get("monitoring_terms") or ())
        self._intervention_action_terms = tuple(
            monitoring_signal_config.get("intervention_action_terms") or ()
        )
        self._monitoring_kb_pos_count = int(monitoring_signal_config.get("kb_pos_count") or 0)
        self._monitoring_kb_neg_count = int(monitoring_signal_config.get("kb_neg_count") or 0)

        skip_selector_init = self.split_only and self.stage in {"full_text", "data_extraction"}
        if skip_selector_init:
            self.selector = cast(SelectionEngine, _SplitOnlySelectionEngineStub())
        else:
            active_selection_engine = self.selection_engine
            if active_selection_engine is None:
                active_selection_engine = SelectionEngine(
                    examples=kb_examples,
                    batch_size=self.batch_size,
                    always_include_kinds=self._always_include_kinds,
                    embedder=self.embedder,
                )
                self.selection_engine = active_selection_engine
            self.selector = cast(SelectionEngine, active_selection_engine)

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
            # human readable hint: reuse an existing same-campaign snapshot when content is identical.
            existing_campaign_snapshots = sorted(
                self.stage_output_dir.glob(f"{self.stage}_prompt_template_*_{self._prompt_campaign_id}.txt"),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
            for candidate in existing_campaign_snapshots:
                try:
                    if candidate.read_text(encoding="utf-8") == self.prompt_template:
                        self._prompt_snapshot_path = candidate
                        return
                except Exception:
                    continue

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

        start_time = time.time()
        tracking_started = False

        def _start_tracking_if_needed() -> None:
            nonlocal tracking_started, start_time
            if tracking_started or not self.sustainability_tracking:
                return
            self.resource_tracker.start_run()
            tracking_started = True
            # human readable hint: runtime accounting begins when tracking actually starts.
            start_time = time.time()

        def _finish_tracking_if_started() -> None:
            nonlocal tracking_started
            if not tracking_started:
                return
            self._total_runtime_seconds = max(0.0, time.time() - start_time)
            self.resource_tracker.stop_run(self._total_runtime_seconds, self._paper_count)
            tracking_started = False

        class _RunTrackingGuard:
            """human readable hint: fail-safe finalizer so run tracking still closes on unexpected exceptions."""

            def __init__(self, closer) -> None:
                self._closer = closer
                self._closed = False

            def close(self) -> None:
                if self._closed:
                    return
                self._closer()
                self._closed = True

            def __del__(self) -> None:
                self.close()

        tracking_guard = _RunTrackingGuard(_finish_tracking_if_started)

        if not self.quiet:
            if self.split_only:
                print(f"[prep] Creating per-paper folders from: {self.csv_dir.resolve()}")
            else:
                print(f"[pipeline] Screening from CSV dir: {self.csv_dir.resolve()}")

        if not self.split_only:
            self._persist_prompt_template_snapshot()
            self._warn_prompt_hash_drift_in_stage_outputs()
        else:
            self._prompt_snapshot_path = None

        # human readable hint: start tracking immediately for split_only preflight and QC-enabled runs
        # so operators can see tracking from run start of the whole pipeline flow.
        # Remaining (non-QC) runs keep deferred startup to avoid empty tracking files when no papers remain.
        if self.split_only or self.qc_enabled:
            _start_tracking_if_needed()

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
                _finish_tracking_if_started()
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
                _finish_tracking_if_started()
                return False

        planned_papers = self._collect_planned_papers()
        if not planned_papers:
            if not self.quiet:
                print("[progress] No papers to process")
            _finish_tracking_if_started()
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
                    _finish_tracking_if_started()
                    return False
                total_planned = len(planned_papers)
            if not self.confirm_sampling:
                proceed = self._prompt_sampling_confirmation(created_sample)
                if not proceed:
                    if not self.quiet:
                        print(
                            "[qc] QC screening pending. Review the QC files and rerun main.py to continue."
                        )
                    _finish_tracking_if_started()
                    return False
                self.confirm_sampling = True

            # Remaining run: skip QC papers to avoid re-screening the sample twice.
            if self.confirm_sampling and not self.qc_only and self._qc_sample_ids:
                planned_papers = [p for p in planned_papers if p.paper_id not in self._qc_sample_ids]
                total_planned = len(planned_papers)
                if not planned_papers:
                    if not self.quiet:
                        print("[qc] Remaining run has no papers after removing QC sample; nothing to do.")
                    _finish_tracking_if_started()
                    return False

        # When QC is disabled for the remaining run, still skip any known QC sample IDs.
        if not self.qc_enabled and self.confirm_sampling and self._qc_sample_ids:
            planned_papers = [p for p in planned_papers if p.paper_id not in self._qc_sample_ids]
            total_planned = len(planned_papers)
            if not planned_papers:
                if not self.quiet:
                    print("[qc] Remaining run has no papers after removing QC sample; nothing to do.")
                _finish_tracking_if_started()
                return False

        # For non-QC runs, start tracking only after we confirmed there are papers to process.
        _start_tracking_if_needed()

        if self.stage == "data_extraction":
            self._start_data_extraction_aggregate_writer()

        if self._fulltext_preparse_enabled and self.stage == "full_text" and planned_papers:
            self._preparse_full_text_pdfs(planned_papers)

        progress_total = total_planned if total_planned is not None else (self.sample_size or None)
        progress = (
            tqdm(total=progress_total, desc="screening", ncols=100)
            if (tqdm and progress_total and not self.quiet)
            else None
        )

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
        sync_loop: asyncio.AbstractEventLoop | None = None
        if self.stage != "title_abstract" and not self._use_async_stage_processing():
            # human readable hint: reuse one event loop in sync mode to avoid per-paper asyncio.run overhead.
            sync_loop = asyncio.new_event_loop()

        def _process_with_timing(paper_item: PaperRecord) -> tuple[PaperRecord, dict, dict, dict | None, float]:
            """human readable hint: keep per-paper runtime while allowing optional concurrent processing."""

            paper_start_ts = time.time()
            if sync_loop is not None:
                record_obj, token_stats_obj, extraction_obj = sync_loop.run_until_complete(
                    self._process_paper_async(paper_item)
                )
            else:
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
                        "run_label": self.run_label,
                        "run_id": self.run_id,
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
                                "run_label": self.run_label,
                                "run_id": self.run_id,
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
                    "stage": self.stage,
                    "run_label": self.run_label,
                    "run_id": self.run_id,
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
                        "run_label": self.run_label,
                        "run_id": self.run_id,
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
                    "run_label",
                    "run_id",
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
            if sync_loop is not None:
                sync_loop.close()

        self._total_runtime_seconds = time.time() - start_time

        if not self.quiet and self.summary_to_console:
            print("[pipeline] screening run completed successfully")

        def _append_error_summary(total_planned: int) -> None:
            """human readable hint: append a run-level summary row into the error log."""

            if not self.error_log_path.exists() or self.error_log_path.stat().st_size == 0:
                return

            paper_error_count = len(self._error_ids)

            summary_payload = {
                "meta": "error_summary",
                "stage": self.stage,
                "run_label": self.run_label,
                "run_id": self.run_id,
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

        tracking_guard.close()

        return True

    def _iter_papers(self) -> Iterable[PaperRecord]:
        """Yield papers sequentially; sample only if requested."""

        if self.stage in {"full_text", "data_extraction"} and self._paper_folders:
            folders = self._paper_folders
            if self.sample_size is not None:
                rng = random.Random(self.sample_seed)
                folders = rng.sample(folders, min(self.sample_size, len(folders)))

            for folder in folders:
                row = self._metadata_snapshot_for_folder(folder)
                if not row:
                    continue
                title = read_metadata_value(row, "title")
                abstract = read_metadata_value(row, "abstract")
                paper_id = read_metadata_value(row, "paper_id", folder.name)
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

    @staticmethod
    def _suppress_noisy_parser_library_logs() -> None:
        """Reduce third-party parser chatter so terminal output stays focused."""

        os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
        os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
        os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

        for logger_name in (
            "RapidOCR",
            "rapidocr",
            "huggingface_hub",
            "filelock",
            "transformers",
            "docling",
            "urllib3",
        ):
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)

    def _read_parser_level_for_folder(self, folder_path: Path) -> str:
        """Read parser_level from compact artifact when available."""

        artifact_path = self._compact_artifact_path_for_folder(folder_path, stage="full_text")
        if not artifact_path.exists():
            return ""

        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return ""
            return str(payload.get("parser_level") or "").strip()
        except Exception:
            return ""

    def _preparse_full_text_pdfs(self, papers: list[PaperRecord]) -> None:
        """Parse full_text PDFs before screening to warm caches and surface parse status."""

        self._suppress_noisy_parser_library_logs()

        total = len(papers)
        ok_count = 0
        fail_count = 0
        report_rows: list[dict[str, str]] = []

        if not self.quiet:
            print(f"[preparse] Starting full_text preflight parsing for {total} paper(s)...")

        for idx, paper in enumerate(papers, start=1):
            resolved_path = self._resolve_pdf_path(paper)
            text, _page_count, used_path, _pages = self._load_pdf_text(
                paper,
                resolved_path,
                include_pages=False,
            )

            success = bool((text or "").strip())
            if success:
                ok_count += 1
            else:
                fail_count += 1

            parser_level = ""
            if success:
                if self.use_advanced_pdf_parser and self._compact_artifacts_enabled():
                    target_path = used_path or resolved_path
                    if target_path is not None:
                        parser_level = self._read_parser_level_for_folder(Path(target_path).parent)
                elif not self.use_advanced_pdf_parser:
                    parser_level = "Legacy reader"

            report_rows.append(
                {
                    "paper_id": str(paper.paper_id),
                    "status": "OK" if success else "FAIL",
                    "parser_level": parser_level,
                    "pdf_path": str((used_path or resolved_path) or ""),
                }
            )

            if not self.quiet and self._fulltext_preparse_log_each_paper:
                suffix = f" parser='{parser_level}'" if parser_level else ""
                status = "OK" if success else "FAIL"
                print(
                    f"[preparse] {idx}/{total} paper={paper.paper_id} status={status}{suffix}",
                    flush=True,
                )

        self._write_fulltext_preparse_report(report_rows, total, ok_count, fail_count)

        if not self.quiet:
            print(
                f"[preparse] Completed preflight parsing: ok={ok_count} fail={fail_count} total={total}",
                flush=True,
            )

    def _write_fulltext_preparse_report(
        self,
        rows: list[dict[str, str]],
        total: int,
        ok_count: int,
        fail_count: int,
    ) -> None:
        """Write one compact JSON report for full_text preparse outcomes."""

        def _starts_with(level: str, prefix: str) -> bool:
            return bool(level and level.startswith(prefix))

        primary_pymupdf_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_PYMUPDF_FALLBACK)
        )
        fallback_docling_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_DOCLING_SUCCESS)
        )
        fallback_ocr_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_OCR_SUCCESS)
        )
        low_density_without_ocr_count = sum(
            1
            for row in rows
            if _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_LOW_DENSITY)
        )
        low_density_trigger_count = fallback_ocr_count + low_density_without_ocr_count
        legacy_reader_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and str(row.get("parser_level") or "") == "Legacy reader"
        )
        unknown_parser_level_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and not str(row.get("parser_level") or "").strip()
        )

        parser_outcome_counts = {
            PARSER_LEVEL_PYMUPDF_FALLBACK: primary_pymupdf_count,
            PARSER_LEVEL_DOCLING_SUCCESS: fallback_docling_count,
            PARSER_LEVEL_OCR_SUCCESS: fallback_ocr_count,
            PARSER_LEVEL_LOW_DENSITY: low_density_without_ocr_count,
            "Legacy reader": legacy_reader_count,
            "Unknown parser level": unknown_parser_level_count,
        }
        parser_outcome_counts = {k: v for k, v in parser_outcome_counts.items() if v > 0}

        failures = [
            {
                "paper_id": str(row.get("paper_id") or ""),
                "pdf_path": str(row.get("pdf_path") or ""),
            }
            for row in rows
            if str(row.get("status") or "") == "FAIL"
        ]

        report_payload: dict[str, Any] = {
            "meta": "full_text_preparse_report",
            "schema_version": 2,
            "stage": self.stage,
            "run_label": self.run_label,
            "run_id": self.run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": {
                "papers": total,
                "parsed_ok": ok_count,
                "parsed_fail": fail_count,
            },
            "parser_handler_order": [
                PARSER_LEVEL_PYMUPDF_FALLBACK,
                PARSER_LEVEL_DOCLING_SUCCESS,
                PARSER_LEVEL_LOW_DENSITY,
                PARSER_LEVEL_OCR_SUCCESS,
            ],
            "parser_outcome_counts": parser_outcome_counts,
            "low_text_density_trigger_count": low_density_trigger_count,
            "failures": failures,
        }

        report_path = self.stage_output_dir / f"{self.stage}_preparse_report.json"
        try:
            self.stage_output_dir.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            if not self.quiet:
                print(f"[preparse] Report: {report_path}", flush=True)
        except Exception as exc:  # pylint: disable=broad-except
            if not self.quiet:
                print(f"[warning] Could not write preparse report: {exc}")

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

        normalized = normalize_metadata_row(row, default_id=default_id)
        year_val = extract_year_from_metadata(normalized)
        if year_val:
            normalized["publication_year"] = normalized.get("publication_year") or year_val
            normalized["year"] = normalized.get("year") or year_val
        return normalized

    def _canonicalize_row(self, row: dict) -> dict:
        """Map normalized fields into the canonical metadata schema."""

        normalized = self._normalize_row(row, default_id="")

        canonical = {key: read_metadata_value(normalized, key) for key in CANONICAL_FIELDS}
        canonical["publication_year"] = canonical.get("publication_year") or self._extract_year(normalized)
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
            total = len(planned_papers)
            completed = 0
            warn_count = 0
            last_completion_ts = time.time()
            runner_error: Exception | None = None
            next_index = 0
            index_lock = asyncio.Lock()

            async def _next_paper() -> PaperRecord | None:
                nonlocal next_index
                async with index_lock:
                    if next_index >= total:
                        return None
                    paper_item = planned_papers[next_index]
                    next_index += 1
                    return paper_item

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

            async def _worker() -> None:
                nonlocal completed, warn_count, last_completion_ts

                while True:
                    paper_item = await _next_paper()
                    if paper_item is None:
                        return

                    paper_start_ts = time.time()
                    async with request_semaphore:
                        record_obj, token_stats_obj, extraction_obj = await processor(paper_item)
                    result = (
                        paper_item,
                        record_obj,
                        token_stats_obj,
                        extraction_obj,
                        (time.time() - paper_start_ts),
                    )

                    completed += 1
                    last_completion_ts = time.time()
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

            heartbeat_task = asyncio.create_task(_heartbeat())
            worker_limit = self._async_max_concurrency
            worker_count = max(1, min(worker_limit, total if total > 0 else 1))
            # human readable hint: this semaphore is the real API pressure valve; lower it when the endpoint returns 502/proxy errors.
            request_semaphore = asyncio.Semaphore(worker_count)
            worker_tasks = [asyncio.create_task(_worker()) for _ in range(worker_count)]

            try:
                await asyncio.gather(*worker_tasks)
            except Exception as exc:  # pylint: disable=broad-except
                runner_error = exc
            finally:
                # human readable hint: cancel any pending workers explicitly so large async runs do not leave dangling tasks.
                for task in worker_tasks:
                    if not task.done():
                        task.cancel()
                if worker_tasks:
                    await asyncio.gather(*worker_tasks, return_exceptions=True)

                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

                if runner_error is not None:
                    queue.put(("error", runner_error))
                queue.put(("done", None))

        def _thread_target() -> None:
            asyncio.run(_runner())

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

        missing_abstract_reason = self._missing_title_abstract_reason(paper)
        if missing_abstract_reason and self._insufficient_context_reason_key:
            llm_decision = json.dumps(
                self._deterministic_insufficient_context_decision(paper, missing_abstract_reason),
                ensure_ascii=False,
            )
        elif not use_api:
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
                except (ValidationError, ValueError) as exc:
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
            "run_label": self.run_label,
            "run_id": self.run_id,
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
                "run_label": self.run_label,
                "run_id": self.run_id,
                "llm_seed": llm_seed,
                "llm_top_p": llm_top_p,
                "selected_score_stats": selected_score_stats,
                "selected_page_coverage": selected_coverage,
                "selection_trace": {
                    "fallback_triggered": False,
                    "effective_top_k": self.top_k,
                    "notes": (
                        f"deterministic insufficient_context: {missing_abstract_reason}"
                        if missing_abstract_reason
                        else "title_abstract uses full input block by design"
                    ),
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

    def _missing_title_abstract_reason(self, paper: PaperRecord) -> str:
        """human readable hint: detect records that cannot support title/abstract screening."""

        metadata = paper.metadata or {}
        flag = str(metadata.get("citation_ingestion_missing_abstract") or "").strip().casefold()
        if flag in {"true", "1", "yes"}:
            return "abstract_missing_in_citation_ingestion"
        abstract_text = str(paper.abstract or "").strip()
        if abstract_text.casefold() in MISSING_ABSTRACT_MARKERS:
            return "abstract_missing_or_placeholder"
        return ""

    def _deterministic_insufficient_context_decision(self, paper: PaperRecord, reason: str) -> dict[str, Any]:
        """human readable hint: exclude title/abstract records with no usable abstract without calling the LLM."""

        reason_key = self._insufficient_context_reason_key or "insufficient_context"
        payload: dict[str, Any] = {
            "step_by_step_deliberation": (
                "The record has no usable abstract text for title/abstract screening."
            ),
            "confidence_score": 1.0,
            "justification": (
                "The abstract is missing or encoded as a missing-value placeholder; "
                "title/abstract screening therefore has insufficient context."
            ),
            "exclusion_reason_category": reason_key,
            "is_eligible": False,
        }
        for key in self._active_exclusion_flag_keys:
            payload[key] = key == reason_key
        payload["deterministic_screening_reason"] = reason
        return payload

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
            "topic_signal_source": self._topic_signal_source,
            "topic_intervention_term_count": len(self._topic_intervention_terms),
            "topic_primary_term_count": len(self._topic_primary_terms),
            "topic_secondary_term_count": len(self._topic_secondary_terms),
            "topic_primary_terms_preview": list(self._topic_primary_terms[:8]),
            "topic_secondary_terms_preview": list(self._topic_secondary_terms[:8]),
            "monitoring_signal_source": self._monitoring_signal_source,
            "monitoring_deprioritization_enabled": self._monitoring_deprioritization_enabled,
            "monitoring_term_count": len(self._monitoring_signal_terms),
            "intervention_action_term_count": len(self._intervention_action_terms),
            "monitoring_kb_pos_count": self._monitoring_kb_pos_count,
            "monitoring_kb_neg_count": self._monitoring_kb_neg_count,
            "schema_exclusion_tag_count": len(self._active_exclusion_flag_keys),
            "schema_exclusion_tags_preview": list(self._active_exclusion_flag_keys[:8]),
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
            data_extraction_evidence_mode = str(
                LLM_SETTINGS.get("data_extraction_evidence_mode", "full_text") or "full_text"
            ).strip().lower()
            preselected_chunks = self._load_selected_chunks_from_input(paper)
            full_text_input = ""
            if data_extraction_evidence_mode == "full_text":
                full_text_input = self._load_data_extraction_full_text_input(paper)
            if full_text_input:
                selected = preselected_chunks
                selected, selected_score_stats = self._attach_chunk_certainty_metrics(selected)
                selected_page_coverage = self._build_selected_coverage_metrics(selected, page_count=None)
                preselected = True
                llm_input = full_text_input
                prompt_tokens = len((llm_input or "").split())
                estimated_input_tokens = prompt_tokens
                selection_trace = {
                    "fallback_triggered": False,
                    "effective_top_k": self.top_k,
                    "source": "full_normalized_text",
                    "selected_chunks_available_for_audit": len(preselected_chunks),
                    "topic_signal_source": self._topic_signal_source,
                    "topic_intervention_term_count": len(self._topic_intervention_terms),
                    "topic_primary_term_count": len(self._topic_primary_terms),
                    "topic_secondary_term_count": len(self._topic_secondary_terms),
                    "topic_primary_terms_preview": list(self._topic_primary_terms[:8]),
                    "topic_secondary_terms_preview": list(self._topic_secondary_terms[:8]),
                    "monitoring_signal_source": self._monitoring_signal_source,
                    "monitoring_deprioritization_enabled": self._monitoring_deprioritization_enabled,
                    "monitoring_term_count": len(self._monitoring_signal_terms),
                    "intervention_action_term_count": len(self._intervention_action_terms),
                    "monitoring_kb_pos_count": self._monitoring_kb_pos_count,
                    "monitoring_kb_neg_count": self._monitoring_kb_neg_count,
                    "schema_exclusion_tag_count": len(self._active_exclusion_flag_keys),
                    "schema_exclusion_tags_preview": list(self._active_exclusion_flag_keys[:8]),
                }
            elif preselected_chunks:
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
                    "topic_signal_source": self._topic_signal_source,
                    "topic_intervention_term_count": len(self._topic_intervention_terms),
                    "topic_primary_term_count": len(self._topic_primary_terms),
                    "topic_secondary_term_count": len(self._topic_secondary_terms),
                    "topic_primary_terms_preview": list(self._topic_primary_terms[:8]),
                    "topic_secondary_terms_preview": list(self._topic_secondary_terms[:8]),
                    "monitoring_signal_source": self._monitoring_signal_source,
                    "monitoring_deprioritization_enabled": self._monitoring_deprioritization_enabled,
                    "monitoring_term_count": len(self._monitoring_signal_terms),
                    "intervention_action_term_count": len(self._intervention_action_terms),
                    "monitoring_kb_pos_count": self._monitoring_kb_pos_count,
                    "monitoring_kb_neg_count": self._monitoring_kb_neg_count,
                    "schema_exclusion_tag_count": len(self._active_exclusion_flag_keys),
                    "schema_exclusion_tags_preview": list(self._active_exclusion_flag_keys[:8]),
                }

        if not preselected:
            if self.stage == "full_text":
                publication_prefilter = self._publication_type_prefilter(paper.metadata)
            chunks, pdf_text_tokens, pdf_visual_tokens, language_used = await asyncio.to_thread(
                self._prepare_chunks,
                paper,
            )

            raw_chunk_count = len(chunks)
            dropped_low_quality_candidates: list[dict] = []
            if pdf_visual_tokens:
                page_count = max(int(pdf_visual_tokens / TOKENS_PER_PAGE_IMAGE), 0)
            if self.stage in {"full_text", "data_extraction"} and chunks:
                chunks, dropped_low_quality_chunks, dropped_low_quality_candidates = self._filter_low_quality_chunks(chunks)
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
                    dropped_low_quality_candidates,
                )
                selected, selected_score_stats = self._attach_chunk_certainty_metrics(selected)
                selected_page_coverage = self._build_selected_coverage_metrics(selected, page_count=page_count)

            if self.stage == "full_text":
                normalized_language = str(language_used or "").strip().lower()
                detected_language_code: str | None = None
                if normalized_language and normalized_language not in {"english", "german"}:
                    detected_language_code = normalized_language
                selection_trace["language_gate_excluded"] = False
                selection_trace["detected_language_code"] = detected_language_code
                if detected_language_code:
                    selection_trace["detected_language_supported"] = (
                        detected_language_code in SUPPORTED_FULLTEXT_LANGUAGE_CODES
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

            prompt_language_hint: str | None = None
            if self.stage == "full_text":
                raw_hint = selection_trace.get("detected_language_code")
                if isinstance(raw_hint, str) and raw_hint.strip():
                    prompt_language_hint = raw_hint.strip().lower()
            llm_input = self._format_chunks_for_prompt(
                paper,
                selected,
                detected_language_code=prompt_language_hint,
            )
            prompt_tokens = len((llm_input or "").split())
            estimated_input_tokens = prompt_tokens + pdf_text_tokens + pdf_visual_tokens

        if llm_decision is None and not selected and not str(llm_input or "").strip():
            llm_decision = "LLM skipped: no evidence available after selection."
            self._log_error(
                paper.paper_id,
                "LLM skipped: empty evidence set (title/abstract or PDF missing).",
                error_type="no_evidence",
                total_estimated_tokens=estimated_input_tokens,
            )

        if llm_decision is None:
            estimated_total_tokens = int(prompt_tokens) + int(llm_max_tokens)
            if estimated_total_tokens > CONTEXT_WINDOW:
                llm_decision = (
                    "LLM skipped: prompt and output token budget exceed context window "
                    f"({prompt_tokens} + {llm_max_tokens} = {estimated_total_tokens} > {CONTEXT_WINDOW})."
                )
                self._log_overflow(paper.paper_id, estimated_total_tokens)
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
                    domain_errors: dict[str, str] = {}
                    if self.stage == "data_extraction" and bool(LLM_SETTINGS.get("data_extraction_split_by_domain", True)):
                        current_decision, llm_usage, domain_errors = await self._call_data_extraction_domains_async(
                            attempt_context,
                            paper.paper_id,
                        )
                    else:
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

                    if domain_errors:
                        llm_decision_incomplete = True
                        failure_reason = (
                            "Data extraction failed for domain(s): "
                            + ", ".join(f"{domain}={error}" for domain, error in domain_errors.items())
                        )
                        failure_type = "data_extraction_domain_validation_failed"
                        failure_attempt = attempt
                        if attempt < max_attempts:
                            if not self.quiet:
                                print(
                                    f"[warn] async data extraction domain validation failed on attempt {attempt}/{max_attempts}; retrying paper {paper.paper_id}"
                                )
                            llm_decision = None
                            continue
                        llm_decision = current_decision
                        break

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
                                selected,
                                selection_trace,
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
                                # human readable hint: after the final adjudication pass, keep a valid payload but tag it for audit.
                                decision_guardrails["adjudication_resolution"] = "borderline_after_final_adjudication_pass"

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

        if (
            self.stage in {"title_abstract", "full_text"}
            and failure_type is None
            and not api_disabled
            and self._decision_missing_fields(llm_decision)
        ):
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
            "run_label": self.run_label,
            "run_id": self.run_id,
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
                "run_label": self.run_label,
                "run_id": self.run_id,
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

    def _format_chunks_for_prompt(
        self,
        paper: PaperRecord,
        chunks: list[dict],
        detected_language_code: str | None = None,
    ) -> str:
        """Format selected chunks into a readable prompt section."""

        if not chunks:
            return ""

        authors = self._authors_for_paper(paper) if self.stage in {"title_abstract", "full_text"} else ""

        title_text = (paper.title or "").strip()
        if self.stage in {"title_abstract", "full_text"}:
            title_text = self._strip_author_mentions(title_text, authors)

        parts: list[str] = [f"Paper ID: {paper.paper_id}", f"Title: {title_text}".strip()]
        if self.stage == "full_text" and detected_language_code:
            # human readable hint: expose language detector output to the LLM as context instead of hard gating.
            parts.append(f"Detected full-text language code (auto): {detected_language_code}")

        prompt_chunks = [
            chunk for chunk in chunks if str(chunk.get("kind") or "") != "title"
        ]

        for idx, chunk in enumerate(prompt_chunks, start=1):
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
            sentence_count = chunk.get("sentence_count")
            if isinstance(sentence_count, int) and sentence_count > 0:
                prefix_parts.append(f"sentences {sentence_count}")
            prefix = "[" + ", ".join(prefix_parts) + "]"
            parts.append(f"{prefix}\n{text}")
        return "\n\n".join(parts)

    def _load_data_extraction_full_text_input(self, paper: PaperRecord) -> str:
        """human readable hint: use the cached normalized full text as extraction evidence when available."""

        folder = paper.metadata.get("folder_path")
        if not folder:
            return ""

        folder_path = Path(folder)
        candidates = [
            folder_path / "full_text_normalized.txt",
            folder_path / "data_extraction_normalized.txt",
        ]
        raw_text = ""
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                raw_text = candidate.read_text(encoding="utf-8")
                break
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    paper.paper_id,
                    f"failed to read normalized full text for extraction: {exc}",
                    error_type="data_extraction_full_text_read_failed",
                )
                return ""

        if not raw_text:
            return ""

        marker = "=== normalized_full_text ==="
        marker_index = raw_text.find(marker)
        if marker_index >= 0:
            raw_text = raw_text[marker_index + len(marker):]
        normalized_text = normalize_extracted_text(raw_text).strip()
        if not normalized_text:
            return ""

        max_words = int(LLM_SETTINGS.get("data_extraction_full_text_max_words", 0) or 0)
        if max_words > 0:
            words = normalized_text.split()
            if len(words) > max_words:
                normalized_text = " ".join(words[:max_words])

        title_text = (paper.title or "").strip()
        parts = [f"Paper ID: {paper.paper_id}"]
        if title_text:
            parts.append(f"Title: {title_text}")
        parts.append("[Full Normalized Text]\n" + normalized_text)
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
        # human readable hint: Author column names differ between exports; the
        # user-editable alias list tells the pipeline which metadata keys should
        # be hidden from reviewer-facing screening outputs.
        for alias in metadata_aliases("authors"):
            cleaned.pop(alias, None)
        return cleaned

    @staticmethod
    def _authors_for_paper(paper: PaperRecord) -> str:
        """Get author string for redaction matching."""

        metadata = paper.metadata or {}
        return read_metadata_value(metadata, "authors")

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
        title = read_metadata_value(meta, "title")
        abstract = read_metadata_value(meta, "abstract")
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
        if PaperScreeningPipeline._is_reference_dominant_text(value):
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

    @staticmethod
    def _has_substantive_main_text(text: str) -> bool:
        """Detect whether citation-heavy text still contains substantive method/results narrative."""

        value = (text or "").strip()
        if not value:
            return False

        metrics = PaperScreeningPipeline._chunk_readability_metrics(value)
        word_count = int(metrics.get("word_count") or 0)
        unique_alpha_words = int(metrics.get("unique_alpha_word_count") or 0)
        readability = float(metrics.get("readability_score") or 0.0)
        if word_count < 25 or unique_alpha_words < 10 or readability < 0.40:
            return False

        citation_hits = len(INLINE_CITATION_PATTERN.findall(value))
        citation_density = citation_hits / max(word_count, 1)
        if citation_density > 0.45:
            return False

        long_alpha_tokens = len(re.findall(r"\b[A-Za-z]{5,}\b", value))
        has_substantive_signals = bool(
            METHOD_EVIDENCE_PATTERN.search(value) or SUBSTANTIVE_MAIN_TEXT_PATTERN.search(value)
        )
        return bool(has_substantive_signals or long_alpha_tokens >= 12)

    @staticmethod
    def _is_reference_dominant_text(text: str) -> bool:
        """Detect chunks dominated by references/citations instead of evidence narrative."""

        value = (text or "").strip()
        if not value:
            return False

        compact = " ".join(value.split())
        lower = compact.lower()
        head = " ".join(compact.split()[:35])
        has_substantive_main_text = PaperScreeningPipeline._has_substantive_main_text(compact)
        if REFERENCE_HEAVY_PATTERN.search(head) and not has_substantive_main_text:
            return True

        citation_hits = len(INLINE_CITATION_PATTERN.findall(compact))
        doi_hits = lower.count("doi")
        et_al_hits = lower.count("et al")
        pmid_hits = lower.count("pmid")
        year_hits = len(re.findall(r"\b(?:19|20)\d{2}\b", compact))
        reference_entry_hits = len(
            re.findall(r"\b[A-Z][a-z]+\s*(?:et\s+al\.?|,\s*[A-Z]\.)\s*\(?[12][0-9]{3}\)?", compact)
        )

        if citation_hits >= 8 and not has_substantive_main_text:
            return True
        if citation_hits >= 4 and (doi_hits + et_al_hits + pmid_hits) >= 3 and not has_substantive_main_text:
            return True
        if year_hits >= 10 and citation_hits >= 3 and not has_substantive_main_text:
            return True
        if reference_entry_hits >= 6 and not has_substantive_main_text:
            return True

        return False

    @staticmethod
    def _estimate_chunk_prompt_tokens(row: dict) -> int:
        """Estimate prompt token impact for one selected chunk row."""

        text = str(row.get("text") or "")
        word_count = int(row.get("word_count") or 0)
        if word_count <= 0:
            word_count = len(text.split())

        text_tokens = int(math.ceil(max(word_count, 0) * TOKENS_PER_WORD))
        meta_tokens = RETRIEVAL_CHUNK_PROMPT_OVERHEAD_TOKENS
        if row.get("page_start") is not None:
            meta_tokens += 4
        sentence_count = int(row.get("sentence_count") or 0)
        if sentence_count > 0:
            meta_tokens += 4

        return max(32, text_tokens + meta_tokens)

    @staticmethod
    def _is_always_included_chunk_kind(kind: str | None) -> bool:
        """Check whether a chunk kind bypasses relevance scoring and denoising."""

        return str(kind or "") in ALWAYS_INCLUDED_CHUNK_KINDS

    def _filter_low_quality_chunks(self, chunks: list[dict]) -> tuple[list[dict], int, list[dict]]:
        """Drop low-information chunks while retaining dropped rows for last-resort retrieval backfill."""

        filtered: list[dict] = []
        dropped = 0
        dropped_rows: list[dict] = []
        for chunk in chunks:
            kind = str(chunk.get("kind") or "")
            if self._is_always_included_chunk_kind(kind):
                filtered.append(chunk)
                continue
            if self._is_low_quality_evidence_text(str(chunk.get("text") or "")):
                dropped += 1
                dropped_rows.append(dict(chunk))
                continue
            filtered.append(chunk)
        return filtered, dropped, dropped_rows

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

    def _enforce_non_title_context_quota(
        self,
        selected_rows: list[dict],
        candidate_rows: list[dict],
        min_non_title: int,
    ) -> tuple[list[dict], dict[str, int]]:
        """Ensure a minimum non-title evidence quota to reduce title-only exclusions."""

        ensured: list[dict] = [dict(row) for row in selected_rows]
        seen_ids = {str(row.get("chunk_id") or "") for row in ensured if str(row.get("chunk_id") or "")}
        non_title_count = sum(
            1
            for row in ensured
            if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
        )
        forced_added = 0
        forced_low_quality = 0

        if non_title_count >= min_non_title:
            return ensured, {
                "forced_context_chunks_added": 0,
                "forced_low_quality_added": 0,
                "final_non_title_count": non_title_count,
            }

        ranked_candidates = sorted(
            [
                row
                for row in candidate_rows
                if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ],
            key=self._method_priority_sort_key,
        )

        def _append_candidates(allow_low_quality: bool) -> None:
            nonlocal non_title_count, forced_added, forced_low_quality
            for row in ranked_candidates:
                if non_title_count >= min_non_title:
                    break
                cid = str(row.get("chunk_id") or "")
                if not cid or cid in seen_ids:
                    continue
                if self._is_reference_dominant_text(str(row.get("text") or "")):
                    continue
                is_low_quality = self._is_low_quality_evidence_text(str(row.get("text") or ""))
                if is_low_quality and not allow_low_quality:
                    continue
                ensured.append(dict(row))
                seen_ids.add(cid)
                non_title_count += 1
                forced_added += 1
                if is_low_quality:
                    forced_low_quality += 1

        _append_candidates(allow_low_quality=False)
        if non_title_count < min_non_title:
            _append_candidates(allow_low_quality=True)

        return ensured, {
            "forced_context_chunks_added": forced_added,
            "forced_low_quality_added": forced_low_quality,
            "final_non_title_count": non_title_count,
        }

    def _method_signal_flags(self, text: str) -> tuple[bool, bool, bool, bool]:
        """Extract prompt-driven intervention/topic cue flags used by full-text ranking."""

        value = (text or "").strip()
        has_intervention = bool(self._intervention_signal_pattern.search(value))
        has_primary_topic = bool(self._topic_primary_signal_pattern.search(value))
        has_secondary_topic = bool(self._topic_secondary_signal_pattern.search(value))
        has_triad = has_intervention and has_primary_topic and has_secondary_topic
        return has_intervention, has_primary_topic, has_secondary_topic, has_triad

    def _is_monitoring_only_text(self, text: str) -> bool:
        """Detect assessment/monitoring content that lacks explicit intervention mechanics."""

        if not self._monitoring_deprioritization_enabled:
            return False

        value = (text or "").strip()
        if not value:
            return False

        has_intervention, has_primary_topic, has_secondary_topic, _ = self._method_signal_flags(value)
        if not (has_primary_topic or has_secondary_topic):
            return False

        has_monitoring_cues = bool(self._monitoring_signal_pattern.search(value))
        if not has_monitoring_cues:
            return False

        has_intervention_action = bool(
            self._intervention_action_pattern.search(value)
            or has_intervention
            or self._intervention_signal_pattern.search(value)
        )
        return has_monitoring_cues and not has_intervention_action

    def _hybrid_chunk_score(self, row: dict) -> float:
        """human readable hint: combine embedding relevance with semantic/readability signals for robust retrieval."""

        text = str(row.get("text") or "")
        base_score = float(row.get("score", 0.0) or 0.0)
        section_label = self._infer_chunk_section_label(row)
        has_intervention, has_ai, has_pa, has_triad = self._method_signal_flags(text)
        readability = self._chunk_readability_metrics(text)
        sentence_count = int(row.get("sentence_count") or 0)

        hybrid_score = base_score
        if section_label == "method":
            hybrid_score += 0.05
        if has_triad:
            hybrid_score += 0.08
        elif has_intervention and (has_ai or has_pa):
            hybrid_score += 0.04

        hybrid_score += 0.04 * float(readability["readability_score"])

        if sentence_count > 0:
            completeness = min(float(sentence_count) / max(FULLTEXT_SENTENCE_TARGET, 1), 1.0)
            hybrid_score += 0.06 * completeness
            if sentence_count < RETRIEVAL_MIN_SENTENCE_FLOOR:
                hybrid_score -= 0.08

        digit_ratio = sum(1 for ch in text if ch.isdigit()) / max(len(text), 1)
        if digit_ratio > 0.20:
            hybrid_score -= 0.05

        citation_hits = len(INLINE_CITATION_PATTERN.findall(text))
        has_substantive_main_text = self._has_substantive_main_text(text)
        if citation_hits >= 6 and not has_substantive_main_text:
            hybrid_score -= 0.04

        if self._is_monitoring_only_text(text):
            hybrid_score -= RETRIEVAL_MONITORING_ONLY_PENALTY

        if section_label == "reference" or (
            REFERENCE_HEAVY_PATTERN.search(" ".join(text.split()[:25])) and not has_substantive_main_text
        ):
            hybrid_score -= 0.10

        if self._is_low_quality_evidence_text(text):
            hybrid_score -= 0.15

        return float(hybrid_score)

    def _method_priority_sort_key(self, row: dict) -> tuple[Any, ...]:
        """Rank rows with method evidence first in full_text, then triad-rich evidence."""

        score = float(row.get("score", 0.0) or 0.0)
        page_start = row.get("page_start") or 0
        line_start = row.get("line_start") or 0
        chunk_id = str(row.get("chunk_id") or "")

        if self.stage != "full_text":
            return (True, True, True, -score, page_start, line_start, chunk_id)

        section_label = self._infer_chunk_section_label(row)
        text = str(row.get("text") or "")
        is_reference_dominant = bool(
            section_label == "reference" or self._is_reference_dominant_text(text)
        )
        is_monitoring_only = self._is_monitoring_only_text(text)
        has_method_evidence = bool(section_label == "method" or METHOD_EVIDENCE_PATTERN.search(text))
        has_intervention, has_ai, has_pa, has_triad = self._method_signal_flags(text)
        has_dual = has_intervention and (has_ai or has_pa)
        hybrid_score = self._hybrid_chunk_score(row)

        return (
            is_reference_dominant,
            is_monitoring_only,
            not has_method_evidence,
            section_label != "method",
            not has_triad,
            not has_dual,
            -hybrid_score,
            -score,
            page_start,
            line_start,
            chunk_id,
        )

    def _is_method_evidence_chunk(self, row: dict) -> bool:
        """Return true when a chunk likely contains method-level evidence."""

        if self._is_always_included_chunk_kind(str(row.get("kind") or "")):
            return False

        section_label = self._infer_chunk_section_label(row)
        if section_label == "method":
            if self._is_monitoring_only_text(str(row.get("text") or "")):
                return False
            return True

        text = str(row.get("text") or "")
        if self._is_reference_dominant_text(text):
            return False
        if self._is_monitoring_only_text(text):
            return False
        return bool(METHOD_EVIDENCE_PATTERN.search(text))

    def _enforce_method_context_quota(
        self,
        selected_rows: list[dict],
        candidate_rows: list[dict],
        min_method: int,
    ) -> tuple[list[dict], dict[str, int]]:
        """Guarantee method-section evidence when method chunks are available."""

        ensured: list[dict] = [dict(row) for row in selected_rows]
        seen_ids = {str(row.get("chunk_id") or "") for row in ensured if str(row.get("chunk_id") or "")}
        method_count = sum(
            1
            for row in ensured
            if self._is_method_evidence_chunk(row)
        )

        if min_method <= 0 or self.stage != "full_text":
            return ensured, {
                "method_chunk_available": 0,
                "method_chunks_added": 0,
                "method_low_quality_added": 0,
                "final_method_chunk_count": method_count,
            }

        method_candidates = [
            row
            for row in candidate_rows
            if self._is_method_evidence_chunk(row)
        ]
        method_available = len(
            {
                str(row.get("chunk_id") or "")
                for row in method_candidates
                if str(row.get("chunk_id") or "")
            }
        )

        if method_count >= min_method or method_available == 0:
            return ensured, {
                "method_chunk_available": method_available,
                "method_chunks_added": 0,
                "method_low_quality_added": 0,
                "final_method_chunk_count": method_count,
            }

        ranked_method_candidates = sorted(method_candidates, key=self._method_priority_sort_key)
        forced_added = 0
        forced_low_quality = 0

        def _append_candidates(allow_low_quality: bool) -> None:
            nonlocal method_count, forced_added, forced_low_quality
            for row in ranked_method_candidates:
                if method_count >= min_method:
                    break
                cid = str(row.get("chunk_id") or "")
                if not cid or cid in seen_ids:
                    continue
                if self._is_reference_dominant_text(str(row.get("text") or "")):
                    continue
                is_low_quality = self._is_low_quality_evidence_text(str(row.get("text") or ""))
                if is_low_quality and not allow_low_quality:
                    continue
                ensured.append(dict(row))
                seen_ids.add(cid)
                method_count += 1
                forced_added += 1
                if is_low_quality:
                    forced_low_quality += 1

        _append_candidates(allow_low_quality=False)
        if method_count < min_method:
            _append_candidates(allow_low_quality=True)

        return ensured, {
            "method_chunk_available": method_available,
            "method_chunks_added": forced_added,
            "method_low_quality_added": forced_low_quality,
            "final_method_chunk_count": method_count,
        }

    def _select_chunks_with_rescue(
        self,
        chunks: list[dict],
        supplemental_rows: list[dict] | None = None,
    ) -> tuple[list[dict], dict | None, dict]:
        """Select chunks with adaptive fallback and backfill-to-target evidence coverage."""

        supplemental_candidates = [
            dict(row)
            for row in (supplemental_rows or [])
            if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
        ]
        supplemental_candidate_ids = {
            str(row.get("chunk_id") or "")
            for row in supplemental_candidates
            if str(row.get("chunk_id") or "")
        }

        all_candidate_rows = [dict(row) for row in chunks]
        all_candidate_rows.extend(supplemental_candidates)

        selected_all_ranked: list[dict] = []
        scored_all_candidate_rows: list[dict] = []
        usage_all_ranked: dict | None = None
        if all_candidate_rows:
            scored_all_candidate_rows, _, usage_all_ranked = self.selector.score_chunks(all_candidate_rows)
            selected_all_ranked = self.selector.select_scored(
                scored_all_candidate_rows,
                top_k=None,
                score_threshold=None,
            )

        all_non_title_ranked = [
            dict(row)
            for row in selected_all_ranked
            if (
                not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
                and not self._is_reference_dominant_text(str(row.get("text") or ""))
            )
        ]
        all_non_title_ranked.sort(key=self._method_priority_sort_key)

        available_non_title_count = len(
            {
                str(row.get("chunk_id") or "")
                for row in all_non_title_ranked
                if str(row.get("chunk_id") or "")
            }
        )
        data_prompt_budget_tokens = min(
            PROMPT_TOKEN_BUDGET,
            max(
                RETRIEVAL_DATA_PROMPT_BUDGET_MIN_TOKENS,
                int(PROMPT_TOKEN_BUDGET * RETRIEVAL_DATA_PROMPT_BUDGET_RATIO),
            ),
        )
        budget_non_title_cap = max(
            RETRIEVAL_MIN_NON_TITLE_TARGET,
            min(
                RETRIEVAL_MAX_NON_TITLE_CHUNKS,
                int(data_prompt_budget_tokens / max(RETRIEVAL_ASSUMED_CHUNK_TOKENS, 1)),
            ),
        )
        configured_top_k = None if self.top_k is None else max(int(self.top_k or 0), 0)
        if configured_top_k is None:
            target_non_title_count = min(budget_non_title_cap, available_non_title_count)
        else:
            target_non_title_count = min(
                configured_top_k,
                budget_non_title_cap,
                available_non_title_count,
            )

        original_chunk_ids = {
            str(row.get("chunk_id") or "")
            for row in chunks
            if str(row.get("chunk_id") or "")
        }
        scored_primary_pool = [
            row
            for row in scored_all_candidate_rows
            if str(row.get("chunk_id") or "") in original_chunk_ids
        ]
        selected_primary = self.selector.select_scored(
            scored_primary_pool,
            self.top_k,
            self.score_threshold,
        )
        usage_primary: dict | None = None

        primary_scored = [
            c for c in selected_primary if not self._is_always_included_chunk_kind(str(c.get("kind") or ""))
        ]
        primary_words = sum(len(str(c.get("text") or "").split()) for c in primary_scored)
        primary_max_score = max([float(c.get("score", 0.0) or 0.0) for c in primary_scored], default=0.0)
        primary_fragmented = sum(
            1
            for c in primary_scored
            if self._is_low_quality_evidence_text(str(c.get("text") or ""))
        )
        primary_fragmented_share = primary_fragmented / max(len(primary_scored), 1)
        weak_evidence = (
            len(primary_scored) < RETRIEVAL_WEAK_MIN_NON_TITLE
            or primary_words < RETRIEVAL_WEAK_MIN_WORDS
            or primary_max_score < 0.02
            or primary_fragmented_share > RETRIEVAL_FRAGMENTED_MAX_SHARE
        )
        configured_score_threshold = float(self.score_threshold) if self.score_threshold is not None else 0.0

        if target_non_title_count <= 0:
            precision_non_title_cap = 0
        elif weak_evidence:
            precision_non_title_cap = min(target_non_title_count, RETRIEVAL_MIN_NON_TITLE_TARGET)
        else:
            strong_threshold = max(configured_score_threshold + 0.02, 0.04)
            medium_threshold = max(configured_score_threshold + 0.01, 0.02)
            if primary_max_score >= strong_threshold and len(primary_scored) >= max(RETRIEVAL_WEAK_MIN_NON_TITLE, 4):
                precision_non_title_cap = target_non_title_count
            elif primary_max_score >= medium_threshold and len(primary_scored) >= RETRIEVAL_MIN_NON_TITLE_TARGET:
                precision_non_title_cap = min(target_non_title_count, RETRIEVAL_PRECISION_MEDIUM_NON_TITLE_CAP)
            else:
                precision_non_title_cap = min(target_non_title_count, RETRIEVAL_MIN_NON_TITLE_TARGET)

        min_non_title_target = min(precision_non_title_cap, RETRIEVAL_MIN_NON_TITLE_TARGET)
        counterevidence_enabled = bool(
            self.stage == "full_text"
            and not weak_evidence
            and precision_non_title_cap >= RETRIEVAL_COUNTEREVIDENCE_MIN_NON_TITLE_CAP
            and primary_max_score >= max(configured_score_threshold + 0.01, RETRIEVAL_COUNTEREVIDENCE_MIN_PRIMARY_SCORE)
        )

        sources_by_chunk: dict[str, set[str]] = {}

        def _mark_sources(selected_rows: list[dict], source_name: str) -> None:
            for row in selected_rows:
                cid = str(row.get("chunk_id") or "")
                if not cid:
                    continue
                sources_by_chunk.setdefault(cid, set()).add(source_name)

        def _non_title_rows(rows: list[dict]) -> list[dict]:
            return [
                row
                for row in rows
                if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ]

        def _non_title_id_set(rows: list[dict]) -> set[str]:
            return {
                str(row.get("chunk_id") or "")
                for row in _non_title_rows(rows)
                if str(row.get("chunk_id") or "")
            }

        signature_cache: dict[str, set[str]] = {}
        diversity_trace: dict[str, Any] = {
            "diversity_section_unique": 0,
            "diversity_page_window_unique": 0,
            "diversity_near_duplicate_skipped": 0,
            "diversity_section_softcap_skipped": 0,
            "diversity_page_softcap_skipped": 0,
            "diversity_counterevidence_required": 0,
            "diversity_counterevidence_kept": 0,
        }

        def _row_signature_tokens(row: dict) -> set[str]:
            """Build compact lexical signatures used for near-duplicate suppression."""

            cid = str(row.get("chunk_id") or "")
            cache_key = cid or hashlib.sha256(str(row.get("text") or "").encode("utf-8")).hexdigest()
            cached = signature_cache.get(cache_key)
            if cached is not None:
                return cached

            text = str(row.get("text") or "").lower()
            tokens = re.findall(r"\b[a-z]{3,}\b", text)
            stopwords = {
                "the",
                "and",
                "for",
                "with",
                "that",
                "this",
                "from",
                "were",
                "was",
                "are",
                "have",
                "has",
                "had",
                "into",
                "using",
                "used",
                "than",
                "their",
                "which",
                "when",
                "while",
                "also",
            }
            signature = {
                token
                for token in tokens
                if token not in stopwords
            }
            if len(signature) > 90:
                signature = set(sorted(signature)[:90])
            signature_cache[cache_key] = signature
            return signature

        def _jaccard_similarity(left: set[str], right: set[str]) -> float:
            if not left or not right:
                return 0.0
            union = left | right
            if not union:
                return 0.0
            return len(left & right) / len(union)

        def _row_page_window_key(row: dict) -> int | None:
            page_start = row.get("page_start")
            if not isinstance(page_start, int) or page_start <= 0:
                return None
            return (page_start - 1) // max(RETRIEVAL_DIVERSITY_PAGE_WINDOW_SIZE, 1)

        def _select_diverse_non_title(
            rows: list[dict],
            target: int,
            required_ids: set[str] | None = None,
        ) -> tuple[list[dict], dict[str, int]]:
            """Prefer section/page-diverse evidence and suppress semantic near-duplicates."""

            deduped_by_id: dict[str, dict] = {}
            for row in rows:
                cid = str(row.get("chunk_id") or "")
                if not cid:
                    continue
                current = deduped_by_id.get(cid)
                if current is None or self._method_priority_sort_key(row) < self._method_priority_sort_key(current):
                    deduped_by_id[cid] = dict(row)

            ranked = sorted(deduped_by_id.values(), key=self._method_priority_sort_key)
            if target < 0:
                target = len(ranked)
            target = min(max(target, 0), len(ranked))
            if target == 0:
                return [], {
                    "diversity_section_unique": 0,
                    "diversity_page_window_unique": 0,
                    "diversity_near_duplicate_skipped": 0,
                    "diversity_section_softcap_skipped": 0,
                    "diversity_page_softcap_skipped": 0,
                    "diversity_counterevidence_required": len(required_ids or set()),
                    "diversity_counterevidence_kept": 0,
                }

            if self.stage != "full_text":
                passthrough = [dict(row) for row in ranked[:target]]
                return passthrough, {
                    "diversity_section_unique": 0,
                    "diversity_page_window_unique": 0,
                    "diversity_near_duplicate_skipped": 0,
                    "diversity_section_softcap_skipped": 0,
                    "diversity_page_softcap_skipped": 0,
                    "diversity_counterevidence_required": len(required_ids or set()),
                    "diversity_counterevidence_kept": sum(
                        1
                        for row in passthrough
                        if str(row.get("chunk_id") or "") in (required_ids or set())
                    ),
                }

            required_lookup = set(required_ids or set())
            selected: list[dict] = []
            seen_ids: set[str] = set()
            section_counts: dict[str, int] = {}
            page_window_counts: dict[int, int] = {}
            near_duplicate_skipped = 0
            section_softcap_skipped = 0
            page_softcap_skipped = 0

            def _add_row(row: dict) -> None:
                cid = str(row.get("chunk_id") or "")
                if not cid or cid in seen_ids:
                    return
                selected.append(dict(row))
                seen_ids.add(cid)

                section_label = self._infer_chunk_section_label(row)
                if section_label in SECTION_PRIORITY:
                    section_counts[section_label] = section_counts.get(section_label, 0) + 1

                window_key = _row_page_window_key(row)
                if window_key is not None:
                    page_window_counts[window_key] = page_window_counts.get(window_key, 0) + 1

            def _is_near_duplicate(candidate: dict) -> bool:
                candidate_sig = _row_signature_tokens(candidate)
                if not candidate_sig:
                    return False
                for existing in selected:
                    if _jaccard_similarity(candidate_sig, _row_signature_tokens(existing)) >= RETRIEVAL_DIVERSITY_NEAR_DUPLICATE_JACCARD:
                        return True
                return False

            for row in ranked:
                if len(selected) >= target:
                    break
                cid = str(row.get("chunk_id") or "")
                if cid in required_lookup:
                    _add_row(row)

            for section_label in SECTION_PRIORITY:
                if len(selected) >= target:
                    break
                for row in ranked:
                    if len(selected) >= target:
                        break
                    cid = str(row.get("chunk_id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    if self._infer_chunk_section_label(row) != section_label:
                        continue
                    if self._is_reference_dominant_text(str(row.get("text") or "")):
                        continue
                    if _is_near_duplicate(row):
                        near_duplicate_skipped += 1
                        continue
                    _add_row(row)
                    break

            for relax_level in (0, 1, 2):
                if len(selected) >= target:
                    break
                for row in ranked:
                    if len(selected) >= target:
                        break
                    cid = str(row.get("chunk_id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    if self._is_reference_dominant_text(str(row.get("text") or "")):
                        continue

                    section_label = self._infer_chunk_section_label(row)
                    window_key = _row_page_window_key(row)

                    if relax_level == 0:
                        if section_label in SECTION_PRIORITY and section_counts.get(section_label, 0) >= RETRIEVAL_DIVERSITY_SECTION_SOFT_CAP:
                            section_softcap_skipped += 1
                            continue
                        if window_key is not None and page_window_counts.get(window_key, 0) >= RETRIEVAL_DIVERSITY_PAGE_WINDOW_SOFT_CAP:
                            page_softcap_skipped += 1
                            continue
                    elif relax_level == 1:
                        if window_key is not None and page_window_counts.get(window_key, 0) >= (RETRIEVAL_DIVERSITY_PAGE_WINDOW_SOFT_CAP + 1):
                            page_softcap_skipped += 1
                            continue

                    if relax_level <= 1 and _is_near_duplicate(row):
                        near_duplicate_skipped += 1
                        continue

                    _add_row(row)

            if len(selected) < target:
                for row in ranked:
                    if len(selected) >= target:
                        break
                    cid = str(row.get("chunk_id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    _add_row(row)

            kept_required = sum(
                1
                for row in selected
                if str(row.get("chunk_id") or "") in required_lookup
            )

            return selected[:target], {
                "diversity_section_unique": len(section_counts),
                "diversity_page_window_unique": len(page_window_counts),
                "diversity_near_duplicate_skipped": near_duplicate_skipped,
                "diversity_section_softcap_skipped": section_softcap_skipped,
                "diversity_page_softcap_skipped": page_softcap_skipped,
                "diversity_counterevidence_required": len(required_lookup),
                "diversity_counterevidence_kept": kept_required,
            }

        def _inject_counterevidence_pairs(
            selected_rows: list[dict],
            candidate_rows: list[dict],
            target_non_title: int,
            enabled: bool,
        ) -> tuple[list[dict], dict[str, int]]:
            """Add nearby counterevidence chunks so the LLM sees supporting and conflicting signals."""

            ensured = [dict(row) for row in selected_rows]
            if not enabled:
                return ensured, {
                    "counterevidence_target": 0,
                    "counterevidence_added": 0,
                    "counterevidence_candidate_pool": 0,
                    "counterevidence_anchor_pool": 0,
                }

            if self.stage != "full_text":
                return ensured, {
                    "counterevidence_target": 0,
                    "counterevidence_added": 0,
                    "counterevidence_candidate_pool": 0,
                    "counterevidence_anchor_pool": 0,
                }

            if target_non_title < 4:
                return ensured, {
                    "counterevidence_target": 0,
                    "counterevidence_added": 0,
                    "counterevidence_candidate_pool": 0,
                    "counterevidence_anchor_pool": 0,
                }

            desired_pairs = min(
                RETRIEVAL_COUNTEREVIDENCE_MAX_PAIRS,
                max(1, target_non_title // 6),
            )

            seen_ids = {
                str(row.get("chunk_id") or "")
                for row in ensured
                if str(row.get("chunk_id") or "")
            }
            selected_non_title = [
                row
                for row in ensured
                if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ]
            anchor_pool = sorted(
                [
                    row
                    for row in selected_non_title
                    if float(row.get("score", 0.0) or 0.0) >= 0.04
                ],
                key=self._method_priority_sort_key,
            )
            if not anchor_pool:
                anchor_pool = sorted(selected_non_title, key=self._method_priority_sort_key)

            candidate_pool: list[dict] = []
            candidate_seen: set[str] = set()
            for row in sorted(candidate_rows, key=self._method_priority_sort_key):
                if self._is_always_included_chunk_kind(str(row.get("kind") or "")):
                    continue
                cid = str(row.get("chunk_id") or "")
                if not cid or cid in candidate_seen or cid in seen_ids:
                    continue
                if self._is_reference_dominant_text(str(row.get("text") or "")):
                    continue
                if self._is_low_quality_evidence_text(str(row.get("text") or "")):
                    continue

                neg_score = float(row.get("neg_score", 0.0) or 0.0)
                pos_score = float(row.get("pos_score", 0.0) or 0.0)
                score = float(row.get("score", 0.0) or 0.0)
                if neg_score < RETRIEVAL_COUNTEREVIDENCE_MIN_NEG_SCORE:
                    continue
                if score > RETRIEVAL_COUNTEREVIDENCE_MAX_SCORE and neg_score <= pos_score:
                    continue

                candidate_pool.append(dict(row))
                candidate_seen.add(cid)

            added = 0
            used_candidates: set[str] = set()
            for anchor in anchor_pool:
                if added >= desired_pairs:
                    break

                anchor_id = str(anchor.get("chunk_id") or "")
                anchor_section = self._infer_chunk_section_label(anchor)
                anchor_page = anchor.get("page_start")
                anchor_sig = _row_signature_tokens(anchor)

                best: dict | None = None
                best_rank: tuple[Any, ...] | None = None

                for candidate in candidate_pool:
                    cid = str(candidate.get("chunk_id") or "")
                    if not cid or cid in used_candidates:
                        continue

                    cand_section = self._infer_chunk_section_label(candidate)
                    cand_page = candidate.get("page_start")
                    page_distance = (
                        abs(int(anchor_page) - int(cand_page))
                        if isinstance(anchor_page, int) and isinstance(cand_page, int)
                        else None
                    )
                    lexical_overlap = _jaccard_similarity(anchor_sig, _row_signature_tokens(candidate))

                    same_section = bool(anchor_section and cand_section == anchor_section)
                    nearby = bool(page_distance is not None and page_distance <= RETRIEVAL_COUNTEREVIDENCE_PAGE_DISTANCE)
                    if not (same_section or nearby or lexical_overlap >= 0.10):
                        continue

                    neg_score = float(candidate.get("neg_score", 0.0) or 0.0)
                    pos_score = float(candidate.get("pos_score", 0.0) or 0.0)
                    score = float(candidate.get("score", 0.0) or 0.0)
                    rank = (
                        not same_section,
                        0 if nearby else 1,
                        -(neg_score - pos_score),
                        abs(score),
                        -lexical_overlap,
                        *self._method_priority_sort_key(candidate),
                    )
                    if best_rank is None or rank < best_rank:
                        best = candidate
                        best_rank = rank

                if best is None:
                    continue

                best_id = str(best.get("chunk_id") or "")
                if not best_id:
                    continue
                ensured.append(dict(best))
                used_candidates.add(best_id)
                seen_ids.add(best_id)
                sources_by_chunk.setdefault(best_id, set()).add("counterevidence")
                if anchor_id:
                    sources_by_chunk.setdefault(anchor_id, set()).add("counterevidence_anchor")
                added += 1

            return ensured, {
                "counterevidence_target": desired_pairs,
                "counterevidence_added": added,
                "counterevidence_candidate_pool": len(candidate_pool),
                "counterevidence_anchor_pool": len(anchor_pool),
            }

        def _trim_to_non_title_target(selected_rows: list[dict], target: int) -> list[dict]:
            nonlocal diversity_trace
            always_rows = [
                dict(row)
                for row in selected_rows
                if self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ]
            non_title_selected = [
                dict(row)
                for row in selected_rows
                if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ]
            required_counterevidence_ids = {
                cid
                for cid, source_marks in sources_by_chunk.items()
                if "counterevidence" in source_marks
            }
            non_title_selected, diversity_trace = _select_diverse_non_title(
                non_title_selected,
                target,
                required_counterevidence_ids,
            )
            return always_rows + non_title_selected

        def _trim_to_context_budget(
            selected_rows: list[dict],
            target: int,
        ) -> tuple[list[dict], dict[str, int | bool]]:
            nonlocal diversity_trace
            always_rows = [
                dict(row)
                for row in selected_rows
                if self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ]
            non_title_selected = [
                dict(row)
                for row in selected_rows
                if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ]
            required_counterevidence_ids = {
                cid
                for cid, source_marks in sources_by_chunk.items()
                if "counterevidence" in source_marks
            }
            non_title_selected, diversity_trace = _select_diverse_non_title(
                non_title_selected,
                target,
                required_counterevidence_ids,
            )

            min_keep = min(RETRIEVAL_MIN_NON_TITLE_TARGET, len(non_title_selected))
            kept_non_title: list[dict] = []
            estimated_tokens = 0
            for row in non_title_selected:
                row_tokens = self._estimate_chunk_prompt_tokens(row)
                if len(kept_non_title) < min_keep or (estimated_tokens + row_tokens) <= data_prompt_budget_tokens:
                    kept_non_title.append(dict(row))
                    estimated_tokens += row_tokens

            if len(kept_non_title) < min_keep:
                kept_non_title = [dict(row) for row in non_title_selected[:min_keep]]
                estimated_tokens = sum(self._estimate_chunk_prompt_tokens(row) for row in kept_non_title)

            dropped_for_budget = max(0, len(non_title_selected) - len(kept_non_title))
            return always_rows + kept_non_title, {
                "context_window_tokens": CONTEXT_WINDOW,
                "max_output_tokens": int(llm_max_tokens),
                "prompt_token_budget_tokens": PROMPT_TOKEN_BUDGET,
                "data_prompt_budget_tokens": data_prompt_budget_tokens,
                "budget_non_title_cap": budget_non_title_cap,
                "estimated_selected_prompt_tokens": estimated_tokens,
                "context_budget_trimmed_chunks": dropped_for_budget,
                "trimmed_for_context_budget": dropped_for_budget > 0,
            }

        def _build_backfill_stats(before_rows: list[dict], after_rows: list[dict]) -> dict[str, int]:
            before_ids = _non_title_id_set(before_rows)
            after_ids = _non_title_id_set(after_rows)
            added_ids = after_ids - before_ids
            threshold_value = float(self.score_threshold) if self.score_threshold is not None else None

            rows_by_id: dict[str, dict] = {}
            for row in all_non_title_ranked + after_rows:
                cid = str(row.get("chunk_id") or "")
                if not cid:
                    continue
                rows_by_id[cid] = row

            added_low_quality = 0
            added_from_supplemental = 0
            added_below_threshold = 0
            added_reference_dominant = 0

            for cid in added_ids:
                row = rows_by_id.get(cid)
                if not row:
                    continue
                if self._is_reference_dominant_text(str(row.get("text") or "")):
                    added_reference_dominant += 1
                if self._is_low_quality_evidence_text(str(row.get("text") or "")):
                    added_low_quality += 1
                if cid in supplemental_candidate_ids:
                    added_from_supplemental += 1
                if threshold_value is not None and float(row.get("score", 0.0) or 0.0) < threshold_value:
                    added_below_threshold += 1

            return {
                "backfill_total_added": len(added_ids),
                "backfill_low_quality_added": added_low_quality,
                "backfill_reference_dominant_added": added_reference_dominant,
                "backfill_from_supplemental_pool": added_from_supplemental,
                "backfill_below_threshold_added": added_below_threshold,
            }

        _mark_sources(selected_primary, "primary")

        def _post_selection_denoise(selected_rows: list[dict]) -> tuple[list[dict], int]:
            """Remove fragmented rows from final selected chunks unless rescue evidence is strong."""

            cleaned: list[dict] = []
            dropped = 0
            for row in selected_rows:
                if self._is_always_included_chunk_kind(str(row.get("kind") or "")):
                    cleaned.append(row)
                    continue

                text = str(row.get("text") or "")
                metrics = self._chunk_readability_metrics(text)
                sentence_count = int(row.get("sentence_count") or 0)
                word_count = int(row.get("word_count") or 0)
                is_reference_dominant = self._is_reference_dominant_text(text)
                is_substantive_main_text = self._has_substantive_main_text(text)
                keep = not self._is_low_quality_evidence_text(text) and not is_reference_dominant

                if (
                    not keep
                    and is_substantive_main_text
                    and not self._is_publisher_boilerplate_text(text)
                ):
                    keep = True

                if keep and sentence_count > 0 and sentence_count < RETRIEVAL_MIN_SENTENCE_FLOOR:
                    if word_count < max(40, RETRIEVAL_MIN_SENTENCE_FLOOR * 5):
                        keep = False

                sources = sources_by_chunk.get(str(row.get("chunk_id") or ""), set())
                if not keep and "section_rescue" in sources:
                    # Keep rescue evidence only if it remains reasonably readable and informative.
                    keep = bool(
                        not is_reference_dominant
                        and metrics["readability_score"] >= 0.45
                        and metrics["word_count"] >= max(35, RETRIEVAL_MIN_SENTENCE_FLOOR * 4)
                        and metrics["single_char_token_ratio"] <= 0.25
                        and sentence_count >= max(4, RETRIEVAL_MIN_SENTENCE_FLOOR // 2)
                    )

                if keep:
                    cleaned.append(row)
                else:
                    dropped += 1

            return cleaned, dropped

        def _raw_non_title_rescue_candidates(limit: int) -> list[dict]:
            """human readable hint: safety net for readable non-title evidence from filtered chunks."""

            rescued: list[dict] = []
            seen_ids: set[str] = set()
            target = max(0, int(limit or 0))
            if target == 0:
                return rescued

            for row in sorted(chunks, key=self._method_priority_sort_key):
                if self._is_always_included_chunk_kind(str(row.get("kind") or "")):
                    continue
                cid = str(row.get("chunk_id") or "")
                if not cid or cid in seen_ids:
                    continue
                if self._is_reference_dominant_text(str(row.get("text") or "")):
                    continue
                if self._is_low_quality_evidence_text(str(row.get("text") or "")):
                    continue
                candidate = dict(row)
                candidate["score"] = float(candidate.get("score", 0.0) or 0.0)
                candidate["pos_score"] = float(candidate.get("pos_score", 0.0) or 0.0)
                candidate["neg_score"] = float(candidate.get("neg_score", 0.0) or 0.0)
                rescued.append(candidate)
                seen_ids.add(cid)
                if len(rescued) >= target:
                    break
            return rescued

        method_target = min(
            RETRIEVAL_MIN_METHOD_TARGET if self.stage == "full_text" else 0,
            precision_non_title_cap,
        )
        quota_candidates = all_non_title_ranked or sorted(
            [dict(item) for item in primary_scored],
            key=self._method_priority_sort_key,
        )

        if not weak_evidence:
            final_selected, dropped_after_merge = _post_selection_denoise([dict(item) for item in selected_primary])
            raw_non_title_rescue_added = 0
            if min_non_title_target > 0 and not _non_title_rows(final_selected):
                seen_ids = {
                    str(item.get("chunk_id") or "")
                    for item in final_selected
                    if str(item.get("chunk_id") or "")
                }
                for candidate in _raw_non_title_rescue_candidates(min_non_title_target):
                    cid = str(candidate.get("chunk_id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    final_selected.append(candidate)
                    seen_ids.add(cid)
                    sources_by_chunk.setdefault(cid, set()).add("raw_rescue")
                    raw_non_title_rescue_added += 1

            pre_backfill_selected = [dict(item) for item in final_selected]
            final_selected, method_stats = self._enforce_method_context_quota(
                final_selected,
                quota_candidates,
                method_target,
            )
            final_selected, quota_stats = self._enforce_non_title_context_quota(
                final_selected,
                quota_candidates,
                min_non_title_target,
            )
            counterevidence_candidates = all_non_title_ranked or quota_candidates
            final_selected, counterevidence_stats = _inject_counterevidence_pairs(
                final_selected,
                counterevidence_candidates,
                precision_non_title_cap,
                counterevidence_enabled,
            )
            final_selected = _trim_to_non_title_target(final_selected, precision_non_title_cap)
            final_selected, context_budget_stats = _trim_to_context_budget(
                final_selected,
                precision_non_title_cap,
            )
            backfill_stats = _build_backfill_stats(pre_backfill_selected, final_selected)

            final_selected.sort(
                key=lambda item: (
                    not self._is_always_included_chunk_kind(str(item.get("kind") or "")),
                    *self._method_priority_sort_key(item),
                )
            )
            for item in final_selected:
                cid = str(item.get("chunk_id") or "")
                item["hybrid_score"] = round(self._hybrid_chunk_score(item), 6)
                item["selection_sources"] = sorted(sources_by_chunk.get(cid, {"primary"}))
            final_sentence_counts = [
                int(row.get("sentence_count") or 0)
                for row in final_selected
                if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
            ]
            trace = {
                "fallback_triggered": False,
                "configured_top_k": configured_top_k,
                "monitoring_signal_source": self._monitoring_signal_source,
                "monitoring_deprioritization_enabled": self._monitoring_deprioritization_enabled,
                "monitoring_term_count": len(self._monitoring_signal_terms),
                "intervention_action_term_count": len(self._intervention_action_terms),
                "monitoring_kb_pos_count": self._monitoring_kb_pos_count,
                "monitoring_kb_neg_count": self._monitoring_kb_neg_count,
                "available_non_title_count": available_non_title_count,
                "target_non_title_count": target_non_title_count,
                "applied_non_title_cap": precision_non_title_cap,
                "configured_score_threshold": configured_score_threshold,
                "supplemental_low_quality_pool": len(supplemental_candidate_ids),
                "primary_selected_count": len(selected_primary),
                "primary_non_title_count": len(primary_scored),
                "primary_non_title_word_count": primary_words,
                "primary_max_score": primary_max_score,
                "primary_fragmented_share": round(float(primary_fragmented_share), 6),
                "post_selection_dropped_fragments": dropped_after_merge,
                "raw_non_title_rescue_added": raw_non_title_rescue_added,
                "min_non_title_target": min_non_title_target,
                "min_method_target": method_target,
                "target_chunk_sentence_count": FULLTEXT_SENTENCE_TARGET,
                "selected_sentence_count_min": min(final_sentence_counts) if final_sentence_counts else 0,
                "selected_sentence_count_mean": round(mean(final_sentence_counts), 4) if final_sentence_counts else 0.0,
                **method_stats,
                **quota_stats,
                **counterevidence_stats,
                **diversity_trace,
                **context_budget_stats,
                **backfill_stats,
                "final_selected_count": len(final_selected),
                "effective_top_k": configured_top_k,
            }
            merged_usage = self._merge_usage_dicts(usage_all_ranked, usage_primary)
            return final_selected, merged_usage, trace

        if configured_top_k is None:
            fallback_top_k = min(
                RETRIEVAL_FALLBACK_TOP_K,
                max(target_non_title_count, RETRIEVAL_MIN_NON_TITLE_TARGET),
            )
        else:
            fallback_top_k = max(configured_top_k, 0)
        fallback_threshold = configured_score_threshold
        selected_fallback = self.selector.select_scored(
            scored_primary_pool,
            fallback_top_k,
            fallback_threshold,
        )
        usage_fallback: dict | None = None
        _mark_sources(selected_fallback, "fallback")

        keyword_terms = self._section_rescue_keywords or SECTION_RESCUE_KEYWORDS
        keyword_pattern = re.compile("|".join(re.escape(k) for k in keyword_terms), re.IGNORECASE)
        for row in selected_fallback:
            if self._is_always_included_chunk_kind(str(row.get("kind") or "")):
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

        always_included = [
            row for row in merged_map.values() if self._is_always_included_chunk_kind(str(row.get("kind") or ""))
        ]
        non_titles = [
            row
            for row in merged_map.values()
            if (
                not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
                and not self._is_reference_dominant_text(str(row.get("text") or ""))
            )
        ]
        non_titles.sort(
            key=lambda item: (
                "section_rescue" not in sources_by_chunk.get(str(item.get("chunk_id") or ""), set()),
                *self._method_priority_sort_key(item),
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
        ranked_non_titles = [dict(item) for item in non_titles]

        final_selected = always_included + non_titles
        final_selected, dropped_after_merge = _post_selection_denoise(final_selected)
        raw_non_title_rescue_added = 0

        # Keep at least a small non-title context to avoid over-pruning into empty evidence.
        scored_after = _non_title_rows(final_selected)
        if min_non_title_target > 0 and not scored_after:
            rescue_candidates = [
                row
                for row in non_titles
                if (
                    not self._is_low_quality_evidence_text(str(row.get("text") or ""))
                    and not self._is_reference_dominant_text(str(row.get("text") or ""))
                )
            ]
            final_selected.extend(rescue_candidates[: min(min_non_title_target, len(rescue_candidates))])

            scored_after = _non_title_rows(final_selected)
            if not scored_after:
                seen_ids = {
                    str(item.get("chunk_id") or "")
                    for item in final_selected
                    if str(item.get("chunk_id") or "")
                }
                for candidate in _raw_non_title_rescue_candidates(min_non_title_target):
                    cid = str(candidate.get("chunk_id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    final_selected.append(candidate)
                    seen_ids.add(cid)
                    sources_by_chunk.setdefault(cid, set()).add("raw_rescue")
                    raw_non_title_rescue_added += 1

        pre_backfill_selected = [dict(item) for item in final_selected]
        quota_candidates = all_non_title_ranked or ranked_non_titles
        final_selected, method_stats = self._enforce_method_context_quota(
            final_selected,
            quota_candidates,
            method_target,
        )
        final_selected, quota_stats = self._enforce_non_title_context_quota(
            final_selected,
            quota_candidates,
            min_non_title_target,
        )
        counterevidence_candidates = all_non_title_ranked or quota_candidates
        final_selected, counterevidence_stats = _inject_counterevidence_pairs(
            final_selected,
            counterevidence_candidates,
            precision_non_title_cap,
            False,
        )
        final_selected = _trim_to_non_title_target(final_selected, precision_non_title_cap)
        final_selected, context_budget_stats = _trim_to_context_budget(
            final_selected,
            precision_non_title_cap,
        )
        backfill_stats = _build_backfill_stats(pre_backfill_selected, final_selected)

        final_selected.sort(
            key=lambda item: (
                not self._is_always_included_chunk_kind(str(item.get("kind") or "")),
                *self._method_priority_sort_key(item),
            )
        )

        for item in final_selected:
            cid = str(item.get("chunk_id") or "")
            item["hybrid_score"] = round(self._hybrid_chunk_score(item), 6)
            item["selection_sources"] = sorted(sources_by_chunk.get(cid, {"fallback"}))

        final_sentence_counts = [
            int(row.get("sentence_count") or 0)
            for row in final_selected
            if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
        ]

        trace = {
            "fallback_triggered": True,
            "configured_top_k": configured_top_k,
            "monitoring_signal_source": self._monitoring_signal_source,
            "monitoring_deprioritization_enabled": self._monitoring_deprioritization_enabled,
            "monitoring_term_count": len(self._monitoring_signal_terms),
            "intervention_action_term_count": len(self._intervention_action_terms),
            "monitoring_kb_pos_count": self._monitoring_kb_pos_count,
            "monitoring_kb_neg_count": self._monitoring_kb_neg_count,
            "available_non_title_count": available_non_title_count,
            "target_non_title_count": target_non_title_count,
            "applied_non_title_cap": precision_non_title_cap,
            "configured_score_threshold": configured_score_threshold,
            "supplemental_low_quality_pool": len(supplemental_candidate_ids),
            "primary_selected_count": len(selected_primary),
            "primary_non_title_count": len(primary_scored),
            "primary_non_title_word_count": primary_words,
            "primary_max_score": primary_max_score,
            "primary_fragmented_share": round(float(primary_fragmented_share), 6),
            "fallback_selected_count": len(selected_fallback),
            "fallback_top_k": fallback_top_k,
            "fallback_threshold": fallback_threshold,
            "section_rescue_hits": sum(
                1 for row in final_selected if "section_rescue" in set(row.get("selection_sources") or [])
            ),
            "raw_non_title_rescue_added": raw_non_title_rescue_added,
            "section_balance_hits": [
                section
                for section in SECTION_PRIORITY
                if any(
                    self._infer_chunk_section_label(row) == section
                    for row in final_selected
                    if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
                )
            ],
            "post_selection_dropped_fragments": dropped_after_merge,
            "min_non_title_target": min_non_title_target,
            "min_method_target": method_target,
            "target_chunk_sentence_count": FULLTEXT_SENTENCE_TARGET,
            "selected_sentence_count_min": min(final_sentence_counts) if final_sentence_counts else 0,
            "selected_sentence_count_mean": round(mean(final_sentence_counts), 4) if final_sentence_counts else 0.0,
            **method_stats,
            **quota_stats,
            **counterevidence_stats,
            **diversity_trace,
            **context_budget_stats,
            **backfill_stats,
            "final_selected_count": len(final_selected),
            "effective_top_k": configured_top_k,
        }

        merged_usage = self._merge_usage_dicts(usage_all_ranked, usage_primary)
        merged_usage = self._merge_usage_dicts(merged_usage, usage_fallback)
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

        scored_chunks = [
            c for c in selected if not PaperScreeningPipeline._is_always_included_chunk_kind(str(c.get("kind") or ""))
        ]
        scores = [float(c.get("score", 0.0) or 0.0) for c in scored_chunks]
        score_min = min(scores) if scores else 0.0
        score_max = max(scores) if scores else 0.0
        score_mean = mean(scores) if scores else 0.0
        score_std = pstdev(scores) if len(scores) > 1 else 0.0
        denom = (score_max - score_min) if score_max != score_min else 0.0

        ranked_ids: list[str] = [
            str(c.get("chunk_id") or "")
            for c in sorted(scored_chunks, key=lambda x: -float(x.get("score", 0.0) or 0.0))
        ]
        rank_map = {cid: idx + 1 for idx, cid in enumerate(ranked_ids)}
        non_title_count = len(scored_chunks)

        enriched: list[dict] = []
        for chunk in selected:
            item = dict(chunk)
            cid = str(item.get("chunk_id") or "")
            score = float(item.get("score", 0.0) or 0.0)
            readability = PaperScreeningPipeline._chunk_readability_metrics(str(item.get("text") or ""))
            sentence_count = int(item.get("sentence_count") or 0)
            word_count = int(item.get("word_count") or readability["word_count"])
            item["relevance_score"] = score
            item["relevance_margin"] = score
            item["positive_alignment_score"] = float(item.get("pos_score", 0.0) or 0.0)
            item["negative_alignment_score"] = float(item.get("neg_score", 0.0) or 0.0)
            item["hybrid_score"] = round(float(item.get("hybrid_score", score) or score), 6)
            item["sentence_count"] = sentence_count
            item["word_count"] = word_count
            item["readability_score"] = round(float(readability["readability_score"]), 4)
            item["single_char_token_ratio"] = round(float(readability["single_char_token_ratio"]), 4)
            item["avg_alpha_word_length"] = round(float(readability["avg_alpha_word_length"]), 4)

            if PaperScreeningPipeline._is_always_included_chunk_kind(str(item.get("kind") or "")):
                # human readable hint: always-included context chunks bypass relevance scoring by design.
                item["score"] = None
                item["pos_score"] = None
                item["neg_score"] = None
                item["relevance_score"] = None
                item["relevance_margin"] = None
                item["positive_alignment_score"] = None
                item["negative_alignment_score"] = None
                item["retrieval_rank"] = 0
                item["certainty_percentile"] = 1.0
                item["certainty_label"] = "always_included_context"
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
            if PaperScreeningPipeline._is_always_included_chunk_kind(str(chunk.get("kind") or "")):
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
        if label in {"references", "reference", "bibliography", "acknowledgements", "acknowledgments"}:
            return "reference"
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
        language_setting_lower = str(language_setting or "").strip().lower()
        resolved_language = "german" if language_setting_lower in {"de", "german"} else "english"

        if self.stage in {"full_text", "data_extraction"}:
            resolved_path = self._resolve_pdf_path(paper)
            pdf_text, page_count, _used_path, page_texts = self._load_pdf_text(paper, resolved_path, include_pages=True)
            if not pdf_text:
                return [], 0, 0, resolved_language

            sample_text = f"{paper.title}\n{pdf_text[:4000]}" if pdf_text else f"{paper.title}\n{paper.abstract}"
            detected_language_code = (self._detect_language_code(sample_text) or "").strip().lower()

            if detected_language_code.startswith("de"):
                resolved_language = "german"
            elif detected_language_code.startswith("en"):
                resolved_language = "english"
            elif language_setting_lower in {"auto_first", "auto-first", "auto"}:
                # human readable hint: keep tokenizer stable when language detection is uncertain.
                resolved_language = "english"

            pdf_text_tokens = self._estimate_text_tokens(pdf_text)
            pdf_visual_tokens = page_count * TOKENS_PER_PAGE_IMAGE

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
            language_used = detected_language_code or resolved_language
            return chunks, pdf_text_tokens, pdf_visual_tokens, language_used

        if language_setting_lower in {"auto_first", "auto-first", "auto"}:
            resolved_language = self._detect_language(f"{paper.title}\n{paper.abstract}")

        chunks = chunk_paper_sentences(paper.paper_id, paper.title, paper.abstract, resolved_language)
        return chunks, 0, 0, resolved_language

    def _compact_artifacts_enabled(self) -> bool:
        """Enable compact per-paper artifacts only during full_text runs."""

        return self.stage == "full_text" and self.artifact_mode == "compact"

    def _compact_artifact_path_for_folder(self, folder_path: Path, stage: str | None = None) -> Path:
        """Return per-paper compact artifact path for the target stage."""

        target_stage = stage or self.stage
        return folder_path / f"{target_stage}_artifact.json"

    def _metadata_snapshot_for_folder(self, folder_path: Path, fallback: dict | None = None) -> dict:
        """Read metadata from per-stage artifact files only."""

        candidate_paths = [
            self._compact_artifact_path_for_folder(folder_path, stage=self.stage),
            self._compact_artifact_path_for_folder(folder_path, stage="full_text"),
            self._compact_artifact_path_for_folder(folder_path, stage="data_extraction"),
        ]

        seen: set[Path] = set()
        for artifact_path in candidate_paths:
            if artifact_path in seen or not artifact_path.exists():
                continue
            seen.add(artifact_path)

            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            if not isinstance(payload, dict):
                continue

            metadata = payload.get("metadata")
            if isinstance(metadata, dict):
                return metadata

        fallback_payload = dict(fallback or {})
        fallback_payload.pop("folder_path", None)
        return fallback_payload

    def _write_compact_human_normalized_text(
        self,
        folder_path: Path,
        metadata_snapshot: dict,
        normalized_text: str,
    ) -> None:
        """Write human-checkable normalized text with metadata copied from artifact metadata."""

        normalized_path = folder_path / f"{self.stage}_normalized.txt"
        content = [
            "=== metadata ===",
            json.dumps(metadata_snapshot, ensure_ascii=False, indent=2),
            "",
            "=== normalized_full_text ===",
            normalized_text,
        ]
        normalized_path.write_text("\n".join(content), encoding="utf-8")

    def _persist_compact_text_artifacts(
        self,
        paper: PaperRecord,
        pdf_path: Path,
        cache_key: dict[str, int],
        normalized_text: str,
        normalized_pages: list[str],
        parser_level: str | None = None,
    ) -> None:
        """Persist compact machine artifact and synchronized human text sidecar."""

        folder_path = pdf_path.parent
        artifact_path = self._compact_artifact_path_for_folder(folder_path, stage="full_text")
        metadata_snapshot = self._metadata_snapshot_for_folder(folder_path, fallback=paper.metadata)

        artifact_payload: dict[str, Any] = {}
        if artifact_path.exists():
            try:
                loaded = json.loads(artifact_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    artifact_payload = loaded
            except Exception:
                artifact_payload = {}

        resolved_parser_level = str(parser_level or artifact_payload.get("parser_level") or "").strip()

        # Keep parser_level at the top for quick manual auditing of extraction fallback level.
        payload_without_level = {k: v for k, v in artifact_payload.items() if k != "parser_level"}
        artifact_payload = {"parser_level": resolved_parser_level}
        artifact_payload.update(payload_without_level)

        artifact_payload.update(
            {
                "meta": "stage_artifact",
                "schema_version": 1,
                "stage": "full_text",
                "paper_id": str(paper.paper_id),
                "metadata": metadata_snapshot,
                "source_pdf_name": pdf_path.name,
                "source_pdf_cache_key": cache_key,
                "normalized_text": normalized_text,
                "normalized_pages": normalized_pages,
                "normalized_text_sha256": self._sha256_text(normalized_text),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        artifact_path.write_text(
            json.dumps(artifact_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._write_compact_human_normalized_text(folder_path, metadata_snapshot, normalized_text)

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

            artifact_path = self._compact_artifact_path_for_folder(folder_path, stage="full_text")
            artifact_payload: dict[str, Any] = {}
            if artifact_path.exists():
                try:
                    loaded = json.loads(artifact_path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        artifact_payload = loaded
                except Exception:
                    artifact_payload = {}

            resolved_parser_level = str(artifact_payload.get("parser_level") or "").strip()
            payload_without_level = {k: v for k, v in artifact_payload.items() if k != "parser_level"}
            artifact_payload = {"parser_level": resolved_parser_level}
            artifact_payload.update(payload_without_level)
            artifact_payload.update(
                {
                    "meta": "stage_artifact",
                    "schema_version": 1,
                    "stage": "full_text",
                    "paper_id": str(canonical.get("paper_id") or folder_name),
                    "metadata": canonical,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            artifact_path.write_text(
                json.dumps(artifact_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            for stale_name in ("metadata.json", "metadata.csv"):
                stale_path = folder_path / stale_name
                if stale_path.exists():
                    try:
                        stale_path.unlink()
                    except Exception:
                        pass

            if not any(folder_path.glob("*.pdf")):
                paper_id = str(canonical.get("paper_id") or "").strip()
                source_pdf = self._find_source_pdf_for_paper_id(paper_id)
                if source_pdf and source_pdf.exists():
                    target_pdf = folder_path / source_pdf.name
                    try:
                        if source_pdf.resolve() != target_pdf.resolve():
                            shutil.copy2(source_pdf, target_pdf)
                    except Exception as exc:  # pylint: disable=broad-except
                        self._log_error(
                            paper_id or folder_name,
                            f"failed to copy source PDF into retry folder: {exc}",
                            error_type="retry_pdf_copy_failed",
                        )
                elif self.pdf_root and self.pdf_root.exists():
                    self._log_error(
                        paper_id or folder_name,
                        f"no source PDF found in pdf_root={self.pdf_root} for retry folder materialization",
                        error_type="retry_pdf_source_missing",
                    )

            folders.append(folder_path)

        self._paper_folders = folders

    def _find_source_pdf_for_paper_id(self, paper_id: str) -> Path | None:
        """Locate source PDF in pdf_root using the configured paper ID for retry materialization."""

        if not self.pdf_root or not self.pdf_root.exists():
            return None

        cid = str(paper_id or "").strip().lstrip("#")
        if not cid:
            return None

        candidate_folders: list[Path] = []
        direct = self.pdf_root / cid
        if direct.is_dir():
            candidate_folders.append(direct)

        try:
            prefixed = sorted(
                [
                    folder
                    for folder in self.pdf_root.iterdir()
                    if folder.is_dir() and folder.name.startswith(f"{cid}_")
                ],
                key=lambda folder: folder.stat().st_mtime,
                reverse=True,
            )
            candidate_folders.extend(prefixed)
        except Exception:
            pass

        unique_folders: list[Path] = []
        seen: set[str] = set()
        for folder in candidate_folders:
            key = str(folder.resolve())
            if key in seen:
                continue
            seen.add(key)
            unique_folders.append(folder)

        for folder in unique_folders:
            preferred = folder / f"{cid}.pdf"
            if preferred.exists():
                return preferred
            canonical = folder / PAPER_PDF_NAME
            if canonical.exists():
                return canonical
            pdfs = sorted(folder.glob("*.pdf"))
            if pdfs:
                return pdfs[0]

        return None

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
            row = self._metadata_snapshot_for_folder(folder)
            if not row:
                continue

            paper_id = self._extract_paper_id(row)
            if paper_id not in included_ids:
                continue

            dest = target_dir / folder.name
            try:
                dest.mkdir(parents=True, exist_ok=True)

                pdfs = sorted(folder.glob("*.pdf"))
                if pdfs:
                    shutil.copy2(pdfs[0], dest / pdfs[0].name)
                # human readable hint: PDFs are not duplicated; normalized text and artifact JSON are sufficient.
                # pdfs = sorted(folder.glob("*.pdf"))
                # if pdfs: shutil.copy2(pdfs[0], dest / pdfs[0].name)

                full_text_chunks = folder / "full_text_selected_chunks.jsonl"
                if full_text_chunks.exists():
                    shutil.copy2(full_text_chunks, dest / "data_extraction_selected_chunks.jsonl")

                full_text_artifact = folder / "full_text_artifact.json"
                if full_text_artifact.exists():
                    shutil.copy2(full_text_artifact, dest / full_text_artifact.name)

                full_text_normalized = folder / "full_text_normalized.txt"
                if full_text_normalized.exists():
                    shutil.copy2(full_text_normalized, dest / full_text_normalized.name)

                data_artifact_path = self._compact_artifact_path_for_folder(dest, stage="data_extraction")
                data_artifact_payload: dict[str, Any] = {}
                if data_artifact_path.exists():
                    try:
                        loaded = json.loads(data_artifact_path.read_text(encoding="utf-8"))
                        if isinstance(loaded, dict):
                            data_artifact_payload = loaded
                    except Exception:
                        data_artifact_payload = {}

                data_artifact_payload.update(
                    {
                        "meta": "stage_artifact",
                        "schema_version": 1,
                        "stage": "data_extraction",
                        "paper_id": str(read_metadata_value(row, "paper_id", folder.name)),
                        "metadata": row,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                data_artifact_path.write_text(
                    json.dumps(data_artifact_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                for stale_name in ("metadata.json", "metadata.csv"):
                    stale_path = dest / stale_name
                    if stale_path.exists():
                        try:
                            stale_path.unlink()
                        except Exception:
                            pass

                copied.append(dest)
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    str(paper_id),
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

        if self.input_files:
            resolved: list[Path] = []
            for configured_path in self.input_files:
                path = configured_path
                if not path.is_absolute():
                    path = path if path.exists() else self.csv_dir / path
                if path.exists() and path.is_file():
                    resolved.append(path)
            self._stage_csv_cache[select_only] = resolved
            return list(resolved)

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
        # human readable hint: retries can run from retry_runs itself or from isolated child folders under retry_runs.
        in_retry_scope = self.csv_dir.name == "retry_runs" or self.csv_dir.parent.name == "retry_runs"
        if in_retry_scope:
            resolved = [max(files, key=lambda p: p.stat().st_mtime)] if files else []
            self._stage_csv_cache[select_only] = resolved
            return list(resolved)
        resolved = sorted(files)
        self._stage_csv_cache[select_only] = resolved
        return list(resolved)

    def _load_included_ids(self, csv_path: Path) -> set[str]:
        """Read included IDs from the configured included-paper CSV."""

        ids: set[str] = set()
        with open(csv_path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                paper_id = self._extract_paper_id(row)
                if paper_id:
                    ids.add(paper_id)
        return ids

    def _extract_paper_id(self, row: dict) -> str:
        """Extract the best available paper ID from user-configured CSV headers."""

        return read_metadata_value(row, "paper_id")

    def _extract_year(self, row: dict) -> str:
        """Try to find a publication year from many possible columns."""

        return extract_year_from_metadata(row)

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

        paper_id = read_metadata_value(row, "paper_id", "unknown")
        authors = read_metadata_value(row, "authors")
        first_author = authors.split(",")[0].strip()
        first_author = first_author.split(" ")[0] if first_author else ""
        year = self._extract_year(row)
        title = read_metadata_value(row, "title")

        parts = [norm(str(paper_id)), norm(str(first_author)), norm(str(year)), norm(str(title))[:TITLE_TRUNC]]
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

        cache_text_path = path.parent / f"{path.stem}_normalized_text.txt"
        cache_pages_path = path.parent / f"{path.stem}_normalized_pages.json"
        cache_meta_path = path.parent / f"{path.stem}_normalized_meta.json"
        advanced_mode_for_pdf = bool(self.use_advanced_pdf_parser and path.suffix.lower() == ".pdf")
        compact_mode = self._compact_artifacts_enabled()
        compact_artifact_path = self._compact_artifact_path_for_folder(path.parent, stage="full_text")
        cache_key: dict[str, int] = {}

        def _effective_page_count(pages_value: list[str]) -> int:
            if advanced_mode_for_pdf:
                return self._count_pdf_pages(path)
            return len(pages_value) if include_pages else self._count_pdf_pages(path)

        try:
            pdf_stat = path.stat()
            cache_key = {
                "pdf_size": int(pdf_stat.st_size),
                "pdf_mtime_ns": int(pdf_stat.st_mtime_ns),
            }

            if compact_mode and compact_artifact_path.exists():
                compact_payload = json.loads(compact_artifact_path.read_text(encoding="utf-8"))
                if isinstance(compact_payload, dict):
                    cached_key = compact_payload.get("source_pdf_cache_key")
                    if isinstance(cached_key, dict) and all(cached_key.get(k) == cache_key.get(k) for k in cache_key):
                        text = str(compact_payload.get("normalized_text") or "")
                        pages_payload = compact_payload.get("normalized_pages")
                        pages = [str(page or "") for page in pages_payload] if isinstance(pages_payload, list) else []
                        if advanced_mode_for_pdf and len(pages) <= 1:
                            pages = []
                        if text.strip():
                            if include_pages and not pages and not advanced_mode_for_pdf:
                                pages = [text]
                            metadata_snapshot = self._metadata_snapshot_for_folder(path.parent, fallback=paper.metadata)
                            self._write_compact_human_normalized_text(path.parent, metadata_snapshot, text)
                            for stale_path in (cache_text_path, cache_pages_path, cache_meta_path):
                                try:
                                    if stale_path.exists():
                                        stale_path.unlink()
                                except Exception:
                                    pass
                            page_count = _effective_page_count(pages)
                            return text, page_count, path, pages

            if cache_text_path.exists() and cache_meta_path.exists() and (cache_pages_path.exists() or not include_pages):
                cache_meta = json.loads(cache_meta_path.read_text(encoding="utf-8"))
                if isinstance(cache_meta, dict) and all(cache_meta.get(k) == cache_key.get(k) for k in cache_key):
                    text = cache_text_path.read_text(encoding="utf-8")
                    pages: list[str] = []
                    if include_pages:
                        pages_payload = json.loads(cache_pages_path.read_text(encoding="utf-8"))
                        if isinstance(pages_payload, list):
                            pages = [str(page or "") for page in pages_payload]
                    if advanced_mode_for_pdf and len(pages) <= 1:
                        pages = []
                    if include_pages and not pages and not advanced_mode_for_pdf:
                        pages = [text]
                    if compact_mode and text.strip():
                        pages_for_cache = pages
                        if not pages_for_cache and not advanced_mode_for_pdf:
                            pages_for_cache = [text]
                        self._persist_compact_text_artifacts(
                            paper,
                            path,
                            cache_key,
                            text,
                            pages_for_cache,
                            parser_level=None,
                        )
                        for stale_path in (cache_text_path, cache_pages_path, cache_meta_path):
                            try:
                                if stale_path.exists():
                                    stale_path.unlink()
                            except Exception:
                                pass
                    page_count = _effective_page_count(pages)
                    return text, page_count, path, pages
        except Exception:
            # human readable hint: cache read failures should never stop screening; pipeline falls back to live extraction.
            pass

        try:
            used_advanced_pdf_parser = False
            parser_level: str | None = None
            if advanced_mode_for_pdf:
                used_advanced_pdf_parser = True
                pages = []
                text, parser_level = extract_markdown_from_pdf_with_level(path)
            elif include_pages:
                pages = read_pdf_pages(str(path))
                text = "\n".join(pages)
            else:
                pages = []
                text = read_pdf_file(str(path))

            if not text or not text.strip():
                parser_chain = "advanced parser chain" if used_advanced_pdf_parser else "legacy parser chain"
                self._log_error(
                    paper.paper_id,
                    f"PDF has no extractable text after {parser_chain}; skipping: {path}",
                    error_type="pdf_unreadable",
                )
                return "", 0, None, []

            if include_pages and not pages and not used_advanced_pdf_parser:
                pages = [text]

            try:
                if not cache_key:
                    pdf_stat = path.stat()
                    cache_key = {
                        "pdf_size": int(pdf_stat.st_size),
                        "pdf_mtime_ns": int(pdf_stat.st_mtime_ns),
                    }
                if compact_mode:
                    pages_for_cache = pages
                    if not pages_for_cache and not used_advanced_pdf_parser:
                        pages_for_cache = [text]
                    self._persist_compact_text_artifacts(
                        paper,
                        path,
                        cache_key,
                        text,
                        pages_for_cache,
                        parser_level=parser_level,
                    )
                    for stale_path in (cache_text_path, cache_pages_path, cache_meta_path):
                        try:
                            if stale_path.exists():
                                stale_path.unlink()
                        except Exception:
                            pass
                else:
                    cache_text_path.write_text(text, encoding="utf-8")
                    cache_pages_path.write_text(json.dumps(pages, ensure_ascii=False), encoding="utf-8")
                    cache_meta_path.write_text(json.dumps(cache_key, ensure_ascii=False), encoding="utf-8")
            except Exception:
                # human readable hint: cache write failures are non-blocking and do not affect screening correctness.
                pass

            page_count = _effective_page_count(pages)
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
        paper_id = str(read_metadata_value(paper.metadata, "paper_id", paper.paper_id)).strip().lstrip("#") or "paper"
        target = folder_path / f"{paper_id}.pdf"
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

    @staticmethod
    def _add_llm_usage(target: dict[str, int], usage: dict | None) -> None:
        """human readable hint: sum provider token counters across several smaller domain-level calls."""

        if not usage:
            return
        prompt_value = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
        response_value = int(
            usage.get("completion_tokens")
            or usage.get("output_tokens")
            or usage.get("response_tokens")
            or 0
        )
        total_value = int(usage.get("total_tokens") or 0)
        target["prompt_tokens"] = target.get("prompt_tokens", 0) + prompt_value
        target["completion_tokens"] = target.get("completion_tokens", 0) + response_value
        target["response_tokens"] = target.get("response_tokens", 0) + response_value
        target["total_tokens"] = target.get("total_tokens", 0) + total_value

    async def _call_data_extraction_domains_async(
        self,
        context: str,
        paper_id: str,
    ) -> tuple[str | None, dict | None, dict[str, str]]:
        """human readable hint: extract each KB domain separately, then merge validated domain JSON."""

        if self._extraction_schema is None:
            return "LLM error: data_extraction requires a configured DynamicExtractionSchema.", None, {
                "schema": "missing_extraction_schema"
            }

        if not bool(LLM_SETTINGS.get("data_extraction_split_by_domain", True)):
            raw_text, usage = await self._call_llm_async(context)
            return raw_text, usage, {}

        merged_payload = self._extraction_schema.default_payload()
        errors_by_domain: dict[str, str] = {}
        usage_totals: dict[str, int] = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "response_tokens": 0,
            "total_tokens": 0,
        }
        domain_max_tokens = max(256, int(LLM_SETTINGS.get("data_extraction_domain_max_tokens", 3000) or 3000))
        response_format_mode = str(
            LLM_SETTINGS.get("data_extraction_response_format_mode", "prompt_only") or "prompt_only"
        ).strip().lower()

        domain_groups = domain_groups_for_schema(
            self._extraction_schema,
            LLM_SETTINGS.get("data_extraction_domain_groups"),
        )
        for domains in domain_groups:
            group_label = "+".join(domains)
            domain_schema = self._extraction_schema.for_domains(domains)
            domain_prompt = domain_schema.inject_into_prompt(self._base_prompt_template)
            response_format_override: dict | None = None
            use_schema_response_format = response_format_mode == "json_schema"
            if response_format_mode == "json_object":
                response_format_override = {"type": "json_object"}
            raw_text, usage = await self._call_llm_async(
                context,
                prompt_template=domain_prompt,
                extraction_schema=domain_schema,
                response_format_override=response_format_override,
                use_extraction_response_format=use_schema_response_format,
                max_tokens=domain_max_tokens,
                system_prompt="",
            )
            self._add_llm_usage(usage_totals, usage)
            if not raw_text:
                errors_by_domain[group_label] = "empty_response"
                continue
            if isinstance(raw_text, str) and raw_text.startswith("LLM error"):
                errors_by_domain[group_label] = raw_text
                continue

            parsed_domain, validation_error = parse_and_validate(raw_text, domain_schema)
            if validation_error:
                errors_by_domain[group_label] = validation_error
                continue

            for domain in domains:
                domain_payload = parsed_domain.get(domain)
                if isinstance(domain_payload, dict):
                    merged_payload[domain] = domain_payload
                else:
                    errors_by_domain[domain] = "validated domain payload missing expected domain key"

        try:
            merged_payload = self._extraction_schema.validate_payload(merged_payload)
        except Exception as exc:  # pylint: disable=broad-except
            errors_by_domain["merged_payload"] = str(exc)
            merged_payload = self._extraction_schema.default_payload()

        usage_totals["domain_count"] = len(self._extraction_schema.domains)
        usage_totals["domain_group_count"] = len(domain_groups)
        usage_totals["domain_error_count"] = len(errors_by_domain)
        return json.dumps(merged_payload, ensure_ascii=False), usage_totals, errors_by_domain

    def _call_llm(
        self,
        context: str,
        *,
        prompt_template: str | None = None,
        extraction_schema: DynamicExtractionSchema | None = None,
        response_format_override: dict | None = None,
        use_extraction_response_format: bool = True,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
    ) -> tuple[str | None, dict | None]:
        """Call the LLM and return both text and usage (if provided by the API)."""

        try:
            if use_api:
                active_schema = extraction_schema if extraction_schema is not None else self._extraction_schema
                response_format = response_format_override
                if response_format is None and use_extraction_response_format:
                    response_format = (
                        active_schema.openai_response_format()
                        if self.stage == "data_extraction" and active_schema is not None
                        else None
                    )
                resolved_system_prompt = system_prompt
                if resolved_system_prompt is None and self.stage == "data_extraction":
                    resolved_system_prompt = ""
                responder = OpenAIResponder(
                    data=context,
                    model=self._llm_model,
                    prompt_template=prompt_template or self.prompt_template,
                    client=self._get_openai_client(base_url=self._llm_base_url),
                    response_format=response_format,
                    system_prompt=resolved_system_prompt,
                    max_tokens=max_tokens,
                )
                return responder.generate_response()
        except Exception as exc:  # pylint: disable=broad-except
            return f"LLM error: {exc}", None
        return None, None

    async def _call_llm_async(
        self,
        context: str,
        *,
        prompt_template: str | None = None,
        extraction_schema: DynamicExtractionSchema | None = None,
        response_format_override: dict | None = None,
        use_extraction_response_format: bool = True,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
    ) -> tuple[str | None, dict | None]:
        """Call the LLM asynchronously and return text plus usage metadata."""

        try:
            if use_api:
                active_schema = extraction_schema if extraction_schema is not None else self._extraction_schema
                response_format = response_format_override
                if response_format is None and use_extraction_response_format:
                    response_format = (
                        active_schema.openai_response_format()
                        if self.stage == "data_extraction" and active_schema is not None
                        else None
                    )
                resolved_system_prompt = system_prompt
                if resolved_system_prompt is None and self.stage == "data_extraction":
                    resolved_system_prompt = ""
                responder = OpenAIResponder(
                    data=context,
                    model=self._llm_model,
                    prompt_template=prompt_template or self.prompt_template,
                    client=self._get_async_openai_client(base_url=self._llm_base_url),
                    response_format=response_format,
                    system_prompt=resolved_system_prompt,
                    max_tokens=max_tokens,
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

    def _configure_dynamic_screening_schema(self) -> None:
        """Build dynamic exclusion schema keys from user tags and prompt-required fields."""

        study_tag_flag_keys = set(build_study_tag_field_keys(STUDY_TAGS_INCLUDE))
        prompt_exclusion_flag_keys = {
            normalize_schema_key(field)
            for field in self._prompt_required_json_fields
            if looks_like_exclusion_field(field)
            and normalize_schema_key(field)
        }

        active_keys = sorted(study_tag_flag_keys | prompt_exclusion_flag_keys)
        if not active_keys:
            raise ValueError(
                "No exclusion schema keys found. Define STUDY_TAGS_INCLUDE in config/user_orchestrator.py "
                "or include exclusion flag keys in the prompt END GOAL section."
            )

        self._study_tag_flag_keys = tuple(sorted(study_tag_flag_keys))
        self._prompt_exclusion_flag_keys = tuple(sorted(prompt_exclusion_flag_keys))
        self._active_exclusion_flag_keys = tuple(active_keys)
        self._allowed_exclusion_reason_categories = tuple(sorted(set(active_keys)))

        neutral_keys = {key for key in active_keys if key.endswith("_context")}
        self._neutral_exclusion_flag_keys = tuple(sorted(neutral_keys))

        configured_reason_keys = set(self._allowed_exclusion_reason_categories)
        if "insufficient_context" in configured_reason_keys:
            self._insufficient_context_reason_key = "insufficient_context"
        else:
            self._insufficient_context_reason_key = next(
                (key for key in sorted(configured_reason_keys) if "insufficient" in key),
                None,
            )

        self._primary_topic_absence_reason_key = select_topic_absence_reason_key(
            configured_reason_keys,
            self._topic_primary_terms,
        )
        self._secondary_topic_absence_reason_key = select_topic_absence_reason_key(
            configured_reason_keys,
            self._topic_secondary_terms,
        )

    def _print_dynamic_schema_summary(self) -> None:
        """Print a compact startup summary of the active dynamic exclusion schema."""

        if self.quiet:
            return

        preview_count = 8
        key_preview = ", ".join(self._active_exclusion_flag_keys[:preview_count])
        suffix = " ..." if len(self._active_exclusion_flag_keys) > preview_count else ""
        primary_reason = self._primary_topic_absence_reason_key or "n/a"
        secondary_reason = self._secondary_topic_absence_reason_key or "n/a"

        print(
            "[schema] Active exclusion keys "
            f"({len(self._active_exclusion_flag_keys)}): {key_preview or 'none'}{suffix}"
        )
        print(
            "[schema] Sources: "
            f"study_tags={len(self._study_tag_flag_keys)} "
            f"prompt_end_goal={len(self._prompt_exclusion_flag_keys)} "
            f"topic_signals={self._topic_signal_source}"
        )
        print(
            "[schema] Topic absence reason keys: "
            f"primary={primary_reason}, secondary={secondary_reason}"
        )

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

        payload = self._normalize_dynamic_exclusion_payload(payload)
        self._validate_dynamic_exclusion_schema(payload)

        return payload

    def _coerce_screening_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
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

        for key in self._active_exclusion_flag_keys:
            value = normalized.get(key)
            if not isinstance(value, str):
                continue

            lowered = value.strip().lower()
            if key in self._neutral_exclusion_flag_keys and lowered == "neutral":
                normalized[key] = "NEUTRAL"
                continue

            bool_like = PaperScreeningPipeline._parse_bool_like(lowered)
            if bool_like is not None:
                normalized[key] = bool_like

        return normalized

    def _normalize_dynamic_exclusion_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Normalize exclusion reason and dynamic flag values for schema checks."""

        normalized = dict(payload)

        for key in list(normalized.keys()):
            if not isinstance(key, str):
                continue
            normalized_key = normalize_schema_key(key)
            if not normalized_key or normalized_key == key or normalized_key in normalized:
                continue
            normalized[normalized_key] = normalized[key]

        for key in self._active_exclusion_flag_keys:
            if key not in normalized:
                continue
            value = normalized.get(key)

            if isinstance(value, str) and key in self._neutral_exclusion_flag_keys and value.strip().lower() == "neutral":
                normalized[key] = "NEUTRAL"
                continue

            bool_like = PaperScreeningPipeline._parse_bool_like(value)
            if bool_like is not None:
                normalized[key] = bool_like

        reason = self._parse_exclusion_reason(normalized)
        if reason:
            normalized["exclusion_reason_category"] = reason

        return normalized

    def _validate_dynamic_exclusion_schema(self, payload: dict[str, Any]) -> None:
        """Validate exclusion flags/reasons against dynamic tag-driven schema."""

        allowed_reasons: set[str] = {
            str(key)
            for key in self._allowed_exclusion_reason_categories
            if isinstance(key, str)
        }
        payload_flag_like_keys = {
            normalize_schema_key(key)
            for key, value in payload.items()
            if isinstance(key, str)
            and key not in CORE_SCREENING_SCHEMA_FIELDS
            and (
                isinstance(value, bool)
                or (isinstance(value, str) and value.strip().upper() == "NEUTRAL")
                or looks_like_exclusion_field(key)
            )
            and normalize_schema_key(key)
        }
        allowed_reasons.update(payload_flag_like_keys)

        for key in self._active_exclusion_flag_keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if isinstance(value, bool):
                continue
            if key in self._neutral_exclusion_flag_keys and isinstance(value, str) and value.strip().upper() == "NEUTRAL":
                continue

            parsed = PaperScreeningPipeline._parse_bool_like(value)
            if parsed is not None:
                payload[key] = parsed
                continue

            raise ValueError(
                f"Exclusion flag '{key}' must be boolean"
                + (" or 'NEUTRAL'" if key in self._neutral_exclusion_flag_keys else "")
                + "."
            )

        is_eligible = self._parse_is_eligible(payload)
        reason = (self._parse_exclusion_reason(payload) or "").strip()

        if reason:
            payload["exclusion_reason_category"] = reason

        if is_eligible is False and not reason:
            raise ValueError("exclusion_reason_category is required when is_eligible is false")
        if reason and reason not in allowed_reasons:
            raise ValueError(
                "exclusion_reason_category is not configured in STUDY_TAGS_INCLUDE/prompt schema: "
                + reason
            )

    @staticmethod
    def _extract_required_json_fields_from_prompt(prompt_template: str) -> set[str]:
        """human readable hint: detect required screening keys only from the END GOAL key list."""

        if not prompt_template:
            return set()

        in_goal = False
        fields: set[str] = set()
        for line in prompt_template.splitlines():
            stripped = line.strip()
            lower = stripped.lower()
            if lower.startswith("# end goal"):
                in_goal = True
                continue
            if in_goal and lower.startswith("# narrowing"):
                break
            if not in_goal:
                continue

            match = re.match(r"^\s*-\s*\"([a-zA-Z_][a-zA-Z0-9_]*)\"\s*:\s*$", stripped)
            if match:
                fields.add(match.group(1))

        return fields

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
                    normalized = normalize_schema_key(str(val))
                    if normalized:
                        return normalized
        return None

    @staticmethod
    def _publication_type_prefilter(metadata: dict | None) -> dict[str, Any]:
        """human readable hint: deterministic metadata prefilter to flag likely non-empirical publication types."""

        info = metadata or {}
        fields = {
            "title": read_metadata_value(info, "title"),
            "journal": read_metadata_value(info, "journal"),
            "tags": read_metadata_value(info, "tags"),
            "notes": read_metadata_value(info, "notes"),
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

    def _collect_exclusion_flag_values(self, payload: dict[str, Any]) -> dict[str, bool | None]:
        """human readable hint: map configured exclusion flag values from a screening payload."""

        candidate_keys = set(self._active_exclusion_flag_keys)
        candidate_keys.update(
            key
            for key in payload.keys()
            if isinstance(key, str) and looks_like_exclusion_field(key)
        )

        values: dict[str, bool | None] = {}
        for key in sorted(candidate_keys):
            raw_value = payload.get(key)
            if (
                key in self._neutral_exclusion_flag_keys
                and isinstance(raw_value, str)
                and raw_value.strip().upper() == "NEUTRAL"
            ):
                values[key] = None
                continue
            values[key] = PaperScreeningPipeline._parse_bool_like(raw_value)

        return values

    def _detect_decision_contradictions(self, payload: dict[str, Any]) -> list[str]:
        """human readable hint: identify logical contradictions in the LLM screening output."""

        contradictions: list[str] = []
        is_eligible = self._parse_is_eligible(payload)
        reason = (self._parse_exclusion_reason(payload) or "").strip()
        flag_values = self._collect_exclusion_flag_values(payload)
        payload_flag_keys = [name for name in flag_values.keys() if name in payload]
        active_flags = sorted([name for name in payload_flag_keys if flag_values.get(name) is True])
        allowed_reasons = set(self._allowed_exclusion_reason_categories)

        if is_eligible is False and payload_flag_keys and not active_flags:
            contradictions.append("is_eligible_false_without_true_exclusion_flag")
        if is_eligible is True and active_flags:
            contradictions.append("is_eligible_true_with_true_exclusion_flag")
        if reason and allowed_reasons and reason not in allowed_reasons:
            contradictions.append("reason_not_in_configured_exclusion_categories")
        if (
            is_eligible is False
            and reason
            and reason in self._neutral_exclusion_flag_keys
            and reason in payload
            and flag_values.get(reason) is not True
        ):
            contradictions.append("context_reason_without_context_exclusion")

        return contradictions

    def _assess_borderline_decision(
        self,
        payload: dict[str, Any],
        contradictions: list[str],
        publication_prefilter: dict[str, Any],
        selected_chunks: list[dict],
        selection_trace: dict[str, Any],
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

        if is_eligible is False and reason and reason in self._neutral_exclusion_flag_keys:
            reasons.append("context_only_exclusion")

        if publication_prefilter.get("likely_non_empirical_publication") and is_eligible is True:
            reasons.append("publication_type_prefilter_conflict")

        non_title_chunks = [
            row
            for row in selected_chunks
            if not self._is_always_included_chunk_kind(str(row.get("kind") or ""))
        ]
        non_title_word_count = sum(len(str(row.get("text") or "").split()) for row in non_title_chunks)
        section_rescue_hits = int(selection_trace.get("section_rescue_hits") or 0)

        if (
            is_eligible is False
            and self._insufficient_context_reason_key
            and reason == self._insufficient_context_reason_key
        ):
            if len(non_title_chunks) > 0 and (non_title_word_count >= 40 or section_rescue_hits > 0):
                reasons.append("insufficient_context_with_non_title_evidence")

        signal_text = self._build_borderline_signal_text(payload, non_title_chunks)
        has_intervention_signal = bool(self._intervention_signal_pattern.search(signal_text))

        if (
            is_eligible is False
            and self._primary_topic_absence_reason_key
            and reason == self._primary_topic_absence_reason_key
        ):
            if has_intervention_signal and self._topic_primary_signal_pattern.search(signal_text):
                reasons.append("primary_topic_exclusion_with_intervention_or_topic_signal")

        if (
            is_eligible is False
            and self._secondary_topic_absence_reason_key
            and reason == self._secondary_topic_absence_reason_key
        ):
            if has_intervention_signal and self._topic_secondary_signal_pattern.search(signal_text):
                reasons.append("secondary_topic_exclusion_with_intervention_or_topic_signal")

        deduped = sorted(set(reasons))
        return {
            "needs_adjudication": bool(deduped),
            "reasons": deduped,
            "confidence": confidence,
        }

    def _build_borderline_signal_text(self, payload: dict[str, Any], non_title_chunks: list[dict]) -> str:
        """Aggregate decision and evidence snippets for conservative borderline signal checks."""

        snippets: list[str] = []
        for key in ("step_by_step_deliberation", "justification"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                snippets.append(value.strip())

        char_budget = 4500
        used = sum(len(part) for part in snippets)
        for chunk in non_title_chunks:
            if used >= char_budget:
                break
            text = str(chunk.get("text") or "").strip()
            if not text:
                continue
            remaining = max(char_budget - used, 0)
            if remaining <= 0:
                break
            excerpt = text[:remaining]
            snippets.append(excerpt)
            used += len(excerpt)

        return "\n".join(snippets)

    def _build_adjudication_context(
        self,
        base_context: str,
        current_payload: dict[str, Any],
        reasons: list[str],
        publication_prefilter: dict[str, Any],
    ) -> str:
        """human readable hint: append a deterministic adjudication note for one final consistency pass."""

        reason_text = ", ".join(reasons) if reasons else "none"
        prefilter_text = json.dumps(publication_prefilter, ensure_ascii=False)
        prior = json.dumps(current_payload, ensure_ascii=False)
        guidance_lines: list[str] = [
            "Perform a strict prompt-criteria re-check using non-title full-text evidence.",
        ]

        if "insufficient_context_with_non_title_evidence" in reasons:
            guidance_lines.append(
                "You previously marked the context-evidence exclusion despite non-title evidence; keep this exclusion true only when evidence is unreadable or genuinely too sparse."
            )

        if "primary_topic_exclusion_with_intervention_or_topic_signal" in reasons:
            reason_key = self._primary_topic_absence_reason_key or "primary-topic exclusion"
            guidance_lines.append(
                f"You previously excluded for {reason_key} despite intervention/topic cues; set this exclusion true only when methods clearly lack the required primary topic component."
            )

        if "secondary_topic_exclusion_with_intervention_or_topic_signal" in reasons:
            reason_key = self._secondary_topic_absence_reason_key or "secondary-topic exclusion"
            guidance_lines.append(
                f"You previously excluded for {reason_key} despite intervention/topic cues; set this exclusion true only when outcomes clearly lack the required secondary topic component."
            )

        neutral_hint = ""
        if self._neutral_exclusion_flag_keys:
            neutral_hint = (
                " If a context flag is NEUTRAL, do not use that NEUTRAL flag as exclusion_reason_category."
            )

        guidance_text = "\n".join(f"- {line}" for line in guidance_lines)

        note = (
            "\n\n[ADJUDICATION NOTE]\n"
            "Your prior output was flagged as borderline or inconsistent."
            f" Reasons: {reason_text}.\n"
            f" Deterministic publication prefilter: {prefilter_text}.\n"
            f" Guardrails:\n{guidance_text}\n"
            "Re-evaluate using only the provided criteria."
            " Ensure is_eligible is logically consistent with exclusion flags and exclusion_reason_category."
            f"{neutral_hint}"
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
            "run_id": getattr(self, "run_id", None),
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
        """Record a total-token context-window overflow event."""

        msg = {
            "paper_id": paper_id,
            "estimated_tokens": estimated_tokens,
            "context_window": CONTEXT_WINDOW,
            "max_output_tokens": int(llm_max_tokens),
            "prompt_token_budget_tokens": PROMPT_TOKEN_BUDGET,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.overflow_log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.overflow_log_path, "a", encoding="utf-8") as logf:
            logf.write(json.dumps(msg) + "\n")
        self._log_error(
            paper_id,
            "context overflow(total): "
            f"{estimated_tokens} > {CONTEXT_WINDOW} "
            f"(max_output={llm_max_tokens}, prompt_budget={PROMPT_TOKEN_BUDGET})",
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
            "run_label": self.run_label,
            "run_id": self.run_id,
            "llm_decision": decision,
            "selected_chunks": selected,
            "extracted_data": extraction_payload.get("extracted_data") if extraction_payload else None,
            "extracted_data_flat": extraction_payload.get("extracted_data_flat") if extraction_payload else None,
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

            if self._compact_artifacts_enabled():
                artifact_path = self._compact_artifact_path_for_folder(folder_path, stage="full_text")
                artifact_payload: dict[str, Any] = {}
                if artifact_path.exists():
                    try:
                        loaded = json.loads(artifact_path.read_text(encoding="utf-8"))
                        if isinstance(loaded, dict):
                            artifact_payload = loaded
                    except Exception:
                        artifact_payload = {}

                metadata_snapshot = self._metadata_snapshot_for_folder(folder_path, fallback=paper.metadata)
                artifact_payload.update(
                    {
                        "meta": "stage_artifact",
                        "schema_version": 1,
                        "stage": "full_text",
                        "paper_id": str(paper.paper_id),
                        "run_label": self.run_label,
                        "run_id": self.run_id,
                        "metadata": metadata_snapshot,
                        "selected_chunks": selected,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                artifact_path.write_text(
                    json.dumps(artifact_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                if not self.compact_keep_legacy_selected_chunks:
                    return

            chunks_path = folder_path / f"{self.stage}_selected_chunks.jsonl"
            with open(chunks_path, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "meta": "selected_chunks",
                            "description": f"Chunks retained for LLM input for stage '{self.stage}' (JSONL).",
                            "run_label": self.run_label,
                            "run_id": self.run_id,
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
                            "run_label": self.run_label,
                            "run_id": self.run_id,
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

        folder_path = Path(folder)
        chunks_path = folder_path / f"{self.stage}_selected_chunks.jsonl"

        if chunks_path.exists():
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

        artifact_candidates = [
            self._compact_artifact_path_for_folder(folder_path, stage=self.stage),
            self._compact_artifact_path_for_folder(folder_path, stage="full_text"),
        ]
        seen_paths: set[Path] = set()
        for artifact_path in artifact_candidates:
            if artifact_path in seen_paths or not artifact_path.exists():
                continue
            seen_paths.add(artifact_path)

            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    paper.paper_id,
                    f"failed to read compact artifact selected chunks: {exc}",
                    error_type="selected_chunks_read_failed",
                )
                continue

            if not isinstance(payload, dict):
                continue
            selected = payload.get("selected_chunks")
            if isinstance(selected, list):
                return selected
        return []

    def _write_data_extraction_outputs(self, paper: PaperRecord, extraction_payload: dict) -> None:
        """Write per-paper extraction outputs (JSONL + CSV)."""

        output_dir = self._data_extraction_output_dir(paper)
        output_dir.mkdir(parents=True, exist_ok=True)

        extraction_jsonl_payload = (
            json.dumps(
                {
                    "meta": "extraction_results",
                    "description": "Per-paper extracted fields (JSONL).",
                    "criteria": self._extraction_criteria,
                    "run_label": self.run_label,
                    "run_id": self.run_id,
                }
            )
            + "\n"
            + json.dumps(extraction_payload)
            + "\n"
        )
        # human readable hint: write one canonical extraction JSONL to avoid duplicate artifact names.
        extraction_jsonl_path = output_dir / f"{self.stage}_results.jsonl"
        extraction_jsonl_path.write_text(extraction_jsonl_payload, encoding="utf-8")
        stale_jsonl_path = output_dir / f"{self.stage}_extraction_results.jsonl"
        if stale_jsonl_path.exists():
            stale_jsonl_path.unlink()

        flat_extracted = extraction_payload.get("extracted_data_flat") or {}
        fieldnames = ["paper_id", "run_id"]
        if isinstance(flat_extracted, dict) and flat_extracted:
            fieldnames.extend(sorted(str(key) for key in flat_extracted.keys()))
        elif self._extraction_criteria:
            fieldnames.extend(self._extraction_criteria)
        else:
            fieldnames.append("extracted_data")

        csv_rows: list[dict[str, Any]] = []
        row = {
            "paper_id": extraction_payload.get("paper_id", ""),
            "run_id": extraction_payload.get("run_id", self.run_id),
        }
        extracted = extraction_payload.get("extracted_data") or {}
        if isinstance(flat_extracted, dict) and flat_extracted:
            for key in fieldnames[2:]:
                row[key] = flat_extracted.get(key, "")
        elif self._extraction_criteria:
            for key in self._extraction_criteria:
                value = extracted.get(key, "")
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False)
                row[key] = value
        else:
            row["extracted_data"] = json.dumps(extracted, ensure_ascii=False)
        csv_rows.append(row)

        # human readable hint: write one canonical extraction CSV with machine-readable dot-path columns.
        extraction_csv_path = output_dir / f"{self.stage}_results.csv"
        with open(extraction_csv_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        stale_csv_path = output_dir / f"{self.stage}_extraction_results.csv"
        if stale_csv_path.exists():
            stale_csv_path.unlink()

        if self._extraction_aggregate_writer is not None:
            # human readable hint: update run-level comparison and quote-audit CSVs as soon as this paper finishes.
            try:
                self._extraction_aggregate_writer.append_record(extraction_payload)
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    paper.paper_id,
                    f"data extraction aggregate table append failed: {exc}",
                    error_type="data_extraction_aggregate_append_failed",
                )

    def _start_data_extraction_aggregate_writer(self) -> None:
        """human readable hint: create live aggregate extraction CSVs before the first paper is screened."""

        try:
            self._extraction_aggregate_writer = ExtractionAggregateWriter(
                output_dir=self.stage_output_dir,
                consensus_path=self.csv_dir / "data_extraction_schema.csv",
                input_paper_dir=self.csv_dir / "per_paper_data_extraction",
                reset=bool(self.qc_only),
            )
            if not self.quiet:
                print(
                    "[extraction] Aggregate tables ready: "
                    f"{self._extraction_aggregate_writer.comparison_path.name}, "
                    f"{self._extraction_aggregate_writer.quote_path.name}"
                )
        except Exception as exc:  # pylint: disable=broad-except
            self._extraction_aggregate_writer = None
            self._log_error(
                "run",
                f"data extraction aggregate table initialization failed: {exc}",
                error_type="data_extraction_aggregate_init_failed",
            )

    def _build_extraction_payload(self, paper: PaperRecord, llm_decision: str | None) -> dict | None:
        """Parse the LLM output into structured extraction data."""

        if not llm_decision:
            return None

        if self._extraction_schema is None:
            raise RuntimeError("data_extraction requires a configured DynamicExtractionSchema.")

        # human readable hint: validate LLM JSON against the same KB-generated Pydantic model sent to OpenAI.
        filtered, validation_error = parse_and_validate(llm_decision, self._extraction_schema)
        if validation_error:
            self._log_error(
                paper.paper_id,
                f"data extraction schema validation failed: {validation_error}",
                error_type="data_extraction_schema_validation_failed",
            )
        return {
            "paper_id": paper.paper_id,
            "run_label": self.run_label,
            "run_id": self.run_id,
            "extracted_data": filtered,
            "extracted_data_flat": flatten_extracted_data(filtered),
            "field_provenance": {},
            "raw_output": llm_decision,
            "schema_kb_path": str(self._extraction_schema.kb_path),
            "schema_validation_error": validation_error,
        }
