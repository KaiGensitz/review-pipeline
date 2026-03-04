import os
from pathlib import Path
from dataclasses import dataclass
from typing import Type, TypeVar, overload

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

# ---------------------------------------------------------------------------
# Edit these each run (minimal inputs for non-coders)
# ---------------------------------------------------------------------------

CURRENT_STAGE = "title_abstract"  # title_abstract | full_text | data_extraction
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")  # API key loaded from .env
LLM_MODEL = "gpt-oss-120b"  # screening model name on your endpoint; working best: "gpt-oss-120b", working very fast: "qwen3-coder-30b-a3b-instruct"
EMBED_MODEL = "qwen3-embedding-0.6b"  # embedding model name on your endpoint; working for sure: "qwen3-embedding-0.6b"
CSV_DIR = REPO_ROOT / "input"  # where you drop Covidence exports
QC_ENABLED = True  # False = skip QC sampling and go straight to full screening
QC_SAMPLE_RATE = 0.05  # 0.0–1.0; 0.10 = ~10% QC sample

# Covidence study tags used for validation (case-insensitive)
STUDY_TAGS_INCLUDE = [
    "no smartphone technology",   # Maps to Concept 1
    "no artificial intelligence", # Maps to Concept 2
    "no physical activity",       # Maps to Outcome
    "not adult population",       # Maps to Population
    "not urban context",          # Maps to Context
    "wrong publication type",     # Commentary, Review, etc.
    "language not en/de",         # Specificity improves rigor
    "full text not available"     # Standard scoping review exclusion
]

# Study tags to ignore (e.g., test/sample markers)
STUDY_TAGS_IGNORE = [
	"title/abstract screening test sample",
	"data extraction test sample",
	"fulltext screening test sample",
	"ongoing study",
	"possible rct",
	"not rct",
]


# STAGE_RULES defines what each phase needs (read this like a checklist):
# - screen_patterns: which CSV files are the main inputs for that phase.
# - neg_patterns: optional CSVs used to add negative examples to the knowledge base.
# - pdf_dir: the folder under input/ that should hold one PDF per paper (None if PDFs are not used).
# Fixed internal behavior (not user-configurable):
# - full_text and data_extraction always create per-paper folders.
# - data_extraction always requires full_text folders to exist.
STAGE_RULES = {
	"title_abstract": {
		# Screening only uses titles/abstracts from the CSV.
		"screen_patterns": ["*_screen_csv_*.csv"],
		"neg_patterns": [],
		"pdf_dir": None,
	},
	"full_text": {
		# Screening uses full-text PDFs (one PDF per folder).
		"screen_patterns": ["*_select_csv_*.csv"],
		"neg_patterns": ["*_irrelevant_csv_*.csv"],
		"pdf_dir": "per_paper_full_text",
	},
	"data_extraction": {
		# Extraction uses full-text PDFs from the included set.
		"screen_patterns": ["*_included_csv_*.csv"],
		"neg_patterns": ["*_excluded_csv_*.csv"],
		"pdf_dir": "per_paper_data_extraction",
	},
}

# Note: most users only change CURRENT_STAGE, API key, and model names above.
# QC is usually required: the run creates a QC sample, asks before QC-only screening,
# and only proceeds to full screening after you confirm validation results.
# Set QC_ENABLED=False to skip QC sampling entirely.

# ---------------------------------------------------------------------------
# Further settings which can (usually) stay as they are
# ---------------------------------------------------------------------------

# API key is loaded from .env; no hardcoded secrets in this file.

# Stage-specific prompt scripts (edit paths only if you rename prompt files)
PROMPT_FILES = {
	"title_abstract": REPO_ROOT / "config" / "prompt_script_title_abstract.txt",
	"full_text": REPO_ROOT / "config" / "prompt_script_full_text.txt",
	"data_extraction": REPO_ROOT / "config" / "prompt_script_data_extraction.txt",
}

if CURRENT_STAGE not in PROMPT_FILES:
	raise RuntimeError(
		f"No prompt mapping for CURRENT_STAGE='{CURRENT_STAGE}'. Update PROMPT_FILES in config/user_orchestrator.py."
	)

PROMPT_FILE = PROMPT_FILES[CURRENT_STAGE]

if not PROMPT_FILE.exists():
	raise FileNotFoundError(
		f"Missing prompt script for CURRENT_STAGE='{CURRENT_STAGE}'. Expected file at: {PROMPT_FILE}."
	)

ELIGIBILITY_CRITERIA_FILES = {
	"title_abstract": REPO_ROOT / "knowledge-base" / "eligibility_criteria.txt",
	"full_text": REPO_ROOT / "knowledge-base" / "eligibility_criteria.txt",
	"data_extraction": None,
}

ELIGIBILITY_CRITERIA_FILE = ELIGIBILITY_CRITERIA_FILES[CURRENT_STAGE]

if ELIGIBILITY_CRITERIA_FILE is not None and not ELIGIBILITY_CRITERIA_FILE.exists():
	raise FileNotFoundError(
		"Missing external eligibility criteria file for CURRENT_STAGE. "
		f"Expected file at: {ELIGIBILITY_CRITERIA_FILE}."
	)

EMBEDDING_SETTINGS = {
	"gpustack_embedding_model": EMBED_MODEL,  # embedding model; affects relevance ranking in all stages
	"use_api_embeddings": True,  # True = use API embeddings; False would disable embedding-based selection
	"gpustack_base_url": "https://gpustack.unibe.ch/v1",  # embedding endpoint URL; must match your server
	"data_language": "auto_first",  # "english" | "german" | "auto" | "auto_first"; auto_first = detect once per paper, then reuse
	"chunk_size": 20,  # sentences per chunk; larger = fewer chunks, cheaper but less granular evidence (increase slightly for throughput)
	"overlap_size": 2,  # sentences overlapped; higher = better continuity but more duplicate cost
	"embedding_cache_size": 2048,  # cached embeddings in RAM; higher = faster, more memory
}

LLM_SETTINGS = {
	"screening_model": LLM_MODEL,  # LLM used for decisions/extraction in all stages
	"use_api": True,  # True = call API; False would skip LLM calls (not recommended)
	"gpustack_base_url": "https://gpustack.unibe.ch/v1",  # LLM endpoint URL; must match your server
	"prompt_path": str(PROMPT_FILE),  # stage-specific prompt; changes decision logic per stage
	"max_tokens": 2048,  # response length cap; too low can truncate JSON, too high costs more
	"temperature": 0.0,  # randomness; lower = more stable decisions, higher = more variable
	"top_p": 1.0,  # keep at 1.0 with temperature=0.0 for stable decoding behavior
	"seed": 42,  # reproducibility seed (set an integer number like 42 to stabilize provider-side sampling)
}

# Screening knobs (advanced; keep defaults unless you know why to change)
SCREENING_DEFAULTS = {
	"top_k": 10,  # non-title chunks kept; higher = more evidence, higher cost (lower top_k for faster runs)
	"score_threshold": 0.005,  # minimum relevance score; higher = stricter filtering
	"sample_size": None,  # limit papers per run; set for pilots in any stage
	"sample_seed": None,  # fixed seed for deterministic sampling when sample_size is set
	"batch_size": 32,  # embedding batch size; higher = faster, more memory
	"title_abstract_workers": 1, # controls how many title/abstract papers are sent to the LLM at once; practical rule: keep 1 for maximum stability; use 2-4 only when you need speed and your API endpoint is stable.
	"sustainability_tracking": True,  # True = write resource logs; False = no tracking
	"enable_time_savings": True,  # True = compute human-time savings when QC minutes exist; set False to skip
}

# CodeCarbon configuration (all tunable parameters live here)
CARBON_CONFIG = {
	"project_name": "review_pipeline",  # label used by CodeCarbon in all stages
	"output_dir": str(REPO_ROOT / "output" / CURRENT_STAGE),  # where emissions logs are written
	"measure_power_secs": 60,  # sampling interval in seconds; lower = more detail, more overhead
	"tracking_mode": "process",  # "machine" = whole device; "process" = this run only
	"on_csv_write": "append",  # "append" keeps a history; "update" overwrites totals
	"is_offline": False,  # True uses offline factors; requires country_iso_code
	"country_iso_code": "CHE",  # used only when offline; impacts emissions factors
}

PATH_SETTINGS = {
	"csv_dir": str(CSV_DIR),  # input folder for stage CSVs (all stages)
	"prompt_file": str(PROMPT_FILE),  # resolved prompt file path for CURRENT_STAGE
	"eligibility_criteria_file": str(ELIGIBILITY_CRITERIA_FILE) if ELIGIBILITY_CRITERIA_FILE else "",  # resolved criteria file path for CURRENT_STAGE (optional)
	"eligibility_criteria_files": {stage: (str(path) if path else "") for stage, path in ELIGIBILITY_CRITERIA_FILES.items()},  # stage->criteria file mapping (optional)
	# Root folder for outputs; files will be placed under output/<stage>/...
	"output_root": str(REPO_ROOT / "output"),  # base output folder for all stages
}

# Human reviewer timing (per stage)
# - reviewers: optional self-reported time on the quality control set. Enter total_minutes per person (rough guess is fine).
# - tip: hours × 60 = minutes (e.g., 2 hours ≈ 120 minutes). If you do not track time, leave 0 so the tool will skip time-savings and note that no human minutes were provided.
# - you can add or delete reviewer rows per stage: each entry refers to one person. The pipeline only reads reviewers for the active CURRENT_STAGE, ignores zero-minute entries, and averages the remaining minutes-per-paper to estimate human time and time-savings via the pipeline.
HUMAN_TIME_CONFIG = {
	"title_abstract": {
		"reviewers": [
			{"id": "human_1", "total_minutes": 110}, # Marc for 223 articles (Email 12.02.2026): 1h 50 min = 110 min
			{"id": "human_2", "total_minutes": 240}, # Shawan for 223 articles (Slack 19.02.2026): 4h = 240 min
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
	"full_text": {
		"reviewers": [
			{"id": "human_1", "total_minutes": 0},
			{"id": "human_2", "total_minutes": 0},
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
	"data_extraction": {
		"reviewers": [
			{"id": "human_1", "total_minutes": 0},
			{"id": "human_2", "total_minutes": 0},
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
}


T = TypeVar("T")


@overload
def require_setting(container: dict, key: str, container_name: str) -> object:
	...


@overload
def require_setting(container: dict, key: str, container_name: str, expected_type: Type[T]) -> T:
	...


def require_setting(container: dict, key: str, container_name: str, expected_type: Type[T] | None = None) -> object:
	"""Fetch a required setting from a config dict and warn if missing.

	Args:
		container: Settings dictionary (e.g., LLM_SETTINGS).
		key: Key to look up in the settings dict.
		container_name: Human-readable container name for warnings.

	Returns:
		The value stored under the key.

	Note: missing settings stop the run so you can fix the config.
	"""
	if key not in container:
		print(
			f"[warning] Missing required setting '{key}' in {container_name}. "
			"Add it to config/user_orchestrator.py before running the pipeline."
		)
		raise KeyError(f"Missing required setting '{key}' in {container_name}.")
	value = container[key]
	if expected_type is not None and not isinstance(value, expected_type):
		print(
			f"[warning] Setting '{key}' in {container_name} must be {expected_type.__name__}; "
			f"got {type(value).__name__}. Fix config/user_orchestrator.py and rerun."
		)
		raise TypeError(f"Invalid type for '{key}' in {container_name}.")
	return value


@dataclass(frozen=True)
class UserConfig:
	"""Static snapshot of user-facing settings for the current run.

	Note: this bundles all inputs so other scripts can read a single object.
	"""

	current_stage: str
	llm_api_key: str
	llm_model: str
	embed_model: str
	csv_dir: Path
	qc_enabled: bool
	qc_sample_rate: float
	stage_rules: dict
	prompt_file: Path
	embedding_settings: dict
	llm_settings: dict
	screening_defaults: dict
	carbon_config: dict
	path_settings: dict
	human_time_config: dict


def load_user_config() -> UserConfig:
	"""Build and validate a UserConfig from module globals (one call per run).

	Note: you do not edit this function; it just packages the values above.
	"""

	if CURRENT_STAGE not in STAGE_RULES:
		raise RuntimeError(f"Unknown CURRENT_STAGE='{CURRENT_STAGE}'. Update STAGE_RULES in config/user_orchestrator.py.")
	if CURRENT_STAGE not in PROMPT_FILES:
		raise RuntimeError(
			f"No prompt mapping for CURRENT_STAGE='{CURRENT_STAGE}'. Update PROMPT_FILES in config/user_orchestrator.py."
		)
	if not PROMPT_FILE.exists():
		raise FileNotFoundError(f"Missing prompt script for CURRENT_STAGE='{CURRENT_STAGE}'. Expected file at: {PROMPT_FILE}.")
	if QC_SAMPLE_RATE < 0 or QC_SAMPLE_RATE > 1:
		raise ValueError("QC_SAMPLE_RATE must be between 0.0 and 1.0 (e.g., 0.10 for ~10%).")
	if not LLM_API_KEY:
		raise RuntimeError("LLM_API_KEY is empty. Add it to .env or set it before running the pipeline.")

	return UserConfig(
		current_stage=CURRENT_STAGE,
		llm_api_key=LLM_API_KEY,
		llm_model=LLM_MODEL,
		embed_model=EMBED_MODEL,
		csv_dir=CSV_DIR,
		qc_enabled=QC_ENABLED,
		qc_sample_rate=QC_SAMPLE_RATE,
		stage_rules=STAGE_RULES,
		prompt_file=PROMPT_FILE,
		embedding_settings=EMBEDDING_SETTINGS,
		llm_settings=LLM_SETTINGS,
		screening_defaults=SCREENING_DEFAULTS,
		carbon_config=CARBON_CONFIG,
		path_settings=PATH_SETTINGS,
		human_time_config=HUMAN_TIME_CONFIG,
	)