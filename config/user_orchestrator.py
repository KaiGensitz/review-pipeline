import os
from pathlib import Path
from dataclasses import dataclass
from typing import Type, TypeVar, overload

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

# ---------------------------------------------------------------------------
# USER-EDITABLE RUN SETTINGS
# ---------------------------------------------------------------------------

CURRENT_STAGE = "data_extraction"  # user-editable: title_abstract | full_text | data_extraction
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")  # API key loaded from .env
LLM_MODEL = "gpt-oss-120b"  # screening model name on your endpoint; working for sure: "gpt-oss-120b" (17.04.2026)
EMBED_MODEL = "qwen3-embedding-0.6b"  # embedding model name on your endpoint; working for sure: "qwen3-embedding-0.6b" (17.04.2026)
CSV_DIR = REPO_ROOT / "input"  # where you drop Covidence exports
QC_ENABLED = True  # False = skip QC sampling and go straight to full screening
QC_SAMPLE_RATE = 0.05  # 0.0–1.0; 0.10 ~10% QC sample
CITATION_SEARCHING_SCREENING = False  # True = use citation-search CSV patterns and skip QC sampling

# USER-EDITABLE STUDY TAGS.
# human readable hint: these tags encode the current protocol's exclusion reasons.
# When the review topic changes, update these labels and the prompt/KB files together.
STUDY_TAGS_INCLUDE = [
    "no smartphone technology",   # Maps to Intervention
    "no artificial intelligence", # Maps to Phenomenon
    "no physical activity",       # Maps to Outcome
    "not adult population",       # Maps to Population
    "not urban context",          # Maps to Context
    "wrong publication type",     # Commentary, Review, etc.
    "language not en/de",         # Specificity improves rigor
    "full text not available",    # Standard scoping review exclusion
	"No intervention"			  # no (realworld) intervention
]

# USER-EDITABLE TAGS TO IGNORE.
# human readable hint: these labels are metadata/test markers, not scientific exclusion reasons.
STUDY_TAGS_IGNORE = [
	"title/abstract screening test sample",
	"title/abstract screening test sample - validation",
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
		"screen_patterns": ["*_included_csv_*.csv", "citationSearching_data-extraction_*.csv"],
		"neg_patterns": ["*_excluded_csv_*.csv"],
		"pdf_dir": "per_paper_data_extraction",
	},
}

# USER-EDITABLE CITATION-SEARCH SCREENING SETTINGS.
# human readable hint: citation-search screening is a separate no-QC workflow.
# Set CITATION_SEARCHING_SCREENING=True and CURRENT_STAGE to the stage you want.
CITATION_SEARCHING_STAGE_RULES = {
	"title_abstract": {
		"screen_patterns": ["citationSearching_title-abstract_*.csv"],
		"output_dir": "title_abstract_citationSearching",
	},
	"full_text": {
		"screen_patterns": ["citationSearching_full-text_*.csv"],
		"pdf_dir": "per_paper_full_text",
		"output_dir": "full_text_citationSearching",
	},
	"data_extraction": {
		"screen_patterns": ["citationSearching_data-extraction_*.csv"],
		"pdf_dir": "per_paper_data_extraction",
		"output_dir": "data_extraction_citationSearching",
	},
}

# Note: most users only change CURRENT_STAGE, API key, and model names above.
# QC is usually required: the run creates a QC sample, asks before QC-only screening,
# and only proceeds to full screening after you confirm validation results.
# Set QC_ENABLED=False to skip QC sampling entirely.

# Human reviewer timing (per stage)
# - reviewers: optional self-reported time on the quality control set. Enter total_minutes per person (rough guess is fine).
# - tip: hours × 60 = minutes (e.g., 2 hours ≈ 120 minutes). If you do not track time, leave 0 so the tool will skip time-savings and note that no human minutes were provided.
# - you can add or delete reviewer rows per stage: each entry refers to one person. The pipeline only reads reviewers for the active CURRENT_STAGE, ignores zero-minute entries, and averages the remaining minutes-per-paper to estimate human time and time-savings via the pipeline.
HUMAN_TIME_CONFIG = {
	"title_abstract": {
		"reviewers": [
			{"id": "human_1", "total_minutes": 50}, # Marc for 223 articles (Email 12.02.2026): 1h 50 min = 110 min
			{"id": "human_2", "total_minutes": 110}, # Shawan for 223 articles (Slack 19.02.2026): 4h = 240 min
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
	"full_text": {
		"reviewers": [
			{"id": "human_1", "total_minutes": 270}, # Reviewer 1 for 50 articles (Email 09.04.2026): 4.5 h = 270 min
			{"id": "human_2", "total_minutes": 300}, # Reviewer 2 for 50 articles (Slack): 5 h = 300 min
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
	"data_extraction": {
		"reviewers": [
			{"id": "human_1", "total_minutes": 75}, # Reviewer 1 for 2 articles (Email 08.05.2026): ca. 75 Min
			{"id": "human_2", "total_minutes": 155}, # Reviewer 2 for 2 articles (Email 08.05.2026): ca. 40 min per paper for reading, 15 min per paper for controlling, and ca 45 min extra for in-depth review of 1 article = 155 min
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
}

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

# USER-EDITABLE DATA-EXTRACTION SCHEMA.
# human readable hint: this CSV defines extraction variables, types, instructions, and Covidence headers.
DATA_EXTRACTION_SCHEMA_FILE = REPO_ROOT / "knowledge-base" / "data_extraction_schema.csv"

# USER-EDITABLE CSV METADATA COLUMN ALIASES.
# human readable hint: external CSV exports use study-specific/admin-specific column names.
# The pipeline uses generic internal keys; edit these aliases when your export headers differ.
CSV_METADATA_COLUMN_ALIASES = {
	"paper_id": ["paper_id", "Covidence #", "Covidence#", "covidence id", "covidence_id", "covidence number", "Ref", "Study", "ID", "id"],
	"title": ["title", "Title"],
	"abstract": ["abstract", "Abstract"],
	"authors": ["authors", "Authors", "author", "Author"],
	"publication_year": ["publication_year", "year", "Year", "Published Year", "Year of Publication", "Publication Year", "publication year", "PublicationYear", "PublishedYear", "PubYear", "PY", "date"],
	"publication_month": ["publication_month", "month", "Month", "Published Month", "Published month"],
	"journal": ["journal", "Journal", "Source", "Source Title", "Publication Title"],
	"volume": ["volume", "Volume"],
	"issue": ["issue", "Issue"],
	"pages": ["pages", "Pages", "Page", "page", "Page range", "Page Range"],
	"accession_number": ["accession_number", "Accession Number", "AccessionNumber", "Accession", "WOS Accession Number"],
	"doi": ["doi", "DOI", "Doi"],
	"reference": ["reference", "Reference", "Ref"],
	"study_id": ["study_id", "Study ID", "Study"],
	"notes": ["notes", "Notes"],
	"tags": ["tags", "Tags", "Keywords", "keywords", "label", "labels"],
	"reviewer_name": ["Reviewer Name", "reviewer_name", "reviewer"],
}

# USER-EDITABLE DATA-EXTRACTION PROMPT ALIASES.
# human readable hint: optional bridge terms that connect prompt sections to schema domains.
# Keep these current-study terms here, not in pipeline/ Python files.
DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES = {
	"study_details": ["study metadata", "study characteristics", "first author", "author year", "study design", "duration", "funding"],
	"population": ["population", "sample size", "mean age", "gender overall", "ethnicity overall", "health status overall", "demographics", "baseline table"],
	"context": ["urban context", "urban", "setting", "built environment", "location", "real-world", "metropolitan"],
	"outcomes": ["outcomes", "physical activity", "primary pa outcome", "pa outcome", "mvpa", "step count"],
	"concepts": ["rq1", "rq2", "rq3", "rq4", "ai & tech", "ai and technology", "architecture", "smartphone", "sensing", "psychosocial", "behavior change", "inclusivity", "ethics", "sustainability"],
	"synthesis": ["synthesis", "findings", "implications", "limitations", "notes"],
}

# USER-EDITABLE DATA-EXTRACTION EXPORT SETTINGS.
# human readable hint: these describe the human consensus/export table layout and AI reviewer label.
DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS = {
	"comparison_default_headers": ["Covidence #", "Title"],
	"quote_audit_headers": ["Covidence #", "Title", "Domain", "Variable", "Consensus_Column", "AI_Value", "AI_Quote"],
	"paper_id_column": "Covidence #",
	"title_column": "Title",
	"quote_audit_domain_column": "Domain",
	"quote_audit_variable_column": "Variable",
	"quote_audit_consensus_column": "Consensus_Column",
	"quote_audit_value_column": "AI_Value",
	"quote_audit_quote_column": "AI_Quote",
	"reviewer_name_column": "Reviewer Name",
	"reviewer_name_value": "AI",
	"study_id_column": "Study ID",
	"authors_column": "authors",
	"publication_year_column": "publication_year",
}

# USER-EDITABLE FALLBACK HEADER ALIASES FOR EXTRACTION VARIABLES.
# human readable hint: exact covidence_column_name in the schema CSV is tried first; these aliases are optional fallbacks.
DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES = {
	"population.mean_age": ["Age", "Mean age", "Mean age Overall", "Mean age (years) ± SD Overall"],
	"population.sample_size": ["Sample_size", "Sample size", "Sample size Overall"],
	"population.gender_overall": ["Gender", "Gender Overall"],
	"population.ethnicity_overall": ["Ethnicity", "Ethnicity Overall"],
	"population.health_status": ["Health_status", "Health status", "Health status Overall"],
	"population.country_overall": ["country", "Country", "Country Overall"],
	"outcomes.reported": ["outcomes_reported"],
}

# USER-EDITABLE DATA-EXTRACTION VALIDATION VALUE ALIASES.
# human readable hint: groups of semantically equivalent validation values; stats_engine uses these only after human ground truth has been built.
DATA_EXTRACTION_VALIDATION_VALUE_ALIASES = {
	"context.evidence_source": [
		["peer-reviewed", "peer reviewed", "peer-reviewed journal article", "journal article", "original article", "original investigation"],
		["conference paper", "conference proceeding", "proceedings paper"],
		["pre-print", "preprint"],
	],
	"synthesis.key_findings": [
		["keine ergebnisse vorhanden", "keine finalen ergebnisse", "not yet available", "no final outcome findings", "protocol"],
	],
	"concepts.inclusivity_considerations": [
		["nicht gesondert ausgewertet", "not separately evaluated", "not separately analysed", "not separately analyzed"],
	],
	"population.mean_age": [
		["45-64 years", "45 to 64 years", "45-64 age group", "45-64 age band", "45-64 years: 28/36", "45-64 years 28 36 77.8%"],
	],
	"outcomes.reported": [
		["physical activity", "pa outcome", "physical-activity outcome", "exercise behavior", "exercise adherence", "steps", "step count", "mvpa", "walking"],
		["feasibility/usability", "feasibility and usability", "feasibility", "usability", "engagement", "acceptability"],
	],
	"concepts.smartphone_usage": [
		["mobile app", "smartphone app", "native app", "android app", "ios app", "phone app", "companion app"],
		["chatbot", "chat bot", "conversational agent", "voice assistant", "voice-assistant companion app"],
		["sms/text messages", "sms", "text message", "text messages", "push notification", "push notifications", "reminder", "reminders"],
		["wearable sync", "wearable synchronization", "wearable-to-phone", "activity tracker sync", "data transmission"],
		["avatar selection", "badges/profile", "digital badges", "profile", "leaderboard", "leaderboards"],
	],
	"concepts.AI_transparency": [
		["not available", "not reported", "not described", "no explicit ai transparency", "no transparency information", "explainability not reported", "interpretability not reported"],
	],
	"concepts.human_AI_interaction": [
		["chatbot query", "chatbot conversation", "chat bot conversation", "conversational interaction", "user queries"],
		["voice command", "voice assistant", "voice interaction", "voice-based interaction"],
		["avatar selection", "goal setting", "audio feedback", "leaderboard", "leaderboards", "self-monitoring", "recommendation response", "alert response"],
		["adaptive recommendation", "adaptive recommendations", "model-generated prompts", "ai feedback", "ai-selected goals"],
	],
	"concepts.development_process": [
		["design and development process described", "design/development process", "technology development", "app development", "app implementation", "technical integration"],
		["user-centered design", "user centred design", "co-design", "participatory design", "stakeholder involvement", "user testing", "prototype testing", "iterative prototyping"],
		["model training", "training-data construction", "dataset creation", "expert annotation"],
	],
	"context.setting": [
		["both", "urban and rural", "urban/suburban plus rural", "urban/suburban and rural/suburban", "urban area and rural area"],
		["home plus clinic", "home and clinic", "rehabilitation clinic", "clinical site", "clinic"],
		["university", "workplace", "community", "field-study", "field study", "metropolitan trial site"],
	],
	"context.continent": [
		["europe", "eu", "european"],
		["north america", "usa", "united states", "canada"],
		["asia", "asian"],
		["australia", "oceania"],
	],
}

# USER-EDITABLE DATA-EXTRACTION VALIDATION MATCH SETTINGS.
# human readable hint: reviewer gold-standard prose and AI prose may be factually congruent without identical wording.
DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS = {
	"quote_aware_match_in_metrics": True,
	"quote_aware_compare_ai_quote": True,
	"quote_aware_compare_reviewer_note": True,
	"count_fuzzy_matches_in_metrics": False,
	"quote_aware_free_text_token_overlap_threshold": 0.42,
	"quote_aware_list_token_overlap_threshold": 0.35,
	"quote_aware_short_text_token_overlap_threshold": 0.50,
	"free_text_token_overlap_threshold": 0.80,
	"list_token_overlap_threshold": 0.80,
	"short_text_token_overlap_threshold": 0.80,
	"minimum_token_count_for_fuzzy": 2,
	"numeric_relative_tolerance": 0.02,
}

# USER-EDITABLE DATA-EXTRACTION QUOTE EXPORT ALIASES.
# human readable hint: optional wide-table quote columns are human layout choices.
# The long quote-audit table always contains every schema variable quote; these aliases fill selected quote columns in consensus-style exports.
DATA_EXTRACTION_QUOTE_COLUMN_ALIASES = {
	"study_details.study_design": ["study_design_quote"],
	"population.sample_size": ["sample_quote"],
	"context.setting": ["setting_quote"],
	"context.data_collection_method": ["data_collection_quote"],
}

# USER-EDITABLE DATA-EXTRACTION EVIDENCE-HINT ALIASES.
# human readable hint: these terms help the generic snippet selector find review-relevant evidence in long normalized PDFs.
# Keep review-topic vocabulary here, in prompts, or in the schema CSV rather than in pipeline/ Python files.
DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES = {
	"population.mean_age": ["age", "aged", "ages", "years", "year-old", "age group", "age groups", "18-25", "26-35", "36-45", "eligibility", "inclusion", "criteria", "baseline characteristics", "participant characteristics"],
	"population.gender_overall": ["gender", "sex", "female", "male", "women", "men", "baseline characteristics", "participant characteristics", "demographic", "demographics"],
	"population.sample_size": [
		"sample size",
		"target sample",
		"final sample",
		"planned sample",
		"power calculation",
		"power analysis",
		"powered to",
		"participants",
		"participant",
		"n =",
		"eligible",
		"enroll",
		"enrolled",
		"enrollment",
		"consented",
		"recruited",
		"recruitment",
	],
	"population.ethnicity_overall": ["ethnicity", "ethnic", "race", "racial", "hispanic", "latino", "not hispanic", "declined", "unknown", "baseline characteristics", "participant characteristics", "demographic", "demographics"],
	"population.country_overall": ["country", "countries", "site", "clinic", "recruitment", "residing"],
	"context.setting": ["country", "countries", "setting", "site", "clinic", "clinical", "hospital", "home", "homes", "in-home", "rehabilitation", "university", "workplace", "community", "urban", "suburban", "rural", "metropolitan", "recruitment", "residing", "location"],
	"context.evidence_source": [
		"journal",
		"published",
		"published online",
		"doi",
		"original investigation",
		"article",
		"peer-reviewed",
		"preprint",
		"conference",
		"proceedings",
		"trial registration",
		"thesis",
		"dissertation",
		"report",
	],
	"study_details.funding_sources": ["funding", "funded", "grant", "support", "sponsor"],
	"study_details.conflicts_of_interest": ["conflict", "interest", "competing", "disclosure", "advisory", "consultant", "consulting", "patent", "employment", "salary", "stock", "royalties", "grant", "company"],
	"concepts.AI_model": ["model", "algorithm", "agent", "system", "decision", "recommendation"],
	"concepts.AI_transparency": ["transparent", "transparency", "explain", "explainable", "interpretability", "interpretable", "black-box", "feedback", "expected", "performance", "actual performance", "health belief", "rationale", "decision rule", "algorithm reads", "goal-based", "pose", "poses", "training", "dataset", "learned knowledge", "model training", "NumPy", "OpenCV"],
	"concepts.ethical_considerations": ["ethics", "ethical", "approval", "consent", "privacy", "safety"],
	"concepts.sustainability_considerations": [
		"sustainability",
		"sustainable",
		"environmental",
		"battery",
		"low-power",
		"power consumption",
		"energy consumption",
		"energy-efficient",
		"resource-efficient",
		"green AI",
		"pro-nature",
	],
	"concepts.behavioral_theory": ["theory", "framework", "model", "construct"],
	"concepts.behavioral_strategies": ["strategy", "strategies", "technique", "intervention", "component"],
	"concepts.inclusivity_considerations": ["inclusivity", "inclusive", "accessibility", "equity", "diversity", "cultural diversity", "race", "ethnicity", "gender", "sex", "age", "language", "Android-only", "device ownership", "under-represented", "underserved", "generalizability"],
	"synthesis.key_findings": ["finding", "findings", "outcome", "results", "effect", "change"],
	"synthesis.limitations": ["limitation", "limitations", "future", "caution", "cultural diversity", "music", "music personalization", "selected music", "platform", "Android-only", "short duration", "small sample", "nonresponse", "generalizability"],
	"concepts.development_process": ["development", "design", "iterative", "prototype", "testing", "literature review", "expert opinion", "message content", "pose", "poses", "dataset", "training", "Flutter", "NumPy", "OpenCV", "DeepMotion", "user testing"],
	"concepts.sensing_modalities": ["sensor", "sensors", "sensing", "wearable", "device", "monitor"],
	"study_details.study_design": ["feasibility", "usability", "pilot", "acceptability", "protocol", "trial design", "study design", "user study", "mixed methods", "randomized", "randomised"],
	"concepts.smartphone_usage": ["android", "ios", "app", "mobile app", "companion app", "cellphone application", "SMS", "text message", "text messages", "Coachtext", "push notification", "chatbot", "schedule", "badges", "profile", "coaching", "reminders", "wearable", "sync"],
	"concepts.human_AI_interaction": ["chat", "chatbot", "interface", "avatar", "goal setting", "audio feedback", "leaderboard", "self-monitoring", "recommendation"],
	"context.data_collection_method": ["questionnaire", "questionnaires", "interview", "interviews", "redcap", "survey", "app use", "app logs"],
	"synthesis.implications": ["future", "recommend", "recommendations", "incorporate", "personalization", "multimodal", "adaptive", "feedback", "gamification", "persuasive", "cultural diversity", "music personalization"],
}

# USER-EDITABLE DATA-EXTRACTION EVIDENCE-HINT LOW-PRIORITY PATTERNS.
# human readable hint: these generic source zones often contain affiliations, references, licenses, or PDF boilerplate rather than participant evidence.
DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_LOW_PRIORITY_PATTERNS = [
	"author affiliation",
	"corresponding author",
	"e-mail address",
	"downloaded on",
	"licensed use",
	"restrictions apply",
	"references",
	"bibliography",
	"copyright",
	"all rights reserved",
	"answer categories",
	"number of items",
]

# USER-EDITABLE DATA-EXTRACTION SUPPLEMENTAL CITED EVIDENCE SETTINGS.
# human readable hint: cited source texts stay outside pipeline code; put user-supplied protocol/development/source evidence in one of these per-paper subfolders.
DATA_EXTRACTION_SUPPLEMENTAL_CITED_EVIDENCE = {
	"enabled": True,
	"folder_names": ["supplemental_cited_evidence", "cited_evidence", "supplemental_evidence"],
	"file_globs": ["*.txt", "*.md", "*.pdf"],
	"max_files_per_paper": 8,
	"max_words_per_file": 4000,
}

# USER-EDITABLE DATA-EXTRACTION EXPERT OVERSIGHT SETTINGS.
# human readable hint: this validation round is AI-first extraction with expert human oversight, not AI-vs-human gold standard extraction.
DATA_EXTRACTION_EXPERT_REVIEW_SETTINGS = {
	"source_output_dir": REPO_ROOT / "output" / "data_extraction_v6",
	"packet_output_dir": REPO_ROOT / "output" / "data_extraction_v6" / "expert_review_packets",
	"sample_mode": "all_papers_in_output",
	"include_shared_methodological_variables_in_each_packet": True,
	"review_decision_options": ["accept", "correct", "mark_unavailable", "unclear"],
	"error_type_options": ["missed_data", "misallocated_data", "fabricated_or_unsupported_data", "formatting_or_type_issue"],
	"error_effect_options": ["inconsequential", "minor", "major"],
	"prompt_refinement_trigger_decisions": ["correct", "mark_unavailable"],
	"prompt_refinement_trigger_error_effects": ["major"],
}

# USER-EDITABLE DATA-EXTRACTION EXPERT REVIEWERS.
# human readable hint: reviewer names and review-topic assignments live here, not in pipeline Python files.
DATA_EXTRACTION_EXPERT_REVIEWERS = {
	"ai_technology_expert": {
		"display_name": "Shawan",
		"variables": [
			"concepts.AI_architecture",
			"concepts.AI_model",
			"concepts.AI_transparency",
			"concepts.AI_input_features",
			"concepts.human_AI_interaction",
			"concepts.sensing_modalities",
			"concepts.smartphone_usage",
			"concepts.ethical_considerations",
		],
	},
	"psychology_theory_expert": {
		"display_name": "Marc",
		"variables": [
			"concepts.behavioral_theory",
			"concepts.behavioral_strategies",
			"concepts.human_AI_interaction",
			"concepts.inclusivity_considerations",
		],
	},
}

# USER-EDITABLE SHARED DATA-EXTRACTION OVERSIGHT VARIABLES.
# human readable hint: table-sensitive or recurrent-risk fields can be included in every expert packet.
DATA_EXTRACTION_EXPERT_REVIEW_SHARED_VARIABLES = [
	"population.mean_age",
	"population.sample_size",
	"population.gender_overall",
	"population.health_status",
	"study_details.study_design",
]

# USER-EDITABLE PROMPT SIGNAL SECTION ALIASES.
# human readable hint: used only when prompt sections contain "- Include:" / "- Exclude:" lists for retrieval signals.
PROMPT_SIGNAL_SECTION_ALIASES = {
	"primary": ["intervention / exposure", "intervention/exposure", "intervention", "exposure"],
	"secondary": ["outcome", "outcomes"],
}

# Stage-specific knowledge-base (KB) files.
# - KNOWLEDGE_BASE_FILES holds default KB paths per stage.
# - KB_FILE_OVERRIDES optionally swaps a stage KB for a single run.
# - Override paths may be absolute or relative to REPO_ROOT.
# - Optional full_text cleaned-hybrid draft can be generated with:
#   python -m pipeline.additions.generate_cleaned_hybrid_kb_draft
KNOWLEDGE_BASE_FILES = {
	"title_abstract": REPO_ROOT / "knowledge-base" / "title_abstract_pos-neg_examples.csv",
	"full_text": REPO_ROOT / "knowledge-base" / "full_text_pos-neg_examples.csv",
	"data_extraction": REPO_ROOT / "knowledge-base" / "data_extraction_pos-neg_examples.csv",
}

# Optional full_text draft assets generated by the cleaned-hybrid utility.
FULL_TEXT_CLEANED_HYBRID_DRAFT = (
	REPO_ROOT / "knowledge-base" / "full_text_pos-neg_examples_cleaned_hybrid_draft.csv"
)
FULL_TEXT_CLEANED_HYBRID_DRAFT_REPORT = (
	REPO_ROOT
	/ "knowledge-base"
	/ "full_text_pos-neg_examples_cleaned_hybrid_draft_report.json"
)

# To select the KB file manually for each stage, replace None with:
# REPO_ROOT / "knowledge-base" / <file_of_interest>
KB_FILE_OVERRIDES: dict[str, str | Path | None] = {
	"title_abstract": None,
	"full_text": None,
	"data_extraction": None,
}


def _resolve_repo_path(path_value: str | Path) -> Path:
	"""Resolve a possibly relative path against REPO_ROOT."""

	path_obj = Path(path_value)
	return path_obj if path_obj.is_absolute() else (REPO_ROOT / path_obj)


for override_stage in KB_FILE_OVERRIDES:
	if override_stage not in KNOWLEDGE_BASE_FILES:
		raise RuntimeError(
			f"Unknown stage '{override_stage}' in KB_FILE_OVERRIDES. "
			f"Expected one of {sorted(KNOWLEDGE_BASE_FILES)}."
		)

EFFECTIVE_KNOWLEDGE_BASE_FILES: dict[str, Path] = {}
for stage_name, default_path in KNOWLEDGE_BASE_FILES.items():
	override_path = KB_FILE_OVERRIDES.get(stage_name)
	if override_path:
		EFFECTIVE_KNOWLEDGE_BASE_FILES[stage_name] = _resolve_repo_path(override_path)
	else:
		EFFECTIVE_KNOWLEDGE_BASE_FILES[stage_name] = _resolve_repo_path(default_path)

if CURRENT_STAGE not in PROMPT_FILES:
	raise RuntimeError(
		f"No prompt mapping for CURRENT_STAGE='{CURRENT_STAGE}'. Update PROMPT_FILES in config/user_orchestrator.py."
	)

if CURRENT_STAGE not in EFFECTIVE_KNOWLEDGE_BASE_FILES:
	raise RuntimeError(
		f"No knowledge-base mapping for CURRENT_STAGE='{CURRENT_STAGE}'. "
		"Update KNOWLEDGE_BASE_FILES/KB_FILE_OVERRIDES in config/user_orchestrator.py."
	)

PROMPT_FILE = PROMPT_FILES[CURRENT_STAGE]
KNOWLEDGE_BASE_FILE = EFFECTIVE_KNOWLEDGE_BASE_FILES[CURRENT_STAGE]

if not PROMPT_FILE.exists():
	raise FileNotFoundError(
		f"Missing prompt script for CURRENT_STAGE='{CURRENT_STAGE}'. Expected file at: {PROMPT_FILE}."
	)

if not KNOWLEDGE_BASE_FILE.exists():
	raise FileNotFoundError(
		f"Missing knowledge-base file for CURRENT_STAGE='{CURRENT_STAGE}'. "
		f"Expected file at: {KNOWLEDGE_BASE_FILE}."
	)

if CURRENT_STAGE == "data_extraction" and not DATA_EXTRACTION_SCHEMA_FILE.exists():
	raise FileNotFoundError(
		"Missing data-extraction schema CSV for CURRENT_STAGE='data_extraction'. "
		f"Expected file at: {DATA_EXTRACTION_SCHEMA_FILE}."
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
	"context_window_total_tokens": 78000,  # model context window (input + output combined); set per model
	"max_tokens": 10000,  # response length cap; too low can truncate JSON, too high costs more
	"data_extraction_split_by_domain": True,  # True = validate smaller schema batches; False = one fragile full-schema response
	"data_extraction_domain_groups": [["study_details"], ["population"], ["outcomes"], ["context"], ["concepts"], ["synthesis"]],  # optional schema-domain batches; population and context stay separate because they are evidence-location-sensitive
	"data_extraction_response_format_mode": "prompt_only",  # prompt_only avoids broken json_schema handling on some GPUSstack models
	"data_extraction_domain_max_tokens": 5000,  # per-domain output cap; quote-heavy domains need room to finish valid JSON
	"data_extraction_semantic_rag_enabled": False,  # True = direct run_extraction ranks chunks by schema semantic_anchors before prompting
	"data_extraction_semantic_top_k": 28,  # number of semantic chunks sent to the LLM in direct run_extraction; study protocol proposes top_k = 10 for screening; data extraction schema contains 35 unique variables
	"data_extraction_semantic_score_threshold": 0.005,  # optional minimum semantic score; None keeps top_k regardless of absolute score
	"data_extraction_hybrid_rescue_enabled": False,  # True = keep full_text primary, then run semantic second-opinion rescue for configured variables/domains
	"data_extraction_hybrid_rescue_top_k": 28,  # semantic chunks used only for the rescue/supplement layer, not as primary extraction evidence
	"data_extraction_hybrid_rescue_score_threshold": 0.005,  # optional rescue threshold; keep aligned with calibrated semantic tests unless QC suggests otherwise
	"data_extraction_hybrid_rescue_variables": [
		"concepts.AI_model",
		"concepts.AI_transparency",
		"concepts.sensing_modalities",
		"concepts.behavioral_strategies",
		"concepts.development_process",
		"concepts.ethical_considerations",
		"concepts.inclusivity_considerations",
		"concepts.sustainability_considerations",
	],  # user-editable detail-sensitive fields for semantic second opinion
	"data_extraction_hybrid_rescue_domains": ["context", "synthesis"],  # user-editable domains where a semantic second opinion is useful even when the primary value is present
	"data_extraction_hybrid_full_text_preferred_variables": [
		"population.sample_size",
		"population.mean_age",
		"population.gender_overall",
		"population.ethnicity_overall",
		"population.health_status",
	],  # table-sensitive fields keep full-text table evidence unless semantic quote support is clearly stronger
	"data_extraction_evidence_mode": "full_text",  # full_text = use cached normalized full text; selected_chunks = use retrieval slice
	"data_extraction_generate_normalized_text": True,  # True = preflight-create full_text_normalized.txt + data_extraction chunk artifacts from PDFs when missing
	"data_extraction_full_text_length_ratio_min": 0.80,  # sanity check: normalized length / direct parser length must meet this ratio
	"data_extraction_full_text_max_words": 0,  # 0 = no word cap; set a number only if full texts exceed model context
	"data_extraction_schema_evidence_hints": True,  # True = prepend compact schema-derived evidence snippets before full normalized text
	"data_extraction_evidence_hints_per_variable": 4,  # snippets per schema variable; table-heavy extraction needs enough hints to surface demographic rows
	"data_extraction_evidence_hint_max_chars": 520,  # max characters per snippet
	"data_extraction_evidence_hints_max_total_chars": 24000,  # total cap for the evidence-hints block
	"data_extraction_evidence_hint_context_lines": 3,  # neighboring normalized-text lines kept with matches so table labels and values stay together
	"temperature": 0.0,  # randomness; lower = more stable decisions, higher = more variable
	"top_p": 1.0,  # keep at 1.0 with temperature=0.0 for stable decoding behavior
	"seed": 42,  # reproducibility seed (set an integer number like 42 to stabilize provider-side sampling)
	"async_max_concurrency": 2,  # endpoint-safe default for full_text/data_extraction; raise only after stable QC runs
	"async_max_retries": 2,  # two total attempts gives one retry for empty transient domain responses
	"async_backoff_base_seconds": 2.0,  # slower retry start reduces repeated pressure on a failing endpoint
	"async_backoff_max_seconds": 20.0,  # maximum retry delay cap
	"async_jitter_seconds": 0.2,  # random jitter added to backoff to reduce thundering herd
	"async_heartbeat_seconds": 30,  # operator heartbeat interval in seconds for async progress logs
	"async_enable_full_text": True,  # True = use async-concurrent LLM calls in full_text stage
	"async_enable_data_extraction": True,  # True = use async-concurrent LLM calls in data_extraction stage
}

# Screening knobs (advanced; keep defaults unless you know why to change)
SCREENING_DEFAULTS = {
	"top_k": 10,  # non-title chunks kept; higher = more evidence, higher cost (lower top_k for faster runs)
	"score_threshold": 0.005,  # minimum relevance score; higher = stricter filtering
	"sample_size": None,  # limit papers per run; set for pilots in any stage
	"sample_seed": None,  # fixed seed for deterministic sampling when sample_size is set
	"batch_size": 32,  # embedding batch size; higher = faster, more memory
	"artifact_mode": "compact",  # "full" = legacy multi-file outputs; "compact" = merged machine artifacts + human-readable outputs
	"compact_keep_legacy_selected_chunks": False,  # True keeps *_selected_chunks.jsonl sidecars in compact mode for interoperability
	"fulltext_preparse_before_screening": True,  # True = preflight-parse full-text PDFs before screening; set False for fastest large runs
	"fulltext_preparse_log_each_paper": True,  # True = print one preparse status line per paper
	"sustainability_tracking": True,  # True = write resource logs; False = no tracking
	"enable_time_savings": True,  # True = compute human-time savings when QC minutes exist; set False to skip
}

def _active_output_stage_dir_name(stage: str) -> str:
	"""human readable hint: citation-search runs can write into visibly separate stage folders."""

	if CITATION_SEARCHING_SCREENING:
		citation_rule = CITATION_SEARCHING_STAGE_RULES.get(stage, {})
		output_dir = citation_rule.get("output_dir")
		if output_dir:
			return str(output_dir)
	return stage


# CodeCarbon configuration (all tunable parameters live here)
CARBON_CONFIG = {
	"project_name": "review_pipeline",  # label used by CodeCarbon in all stages
	"output_dir": str(REPO_ROOT / "output" / _active_output_stage_dir_name(CURRENT_STAGE)),  # where emissions logs are written
	"measure_power_secs": 60,  # sampling interval in seconds; lower = more detail, more overhead
	"tracking_mode": "process",  # "machine" = whole device; "process" = this run only
	"on_csv_write": "append",  # "append" keeps a history; "update" overwrites totals
	"is_offline": False,  # True uses offline factors; requires country_iso_code
	"country_iso_code": "CHE",  # used only when offline; impacts emissions factors
}

# UBELIX operational estimate (rough, optional)
# - Uses runtime + selected resources + TDP + PUE to estimate operational electricity.
# - This approximates the Green Algorithms calculator style estimate.
# - It does NOT include embodied emissions (hardware manufacturing/transport).
UBELIX_ESTIMATION_CONFIG = {
	"enabled": True,  # set True to include UBELIX rough estimate in resource log TOTAL line
	"pue": 1.58,  # data-center overhead factor (Power Usage Effectiveness)
	"grid_carbon_intensity_g_per_kwh": 120.0,  # adjust to your electricity mix assumption
	"core_usage_factor": 1.0,  # 0.0-1.0 average hardware utilization during runtime (Green Algorithms usage factor)
	"memory_gb": 0.0,  # RAM attributable to the run in GB (set with scheduler/admin info)
	"memory_power_watts_per_gb": 0.0,  # memory power draw per GB (set to official value when available)
	"multiplicative_factor": 1.0,  # multiplier for repeated identical runs (e.g., retries/hyperparameter runs)
	"resource_tdp_watts": {
		"anode_core": 8.5,
		"bnode_core": 3.5,
		"cnode_core": 3.75,
		"rtx4090": 450.0,
		"h100": 350.0,
	},
	"resource_usage": {
		"anode_core": 0,  # number of anode CPU cores used by your job
		"bnode_core": 0,  # number of bnode CPU cores used by your job
		"cnode_core": 0,  # number of cnode CPU cores used by your job
		"rtx4090": 0,  # number of RTX4090 GPUs used by your job
		"h100": 1,  # number of H100 GPUs used by your job
	},
	"assumptions": {
		"pue_source": "",  # e.g., UBELIX ops email/ticket reference
		"pue_source_date": "",  # YYYY-MM-DD
		"grid_intensity_source": "",  # e.g., ElectricityMap/official Swiss source URL or doc
		"grid_intensity_source_date": "",  # YYYY-MM-DD
		"resource_usage_source": "",  # e.g., sacct output, GPUSstack dashboard, admin confirmation
		"resource_usage_source_date": "",  # YYYY-MM-DD
		"core_usage_factor_source": "",  # where utilization estimate comes from
		"core_usage_factor_source_date": "",  # YYYY-MM-DD
		"memory_source": "",  # where memory_gb / memory power assumption comes from
		"memory_source_date": "",  # YYYY-MM-DD
		"multiplicative_factor_source": "",  # where repeated-run multiplier comes from
		"multiplicative_factor_source_date": "",  # YYYY-MM-DD
		"notes": "",  # free-text assumptions/limitations for audit trail
	},
}

PATH_SETTINGS = {
	"csv_dir": str(CSV_DIR),  # input folder for stage CSVs (all stages)
	"prompt_file": str(PROMPT_FILE),  # resolved prompt file path for CURRENT_STAGE
	"knowledge_base_file": str(KNOWLEDGE_BASE_FILE),  # resolved stage KB file for CURRENT_STAGE
	"data_extraction_schema_file": str(DATA_EXTRACTION_SCHEMA_FILE),  # schema KB used for extraction + validation
	"knowledge_base_files": {  # per-stage KB mapping used as run defaults
		stage_name: str(path_value)
		for stage_name, path_value in EFFECTIVE_KNOWLEDGE_BASE_FILES.items()
	},
	"eligibility_criteria_file": str(REPO_ROOT / "knowledge-base" / "eligibility_criteria.txt"),  # optional shared criteria text used only when prompts include {eligibility_criteria}
	# Root folder for outputs; files will be placed under output/<stage>/...
	"output_root": str(REPO_ROOT / "output"),  # base output folder for all stages
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
	knowledge_base_file: Path
	data_extraction_schema_file: Path
	knowledge_base_files: dict
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
	if CURRENT_STAGE == "data_extraction" and not DATA_EXTRACTION_SCHEMA_FILE.exists():
		raise FileNotFoundError(f"Missing data-extraction schema CSV at: {DATA_EXTRACTION_SCHEMA_FILE}.")
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
		knowledge_base_file=KNOWLEDGE_BASE_FILE,
		data_extraction_schema_file=DATA_EXTRACTION_SCHEMA_FILE,
		knowledge_base_files=EFFECTIVE_KNOWLEDGE_BASE_FILES,
		embedding_settings=EMBEDDING_SETTINGS,
		llm_settings=LLM_SETTINGS,
		screening_defaults=SCREENING_DEFAULTS,
		carbon_config=CARBON_CONFIG,
		path_settings=PATH_SETTINGS,
		human_time_config=HUMAN_TIME_CONFIG,
	)
