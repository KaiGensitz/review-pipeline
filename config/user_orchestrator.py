import os
from pathlib import Path
from dataclasses import dataclass
from typing import Type, TypeVar, overload

from dotenv import load_dotenv

# human readable hint: repository root is derived automatically from this config file location.
REPO_ROOT = Path(__file__).resolve().parent.parent
# human readable hint: load .env so LLM_API_KEY and similar secrets stay outside tracked code.
load_dotenv(REPO_ROOT / ".env")

# ---------------------------------------------------------------------------
# HOW THIS FILE IS ORDERED
# ---------------------------------------------------------------------------
# human readable hint: section 1 contains values to check/change for every run.
# human readable hint: section 2 contains review-topic/protocol settings.
# human readable hint: section 3 contains workflow settings that may change often.
# human readable hint: section 4 contains validation/retrieval/model settings for investigating different results.
# human readable hint: section 5 contains high-consequence infrastructure/path settings; change only with a clear purpose.
# human readable hint: some section-5 path resolution appears before later runtime dictionaries because Python must compute paths before those dictionaries can reference them.

# ---------------------------------------------------------------------------
# 1. CHANGE/CHECK THESE FOR EVERY RUN
# ---------------------------------------------------------------------------

# human readable hint: choose the active pipeline stage for this terminal run.
# Allowed values are "title_abstract", "full_text", and "data_extraction".
CURRENT_STAGE = "data_extraction"

# human readable hint: API key is loaded from .env as LLM_API_KEY.
# Keep secrets out of this file; an empty value stops the run with a clear error.
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")

# human readable hint: model used for screening and extraction decisions.
# Change when your endpoint/model changes, and record model changes for validation traceability.
LLM_MODEL = "gpt-oss-120b"

# human readable hint: embedding model used for chunk ranking and semantic evidence retrieval.
# Change only together with retrieval/QC checks because ranking behavior can shift.
EMBED_MODEL = "qwen3-embedding-0.6b"

# human readable hint: folder where stage CSV exports and prepared input files live.
# Most users keep this as REPO_ROOT / "input".
CSV_DIR = REPO_ROOT / "input"

# human readable hint: True runs the quality-control workflow before remaining-paper processing.
# Set False only when you intentionally want to skip QC gating.
QC_ENABLED = True

# human readable hint: QC sample fraction for the active stage.
# Example: 0.05 means roughly 5% of eligible papers enter QC.
QC_SAMPLE_RATE = 0.05

# ---------------------------------------------------------------------------
# 2. TOPIC/PROTOCOL SETTINGS
# ---------------------------------------------------------------------------

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
	# human readable hint: these labels identify internal title/abstract test samples, not exclusion reasons.
	"title/abstract screening test sample",
	"title/abstract screening test sample - validation",
	# human readable hint: these labels identify internal data-extraction/full-text test samples, not exclusion reasons.
	"data extraction test sample",
	"fulltext screening test sample",
	# human readable hint: these labels carry study-status/design notes that should not become exclusion reasons.
	"ongoing study",
	"possible rct",
	"not rct",
]

# Stage-specific prompt scripts.
# human readable hint: edit only if you rename or replace a stage prompt file.
PROMPT_FILES = {
	"title_abstract": REPO_ROOT / "config" / "prompt_script_title_abstract.txt",
	"full_text": REPO_ROOT / "config" / "prompt_script_full_text.txt",
	"data_extraction": REPO_ROOT / "config" / "prompt_script_data_extraction.txt",
}

# USER-EDITABLE DATA-EXTRACTION SCHEMA.
# human readable hint: this CSV defines extraction variables, types, instructions, and consensus/export headers.
# human readable hint: change this only when you intentionally switch to a different machine-readable extraction schema.
DATA_EXTRACTION_SCHEMA_FILE = REPO_ROOT / "knowledge-base" / "data_extraction_schema.csv"

# USER-EDITABLE CSV METADATA COLUMN ALIASES.
# human readable hint: external CSV exports use study-specific/admin-specific column names.
# The pipeline uses generic internal keys; edit these aliases when your export headers differ.
CSV_METADATA_COLUMN_ALIASES = {
	# human readable hint: paper_id aliases identify the unique study record ID from exported CSVs.
	"paper_id": ["paper_id", "Key", "Covidence #", "Covidence#", "covidence id", "covidence_id", "covidence number", "Ref", "Study", "ID", "id"],
	# human readable hint: title aliases identify the publication title.
	"title": ["title", "Title"],
	# human readable hint: abstract aliases identify the title/abstract screening abstract text.
	"abstract": ["abstract", "Abstract", "Abstract Note"],
	# human readable hint: authors aliases identify publication authors for prompts, metadata, and exports.
	"authors": ["authors", "Authors", "author", "Author"],
	# human readable hint: publication_year aliases identify the year used in citations and metadata.
	"publication_year": ["publication_year", "year", "Year", "Published Year", "Year of Publication", "Publication Year", "publication year", "PublicationYear", "PublishedYear", "PubYear", "PY", "date"],
	# human readable hint: publication_month aliases identify optional month metadata.
	"publication_month": ["publication_month", "month", "Month", "Published Month", "Published month"],
	# human readable hint: journal aliases identify source journal or publication title.
	"journal": ["journal", "Journal", "Source", "Source Title", "Publication Title", "Journal Abbreviation"],
	# human readable hint: volume aliases identify bibliographic volume metadata.
	"volume": ["volume", "Volume"],
	# human readable hint: issue aliases identify bibliographic issue metadata.
	"issue": ["issue", "Issue"],
	# human readable hint: pages aliases identify page range metadata.
	"pages": ["pages", "Pages", "Page", "page", "Page range", "Page Range"],
	# human readable hint: accession_number aliases identify database accession numbers when present.
	"accession_number": ["accession_number", "Accession Number", "AccessionNumber", "Accession", "WOS Accession Number"],
	# human readable hint: doi aliases identify the publication DOI.
	"doi": ["doi", "DOI", "Doi"],
	# human readable hint: reference aliases identify full citation/reference text when exported.
	"reference": ["reference", "Reference", "Ref", "Url", "URL", "url"],
	# human readable hint: study_id aliases identify optional internal study IDs distinct from paper_id.
	"study_id": ["study_id", "Study ID", "Study"],
	# human readable hint: notes aliases identify free-text note columns from external exports.
	"notes": ["notes", "Notes"],
	# human readable hint: tags aliases identify screening tags or labels from external exports.
	"tags": ["tags", "Tags", "Keywords", "keywords", "label", "labels", "Manual Tags", "Automatic Tags"],
	# human readable hint: reviewer_name aliases identify reviewer rows in human/AI comparison exports.
	"reviewer_name": ["Reviewer Name", "reviewer_name", "reviewer"],
}

# USER-EDITABLE STAGE HANDOFF SETTINGS.
# human readable hint: each screening stage can write a canonical CSV that is directly usable by the next stage.
STAGE_HANDOFF_SETTINGS = {
	# human readable hint: True writes next-stage CSV handoff files after title_abstract and full_text screening.
	"enabled": True,
	# human readable hint: handoff CSVs keep the existing input/ naming conventions (*_select_csv_* and *_included_csv_*).
	"output_dir": REPO_ROOT / "input",
	# human readable hint: True lets main.py use the latest previous-stage handoff when no explicit --input-file is supplied.
	"auto_use_latest_previous_handoff": True,
	# human readable hint: True also writes false-decision CSVs for audit/validation, not only the next-stage input.
	"write_excluded_audit_csv": True,
}

# USER-EDITABLE PIPELINE BOUNDARY CHECK TERMS.
# human readable hint: smoke tests use these terms to confirm pipeline/ Python stayed generic.
PIPELINE_BOUNDARY_CHECK_TERMS = {
	"topic_terms": [
		"smartphone",
		"physical activity",
		"urban",
		"MVPA",
		"RQ1",
		"RQ2",
		"RQ3",
		"RQ4",
		"green AI",
	],
	"admin_header_terms": [
		"Covidence #",
		"Reviewer Name",
		"Study ID",
		"Published Year",
	],
}

# USER-EDITABLE DATA-EXTRACTION PROMPT ALIASES.
# human readable hint: optional bridge terms that connect prompt sections to schema domains.
# Keep these current-study terms here, not in pipeline/ Python files.
DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES = {
	# human readable hint: terms that connect prompt text to study metadata schema variables.
	"study_details": ["study metadata", "study characteristics", "first author", "author year", "study design", "duration", "funding"],
	# human readable hint: terms that connect prompt text to population/demographic schema variables.
	"population": ["population", "sample size", "mean age", "gender overall", "ethnicity overall", "health status overall", "demographics", "baseline table"],
	# human readable hint: terms that connect prompt text to context/geography schema variables.
	"context": ["urban context", "urban", "setting", "built environment", "location", "real-world", "metropolitan"],
	# human readable hint: terms that connect prompt text to outcome schema variables.
	"outcomes": ["outcomes", "physical activity", "primary pa outcome", "pa outcome", "mvpa", "step count"],
	# human readable hint: terms that connect prompt text to intervention/technology/ethics schema variables.
	"concepts": ["rq1", "rq2", "rq3", "rq4", "ai & tech", "ai and technology", "architecture", "smartphone", "sensing", "psychosocial", "behavior change", "inclusivity", "ethics", "sustainability"],
	# human readable hint: terms that connect prompt text to synthesis/discussion schema variables.
	"synthesis": ["synthesis", "findings", "implications", "limitations", "notes"],
}

# USER-EDITABLE DATA-EXTRACTION EXPORT SETTINGS.
# human readable hint: these describe the human consensus/export table layout and AI reviewer label.
DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS = {
	# human readable hint: columns shown first in the consensus-comparison CSV.
	"comparison_default_headers": ["Covidence #", "Title"],
	# human readable hint: columns used in the long quote-audit CSV.
	"quote_audit_headers": ["Covidence #", "Title", "Domain", "Variable", "Consensus_Column", "AI_Value", "AI_Quote"],
	# human readable hint: output column name for paper IDs in human-facing tables.
	"paper_id_column": "Covidence #",
	# human readable hint: output column name for paper titles in human-facing tables.
	"title_column": "Title",
	# human readable hint: quote-audit column that stores schema domain names.
	"quote_audit_domain_column": "Domain",
	# human readable hint: quote-audit column that stores schema variable names.
	"quote_audit_variable_column": "Variable",
	# human readable hint: quote-audit column that stores the mapped human consensus column name.
	"quote_audit_consensus_column": "Consensus_Column",
	# human readable hint: quote-audit column that stores the extracted AI value.
	"quote_audit_value_column": "AI_Value",
	# human readable hint: quote-audit column that stores the supporting AI quote.
	"quote_audit_quote_column": "AI_Quote",
	# human readable hint: output column name for reviewer labels.
	"reviewer_name_column": "Reviewer Name",
	# human readable hint: reviewer label assigned to AI-generated extraction rows.
	"reviewer_name_value": "AI",
	# human readable hint: optional output column name for study IDs.
	"study_id_column": "Study ID",
	# human readable hint: output column name for authors metadata.
	"authors_column": "authors",
	# human readable hint: output column name for publication-year metadata.
	"publication_year_column": "publication_year",
}

# USER-EDITABLE FALLBACK CONSENSUS/EXPORT HEADER ALIASES FOR EXTRACTION VARIABLES.
# human readable hint: exact consensus/export column names in the schema CSV are tried first; these aliases are optional fallbacks.
DATA_EXTRACTION_CONSENSUS_HEADER_ALIASES = {
	# human readable hint: fallback human-export headers for age.
	"population.mean_age": ["Age", "Mean age", "Mean age Overall", "Mean age (years) ± SD Overall"],
	# human readable hint: fallback human-export headers for sample size.
	"population.sample_size": ["Sample_size", "Sample size", "Sample size Overall"],
	# human readable hint: fallback human-export headers for gender distribution.
	"population.gender_overall": ["Gender", "Gender Overall"],
	# human readable hint: fallback human-export headers for ethnicity/race.
	"population.ethnicity_overall": ["Ethnicity", "Ethnicity Overall"],
	# human readable hint: fallback human-export headers for health status.
	"population.health_status": ["Health_status", "Health status", "Health status Overall"],
	# human readable hint: fallback human-export headers for participant/study country.
	"population.country_overall": ["country", "Country", "Country Overall"],
	# human readable hint: fallback human-export header for reported outcome domains.
	"outcomes.reported": ["outcomes_reported"],
}

# BACKWARD-COMPATIBILITY ALIAS.
# human readable hint: legacy code/config may still import this name; keep it pointing at the generic setting.
DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES = DATA_EXTRACTION_CONSENSUS_HEADER_ALIASES

# USER-EDITABLE DATA-EXTRACTION VALIDATION VALUE ALIASES.
# human readable hint: groups of semantically equivalent validation values; stats_engine uses these only after human ground truth has been built.
DATA_EXTRACTION_VALIDATION_VALUE_ALIASES = {
	# human readable hint: evidence_source groups names for equivalent publication/source types.
	"context.evidence_source": [
		["peer-reviewed", "peer reviewed", "peer-reviewed journal article", "journal article", "original article", "original investigation"],
		["conference paper", "conference proceeding", "proceedings paper"],
		["pre-print", "preprint"],
	],
	# human readable hint: key_findings group treats protocol/no-final-results wording as equivalent when scientifically appropriate.
	"synthesis.key_findings": [
		["keine ergebnisse vorhanden", "keine finalen ergebnisse", "not yet available", "no final outcome findings", "protocol"],
	],
	# human readable hint: inclusivity group treats German/English not-separately-evaluated wording as equivalent.
	"concepts.inclusivity_considerations": [
		["nicht gesondert ausgewertet", "not separately evaluated", "not separately analysed", "not separately analyzed"],
	],
	# human readable hint: mean_age groups support reviewer-equivalent age-band and age-statistic wording.
	"population.mean_age": [
		["45-64 years", "45 to 64 years", "45-64 age group", "45-64 age band", "45-64 years: 28/36", "45-64 years 28 36 77.8%"],
		["age band", "age bands", "age group", "age groups", "age category", "age categories"],
		["mean age", "median age", "age range", "baseline age", "demographic age"],
	],
	# human readable hint: reported groups align behavior-domain labels that reviewers may phrase differently.
	"outcomes.reported": [
		["physical activity", "pa outcome", "physical-activity outcome", "exercise behavior", "exercise adherence", "steps", "step count", "mvpa", "walking"],
		["feasibility/usability", "feasibility and usability", "feasibility", "usability", "engagement", "acceptability"],
	],
	# human readable hint: smartphone_usage groups equivalent delivery-channel and app-feature labels.
	"concepts.smartphone_usage": [
		["mobile app", "smartphone app", "native app", "android app", "ios app", "phone app", "companion app"],
		["chatbot", "chat bot", "conversational agent", "voice assistant", "voice-assistant companion app"],
		["sms/text messages", "sms", "text message", "text messages", "push notification", "push notifications", "reminder", "reminders"],
		["wearable sync", "wearable synchronization", "wearable-to-phone", "activity tracker sync", "data transmission"],
		["avatar selection", "badges/profile", "digital badges", "profile", "leaderboard", "leaderboards"],
	],
	# human readable hint: AI_transparency group keeps common missing-transparency phrases comparable.
	"concepts.AI_transparency": [
		["not available", "not reported", "not described", "no explicit ai transparency", "no transparency information", "explainability not reported", "interpretability not reported"],
		["usage described transparently", "ai usage described", "ai use described", "intervention use described", "assessment use described"],
		["deployment described transparently", "ai deployment described", "server workflow described", "app workflow described", "system workflow described", "implementation described"],
		["development described transparently", "ai development described", "model development described", "training described", "training data described", "dataset creation described"],
		["algorithm workflow described", "decision rule described", "goal-based selection described", "feedback rationale described", "actual versus expected performance described", "actual-vs-expected performance described"],
		["method detailed in cited source", "methods detailed in cited source", "detailed in supplemental source", "described in protocol", "described in development paper", "described in appendix", "described in supplement"],
	],
	# human readable hint: human_AI_interaction groups equivalent ways humans interact with AI outputs or app-mediated AI controls.
	"concepts.human_AI_interaction": [
		["chatbot query", "chatbot conversation", "chat bot conversation", "conversational interaction", "user queries"],
		["voice command", "voice assistant", "voice interaction", "voice-based interaction"],
		["avatar selection", "goal setting", "audio feedback", "leaderboard", "leaderboards", "self-monitoring", "recommendation response", "alert response"],
		["adaptive recommendation", "adaptive recommendations", "model-generated prompts", "ai feedback", "ai-selected goals"],
	],
	# human readable hint: development_process groups equivalent design/development/prototyping/model-training descriptions.
	"concepts.development_process": [
		["design and development process described", "design/development process", "technology development", "app development", "app implementation", "technical integration"],
		["user-centered design", "user centred design", "co-design", "participatory design", "stakeholder involvement", "user testing", "prototype testing", "iterative prototyping"],
		["model training", "training-data construction", "dataset creation", "expert annotation"],
	],
	# human readable hint: setting groups high-level urbanicity categories and explicit n/a-style cases.
	"context.setting": [
		["urban area", "urban", "suburban", "city", "metropolitan", "municipal"],
		["rural area", "rural", "countryside", "remote", "nonmetropolitan", "non-metropolitan"],
		["both", "urban and rural", "urban/suburban plus rural", "urban/suburban and rural/suburban", "urban area and rural area"],
		["n/a", "not available", "not mentioned", "not reported", "no urbanicity reported", "venue only", "clinic only", "rehabilitation clinic"],
	],
	# human readable hint: continent groups common country/region words under continent-level labels.
	"context.continent": [
		["europe", "eu", "european"],
		["north america", "usa", "united states", "canada"],
		["asia", "asian"],
		["australia", "oceania"],
		["south america", "africa", "antarctica"],
	],
}

# ---------------------------------------------------------------------------
# 3. FREQUENTLY CHANGED WORKFLOW SETTINGS
# ---------------------------------------------------------------------------

# human readable hint: True switches the run into citation-search file patterns and separate output folders.
# Keep False for the regular database/bibliographic-export workflow.
CITATION_SEARCHING_SCREENING = False

# Human reviewer timing (per stage).
# human readable hint: edit total_minutes after each QC round if you want time-savings estimates.
# human readable hint: each reviewer row represents one person; zero-minute reviewers are ignored.
# human readable hint: total_minutes is the person's total time on the active stage QC sample, not minutes per paper.
HUMAN_TIME_CONFIG = {
	"title_abstract": {
		"reviewers": [
			# human readable hint: title_abstract reviewer IDs are anonymized labels for timing summaries.
			# human readable hint: title_abstract total_minutes is total human time spent on the title/abstract QC sample.
			{"id": "human_1", "total_minutes": 50}, # Reviewer 1 for 223 articles (Email 12.02.2026): 1h 50 min = 110 min
			{"id": "human_2", "total_minutes": 110}, # Reviewer 2 for 223 articles (Slack 19.02.2026): 4h = 240 min
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
	"full_text": {
		"reviewers": [
			# human readable hint: full_text total_minutes is total human time spent on the full-text QC sample.
			{"id": "human_1", "total_minutes": 270}, # Reviewer 1 for 50 articles (Email 09.04.2026): 4.5 h = 270 min
			{"id": "human_2", "total_minutes": 300}, # Reviewer 2 for 50 articles (Slack 15.04.2026): 5 h = 300 min
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
	"data_extraction": {
		"reviewers": [
			# human readable hint: data_extraction total_minutes is total human time spent on the extraction QC sample.
			{"id": "human_1", "total_minutes": 75}, # Reviewer 1 for 2 articles (Email 08.05.2026): ca. 75 Min
			{"id": "human_2", "total_minutes": 155}, # Reviewer 2 for 2 articles (Email 08.05.2026): ca. 40 min per paper for reading, 15 min per paper for controlling, and ca 45 min extra for in-depth review of 1 article = 155 min
			{"id": "human_3", "total_minutes": 0},
			{"id": "human_4", "total_minutes": 0},
		],
	},
}

# ---------------------------------------------------------------------------
# 4. SETTINGS FOR INVESTIGATING DIFFERENT RESULTS
# ---------------------------------------------------------------------------

# USER-EDITABLE DATA-EXTRACTION VALIDATION MATCH SETTINGS.
# human readable hint: reviewer gold-standard prose and AI prose may be factually congruent without identical wording.
# human readable hint: these values affect validation only; they do not change LLM extraction outputs.
DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS = {
	# human readable hint: True means quote-aware matches can count in final accuracy/concordance metrics.
	# This lets the validator accept a cell when the AI value, AI quote, or reviewer note proves the same manuscript fact.
	"quote_aware_match_in_metrics": True,

	# human readable hint: True means the validator may compare the AI's supporting quote against the human value/reviewer note.
	# This helps when the short AI value is imperfect but the quoted evidence contains the correct fact.
	"quote_aware_compare_ai_quote": True,

	# human readable hint: True means reviewer correction notes can be used as part of quote-aware validation.
	# This is useful when the human reviewer wrote the correct manuscript-derived value in a note/correction cell.
	"quote_aware_compare_reviewer_note": True,

	# human readable hint: False keeps ordinary fuzzy value-only matches out of final metrics.
	# Leave this False for conservative manuscript reporting unless fuzzy thresholds have been independently calibrated.
	"count_fuzzy_matches_in_metrics": False,

	# human readable hint: quote-aware free-text threshold for longer prose fields.
	# 0.42 means about 42% token overlap can be enough when quote/reviewer-note context supports the same fact.
	"quote_aware_free_text_token_overlap_threshold": 0.42,

	# human readable hint: quote-aware threshold for list-like fields.
	# 0.35 is lower because lists often contain equivalent items in different order or with slightly different wording.
	"quote_aware_list_token_overlap_threshold": 0.35,

	# human readable hint: quote-aware threshold for short text fields.
	# 0.50 is stricter than long prose because one missing word can change the meaning of a short label.
	"quote_aware_short_text_token_overlap_threshold": 0.50,

	# human readable hint: plain value-vs-value fuzzy threshold for longer prose fields.
	# This is currently an audit setting because count_fuzzy_matches_in_metrics is False.
	"free_text_token_overlap_threshold": 0.80,

	# human readable hint: plain value-vs-value fuzzy threshold for list-like fields.
	# This is intentionally high to avoid accepting lists that only partly overlap.
	"list_token_overlap_threshold": 0.80,

	# human readable hint: plain value-vs-value fuzzy threshold for short labels.
	# This is intentionally high because short labels need near-exact agreement to be trusted automatically.
	"short_text_token_overlap_threshold": 0.80,

	# human readable hint: minimum number of meaningful tokens before any token-overlap comparison is attempted.
	# A value of 2 prevents one-word accidental matches from counting as factual agreement.
	"minimum_token_count_for_fuzzy": 2,

	# human readable hint: accepted relative difference for numeric comparisons, such as rounded percentages or means.
	# 0.02 means roughly 2% tolerance, while the validator also applies a small absolute tolerance for tiny values.
	"numeric_relative_tolerance": 0.02,
}

# USER-EDITABLE DATA-EXTRACTION QUOTE EXPORT ALIASES.
# human readable hint: optional wide-table quote columns are human layout choices.
# The long quote-audit table always contains every schema variable quote; these aliases fill selected quote columns in consensus-style exports.
DATA_EXTRACTION_QUOTE_COLUMN_ALIASES = {
	# human readable hint: fills an optional study-design quote column in wide exports.
	"study_details.study_design": ["study_design_quote"],
	# human readable hint: fills an optional sample-size quote column in wide exports.
	"population.sample_size": ["sample_quote"],
	# human readable hint: fills an optional setting quote column in wide exports.
	"context.setting": ["setting_quote"],
	# human readable hint: fills an optional data-collection quote column in wide exports.
	"context.data_collection_method": ["data_collection_quote"],
}

# USER-EDITABLE DATA-EXTRACTION EVIDENCE-HINT ALIASES.
# human readable hint: these terms help the generic snippet selector find review-relevant evidence in long normalized PDFs.
# Keep review-topic vocabulary here, in prompts, or in the schema CSV rather than in pipeline/ Python files.
DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES = {
	# human readable hint: age retrieval terms emphasize baseline/demographic tables and age-band rescue.
	"population.mean_age": ["age", "aged", "ages", "years", "year-old", "age group", "age groups", "age category", "age categories", "age band", "age bands", "mean age", "median age", "age range", "standard deviation", "sd", "iqr", "n (%)", "count", "percentage", "baseline characteristics", "participant characteristics", "demographic characteristics", "table 1", "18-25", "26-35", "36-45", "45-64", "eligibility", "inclusion", "criteria"],
	# human readable hint: gender retrieval terms target demographic/baseline table labels.
	"population.gender_overall": ["gender", "sex", "female", "male", "women", "men", "baseline characteristics", "participant characteristics", "demographic", "demographics"],
	# human readable hint: sample-size retrieval terms target recruitment, enrollment, and denominator language.
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
	# human readable hint: ethnicity retrieval terms target race/ethnicity table labels and response categories.
	"population.ethnicity_overall": ["ethnicity", "ethnic", "race", "racial", "hispanic", "latino", "not hispanic", "declined", "unknown", "baseline characteristics", "participant characteristics", "demographic", "demographics"],
	# human readable hint: country_overall retrieval terms target participant/study geography.
	"population.country_overall": ["country", "countries", "site", "clinic", "recruitment", "residing"],
	# human readable hint: setting retrieval terms target high-level urbanicity, not concrete venue alone.
	"context.setting": ["setting", "site", "urban", "suburban", "rural", "urban area", "rural area", "urban/suburban", "suburban/rural", "metropolitan", "city", "municipal", "countryside", "remote", "nonmetropolitan", "non-metropolitan", "recruitment", "residing", "location"],
	# human readable hint: country retrieval terms support study-country and first-author fallback evidence.
	"context.country": ["country", "countries", "city", "region", "site", "clinical site", "trial site", "recruitment", "recruited", "institution", "affiliation", "first author", "corresponding author"],
	# human readable hint: continent retrieval terms support mapping countries/regions to continent choices.
	"context.continent": ["country", "countries", "city", "region", "continent", "north america", "south america", "africa", "europe", "asia", "australia", "antarctica", "affiliation", "first author"],
	# human readable hint: evidence_source retrieval terms target publication/source legitimacy language.
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
	# human readable hint: funding retrieval terms target acknowledgments and funding declarations.
	"study_details.funding_sources": ["funding", "funded", "grant", "support", "sponsor"],
	# human readable hint: conflict-of-interest retrieval terms target disclosure and competing-interest sections.
	"study_details.conflicts_of_interest": ["conflict", "interest", "competing", "disclosure", "advisory", "consultant", "consulting", "patent", "employment", "salary", "stock", "royalties", "grant", "company"],
	# human readable hint: AI_model retrieval terms target model/algorithm naming.
	"concepts.AI_model": ["model", "algorithm", "agent", "system", "decision", "recommendation"],
	# human readable hint: AI_transparency retrieval terms target transparent usage, deployment, development, method detail, explainability, and explicit source referrals.
	"concepts.AI_transparency": ["transparent", "transparency", "explain", "explainable", "interpretability", "interpretable", "black-box", "usage", "deployment", "deployed", "implementation", "implemented", "workflow", "app workflow", "server workflow", "feedback rationale", "expected", "performance", "actual performance", "health belief", "rationale", "decision rule", "algorithm reads", "goal-based", "feature extraction", "input features", "data used", "pose", "poses", "training", "training data", "dataset", "learned knowledge", "model training", "development", "developed", "NumPy", "OpenCV", "method detailed", "described elsewhere", "reported elsewhere", "detailed in", "companion paper", "protocol", "development paper", "appendix", "supplement"],
	# human readable hint: ethics retrieval terms target consent, privacy, safety, and ethics language.
	"concepts.ethical_considerations": ["ethics", "ethical", "approval", "consent", "privacy", "safety"],
	# human readable hint: sustainability retrieval terms target computational and environmental sustainability language.
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
	# human readable hint: behavioral_theory retrieval terms target named theories, frameworks, and models.
	"concepts.behavioral_theory": ["theory", "framework", "model", "construct"],
	# human readable hint: behavioral_strategies retrieval terms target behavior-change components.
	"concepts.behavioral_strategies": ["strategy", "strategies", "technique", "intervention", "component"],
	# human readable hint: inclusivity retrieval terms target equity, accessibility, diversity, and generalizability.
	"concepts.inclusivity_considerations": ["inclusivity", "inclusive", "accessibility", "equity", "diversity", "cultural diversity", "race", "ethnicity", "gender", "sex", "age", "language", "Android-only", "device ownership", "under-represented", "underserved", "generalizability"],
	# human readable hint: key_findings retrieval terms target results and outcome findings.
	"synthesis.key_findings": ["finding", "findings", "outcome", "results", "effect", "change"],
	# human readable hint: limitations retrieval terms target author caveats and future-work constraints.
	"synthesis.limitations": ["limitation", "limitations", "future", "caution", "cultural diversity", "music", "music personalization", "selected music", "platform", "Android-only", "short duration", "small sample", "nonresponse", "generalizability"],
	# human readable hint: development_process retrieval terms target design, prototyping, training, and implementation.
	"concepts.development_process": ["development", "design", "iterative", "prototype", "testing", "literature review", "expert opinion", "message content", "pose", "poses", "dataset", "training", "Flutter", "NumPy", "OpenCV", "DeepMotion", "user testing"],
	# human readable hint: sensing_modalities retrieval terms target sensor/device descriptions.
	"concepts.sensing_modalities": ["sensor", "sensors", "sensing", "wearable", "device", "monitor"],
	# human readable hint: study_design retrieval terms target design/status labels.
	"study_details.study_design": ["feasibility", "usability", "pilot", "acceptability", "protocol", "trial design", "study design", "user study", "mixed methods", "randomized", "randomised"],
	# human readable hint: smartphone_usage retrieval terms target mobile delivery channels and app features.
	"concepts.smartphone_usage": ["android", "ios", "app", "mobile app", "companion app", "cellphone application", "SMS", "text message", "text messages", "Coachtext", "push notification", "chatbot", "schedule", "badges", "profile", "coaching", "reminders", "wearable", "sync"],
	# human readable hint: human_AI_interaction retrieval terms target user-facing interaction modes.
	"concepts.human_AI_interaction": ["chat", "chatbot", "interface", "avatar", "goal setting", "audio feedback", "leaderboard", "self-monitoring", "recommendation"],
	# human readable hint: data_collection_method retrieval terms target survey/interview/app-log methods.
	"context.data_collection_method": ["questionnaire", "questionnaires", "interview", "interviews", "redcap", "survey", "app use", "app logs"],
	# human readable hint: implications retrieval terms target author recommendations and downstream use cases.
	"synthesis.implications": ["future", "recommend", "recommendations", "incorporate", "personalization", "multimodal", "adaptive", "feedback", "gamification", "persuasive", "cultural diversity", "music personalization"],
}

# USER-EDITABLE DATA-EXTRACTION EVIDENCE-HINT LOW-PRIORITY PATTERNS.
# human readable hint: these generic source zones often contain affiliations, references, licenses, or PDF boilerplate rather than participant evidence.
DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_LOW_PRIORITY_PATTERNS = [
	# human readable hint: these patterns are down-ranked because they usually identify author/contact metadata.
	"author affiliation",
	"corresponding author",
	"e-mail address",
	# human readable hint: these patterns are down-ranked because they usually identify PDF license/download boilerplate.
	"downloaded on",
	"licensed use",
	"restrictions apply",
	# human readable hint: these patterns are down-ranked because they usually identify reference lists or copyright blocks.
	"references",
	"bibliography",
	"copyright",
	"all rights reserved",
	# human readable hint: these patterns are down-ranked because they often describe questionnaires rather than participant results.
	"answer categories",
	"number of items",
]

# USER-EDITABLE DATA-EXTRACTION SUPPLEMENTAL CITED EVIDENCE SETTINGS.
# human readable hint: cited source texts stay outside pipeline code; put user-supplied protocol/development/source evidence in one of these per-paper subfolders.
DATA_EXTRACTION_SUPPLEMENTAL_CITED_EVIDENCE = {
	# human readable hint: True lets per-paper supplemental/cited-source files be appended to extraction prompts.
	"enabled": True,
	# human readable hint: folder_names are accepted subfolder names inside each per-paper folder.
	"folder_names": ["supplemental_cited_evidence", "cited_evidence", "supplemental_evidence"],
	# human readable hint: file_globs are the supplemental file types the loader will read.
	"file_globs": ["*.txt", "*.md", "*.pdf"],
	# human readable hint: max_files_per_paper caps evidence volume per paper to control prompt size.
	"max_files_per_paper": 8,
	# human readable hint: max_words_per_file trims very long supplemental sources before prompt assembly.
	"max_words_per_file": 4000,
}

# USER-EDITABLE DATA-EXTRACTION EXPERT OVERSIGHT SETTINGS.
# human readable hint: this validation round is AI-first extraction with expert human oversight, not AI-vs-human gold standard extraction.
DATA_EXTRACTION_EXPERT_REVIEW_SETTINGS = {
	# human readable hint: source_output_dir is the extraction output folder used to build expert packets.
	"source_output_dir": REPO_ROOT / "output" / "data_extraction_v6",
	# human readable hint: packet_output_dir is where expert-review CSV packets are written.
	"packet_output_dir": REPO_ROOT / "output" / "data_extraction_v6" / "expert_review_packets",
	# human readable hint: sample_mode decides which papers from source_output_dir enter expert review packets.
	"sample_mode": "all_papers_in_output",
	# human readable hint: True adds shared methodological variables to every expert packet.
	"include_shared_methodological_variables_in_each_packet": True,
	# human readable hint: review_decision_options are the allowed expert decision labels.
	"review_decision_options": ["accept", "correct", "mark_unavailable", "unclear"],
	# human readable hint: error_type_options classify what went wrong when experts correct a value.
	"error_type_options": ["missed_data", "misallocated_data", "fabricated_or_unsupported_data", "formatting_or_type_issue"],
	# human readable hint: error_effect_options classify how much the error matters scientifically.
	"error_effect_options": ["inconsequential", "minor", "major"],
	# human readable hint: these decision labels trigger prompt/schema refinement suggestions.
	"prompt_refinement_trigger_decisions": ["correct", "mark_unavailable"],
	# human readable hint: these severity labels trigger prompt/schema refinement suggestions.
	"prompt_refinement_trigger_error_effects": ["major"],
}

# USER-EDITABLE DATA-EXTRACTION EXPERT REVIEWERS.
# human readable hint: reviewer names and review-topic assignments live here, not in pipeline Python files.
DATA_EXTRACTION_EXPERT_REVIEWERS = {
	# human readable hint: AI technology expert receives AI/model/sensing/smartphone/ethics variables.
	"ai_technology_expert": {
		# human readable hint: display_name appears in expert packets and summaries.
		"display_name": "Shawan",
		# human readable hint: variables are schema paths assigned to this expert packet.
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
	# human readable hint: psychology/theory expert receives theory, strategies, interaction, and inclusivity variables.
	"psychology_theory_expert": {
		# human readable hint: display_name appears in expert packets and summaries.
		"display_name": "Marc",
		# human readable hint: variables are schema paths assigned to this expert packet.
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
	# human readable hint: include age in every expert packet because demographic table extraction is recurrent-risk.
	"population.mean_age",
	# human readable hint: include sample size in every expert packet because denominators affect interpretation.
	"population.sample_size",
	# human readable hint: include gender in every expert packet because demographic extraction is table-sensitive.
	"population.gender_overall",
	# human readable hint: include health status in every expert packet because population eligibility may be nuanced.
	"population.health_status",
	# human readable hint: include study design in every expert packet because protocol/feasibility distinctions affect methods.
	"study_details.study_design",
]

# USER-EDITABLE PROMPT SIGNAL SECTION ALIASES.
# human readable hint: used only when prompt sections contain "- Include:" / "- Exclude:" lists for retrieval signals.
PROMPT_SIGNAL_SECTION_ALIASES = {
	# human readable hint: section names treated as primary intervention/exposure signals when prompts expose Include/Exclude lists.
	"primary": ["intervention / exposure", "intervention/exposure", "intervention", "exposure"],
	# human readable hint: section names treated as secondary outcome signals when prompts expose Include/Exclude lists.
	"secondary": ["outcome", "outcomes"],
}

# USER-EDITABLE RETRIEVAL AND CHUNK-SIGNAL VOCABULARY.
# human readable hint: these terms affect chunk ranking and smoke/bootstrap helpers; edit here for a new protocol.
RETRIEVAL_SIGNAL_SETTINGS = {
	# human readable hint: canonical publication sections used for section-aware diversity and rescue.
	"section_priority": ["introduction", "method", "results", "discussion", "conclusion"],
	# human readable hint: aliases mapped to canonical section labels during PDF chunking and section inference.
	"section_heading_aliases": {
		"introduction": ["introduction", "background"],
		"method": ["method", "methods", "materials and methods", "methodology", "study design"],
		"results": ["result", "results", "finding", "findings"],
		"discussion": ["discussion"],
		"conclusion": ["conclusion", "conclusions", "summary"],
		"reference": ["reference", "references", "bibliography", "acknowledgement", "acknowledgements"],
	},
	# human readable hint: broad section/evidence terms used to rescue relevant full-text chunks.
	"section_rescue_terms": [
		"introduction", "background", "method", "methods", "methodology", "materials and methods",
		"participant", "participants", "intervention", "procedure", "outcome", "results",
		"discussion", "conclusion", "conclusions", "trial", "protocol",
	],
	# human readable hint: substantive sentence terms that prevent useful citation-heavy method/result lines being discarded.
	"substantive_sentence_terms": [
		"method", "methods", "participant", "participants", "intervention", "procedure",
		"analysis", "result", "results", "finding", "findings", "outcome", "baseline",
		"follow-up", "effect", "significant", "comparison",
	],
	# human readable hint: method/evidence terms used to identify chunks with concrete study evidence.
	"method_evidence_terms": [
		"method", "methods", "methodology", "materials and methods", "study design",
		"participant", "participants", "recruit", "recruited", "recruitment",
		"intervention", "procedure", "protocol", "randomized", "randomised",
		"outcome measure", "baseline", "follow-up",
	],
	# human readable hint: main-text evidence terms used to distinguish substantive findings from boilerplate.
	"main_text_evidence_terms": [
		"result", "results", "finding", "findings", "analysis", "effect", "improved",
		"improvement", "increase", "decrease", "significant", "comparison", "group",
		"sample", "participant", "participants", "outcome", "baseline", "follow-up",
	],
	# human readable hint: prefix roots for monitoring/assessment-like content that may need deprioritization.
	"monitoring_seed_roots": [
		"monitor", "assess", "evaluat", "feasib", "usabil", "acceptab", "observ",
		"classif", "predict", "detect", "framework", "protocol", "pilot", "measur",
		"diagnos", "benchmark",
	],
	# human readable hint: prefix roots that indicate active primary-scope/action evidence in a study.
	"primary_action_seed_roots": [
		"interven", "randomi", "trial", "assign", "arm", "program", "coach",
		"feedback", "counsel", "behavior", "treat", "support", "nudge", "goal",
		"recommend", "prescrib", "prompt", "deliver",
	],
	# human readable hint: terms that indicate the prompt expects primary-scope evidence even without include-list cues.
	"primary_scope_requirement_terms": ["intervention", "intervention-first"],
	# human readable hint: generic positive-evidence signal families used by the optional cleaned-hybrid KB draft utility.
	"cleaned_hybrid_domain_terms": [
		"intervention", "exposure", "participants", "sample", "outcome", "measure",
		"method", "analysis", "follow-up",
	],
	"cleaned_hybrid_negative_terms": [
		"review", "meta-analysis", "simulation", "abm", "non-empirical", "commentary", "protocol",
	],
	"cleaned_hybrid_positive_method_terms": ["participants", "sample", "baseline", "follow-up", "randomized", "cohort"],
	"cleaned_hybrid_positive_primary_terms": ["intervention", "exposure", "program", "implementation", "delivery"],
	"cleaned_hybrid_positive_secondary_terms": ["outcome", "measure", "effect", "result", "finding", "endpoint"],
}

# ---------------------------------------------------------------------------
# 5. HIGH-CONSEQUENCE INFRASTRUCTURE SETTINGS
# ---------------------------------------------------------------------------

# STAGE_RULES defines what each phase needs.
# human readable hint: change only if your input file naming convention or per-paper PDF folder layout changes.
STAGE_RULES = {
	"title_abstract": {
		# human readable hint: screen_patterns tells the stage which input CSV exports count as title/abstract records.
		"screen_patterns": ["*_screen_csv_*.csv"],
		# human readable hint: neg_patterns can add explicit negative examples; title/abstract currently has none.
		"neg_patterns": [],
		# human readable hint: pdf_dir is None because title/abstract screening does not need PDFs.
		"pdf_dir": None,
	},
	"full_text": {
		# human readable hint: screen_patterns tells full_text which selected-record CSV exports to use.
		"screen_patterns": ["*_select_csv_*.csv"],
		# human readable hint: neg_patterns points to full-text excluded examples for KB/context building.
		"neg_patterns": ["*_irrelevant_csv_*.csv"],
		# human readable hint: pdf_dir is the input subfolder holding one PDF per paper for full-text screening.
		"pdf_dir": "per_paper_full_text",
	},
	"data_extraction": {
		# human readable hint: screen_patterns tells data_extraction which included-record CSV exports to use.
		"screen_patterns": ["*_included_csv_*.csv", "citationSearching_data-extraction_*.csv"],
		# human readable hint: neg_patterns points to excluded examples useful for retrieval/schema context.
		"neg_patterns": ["*_excluded_csv_*.csv"],
		# human readable hint: pdf_dir is the input subfolder holding one PDF per paper for extraction.
		"pdf_dir": "per_paper_data_extraction",
	},
}

# USER-EDITABLE CITATION-SEARCH SCREENING SETTINGS.
# human readable hint: change only when the citation-search workflow uses different file patterns or output folders.
CITATION_SEARCHING_STAGE_RULES = {
	"title_abstract": {
		# human readable hint: citation-search title/abstract CSV pattern.
		"screen_patterns": ["citationSearching_title-abstract_*.csv"],
		# human readable hint: separate output folder for citation-search title/abstract runs.
		"output_dir": "title_abstract_citationSearching",
	},
	"full_text": {
		# human readable hint: citation-search full-text CSV pattern.
		"screen_patterns": ["citationSearching_full-text_*.csv"],
		# human readable hint: citation-search full-text PDF folder.
		"pdf_dir": "per_paper_full_text",
		# human readable hint: separate output folder for citation-search full-text runs.
		"output_dir": "full_text_citationSearching",
	},
	"data_extraction": {
		# human readable hint: citation-search data-extraction CSV pattern.
		"screen_patterns": ["citationSearching_data-extraction_*.csv"],
		# human readable hint: citation-search data-extraction PDF folder.
		"pdf_dir": "per_paper_data_extraction",
		# human readable hint: separate output folder for citation-search data-extraction runs.
		"output_dir": "data_extraction_citationSearching",
	},
}

# Stage-specific knowledge-base (KB) files.
# - KNOWLEDGE_BASE_FILES holds default KB paths per stage.
# - KB_FILE_OVERRIDES optionally swaps a stage KB for a single run.
# - Override paths may be absolute or relative to REPO_ROOT.
# - Optional full_text cleaned-hybrid draft can be generated with:
#   python -m pipeline.additions.generate_cleaned_hybrid_kb_draft
KNOWLEDGE_BASE_FILES = {
	# human readable hint: default positive/negative examples for title/abstract screening.
	"title_abstract": REPO_ROOT / "knowledge-base" / "title_abstract_pos-neg_examples.csv",
	# human readable hint: default positive/negative examples for full-text screening.
	"full_text": REPO_ROOT / "knowledge-base" / "full_text_pos-neg_examples.csv",
	# human readable hint: default structural examples for data extraction.
	"data_extraction": REPO_ROOT / "knowledge-base" / "data_extraction_pos-neg_examples.csv",
}

# Optional full_text draft assets generated by the cleaned-hybrid utility.
# human readable hint: optional draft CSV path; used only if you run the cleaned-hybrid KB utility.
FULL_TEXT_CLEANED_HYBRID_DRAFT = (
	REPO_ROOT / "knowledge-base" / "full_text_pos-neg_examples_cleaned_hybrid_draft.csv"
)
# human readable hint: JSON report explaining how the optional cleaned-hybrid draft was generated.
FULL_TEXT_CLEANED_HYBRID_DRAFT_REPORT = (
	REPO_ROOT
	/ "knowledge-base"
	/ "full_text_pos-neg_examples_cleaned_hybrid_draft_report.json"
)

# To select the KB file manually for each stage, replace None with:
# REPO_ROOT / "knowledge-base" / <file_of_interest>
KB_FILE_OVERRIDES: dict[str, str | Path | None] = {
	# human readable hint: set to a CSV path to temporarily override title/abstract KB examples.
	"title_abstract": None,
	# human readable hint: set to a CSV path to temporarily override full-text KB examples.
	"full_text": None,
	# human readable hint: set to a CSV path to temporarily override data-extraction KB examples.
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

# human readable hint: derived mapping after applying KB_FILE_OVERRIDES; users do not edit this directly.
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

# human readable hint: resolved active prompt path for CURRENT_STAGE; users edit PROMPT_FILES/CURRENT_STAGE above.
PROMPT_FILE = PROMPT_FILES[CURRENT_STAGE]
# human readable hint: resolved active KB path for CURRENT_STAGE; users edit KNOWLEDGE_BASE_FILES/KB_FILE_OVERRIDES above.
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

# ---------------------------------------------------------------------------
# 5. HIGH-CONSEQUENCE INFRASTRUCTURE SETTINGS, CONTINUED
# ---------------------------------------------------------------------------

# human readable hint: embedding settings affect evidence retrieval, speed, memory use, and multilingual handling.
# Treat model/endpoint/retrieval changes as high-consequence and rerun QC after changing them.
EMBEDDING_SETTINGS = {
	"gpustack_embedding_model": EMBED_MODEL,  # embedding model; affects relevance ranking in all stages
	"use_api_embeddings": True,  # True = use API embeddings; False would disable embedding-based selection
	"gpustack_base_url": "https://gpustack.unibe.ch/v1",  # embedding endpoint URL; must match your server
	"data_language": "auto_first",  # "english" | "german" | "auto" | "auto_first"; auto_first = detect once per paper, then reuse
	"chunk_size": 20,  # sentences per chunk; larger = fewer chunks, cheaper but less granular evidence (increase slightly for throughput)
	"overlap_size": 2,  # sentences overlapped; higher = better continuity but more duplicate cost
	"embedding_cache_size": 2048,  # cached embeddings in RAM; higher = faster, more memory
}

# human readable hint: LLM settings affect prompting, response size, extraction batching, retries, and concurrency.
# Treat these as high-consequence runtime settings because they can alter validation metrics and resource use.
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
		# human readable hint: AI/model fields are detail-sensitive and may benefit from semantic second-opinion evidence.
		"concepts.AI_model",
		"concepts.AI_transparency",
		"concepts.sensing_modalities",
		# human readable hint: intervention-design and ethics fields are often scattered across methods/discussion.
		"concepts.behavioral_strategies",
		"concepts.development_process",
		"concepts.ethical_considerations",
		"concepts.inclusivity_considerations",
		"concepts.sustainability_considerations",
	],  # user-editable detail-sensitive fields for semantic second opinion
	"data_extraction_hybrid_rescue_domains": ["context", "synthesis"],  # user-editable domains where a semantic second opinion is useful even when the primary value is present
	"data_extraction_hybrid_full_text_preferred_variables": [
		# human readable hint: population table fields usually need full normalized text rather than retrieved snippets.
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

# Screening knobs.
# human readable hint: these affect evidence selection, sampling, artifacts, and resource/time tracking.
# Change to investigate performance/speed tradeoffs; keep defaults for manuscript-grade runs unless QC supports a change.
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


# ---------------------------------------------------------------------------
# 5. HIGH-CONSEQUENCE INFRASTRUCTURE SETTINGS, CONTINUED
# ---------------------------------------------------------------------------

# CodeCarbon configuration (all tunable parameters live here)
# human readable hint: these values affect resource/emissions logging, not screening decisions.
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
		# human readable hint: approximate watts per anode CPU core for UBELIX estimates.
		"anode_core": 8.5,
		# human readable hint: approximate watts per bnode CPU core for UBELIX estimates.
		"bnode_core": 3.5,
		# human readable hint: approximate watts per cnode CPU core for UBELIX estimates.
		"cnode_core": 3.75,
		# human readable hint: approximate watts per RTX4090 GPU for UBELIX estimates.
		"rtx4090": 450.0,
		# human readable hint: approximate watts per H100 GPU for UBELIX estimates.
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
		# human readable hint: document the source of the PUE assumption, if available.
		"pue_source": "",  # e.g., UBELIX ops email/ticket reference
		# human readable hint: date for the PUE source.
		"pue_source_date": "",  # YYYY-MM-DD
		# human readable hint: document the source of the grid-carbon-intensity assumption.
		"grid_intensity_source": "",  # e.g., ElectricityMap/official Swiss source URL or doc
		# human readable hint: date for the grid-intensity source.
		"grid_intensity_source_date": "",  # YYYY-MM-DD
		# human readable hint: document where CPU/GPU resource-use counts came from.
		"resource_usage_source": "",  # e.g., sacct output, GPUSstack dashboard, admin confirmation
		# human readable hint: date for the resource-use source.
		"resource_usage_source_date": "",  # YYYY-MM-DD
		# human readable hint: document where the average utilization factor came from.
		"core_usage_factor_source": "",  # where utilization estimate comes from
		# human readable hint: date for the utilization-factor source.
		"core_usage_factor_source_date": "",  # YYYY-MM-DD
		# human readable hint: document where memory assumptions came from.
		"memory_source": "",  # where memory_gb / memory power assumption comes from
		# human readable hint: date for the memory source.
		"memory_source_date": "",  # YYYY-MM-DD
		# human readable hint: document why multiplicative_factor differs from 1.0, if changed.
		"multiplicative_factor_source": "",  # where repeated-run multiplier comes from
		# human readable hint: date for the multiplicative-factor source.
		"multiplicative_factor_source_date": "",  # YYYY-MM-DD
		# human readable hint: free-text caveats for resource/emissions assumptions.
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
