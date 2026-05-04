# Pipeline Architecture Reference (document 5/6)

**Read prior:** [pipeline_validation_checks.md](pipeline_validation_checks.md)

## Document Purpose

This document describes how the pipeline is implemented and what guarantees are enforced in code.

## What to Expect

- Runtime guarantees and decision gates.
- Retrieval, selection, and validation architecture details.
- Output, retry, and integration behavior.

## How to Use This Document

1. Use this file when you need implementation-level reasoning.
2. Cross-check behavior against validation artifacts and runtime logs.
3. Continue to governance guidance for publication-facing commitments.

Cross-reference: for an operator-facing 1-X runtime sequence (including exactly when parsing, embedding scoring, LLM calls, and decision writes occur), see [review_procedure.md](review_procedure.md) section "Exact Runtime Sequence (1-X)".

## Implemented Guarantees

- Deterministic quality control (QC) sample generation per stage (`ceil(sample_rate * planned_papers)`).
- QC/remaining/retry outputs remain separated by filename and run label.
- Retry runs are tracked in `output/<stage>/<stage>_retry_manifest.jsonl`.
- Large language model decisions missing required fields are logged and eligible for retry.
- Validation compares against matching QC timestamps when available.
- Per-run resource and emissions outputs are written with stage and sample context.
- Screening diagnostics store per-paper input fingerprints (`llm_input_sha256`, `prompt_template_sha256`, `full_prompt_sha256`).
- Prompt assembly supports optional shared criteria injection from `knowledge-base/eligibility_criteria.txt` only when `{eligibility_criteria}` is present in the active prompt.
- `title_abstract` screening executes via asyncio with bounded request concurrency.
- Async stage execution uses bounded worker scheduling (no one-task-per-paper fan-out) to keep memory stable for large corpora.
- Transient API failures (rate limit/timeout/5xx) use exponential backoff with jitter.
- Screening decisions are validated via a strict Pydantic schema before being accepted.
- Prompt scripts are normalized to strict JSON-shape expectations to reduce malformed model outputs and retry churn.
- Before schema validation, the pipeline performs a narrow normalization pass for known near-valid drift (for example, object-form `step_by_step_deliberation`), then applies strict validation.
- Sync execution path reuses the same async paper-processing core to reduce duplicate decision logic.
- Relevance selection skips embedding/scoring for always-included chunk kinds (for example title chunks), and full-text/data-extraction retrieval reuses one ranked chunk list for primary, fallback, and rescue selection.
- Full-text PDF loading avoids duplicate page-count reads when page-level text is already loaded, reducing per-paper I/O overhead.
- A conservative text-normalization pass now runs before sentence splitting (whitespace cleanup, punctuation boundary spacing, soft-hyphen cleanup), improving chunk readability without changing eligibility decision rules.
- Full-text PDF extraction now uses a hybrid backend (pdfplumber primary + PyPDF fallback) and strips repeated header/footer lines.
- Full-text per-paper artifact persistence supports `full` and `compact` modes (`compact` default).
- Compact mode writes one machine artifact (`full_text_artifact.json`) plus one human-readable file (`full_text_normalized.txt`) and removes legacy normalized sidecars.
- In compact mode, the metadata block written to `full_text_normalized.txt` is synchronized from `full_text_artifact.json` -> `metadata`.
- Full mode retains legacy normalized sidecars (`*_normalized_text.txt`, `*_normalized_pages.json`, `*_normalized_meta.json`).
- Full-text sentence assembly now removes low-information sentence fragments (table/citation noise) before chunk windows are formed.
- Full-text chunk ranking now uses a hybrid score (embedding relevance + method/triad lexical evidence + readability + sentence completeness) to prioritize richer evidence blocks.
- Total context budget is model-configurable via `LLM_SETTINGS["context_window_total_tokens"]` (input + output), with prompt budget derived as `context_window_total_tokens - max_tokens`.
- A second conservative normalization pass is applied to full_text chunks immediately before prompt assembly to catch residual extraction artifacts from non-standard PDFs.
- Full-text retrieval applies a final raw-chunk non-title rescue fallback when selected evidence collapses, preventing title-only decisions.
- Full-text stage applies one deterministic pre-LLM policy gate for language only (non-EN/DE excluded); all other inclusion/exclusion criteria remain LLM decisions.
- Full-text adjudication now preserves a valid final JSON payload after the last adjudication pass and records unresolved borderline state in diagnostics, reducing unnecessary retry churn.
- Prompt template snapshots are skipped for split-only prep runs and deduplicated by prompt campaign content hash.
- Optional runtime integrations are now resilient: missing CodeCarbon disables emissions tracking only, and missing PDF readers fail when PDF-read paths are invoked (not at module import).

## Pipeline Behavior by Stage

- `title_abstract`: full `Title + Abstract` is injected directly into prompt `{data}` (no chunking/top-k filtering), eligibility JSONL outputs.
- `full_text`: per-paper folder/PDF workflow, page-line chunking with relevance selection (`top_k`/threshold), eligibility JSONL outputs, and compact/full per-paper artifact persistence.
- `data_extraction`: human-readable extraction prompt plus the configured extraction schema CSV, prompt-derived domain guidance, KB-derived dynamic schema validation, per-domain LLM calls, merged per-paper extraction JSONL/CSV outputs, evidence JSON.
- External CSV metadata headers and extraction aggregate administrative columns are resolved through user-editable aliases in `config/user_orchestrator.py`, keeping `pipeline/` Python generic across review topics and export systems.
- Direct extraction runner: `pipeline/core/run_extraction.py` remains executable and delegates schema validation to `pipeline/core/extraction_schema.py` and file handling to `pipeline/core/extraction_io.py`.
- Prompt-derived retrieval/schema signals are isolated in `pipeline/selection/prompt_signals.py`; `pipeline/core/pipeline.py` now consumes those helpers instead of carrying the parsing logic inline.
- Retrieval tuning constants live in `pipeline/selection/retrieval_config.py`, screening response schemas live in `pipeline/core/screening_schema.py`, and prompt loading lives in `pipeline/core/prompt_context.py`.
- Interactive workflow bookkeeping is split out of `main.py`: retry helpers in `pipeline/additions/retry_flow.py`, run indexes/summaries in `pipeline/additions/run_index.py`, and startup checks in `pipeline/additions/startup_checks.py`.

Prompt/schema behavior:
- For data extraction, the prompt is the conceptual research framework and should remain readable for scientists. Domain-wise runtime prompts parse `# STEPS`, copy only the active domain guidance, and insert the exact schema CSV contract before `# CONTEXT`; the conceptual response guide remains user-facing explanation and is not duplicated into domain calls.
- The configured extraction schema CSV remains authoritative for JSON keys, Pydantic validation, missing-value defaults, and Covidence header mapping.
- Prompt-to-domain matching uses schema text first and optional `DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES` from `config/user_orchestrator.py` when a study needs extra bridge terms.
- Paper metadata uses generic internal keys (`paper_id`, `title`, `authors`, `publication_year`) and reads external header variants from `CSV_METADATA_COLUMN_ALIASES`.
- Aggregate extraction output labels and the AI reviewer label are controlled by `DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS`, not by hardcoded pipeline constants.
- `data_extraction` generates `{variable_name}_value` and `{variable_name}_quote` for every row in the configured extraction schema CSV; missing values use `Not Available`, `false`, or `[]`, with quote `null`.
- `data_extraction` is performed asynchronously with the user-editable `LLM_SETTINGS["async_max_concurrency"]` semaphore for LLM requests.
- `data_extraction` defaults to one Structured Outputs request per KB domain (`data_extraction_split_by_domain=True`) and caps each domain with `data_extraction_domain_max_tokens`.
- The extraction response-format transport is configurable. `prompt_only` avoids provider-side `json_schema` incompatibilities while still validating every response against the KB-derived Pydantic model after generation.
- Data extraction evidence defaults to cached normalized full text (`full_text_normalized.txt`); selected chunks are retained for audit and fallback.
- With `data_extraction_split_by_domain=True`, the saved `data_extraction_prompt_template_*.txt` snapshot remains the user-authored prompt; exact KB-injected domain prompts are checked through per-paper input traces.
- Evidence-mode tradeoff:
  - `full_text`: best recall and quote coverage, but highest token cost because the full text is repeated across domain-wise calls. In this mode `data_extraction_pos-neg_examples.csv` has low direct impact.
  - `selected_chunks`: lower token cost and faster runs, but recall depends on retrieval. In this mode `data_extraction_pos-neg_examples.csv` has high impact because it guides evidence selection.
- If `{eligibility_criteria}` is in the prompt, the pipeline attempts to inject `knowledge-base/eligibility_criteria.txt`.
- If the placeholder is absent, no criteria-file lookup is performed.
- If the placeholder exists but the file is missing, execution continues with a warning and empty replacement.

## Runtime Integrations

- LLM API client wrapper: `pipeline/integrations/llm_client.py`
- PDF/text/language utilities used by selection and pipeline: `pipeline/integrations/embedding_utils.py`
- Dominant selector API: `pipeline/selection/selector.py` via `SelectionEngine`
- Dominant resource-tracking API: `pipeline/additions/resource_usage.py` via `ResourceUsageEngine`

## Deterministic QC Model

- QC sample size is `ceil(sample_rate * planned_papers)`.
- QC sample is timestamped and stage-scoped.
- Remaining run excludes QC IDs to prevent double screening.
- Validation reads the QC sample matching the same timestamp.

## Retry Model

- Retry files are generated from papers logged in the stage error log.
- Retry runs are isolated from base outputs.
- Retry naming pattern:
  - `<stage>_<sample>_sample_retry_<attempt>_<output>_<timestamp>`
- Retry manifest:
  - `output/<stage>/<stage>_retry_manifest.jsonl`
- Pending retry CSVs in `input/retry_runs/` are detected before new screening and can be executed first.
- Deterministic failures (`llm_output_token_limit`, `context_overflow`) are filtered out from automatic retry prompts.

## LLM Decision Quality Gates

- Every response is checked for parseability and completion.
- Screening stages enforce schema-level validation (`bool`/`NEUTRAL` rules, required justification/reason fields).
- `justification` and `exclusion_reason_category` are required for accepted decisions.
- Missing/invalid decisions are logged and queued for retry.
- Validation errors are retried automatically up to 3 attempts per paper.
- Token-limit truncation is explicitly labeled as `llm_output_token_limit` to avoid masked retry loops.
- Neutral/maybe is accepted only in `title_abstract`; ambiguous full-text decisions are retried.
- Full-text high-confidence seed logic is enforced in schema validation (`seed_references=true` only when `is_eligible=true` and `confidence_score>0.98`; explicit seed flag required for such high-confidence includes).

## Knowledge-Base and Evidence Selection

- Stage-specific KB defaults:
  - `knowledge-base/title_abstract_pos-neg_examples.csv`
  - `knowledge-base/full_text_pos-neg_examples.csv`
  - `knowledge-base/data_extraction_pos-neg_examples.csv`
- Data extraction also requires the schema CSV configured by `DATA_EXTRACTION_SCHEMA_FILE` in `config/user_orchestrator.py` (default: `knowledge-base/data_extraction_schema.csv`); this schema KB defines variable names, types, allowed enum values, prompt instructions, and exact Covidence column mappings.
- The current protocol tags in `STUDY_TAGS_INCLUDE`/`STUDY_TAGS_IGNORE` are marked as user-editable. They define review-specific validation labels, while the core pipeline keeps topic retrieval cues in prompts and KB examples rather than hardcoded Python regex defaults.
- Review-topic words, prompt-domain aliases, export-header variants, and aggregate administrative column labels are user-editable config/prompt/schema inputs. Pipeline modules may keep generic publication concepts, but should not encode one review topic or one vendor's administrative headers.
- Optional full_text draft generation utility:
  - `python -m pipeline.additions.generate_cleaned_hybrid_kb_draft`
  - writes `knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft.csv`
  - writes `knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft_report.json`
  - non-destructive by design (source KB files are not modified)
- Stage KB paths are configurable in `config/user_orchestrator.py` via:
  - `KNOWLEDGE_BASE_FILES` (per-stage defaults)
  - `KB_FILE_OVERRIDES` (one-run stage-specific swaps)
  - for full_text draft use: `KB_FILE_OVERRIDES["full_text"] = FULL_TEXT_CLEANED_HYBRID_DRAFT`
- Required KB columns: `label` (`POS`/`NEG`) and `text`.
- Relevance selection uses embedding centroids (POS vs NEG) for `full_text` and for `data_extraction` only when extraction uses selected chunks; `title_abstract` and default full-text data extraction pass text directly.

## Validation Engine Behavior

- Screening validation compares AI decisions against human labels with:
  - confusion matrix
  - accuracy, sensitivity, specificity, PPV, NPV
  - PABAK
  - Clopper-Pearson 95% confidence intervals
- Data extraction validation maps each LLM `{variable_name}_value` to the KB `covidence_column_name`, calculates per-variable concordance and accuracy, and writes `extraction_error_audit.csv` with the LLM quote.

## Resource and Emissions Tracking

- Per-paper token/runtime stats are written to run-specific resource logs.
- CodeCarbon tracks emissions and energy totals.
- CodeCarbon retries are merged into one sample-level file with a `run` column (`main`, `retry_<attempt>`).
- Time-savings estimation uses QC sample size and reviewer-minute inputs from `config/user_orchestrator.py`.

## Key Outputs to Know

- Screening eligibility files and decision splits:
  - `...eligibility_*.jsonl`
  - `...eligibility_select|included_*.jsonl`
  - `...eligibility_irrelevant|excluded_*.jsonl`
- Eligibility index:
  - `output/<stage>/<stage>_eligibility_index.csv`
- Validation files:
  - `...validation_alignment_*.csv`
  - `...validation_stats_report_*.txt`
  - `...validation_matrix_*.png`
- Full-text per-paper artifacts (`input/per_paper_full_text/<paper_folder>/`):
  - compact mode (default): `full_text_artifact.json`, `full_text_normalized.txt`
  - optional compact sidecar: `full_text_selected_chunks.jsonl` when enabled
  - full mode: `*_normalized_text.txt`, `*_normalized_pages.json`, `*_normalized_meta.json`
- Data extraction files:
  - `output/data_extraction/<paper>/data_extraction_results.jsonl`
  - `output/data_extraction/<paper>/data_extraction_results.csv`
  - `output/data_extraction/<paper>/data_extraction_evidence.json`
  - run-level aggregates: `data_extraction_all_papers_for_consensus_comparison.csv` and `data_extraction_all_papers_quote_audit.csv`

---
**Read next:** [study_protocol_and_governance.md](study_protocol_and_governance.md)
