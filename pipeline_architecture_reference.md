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
- Relevance selection skips embedding/scoring for always-included chunk kinds (for example title chunks), reducing avoidable embedding workload.
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
- `data_extraction`: extraction-focused prompt, per-paper extraction JSONL/CSV outputs, evidence JSON.

Placeholder behavior (all stages):
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
- Relevance selection uses embedding centroids (POS vs NEG) for `full_text`/`data_extraction`; `title_abstract` uses full text input directly.

## Validation Engine Behavior

- Screening validation compares AI decisions against human labels with:
  - confusion matrix
  - accuracy, sensitivity, specificity, PPV, NPV
  - PABAK
  - Clopper-Pearson 95% confidence intervals
- Data extraction validation compares extracted fields to consensus values and produces per-field concordance with confidence intervals.

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
  - `output/data_extraction/<paper>/data_extraction_extraction_results.jsonl`
  - `output/data_extraction/<paper>/data_extraction_extraction_results.csv`
  - `output/data_extraction/<paper>/data_extraction_evidence.json`

---
**Read next:** [study_protocol_and_governance.md](study_protocol_and_governance.md)
