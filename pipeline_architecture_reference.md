# Pipeline Architecture Reference (document 5/6)

**Read prior:** [pipeline_validation_checks.md](pipeline_validation_checks.md)
**Read next:** [study_protocol_and_governance.md](study_protocol_and_governance.md)

Advanced technical reference for operators and maintainers.

## Implemented guarantees

- Deterministic quality control (QC) sample generation per stage (`ceil(sample_rate * planned_papers)`).
- QC/remaining/retry outputs remain separated by filename and run label.
- Retry runs are tracked in `output/<stage>/<stage>_retry_manifest.jsonl`.
- Large language model decisions missing required fields are logged and eligible for retry.
- Validation compares against matching QC timestamps when available.
- Per-run resource and emissions outputs are written with stage and sample context.
- Screening diagnostics store per-paper input fingerprints (`llm_input_sha256`, `prompt_template_sha256`, `full_prompt_sha256`).
- Prompt assembly supports optional shared criteria injection from `knowledge-base/eligibility_criteria.txt` only when `{eligibility_criteria}` is present in the active prompt.
- `title_abstract` screening executes via asyncio with bounded request concurrency.
- Transient API failures (rate limit/timeout/5xx) use exponential backoff with jitter.
- Screening decisions are validated via a strict Pydantic schema before being accepted.
- Sync execution path reuses the same async paper-processing core to reduce duplicate decision logic.
- Relevance selection skips embedding/scoring for always-included chunk kinds (for example title chunks), reducing avoidable embedding workload.

## Pipeline behavior by stage

- `title_abstract`: full `Title + Abstract` is injected directly into prompt `{data}` (no chunking/top-k filtering), eligibility JSONL outputs.
- `full_text`: per-paper folder/PDF workflow, page-line chunking with relevance selection (`top_k`/threshold), eligibility JSONL outputs.
- `data_extraction`: extraction-focused prompt, per-paper extraction JSONL/CSV outputs, evidence JSON.

Placeholder behavior (all stages):
- If `{eligibility_criteria}` is in the prompt, the pipeline attempts to inject `knowledge-base/eligibility_criteria.txt`.
- If the placeholder is absent, no criteria-file lookup is performed.
- If the placeholder exists but the file is missing, execution continues with a warning and empty replacement.

## Runtime integrations

- LLM API client wrapper: `pipeline/integrations/llm_client.py`
- PDF/text/language utilities used by selection and pipeline: `pipeline/integrations/embedding_utils.py`
- Dominant selector API: `pipeline/selection/selector.py` via `SelectionEngine`
- Dominant resource-tracking API: `pipeline/additions/resource_usage.py` via `ResourceUsageEngine`

## Deterministic QC model

- QC sample size is `ceil(sample_rate * planned_papers)`.
- QC sample is timestamped and stage-scoped.
- Remaining run excludes QC IDs to prevent double screening.
- Validation reads the QC sample matching the same timestamp.

## Retry model (strict isolation)

- Retry files are generated from papers logged in the stage error log.
- Retry runs are isolated from base outputs.
- Retry naming pattern:
  - `<stage>_<sample>_sample_retry_<attempt>_<output>_<yyyymmdd>_<hh-mm>`
- Retry manifest:
  - `output/<stage>/<stage>_retry_manifest.jsonl`
- Pending retry CSVs in `input/retry_runs/` are detected before new screening and can be executed first.
- Deterministic failures (`llm_output_token_limit`, `context_overflow`) are filtered out from automatic retry prompts.

## LLM decision quality gates

- Every response is checked for parseability and completion.
- Screening stages enforce schema-level validation (`bool`/`NEUTRAL` rules, required justification/reason fields).
- `justification` and `exclusion_reason_category` are required for accepted decisions.
- Missing/invalid decisions are logged and queued for retry.
- Validation errors are retried automatically up to 3 attempts per paper.
- Token-limit truncation is explicitly labeled as `llm_output_token_limit` to avoid masked retry loops.
- Neutral/maybe is accepted only in `title_abstract`; ambiguous full-text decisions are retried.

## Knowledge-base (KB) and evidence selection

- Stage-specific KB defaults:
  - `knowledge-base/title_abstract_pos-neg_examples.csv`
  - `knowledge-base/full_text_pos-neg_examples.csv`
  - `knowledge-base/data_extraction_pos-neg_examples.csv`
- Required KB columns: `label` (`POS`/`NEG`) and `text`.
- Relevance selection uses embedding centroids (POS vs NEG) for `full_text`/`data_extraction`; `title_abstract` uses full text input directly.

## Validation engine behavior

- Screening validation compares AI decisions against human labels with:
  - confusion matrix
  - accuracy, sensitivity, specificity, PPV, NPV
  - PABAK
  - Clopper-Pearson 95% confidence intervals
- Data extraction validation compares extracted fields to consensus values and produces per-field concordance with confidence intervals.

## Resource and emissions tracking

- Per-paper token/runtime stats are written to run-specific resource logs.
- CodeCarbon tracks emissions and energy totals.
- CodeCarbon retries are merged into one sample-level file with a `run` column (`main`, `retry_<attempt>`).
- Time-savings estimation uses QC sample size and reviewer-minute inputs from `config/user_orchestrator.py`.

## Key outputs to know

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
- Data extraction files:
  - `output/data_extraction/<paper>/data_extraction_extraction_results.jsonl`
  - `output/data_extraction/<paper>/data_extraction_extraction_results.csv`
  - `output/data_extraction/<paper>/data_extraction_evidence.json`

---
**Read next:** [study_protocol_and_governance.md](study_protocol_and_governance.md)
