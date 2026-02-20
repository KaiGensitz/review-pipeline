# Pipeline Architecture Reference

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

## Pipeline behavior by stage

- `title_abstract`: sentence chunking from title/abstract, relevance selection, eligibility JSONL outputs.
- `full_text`: per-paper folder/PDF workflow, page-line chunking, eligibility JSONL outputs.
- `data_extraction`: extraction-focused prompt, per-paper extraction JSONL/CSV outputs, evidence JSON.

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

## LLM decision quality gates

- Every response is checked for parseability and completion.
- `justification` and `exclusion_reason_category` are required for accepted decisions.
- Missing/invalid decisions are logged and queued for retry.
- Neutral/maybe is accepted only in `title_abstract`; ambiguous full-text decisions are retried.

## Knowledge-base (KB) and evidence selection

- Stage-specific KB defaults:
  - `knowledge-base/title_abstract_pos-neg_examples.csv`
  - `knowledge-base/full_text_pos-neg_examples.csv`
  - `knowledge-base/data_extraction_pos-neg_examples.csv`
- Required KB columns: `label` (`POS`/`NEG`) and `text`.
- Relevance selection uses embedding centroids (POS vs NEG) and keeps title chunks.

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
