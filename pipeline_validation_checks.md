# Pipeline Validation Checks (document 4/6)

**Read prior:** [review_procedure.md](review_procedure.md)

## Document Purpose

This document lists implementation-level checks to verify run readiness and output integrity.

## What to Expect

- Global pre-run and post-run checks.
- Stage-specific expected outputs and validation commands.
- Reproducibility and integrity controls.

## How to Use This Document

1. Review Global checks before launching a run.
2. Confirm stage-specific expected outputs after each run.
3. Use the listed commands for manual validation when needed.

## Scope Note

- This file documents checks and outputs implemented in code.
- Methodological publication/protocol commitments are documented separately in [study_protocol_and_governance.md](study_protocol_and_governance.md).

## Global Checks

- You are connected to Bern network (eduroam/campus LAN/VPN).
- `.env` contains `LLM_API_KEY`.
- `LLM_SETTINGS["context_window_total_tokens"]` is set for the selected model and `max_tokens` is lower than that value.
- Stage KB file exists and has `POS` and `NEG` rows.
- If a stage prompt uses `{eligibility_criteria}`, `knowledge-base/eligibility_criteria.txt` exists and is current.
- QC is enabled unless intentionally bypassed (`QC_ENABLED=True`).
- QC files exist in `output/<stage>/`:
  - `<stage>_qc_sample_batch_<timestamp>.csv`
  - `<stage>_qc_sample_batch_readable_<timestamp>.txt`
- Validation is run against the matching QC sample timestamp.
- Eligibility diagnostics include per-paper hashes (`llm_input_sha256`, `full_prompt_sha256`) for reproducibility audits.
- `LLM_SETTINGS` async controls are set to endpoint-safe values (`async_max_concurrency`, retry/backoff settings).
- Screening JSON responses are schema-validated (Pydantic); repeated validation failures are visible in error logs.
- Known near-valid shape drift is normalized before schema validation (for example, object-form `step_by_step_deliberation`), then strict validation is applied.
- Placeholder rule is respected: criteria file is only used when `{eligibility_criteria}` is present in the active prompt.
- Full-text retrieval now drops low-information extraction noise chunks (for example punctuation/dot leader fragments) before embedding selection.
- Full-text retrieval now applies adaptive fallback (`top_k` expansion and threshold relaxation) when primary evidence is too weak.
- Full-text retrieval now applies a final raw-chunk non-title rescue when adaptive selection still yields title-only evidence.
- Topic keyword signals used by full-text ranking are now derived from the active prompt (`Intervention / Exposure` + `Outcome` include lists), not fixed to one review theme.
- Full-text retrieval now uses hybrid PDF extraction quality controls (pdfplumber + PyPDF fallback, repeated header/footer cleanup).
- Per-paper full_text artifacts depend on `SCREENING_DEFAULTS["artifact_mode"]`:
  - `compact` (default): writes `full_text_artifact.json` and `full_text_normalized.txt`
  - `full`: writes legacy normalized sidecars (`*_normalized_text.txt`, `*_normalized_pages.json`, `*_normalized_meta.json`)
- In compact mode, verify the metadata block in `full_text_normalized.txt` matches `metadata.json`.
- Full-text retrieval ranking now includes chunk completeness/readability; check `selection_trace.target_chunk_sentence_count`, `selection_trace.selected_sentence_count_min`, and `selection_trace.selected_sentence_count_mean` for context depth.
- Selected chunks include per-chunk certainty metrics (`relevance_score`, `retrieval_rank`, `certainty_percentile`, `certainty_label`, `selection_sources`) for human audit.
- Eligibility diagnostics include selected-chunk quality/coverage summaries (`selected_score_stats`, `selected_page_coverage`, `selection_trace`).
- Verify prompt-driven topic config in diagnostics: `selection_trace.topic_signal_source`, `selection_trace.topic_primary_term_count`, `selection_trace.topic_secondary_term_count`, and `selection_trace.topic_*_terms_preview`.
- Screening schema exclusion flags/reason categories are now configured dynamically from `STUDY_TAGS_INCLUDE` plus prompt END GOAL exclusion keys; verify with `selection_trace.schema_exclusion_tag_count` and `selection_trace.schema_exclusion_tags_preview`.
- `selection_trace.raw_non_title_rescue_added` should be `> 0` only in rescue edge-cases and signals a title-only collapse was prevented.
- Full-text language policy now excludes non-EN/DE papers before LLM adjudication; verify `selection_trace.language_gate_excluded=True` and `selection_trace.detected_language_code` on those records.
- Split-only folder preparation runs should not create prompt template snapshots; snapshots should appear only for screening runs.
- Borderline full-text outputs after final adjudication should be visible via `diagnostics.decision_guardrails.adjudication_resolution` rather than only as validation-error retries.
- For high-confidence full_text includes (`confidence_score > 0.98`), verify `seed_references` is explicitly set and only true when `is_eligible=true`.

## Integrity Checks

- Retry files follow: `<stage>_<sample>_sample_retry_<attempt>_*_<timestamp>`.
- Retry outputs are separate from base outputs.
- Retry manifest exists: `output/<stage>/<stage>_retry_manifest.jsonl`.
- CodeCarbon emissions for retries are merged with a `run` column (`main`, `retry_<attempt>`).
- Deterministic failures (`llm_output_token_limit`, `context_overflow`) are excluded from automatic retry prompts.
- Validation failures are labeled (`llm_validation_error`) and retried up to 3 attempts before logging.

### Reproducibility Controls

- `LLM_SETTINGS` supports optional deterministic request controls:
  - `temperature` (recommended `0.0`)
  - `top_p` (recommended `1.0`)
  - `seed` (set any integer number to create reproducibility audits)
- Screening eligibility outputs now include per-paper diagnostics:
  - `llm_input_sha256`
  - `prompt_template_sha256`
  - `full_prompt_sha256`
  - `llm_seed`, `llm_top_p`

| Hash Name              | Tracks…                            | What it proves if identical?                |
|------------------------|------------------------------------|---------------------------------------------|
| llm_input_sha256       | The paper’s input data             | The AI saw the same paper content           |
| prompt_template_sha256 | The question/instruction form      | The instructions to the AI were unchanged   |
| full_prompt_sha256     | The full prompt (template + paper) | The AI got the same instructions and content |

### On-Demand Exact Input Trace

- Reconstruct and verify the exact model input text for one paper (no full-input storage by default):
  - `python -m pipeline.additions.input_trace --paper-id <ID> --stage <title_abstract|full_text|data_extraction>`
- Optional: include full merged prompt in the trace report:
  - `python -m pipeline.additions.input_trace --paper-id <ID> --stage <stage> --show-full-prompt`
- Output report is written to `output/<stage>/..._input_trace_...txt` with hash match flags.
- Trace report also includes retrieval diagnostics (`top_k`, `score_threshold`, `selected_score_stats`, `selected_page_coverage`, `selection_trace`) and a per-chunk confidence section (`retrieval_rank`, `relevance_score`, `certainty_percentile`, `certainty_label`, `selection_sources`).
- Confirm `context hash match: True` in the trace output report.

## Stage Checks

### Title Abstract

Required inputs:
- `input/*_screen_csv_*.csv`
- `knowledge-base/title_abstract_pos-neg_examples.csv`

Expected outputs:
- `output/title_abstract/title_abstract_<sample>_sample_<main|retry_#>_eligibility_*.jsonl`
- split files:
  - `..._eligibility_select_...jsonl`
  - `..._eligibility_irrelevant_...jsonl`
- `..._selected_chunks_...jsonl`
- `..._screening_results_readable_...txt`
- `..._resource_usage_...log`
- validation files:
  - `..._qc_sample_validation_alignment_...csv`
  - `..._qc_sample_validation_stats_report_...txt`
  - `..._qc_sample_validation_matrix_...png`

Validation command:
- `python -m pipeline.additions.stats_engine --select <select_csv> --irrelevant <irrelevant_csv>`

### Full Text

Required inputs:
- `input/*_select_csv_*.csv`
- `knowledge-base/full_text_pos-neg_examples.csv`
- one PDF per paper folder in `input/per_paper_full_text/`
- first-run rule: `main.py` creates per-paper folders and stops; screening starts only after all folders contain a PDF

Expected outputs:
- `output/full_text/full_text_<sample>_sample_<main|retry_#>_eligibility_*.jsonl`
- split files:
  - `..._eligibility_included_...jsonl`
  - `..._eligibility_excluded_...jsonl`
- `output/full_text/full_text_<qc_sample|remaining_sample>_selected_chunks_*.jsonl`
- `..._screening_results_readable_...txt`
- `..._resource_usage_...log`
- validation files (`alignment`, `stats_report`, `matrix`)
- per-paper files in `input/per_paper_full_text/<paper_folder>/`:
  - always: `metadata.json` and one PDF
  - compact mode (default): `full_text_artifact.json`, `full_text_normalized.txt`
  - optional in compact mode: `full_text_selected_chunks.jsonl` only when `compact_keep_legacy_selected_chunks=True`
  - full mode: `*_normalized_text.txt`, `*_normalized_pages.json`, `*_normalized_meta.json`

Validation command:
- `python -m pipeline.additions.stats_engine --included <included_csv> --excluded <excluded_csv>`

### Data Extraction

Required inputs:
- `input/*_included_csv_*.csv`
- `knowledge-base/data_extraction_pos-neg_examples.csv`
- consensus CSV: `input/data_extraction_consensus.csv` (or explicit `--consensus`)

Expected outputs:
- per-paper in `output/data_extraction/<paper_folder>/`:
  - `data_extraction_extraction_results.jsonl`
  - `data_extraction_extraction_results.csv`
  - `data_extraction_evidence.json`
- run-level:
  - `output/data_extraction/data_extraction_<sample>_sample_<main|retry_#>_resource_usage_<timestamp>.log`

Validation command:
- `python -m pipeline.additions.stats_engine --consensus <data_extraction_consensus.csv>`

Validation exports for repository upload:
- `..._extraction_accuracy_summary_...json`
- `..._extraction_accuracy_by_field_...csv`
- `..._extraction_scoring_...csv`

---
**Read next:** [pipeline_architecture_reference.md](pipeline_architecture_reference.md)
