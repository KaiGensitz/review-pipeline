# Pipeline Validation Checks

**Read prior:** [review_procedure.md](review_procedure.md)
**Read next:** [pipeline_architecture_reference.md](pipeline_architecture_reference.md)

Use this checklist before running `main.py`.

## Scope note

- This file documents checks and outputs implemented in code.
- Methodological publication/protocol commitments are documented separately in [study_protocol_and_governance.md](study_protocol_and_governance.md).

## Global checks

- You are connected to Bern network (eduroam/campus LAN/VPN).
- `.env` contains `LLM_API_KEY`.
- Stage KB file exists and has `POS` and `NEG` rows.
- QC is enabled unless intentionally bypassed (`QC_ENABLED=True`).
- QC files exist in `output/<stage>/`:
  - `<stage>_qc_sample_batch_<yyyymmdd>_<hh-mm>.csv`
  - `<stage>_qc_sample_batch_readable_<yyyymmdd>_<hh-mm>.txt`
- Validation is run against the matching QC sample timestamp.

## Retry integrity checks

- Retry files follow: `<stage>_<sample>_sample_retry_<attempt>_*_<yyyymmdd>_<hh-mm>`.
- Retry outputs are separate from base outputs.
- Retry manifest exists: `output/<stage>/<stage>_retry_manifest.jsonl`.
- CodeCarbon emissions for retries are merged with a `run` column (`main`, `retry_<attempt>`).

## Stage checks

### title_abstract

Required inputs:
- `input/*_screen_csv_*.csv`
- `knowledge-base/title_abstract_pos-neg_examples.csv`

Expected outputs:
- `output/title_abstract/title_abstract_eligibility_<qc_sample|remaining_sample>_*.jsonl`
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

### full_text

Required inputs:
- `input/*_select_csv_*.csv`
- `knowledge-base/full_text_pos-neg_examples.csv`
- one PDF per paper folder in `input/per_paper_full_text/`

Expected outputs:
- `output/full_text/full_text_eligibility_<qc_sample|remaining_sample>_*.jsonl`
- split files:
  - `..._eligibility_included_...jsonl`
  - `..._eligibility_excluded_...jsonl`
- `..._screening_results_readable_...txt`
- `..._resource_usage_...log`
- validation files (`alignment`, `stats_report`, `matrix`)

Validation command:
- `python -m pipeline.additions.stats_engine --included <included_csv> --excluded <excluded_csv>`

### data_extraction

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
  - `output/data_extraction/data_extraction_<sample>_sample_<main|retry_#>_resource_usage_<yyyymmdd>_<hh-mm>.log`

Validation command:
- `python -m pipeline.additions.stats_engine --consensus <data_extraction_consensus.csv>`

---
**Read next:** [pipeline_architecture_reference.md](pipeline_architecture_reference.md)
