# Pipeline validation checks (all stages)

Use this checklist after running `python main.py` (or `.venv\Scripts\python main.py` on Windows) to confirm the pipeline and QC behave correctly.

## Shared checks (all stages)
- CodeCarbon will print INFO/WARNING lines every tracking interval (default 15s); this is expected.
- The terminal can run validation for you and prompt for required CSVs (press Enter to auto-detect).
- LLM input per paper consists of a static stage prompt (prompt_script) plus dynamic evidence chunks; `prompt_tokens` from the API include both, so counts vary by paper.
- If QC_ENABLED=False, QC-only and the prompt are skipped.
- If a knowledge base CSV is empty or missing for the stage, the run stops until you add POS/NEG rows.
- Validation compares only the QC sample list that matches the eligibility timestamp.
- **QC sample files exist** in output/<stage>/
  - `qc_sample_batch_YYYYMMDD_HH-MM.csv` (new rounds create new timestamps)
  - `qc_sample_batch_readable_YYYYMMDD_HH-MM.txt`
- **Suggestion (QC-only validation pass)**: run a QC-only pass (LLM screens only the QC sample), have humans screen the same QC CSV, then run validation before a full run.
- **QC-only check** (if enabled): only QC papers should appear in eligibility outputs.
- **Human QC option** (optional): you can add a file named `<stage>_human_validation_qc_sample_batch_YYYYMMDD.csv` to use instead of select/irrelevant or included/excluded exports.
- **Outputs are written** in output/<stage>/ (screening stages):
  - `*_eligibility_<qc_sample|remaining_sample>_*.jsonl`
  - `*_selected_chunks_<qc_sample|remaining_sample>_*.jsonl`
  - `*_screening_results_readable_<qc_sample|remaining_sample>_*.txt`
  - `*_resource_usage_<qc_sample|remaining_sample>_*.log` (records API token usage when available; falls back to estimates)
- **Error log empty**: the console should say “No errors recorded.”

---

## Stage: title_abstract
**Inputs required**
- Only `*_screen_csv_*.csv` in input/
- Knowledge base: knowledge-base/title_abstract_pos-neg_examples.csv (label/text columns, POS+NEG entries)

**Checks**
- QC sampling occurs **before** any screening.
- Suggestion: do a QC-only pass first (LLM screens only the QC sample) and run validation on the same 10% human sample before the full run.
- Progress count equals the number of rows in the screen CSV.
  - If QC-only is enabled, progress count equals the number of rows in the QC sample.
- If validation fails and you want a new QC round:
  1) Answer **n** to “Are you satisfied with validation results and do you want to continue with screening of the remaining papers?”
  2) Answer **y** to “Start a new QC round with a fresh sample?”
  3) Confirm the new QC files have a new timestamp suffix
- No PDF folder checks appear.
- Outputs include (output/title_abstract/):
  - `title_abstract_eligibility_<qc_sample|remaining_sample>_*.jsonl` (summary: count, % of run, p50/p95/max seconds, exclusion reasons if present)
  - Split eligibility (each ends with same summary fields):
    - `title_abstract_eligibility_select_<qc_sample|remaining_sample>_*.jsonl` (is_eligible=True)
    - `title_abstract_eligibility_irrelevant_<qc_sample|remaining_sample>_*.jsonl` (is_eligible=False)
    - Eligibility index: output/title_abstract/title_abstract_eligibility_index.csv (paths, counts, %, p50/p95/max)
  - `title_abstract_selected_chunks_<qc_sample|remaining_sample>_*.jsonl`
  - `title_abstract_screening_results_readable_<qc_sample|remaining_sample>_*.txt`
  - `title_abstract_resource_usage_<qc_sample|remaining_sample>_*.log`
  - `title_abstract_qc_sample_batch_YYYYMMDD_HH-MM.csv`
  - `title_abstract_qc_sample_batch_readable_YYYYMMDD_HH-MM.txt`
- stats_engine check: run `python -m pipeline.additions.stats_engine --select <path_to_select_csv> --irrelevant <path_to_irrelevant_csv>` and confirm outputs in output/title_abstract/ with the same timestamp as the eligibility file used.

---

## Stage: full_text
**Inputs required**
- `*_select_csv_*.csv` in input/
- Knowledge base: knowledge-base/full_text_pos-neg_examples.csv (label/text columns, POS+NEG entries)
- Optional NEG examples: `*_irrelevant_csv_*.csv` (adds negatives to KB)

**Checks**
- QC sampling occurs **before** any screening.
- First run creates input/per_paper_full_text/ folders.
- Each per-paper folder in input/per_paper_full_text/ contains:
  - metadata.csv
  - metadata.json
- If PDFs are missing, you see a list of missing folders.
- After adding one PDF per folder, screening proceeds and logs missing PDFs (if any).
- Outputs include (output/full_text/):
  - `full_text_eligibility_<qc_sample|remaining_sample>_*.jsonl` (summary: count, % of run, p50/p95/max seconds, exclusion reasons if present)
  - `full_text_eligibility_included_<qc_sample|remaining_sample>_*.jsonl` (is_eligible=True, same summary fields)
  - `full_text_eligibility_excluded_<qc_sample|remaining_sample>_*.jsonl` (is_eligible=False, same summary fields)
  - Eligibility index: output/full_text/full_text_eligibility_index.csv (paths, counts, %, p50/p95/max)
  - `full_text_screening_results_readable_<qc_sample|remaining_sample>_*.txt`
  - `full_text_resource_usage_<qc_sample|remaining_sample>_*.log`
  - `full_text_qc_sample_batch_YYYYMMDD_HH-MM.csv`
  - `full_text_qc_sample_batch_readable_YYYYMMDD_HH-MM.txt`
- Selected chunks are stored per paper in input/per_paper_full_text/<paper_folder>/full_text_selected_chunks.jsonl
- stats_engine check: run `python -m pipeline.additions.stats_engine --included <path_to_included_csv> --excluded <path_to_excluded_csv>` and confirm outputs in output/full_text/.

---

## Stage: data_extraction
**Inputs required**
- `*_included_csv_*.csv` in input/
- Knowledge base: knowledge-base/data_extraction_pos-neg_examples.csv (label/text columns, POS+NEG entries)
- Optional NEG examples: `*_excluded_csv_*.csv` (adds negatives to KB)
- Prompt file: config/prompt_script_data_extraction.txt

**Checks**
- QC sampling occurs **before** any screening.
- Requires input/per_paper_full_text/ from full_text stage.
- Creates input/per_paper_data_extraction/ subset folders.
- Copies PDFs into input/per_paper_data_extraction/.
- Each per-paper folder in input/per_paper_data_extraction/ contains:
  - metadata.csv
  - metadata.json
- If full_text_selected_chunks.jsonl exists, it is copied into input/per_paper_data_extraction/<paper_folder>/data_extraction_selected_chunks.jsonl and reused for extraction.
- Writes evidence.json per paper in output/data_extraction/<paper_folder>/ (links extracted data to the selected chunks).
- Outputs include:
  - Per-paper subfolders in output/data_extraction/<paper_folder>/ containing:
    - `data_extraction_extraction_results.jsonl`
    - `data_extraction_extraction_results.csv`
    - `data_extraction_evidence.json`
  - Aggregated run artifacts in output/data_extraction/:
    - `data_extraction_resource_usage_<qc_sample|remaining_sample>_*.log`
    - `data_extraction_qc_sample_batch_YYYYMMDD_HH-MM.csv`
    - `data_extraction_qc_sample_batch_readable_YYYYMMDD_HH-MM.txt`
- Selected chunks are stored per paper in input/per_paper_data_extraction/<paper_folder>/data_extraction_selected_chunks.jsonl (each chunk includes page/line metadata when extracted from PDFs; note: page/line provenance is approximate and derived from extracted text lines, not PDF layout coordinates)
- If PDFs are missing in the subset, you see a list of missing folders.
- Screening proceeds and logs missing PDFs (if any).
- stats_engine check (extraction): run `python -m pipeline.additions.stats_engine --consensus <path_to_data_extraction_consensus.csv>` and confirm outputs in output/data_extraction/.

- CodeCarbon check: resource log TOTAL line includes codecarbon_emissions_kg, codecarbon_energy_kwh, per‑token rates, and uses API token counts when provided (otherwise estimates).


