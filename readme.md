
# Smartphone AI PA Screening Tool

End-to-end study screening for a scoping review. Built for fast LLM decisions, reproducible sampling, and transparent logs.

### Advanced Explanation (short)

## Before you start
- Install Python 3.12.
- Put a compatible API key in [.env](.env) as LLM_API_KEY.
- Terminals: on Windows you can use Command Prompt, PowerShell, or the VS Code terminal. On macOS/Linux, any shell works.
- If your prompt shows a folder like C:\>, move into the project folder:
~~~bash
cd "C:\Users\gensitz\OneDrive - Universitaet Bern\Desktop\my_new_project\review-pipeline"
~~~
- Repo footprint: empty placeholders for [input/](input/), [output/](output/), and [knowledge-base/](knowledge-base/) are tracked; their contents stay local and ignored. [papers/](papers/) and [_tests/](\_tests/) stay fully local and are not tracked.

## Preparation (chronological by stage)
- Assure you have a active connection to the University of Bern server; either via eduroam/unibe.ch ethernet or via VPN
- Title_abstract stage:
   - Input: (Covidence) screen CSV (`*_screen_csv_*`) in [input/](input/).
   - Knowledge base: create a CSV with `label` (POS/NEG) and `text` (1–3 sentences). Minimum 1 POS, recommended ≥10 POS and ≥10 NEG.
      - Source: use **human-screened** title/abstract decisions from the first pass (clear includes and excludes).
      - File: [knowledge-base/title_abstract_pos-neg_examples.csv](knowledge-base/title_abstract_pos-neg_examples.csv).
- Full_text stage:
   - Input: (Covidence) select CSV (`*_select_csv_*`) in [input/](input/).
   - PDFs: will be placed after the split into [input/per_paper_full_text/](input/per_paper_full_text/) (one PDF per folder; the tool matches by folder, not filename).
   - Knowledge base: build from **human QC** full-text decisions; store as [knowledge-base/full_text_pos-neg_examples.csv](knowledge-base/full_text_pos-neg_examples.csv).
      - POS: included full-text papers; NEG: excluded full-text papers (short PDF excerpts that justify the decision).
      - Additionally, papers in irrelevant CSV (`*_irrelevant_csv_*`) are auto-added as NEG examples.
      - Tip: use [input/per_paper_full_text/<paper>/full_text_selected_chunks.jsonl](input/per_paper_full_text/) to copy evidence text with page/line metadata into the KB `text` field; store paper_id/page/line in the optional `source` column.
- Data_extraction stage:
   - Input: (Covidence) included CSV (`*_included_csv_*`) in [input/](input/); relies on the full_text folders already created.
   - PDFs: reuse the PDFs from the copied subset folders in [input/per_paper_data_extraction/](input/per_paper_data_extraction/) (matched by folder).
   - Knowledge base: build from **human QC** data-extraction decisions; store as [knowledge-base/data_extraction_pos-neg_examples.csv](knowledge-base/data_extraction_pos-neg_examples.csv).
      - POS: included papers; NEG: excluded papers (short excerpts tied to extraction fields).
      - Additionally, papers in excluded CSV (`*_excluded_csv_*`) are auto-added as NEG examples.
      - Tip: use [input/per_paper_data_extraction/<paper>/data_extraction_selected_chunks.jsonl](input/per_paper_data_extraction/) for evidence snippets (page/line) to populate the KB `text`.
- user_orchestrator setup: set `CURRENT_STAGE` in [config/user_orchestrator.py](config/user_orchestrator.py) before each stage (`title_abstract` → `full_text` → `data_extraction`).

## One-command flow (all stages)
0) (Optional) virtual environment creation/activation
   - python -m venv .venv
   - .venv\Scripts\activate
   - python -m pip install -r requirement.txt
1) virtual environment activation
   - .venv\Scripts\activate
2) Set inputs at the top of [config/user_orchestrator.py](config/user_orchestrator.py).
   - EVERY TIME: CURRENT_STAGE
   - As needed: LLM_MODEL and EMBED_MODEL
   - Human time (optional): the tool auto-counts the QC sample size from the QC CSV; you only enter rough reviewer minutes if you want time-savings to be computed. Leave minutes at 0 to skip time-savings; the log will note that no human minutes were provided.
3) Drop your CSV exports into [input/](input/):
   - title_abstract: `_screen_csv_`
   - full_text: `_select_csv_`
   - data_extraction: `_included_csv_`
4) Run everything with one command (auto-splits folders and checks PDFs where needed):
   - Windows: .venv\Scripts\python main.py (ensures the venv interpreter is used)
   - macOS/Linux: python main.py
   - The pipeline creates a ~10% QC sample (unless QC_ENABLED=False), prompts before QC-only screening, and then asks whether to run validation and proceed to full screening.
   - Validation now compares only the QC sample list that matches the QC screening timestamp.
   - Suggestion (QC-only validation): do one run where the LLM screens only the QC sample, compare against human reviewers, run validation, then run the full stage if metrics look good. This keeps costs low and gives confidence early.
5) Validation (any stage):
   - python -m pipeline.additions.stats_engine

Details for each stage are at the end of this file.

## Backup to GitHub (for non-coders)
After running the pipeline, you will be prompted in the terminal:
   "Do you want to back up your changes to GitHub now? (y/n):"
If you type `y`, the tool will automatically commit and push all your changes to GitHub for you.

You can also run the backup manually anytime by running:
```
python backup_to_github.py
```
This ensures your work is always safely backed up online.

## What the system does
- Human researchers run the upstream JBI + PRISMA-ScR search. Raw citation exports are hashed and archived (e.g., BORIS Portal) to preserve static inputs.
- The machine-to-machine pipeline runs inside a Docker container on university servers.
- Screening and extraction use [Specific Model Name] with Temperature=XX and RAG principles (Chunk size: YYY tokens, Top-K: Z). Full system prompts are stored in Supplementary Appendix A when publishing.
- Each stage enforces a blinded 10% QC sample with stop/go thresholds (Sensitivity > 0.95; PABAK > 0.80); failures trigger prompt refinement before proceeding.
- CodeCarbon (https://mlco2.github.io/codecarbon/index.html) tracks energy and CO2eq alongside time-savings logs.
- After AI screening/extraction, human experts complete targeted citation searching and synthesis. Code, validated datasets, and AI exclusion reasons are released under CC-BY-NC-SA.
- Loads Covidence-style CSVs from [input/](input/) (accepts Title, Abstract, Paper # / Accession Number / DOI / Ref / Study). Keeps all other columns as metadata; if no obvious ID, auto-assigns row-xxxxx.
- Splits title + abstract into deterministic sentence chunks and embeds them against a POS/NEG knowledge base to keep only the most relevant sentences (titles are always kept).
- Full-text chunks now include page/line metadata (from the PDF text extraction) to support provenance checks. This is approximate (text-line based, not PDF layout coordinates).
- Picks the knowledge base by CURRENT_STAGE (default stage files: title_abstract_pos-neg_examples.csv, full_text_pos-neg_examples.csv, data_extraction_pos-neg_examples.csv; override with other knowledgebase file if needed).
- Sends selected evidence plus a stage-specific domain prompt to a fast screening LLM to return a JSON with is_eligible, confidence_score, justification, exclusion_reason_category. LLM input has two parts: a static stage prompt (the prompt_script file) and dynamic per-paper evidence (selected chunks); the API’s prompt_tokens total covers both, so per-paper prompt size varies with evidence length. Prompt text lives in [config/prompt_script_title_abstract.txt](config/prompt_script_title_abstract.txt) / [config/prompt_script_full_text.txt](config/prompt_script_full_text.txt) / [config/prompt_script_data_extraction.txt](config/prompt_script_data_extraction.txt), chosen automatically by CURRENT_STAGE in [config/user_orchestrator.py](config/user_orchestrator.py).
- Before screening, builds the full planned list for the stage, creates a deterministic ~10% QC sample (ceil(sample_rate * count)), writes it to stage-scoped CSV + readable files, and asks for confirmation before QC-only screening.
- Writes per-paper results to JSONL at your chosen output path, with sustainability/resource logs that record exact token usage from the API when available (fallback to estimates otherwise). All default outputs are stage-prefixed (e.g., title_abstract_*, full_text_*). The QC sample size used in time-savings is pulled automatically from the generated QC CSV; there is no QC count to fill in.
- Data_extraction does not write screening eligibility outputs; it writes extraction results (JSONL + CSV) and selected chunk evidence. Per-paper evidence.json is stored in output/data_extraction/<paper_folder>/ to link extracted fields to the selected chunks.


### Beginners Explanation (long)

## Install (step-by-step for non-coders)
1) Open a terminal (Command Prompt or PowerShell is fine). Copy-paste the commands exactly.
2) Create a virtual environment (only once):
~~~bash
python -m venv .venv
~~~
   - If you see python not found, install Python 3.12 and try again.
3) Activate the environment every time before running the tool:
   - Windows:
~~~bash
.venv\Scripts\activate
~~~
   - macOS/Linux:
~~~bash
source venv/bin/activate
~~~
   - Check that you see (venv) at the start of the prompt.
4) Install required packages from [requirement.txt](requirement.txt) (internet needed; takes a minute):
~~~bash
python -m pip install -r requirement.txt
~~~
   - Wait for a Successfully installed message; setup is then complete.
   - On first run, the tool will auto-download the NLTK sentence tokenizer.

## Configure (one file to edit)
- Only edit [config/user_orchestrator.py](config/user_orchestrator.py). The top block has CURRENT_STAGE, LLM_MODEL, and EMBED_MODEL for day-to-day runs; the rest holds advanced defaults (sampling, logs, paths).
- QC is the default: the run creates a QC sample, prompts before QC-only screening, and only proceeds to full screening after you confirm validation results in the terminal.
- To skip QC entirely, set QC_ENABLED=False.
- Vector DB is not used in the screening pipeline; PDFs are read from the per-paper folders.
- Embedding/LLM settings are defined in [config/user_orchestrator.py](config/user_orchestrator.py); the pipeline reads them directly.
   - `data_language` can be "english", "german", or "auto" (auto detects per text).
   - LLM response length + creativity: adjust `max_tokens` and `temperature` under `LLM_SETTINGS`.
- Do not remove keys from `EMBEDDING_SETTINGS`, `LLM_SETTINGS`, or `SCREENING_DEFAULTS`; missing keys stop the run with a clear warning.
- Embedding cache: set `embedding_cache_size` in [config/user_orchestrator.py](config/user_orchestrator.py) to cap memory use (0 disables eviction).
- Screening prompt: adjust the stage-specific file matching your CURRENT_STAGE — [config/prompt_script_title_abstract.txt](config/prompt_script_title_abstract.txt), [config/prompt_script_full_text.txt](config/prompt_script_full_text.txt), or [config/prompt_script_data_extraction.txt](config/prompt_script_data_extraction.txt) — if you want to change inclusion/exclusion rules or the JSON response format.
- Resource tracking: adjust CodeCarbon settings in [config/user_orchestrator.py](config/user_orchestrator.py) under `CARBON_CONFIG`. The resource log uses CodeCarbon totals plus token counts to compute per‑token rates.
- Knowledge base (required for every stage): edit the stage-specific file (columns: `label` = POS/NEG, `text` = short evidence):
   - title_abstract → [knowledge-base/title_abstract_pos-neg_examples.csv](knowledge-base/title_abstract_pos-neg_examples.csv)
   - full_text → [knowledge-base/full_text_pos-neg_examples.csv](knowledge-base/full_text_pos-neg_examples.csv)
   - data_extraction → [knowledge-base/data_extraction_pos-neg_examples.csv](knowledge-base/data_extraction_pos-neg_examples.csv)
  - Minimum: 1 POS entry. Recommended: ≥10 POS and ≥10 NEG. Missing or empty KB files will stop the run.
- API key: add to [.env](.env) as LLM_API_KEY=....

## Run the screening (step-by-step for non-coders)
1) Set CURRENT_STAGE and LLM_MODEL at the top of [config/user_orchestrator.py](config/user_orchestrator.py).
2) Place the right CSVs in [input/](input/):
   - `*_screen_csv_*` for title_abstract.
   - `*_select_csv_*` for full_text.
   - `*_included_csv_*` for data_extraction.
3) Open a terminal, activate the virtual environment, and run:
~~~bash
# Windows
.venv\Scripts\python main.py
# macOS/Linux
python main.py
~~~
   - For full_text, the command auto-builds [input/per_paper_full_text/](input/per_paper_full_text/); add one PDF per folder if prompted.
   - For data_extraction, the command auto-builds [input/per_paper_data_extraction/](input/per_paper_data_extraction/) from included IDs.
4) First run per stage: a QC sample (~10% of planned papers) is generated (unless QC_ENABLED=False).
   - The terminal asks whether to proceed (reply `y` to continue or `n` to abort).
   - Suggestion: keep a “QC-only pass” where the LLM screens only the QC sample. Humans screen the same QC CSV, then you run validation to check agreement before running the full stage.
   - When QC-only screening finishes, the terminal asks if it should run validation:
     - If you choose **y**, it will ask for the needed CSVs (press Enter to auto-detect).
     - If you choose **n**, the run stops so you can do validation later.
    - To start a new QC round after failed validation:
       1) When asked “Are you satisfied with validation results and do you want to continue with screening of the remaining papers?”, reply `n`.
     2) When asked “Start a new QC round with a fresh sample?”, reply `y`.
       3) A new QC sample is created with a new timestamp suffix (YYYYMMDD_HH-MM).
5) After confirmation, the run prints progress and writes output files into [output/](output/).
   - CodeCarbon prints INFO/WARNING lines about power estimation roughly every 15 seconds (controlled by `measure_power_secs`); this is expected. On Windows it will warn about CPU estimation unless Intel Power Gadget is installed.
6) To change defaults (e.g., sample size, seed, skipping LLM calls), edit the settings in [config/user_orchestrator.py](config/user_orchestrator.py).

## Manual PDF placement workflow (stage-aware)
- Full_text:
   - Keep the `_select_csv_` export in [input/](input/).
   - On Windows, run `.venv\Scripts\python main.py` to create [input/per_paper_full_text/](input/per_paper_full_text/) and stop if PDFs are missing. On macOS/Linux, use `python main.py`. Add one PDF per folder (best: `<folder_name>.pdf`).
   - Rerun with the same interpreter after PDFs are in place to screen full_text.
- Data_extraction:
   - Keep the `_included_csv_` export in [input/](input/); ensure [input/per_paper_full_text/](input/per_paper_full_text/) exists from the prior stage.
   - On Windows, run `.venv\Scripts\python main.py` to build [input/per_paper_data_extraction/](input/per_paper_data_extraction/) and stop if PDFs are missing. On macOS/Linux, use `python main.py`. Add PDFs if prompted, then rerun with the same interpreter.

## Outputs (what to look for)
   - `title_abstract_eligibility_<qc_sample|remaining_sample>_*.jsonl` (summary: count, % of run, p50/p95/max seconds, exclusion reasons if present)
   - Split eligibility files (each also ends with the same summary fields):
      - `title_abstract_eligibility_select_<qc_sample|remaining_sample>_*.jsonl` (is_eligible=True)
      - `title_abstract_eligibility_irrelevant_<qc_sample|remaining_sample>_*.jsonl` (is_eligible=False)
   - Eligibility index for non-coders: output/title_abstract/title_abstract_eligibility_index.csv (one row per eligibility file). Column definitions:
      - sample_selection: stage + run segment + decision split (e.g., title_abstract_qc_sample_irrelevant).
      - stage: pipeline stage (title_abstract, full_text, data_extraction).
      - decision_split: which slice the file holds (all, select/included, irrelevant/excluded).
      - paper_count: number of papers in that file.
      - percent_of_stage: share of that split relative to all papers processed in the stage (%).
      - p50_seconds: median per-paper processing time for this split.
      - p95_seconds: 95th-percentile per-paper processing time (tail/near-worst typical case).
      - max_seconds: slowest single paper in the split.
      - timestamp: when the summary row was written (UTC).
      - file_path: absolute path to the eligibility JSONL file (placed last for easy copy/open).
   - QC validation alignment (AI vs human decisions and reasons, including agreements): 
      - output/title_abstract/title_abstract_validation_alignment.csv and 
      - output/full_text/full_text_validation_alignment.csv; 
         Columns: ID, human_decision, ai_decision, decision_match (bool), human_note (free-text Notes), human_tag (explicit Covidence tags), ai_reason (LLM tag), reason_match (bool, compares ai_reason to human_tag), plus Title/Abstract/Authors/Year when present.
   
### Validate AI vs human labels (optional, non-coder steps)
## Screening validation (title/abstract)
1) Set CURRENT_STAGE = "title_abstract" in [config/user_orchestrator.py](config/user_orchestrator.py).
2) Ensure a title_abstract eligibility file exists in [output/title_abstract/](output/title_abstract/) (name includes `_qc_sample_` or `_remaining_sample_`).
3) Provide the latest (Covidence) exports: one *_select_csv_* (Yes/Maybe) and one *_irrelevant_csv_* (No). Place anywhere in the project.
   - Extra option: place a human QC file named title_abstract_human_validation_qc_sample_batch_YYYYMMDD.csv in [output/](output/) or [input/](input/). If present, it will be used automatically.
4) Run (venv activated) with explicit file paths if you are not using the interactive prompt:
~~~bash
python -m pipeline.additions.stats_engine --select <path_to_select_csv> --irrelevant <path_to_irrelevant_csv>
~~~
5) Outputs in [output/title_abstract/](output/title_abstract/):
   - title_abstract_validation_stats_report_YYYYMMDD_HH-MM.txt
   - title_abstract_discrepancy_log_YYYYMMDD_HH-MM.csv
   - title_abstract_validation_matrix_YYYYMMDD_HH-MM.png

## Screening validation (full_text)
1) Set CURRENT_STAGE = "full_text" in [config/user_orchestrator.py](config/user_orchestrator.py).
2) Provide (Covidence) full-text exports: one *_included_csv_* and one *_excluded_csv_* (Notes may contain "Exclusion reason:").
   - Extra option: place a human QC file named full_text_human_validation_qc_sample_batch_YYYYMMDD.csv in [output/](output/) or [input/](input/). If present, it will be used automatically.
3) Run with explicit file paths if you are not using the interactive prompt:
~~~bash
python -m pipeline.additions.stats_engine --included <path_to_included_csv> --excluded <path_to_excluded_csv>
~~~
4) Outputs in [output/full_text/](output/full_text/):
   - full_text_validation_stats_report_YYYYMMDD_HH-MM.txt
   - full_text_discrepancy_log_YYYYMMDD_HH-MM.csv
   - full_text_validation_matrix_YYYYMMDD_HH-MM.png

## Data extraction validation
Use when you have AI extraction outputs and (Covidence) adjudicated consensus.
1) Set CURRENT_STAGE = "data_extraction" in [config/user_orchestrator.py](config/user_orchestrator.py).
2) Ensure AI extraction results are in per-paper folders under [output/data_extraction/](output/data_extraction/) (data_extraction_extraction_results.jsonl).
   - Extra option: place a human QC file named data_extraction_human_validation_qc_sample_batch_YYYYMMDD.csv in [output/](output/) or [input/](input/). If present, it will be used automatically.
3) Place the consensus file at [input/data_extraction_consensus.csv](input/data_extraction_consensus.csv).
4) Run: `python -m pipeline.additions.stats_engine` (or use the interactive prompt in main.py).
5) Outputs in [output/](output/): data_extraction_extraction_accuracy_report.txt (also appended to data_extraction_validation_stats_report.txt) and data_extraction_extraction_discrepancies.csv.

## How QC sampling works
- Before screening, the pipeline selects a deterministic QC sample of ceil(sample_rate * planned_papers) (default 10%).
- It writes stage-scoped CSV + readable files, prompts before QC-only screening, and waits for your validation approval before full screening.
- Validation uses only the QC sample list that matches the screening timestamp.
