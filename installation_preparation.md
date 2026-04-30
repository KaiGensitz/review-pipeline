# Installation and Preparation Guide (document 2/6)

**Read prior:** [readme.md](readme.md)

## Document Purpose

This document explains how to prepare the environment and inputs before any pipeline run.

## What to Expect

- Python and environment setup steps.
- Dependency and API key configuration.
- Stage-specific CSV, knowledge-base, and PDF preparation requirements.

## How to Use This Document

1. Execute sections from top to bottom.
2. Confirm checklist items before launching `main.py`.
3. Continue to the next document for runtime procedure.

## Before You Start

- Use Python 3.12 or newer.
- Ensure University of Bern network access (eduroam, campus LAN, or VPN).
- Open a terminal in the project folder:

~~~bash
cd "path_to_working_directory"
~~~

## Create and Activate the Virtual Environment

Create once:

~~~bash
python -m venv .venv
~~~

Activate every run:

- Windows:

~~~bash
.venv\Scripts\activate
~~~

- macOS/Linux:

~~~bash
source .venv/bin/activate
~~~

If `python` is not found, install Python and try again.

## Install Dependencies

~~~bash
python -m pip install -r requirement.txt
~~~

Notes:
- Wait until installation completes successfully.
- Runtime tokenizer/model downloads are now moved out of screening runs.
- `pdfplumber` is required for PDF stages.
- If `codecarbon` is unavailable, the pipeline now continues without emissions tracking (warning only).

## Preload Runtime Assets (One-Time)

Run once before screening:

~~~bash
.venv\Scripts\python -m pipeline.additions.preload_runtime_assets
~~~

What this does:
- Downloads required NLTK tokenizer data (`punkt`, `punkt_tab`) if missing.
- If `USE_ADVANCED_PDF_PARSER=1`, warms the Docling parser cache using the smallest local PDF (fastest candidate).
- Keeps screening runs deterministic by avoiding runtime tokenizer/model downloads.

Important:
- Screening runs now enforce offline cache mode for Hugging Face assets.
- If preload is skipped and assets are missing, `main.py` stops early with a setup message.
- `full_text` now runs a preflight parse before screening and prints one minimal status line per paper (`OK`/`FAIL`, plus parser level when available).
- Optional toggles (environment variables):
	- `FULLTEXT_PREPARSE_BEFORE_SCREENING=1` (default) enables/disables the preflight parse.
	- `FULLTEXT_PREPARSE_LOG_EACH_PAPER=1` (default) enables/disables per-paper preparse status lines.
	- `DOCLING_WARMUP_TIMEOUT_SECONDS=0` (default) means warmup waits until completion.
	  - Set `>0` (for example `300`) if you want a bounded warmup timeout.
	- Optional Hugging Face auth token for preload speed/rate limits: `HF_TOKEN=...` in `.env`.

## Add API Key

Create/update `.env` in project root with:

~~~bash
LLM_API_KEY=your_api_key_here
~~~

## Configure One File Only

Edit [config/user_orchestrator.py](config/user_orchestrator.py):

- `CURRENT_STAGE` (`title_abstract`, `full_text`, `data_extraction`)
- `LLM_MODEL`
- `EMBED_MODEL`
- KB selection controls:
	- `KNOWLEDGE_BASE_FILES` for per-stage default KB paths
	- `KB_FILE_OVERRIDES` for one-run stage-specific swaps (absolute or repo-relative paths)
- per-paper artifact controls in `SCREENING_DEFAULTS`:
	- `artifact_mode`: `compact` (default) or `full`
	- `compact_keep_legacy_selected_chunks`: keep/remove legacy per-paper `*_selected_chunks.jsonl` in compact mode
- reproducibility controls in `LLM_SETTINGS`:
	- `temperature` (recommended `0.0`)
	- `top_p` (recommended `1.0`)
	- `seed` (set for reproducibility audits; any integer as value, e.g. `42`)
	- `context_window_total_tokens` (total model context: input + output)
	- keep `max_tokens` lower than `context_window_total_tokens`; prompt budget is derived automatically
	- async controls for `title_abstract` throughput and stability:
		- `async_max_concurrency` (concurrent abstract calls)
		- `async_max_retries` (transient API retries)
		- `async_backoff_base_seconds`, `async_backoff_max_seconds`, `async_jitter_seconds` (rate-limit backoff)
	- full-text preparse controls in `SCREENING_DEFAULTS`:
		- `fulltext_preparse_before_screening=True` keeps the default preflight parse
		- `fulltext_preparse_log_each_paper=True` prints one status line per paper
		- environment variables `FULLTEXT_PREPARSE_BEFORE_SCREENING` and `FULLTEXT_PREPARSE_LOG_EACH_PAPER` may still override these values for one shell session

Keep defaults unless you know why to change them.

## Prepare Required Inputs

Load CSVs in [input/](input/) with this structure: accepts Title, Abstract, Paper # / Accession Number / DOI / Ref / Study.
Place CSV exports in [input](input) with file names including:

- `title_abstract`: `*_screen_csv_*.csv`
- `full_text`: `*_select_csv_*.csv`
- `data_extraction`: `*_included_csv_*.csv`

Prepare initial example PDFs for bootstrap generation:

- Positive examples in [papers/pos_examples](papers/pos_examples)
- Negative examples in [papers/neg_examples](papers/neg_examples)

Recommended minimum for stable first-pass behavior:

1. At least 5 POS and 5 NEG PDFs (better: >=10 each)
2. Extractable text PDFs (avoid image-only scans)
3. Representative records for your target question
4. Clear file names (author/year/title) for cleaner source traceability

Generate stage KB files and suggested prompts from those PDFs:

~~~bash
python -m pipeline.additions.bootstrap_stage_kb_and_prompts
~~~

This command creates/updates:

1. [knowledge-base/title_abstract_pos-neg_examples.csv](knowledge-base/title_abstract_pos-neg_examples.csv)
2. [knowledge-base/full_text_pos-neg_examples.csv](knowledge-base/full_text_pos-neg_examples.csv)
3. [knowledge-base/data_extraction_pos-neg_examples.csv](knowledge-base/data_extraction_pos-neg_examples.csv)
4. [config/prompt_script_title_abstract_suggested.txt](config/prompt_script_title_abstract_suggested.txt)
5. [config/prompt_script_full_text_suggested.txt](config/prompt_script_full_text_suggested.txt)
6. [config/prompt_script_data_extraction_suggested.txt](config/prompt_script_data_extraction_suggested.txt)
7. [knowledge-base/kb_bootstrap_summary.json](knowledge-base/kb_bootstrap_summary.json)

If you accept the generated prompt suggestions, copy them into the active prompt files under [config](config).

Optional full-text cleaned-hybrid draft generation (non-destructive):

~~~bash
python -m pipeline.additions.generate_cleaned_hybrid_kb_draft
~~~

This command writes:

1. [knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft.csv](knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft.csv)
2. [knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft_report.json](knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft_report.json)

Use this only if you want a cleaned-hybrid full-text draft. Existing KB source files remain unchanged.

Prepare knowledge-base files:

- [knowledge-base/title_abstract_pos-neg_examples.csv](knowledge-base/title_abstract_pos-neg_examples.csv)
- [knowledge-base/full_text_pos-neg_examples.csv](knowledge-base/full_text_pos-neg_examples.csv)
- [knowledge-base/data_extraction_pos-neg_examples.csv](knowledge-base/data_extraction_pos-neg_examples.csv)
- [knowledge-base/data_extraction_schema.csv](knowledge-base/data_extraction_schema.csv) for data-extraction fields and Covidence header mapping, unless `DATA_EXTRACTION_SCHEMA_FILE` in [config/user_orchestrator.py](config/user_orchestrator.py) points elsewhere
- Optional full_text draft: [knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft.csv](knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft.csv)
- Optional draft report: [knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft_report.json](knowledge-base/full_text_pos-neg_examples_cleaned_hybrid_draft_report.json)
- Optional shared criteria file: [knowledge-base/eligibility_criteria.txt](knowledge-base/eligibility_criteria.txt)
	- It is used only when the active prompt contains `{eligibility_criteria}`.
	- If the placeholder is not present, prompt text is used as-is.
	- If the placeholder is present but the file is missing, the run continues with a warning.

If you want to use a non-default KB for a run, set the stage path in `KB_FILE_OVERRIDES` in [config/user_orchestrator.py](config/user_orchestrator.py).
For `full_text`, this can point to the optional cleaned-hybrid draft file.

Required columns in all knowledge-base files:
- `label` (`POS`/`NEG`)
- `text` (short evidence)

Required columns in the configured extraction schema CSV:
- `domain`
- `variable_name`
- `variable_type`
- `allowed_options`
- `instruction`
- `covidence_column_name`

Recommended: at least 10 POS and 10 NEG examples per file.

## PDF Preparation by Stage

- `full_text`: first run creates `input/per_paper_full_text/` folders only and then stops; add one PDF per folder and rerun `main.py` to start screening.
- `full_text` compact mode (default): expect `full_text_artifact.json` and `full_text_normalized.txt` per paper after screening.
- `full_text` full mode: expect legacy normalized sidecars (`*_normalized_text.txt`, `*_normalized_pages.json`, `*_normalized_meta.json`) per paper.
- `data_extraction`: ensure `input/per_paper_full_text/` already exists; pipeline builds `input/per_paper_data_extraction/`.

## Run the Pipeline

- Windows:

~~~bash
.venv\Scripts\python main.py
~~~

- macOS/Linux:

~~~bash
python main.py
~~~

Optional forensic check for one paper after a run:

~~~bash
python -m pipeline.additions.input_trace --paper-id <ID> --stage <stage> --show-full-prompt
~~~

## Quality Control Confirmation Workflow

- Pipeline creates QC sample files in `output/<stage>/`.
- Review QC CSV and readable TXT.
- Continue only if QC quality is acceptable.
- If not acceptable, adjust prompt/knowledge base and start a new QC round.

## Quick Pre-Run Checklist

- Bern network connected.
- `.env` has `LLM_API_KEY`.
- Correct `CURRENT_STAGE` in [config/user_orchestrator.py](config/user_orchestrator.py).
- Correct stage CSV present in [input](input).
- Stage knowledge-base file selected via `KNOWLEDGE_BASE_FILES`/`KB_FILE_OVERRIDES` exists and is not empty.
- If you selected the cleaned-hybrid full_text draft, verify its report JSON exists and confirms balanced POS/NEG output.
- If you use `{eligibility_criteria}` in a prompt, verify [knowledge-base/eligibility_criteria.txt](knowledge-base/eligibility_criteria.txt) exists and is up to date.
- For PDF stages, PDFs are placed in the generated per-paper folders.

## Throughput Tips
- Balanced profile defaults are now preconfigured in [config/user_orchestrator.py](config/user_orchestrator.py): `top_k=10`, `chunk_size=20`, `async_max_concurrency=18`.
- Keep `top_k` modest (e.g., 6–10) and `chunk_size` moderately sized (e.g., 20-25) for `full_text`/`data_extraction` to cut embedding/LLM load (`title_abstract` now uses full Title+Abstract directly).
- For large `title_abstract` runs, tune async throughput with `LLM_SETTINGS["async_max_concurrency"]` and retry/backoff parameters in [config/user_orchestrator.py](config/user_orchestrator.py).
- Use QC-only first, then full run; each run writes new timestamped outputs—no need to merge manually.
- Large PDFs: keep under a practical size budget; the reader now supports optional page caps (configurable in code if needed).
- Ensure the latest CSV per stage is present; the tool auto-picks the newest match per pattern.

---
**Read next:** [review_procedure.md](review_procedure.md)
