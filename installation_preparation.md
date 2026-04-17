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
- First run may download NLTK tokenizer data.
- `pdfplumber` is required for PDF stages.
- If `codecarbon` is unavailable, the pipeline now continues without emissions tracking (warning only).

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

Keep defaults unless you know why to change them.

## Prepare Required Inputs

Load CSVs in [input/](input/) with this structure: accepts Title, Abstract, Paper # / Accession Number / DOI / Ref / Study.
Place CSV exports in [input](input) with file names including:

- `title_abstract`: `*_screen_csv_*.csv`
- `full_text`: `*_select_csv_*.csv`
- `data_extraction`: `*_included_csv_*.csv`

Prepare knowledge-base files:

- [knowledge-base/title_abstract_pos-neg_examples.csv](knowledge-base/title_abstract_pos-neg_examples.csv)
- [knowledge-base/full_text_pos-neg_examples.csv](knowledge-base/full_text_pos-neg_examples.csv)
- [knowledge-base/data_extraction_pos-neg_examples.csv](knowledge-base/data_extraction_pos-neg_examples.csv)
- Optional shared criteria file: [knowledge-base/eligibility_criteria.txt](knowledge-base/eligibility_criteria.txt)
	- It is used only when the active prompt contains `{eligibility_criteria}`.
	- If the placeholder is not present, prompt text is used as-is.
	- If the placeholder is present but the file is missing, the run continues with a warning.

Required columns in all knowledge-base files:
- `label` (`POS`/`NEG`)
- `text` (short evidence)

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
- Stage knowledge-base file exists and is not empty.
- If you use `{eligibility_criteria}` in a prompt, verify [knowledge-base/eligibility_criteria.txt](knowledge-base/eligibility_criteria.txt) exists and is up to date.
- For PDF stages, PDFs are placed in the generated per-paper folders.

## Throughput Tips
- Balanced profile defaults are now preconfigured in [config/user_orchestrator.py](config/user_orchestrator.py): `top_k=8`, `chunk_size=24`, `async_max_concurrency=18`.
- Keep `top_k` modest (e.g., 6–10) and `chunk_size` moderately sized (e.g., 20-25) for `full_text`/`data_extraction` to cut embedding/LLM load (`title_abstract` now uses full Title+Abstract directly).
- For large `title_abstract` runs, tune async throughput with `LLM_SETTINGS["async_max_concurrency"]` and retry/backoff parameters in [config/user_orchestrator.py](config/user_orchestrator.py).
- Use QC-only first, then full run; each run writes new timestamped outputs—no need to merge manually.
- Large PDFs: keep under a practical size budget; the reader now supports optional page caps (configurable in code if needed).
- Ensure the latest CSV per stage is present; the tool auto-picks the newest match per pattern.

---
**Read next:** [review_procedure.md](review_procedure.md)