# Installation and Preparation Guide (document 2/6)

**Read prior:** [readme.md](readme.md)
**Read next:** [review_procedure.md](review_procedure.md)

This guide focuses only on installation and preparation before running the pipeline.

## 1) Before you start

- Use Python 3.12 or newer.
- Ensure University of Bern network access (eduroam, campus LAN, or VPN).
- Open a terminal in the project folder:

~~~bash
cd "path_to_working_directory"
~~~

## 2) Create and activate the virtual environment

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

## 3) Install dependencies

~~~bash
python -m pip install -r requirement.txt
~~~

Notes:
- Wait until installation completes successfully.
- First run may download NLTK tokenizer data.

## 4) Add API key

Create/update `.env` in project root with:

~~~bash
LLM_API_KEY=your_api_key_here
~~~

## 5) Configure one file only

Edit [config/user_orchestrator.py](config/user_orchestrator.py):

- `CURRENT_STAGE` (`title_abstract`, `full_text`, `data_extraction`)
- `LLM_MODEL`
- `EMBED_MODEL`
- reproducibility controls in `LLM_SETTINGS`:
	- `temperature` (recommended `0.0`)
	- `top_p` (recommended `1.0`)
	- `seed` (set for reproducibility audits; any integer as value, e.g. `42`)

Keep defaults unless you know why to change them.

## 6) Prepare required inputs

Load CSVs in [input/](input/) with this structure: accepts Title, Abstract, Paper # / Accession Number / DOI / Ref / Study.
Place CSV exports in [input](input) with file names including:

- `title_abstract`: `*_screen_csv_*.csv`
- `full_text`: `*_select_csv_*.csv`
- `data_extraction`: `*_included_csv_*.csv`

Prepare knowledge-base files:

- [knowledge-base/title_abstract_pos-neg_examples.csv](knowledge-base/title_abstract_pos-neg_examples.csv)
- [knowledge-base/full_text_pos-neg_examples.csv](knowledge-base/full_text_pos-neg_examples.csv)
- [knowledge-base/data_extraction_pos-neg_examples.csv](knowledge-base/data_extraction_pos-neg_examples.csv)
- [knowledge-base/eligibility_criteria.txt](knowledge-base/eligibility_criteria.txt) (external criteria injected at runtime for `title_abstract` and `full_text`)

Required columns in all knowledge-base files:
- `label` (`POS`/`NEG`)
- `text` (short evidence)

Recommended: at least 10 POS and 10 NEG examples per file.

## 7) PDF preparation (stage-specific)

- `full_text`: run once, then add one PDF per folder in `input/per_paper_full_text/`.
- `data_extraction`: ensure `input/per_paper_full_text/` already exists; pipeline builds `input/per_paper_data_extraction/`.

## 8) Run the pipeline

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

## 9) Quality control (QC) confirmation workflow

- Pipeline creates QC sample files in `output/<stage>/`.
- Review QC CSV and readable TXT.
- Continue only if QC quality is acceptable.
- If not acceptable, adjust prompt/knowledge base and start a new QC round.

## 10) Quick pre-run checklist

- Bern network connected.
- `.env` has `LLM_API_KEY`.
- Correct `CURRENT_STAGE` in [config/user_orchestrator.py](config/user_orchestrator.py).
- Correct stage CSV present in [input](input).
- Stage knowledge-base file exists and is not empty.
- For `title_abstract` or `full_text`: [knowledge-base/eligibility_criteria.txt](knowledge-base/eligibility_criteria.txt) exists and is not empty.
- For PDF stages, PDFs are placed in the generated per-paper folders.

## 11) Throughput tips (handling thousands of papers)
- Keep `top_k` modest (e.g., 6–10) and `chunk_size` moderately sized (e.g., 20-25) for `full_text`/`data_extraction` to cut embedding/LLM load (`title_abstract` now uses full Title+Abstract directly).
- For large `title_abstract` runs, you can set `SCREENING_DEFAULTS["title_abstract_workers"]` in [config/user_orchestrator.py](config/user_orchestrator.py) to `2-4` for concurrent API calls.
- Use QC-only first, then full run; each run writes new timestamped outputs—no need to merge manually.
- Large PDFs: keep under a practical size budget; the reader now supports optional page caps (configurable in code if needed).
- Ensure the latest CSV per stage is present; the tool auto-picks the newest match per pattern.

---
**Read next:** [review_procedure.md](review_procedure.md)