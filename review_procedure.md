# Review Procedure (document 3/6)

**Read prior:** [installation_preparation.md](installation_preparation.md)
**Read next:** [pipeline_validation_checks.md](pipeline_validation_checks.md)

End-to-end procedure for the three pipeline stages with mandatory human quality control (QC).

## Core principles

- Follow JBI and PRISMA-ScR for review design and reporting.
- Use deterministic QC sampling (~10%) before full automation at each stage.
- Validate AI vs human QC decisions before processing remaining papers.
- Keep stage outputs, retries, and logs fully traceable.
- For `title_abstract`, screening uses asynchronous batched LLM requests with bounded concurrency and backoff.
- Screening outputs are accepted only after strict JSON schema validation; invalid responses are retried automatically.

## Required setup

- Bern network access (eduroam/campus LAN/VPN).
- `.env` with `LLM_API_KEY`.
- `CURRENT_STAGE` set in [config/user_orchestrator.py](config/user_orchestrator.py).
- Stage KB file exists with `label` (`POS`/`NEG`) and `text` columns.

## Stage order

1. `title_abstract`
2. `full_text`
3. `data_extraction`

Do not skip stage order.

## Run model per stage

Every stage follows two passes:

1. QC-only pass (deterministic ~10% sample)
2. Remaining-pass run (after validation approval)

This is enforced by terminal prompts in `main.py`.

## Stage 1: title_abstract

1. Import screen CSV to `input/` (`*_screen_csv_*.csv`).
2. Prepare `knowledge-base/title_abstract_pos-neg_examples.csv`.
3. Run `main.py` to create and screen QC sample.
4. Humans review the same QC sample.
5. Run validation (`stats_engine`) with select/irrelevant CSVs.
6. If validation is acceptable, continue to remaining papers.

## Stage 2: full_text

1. Export select CSV to `input/` (`*_select_csv_*.csv`).
2. Prepare `knowledge-base/full_text_pos-neg_examples.csv`.
3. Run `main.py` to create `input/per_paper_full_text/`.
4. Add one PDF per paper folder.
5. Run QC-only screening, then human QC, then validation.
6. If validation is acceptable, continue to remaining papers.

## Stage 3: data_extraction

1. Export included CSV to `input/` (`*_included_csv_*.csv`).
2. Prepare `knowledge-base/data_extraction_pos-neg_examples.csv`.
3. Ensure `input/per_paper_full_text/` exists from prior stage.
4. Run `main.py` to build `input/per_paper_data_extraction/`.
5. Run QC-only extraction, then human QC extraction, then validation.
6. If validation is acceptable, continue to remaining papers.

## Terminal commands and decision tree (exact runtime prompts)

### Commands you run manually

- Windows main run: `.venv\Scripts\python main.py`
- macOS/Linux main run: `python main.py`
- Manual validation (optional): `python -m pipeline.additions.stats_engine`
- Manual reproducibility trace (optional): `python -m pipeline.additions.input_trace --paper-id <ID> --stage <stage>`
- Manual backup (optional): `python backup_to_github.py`

### Command automatically started by main run

- Validation subprocess after confirmation: `python -m pipeline.additions.stats_engine` (using active interpreter)

### Exact prompt decision tree

```text
START -> run main.py
	|
	+-- Prompt: [qc] Are study tags the same since the last run? [y/n]:
	|     |- y -> continue
	|     \- n -> STOP (update STUDY_TAGS_INCLUDE/STUDY_TAGS_IGNORE in config/user_orchestrator.py)
	|
	+-- If pending retry CSV exists:
	|     Prompt: [retry] Run pending retry CSV first? [y/n]:
	|       |- y -> retry run executes first (QC disabled for retry), retry manifest updated
	|       \- n -> continue with normal stage flow
	|
	+-- QC flow (if QC_ENABLED=True):
	|     Prompt from pipeline: Proceed with QC screening? [y/n]:
	|       |- y -> QC-only run executes
	|       \- n -> STOP (QC files are created; rerun later to continue)
	|
	+-- After QC-only run:
	|     Prompt: [qc] Have estimated reviewer times (minutes) been inserted for human reviewers at CURRENT_STAGE='<stage>'? [y/n]:
	|       |- y -> continue
	|       \- n -> STOP (validation not run; continue only after entering reviewer minutes)
	|
	+-- Prompt: [qc] Run validation now? [y/n]:
	|       |- y -> runs: python -m pipeline.additions.stats_engine
	|       |      |- validation success -> next prompt
	|       |      \- validation failure -> STOP
	|       \- n -> STOP
	|
	+-- Prompt: [qc] Are you satisfied with validation results and do you want to continue with screening of the remaining papers? [y/n]:
	|       |- y -> remaining-pass run executes
	|       \- n -> STOP (refine prompt/KB, then rerun)
	|
	+-- During/after any run with unresolved errors:
	|     Note: papers with deterministic token-limit/context-overflow errors are filtered out from auto-retry prompts
	|     Prompt: [retry] Re-screen these papers now? [y/n]:
	|       |- y -> retry run executes for listed papers, outputs kept separate, retry manifest updated
	|       \- n -> continue without retry (errors remain logged)
	|
	\-- End-of-run backup prompt (only when all prior prompts were yes):
				Prompt: Do you want to back up your changes to GitHub now? (y/n):
					|- y -> runs backup_to_github.py, which executes git pull -> git add . -> git commit -> git push
					\- n -> finish without backup
```

### Invalid prompt input behavior

- For yes/no prompts, invalid input loops with `Please answer 'y' or 'n'.`
- In non-interactive terminals, prompt-driven flow stops early.

## Validation outputs

Screening stages:
- QC stats report (`*_qc_sample_validation_stats_report_*.txt`)
- QC matrix (`*_qc_sample_validation_matrix_*.png`)
- QC alignment (`*_qc_sample_validation_alignment_*.csv`)

Data extraction:
- extraction accuracy report
- extraction discrepancies CSV

## Resource and audit outputs

- Resource usage logs per run (`*_resource_usage_*.log`)
- CodeCarbon emissions CSVs
- Retry manifest (`output/<stage>/<stage>_retry_manifest.jsonl`)

## Decision rule

- If QC validation is weak: refine prompt/knowledge-base and screen QC sample again.
- If QC validation is strong: continue to remaining papers.

---
**Read next:** [pipeline_validation_checks.md](pipeline_validation_checks.md)
