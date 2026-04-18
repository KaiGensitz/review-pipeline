# Review Procedure (document 3/6)

**Read prior:** [installation_preparation.md](installation_preparation.md)

## Document Purpose

This document defines the execution order for screening and extraction runs.

## What to Expect

- Stage-by-stage run sequence.
- QC and validation decision flow.
- Retry behavior and operator prompt tree.

## How to Use This Document

1. Use this as the runbook during active execution.
2. Follow the prompt decision tree exactly.
3. Continue to validation checks after understanding the flow.

## Core Principles

- Follow JBI and PRISMA-ScR for review design and reporting.
- Use deterministic QC sampling (~10%) before full automation at each stage.
- Validate AI vs human QC decisions before processing remaining papers.
- Keep stage outputs, retries, and logs fully traceable.
- For `title_abstract`, screening uses asynchronous batched LLM requests with bounded concurrency and backoff.
- Screening outputs are accepted only after strict JSON schema validation; invalid responses are retried automatically.

## Required Setup

- Bern network access (eduroam/campus LAN/VPN).
- `.env` with `LLM_API_KEY`.
- `CURRENT_STAGE` set in [config/user_orchestrator.py](config/user_orchestrator.py).
- `LLM_SETTINGS["context_window_total_tokens"]` and `LLM_SETTINGS["max_tokens"]` set consistently for the active model (`max_tokens < context_window_total_tokens`).
- Stage KB file exists with `label` (`POS`/`NEG`) and `text` columns.

## Stage Order

1. `title_abstract`
2. `full_text`
3. `data_extraction`

Do not skip stage order.

## Run Model per Stage

Every stage follows two passes:

1. QC-only pass (deterministic ~10% sample)
2. Remaining-pass run (after validation approval)

This is enforced by terminal prompts in `main.py`.

## Exact Runtime Sequence (1-X)

Use this section when you need to know exactly when parsing, embeddings, LLM calls, and decision files happen.

### A) Operator flow with optional QC branch

1. Run `main.py`.
2. Preflight checks run first: API key present, stage valid, required CSVs exist, tokenizer resources available, interactive terminal available.
3. If a pending retry CSV exists, you can run retry first. If yes, retry runs before any new QC/main pass.
4. Stage preflight runs:
	- `full_text`: first run can be setup-only (`split_only=True`) to create `input/per_paper_full_text/`; then you upload one PDF per folder.
	- `data_extraction`: setup-only pass creates/refreshes `input/per_paper_data_extraction/`.
5. If `QC_ENABLED=True`, QC-only pass runs first.
6. After QC-only pass, validation prompts run in order:
	- reviewer-minutes confirmation,
	- validation execution (`python -m pipeline.additions.stats_engine`),
	- approval to continue.
7. If QC validation is accepted, remaining-pass screening starts.
8. If `QC_ENABLED=False`, the run skips QC branch and goes directly to remaining-pass screening.
9. At end of any pass, unresolved errors can trigger retry prompts. Retry outputs are written separately and tracked in the retry manifest.

### B) Per-paper technical call chain (screening run)

1. `PaperScreeningPipeline.run()` builds the planned paper list and opens output writers.
2. For each paper, `_process_paper_async()` is executed (sync wrapper or async batch, depending on stage/config).
3. `_prepare_chunks()` is called.
4. Parsing happens inside `_prepare_chunks()` only for `full_text`/`data_extraction`:
	- `_load_pdf_text()` resolves the paper PDF and extracts normalized text.
	- If `USE_ADVANCED_PDF_PARSER=1`, extraction uses `extract_markdown_from_pdf_with_level()` with parser order (pymupdf4llm primary -> Docling on parser failure -> OCR only when needed).
	- Parser provenance is stored as `parser_level` in each paper's `full_text_artifact.json` (compact artifact mode).
5. Chunking runs after parsing (`chunk_fulltext_sentences`). If no usable text/chunks are produced, LLM is skipped and an error is logged.
6. Embedding-based retrieval is called next via `_select_chunks_with_rescue()`.
7. Inside retrieval, `SelectionEngine.select()` embeds candidate chunk texts (non-title chunks) and scores them against POS/NEG centroids from the stage knowledge base.
8. Prompt assembly runs after retrieval (`_format_chunks_for_prompt()`), using selected evidence chunks.
9. LLM call happens after prompt assembly via `_call_llm_async()` (or sync variant), with retry/backoff and schema validation.
10. Decision is sanitized/validated, then recorded in memory for the current paper.
11. Decision/output visibility timing:
	- Eligibility JSONL: appended to buffer immediately for that paper, flushed to disk every 64 records (or at final run flush).
	- Selected chunks: written per paper in the same loop iteration.
	- Readable text summary: written in the same loop iteration.
	- Run-level summaries/index rows: written at finalization after loop completion.
12. After all papers are processed, remaining buffers are flushed, summary rows are appended, and stage index files are updated.

## Stage 1: Title Abstract

1. Import screen CSV to `input/` (`*_screen_csv_*.csv`).
2. Prepare `knowledge-base/title_abstract_pos-neg_examples.csv`.
3. Run `main.py` to create and screen QC sample.
4. Humans review the same QC sample.
5. Run validation (`stats_engine`) with select/irrelevant CSVs.
7. If validation is acceptable, continue to remaining papers.

## Stage 2: Full Text

1. Export select CSV to `input/` (`*_select_csv_*.csv`).
2. Prepare `knowledge-base/full_text_pos-neg_examples.csv`.
3. Run `main.py` once to create `input/per_paper_full_text/` folders (setup-only run).
4. Add one PDF per paper folder.
5. Run QC-only screening, then human QC, then validation.
6. Check per-paper artifacts after QC:
	- compact mode (default): `full_text_artifact.json` and `full_text_normalized.txt`
	- full mode: legacy normalized sidecars (`*_normalized_text.txt`, `*_normalized_pages.json`, `*_normalized_meta.json`)
	- optional compact sidecar: `full_text_selected_chunks.jsonl` only if enabled in config
6. If validation is acceptable, continue to remaining papers.

## Stage 3: Data Extraction

1. Export included CSV to `input/` (`*_included_csv_*.csv`).
2. Prepare `knowledge-base/data_extraction_pos-neg_examples.csv`.
3. Ensure `input/per_paper_full_text/` exists from prior stage.
4. Run `main.py` to build `input/per_paper_data_extraction/`.
5. Run QC-only extraction, then human QC extraction, then validation.
6. If validation is acceptable, continue to remaining papers.

## Terminal Commands and Decision Tree

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
					|- y -> runs backup_to_github.py, which executes git pull --ff-only -> git add -A -> git commit -> git push
					\- n -> finish without backup
```

### Invalid prompt input behavior

- For yes/no prompts, invalid input loops with `Please answer 'y' or 'n'.`
- In non-interactive terminals, prompt-driven flow stops early.

## Validation Outputs

Screening stages:
- QC stats report (`*_qc_sample_validation_stats_report_*.txt`)
- QC matrix (`*_qc_sample_validation_matrix_*.png`)
- QC alignment (`*_qc_sample_validation_alignment_*.csv`)

Data extraction:
- extraction accuracy report
- extraction discrepancies CSV

## Resource and Audit Outputs

- Resource usage logs per run (`*_resource_usage_*.log`)
- CodeCarbon emissions CSVs
- Retry manifest (`output/<stage>/<stage>_retry_manifest.jsonl`)

## Decision Rule

- If QC validation is weak: refine prompt/knowledge-base and screen QC sample again.
- If QC validation is strong: continue to remaining papers.

---
**Read next:** [pipeline_validation_checks.md](pipeline_validation_checks.md)
