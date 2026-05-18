# Pipeline Architecture Map

This map is the shortest route through the codebase for human maintainers.

## Boundary Rule

`pipeline/` is generic machinery. Review topic facts belong in `config/`, prompt text files, `input/`, `knowledge-base/`, and the data-extraction schema CSV.

## Main Runtime Path

1. `main.py` handles the interactive workflow and delegates execution.
2. `pipeline/core/run_screening.py` resolves stage defaults, output paths, QC paths, and launches the pipeline.
3. `pipeline/core/pipeline.py` owns shared orchestration, config wiring, common retrieval/LLM utilities, and the stable `PaperScreeningPipeline` public import.
4. Stage mixins hold stage-specific methods:
   - `pipeline/core/stage_title_abstract.py`
   - `pipeline/core/stage_full_text.py`
   - `pipeline/core/stage_data_extraction.py`
5. `pipeline/core/extraction_schema.py` converts `DATA_EXTRACTION_SCHEMA_FILE` into the runtime JSON contract.
6. `pipeline/core/extraction_io.py` is the data-extraction artifact boundary for prepared paper inputs, dictionary artifact validation, idempotent completed-output checks, and atomic per-paper JSONL/CSV writes.
7. `pipeline/selection/` handles chunking, configured section-heading detection, prompt-derived retrieval signals, PDF parsing, and embedding-based chunk selection; retrieval vocabulary is read from `RETRIEVAL_SIGNAL_SETTINGS`.
8. `pipeline/additions/` contains operator-facing utilities: validation, audit, retry, run indexing, resource reporting, exports, and trace generation.

## Stage-Specific Reading Guide

- Title/abstract: start in `stage_title_abstract.py`, then read `PaperScreeningPipeline.run()` for shared output handling.
- Full text: start in `stage_full_text.py`, then read `_process_non_title_async_batch()` and `_process_paper_async()` in `pipeline.py`.
- Data extraction: start in `stage_data_extraction.py`, then read `extraction_schema.py` and `extraction_io.py`.
- Validation/manuscript support: start in `pipeline/additions/stats_engine.py`, `input_trace.py`, `resource_usage.py`, and `extraction_plausibility_audit.py`.

## Smoke And Boundary Check

Run:

```powershell
.\.venv\Scripts\python.exe -m pipeline.smoke.generic_pipeline_smoke
.\.venv\Scripts\python.exe -m pipeline.smoke.stage_handoff_smoke
```

These checks confirm that `pipeline/` has no active-protocol topic terms or hardcoded admin headers, verify data-extraction prompt/schema assembly, run one local fake-model paper through `title_abstract`, `full_text`, and `data_extraction`, and validate the stage handoff CSV writer.

## Refactor Rule

Prefer deleting stale wrappers and dead branches before moving code. Stage-specific methods belong in the stage mixins; shared retrieval, LLM, metadata, and output orchestration stays in `pipeline.py`. Retrieval or section terms must be edited in config, not in `pipeline/`.
