For current pipeline found in readme, over 3800 lines of code were added from codex (Status at 19th December 2025, at 5:10 p.m.).


# Roadmap: Screening and Data Extraction (current stage: validation + data_extraction proof)

Proof-of-concept runs for title/abstract and full_text screening are complete. The next priority is gold‑standard validation (10% human sample) and proof of concept for data_extraction.

## Guiding principles
- Single configuration surface: keep user-facing knobs in config/user_orchestrator.py (including CURRENT_STAGE for validation mode).
- Deterministic preprocessing: stable chunk IDs for traceability, reproducible sampling, and auditable logs.
- Lightweight, auditable outputs: JSONL for machine use, TXT/CSV for non-coders, with CodeCarbon totals and per‑token rates where possible.
- Incremental rollout: confirm abstract-only parity, then add fulltext in gated stages, then data extraction.

## Phase 0 — Baseline & inputs (done)
- Title/abstract and full_text CSV inputs wired; per‑paper full_text folders created automatically.
- Knowledge base templates are in place; human QC evidence still required for full_text/data_extraction KBs.
- Chunking now includes page/line metadata (approximate; derived from extracted text lines).

## Phase 1 — Full_text screening (proof completed)
- Full_text screening uses per‑paper PDFs and chunk selection; missing PDFs are logged and skipped.
- Selected chunks are stored per paper in input/per_paper_full_text/<paper_folder>/full_text_selected_chunks.jsonl.

## Phase 2 — Relevance filtering (done)
- POS/NEG selector already applied to title/abstract and full_text chunks; titles always kept.

## Phase 3 — LLM screening with fulltext context (done)
- Full_text LLM screening is wired with context size guardrails and resource logging.

## Phase 4 — Data extraction MVP (proof pending)
- Extraction prompt and outputs are wired (JSONL + CSV per paper).
- Evidence.json stored per paper in output/data_extraction/<paper_folder>/ linking extracted fields to selected chunks.
- Selected chunks are reused from full_text when available (copied to input/per_paper_data_extraction/).

## Phase 5 — Quality, evaluation, and QC (next priority)
- Screening: implement gold‑standard validation on the 10% human sample for title_abstract and full_text.
- Extraction: run consensus‑table validation (already implemented in pipeline/additions/stats_engine.py) once adjudicated data are ready.
- Suggestion: add a QC-only LLM pass (LLM screens only the QC list) so humans and the LLM evaluate the same 10% before running a full stage.
- Extra option: allow a human QC file named <stage>_human_validation_qc_sample_batch_YYYYMMDD.csv to be used directly for validation.
- Validation now uses only the QC sample list that matches the screening timestamp.

## Phase 6 — Robustness and ergonomics (next)
- Confirm OCR stays disabled; scanned PDFs are skipped with clear warnings.
- Consider embedding cache persistence to reduce API costs (optional).
- Add a KB provenance log template (paper_id/page/line/chunk_id) for reproducible evidence trails.

## Phase 7 — Documentation & handoff (ongoing)
- README and pipeline_validation_checks reflect current outputs and workflows.
- Quick-start + pitfalls added for non-coders; keep stage prompts and QC guardrails aligned with README updates.

## Milestones (updated)
- M1: Gold‑standard validation for title_abstract and full_text (10% human sample).
- M2: Data_extraction proof‑of‑concept run with agreed extraction schema and consensus table.
- M3: Evaluation on gold set + README/docs refresh.

## Risks & mitigations
- Token/cost blow-up: add per-paper chunk caps, dual-pass fallback, and clear warnings when near context limits.
- Noisy PDFs: fallback extractor, minimal cleaning, and section tagging to downweight low-signal parts.
- Schema drift: keep extraction schema versioned in config; validate outputs and coerce missing fields to null with reasons.

## Immediate next actions
- Finalize the extraction fields and success criteria for the gold set; adjudicate consensus labels.
- Run gold‑standard validation for title_abstract and full_text on the 10% sample.
- Run data_extraction proof‑of‑concept with the finalized prompt and validate via consensus table.
