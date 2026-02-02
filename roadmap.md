For current pipeline found in readme, over 3800 lines of code were added from codex (Status at 19th December 2025, at 5:10 p.m.).


# Roadmap: Screening and Data Extraction (current stage: validation + data_extraction proof)

Status snapshot (Feb 2026)
- Title/abstract screening: proof-of-concept done; awaiting gold-standard validation on the 10% human sample.
- Full_text screening: proof-of-concept done; awaiting gold-standard validation on the 10% human sample.
- Data_extraction: wiring and prompts ready; proof-of-concept run still pending once consensus table is defined.

Milestones (state)
- M1 (gold-standard validation for title_abstract & full_text): pending – run stats_engine on latest QC batches and lock prompts if thresholds met.
- M2 (data_extraction proof with agreed schema + consensus table): pending – finalize extraction fields and run first QC-only pass.
- M3 (evaluation on gold set + docs refresh): pending – schedule after M1/M2 results.

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
- Screening: run gold-standard validation on the 10% human sample for title_abstract and full_text (stats_engine on latest QC batches).
- Extraction: validate against the adjudicated consensus table once it is ready (already supported in pipeline/additions/stats_engine.py).
- QC-only pass: keep LLM on the QC list first so humans and the model judge the same 10% before the 90% run.
- Optional: use <stage>_human_validation_qc_sample_batch_YYYYMMDD.csv for validation when provided.
- Validation binds to the QC sample matching the screening timestamp.

## Phase 6 — Robustness and ergonomics (next)
- Confirm OCR stays disabled; scanned PDFs are skipped with clear warnings.
- Consider embedding cache persistence to reduce API costs (optional).
- Add a KB provenance log template (paper_id/page/line/chunk_id) for reproducible evidence trails.

## Phase 7 — Documentation & handoff (ongoing)
- README and pipeline_validation_checks reflect current outputs and workflows.
- Quick-start + pitfalls added for non-coders; keep stage prompts and QC guardrails aligned with README updates.

## Upcoming actions (explicit)
- Lock the QC sample used for validation (reuse the latest stage-scoped QC files in output/<stage>/).
- Run stats_engine for title_abstract and full_text using the newest QC eligibility files; record PABAK and sensitivity/specificity.
- If metrics meet thresholds, proceed to remaining-sample screening; otherwise, refine prompts and rerun QC.
- Finalize the data_extraction schema and consensus table, then run a QC-only extraction pass and validate.
- After M1/M2 are achieved, refresh README/pipeline_validation_checks with the validated thresholds and any prompt updates.

## Risks & mitigations
- Token/cost blow-up: add per-paper chunk caps, dual-pass fallback, and clear warnings when near context limits.
- Noisy PDFs: fallback extractor, minimal cleaning, and section tagging to downweight low-signal parts.
- Schema drift: keep extraction schema versioned in config; validate outputs and coerce missing fields to null with reasons.

## Immediate next actions
- Finalize the extraction fields and success criteria for the gold set; adjudicate consensus labels.
- Run gold‑standard validation for title_abstract and full_text on the 10% sample.
- Run data_extraction proof‑of‑concept with the finalized prompt and validate via consensus table.
