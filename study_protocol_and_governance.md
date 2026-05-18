# Study Protocol and Governance (document 6/6)

**Read prior:** [pipeline_architecture_reference.md](pipeline_architecture_reference.md)

## Document Purpose

This document captures methodological and governance commitments that are not directly enforced by runtime code.

## What to Expect

- Protocol-level reporting and acceptance guidance.
- Sustainability, ethics, and release considerations.
- Clear boundaries between governance and implementation docs.

## How to Use This Document

1. Use this file when preparing protocol, methods, or publication material.
2. Pair these commitments with implementation evidence from technical docs.
3. Continue to the function appendix for script-level references.

## Intended Methodological Framework

- Review planning and reporting follow JBI and PRISMA-ScR guidance.
- Human experts define inclusion/exclusion logic and supervise quality control (QC).
- Large language model outputs are support artifacts for screening/extraction, not autonomous publication decisions.

## Infrastructure and Deployment Context

- Runs are intended for University of Bern infrastructure and network access patterns, can though be executed with any API key containing embedding and large language models.
- Endpoint model availability can change; each study should archive exact model IDs and endpoint settings used.

## Publication-Level Reporting Commitments

For publication and reproducibility packages, document:

- Exact model identifiers and runtime settings (including temperature, `max_tokens`, and `context_window_total_tokens`).
- Prompt versions used per stage.
- Data-extraction schema KB version (`DATA_EXTRACTION_SCHEMA_FILE`, default `knowledge-base/data_extraction_schema.csv`) and its consensus/export column mappings (`consensus_column_name`, with legacy `covidence_column_name` support).
- For data extraction, report the prompt as the conceptual review framework and the schema CSV as the authoritative machine contract; the runtime prompt combines both automatically before the evidence section.
- External export/admin mappings configured for the study (`CSV_METADATA_COLUMN_ALIASES`, `DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS`, optional `DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES`), especially if the input source changes.
- Data-extraction evidence mode:
  - `full_text`: higher recall and better quote auditability, with higher token/resource use; `data_extraction_pos-neg_examples.csv` is mostly optional.
  - `selected_chunks`: lower token/resource use, but depends on retrieval quality; `data_extraction_pos-neg_examples.csv` should be archived and justified because it shapes evidence selection.
- Current protocol-specific study tags and domain/export aliases are user-editable in `config/user_orchestrator.py`; changing topic should require prompt/KB/config edits, not Python code edits.
- Per-paper input fingerprint policy (`llm_input_sha256` and `full_prompt_sha256`) and how discrepant cases were traced.
- Per-paper artifact persistence mode used in full_text (`artifact_mode=compact|full`) and whether legacy sidecars were retained.
- Chunking and retrieval settings (`chunk_size`, overlap, `top_k`, threshold).
- Validation metrics and acceptance rationale.
- Human adjudication workflow and reviewer role definitions.

## Suggested Study-Level Acceptance Thresholds

These are protocol suggestions and must be explicitly confirmed per study:

- Screening: sensitivity target and agreement target (for example PABAK threshold).
- Data extraction: concordance/accuracy threshold per field and overall.
- AI-first expert oversight: reviewer assignment config, packet paths, expert decisions, corrections, error types/effects, and prompt/schema refinement triggers.
- Escalation rule when QC performance is below threshold (prompt/knowledge-base refinement and repeat QC).

## Sustainability and Ethics Notes

- CodeCarbon and resource logs provide run-level environmental and compute traces.
- Time-savings estimates depend on reviewer-minute inputs and should be interpreted as operational estimates.
- Human reviewers remain accountable for final inclusion/exclusion and extraction decisions.

## Data Governance and Release

Per study, define and archive:

- Input export provenance and hash/archive process.
- Procedure for on-demand reconstruction of paper-level model input text (`pipeline.additions.input_trace`) for audit cases.
- Which artifacts are public vs restricted.
- Policy for metadata synchronization between `full_text_artifact.json` (`metadata`) and human-readable normalized sidecars (`full_text_normalized.txt`) in compact mode.
- Final release license and repository destination.
- Redaction rules for sensitive metadata before release.

## Boundary to Implementation Docs

- For implemented runtime behavior, see [pipeline_architecture_reference.md](pipeline_architecture_reference.md).
- For operational run sequence, see [review_procedure.md](review_procedure.md).
- For pre-run checks and expected outputs, see [pipeline_validation_checks.md](pipeline_validation_checks.md).

---
**Read next:** [function_explanations_uid.md](function_explanations_uid.md)
**Go back to:** [readme.md](readme.md)
