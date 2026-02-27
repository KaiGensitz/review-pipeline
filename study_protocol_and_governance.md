# Study Protocol and Governance (document 6/6)

**Read prior:** [pipeline_architecture_reference.md](pipeline_architecture_reference.md)
**Go back to:** [readme.md](readme.md)

This file captures methodological and governance statements that are not directly enforced by runtime code.

## Intended methodological framework

- Review planning and reporting follow JBI and PRISMA-ScR guidance.
- Human experts define inclusion/exclusion logic and supervise quality control (QC).
- Large language model outputs are support artifacts for screening/extraction, not autonomous publication decisions.

## Infrastructure and deployment context

- Runs are intended for University of Bern infrastructure and network access patterns, can though be executed with any API key containing embedding and large language models.
- If containerization (for example Docker) is used in a deployment, this should be documented per study run.
- Endpoint model availability can change; each study should archive exact model IDs and endpoint settings used.

## Publication-level reporting commitments

For publication and reproducibility packages, document:

- Exact model identifiers and runtime settings (including temperature and token limits).
- Prompt versions used per stage.
- Per-paper input fingerprint policy (`llm_input_sha256` and `full_prompt_sha256`) and how discrepant cases were traced.
- Chunking and retrieval settings (`chunk_size`, overlap, `top_k`, threshold).
- Validation metrics and acceptance rationale.
- Human adjudication workflow and reviewer role definitions.

## Suggested study-level acceptance thresholds

These are protocol suggestions and must be explicitly confirmed per study:

- Screening: sensitivity target and agreement target (for example PABAK threshold).
- Data extraction: concordance/accuracy threshold per field and overall.
- Escalation rule when QC performance is below threshold (prompt/knowledge-base refinement and repeat QC).

## Sustainability and ethics notes

- CodeCarbon and resource logs provide run-level environmental and compute traces.
- Time-savings estimates depend on reviewer-minute inputs and should be interpreted as operational estimates.
- Human reviewers remain accountable for final inclusion/exclusion and extraction decisions.

## Data governance and release

Per study, define and archive:

- Input export provenance and hash/archive process.
- Procedure for on-demand reconstruction of paper-level model input text (`pipeline.additions.input_trace`) for audit cases.
- Which artifacts are public vs restricted.
- Final release license and repository destination.
- Redaction rules for sensitive metadata before release.

## Boundary to implementation docs

- For implemented runtime behavior, see [pipeline_architecture_reference.md](pipeline_architecture_reference.md).
- For operational run sequence, see [review_procedure.md](review_procedure.md).
- For pre-run checks and expected outputs, see [pipeline_validation_checks.md](pipeline_validation_checks.md).

---
**Go back to:** [readme.md](readme.md)
