# Function Explanation UID (appendix)

**Read prior:** [study_protocol_and_governance.md](study_protocol_and_governance.md)

## Document Purpose

This appendix provides function-level explanations for scripts and classes in the workspace.

## What to Expect

- Primary class descriptions per script.
- Constructor parameters and callable methods.
- Human-readable intent notes for key functions.

## How to Use This Document

1. Use this file when you need quick code-navigation support.
2. Search by filename heading, then by class or function name.
3. Return to the main document flow in [readme.md](readme.md) for operational guidance.

## backup_to_github.py

### Class BackupToGitHub
- Human readable hint: one-class backup workflow with explicit command methods and one run entrypoint.
- __init__ parameters: backup_message
#### BackupToGitHub.__init__(backup_message)
- Human readable hint: __init__ stores the commit message used for the backup commit.

#### BackupToGitHub.run_command(cmd)
- Human readable hint: run one git command and stop the script when the command fails.

#### BackupToGitHub.run_backup()
- Human readable hint: execute pull, stage tracked code/doc updates, commit, and push in safe sequence.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### run(cmd)
- Human readable hint: Compatibility wrapper for older calls.

#### main()
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

## config/user_orchestrator.py

### Class UserConfig
- Human readable hint: Static snapshot of user-facing settings for the current run. Note: this bundles all inputs so other scripts can read a single object.
- Human readable hint: The top block marked `USER-EDITABLE` is the protocol boundary: change `CURRENT_STAGE`, prompts/KBs, `STUDY_TAGS_INCLUDE`, `STUDY_TAGS_IGNORE`, and `DATA_EXTRACTION_SCHEMA_FILE` there when the review topic changes.
- Human readable hint: `DATA_EXTRACTION_SCHEMA_FILE` points to the CSV that defines extraction variables and exact human consensus/export header mappings.
- Human readable hint: `CSV_METADATA_COLUMN_ALIASES` is where external export headers are mapped to generic internal metadata such as `paper_id`, `title`, `authors`, and `publication_year`; these export-header facts do not live in `pipeline/` code.
- Human readable hint: `DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES` is the optional bridge between human prompt section wording and schema domains. Keep review-topic vocabulary here or in the prompt/schema CSV, not inside pipeline Python.
- Human readable hint: `DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS` controls aggregate extraction output labels, including the AI reviewer label, while `DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES` supplies optional fallback consensus-column aliases for variables.
- Human readable hint: `DATA_EXTRACTION_EXPERT_REVIEW_SETTINGS`, `DATA_EXTRACTION_EXPERT_REVIEWERS`, and `DATA_EXTRACTION_EXPERT_REVIEW_SHARED_VARIABLES` configure AI-first expert oversight packets without hardcoding reviewer names or schema-variable assignments in pipeline Python.
- Human readable hint: `PROMPT_SIGNAL_SECTION_ALIASES` defines which prompt section names are treated as primary/secondary retrieval signal lists when prompts contain include/exclude sections.
- Human readable hint: `CITATION_SEARCHING_SCREENING` switches screening into the citation-search workflow, reads `CITATION_SEARCHING_STAGE_RULES`, runs delta extraction against the baseline export, skips QC sampling, and keeps outputs scoped separately.
### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### require_setting(container, key, container_name)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### require_setting(container, key, container_name, expected_type)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### require_setting(container, key, container_name, expected_type)
- Human readable hint: Fetch a required setting from a config dict and warn if missing. Args: container: Settings dictionary (e.g., LLM_SETTINGS). key: Key to look up in the settings dict. container_name: Human-readable container name for warnings. Returns: The value stored under the key. Note: missing settings stop the run so you can fix the config.

#### load_user_config()
- Human readable hint: Build and validate a UserConfig from module globals (one call per run). Note: you do not edit this function; it just packages the values above.
- Human readable hint: Current endpoint-safe default profile uses `top_k=10`, `chunk_size=20`, and `async_max_concurrency=2` so full-text/data-extraction runs do not overload the LLM proxy during QC.
- Human readable hint: Data extraction defaults to `data_extraction_response_format_mode="prompt_only"` because this GPUSstack model returned malformed JSON when sent strict `json_schema` response_format payloads.
- Human readable hint: Data extraction defaults to `data_extraction_evidence_mode="full_text"` so extraction prompts use cached normalized full text rather than only the screening-selected chunks.
- Human readable hint: Data extraction preflights every included per-paper folder before QC sampling, creating `full_text_normalized.txt`, `full_text_artifact.json`, `data_extraction_artifact.json`, and `data_extraction_selected_chunks.jsonl` when needed.
- Human readable hint: `DATA_EXTRACTION_SUPPLEMENTAL_CITED_EVIDENCE` controls optional per-paper cited-source evidence folders; supplied `.txt`, `.md`, or `.pdf` protocol/development/source evidence is appended to data-extraction prompts with source labels and also appears in input traces.
- Human readable hint: With `data_extraction_evidence_mode="full_text"`, `DATA_EXTRACTION_SCHEMA_FILE` is the strongest extraction contract; `knowledge-base/data_extraction_pos-neg_examples.csv` still helps retrieval/ranking and becomes especially important when extraction uses `selected_chunks`.
- Human readable hint: `data_extraction_hybrid_rescue_enabled` keeps full-text extraction as the primary path and optionally adds a schema-anchor semantic second opinion for user-configured variables/domains, writing separate hybrid audit files instead of silently replacing the main extraction.
- Human readable hint: Data-extraction schema instructions include generic demographic safeguards for denominator consistency, full-cohort-over-subgroup preference for overall fields, same-table population sweeps, per-arm protocol sample-size arithmetic, repeated-row age/gender table counting, and separate race/ethnicity preservation.
- Human readable hint: The `context.evidence_source` schema variable now means publication/source legitimacy type, such as peer-reviewed journal article, conference proceeding, preprint, trial registration, thesis/dissertation, report, or other grey literature, not an article-internal section name.
- Human readable hint: Data extraction defaults to `data_extraction_split_by_domain=True` with optional `data_extraction_domain_groups`; population and context are kept as focused batches because they depend on table, recruitment, and location evidence.
- Human readable hint: Total model context budget is configured in `LLM_SETTINGS["context_window_total_tokens"]` and combined with `max_tokens` to derive prompt budget at runtime.

## main.py

### Class MainWorkflow
- Human readable hint: one-class orchestrator for terminal flow, retries, QC gating, and stage execution.
- __init__ parameters: none

#### MainWorkflow.__init__()
- Human readable hint: stores the active stage, input folder, QC sample rate, optional exact input files, and optional run scope for separated citation-search runs.

#### MainWorkflow.run()
- Human readable hint: readable stage decision tree; helper details now live in focused `pipeline/additions` modules.

### Script-level functions
- Human readable hint: main.py now keeps interactive workflow control only; bookkeeping helpers are imported from focused modules.

#### _last_artifact_dict()
- Human readable hint: return the last pipeline artifact only when it is a dictionary.

#### _run_pipeline_guarded()
- Human readable hint: run one pipeline pass, store the returned artifact, and mark failures for the backup prompt.

#### _execute_retry_run(stage, run_label, retry_csv, attempt_map)
- Human readable hint: run one retry attempt while delegating retry file naming and manifests to `pipeline/additions/retry_flow.py`.

#### _prompt_retry_if_needed(stage, artifact, depth)
- Human readable hint: ask whether incomplete/error cases should be retried, then create a focused retry CSV.

#### _prompt_yes_no(message)
- Human readable hint: ask one explicit yes/no terminal question and update the run's all-yes audit flag.

#### _run_validation()
- Human readable hint: confirm reviewer minutes, backfill time-savings, then run `pipeline.additions.stats_engine`.

#### _run_qc_loop(stage, sample_rate, quiet)
- Human readable hint: run QC screening, validation, and the user decision gate before remaining-paper processing.

#### main()
- Human readable hint: compatibility entrypoint that parses optional `--stage`, `--input-file`, and `--run-scope` arguments before running `MainWorkflow`.

## pipeline/additions/retry_flow.py

### Script-level functions
- Human readable hint: retry CSV creation, retry artifact naming, retry manifests, and retry-completeness checks.

#### _write_retry_csv(source_csv, target_dir, paper_ids, stage, run_label)
- Human readable hint: create a focused retry CSV containing only papers that still need decisions.

#### _retry_output_paths(stage, run_label, attempt_index)
- Human readable hint: create collision-safe retry output paths separated from base run outputs.

#### _record_retry_manifest(retry_artifact, stage, attempt_map, source_csv, emissions_info)
- Human readable hint: append one manifest row listing retry files and paper IDs.

#### _retry_csv_needed(retry_csv, stage)
- Human readable hint: identify retry rows that still lack complete eligibility decisions.

#### _latest_retry_csv(stage)
- Human readable hint: find the newest pending retry CSV for the active stage.

## pipeline/additions/run_index.py

### Script-level functions
- Human readable hint: output discovery, eligibility index maintenance, emissions/resource summaries, and stale tracking cleanup.

#### _latest_base_outputs(stage, run_label)
- Human readable hint: find the newest base output files for a stage and run label.

#### _artifact_from_latest_base_outputs(stage, run_label)
- Human readable hint: synthesize a minimal artifact when existing outputs already exist on disk.
- Human readable hint: data_extraction QC reuse is based on completed per-paper `data_extraction_results.jsonl` files, because this stage does not write eligibility JSONL outputs.

#### _normalize_qc_paper_id(value)
- Human readable hint: normalize paper identifiers from QC sample CSVs and result files so `#22`, `22`, and `22.0` compare as the same paper.

#### _read_qc_sample_ids(stage)
- Human readable hint: read the latest stage QC sample CSV through configured metadata aliases instead of hardcoded export headers.

#### _data_extraction_result_ids(stage)
- Human readable hint: collect completed data_extraction paper IDs from per-paper result JSONL files.

#### _data_extraction_qc_screened_already(stage)
- Human readable hint: decide whether a data_extraction QC sample can be reused by checking that every QC paper already has a result file.

#### _update_index_from_artifact(stage, artifact, attempt_index)
- Human readable hint: refresh the eligibility index rows for all decision splits from a run artifact.

#### _post_run_updates(stage, artifact, attempt_index)
- Human readable hint: merge CodeCarbon rows, update the eligibility index, and refresh QC+remaining summaries.

#### _cleanup_stale_remaining_tracking_files(stage)
- Human readable hint: remove duplicate minute-stamped tracking sidecars after a cleaner run-level file exists.

## pipeline/additions/startup_checks.py

### Script-level functions
- Human readable hint: startup checks that keep `main.py` focused on workflow decisions.

#### active_prompt_and_kb(stage)
- Human readable hint: resolve prompt and KB paths shown to the operator before running.

#### ensure_csv_inputs(csv_dir)
- Human readable hint: confirm that the input folder exists and contains CSV exports.

#### require_pattern(csv_dir, pattern, description, stage)
- Human readable hint: pick the newest required stage CSV and warn about ambiguous naming.

#### missing_pdf_folders(base_dir)
- Human readable hint: list per-paper folders without uploaded PDFs.

#### ensure_nltk_tokenizers()
- Human readable hint: verify sentence tokenizer assets are preloaded without runtime downloads.

## pipeline/additions/input_trace.py

### Class InputTraceRunner
- Human readable hint: one-class trace utility that reconstructs one paper input and verifies its hashes.
- __init__ parameters: stage
#### InputTraceRunner.__init__(stage)
- Human readable hint: __init__ stores the default stage used when CLI arguments omit --stage.

#### InputTraceRunner.run(args)
- Human readable hint: execute the full trace workflow from eligibility record lookup to report writing.

#### InputTraceRunner.run_all_data_extraction_output(args)
- Human readable hint: create one input trace per paper from a data-extraction output folder, including versioned folders passed with `--stage-output-dir`, prefer the newest most-complete retry output per paper, link each result back to its per-paper normalized full text, list present/missing schema fields, and add schema-derived nearby evidence hits for missing fields.
- Human readable hint: data-extraction input traces mirror runtime prompt input and therefore include configured supplemental cited evidence blocks when the per-paper folder contains them.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _sha256_text(value)
- Human readable hint: compute a stable fingerprint of any text.

#### _latest_eligibility_file(stage)
- Human readable hint: pick the latest eligibility file (excluding split files).

#### _find_record(eligibility_file, paper_id, input_hash)
- Human readable hint: find one paper in eligibility output by paper_id (with or without leading '#') or stored input hash.

#### _strip_author_mentions(text, authors)
- Human readable hint: mirror screening redaction logic for exact reproducibility.

#### _format_chunks_for_prompt(stage, paper_id, title, authors, chunks)
- Human readable hint: rebuild the same context text format sent to the model.

#### _title_abstract_context(stage, paper_id)
- Human readable hint: title_abstract stores the full model context in selected_chunks output.

#### _load_folder_metadata(folder)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### _extract_paper_id(row)
- Human readable hint: read the paper ID through user-configured metadata aliases.

#### _find_paper_folder(stage, paper_id, csv_root)
- Human readable hint: locate the per-paper folder by matching configured paper IDs in metadata.

#### _load_selected_chunks(folder, stage, paper_id)
- Human readable hint: load selected chunks from stage JSONL and fall back to compact per-paper artifact files.

#### _folder_stage_context(stage, paper_id, csv_root)
- Human readable hint: rebuild full_text/data_extraction model context from metadata + selected chunks.

#### _reconstruct_context(stage, paper_id, csv_root)
- Human readable hint: stage-aware reconstruction of exact model context with selected chunks.

#### _load_prompt_template(stage)
- Human readable hint: mirror runtime prompt assembly with optional eligibility criteria injection.

#### _load_jsonl_payload(path)
- Human readable hint: read the first non-meta JSONL payload from a stage output file.

#### _trace_search_terms_for_variable(variable)
- Human readable hint: derive audit search terms from schema variable names, export columns, and instructions so input traces stay generic.

#### _line_hits_for_terms(text, terms)
- Human readable hint: find compact normalized-text snippets that may explain why a missing extraction field deserves human review.

#### _parse_args()
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### run_trace()
- Human readable hint: Compatibility wrapper for direct module execution.

## pipeline/additions/extraction_plausibility_audit.py

### Class ExtractionPlausibilityAuditRunner
- Human readable hint: orchestrate loading the candidate and baseline data-extraction exports, comparing schema variables, attaching input-trace evidence, running generic plausibility checks, and writing reviewer-audit artifacts.

#### ExtractionPlausibilityAuditRunner.run()
- Human readable hint: produce `data_extraction_version_diff.csv`, `data_extraction_plausibility_flags.csv`, and `data_extraction_plausibility_summary.md` under the candidate output folder.

### Class ConsensusTable
- Human readable hint: represent one wide reviewer-facing consensus CSV and resolve paper ID/title columns through user-editable config.

### Class SchemaColumnResolver
- Human readable hint: map each schema CSV variable to the reviewer-facing export column using `covidence_column_name` plus user-editable aliases.

### Class QuoteAuditTable
- Human readable hint: index the long quote-audit CSV so every schema variable can be checked for value/quote support.

### Class InputTraceIndex
- Human readable hint: parse generated input traces and summarize schema-derived evidence hits for fields the LLM returned as missing.

### Class VersionComparator
- Human readable hint: compare candidate versus baseline outputs and classify changed cells as present-to-missing, missing-to-present, or changed-present-value.

### Class PlausibilityAuditor
- Human readable hint: run generic reviewer checks for version regressions/recoveries, schema contract mismatches, missing quote support, missing values with trace evidence, and population denominator/count inconsistencies.
- Human readable hint: flags `evidence_source` values that look like article-internal section names after the schema contract defines that field as publication/source type.

### Class AuditReportWriter
- Human readable hint: write spreadsheet-friendly diff and flag reports plus a compact Markdown summary for human QC triage.

### Script-level functions
- Human readable hint: command-line helpers for running `python -m pipeline.additions.extraction_plausibility_audit` with configurable candidate, baseline, schema, and report paths.

## pipeline/additions/resource_usage.py

### Class ResourceUsageConfig
- Human readable hint: Configuration for resource usage tracking. Args: resource_log_path: Path to JSONL resource log. enable_tracking: If True, write resource logs and totals. enable_codecarbon: If True, track emissions via CodeCarbon (if installed). stage: Current pipeline stage (title_abstract | full_text | data_extraction). qc_sample_path: Optional QC sample CSV path to derive actual QC counts. qc_paper_count: Optional precomputed QC size to avoid re-reading the QC CSV. run_label: Run label suffix (qc_sample or remaining_sample) for file naming. enable_time_savings: If True, compute human-time savings (only when validation ran).
### Class CarbonTrackerManager
- Human readable hint: Initialize and manage CodeCarbon trackers with offline/online support.
- __init__ parameters: enabled
#### CarbonTrackerManager.__init__(enabled)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### CarbonTrackerManager._init_tracker()
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### CarbonTrackerManager.start()
- Human readable hint: Start the tracker (no-op if unavailable).

#### CarbonTrackerManager.stop()
- Human readable hint: Stop the tracker and return emissions (kg CO2eq), if available.

#### CarbonTrackerManager.rename_emissions_csv(timestamp_label, run_label)
- Human readable hint: Rename CodeCarbon's emissions.csv to stage/sample naming: <stage>_<sample>_codecarbon_emissions_<timestamp>.

#### CarbonTrackerManager.energy_kwh()
- Human readable hint: Return final energy consumed in kWh, if available.

#### CarbonTrackerManager.__enter__()
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### CarbonTrackerManager.__exit__(exc_type, exc, tb)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### CarbonTrackerManager.measure_energy(func)
- Human readable hint: Decorator for function-level emissions tracking.

### Class ResourceUsageTracker
- Human readable hint: Track per-paper and per-run resource usage, with optional CodeCarbon.
- __init__ parameters: config
#### ResourceUsageTracker.__init__(config)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### ResourceUsageTracker.start_run()
- Human readable hint: Start CodeCarbon tracking (if enabled and available).

#### ResourceUsageTracker.set_qc_count(qc_count)
- Human readable hint: Allow callers to set QC paper count without re-reading the QC CSV.

#### ResourceUsageTracker.stop_run(total_runtime_seconds, paper_count)
- Human readable hint: Stop CodeCarbon tracking and append per-run totals.

#### ResourceUsageTracker.log_paper(paper_id, prompt_tokens, response_tokens, pdf_text_tokens, pdf_visual_tokens, embedding_tokens, prompt_tokens_source, response_tokens_source, embedding_tokens_source, paper_seconds)
- Human readable hint: Append per-paper resource usage to the JSONL log (prefers API token counts when available).

#### ResourceUsageTracker._write_totals(total_runtime_seconds, paper_count, emissions_kg, energy_kwh)
- Human readable hint: Append buffered per-paper entries plus per-run totals in one write.

#### ResourceUsageTracker._resolve_qc_papers(stage_cfg)
- Human readable hint: Determine QC paper count from the QC sample file; falls back to zero if unavailable.

### Class ResourceUsageEngine
- Human readable hint: dominant class for this script; it exposes one stable API for run/resource tracking.
- __init__ parameters: resource_log_path, enable_tracking, enable_codecarbon, stage, qc_sample_path, qc_paper_count, run_label, enable_time_savings
#### ResourceUsageEngine.__init__(resource_log_path, enable_tracking, enable_codecarbon, stage, qc_sample_path, qc_paper_count, run_label, enable_time_savings)
- Human readable hint: __init__ captures all run-level tracking parameters in one visible constructor.

#### ResourceUsageEngine.start_run()
- Human readable hint: start CodeCarbon/resource tracking for the current run.

#### ResourceUsageEngine.set_qc_count(qc_count)
- Human readable hint: set QC paper count once so the tracker does not re-read QC CSV files.

#### ResourceUsageEngine.log_paper(paper_id, prompt_tokens, response_tokens, pdf_text_tokens, pdf_visual_tokens, embedding_tokens, prompt_tokens_source, response_tokens_source, embedding_tokens_source, paper_seconds)
- Human readable hint: log per-paper token/runtime metrics in the shared run tracker.

#### ResourceUsageEngine.stop_run(total_runtime_seconds, paper_count)
- Human readable hint: stop tracking and write final TOTAL summary lines.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _estimate_ubelix_operational(total_runtime_seconds)
- Human readable hint: Estimate UBELIX operational energy/CO2e with Green-Algorithms style factors.

#### _build_ubelix_assumption_log()
- Human readable hint: Build a compact assumption log for reproducibility/audit reporting.

#### _ubelix_assumption_missing_fields(assumption_log)
- Human readable hint: Return required assumption source fields that are still blank.

#### _print_ubelix_summary_line(total_runtime_seconds, stage, run_label)
- Human readable hint: Print one operator-friendly summary line after a run.

#### _count_qc_papers(qc_sample_path)
- Human readable hint: Count QC sample rows (header excluded).

#### backfill_time_savings(resource_log_path, stage, qc_sample_path)
- Human readable hint: Recompute human-time fields in an existing resource_usage log after minutes are confirmed. Returns True if the log was updated.

## pipeline/additions/stats_engine.py

### Class ValidationEngine
- Human readable hint: one-class validation orchestrator for screening and extraction stages.
- Human readable hint: data-extraction validation reads reviewer binary scoring directly from `input/data_extraction_human_review_qc_sample_binary_scoring.csv` when no explicit consensus file is supplied, keeping generated gold-standard side files out of the normal workflow.
- Human readable hint: validation text normalization standardizes Unicode punctuation such as nonbreaking hyphens, dash variants, smart quotes, and nonbreaking spaces before exact/alias/fuzzy comparison, so typographic variants such as `GPT‑4` and `GPT-4` do not create false mismatches.
- Human readable hint: data-extraction reports include `n_papers`, per-variable paper counts, strict value-only lower-bound metrics, and quote-aware metrics that can compare AI values and AI quotes against reviewer-derived values and reviewer notes/corrections.
- Human readable hint: quote-aware validation is controlled by `DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS`; language/equivalence bridges such as German reviewer notes versus English AI wording belong in user-editable `DATA_EXTRACTION_VALIDATION_VALUE_ALIASES`, not in `pipeline/` code.
- __init__ parameters: stage
#### ValidationEngine.__init__(stage)
- Human readable hint: __init__ stores the active stage used to route validation.

#### ValidationEngine.run(args)
- Human readable hint: run the correct validation branch based on the configured stage.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _stage_file(name, suffix)
- Human readable hint: Build a stage-prefixed output path under output/<stage>/. Validation outputs must include `qc_sample` in the filename to match the QC-only comparison scope.

#### _find_latest_match(patterns, search_dirs)
- Human readable hint: Return the most recently modified file matching any pattern.

#### _require_path(value, label)
- Human readable hint: Require an explicit CSV path to avoid ambiguous auto-search.

#### _auto_or_require(value, label, patterns)
- Human readable hint: Use explicit path if provided, otherwise auto-detect from input/.

#### _clean_cols(df)
- Human readable hint: Trim whitespace from CSV column names.

#### _normalize_id_column(df)
- Human readable hint: Find the best ID column and return it as strings.

#### _normalize_tag_text(value)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### _extract_tags(value)
- Human readable hint: map explicit human-export tags to the curated include list; ignores notes.

#### _extract_ft_reason(notes_val)
- Human readable hint: Extract the full-text exclusion reason from Notes/Tags.

#### _parse_human_decision(val)
- Human readable hint: Normalize human include/exclude values into 1 (include) or 0 (exclude).

#### _load_qc_human_file(path)
- Human readable hint: Load a human QC-only file with decisions for the QC sample.

#### _load_human(stage, args)
- Human readable hint: Load human labels depending on stage.

#### _normalize_text_value(val)
- Human readable hint: Normalize values to compare AI vs human extraction consistently.

#### _parse_ai_decision(val)
- Human readable hint: Parse the AI decision JSON into a binary include/exclude label.

#### _load_ai()
- Human readable hint: Aggregate AI QC decisions across main and retry runs, keeping latest per paper.

#### _merge(ai, human)
- Human readable hint: Merge AI and human labels on the paper ID.

#### _confusion(df)
- Human readable hint: Compute confusion-matrix counts.

#### _prop_ci(k, n, alpha)
- Human readable hint: exact (Clopper-Pearson) CI via statsmodels (Seabold & Perktold, 2010).

#### _metrics(tp, tn, fp, fn)
- Human readable hint: Compute agreement metrics for screening.

#### _write_alignment(df, suffix)
- Human readable hint: single QC alignment file with decisions and reasons.

#### _write_report(stats, tp, tn, fp, fn, stage, suffix)
- Human readable hint: Write a readable validation summary report.

#### _plot_confusion(tp, tn, fp, fn, suffix)
- Human readable hint: Draw and save a confusion-matrix plot.

#### _extract_timestamp_suffix(path)
- Human readable hint: Extract the QC timestamp anchor (YYYYMMDD_HH-MM) from stage output filenames, including second/microsecond variants.

#### _load_qc_sample_ids(suffix)
- Human readable hint: Load QC sample IDs for the matching timestamp suffix.

#### validate_screening(stage, args)
- Human readable hint: Validate screening decisions against human labels.

#### _load_ai_extraction_records()
- Human readable hint: Load extraction outputs from per-paper JSONL files.

#### _value_from_extracted_data(extracted, variable)
- Human readable hint: read the KB-generated `{variable_name}_value` field from the LLM JSON.

#### _quote_from_extracted_data(extracted, variable)
- Human readable hint: read the KB-generated `{variable_name}_quote` field for audit review.

#### _normalization_key(value, variable)
- Human readable hint: coerce AI and human-export values into comparable typed values using the KB variable type.

#### _human_score_column_name(consensus_column_name)
- Human readable hint: companion columns can carry human 0/1 judgements without changing schema columns.

#### _parse_human_cell_score(value)
- Human readable hint: accept common binary reviewer score encodings for data-extraction cells.

#### _validation_alias_match(human_value, ai_value, variable)
- Human readable hint: apply user-editable semantic equivalence groups from `DATA_EXTRACTION_VALIDATION_VALUE_ALIASES` after plain normalization.

#### _validation_bool_setting(key, default)
- Human readable hint: keep optional fuzzy validation behavior explicit and user-editable; fuzzy similarity is disabled for primary metric counting by default.

#### _extraction_values_match(human_value, ai_value, variable)
- Human readable hint: compare AI output against reviewer-derived ground truth, including configured validation aliases.

#### _factual_text_match(human_value, ai_value, variable)
- Human readable hint: optional exploratory helper for reviewer-derived prose fields; it is not used for primary accuracy/concordance unless `count_fuzzy_matches_in_metrics=True`.

#### _human_score_columns_present(human_wide, variables)
- Human readable hint: detect generated reviewer 0/1 score columns for explicit legacy validation mode; default validation uses reviewer-derived ground truth values instead.

#### _latest_binary_review_source()
- Human readable hint: prefer `input/data_extraction_human_review_qc_sample_binary_scoring.csv` as the editable source of truth for data-extraction validation.

#### _build_gold_standard_frame_from_binary_source(source_path, schema_path)
- Human readable hint: convert the editable binary reviewer sheet into an in-memory validation table without writing derived gold-standard files.

#### _load_extraction_human_wide(consensus_path, schema_path)
- Human readable hint: default validation reads the editable input scoring CSV directly; explicit consensus files are only legacy/special-case inputs.

#### validate_extraction(consensus_path, ai_output_dir)
- Human readable hint: validate extraction outputs against a human gold-standard CSV by mapping each KB `variable_name` to its exact `covidence_column_name`.
- Optional `ai_output_dir` validates archived data-extraction runs without moving files into the active output folder.
- If the editable binary scoring CSV is present, it is converted into reviewer-derived ground truth first: accepted AI values for score `1`, quote-row corrections for score `0`, and non-evaluable cells skipped.
- The optional `--use-human-score-columns` mode remains available for legacy binary-score validation, but default validation compares current AI output against human ground-truth values.
- Concordance is exact matches among human-present values divided by human-present values, excluding `Not Available` and `n/a`.
- Accuracy is exact matches plus correctly identified missing values divided by all variable-paper comparisons parsed from the KB.
- Variables below concordance `<0.80` or accuracy `<0.90` log a critical `Prompt Refinement Triggered` warning.
- Mismatches are written to `output/data_extraction/extraction_error_audit.csv` with the LLM quote retained for manual review.

#### _parse_args()
- Human readable hint: Parse CLI arguments for validation.

#### run_validation()
- Human readable hint: Compatibility wrapper for direct execution.

## pipeline/additions/human_gold_standard_builder.py

### Class SchemaVariable
- Human readable hint: minimal schema row needed to shape a validation-ready human table.
### Class ReviewedPaperBlock
- Human readable hint: one AI row plus its following human 0/1 score and note rows.
### Class ReviewSheetReader
- Human readable hint: parse a reviewer sheet without hardcoding study-specific variables.
### Class SchemaColumnMatcher
- Human readable hint: map schema variables to reviewer-sheet columns by generic normalized labels.
### Class HumanGoldStandardBuilder
- Human readable hint: convert binary reviewer judgements into stats-engine consensus inputs.
- Human readable hint: the editable `input/data_extraction_human_review_qc_sample_binary_scoring.csv` remains the source of truth; normal validation converts it in memory and does not write derived gold-standard files.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### parse_args()
- Human readable hint: CLI keeps source files user-supplied instead of hardcoded in pipeline code.

#### HumanGoldStandardBuilder._find_reviewed_blocks(headers, rows)
- Human readable hint: detect reviewed paper blocks from cleaned paper-id cells followed by `0/1` score and `quote` rows, while still supporting older `Reviewer: paper_id` labels.

#### HumanGoldStandardBuilder._parse_paper_id_cell(paper_id_cell)
- Human readable hint: parse both cleaned `22` and older `Marc: 22` paper-id cells into a paper ID and optional reviewer hint.

#### main()
- Human readable hint: command-line entrypoint for repeatable human-gold generation.

## pipeline/additions/generate_cleaned_hybrid_kb_draft.py

### Class ExampleRow
- Human readable hint: typed container for one KB row (`label`, `source`, `text`).
### Class ChunkCandidate
- Human readable hint: typed container for one cleaned chunk candidate with score and source metadata.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _parse_args()
- Human readable hint: parse CLI options for short/chunk KB inputs, draft outputs, and cleaning thresholds.

#### _read_kb_rows(path)
- Human readable hint: load KB rows from CSV (`utf-8-sig`), keeping only non-empty POS/NEG examples.

#### _quality_metrics(text)
- Human readable hint: compute chunk quality signals (token ratios, domain signals, low-value/legal-text flags).

#### _best_window(text, max_words)
- Human readable hint: select the highest-quality text window from long chunk rows while preserving punctuation.

#### _build_chunk_candidates(rows, max_words, min_words)
- Human readable hint: clean, filter, deduplicate, and score chunk rows into reusable candidate snippets.

#### _select_candidates(candidates, needed, max_chunks_per_source)
- Human readable hint: choose top-scoring candidates with per-source diversity limits.

#### _to_hybrid_rows(candidates)
- Human readable hint: convert selected chunk candidates into final POS/NEG KB rows with explicit reasoning preface.

#### _write_csv(path, rows)
- Human readable hint: write the cleaned-hybrid draft KB CSV without touching source KB files.

#### _write_report(path, payload)
- Human readable hint: write a JSON report with candidate stats, skip reasons, and final class-balance counts.

#### main()
- Human readable hint: end-to-end non-destructive draft generation entrypoint for full-text cleaned-hybrid KB creation.

## pipeline/additions/bootstrap_stage_kb_and_prompts.py

### Class PaperRecord
- Human readable hint: one POS/NEG example PDF after parsing, chunking, and metadata extraction.

### Class BootstrapSignals
- Human readable hint: data-derived include, exclude, and extraction cue terms learned from the local POS/NEG example PDFs.

### Script-level functions
- Human readable hint: build stage KB CSVs and suggested prompts without fixed review-topic term lists.

#### _build_bootstrap_signals(papers)
- Human readable hint: calculate discriminative include/exclude terms from POS and NEG examples for prompt suggestions and chunk ranking.

#### _score_fulltext_chunk(chunk, paper, signals)
- Human readable hint: score candidate full-text chunks using local data-derived signals plus generic section/readability checks.

#### _score_data_extraction_chunk(chunk, paper, signals)
- Human readable hint: score candidate extraction chunks using local data-derived extraction terms.

#### _build_prompt_suggestions(signals)
- Human readable hint: render suggested prompt files from the learned bootstrap signals.

## pipeline/core/pipeline.py

### Class PaperRecord
- Human readable hint: No class docstring in source; placeholder retained intentionally for exhaustive traceability.
### Class PaperScreeningPipeline
- Human readable hint: No class docstring in source; placeholder retained intentionally for exhaustive traceability.
- __init__ parameters: csv_dir, knowledge_base_path, eligibility_output_path, chunks_output_path, text_output_path, top_k, score_threshold, batch_size, embedder, examples, sample_size, sample_seed, sustainability_tracking, resource_log_path, enable_time_savings, run_label, codecarbon_enabled, qc_sample_path, qc_sample_readable_path, confirm_sampling, sample_rate, qc_only, qc_enabled, force_new_qc, error_log_path, stage, pdf_root, overflow_log_path, split_only, quiet, summary_to_console, artifact_mode
#### PaperScreeningPipeline.__init__(csv_dir, knowledge_base_path, eligibility_output_path, chunks_output_path, text_output_path, top_k, score_threshold, batch_size, embedder, examples, sample_size, sample_seed, sustainability_tracking, resource_log_path, enable_time_savings, run_label, codecarbon_enabled, qc_sample_path, qc_sample_readable_path, confirm_sampling, sample_rate, qc_only, qc_enabled, force_new_qc, error_log_path, stage, pdf_root, overflow_log_path, split_only, quiet, summary_to_console, artifact_mode)
- Human readable hint: Initialize the screening/extraction pipeline with configuration. All arguments are strictly typed and have clear defaults for robust, reproducible runs. Non-coders: Each parameter controls a key aspect of the workflow (see README for details).

#### PaperScreeningPipeline._sha256_text(value)
- Human readable hint: stable fingerprint to verify whether two input texts are exactly identical.

#### PaperScreeningPipeline._persist_prompt_template_snapshot()
- Human readable hint: persist one prompt snapshot per campaign hash and reuse identical existing snapshots to avoid duplicate files; grouped data extraction snapshots stay close to the user prompt while exact schema-injected batch prompts live in input traces.

#### PaperScreeningPipeline.run()
- Human readable hint: Main pipeline: prep folders (if needed), QC sample, then screen papers. Split-only prep skips prompt snapshot persistence.

#### PaperScreeningPipeline._iter_papers()
- Human readable hint: Yield papers sequentially; sample only if requested.

#### PaperScreeningPipeline._collect_planned_papers()
- Human readable hint: Materialize the papers to be screened so sampling and progress are deterministic.

#### PaperScreeningPipeline._ensure_qc_sample(planned_papers, force_new)
- Human readable hint: Create (or load) a QC sample and record its paper_ids.

#### PaperScreeningPipeline._prompt_sampling_confirmation(created_sample)
- Human readable hint: Ask the user on the CLI whether to proceed after QC sample is ready.

#### PaperScreeningPipeline._normalize_row(row, default_id)
- Human readable hint: Normalize a raw CSV row into standard fields.

#### PaperScreeningPipeline._canonicalize_row(row)
- Human readable hint: Map normalized fields into the canonical metadata schema.

#### PaperScreeningPipeline._iter_file_rows(csv_file)
- Human readable hint: Yield PaperRecord items from a CSV file.

#### PaperScreeningPipeline._collect_csv_rows(select_only)
- Human readable hint: Collect raw CSV rows into a list (used for folder creation).

#### PaperScreeningPipeline._process_title_abstract_batch(planned_papers)
- Human readable hint: stream title_abstract completions paper-by-paper as async calls finish.

#### PaperScreeningPipeline._use_async_stage_processing()
- Human readable hint: allow stage-specific opt-in async processing beyond title_abstract.

#### PaperScreeningPipeline._process_non_title_async_batch(planned_papers)
- Human readable hint: stream full_text/data_extraction completions paper-by-paper.

#### PaperScreeningPipeline._stream_async_batch(planned_papers, processor)
- Human readable hint: bridge async processing to sync caller while emitting per-paper completion updates.

#### PaperScreeningPipeline._process_paper_async(paper)
- Human readable hint: run stage-specific chunking, selection, LLM calls, validation, and diagnostics in one async flow.
- Human readable hint: full_text keeps the final valid adjudication output and records borderline state in diagnostics instead of forcing a final hard validation failure.
- Human readable hint: data_extraction normally calls the LLM once per configured schema-domain batch, validates each smaller response, merges domains, and logs only batches that fail.

#### PaperScreeningPipeline._process_paper(paper)
- Human readable hint: sync mode reuses the async processing core to avoid duplicate decision logic.

#### PaperScreeningPipeline._format_chunks_for_prompt(paper, chunks, detected_language_code)
- Human readable hint: Format selected chunks into a readable prompt section.

#### PaperScreeningPipeline._load_data_extraction_full_text_input(paper)
- Human readable hint: load cached normalized full text for extraction prompts, preserve table structure with LLM-focused cleanup, apply the optional word cap from `LLM_SETTINGS`, prepend schema-guided evidence hints when enabled, and append configured supplemental cited evidence when present.

#### PaperScreeningPipeline._with_domain_scoped_schema_evidence_hints(context, domain_schema)
- Human readable hint: when data extraction is split by schema domain, replace the broad all-variable evidence map with a smaller evidence map for the active domain group before each LLM call.

#### PaperScreeningPipeline._preflight_data_extraction_full_text_inputs(papers)
- Human readable hint: before QC sampling or remaining-paper filtering, ensure every data-extraction folder has usable normalized full text and data-extraction chunk-audit sidecars.
- Human readable hint: preflight writes one latest JSON report so repeated QC runs do not clutter the output folder.

#### PaperScreeningPipeline._ensure_full_text_normalized_for_data_extraction(paper)
- Human readable hint: run the full-text PDF parsing/cache path inside a data-extraction run when `full_text_normalized.txt` is missing or empty.

#### PaperScreeningPipeline._ensure_data_extraction_selected_chunks(paper)
- Human readable hint: create `data_extraction_selected_chunks.jsonl` and update `data_extraction_artifact.json` from generic chunk selection before LLM extraction.

#### PaperScreeningPipeline._title_abstract_full_input(paper)
- Human readable hint: Build one full context block for title_abstract (no chunking/retrieval).

#### PaperScreeningPipeline._metadata_without_authors(metadata)
- Human readable hint: Remove author fields from screening outputs.

#### PaperScreeningPipeline._authors_for_paper(paper)
- Human readable hint: Get author string for redaction matching.

#### PaperScreeningPipeline._strip_author_mentions(text, authors)
- Human readable hint: Redact exact author names/blocks from text to avoid author-based screening.

#### PaperScreeningPipeline._sanitize_screening_decision(decision, paper)
- Human readable hint: Remove author mentions from LLM output for screening stages.

#### PaperScreeningPipeline._write_plain_text_summary(writer, record)
- Human readable hint: Write a simple per-paper summary for manual review text files.

#### PaperScreeningPipeline._estimate_text_tokens(text)
- Human readable hint: Estimate token count using a simple whitespace split heuristic.

#### PaperScreeningPipeline._select_chunks_with_rescue(chunks, supplemental_rows)
- Human readable hint: select evidence with adaptive fallback and enforce non-title/method quotas for full_text.
- Human readable hint: applies hybrid chunk ranking (embedding + method evidence + prompt-derived topic cues + readability + sentence completeness).
- Human readable hint: applies a final raw-chunk non-title rescue when selected evidence collapses to title-only context.

#### PaperScreeningPipeline._hybrid_chunk_score(row)
- Human readable hint: combine semantic relevance with readability and chunk-completeness signals for robust ranking.

#### PaperScreeningPipeline._count_pdf_pages(pdf_path)
- Human readable hint: Return number of pages in a PDF; fall back to 0 on failure.

#### PaperScreeningPipeline._prepare_chunks(paper)
- Human readable hint: Create evidence chunks, token counts, and resolved language for one paper.
- Human readable hint: full_text now performs language-code detection and returns an unsupported-language marker for non-EN/DE policy handling.

#### PaperScreeningPipeline._compact_artifacts_enabled()
- Human readable hint: enable compact per-paper artifact mode only for full_text runs.

#### PaperScreeningPipeline._compact_artifact_path_for_folder(folder_path, stage)
- Human readable hint: build stage artifact filename paths for per-paper compact machine outputs.

#### PaperScreeningPipeline._metadata_snapshot_for_folder(folder_path, fallback)
- Human readable hint: load canonical metadata from per-stage artifact files for synchronized sidecar exports.

#### PaperScreeningPipeline._write_compact_human_normalized_text(folder_path, metadata_snapshot, normalized_text)
- Human readable hint: write human-checkable normalized text with metadata copied from stage artifact metadata; data extraction writes the full-text evidence sidecar as `full_text_normalized.txt`.

#### PaperScreeningPipeline._persist_compact_text_artifacts(paper, pdf_path, cache_key, normalized_text, normalized_pages)
- Human readable hint: persist per-paper compact full-text machine artifacts and synchronized normalized text sidecar from both full-text and data-extraction PDF-backed runs.

#### PaperScreeningPipeline._materialize_paper_folders_full_text()
- Human readable hint: Split select CSV rows into per-paper folders under csv_dir/per_paper_full_text.

#### PaperScreeningPipeline._materialize_data_extraction_subset()
- Human readable hint: Create per-paper data_extraction folders from included IDs.

#### PaperScreeningPipeline._materialize_data_extraction_from_csv_inputs()
- Human readable hint: Create per-paper data_extraction folders from explicit CSV inputs (for example citation-search deltas), preserve existing evidence files, and reuse full_text artifacts when available.

#### PaperScreeningPipeline._find_missing_pdfs(base_dir)
- Human readable hint: List folders that do not contain any PDF.

#### PaperScreeningPipeline._find_included_csv()
- Human readable hint: Find the most recent included CSV used for data_extraction.

#### PaperScreeningPipeline._stage_csv_files(select_only)
- Human readable hint: Return stage-appropriate CSV files.

#### PaperScreeningPipeline._load_included_ids(csv_path)
- Human readable hint: Read included IDs from the configured included-paper CSV.

#### PaperScreeningPipeline._extract_paper_id(row)
- Human readable hint: Extract the best available paper ID from user-configured CSV headers.

#### PaperScreeningPipeline._extract_year(row)
- Human readable hint: Try to find a publication year from many possible columns.

#### PaperScreeningPipeline._match_row_value(row, key)
- Human readable hint: Find a value in a row using exact, case-insensitive, or compact keys.

#### PaperScreeningPipeline._build_paper_folder_name(row)
- Human readable hint: Create a safe per-paper folder name using ID/author/year/title.

#### PaperScreeningPipeline._load_pdf_text(paper, resolved_path)
- Human readable hint: Read PDF text once (optionally page-level), support compact/full cache modes, and return page counts with fallback-safe behavior.

#### PaperScreeningPipeline._resolve_pdf_path(paper)
- Human readable hint: Find the PDF inside the per-paper folder and normalize its filename.

#### PaperScreeningPipeline._call_llm(context)
- Human readable hint: call the LLM and return both text and usage; optional prompt/schema/max-token overrides support small domain-level extraction calls.

#### PaperScreeningPipeline._call_data_extraction_domains_async(context, paper_id)
- Human readable hint: run data extraction as one smaller Structured Outputs request per configured KB domain group, send only domain-scoped evidence hints for that call, then merge validated domain payloads.

#### PaperScreeningPipeline._maybe_run_data_extraction_hybrid_rescue_async(paper, extraction_payload, primary_context)
- Human readable hint: after primary full-text data extraction succeeds, optionally build schema-anchor semantic evidence for configured variables/domains, call the same domain-scoped validator, and write auditable primary-vs-rescue decisions without changing the primary aggregate table.

#### PaperScreeningPipeline._get_openai_client(base_url)
- Human readable hint: Create a configured OpenAI API client.

#### PaperScreeningPipeline._get_async_openai_client(base_url)
- Human readable hint: Create a configured async OpenAI API client.

#### PaperScreeningPipeline._validate_screening_decision(decision_text)
- Human readable hint: validate screening JSON and enforce prompt-demanded keys for this stage.

#### PaperScreeningPipeline._extract_required_json_fields_from_prompt(prompt_template)
- Human readable hint: detect field names declared in the prompt schema section.

#### PaperScreeningPipeline._percentiles(values)
- Human readable hint: provide quick p50/p95/max without heavy deps.

#### PaperScreeningPipeline._parse_is_eligible(decision)
- Human readable hint: stage-aware extraction of is_eligible from the LLM decision payload.

#### PaperScreeningPipeline._decision_payload(decision)
- Human readable hint: parse JSON text decisions once so downstream checks can reuse the payload.

#### PaperScreeningPipeline._parse_exclusion_reason(decision)
- Human readable hint: derive exclusion_reason_category if present in LLM output.

#### PaperScreeningPipeline._decision_missing_fields(decision)
- Human readable hint: detect missing justification or exclusion_reason_category without altering the decision.

#### PaperScreeningPipeline._log_error(paper_id, message, context, error_type, attempt, prompt_tokens, response_tokens, embedding_tokens, pdf_text_tokens, pdf_visual_tokens, total_estimated_tokens)
- Human readable hint: Append errors to the error log with detailed context for transparency.

#### PaperScreeningPipeline._log_overflow(paper_id, estimated_tokens)
- Human readable hint: Record a context-window overflow event.

#### PaperScreeningPipeline._write_data_extraction_metadata(paper, selected, decision, extraction_payload)
- Human readable hint: Write evidence.json linking extracted fields to selected chunks.

#### PaperScreeningPipeline._data_extraction_output_dir(paper)
- Human readable hint: Return the output folder for this paper in data_extraction.

#### PaperScreeningPipeline._write_selected_chunks_to_input(paper, selected)
- Human readable hint: Save selected chunks inside the input per-paper folder.

#### PaperScreeningPipeline._load_selected_chunks_from_input(paper)
- Human readable hint: Load preselected chunks from the input folder, if present.

#### PaperScreeningPipeline._write_data_extraction_outputs(paper, extraction_payload)
- Human readable hint: write one canonical per-paper `data_extraction_results.jsonl` and `data_extraction_results.csv` pair.

#### PaperScreeningPipeline._build_extraction_payload(paper, llm_decision)
- Human readable hint: validate data-extraction JSON strictly against the KB-generated schema; the older prompt-field fallback path has been removed.

## pipeline/core/extraction_hybrid_rescue.py

### Class HybridRescueConfig
- Human readable hint: reads optional hybrid rescue settings from `LLM_SETTINGS`, including rescue Top-K, threshold, target variables/domains, and full-text-preferred variables.

### Class HybridRescuePlanner
- Human readable hint: selects variables for semantic second opinion from schema variables and user-editable config; no topic terms or field aliases are embedded in pipeline code.

### Class HybridSemanticEvidenceBuilder
- Human readable hint: chunks normalized evidence, uses `semantic_anchors` from the active schema CSV plus structural NEG examples from the configured stage KB, and formats only top-ranked chunks for rescue prompts.

### Class HybridRescueSelector
- Human readable hint: keeps the primary full-text value unless semantic rescue has explicit quote support and the primary value is missing, quote-less, or clearly weaker under generic selection rules.

### Class HybridRescueRunWriter
- Human readable hint: writes `data_extraction_hybrid_rescue_audit.csv`, `data_extraction_hybrid_selected_values.csv`, and `data_extraction_hybrid_summary.md` incrementally during a run.

## pipeline/core/prompt_context.py

### Script-level functions
- Human readable hint: prompt-template loading and optional shared eligibility-criteria injection.

#### load_optional_eligibility_criteria_text()
- Human readable hint: read configured eligibility criteria text when present.

#### load_stage_prompt_template(stage)
- Human readable hint: load the active stage prompt and replace `{eligibility_criteria}` only when the prompt requests it.

## pipeline/core/metadata_aliases.py

### Script-level functions
- Human readable hint: central metadata-header adapter that lets the generic pipeline read different CSV exports without hardcoding administrative column names.

#### metadata_aliases(key)
- Human readable hint: return the configured external header names for one generic metadata key, plus conservative generic fallbacks.

#### read_metadata_value(row, key, default)
- Human readable hint: read one metadata value from a row using `CSV_METADATA_COLUMN_ALIASES`, so paper IDs, titles, authors, and years stay configurable in `user_orchestrator.py`.

#### normalize_metadata_row(row, default_id)
- Human readable hint: copy raw CSV metadata into generic internal keys while preserving original columns for auditability.

#### extract_year_from_metadata(row)
- Human readable hint: parse a publication year from configured year/date columns without assuming one export vendor.

## pipeline/core/citation_io.py

### Class CitationCsvParser
- Human readable hint: citation-search ingestion bridge that converts an upstream deduplicated citation CSV into generic screening records using `CSV_METADATA_COLUMN_ALIASES`.
- __init__ parameters: none

#### CitationCsvParser.__init__()
- Human readable hint: initialize parser state, source tracking, and missing-metadata counters.

#### CitationCsvParser.ingest_csv(filepath)
- Human readable hint: read a citation CSV, resolve metadata through user-configured aliases, standardize publication fields, and flag missing title/abstract/DOI values without failing the run.

#### CitationCsvParser.find_target_files(input_dir, stage)
- Human readable hint: locate the newest baseline database export and citation-search whole-stage export from strict stage-specific filename patterns.

#### CitationCsvParser.ingest_and_diff(current_export_path, previous_export_path, stage)
- Human readable hint: read current and baseline exports with Pandas, filter already-seen rows by configured paper ID aliases with DOI/title fallback, and standardize only novel records.

#### CitationCsvParser.export_for_screening(output_dir)
- Human readable hint: write a citation-search novel-record handoff CSV, a JSONL audit copy, and `citation_ingestion_log.txt`; default filenames match the citation-search stage patterns.

#### CitationCsvParser._standardize_row(row, row_number, source_path)
- Human readable hint: map one source row into generic citation metadata and ingestion flags.

## pipeline/core/screening_schema.py

### Class ScreeningDecisionBaseModel
- Human readable hint: shared strict fields expected from every screening LLM response.

#### ScreeningDecisionBaseModel._check_reason_for_exclusion()
- Human readable hint: exclusion decisions must carry an explicit exclusion reason.

### Class TitleAbstractScreeningDecisionModel
- Human readable hint: title/abstract screening allows a neutral uncertainty outcome.

### Class FullTextScreeningDecisionModel
- Human readable hint: full-text screening requires a strict include/exclude decision.

#### FullTextScreeningDecisionModel._check_seed_references_threshold()
- Human readable hint: seed references are only accepted for very high-confidence eligible calls.

## pipeline/selection/retrieval_config.py

### Script-level constants
- Human readable hint: retrieval tuning constants that control chunk counts, diversity, fallback, and prompt-budget trimming.

## pipeline/selection/prompt_signals.py

### Script-level functions
- Human readable hint: prompt and KB signal helpers moved out of `pipeline/core/pipeline.py` so retrieval/schema adaptation is easier to inspect.

#### build_prompt_signal_config(prompt_template)
- Human readable hint: derive topic-sensitive retrieval regexes from the active prompt include lists.

#### build_monitoring_signal_config(prompt_template, topic_signal_config, kb_examples)
- Human readable hint: derive monitoring/action deprioritization cues from prompt terms and POS/NEG KB examples.

#### normalize_schema_key(value)
- Human readable hint: normalize user tag labels and prompt JSON fields into comparable snake_case keys.

#### build_study_tag_field_keys(tags)
- Human readable hint: convert user-editable study tags into dynamic screening schema exclusion candidates.

#### looks_like_exclusion_field(field_name)
- Human readable hint: detect exclusion-style JSON fields without hardcoding one review protocol.

#### select_topic_absence_reason_key(reason_keys, topic_terms, preferred_key)
- Human readable hint: connect topic terms to the most likely `no_*` exclusion reason key.

## pipeline/core/run_extraction.py

### Class ExtractionEvidenceBundle
- Human readable hint: carries one direct-run extraction evidence payload, including whether schema evidence hints still apply and whether the source is full text or semantic retrieval.

### Class SchemaSemanticAnchorFactory
- Human readable hint: builds POS embedding targets only from `semantic_anchors` in the active `DATA_EXTRACTION_SCHEMA_FILE`; no review-topic anchors live in pipeline Python.

### Class StructuralNegativeExampleFactory
- Human readable hint: loads optional NEG structural-noise examples from the active stage KB so embedding retrieval can down-rank boilerplate without using prompt-rule negatives.

### Class SemanticExtractionEvidenceAssembler
- Human readable hint: chunks normalized and supplemental evidence, scores chunks with `EmbeddingBackend` and `RelevanceSelector`, and formats only top-ranked chunks for the direct data-extraction prompt.

### Script-level functions
- Human readable hint: async data-extraction entrypoint that keeps direct execution (`python -m pipeline.core.run_extraction`) while delegating schema, semantic chunk selection, and file handling to smaller modules.

#### _truncate_to_budget(text, max_tokens)
- Human readable hint: trim evidence text using a lightweight token estimate before sending it to the model.

#### _build_llm_input(paper, prompt_template, max_prompt_tokens)
- Human readable hint: insert paper evidence into `{data}` or append it as an Evidence block.

#### _call_llm(client, model, prompt, response_format, max_tokens, temperature, top_p)
- Human readable hint: send one extraction prompt with the KB-generated OpenAI `response_format` and return the raw JSON text.

#### _process_paper(...)
- Human readable hint: process one paper with bounded concurrency, semantic evidence assembly, CSV-schema validation, fallback output, and append-only errors; grouped mode reuses the same constrained chunk evidence across configured domain batches.

#### run_extraction()
- Human readable hint: direct async runner for prepared `input/per_paper_data_extraction/` folders; when `data_extraction_semantic_rag_enabled=True`, it embeds chunks against schema-owned semantic anchors before calling the LLM.

## pipeline/additions/export_extraction_tables.py

### Script-level functions
- Human readable hint: export aggregated data-extraction tables for validation and quote audit.

#### export_tables(output_dir, consensus_path, input_paper_dir)
- Human readable hint: write one AI-vs-human comparison CSV and one quote-audit CSV, deduplicating per-paper outputs so successful retries supersede stale failed rows.

### Class ExtractionAggregateWriter
- Human readable hint: create the two run-level extraction CSVs at run start and replace each paper's aggregate rows when a newer retry completes.
- Human readable hint: fill the configured reviewer column with the configured AI label from `DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS` so AI rows can be distinguished from human reviewer rows.
- Human readable hint: fill optional wide-table quote columns through `DATA_EXTRACTION_QUOTE_COLUMN_ALIASES`; the long quote-audit CSV remains the complete quote record for every schema variable.

#### build_consensus_comparison_rows(records, schema, headers, input_paper_dir)
- Human readable hint: map nested KB extraction values into the same column layout as the human consensus CSV.

#### build_quote_audit_rows(records, schema, variable_to_header, input_paper_dir)
- Human readable hint: keep extracted values and supporting quotes in a long table for reviewer checking.

## pipeline/additions/export_expert_review_packets.py

### Script-level functions
- Human readable hint: create and summarize AI-first extraction expert oversight packets from existing extraction outputs.

#### export_expert_review_packets(source_output_dir, packet_output_dir)
- Human readable hint: read the configured schema, quote-audit CSV, per-paper extraction sidecars, and reviewer assignments to write one CSV per expert plus one combined tracking CSV.

#### summarize_expert_review(review_file, packet_output_dir)
- Human readable hint: summarize completed expert oversight decisions by schema variable, including acceptance/correction rates, error type/effect counts, and prompt/schema refinement triggers.

#### main()
- Human readable hint: command-line entrypoint with `export` and `summarize` subcommands.

## pipeline/core/extraction_schema.py

### Class ExtractionVariable
- Human readable hint: one row from `knowledge-base/data_extraction_schema.csv`, including the human consensus/export column used for validation, semantic anchors used by embedding retrieval, and the default user-editable extraction guidance fields.
- Human readable hint: active data-extraction schemas should include `human_reviewer_instruction`, `evidence_profile`, and `do_not_infer_from`; these columns are rendered into the LLM field instructions and used as schema-derived evidence-hint terms. The parser tolerates older schemas without them for compatibility, but the project default is to keep them in the KB rather than in `pipeline/` code.

### Class SchemaEvidenceHintConfig
- Human readable hint: stores user-tunable limits for the compact schema-guided evidence map inserted before long normalized text.

### Class SchemaEvidenceHintBuilder
- Human readable hint: scans normalized full text with terms derived from the schema CSV plus optional `DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES`, keeping review-specific bridge words in user config rather than pipeline code.
- Human readable hint: keeps nearby table rows with matched lines so labels and values survive PDF normalization, and uses configurable low-priority patterns to rank boilerplate, affiliation, and measurement-layout rows below participant/method evidence.

### Class DynamicExtractionSchema
- Human readable hint: combines two sources at runtime: the prompt provides the human-readable research framework, while the schema CSV provides the exact machine contract, default missing-data payload, and optional OpenAI Structured Outputs schema.

#### DynamicExtractionSchema.from_kb(kb_path)
- Human readable hint: read the configured extraction schema CSV, validate required columns, preserve semantic anchors and reviewer guidance metadata when present, and build the grouped extraction model.

#### DynamicExtractionSchema.from_prompt(prompt_text)
- Human readable hint: compatibility shim; extraction schemas now come from the CSV KB, not from prompt JSON.

#### DynamicExtractionSchema.inject_into_prompt(prompt_template)
- Human readable hint: keeps the user prompt as the conceptual framework, removes the conceptual response guide from runtime prompts, and inserts the KB-generated field contract before `# CONTEXT`; scoped runtime calls copy only matching `# STEPS` guidance for one domain or one configured domain batch.
- Human readable hint: legacy insertion placeholders are still tolerated for old prompt files, but future user prompts should be plain, readable review frameworks without technical markers.

#### DynamicExtractionSchema.domains
- Human readable hint: list KB domains in CSV order so extraction can split one paper into smaller schema requests.

#### DynamicExtractionSchema.for_domain(domain)
- Human readable hint: build a one-domain schema and instruction block for short, more reliable extraction responses.

#### DynamicExtractionSchema.for_domains(domains)
- Human readable hint: build a scoped schema for a configured group of domains so the run can balance fewer LLM calls with reliable smaller JSON responses.

#### DynamicExtractionSchema.validate_payload(payload)
- Human readable hint: validate one model JSON response and serialize it with exact KB-generated keys.

#### DynamicExtractionSchema.default_payload()
- Human readable hint: create a complete fallback payload for failed or empty extractions.

#### DynamicExtractionSchema.openai_response_format()
- Human readable hint: convert the generated Pydantic model into the `response_format` JSON schema sent to OpenAI.

### Script-level functions
- Human readable hint: CSV parsing, model generation, prompt formatting, value coercion, and JSON parsing helpers used by extraction and validation.

#### default_extraction_schema_path()
- Human readable hint: resolve the user-configured extraction schema path from `config/user_orchestrator.py`, falling back to `knowledge-base/data_extraction_schema.csv`.

#### load_extraction_variables(kb_path)
- Human readable hint: parse required KB columns: `domain`, `variable_name`, `variable_type`, `allowed_options`, `instruction`, and `covidence_column_name`, plus default project guidance columns `semantic_anchors`, `human_reviewer_instruction`, `evidence_profile`, and `do_not_infer_from` when available.

#### format_domain_overview(variables)
- Human readable hint: create the prompt-visible domain list from the active schema CSV so users can see the extraction plan.

#### format_prompt_domain_guidance(prompt_template, variables)
- Human readable hint: select only the conceptual prompt guidance relevant to the active CSV domain(s), using schema text plus optional `DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES` from `user_orchestrator.py`.

#### domain_groups_for_schema(schema, configured_groups)
- Human readable hint: validate user-configured extraction domain batches against the active schema CSV and append any missing domains as singleton batches.

#### _contains_normalized_phrase(searchable, alias)
- Human readable hint: match configured/schema aliases as whole words or phrases, so a domain label such as `population` does not accidentally match a neighboring concept such as `target populations`.

#### extract_prompt_guidance_blocks(prompt_template)
- Human readable hint: split the human prompt's `# STEPS` into reusable domain guidance; the conceptual response guide is left as user-facing explanation, not copied into runtime domain prompts.

#### remove_prompt_conceptual_schema_sections(prompt_template)
- Human readable hint: remove broad conceptual blocks only when the runtime mode no longer needs them; full-schema calls keep `# STEPS`, while scoped calls remove broad steps after the matching guidance has been extracted.

#### build_pydantic_model(variables)
- Human readable hint: create one nested domain model where every variable has `{variable_name}_value` and `{variable_name}_quote`.

#### format_instruction_block(variables, response_shape)
- Human readable hint: turn KB rows into LLM instructions, require a focused availability sweep before missing-value defaults, and include the consensus/export mapping for audit traceability.

#### parse_and_validate(raw_text, schema)
- Human readable hint: parse raw LLM output, validate dynamically, and return fallback data on failure.

## pipeline/core/extraction_io.py

### Class PaperItem
- Human readable hint: one prepared paper folder with metadata, selected chunks, optional PDF path, normalized text, and optional supplemental cited evidence.

### Class SupplementalEvidenceSource
- Human readable hint: one user-supplied cited source text attached to a data-extraction paper folder.

### Class SupplementalCitedEvidenceLoader
- Human readable hint: reads only user-configured per-paper supplemental evidence subfolders, trims source text with visible limits, and renders source-labeled prompt blocks without hardcoding review-topic or study-specific facts.

### Script-level functions
- Human readable hint: file and formatting helpers for data extraction.

#### collect_papers(csv_dir)
- Human readable hint: collect prepared paper folders from `input/per_paper_data_extraction/`, apply LLM-focused text cleanup that preserves tables, and attach optional supplemental cited evidence configured in `user_orchestrator.py`.

#### format_evidence(paper)
- Human readable hint: build the compact evidence block used by the extraction prompt, including source-labeled supplemental cited evidence when supplied.

#### flatten_extracted(payload, prefix)
- Human readable hint: flatten nested extraction output into dot-path CSV columns while keeping quote columns separate.

#### serialize_result(paper, extracted_data, run_id, raw_output, error)
- Human readable hint: build one stable JSONL record for downstream validation and audit.

#### write_outputs(payload, output_root, folder_name)
- Human readable hint: write per-paper JSONL and CSV extraction artifacts.

## pipeline/core/run_screening.py

### Class StagePipelineRunner
- Human readable hint: one-class stage runner that centralizes stage defaults and the run entrypoint.
- __init__ parameters: stage, csv_dir
#### StagePipelineRunner.__init__(stage, csv_dir)
- Human readable hint: __init__ stores the stage and input folder used to start screening.

#### StagePipelineRunner.run()
- Human readable hint: execute one stage run while allowing explicit overrides from callers.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _timestamp_label()
- Human readable hint: Create a timestamp string for output filenames. Returns: Timestamp string formatted as YYYYMMDD_HH-MM-SS. Note: higher precision reduces accidental overwrite risk across rapid reruns.

#### _stage_root(stage)
- Human readable hint: Return the output folder for a given stage. Args: stage: Current stage name (title_abstract/full_text/data_extraction). Returns: Path to output/<stage>/. Note: each stage writes into output/<stage>/.

#### _existing_qc_files(stage_root, stage_prefix)
- Human readable hint: Reuse the latest QC sample if present so the list stays stable across runs. Args: stage_root: Output directory for the stage. stage_prefix: Prefix for stage files (e.g., "title_abstract_"). Returns: Tuple of (qc_sample_csv_path, qc_sample_readable_path), or (None, None). Note: QC sample reuse ensures the same list is validated.

#### _stage_prefixed(path, target_stage)
- Human readable hint: Ensure a file path is placed under output/<stage>/ for consistency. Args: path: Desired file path (possibly outside output/<stage>/). target_stage: Stage name for output placement. Returns: Path under output/<stage>/ with the same filename. Note: keeps all outputs stage-scoped.

#### _extract_text(row, keys)
- Human readable hint: Read a text field from a CSV row using a list of possible column names. Args: row: A CSV row as a dict. keys: Candidate column names to search for. Returns: The first non-empty matching value, or empty string. Note: handles minor column-name variations in exports.

#### _load_negative_examples_from_csvs(csv_dir, patterns)
- Human readable hint: Load extra negative examples from CSVs to enrich the knowledge base. Args: csv_dir: Directory containing exported screening CSV files. patterns: List of glob patterns for negative-example CSVs. Returns: List of NEG example dicts with label/text. Note: these negatives improve evidence filtering precision.

#### _safe_int(val, default)
- Human readable hint: safely coerce config values to int and fail fast on invalid values.

#### _safe_float(val, default)
- Human readable hint: safely coerce config values to float and fail fast on invalid values.

#### _safe_bool(val, default)
- Human readable hint: safely coerce config values to bool using common yes/no string forms.

#### _append_qc_records_to_remaining(stage_root, stage_prefix, remaining_path)
- Human readable hint: Append QC sample eligibility records to the remaining-sample output.

#### run_pipeline(stage, split_only, csv_dir, kb_file, eligibility_output, chunks_output, text_output, error_log, resource_log, top_k, score_threshold, sample_size, sample_seed, batch_size, sustainability_tracking, pdf_root, quiet, confirm_sampling, sample_rate, qc_only, qc_enabled, force_new_qc, enable_time_savings, run_label_override, artifact_mode)
- Human readable hint: Run one pipeline stage with stage-specific defaults and outputs. Supports optional artifact mode override (`compact` or `full`) for per-paper full_text artifact persistence.

## pipeline/integrations/embedding_utils.py

### Class TextPdfUtils
- Human readable hint: one utility class for language detection, PDF reading, and sentence splitting.
#### TextPdfUtils.normalize_extracted_text(text)
- Human readable hint: apply conservative cleanup to extracted PDF text before sentence splitting and prompt assembly.

#### TextPdfUtils.normalize_extracted_text_for_llm(text)
- Human readable hint: preserve table structure while removing common PDF layout artifacts before LLM extraction.

#### TextPdfUtils.detect_language_code(text)
- Human readable hint: detect language code (for example en/de/fr) used by full_text language policy checks.

#### TextPdfUtils.detect_language(text)
- Human readable hint: Detect whether text is English or German using stopword counts.

#### TextPdfUtils._normalize_margin_line(line)
- Human readable hint: normalize page-margin text to detect repeated headers/footers across pages.

#### TextPdfUtils._remove_repeated_margin_lines(raw_pages)
- Human readable hint: remove repetitive header/footer lines from page text before retrieval chunking.

#### TextPdfUtils._read_pypdf_pages(file_path, max_pages)
- Human readable hint: read page-level text through PyPDF fallback when pdfplumber extraction is sparse.

#### TextPdfUtils.read_pdf_file(file_path, max_pages)
- Human readable hint: Read PDF text and return a single combined string (optionally capped by max_pages).

#### TextPdfUtils.read_pdf_pages(file_path, max_pages)
- Human readable hint: Read PDF text and return a list of page-level strings (optionally capped).

#### TextPdfUtils.split_text_into_sentences(text, language)
- Human readable hint: Split text into sentences using NLTK.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### detect_language(text)
- Human readable hint: Detect whether text is English or German using stopword counts.

#### detect_language_code(text)
- Human readable hint: Detect language code for deterministic language policy checks.

#### read_pdf_file(file_path, max_pages)
- Human readable hint: Read PDF text and return a single combined string (optionally capped by max_pages).

#### read_pdf_pages(file_path, max_pages)
- Human readable hint: Read PDF text and return a list of page-level strings (optionally capped).

#### split_text_into_sentences(text, language)
- Human readable hint: Split text into sentences using NLTK.

#### normalize_extracted_text_for_llm(text)
- Human readable hint: Apply LLM-focused cleanup while preserving table structure.

## pipeline/integrations/llm_client.py

### Class OpenAIResponder
- Human readable hint: Generate responses using the OpenAI API within a RAG workflow.
- __init__ parameters: data, model, prompt_template, client
#### OpenAIResponder.__init__(data, model, prompt_template, client)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### OpenAIResponder._request_kwargs()
- Human readable hint: build one consistent chat request payload for sync and async calls.

#### OpenAIResponder._usage_to_dict(usage)
- Human readable hint: normalize provider usage objects into plain dictionaries.

#### OpenAIResponder._response_to_tuple(response)
- Human readable hint: parse one response object and return content plus usage metadata.

#### OpenAIResponder._is_retryable_error(exc)
- Human readable hint: retry only on transient transport/rate-limit provider failures.

#### OpenAIResponder.generate_response(retries, backoff_seconds)
- Human readable hint: Get one response from the model and return text plus usage metadata.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _format_prompt(template, data)
- Human readable hint: Insert the evidence text into the prompt template.

## pipeline/selection/chunking.py

### Class ChunkBuilder
- Human readable hint: one class that groups all chunk-building methods for title/abstract and full-text.
#### ChunkBuilder.clean_text(value)
- Human readable hint: Trim whitespace from text fields safely.

#### ChunkBuilder._is_low_information_sentence(sentence)
- Human readable hint: discard table-like/citation-like sentence fragments before full-text chunk assembly.

#### ChunkBuilder.chunk_sentence_entries(entries, chunk_size, overlap_size)
- Human readable hint: Group sentence entries into overlapping chunks with page/line spans and sentence/word count metadata.

#### ChunkBuilder.chunk_paper_sentences(paper_id, title, abstract, language)
- Human readable hint: Split title and abstract into sentence chunks (title sentences are always kept).

#### ChunkBuilder.chunk_fulltext_sentences(paper_id, title, full_text, language, page_texts)
- Human readable hint: Split full-text into overlapping blocks to stay within context limits.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _clean_text(value)
- Human readable hint: Trim whitespace from text fields safely.

#### chunk_paper_sentences(paper_id, title, abstract, language)
- Human readable hint: Split title and abstract into sentence chunks (title sentences are always kept).

#### _chunk_sentence_entries(entries, chunk_size, overlap_size)
- Human readable hint: Group sentence entries into overlapping chunks with page/line spans.

#### chunk_fulltext_sentences(paper_id, title, full_text, language, page_texts)
- Human readable hint: Split full-text into overlapping blocks to stay within context limits.

## pipeline/selection/selector.py

### Class EmbeddingBackend
- Human readable hint: Fetch embeddings from the API and cache them for reuse.
#### EmbeddingBackend.__post_init__()
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### EmbeddingBackend.embed_texts(texts)
- Human readable hint: Return embeddings for texts plus usage metadata when the API provides it.

#### EmbeddingBackend._maybe_evict_cache()
- Human readable hint: Evict oldest cached embeddings to cap memory usage.

#### EmbeddingBackend._embed_in_batches(texts)
- Human readable hint: Request embeddings in batches and accumulate usage if available.

### Class RelevanceSelector
- Human readable hint: Score chunks against POS/NEG examples and keep the most relevant ones.
- __init__ parameters: embedder, examples, always_include_kinds
#### RelevanceSelector.__init__(embedder, examples, always_include_kinds)
- Human readable hint: No docstring in source; placeholder retained intentionally for exhaustive traceability.

#### RelevanceSelector._score_vectors(vectors)
- Human readable hint: Compute relevance scores for each vector.

#### RelevanceSelector.select(chunks, top_k, score_threshold)
- Human readable hint: score only candidate chunks; always-include kinds bypass embedding for speed.

### Class SelectionEngine
- Human readable hint: dominant selector class that owns embedding and relevance-scoring setup for one script.
- __init__ parameters: examples, batch_size, always_include_kinds, embedder
#### SelectionEngine.__init__(examples, batch_size, always_include_kinds, embedder)
- Human readable hint: __init__ stores examples and prepares the underlying selector with one consistent interface.

#### SelectionEngine.select(chunks, top_k, score_threshold)
- Human readable hint: return selected chunks and scores using the configured embedding+relevance backend.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### load_labeled_examples(path)
- Human readable hint: Load POS/NEG training examples used to score relevance.

#### _normalize(vec)
- Human readable hint: Normalize a vector to unit length.

## Update Log (2026-04-18)

### main.py

#### _merge_emissions_with_run_column(stage, run_label, attempt_index)
- Human readable hint: delegates CodeCarbon run-label merge to a dedicated CSV-safe helper module to avoid ad-hoc comma-splitting bugs.

#### _require_base_outputs(stage, run_label)
- Human readable hint: retry requires base eligibility output; missing base emissions now logs a warning but does not block retry execution.

#### _execute_retry_run(stage, run_label, retry_csv, attempt_map)
- Human readable hint: unified retry executor for both startup-pending and post-error retry paths; runs isolated retry screening, updates post-run indices/emissions, and appends retry manifest entries.

### pipeline/core/pipeline.py

#### PaperScreeningPipeline.run()
- Human readable hint: run tracking now uses an explicit fail-safe guard so CodeCarbon/resource finalization still executes on unexpected exceptions.

#### PaperScreeningPipeline._stage_csv_files(select_only)
- Human readable hint: retry CSV resolution now recognizes both `retry_runs/` roots and isolated child folders under `retry_runs/` for robust retry-file discovery.

### pipeline/additions/emissions_merge.py

#### merge_emissions_with_run_column(stage_root, stage, run_label, attempt_index)
- Human readable hint: CSV-safe merge for CodeCarbon outputs that labels rows with `run` (`main` / `retry_N`), merges latest retry rows into a stable base file, and returns appended row indices for manifest traceability.

#### _read_csv_rows(path), _write_csv_rows(path, fieldnames, rows), _ensure_run_column(fieldnames)
- Human readable hint: low-level CSV helpers keep quoting and column alignment deterministic across merge/rewrite operations.

#### _fill_run_values(rows, run_value, override_existing), _label_single_file_retry_rows(rows, attempt_index)
- Human readable hint: row-label helpers preserve existing retry labels while filling missing/legacy `run` values in both single-file and multi-file CodeCarbon scenarios.

### pipeline/additions/stats_engine.py

#### _load_ai()
- Human readable hint: returns schema-stable empty DataFrames (`paper_id`, `ai_decision`, `ai_reason`, `source_path`) when no usable AI records exist, preventing downstream merge failures.

#### _merge(ai, human)
- Human readable hint: guards required AI merge columns on empty inputs so QC validation can complete with explicit zero-overlap outputs instead of raising KeyError.

### pipeline/additions/bulk_pdf_match.py

#### load_targets(target_root, overwrite), load_candidates(source_root), score_candidate(target, candidate)
- Human readable hint: public API wrappers expose matcher internals for companion tooling without importing private underscore-prefixed functions.

### pipeline/additions/bulk_pdf_match_review.py

#### _targets_by_folder(target_root), run(args)
- Human readable hint: now uses public matcher APIs (`load_targets`, `load_candidates`, `score_candidate`) to reduce fragility when matcher internals evolve.

---
**Read next:** [readme.md](readme.md)
