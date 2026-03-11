# Function Explanation UID

This file explains each script through its primary class, visible __init__ parameters, and callable methods.

## .tmp_generate_function_docs.py

## backup_to_github.py

### Class BackupToGitHub
- Human readable hint: human readable hint: one-class backup workflow with explicit command methods and one run entrypoint.
- __init__ parameters: backup_message
#### BackupToGitHub.__init__(backup_message)
- Human readable hint: human readable hint: __init__ stores the commit message used for the backup commit.

#### BackupToGitHub.run_command(cmd)
- Human readable hint: human readable hint: run one git command and stop the script when the command fails.

#### BackupToGitHub.run_backup()
- Human readable hint: human readable hint: execute pull, add, commit, and push in safe sequence.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### run(cmd)
- Human readable hint: Compatibility wrapper for older calls.

#### main()
- Human readable hint: No docstring provided.

## config/user_orchestrator.py

### Class UserConfig
- Human readable hint: Static snapshot of user-facing settings for the current run. Note: this bundles all inputs so other scripts can read a single object.
### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### require_setting(container, key, container_name)
- Human readable hint: No docstring provided.

#### require_setting(container, key, container_name, expected_type)
- Human readable hint: No docstring provided.

#### require_setting(container, key, container_name, expected_type)
- Human readable hint: Fetch a required setting from a config dict and warn if missing. Args: container: Settings dictionary (e.g., LLM_SETTINGS). key: Key to look up in the settings dict. container_name: Human-readable container name for warnings. Returns: The value stored under the key. Note: missing settings stop the run so you can fix the config.

#### load_user_config()
- Human readable hint: Build and validate a UserConfig from module globals (one call per run). Note: you do not edit this function; it just packages the values above.

## main.py

### Class MainWorkflow
- Human readable hint: human readable hint: one-class orchestrator for terminal flow, retries, QC gating, and stage execution.
- __init__ parameters: none
#### MainWorkflow.__init__()
- Human readable hint: human readable hint: __init__ keeps the key runtime attributes visible in one place.

#### MainWorkflow.run()
- Human readable hint: Run the pipeline for the selected stage with safety checks.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _last_artifact_dict()
- Human readable hint: Return last_artifact only when it is a dict; otherwise None for type safety.

#### _qc_screened_already(stage)
- Human readable hint: Detect whether a QC sample for this stage was already screened.

#### _run_pipeline_guarded()
- Human readable hint: Run the pipeline and store artifacts; mark prompts as not-all-yes on failure.

#### _parse_is_eligible(decision, stage)
- Human readable hint: Best-effort extraction of is_eligible from an LLM decision payload (stage-aware).

#### _parse_exclusion_reason(decision)
- Human readable hint: human readable hint: extract exclusion_reason_category if present.

#### _collect_missing_is_eligible(error_log_path, eligibility_path, stage)
- Human readable hint: Find paper_ids that have errors AND no is_eligible in eligibility output.

#### _write_retry_csv(source_csv, target_dir, paper_ids, stage, run_label)
- Human readable hint: Create a stage-valid retry CSV using run_label and stage-specific token (screen/select).

#### _retry_output_paths(stage, run_label, attempt_index)
- Human readable hint: human readable hint: retry outputs stay separate using stage_runlabel_retry_attempt_output_timestamp order.

#### _latest_base_outputs(stage, run_label)
- Human readable hint: human readable hint: locate the most recent outputs for a stage+run_label.

#### _require_base_outputs(stage, run_label)
- Human readable hint: Ensure base outputs exist before running a retry; avoid orphan retry files.

#### _infer_run_label_from_retry_csv(path, stage)
- Human readable hint: human readable hint: infer run_label from retry CSV name or existing base files.

#### _first_available_run_label(stage, preferred)
- Human readable hint: human readable hint: pick a run_label that has base outputs (eligibility + emissions).

#### _record_retry_manifest(retry_artifact, stage, attempt_map, source_csv, emissions_info)
- Human readable hint: Keep retry artifacts separate and append a manifest entry listing files and paper_ids.

#### _merge_emissions_with_run_column(stage, run_label, attempt_index)
- Human readable hint: human readable hint: keep one CodeCarbon CSV per run_label; append new rows with run=main/retry_N and report row numbers.

#### _extract_summary_stats(path)
- Human readable hint: human readable hint: derive counts and summary percentiles from eligibility JSONL.

#### _run_tag_for_path(path, stage, output_token)
- Human readable hint: human readable hint: derive run tag (sample + timestamp + retry) from filename.

#### _append_index_row(idx_path, sample_selection, stage, decision_split, path, stats, total_paper_count)
- Human readable hint: human readable hint: write/update one row in eligibility index for a decision split.

#### _update_index_from_artifact(stage, artifact, attempt_index)
- Human readable hint: human readable hint: append index rows for all eligibility splits from a run (base or retry).

#### _post_run_updates(stage, artifact, attempt_index)
- Human readable hint: human readable hint: after any run, merge emissions and refresh eligibility index.

#### _next_retry_attempt(stage, run_label)
- Human readable hint: human readable hint: derive the next retry attempt index from the manifest (per run_label).

#### _latest_eligibility_map(stage)
- Human readable hint: human readable hint: load the most recent eligibility JSONL into a paper_id->decision map.

#### _decision_is_complete(decision, stage)
- Human readable hint: human readable hint: validate presence of is_eligible and required justification/reason.

#### _retry_csv_needed(retry_csv, stage)
- Human readable hint: human readable hint: return paper_ids in retry_csv that still lack complete decisions.

#### _archive_retry_csv(retry_csv)
- Human readable hint: human readable hint: archive a fully resolved retry CSV to processed/.

#### _latest_retry_csv(stage)
- Human readable hint: Locate the most recent retry CSV under input/retry_runs for this stage (any sample, screen/select).

#### _error_ids_by_type(error_log_path, blocked_types)
- Human readable hint: human readable hint: collect paper_ids with deterministic errors that should not trigger auto-retry.

#### _prompt_retry_if_needed(stage, artifact)
- Human readable hint: Prompt for re-screening when errors are present for this stage.

#### _ensure_csv_inputs(csv_dir)
- Human readable hint: Check that the input folder exists and has at least one CSV file. Args: csv_dir: Path to the input/ folder containing Covidence exports. Returns: True if at least one CSV exists; False otherwise. Note: this prevents running the pipeline with missing exports.

#### _require_pattern(csv_dir, pattern, description, stage)
- Human readable hint: Ensure required CSVs exist for the current stage (pick latest when multiple). Args: csv_dir: Path to the input/ folder. pattern: Glob pattern for required CSV files. description: Human-readable description of the required export. stage: Optional stage label for extra sanity checks. Returns: A list containing the latest matching CSV path (empty if none found). Note: deterministic choice avoids ambiguity when several exports are present.

#### _missing_pdf_folders(base_dir)
- Human readable hint: List per-paper folders that still have no PDF file. Args: base_dir: Path to the per-paper folder root (e.g., input/per_paper_full_text/). Returns: A list of folder names missing a PDF file. Note: missing PDFs are skipped in full_text/data_extraction.

#### _ensure_nltk_tokenizers()
- Human readable hint: Download NLTK sentence tokenizers once so sentence splitting works. Note: required for consistent sentence chunking.

#### _prompt_yes_no(message)
- Human readable hint: Ask a yes/no question in the terminal and return True for yes. Args: message: Prompt text displayed to the user. Returns: True for yes, False for no (or non-interactive terminal). Note: keeps QC decisions explicit and auditable.

#### _run_validation()
- Human readable hint: human readable hint: run validation and return True on success.

#### _run_qc_loop(stage, sample_rate, quiet)
- Human readable hint: Run QC-only screening, validation prompt, and decision loop. Returns True if the user approves validation and wants full screening. Args: stage: Current pipeline stage (title_abstract/full_text/data_extraction). sample_rate: Fraction of planned papers to include in QC. quiet: If True, suppress most console output. Returns: True if user approves validation and proceeds to full screening; False otherwise.

#### main()
- Human readable hint: Compatibility entrypoint that runs the class-based main workflow.

## pipeline/additions/input_trace.py

### Class InputTraceRunner
- Human readable hint: human readable hint: one-class trace utility that reconstructs one paper input and verifies its hashes.
- __init__ parameters: stage
#### InputTraceRunner.__init__(stage)
- Human readable hint: human readable hint: __init__ stores the default stage used when CLI arguments omit --stage.

#### InputTraceRunner.run(args)
- Human readable hint: human readable hint: execute the full trace workflow from eligibility record lookup to report writing.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _sha256_text(value)
- Human readable hint: human readable hint: compute a stable fingerprint of any text.

#### _latest_eligibility_file(stage)
- Human readable hint: human readable hint: pick the latest eligibility file (excluding split files).

#### _find_record(eligibility_file, paper_id, input_hash)
- Human readable hint: human readable hint: find one paper in eligibility output by paper_id or stored input hash.

#### _strip_author_mentions(text, authors)
- Human readable hint: human readable hint: mirror screening redaction logic for exact reproducibility.

#### _format_chunks_for_prompt(stage, paper_id, title, authors, chunks)
- Human readable hint: human readable hint: rebuild the same context text format sent to the model.

#### _title_abstract_context(stage, paper_id)
- Human readable hint: human readable hint: title_abstract stores the full model context in selected_chunks output.

#### _load_folder_metadata(folder)
- Human readable hint: No docstring provided.

#### _extract_covidence_id(row)
- Human readable hint: No docstring provided.

#### _find_paper_folder(stage, paper_id, csv_root)
- Human readable hint: human readable hint: locate the per-paper folder by matching Covidence/paper ID in metadata.

#### _load_selected_chunks(folder, stage, paper_id)
- Human readable hint: No docstring provided.

#### _folder_stage_context(stage, paper_id, csv_root)
- Human readable hint: human readable hint: rebuild full_text/data_extraction model context from metadata + selected chunks.

#### _reconstruct_context(stage, paper_id, csv_root)
- Human readable hint: human readable hint: stage-aware reconstruction of exact model context.

#### _load_prompt_template(stage)
- Human readable hint: human readable hint: mirror runtime prompt assembly with optional eligibility criteria injection.

#### _parse_args()
- Human readable hint: No docstring provided.

#### run_trace()
- Human readable hint: Compatibility wrapper for direct module execution.

## pipeline/additions/resource_usage.py

### Class ResourceUsageConfig
- Human readable hint: Configuration for resource usage tracking. Args: resource_log_path: Path to JSONL resource log. enable_tracking: If True, write resource logs and totals. enable_codecarbon: If True, track emissions via CodeCarbon (if installed). stage: Current pipeline stage (title_abstract | full_text | data_extraction). qc_sample_path: Optional QC sample CSV path to derive actual QC counts. qc_paper_count: Optional precomputed QC size to avoid re-reading the QC CSV. run_label: Run label suffix (qc_sample or remaining_sample) for file naming. enable_time_savings: If True, compute human-time savings (only when validation ran).
### Class CarbonTrackerManager
- Human readable hint: Initialize and manage CodeCarbon trackers with offline/online support.
- __init__ parameters: enabled
#### CarbonTrackerManager.__init__(enabled)
- Human readable hint: No docstring provided.

#### CarbonTrackerManager._init_tracker()
- Human readable hint: No docstring provided.

#### CarbonTrackerManager.start()
- Human readable hint: Start the tracker (no-op if unavailable).

#### CarbonTrackerManager.stop()
- Human readable hint: Stop the tracker and return emissions (kg CO2eq), if available.

#### CarbonTrackerManager.rename_emissions_csv(timestamp_label, run_label)
- Human readable hint: Rename CodeCarbon's emissions.csv to stage/sample naming: <stage>_<sample>_codecarbon_emissions_<timestamp>.

#### CarbonTrackerManager.energy_kwh()
- Human readable hint: Return final energy consumed in kWh, if available.

#### CarbonTrackerManager.__enter__()
- Human readable hint: No docstring provided.

#### CarbonTrackerManager.__exit__(exc_type, exc, tb)
- Human readable hint: No docstring provided.

#### CarbonTrackerManager.measure_energy(func)
- Human readable hint: Decorator for function-level emissions tracking.

### Class ResourceUsageTracker
- Human readable hint: Track per-paper and per-run resource usage, with optional CodeCarbon.
- __init__ parameters: config
#### ResourceUsageTracker.__init__(config)
- Human readable hint: No docstring provided.

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
- Human readable hint: human readable hint: dominant class for this script; it exposes one stable API for run/resource tracking.
- __init__ parameters: resource_log_path, enable_tracking, enable_codecarbon, stage, qc_sample_path, qc_paper_count, run_label, enable_time_savings
#### ResourceUsageEngine.__init__(resource_log_path, enable_tracking, enable_codecarbon, stage, qc_sample_path, qc_paper_count, run_label, enable_time_savings)
- Human readable hint: human readable hint: __init__ captures all run-level tracking parameters in one visible constructor.

#### ResourceUsageEngine.start_run()
- Human readable hint: human readable hint: start CodeCarbon/resource tracking for the current run.

#### ResourceUsageEngine.set_qc_count(qc_count)
- Human readable hint: human readable hint: set QC paper count once so the tracker does not re-read QC CSV files.

#### ResourceUsageEngine.log_paper(paper_id, prompt_tokens, response_tokens, pdf_text_tokens, pdf_visual_tokens, embedding_tokens, prompt_tokens_source, response_tokens_source, embedding_tokens_source, paper_seconds)
- Human readable hint: human readable hint: log per-paper token/runtime metrics in the shared run tracker.

#### ResourceUsageEngine.stop_run(total_runtime_seconds, paper_count)
- Human readable hint: human readable hint: stop tracking and write final TOTAL summary lines.

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
- Human readable hint: human readable hint: one-class validation orchestrator for screening and extraction stages.
- __init__ parameters: stage
#### ValidationEngine.__init__(stage)
- Human readable hint: human readable hint: __init__ stores the active stage used to route validation.

#### ValidationEngine.run(args)
- Human readable hint: human readable hint: run the correct validation branch based on the configured stage.

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
- Human readable hint: No docstring provided.

#### _extract_tags(value)
- Human readable hint: human readable hint: map explicit Covidence tags to the curated include list; ignores notes.

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
- Human readable hint: human readable hint: exact (Clopper-Pearson) CI via statsmodels (Seabold & Perktold, 2010).

#### _metrics(tp, tn, fp, fn)
- Human readable hint: Compute agreement metrics for screening.

#### _write_alignment(df, suffix)
- Human readable hint: human readable hint: single QC alignment file with decisions and reasons.

#### _write_report(stats, tp, tn, fp, fn, stage, suffix)
- Human readable hint: Write a readable validation summary report.

#### _plot_confusion(tp, tn, fp, fn, suffix)
- Human readable hint: Draw and save a confusion-matrix plot.

#### _extract_timestamp_suffix(path)
- Human readable hint: Extract the YYYYMMDD_HH-MM suffix from a stage output filename.

#### _load_qc_sample_ids(suffix)
- Human readable hint: Load QC sample IDs for the matching timestamp suffix.

#### validate_screening(stage, args)
- Human readable hint: Validate screening decisions against human labels.

#### _load_ai_extraction_records()
- Human readable hint: Load extraction outputs from per-paper JSONL files.

#### validate_extraction(consensus_path)
- Human readable hint: Validate extraction outputs against the adjudicated consensus table.

#### _parse_args()
- Human readable hint: Parse CLI arguments for validation.

#### run_validation()
- Human readable hint: Compatibility wrapper for direct execution.

## pipeline/core/pipeline.py

### Class PaperRecord
- Human readable hint: No class docstring provided.
### Class _ScreeningDecisionBaseModel
- Human readable hint: human readable hint: shared schema for screening decisions returned by the LLM.
#### _ScreeningDecisionBaseModel._check_reason_for_exclusion()
- Human readable hint: human readable hint: exclusion decisions must carry an explicit exclusion reason.

### Class TitleAbstractScreeningDecisionModel
- Human readable hint: human readable hint: title_abstract allows a NEUTRAL eligibility outcome.
### Class FullTextScreeningDecisionModel
- Human readable hint: human readable hint: full_text requires a strict boolean eligibility outcome.
### Class PaperScreeningPipeline
- Human readable hint: No class docstring provided.
- __init__ parameters: csv_dir, knowledge_base_path, eligibility_output_path, chunks_output_path, text_output_path, top_k, score_threshold, batch_size, embedder, examples, sample_size, sample_seed, sustainability_tracking, resource_log_path, enable_time_savings, run_label, codecarbon_enabled, qc_sample_path, qc_sample_readable_path, confirm_sampling, sample_rate, qc_only, qc_enabled, force_new_qc, error_log_path, stage, pdf_root, overflow_log_path, split_only, quiet, summary_to_console
#### PaperScreeningPipeline.__init__(csv_dir, knowledge_base_path, eligibility_output_path, chunks_output_path, text_output_path, top_k, score_threshold, batch_size, embedder, examples, sample_size, sample_seed, sustainability_tracking, resource_log_path, enable_time_savings, run_label, codecarbon_enabled, qc_sample_path, qc_sample_readable_path, confirm_sampling, sample_rate, qc_only, qc_enabled, force_new_qc, error_log_path, stage, pdf_root, overflow_log_path, split_only, quiet, summary_to_console)
- Human readable hint: Initialize the screening/extraction pipeline with configuration. All arguments are strictly typed and have clear defaults for robust, reproducible runs. Non-coders: Each parameter controls a key aspect of the workflow (see README for details).

#### PaperScreeningPipeline._sha256_text(value)
- Human readable hint: human readable hint: stable fingerprint to verify whether two input texts are exactly identical.

#### PaperScreeningPipeline.run()
- Human readable hint: Main pipeline: prep folders (if needed), QC sample, then screen papers.

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
- Human readable hint: human readable hint: stream title_abstract completions paper-by-paper as async calls finish.

#### PaperScreeningPipeline._use_async_stage_processing()
- Human readable hint: human readable hint: allow stage-specific opt-in async processing beyond title_abstract.

#### PaperScreeningPipeline._process_non_title_async_batch(planned_papers)
- Human readable hint: human readable hint: stream full_text/data_extraction completions paper-by-paper.

#### PaperScreeningPipeline._stream_async_batch(planned_papers, processor)
- Human readable hint: human readable hint: bridge async processing to sync caller while emitting per-paper completion updates.

#### PaperScreeningPipeline._process_paper(paper)
- Human readable hint: human readable hint: sync mode reuses the async processing core to avoid duplicate decision logic.

#### PaperScreeningPipeline._format_chunks_for_prompt(paper, chunks)
- Human readable hint: Format selected chunks into a readable prompt section.

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

#### PaperScreeningPipeline._count_pdf_pages(pdf_path)
- Human readable hint: Return number of pages in a PDF; fall back to 0 on failure.

#### PaperScreeningPipeline._prepare_chunks(paper)
- Human readable hint: Create evidence chunks, token counts, and resolved language for one paper.

#### PaperScreeningPipeline._materialize_paper_folders_full_text()
- Human readable hint: Split select CSV rows into per-paper folders under csv_dir/per_paper_full_text.

#### PaperScreeningPipeline._materialize_data_extraction_subset()
- Human readable hint: Create per-paper data_extraction folders from included IDs.

#### PaperScreeningPipeline._find_missing_pdfs(base_dir)
- Human readable hint: List folders that do not contain any PDF.

#### PaperScreeningPipeline._find_included_csv()
- Human readable hint: Find the most recent included CSV used for data_extraction.

#### PaperScreeningPipeline._stage_csv_files(select_only)
- Human readable hint: Return stage-appropriate CSV files.

#### PaperScreeningPipeline._load_included_ids(csv_path)
- Human readable hint: Read included IDs from a Covidence CSV.

#### PaperScreeningPipeline._extract_covidence_id(row)
- Human readable hint: Extract the best available Covidence/paper ID.

#### PaperScreeningPipeline._extract_year(row)
- Human readable hint: Try to find a publication year from many possible columns.

#### PaperScreeningPipeline._match_row_value(row, key)
- Human readable hint: Find a value in a row using exact, case-insensitive, or compact keys.

#### PaperScreeningPipeline._build_paper_folder_name(row)
- Human readable hint: Create a safe per-paper folder name using ID/author/year/title.

#### PaperScreeningPipeline._load_pdf_text(paper, resolved_path)
- Human readable hint: Read PDF text once (optionally page-level) and count pages; returns the path used.

#### PaperScreeningPipeline._resolve_pdf_path(paper)
- Human readable hint: Find the PDF inside the per-paper folder and normalize its filename.

#### PaperScreeningPipeline._call_llm(context)
- Human readable hint: Call the LLM and return both text and usage (if provided by the API).

#### PaperScreeningPipeline._get_openai_client(base_url)
- Human readable hint: Create a configured OpenAI API client.

#### PaperScreeningPipeline._get_async_openai_client(base_url)
- Human readable hint: Create a configured async OpenAI API client.

#### PaperScreeningPipeline._validate_screening_decision(decision_text)
- Human readable hint: human readable hint: validate screening JSON and enforce prompt-demanded keys for this stage.

#### PaperScreeningPipeline._extract_required_json_fields_from_prompt(prompt_template)
- Human readable hint: human readable hint: detect field names declared in the prompt schema section.

#### PaperScreeningPipeline._percentiles(values)
- Human readable hint: human readable hint: provide quick p50/p95/max without heavy deps.

#### PaperScreeningPipeline._parse_is_eligible(decision)
- Human readable hint: human readable hint: stage-aware extraction of is_eligible from the LLM decision payload.

#### PaperScreeningPipeline._decision_payload(decision)
- Human readable hint: human readable hint: parse JSON text decisions once so downstream checks can reuse the payload.

#### PaperScreeningPipeline._parse_exclusion_reason(decision)
- Human readable hint: human readable hint: derive exclusion_reason_category if present in LLM output.

#### PaperScreeningPipeline._decision_missing_fields(decision)
- Human readable hint: human readable hint: detect missing justification or exclusion_reason_category without altering the decision.

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
- Human readable hint: Write per-paper extraction outputs (JSONL + CSV).

#### PaperScreeningPipeline._normalize_criterion(text)
- Human readable hint: Normalize a prompt bullet line into a clean field name.

#### PaperScreeningPipeline._extract_criteria_from_prompt(cls, prompt_text)
- Human readable hint: Infer extraction fields from the "Fields to extract" section only.

#### PaperScreeningPipeline._build_extraction_payload(paper, llm_decision)
- Human readable hint: Parse the LLM output into structured extraction data.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _load_optional_eligibility_criteria_text()
- Human readable hint: human readable hint: load shared eligibility criteria text when configured and available.

#### _load_stage_prompt_template(stage)
- Human readable hint: human readable hint: load stage prompt and inject shared criteria only when placeholder is present.

## pipeline/core/run_screening.py

### Class StagePipelineRunner
- Human readable hint: human readable hint: one-class stage runner that centralizes stage defaults and the run entrypoint.
- __init__ parameters: stage, csv_dir
#### StagePipelineRunner.__init__(stage, csv_dir)
- Human readable hint: human readable hint: __init__ stores the stage and input folder used to start screening.

#### StagePipelineRunner.run()
- Human readable hint: human readable hint: execute one stage run while allowing explicit overrides from callers.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _timestamp_label()
- Human readable hint: Create a timestamp string for output filenames. Returns: Timestamp string formatted as YYYYMMDD_HH-MM. Note: timestamps prevent overwriting prior runs.

#### _stage_root(stage)
- Human readable hint: Return the output folder for a given stage. Args: stage: Current stage name (title_abstract/full_text/data_extraction). Returns: Path to output/<stage>/. Note: each stage writes into output/<stage>/.

#### _existing_qc_files(stage_root, stage_prefix)
- Human readable hint: Reuse the latest QC sample if present so the list stays stable across runs. Args: stage_root: Output directory for the stage. stage_prefix: Prefix for stage files (e.g., "title_abstract_"). Returns: Tuple of (qc_sample_csv_path, qc_sample_readable_path), or (None, None). Note: QC sample reuse ensures the same list is validated.

#### _stage_prefixed(path, target_stage)
- Human readable hint: Ensure a file path is placed under output/<stage>/ for consistency. Args: path: Desired file path (possibly outside output/<stage>/). target_stage: Stage name for output placement. Returns: Path under output/<stage>/ with the same filename. Note: keeps all outputs stage-scoped.

#### _extract_text(row, keys)
- Human readable hint: Read a text field from a CSV row using a list of possible column names. Args: row: A CSV row as a dict. keys: Candidate column names to search for. Returns: The first non-empty matching value, or empty string. Note: handles minor column-name variations in exports.

#### _load_negative_examples_from_csvs(csv_dir, patterns)
- Human readable hint: Load extra negative examples from CSVs to enrich the knowledge base. Args: csv_dir: Directory containing Covidence exports. patterns: List of glob patterns for negative-example CSVs. Returns: List of NEG example dicts with label/text. Note: these negatives improve evidence filtering precision.

#### _safe_int(val, default)
- Human readable hint: No docstring provided.

#### _safe_float(val, default)
- Human readable hint: No docstring provided.

#### _safe_bool(val, default)
- Human readable hint: No docstring provided.

#### _append_qc_records_to_remaining(stage_root, stage_prefix, remaining_path)
- Human readable hint: Append QC sample eligibility records to the remaining-sample output.

#### run_pipeline(stage, split_only, csv_dir, kb_file, eligibility_output, chunks_output, text_output, error_log, resource_log, top_k, score_threshold, sample_size, sample_seed, batch_size, sustainability_tracking, pdf_root, quiet, confirm_sampling, sample_rate, qc_only, qc_enabled, force_new_qc, enable_time_savings, run_label_override)
- Human readable hint: Run one pipeline stage with stage-specific defaults and outputs. Args: stage: Stage name (title_abstract/full_text/data_extraction). split_only: If True, only prepare folders and exit. csv_dir: Override input/ folder path. kb_file: Override KB file path for this run. eligibility_output: Override eligibility JSONL output path. chunks_output: Override selected-chunks JSONL output path. text_output: Override readable summary output path. error_log: Override error log path. top_k: Max number of evidence chunks per paper. score_threshold: Minimum relevance score threshold. sample_size: Optional fixed number of papers to sample. sample_seed: Random seed for sampling. batch_size: Embedding batch size. sustainability_tracking: If True, write resource logs. pdf_root: Optional PDF root path override. quiet: If True, suppress most console output. confirm_sampling: If True, skip QC prompt (already confirmed). sample_rate: QC sample fraction (0–1). qc_only: If True, screen QC sample only. qc_enabled: If False, skip QC sampling entirely. force_new_qc: If True, generate a new QC sample even if one exists. Returns: True if screening executed; False if the run exited early. Note: this is the core launcher used by main.py.

## pipeline/integrations/embedding_utils.py

### Class TextPdfUtils
- Human readable hint: human readable hint: one utility class for language detection, PDF reading, and sentence splitting.
#### TextPdfUtils.detect_language(text)
- Human readable hint: Detect whether text is English or German using stopword counts.

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

#### read_pdf_file(file_path, max_pages)
- Human readable hint: Read PDF text and return a single combined string (optionally capped by max_pages).

#### read_pdf_pages(file_path, max_pages)
- Human readable hint: Read PDF text and return a list of page-level strings (optionally capped).

#### split_text_into_sentences(text, language)
- Human readable hint: Split text into sentences using NLTK.

## pipeline/integrations/llm_client.py

### Class OpenAIResponder
- Human readable hint: Generate responses using the OpenAI API within a RAG workflow.
- __init__ parameters: data, model, prompt_template, client
#### OpenAIResponder.__init__(data, model, prompt_template, client)
- Human readable hint: No docstring provided.

#### OpenAIResponder._request_kwargs()
- Human readable hint: human readable hint: build one consistent chat request payload for sync and async calls.

#### OpenAIResponder._usage_to_dict(usage)
- Human readable hint: human readable hint: normalize provider usage objects into plain dictionaries.

#### OpenAIResponder._response_to_tuple(response)
- Human readable hint: human readable hint: parse one response object and return content plus usage metadata.

#### OpenAIResponder._is_retryable_error(exc)
- Human readable hint: human readable hint: retry only on transient transport/rate-limit provider failures.

#### OpenAIResponder.generate_response(retries, backoff_seconds)
- Human readable hint: Get one response from the model and return text plus usage metadata.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### _format_prompt(template, data)
- Human readable hint: Insert the evidence text into the prompt template.

## pipeline/selection/chunking.py

### Class ChunkBuilder
- Human readable hint: human readable hint: one class that groups all chunk-building methods for title/abstract and full-text.
#### ChunkBuilder.clean_text(value)
- Human readable hint: Trim whitespace from text fields safely.

#### ChunkBuilder.chunk_sentence_entries(entries, chunk_size, overlap_size)
- Human readable hint: Group sentence entries into overlapping chunks with page/line spans.

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
- Human readable hint: No docstring provided.

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
- Human readable hint: No docstring provided.

#### RelevanceSelector._score_vectors(vectors)
- Human readable hint: Compute relevance scores for each vector.

#### RelevanceSelector.select(chunks, top_k, score_threshold)
- Human readable hint: human readable hint: score only candidate chunks; always-include kinds bypass embedding for speed.

### Class SelectionEngine
- Human readable hint: human readable hint: dominant selector class that owns embedding and relevance-scoring setup for one script.
- __init__ parameters: examples, batch_size, always_include_kinds, embedder
#### SelectionEngine.__init__(examples, batch_size, always_include_kinds, embedder)
- Human readable hint: human readable hint: __init__ stores examples and prepares the underlying selector with one consistent interface.

#### SelectionEngine.select(chunks, top_k, score_threshold)
- Human readable hint: human readable hint: return selected chunks and scores using the configured embedding+relevance backend.

### Script-level functions
- Human readable hint: compatibility wrappers or helper functions used by the primary class.

#### load_labeled_examples(path)
- Human readable hint: Load POS/NEG training examples used to score relevance.

#### _normalize(vec)
- Human readable hint: Normalize a vector to unit length.
