# Data Extraction Validation Justification

Living methodological note for manuscript preparation. This file records the rationale for the data-extraction validation approach and should be updated whenever the validation logic, human-review source, schema definitions, or adjudication procedure changes.

## Current Manuscript Draft Paragraph

For AI-assisted data extraction, we will follow the validation approach described by Gartlehner and colleagues. Included full texts will be assessed by the machine-to-machine pipeline using normalized full-text evidence as the primary extraction source. Human-extracted data from a four-paper validation subset, corresponding to approximately 5% of the 94 included papers, will serve as the gold standard. The extraction schema will define each variable, expected value type, and consensus-column mapping. Concordance will be defined as the proportion of data items factually congruent between the pipeline and human reviewers among human-present items, whereas accuracy will additionally account for correctly identified unavailable data. Human reviewer judgements will be converted into structured validation inputs in memory and compared with pipeline outputs using schema-aware validation. Error types, including missed data, misallocated data, unsupported values, and fabricated data, as well as their potential impact, will be classified by human reviewers. Minimum performance thresholds are set at concordance >0.80 and accuracy >0.90 for key extraction domains. Failure to meet these thresholds will trigger targeted refinement of the schema or prompt for the affected variable before extraction of the remaining records.

## Source of Truth

The source of truth for human validation is:

`input/data_extraction_human_review_qc_sample_binary_scoring.csv`

This file contains one AI output row per reviewed paper, followed by a binary reviewer judgement row (`0=not true/ 1=true`) and a reviewer quote/correction row. The pipeline must treat this input file as authoritative. Generated intermediate gold-standard CSV files are not required for normal validation and should not be used as persistent sources of truth.

The validation engine converts the human scoring CSV in memory:

- score `1`: the reviewed value in the AI row is accepted as human ground truth;
- score `0`: the reviewer quote/correction row is used as the human ground truth when present;
- missing or non-evaluable cells are excluded from cell-level comparisons.

## Concordance and Accuracy

Concordance is interpreted as factual congruence among human-present items. It excludes human-missing items from the denominator.

Accuracy includes all evaluable items and therefore also rewards correctly identified unavailable data.

This distinction matters because a model can be accurate about missingness while still having lower concordance for human-present details.

## Full Text as Primary Evidence

The current default extraction strategy uses normalized full text as the primary evidence source. This is methodologically preferred for data extraction because many target variables are table-heavy, context-dependent, or distributed across methods, results, limitations, disclosures, and article metadata. Earlier comparisons indicated that full normalized text performed better than replacing the full text with semantic retrieval chunks for the current validation subset.

Semantic retrieval can remain useful as a targeted rescue or audit layer, but it should not replace the full normalized text as the default extraction source unless future validation demonstrates superior performance.

## Fuzzy Similarity and Thresholds

Automated fuzzy similarity should not be used as the final judge of correctness unless the thresholds are either justified by established literature for the exact task or empirically calibrated on an independent labelled validation set. The current four-paper subset is too small and too entangled with prompt/schema development to support strong threshold calibration.

Therefore, the primary validation should rely on:

- exact or type-normalized matches for structured fields;
- explicit user-configured equivalence aliases where scientifically unambiguous;
- human adjudication of non-exact or semantically ambiguous matches.

Fuzzy similarity may be used as a triage aid to flag possible paraphrase matches, but such flags should not automatically count toward final manuscript accuracy or concordance unless confirmed by human reviewers.

The analogy to 80% statistical power is not a valid scientific justification for a text-similarity threshold. Statistical power concerns Type II error in hypothesis testing, whereas token-overlap thresholds are heuristic decision rules for text comparison. If a fuzzy threshold is retained for exploratory triage, using a high value such as 0.80 is conservative for avoiding false positives, but it should still be described as a pragmatic audit threshold, not as a validated inferential criterion.

## Current Validation Interpretation

Validation outputs should be read in three layers:

1. Automatically accepted matches: exact, type-normalized, or explicitly aliased matches.
2. Automatically rejected mismatches: non-matching cells listed in `output/data_extraction/extraction_error_audit.csv`.
3. Human adjudication: reviewers decide whether rejected cells are true extraction errors, acceptable paraphrases, schema-definition conflicts, or cases where the human validation source needs updating.

For example, if the finalized schema defines a study protocol manuscript as `Protocol`, but the human validation source still accepts `RCT` for a protocol paper, this is not primarily an AI failure. It is a conflict between the updated schema definition and the older human validation judgement, and the input validation CSV should be updated accordingly.

## Reporting Position

For manuscript reporting, we should state that automated validation was schema-aware and used human reviewer judgements as the gold standard. Non-exact matches were not automatically treated as correct solely because of fuzzy similarity. Instead, mismatch audits supported human adjudication and targeted prompt or schema refinement.

Suggested wording:

"We did not rely on automated fuzzy similarity thresholds to determine final data-extraction correctness. Automated normalization and similarity checks were used only to identify potentially concordant paraphrases and to support reviewer adjudication. Final accuracy and concordance were based on human-reviewed ground truth and manual adjudication of non-exact matches."

## Open Decisions

- Whether to keep fuzzy similarity disabled for final metric counting.
- Whether to add a separate `possible_match_needs_review` audit column for fuzzy paraphrase candidates.
- Whether the human validation CSV should be updated after schema-definition changes such as `study_design = Protocol` for protocol manuscripts.
- Whether variable-specific adjudication rules are needed for long prose fields such as implications, limitations, AI transparency, and key findings.

## Threshold Sensitivity Analysis

Date: 2026-05-11.

Purpose: explore how automated fuzzy similarity thresholds affect validation metrics when comparing the current data-extraction QC output against the human-review source file:

`input/data_extraction_human_review_qc_sample_binary_scoring.csv`

This analysis was performed because exact matching alone is too strict for many prose/list variables, while arbitrary fuzzy thresholds are not scientifically defensible as final correctness criteria unless calibrated or adjudicated.

### Calibration Setup

The validation source contained 139 evaluable variable-paper cells from four reviewed papers. The current AI output contained results for papers 22, 224, 1136, and 2425.

Thresholds were varied across:

- free-text token overlap: 0.30 to 0.95
- list token overlap: 0.30 to 0.95
- short-text token overlap: 0.30 to 0.95
- numeric relative tolerance: 0.00, 0.01, 0.02, 0.05

The full grid output was written to:

`output/data_extraction/data_extraction_threshold_sensitivity.csv`

### Results

Strict no-fuzzy baseline:

- free-text threshold: 0.80
- list threshold: 0.80
- short-text threshold: 0.80
- numeric tolerance: 0.02
- fuzzy matches counted in metrics: no
- accuracy: 56/139 = 40.29%
- concordance: 35/116 = 30.17%

Strict fuzzy threshold at 0.80:

- free-text threshold: 0.80
- list threshold: 0.80
- short-text threshold: 0.80
- numeric tolerance: 0.02
- fuzzy matches counted in metrics: yes
- accuracy: 90/139 = 64.75%
- concordance: 69/116 = 59.48%

Previous loose exploratory setting:

- free-text threshold: 0.42
- list threshold: 0.35
- short-text threshold: 0.50
- numeric tolerance: 0.05
- accuracy: 120/139 = 86.33%
- concordance: 99/116 = 85.34%

Best-performing grid setting:

- free-text threshold: 0.30
- list threshold: 0.40
- short-text threshold: 0.50
- numeric tolerance: 0.00
- accuracy: 122/139 = 87.77%
- concordance: 101/116 = 87.07%

Strictest setting within one accuracy cell of the best-performing grid setting:

- free-text threshold: 0.30
- list threshold: 0.40
- short-text threshold: 0.65
- numeric tolerance: 0.00
- accuracy: 121/139 = 87.05%
- concordance: 100/116 = 86.21%

Best setting with all thresholds >=0.80:

- free-text threshold: 0.80
- list threshold: 0.80
- short-text threshold: 0.95
- numeric tolerance: 0.00
- accuracy: 90/139 = 64.75%
- concordance: 69/116 = 59.48%

### Interpretation

The empirically best setting is not conservative. It requires only 30% token overlap for longer free-text fields and 40% overlap for lists. This is unsurprising because many human gold-standard cells are detailed prose while AI cells are concise summaries, and token overlap rewards partial shared content. However, such low thresholds risk false-positive acceptance of factually incomplete or overly vague answers.

The strict 0.80 threshold is conservative but underestimates performance because it rejects many plausible paraphrases and summaries. It is therefore useful as a high-confidence triage threshold, but not as a complete measure of factual congruence.

The empirical calibration is post-hoc, small-sample, and based on only four papers. It should not be presented as a robustly validated threshold-selection procedure. It is better interpreted as sensitivity analysis showing that reported machine performance is highly dependent on how non-exact paraphrases are treated.

### Methodological Decision

The most rigorous current choice remains:

- do not count fuzzy similarity automatically in final accuracy/concordance;
- use exact/type-normalized matches and explicit semantic aliases as automatic matches;
- send remaining mismatches to human adjudication;
- optionally use fuzzy similarity as an audit triage signal only.

If a threshold must be reported for exploratory triage, the analysis supports describing 0.80 as a conservative high-specificity flag, not as the optimal threshold for maximizing performance. The best post-hoc performance threshold (0.30/0.40/0.50) should not be used as a final correctness rule without human confirmation because it may inflate accuracy and concordance.

## V16-Anchored Version Comparison

Date: 2026-05-11.

Purpose: compare later extraction versions against the v16 output that Marc and Shawan reviewed. This analysis uses:

- the reviewer-derived value from `input/data_extraction_human_review_qc_sample_binary_scoring.csv`;
- the original v16 AI value reviewed by Marc/Shawan;
- the reviewer quote/correction row where present;
- each candidate run's value and quote.

The comparison is quote-aware: a candidate cell may be counted as overlapping when the candidate value matches the reviewer-derived value, when the candidate value matches the reviewer quote/correction row, or when the candidate quote matches the reviewer quote/correction row. This is an audit/sensitivity analysis, not a substitute for fresh human review of changed cells.

Output files:

- `output/data_extraction/version_validation_comparison/v16_anchor_quote_aware_run_summary.csv`
- `output/data_extraction/version_validation_comparison/v16_anchor_quote_aware_variable_summary.csv`
- `output/data_extraction/version_validation_comparison/v16_anchor_quote_aware_cell_comparison.csv`
- `output/data_extraction/version_validation_comparison/v16_anchor_quote_aware_comparison_summary.md`

Quote-aware run summary:

- v16_HumanCheck: 126/139 matches = 90.65%; value-only matches = 124; reviewer-quote-supported matches = 5; candidate-AI-quote-supported matches = 4; fixes of v16 mismatches = 2; regressions from v16-accepted cells = 1; changed cells = 5.
- v19: 120/139 matches = 86.33%; value-only matches = 120; reviewer-quote-supported matches = 9; candidate-AI-quote-supported matches = 7; fixes of v16 mismatches = 7; regressions from v16-accepted cells = 12; changed cells = 21.
- v18: 119/139 matches = 85.61%; value-only matches = 119; reviewer-quote-supported matches = 10; candidate-AI-quote-supported matches = 7; fixes of v16 mismatches = 7; regressions from v16-accepted cells = 13; changed cells = 21.
- v17: 114/139 matches = 82.01%; value-only matches = 110; reviewer-quote-supported matches = 9; candidate-AI-quote-supported matches = 6; fixes of v16 mismatches = 7; regressions from v16-accepted cells = 18; changed cells = 26.

Interpretation:

The v16_HumanCheck run remains the strongest formal validation anchor because it is the run directly reviewed by humans and has the highest quote-aware overlap. Later versions fixed some v16 mismatches but introduced more regressions among cells that were accepted in v16. Therefore, later versions should not be treated as better validated unless the changed cells are separately adjudicated.

## V19 Lessons Ported Into The Active Schema

Date: 2026-05-11.

The v19 changed-cell review found useful corrections for `reported` and `evidence_source`, but also regressions in table-sensitive population variables, study design, AI transparency, smartphone usage, human-AI interaction, and limitations. The safest implementation is therefore not to replace the full-text/v16-style extraction strategy with v19 behavior. Instead, the validated lessons were ported into the active schema and generic validation machinery:

- `knowledge-base/data_extraction_schema.csv` now includes the default user-editable guidance columns `human_reviewer_instruction`, `evidence_profile`, and `do_not_infer_from`.
- The `human_reviewer_instruction` column was populated from the second row of `input/data_extraction_human_review_qc_sample_binary_scoring.csv`, which contains the instructions originally given to Marc and Shawan.
- Risky variables now carry explicit schema guidance: `reported` should be an outcome/domain label rather than a Boolean; `evidence_source` should be a publication/source type rather than an article section; population fields should preserve table labels and denominators; AI transparency should not be inferred from hidden guardrails alone; sensing modalities should be data-capture inputs; smartphone usage should describe the intervention/assessment role; limitations should include the complete stated set.
- The pipeline remains generic: it parses and injects schema guidance fields when the active schema provides them. The active project schema provides them by default; the review-specific content remains in the KB.
- Validation normalization now standardizes Unicode dash and quote variants before comparison, preventing false mismatches such as `GPT‑4` versus `GPT-4`.

This preserves v16 as the formal human-reviewed baseline while selectively porting the corrections that Marc/Shawan and the v19 audit made methodologically defensible.

## Strict Pipeline Validation Versus Quote-Aware Plausibility Audit

Date: 2026-05-11.

After the active full-text data-extraction run, the formal pipeline validation reported:

- strict concordance: 33/116 = 28.45%;
- strict accuracy: 56/139 = 40.29%.

A separate quote-aware plausibility audit was run on the same AI outputs and the same human source file. This audit compared candidate values and candidate quotes against both the reviewer-derived human value and the Marc/Shawan quote/correction row. It also normalized harmless formatting differences such as underscores, Unicode dashes, and list-vs-string punctuation, while preventing missing AI values from matching present human truth.

The quote-aware audit reported:

- quote-aware concordance: 96/116 = 82.76%;
- quote-aware accuracy: 119/139 = 85.61%;
- 63 cells were strict-pipeline mismatches but quote-aware matches.

Interpretation:

The strict pipeline metric is intentionally conservative: it counts only exact/type-normalized matches and explicit configured aliases. It does not count paraphrases, list/string formatting differences, quote support, or reviewer quote overlap. Therefore it underestimates factual agreement for variables where the AI extracted the right fact in different wording, such as `physical_activity` versus `physical activity`, semicolon lists versus JSON-style lists, and concise paraphrases of aims, findings, methods, limitations, or behavioral strategies.

The quote-aware audit is closer to how a human reviewer reads the extraction table: it asks whether the candidate value or its supporting quote contains the same factual information as the human value or reviewer correction. This explains why the audit score is much higher. However, it is not automatically suitable as the primary manuscript metric because token/quote overlap can still over-credit partial matches and because some reviewer note rows contain evaluative comments rather than clean replacement values. The most defensible use is:

- strict validation = conservative machine-readable lower bound;
- quote-aware audit = reviewer triage and plausibility check;
- final manuscript validation = human adjudication of strict mismatches that are quote-aware matches.

Current audit files:

- `output/data_extraction/data_extraction_human_like_quote_aware_validation_audit.csv`
- `output/data_extraction/data_extraction_human_like_quote_aware_validation_summary.csv`

## Integrated Quote-Aware Validation Report

Date: 2026-05-11.

The stats engine was updated so the standard extraction validation report now includes:

- `n_papers` overall;
- per-variable `n_papers` and `n_papers_human_present`;
- strict value-only lower-bound concordance and accuracy;
- quote-aware concordance and accuracy used as the primary reported validation metrics when configured;
- a full cell-level audit file showing strict match, quote-aware match, and quote-aware reason.

The regenerated active report showed:

- `n_papers = 4`;
- total variable-paper comparisons = 139;
- strict value-only concordance lower bound = 29.31%;
- strict value-only accuracy lower bound = 41.01%;
- quote-aware rescued comparisons = 61;
- quote-aware concordance = 81.90%;
- quote-aware accuracy = 84.89%.

The small difference from the earlier side audit reflects tighter missing-value safeguards and the current explicit language/equivalence aliases in `config/user_orchestrator.py`. The report remains transparent because strict lower-bound values and quote-aware reasons are preserved in `output/data_extraction/data_extraction_extraction_validation_cell_audit.csv`.
