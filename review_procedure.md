# Review Procedure (End-to-End Overview)

This is the full review workflow, including initial search in scientific databases (e.g., MEDLINE [Pubmed], APA PsychInfo [Ovid], Sportdiscus [Embase], Scopus, Web of Science [all indices], IEEExplore [IEEE], ACM Digital Libary), paid review tool (i.e., Covidence), and the application pipeline in this working directory.

## Open science + sustainability protocol
- Human researchers follow JBI and PRISMA-ScR for the upstream search and screening design.
- Raw citation exports are hashed and archived in a Zotero (Version 7.0.24) repository to preserve static inputs.
- The pipeline runs in a Docker container on university servers and uses [Specific Model Name] (Temperature=XX) with RAG (Chunk size: YYY tokens; Top-K: Z).
- Full system prompts are stored as Supplementary Appendix A when publishing.
- A blinded 10% QC sample is enforced per stage with stop/go thresholds (Sensitivity > 0.95; PABAK > 0.80). If thresholds fail, prompts are refined and QC is rerun.
- CodeCarbon (https://mlco2.github.io/codecarbon/index.html) logs kWh and CO2eq, plus time-savings metadata; per‑token rates are derived from these totals and token counts captured from the API when available (otherwise estimated).
- Consumption parameters can be aligned with the Swiss Supercomputer approach (here) and other local guidance (link).
- Final outputs (code, validated datasets, and AI exclusion reasons) are released under CC-BY-NC-SA.

## Knowledge base setup (mandatory for every stage)
The pipeline uses a stage-specific knowledge base (KB) to select the most relevant evidence chunks before calling the LLM. **You must create and maintain one KB per stage** or the run fails.

**KB file format (all stages)**
- CSV with columns: `label` (POS/NEG), `text` (short evidence), optional `source`.
- Minimum: 1 POS entry. Recommended: ≥10 POS and ≥10 NEG.
- Each `text` should be 1–3 sentences (short, specific, and aligned with inclusion/exclusion rules).

**KB files (required paths)**
- title_abstract → knowledge-base/title_abstract_pos-neg_examples.csv
- full_text → knowledge-base/full_text_pos-neg_examples.csv
- data_extraction → knowledge-base/data_extraction_pos-neg_examples.csv

**How to build each KB (strict, stage-specific)**
- Title/abstract KB: Use **human chosen** title/abstracts from already known papers that are clearly relevant/irrelevant. Select clear POS (include) and NEG (exclude) examples (3–5 papers each). Keep short evidence snippets from titles/abstracts, and synthetically extend (e.g., with an external AI tool) the knowledge base to >25 POS and >25 NEG entries.
- Full_text KB: Use **human QC** full-text decisions (included/excluded). Add POS from included papers and NEG from excluded papers (short excerpts from PDFs or abstracts that justify the decision). The pipeline also auto-adds negatives from *_irrelevant_csv_*.
- Data_extraction KB: Use **human QC** data-extraction decisions. Add POS from included papers and NEG from excluded papers (short excerpts tied to extraction fields). The pipeline also auto-adds negatives from *_excluded_csv_*.

**How to use PDFs to build KB evidence (full_text + data_extraction)**
- Use the per-paper selected chunk files created by the pipeline:
	- full_text: input/per_paper_full_text/<paper>/full_text_selected_chunks.jsonl
	- data_extraction: input/per_paper_data_extraction/<paper>/data_extraction_selected_chunks.jsonl
- Copy short, decisive snippets (1–3 sentences) into the KB `text` field.
- Store provenance in the optional `source` column (e.g., paper_id | page | line | chunk_id).
- Prefer human QC decisions to label POS/NEG; do not label from AI outputs alone.

**Scientific-rigour protocol for KB development (recommended)**
1) **Seed set (pre-QC):** build a small, expert-curated seed with diverse inclusion/exclusion reasons.
	- Title/abstract: 3–5 POS + 3–5 NEG with short, decisive snippets.
	- Full_text: 3–5 POS + 3–5 NEG excerpts of relevant paragraphs from full texts.
	- Data_extraction: 3–5 POS + 3–5 NEG excerpts of information tied to extraction criteria.
2) **Balance & coverage:** ensure all main exclusion reasons are represented at least once.
3) **No leakage:** do not use AI-generated decisions from the same run as training evidence.
4) **QC-based expansion:** after the QC round, replace or expand the seed using human QC decisions only.
5) **Freeze for production:** lock the KB for the 90% run to avoid shifting decision boundaries mid-stage.
6) **Record provenance:** keep a simple log (who selected, source paper ID, rationale) outside the KB file.

**Optional dual-KB approach (rigorous)**
- Keep two KBs per stage: **initial_seed** and **final_qc**.
- Use initial_seed for the QC-only run, then build final_qc from human QC decisions and use it for the full 90%.
- Preserve both for audit and comparison of evidence selection drift.

## 1) Title/Abstract Screening (large set)
1) Search databases and upload all records from the different databases via RIS file to Covidence for automatized dedublication of papers.
2) Export the Covidence screen CSV (`*_screen_csv_*`) into input/.
3) Ensure the title_abstract KB exists and is populated (knowledge-base/title_abstract_pos-neg_examples.csv).
4) Run the pipeline (Windows: .venv\Scripts\python main.py; macOS/Linux: python main.py).
5) QC-only screening happens first; humans screen the same QC sample in Covidence.
6) Export Covidence initial select CSV (`*_select_csv_*`) and irrelevant CSV (`*_irrelevant_csv_*`) into input/.
7) Validation uses only the QC sample list.
8) If satisfied, run full title_abstract screening with the AI; if not, start a new QC round.
9) After full AI screening, record final title/abstract decisions in Covidence.
10) Export select/irrelevant CSVs for the next stage.

## 2) Full-Text Screening (smaller set)
1) Export Covidence final select CSV (`*_select_csv_*`) and irrelevant CSV (`*_irrelevant_csv_*`) into input/.
2) Ensure the full_text KB exists and is populated (knowledge-base/full_text_pos-neg_examples.csv).
3) Run the pipeline to create per-paper folders in input/per_paper_full_text/.
4) Add one related PDF per paper folder.
5) Run the pipeline again for QC-only full_text screening, using irrelevant CSV (`*_irrelevant_csv_*`) as additional negative examples for the knowledge base.
6) Humans review the same QC sample in Covidence.
7) Export Covidence initial included CSV (`*_included_csv_*`) and excluded CSV (`*_excluded_csv_*`) into input/.
8) Validation uses only the QC sample list.
9) If satisfied, run full full_text screening; if not, start a new QC round.
10) Record final full_text decisions in Covidence and export included/excluded CSVs.

## 3) Data Extraction (smallest set)
1) Export Covidence final included CSV (`*_included_csv_*`) and excluded CSV (`*_excluded_csv_*`) into input/.
2) Ensure the data_extraction KB exists and is populated (knowledge-base/data_extraction_pos-neg_examples.csv).
3) Run the pipeline to build input/per_paper_data_extraction/ (PDFs from full_text screening are reused).
4) Run QC-only data_extraction, using a data extraction template.
5) Humans extract the same QC sample in Covidence with data extraction template.
6) Validation compares QC sample only.
7) If satisfied, run full data_extraction; if not, start a new QC round.
8) After satisfaction, take the information filled in the data extraction template and report them in review manuscript for article publication.

## Notes
- QC samples are versioned by timestamp in output/<stage>/.
- Validation outputs include the same timestamp as the AI eligibility file used.
- QC_ENABLED=False skips QC (not recommended for formal reviews).
- CodeCarbon prints INFO/WARNING lines periodically (every tracking interval, default 15s) and once per run; Windows shows estimation warnings unless Intel Power Gadget is installed.
- LLM input per paper combines a static stage prompt (prompt_script) and dynamic evidence (selected chunks). API `prompt_tokens` reflect both; prompt size varies with evidence length.
- Result files are versioned by timestamp in output/<stage>/.
