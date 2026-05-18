"""Audit data-extraction outputs for version drift and reviewer plausibility checks.

Direct run:
    python -m pipeline.additions.extraction_plausibility_audit
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config.user_orchestrator import (
    DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS,
    DATA_EXTRACTION_CONSENSUS_HEADER_ALIASES,
    DATA_EXTRACTION_SCHEMA_FILE,
    PATH_SETTINGS,
)
from pipeline.core.extraction_schema import DynamicExtractionSchema, ExtractionVariable, MISSING_TEXT_VALUES


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_ROOT = ROOT / str(PATH_SETTINGS.get("output_root", "output"))
DEFAULT_CANDIDATE_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "data_extraction"
DEFAULT_BASELINE_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "data_extraction_v11"
CONSENSUS_FILENAME = "data_extraction_all_papers_for_consensus_comparison.csv"
QUOTE_AUDIT_FILENAME = "data_extraction_all_papers_quote_audit.csv"
HYBRID_RESCUE_AUDIT_FILENAME = "data_extraction_hybrid_rescue_audit.csv"


def _admin_setting(key: str, default: Any) -> Any:
    """human readable hint: administrative export labels are user-editable config, not pipeline constants."""

    if isinstance(DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS, dict):
        return DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS.get(key, default)
    return default


PAPER_ID_COLUMN = str(_admin_setting("paper_id_column", "paper_id"))
TITLE_COLUMN = str(_admin_setting("title_column", "title"))
QUOTE_AUDIT_DOMAIN_COLUMN = str(_admin_setting("quote_audit_domain_column", "domain"))
QUOTE_AUDIT_VARIABLE_COLUMN = str(_admin_setting("quote_audit_variable_column", "variable"))
QUOTE_AUDIT_VALUE_COLUMN = str(_admin_setting("quote_audit_value_column", "ai_value"))
QUOTE_AUDIT_QUOTE_COLUMN = str(_admin_setting("quote_audit_quote_column", "ai_quote"))


def _normal_key(value: str) -> str:
    """human readable hint: compare headers and values across minor spacing and punctuation differences."""

    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _clean_text(value: Any) -> str:
    """human readable hint: keep CSV comparisons stable while preserving reviewer-visible wording."""

    return " ".join(str(value or "").split())


def _is_missing(value: Any) -> bool:
    """human readable hint: treat configured missing-value spellings consistently across reports."""

    text = _clean_text(value)
    return not text or text.casefold() in {item.casefold() for item in MISSING_TEXT_VALUES}


def _short(value: Any, limit: int = 260) -> str:
    """human readable hint: keep audit rows readable in spreadsheet cells."""

    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    """human readable hint: load CSVs with BOM-tolerant UTF-8 for Excel-generated files."""

    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def _write_csv(path: Path, rows: list[dict[str, Any]], headers: list[str]) -> None:
    """human readable hint: write stable empty reports as well as populated audit tables."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


@dataclass(frozen=True)
class SchemaVariableRef:
    """human readable hint: bind a schema variable to the reviewer-facing CSV column when available."""

    variable: ExtractionVariable
    value_column: str | None

    @property
    def field_path(self) -> str:
        return f"{self.variable.domain}.{self.variable.variable_name}"


class ConsensusTable:
    """human readable hint: represent one reviewer-facing wide data-extraction table."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.rows, self.headers = _read_csv(path)
        self.header_by_key = {_normal_key(header): header for header in self.headers}
        self.paper_id_column = self._resolve_header(PAPER_ID_COLUMN, "paper_id") or ""
        self.title_column = self._resolve_header(TITLE_COLUMN, "title") or ""
        self.rows_by_id = self._index_rows()

    def _resolve_header(self, *candidates: str) -> str | None:
        """human readable hint: resolve configured headers without hardcoding export-system labels."""

        for candidate in candidates:
            header = self.header_by_key.get(_normal_key(candidate))
            if header:
                return header
        return None

    def _index_rows(self) -> dict[str, dict[str, str]]:
        """human readable hint: use paper IDs as the join key for version comparisons."""

        if not self.paper_id_column:
            return {}
        indexed: dict[str, dict[str, str]] = {}
        for row in self.rows:
            paper_id = _clean_text(row.get(self.paper_id_column)).lstrip("#")
            if paper_id:
                indexed[paper_id] = row
        return indexed

    def title_for(self, paper_id: str) -> str:
        """human readable hint: attach titles to audit findings for faster human scanning."""

        row = self.rows_by_id.get(paper_id, {})
        return _clean_text(row.get(self.title_column))

    def value(self, paper_id: str, header: str | None) -> str:
        """human readable hint: read a value only when the schema column exists in this export."""

        if not header:
            return ""
        return _clean_text(self.rows_by_id.get(paper_id, {}).get(header))


class SchemaColumnResolver:
    """human readable hint: map schema variables to wide-table columns through schema labels plus user aliases."""

    def __init__(self, schema: DynamicExtractionSchema, table: ConsensusTable) -> None:
        self.schema = schema
        self.table = table
        self.refs = [SchemaVariableRef(variable, self._value_column(variable)) for variable in schema.variables]

    def _value_column(self, variable: ExtractionVariable) -> str | None:
        candidates = [
            variable.consensus_column_name,
            variable.variable_name,
            f"{variable.domain}.{variable.variable_name}",
        ]
        candidates.extend(DATA_EXTRACTION_CONSENSUS_HEADER_ALIASES.get(f"{variable.domain}.{variable.variable_name}", []))
        for candidate in candidates:
            header = self.table.header_by_key.get(_normal_key(candidate))
            if header:
                return header
        return None


class QuoteAuditTable:
    """human readable hint: provide long-format value and quote lookups for every schema variable."""

    def __init__(self, output_dir: Path) -> None:
        self.path = output_dir / QUOTE_AUDIT_FILENAME
        self.rows, self.headers = _read_csv(self.path)
        self.index = self._build_index()

    def _build_index(self) -> dict[tuple[str, str], dict[str, str]]:
        """human readable hint: the quote audit links a paper ID to a schema domain.variable key."""

        indexed: dict[tuple[str, str], dict[str, str]] = {}
        for row in self.rows:
            paper_id = _clean_text(row.get(PAPER_ID_COLUMN) or row.get("paper_id")).lstrip("#")
            domain = _clean_text(row.get(QUOTE_AUDIT_DOMAIN_COLUMN))
            variable = _clean_text(row.get(QUOTE_AUDIT_VARIABLE_COLUMN))
            if paper_id and domain and variable:
                indexed[(paper_id, f"{domain}.{variable}")] = row
        return indexed

    def value(self, paper_id: str, field_path: str) -> str:
        """human readable hint: prefer the long quote audit as the complete extracted-value source."""

        row = self.index.get((paper_id, field_path), {})
        return _clean_text(row.get(QUOTE_AUDIT_VALUE_COLUMN))

    def quote(self, paper_id: str, field_path: str) -> str:
        """human readable hint: quotes are the strongest quick check for reviewer trust."""

        row = self.index.get((paper_id, field_path), {})
        return _clean_text(row.get(QUOTE_AUDIT_QUOTE_COLUMN))


class HybridRescueAuditTable:
    """human readable hint: expose optional semantic rescue changes inside plausibility reports."""

    def __init__(self, output_dir: Path) -> None:
        self.path = output_dir / HYBRID_RESCUE_AUDIT_FILENAME
        self.rows, self.headers = _read_csv(self.path)
        self.index = self._build_index()

    def _build_index(self) -> dict[tuple[str, str], dict[str, str]]:
        indexed: dict[tuple[str, str], dict[str, str]] = {}
        for row in self.rows:
            paper_id = _clean_text(row.get("paper_id")).lstrip("#")
            variable = _clean_text(row.get("variable"))
            if paper_id and variable:
                indexed[(paper_id, variable)] = row
        return indexed

    def changed_rows(self) -> list[dict[str, str]]:
        """human readable hint: list only rows where the semantic second opinion became the selected value."""

        changed: list[dict[str, str]] = []
        for row in self.rows:
            if _clean_text(row.get("evidence_mode_used")) != "semantic_rescue":
                continue
            primary_value = _clean_text(row.get("primary_full_text_value"))
            selected_value = _clean_text(row.get("selected_value"))
            primary_quote = _clean_text(row.get("primary_full_text_quote"))
            selected_quote = _clean_text(row.get("selected_quote"))
            if primary_value != selected_value or primary_quote != selected_quote:
                changed.append(row)
        return changed


@dataclass(frozen=True)
class TraceEvidence:
    """human readable hint: summarize the trace evidence attached to a missing extracted field."""

    trace_path: Path | None = None
    normalized_text_path: Path | None = None
    hit_count: int = 0
    first_hit: str = ""


class InputTraceIndex:
    """human readable hint: parse generated input traces so missing values can be checked against evidence hints."""

    def __init__(self, output_dir: Path) -> None:
        self.trace_dir = output_dir / "input_traces"
        self.evidence_by_key: dict[tuple[str, str], TraceEvidence] = {}
        self._load()

    def evidence(self, paper_id: str, field_path: str) -> TraceEvidence:
        """human readable hint: return an empty evidence object when no trace exists."""

        return self.evidence_by_key.get((paper_id, field_path), TraceEvidence())

    def _load(self) -> None:
        """human readable hint: scan all per-paper trace reports produced by input_trace.py."""

        if not self.trace_dir.exists():
            return
        for trace_path in sorted(self.trace_dir.glob("data_extraction_*_input_trace.txt")):
            self._parse_trace(trace_path)

    def _parse_trace(self, trace_path: Path) -> None:
        """human readable hint: only the missing-field evidence section is needed for plausibility triage."""

        text = trace_path.read_text(encoding="utf-8", errors="replace")
        paper_match = re.search(r"^paper_id:\s*#?(.+?)\s*$", text, flags=re.MULTILINE)
        paper_id = _clean_text(paper_match.group(1)).lstrip("#") if paper_match else ""
        if not paper_id:
            return
        normalized_match = re.search(r"^normalized_text_path:\s*(.+?)\s*$", text, flags=re.MULTILINE)
        normalized_path = Path(_clean_text(normalized_match.group(1))) if normalized_match else None
        section_match = re.search(
            r"=== Evidence Hint Search For Missing Fields ===\s*(.*?)(?:\n=== |\Z)",
            text,
            flags=re.DOTALL,
        )
        if not section_match:
            return
        section = section_match.group(1)
        blocks = re.split(r"\n(?=\[[^\]\n]+\]\s+search_terms=)", section)
        for block in blocks:
            header_match = re.match(r"\[([^\]\n]+)\]\s+search_terms=", block.strip())
            if not header_match:
                continue
            field_path = _clean_text(header_match.group(1))
            raw_hits = re.findall(r"^\s+L\d+:\s*(.+)$", block, flags=re.MULTILINE)
            hits = [hit for hit in raw_hits if self._is_informative_hit(hit)]
            first_hit = _short(hits[0], 500) if hits else ""
            self.evidence_by_key[(paper_id, field_path)] = TraceEvidence(
                trace_path=trace_path,
                normalized_text_path=normalized_path,
                hit_count=len(hits),
                first_hit=first_hit,
            )

    @staticmethod
    def _is_informative_hit(value: str) -> bool:
        """human readable hint: discard trace matches that are likely headers, legal text, or tables of contents."""

        text = _clean_text(value)
        lowered = text.casefold()
        if not text:
            return False
        if re.search(r"(?:\.\s*){5,}", text):
            return False
        weak_phrases = (
            '"title":',
            "all rights reserved",
            "creative commons",
            "public domain dedication",
            "provided you give appropriate credit",
            "open access",
        )
        return not any(phrase in lowered for phrase in weak_phrases)


class VersionComparator:
    """human readable hint: compare current and baseline wide exports without judging topic meaning."""

    def __init__(
        self,
        candidate: ConsensusTable,
        baseline: ConsensusTable,
        schema_refs: list[SchemaVariableRef],
        candidate_quotes: QuoteAuditTable,
        baseline_quotes: QuoteAuditTable,
    ) -> None:
        self.candidate = candidate
        self.baseline = baseline
        self.schema_refs = schema_refs
        self.candidate_quotes = candidate_quotes
        self.baseline_quotes = baseline_quotes

    def rows(self) -> list[dict[str, str]]:
        """human readable hint: emit one row per changed paper-variable pair."""

        rows: list[dict[str, str]] = []
        shared_ids = sorted(set(self.candidate.rows_by_id) & set(self.baseline.rows_by_id), key=lambda item: (len(item), item))
        for paper_id in shared_ids:
            for ref in self.schema_refs:
                if not ref.value_column:
                    continue
                candidate_value = self.candidate.value(paper_id, ref.value_column)
                baseline_value = self.baseline.value(paper_id, ref.value_column)
                if candidate_value == baseline_value:
                    continue
                rows.append(
                    {
                        "paper_id": paper_id,
                        "title": self.candidate.title_for(paper_id) or self.baseline.title_for(paper_id),
                        "domain": ref.variable.domain,
                        "variable": ref.variable.variable_name,
                        "consensus_column": ref.value_column,
                        "change_type": self._change_type(baseline_value, candidate_value),
                        "baseline_value": baseline_value,
                        "candidate_value": candidate_value,
                        "baseline_quote": self.baseline_quotes.quote(paper_id, ref.field_path),
                        "candidate_quote": self.candidate_quotes.quote(paper_id, ref.field_path),
                    }
                )
        return rows

    @staticmethod
    def _change_type(baseline_value: str, candidate_value: str) -> str:
        """human readable hint: distinguish regressions from recoveries for fast QC prioritization."""

        baseline_missing = _is_missing(baseline_value)
        candidate_missing = _is_missing(candidate_value)
        if not baseline_missing and candidate_missing:
            return "present_to_missing"
        if baseline_missing and not candidate_missing:
            return "missing_to_present"
        if not baseline_missing and not candidate_missing:
            return "changed_present_value"
        return "changed_missing_text"


class PlausibilityAuditor:
    """human readable hint: run generic checks that help a reviewer find likely extraction mistakes."""

    def __init__(
        self,
        candidate: ConsensusTable,
        baseline: ConsensusTable,
        schema_refs: list[SchemaVariableRef],
        candidate_quotes: QuoteAuditTable,
        hybrid_rescue: HybridRescueAuditTable,
        trace_index: InputTraceIndex,
        version_diff_rows: list[dict[str, str]],
    ) -> None:
        self.candidate = candidate
        self.baseline = baseline
        self.schema_refs = schema_refs
        self.candidate_quotes = candidate_quotes
        self.hybrid_rescue = hybrid_rescue
        self.trace_index = trace_index
        self.version_diff_rows = version_diff_rows

    def flags(self) -> list[dict[str, str]]:
        """human readable hint: combine version drift, schema contracts, quotes, and trace evidence."""

        flags: list[dict[str, str]] = []
        flags.extend(self._version_flags())
        flags.extend(self._schema_contract_flags())
        flags.extend(self._evidence_source_flags())
        flags.extend(self._quote_flags())
        flags.extend(self._trace_evidence_flags())
        flags.extend(self._population_count_flags())
        flags.extend(self._hybrid_rescue_flags())
        return sorted(flags, key=lambda row: (self._severity_rank(row["severity"]), row["paper_id"], row["domain"], row["variable"]))

    def _base_row(
        self,
        *,
        severity: str,
        check: str,
        paper_id: str,
        ref: SchemaVariableRef,
        details: str,
        value: str = "",
        quote: str = "",
        evidence: TraceEvidence | None = None,
        baseline_value: str = "",
        candidate_value: str = "",
    ) -> dict[str, str]:
        """human readable hint: use one stable row shape for every plausibility finding."""

        evidence = evidence or TraceEvidence()
        return {
            "severity": severity,
            "check": check,
            "paper_id": paper_id,
            "title": self.candidate.title_for(paper_id) or self.baseline.title_for(paper_id),
            "domain": ref.variable.domain,
            "variable": ref.variable.variable_name,
            "consensus_column": ref.value_column or ref.variable.consensus_column_name,
            "value": value,
            "quote": quote,
            "details": details,
            "trace_path": str(evidence.trace_path or ""),
            "normalized_text_path": str(evidence.normalized_text_path or ""),
            "evidence_excerpt": evidence.first_hit,
            "baseline_value": baseline_value,
            "candidate_value": candidate_value,
        }

    def _version_flags(self) -> list[dict[str, str]]:
        """human readable hint: present-to-missing flips are likely regressions needing human review."""

        by_field = {ref.field_path: ref for ref in self.schema_refs}
        sample_ref = self._find_ref(["sample", "size"])
        sample_domain = sample_ref.variable.domain if sample_ref else ""
        flags: list[dict[str, str]] = []
        for row in self.version_diff_rows:
            ref = by_field.get(f"{row['domain']}.{row['variable']}")
            if not ref:
                continue
            change_type = row["change_type"]
            if change_type == "present_to_missing":
                severity = "high"
                details = "Candidate lost a non-missing value that existed in the baseline output."
            elif change_type == "missing_to_present":
                severity = "info"
                details = "Candidate recovered a value that was missing in the baseline output."
            else:
                if not self._changed_present_value_needs_flag(ref, sample_domain):
                    continue
                severity = "review"
                details = "Candidate and baseline both contain values, but the wording or denominator changed."
            flags.append(
                self._base_row(
                    severity=severity,
                    check=f"version_{change_type}",
                    paper_id=row["paper_id"],
                    ref=ref,
                    value=row["candidate_value"],
                    quote=row["candidate_quote"],
                    details=details,
                    baseline_value=row["baseline_value"],
                    candidate_value=row["candidate_value"],
                )
            )
        return flags

    def _changed_present_value_needs_flag(self, ref: SchemaVariableRef, sample_domain: str) -> bool:
        """human readable hint: keep verbose wording changes in the diff table and flag compact contract fields."""

        if ref.variable.domain == sample_domain:
            return True
        if ref.variable.variable_type.casefold() in {"integer", "boolean"}:
            return True
        return bool(self._allowed_option_values(ref.variable.allowed_options))

    def _schema_contract_flags(self) -> list[dict[str, str]]:
        """human readable hint: validate values against simple schema-level machine contracts."""

        flags: list[dict[str, str]] = []
        for paper_id in sorted(self.candidate.rows_by_id, key=lambda item: (len(item), item)):
            for ref in self.schema_refs:
                value = self._candidate_value(paper_id, ref)
                if _is_missing(value):
                    continue
                variable_type = ref.variable.variable_type.casefold()
                if variable_type == "integer" and not re.search(r"\d+", value):
                    flags.append(
                        self._base_row(
                            severity="high",
                            check="schema_integer_not_numeric",
                            paper_id=paper_id,
                            ref=ref,
                            value=value,
                            details="Schema declares an integer, but the exported value has no parseable number.",
                        )
                    )
                if variable_type == "boolean" and _normal_key(value) not in {"true", "false", "yes", "no"}:
                    flags.append(
                        self._base_row(
                            severity="review",
                            check="schema_boolean_unexpected_value",
                            paper_id=paper_id,
                            ref=ref,
                            value=value,
                            details="Schema declares a boolean, but the exported value is not a common boolean spelling.",
                        )
                    )
                allowed_options = self._allowed_option_values(ref.variable.allowed_options)
                if allowed_options and _normal_key(value) not in {_normal_key(item) for item in allowed_options}:
                    flags.append(
                        self._base_row(
                            severity="review",
                            check="schema_allowed_option_unmatched",
                            paper_id=paper_id,
                            ref=ref,
                            value=value,
                            details="Value does not exactly match one of the schema allowed options.",
                        )
                    )
        return flags

    def _evidence_source_flags(self) -> list[dict[str, str]]:
        """human readable hint: evidence-source fields should describe source type, not an article section."""

        flags: list[dict[str, str]] = []
        section_markers = {
            "abstract",
            "methods",
            "methods section",
            "design setting and participants",
            "recruitment",
            "eligibility",
            "setting",
            "participants",
            "results section",
            "discussion",
        }
        normalized_markers = {_normal_key(marker) for marker in section_markers}
        refs = [ref for ref in self.schema_refs if "evidencesource" in _normal_key(ref.variable.variable_name)]
        for paper_id in sorted(self.candidate.rows_by_id, key=lambda item: (len(item), item)):
            for ref in refs:
                value = self._candidate_value(paper_id, ref)
                if _is_missing(value):
                    continue
                normalized = _normal_key(value)
                if normalized in normalized_markers or any(normalized.startswith(marker) for marker in normalized_markers):
                    flags.append(
                        self._base_row(
                            severity="review",
                            check="evidence_source_looks_like_article_section",
                            paper_id=paper_id,
                            ref=ref,
                            value=value,
                            details="The schema expects publication/source type, but this value looks like an article-internal section name.",
                        )
                    )
        return flags

    def _quote_flags(self) -> list[dict[str, str]]:
        """human readable hint: values without supporting quotes are hard for reviewers to trust."""

        flags: list[dict[str, str]] = []
        for paper_id in sorted(self.candidate.rows_by_id, key=lambda item: (len(item), item)):
            for ref in self.schema_refs:
                value = self._candidate_value(paper_id, ref)
                quote = self.candidate_quotes.quote(paper_id, ref.field_path)
                if not _is_missing(value) and not quote:
                    flags.append(
                        self._base_row(
                            severity="review",
                            check="present_value_without_quote",
                            paper_id=paper_id,
                            ref=ref,
                            value=value,
                            details="Candidate exported a value but the quote audit has no supporting quote.",
                        )
                    )
                if _is_missing(value) and quote:
                    flags.append(
                        self._base_row(
                            severity="review",
                            check="missing_value_with_quote",
                            paper_id=paper_id,
                            ref=ref,
                            quote=quote,
                            details="Candidate exported a missing value while the quote audit contains evidence text.",
                        )
                    )
        return flags

    def _trace_evidence_flags(self) -> list[dict[str, str]]:
        """human readable hint: a missing value with trace evidence is a high-yield recheck for the reviewer."""

        flags: list[dict[str, str]] = []
        for paper_id in sorted(self.candidate.rows_by_id, key=lambda item: (len(item), item)):
            for ref in self.schema_refs:
                value = self._candidate_value(paper_id, ref)
                if not _is_missing(value):
                    continue
                evidence = self.trace_index.evidence(paper_id, ref.field_path)
                if evidence.hit_count <= 0:
                    continue
                baseline_value = self.baseline.value(paper_id, ref.value_column)
                severity = "high" if not _is_missing(baseline_value) else "review"
                flags.append(
                    self._base_row(
                        severity=severity,
                        check="missing_value_with_trace_evidence",
                        paper_id=paper_id,
                        ref=ref,
                        value=value,
                        evidence=evidence,
                        details=f"Input trace found {evidence.hit_count} schema-derived evidence hit(s) for a missing field.",
                        baseline_value=baseline_value,
                        candidate_value=value,
                    )
                )
        return flags

    def _population_count_flags(self) -> list[dict[str, str]]:
        """human readable hint: compare population denominators and extracted counts with the sample-size field."""

        sample_ref = self._find_ref(["sample", "size"])
        if not sample_ref:
            return []
        flags: list[dict[str, str]] = []
        population_refs = [ref for ref in self.schema_refs if ref.variable.domain == sample_ref.variable.domain and ref != sample_ref]
        for paper_id in sorted(self.candidate.rows_by_id, key=lambda item: (len(item), item)):
            sample_size = self._first_integer(self._candidate_value(paper_id, sample_ref))
            if sample_size is None or sample_size <= 0:
                continue
            for ref in population_refs:
                value = self._candidate_value(paper_id, ref)
                quote = self.candidate_quotes.quote(paper_id, ref.field_path)
                combined = f"{value} {quote}"
                denominators = sorted(set(self._ratio_denominators(combined)))
                if denominators and sample_size not in denominators:
                    flags.append(
                        self._base_row(
                            severity="high",
                            check="population_denominator_differs_from_sample_size",
                            paper_id=paper_id,
                            ref=ref,
                            value=value,
                            quote=quote,
                            details=f"Extracted sample size is {sample_size}, but this field cites denominator(s): {', '.join(map(str, denominators))}.",
                        )
                    )
                counts = self._labeled_counts(combined)
                if len(counts) >= 2 and sum(counts) > sample_size:
                    flags.append(
                        self._base_row(
                            severity="review",
                            check="population_counts_exceed_sample_size",
                            paper_id=paper_id,
                            ref=ref,
                            value=value,
                            quote=quote,
                            details=f"Extracted sample size is {sample_size}, but visible category counts sum to {sum(counts)}.",
                        )
                    )
        return flags

    def _hybrid_rescue_flags(self) -> list[dict[str, str]]:
        """human readable hint: show reviewers when semantic rescue changed a primary full-text value."""

        by_field = {ref.field_path: ref for ref in self.schema_refs}
        flags: list[dict[str, str]] = []
        for row in self.hybrid_rescue.changed_rows():
            paper_id = _clean_text(row.get("paper_id")).lstrip("#")
            variable = _clean_text(row.get("variable"))
            ref = by_field.get(variable)
            if not paper_id or ref is None:
                continue
            selected_value = _clean_text(row.get("selected_value"))
            selected_quote = _clean_text(row.get("selected_quote"))
            flags.append(
                self._base_row(
                    severity="review",
                    check="hybrid_rescue_changed_primary_value",
                    paper_id=paper_id,
                    ref=ref,
                    value=selected_value,
                    quote=selected_quote,
                    details=(
                        "Hybrid rescue selected semantic evidence over the primary full-text value. "
                        f"Reason: {_clean_text(row.get('selection_reason'))}. "
                        f"Primary value: {_short(row.get('primary_full_text_value'))}. "
                        f"Semantic rescue value: {_short(row.get('semantic_rescue_value'))}."
                    ),
                    candidate_value=selected_value,
                )
            )
        return flags

    def _candidate_value(self, paper_id: str, ref: SchemaVariableRef) -> str:
        """human readable hint: use quote-audit values first because they cover every schema variable."""

        audit_value = self.candidate_quotes.value(paper_id, ref.field_path)
        if audit_value:
            return audit_value
        return self.candidate.value(paper_id, ref.value_column)

    def _find_ref(self, required_tokens: list[str]) -> SchemaVariableRef | None:
        """human readable hint: locate generic population variables by schema names rather than export headers."""

        for ref in self.schema_refs:
            haystack = _normal_key(f"{ref.variable.variable_name} {ref.variable.consensus_column_name}")
            if all(token in haystack for token in required_tokens):
                return ref
        return None

    @staticmethod
    def _first_integer(value: str) -> int | None:
        """human readable hint: parse the first visible integer from a human-readable value."""

        match = re.search(r"\d+", value or "")
        return int(match.group(0)) if match else None

    @staticmethod
    def _ratio_denominators(value: str) -> list[int]:
        """human readable hint: identify denominators in compact values such as 25/36."""

        return [int(match.group(1)) for match in re.finditer(r"/\s*(\d+)", value or "")]

    @staticmethod
    def _labeled_counts(value: str) -> list[int]:
        """human readable hint: collect category counts while avoiding percentages and ratio denominators."""

        counts: list[int] = []
        patterns = [
            r"\b(?:n|no\.?|count)\s*[=:]\s*(\d+)\b",
            r"\b[A-Za-z][A-Za-z \-/]{0,30}\s*:\s*(\d+)\b",
            r"\b(\d+)\s*\(\s*\d+(?:\.\d+)?\s*%\s*\)",
        ]
        for pattern in patterns:
            counts.extend(int(match.group(1)) for match in re.finditer(pattern, value or "", flags=re.IGNORECASE))
        return counts

    @staticmethod
    def _severity_rank(severity: str) -> int:
        """human readable hint: sort high-priority reviewer checks first."""

        return {"high": 0, "review": 1, "info": 2}.get(severity, 3)

    @staticmethod
    def _allowed_option_values(value: Any) -> list[str]:
        """human readable hint: support schema options already parsed as tuples plus raw delimited strings."""

        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        return [item.strip() for item in re.split(r"[;|]", str(value or "")) if item.strip()]


class AuditReportWriter:
    """human readable hint: write spreadsheet reports plus a compact Markdown reviewer summary."""

    DIFF_HEADERS = [
        "paper_id",
        "title",
        "domain",
        "variable",
        "consensus_column",
        "change_type",
        "baseline_value",
        "candidate_value",
        "baseline_quote",
        "candidate_quote",
    ]
    FLAG_HEADERS = [
        "severity",
        "check",
        "paper_id",
        "title",
        "domain",
        "variable",
        "consensus_column",
        "value",
        "quote",
        "details",
        "trace_path",
        "normalized_text_path",
        "evidence_excerpt",
        "baseline_value",
        "candidate_value",
    ]

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir

    def write(self, diff_rows: list[dict[str, str]], flags: list[dict[str, str]]) -> tuple[Path, Path, Path]:
        """human readable hint: place audit artifacts together under the candidate output folder."""

        self.output_dir.mkdir(parents=True, exist_ok=True)
        diff_path = self.output_dir / "data_extraction_version_diff.csv"
        flags_path = self.output_dir / "data_extraction_plausibility_flags.csv"
        summary_path = self.output_dir / "data_extraction_plausibility_summary.md"
        _write_csv(diff_path, diff_rows, self.DIFF_HEADERS)
        _write_csv(flags_path, flags, self.FLAG_HEADERS)
        summary_path.write_text(self._summary_text(diff_rows, flags), encoding="utf-8")
        return diff_path, flags_path, summary_path

    def _summary_text(self, diff_rows: list[dict[str, str]], flags: list[dict[str, str]]) -> str:
        """human readable hint: summarize counts and top review queues without replacing spreadsheet detail."""

        change_counts = Counter(row["change_type"] for row in diff_rows)
        check_counts = Counter(row["check"] for row in flags)
        severity_counts = Counter(row["severity"] for row in flags)
        lines = [
            "# Data Extraction Plausibility Audit",
            "",
            "## Version Diff",
            f"- Changed paper-variable cells: {len(diff_rows)}",
        ]
        lines.extend(f"- {key}: {count}" for key, count in sorted(change_counts.items()))
        lines.extend(
            [
                "",
                "## Plausibility Flags",
                f"- Total flags: {len(flags)}",
            ]
        )
        lines.extend(f"- {key}: {count}" for key, count in sorted(severity_counts.items()))
        lines.extend(["", "## Checks"])
        lines.extend(f"- {key}: {count}" for key, count in sorted(check_counts.items()))
        lines.extend(["", "## Highest-Priority Examples"])
        high_rows = [row for row in flags if row["severity"] == "high"][:15]
        if not high_rows:
            lines.append("- No high-priority flags.")
        for row in high_rows:
            lines.append(
                f"- paper {row['paper_id']} | {row['domain']}.{row['variable']} | {row['check']} | {row['details']}"
            )
        lines.extend(
            [
                "",
                "## Suggested Human QC Use",
                "- Start with high-severity present-to-missing and denominator flags.",
                "- Use the trace and normalized-text paths in the flags CSV to verify whether the candidate value, baseline value, or neither is evidence-backed.",
                "- If `hybrid_rescue_changed_primary_value` appears, compare the primary full-text quote and semantic rescue quote before accepting the selected value.",
                "- Use the full version diff CSV for wording-only or interpretation changes that were not promoted to plausibility flags.",
                "",
                "## Generic Plausibility Checks Worth Keeping",
                "- Missing value despite schema-derived trace evidence.",
                "- Present value without a quote in the long quote-audit table.",
                "- Integer, boolean, or allowed-option schema contract mismatch.",
                "- Population denominators or visible category counts that conflict with extracted sample size.",
                "- Present-to-missing and missing-to-present flips between output versions.",
            ]
        )
        lines.append("")
        return "\n".join(lines)


class ExtractionPlausibilityAuditRunner:
    """human readable hint: orchestrate loading, comparison, plausibility checks, and report writing."""

    def __init__(
        self,
        candidate_output_dir: Path,
        baseline_output_dir: Path,
        schema_path: Path,
        report_output_dir: Path | None = None,
    ) -> None:
        self.candidate_output_dir = candidate_output_dir
        self.baseline_output_dir = baseline_output_dir
        self.schema_path = schema_path
        self.report_output_dir = report_output_dir or candidate_output_dir / "plausibility_audit"

    def run(self) -> tuple[Path, Path, Path]:
        """human readable hint: produce the three audit artifacts used for reviewer triage."""

        schema = DynamicExtractionSchema.from_kb(self.schema_path)
        candidate = ConsensusTable(self.candidate_output_dir / CONSENSUS_FILENAME)
        baseline = ConsensusTable(self.baseline_output_dir / CONSENSUS_FILENAME)
        refs = SchemaColumnResolver(schema, candidate).refs
        candidate_quotes = QuoteAuditTable(self.candidate_output_dir)
        baseline_quotes = QuoteAuditTable(self.baseline_output_dir)
        hybrid_rescue = HybridRescueAuditTable(self.candidate_output_dir)
        diff_rows = VersionComparator(candidate, baseline, refs, candidate_quotes, baseline_quotes).rows()
        flags = PlausibilityAuditor(
            candidate=candidate,
            baseline=baseline,
            schema_refs=refs,
            candidate_quotes=candidate_quotes,
            hybrid_rescue=hybrid_rescue,
            trace_index=InputTraceIndex(self.candidate_output_dir),
            version_diff_rows=diff_rows,
        ).flags()
        return AuditReportWriter(self.report_output_dir).write(diff_rows, flags)


def build_arg_parser() -> argparse.ArgumentParser:
    """human readable hint: expose paths so the audit works for any stage output version folders."""

    parser = argparse.ArgumentParser(description="Compare data-extraction output versions and flag plausibility issues.")
    parser.add_argument("--candidate-output-dir", type=Path, default=DEFAULT_CANDIDATE_OUTPUT_DIR)
    parser.add_argument("--baseline-output-dir", type=Path, default=DEFAULT_BASELINE_OUTPUT_DIR)
    parser.add_argument("--schema", type=Path, default=Path(DATA_EXTRACTION_SCHEMA_FILE))
    parser.add_argument("--report-output-dir", type=Path, default=None)
    return parser


def main() -> None:
    """human readable hint: command-line entrypoint for focused reviewer-audit runs."""

    args = build_arg_parser().parse_args()
    diff_path, flags_path, summary_path = ExtractionPlausibilityAuditRunner(
        candidate_output_dir=args.candidate_output_dir,
        baseline_output_dir=args.baseline_output_dir,
        schema_path=args.schema,
        report_output_dir=args.report_output_dir,
    ).run()
    print("[audit] data_extraction_plausibility status=completed")
    print(f"[audit] diff={diff_path}")
    print(f"[audit] flags={flags_path}")
    print(f"[audit] summary={summary_path}")


if __name__ == "__main__":
    main()
