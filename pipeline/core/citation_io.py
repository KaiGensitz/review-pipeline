"""Generic citation-ingestion bridge for screening-stage CSV exports.

This module converts a user-supplied citation CSV into the generic metadata
shape consumed by the screening pipeline. Export-specific header names are
resolved through ``config/user_orchestrator.py`` via ``metadata_aliases.py``;
no review-topic terms or administrative column names are embedded here.
"""

from __future__ import annotations

import csv
import fnmatch
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.core.metadata_aliases import (
    extract_year_from_metadata,
    metadata_aliases,
    normalize_metadata_row,
    read_metadata_value,
)


CANONICAL_CITATION_FIELDS: tuple[str, ...] = (
    "paper_id",
    "title",
    "abstract",
    "authors",
    "publication_year",
    "publication_month",
    "journal",
    "volume",
    "issue",
    "pages",
    "accession_number",
    "doi",
    "reference",
    "study_id",
    "notes",
    "tags",
)

INGESTION_FLAG_FIELDS: tuple[str, ...] = (
    "citation_ingestion_missing_title",
    "citation_ingestion_missing_abstract",
    "citation_ingestion_missing_doi",
    "citation_ingestion_source_file",
    "citation_ingestion_source_row_number",
)

MISSING_TEXT_MARKERS = {"", "na", "n/a", "not available", "none", "null", "nan"}

STAGE_FILE_PATTERNS: dict[str, dict[str, str]] = {
    "title_abstract": {
        "baseline": "*_screen_csv_*.csv",
        "citation": "citationSearching_title-abstract_*.csv",
        "export_prefix": "citationSearching_title-abstract_novel",
        "label": "Title/Abstract",
    },
    "full_text": {
        "baseline": "*_select_csv_*.csv",
        "citation": "citationSearching_full-text_*.csv",
        "export_prefix": "citationSearching_full-text_novel",
        "label": "Full-Text",
    },
    "data_extraction": {
        "baseline": "*_included_csv_*.csv",
        "citation": "citationSearching_data-extraction_*.csv",
        "export_prefix": "citationSearching_data-extraction_novel",
        "label": "Data Extraction",
    },
}


@dataclass
class CitationDiffAudit:
    """Human-readable counters for one citation-search delta extraction."""

    target_stage: str = ""
    baseline_file: str = ""
    baseline_total_records: int = 0
    citation_file: str = ""
    citation_total_records: int = 0
    old_records_filtered_out: int = 0
    novel_records_for_screening: int = 0


class CovidenceCitationParser:
    """Parse a deduplicated citation CSV into generic screening input records.

    The class name reflects the current upstream export workflow, but the
    implementation is intentionally export-header agnostic. Header mappings are
    read from user-editable metadata aliases, and every output row uses generic
    pipeline keys such as ``paper_id``, ``title``, ``abstract``, and ``doi``.
    """

    def __init__(self) -> None:
        """Initialize an empty parser state."""

        self.records: list[dict[str, Any]] = []
        self.source_path: Path | None = None
        self.ingested_at: datetime | None = None
        self.missing_counts: dict[str, int] = {
            "title": 0,
            "abstract": 0,
            "doi": 0,
        }
        self.audit = CitationDiffAudit()

    def find_target_files(self, input_dir: str, stage: str) -> dict[str, str]:
        """Locate baseline and citation-search exports for one pipeline stage.

        Args:
            input_dir: Directory containing both normal database exports and
                current citation-search exports.
            stage: One of ``title_abstract``, ``full_text``, or
                ``data_extraction``.

        Returns:
            Dictionary with ``baseline`` and ``citation`` file paths.

        Raises:
            ValueError: If the stage is unknown.
            FileNotFoundError: If either required file type is absent.
        """

        if stage not in STAGE_FILE_PATTERNS:
            raise ValueError(f"Unknown citation-ingestion stage: {stage}")

        source_dir = Path(input_dir)
        if not source_dir.exists() or not source_dir.is_dir():
            raise FileNotFoundError(f"Input directory not found: {source_dir}")

        patterns = STAGE_FILE_PATTERNS[stage]

        # human readable hint: route by strict stage-specific filename contracts.
        baseline = self._newest_matching_file(
            source_dir,
            str(patterns["baseline"]),
            exclude_prefix="citationSearching",
        )
        citation = self._newest_matching_file(source_dir, str(patterns["citation"]))
        if baseline is None:
            raise FileNotFoundError(
                f"No baseline export found for stage '{stage}' with pattern {patterns['baseline']!r}."
            )
        if citation is None:
            raise FileNotFoundError(
                f"No citation-search export found for stage '{stage}' with pattern {patterns['citation']!r}."
            )

        return {"baseline": str(baseline), "citation": str(citation)}

    def ingest_covidence_csv(self, filepath: str) -> list[dict[str, Any]]:
        """Read a citation CSV and standardize rows for screening.

        Args:
            filepath: Path to a CSV export whose records have already passed
                upstream deduplication.

        Returns:
            A list of standardized record dictionaries ready to export for the
            title/abstract screening stage.

        Raises:
            FileNotFoundError: If ``filepath`` does not exist.
            ValueError: If the CSV has no parseable header row.
            csv.Error: If the CSV parser encounters malformed input.
        """

        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"Citation CSV not found: {path}")
        if not path.is_file():
            raise ValueError(f"Citation input is not a file: {path}")

        # human readable hint: reset parser state so repeated ingests are deterministic.
        resolved_stage = self._infer_stage_from_paths(path)
        self._reset_state(source_path=path, target_stage=resolved_stage)

        # human readable hint: utf-8-sig accepts ordinary UTF-8 and CSV files with a BOM.
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError(f"Citation CSV has no header row: {path}")

            for row_number, row in enumerate(reader, start=2):
                if row is None:
                    continue
                record = self._standardize_row(row, row_number=row_number, source_path=path)
                self.records.append(record)

        self.audit.citation_file = path.name
        self.audit.citation_total_records = len(self.records)
        self.audit.novel_records_for_screening = len(self.records)
        return list(self.records)

    def ingest_and_diff(
        self,
        current_export_path: str,
        previous_export_path: str,
        stage: str | None = None,
    ) -> list[dict[str, Any]]:
        """Ingest only novel citation-search records by diffing two exports.

        Args:
            current_export_path: Current whole-stage export containing prior
                database records plus citation-search records.
            previous_export_path: Earlier database-only export used as the
                baseline set of already-screened records.
            stage: Optional pipeline stage label for audit logs. If omitted,
                the stage is inferred from filenames when possible.

        Returns:
            Standardized dictionaries for novel records only.

        Raises:
            FileNotFoundError: If either CSV path does not exist.
            ValueError: If either CSV cannot be read as tabular data.
        """

        current_path = Path(current_export_path)
        previous_path = Path(previous_export_path)
        if not current_path.exists() or not current_path.is_file():
            raise FileNotFoundError(f"Citation export not found: {current_path}")
        if not previous_path.exists() or not previous_path.is_file():
            raise FileNotFoundError(f"Baseline export not found: {previous_path}")

        # human readable hint: read both exports as strings so IDs and DOIs stay stable.
        current_df = self._read_csv_frame(current_path)
        previous_df = self._read_csv_frame(previous_path)

        resolved_stage = stage or self._infer_stage_from_paths(current_path, previous_path)
        self._reset_state(source_path=current_path, target_stage=resolved_stage)

        # human readable hint: build prior-record fingerprints from configured metadata aliases.
        seen_ids = self._fingerprint_set(previous_df, "paper_id", self._normalize_identifier)
        seen_dois = self._fingerprint_set(previous_df, "doi", self._normalize_doi)
        seen_titles = self._fingerprint_set(previous_df, "title", self._normalize_title)

        novel_rows: list[dict[str, Any]] = []
        filtered_old_count = 0
        for row_number, row in self._dataframe_rows(current_df):
            row_dict = self._clean_row_dict(row)
            if self._row_seen_before(row_dict, seen_ids, seen_dois, seen_titles):
                filtered_old_count += 1
                continue
            novel_rows.append(row_dict)
            record = self._standardize_row(row_dict, row_number=row_number, source_path=current_path)
            self.records.append(record)

        self.audit = CitationDiffAudit(
            target_stage=resolved_stage,
            baseline_file=previous_path.name,
            baseline_total_records=len(previous_df),
            citation_file=current_path.name,
            citation_total_records=len(current_df),
            old_records_filtered_out=filtered_old_count,
            novel_records_for_screening=len(novel_rows),
        )
        self.ingested_at = datetime.now(timezone.utc)
        return list(self.records)

    def export_for_screening(
        self,
        output_dir: str,
        filename_prefix: str | None = None,
    ) -> dict[str, str]:
        """Write standardized records and an audit log for the screening stage.

        Args:
            output_dir: Directory where the screening-ready files should be
                created.
            filename_prefix: Optional prefix for the CSV/JSONL handoff
                filenames. When omitted, the prefix is derived from the target
                stage and matches the citation-search stage patterns.

        Returns:
            Paths to the written CSV, JSONL, and ingestion-log files.

        Raises:
            RuntimeError: If no records have been ingested.
        """

        if self.source_path is None:
            raise RuntimeError("No citation export has been ingested; call an ingest method first.")

        target_dir = Path(output_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        timestamp = (self.ingested_at or datetime.now(timezone.utc)).strftime("%Y%m%d_%H-%M-%S")
        safe_prefix = self._safe_filename_prefix(filename_prefix or self._default_export_prefix())
        csv_path = target_dir / f"{safe_prefix}_{timestamp}.csv"
        jsonl_path = target_dir / f"{safe_prefix}_records_{timestamp}.jsonl"
        log_path = target_dir / "citation_ingestion_log.txt"

        # human readable hint: emit citation-search files that stay separate from normal stage inputs.
        fieldnames = list(CANONICAL_CITATION_FIELDS) + list(INGESTION_FLAG_FIELDS)
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for record in self.records:
                writer.writerow(self._csv_safe_record(record, fieldnames))

        # human readable hint: JSONL preserves booleans and row-level flags for reproducible audits.
        with jsonl_path.open("w", encoding="utf-8") as handle:
            for record in self.records:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

        # human readable hint: write a compact human-readable audit trail beside the handoff file.
        self._write_ingestion_log(log_path)

        return {
            "csv": str(csv_path),
            "jsonl": str(jsonl_path),
            "log": str(log_path),
        }

    @staticmethod
    def _safe_filename_prefix(value: str) -> str:
        """Return a conservative filename prefix for citation handoff files."""

        cleaned = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in str(value or ""))
        cleaned = cleaned.strip("._-")
        return cleaned or "citationSearching_title-abstract_novel"

    def _standardize_row(self, row: dict[str, Any], *, row_number: int, source_path: Path) -> dict[str, Any]:
        """Map one raw CSV row into generic citation metadata."""

        default_id = f"row-{row_number - 1:05d}"
        normalized = normalize_metadata_row(row, default_id=default_id)

        # human readable hint: resolve publication metadata through configured aliases only.
        record: dict[str, Any] = {
            field: read_metadata_value(normalized, field)
            for field in CANONICAL_CITATION_FIELDS
        }
        record["paper_id"] = record.get("paper_id") or default_id
        record["publication_year"] = record.get("publication_year") or extract_year_from_metadata(normalized)

        # human readable hint: convert common export missing-value markers to real blanks before screening.
        if self._is_missing_marker(record.get("abstract")):
            record["abstract"] = ""
        if self._is_missing_marker(record.get("doi")):
            record["doi"] = ""

        # human readable hint: keep missing critical fields explicit instead of failing during screening.
        missing_title = not bool(str(record.get("title") or "").strip())
        missing_abstract = not bool(str(record.get("abstract") or "").strip())
        missing_doi = not bool(str(record.get("doi") or "").strip())
        if missing_title:
            self.missing_counts["title"] += 1
        if missing_abstract:
            self.missing_counts["abstract"] += 1
        if missing_doi:
            self.missing_counts["doi"] += 1

        record.update(
            {
                "citation_ingestion_missing_title": missing_title,
                "citation_ingestion_missing_abstract": missing_abstract,
                "citation_ingestion_missing_doi": missing_doi,
                "citation_ingestion_source_file": source_path.name,
                "citation_ingestion_source_row_number": row_number,
            }
        )
        return record

    @staticmethod
    def _csv_safe_record(record: dict[str, Any], fieldnames: list[str]) -> dict[str, str]:
        """Convert record values into stable CSV strings."""

        csv_record: dict[str, str] = {}
        for field in fieldnames:
            value = record.get(field, "")
            if isinstance(value, bool):
                csv_record[field] = "true" if value else "false"
            elif value is None:
                csv_record[field] = ""
            else:
                csv_record[field] = str(value)
        return csv_record

    @staticmethod
    def _newest_matching_file(
        source_dir: Path,
        pattern: str,
        *,
        exclude_prefix: str = "",
    ) -> Path | None:
        """Return the newest file matching one strict stage pattern."""

        matches = [
            path
            for path in source_dir.glob(pattern)
            if path.is_file()
            and not (exclude_prefix and path.name.startswith(exclude_prefix))
        ]
        return max(matches, key=lambda path: path.stat().st_mtime) if matches else None

    @staticmethod
    def _read_csv_frame(path: Path) -> pd.DataFrame:
        """Read a CSV as string columns with stable missing-value handling."""

        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        except Exception as exc:
            raise ValueError(f"Could not read CSV file {path}: {exc}") from exc

    def _reset_state(self, *, source_path: Path, target_stage: str = "") -> None:
        """Reset parser state before an ingest or diff operation."""

        self.records = []
        self.source_path = source_path
        self.ingested_at = datetime.now(timezone.utc)
        self.missing_counts = {"title": 0, "abstract": 0, "doi": 0}
        self.audit = CitationDiffAudit(target_stage=target_stage)

    @staticmethod
    def _dataframe_rows(frame: pd.DataFrame) -> list[tuple[int, dict[str, Any]]]:
        """Return CSV-like row numbers and row dictionaries from a DataFrame."""

        return [(int(index) + 2, dict(row)) for index, row in frame.iterrows()]

    @staticmethod
    def _clean_row_dict(row: dict[str, Any]) -> dict[str, str]:
        """Normalize Pandas row values into plain strings."""

        return {str(key): "" if value is None else str(value) for key, value in row.items()}

    def _fingerprint_set(
        self,
        frame: pd.DataFrame,
        metadata_key: str,
        normalizer,
    ) -> set[str]:
        """Collect normalized fingerprints for one generic metadata key."""

        column = self._find_metadata_column(frame.columns, metadata_key)
        if column is None:
            return set()
        values: set[str] = set()
        for value in frame[column].tolist():
            normalized = normalizer(value)
            if normalized:
                values.add(normalized)
        return values

    @staticmethod
    def _find_metadata_column(columns: Any, metadata_key: str) -> str | None:
        """Find an export column using user-configured generic aliases."""

        column_names = [str(column) for column in columns]
        exact = {name: name for name in column_names}
        for alias in metadata_aliases(metadata_key):
            if alias in exact:
                return exact[alias]
        normalized = {_normal_header(name): name for name in column_names}
        for alias in metadata_aliases(metadata_key):
            match = normalized.get(_normal_header(alias))
            if match:
                return match
        return None

    def _row_seen_before(
        self,
        row: dict[str, str],
        seen_ids: set[str],
        seen_dois: set[str],
        seen_titles: set[str],
    ) -> bool:
        """Determine whether a current export row was already present."""

        row_id = self._normalize_identifier(read_metadata_value(row, "paper_id"))
        if row_id and row_id in seen_ids:
            return True

        # human readable hint: stable bibliographic fingerprints catch duplicates even when export IDs shift.
        row_doi = self._normalize_doi(read_metadata_value(row, "doi"))
        if row_doi and row_doi in seen_dois:
            return True
        row_title = self._normalize_title(read_metadata_value(row, "title"))
        return bool(row_title and row_title in seen_titles)

    @staticmethod
    def _normalize_identifier(value: Any) -> str:
        """Normalize a row identifier while tolerating punctuation prefixes."""

        return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())

    @staticmethod
    def _normalize_doi(value: Any) -> str:
        """Normalize DOI-like strings for duplicate detection."""

        return str(value or "").strip().casefold()

    @staticmethod
    def _normalize_title(value: Any) -> str:
        """Normalize titles to alphanumeric fingerprints for fallback matching."""

        return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())

    @staticmethod
    def _is_missing_marker(value: Any) -> bool:
        """Return True for common CSV placeholders that mean no value."""

        return str(value or "").strip().casefold() in MISSING_TEXT_MARKERS

    @staticmethod
    def _infer_stage_from_paths(*paths: Path) -> str:
        """Infer target stage from known citation-search and baseline patterns."""

        names = " ".join(path.name for path in paths)
        for stage, patterns in STAGE_FILE_PATTERNS.items():
            if any(fnmatch.fnmatch(path.name, str(patterns["citation"])) for path in paths):
                return stage
            if any(fnmatch.fnmatch(path.name, str(patterns["baseline"])) for path in paths):
                return stage
        return ""

    def _default_export_prefix(self) -> str:
        """Return a stage-specific citation-search output prefix."""

        stage = self.audit.target_stage or self._infer_stage_from_paths(self.source_path or Path(""))
        if stage in STAGE_FILE_PATTERNS:
            return str(STAGE_FILE_PATTERNS[stage]["export_prefix"])
        return "citationSearching_title-abstract_novel"

    def _write_ingestion_log(self, log_path: Path) -> None:
        """Write a compact audit log for the most recent ingestion."""

        timestamp = (self.ingested_at or datetime.now(timezone.utc)).isoformat()
        source_name = self.source_path.name if self.source_path else ""
        stage_label = self._stage_label(self.audit.target_stage)
        lines = [
            "citation_ingestion_log",
            f"timestamp_utc: {timestamp}",
            f"target_stage: {stage_label}",
            f"baseline_file: {self.audit.baseline_file}",
            f"baseline_total_records: {self.audit.baseline_total_records}",
            f"citation_file: {self.audit.citation_file or source_name}",
            f"citation_total_records: {self.audit.citation_total_records or len(self.records)}",
            f"old_records_filtered_out: {self.audit.old_records_filtered_out}",
            f"novel_records_for_llm_screening: {self.audit.novel_records_for_screening or len(self.records)}",
            f"missing_title_count: {self.missing_counts.get('title', 0)}",
            f"missing_abstract_count: {self.missing_counts.get('abstract', 0)}",
            f"missing_doi_count: {self.missing_counts.get('doi', 0)}",
        ]
        log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    @staticmethod
    def _stage_label(stage: str) -> str:
        """Return a human-readable stage label for audit logs."""

        if stage in STAGE_FILE_PATTERNS:
            return str(STAGE_FILE_PATTERNS[stage]["label"])
        return stage or "Unknown"


def _normal_header(value: str) -> str:
    """Normalize external CSV headers for alias matching."""

    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())
