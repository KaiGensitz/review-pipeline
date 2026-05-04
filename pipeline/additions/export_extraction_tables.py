"""Export run-level data-extraction tables for validation and audit review.

Direct run:
    python -m pipeline.additions.export_extraction_tables
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

from config.user_orchestrator import (
    DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS,
    DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES,
    PATH_SETTINGS,
)
from pipeline.core.extraction_schema import DynamicExtractionSchema, ExtractionVariable
from pipeline.core.metadata_aliases import read_metadata_value


ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = ROOT / "output" / "data_extraction"
DEFAULT_CONSENSUS = ROOT / "input" / "data_extraction_schema.csv"
DEFAULT_INPUT_PAPER_DIR = ROOT / "input" / "per_paper_data_extraction"


def _admin_setting(key: str, default: Any) -> Any:
    """human readable hint: administrative export labels live in user_orchestrator.py, not pipeline code."""

    if isinstance(DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS, dict):
        return DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS.get(key, default)
    return default


QUOTE_AUDIT_HEADERS = list(
    _admin_setting(
        "quote_audit_headers",
        ["paper_id", "title", "domain", "variable", "consensus_column", "ai_value", "ai_quote"],
    )
)


def _normal_key(value: str) -> str:
    """human readable hint: normalize CSV headers so small spacing/punctuation differences still match."""

    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _strip_overall(value: str) -> str:
    """human readable hint: human exports may omit trailing aggregate words although the KB includes them."""

    return re.sub(r"overall$", "", _normal_key(value))


def _stringify(value: Any) -> str:
    """human readable hint: convert JSON values into Excel-friendly CSV cells without changing meaning."""

    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(str(item).strip() for item in value if str(item).strip())
    return str(value)


def _display_extracted_value(value: Any, variable: ExtractionVariable) -> str:
    """human readable hint: show absent extracted variables consistently for human review CSVs."""

    if value is None:
        return "Not Available"
    if variable.variable_type == "list":
        items = [str(item).strip() for item in value] if isinstance(value, list) else [str(value).strip()]
        items = [item for item in items if item]
        return "; ".join(items) if items else "Not Available"
    text = _stringify(value).strip()
    return text if text else "Not Available"


def _read_jsonl_record(path: Path) -> dict[str, Any] | None:
    """human readable hint: read the non-meta extraction payload from one per-paper JSONL file."""

    record: dict[str, Any] | None = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict) and not payload.get("meta"):
                record = payload
    return record


def _load_extraction_records(output_dir: Path) -> list[dict[str, Any]]:
    """human readable hint: collect all canonical per-paper extraction JSONL records."""

    records: list[dict[str, Any]] = []
    for path in sorted(output_dir.glob("*/data_extraction_results.jsonl")):
        record = _read_jsonl_record(path)
        if not record:
            continue
        record["_output_folder"] = str(path.parent)
        records.append(record)
    return records


def _load_consensus_headers(consensus_path: Path) -> list[str]:
    """human readable hint: reuse the human consensus CSV header order for direct AI-vs-human comparison."""

    default_headers = list(_admin_setting("comparison_default_headers", ["paper_id", "title"]))
    if not consensus_path.exists():
        return default_headers
    with consensus_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or default_headers)


def _load_folder_metadata(paper_id: str, input_paper_dir: Path) -> dict[str, Any]:
    """human readable hint: recover publication metadata from prepared per-paper input artifacts."""

    clean_id = str(paper_id or "").lstrip("#")
    for folder in sorted(input_paper_dir.glob(f"{clean_id}_*")):
        if not folder.is_dir():
            continue
        for name in ("data_extraction_artifact.json", "full_text_artifact.json"):
            artifact = folder / name
            if not artifact.exists():
                continue
            try:
                payload = json.loads(artifact.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            metadata = payload.get("metadata")
            if isinstance(metadata, dict):
                return metadata
            return payload
    return {}


def _value_from_record(record: dict[str, Any], variable: ExtractionVariable) -> Any:
    """human readable hint: locate one KB variable's value inside the nested LLM extraction JSON."""

    extracted = record.get("extracted_data")
    if not isinstance(extracted, dict):
        return None
    domain_payload = extracted.get(variable.domain)
    if not isinstance(domain_payload, dict):
        return None
    return domain_payload.get(variable.value_key)


def _quote_from_record(record: dict[str, Any], variable: ExtractionVariable) -> str:
    """human readable hint: locate the quote that supports one KB variable for audit review."""

    extracted = record.get("extracted_data")
    if not isinstance(extracted, dict):
        return ""
    domain_payload = extracted.get(variable.domain)
    if not isinstance(domain_payload, dict):
        return ""
    return _stringify(domain_payload.get(variable.quote_key))


def _candidate_headers(variable: ExtractionVariable) -> list[str]:
    """human readable hint: exact schema mapping first, optional user-configured aliases second."""

    candidates = [
        variable.covidence_column_name,
        variable.variable_name,
        f"{variable.domain}_{variable.variable_name}",
    ]
    config_key = f"{variable.domain}.{variable.variable_name}"
    aliases = DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES.get(config_key, [])
    if isinstance(aliases, (list, tuple)):
        candidates.extend(str(alias) for alias in aliases)
    if variable.variable_name.endswith("_overall"):
        candidates.append(variable.variable_name[: -len("_overall")])
    if variable.covidence_column_name.casefold().endswith(" overall"):
        candidates.append(variable.covidence_column_name[: -len(" overall")])
    return [candidate for candidate in candidates if str(candidate or "").strip()]


def _resolve_consensus_column(variable: ExtractionVariable, headers: list[str]) -> str:
    """human readable hint: map a KB variable to the closest exact human consensus header."""

    exact = {header: header for header in headers}
    normal = {_normal_key(header): header for header in headers}
    stripped = {_strip_overall(header): header for header in headers}

    for candidate in _candidate_headers(variable):
        if candidate in exact:
            return exact[candidate]
        key = _normal_key(candidate)
        if key in normal:
            return normal[key]
        stripped_key = _strip_overall(candidate)
        if stripped_key in stripped:
            return stripped[stripped_key]
    return ""


def _put_if_header(row: dict[str, str], headers: list[str], desired_header: str, value: str) -> None:
    """human readable hint: fill a configured output header using case/punctuation tolerant matching."""

    normal_to_header = {_normal_key(header): header for header in headers}
    header = normal_to_header.get(_normal_key(desired_header))
    if header:
        row[header] = value


def _paper_metadata_row(record: dict[str, Any], headers: list[str], input_paper_dir: Path) -> dict[str, str]:
    """human readable hint: fill administrative columns shared with the human consensus table."""

    paper_id = str(record.get("paper_id") or "").lstrip("#")
    metadata = _load_folder_metadata(paper_id, input_paper_dir)
    row = {header: "" for header in headers}

    configured_values = {
        str(_admin_setting("paper_id_column", "paper_id")): paper_id,
        str(_admin_setting("study_id_column", "study_id")): read_metadata_value(metadata, "study_id"),
        str(_admin_setting("reviewer_name_column", "reviewer_name")): str(
            _admin_setting("reviewer_name_value", "AI")
        ),
        str(_admin_setting("title_column", "title")): read_metadata_value(metadata, "title"),
        str(_admin_setting("authors_column", "authors")): read_metadata_value(metadata, "authors"),
        str(_admin_setting("publication_year_column", "publication_year")): read_metadata_value(
            metadata, "publication_year"
        ),
    }
    for header, value in configured_values.items():
        _put_if_header(row, headers, header, value)
    return row


def build_consensus_comparison_rows(
    records: list[dict[str, Any]],
    schema: DynamicExtractionSchema,
    headers: list[str],
    input_paper_dir: Path,
) -> tuple[list[dict[str, str]], dict[str, str]]:
    """human readable hint: create AI rows in the same wide layout as the human consensus CSV."""

    variable_to_header = {
        variable.value_path: _resolve_consensus_column(variable, headers)
        for variable in schema.variables
    }
    rows: list[dict[str, str]] = []
    for record in records:
        row = _paper_metadata_row(record, headers, input_paper_dir)
        for variable in schema.variables:
            header = variable_to_header[variable.value_path]
            if not header:
                continue
            row[header] = _display_extracted_value(_value_from_record(record, variable), variable)
        rows.append(row)
    return rows, variable_to_header


def build_quote_audit_rows(
    records: list[dict[str, Any]],
    schema: DynamicExtractionSchema,
    variable_to_header: dict[str, str],
    input_paper_dir: Path,
) -> list[dict[str, str]]:
    """human readable hint: keep quotes in a long audit table so manuscript tables stay readable."""

    rows: list[dict[str, str]] = []
    paper_id_column = str(_admin_setting("paper_id_column", "paper_id"))
    title_column = str(_admin_setting("title_column", "title"))
    for record in records:
        paper_id = str(record.get("paper_id") or "").lstrip("#")
        metadata = _load_folder_metadata(paper_id, input_paper_dir)
        title = read_metadata_value(metadata, "title")
        for variable in schema.variables:
            rows.append(
                {
                    paper_id_column: paper_id,
                    title_column: title,
                    "Domain": variable.domain,
                    "Variable": variable.variable_name,
                    "Consensus_Column": variable_to_header.get(variable.value_path, ""),
                    "AI_Value": _display_extracted_value(_value_from_record(record, variable), variable),
                    "AI_Quote": _quote_from_record(record, variable),
                }
            )
    return rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    """human readable hint: write CSV with stable column order and UTF-8 encoding for Excel/Word use."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _append_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    """human readable hint: append completed-paper rows without waiting for the full run to finish."""

    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writerows(rows)


class ExtractionAggregateWriter:
    """human readable hint: live writer for the two run-level data-extraction audit tables."""

    def __init__(
        self,
        output_dir: Path = OUTPUT_DIR,
        consensus_path: Path = DEFAULT_CONSENSUS,
        input_paper_dir: Path = DEFAULT_INPUT_PAPER_DIR,
        reset: bool = False,
    ) -> None:
        # human readable hint: load the same KB schema and export column mapping used by validation.
        self.output_dir = output_dir
        self.input_paper_dir = input_paper_dir
        self.schema = DynamicExtractionSchema.from_kb()
        self.headers = _load_consensus_headers(consensus_path)
        self.variable_to_header = {
            variable.value_path: _resolve_consensus_column(variable, self.headers)
            for variable in self.schema.variables
        }
        self.comparison_path = output_dir / "data_extraction_all_papers_for_consensus_comparison.csv"
        self.quote_path = output_dir / "data_extraction_all_papers_quote_audit.csv"
        self._initialize_files(reset=reset)

    def _initialize_files(self, reset: bool) -> None:
        """human readable hint: create empty aggregate CSVs at run start so progress is visible immediately."""

        self.output_dir.mkdir(parents=True, exist_ok=True)
        if reset or not self.comparison_path.exists():
            _write_csv(self.comparison_path, self.headers, [])
        if reset or not self.quote_path.exists():
            _write_csv(self.quote_path, QUOTE_AUDIT_HEADERS, [])

    def append_record(self, record: dict[str, Any]) -> None:
        """human readable hint: add one completed paper to both aggregate tables."""

        comparison_rows, _unused = build_consensus_comparison_rows(
            [record],
            self.schema,
            self.headers,
            self.input_paper_dir,
        )
        quote_rows = build_quote_audit_rows(
            [record],
            self.schema,
            self.variable_to_header,
            self.input_paper_dir,
        )
        _append_csv(self.comparison_path, self.headers, comparison_rows)
        _append_csv(self.quote_path, QUOTE_AUDIT_HEADERS, quote_rows)


def export_tables(
    output_dir: Path = OUTPUT_DIR,
    consensus_path: Path = DEFAULT_CONSENSUS,
    input_paper_dir: Path = DEFAULT_INPUT_PAPER_DIR,
) -> tuple[Path, Path]:
    """human readable hint: export the wide comparison table and the long quote-audit table in one step."""

    schema = DynamicExtractionSchema.from_kb()
    headers = _load_consensus_headers(consensus_path)
    records = _load_extraction_records(output_dir)
    if not records:
        raise FileNotFoundError(f"No data_extraction_results.jsonl files found under {output_dir}")

    comparison_rows, variable_to_header = build_consensus_comparison_rows(
        records, schema, headers, input_paper_dir
    )
    quote_rows = build_quote_audit_rows(records, schema, variable_to_header, input_paper_dir)

    comparison_path = output_dir / "data_extraction_all_papers_for_consensus_comparison.csv"
    quote_path = output_dir / "data_extraction_all_papers_quote_audit.csv"

    _write_csv(comparison_path, headers, comparison_rows)
    _write_csv(quote_path, QUOTE_AUDIT_HEADERS, quote_rows)
    return comparison_path, quote_path


def _parse_args() -> argparse.Namespace:
    """human readable hint: parse optional paths while keeping defaults aligned with the active project."""

    parser = argparse.ArgumentParser(description="Export aggregated data-extraction tables.")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Folder containing per-paper extraction outputs.")
    parser.add_argument(
        "--consensus",
        default=str(DEFAULT_CONSENSUS),
        help="Human consensus CSV whose headers define the comparison layout.",
    )
    parser.add_argument(
        "--input-paper-dir",
        default=str(Path(PATH_SETTINGS.get("csv_dir", ROOT / "input")) / "per_paper_data_extraction"),
        help="Prepared per-paper input folder used to recover titles and metadata.",
    )
    return parser.parse_args()


def main() -> None:
    """human readable hint: command-line entrypoint for one-step export after data extraction."""

    args = _parse_args()
    paths = export_tables(
        output_dir=Path(args.output_dir),
        consensus_path=Path(args.consensus),
        input_paper_dir=Path(args.input_paper_dir),
    )
    print("Exported data-extraction tables:")
    for path in paths:
        print(f"- {path}")


if __name__ == "__main__":
    main()
