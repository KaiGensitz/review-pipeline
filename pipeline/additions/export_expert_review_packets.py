"""Export and summarize AI-first extraction expert oversight packets.

Direct run:
    python -m pipeline.additions.export_expert_review_packets export
    python -m pipeline.additions.export_expert_review_packets summarize
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from config.user_orchestrator import (
    DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS,
    DATA_EXTRACTION_EXPERT_REVIEWERS,
    DATA_EXTRACTION_EXPERT_REVIEW_SETTINGS,
    DATA_EXTRACTION_EXPERT_REVIEW_SHARED_VARIABLES,
)
from pipeline.core.extraction_schema import DynamicExtractionSchema, ExtractionVariable


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SOURCE_OUTPUT_DIR = ROOT / "output" / "data_extraction"
DEFAULT_PACKET_DIR = DEFAULT_SOURCE_OUTPUT_DIR / "expert_review_packets"
QUOTE_AUDIT_FILENAME = "data_extraction_all_papers_quote_audit.csv"
COMBINED_PACKET_NAME = "expert_review_combined_tracking.csv"
SUMMARY_CSV_NAME = "expert_review_summary_by_variable.csv"
SUMMARY_REPORT_NAME = "expert_review_summary_report.txt"
MANIFEST_NAME = "expert_review_packet_manifest.json"

REVIEW_PACKET_COLUMNS = [
    "expert_key",
    "expert_name",
    "review_scope",
    "paper_id",
    "title",
    "domain",
    "variable",
    "schema_variable_path",
    "variable_type",
    "allowed_options",
    "schema_instruction",
    "consensus_column",
    "ai_value",
    "ai_quote",
    "evidence_context",
    "expert_decision",
    "corrected_value",
    "corrected_quote",
    "error_type",
    "error_effect",
    "expert_notes",
]


def _setting(key: str, default: Any) -> Any:
    """human readable hint: expert-review runtime settings live in user_orchestrator.py."""

    if isinstance(DATA_EXTRACTION_EXPERT_REVIEW_SETTINGS, dict):
        return DATA_EXTRACTION_EXPERT_REVIEW_SETTINGS.get(key, default)
    return default


def _admin_setting(key: str, default: Any) -> Any:
    """human readable hint: read human-facing extraction export labels from user config."""

    if isinstance(DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS, dict):
        return DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS.get(key, default)
    return default


def _resolve_path(value: Any, default: Path) -> Path:
    """human readable hint: allow config paths to be absolute or repo-relative."""

    if value is None or str(value).strip() == "":
        return default
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _normal_key(value: str) -> str:
    """human readable hint: match CSV headers despite punctuation or capitalization changes."""

    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _read_csv(path: Path) -> list[dict[str, str]]:
    """human readable hint: read reviewer/editable CSVs with BOM-tolerant UTF-8."""

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    """human readable hint: write stable Excel-friendly CSVs for expert review."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _read_json(path: Path) -> dict[str, Any]:
    """human readable hint: tolerate malformed sidecar files by treating them as absent context."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _schema_variable_map(schema: DynamicExtractionSchema) -> dict[str, ExtractionVariable]:
    """human readable hint: expose schema rows by the user-configured domain.variable_name key."""

    return {f"{variable.domain}.{variable.variable_name}": variable for variable in schema.variables}


def _configured_reviewers() -> dict[str, dict[str, Any]]:
    """human readable hint: reviewer identities and assigned variables are user-editable config data."""

    if not isinstance(DATA_EXTRACTION_EXPERT_REVIEWERS, dict):
        return {}
    reviewers: dict[str, dict[str, Any]] = {}
    for key, payload in DATA_EXTRACTION_EXPERT_REVIEWERS.items():
        if not isinstance(payload, dict):
            continue
        reviewer_key = str(key).strip()
        if not reviewer_key:
            continue
        variables = [
            str(item).strip()
            for item in payload.get("variables", [])
            if str(item).strip()
        ]
        reviewers[reviewer_key] = {
            "display_name": str(payload.get("display_name") or reviewer_key).strip() or reviewer_key,
            "variables": variables,
        }
    return reviewers


def _validate_variable_assignments(
    reviewers: dict[str, dict[str, Any]],
    shared_variables: Iterable[str],
    variables_by_path: dict[str, ExtractionVariable],
) -> None:
    """human readable hint: fail before export if config references a variable absent from the schema CSV."""

    missing: list[str] = []
    for reviewer_key, payload in reviewers.items():
        for variable_path in payload.get("variables", []):
            if variable_path not in variables_by_path:
                missing.append(f"{reviewer_key}:{variable_path}")
    for variable_path in shared_variables:
        if variable_path not in variables_by_path:
            missing.append(f"shared:{variable_path}")
    if missing:
        raise KeyError(
            "Expert review config references schema variable(s) absent from DATA_EXTRACTION_SCHEMA_FILE: "
            + ", ".join(missing)
        )


def _header_value(row: dict[str, str], desired_header: str) -> str:
    """human readable hint: read one CSV value using tolerant header matching."""

    if desired_header in row:
        return str(row.get(desired_header) or "")
    normal_to_header = {_normal_key(header): header for header in row}
    actual = normal_to_header.get(_normal_key(desired_header))
    return str(row.get(actual) or "") if actual else ""


def _audit_columns() -> dict[str, str]:
    """human readable hint: quote-audit column labels are configurable and may come from older runs."""

    return {
        "paper_id": str(_admin_setting("paper_id_column", "paper_id")),
        "title": str(_admin_setting("title_column", "title")),
        "domain": str(_admin_setting("quote_audit_domain_column", "domain")),
        "variable": str(_admin_setting("quote_audit_variable_column", "variable")),
        "consensus": str(_admin_setting("quote_audit_consensus_column", "consensus_column")),
        "value": str(_admin_setting("quote_audit_value_column", "ai_value")),
        "quote": str(_admin_setting("quote_audit_quote_column", "ai_quote")),
    }


def _normalize_paper_id(value: Any) -> str:
    """human readable hint: match '#250', '250', and '250.0' as the same paper id."""

    text = str(value or "").strip().lstrip("#")
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _quote_audit_index(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    """human readable hint: index quote-audit rows by paper id and schema variable path."""

    columns = _audit_columns()
    indexed: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        paper_id = _normalize_paper_id(_header_value(row, columns["paper_id"]))
        domain = _header_value(row, columns["domain"]).strip()
        variable = _header_value(row, columns["variable"]).strip()
        if not paper_id or not domain or not variable:
            continue
        indexed[(paper_id, f"{domain}.{variable}")] = row
    return indexed


def _load_evidence_index(source_output_dir: Path) -> dict[str, dict[str, Any]]:
    """human readable hint: load per-paper evidence sidecars for manuscript context snippets."""

    index: dict[str, dict[str, Any]] = {}
    for path in sorted(source_output_dir.glob("*/data_extraction_evidence.json")):
        payload = _read_json(path)
        paper_id = _normalize_paper_id(payload.get("paper_id") or path.parent.name.split("_", 1)[0])
        if paper_id:
            index[paper_id] = payload
    return index


def _paper_ids_from_output(source_output_dir: Path) -> list[str]:
    """human readable hint: all QC papers are inferred from per-paper result folders."""

    paper_ids: list[str] = []
    for path in sorted(source_output_dir.glob("*/data_extraction_results.jsonl")):
        paper_ids.append(_normalize_paper_id(path.parent.name.split("_", 1)[0]))
    return [paper_id for paper_id in paper_ids if paper_id]


def _shorten(value: str, max_chars: int = 1800) -> str:
    """human readable hint: keep reviewer packet cells readable without dropping the original quote."""

    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 15].rstrip() + " ... [truncated]"


def _chunk_label(chunk: dict[str, Any]) -> str:
    """human readable hint: cite chunk metadata so experts can relocate context in the manuscript trace."""

    parts = [str(chunk.get("chunk_id") or "").strip()]
    section = str(chunk.get("section") or "").strip()
    if section:
        parts.append(f"section={section}")
    page_start = chunk.get("page_start")
    page_end = chunk.get("page_end")
    if page_start is not None:
        page_text = f"page={page_start}" if page_end in {None, page_start} else f"pages={page_start}-{page_end}"
        parts.append(page_text)
    line_start = chunk.get("line_start")
    line_end = chunk.get("line_end")
    if line_start is not None:
        line_text = f"line={line_start}" if line_end in {None, line_start} else f"lines={line_start}-{line_end}"
        parts.append(line_text)
    return " | ".join(part for part in parts if part)


def _evidence_context(evidence_payload: dict[str, Any], quote: str, max_chars: int = 1800) -> str:
    """human readable hint: attach quote-near or top-ranked selected chunks for expert checking."""

    chunks = evidence_payload.get("selected_chunks")
    if not isinstance(chunks, list):
        return ""

    quote_text = re.sub(r"\s+", " ", str(quote or "")).strip()
    quote_probe = quote_text[:120].casefold()
    selected: list[dict[str, Any]] = []
    if quote_probe:
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            chunk_text = re.sub(r"\s+", " ", str(chunk.get("text") or "")).casefold()
            if quote_probe and quote_probe in chunk_text:
                selected.append(chunk)
                break

    quote_context = ""
    if quote_text and not selected:
        quote_context = f"[AI quote]\n{_shorten(quote_text, max_chars=max_chars // 2)}"

    if not selected:
        selected = [
            chunk for chunk in chunks
            if isinstance(chunk, dict)
        ][:2]

    parts: list[str] = []
    if quote_context:
        parts.append(quote_context)
    for chunk in selected:
        label = _chunk_label(chunk)
        divisor = max(len(selected) + (1 if quote_context else 0), 1)
        text = _shorten(str(chunk.get("text") or ""), max_chars=max_chars // divisor)
        if text:
            parts.append(f"[{label}]\n{text}" if label else text)
    return "\n\n".join(parts)


def _reviewer_variables(reviewer_payload: dict[str, Any], shared_variables: list[str]) -> list[tuple[str, str]]:
    """human readable hint: merge assigned and shared schema variables while preserving review scope."""

    ordered: list[tuple[str, str]] = []
    seen: set[str] = set()
    for variable_path in reviewer_payload.get("variables", []):
        if variable_path not in seen:
            ordered.append((variable_path, "assigned_expertise"))
            seen.add(variable_path)
    if bool(_setting("include_shared_methodological_variables_in_each_packet", True)):
        for variable_path in shared_variables:
            if variable_path not in seen:
                ordered.append((variable_path, "shared_methodological_audit"))
                seen.add(variable_path)
    return ordered


def _packet_path(packet_output_dir: Path, reviewer_key: str, reviewer_name: str) -> Path:
    """human readable hint: stable filenames use generic reviewer keys plus display labels."""

    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", reviewer_name).strip("_") or reviewer_key
    return packet_output_dir / f"expert_review_{reviewer_key}_{safe_name}.csv"


def export_expert_review_packets(
    source_output_dir: Path | None = None,
    packet_output_dir: Path | None = None,
) -> dict[str, Any]:
    """human readable hint: create expert-specific and combined AI-first oversight CSV packets."""

    source_dir = source_output_dir or _resolve_path(
        _setting("source_output_dir", DEFAULT_SOURCE_OUTPUT_DIR),
        DEFAULT_SOURCE_OUTPUT_DIR,
    )
    packet_dir = packet_output_dir or _resolve_path(
        _setting("packet_output_dir", DEFAULT_PACKET_DIR),
        DEFAULT_PACKET_DIR,
    )
    quote_path = source_dir / QUOTE_AUDIT_FILENAME
    if not quote_path.exists():
        raise FileNotFoundError(f"Missing quote-audit CSV: {quote_path}")

    schema = DynamicExtractionSchema.from_kb()
    variables_by_path = _schema_variable_map(schema)
    reviewers = _configured_reviewers()
    if not reviewers:
        raise ValueError("No expert reviewers configured in DATA_EXTRACTION_EXPERT_REVIEWERS.")
    shared_variables = [
        str(item).strip()
        for item in DATA_EXTRACTION_EXPERT_REVIEW_SHARED_VARIABLES
        if str(item).strip()
    ]
    _validate_variable_assignments(reviewers, shared_variables, variables_by_path)

    audit_rows = _read_csv(quote_path)
    audit_index = _quote_audit_index(audit_rows)
    evidence_index = _load_evidence_index(source_dir)
    paper_ids = _paper_ids_from_output(source_dir)
    if not paper_ids:
        raise FileNotFoundError(f"No per-paper extraction result folders found under {source_dir}")

    columns = _audit_columns()
    decision_options = "; ".join(str(item) for item in _setting("review_decision_options", []))
    error_type_options = "; ".join(str(item) for item in _setting("error_type_options", []))
    error_effect_options = "; ".join(str(item) for item in _setting("error_effect_options", []))

    combined_rows: list[dict[str, str]] = []
    packet_paths: dict[str, str] = {}
    quote_blank_count = 0
    missing_audit_count = 0
    for reviewer_key, reviewer_payload in reviewers.items():
        reviewer_name = str(reviewer_payload.get("display_name") or reviewer_key)
        rows: list[dict[str, str]] = []
        for variable_path, review_scope in _reviewer_variables(reviewer_payload, shared_variables):
            variable = variables_by_path[variable_path]
            for paper_id in paper_ids:
                audit_row = audit_index.get((paper_id, variable_path), {})
                if not audit_row:
                    missing_audit_count += 1
                title = _header_value(audit_row, columns["title"])
                ai_value = _header_value(audit_row, columns["value"])
                ai_quote = _header_value(audit_row, columns["quote"])
                if not ai_quote.strip():
                    quote_blank_count += 1
                evidence_context = _evidence_context(evidence_index.get(paper_id, {}), ai_quote)
                row = {
                    "expert_key": reviewer_key,
                    "expert_name": reviewer_name,
                    "review_scope": review_scope,
                    "paper_id": paper_id,
                    "title": title,
                    "domain": variable.domain,
                    "variable": variable.variable_name,
                    "schema_variable_path": variable_path,
                    "variable_type": variable.variable_type,
                    "allowed_options": "; ".join(variable.allowed_options),
                    "schema_instruction": variable.instruction,
                    "consensus_column": _header_value(audit_row, columns["consensus"]) or variable.consensus_column_name,
                    "ai_value": ai_value,
                    "ai_quote": ai_quote,
                    "evidence_context": evidence_context,
                    "expert_decision": "",
                    "corrected_value": "",
                    "corrected_quote": "",
                    "error_type": "",
                    "error_effect": "",
                    "expert_notes": (
                        f"Decision options: {decision_options}. "
                        f"Error types: {error_type_options}. "
                        f"Error effects: {error_effect_options}."
                    ),
                }
                rows.append(row)
                combined_rows.append(row)
        packet_path = _packet_path(packet_dir, reviewer_key, reviewer_name)
        _write_csv(packet_path, REVIEW_PACKET_COLUMNS, rows)
        packet_paths[reviewer_key] = str(packet_path)

    combined_path = packet_dir / COMBINED_PACKET_NAME
    _write_csv(combined_path, REVIEW_PACKET_COLUMNS, combined_rows)

    manifest = {
        "methodology": "AI-first extraction with expert human oversight",
        "source_output_dir": str(source_dir),
        "packet_output_dir": str(packet_dir),
        "schema_kb_path": str(schema.kb_path),
        "paper_count": len(paper_ids),
        "paper_ids": paper_ids,
        "reviewer_count": len(reviewers),
        "combined_row_count": len(combined_rows),
        "quote_blank_count": quote_blank_count,
        "missing_audit_row_count": missing_audit_count,
        "packets": packet_paths,
        "combined_tracking_csv": str(combined_path),
    }
    manifest_path = packet_dir / MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def _normalize_choice(value: str) -> str:
    """human readable hint: normalize expert-entered option labels for summary counts."""

    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().casefold()).strip("_")


def _counter_text(counter: Counter[str]) -> str:
    """human readable hint: compact counter formatting for CSV report cells."""

    return "; ".join(f"{key}={count}" for key, count in sorted(counter.items()) if key)


def summarize_expert_review(
    review_file: Path | None = None,
    packet_output_dir: Path | None = None,
) -> tuple[Path, Path]:
    """human readable hint: summarize completed expert oversight decisions by schema variable."""

    packet_dir = packet_output_dir or _resolve_path(
        _setting("packet_output_dir", DEFAULT_PACKET_DIR),
        DEFAULT_PACKET_DIR,
    )
    source_file = review_file or packet_dir / COMBINED_PACKET_NAME
    if not source_file.exists():
        raise FileNotFoundError(f"Missing expert review CSV: {source_file}")

    rows = _read_csv(source_file)
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        domain = str(row.get("domain") or "").strip()
        variable = str(row.get("variable") or "").strip()
        if domain and variable:
            grouped[(domain, variable)].append(row)

    trigger_decisions = {
        _normalize_choice(item)
        for item in _setting("prompt_refinement_trigger_decisions", ["correct", "mark_unavailable"])
    }
    trigger_effects = {
        _normalize_choice(item)
        for item in _setting("prompt_refinement_trigger_error_effects", ["major"])
    }

    summary_rows: list[dict[str, str]] = []
    trigger_fields: list[str] = []
    total_decisions = 0
    for (domain, variable), group_rows in sorted(grouped.items()):
        decisions = [_normalize_choice(row.get("expert_decision", "")) for row in group_rows]
        decisions = [decision for decision in decisions if decision]
        decision_counts = Counter(decisions)
        error_type_counts = Counter(
            _normalize_choice(row.get("error_type", ""))
            for row in group_rows
            if _normalize_choice(row.get("error_type", ""))
        )
        error_effect_counts = Counter(
            _normalize_choice(row.get("error_effect", ""))
            for row in group_rows
            if _normalize_choice(row.get("error_effect", ""))
        )
        reviewed_n = len(decisions)
        total_decisions += reviewed_n
        accepted_n = decision_counts.get("accept", 0)
        corrected_n = (
            decision_counts.get("correct", 0)
            + decision_counts.get("mark_unavailable", 0)
        )
        unclear_n = decision_counts.get("unclear", 0)
        trigger = bool((set(decision_counts) & trigger_decisions) or (set(error_effect_counts) & trigger_effects))
        field_path = f"{domain}.{variable}"
        if trigger:
            trigger_fields.append(field_path)
        summary_rows.append(
            {
                "domain": domain,
                "variable": variable,
                "schema_variable_path": field_path,
                "assigned_rows": str(len(group_rows)),
                "reviewed_rows": str(reviewed_n),
                "acceptance_rate": f"{accepted_n / reviewed_n:.4f}" if reviewed_n else "",
                "correction_rate": f"{corrected_n / reviewed_n:.4f}" if reviewed_n else "",
                "unclear_rate": f"{unclear_n / reviewed_n:.4f}" if reviewed_n else "",
                "decision_counts": _counter_text(decision_counts),
                "error_type_counts": _counter_text(error_type_counts),
                "error_effect_counts": _counter_text(error_effect_counts),
                "prompt_or_schema_refinement_trigger": "true" if trigger else "false",
            }
        )

    summary_path = packet_dir / SUMMARY_CSV_NAME
    _write_csv(
        summary_path,
        [
            "domain",
            "variable",
            "schema_variable_path",
            "assigned_rows",
            "reviewed_rows",
            "acceptance_rate",
            "correction_rate",
            "unclear_rate",
            "decision_counts",
            "error_type_counts",
            "error_effect_counts",
            "prompt_or_schema_refinement_trigger",
        ],
        summary_rows,
    )

    report_lines = [
        "AI-first extraction with expert human oversight",
        f"Review file: {source_file}",
        f"Variables summarized: {len(summary_rows)}",
        f"Expert decisions entered: {total_decisions}",
        "Prompt/schema refinement trigger fields:",
    ]
    if trigger_fields:
        report_lines.extend(f"- {field}" for field in trigger_fields)
    else:
        report_lines.append("- none")
    report_path = packet_dir / SUMMARY_REPORT_NAME
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    return summary_path, report_path


def _parse_args() -> argparse.Namespace:
    """human readable hint: expose export and summary commands for expert oversight packets."""

    parser = argparse.ArgumentParser(
        description="Export or summarize AI-first data-extraction expert oversight packets."
    )
    subparsers = parser.add_subparsers(dest="command")

    export_parser = subparsers.add_parser("export", help="Create expert review packets.")
    export_parser.add_argument("--source-output-dir", default=None, help="Extraction output folder to review.")
    export_parser.add_argument("--packet-output-dir", default=None, help="Folder where review packets are written.")

    summary_parser = subparsers.add_parser("summarize", help="Summarize completed expert review packets.")
    summary_parser.add_argument("--review-file", default=None, help="Completed combined or per-expert review CSV.")
    summary_parser.add_argument("--packet-output-dir", default=None, help="Folder where summary files are written.")

    parser.set_defaults(command="export")
    return parser.parse_args()


def main() -> None:
    """human readable hint: command-line entrypoint for export and summary workflows."""

    args = _parse_args()
    if args.command == "summarize":
        summary_path, report_path = summarize_expert_review(
            review_file=Path(args.review_file) if args.review_file else None,
            packet_output_dir=Path(args.packet_output_dir) if args.packet_output_dir else None,
        )
        print("[expert-review] summary status=written")
        print(f"[output] expert_review_summary path={summary_path}")
        print(f"[output] expert_review_report path={report_path}")
        return

    manifest = export_expert_review_packets(
        source_output_dir=Path(getattr(args, "source_output_dir", "")) if getattr(args, "source_output_dir", None) else None,
        packet_output_dir=Path(getattr(args, "packet_output_dir", "")) if getattr(args, "packet_output_dir", None) else None,
    )
    print("[expert-review] packets status=written")
    print(f"[output] expert_review_combined path={manifest['combined_tracking_csv']}")
    print(f"[output] expert_review_manifest path={manifest['manifest_path']}")
    print(f"[expert-review] papers={manifest['paper_count']}")
    print(f"[expert-review] rows={manifest['combined_row_count']}")


if __name__ == "__main__":
    main()
