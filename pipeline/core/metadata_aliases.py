"""Generic helpers for user-configured CSV metadata aliases."""

from __future__ import annotations

import re
from typing import Any, Iterable


GENERIC_METADATA_ALIASES = {
    "paper_id": ["paper_id", "id"],
    "title": ["title"],
    "abstract": ["abstract"],
    "authors": ["authors", "author"],
    "publication_year": ["publication_year", "year", "date"],
    "publication_month": ["publication_month", "month"],
    "journal": ["journal", "source"],
    "volume": ["volume"],
    "issue": ["issue"],
    "pages": ["pages", "page"],
    "accession_number": ["accession_number", "accession"],
    "doi": ["doi"],
    "reference": ["reference", "ref"],
    "study_id": ["study_id", "study", "study_number"],
    "notes": ["notes"],
    "tags": ["tags", "keywords", "label", "labels"],
    "reviewer_name": ["reviewer_name", "reviewer"],
}


def _configured_aliases() -> dict[str, list[str]]:
    """human readable hint: read external CSV header aliases from the user-editable config file."""

    try:
        from config.user_orchestrator import CSV_METADATA_COLUMN_ALIASES

        if isinstance(CSV_METADATA_COLUMN_ALIASES, dict):
            return {
                str(key): [str(item) for item in value]
                for key, value in CSV_METADATA_COLUMN_ALIASES.items()
                if isinstance(value, (list, tuple))
            }
    except Exception:
        pass
    return {}


def metadata_aliases(key: str) -> list[str]:
    """human readable hint: return configured external header names for one generic metadata key."""

    configured = _configured_aliases()
    aliases = list(configured.get(key, []))
    aliases.extend(GENERIC_METADATA_ALIASES.get(key, []))
    aliases.append(key)

    deduped: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        cleaned = str(alias or "").strip()
        if not cleaned or cleaned.casefold() in seen:
            continue
        seen.add(cleaned.casefold())
        deduped.append(cleaned)
    return deduped


def metadata_aliases_many(keys: Iterable[str]) -> list[str]:
    """human readable hint: combine aliases for several generic metadata keys while preserving order."""

    values: list[str] = []
    seen: set[str] = set()
    for key in keys:
        for alias in metadata_aliases(str(key)):
            folded = alias.casefold()
            if folded in seen:
                continue
            seen.add(folded)
            values.append(alias)
    return values


def read_metadata_value(row: dict[str, Any], key: str, default: str = "") -> str:
    """human readable hint: read one metadata value without hardcoding export-specific column names."""

    if not isinstance(row, dict):
        return default
    for alias in metadata_aliases(key):
        if alias in row and str(row.get(alias) or "").strip():
            return str(row.get(alias) or "").strip()
    normalized = {_normal_header(name): name for name in row.keys()}
    for alias in metadata_aliases(key):
        match = normalized.get(_normal_header(alias))
        if match and str(row.get(match) or "").strip():
            return str(row.get(match) or "").strip()
    return default


def read_first_metadata_value(row: dict[str, Any], keys: Iterable[str], default: str = "") -> str:
    """human readable hint: read the first available value across multiple generic metadata keys."""

    for key in keys:
        value = read_metadata_value(row, str(key), "")
        if value:
            return value
    return default


def normalize_metadata_row(row: dict[str, Any], default_id: str = "") -> dict[str, str]:
    """human readable hint: add generic metadata keys while preserving original CSV columns for traceability."""

    normalized = {str(k).strip(): str(v or "") for k, v in (row or {}).items()}
    paper_id = read_metadata_value(normalized, "paper_id", default_id)
    if paper_id:
        normalized["paper_id"] = paper_id
    for key in GENERIC_METADATA_ALIASES:
        value = read_metadata_value(normalized, key, "")
        if value:
            normalized[key] = value
    return normalized


def extract_year_from_metadata(row: dict[str, Any]) -> str:
    """human readable hint: extract a four-digit publication year from configured year/date columns."""

    value = read_metadata_value(row, "publication_year", "")
    match = re.search(r"(19|20)\d{2}", value)
    if match:
        return match.group(0)
    for raw in (row or {}).values():
        match = re.search(r"(19|20)\d{2}", str(raw or ""))
        if match:
            return match.group(0)
    return ""


def _normal_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())
