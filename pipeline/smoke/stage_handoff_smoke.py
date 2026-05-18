"""Focused smoke checks for configurable stage handoff CSVs."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from uuid import uuid4

# human readable hint: this file lives in pipeline/smoke/, so parents[2] is the repository root.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.core.metadata_aliases import read_metadata_value
from pipeline.core.stage_handoff import StageHandoffCsvWriter, latest_handoff_for_stage


def _write_jsonl(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"meta": "eligibility_records", "description": "smoke"},
        {
            "paper_id": "A1",
            "error_flag": False,
            "llm_decision": json.dumps(
                {"is_eligible": True, "confidence_score": 0.91, "exclusion_reason_category": None}
            ),
            "metadata": {
                "paper_id": "A1",
                "title": "Eligible generic record",
                "abstract": "A generic abstract.",
                "authors": "Example Author",
                "publication_year": "2026",
            },
            "stage": "title_abstract",
            "run_label": "remaining_sample",
            "run_id": "title_abstract_remaining_sample_smoke",
        },
        {
            "paper_id": "B2",
            "error_flag": False,
            "llm_decision": {"is_eligible": False, "confidence_score": 0.84, "exclusion_reason_category": "generic_reason"},
            "metadata": {
                "paper_id": "B2",
                "title": "Excluded generic record",
                "abstract": "Another generic abstract.",
                "authors": "Second Author",
                "publication_year": "2025",
            },
            "stage": "title_abstract",
            "run_label": "remaining_sample",
            "run_id": "title_abstract_remaining_sample_smoke",
        },
        {"meta": "summary", "paper_count": 2},
    ]
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _assert_configured_aliases() -> None:
    """human readable hint: verify alias normalization without hardcoding any export header names."""

    from config.user_orchestrator import CSV_METADATA_COLUMN_ALIASES

    expected = {
        "paper_id": "A1",
        "abstract": "Configurable abstract",
        "authors": "Configurable Author",
        "journal": "Configurable Journal",
        "tags": "tag-one",
        "reference": "https://example.invalid/item",
    }
    row = {}
    for key, value in expected.items():
        aliases = [alias for alias in CSV_METADATA_COLUMN_ALIASES.get(key, []) if str(alias).strip()]
        alias = next((item for item in aliases if item != key), key)
        row[alias] = value

    for key, value in expected.items():
        assert read_metadata_value(row, key) == value


def _assert_screening_handoffs(root: Path) -> None:
    eligibility = root / "eligibility.jsonl"
    handoffs = root / "handoffs"
    _write_jsonl(eligibility)

    title_result = StageHandoffCsvWriter(
        stage="title_abstract",
        eligibility_path=eligibility,
        output_dir=handoffs,
        run_id="title_abstract_remaining_sample_smoke",
        run_label="remaining_sample",
    ).write()
    assert set(title_result) == {"select", "irrelevant"}
    select_path = Path(str(title_result["select"]["path"]))
    irrelevant_path = Path(str(title_result["irrelevant"]["path"]))
    assert select_path.name.startswith("title_abstract_to_full_text_select_csv_")
    assert irrelevant_path.name.startswith("title_abstract_to_full_text_irrelevant_csv_")
    assert len(_read_csv_rows(select_path)) == 1
    assert len(_read_csv_rows(irrelevant_path)) == 1
    assert latest_handoff_for_stage("full_text", handoffs) == select_path

    full_text_result = StageHandoffCsvWriter(
        stage="full_text",
        eligibility_path=eligibility,
        output_dir=handoffs,
        run_id="full_text_remaining_sample_smoke",
        run_label="remaining_sample",
    ).write()
    assert set(full_text_result) == {"included", "excluded"}
    included_path = Path(str(full_text_result["included"]["path"]))
    excluded_path = Path(str(full_text_result["excluded"]["path"]))
    assert included_path.name.startswith("full_text_to_data_extraction_included_csv_")
    assert excluded_path.name.startswith("full_text_to_data_extraction_excluded_csv_")
    assert len(_read_csv_rows(included_path)) == 1
    assert len(_read_csv_rows(excluded_path)) == 1
    assert latest_handoff_for_stage("data_extraction", handoffs) == included_path


def main() -> None:
    root = REPO_ROOT / "output" / f"_stage_handoff_smoke_{uuid4().hex[:8]}"
    _assert_configured_aliases()
    _assert_screening_handoffs(root)
    print(json.dumps({"status": "ok", "smoke_root": str(root)}, indent=2))


if __name__ == "__main__":
    main()
