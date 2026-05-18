"""Generic CSV handoff writers for stage-to-stage review workflows."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.core.metadata_aliases import read_metadata_value


CANONICAL_HANDOFF_FIELDS: tuple[str, ...] = (
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

HANDOFF_AUDIT_FIELDS: tuple[str, ...] = (
    "source_stage",
    "target_stage",
    "source_run_label",
    "source_run_id",
    "source_eligibility_path",
    "source_decision",
    "source_confidence_score",
    "source_exclusion_reason_category",
    "source_error_flag",
)

HANDOFF_FIELDS: tuple[str, ...] = CANONICAL_HANDOFF_FIELDS + HANDOFF_AUDIT_FIELDS


@dataclass(frozen=True)
class StageHandoffSpec:
    """human readable hint: one decision split and its next-stage CSV naming contract."""

    target_stage: str
    decision_label: str
    eligible_value: bool


class StageHandoffCsvWriter:
    """human readable hint: write generic next-stage CSVs from final eligibility JSONL records."""

    STAGE_HANDOFFS: dict[str, tuple[StageHandoffSpec, ...]] = {
        "title_abstract": (
            StageHandoffSpec("full_text", "select", True),
            StageHandoffSpec("full_text", "irrelevant", False),
        ),
        "full_text": (
            StageHandoffSpec("data_extraction", "included", True),
            StageHandoffSpec("data_extraction", "excluded", False),
        ),
    }

    def __init__(
        self,
        *,
        stage: str,
        eligibility_path: Path,
        output_dir: Path,
        run_id: str = "",
        run_label: str = "",
        write_excluded_audit_csv: bool = True,
    ) -> None:
        self.stage = str(stage)
        self.eligibility_path = Path(eligibility_path)
        self.output_dir = Path(output_dir)
        self.run_id = str(run_id or "")
        self.run_label = str(run_label or "")
        self.write_excluded_audit_csv = bool(write_excluded_audit_csv)

    def write(self) -> dict[str, dict[str, object]]:
        """human readable hint: write eligible handoff CSVs and optional false-decision audit CSVs."""

        if self.stage not in self.STAGE_HANDOFFS:
            return {}
        if not self.eligibility_path.exists():
            raise FileNotFoundError(f"Eligibility JSONL not found: {self.eligibility_path}")

        records = self._read_decision_rows()
        results: dict[str, dict[str, object]] = {}

        # human readable hint: true-decision CSVs are operational handoffs; false-decision CSVs are audit files.
        for spec in self.STAGE_HANDOFFS[self.stage]:
            if not spec.eligible_value and not self.write_excluded_audit_csv:
                continue
            rows = [row for row in records if row.get("_eligible_value") is spec.eligible_value]
            path = self._handoff_path(spec)
            self._write_csv(path, rows)
            results[spec.decision_label] = {
                "path": str(path),
                "paper_count": len(rows),
                "source_stage": self.stage,
                "target_stage": spec.target_stage,
                "decision_label": spec.decision_label,
                "eligible_value": spec.eligible_value,
            }
        return results

    def _read_decision_rows(self) -> list[dict[str, object]]:
        """human readable hint: parse JSONL records and keep only records with an explicit boolean decision."""

        rows: list[dict[str, object]] = []
        with self.eligibility_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict) or payload.get("meta"):
                    continue
                decision = self._decision_payload(payload.get("llm_decision"))
                eligible = decision.get("is_eligible")
                if not isinstance(eligible, bool):
                    continue
                rows.append(self._row_from_payload(payload, decision, eligible))
        return rows

    def _row_from_payload(
        self,
        payload: dict[str, Any],
        decision: dict[str, Any],
        eligible: bool,
    ) -> dict[str, object]:
        """human readable hint: flatten one eligibility record into generic CSV metadata plus audit fields."""

        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        row = {field: read_metadata_value(metadata, field) for field in CANONICAL_HANDOFF_FIELDS}
        row["paper_id"] = row.get("paper_id") or str(payload.get("paper_id") or "").strip()
        row["source_stage"] = self.stage
        row["target_stage"] = self._target_stage_for_decision(eligible)
        row["source_run_label"] = str(payload.get("run_label") or self.run_label)
        row["source_run_id"] = str(payload.get("run_id") or self.run_id)
        row["source_eligibility_path"] = str(self.eligibility_path)
        row["source_decision"] = "eligible" if eligible else "not_eligible"
        row["source_confidence_score"] = decision.get("confidence_score", "")
        row["source_exclusion_reason_category"] = decision.get("exclusion_reason_category", "")
        row["source_error_flag"] = payload.get("error_flag", "")
        row["_eligible_value"] = eligible
        return row

    def _target_stage_for_decision(self, eligible: bool) -> str:
        for spec in self.STAGE_HANDOFFS.get(self.stage, ()):
            if spec.eligible_value is eligible:
                return spec.target_stage
        return ""

    def _handoff_path(self, spec: StageHandoffSpec) -> Path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H-%M-%S")
        run_component = _safe_filename_component(self.run_id or self.eligibility_path.stem)
        filename = (
            f"{self.stage}_to_{spec.target_stage}_{spec.decision_label}_csv_"
            f"{timestamp}_{run_component}.csv"
        )
        return self.output_dir / filename

    @staticmethod
    def _decision_payload(raw: object) -> dict[str, Any]:
        """human readable hint: accept stored decisions whether they are JSON strings or dicts."""

        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
        """human readable hint: write a stable CSV header even when a split has zero papers."""

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(HANDOFF_FIELDS), extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in HANDOFF_FIELDS})


def latest_handoff_for_stage(stage: str, output_dir: Path) -> Path | None:
    """human readable hint: find the newest prior-stage handoff that can feed the requested stage."""

    patterns = {
        "full_text": "title_abstract_to_full_text_select_csv_*.csv",
        "data_extraction": "full_text_to_data_extraction_included_csv_*.csv",
    }
    pattern = patterns.get(str(stage))
    if not pattern:
        return None
    root = Path(output_dir)
    matches = [path for path in root.glob(pattern) if path.is_file()] if root.exists() else []
    return max(matches, key=lambda path: path.stat().st_mtime) if matches else None


def _safe_filename_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", str(value or "").strip())
    cleaned = cleaned.strip("-_")
    return cleaned or "run"
