"""Build data-extraction human gold-standard tables from binary review sheets."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SCHEMA_PATH = REPO_ROOT / "knowledge-base" / "data_extraction_schema.csv"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "data_extraction" / "human_review_audit"
HUMAN_SCORE_SUFFIX = "__human_score"
HUMAN_NOTE_SUFFIX = "__human_note"
SOURCE_VALUE_SUFFIX = "__source_ai_value"
HUMAN_EVALUABLE_SUFFIX = "__human_evaluable"
HUMAN_GROUND_TRUTH_SOURCE_SUFFIX = "__human_ground_truth_source"


def _admin_header_setting(key: str, fallback: str) -> str:
    """human readable hint: read source-sheet admin labels from user config instead of pipeline constants."""

    try:
        from config.user_orchestrator import DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS

        if isinstance(DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS, dict):
            value = str(DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS.get(key, "")).strip()
            if value:
                return value
    except Exception:
        pass
    return fallback


@dataclass(frozen=True)
class SchemaVariable:
    """human readable hint: minimal schema row needed to shape a validation-ready human table."""

    domain: str
    variable_name: str
    covidence_column_name: str


@dataclass(frozen=True)
class ReviewedPaperBlock:
    """human readable hint: one AI row plus its following human 0/1 score and note rows."""

    paper_id: str
    reviewer: str
    ai_row: list[str]
    score_row: list[str]
    note_row: list[str]


class ReviewSheetReader:
    """human readable hint: parse a reviewer sheet without hardcoding study-specific variables."""

    def __init__(self, source_path: Path) -> None:
        self.source_path = source_path

    def read(self) -> tuple[list[str], list[list[str]], str]:
        """human readable hint: tolerate common spreadsheet encodings and detect the delimiter."""

        raw = self._read_text()
        sample = raw[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"
        rows = list(csv.reader(raw.splitlines(), dialect))
        if not rows:
            raise ValueError(f"Review sheet is empty: {self.source_path}")
        header = rows[0]
        body = rows[1:]
        return header, body, dialect.delimiter

    def _read_text(self) -> str:
        """human readable hint: exported review sheets may be UTF-8 or Windows encoded."""

        for encoding in ("utf-8-sig", "cp1252", "latin-1"):
            try:
                return self.source_path.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue
        return self.source_path.read_text()


class SchemaColumnMatcher:
    """human readable hint: map schema variables to reviewer-sheet columns by generic normalized labels."""

    LOW_PRIORITY_TOKENS = {"note", "notes", "quote", "quotes"}

    def __init__(self, headers: Iterable[str]) -> None:
        self.headers = list(headers)
        self.normalized_headers = [self._normalize(header) for header in self.headers]

    def build_mapping(self, variables: Iterable[SchemaVariable]) -> dict[str, int]:
        """human readable hint: choose one source column per schema variable using transparent label scoring."""

        mapping: dict[str, int] = {}
        for variable in variables:
            best_index = self._best_index_for_variable(variable)
            if best_index is None:
                continue
            mapping[variable.covidence_column_name] = best_index
        return mapping

    def _best_index_for_variable(self, variable: SchemaVariable) -> int | None:
        variable_label = self._normalize(variable.variable_name)
        variable_stem = self._normalize(variable.variable_name.replace("_overall", "").replace("_", " "))
        consensus_label = self._normalize(variable.covidence_column_name)
        best: tuple[int, int] | None = None
        for index, header_label in enumerate(self.normalized_headers):
            score = self._score(header_label, variable_label, variable_stem, consensus_label)
            if score <= 0:
                continue
            candidate = (score, -index)
            if best is None or candidate > best:
                best = candidate
        return -best[1] if best is not None else None

    def _score(self, source: str, variable: str, stem: str, consensus: str) -> int:
        if not source:
            return 0
        source_tokens = set(source.split())
        penalty = 35 if source_tokens & self.LOW_PRIORITY_TOKENS else 0
        if source == consensus:
            return 100 - penalty
        if source == variable:
            return 98 - penalty
        if stem and source == stem:
            return 94 - penalty
        if source.endswith(variable):
            return 90 - penalty
        if stem and source.endswith(stem):
            return 86 - penalty
        if self._tokens_subset(consensus, source):
            return 82 - penalty
        if self._tokens_subset(variable, source):
            return 78 - penalty
        if stem and self._tokens_subset(stem, source):
            return 72 - penalty
        return 0

    @staticmethod
    def _tokens_subset(needle: str, haystack: str) -> bool:
        needle_tokens = set(needle.split())
        haystack_tokens = set(haystack.split())
        return bool(needle_tokens) and needle_tokens <= haystack_tokens

    @staticmethod
    def _normalize(value: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold())
        return re.sub(r"\s+", " ", cleaned).strip()


class HumanGoldStandardBuilder:
    """human readable hint: convert binary reviewer judgements into stats-engine consensus inputs."""

    def __init__(self, source_path: Path, schema_path: Path, output_dir: Path) -> None:
        self.source_path = source_path
        self.schema_path = schema_path
        self.output_dir = output_dir

    def run(self) -> dict[str, Path]:
        """human readable hint: write wide validation input, long audit details, and a short README."""

        build = self.build()
        variables = build["variables"]
        headers = build["headers"]
        delimiter = build["delimiter"]
        column_map = build["column_map"]
        blocks = build["blocks"]
        wide_rows = build["wide_rows"]
        long_rows = build["long_rows"]

        self.output_dir.mkdir(parents=True, exist_ok=True)
        paths = {
            "wide": self.output_dir / "data_extraction_human_gold_standard_wide.csv",
            "long": self.output_dir / "data_extraction_human_gold_standard_long.csv",
            "mapping": self.output_dir / "data_extraction_human_gold_standard_column_mapping.csv",
            "readme": self.output_dir / "data_extraction_human_gold_standard_readme.md",
        }
        self._write_wide(paths["wide"], variables, wide_rows)
        self._write_long(paths["long"], long_rows)
        self._write_mapping(paths["mapping"], headers, variables, column_map)
        self._write_readme(paths["readme"], delimiter, variables, column_map, blocks, long_rows)
        return paths

    def build(self) -> dict[str, object]:
        """human readable hint: build validation-ready rows in memory without writing derived files."""

        variables = self._load_schema_variables()
        headers, body, delimiter = ReviewSheetReader(self.source_path).read()
        matcher = SchemaColumnMatcher(headers)
        column_map = matcher.build_mapping(variables)
        blocks = self._find_reviewed_blocks(headers, body)
        if not blocks:
            raise ValueError("No reviewed paper blocks found. Expected an AI row followed by score and note rows.")

        wide_rows, long_rows = self._build_rows(headers, variables, column_map, blocks)
        return {
            "variables": variables,
            "headers": headers,
            "delimiter": delimiter,
            "column_map": column_map,
            "blocks": blocks,
            "wide_rows": wide_rows,
            "long_rows": long_rows,
        }

    def _load_schema_variables(self) -> list[SchemaVariable]:
        """human readable hint: the active schema CSV defines the validation contract columns."""

        with self.schema_path.open(encoding="utf-8-sig", newline="") as handle:
            rows = csv.DictReader(handle)
            variables = [
                SchemaVariable(
                    domain=str(row.get("domain", "")).strip(),
                    variable_name=str(row.get("variable_name", "")).strip(),
                    covidence_column_name=str(row.get("covidence_column_name", "")).strip(),
                )
                for row in rows
                if str(row.get("domain", "")).strip()
                and str(row.get("variable_name", "")).strip()
                and str(row.get("covidence_column_name", "")).strip()
            ]
        if not variables:
            raise ValueError(f"No schema variables found in {self.schema_path}")
        return variables

    def _find_reviewed_blocks(self, headers: list[str], rows: list[list[str]]) -> list[ReviewedPaperBlock]:
        """human readable hint: reviewer blocks are detected from paper-id rows followed by score and quote rows."""

        blocks: list[ReviewedPaperBlock] = []
        reviewer_index = self._best_plain_header(headers, _admin_header_setting("reviewer_name_column", "reviewer"))
        index = 0
        while index + 2 < len(rows):
            row = rows[index]
            score_row = rows[index + 1]
            note_row = rows[index + 2]
            paper_id_cell = self._cell(row, 0)
            paper_id, source_reviewer = self._parse_paper_id_cell(paper_id_cell)
            if paper_id and self._looks_like_score_row(score_row):
                reviewer = (
                    self._cell(score_row, reviewer_index)
                    or self._cell(note_row, reviewer_index)
                    or source_reviewer
                )
                blocks.append(
                    ReviewedPaperBlock(
                        paper_id=paper_id,
                        reviewer=reviewer,
                        ai_row=row,
                        score_row=score_row,
                        note_row=note_row,
                    )
                )
                index += 3
                continue
            index += 1
        return blocks

    @staticmethod
    def _parse_paper_id_cell(paper_id_cell: str) -> tuple[str, str]:
        """human readable hint: support both `Reviewer: 22` and cleaned `22` paper-id cells."""

        text = str(paper_id_cell or "").strip()
        match = re.match(r"^(?:(?P<reviewer>.+?)\s*:\s*)?#?(?P<paper_id>\d+)\s*$", text)
        if not match:
            return "", ""
        return match.group("paper_id"), str(match.group("reviewer") or "").strip()

    @staticmethod
    def _looks_like_score_row(row: list[str]) -> bool:
        """human readable hint: score rows contain many binary 0/1 cells after the leading legend cell."""

        binary_cells = sum(1 for value in row[1:] if str(value).strip() in {"0", "1"})
        return binary_cells >= 5

    def _build_rows(
        self,
        headers: list[str],
        variables: list[SchemaVariable],
        column_map: dict[str, int],
        blocks: list[ReviewedPaperBlock],
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """human readable hint: wide rows feed stats_engine; long rows preserve reviewer traceability."""

        wide_rows: list[dict[str, str]] = []
        long_rows: list[dict[str, str]] = []
        title_index = self._best_plain_header(headers, "title")
        study_index = self._best_plain_header(headers, _admin_header_setting("study_id_column", "study_id"))
        for block in blocks:
            wide: dict[str, str] = {
                "paper_id": block.paper_id,
                "reviewer": block.reviewer,
                "source_study_id": self._cell(block.ai_row, study_index) if study_index is not None else "",
                "title": self._cell(block.ai_row, title_index) if title_index is not None else "",
            }
            for variable in variables:
                source_index = column_map.get(variable.covidence_column_name)
                source_value = self._cell(block.ai_row, source_index)
                human_score = self._normalize_score(self._cell(block.score_row, source_index))
                human_note = self._reviewer_note_for_variable(block.note_row, block.score_row, source_index, human_score)
                gold_value, evaluable, ground_truth_source = self._ground_truth_value(
                    source_value,
                    human_score,
                    human_note,
                )

                wide[variable.covidence_column_name] = gold_value
                wide[f"{variable.covidence_column_name}{HUMAN_SCORE_SUFFIX}"] = human_score
                wide[f"{variable.covidence_column_name}{HUMAN_NOTE_SUFFIX}"] = human_note
                wide[f"{variable.covidence_column_name}{SOURCE_VALUE_SUFFIX}"] = source_value
                wide[f"{variable.covidence_column_name}{HUMAN_EVALUABLE_SUFFIX}"] = evaluable
                wide[f"{variable.covidence_column_name}{HUMAN_GROUND_TRUTH_SOURCE_SUFFIX}"] = ground_truth_source

                long_rows.append(
                    {
                        "paper_id": block.paper_id,
                        "reviewer": block.reviewer,
                        "domain": variable.domain,
                        "variable": variable.variable_name,
                        "covidence_column_name": variable.covidence_column_name,
                        "source_column": headers[source_index] if source_index is not None else "",
                        "source_ai_value": source_value,
                        "human_score": human_score,
                        "human_gold_value": gold_value,
                        "human_note": human_note,
                        "human_evaluable": evaluable,
                        "human_ground_truth_source": ground_truth_source,
                        "needs_manual_adjudication": "true" if human_score == "0" and not human_note else "false",
                    }
                )
            wide_rows.append(wide)
        return wide_rows, long_rows

    def _reviewer_note_for_variable(
        self,
        note_row: list[str],
        score_row: list[str],
        source_index: int | None,
        human_score: str,
    ) -> str:
        """human readable hint: recover reviewer corrections from quote rows even when spreadsheets place them nearby."""

        own_note = self._cell(note_row, source_index)
        if own_note or human_score != "0" or source_index is None:
            return own_note
        for offset in (1, 2):
            candidate_index = source_index + offset
            candidate_note = self._cell(note_row, candidate_index)
            candidate_score = self._normalize_score(self._cell(score_row, candidate_index))
            if candidate_note and candidate_score != "0":
                return candidate_note
        return ""

    @staticmethod
    def _ground_truth_value(source_value: str, human_score: str, human_note: str) -> tuple[str, str, str]:
        """human readable hint: turn AI-row, 0/1 row, and quote row into one human ground-truth cell."""

        if human_score == "1":
            return source_value, "true", "reviewer_accepted_source_value"
        if human_score == "0":
            if human_note:
                return human_note, "true", "reviewer_quote_correction"
            return "", "false", "reviewer_rejected_without_correction"
        if human_note:
            return human_note, "true", "reviewer_quote_without_binary_score"
        return "", "false", "not_human_reviewed"

    @staticmethod
    def _best_plain_header(headers: list[str], label: str) -> int | None:
        """human readable hint: keep source title/study labels when a sheet provides them."""

        normalized_label = SchemaColumnMatcher._normalize(label)
        for index, header in enumerate(headers):
            if SchemaColumnMatcher._normalize(header) == normalized_label:
                return index
        return None

    @staticmethod
    def _normalize_score(value: str) -> str:
        """human readable hint: preserve empty cells and normalize only explicit binary reviewer scores."""

        cleaned = str(value or "").strip()
        return cleaned if cleaned in {"0", "1"} else ""

    @staticmethod
    def _cell(row: list[str], index: int | None) -> str:
        """human readable hint: safely read ragged CSV rows."""

        if index is None or index < 0 or index >= len(row):
            return ""
        return re.sub(r"\s+", " ", str(row[index]).strip())

    @staticmethod
    def _write_wide(path: Path, variables: list[SchemaVariable], rows: list[dict[str, str]]) -> None:
        """human readable hint: stats_engine requires the schema consensus columns to be present in wide format."""

        fieldnames = ["paper_id", "reviewer", "source_study_id", "title"]
        for variable in variables:
            base = variable.covidence_column_name
            fieldnames.extend(
                [
                    base,
                    f"{base}{HUMAN_SCORE_SUFFIX}",
                    f"{base}{HUMAN_NOTE_SUFFIX}",
                    f"{base}{SOURCE_VALUE_SUFFIX}",
                    f"{base}{HUMAN_EVALUABLE_SUFFIX}",
                    f"{base}{HUMAN_GROUND_TRUTH_SOURCE_SUFFIX}",
                ]
            )
        HumanGoldStandardBuilder._write_dicts(path, fieldnames, rows)

    @staticmethod
    def _write_long(path: Path, rows: list[dict[str, str]]) -> None:
        """human readable hint: long output is easier for adjudication and manuscript audit trails."""

        fieldnames = [
            "paper_id",
            "reviewer",
            "domain",
            "variable",
            "covidence_column_name",
            "source_column",
            "source_ai_value",
            "human_score",
            "human_gold_value",
            "human_note",
            "human_evaluable",
            "human_ground_truth_source",
            "needs_manual_adjudication",
        ]
        HumanGoldStandardBuilder._write_dicts(path, fieldnames, rows)

    @staticmethod
    def _write_mapping(
        path: Path,
        headers: list[str],
        variables: list[SchemaVariable],
        column_map: dict[str, int],
    ) -> None:
        """human readable hint: expose every automatic source-to-schema column match for human review."""

        rows: list[dict[str, str]] = []
        for variable in variables:
            index = column_map.get(variable.covidence_column_name)
            rows.append(
                {
                    "domain": variable.domain,
                    "variable": variable.variable_name,
                    "covidence_column_name": variable.covidence_column_name,
                    "source_column": headers[index] if index is not None else "",
                    "source_column_index": "" if index is None else str(index),
                }
            )
        HumanGoldStandardBuilder._write_dicts(
            path,
            ["domain", "variable", "covidence_column_name", "source_column", "source_column_index"],
            rows,
        )

    @staticmethod
    def _write_dicts(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
        """human readable hint: one CSV writer keeps quoting and newline handling consistent."""

        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    def _write_readme(
        self,
        path: Path,
        delimiter: str,
        variables: list[SchemaVariable],
        column_map: dict[str, int],
        blocks: list[ReviewedPaperBlock],
        long_rows: list[dict[str, str]],
    ) -> None:
        """human readable hint: document how to use the generated gold-standard files."""

        scored = [row for row in long_rows if row["human_score"] in {"0", "1"}]
        correct = [row for row in scored if row["human_score"] == "1"]
        evaluable = [row for row in long_rows if row["human_evaluable"] == "true"]
        unresolved = [row for row in long_rows if row["needs_manual_adjudication"] == "true"]
        lines = [
            "# Data Extraction Human Gold Standard",
            "",
            f"Source sheet: `{self.source_path}`",
            f"Schema CSV: `{self.schema_path}`",
            f"Detected delimiter: `{delimiter}`",
            f"Reviewed paper blocks: {len(blocks)}",
            f"Schema variables: {len(variables)}",
            f"Mapped variables: {len(column_map)}",
            f"Explicit 0/1 reviewer cells: {len(scored)}",
            f"Reviewer-accepted cells: {len(correct)}",
            f"Reviewer-ground-truth evaluable cells: {len(evaluable)}",
            f"Reviewer-mismatch cells needing adjudication: {len(unresolved)}",
            "",
            "## Files",
            "",
            "- `data_extraction_human_gold_standard_wide.csv`: validation input for `python -m pipeline.additions.stats_engine --consensus <this file>`; schema columns contain reviewer-derived ground truth, not raw AI values.",
            "- `data_extraction_human_gold_standard_long.csv`: cell-level audit with reviewer scores, source AI values, reviewer-derived ground truth, notes, and manual-adjudication flags.",
            "- `data_extraction_human_gold_standard_column_mapping.csv`: automatic mapping from schema consensus columns to source review-sheet columns.",
            "",
            "## Validation Convention",
            "",
            f"Columns ending in `{HUMAN_SCORE_SUFFIX}` preserve the original 0/1 reviewer judgement for audit only. stats_engine validates against the schema ground-truth columns and ignores non-evaluable cells marked by `{HUMAN_EVALUABLE_SUFFIX}`.",
            "Cells scored `1` use the reviewed source value as human ground truth. Cells scored `0` use the reviewer quote/correction row as human ground truth when present.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    """human readable hint: CLI keeps source files user-supplied instead of hardcoded in pipeline code."""

    parser = argparse.ArgumentParser(description="Build data-extraction human gold-standard tables.")
    parser.add_argument("--source", required=True, help="Path to reviewer sheet CSV.")
    parser.add_argument("--schema", default=str(DEFAULT_SCHEMA_PATH), help="Path to data_extraction_schema.csv.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for generated gold-standard files.")
    return parser.parse_args()


def main() -> None:
    """human readable hint: command-line entrypoint for repeatable human-gold generation."""

    args = parse_args()
    paths = HumanGoldStandardBuilder(
        source_path=Path(args.source),
        schema_path=Path(args.schema),
        output_dir=Path(args.output_dir),
    ).run()
    for label, path in paths.items():
        try:
            display_path = path.resolve().relative_to(REPO_ROOT)
        except ValueError:
            display_path = path
        print(f"[output] {label}: {display_path}")


if __name__ == "__main__":
    main()
