"""Generic hybrid rescue support for data extraction.

The module keeps full-text extraction as the primary record and adds an
optional, auditable semantic second opinion driven only by schema-owned
semantic anchors and user-configured runtime settings.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from config.user_orchestrator import LLM_SETTINGS, PATH_SETTINGS
from pipeline.core.extraction_schema import (
    DynamicExtractionSchema,
    ExtractionVariable,
    MISSING_TEXT_VALUES,
)
from pipeline.selection.chunking import chunk_fulltext_sentences
from pipeline.selection.selector import EmbeddingBackend, LabeledExample, RelevanceSelector, load_labeled_examples


@dataclass(frozen=True)
class HybridRescueConfig:
    """human readable hint: user-editable knobs for targeted semantic rescue."""

    enabled: bool
    top_k: int
    score_threshold: float | None
    configured_variables: tuple[str, ...]
    configured_domains: tuple[str, ...]
    full_text_preferred_variables: tuple[str, ...]
    language: str

    @classmethod
    def from_settings(cls, language: str) -> "HybridRescueConfig":
        """human readable hint: read hybrid settings without embedding study facts in pipeline code."""

        raw_threshold = LLM_SETTINGS.get("data_extraction_hybrid_rescue_score_threshold")
        if raw_threshold is None:
            raw_threshold = LLM_SETTINGS.get("data_extraction_semantic_score_threshold")
        score_threshold = None if raw_threshold in {None, ""} else float(raw_threshold)

        return cls(
            enabled=bool(LLM_SETTINGS.get("data_extraction_hybrid_rescue_enabled", False)),
            top_k=max(
                1,
                int(
                    LLM_SETTINGS.get(
                        "data_extraction_hybrid_rescue_top_k",
                        LLM_SETTINGS.get("data_extraction_semantic_top_k", 10),
                    )
                    or 10
                ),
            ),
            score_threshold=score_threshold,
            configured_variables=tuple(
                _clean_text(item)
                for item in _list_setting("data_extraction_hybrid_rescue_variables")
                if _clean_text(item)
            ),
            configured_domains=tuple(
                _clean_text(item)
                for item in _list_setting("data_extraction_hybrid_rescue_domains")
                if _clean_text(item)
            ),
            full_text_preferred_variables=tuple(
                _clean_text(item)
                for item in _list_setting("data_extraction_hybrid_full_text_preferred_variables")
                if _clean_text(item)
            ),
            language=language or "en",
        )


@dataclass(frozen=True)
class HybridRescueDecision:
    """human readable hint: one variable-level primary-vs-rescue decision for reviewer audit."""

    paper_id: str
    variable: str
    primary_full_text_value: str
    primary_full_text_quote: str
    semantic_rescue_value: str
    semantic_rescue_quote: str
    selected_value: str
    selected_quote: str
    evidence_mode_used: str
    selection_reason: str
    domain: str


class HybridRescuePlanner:
    """human readable hint: decide which schema variables deserve a semantic second opinion."""

    def __init__(self, schema: DynamicExtractionSchema, config: HybridRescueConfig) -> None:
        self.schema = schema
        self.config = config
        self._variables_by_path = {
            _normalize_variable_reference(v.value_path): v
            for v in schema.variables
        }
        self._variables_by_path.update(
            {
                _normalize_variable_reference(f"{v.domain}.{v.variable_name}"): v
                for v in schema.variables
            }
        )
        self._variables_by_short_name = {_normalize_path(v.variable_name): v for v in schema.variables}
        self._full_text_preferred = {
            _normalize_variable_reference(reference)
            for reference in config.full_text_preferred_variables
        }

    def target_variables(self, primary_payload: dict[str, Any]) -> tuple[ExtractionVariable, ...]:
        """human readable hint: include configured detail-sensitive fields and missing fields in configured domains."""

        if not self.config.enabled:
            return ()

        selected: dict[str, ExtractionVariable] = {}
        configured_domains = set(self.config.configured_domains)
        for reference in self.config.configured_variables:
            variable = self._resolve_variable(reference)
            if variable is not None:
                selected[variable.value_path] = variable

        for variable in self.schema.variables:
            if configured_domains and variable.domain in configured_domains:
                selected[variable.value_path] = variable
                continue
            if _is_missing(_value_for_variable(primary_payload, variable)):
                selected[variable.value_path] = variable
                continue
            if _is_missing(_quote_for_variable(primary_payload, variable)):
                selected[variable.value_path] = variable

        return tuple(selected.values())

    def is_full_text_preferred(self, variable: ExtractionVariable) -> bool:
        """human readable hint: table-sensitive variables can be protected by user-editable config."""

        return _normalize_variable_reference(variable.value_path) in self._full_text_preferred or (
            _normalize_variable_reference(f"{variable.domain}.{variable.variable_name}") in self._full_text_preferred
        )

    def _resolve_variable(self, reference: str) -> ExtractionVariable | None:
        key = _normalize_variable_reference(reference)
        return self._variables_by_path.get(key) or self._variables_by_short_name.get(key)


class HybridSemanticEvidenceBuilder:
    """human readable hint: retrieve semantic rescue chunks from schema anchors only."""

    def __init__(self, schema: DynamicExtractionSchema, config: HybridRescueConfig) -> None:
        self.schema = schema
        self.config = config
        self._selector_cache: dict[tuple[str, ...], RelevanceSelector] = {}

    def build_context(
        self,
        *,
        paper_id: str,
        title: str,
        primary_context: str,
        variables: Iterable[ExtractionVariable],
    ) -> str:
        """human readable hint: make a constrained prompt context for semantic rescue."""

        selected_variables = tuple(variables)
        chunks = self._build_chunks(paper_id=paper_id, title=title, primary_context=primary_context)
        if not selected_variables or not chunks:
            return ""

        selector = self._selector_for_variables(selected_variables)
        selected, _scores, _usage = selector.select(
            chunks=chunks,
            top_k=self.config.top_k,
            score_threshold=self.config.score_threshold,
        )
        if not selected:
            return ""
        return self._format_selected_chunks(paper_id=paper_id, title=title, selected=selected)

    def _selector_for_variables(self, variables: tuple[ExtractionVariable, ...]) -> RelevanceSelector:
        variable_keys = tuple(sorted(variable.value_path for variable in variables))
        cached = self._selector_cache.get(variable_keys)
        if cached is not None:
            return cached

        examples = self._positive_examples(variables) + self._negative_examples()
        if not any(example["label"] == "POS" for example in examples):
            raise ValueError(
                "Hybrid data-extraction rescue is enabled, but selected schema variables have no semantic_anchors."
            )
        selector = RelevanceSelector(
            embedder=EmbeddingBackend(),
            examples=examples,
            always_include_kinds=("title",),
        )
        self._selector_cache[variable_keys] = selector
        return selector

    @staticmethod
    def _positive_examples(variables: tuple[ExtractionVariable, ...]) -> list[LabeledExample]:
        examples: list[LabeledExample] = []
        for variable in variables:
            for anchor in variable.semantic_anchors:
                text = _clean_text(anchor)
                if text:
                    examples.append({"label": "POS", "text": text})
        return examples

    @staticmethod
    def _negative_examples() -> list[LabeledExample]:
        configured_path = PATH_SETTINGS.get("knowledge_base_file")
        if not configured_path:
            return []
        path = Path(configured_path)
        if not path.exists():
            return []
        try:
            examples = load_labeled_examples(str(path))
        except Exception:
            return []
        return [{"label": "NEG", "text": example["text"]} for example in examples if example["label"] == "NEG"]

    def _build_chunks(self, *, paper_id: str, title: str, primary_context: str) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        if title:
            chunks.append(
                {
                    "paper_id": paper_id,
                    "chunk_id": f"{paper_id}::title::0000",
                    "kind": "title",
                    "text": f"Title: {title}",
                }
            )

        main_text, supplemental_text = _split_primary_context(primary_context)
        if main_text:
            chunks.extend(
                chunk_fulltext_sentences(
                    paper_id=paper_id,
                    title=title,
                    full_text=main_text,
                    language=self.config.language,
                )
            )
        if supplemental_text:
            supplemental_chunks = chunk_fulltext_sentences(
                paper_id=f"{paper_id}::supplemental",
                title=title,
                full_text=supplemental_text,
                language=self.config.language,
            )
            for chunk in supplemental_chunks:
                item = dict(chunk)
                item["kind"] = "supplemental_cited_evidence"
                chunks.append(item)
        return chunks

    @staticmethod
    def _format_selected_chunks(paper_id: str, title: str, selected: list[dict[str, Any]]) -> str:
        parts = [f"Paper ID: {paper_id}"]
        if title:
            parts.append(f"Title: {title}")
        parts.append(
            "[Semantic Rescue Evidence]\n"
            "The extraction model may quote only from these retrieved chunks and title metadata shown above."
        )
        for index, chunk in enumerate(selected, start=1):
            score = float(chunk.get("score", 0.0) or 0.0)
            kind = str(chunk.get("kind") or "chunk")
            location = _chunk_location(chunk)
            text = _clean_text(chunk.get("text"))
            parts.append(f"[Chunk {index} | kind={kind} | score={score:.4f}{location}]\n{text}")
        return "\n\n".join(parts)


class HybridRescueSelector:
    """human readable hint: keep primary full text unless semantic rescue is clearly better."""

    def __init__(self, planner: HybridRescuePlanner) -> None:
        self.planner = planner

    def decide(
        self,
        *,
        paper_id: str,
        variable: ExtractionVariable,
        primary_payload: dict[str, Any],
        rescue_payload: dict[str, Any],
    ) -> HybridRescueDecision:
        primary_value = _display_value(_value_for_variable(primary_payload, variable))
        primary_quote = _display_value(_quote_for_variable(primary_payload, variable))
        rescue_value = _display_value(_value_for_variable(rescue_payload, variable))
        rescue_quote = _display_value(_quote_for_variable(rescue_payload, variable))

        primary_present = not _is_missing(primary_value)
        primary_quote_present = not _is_missing(primary_quote)
        rescue_present = not _is_missing(rescue_value)
        rescue_quote_present = not _is_missing(rescue_quote)
        full_text_preferred = self.planner.is_full_text_preferred(variable)

        selected_value = primary_value
        selected_quote = primary_quote
        evidence_mode = "primary_full_text"
        reason = "primary_full_text_retained"

        if full_text_preferred and primary_present and primary_quote_present:
            reason = "full_text_preferred_variable_primary_quote_supported"
        elif not primary_present and rescue_present and rescue_quote_present:
            selected_value = rescue_value
            selected_quote = rescue_quote
            evidence_mode = "semantic_rescue"
            reason = "primary_missing_semantic_quote_supported"
        elif primary_present and not primary_quote_present and rescue_present and rescue_quote_present:
            selected_value = rescue_value
            selected_quote = rescue_quote
            evidence_mode = "semantic_rescue"
            reason = "primary_quote_missing_semantic_quote_supported"
        elif not primary_quote_present and rescue_quote_present and rescue_present:
            selected_value = rescue_value
            selected_quote = rescue_quote
            evidence_mode = "semantic_rescue"
            reason = "semantic_quote_support_stronger"
        elif rescue_present and rescue_quote_present and not full_text_preferred and _is_vague(primary_value):
            selected_value = rescue_value
            selected_quote = rescue_quote
            evidence_mode = "semantic_rescue"
            reason = "semantic_rescue_less_vague_with_quote"
        elif rescue_present and not rescue_quote_present:
            reason = "semantic_rescue_rejected_missing_quote"
        elif not rescue_present:
            reason = "semantic_rescue_rejected_missing_value"

        return HybridRescueDecision(
            paper_id=paper_id,
            variable=f"{variable.domain}.{variable.variable_name}",
            primary_full_text_value=primary_value,
            primary_full_text_quote=primary_quote,
            semantic_rescue_value=rescue_value,
            semantic_rescue_quote=rescue_quote,
            selected_value=selected_value,
            selected_quote=selected_quote,
            evidence_mode_used=evidence_mode,
            selection_reason=reason,
            domain=variable.domain,
        )


class HybridRescueRunWriter:
    """human readable hint: write live run-level hybrid audit files for human reviewers."""

    audit_headers = [
        "paper_id",
        "variable",
        "primary_full_text_value",
        "primary_full_text_quote",
        "semantic_rescue_value",
        "semantic_rescue_quote",
        "selected_value",
        "selected_quote",
        "evidence_mode_used",
        "selection_reason",
    ]
    selected_headers = ["paper_id", "variable", "selected_value", "selected_quote", "evidence_mode_used", "selection_reason"]

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.decisions: list[HybridRescueDecision] = []
        self.audit_path = output_dir / "data_extraction_hybrid_rescue_audit.csv"
        self.selected_path = output_dir / "data_extraction_hybrid_selected_values.csv"
        self.summary_path = output_dir / "data_extraction_hybrid_summary.md"

    def reset(self) -> None:
        """human readable hint: QC-only runs start fresh while remaining runs can append naturally."""

        self.decisions = []
        for path in (self.audit_path, self.selected_path, self.summary_path):
            if path.exists():
                path.unlink()

    def add_decisions(self, decisions: Iterable[HybridRescueDecision]) -> None:
        """human readable hint: update CSVs after each paper so interrupted runs remain inspectable."""

        self.decisions.extend(decisions)
        self.write()

    def write(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._write_audit()
        self._write_selected()
        self._write_summary()

    def _write_audit(self) -> None:
        with self.audit_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.audit_headers)
            writer.writeheader()
            for decision in self.decisions:
                writer.writerow({header: getattr(decision, header) for header in self.audit_headers})

    def _write_selected(self) -> None:
        with self.selected_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.selected_headers)
            writer.writeheader()
            for decision in self.decisions:
                writer.writerow({header: getattr(decision, header) for header in self.selected_headers})

    def _write_summary(self) -> None:
        total = len(self.decisions)
        changed = sum(1 for item in self.decisions if item.evidence_mode_used == "semantic_rescue")
        rejected = total - changed
        by_reason: dict[str, int] = {}
        by_domain: dict[str, int] = {}
        for item in self.decisions:
            by_reason[item.selection_reason] = by_reason.get(item.selection_reason, 0) + 1
            by_domain[item.domain] = by_domain.get(item.domain, 0) + 1

        lines = [
            "# Data Extraction Hybrid Rescue Summary",
            "",
            f"- audited_variables: {total}",
            f"- semantic_rescue_selected: {changed}",
            f"- primary_full_text_retained: {rejected}",
            "",
            "## Selection Reasons",
        ]
        lines.extend(f"- {reason}: {count}" for reason, count in sorted(by_reason.items()))
        lines.extend(["", "## Audited Domains"])
        lines.extend(f"- {domain}: {count}" for domain, count in sorted(by_domain.items()))
        self.summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def apply_hybrid_selected_values(payload: dict[str, Any], decisions: Iterable[HybridRescueDecision]) -> dict[str, Any]:
    """human readable hint: return a copy where selected hybrid values are materialized for optional downstream use."""

    updated = dict(payload)
    extracted = updated.get("extracted_data")
    if not isinstance(extracted, dict):
        return updated
    copied = {domain: dict(values) if isinstance(values, dict) else values for domain, values in extracted.items()}
    for decision in decisions:
        if decision.evidence_mode_used != "semantic_rescue":
            continue
        domain, variable_name = decision.variable.split(".", 1)
        domain_payload = copied.get(domain)
        if not isinstance(domain_payload, dict):
            continue
        domain_payload[f"{variable_name}_value"] = decision.selected_value
        domain_payload[f"{variable_name}_quote"] = decision.selected_quote
    updated["extracted_data"] = copied
    updated["extracted_data_flat"] = {
        f"{domain}.{key}": value
        for domain, values in copied.items()
        if isinstance(values, dict)
        for key, value in values.items()
    }
    return updated


def _list_setting(key: str) -> list[Any]:
    value = LLM_SETTINGS.get(key, [])
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_path(value: str) -> str:
    return re.sub(r"[^a-z0-9.]+", "_", str(value or "").strip().casefold()).strip("_")


def _normalize_variable_reference(value: str) -> str:
    text = _normalize_path(value)
    text = re.sub(r"_value$", "", text)
    return text


def _value_for_variable(payload: dict[str, Any], variable: ExtractionVariable) -> Any:
    domain_payload = payload.get(variable.domain) if isinstance(payload, dict) else None
    if not isinstance(domain_payload, dict):
        return None
    return domain_payload.get(variable.value_key)


def _quote_for_variable(payload: dict[str, Any], variable: ExtractionVariable) -> Any:
    domain_payload = payload.get(variable.domain) if isinstance(payload, dict) else None
    if not isinstance(domain_payload, dict):
        return None
    return domain_payload.get(variable.quote_key)


def _display_value(value: Any) -> str:
    if value is None:
        return "Not Available"
    if isinstance(value, list):
        items = [_clean_text(item) for item in value if _clean_text(item)]
        return "; ".join(items) if items else "Not Available"
    text = _clean_text(value)
    return text if text else "Not Available"


def _is_missing(value: Any) -> bool:
    if isinstance(value, list):
        return not any(not _is_missing(item) for item in value)
    return str(value or "").strip().casefold() in MISSING_TEXT_VALUES


def _is_vague(value: str) -> bool:
    text = str(value or "").strip().casefold()
    return text in {"yes", "true", "reported", "present", "mentioned", "unclear", "mixed", "various"}


def _split_primary_context(context: str) -> tuple[str, str]:
    marker = "[Full Normalized Text]\n"
    if marker not in context:
        return context, ""
    body = context.split(marker, 1)[1]
    supplemental_markers = (
        "\n\n[Supplemental Cited Evidence]",
        "\n[Supplemental Cited Evidence]",
    )
    for supplemental_marker in supplemental_markers:
        if supplemental_marker in body:
            main_text, supplemental = body.split(supplemental_marker, 1)
            return main_text.strip(), ("[Supplemental Cited Evidence]" + supplemental).strip()
    return body.strip(), ""


def _chunk_location(chunk: dict[str, Any]) -> str:
    page_start = chunk.get("page_start")
    page_end = chunk.get("page_end")
    line_start = chunk.get("line_start")
    line_end = chunk.get("line_end")
    parts: list[str] = []
    if page_start:
        page_text = f"page={page_start}"
        if page_end and page_end != page_start:
            page_text += f"-{page_end}"
        parts.append(page_text)
    if line_start:
        line_text = f"line={line_start}"
        if line_end and line_end != line_start:
            line_text += f"-{line_end}"
        parts.append(line_text)
    return " | " + " | ".join(parts) if parts else ""
