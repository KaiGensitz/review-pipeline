"""KB-driven extraction schema and prompt helpers."""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, create_model


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_EXTRACTION_SCHEMA_KB = REPO_ROOT / "knowledge-base" / "data_extraction_schema.csv"
MISSING_TEXT_VALUE = "Not Available"
MISSING_TEXT_VALUES = {"", "n/a", "na", "not available", "not applicable", "none", "null", "missing"}
REQUIRED_KB_COLUMNS = {
    "domain",
    "variable_name",
    "variable_type",
    "allowed_options",
    "instruction",
}
CONSENSUS_COLUMN_FIELDS = ("consensus_column_name", "covidence_column_name")
PROMPT_MARKER_REPLACEMENTS = {
    "{extraction_domain_overview}": "domain_overview",
    "{extraction_schema_instructions}": "schema_instructions",
    "[[PIPELINE_INSERT_ACTIVE_EXTRACTION_DOMAIN_OVERVIEW_FROM_SCHEMA_CSV]]": "domain_overview",
    "[[PIPELINE_INSERT_ACTIVE_EXTRACTION_VARIABLES_AND_RESPONSE_SHAPE_FROM_SCHEMA_CSV]]": "schema_instructions",
}


def default_extraction_schema_path() -> Path:
    """human readable hint: resolve the user-configured extraction schema path with a safe repo fallback."""

    try:
        from config.user_orchestrator import PATH_SETTINGS

        configured_path = PATH_SETTINGS.get("data_extraction_schema_file")
        if configured_path:
            return Path(configured_path)
    except Exception:
        pass
    return DEFAULT_EXTRACTION_SCHEMA_KB


@dataclass(frozen=True)
class ExtractionVariable:
    """human readable hint: one row from the extraction schema KB after type and name cleanup."""

    domain: str
    variable_name: str
    variable_type: str
    allowed_options: tuple[str, ...]
    instruction: str
    covidence_column_name: str
    semantic_anchors: tuple[str, ...] = ()
    human_reviewer_instruction: str = ""
    evidence_profile: str = ""
    do_not_infer_from: str = ""

    @property
    def consensus_column_name(self) -> str:
        """human readable hint: generic name for the schema-owned human consensus/export column."""

        return self.covidence_column_name

    @property
    def value_key(self) -> str:
        return f"{self.variable_name}_value"

    @property
    def quote_key(self) -> str:
        return f"{self.variable_name}_quote"

    @property
    def value_path(self) -> str:
        return f"{self.domain}.{self.value_key}"

    @property
    def quote_path(self) -> str:
        return f"{self.domain}.{self.quote_key}"


@dataclass(frozen=True)
class SchemaEvidenceHintConfig:
    """human readable hint: tune the compact evidence map prepended to long extraction inputs."""

    enabled: bool = True
    snippets_per_variable: int = 2
    max_snippet_chars: int = 420
    max_total_chars: int = 18000
    context_lines: int = 1
    alias_map: Mapping[str, Iterable[str]] | None = None
    low_priority_patterns: tuple[str, ...] = ()


class SchemaEvidenceHintBuilder:
    """human readable hint: build generic, schema-derived snippets from normalized full text."""

    STOPWORDS = frozenset(
        {
            "about",
            "above",
            "after",
            "also",
            "abstract",
            "and",
            "author",
            "authors",
            "available",
            "baseline",
            "clinical",
            "column",
            "consensus",
            "context",
            "concepts",
            "contribution",
            "contributions",
            "data",
            "declaration",
            "declarations",
            "details",
            "domain",
            "exactly",
            "explicit",
            "explicitly",
            "extract",
            "field",
            "for",
            "from",
            "information",
            "label",
            "location",
            "look",
            "method",
            "methods",
            "missing",
            "not",
            "only",
            "overall",
            "outcomes",
            "paper",
            "participant",
            "population",
            "prefer",
            "preserving",
            "rather",
            "reported",
            "resource",
            "resources",
            "result",
            "results",
            "return",
            "says",
            "schema",
            "search",
            "sections",
            "setting",
            "source",
            "stated",
            "study",
            "summaries",
            "summary",
            "tables",
            "text",
            "that",
            "the",
            "this",
            "trial",
            "such",
            "than",
            "use",
            "using",
            "value",
            "values",
            "variable",
            "when",
            "where",
            "with",
            "energy",
        }
    )

    def __init__(
        self,
        variables: Iterable[ExtractionVariable],
        config: SchemaEvidenceHintConfig | None = None,
    ) -> None:
        self.variables = tuple(variables)
        self.config = config or SchemaEvidenceHintConfig()
        self.alias_map = self._normalize_alias_map(self.config.alias_map)
        self.low_priority_patterns = tuple(
            str(pattern).strip().casefold()
            for pattern in self.config.low_priority_patterns
            if str(pattern).strip()
        )

    def build(self, normalized_text: str) -> str:
        """human readable hint: create a short field-by-field evidence map before the full text."""

        if not self.config.enabled:
            return ""
        text = (normalized_text or "").strip()
        if not text or self.config.snippets_per_variable <= 0:
            return ""

        header_lines: list[str] = [
            "[Schema-Guided Evidence Hints]",
            (
                "These snippets are selected mechanically from the normalized full text using schema variable names, "
                "export labels, and instructions. Use them as a fast evidence map, then verify values against the full text below."
            ),
        ]
        entries: list[tuple[ExtractionVariable, list[tuple[int, str]]]] = []

        # human readable hint: first collect candidate snippets so every schema variable gets a chance before depth is added.
        for variable in self.variables:
            terms = self.terms_for_variable(variable)
            if not terms:
                continue
            # human readable hint: let the schema request deeper evidence for table/detail-sensitive fields without hardcoded variables.
            variable_max_snippets = self.config.snippets_per_variable
            if "table_sensitive" in variable.evidence_profile.casefold():
                variable_max_snippets += 2
            elif "complete_summary" in variable.evidence_profile.casefold() or "detail_sensitive" in variable.evidence_profile.casefold():
                variable_max_snippets += 1
            snippets = self.find_snippets(
                normalized_text=text,
                terms=terms,
                max_snippets=variable_max_snippets,
                max_chars=self.config.max_snippet_chars,
            )
            if not snippets:
                continue
            entries.append((variable, snippets))

        grouped_lines = list(header_lines)
        current_size = sum(len(line) for line in grouped_lines)
        truncated = False

        # human readable hint: render breadth-first so later schema domains are not starved by long early-domain evidence.
        for snippet_index in range(self.config.snippets_per_variable):
            last_domain = ""
            for variable, snippets in entries:
                if snippet_index >= len(snippets):
                    continue
                if variable.domain != last_domain:
                    domain_line = f"Domain: {variable.domain}"
                    if current_size + len(domain_line) > self.config.max_total_chars:
                        truncated = True
                        break
                    grouped_lines.append(domain_line)
                    current_size += len(domain_line)
                    last_domain = variable.domain

                line_number, snippet = snippets[snippet_index]
                label = variable.variable_name if snippet_index == 0 else f"{variable.variable_name} additional"
                line = f"- {label}: L{line_number}: {snippet}"
                if current_size + len(line) > self.config.max_total_chars:
                    truncated = True
                    break
                grouped_lines.append(line)
                current_size += len(line)
            if truncated:
                grouped_lines.append("- evidence_hints_truncated: total hint budget reached")
                break
        return "\n".join(grouped_lines) if len(grouped_lines) > 2 else ""

    def terms_for_variable(self, variable: ExtractionVariable) -> list[str]:
        """human readable hint: derive compact search terms from the schema contract, not the study topic."""

        terms: list[str] = []

        # human readable hint: user-configured aliases provide review-specific bridge terms without hardcoding them here.
        for alias_key in self._alias_keys_for_variable(variable):
            for alias in self.alias_map.get(alias_key, ()):
                if alias not in terms:
                    terms.append(alias)

        raw_text = " ".join(
            [
                variable.domain,
                variable.variable_name.replace("_", " "),
                variable.consensus_column_name,
                variable.instruction,
                variable.human_reviewer_instruction,
                variable.evidence_profile,
            ]
        )
        for token in re.findall(r"[A-Za-z][A-Za-z0-9+\-]{2,}", raw_text.casefold()):
            normalized = token.strip("-")
            if not normalized or normalized in self.STOPWORDS:
                continue
            if normalized not in terms:
                terms.append(normalized)
        return terms[:24]

    def find_snippets(
        self,
        normalized_text: str,
        terms: list[str],
        max_snippets: int,
        max_chars: int,
    ) -> list[tuple[int, str]]:
        """human readable hint: find short field-relevant snippets while preserving line numbers for quote audit."""

        if not normalized_text.strip() or not terms or max_snippets <= 0:
            return []

        pattern = re.compile(
            "|".join(r"(?<![A-Za-z0-9])" + re.escape(term) + r"(?![A-Za-z0-9])" for term in terms),
            flags=re.IGNORECASE,
        )
        lines = normalized_text.splitlines()
        scored: list[tuple[int, int, int, int, str]] = []
        seen_snippets: set[str] = set()
        for line_number, line in enumerate(lines, start=1):
            clean_line = re.sub(r"\s+", " ", line).strip()
            if not clean_line:
                continue
            matches = list(pattern.finditer(clean_line))
            if not matches:
                continue
            snippet_source = self._line_with_context(lines, line_number, clean_line)
            context_offset = max(0, snippet_source.find(clean_line))
            snippet = self.snippet_around_match(snippet_source, context_offset + matches[0].start(), max_chars=max_chars)
            normalized_snippet = snippet.casefold()
            if normalized_snippet in seen_snippets:
                continue
            seen_snippets.add(normalized_snippet)
            score = len({match.group(0).casefold() for match in pattern.finditer(snippet)})
            score += self._structured_bonus(snippet)
            if re.search(r"\d", snippet):
                score += 1
            if self._is_low_priority(snippet):
                score -= 20
            # human readable hint: prefer compact high-signal lines over huge abstracts when scores are similar.
            length_penalty = 1 if len(clean_line) > max_chars * 3 else 0
            source_penalty = 1 if self._is_low_priority(snippet) else 0
            scored.append((score, source_penalty, length_penalty, line_number, snippet))

        usable_scored = [item for item in scored if item[1] == 0] or scored
        usable_scored.sort(key=lambda item: (-item[0], item[1], item[2], item[3]))
        return [
            (line_number, snippet)
            for _score, _source_penalty, _length_penalty, line_number, snippet in usable_scored[:max_snippets]
        ]

    @staticmethod
    def snippet_around_match(line: str, match_start: int, max_chars: int) -> str:
        """human readable hint: keep evidence hints short but centered near the matched schema term."""

        clean_line = re.sub(r"\s+", " ", line).strip()
        if len(clean_line) <= max_chars:
            return clean_line
        half_window = max(60, max_chars // 2)
        start = max(0, match_start - half_window)
        end = min(len(clean_line), start + max_chars)
        if end - start < max_chars:
            start = max(0, end - max_chars)
        prefix = "..." if start > 0 else ""
        suffix = "..." if end < len(clean_line) else ""
        return prefix + clean_line[start:end].strip() + suffix

    @staticmethod
    def _normalize_alias_map(alias_map: Mapping[str, Iterable[str]] | None) -> dict[str, tuple[str, ...]]:
        """human readable hint: normalize optional user aliases once so snippet matching stays fast."""

        normalized: dict[str, tuple[str, ...]] = {}
        if not alias_map:
            return normalized
        for key, aliases in alias_map.items():
            clean_key = str(key or "").strip().casefold()
            if not clean_key:
                continue
            clean_aliases: list[str] = []
            for alias in aliases or ():
                clean_alias = str(alias or "").strip().casefold()
                if clean_alias and clean_alias not in clean_aliases:
                    clean_aliases.append(clean_alias)
            if clean_aliases:
                normalized[clean_key] = tuple(clean_aliases)
        return normalized

    @staticmethod
    def _alias_keys_for_variable(variable: ExtractionVariable) -> tuple[str, ...]:
        """human readable hint: let users target aliases broadly by domain or precisely by schema field."""

        return (
            f"{variable.domain}.{variable.variable_name}".casefold(),
            variable.variable_name.casefold(),
            f"{variable.domain}.*".casefold(),
            variable.domain.casefold(),
        )

    def _line_with_context(self, lines: list[str], line_number: int, clean_line: str) -> str:
        """human readable hint: include neighboring rows so table labels and values stay together."""

        if self.config.context_lines <= 0:
            return clean_line
        index = line_number - 1
        if self._looks_structured(clean_line):
            before = 1
            after = self.config.context_lines + 2
        else:
            before = self.config.context_lines
            after = self.config.context_lines
        start = max(0, index - before)
        end = min(len(lines), index + after + 1)
        joined = " ".join(re.sub(r"\s+", " ", item).strip() for item in lines[start:end] if item.strip())
        return joined or clean_line

    @staticmethod
    def _looks_structured(text: str) -> bool:
        """human readable hint: recognize table-like rows without knowing any study-specific columns."""

        clean = str(text or "")
        return clean.count("|") >= 2 or bool(re.search(r"\b(table|figure)\b", clean, flags=re.IGNORECASE))

    @staticmethod
    def _structured_bonus(text: str) -> int:
        """human readable hint: prefer extracted table rows because numeric participant facts often live there."""

        clean = str(text or "")
        if clean.count("|") >= 2:
            return 6
        if re.search(r"\b(table|figure)\b", clean, flags=re.IGNORECASE):
            return 2
        return 0

    def _is_low_priority(self, text: str) -> bool:
        """human readable hint: rank generic boilerplate below participant/method evidence without deleting it."""

        lowered = str(text or "").casefold()
        return any(pattern in lowered for pattern in self.low_priority_patterns)


@dataclass(frozen=True)
class DynamicExtractionSchema:
    """human readable hint: runtime Pydantic model, prompt text, and defaults generated from the CSV KB."""

    kb_path: Path
    variables: tuple[ExtractionVariable, ...]
    response_shape: dict[str, Any]
    model: type[BaseModel]
    instructions_text: str
    domain_overview_text: str
    prompt_scoped: bool = False

    @classmethod
    def from_kb(cls, kb_path: str | Path | None = None) -> "DynamicExtractionSchema":
        """human readable hint: read data_extraction_schema.csv and build the nested response model."""

        path = Path(kb_path) if kb_path else default_extraction_schema_path()
        variables = tuple(load_extraction_variables(path))
        if not variables:
            raise ValueError(f"Extraction schema KB has no variable rows: {path}")

        response_shape = build_response_shape(variables)
        model = build_pydantic_model(variables)
        instructions_text = format_instruction_block(variables, response_shape)
        domain_overview_text = format_domain_overview(variables)
        return cls(
            kb_path=path,
            variables=variables,
            response_shape=response_shape,
            model=model,
            instructions_text=instructions_text,
            domain_overview_text=domain_overview_text,
        )

    def inject_into_prompt(self, prompt_template: str) -> str:
        """human readable hint: combine the human prompt framework with the CSV machine schema at runtime."""

        prompt = prompt_template or ""
        original_prompt = prompt
        # human readable hint: Full-schema snapshots keep the scientist-written
        # conceptual framework. Scoped runtime calls, whether one domain or a
        # configured domain group, copy only matching # STEPS guidance after
        # broad conceptual sections are removed.
        guidance_text = format_prompt_domain_guidance(original_prompt, self.variables) if self.prompt_scoped else ""
        generated_block = format_generated_prompt_block(guidance_text, self.domain_overview_text, self.instructions_text)

        for placeholder, replacement_kind in PROMPT_MARKER_REPLACEMENTS.items():
            replacement = self.domain_overview_text if replacement_kind == "domain_overview" else self.instructions_text
            prompt = prompt.replace(placeholder, replacement)
        if any(placeholder in original_prompt for placeholder in PROMPT_MARKER_REPLACEMENTS):
            return prompt

        if self.prompt_scoped:
            prompt = remove_prompt_conceptual_schema_sections(prompt, remove_steps=True, remove_end_goal=True)
        else:
            prompt = remove_prompt_conceptual_schema_sections(prompt, remove_steps=False, remove_end_goal=True)

        context_match = re.search(r"(?im)^#\s*CONTEXT\b", prompt)
        if context_match:
            return (
                prompt[: context_match.start()].rstrip()
                + "\n\n"
                + generated_block
                + "\n\n"
                + prompt[context_match.start() :].lstrip()
            )
        return prompt.rstrip() + "\n\n" + generated_block

    @property
    def domains(self) -> tuple[str, ...]:
        """human readable hint: list extraction domains in the same order as the scientist-edited KB."""

        ordered: list[str] = []
        seen: set[str] = set()
        for variable in self.variables:
            if variable.domain in seen:
                continue
            seen.add(variable.domain)
            ordered.append(variable.domain)
        return tuple(ordered)

    def for_domains(self, domains: Iterable[str]) -> "DynamicExtractionSchema":
        """human readable hint: build a scoped schema for one or more configured domains."""

        requested = tuple(dict.fromkeys(str(domain) for domain in domains if str(domain or "").strip()))
        selected = tuple(variable for variable in self.variables if variable.domain in requested)
        if not selected:
            raise ValueError(f"Unknown extraction domain group: {', '.join(requested) or '<empty>'}")
        found_domains = {variable.domain for variable in selected}
        missing_domains = [domain for domain in requested if domain not in found_domains]
        if missing_domains:
            raise ValueError(f"Unknown extraction domain(s): {', '.join(missing_domains)}")
        response_shape = build_response_shape(selected)
        return DynamicExtractionSchema(
            kb_path=self.kb_path,
            variables=selected,
            response_shape=response_shape,
            model=build_pydantic_model(selected),
            instructions_text=format_instruction_block(selected, response_shape),
            domain_overview_text=format_domain_overview(selected),
            prompt_scoped=True,
        )

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """human readable hint: validate and serialize model output using the exact JSON keys requested."""

        normalized = coerce_payload_to_schema(payload, self)
        validated = self.model.model_validate(normalized)
        return validated.model_dump(mode="json", by_alias=True)

    def default_payload(self) -> dict[str, Any]:
        """human readable hint: create complete missing-data output for every KB variable."""

        return default_payload_for_variables(self.variables)

    def openai_response_format(self) -> dict[str, Any]:
        """human readable hint: convert the dynamic Pydantic model into OpenAI Structured Outputs JSON schema."""

        return {
            "type": "json_schema",
            "json_schema": {
                "name": "dynamic_data_extraction",
                "schema": self.model.model_json_schema(by_alias=True),
                "strict": True,
            },
        }


def format_generated_prompt_block(domain_guidance: str, domain_overview: str, schema_instructions: str) -> str:
    """human readable hint: show the prompt-derived research guidance next to the CSV-derived machine contract."""

    parts = [
        "# PIPELINE-GENERATED EXTRACTION CONTRACT",
        "The human prompt defines what scientific concepts to look for. The schema CSV defines the exact JSON keys, value types, missing-value rules, and human consensus/export column mapping used by the machine.",
    ]
    if domain_guidance.strip():
        parts.append(domain_guidance)
    parts.extend([domain_overview, schema_instructions])
    return "\n\n".join(part for part in parts if part.strip())


def format_prompt_domain_guidance(prompt_template: str, variables: tuple[ExtractionVariable, ...]) -> str:
    """human readable hint: select the human prompt guidance that matches the active CSV domain(s)."""

    domains = tuple(dict.fromkeys(variable.domain for variable in variables))
    selected_blocks = prompt_guidance_blocks_for_domains(prompt_template, variables)
    lines = [
        "# PROMPT-DERIVED DOMAIN GUIDANCE",
        "Use this scientist-written guidance for the active schema domain(s): " + ", ".join(domains) + ".",
    ]
    if selected_blocks:
        lines.extend(["", *selected_blocks])
    else:
        lines.extend(
            [
                "",
                "No matching conceptual prompt block was found for these domains. Use the KB-driven extraction schema below as the exact contract.",
            ]
        )
    return "\n".join(lines)


def domain_groups_for_schema(
    schema: DynamicExtractionSchema,
    configured_groups: Any,
) -> tuple[tuple[str, ...], ...]:
    """human readable hint: convert user-configured domain groups into valid schema-domain batches."""

    schema_domains = tuple(schema.domains)
    schema_domain_set = set(schema_domains)
    if not configured_groups:
        return tuple((domain,) for domain in schema_domains)

    groups: list[tuple[str, ...]] = []
    seen: set[str] = set()
    for group_index, group in enumerate(configured_groups, start=1):
        if isinstance(group, str):
            domains = (group.strip(),)
        elif isinstance(group, Iterable):
            domains = tuple(str(domain).strip() for domain in group if str(domain or "").strip())
        else:
            raise ValueError(f"Invalid data extraction domain group #{group_index}: {group!r}")
        domains = tuple(dict.fromkeys(domain for domain in domains if domain))
        unknown = [domain for domain in domains if domain not in schema_domain_set]
        if unknown:
            raise ValueError(
                "DATA_EXTRACTION domain group references unknown schema domain(s): " + ", ".join(unknown)
            )
        if domains:
            groups.append(domains)
            seen.update(domains)

    for domain in schema_domains:
        if domain not in seen:
            groups.append((domain,))
    return tuple(groups)


def prompt_guidance_blocks_for_domains(
    prompt_template: str,
    variables: tuple[ExtractionVariable, ...],
) -> list[str]:
    """human readable hint: map prompt sections to active CSV domains using schema text and user-configured aliases."""

    domains = tuple(dict.fromkeys(variable.domain for variable in variables))
    blocks = extract_prompt_guidance_blocks(prompt_template)
    selected: list[str] = []
    seen: set[str] = set()
    for block in blocks:
        if not _prompt_block_matches_domains(block, domains, variables):
            continue
        normalized = _normalize_prompt_text(block)
        if normalized in seen:
            continue
        seen.add(normalized)
        selected.append(block)
    return selected


def extract_prompt_guidance_blocks(prompt_template: str) -> list[str]:
    """human readable hint: parse the prompt's conceptual STEPS into reusable domain guidance."""

    blocks: list[str] = []
    for heading, _start, _content_start, end, text in _iter_prompt_sections(prompt_template):
        heading_key = _normalize_prompt_text(heading)
        if heading_key.startswith("steps"):
            blocks.extend(_split_numbered_prompt_blocks(text))
    return [block for block in blocks if block.strip()]


def remove_prompt_conceptual_schema_sections(
    prompt_template: str,
    *,
    remove_steps: bool = True,
    remove_end_goal: bool = True,
) -> str:
    """human readable hint: remove broad prompt sections only after their useful guidance has been preserved."""

    removals: list[tuple[int, int]] = []
    for heading, start, _content_start, end, _text in _iter_prompt_sections(prompt_template):
        heading_key = _normalize_prompt_text(heading)
        if (remove_steps and heading_key.startswith("steps")) or (
            remove_end_goal and heading_key.startswith("end goal")
        ):
            removals.append((start, end))

    prompt = prompt_template
    for start, end in reversed(removals):
        prompt = prompt[:start].rstrip() + "\n\n" + prompt[end:].lstrip()
    return prompt.strip() + "\n"


def _iter_prompt_sections(prompt_template: str) -> list[tuple[str, int, int, int, str]]:
    """human readable hint: split Markdown-like prompt headings without requiring a strict document parser."""

    prompt = prompt_template or ""
    matches = list(re.finditer(r"(?m)^#\s+(.+?)\s*$", prompt))
    sections: list[tuple[str, int, int, int, str]] = []
    for idx, match in enumerate(matches):
        heading = match.group(1).strip()
        start = match.start()
        content_start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(prompt)
        sections.append((heading, start, content_start, end, prompt[content_start:end].strip()))
    return sections


def _split_numbered_prompt_blocks(section_text: str) -> list[str]:
    """human readable hint: keep each numbered framework item as a separate candidate domain guide."""

    text = section_text.strip()
    starts = list(re.finditer(r"(?m)^\s*\d+\)\s+", text))
    if not starts:
        return [text] if text else []

    blocks: list[str] = []
    for idx, start_match in enumerate(starts):
        end = starts[idx + 1].start() if idx + 1 < len(starts) else len(text)
        block = text[start_match.start() : end].strip()
        if block:
            blocks.append(block)
    return blocks


def _prompt_block_matches_domains(
    block: str,
    domains: tuple[str, ...],
    variables: tuple[ExtractionVariable, ...],
) -> bool:
    """human readable hint: decide whether a prompt block belongs to one active CSV domain."""

    searchable = _normalize_prompt_text(block)
    domain_aliases = _aliases_for_domains(domains, variables)
    return any(_contains_normalized_phrase(searchable, alias) for alias in domain_aliases)


def _contains_normalized_phrase(searchable: str, alias: str) -> bool:
    """human readable hint: match prompt/schema labels as whole words so one domain does not catch a near-word from another domain."""

    normalized_alias = _normalize_prompt_text(alias)
    if not searchable or not normalized_alias:
        return False
    phrase_pattern = r"(?<![a-z0-9])" + r"\s+".join(re.escape(part) for part in normalized_alias.split()) + r"(?![a-z0-9])"
    return re.search(phrase_pattern, searchable) is not None


def _aliases_for_domains(domains: tuple[str, ...], variables: tuple[ExtractionVariable, ...]) -> set[str]:
    """human readable hint: combine config aliases with schema text; pipeline code stays review-topic generic."""

    aliases: set[str] = set()
    configured_aliases = _configured_domain_prompt_aliases()
    for domain in domains:
        aliases.add(_normalize_prompt_text(domain))
        for alias in configured_aliases.get(domain, ()):
            aliases.add(_normalize_prompt_text(alias))
    for variable in variables:
        aliases.add(_normalize_prompt_text(variable.variable_name))
        aliases.add(_normalize_prompt_text(variable.consensus_column_name))
    return {alias for alias in aliases if alias}


def _normalize_prompt_text(value: str) -> str:
    """human readable hint: compare prompt labels in a forgiving way while leaving original text unchanged."""

    return re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold()).strip()


def _configured_domain_prompt_aliases() -> dict[str, tuple[str, ...]]:
    """human readable hint: load optional current-study domain aliases from user_orchestrator.py, not pipeline code."""

    try:
        from config.user_orchestrator import DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES

        if isinstance(DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES, dict):
            return {
                str(domain): tuple(str(alias) for alias in aliases)
                for domain, aliases in DATA_EXTRACTION_DOMAIN_PROMPT_ALIASES.items()
                if isinstance(aliases, (list, tuple))
            }
    except Exception:
        pass
    return {}


def load_extraction_variables(kb_path: Path) -> list[ExtractionVariable]:
    """human readable hint: parse the scientist-editable CSV and fail early on malformed schema rows."""

    if not kb_path.exists():
        raise FileNotFoundError(f"Missing extraction schema KB: {kb_path}")

    with kb_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = {str(name or "").strip() for name in (reader.fieldnames or [])}
        missing = REQUIRED_KB_COLUMNS - columns
        if missing:
            raise ValueError(f"Extraction schema KB missing required columns: {sorted(missing)}")
        if not any(column_name in columns for column_name in CONSENSUS_COLUMN_FIELDS):
            raise ValueError(
                "Extraction schema KB missing required consensus/export column. "
                "Use 'consensus_column_name' for new schemas or legacy 'covidence_column_name'."
            )

        variables: list[ExtractionVariable] = []
        seen: set[tuple[str, str]] = set()
        for row_number, row in enumerate(reader, start=2):
            domain = _safe_json_key(row.get("domain", ""), label=f"domain row {row_number}", preserve_case=False)
            variable_name = _safe_json_key(
                row.get("variable_name", ""),
                label=f"variable_name row {row_number}",
                preserve_case=True,
            )
            variable_type = _normalize_variable_type(row.get("variable_type", ""))
            instruction = str(row.get("instruction") or "").strip()
            consensus_column_name = _read_consensus_column_name(row)
            allowed_options = tuple(_split_allowed_options(row.get("allowed_options", "")))
            semantic_anchors = tuple(_split_semantic_anchors(row.get("semantic_anchors", "")))
            human_reviewer_instruction = str(row.get("human_reviewer_instruction") or "").strip()
            evidence_profile = str(row.get("evidence_profile") or "").strip()
            do_not_infer_from = str(row.get("do_not_infer_from") or "").strip()

            key = (domain, variable_name)
            if key in seen:
                raise ValueError(f"Duplicate extraction variable in KB at row {row_number}: {domain}.{variable_name}")
            seen.add(key)

            if variable_type == "enum" and not allowed_options:
                raise ValueError(f"Enum variable requires allowed_options at row {row_number}: {domain}.{variable_name}")
            if not instruction:
                raise ValueError(f"Extraction variable requires an instruction at row {row_number}: {domain}.{variable_name}")
            if not consensus_column_name:
                raise ValueError(
                    f"Extraction variable requires consensus_column_name at row {row_number}: {domain}.{variable_name}"
                )

            variables.append(
                ExtractionVariable(
                    domain=domain,
                    variable_name=variable_name,
                    variable_type=variable_type,
                    allowed_options=allowed_options,
                    instruction=instruction,
                    covidence_column_name=consensus_column_name,
                    semantic_anchors=semantic_anchors,
                    human_reviewer_instruction=human_reviewer_instruction,
                    evidence_profile=evidence_profile,
                    do_not_infer_from=do_not_infer_from,
                )
            )
    return variables


def build_pydantic_model(variables: tuple[ExtractionVariable, ...]) -> type[BaseModel]:
    """human readable hint: create a nested Pydantic model where each KB variable has value and quote fields."""

    domain_fields: dict[str, dict[str, tuple[Any, Any]]] = {}
    for variable in variables:
        domain_fields.setdefault(variable.domain, {})
        domain_fields[variable.domain][variable.value_key] = (
            _annotation_for_variable(variable),
            Field(..., alias=variable.value_key, description=variable.instruction),
        )
        domain_fields[variable.domain][variable.quote_key] = (
            str | None,
            Field(..., alias=variable.quote_key, description="Exact source quote, or null when value is absent."),
        )

    root_fields: dict[str, tuple[Any, Any]] = {}
    for domain, fields in domain_fields.items():
        # cast to appease static type checkers: field defs are (annotation, Field)
        domain_model = create_model(
            _model_name(f"Extraction_{domain}"),
            __config__=ConfigDict(extra="forbid", populate_by_name=True),
            **cast(Any, fields),
        )
        root_fields[domain] = (domain_model, Field(..., alias=domain))

    # cast root_fields similarly for typing compatibility
    return create_model(
        "DynamicExtractionOutput",
        __config__=ConfigDict(extra="forbid", populate_by_name=True),
        **cast(Any, root_fields),
    )


def build_response_shape(variables: tuple[ExtractionVariable, ...]) -> dict[str, Any]:
    """human readable hint: create the JSON example shown to the LLM from the same KB rows as the model."""

    shape: dict[str, Any] = {}
    for variable in variables:
        shape.setdefault(variable.domain, {})
        shape[variable.domain][variable.value_key] = _missing_value_for_variable(variable)
        shape[variable.domain][variable.quote_key] = None
    return shape


def format_domain_overview(variables: tuple[ExtractionVariable, ...]) -> str:
    """human readable hint: summarize the active KB domains so the prompt visibly follows the schema CSV."""

    grouped: dict[str, list[str]] = {}
    for variable in variables:
        grouped.setdefault(variable.domain, []).append(variable.variable_name)

    lines = [
        "# DOMAIN-GUIDED EXTRACTION PLAN",
        "Work through the active KB domains one at a time. The domains and variables below come from data_extraction_schema.csv.",
    ]
    for domain, names in grouped.items():
        lines.append(f"- {domain}: {', '.join(names)}")
    lines.extend(
        [
            "",
            "Within each domain, first search the manuscript text and tables for directly stated evidence, then fill the exact JSON keys listed in the KB-driven schema.",
        ]
    )
    return "\n".join(lines)


def format_instruction_block(variables: tuple[ExtractionVariable, ...], response_shape: dict[str, Any]) -> str:
    """human readable hint: turn KB rows into reviewer-readable extraction instructions for the prompt."""

    lines = [
        "# KB-DRIVEN EXTRACTION SCHEMA",
        "For every variable below, return two fields: <variable_name>_value and <variable_name>_quote.",
        f'If evidence is absent, return "{MISSING_TEXT_VALUE}" for string/enum/integer/float values, false for booleans, [] for lists, and null for the quote.',
        "Before using a missing-value default, perform a focused availability sweep for that variable: search the abstract, methods, results, tables, trial/protocol sections, and the schema variable name/export-column wording plus close synonyms from its instruction.",
        "If the variable instruction names acceptable fallback evidence, such as eligibility criteria for protocol papers, that fallback counts as explicit evidence and should be extracted with a supporting quote.",
        "Use the shortest exact quote that proves the value, usually one sentence, table row, or table label plus value. Do not copy long paragraphs.",
        "",
        "Variables and instructions:",
    ]
    last_domain = ""
    for variable in variables:
        if variable.domain != last_domain:
            lines.append(f"\nDomain: {variable.domain}")
            last_domain = variable.domain
        allowed = ""
        if variable.variable_type == "enum":
            allowed_values = _enum_options_with_missing(variable)
            allowed = f" Allowed values: {', '.join(allowed_values)}."
        extras: list[str] = []
        if variable.human_reviewer_instruction:
            extras.append(f"Human reviewer guidance: {variable.human_reviewer_instruction}")
        if variable.evidence_profile:
            extras.append(f"Evidence profile: {variable.evidence_profile}")
        if variable.do_not_infer_from:
            extras.append(f"Do not infer from: {variable.do_not_infer_from}")
        extra_text = " " + " ".join(extras) if extras else ""
        lines.append(
            f"- [{variable.domain}] {variable.variable_name}: {variable.instruction}"
            f" Consensus/export column: {variable.consensus_column_name}.{allowed}{extra_text}"
        )

    lines.extend(
        [
            "",
            "Response JSON shape:",
            json.dumps(response_shape, ensure_ascii=False, indent=2),
        ]
    )
    return "\n".join(lines)


def parse_llm_payload(raw_text: str) -> dict[str, Any]:
    """human readable hint: parse the first complete JSON object from raw LLM output."""

    if not raw_text or not raw_text.strip():
        raise ValueError("empty_response")
    parsed = extract_json_object(raw_text.strip())
    if not isinstance(parsed, dict):
        raise ValueError("json_parse_failed")
    return parsed


def parse_and_validate(raw_text: str, schema: DynamicExtractionSchema) -> tuple[dict[str, Any], str | None]:
    """human readable hint: return validated KB-shaped extraction data or a complete missing-data fallback."""

    try:
        parsed = parse_llm_payload(raw_text)
        return schema.validate_payload(parsed), None
    except (ValueError, json.JSONDecodeError, ValidationError) as exc:
        error = str(exc) or exc.__class__.__name__
        return schema.default_payload(), error


def validate_llm_extraction(raw_text: str | None, schema: DynamicExtractionSchema) -> tuple[dict[str, Any], str | None]:
    """human readable hint: normalize empty/API-error LLM outputs before schema validation."""

    if not raw_text:
        return schema.default_payload(), "empty_response"
    if isinstance(raw_text, str) and raw_text.startswith("LLM error"):
        return schema.default_payload(), raw_text
    return parse_and_validate(raw_text, schema)


def extract_json_object(text: str) -> dict[str, Any]:
    """human readable hint: recover one balanced JSON object from providers that add stray text."""

    start = text.find("{")
    if start < 0:
        raise ValueError("No JSON object found.")

    in_string = False
    escaped = False
    depth = 0
    for idx in range(start, len(text)):
        char = text[idx]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start : idx + 1])
    raise ValueError("Unbalanced JSON object.")


def coerce_payload_to_schema(payload: dict[str, Any], schema: DynamicExtractionSchema) -> dict[str, Any]:
    """human readable hint: accept minor legacy shapes, then coerce values into the KB-defined schema."""

    coerced = schema.default_payload()
    source = payload if isinstance(payload, dict) else {}
    for variable in schema.variables:
        domain_source = source.get(variable.domain)
        if not isinstance(domain_source, dict):
            continue

        value = domain_source.get(variable.value_key)
        quote = domain_source.get(variable.quote_key)

        legacy_leaf = domain_source.get(variable.variable_name)
        if isinstance(legacy_leaf, dict):
            value = legacy_leaf.get("value", legacy_leaf.get("_value", value))
            quote = legacy_leaf.get("quote", legacy_leaf.get("_quote", quote))

        coerced[variable.domain][variable.value_key] = coerce_value_for_variable(value, variable)
        coerced[variable.domain][variable.quote_key] = _coerce_quote(quote, coerced[variable.domain][variable.value_key])
    return coerced


def coerce_value_for_variable(value: Any, variable: ExtractionVariable) -> Any:
    """human readable hint: normalize missing and common CSV/JSON representations by KB variable type."""

    if _is_missing_value(value, variable):
        return _missing_value_for_variable(variable)

    if variable.variable_type == "boolean":
        return _coerce_bool(value)
    if variable.variable_type == "list":
        return _coerce_list(value)
    if variable.variable_type == "integer":
        try:
            return int(str(value).strip())
        except Exception:
            return MISSING_TEXT_VALUE
    if variable.variable_type == "float":
        try:
            return float(str(value).strip())
        except Exception:
            return MISSING_TEXT_VALUE
    if variable.variable_type == "enum":
        text = str(value).strip()
        lookup = {option.casefold(): option for option in _enum_options_with_missing(variable)}
        return lookup.get(text.casefold(), text)
    return str(value).strip()


def default_payload_for_variables(variables: tuple[ExtractionVariable, ...]) -> dict[str, Any]:
    """human readable hint: build a full no-evidence extraction object for failed or skipped papers."""

    payload: dict[str, Any] = {}
    for variable in variables:
        payload.setdefault(variable.domain, {})
        payload[variable.domain][variable.value_key] = _missing_value_for_variable(variable)
        payload[variable.domain][variable.quote_key] = None
    return payload


def flatten_extracted_data(payload: Any, prefix: str = "") -> dict[str, str]:
    """human readable hint: flatten nested KB extraction JSON into stable dot-path CSV columns."""

    flat: dict[str, str] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            full_key = f"{prefix}.{key}" if prefix else str(key)
            flat.update(flatten_extracted_data(value, full_key))
        return flat
    if isinstance(payload, list):
        flat[prefix] = "; ".join(str(item) for item in payload if item is not None)
        return flat
    if prefix:
        flat[prefix] = "" if payload is None else str(payload)
    return flat


def _annotation_for_variable(variable: ExtractionVariable) -> Any:
    """human readable hint: map KB type names to Python/Pydantic field types."""

    if variable.variable_type == "enum":
        return cast(Any, Literal)[tuple(_enum_options_with_missing(variable))]
    if variable.variable_type == "boolean":
        return bool
    if variable.variable_type == "list":
        return list[str]
    if variable.variable_type == "integer":
        return int | Literal[MISSING_TEXT_VALUE]
    if variable.variable_type == "float":
        return float | Literal[MISSING_TEXT_VALUE]
    return str


def _enum_options_with_missing(variable: ExtractionVariable) -> tuple[str, ...]:
    """human readable hint: make Not Available a legal enum answer even if scientists omit it in the CSV."""

    values = list(variable.allowed_options)
    if not any(option.casefold() == MISSING_TEXT_VALUE.casefold() for option in values):
        values.append(MISSING_TEXT_VALUE)
    return tuple(values)


def _missing_value_for_variable(variable: ExtractionVariable) -> Any:
    """human readable hint: centralize the required missing-value convention by variable type."""

    if variable.variable_type == "boolean":
        return False
    if variable.variable_type == "list":
        return []
    return MISSING_TEXT_VALUE


def _is_missing_value(value: Any, variable: ExtractionVariable) -> bool:
    """human readable hint: identify absent data without confusing false/list defaults with present findings."""

    if value is None:
        return True
    if variable.variable_type == "boolean":
        if isinstance(value, bool):
            return value is False
        text = str(value).strip().casefold()
        return text in MISSING_TEXT_VALUES or text in {"false", "0", "no", "n"}
    if variable.variable_type == "list":
        if isinstance(value, list):
            return len([item for item in value if str(item).strip()]) == 0
        return str(value).strip().casefold() in MISSING_TEXT_VALUES
    return str(value).strip().casefold() in MISSING_TEXT_VALUES


def _coerce_quote(value: Any, coerced_value: Any) -> str | None:
    """human readable hint: quotes are kept only when the value itself is not the missing-data default."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if coerced_value is False or coerced_value == [] or str(coerced_value).strip().casefold() in MISSING_TEXT_VALUES:
        return None
    return text


def _coerce_bool(value: Any) -> bool:
    """human readable hint: accept common human/CSV spellings for yes/no fields."""

    if isinstance(value, bool):
        return value
    text = str(value).strip().casefold()
    return text in {"true", "1", "yes", "y", "present", "explicit", "reported"}


def _coerce_list(value: Any) -> list[str]:
    """human readable hint: accept JSON arrays and comma/semicolon human-export multi-select values."""

    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    parts = re.split(r"[;,|\n]", text)
    return [part.strip() for part in parts if part.strip()]


def _normalize_variable_type(value: str) -> str:
    """human readable hint: tolerate scientist-friendly type labels while keeping behavior explicit."""

    normalized = str(value or "").strip().casefold().replace(" ", "_").replace("-", "_")
    aliases = {
        "enum": "enum",
        "categorical": "enum",
        "category": "enum",
        "boolean": "boolean",
        "bool": "boolean",
        "list": "list",
        "array": "list",
        "list_str": "list",
        "list[string]": "list",
        "string": "string",
        "str": "string",
        "text": "string",
        "integer": "integer",
        "int": "integer",
        "float": "float",
        "number": "float",
    }
    if normalized not in aliases:
        raise ValueError(f"Unsupported extraction variable_type: {value!r}")
    return aliases[normalized]


def _split_allowed_options(value: str | None) -> list[str]:
    """human readable hint: parse allowed enum options from pipe, semicolon, or comma separated cells."""

    if not value:
        return []
    parts = re.split(r"\s*(?:\||;|,)\s*", str(value))
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        cleaned = part.strip()
        if not cleaned or cleaned.casefold() in seen:
            continue
        seen.add(cleaned.casefold())
        deduped.append(cleaned)
    return deduped


def _split_semantic_anchors(value: str | None) -> list[str]:
    """human readable hint: parse optional schema-owned semantic retrieval targets."""

    if not value:
        return []
    normalized = str(value).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []
    parts = re.split(r"\n+|\s*\|\|\s*", normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        cleaned = part.strip()
        if not cleaned or cleaned.casefold() in seen:
            continue
        seen.add(cleaned.casefold())
        deduped.append(cleaned)
    return deduped


def _read_consensus_column_name(row: dict[str, Any]) -> str:
    """human readable hint: prefer generic schema naming while accepting legacy schema CSVs."""

    for column_name in CONSENSUS_COLUMN_FIELDS:
        value = str(row.get(column_name) or "").strip()
        if value:
            return value
    return ""


def _safe_json_key(value: str | None, *, label: str, preserve_case: bool = False) -> str:
    """human readable hint: convert KB labels into JSON-safe snake_case keys while rejecting blank names."""

    raw = str(value or "").strip()
    if not preserve_case:
        raw = raw.lower()
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", raw)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        raise ValueError(f"Blank {label} in extraction schema KB.")
    if cleaned[0].isdigit():
        cleaned = f"field_{cleaned}"
    return cleaned


def _model_name(value: str) -> str:
    """human readable hint: create valid generated class names for nested Pydantic models."""

    parts = [part.capitalize() for part in re.split(r"[^A-Za-z0-9]+", value) if part]
    return "".join(parts) or "ExtractionDomain"
