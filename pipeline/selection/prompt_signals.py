"""Prompt-derived screening and retrieval signal helpers."""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping


# human readable hint: this pattern intentionally never matches; it disables topic logic until prompts/KBs provide terms.
NO_TOPIC_SIGNAL_PATTERN = re.compile(r"a^")
NEVER_MATCH_PATTERN = re.compile(r"(?!)")


# human readable hint: these are generic paper-section labels used to rescue relevant methods/results chunks.
SECTION_RESCUE_KEYWORDS = (
    "introduction",
    "background",
    "method",
    "methods",
    "methodology",
    "materials and methods",
    "participant",
    "participants",
    "intervention",
    "procedure",
    "outcome",
    "results",
    "discussion",
    "conclusion",
    "conclusions",
    "trial",
    "protocol",
)

DEFAULT_PRIMARY_TOPIC_SIGNAL_TERMS: tuple[str, ...] = ()
DEFAULT_SECONDARY_TOPIC_SIGNAL_TERMS: tuple[str, ...] = ()
INTERVENTION_SIGNAL_PATTERN = NO_TOPIC_SIGNAL_PATTERN
PRIMARY_TOPIC_SIGNAL_PATTERN = NO_TOPIC_SIGNAL_PATTERN
SECONDARY_TOPIC_SIGNAL_PATTERN = NO_TOPIC_SIGNAL_PATTERN

MONITORING_SIGNAL_SEED_PATTERN = re.compile(
    r"\b(monitor|assess|evaluat|feasib|usabil|acceptab|observ|classif|predict|detect|framework|protocol|pilot|measur|diagnos|benchmark)\w*\b",
    re.IGNORECASE,
)
INTERVENTION_ACTION_SEED_PATTERN = re.compile(
    r"\b(interven|randomi|trial|assign|arm|program|coach|feedback|counsel|behavior|treat|support|nudge|goal|recommend|prescrib|prompt|deliver)\w*\b",
    re.IGNORECASE,
)
KB_SIGNAL_CONTEXT_STOPWORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "with",
        "without",
        "from",
        "into",
        "onto",
        "this",
        "that",
        "these",
        "those",
        "only",
        "using",
        "used",
        "via",
        "among",
        "between",
        "within",
        "across",
        "about",
        "their",
        "there",
        "where",
        "which",
        "while",
        "during",
        "after",
        "before",
    }
)

CORE_SCREENING_SCHEMA_FIELDS = {
    "step_by_step_deliberation",
    "is_eligible",
    "confidence_score",
    "justification",
    "exclusion_reason_category",
    "seed_references",
}
EXCLUSION_FIELD_PREFIXES = (
    "no_",
    "not_",
    "wrong_",
    "insufficient_",
    "language_",
    "full_",
    "outside_",
    "without_",
    "exclude_",
    "non_",
)
LEGACY_DEFAULT_EXCLUSION_KEYS: tuple[str, ...] = ()


def normalize_schema_key(value: str) -> str:
    """human readable hint: normalize human labels into stable snake_case schema keys."""

    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def build_study_tag_field_keys(tags: Iterable[str]) -> tuple[str, ...]:
    """human readable hint: convert user-editable study tags into JSON field-key candidates."""

    keys = {
        normalize_schema_key(tag)
        for tag in tags
        if normalize_schema_key(tag)
    }
    return tuple(sorted(keys))


def looks_like_exclusion_field(field_name: str) -> bool:
    """human readable hint: identify exclusion-flag fields without hardcoding one review topic."""

    key = normalize_schema_key(field_name)
    if not key or key in CORE_SCREENING_SCHEMA_FIELDS:
        return False
    if key.startswith(EXCLUSION_FIELD_PREFIXES):
        return True
    if key.endswith("_context"):
        return True
    return False


def select_topic_absence_reason_key(
    reason_keys: Iterable[str],
    topic_terms: Iterable[str],
    preferred_key: str | None = None,
) -> str | None:
    """human readable hint: match absence-reason keys to topic terms configured in prompts/KBs."""

    normalized_keys = [key for key in {normalize_schema_key(k) for k in reason_keys} if key]
    if preferred_key and preferred_key in normalized_keys:
        return preferred_key

    topic_tokens = {
        token
        for term in topic_terms
        for token in re.findall(r"[a-z0-9]+", str(term).lower())
        if len(token) >= 3
    }
    for key in normalized_keys:
        if not key.startswith("no_"):
            continue
        key_tokens = set(key.split("_"))
        if key_tokens & topic_tokens:
            return key
    return None


def _normalize_prompt_heading(value: str) -> str:
    """human readable hint: normalize prompt headings before extracting Include/Exclude term lists."""

    heading = re.sub(r"\s+", " ", (value or "").strip().lower()).strip(":")
    heading = heading.replace("\\", "/")
    heading = heading.replace(" / ", "/").replace("/ ", "/").replace(" /", "/")
    return heading


def _split_prompt_terms(value: str) -> list[str]:
    """human readable hint: split scientist-written prompt term lists into clean lexical cues."""

    if not value:
        return []

    raw = str(value).replace("\u2013", ",").replace("\u2014", ",")
    parts = [part.strip() for part in raw.split(";")]
    if len(parts) == 1:
        parts = [part.strip() for part in raw.split(",")]

    cleaned: list[str] = []
    seen: set[str] = set()
    for part in parts:
        term = re.sub(r"\s+", " ", part).strip(" .:-")
        term = re.sub(r"^(?:for example|e\.g\.)\s+", "", term, flags=re.IGNORECASE)
        term = term.strip()
        if len(term) < 3 or len(term) > 120:
            continue
        lowered = term.lower()
        if lowered in {"include", "exclude", "and", "or"}:
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        cleaned.append(lowered)
    return cleaned


def _extract_prompt_rule_terms(
    prompt_template: str,
    section_aliases: set[str],
    rule_label: str,
) -> list[str]:
    """human readable hint: read `- Include:` or `- Exclude:` lines from named prompt sections."""

    aliases = {_normalize_prompt_heading(alias) for alias in section_aliases}
    rule = str(rule_label or "").strip().lower()
    if not rule:
        return []

    active_section: str | None = None
    extracted: list[str] = []

    for raw_line in (prompt_template or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue

        if not line.startswith("-"):
            heading = _normalize_prompt_heading(line)
            active_section = heading if heading in aliases else None
            continue

        if active_section is None:
            continue

        match = re.match(
            rf"^-\s*{re.escape(rule)}\s*:\s*(.+)$",
            line,
            flags=re.IGNORECASE,
        )
        if not match:
            continue
        extracted.extend(_split_prompt_terms(match.group(1)))

    deduped: list[str] = []
    seen: set[str] = set()
    for term in extracted:
        if term in seen:
            continue
        seen.add(term)
        deduped.append(term)
    return deduped


def _extract_prompt_include_terms(prompt_template: str, section_aliases: set[str]) -> list[str]:
    """human readable hint: extract prompt Include terms from configured section aliases."""

    return _extract_prompt_rule_terms(prompt_template, section_aliases, "include")


def _extract_prompt_exclude_terms(prompt_template: str, section_aliases: set[str]) -> list[str]:
    """human readable hint: extract prompt Exclude terms from configured section aliases."""

    return _extract_prompt_rule_terms(prompt_template, section_aliases, "exclude")


def _normalize_signal_term(term: str) -> str:
    """human readable hint: normalize one lexical signal before regex compilation."""

    value = re.sub(r"\s+", " ", str(term or "").strip().lower())
    value = value.strip(" .;:,")
    if not value or len(value) < 3 or len(value) > 80:
        return ""
    if re.fullmatch(r"[\W_]+", value):
        return ""
    return value


def _compile_signal_pattern_from_terms(
    terms: Iterable[str],
    fallback_pattern: re.Pattern[str],
    max_terms: int = 120,
) -> tuple[re.Pattern[str], tuple[str, ...]]:
    """human readable hint: compile prompt/KB terms into a safe regex, or use the fallback matcher."""

    normalized: list[str] = []
    seen: set[str] = set()
    for term in terms:
        cleaned = _normalize_signal_term(str(term))
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)

    if not normalized:
        return fallback_pattern, tuple()

    fragments = [re.escape(term).replace(r"\ ", r"\s+") for term in normalized]
    fragments = sorted(set(fragments), key=len, reverse=True)[: max(1, int(max_terms))]

    try:
        compiled = re.compile(r"(?<!\\w)(?:" + "|".join(fragments) + r")(?!\\w)", re.IGNORECASE)
    except re.error:
        return fallback_pattern, tuple()

    return compiled, tuple(normalized[: max(1, int(max_terms))])


def _dedupe_signal_terms(terms: Iterable[str], max_terms: int = 120) -> tuple[str, ...]:
    """human readable hint: normalize and deduplicate lexical terms while preserving order."""

    deduped: list[str] = []
    seen: set[str] = set()
    for term in terms:
        cleaned = _normalize_signal_term(str(term))
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
        if len(deduped) >= max(1, int(max_terms)):
            break
    return tuple(deduped)


def _collect_kb_seed_terms(
    examples: Iterable[Any],
    *,
    target_label: str,
    seed_pattern: re.Pattern[str],
    max_terms: int = 80,
    min_count: int = 1,
) -> tuple[str, ...]:
    """human readable hint: harvest contrastive cue terms from user KB examples."""

    label = str(target_label or "").strip().upper()
    if not label:
        return tuple()

    counts: dict[str, int] = {}
    for example in examples:
        if not isinstance(example, Mapping):
            continue
        if str(example.get("label") or "").strip().upper() != label:
            continue

        text = str(example.get("text") or "")
        if not text:
            continue

        tokens = re.findall(r"[a-z][a-z0-9\-]{2,}", text.lower())
        for idx, token in enumerate(tokens):
            if not seed_pattern.search(token):
                continue

            candidates = [token]
            if idx > 0 and tokens[idx - 1] not in KB_SIGNAL_CONTEXT_STOPWORDS:
                candidates.append(f"{tokens[idx - 1]} {token}")
            if idx + 1 < len(tokens) and tokens[idx + 1] not in KB_SIGNAL_CONTEXT_STOPWORDS:
                candidates.append(f"{token} {tokens[idx + 1]}")

            for candidate in candidates:
                cleaned = _normalize_signal_term(candidate)
                if not cleaned or len(cleaned) > 60:
                    continue
                counts[cleaned] = counts.get(cleaned, 0) + 1

    ranked = [
        term
        for term, count in sorted(
            counts.items(),
            key=lambda item: (-item[1], -len(item[0]), item[0]),
        )
        if count >= max(1, int(min_count))
    ]
    return tuple(ranked[: max(1, int(max_terms))])


def _build_section_rescue_keywords(prompt_terms: Iterable[str]) -> tuple[str, ...]:
    """human readable hint: extend generic section rescue keywords with concise prompt terms."""

    keywords: set[str] = set(SECTION_RESCUE_KEYWORDS)
    for term in prompt_terms:
        cleaned = _normalize_signal_term(str(term))
        if not cleaned:
            continue
        if len(cleaned.split()) > 4:
            continue
        if len(cleaned) > 40:
            continue
        keywords.add(cleaned)
    return tuple(sorted(keywords))


def build_prompt_signal_config(prompt_template: str) -> dict[str, Any]:
    """human readable hint: derive topic-sensitive retrieval signals from the active prompt."""

    section_aliases = _configured_prompt_signal_section_aliases()
    intervention_include = _extract_prompt_include_terms(
        prompt_template,
        section_aliases["primary"],
    )
    outcome_include = _extract_prompt_include_terms(prompt_template, section_aliases["secondary"])

    intervention_seed_terms = intervention_include
    primary_seed_terms = intervention_include if intervention_include else list(DEFAULT_PRIMARY_TOPIC_SIGNAL_TERMS)
    secondary_seed_terms = outcome_include if outcome_include else list(DEFAULT_SECONDARY_TOPIC_SIGNAL_TERMS)

    intervention_pattern, intervention_terms = _compile_signal_pattern_from_terms(
        intervention_seed_terms,
        INTERVENTION_SIGNAL_PATTERN,
    )
    primary_pattern, primary_terms = _compile_signal_pattern_from_terms(
        primary_seed_terms,
        PRIMARY_TOPIC_SIGNAL_PATTERN,
    )
    secondary_pattern, secondary_terms = _compile_signal_pattern_from_terms(
        secondary_seed_terms,
        SECONDARY_TOPIC_SIGNAL_PATTERN,
    )

    source = "prompt_criteria" if intervention_include or outcome_include else "no_prompt_signals"
    section_rescue_keywords = _build_section_rescue_keywords(
        list(intervention_include) + list(outcome_include)
    )

    return {
        "source": source,
        "intervention_pattern": intervention_pattern,
        "primary_pattern": primary_pattern,
        "secondary_pattern": secondary_pattern,
        "intervention_terms": intervention_terms,
        "primary_terms": primary_terms,
        "secondary_terms": secondary_terms,
        "section_rescue_keywords": section_rescue_keywords,
    }


def build_monitoring_signal_config(
    prompt_template: str,
    topic_signal_config: dict[str, Any],
    kb_examples: Iterable[Any],
) -> dict[str, Any]:
    """human readable hint: build monitoring/action cues from prompt and user KB examples."""

    section_aliases = _configured_prompt_signal_section_aliases()
    intervention_section_aliases = section_aliases["primary"]
    outcome_section_aliases = section_aliases["secondary"]

    prompt_intervention_terms = _extract_prompt_include_terms(
        prompt_template,
        intervention_section_aliases,
    )
    prompt_outcome_terms = _extract_prompt_include_terms(prompt_template, outcome_section_aliases)
    prompt_outcome_exclude_terms = _extract_prompt_exclude_terms(prompt_template, outcome_section_aliases)

    kb_examples_list = [dict(item) for item in kb_examples if isinstance(item, Mapping)]
    kb_pos_count = sum(1 for item in kb_examples_list if str(item.get("label") or "").strip().upper() == "POS")
    kb_neg_count = sum(1 for item in kb_examples_list if str(item.get("label") or "").strip().upper() == "NEG")

    kb_pos_action_terms = _collect_kb_seed_terms(
        kb_examples_list,
        target_label="POS",
        seed_pattern=INTERVENTION_ACTION_SEED_PATTERN,
        max_terms=80,
        min_count=1,
    )
    kb_neg_monitor_terms = _collect_kb_seed_terms(
        kb_examples_list,
        target_label="NEG",
        seed_pattern=MONITORING_SIGNAL_SEED_PATTERN,
        max_terms=80,
        min_count=1,
    )
    kb_pos_monitor_terms = set(
        _collect_kb_seed_terms(
            kb_examples_list,
            target_label="POS",
            seed_pattern=MONITORING_SIGNAL_SEED_PATTERN,
            max_terms=80,
            min_count=2,
        )
    )

    topic_intervention_terms = tuple(topic_signal_config.get("intervention_terms") or ())
    action_seed_terms = [
        term
        for term in list(prompt_intervention_terms) + list(topic_intervention_terms)
        if INTERVENTION_ACTION_SEED_PATTERN.search(term)
    ]
    action_seed_terms.extend(kb_pos_action_terms)
    action_seed_terms = list(_dedupe_signal_terms(action_seed_terms, max_terms=120))

    intervention_action_pattern, intervention_action_terms = _compile_signal_pattern_from_terms(
        action_seed_terms,
        topic_signal_config.get("intervention_pattern") or NEVER_MATCH_PATTERN,
        max_terms=120,
    )

    monitoring_prompt_terms = [
        term
        for term in (list(prompt_outcome_terms) + list(prompt_outcome_exclude_terms))
        if MONITORING_SIGNAL_SEED_PATTERN.search(term)
    ]
    monitoring_seed_terms = list(monitoring_prompt_terms) + list(kb_neg_monitor_terms)

    action_term_set = {
        _normalize_signal_term(term)
        for term in list(intervention_action_terms) + list(topic_intervention_terms)
        if _normalize_signal_term(term)
    }
    filtered_monitoring_terms = []
    for term in monitoring_seed_terms:
        cleaned = _normalize_signal_term(term)
        if not cleaned:
            continue
        if cleaned in action_term_set:
            continue
        if cleaned in kb_pos_monitor_terms:
            continue
        filtered_monitoring_terms.append(cleaned)
    filtered_monitoring_terms = list(_dedupe_signal_terms(filtered_monitoring_terms, max_terms=120))

    monitoring_pattern, monitoring_terms = _compile_signal_pattern_from_terms(
        filtered_monitoring_terms,
        NEVER_MATCH_PATTERN,
        max_terms=120,
    )

    prompt_requires_intervention = bool(prompt_intervention_terms) or bool(
        re.search(r"\bintervention(?:[- ]first)?\b", prompt_template, re.IGNORECASE)
    )
    kb_has_contrastive_examples = kb_pos_count > 0 and kb_neg_count > 0
    enabled = bool(
        prompt_requires_intervention
        and kb_has_contrastive_examples
        and monitoring_terms
        and (intervention_action_terms or topic_intervention_terms)
    )

    if enabled:
        source = "prompt_kb_dynamic"
    elif not prompt_requires_intervention:
        source = "disabled_prompt_no_intervention_scope"
    elif not kb_has_contrastive_examples:
        source = "disabled_kb_missing_pos_neg"
    elif not monitoring_terms:
        source = "disabled_no_monitoring_terms"
    else:
        source = "disabled_no_intervention_action_terms"

    if not enabled:
        monitoring_pattern = NEVER_MATCH_PATTERN

    return {
        "source": source,
        "enabled": enabled,
        "monitoring_pattern": monitoring_pattern,
        "monitoring_terms": tuple(monitoring_terms),
        "intervention_action_pattern": intervention_action_pattern,
        "intervention_action_terms": tuple(intervention_action_terms),
        "kb_pos_count": kb_pos_count,
        "kb_neg_count": kb_neg_count,
    }


def _configured_prompt_signal_section_aliases() -> dict[str, set[str]]:
    """human readable hint: prompt section names are user-configured so pipeline code is not topic-specific."""

    defaults = {"primary": set(), "secondary": set()}
    try:
        from config.user_orchestrator import PROMPT_SIGNAL_SECTION_ALIASES

        if isinstance(PROMPT_SIGNAL_SECTION_ALIASES, dict):
            for key in defaults:
                values = PROMPT_SIGNAL_SECTION_ALIASES.get(key, [])
                if isinstance(values, (list, tuple, set)):
                    defaults[key] = {str(value) for value in values if str(value).strip()}
    except Exception:
        pass
    return defaults
