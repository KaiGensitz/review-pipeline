"""Prompt-template loading and optional criteria injection."""

from __future__ import annotations

import sys
from pathlib import Path

from config.user_orchestrator import PATH_SETTINGS, PROMPT_FILES


ELIGIBILITY_CRITERIA_PLACEHOLDER = "{eligibility_criteria}"


def load_optional_eligibility_criteria_text() -> str:
    """human readable hint: load shared eligibility criteria text when configured and available."""

    configured_path = PATH_SETTINGS.get("eligibility_criteria_file")
    if not configured_path:
        return ""

    criteria_path = Path(configured_path)
    if not criteria_path.exists():
        print(
            f"[warning] eligibility criteria file not found at: {criteria_path}. "
            "Continuing without criteria injection.",
            file=sys.stderr,
        )
        return ""

    return criteria_path.read_text(encoding="utf-8").strip()


def load_stage_prompt_template(stage: str) -> str:
    """human readable hint: load the stage prompt and inject shared criteria only when requested."""

    prompt_path = PROMPT_FILES.get(stage)
    if not prompt_path:
        raise ValueError(f"Missing prompt mapping for stage '{stage}'.")

    prompt_template = prompt_path.read_text(encoding="utf-8")
    if ELIGIBILITY_CRITERIA_PLACEHOLDER not in prompt_template:
        return prompt_template.strip()

    criteria_text = load_optional_eligibility_criteria_text()
    if not criteria_text:
        print(
            "[warning] prompt contains {eligibility_criteria} but no criteria text was loaded; "
            "continuing with an empty replacement.",
            file=sys.stderr,
        )

    return prompt_template.replace(ELIGIBILITY_CRITERIA_PLACEHOLDER, criteria_text).strip()
