"""Pydantic schemas for LLM screening decisions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ScreeningDecisionBaseModel(BaseModel):
    """human readable hint: shared strict fields expected from every screening LLM response."""

    model_config = ConfigDict(extra="allow")

    step_by_step_deliberation: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    justification: str = Field(min_length=1)
    exclusion_reason_category: str | None = None

    @model_validator(mode="after")
    def _check_reason_for_exclusion(self) -> "ScreeningDecisionBaseModel":
        """human readable hint: exclusion decisions must carry an explicit exclusion reason."""

        if self.is_eligible is False and not self.exclusion_reason_category:
            raise ValueError("exclusion_reason_category is required when is_eligible is false")
        return self


class TitleAbstractScreeningDecisionModel(ScreeningDecisionBaseModel):
    """human readable hint: title/abstract screening allows a neutral uncertainty outcome."""

    is_eligible: bool | Literal["NEUTRAL"]


class FullTextScreeningDecisionModel(ScreeningDecisionBaseModel):
    """human readable hint: full-text screening requires a strict include/exclude decision."""

    is_eligible: bool
    seed_references: bool | None = None

    @model_validator(mode="after")
    def _check_seed_references_threshold(self) -> "FullTextScreeningDecisionModel":
        """human readable hint: seed references are only accepted for very high-confidence eligible calls."""

        if self.seed_references is True and not (self.confidence_score > 0.98):
            raise ValueError("seed_references can be true only when confidence_score is strictly greater than 0.98")
        if self.seed_references is True and self.is_eligible is not True:
            raise ValueError("seed_references can be true only when is_eligible is true")
        if self.is_eligible is True and self.confidence_score > 0.98 and self.seed_references is None:
            raise ValueError(
                "seed_references must be explicitly true/false when confidence_score is strictly greater than 0.98 and is_eligible is true"
            )
        return self
