from __future__ import annotations
from pipeline.core import pipeline as _pipeline

globals().update({name: getattr(_pipeline, name) for name in dir(_pipeline) if not name.startswith("__")})

class TitleAbstractStageMixin:
    def _process_title_abstract_batch(
        self,
        planned_papers: list[PaperRecord],
    ) -> Generator[tuple[PaperRecord, dict, dict, dict | None, float], None, None]:
        """human readable hint: stream title_abstract completions paper-by-paper as async calls finish."""

        yield from self._stream_async_batch(
            planned_papers,
            self._process_title_abstract_paper_async,
            stage_label="title_abstract",
        )

    async def _process_title_abstract_paper_async(self, paper: PaperRecord) -> tuple[dict, dict, dict | None]:
        """human readable hint: async title_abstract screening with strict schema validation and retry policy."""

        llm_input = self._title_abstract_full_input(paper)
        selected = [
            {
                "paper_id": paper.paper_id,
                "chunk_id": f"{paper.paper_id}::full_input::0000",
                "text": llm_input,
                "kind": "full_input",
                "page_start": None,
                "page_end": None,
                "line_start": None,
                "line_end": None,
            }
        ]
        selected, selected_score_stats = self._attach_chunk_certainty_metrics(selected)
        selected_coverage = self._build_selected_coverage_metrics(selected, page_count=None)

        prompt_tokens = len((llm_input or "").split())
        response_tokens = 0
        prompt_tokens_source = "estimate"
        response_tokens_source = "estimate"
        llm_seed = LLM_SETTINGS.get("seed")
        llm_top_p = float(LLM_SETTINGS.get("top_p", 1.0) or 1.0)
        llm_decision_incomplete = False
        failure_type: str | None = None
        failure_reason: str | None = None

        missing_abstract_reason = self._missing_title_abstract_reason(paper)
        if missing_abstract_reason and self._insufficient_context_reason_key:
            llm_decision = json.dumps(
                self._deterministic_insufficient_context_decision(paper, missing_abstract_reason),
                ensure_ascii=False,
            )
        elif not use_api:
            llm_decision = "LLM disabled: use_api=False; no API call made."
        else:
            llm_decision = None
            max_attempts = self._validation_max_retries
            for attempt in range(1, max_attempts + 1):
                current_decision, llm_usage = await self._call_llm_async(llm_input)

                if llm_usage:
                    prompt_tokens = int(
                        llm_usage.get("prompt_tokens")
                        or llm_usage.get("input_tokens")
                        or llm_usage.get("total_tokens")
                        or prompt_tokens
                    )
                    response_tokens = int(
                        llm_usage.get("completion_tokens")
                        or llm_usage.get("output_tokens")
                        or llm_usage.get("response_tokens")
                        or 0
                    )
                    prompt_tokens_source = "api"
                    response_tokens_source = "api"

                if not current_decision:
                    failure_type = "llm_no_response"
                    failure_reason = "LLM returned no decision after retries."
                    llm_decision_incomplete = True
                    continue

                if isinstance(current_decision, str) and current_decision.startswith("LLM error"):
                    failure_type = "llm_error"
                    failure_reason = current_decision
                    llm_decision_incomplete = True
                    continue

                sanitized = self._sanitize_screening_decision(current_decision, paper)

                try:
                    validated_payload = self._validate_screening_decision(sanitized or "")
                    llm_decision = json.dumps(validated_payload, ensure_ascii=False)
                    llm_decision_incomplete = False
                    failure_type = None
                    failure_reason = None
                    break
                except (ValidationError, ValueError) as exc:
                    llm_decision_incomplete = True
                    failure_type = "llm_validation_error"
                    failure_reason = f"Schema validation failed: {exc}"
                    if attempt == max_attempts:
                        llm_decision = sanitized

        if llm_decision and response_tokens == 0:
            response_tokens = len((llm_decision or "").split())

        if failure_type:
            self._log_error(
                paper.paper_id,
                failure_reason or "LLM decision failed validation.",
                error_type=failure_type,
                prompt_tokens=prompt_tokens,
                response_tokens=response_tokens,
                embedding_tokens=0,
                pdf_text_tokens=0,
                pdf_visual_tokens=0,
                total_estimated_tokens=prompt_tokens,
            )

        context_input_hash = self._sha256_text(llm_input)
        prompt_template_hash = self._sha256_text(self.prompt_template)
        full_prompt_hash = self._sha256_text(self.prompt_template.replace("{data}", llm_input or ""))

        output_metadata = self._metadata_without_authors(paper.metadata)
        record = {
            "paper_id": paper.paper_id,
            "run_label": self.run_label,
            "run_id": self.run_id,
            "selected_chunks": selected,
            "llm_decision": llm_decision,
            "diagnostics": {
                "total_chunks": 1,
                "selected_count": 1,
                "top_k": self.top_k,
                "score_threshold": self.score_threshold,
                "preselected_chunks": True,
                "stage": self.stage,
                "llm_decision_incomplete": llm_decision_incomplete,
                "language_used": str(EMBEDDING_SETTINGS.get("data_language", "en")),
                "llm_input_sha256": context_input_hash,
                "prompt_template_sha256": prompt_template_hash,
                "full_prompt_sha256": full_prompt_hash,
                "prompt_campaign_id": self._prompt_campaign_id,
                "prompt_template_snapshot_path": str(self._prompt_snapshot_path) if self._prompt_snapshot_path else None,
                "run_label": self.run_label,
                "run_id": self.run_id,
                "llm_seed": llm_seed,
                "llm_top_p": llm_top_p,
                "selected_score_stats": selected_score_stats,
                "selected_page_coverage": selected_coverage,
                "selection_trace": {
                    "fallback_triggered": False,
                    "effective_top_k": self.top_k,
                    "notes": (
                        f"deterministic insufficient_context: {missing_abstract_reason}"
                        if missing_abstract_reason
                        else "title_abstract uses full input block by design"
                    ),
                },
            },
            "metadata": output_metadata,
        }

        token_stats = {
            "prompt_tokens": prompt_tokens,
            "prompt_tokens_source": prompt_tokens_source,
            "response_tokens": response_tokens,
            "response_tokens_source": response_tokens_source,
            "embedding_tokens": 0,
            "embedding_tokens_source": "estimate",
            "pdf_text_tokens": 0,
            "pdf_visual_tokens": 0,
        }

        return record, token_stats, None

    def _missing_title_abstract_reason(self, paper: PaperRecord) -> str:
        """human readable hint: detect records that cannot support title/abstract screening."""

        metadata = paper.metadata or {}
        flag = str(metadata.get("citation_ingestion_missing_abstract") or "").strip().casefold()
        if flag in {"true", "1", "yes"}:
            return "abstract_missing_in_citation_ingestion"
        abstract_text = str(paper.abstract or "").strip()
        if abstract_text.casefold() in MISSING_ABSTRACT_MARKERS:
            return "abstract_missing_or_placeholder"
        return ""

    def _deterministic_insufficient_context_decision(self, paper: PaperRecord, reason: str) -> dict[str, Any]:
        """human readable hint: exclude title/abstract records with no usable abstract without calling the LLM."""

        reason_key = self._insufficient_context_reason_key or "insufficient_context"
        payload: dict[str, Any] = {
            "step_by_step_deliberation": (
                "The record has no usable abstract text for title/abstract screening."
            ),
            "confidence_score": 1.0,
            "justification": (
                "The abstract is missing or encoded as a missing-value placeholder; "
                "title/abstract screening therefore has insufficient context."
            ),
            "exclusion_reason_category": reason_key,
            "is_eligible": False,
        }
        for key in self._active_exclusion_flag_keys:
            payload[key] = key == reason_key
        payload["deterministic_screening_reason"] = reason
        return payload

    def _title_abstract_full_input(self, paper: PaperRecord) -> str:
        """Build one full context block for title_abstract (no chunking/retrieval)."""

        title_text = (paper.title or "").strip()
        abstract_text = (paper.abstract or "").strip()
        authors = self._authors_for_paper(paper)
        title_text = self._strip_author_mentions(title_text, authors)
        abstract_text = self._strip_author_mentions(abstract_text, authors)
        parts = [f"Paper ID: {paper.paper_id}", f"Title: {title_text}"]
        if abstract_text:
            parts.append("Abstract:\n" + abstract_text)
        return "\n\n".join(parts)

