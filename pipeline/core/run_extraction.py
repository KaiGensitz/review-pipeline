"""Direct run: python -m pipeline.core.run_extraction."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from threading import Lock

from openai import AsyncOpenAI

from config.user_orchestrator import (
    DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES,
    DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_LOW_PRIORITY_PATTERNS,
    LLM_SETTINGS,
    PATH_SETTINGS,
    require_setting,
)
from pipeline.core.extraction_io import (
    STAGE,
    PaperItem,
    append_error,
    collect_papers,
    format_evidence,
    serialize_result,
    write_outputs,
)
from pipeline.core.extraction_schema import (
    DynamicExtractionSchema,
    SchemaEvidenceHintBuilder,
    SchemaEvidenceHintConfig,
    domain_groups_for_schema,
    parse_and_validate,
)
from pipeline.core.prompt_context import load_stage_prompt_template
from pipeline.additions.export_extraction_tables import ExtractionAggregateWriter
from pipeline.core.metadata_aliases import read_metadata_value
from pipeline.selection.chunking import chunk_fulltext_sentences
from pipeline.selection.selector import EmbeddingBackend, LabeledExample, RelevanceSelector, load_labeled_examples


TOKENS_PER_WORD = 1.3


@dataclass(frozen=True)
class ExtractionEvidenceBundle:
    """human readable hint: one LLM evidence payload plus metadata about how it was assembled."""

    text: str
    source_text_for_hints: str = ""
    schema_hints_enabled: bool = True
    mode: str = "full_text"


class SchemaSemanticAnchorFactory:
    """human readable hint: build POS retrieval targets from the active schema CSV only."""

    def __init__(self, schema: DynamicExtractionSchema) -> None:
        self.schema = schema

    def build_positive_examples(self) -> list[LabeledExample]:
        examples: list[LabeledExample] = []
        for variable in self.schema.variables:
            for anchor in variable.semantic_anchors:
                text = str(anchor or "").strip()
                if text:
                    examples.append({"label": "POS", "text": text})
        if not examples:
            raise ValueError(
                "Semantic data-extraction RAG is enabled, but DATA_EXTRACTION_SCHEMA_FILE has no semantic_anchors."
            )
        return examples


class StructuralNegativeExampleFactory:
    """human readable hint: optionally add user-supplied structural noise examples from the stage KB."""

    def __init__(self, kb_path: Path | None) -> None:
        self.kb_path = kb_path

    def build_negative_examples(self) -> list[LabeledExample]:
        if not self.kb_path or not self.kb_path.exists():
            return []
        try:
            examples = load_labeled_examples(str(self.kb_path))
        except Exception:
            return []
        return [{"label": "NEG", "text": ex["text"]} for ex in examples if ex["label"] == "NEG"]


class SemanticExtractionEvidenceAssembler:
    """human readable hint: create embedding-ranked extraction evidence from schema-owned semantic anchors."""

    def __init__(
        self,
        schema: DynamicExtractionSchema,
        selector: RelevanceSelector | None,
        top_k: int,
        score_threshold: float | None,
        language: str,
    ) -> None:
        self.schema = schema
        self.selector = selector
        self.top_k = max(1, int(top_k))
        self.score_threshold = score_threshold
        self.language = language
        self._selector_lock = Lock()

    @classmethod
    def from_settings(cls, schema: DynamicExtractionSchema) -> "SemanticExtractionEvidenceAssembler":
        """human readable hint: keep all semantic retrieval knobs in user-editable settings."""

        enabled = bool(LLM_SETTINGS.get("data_extraction_semantic_rag_enabled", False))
        if not enabled:
            return cls(schema=schema, selector=None, top_k=0, score_threshold=None, language="en")

        positive_examples = SchemaSemanticAnchorFactory(schema).build_positive_examples()
        negative_examples = StructuralNegativeExampleFactory(_stage_kb_path()).build_negative_examples()
        examples = positive_examples + negative_examples
        selector = RelevanceSelector(
            embedder=EmbeddingBackend(),
            examples=examples,
            always_include_kinds=("title",),
        )
        raw_threshold = LLM_SETTINGS.get("data_extraction_semantic_score_threshold")
        score_threshold = None if raw_threshold in {None, ""} else float(raw_threshold)
        return cls(
            schema=schema,
            selector=selector,
            top_k=int(LLM_SETTINGS.get("data_extraction_semantic_top_k", 24) or 24),
            score_threshold=score_threshold,
            language=_chunking_language(),
        )

    @property
    def enabled(self) -> bool:
        return self.selector is not None

    def build(self, paper: PaperItem) -> ExtractionEvidenceBundle:
        """human readable hint: route each paper through semantic RAG or the legacy full evidence formatter."""

        if not self.enabled:
            return ExtractionEvidenceBundle(
                text=format_evidence(paper),
                source_text_for_hints=paper.normalized_text,
                schema_hints_enabled=True,
                mode="full_text",
            )

        chunks = self._build_chunks(paper)
        if not chunks:
            return ExtractionEvidenceBundle(text=format_evidence(paper), mode="full_text_fallback")

        assert self.selector is not None
        with self._selector_lock:
            selected, _scores, _usage = self.selector.select(
                chunks=chunks,
                top_k=self.top_k,
                score_threshold=self.score_threshold,
            )
        return ExtractionEvidenceBundle(
            text=self._format_selected_chunks(paper, selected),
            source_text_for_hints="",
            schema_hints_enabled=False,
            mode="semantic_rag",
        )

    def _build_chunks(self, paper: PaperItem) -> list[dict]:
        # human readable hint: title is metadata, while normalized and supplemental evidence are semantically ranked.
        chunks: list[dict] = []
        title = read_metadata_value(paper.metadata, "title")
        if title:
            chunks.append(
                {
                    "paper_id": paper.paper_id,
                    "chunk_id": f"{paper.paper_id}::title::0000",
                    "text": f"Title: {title}",
                    "kind": "title",
                }
            )
        if paper.normalized_text:
            chunks.extend(
                chunk_fulltext_sentences(
                    paper_id=paper.paper_id,
                    title=title,
                    full_text=paper.normalized_text,
                    language=self.language,
                )
            )
        if paper.supplemental_cited_evidence:
            supplemental_chunks = chunk_fulltext_sentences(
                paper_id=f"{paper.paper_id}::supplemental",
                title=title,
                full_text=paper.supplemental_cited_evidence,
                language=self.language,
            )
            for chunk in supplemental_chunks:
                item = dict(chunk)
                item["kind"] = "supplemental_cited_evidence"
                chunks.append(item)
        return chunks

    @staticmethod
    def _format_selected_chunks(paper: PaperItem, selected: list[dict]) -> str:
        parts = [f"Paper ID: {paper.paper_id}"]
        title = read_metadata_value(paper.metadata, "title")
        if title:
            parts.append(f"Title: {title}")
        parts.append(
            "[Semantic Retrieval Evidence]\n"
            "The extraction model may quote only from the chunks below and any title metadata shown above."
        )
        for idx, chunk in enumerate(selected, start=1):
            score = float(chunk.get("score", 0.0) or 0.0)
            kind = str(chunk.get("kind") or "chunk")
            location = _chunk_location(chunk)
            parts.append(f"[Chunk {idx} | kind={kind} | score={score:.4f}{location}]\n{chunk.get('text', '')}")
        return "\n\n".join(parts)


def _truncate_to_budget(text: str, max_tokens: int) -> str:
    """human readable hint: trim evidence text using the same lightweight token estimate as the pipeline."""

    if not text:
        return ""
    max_words = max(1, int(max_tokens / TOKENS_PER_WORD))
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words])


def _stage_kb_path() -> Path | None:
    """human readable hint: resolve the active stage KB without hardcoding a protocol file name."""

    configured_path = PATH_SETTINGS.get("knowledge_base_file")
    return Path(configured_path) if configured_path else None


def _chunking_language() -> str:
    """human readable hint: use the configured data language when it is concrete; fall back to English for schema anchors."""

    from config.user_orchestrator import EMBEDDING_SETTINGS

    value = str(EMBEDDING_SETTINGS.get("data_language", "en") or "en").strip().lower()
    if value in {"auto", "auto_first"}:
        return "en"
    return value or "en"


def _chunk_location(chunk: dict) -> str:
    """human readable hint: render generic chunk provenance when page or line metadata survived parsing."""

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


def _build_llm_input(paper: PaperItem, prompt_template: str, max_prompt_tokens: int) -> str:
    """human readable hint: insert paper evidence into the prompt while respecting the model context budget."""

    return _build_llm_input_from_evidence(format_evidence(paper), prompt_template, max_prompt_tokens)


def _build_llm_input_with_schema_hints(
    evidence_bundle: ExtractionEvidenceBundle,
    prompt_template: str,
    schema: DynamicExtractionSchema,
    max_prompt_tokens: int,
) -> str:
    """human readable hint: prepend schema-guided snippets in the direct runner just like the main pipeline."""

    evidence = evidence_bundle.text
    full_text_marker = "[Full Normalized Text]\n"
    if (
        evidence_bundle.schema_hints_enabled
        and
        bool(LLM_SETTINGS.get("data_extraction_schema_evidence_hints", True))
        and evidence_bundle.source_text_for_hints
        and full_text_marker in evidence
    ):
        config = SchemaEvidenceHintConfig(
            enabled=True,
            snippets_per_variable=max(0, int(LLM_SETTINGS.get("data_extraction_evidence_hints_per_variable", 2) or 0)),
            max_snippet_chars=max(120, int(LLM_SETTINGS.get("data_extraction_evidence_hint_max_chars", 420) or 420)),
            max_total_chars=max(1000, int(LLM_SETTINGS.get("data_extraction_evidence_hints_max_total_chars", 18000) or 18000)),
            context_lines=max(0, int(LLM_SETTINGS.get("data_extraction_evidence_hint_context_lines", 1) or 0)),
            alias_map=DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES,
            low_priority_patterns=tuple(DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_LOW_PRIORITY_PATTERNS),
        )
        hints = SchemaEvidenceHintBuilder(schema.variables, config).build(evidence_bundle.source_text_for_hints)
        if hints:
            evidence = evidence.replace(full_text_marker, f"{hints}\n\n{full_text_marker}", 1)
    return _build_llm_input_from_evidence(evidence, prompt_template, max_prompt_tokens)


def _build_llm_input_from_evidence(evidence: str, prompt_template: str, max_prompt_tokens: int) -> str:
    """human readable hint: finish prompt injection after evidence has been assembled."""

    evidence = _truncate_to_budget(evidence, max_prompt_tokens)
    if "{data}" in prompt_template:
        return prompt_template.replace("{data}", evidence)
    return f"{prompt_template}\n\nEvidence:\n{evidence}"


async def _call_llm(
    client: AsyncOpenAI,
    model: str,
    prompt: str,
    response_format: dict[str, Any] | None,
    max_tokens: int,
    temperature: float,
    top_p: float,
) -> str:
    """human readable hint: send one extraction prompt to the LLM and return raw JSON text."""

    request_kwargs: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": top_p,
        "stream": False,
    }
    if response_format is not None:
        request_kwargs["response_format"] = response_format

    response = await client.chat.completions.create(**request_kwargs)
    message = response.choices[0].message if response.choices else None
    return (message.content or "").strip() if message else ""


async def _process_paper(
    paper: PaperItem,
    client: AsyncOpenAI,
    prompt_template: str,
    base_prompt_template: str,
    schema: DynamicExtractionSchema,
    semaphore: asyncio.Semaphore,
    run_id: str,
    model: str,
    max_prompt_tokens: int,
    max_tokens: int,
    temperature: float,
    top_p: float,
    error_log: Path,
    evidence_assembler: SemanticExtractionEvidenceAssembler,
) -> dict[str, Any]:
    """human readable hint: process one paper with bounded concurrency and dynamic schema validation."""

    async with semaphore:
        if not paper.normalized_text and not paper.selected_chunks and not paper.supplemental_cited_evidence:
            error = "no_text_available"
            append_error(error_log, {"paper_id": paper.paper_id, "error": error, "stage": STAGE})
            return serialize_result(paper, schema.default_payload(), run_id, raw_output="", error=error)

        # human readable hint: semantic retrieval runs before prompt injection so the LLM never receives hidden full text.
        evidence_bundle = evidence_assembler.build(paper)
        llm_input = _build_llm_input_with_schema_hints(evidence_bundle, prompt_template, schema, max_prompt_tokens)
        if bool(LLM_SETTINGS.get("data_extraction_split_by_domain", True)):
            merged_payload = schema.default_payload()
            raw_by_domain: dict[str, str] = {}
            errors_by_domain: dict[str, str] = {}
            domain_max_tokens = max(256, int(LLM_SETTINGS.get("data_extraction_domain_max_tokens", 3000) or 3000))
            response_format_mode = str(
                LLM_SETTINGS.get("data_extraction_response_format_mode", "prompt_only") or "prompt_only"
            ).strip().lower()

            # human readable hint: configured domain groups avoid one large malformed JSON while reducing repeated full-text calls.
            domain_groups = domain_groups_for_schema(schema, LLM_SETTINGS.get("data_extraction_domain_groups"))
            for domains in domain_groups:
                group_label = "+".join(domains)
                domain_schema = schema.for_domains(domains)
                domain_prompt = domain_schema.inject_into_prompt(base_prompt_template)
                domain_input = _build_llm_input_with_schema_hints(
                    evidence_bundle,
                    domain_prompt,
                    domain_schema,
                    max_prompt_tokens,
                )
                response_format = None
                if response_format_mode == "json_schema":
                    response_format = domain_schema.openai_response_format()
                elif response_format_mode == "json_object":
                    response_format = {"type": "json_object"}
                raw_text = await _call_llm(
                    client,
                    model=model,
                    prompt=domain_input,
                    response_format=response_format,
                    max_tokens=domain_max_tokens,
                    temperature=temperature,
                    top_p=top_p,
                )
                raw_by_domain[group_label] = raw_text
                domain_data, domain_error = parse_and_validate(raw_text, domain_schema)
                if domain_error:
                    errors_by_domain[group_label] = domain_error
                    append_error(
                        error_log,
                        {
                            "paper_id": paper.paper_id,
                            "error": f"domain group '{group_label}' failed validation: {domain_error}",
                            "stage": STAGE,
                            "error_type": "data_extraction_domain_validation_failed",
                        },
                    )
                    continue
                for domain in domains:
                    if isinstance(domain_data.get(domain), dict):
                        merged_payload[domain] = domain_data[domain]
                    else:
                        errors_by_domain[domain] = "validated domain payload missing expected domain key"

            extracted_data = schema.validate_payload(merged_payload)
            merged_raw = json.dumps(
                {
                    "evidence_mode": evidence_bundle.mode,
                    "domain_errors": errors_by_domain,
                    "raw_domain_outputs": raw_by_domain,
                },
                ensure_ascii=False,
            )
            error = "; ".join(f"{domain}: {message}" for domain, message in errors_by_domain.items()) or None
            return serialize_result(paper, extracted_data, run_id, raw_output=merged_raw, error=error)

        raw_text = await _call_llm(
            client,
            model=model,
            prompt=llm_input,
            response_format=schema.openai_response_format(),
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
        )
        extracted_data, error = parse_and_validate(raw_text, schema)
        if error:
            append_error(error_log, {"paper_id": paper.paper_id, "error": error, "stage": STAGE})
        return serialize_result(paper, extracted_data, run_id, raw_output=raw_text, error=error)


async def run_extraction() -> None:
    """human readable hint: direct async data-extraction runner using the KB-derived schema."""

    # human readable hint: the extraction schema and prompt field instructions are generated from the CSV KB at runtime.
    schema = DynamicExtractionSchema.from_kb()
    base_prompt_template = load_stage_prompt_template(STAGE)
    prompt_template = schema.inject_into_prompt(base_prompt_template)
    evidence_assembler = SemanticExtractionEvidenceAssembler.from_settings(schema)

    csv_dir = Path(PATH_SETTINGS.get("csv_dir") or Path.cwd() / "input")
    output_root = Path(PATH_SETTINGS.get("output_root", Path.cwd() / "output")) / STAGE
    output_root.mkdir(parents=True, exist_ok=True)
    aggregate_writer = ExtractionAggregateWriter(
        output_dir=output_root,
        consensus_path=csv_dir / "data_extraction_schema.csv",
        input_paper_dir=csv_dir / "per_paper_data_extraction",
        reset=True,
    )

    context_window = require_setting(LLM_SETTINGS, "context_window_total_tokens", "LLM_SETTINGS", int)
    max_tokens = require_setting(LLM_SETTINGS, "max_tokens", "LLM_SETTINGS", int)
    max_prompt_tokens = max(1, int(context_window - max_tokens - 1200))
    temperature = float(LLM_SETTINGS.get("temperature", 0.0) or 0.0)
    top_p = float(LLM_SETTINGS.get("top_p", 1.0) or 1.0)
    model_name = require_setting(LLM_SETTINGS, "screening_model", "LLM_SETTINGS", str)
    papers = collect_papers(csv_dir)
    if not papers:
        print("[extraction] No papers found in per_paper_data_extraction.")
        return

    run_id = f"{STAGE}_{datetime.now().strftime('%Y%m%d_%H-%M-%S')}"
    error_log = output_root / f"{STAGE}_error_log_{run_id}.jsonl"
    client = AsyncOpenAI(api_key=os.environ.get("LLM_API_KEY"), base_url=LLM_SETTINGS.get("gpustack_base_url"))
    concurrency = max(1, int(LLM_SETTINGS.get("async_max_concurrency", 2) or 2))
    # human readable hint: the semaphore caps simultaneous extraction calls using the user-editable endpoint load setting.
    semaphore = asyncio.Semaphore(concurrency)

    tasks = [
        _process_paper(
            paper=paper,
            client=client,
            prompt_template=prompt_template,
            base_prompt_template=base_prompt_template,
            schema=schema,
            semaphore=semaphore,
            run_id=run_id,
            model=model_name,
            max_prompt_tokens=max_prompt_tokens,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            error_log=error_log,
            evidence_assembler=evidence_assembler,
        )
        for paper in papers
    ]

    for coro in asyncio.as_completed(tasks):
        payload = await coro
        folder_name = str(payload.get("folder_name") or payload.get("paper_id") or "paper")
        write_outputs(payload, output_root, folder_name)
        aggregate_writer.append_record(payload)

    print(f"[extraction] Completed {len(papers)} papers. Outputs in {output_root}.")


if __name__ == "__main__":
    asyncio.run(run_extraction())
