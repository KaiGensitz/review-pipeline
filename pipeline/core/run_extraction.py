"""Direct run: python -m pipeline.core.run_extraction."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import AsyncOpenAI

from config.user_orchestrator import LLM_SETTINGS, PATH_SETTINGS, require_setting
from pipeline.core.extraction_io import (
    STAGE,
    PaperItem,
    append_error,
    collect_papers,
    format_evidence,
    serialize_result,
    write_outputs,
)
from pipeline.core.extraction_schema import DynamicExtractionSchema, parse_and_validate
from pipeline.core.prompt_context import load_stage_prompt_template
from pipeline.additions.export_extraction_tables import ExtractionAggregateWriter


TOKENS_PER_WORD = 1.3


def _truncate_to_budget(text: str, max_tokens: int) -> str:
    """human readable hint: trim evidence text using the same lightweight token estimate as the pipeline."""

    if not text:
        return ""
    max_words = max(1, int(max_tokens / TOKENS_PER_WORD))
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words])


def _build_llm_input(paper: PaperItem, prompt_template: str, max_prompt_tokens: int) -> str:
    """human readable hint: insert paper evidence into the prompt while respecting the model context budget."""

    evidence = _truncate_to_budget(format_evidence(paper), max_prompt_tokens)
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
) -> dict[str, Any]:
    """human readable hint: process one paper with bounded concurrency and dynamic schema validation."""

    async with semaphore:
        if not paper.normalized_text and not paper.selected_chunks:
            error = "no_text_available"
            append_error(error_log, {"paper_id": paper.paper_id, "error": error, "stage": STAGE})
            return serialize_result(paper, schema.default_payload(), run_id, raw_output="", error=error)

        llm_input = _build_llm_input(paper, prompt_template, max_prompt_tokens)
        if bool(LLM_SETTINGS.get("data_extraction_split_by_domain", True)):
            merged_payload = schema.default_payload()
            raw_by_domain: dict[str, str] = {}
            errors_by_domain: dict[str, str] = {}
            domain_max_tokens = max(256, int(LLM_SETTINGS.get("data_extraction_domain_max_tokens", 3000) or 3000))
            response_format_mode = str(
                LLM_SETTINGS.get("data_extraction_response_format_mode", "prompt_only") or "prompt_only"
            ).strip().lower()

            # human readable hint: smaller domain-level schemas avoid long malformed JSON from one large extraction call.
            for domain in schema.domains:
                domain_schema = schema.for_domain(domain)
                domain_prompt = domain_schema.inject_into_prompt(base_prompt_template)
                domain_input = _build_llm_input(paper, domain_prompt, max_prompt_tokens)
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
                raw_by_domain[domain] = raw_text
                domain_data, domain_error = parse_and_validate(raw_text, domain_schema)
                if domain_error:
                    errors_by_domain[domain] = domain_error
                    append_error(
                        error_log,
                        {
                            "paper_id": paper.paper_id,
                            "error": f"domain '{domain}' failed validation: {domain_error}",
                            "stage": STAGE,
                            "error_type": "data_extraction_domain_validation_failed",
                        },
                    )
                    continue
                if isinstance(domain_data.get(domain), dict):
                    merged_payload[domain] = domain_data[domain]

            extracted_data = schema.validate_payload(merged_payload)
            merged_raw = json.dumps(
                {"domain_errors": errors_by_domain, "raw_domain_outputs": raw_by_domain},
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
