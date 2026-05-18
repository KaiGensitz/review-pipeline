from __future__ import annotations

import csv
import json
from pathlib import Path
import re
import shutil
import sys
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_FAKE_LLM_CALLS = {"data_extraction": 0}


def _compiled_forbidden_patterns() -> tuple[re.Pattern[str] | None, re.Pattern[str] | None]:
    """human readable hint: boundary smoke terms come from user-editable config, not pipeline literals."""

    from config.user_orchestrator import PIPELINE_BOUNDARY_CHECK_TERMS

    topic_terms = [str(value) for value in PIPELINE_BOUNDARY_CHECK_TERMS.get("topic_terms", [])]
    admin_terms = [str(value) for value in PIPELINE_BOUNDARY_CHECK_TERMS.get("admin_header_terms", [])]

    def _compile(terms: list[str], *, ignore_case: bool = True) -> re.Pattern[str] | None:
        selected = [
            re.escape(term)
            for term in sorted(set(terms), key=len, reverse=True)
            if len(term.strip()) >= 3
        ]
        if not selected:
            return None
        flags = re.IGNORECASE if ignore_case else 0
        return re.compile("|".join(selected), flags)

    return _compile(topic_terms), _compile(admin_terms)


def _assert_pipeline_boundary() -> None:
    topic_forbidden, admin_forbidden = _compiled_forbidden_patterns()
    for path in (REPO_ROOT / "pipeline").rglob("*.py"):
        text = path.read_text(encoding="utf-8-sig")
        topic_match = topic_forbidden.search(text) if topic_forbidden else None
        if topic_match:
            raise AssertionError(f"forbidden topic term in {path}: {topic_match.group(0)}")
        admin_match = admin_forbidden.search(text) if admin_forbidden else None
        if admin_match:
            raise AssertionError(f"hardcoded admin header in {path}: {admin_match.group(0)}")


def _assert_schema_prompt_assembly() -> None:
    from config.user_orchestrator import DATA_EXTRACTION_SCHEMA_FILE, PROMPT_FILES
    from pipeline.core.extraction_schema import DynamicExtractionSchema

    schema = DynamicExtractionSchema.from_kb(Path(DATA_EXTRACTION_SCHEMA_FILE))
    prompt = Path(PROMPT_FILES["data_extraction"]).read_text(encoding="utf-8")
    assembled = schema.inject_into_prompt(prompt)
    for marker in (
        "# DOMAIN-GUIDED EXTRACTION PLAN",
        "# KB-DRIVEN EXTRACTION SCHEMA",
        "Response JSON shape:",
        "Consensus/export column:",
        "# CONTEXT",
    ):
        if marker not in assembled:
            raise AssertionError(f"schema prompt assembly missing marker: {marker}")


def _assert_atomic_write() -> None:
    from pipeline.core.extraction_io import _atomic_write_text

    path = REPO_ROOT / "output" / "smoke_genericity_atomic" / "atomic.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(path, "first")
    _atomic_write_text(path, "second")
    if path.read_text(encoding="utf-8") != "second":
        raise AssertionError("atomic write did not replace final file content")


class _FakeSelectionEngine:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def score_chunks(self, chunks: list[dict]) -> tuple[list[dict], list, dict]:
        scored = []
        for index, row in enumerate(chunks):
            item = dict(row)
            item.setdefault("score", max(0.1, 1.0 - index * 0.01))
            scored.append(item)
        return scored, [], {"embedding_tokens": 0, "embedding_calls": 0}

    def select_scored(
        self,
        scored_chunks: list[dict],
        top_k: int | None = None,
        score_threshold: float | None = None,
    ) -> list[dict]:
        rows = [dict(row) for row in scored_chunks]
        if score_threshold is not None:
            rows = [row for row in rows if float(row.get("score", 0.0) or 0.0) >= float(score_threshold)]
        if top_k is not None:
            rows = rows[: max(0, int(top_k))]
        return rows

    def select(
        self,
        chunks: list[dict],
        top_k: int | None = None,
        score_threshold: float | None = None,
    ) -> tuple[list[dict], list, dict]:
        scored, _, usage = self.score_chunks(chunks)
        return self.select_scored(scored, top_k=top_k, score_threshold=score_threshold), [], usage


def _screening_payload(instance) -> dict:
    payload = {
        "step_by_step_deliberation": "Smoke-test decision with generic evidence handling.",
        "justification": "Smoke-test justification.",
        "is_eligible": True,
        "confidence_score": 0.91,
        "exclusion_reason_category": None,
    }
    for key in getattr(instance, "_active_exclusion_flag_keys", set()):
        payload[key] = False
    if getattr(instance, "stage", "") == "full_text":
        payload["seed_references"] = False
    return payload


async def _fake_call_llm_async(self, context, **kwargs):
    if getattr(self, "stage", "") == "data_extraction" and getattr(self, "_extraction_schema", None) is not None:
        _FAKE_LLM_CALLS["data_extraction"] += 1
        return json.dumps(self._extraction_schema.default_payload(), ensure_ascii=False), {
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
        }
    return json.dumps(_screening_payload(self), ensure_ascii=False), {
        "prompt_tokens": 1,
        "completion_tokens": 1,
        "total_tokens": 2,
    }


def _fake_call_llm(self, context, **kwargs):
    if getattr(self, "stage", "") == "data_extraction" and getattr(self, "_extraction_schema", None) is not None:
        _FAKE_LLM_CALLS["data_extraction"] += 1
        return json.dumps(self._extraction_schema.default_payload(), ensure_ascii=False), {
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
        }
    return json.dumps(_screening_payload(self), ensure_ascii=False), {
        "prompt_tokens": 1,
        "completion_tokens": 1,
        "total_tokens": 2,
    }


def _write_smoke_input(out_dir: Path) -> Path:
    path = out_dir / "smoke_one_paper.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["paper_id", "title", "abstract", "authors", "journal", "publication_year"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "paper_id": "SMOKE001",
                "title": "Smoke test study for generic pipeline execution",
                "abstract": "One-row record for local smoke execution with fake model responses.",
                "authors": "Tester",
                "journal": "Smoke Journal",
                "publication_year": "2026",
            }
        )
    return path


def _prepare_pdf_root(out_dir: Path) -> Path:
    pdf_candidates = sorted((REPO_ROOT / "input").rglob("*.pdf"))
    if not pdf_candidates:
        raise FileNotFoundError("No local PDF available under input/ for full_text/data_extraction smoke test.")
    pdf_root = out_dir / "pdf_root"
    folder = pdf_root / "SMOKE001"
    folder.mkdir(parents=True, exist_ok=True)
    shutil.copy2(pdf_candidates[0], folder / "SMOKE001.pdf")
    return pdf_root


def _run_stage_smoke() -> dict[str, dict]:
    import pipeline.core.pipeline as pipeline_mod
    from config.user_orchestrator import STAGE_HANDOFF_SETTINGS
    from pipeline.core.run_screening import run_pipeline

    pipeline_mod.SelectionEngine = _FakeSelectionEngine
    pipeline_mod.PaperScreeningPipeline._call_llm_async = _fake_call_llm_async
    pipeline_mod.PaperScreeningPipeline._call_llm = _fake_call_llm
    original_handoff_enabled = STAGE_HANDOFF_SETTINGS.get("enabled", True)
    STAGE_HANDOFF_SETTINGS["enabled"] = False

    try:
        out_dir = REPO_ROOT / "output" / f"smoke_genericity_{uuid4().hex[:8]}"
        out_dir.mkdir(parents=True, exist_ok=True)
        smoke_input = _write_smoke_input(out_dir)
        pdf_root = _prepare_pdf_root(out_dir)

        full_dir = out_dir / "per_paper_full_text"
        data_dir = out_dir / "per_paper_data_extraction"
        full_dir.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        pipeline_mod.PaperScreeningPipeline._full_text_pdf_dir = lambda self: full_dir
        pipeline_mod.PaperScreeningPipeline._data_extraction_pdf_dir = lambda self: data_dir

        results: dict[str, dict] = {}
        for stage in ("title_abstract", "full_text", "data_extraction"):
            prefix = out_dir / f"{stage}_isolated_one_paper"
            artifact = run_pipeline(
                stage=stage,
                input_files=[smoke_input],
                csv_dir=str(out_dir),
                pdf_root=str(pdf_root),
                sample_size=1,
                sample_seed=7,
                qc_enabled=False,
                confirm_sampling=True,
                quiet=True,
                sustainability_tracking=False,
                enable_time_savings=False,
                run_label_override="smoke_genericity",
                eligibility_output=prefix.with_name(prefix.name + "_eligibility.jsonl"),
                chunks_output=prefix.with_name(prefix.name + "_chunks.jsonl"),
                text_output=prefix.with_name(prefix.name + "_readable.txt"),
                error_log=prefix.with_name(prefix.name + "_errors.jsonl"),
                resource_log=prefix.with_name(prefix.name + "_resource.log"),
                use_advanced_pdf_parser=False,
            )
            data = dict(artifact) if isinstance(artifact, dict) else {"success": bool(artifact)}
            if not data.get("success") or data.get("error_ids"):
                raise AssertionError(f"{stage} smoke failed: {data}")
            results[stage] = {"success": True, "error_ids": data.get("error_ids", [])}

        first_data_extraction_calls = _FAKE_LLM_CALLS["data_extraction"]
        prefix = out_dir / "data_extraction_isolated_one_paper"
        artifact = run_pipeline(
            stage="data_extraction",
            input_files=[smoke_input],
            csv_dir=str(out_dir),
            pdf_root=str(pdf_root),
            sample_size=1,
            sample_seed=7,
            qc_enabled=False,
            confirm_sampling=True,
            quiet=True,
            sustainability_tracking=False,
            enable_time_savings=False,
            run_label_override="smoke_genericity",
            eligibility_output=prefix.with_name(prefix.name + "_eligibility.jsonl"),
            chunks_output=prefix.with_name(prefix.name + "_chunks.jsonl"),
            text_output=prefix.with_name(prefix.name + "_readable.txt"),
            error_log=prefix.with_name(prefix.name + "_errors.jsonl"),
            resource_log=prefix.with_name(prefix.name + "_resource.log"),
            use_advanced_pdf_parser=False,
        )
        data = dict(artifact) if isinstance(artifact, dict) else {"success": bool(artifact)}
        if not data.get("success") or data.get("error_ids"):
            raise AssertionError(f"data_extraction idempotency smoke failed: {data}")
        if _FAKE_LLM_CALLS["data_extraction"] != first_data_extraction_calls:
            raise AssertionError("completed data_extraction output did not skip the fake LLM call")
        results["data_extraction"]["idempotent_skip_llm"] = True
        return results
    finally:
        STAGE_HANDOFF_SETTINGS["enabled"] = original_handoff_enabled


def main() -> None:
    _assert_pipeline_boundary()
    _assert_schema_prompt_assembly()
    _assert_atomic_write()
    print(json.dumps(_run_stage_smoke(), indent=2))


if __name__ == "__main__":
    main()
