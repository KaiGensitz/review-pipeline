"""Input/output helpers for dynamic data extraction runs."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.user_orchestrator import LLM_SETTINGS
from pipeline.core.metadata_aliases import read_metadata_value
from pipeline.integrations.embedding_utils import normalize_extracted_text
from pipeline.selection.pdf_parser import extract_markdown_from_pdf_with_level


STAGE = "data_extraction"


@dataclass
class PaperItem:
    """human readable hint: one prepared paper folder with metadata, evidence chunks, and extracted text."""

    paper_id: str
    folder_path: Path
    pdf_path: Path | None
    metadata: dict[str, Any]
    selected_chunks: list[dict]
    normalized_text: str


def load_metadata(folder: Path) -> tuple[str, dict[str, Any]]:
    """human readable hint: read compact artifact metadata for each included paper."""

    for prefix in ["data_extraction", "full_text"]:
        artifact_path = folder / f"{prefix}_artifact.json"
        if artifact_path.exists():
            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    paper_id = str(payload.get("paper_id") or folder.name)
                    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
                    return paper_id, metadata
            except Exception:
                pass
    return folder.name, {}


def load_paper_text(folder: Path) -> tuple[str, Path | None]:
    """human readable hint: use cached normalized text first, then parse the local PDF when needed."""

    normalized_path = folder / "full_text_normalized.txt"
    if normalized_path.exists():
        return normalized_path.read_text(encoding="utf-8"), None

    pdfs = sorted(folder.glob("*.pdf"))
    if not pdfs:
        return "", None

    pdf_path = pdfs[0]
    try:
        text, _parser_level = extract_markdown_from_pdf_with_level(pdf_path)
        return text, pdf_path
    except Exception:
        return "", pdf_path


def load_selected_chunks(folder: Path, paper_id: str) -> list[dict]:
    """human readable hint: load preselected data_extraction chunks copied from prior pipeline stages."""

    for prefix in ["data_extraction", "full_text"]:
        chunks_path = folder / f"{prefix}_selected_chunks.jsonl"
        if chunks_path.exists():
            try:
                with chunks_path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if not line.strip():
                            continue
                        payload = json.loads(line)
                        if payload.get("paper_id") == paper_id:
                            chunks = payload.get("selected_chunks")
                            return chunks if isinstance(chunks, list) else []
            except Exception:
                pass
    return []


def collect_papers(csv_dir: Path) -> list[PaperItem]:
    """human readable hint: collect all paper folders prepared for data extraction."""

    extraction_dir = csv_dir / "per_paper_data_extraction"
    if not extraction_dir.exists():
        return []

    papers: list[PaperItem] = []
    for folder in sorted(extraction_dir.iterdir()):
        if not folder.is_dir():
            continue
        paper_id, metadata = load_metadata(folder)
        selected_chunks = load_selected_chunks(folder, paper_id)
        text, pdf_path = load_paper_text(folder)
        normalized_text = normalize_extracted_text(text or "") if text else ""
        papers.append(
            PaperItem(
                paper_id=str(paper_id).strip(),
                folder_path=folder,
                pdf_path=pdf_path,
                metadata=metadata,
                selected_chunks=selected_chunks,
                normalized_text=normalized_text,
            )
        )
    return papers


def format_evidence(paper: PaperItem) -> str:
    """human readable hint: convert selected chunks or full text into the compact evidence block sent to the LLM."""

    parts = [f"Paper ID: {paper.paper_id}"]
    title = read_metadata_value(paper.metadata, "title")
    if title:
        parts.append(f"Title: {title}")
    evidence_mode = str(LLM_SETTINGS.get("data_extraction_evidence_mode", "full_text") or "full_text").strip().lower()
    if evidence_mode == "full_text" and paper.normalized_text:
        parts.append(f"[Full Normalized Text]\n{paper.normalized_text}")
    elif paper.selected_chunks:
        for idx, chunk in enumerate(paper.selected_chunks, start=1):
            text = str(chunk.get("text") or "").strip()
            if not text:
                continue
            parts.append(f"[Chunk {idx}]\n{text}")
    elif paper.normalized_text:
        parts.append(f"[Full Text]\n{paper.normalized_text}")
    return "\n\n".join(parts)


def flatten_extracted(payload: Any, prefix: str = "") -> dict[str, str]:
    """human readable hint: flatten nested extraction output for CSV export and consensus validation."""

    flat: dict[str, str] = {}
    if isinstance(payload, dict):
        keys = set(payload)
        value_key = "value" if "value" in keys else "_value" if "_value" in keys else None
        quote_key = "quote" if "quote" in keys else "_quote" if "_quote" in keys else None
        if value_key and quote_key:
            flat[prefix] = _stringify(payload.get(value_key))
            flat[f"{prefix}.quote"] = _stringify(payload.get(quote_key))
            return flat
        for key, value in payload.items():
            key_str = str(key)
            new_prefix = f"{prefix}.{key_str}" if prefix else key_str
            flat.update(flatten_extracted(value, new_prefix))
        return flat
    if isinstance(payload, list):
        flat[prefix] = "; ".join(_stringify(item) for item in payload)
        return flat
    if prefix:
        flat[prefix] = _stringify(payload)
    return flat


def serialize_result(
    paper: PaperItem,
    extracted_data: dict[str, Any],
    run_id: str,
    raw_output: str,
    error: str | None,
) -> dict[str, Any]:
    """human readable hint: build one stable per-paper extraction record for JSONL and CSV outputs."""

    flat = flatten_extracted(extracted_data)
    return {
        "paper_id": paper.paper_id,
        "run_label": STAGE,
        "run_id": run_id,
        "extracted_data": extracted_data,
        "extracted_data_flat": flat,
        "raw_output": raw_output,
        "error": error,
        "metadata": paper.metadata,
        "folder_name": paper.folder_path.name,
        "pdf_path": str(paper.pdf_path) if paper.pdf_path else "",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def write_outputs(payload: dict[str, Any], output_root: Path, folder_name: str) -> None:
    """human readable hint: write per-paper extraction artifacts for downstream validation."""

    output_dir = output_root / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)

    jsonl_text = (
        json.dumps(
            {
                "meta": "extraction_results",
                "description": "Per-paper extracted fields (JSONL).",
                "run_label": STAGE,
                "run_id": payload.get("run_id"),
            },
            ensure_ascii=False,
        )
        + "\n"
        + json.dumps(payload, ensure_ascii=False)
        + "\n"
    )
    # human readable hint: write one canonical extraction JSONL to avoid duplicate artifact names.
    jsonl_path = output_dir / f"{STAGE}_results.jsonl"
    jsonl_path.write_text(jsonl_text, encoding="utf-8")
    stale_jsonl_path = output_dir / f"{STAGE}_extraction_results.jsonl"
    if stale_jsonl_path.exists():
        stale_jsonl_path.unlink()

    flat = payload.get("extracted_data_flat") or {}
    fieldnames = ["paper_id", "run_id"] + sorted(str(key) for key in flat)
    row = {"paper_id": payload.get("paper_id"), "run_id": payload.get("run_id")}
    for key in fieldnames[2:]:
        row[key] = flat.get(key, "")
    csv_path = output_dir / f"{STAGE}_results.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)
    stale_csv_path = output_dir / f"{STAGE}_extraction_results.csv"
    if stale_csv_path.exists():
        stale_csv_path.unlink()


def append_error(path: Path, payload: dict[str, Any]) -> None:
    """human readable hint: keep extraction errors append-only for reliable audits."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _stringify(value: Any) -> str:
    """human readable hint: convert scalar/list values into CSV-safe strings without inventing content."""

    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(_stringify(item) for item in value)
    return str(value)
