"""Input/output helpers for dynamic data extraction runs."""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.user_orchestrator import DATA_EXTRACTION_SUPPLEMENTAL_CITED_EVIDENCE, LLM_SETTINGS
from pipeline.core.metadata_aliases import read_metadata_value
from pipeline.integrations.embedding_utils import normalize_extracted_text_for_llm
from pipeline.selection.pdf_parser import extract_markdown_from_pdf_with_level


STAGE = "data_extraction"


@dataclass
class PerPaperFileIndex:
    """human readable hint: one cached view of a per-paper folder's canonical and legacy filenames."""

    folder: Path
    paper_id: str | None = None

    def __post_init__(self) -> None:
        self.folder = Path(self.folder)
        self.paper_id = self.sanitize_paper_id(self.paper_id or self.paper_id_hint(self.folder))
        self.refresh()

    def refresh(self) -> None:
        """human readable hint: refresh the cached filename map after a write or rename."""

        try:
            files = [path for path in self.folder.iterdir() if path.is_file()]
        except Exception:
            files = []
        self._files_by_name = {path.name: path for path in files}

    @staticmethod
    def sanitize_paper_id(paper_id: str | None) -> str:
        """human readable hint: keep paper IDs usable in filenames without encoding export-vendor facts."""

        safe = "".join(ch for ch in str(paper_id or "").lstrip("#") if ch.isalnum() or ch in {"_", "-"})
        return safe or "paper"

    @classmethod
    def paper_id_hint(cls, folder: Path) -> str:
        """human readable hint: infer the ID from the generic <ID>_ folder naming convention."""

        raw_name = Path(folder).name
        if "_" in raw_name:
            raw_name = raw_name.split("_", 1)[0]
        return cls.sanitize_paper_id(raw_name)

    @classmethod
    def prefixed_filename(cls, paper_id: str | None, name: str) -> str:
        """human readable hint: apply the <paper_id>_ artifact prefix exactly once."""

        resolved_id = cls.sanitize_paper_id(paper_id)
        prefix = f"{resolved_id}_"
        return name if str(name).startswith(prefix) else f"{prefix}{name}"

    @classmethod
    def legacy_prefixed_filename(cls, paper_id: str | None, name: str) -> str:
        """human readable hint: read the old hash-prefixed cache shape without creating it."""

        resolved_id = cls.sanitize_paper_id(paper_id)
        prefix = f"#{resolved_id}_"
        return name if str(name).startswith(prefix) else f"{prefix}{name}"

    @classmethod
    def canonical_pdf_filename(cls, paper_id: str | None) -> str:
        """human readable hint: name the paper PDF by its ID once, without artifact suffixes."""

        return f"{cls.sanitize_paper_id(paper_id)}.pdf"

    def candidates(self, name: str, paper_id: str | None = None) -> list[Path]:
        """human readable hint: return canonical prefixed path first, then legacy fallback."""

        resolved_id = self.sanitize_paper_id(paper_id or self.paper_id)
        prefixed = self.folder / self.prefixed_filename(resolved_id, name)
        hash_prefixed = self.folder / self.legacy_prefixed_filename(resolved_id, name)
        legacy = self.folder / name
        candidates = [prefixed, hash_prefixed, legacy]
        unique: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            key = candidate.name
            if key in seen:
                continue
            unique.append(candidate)
            seen.add(key)
        return unique

    def first_existing(self, name: str, paper_id: str | None = None) -> Path | None:
        """human readable hint: find an existing canonical-or-legacy file without rescanning the folder."""

        for candidate in self.candidates(name, paper_id=paper_id):
            if candidate.name in self._files_by_name or candidate.exists():
                return candidate
        return None

    def ensure_prefixed_path(self, name: str, paper_id: str | None = None) -> Path:
        """human readable hint: migrate a legacy filename only when no canonical file exists."""

        candidates = self.candidates(name, paper_id=paper_id)
        prefixed_path = candidates[0]
        legacy_path = next(
            (
                candidate
                for candidate in candidates[1:]
                if candidate.name in self._files_by_name or candidate.exists()
            ),
            None,
        )
        if prefixed_path.name in self._files_by_name or prefixed_path.exists():
            return prefixed_path
        if not legacy_path or (legacy_path.name not in self._files_by_name and not legacy_path.exists()):
            return prefixed_path
        try:
            if sys.platform == "win32":
                Path("\\\\?\\" + str(legacy_path)).replace(Path("\\\\?\\" + str(prefixed_path)))
            else:
                legacy_path.replace(prefixed_path)
            self.refresh()
            return prefixed_path
        except Exception:
            return legacy_path

    def artifact_candidates(self, stage: str, paper_id: str | None = None) -> list[Path]:
        """human readable hint: return candidate compact artifact names for one stage."""

        return self.candidates(f"{stage}_artifact.json", paper_id=paper_id)

    def selected_chunk_candidates(self, stage: str, paper_id: str | None = None) -> list[Path]:
        """human readable hint: return candidate selected-chunk sidecars for one stage."""

        return self.candidates(f"{stage}_selected_chunks.jsonl", paper_id=paper_id)

    def pdf_candidates(self, paper_id: str | None = None) -> list[Path]:
        """human readable hint: prefer the single ID-named PDF without rescanning per check."""

        resolved_id = self.sanitize_paper_id(paper_id or self.paper_id)
        pdfs = sorted(
            [path for path in self._files_by_name.values() if path.suffix.lower() == ".pdf"],
            key=lambda path: path.name,
        )
        canonical = self.folder / self.canonical_pdf_filename(resolved_id)
        artifact_prefix = f"{resolved_id}_"
        legacy_prefix = f"#{resolved_id}_"
        ordered: list[Path] = []
        if canonical.name in self._files_by_name or canonical.exists():
            ordered.append(canonical)
        ordered.extend(path for path in pdfs if path.name.startswith(artifact_prefix) and path not in ordered)
        ordered.extend(path for path in pdfs if path.name.startswith(legacy_prefix) and path not in ordered)
        ordered.extend(path for path in pdfs if path not in ordered)
        return ordered


def _sanitize_paper_id_for_filename(paper_id: str) -> str:
    return PerPaperFileIndex.sanitize_paper_id(paper_id)


def _paper_id_hint(folder: Path) -> str:
    return PerPaperFileIndex.paper_id_hint(folder)


def _prefixed_candidates(folder: Path, name: str, paper_id: str | None = None) -> list[Path]:
    return PerPaperFileIndex(folder, paper_id=paper_id).candidates(name, paper_id=paper_id)


@dataclass
class PaperItem:
    """human readable hint: one prepared paper folder with metadata, evidence chunks, and extracted text."""

    paper_id: str
    folder_path: Path
    pdf_path: Path | None
    metadata: dict[str, Any]
    selected_chunks: list[dict]
    normalized_text: str
    supplemental_cited_evidence: str = ""


@dataclass(frozen=True)
class SupplementalEvidenceSource:
    """human readable hint: one user-supplied cited source text attached to a paper."""

    source_path: Path
    text: str


class SupplementalCitedEvidenceLoader:
    """human readable hint: load optional cited-source text without encoding review facts in pipeline code."""

    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        settings = settings if isinstance(settings, dict) else {}
        self.enabled = bool(settings.get("enabled", False))
        self.folder_names = self._clean_names(settings.get("folder_names"), default=["supplemental_cited_evidence"])
        self.file_globs = self._clean_names(settings.get("file_globs"), default=["*.txt", "*.md"])
        self.max_files_per_paper = max(0, int(settings.get("max_files_per_paper", 8) or 0))
        self.max_words_per_file = max(0, int(settings.get("max_words_per_file", 4000) or 0))

    @classmethod
    def from_user_config(cls) -> "SupplementalCitedEvidenceLoader":
        """human readable hint: construct the loader from the visible user-editable config block."""

        return cls(DATA_EXTRACTION_SUPPLEMENTAL_CITED_EVIDENCE)

    @staticmethod
    def _clean_names(value: Any, default: list[str]) -> list[str]:
        if not isinstance(value, list):
            return list(default)
        cleaned = [str(item).strip() for item in value if str(item or "").strip()]
        return cleaned or list(default)

    def load_for_folder(self, folder: Path) -> str:
        """human readable hint: combine configured supplemental files into a provenance-labeled prompt block."""

        if not self.enabled or self.max_files_per_paper <= 0:
            return ""

        sources = self._collect_sources(folder)
        if not sources:
            return ""

        parts = [
            "[Supplemental Cited Evidence]",
            "Use these user-supplied cited-source excerpts only when a schema field permits cited or supplemental evidence. Keep provenance visible in the supporting quote.",
        ]
        for source in sources:
            relative_path = self._relative_source_path(folder, source.source_path)
            parts.append(f"[Supplemental Source: {relative_path}]\n{source.text}")
        return "\n\n".join(parts)

    def _collect_sources(self, folder: Path) -> list[SupplementalEvidenceSource]:
        # human readable hint: only files inside configured per-paper subfolders are eligible.
        sources: list[SupplementalEvidenceSource] = []
        seen_paths: set[Path] = set()
        for folder_name in self.folder_names:
            source_dir = folder / folder_name
            if not source_dir.exists() or not source_dir.is_dir():
                continue
            for pattern in self.file_globs:
                for path in sorted(source_dir.glob(pattern)):
                    if not path.is_file() or path in seen_paths:
                        continue
                    text = self._read_text(path)
                    if not text:
                        continue
                    sources.append(SupplementalEvidenceSource(source_path=path, text=text))
                    seen_paths.add(path)
                    if len(sources) >= self.max_files_per_paper:
                        return sources
        return sources

    def _read_text(self, path: Path) -> str:
        if path.suffix.lower() == ".pdf":
            try:
                text, _parser_level = extract_markdown_from_pdf_with_level(path)
            except Exception:
                return ""
            return self._trim_text(text)

        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            try:
                text = path.read_text(encoding="utf-8-sig")
            except Exception:
                return ""
        except Exception:
            return ""

        return self._trim_text(text)

    def _trim_text(self, text: str) -> str:
        # human readable hint: keep supplemental evidence bounded so one cited source cannot crowd out the primary paper.
        normalized = normalize_extracted_text_for_llm(text).strip()
        if self.max_words_per_file > 0:
            words = normalized.split()
            if len(words) > self.max_words_per_file:
                normalized = " ".join(words[: self.max_words_per_file])
        return normalized

    @staticmethod
    def _relative_source_path(folder: Path, path: Path) -> str:
        try:
            return str(path.relative_to(folder))
        except ValueError:
            return path.name


def load_metadata(folder: Path, file_index: PerPaperFileIndex | None = None) -> tuple[str, dict[str, Any]]:
    """human readable hint: read compact artifact metadata for each included paper."""

    file_index = file_index or PerPaperFileIndex(folder)
    hint = file_index.paper_id or _paper_id_hint(folder)
    for prefix in ["data_extraction", "full_text"]:
        artifact_path = file_index.first_existing(f"{prefix}_artifact.json", paper_id=hint)
        if artifact_path:
            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    paper_id = str(payload.get("paper_id") or folder.name)
                    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
                    return paper_id, metadata
            except Exception:
                continue
    return folder.name, {}


def load_paper_text(folder: Path, file_index: PerPaperFileIndex | None = None) -> tuple[str, Path | None]:
    """human readable hint: use cached normalized text first, then parse the local PDF when needed."""

    file_index = file_index or PerPaperFileIndex(folder)
    hint = file_index.paper_id or _paper_id_hint(folder)
    normalized_path = file_index.first_existing("full_text_normalized.txt", paper_id=hint)
    if normalized_path:
        return normalized_path.read_text(encoding="utf-8"), None

    pdfs = file_index.pdf_candidates(paper_id=hint)
    if not pdfs:
        return "", None

    pdf_path = pdfs[0]
    try:
        text, _parser_level = extract_markdown_from_pdf_with_level(pdf_path)
        return text, pdf_path
    except Exception:
        return "", pdf_path


def load_selected_chunks(
    folder: Path,
    paper_id: str,
    file_index: PerPaperFileIndex | None = None,
) -> list[dict]:
    """human readable hint: load preselected data_extraction chunks copied from prior pipeline stages."""

    file_index = file_index or PerPaperFileIndex(folder, paper_id=paper_id)
    for prefix in ["data_extraction", "full_text"]:
        chunks_path = file_index.first_existing(f"{prefix}_selected_chunks.jsonl", paper_id=paper_id)
        if chunks_path:
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
                continue
    return []


def collect_papers(csv_dir: Path) -> list[PaperItem]:
    """human readable hint: collect all paper folders prepared for data extraction."""

    extraction_dir = csv_dir / "per_paper_data_extraction"
    if not extraction_dir.exists():
        return []

    papers: list[PaperItem] = []
    supplemental_loader = SupplementalCitedEvidenceLoader.from_user_config()
    for folder in sorted(extraction_dir.iterdir()):
        if not folder.is_dir():
            continue
        # human readable hint: build one folder file index and reuse it for metadata, chunks, and PDF/text lookup.
        file_index = PerPaperFileIndex(folder)
        paper_id, metadata = load_metadata(folder, file_index=file_index)
        file_index.paper_id = PerPaperFileIndex.sanitize_paper_id(paper_id)
        selected_chunks = load_selected_chunks(folder, paper_id, file_index=file_index)
        text, pdf_path = load_paper_text(folder, file_index=file_index)
        # human readable hint: keep table rows intact for downstream LLM extraction.
        normalized_text = normalize_extracted_text_for_llm(text or "") if text else ""
        supplemental_cited_evidence = supplemental_loader.load_for_folder(folder)
        papers.append(
            PaperItem(
                paper_id=str(paper_id).strip(),
                folder_path=folder,
                pdf_path=pdf_path,
                metadata=metadata,
                selected_chunks=selected_chunks,
                normalized_text=normalized_text,
                supplemental_cited_evidence=supplemental_cited_evidence,
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
    if paper.supplemental_cited_evidence:
        parts.append(paper.supplemental_cited_evidence)
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
