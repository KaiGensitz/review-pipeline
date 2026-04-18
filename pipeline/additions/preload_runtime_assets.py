"""One-time runtime asset preload for stable offline screening runs.

Run this script before screening to download tokenizer/model assets explicitly,
so `python main.py` does not trigger downloads during active runs.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import nltk

from config.user_orchestrator import REPO_ROOT
from pipeline.selection.pdf_parser import extract_markdown_from_pdf_with_level, warm_docling_for_pdf


def _truthy(value: str | None) -> bool:
    """Parse common truthy values from environment-style strings."""

    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int) -> int:
    """Parse an integer setting from environment values safely."""

    try:
        return int(str(value or "").strip())
    except Exception:
        return int(default)


def _configure_preload_logging() -> None:
    """Reduce noisy third-party logs during one-time preload."""

    for logger_name in ("RapidOCR", "rapidocr", "huggingface_hub", "filelock", "transformers", "urllib3"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def _ensure_nltk_assets() -> None:
    """Download required NLTK tokenizer data if missing."""

    required = (
        ("tokenizers/punkt", "punkt"),
        ("tokenizers/punkt_tab", "punkt_tab"),
    )

    for resource_path, package_name in required:
        try:
            nltk.data.find(resource_path)
            print(f"[assets] NLTK resource already present: {package_name}")
        except LookupError:
            print(f"[assets] Downloading NLTK resource: {package_name}")
            nltk.download(package_name, quiet=False)


def _find_sample_pdf(repo_root: Path) -> Path | None:
    """Pick the smallest existing PDF for parser warmup (fastest candidate)."""

    search_roots = (
        repo_root / "input" / "per_paper_full_text",
        repo_root / "input" / "per_paper_data_extraction",
    )

    best_path: Path | None = None
    best_size: int | None = None

    for root in search_roots:
        if not root.exists():
            continue
        for candidate in root.rglob("*.pdf"):
            try:
                size = int(candidate.stat().st_size)
            except Exception:
                continue
            if best_size is None or size < best_size:
                best_size = size
                best_path = candidate

    return best_path


def _warm_docling_assets(sample_pdf: Path) -> bool:
    """Trigger Docling parser once so required model files are cached."""

    warmup_timeout_seconds = _parse_int(os.getenv("DOCLING_WARMUP_TIMEOUT_SECONDS"), 300)
    warmup_timeout_label = "none (wait until completion)" if warmup_timeout_seconds <= 0 else f"{warmup_timeout_seconds}s"

    print(f"[assets] Warming Docling model assets with: {sample_pdf}")
    print(f"[assets] Docling warmup timeout: {warmup_timeout_label}")

    timeout_value = None if warmup_timeout_seconds <= 0 else warmup_timeout_seconds
    if not warm_docling_for_pdf(sample_pdf, timeout_seconds=timeout_value):
        print("[error] Docling warmup failed; required fallback assets may still be missing.")
        return False

    print("[assets] Docling warmup succeeded.")
    print(f"[assets] Verifying normal parser path with: {sample_pdf}")
    try:
        text, parser_level = extract_markdown_from_pdf_with_level(sample_pdf)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[error] Parser warmup failed: {exc}")
        return False

    if not (text or "").strip():
        print("[error] Parser warmup returned empty text; cache warmup may be incomplete.")
        return False

    print(f"[assets] Parser warmup succeeded (level='{parser_level}')")
    return True


def main() -> int:
    """Preload tokenizer and parser assets before screening runs."""

    print("[assets] Starting runtime asset preload...")
    _configure_preload_logging()

    # Explicit preload mode should allow model downloads if needed.
    os.environ.pop("HF_HUB_OFFLINE", None)
    os.environ.pop("TRANSFORMERS_OFFLINE", None)

    hf_token = str(os.getenv("HF_TOKEN", "")).strip()
    if hf_token:
        print("[assets] HF_TOKEN detected. Authenticated Hugging Face downloads enabled.")
    else:
        print("[assets] HF_TOKEN not set. Downloads will be unauthenticated (lower rate limits).")
        print("[next] Optional: add HF_TOKEN=... to .env for faster and more reliable model download.")

    _ensure_nltk_assets()

    use_advanced_parser = _truthy(os.getenv("USE_ADVANCED_PDF_PARSER", "0"))
    if not use_advanced_parser:
        print("[assets] USE_ADVANCED_PDF_PARSER is disabled; skipping Docling warmup.")
        print("[assets] Preload completed.")
        return 0

    sample_pdf = _find_sample_pdf(REPO_ROOT)
    if sample_pdf is None:
        print("[warning] No PDF found for Docling warmup. Add at least one PDF under input/per_paper_full_text and rerun this preload script.")
        return 0

    if not _warm_docling_assets(sample_pdf):
        return 1

    print("[assets] Preload completed. Screening runs now enforce offline model-cache mode.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
