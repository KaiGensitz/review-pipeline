"""Layout-aware PDF to Markdown extraction for screening workflows.

This module provides a single public function,
`extract_markdown_from_pdf`, that is resilient to parser failures
and always returns a string so the main pipeline does not crash.
"""

from __future__ import annotations

from multiprocessing import Process, Queue as MPQueue
from pathlib import Path
from typing import Any
import importlib
import logging
import os
import re
import shutil

LOGGER = logging.getLogger(__name__)

DOCLING_TIMEOUT_SECONDS = 120
MIN_CHARS_PER_PAGE = 100
PARSER_LEVEL_DOCLING_SUCCESS = "Fallback parser: Docling"
PARSER_LEVEL_PYMUPDF_FALLBACK = "Primary parser: pymupdf4llm"
PARSER_LEVEL_LOW_DENSITY = "Low text density"
PARSER_LEVEL_OCR_SUCCESS = "Fallback parser: OCR"


_DoclingConverter: Any | None = None
_docling_import_attempted = False
pymupdf4llm: Any | None = None
_pymupdf4llm_import_attempted = False
fitz: Any | None = None
_fitz_import_attempted = False
pytesseract: Any | None = None
_pytesseract_import_attempted = False
Image: Any | None = None
_pil_import_attempted = False


def _load_docling_converter() -> Any | None:
    """human readable hint: import Docling lazily so ordinary pipeline imports stay fast."""

    global _DoclingConverter, _docling_import_attempted
    if _docling_import_attempted:
        return _DoclingConverter
    _docling_import_attempted = True
    try:
        docling_module = importlib.import_module("docling.document_converter")
        _DoclingConverter = getattr(docling_module, "DoclingDocumentConverter", None)
        if _DoclingConverter is None:
            _DoclingConverter = getattr(docling_module, "DocumentConverter", None)
    except Exception:
        _DoclingConverter = None
    return _DoclingConverter


def _load_pymupdf4llm() -> Any | None:
    """human readable hint: import pymupdf4llm only when the advanced parser is used."""

    global pymupdf4llm, _pymupdf4llm_import_attempted
    if _pymupdf4llm_import_attempted:
        return pymupdf4llm
    _pymupdf4llm_import_attempted = True
    try:
        pymupdf4llm = importlib.import_module("pymupdf4llm")
    except Exception:
        pymupdf4llm = None
    return pymupdf4llm


def _load_fitz() -> Any | None:
    """human readable hint: import PyMuPDF only when page rendering or page counts are needed."""

    global fitz, _fitz_import_attempted
    if _fitz_import_attempted:
        return fitz
    _fitz_import_attempted = True
    try:
        fitz = importlib.import_module("fitz")
    except Exception:
        fitz = None
    return fitz


def _load_pytesseract() -> Any | None:
    """human readable hint: import pytesseract only when OCR fallback is actually attempted."""

    global pytesseract, _pytesseract_import_attempted
    if _pytesseract_import_attempted:
        return pytesseract
    _pytesseract_import_attempted = True
    try:
        pytesseract = importlib.import_module("pytesseract")
    except Exception:
        pytesseract = None
    return pytesseract


def _load_pil_image() -> Any | None:
    """human readable hint: import Pillow only when OCR image conversion is needed."""

    global Image, _pil_import_attempted
    if _pil_import_attempted:
        return Image
    _pil_import_attempted = True
    try:
        Image = importlib.import_module("PIL.Image")
    except Exception:
        Image = None
    return Image


REFERENCE_HEADER_PATTERN = re.compile(
    r"(?im)^\s{0,3}(?:#{1,6}\s*)?(?:\d{1,2}(?:\.\d+)*[\.)]?\s*)?"
    r"(?:references?|bibliography|works\s+cited|literature\s+cited)\s*:?\s*$"
)


def _coerce_to_text(value: Any) -> str:
    """Best-effort conversion of parser outputs to plain strings."""

    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return "\n".join(_coerce_to_text(item) for item in value)
    if isinstance(value, dict):
        for key in ("markdown", "text", "content"):
            if key in value:
                return _coerce_to_text(value[key])
        return str(value)
    return str(value)


def _extract_markdown_from_docling_result(result: Any) -> str:
    """Extract Markdown from a Docling conversion result across common APIs."""

    if result is None:
        return ""

    candidates: list[Any] = []
    document = getattr(result, "document", None)
    if document is not None:
        candidates.append(document)
    candidates.append(result)

    method_names = (
        "export_to_markdown",
        "to_markdown",
        "as_markdown",
        "markdown",
    )

    for candidate in candidates:
        for name in method_names:
            attr = getattr(candidate, name, None)
            if callable(attr):
                try:
                    text = _coerce_to_text(attr())
                    if text.strip():
                        return text
                except TypeError:
                    # Some versions may require arguments; skip safely.
                    continue
                except Exception:
                    continue
            elif attr is not None:
                text = _coerce_to_text(attr)
                if text.strip():
                    return text

    # Final fallback: try direct coercion.
    return _coerce_to_text(result)


def _run_docling(pdf_path: Path) -> str:
    """Run Docling conversion synchronously and return Markdown text."""

    converter_cls = _load_docling_converter()
    if converter_cls is None:
        raise RuntimeError("Docling is not installed or unavailable.")

    converter = converter_cls()
    result = converter.convert(str(pdf_path))
    markdown = _extract_markdown_from_docling_result(result)
    if not markdown.strip():
        raise RuntimeError("Docling returned empty Markdown output.")
    return markdown


def _docling_worker(pdf_path: str, output_queue: MPQueue) -> None:
    """Run Docling in a child process and push either text or error to queue."""

    try:
        markdown = _run_docling(Path(pdf_path))
        output_queue.put(("ok", markdown))
    except Exception as exc:  # pragma: no cover - defensive worker boundary
        output_queue.put(("error", f"{type(exc).__name__}: {exc}"))


def _extract_with_docling(pdf_path: Path, timeout_seconds: int | None = DOCLING_TIMEOUT_SECONDS) -> str:
    """Run Docling with optional timeout handling.

    - Runtime parsing should use a bounded timeout (default).
    - Warmup/preload can pass None (or <=0) to wait until completion.
    """

    output_queue: MPQueue = MPQueue(maxsize=1)
    process = Process(
        target=_docling_worker,
        args=(str(pdf_path), output_queue),
        daemon=True,
    )
    process.start()
    if timeout_seconds is None:
        timeout_value = 0
    else:
        timeout_value = int(timeout_seconds)
    use_timeout = timeout_value > 0
    if use_timeout:
        process.join(timeout_value)
    else:
        process.join()

    if use_timeout and process.is_alive():
        process.terminate()
        process.join(timeout=5)
        raise TimeoutError(f"Docling timed out after {timeout_seconds} seconds for {pdf_path}")

    if output_queue.empty():
        raise RuntimeError(
            f"Docling process finished without output for {pdf_path} (exit code: {process.exitcode})."
        )

    status, payload = output_queue.get()
    if status == "ok":
        markdown = _coerce_to_text(payload)
        if markdown.strip():
            return markdown
        raise RuntimeError("Docling returned empty Markdown output.")

    raise RuntimeError(f"Docling extraction failed for {pdf_path}: {payload}")


def _extract_with_pymupdf4llm(pdf_path: Path) -> str:
    """Fallback parser using pymupdf4llm Markdown export."""

    pymupdf_module = _load_pymupdf4llm()
    if pymupdf_module is None:
        raise RuntimeError("pymupdf4llm is not installed or unavailable.")

    markdown = _coerce_to_text(pymupdf_module.to_markdown(str(pdf_path)))
    if not markdown.strip():
        raise RuntimeError("pymupdf4llm returned empty Markdown output.")
    return markdown


def _get_num_pages(pdf_path: Path) -> int:
    """Safely read PDF page count using fitz; return 0 when unavailable."""

    fitz_module = _load_fitz()
    if fitz_module is None:
        return 0

    try:
        with fitz_module.open(str(pdf_path)) as document:
            return int(document.page_count)
    except Exception:
        return 0


def _check_text_density(text: str, num_pages: int) -> bool:
    """Return False when extracted text is below 100 characters per page."""

    if num_pages <= 0:
        return False
    char_count = len((text or "").strip())
    return (char_count / num_pages) >= MIN_CHARS_PER_PAGE


def _resolve_tesseract_command() -> str | None:
    """Resolve the tesseract executable path from env, PATH, or common install locations."""

    configured = str(os.getenv("TESSERACT_CMD", "")).strip()
    if configured and Path(configured).exists():
        return configured

    discovered = shutil.which("tesseract")
    if discovered:
        return discovered

    local_appdata = os.getenv("LOCALAPPDATA", "")
    candidates = [
        "C:\\Program Files\\Tesseract-OCR\\tesseract.exe",
        "C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe",
    ]
    if local_appdata:
        candidates.append(os.path.join(local_appdata, "Programs", "Tesseract-OCR", "tesseract.exe"))

    for candidate in candidates:
        if Path(candidate).exists():
            return candidate
    return None


def _ocr_with_tesseract(pdf_path: Path) -> str:
    """Render pages with fitz and run page-wise OCR through pytesseract."""

    fitz_module = _load_fitz()
    pytesseract_module = _load_pytesseract()
    image_module = _load_pil_image()
    if fitz_module is None:
        raise RuntimeError("PyMuPDF (fitz) is required for OCR rendering but is unavailable.")
    if pytesseract_module is None:
        raise RuntimeError("pytesseract is not installed or unavailable.")
    if image_module is None:
        raise RuntimeError("Pillow is required for OCR image conversion but is unavailable.")

    tesseract_cmd = _resolve_tesseract_command()
    if not tesseract_cmd:
        raise RuntimeError(
            "Tesseract executable not found. Set TESSERACT_CMD or add tesseract to PATH."
        )
    pytesseract_module.pytesseract.tesseract_cmd = tesseract_cmd

    extracted_pages: list[str] = []
    with fitz_module.open(str(pdf_path)) as document:
        for page_idx in range(int(document.page_count)):
            page_index = page_idx + 1
            page = document.load_page(page_idx)
            try:
                # 2x scaling improves OCR quality on small-font scientific PDFs.
                pix = page.get_pixmap(matrix=fitz_module.Matrix(2.0, 2.0), alpha=False)
                mode = "RGBA" if pix.alpha else "RGB"
                image = image_module.frombytes(mode, (pix.width, pix.height), pix.samples)
                if mode == "RGBA":
                    image = image.convert("RGB")
                page_text = _coerce_to_text(pytesseract_module.image_to_string(image))
                extracted_pages.append(page_text)
            except Exception as exc:
                LOGGER.warning(
                    "OCR failed on page %s of %s: %s",
                    page_index,
                    pdf_path,
                    exc,
                )

    return "\n\n".join(extracted_pages)


def _remove_references(markdown_text: str) -> str:
    """Truncate markdown at reference headers to avoid bibliographic contamination."""

    text = _coerce_to_text(markdown_text)
    if not text.strip():
        return ""

    match = REFERENCE_HEADER_PATTERN.search(text)
    if not match:
        return text.strip()
    return text[: match.start()].rstrip()


def extract_markdown_from_pdf_with_level(pdf_path: Path) -> tuple[str, str]:
    """Extract Markdown and parser-level label with pymupdf4llm primary order.

    Orchestration order:
    1) Try pymupdf4llm once as the primary parser.
    2) If pymupdf4llm fails: try Docling once (timeout-protected).
    3) If extracted text is still low density: OCR with fitz + pytesseract.
    4) Remove trailing references/bibliography section.

    This function is fail-safe by design and always returns a text/level tuple.
    """

    parser_level = ""

    try:
        path = Path(pdf_path)
    except Exception:
        LOGGER.exception("Invalid PDF path input: %r", pdf_path)
        return "", parser_level

    if not path.exists() or not path.is_file():
        LOGGER.error("PDF path does not exist or is not a file: %s", path)
        return "", parser_level

    markdown_text = ""
    parser_error = False
    page_count = _get_num_pages(path)

    try:
        markdown_text = _extract_with_pymupdf4llm(path)
        LOGGER.debug("pymupdf4llm primary extraction succeeded for %s", path)
        parser_level = PARSER_LEVEL_PYMUPDF_FALLBACK
    except Exception as exc:
        parser_error = True
        LOGGER.exception("pymupdf4llm primary extraction failed for %s: %s", path, exc)

    # Only try Docling when the primary parser failed.
    if parser_error:
        try:
            markdown_text = _extract_with_docling(path, timeout_seconds=DOCLING_TIMEOUT_SECONDS)
            LOGGER.debug("Docling fallback extraction succeeded for %s", path)
            parser_level = PARSER_LEVEL_DOCLING_SUCCESS
        except Exception as exc:
            LOGGER.exception("Docling fallback failed for %s: %s", path, exc)
            markdown_text = ""

    # OCR is the last resort when text extraction quality is too low.
    if not _check_text_density(markdown_text, page_count):
        parser_level = PARSER_LEVEL_LOW_DENSITY
        LOGGER.info(
            "Low text density detected for %s (pages=%s); attempting OCR.",
            path,
            page_count,
        )
        try:
            ocr_text = _ocr_with_tesseract(path)
            if ocr_text.strip():
                markdown_text = ocr_text
                LOGGER.debug("OCR fallback produced text for %s", path)
                parser_level = PARSER_LEVEL_OCR_SUCCESS
            else:
                LOGGER.warning("OCR fallback returned empty text for %s", path)
        except Exception as exc:
            LOGGER.exception("OCR fallback failed for %s: %s", path, exc)

    try:
        return _remove_references(markdown_text), parser_level
    except Exception:
        LOGGER.exception("Reference cleanup failed for %s; returning raw extracted text.", path)
        return _coerce_to_text(markdown_text), parser_level


def extract_markdown_from_pdf(pdf_path: Path) -> str:
    """Extract Markdown from PDF and return text only (compatibility wrapper)."""

    markdown_text, _ = extract_markdown_from_pdf_with_level(pdf_path)
    return markdown_text


def warm_docling_for_pdf(pdf_path: Path, timeout_seconds: int | None = None) -> bool:
    """Warm Docling model assets by forcing one Docling conversion attempt."""

    try:
        text = _extract_with_docling(Path(pdf_path), timeout_seconds=timeout_seconds)
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.warning("Docling warmup failed for %s: %s", pdf_path, exc)
        return False
    return bool((text or "").strip())


__all__ = [
    "extract_markdown_from_pdf",
    "extract_markdown_from_pdf_with_level",
    "warm_docling_for_pdf",
]
