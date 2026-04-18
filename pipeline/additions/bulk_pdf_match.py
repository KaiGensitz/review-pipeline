"""Bulk-match PDFs from papers/ into input/per_paper_full_text folders.

This utility matches candidate PDFs by first-author surname, publication year,
and title similarity. It supports a safe dry-run mode (default) and writes a
CSV report for audit and manual review.
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path


def _normalize_text(value: str) -> str:
    """human readable hint: normalize text for robust filename/title comparison."""

    text = unicodedata.normalize("NFKD", value or "")
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"\.pdf$", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_year(value: str) -> str:
    """human readable hint: extract publication year if present (1900-2099)."""

    match = re.search(r"\b(19|20)\d{2}\b", value or "")
    return match.group(0) if match else ""


def _first_author_surname(authors: str) -> str:
    """human readable hint: parse first-author surname from metadata Authors field."""

    raw = (authors or "").strip()
    if not raw:
        return ""

    first_part = raw.split(";")[0].strip()
    if "," in first_part:
        surname = first_part.split(",", 1)[0].strip()
    else:
        surname = first_part.split(" ")[0].strip()

    return _normalize_text(surname).replace(" ", "")


@dataclass
class PdfCandidate:
    path: Path
    normalized_name: str
    name_tokens: set[str]
    year: str


@dataclass
class FolderTarget:
    folder: Path
    paper_id: str
    title: str
    title_norm: str
    title_tokens: set[str]
    first_author: str
    year: str


def _load_targets(target_root: Path, overwrite: bool) -> list[FolderTarget]:
    """human readable hint: collect per-paper folders and metadata for matching."""

    targets: list[FolderTarget] = []
    for folder in sorted(target_root.iterdir()):
        if not folder.is_dir():
            continue

        if not overwrite and any(folder.glob("*.pdf")):
            continue

        metadata: dict = {}
        try:
            import json

            artifact_candidates = [
                folder / "full_text_artifact.json",
                folder / "data_extraction_artifact.json",
            ]
            for artifact_path in artifact_candidates:
                if not artifact_path.exists():
                    continue
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    nested = payload.get("metadata")
                    if isinstance(nested, dict):
                        metadata = nested
                        break
                    metadata = payload
                    break
        except Exception:
            metadata = {}

        if not metadata:
            continue

        title = str(metadata.get("Title") or metadata.get("title") or "").strip()
        authors = str(metadata.get("Authors") or metadata.get("authors") or "").strip()
        year = str(metadata.get("Published Year") or metadata.get("year") or "").strip()
        paper_id = str(metadata.get("Covidence #") or metadata.get("paper_id") or folder.name).strip()

        title_norm = _normalize_text(title)
        title_tokens = {tok for tok in title_norm.split(" ") if len(tok) >= 3}
        first_author = _first_author_surname(authors)
        year_clean = _extract_year(year)

        targets.append(
            FolderTarget(
                folder=folder,
                paper_id=paper_id,
                title=title,
                title_norm=title_norm,
                title_tokens=title_tokens,
                first_author=first_author,
                year=year_clean,
            )
        )

    return targets


def _load_candidates(source_root: Path) -> list[PdfCandidate]:
    """human readable hint: index all candidate PDFs under papers/ recursively."""

    candidates: list[PdfCandidate] = []
    for pdf_path in sorted(source_root.rglob("*.pdf")):
        normalized_name = _normalize_text(pdf_path.stem)
        tokens = {tok for tok in normalized_name.split(" ") if len(tok) >= 3}
        year = _extract_year(normalized_name)
        candidates.append(
            PdfCandidate(
                path=pdf_path,
                normalized_name=normalized_name,
                name_tokens=tokens,
                year=year,
            )
        )
    return candidates


def _score(target: FolderTarget, candidate: PdfCandidate) -> tuple[float, dict[str, float]]:
    """human readable hint: combine author, year, and title signals into one match score."""

    signals = {
        "author": 0.0,
        "year": 0.0,
        "title_ratio": 0.0,
        "token_overlap": 0.0,
    }

    if target.first_author:
        if target.first_author in candidate.normalized_name.replace(" ", ""):
            signals["author"] = 1.0

    if target.year and candidate.year:
        signals["year"] = 1.0 if target.year == candidate.year else 0.0

    if target.title_norm:
        signals["title_ratio"] = SequenceMatcher(
            None, target.title_norm, candidate.normalized_name
        ).ratio()

    if target.title_tokens and candidate.name_tokens:
        inter = len(target.title_tokens.intersection(candidate.name_tokens))
        union = len(target.title_tokens.union(candidate.name_tokens))
        if union > 0:
            signals["token_overlap"] = inter / union

    score = (
        40.0 * signals["author"]
        + 25.0 * signals["year"]
        + 25.0 * signals["title_ratio"]
        + 10.0 * signals["token_overlap"]
    )
    return score, signals


def _destination_pdf_path(target: FolderTarget) -> Path:
    """human readable hint: keep deterministic PDF naming inside each target folder."""

    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "", target.paper_id.lstrip("#")) or "paper"
    return target.folder / f"{safe_id}.pdf"


def run(args: argparse.Namespace) -> int:
    """human readable hint: execute match workflow and optionally copy matched PDFs."""

    source_root = Path(args.source_papers).resolve()
    target_root = Path(args.target_folders).resolve()

    if not source_root.exists():
        print(f"[error] source papers directory not found: {source_root}")
        return 1
    if not target_root.exists():
        print(f"[error] target folder root not found: {target_root}")
        return 1

    targets = _load_targets(target_root, overwrite=args.overwrite)
    candidates = _load_candidates(source_root)

    if not targets:
        print("[info] no target folders require matching (all already contain PDFs or metadata missing).")
        return 0
    if not candidates:
        print("[error] no candidate PDFs found under source directory.")
        return 1

    report_rows: list[dict[str, str]] = []
    copied = 0
    ambiguous = 0
    unmatched = 0

    for target in targets:
        ranked: list[tuple[float, PdfCandidate, dict[str, float]]] = []
        for candidate in candidates:
            score, signals = _score(target, candidate)
            ranked.append((score, candidate, signals))

        ranked.sort(key=lambda x: x[0], reverse=True)
        best_score, best_candidate, best_signals = ranked[0]
        second_score = ranked[1][0] if len(ranked) > 1 else 0.0
        gap = best_score - second_score

        decision = "unmatched"
        reason = "below_threshold"

        if best_score >= args.min_score and gap >= args.min_gap:
            decision = "match"
            reason = "score_and_gap_ok"
        elif best_score >= args.min_score and gap < args.min_gap:
            decision = "ambiguous"
            reason = "small_gap"

        dest_path = _destination_pdf_path(target)

        if decision == "match" and args.apply:
            try:
                if dest_path.exists() and not args.overwrite:
                    decision = "skipped_exists"
                    reason = "target_pdf_exists"
                else:
                    shutil.copy2(best_candidate.path, dest_path)
                    copied += 1
            except Exception as exc:
                decision = "copy_failed"
                reason = str(exc)

        if decision == "ambiguous":
            ambiguous += 1
        elif decision in {"unmatched", "copy_failed"}:
            unmatched += 1

        report_rows.append(
            {
                "paper_id": target.paper_id,
                "folder": str(target.folder),
                "title": target.title,
                "first_author": target.first_author,
                "year": target.year,
                "decision": decision,
                "reason": reason,
                "best_score": f"{best_score:.3f}",
                "second_score": f"{second_score:.3f}",
                "score_gap": f"{gap:.3f}",
                "best_author_signal": f"{best_signals['author']:.3f}",
                "best_year_signal": f"{best_signals['year']:.3f}",
                "best_title_ratio": f"{best_signals['title_ratio']:.3f}",
                "best_token_overlap": f"{best_signals['token_overlap']:.3f}",
                "best_pdf": str(best_candidate.path),
            }
        )

    report_path = Path(args.report).resolve() if args.report else (
        Path("output")
        / "full_text"
        / f"full_text_bulk_pdf_match_report_{datetime.now().strftime('%Y%m%d_%H-%M')}.csv"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "paper_id",
        "folder",
        "title",
        "first_author",
        "year",
        "decision",
        "reason",
        "best_score",
        "second_score",
        "score_gap",
        "best_author_signal",
        "best_year_signal",
        "best_title_ratio",
        "best_token_overlap",
        "best_pdf",
    ]
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[bulk-match] mode={mode}")
    print(f"[bulk-match] targets={len(targets)} candidates={len(candidates)}")
    print(f"[bulk-match] copied={copied} ambiguous={ambiguous} unmatched_or_failed={unmatched}")
    print(f"[bulk-match] report={report_path}")

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    """human readable hint: define CLI flags for safe review-first matching."""

    parser = argparse.ArgumentParser(
        description="Bulk-match PDFs from papers/ to per-paper full_text folders by author/year/title."
    )
    parser.add_argument(
        "--source-papers",
        default="papers",
        help="Directory containing candidate PDFs (recursive search).",
    )
    parser.add_argument(
        "--target-folders",
        default="input/per_paper_full_text",
        help="Directory containing per-paper folders with full_text_artifact.json metadata.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Copy matched PDFs into target folders. Default is dry-run only.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing existing PDFs in target folders.",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=55.0,
        help="Minimum score for an automatic match (default: 55).",
    )
    parser.add_argument(
        "--min-gap",
        type=float,
        default=8.0,
        help="Minimum score gap between best and second-best candidate (default: 8).",
    )
    parser.add_argument(
        "--report",
        default="",
        help="Optional custom CSV report path.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
