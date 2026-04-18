"""Review utility for ambiguous/unmatched bulk PDF matches.

Reads a bulk-match report CSV and writes top-N PDF suggestions per flagged paper
for fast manual confirmation.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

from pipeline.additions import bulk_pdf_match as matcher


def _find_latest_report(default_dir: Path) -> Path | None:
    """human readable hint: locate the newest bulk-match report when path is omitted."""

    patterns = [
        "full_text_bulk_pdf_match_report_*.csv",
        "full_text_bulk_pdf_match_report*.csv",
    ]
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(default_dir.glob(pattern))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _read_report_rows(path: Path) -> list[dict[str, str]]:
    """human readable hint: parse report CSV rows used as review input."""

    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row:
                rows.append({str(k): str(v) for k, v in row.items()})
    return rows


def _targets_by_folder(target_root: Path) -> dict[str, matcher.FolderTarget]:
    """human readable hint: map folder path to target metadata for scoring."""

    targets = matcher._load_targets(target_root, overwrite=True)
    return {str(t.folder.resolve()): t for t in targets}


def run(args: argparse.Namespace) -> int:
    """human readable hint: generate ranked candidate suggestions for manual confirmation."""

    source_root = Path(args.source_papers).resolve()
    target_root = Path(args.target_folders).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not source_root.exists():
        print(f"[error] source papers directory not found: {source_root}")
        return 1
    if not target_root.exists():
        print(f"[error] target folders directory not found: {target_root}")
        return 1

    report_path: Path
    if args.report:
        report_path = Path(args.report).resolve()
        if not report_path.exists():
            print(f"[error] report file not found: {report_path}")
            return 1
    else:
        latest = _find_latest_report(output_dir)
        if not latest:
            print(f"[error] no report found in {output_dir}; run bulk_pdf_match first or pass --report")
            return 1
        report_path = latest

    report_rows = _read_report_rows(report_path)
    include_decisions = {d.strip().lower() for d in args.include_decisions.split(",") if d.strip()}

    filtered = [
        row
        for row in report_rows
        if row.get("decision", "").strip().lower() in include_decisions
    ]
    if args.paper_id:
        filtered = [row for row in filtered if row.get("paper_id", "").strip() == args.paper_id.strip()]

    if not filtered:
        print("[info] no report rows matched the requested filters.")
        return 0

    candidates = matcher._load_candidates(source_root)
    if not candidates:
        print("[error] no candidate PDFs found under source directory.")
        return 1

    target_map = _targets_by_folder(target_root)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"full_text_bulk_pdf_match_suggestions_{datetime.now().strftime('%Y%m%d_%H-%M')}.csv"

    fieldnames = [
        "paper_id",
        "folder",
        "source_decision",
        "source_reason",
        "rank",
        "candidate_pdf",
        "score",
        "author_signal",
        "year_signal",
        "title_ratio",
        "token_overlap",
    ]

    written = 0
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for row in filtered:
            folder = Path(row.get("folder", "")).resolve()
            target = target_map.get(str(folder))
            if target is None:
                continue

            ranked: list[tuple[float, matcher.PdfCandidate, dict[str, float]]] = []
            for candidate in candidates:
                score, signals = matcher._score(target, candidate)
                ranked.append((score, candidate, signals))
            ranked.sort(key=lambda x: x[0], reverse=True)

            top_n = max(1, int(args.top_n))
            for idx, (score, candidate, signals) in enumerate(ranked[:top_n], start=1):
                writer.writerow(
                    {
                        "paper_id": row.get("paper_id", ""),
                        "folder": str(folder),
                        "source_decision": row.get("decision", ""),
                        "source_reason": row.get("reason", ""),
                        "rank": str(idx),
                        "candidate_pdf": str(candidate.path),
                        "score": f"{score:.3f}",
                        "author_signal": f"{signals['author']:.3f}",
                        "year_signal": f"{signals['year']:.3f}",
                        "title_ratio": f"{signals['title_ratio']:.3f}",
                        "token_overlap": f"{signals['token_overlap']:.3f}",
                    }
                )
                written += 1

    print(f"[review] source_report={report_path}")
    print(f"[review] filtered_rows={len(filtered)} top_n={args.top_n} suggestions_written={written}")
    print(f"[review] output={out_path}")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    """human readable hint: define CLI options for focused review exports."""

    parser = argparse.ArgumentParser(
        description="Generate top-N PDF suggestions for ambiguous/unmatched bulk matches."
    )
    parser.add_argument(
        "--report",
        default="",
        help="Path to bulk match report CSV. If omitted, newest report in output/full_text is used.",
    )
    parser.add_argument(
        "--source-papers",
        default="papers",
        help="Directory containing candidate PDFs.",
    )
    parser.add_argument(
        "--target-folders",
        default="input/per_paper_full_text",
        help="Directory containing per-paper folders with full_text_artifact.json metadata.",
    )
    parser.add_argument(
        "--output-dir",
        default="output/full_text",
        help="Directory for suggestions CSV output.",
    )
    parser.add_argument(
        "--include-decisions",
        default="ambiguous,unmatched",
        help="Comma-separated report decisions to review (default: ambiguous,unmatched).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=3,
        help="How many candidate PDFs per paper to export (default: 3).",
    )
    parser.add_argument(
        "--paper-id",
        default="",
        help="Optional single paper_id filter for targeted review.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
