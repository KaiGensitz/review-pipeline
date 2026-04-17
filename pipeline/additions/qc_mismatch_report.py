"""Direct run: python -m pipeline.additions.qc_mismatch_report

Create a compact mismatch report from a QC validation alignment CSV.

Example:
python -m pipeline.additions.qc_mismatch_report \
  --alignment output/full_text_v5_retrievalUpgrade/full_text_qc_sample_validation_alignment_20260416_18-01.csv \
  --eligibility output/full_text_v5_retrievalUpgrade/full_text_qc_sample_main_eligibility_20260416_18-01_05f797bce8fe.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    """Return a typed dict when value is mapping-like; otherwise return an empty dict."""

    return value if isinstance(value, dict) else {}


def _to_bool(value: str | int | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _normalize_id(value: str | None) -> str:
    text = str(value or "").strip()
    return text.lstrip("#")


def _shorten(text: str, limit: int = 180) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _recommended_fix(human_tag: str, human_note: str, mismatch_type: str) -> str:
    tag = " ".join((human_tag or "").lower().split())
    note = " ".join((human_note or "").lower().split())

    if "language not en/de" in tag:
        return "Verify language gate diagnostics and detected language code handling."

    if "no artificial intelligence" in tag:
        return "Require explicit AI method controlling intervention decisions; do not accept generic proxy wording."

    if "no physical activity" in tag:
        return "Reinforce intervention-vs-monitoring distinction for PA outcomes."

    if "wrong publication type" in tag or "no intervention" in note:
        return "Increase exclusion weight for conceptual/framework/guideline papers without participant intervention exposure."

    if mismatch_type == "FP":
        return "Tighten exclusion logic for non-intervention or weak method evidence."

    return "Review false-negative context and recover missing intervention evidence in selected chunks."


def _load_selection_trace(eligibility_path: Path) -> dict[str, dict[str, Any]]:
    traces: dict[str, dict[str, Any]] = {}
    if not eligibility_path.exists():
        return traces

    with eligibility_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(payload, dict):
                continue

            paper_id = _normalize_id(str(payload.get("paper_id") or ""))
            if not paper_id:
                continue

            diagnostics = _as_dict(payload.get("diagnostics"))
            selection_trace = _as_dict(diagnostics.get("selection_trace"))

            traces[paper_id] = {
                "selected_sentence_count_min": selection_trace.get("selected_sentence_count_min"),
                "selected_sentence_count_mean": selection_trace.get("selected_sentence_count_mean"),
                "final_non_title_count": selection_trace.get("final_non_title_count"),
                "language_gate_excluded": selection_trace.get("language_gate_excluded"),
                "detected_language_code": selection_trace.get("detected_language_code"),
            }

    return traces


def build_mismatch_sheet(
    alignment_path: Path,
    output_csv: Path,
    eligibility_path: Path | None = None,
) -> tuple[int, Path]:
    traces = _load_selection_trace(eligibility_path) if eligibility_path else {}

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    mismatch_count = 0

    with alignment_path.open("r", encoding="utf-8-sig", newline="") as src, output_csv.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        fieldnames = [
            "ID",
            "mismatch_type",
            "human_decision",
            "ai_decision",
            "human_tag",
            "ai_reason",
            "human_note",
            "recommended_fix_signal",
            "selected_sentence_count_min",
            "selected_sentence_count_mean",
            "final_non_title_count",
            "language_gate_excluded",
            "detected_language_code",
            "title_short",
        ]
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            if _to_bool(row.get("decision_match")):
                continue

            mismatch_count += 1
            human_decision = int(str(row.get("human_decision") or "0"))
            ai_decision = int(str(row.get("ai_decision") or "0"))
            mismatch_type = "FP" if (human_decision == 0 and ai_decision == 1) else "FN"

            paper_id = _normalize_id(row.get("ID"))
            trace: dict[str, Any] = traces.get(paper_id) or {}

            writer.writerow(
                {
                    "ID": paper_id,
                    "mismatch_type": mismatch_type,
                    "human_decision": human_decision,
                    "ai_decision": ai_decision,
                    "human_tag": (row.get("human_tag") or "").strip(),
                    "ai_reason": (row.get("ai_reason") or "").strip(),
                    "human_note": (row.get("human_note") or "").strip(),
                    "recommended_fix_signal": _recommended_fix(
                        row.get("human_tag") or "", row.get("human_note") or "", mismatch_type
                    ),
                    "selected_sentence_count_min": trace.get("selected_sentence_count_min"),
                    "selected_sentence_count_mean": trace.get("selected_sentence_count_mean"),
                    "final_non_title_count": trace.get("final_non_title_count"),
                    "language_gate_excluded": trace.get("language_gate_excluded"),
                    "detected_language_code": trace.get("detected_language_code"),
                    "title_short": _shorten(row.get("Title") or ""),
                }
            )

    return mismatch_count, output_csv


def _default_output_path(alignment_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H-%M")
    return alignment_path.parent / f"{alignment_path.stem}_mismatch_sheet_{timestamp}.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a compact mismatch sheet from a QC alignment CSV.")
    parser.add_argument("--alignment", required=True, help="Path to *_validation_alignment_*.csv")
    parser.add_argument(
        "--eligibility",
        default=None,
        help="Optional path to matching *_eligibility_*.jsonl for selection_trace columns.",
    )
    parser.add_argument("--out", default=None, help="Optional output CSV path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    alignment_path = Path(args.alignment)
    if not alignment_path.exists():
        raise FileNotFoundError(f"Alignment file not found: {alignment_path}")

    eligibility_path = Path(args.eligibility) if args.eligibility else None
    output_path = Path(args.out) if args.out else _default_output_path(alignment_path)

    count, out_path = build_mismatch_sheet(alignment_path, output_path, eligibility_path=eligibility_path)
    print(f"Mismatch sheet written: {out_path}")
    print(f"Mismatches captured: {count}")


if __name__ == "__main__":
    main()
