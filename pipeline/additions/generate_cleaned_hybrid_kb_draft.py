from __future__ import annotations

import argparse
from collections import Counter
import csv
from dataclasses import dataclass
import json
from pathlib import Path
import re


DOMAIN_KEYWORDS = {
    "smartphone",
    "mobile",
    "app",
    "mhealth",
    "physical",
    "activity",
    "exercise",
    "walking",
    "steps",
    "intervention",
    "chatbot",
    "machine",
    "learning",
    "reinforcement",
    "ai",
}

NEGATIVE_SIGNAL_KEYWORDS = {
    "review",
    "meta-analysis",
    "simulation",
    "abm",
    "children",
    "adolescent",
    "no smartphone",
    "wearable",
    "desktop",
    "protocol",
}

POS_DELIVERY_KEYWORDS = {"smartphone", "mobile", "app", "mhealth"}
POS_AI_KEYWORDS = {
    "artificial intelligence",
    "machine learning",
    "reinforcement learning",
    "deep learning",
    "chatbot",
    "llm",
    "bandit",
    "ai",
}
POS_PA_KEYWORDS = {"physical activity", "exercise", "walking", "steps", "mvpa", "sedentary"}

REFERENCE_PATTERN = re.compile(
    r"\b(references?|bibliography|copyright|all rights reserved|doi)\b",
    flags=re.IGNORECASE,
)
LOW_VALUE_PATTERN = re.compile(
    r"\b(creativecommons|creative commons|copyright holder|license|licence|credit line|"
    r"not for citation purposes|visual abstract|supplementary|received:|accepted:|"
    r"published:|xsl fo|corresponding author|page\s+\d+\s+of\s+\d+)\b",
    flags=re.IGNORECASE,
)
WORD_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9'/_-]*")


@dataclass(slots=True)
class ExampleRow:
    label: str
    source: str
    text: str


@dataclass(slots=True)
class ChunkCandidate:
    label: str
    source_base: str
    source_raw: str
    snippet: str
    score: float


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_args() -> argparse.Namespace:
    repo_root = _repo_root()
    kb_dir = repo_root / "knowledge-base"

    parser = argparse.ArgumentParser(
        description=(
            "Build a cleaned hybrid full-text KB draft from the short reasoning KB and the chunk KB, "
            "without modifying the existing source files."
        )
    )
    parser.add_argument(
        "--short-kb",
        type=Path,
        default=kb_dir / "full_text_pos-neg_examples.csv",
        help="Path to the short reasoning full-text KB CSV.",
    )
    parser.add_argument(
        "--chunk-kb",
        type=Path,
        default=kb_dir / "chunk_full_text_pos-neg_examples.csv",
        help="Path to the chunk full-text KB CSV.",
    )
    parser.add_argument(
        "--output-kb",
        type=Path,
        default=kb_dir / "full_text_pos-neg_examples_cleaned_hybrid_draft.csv",
        help="Path to write the cleaned hybrid draft KB CSV.",
    )
    parser.add_argument(
        "--output-report",
        type=Path,
        default=kb_dir / "full_text_pos-neg_examples_cleaned_hybrid_draft_report.json",
        help="Path to write generation report JSON.",
    )
    parser.add_argument(
        "--max-words",
        type=int,
        default=140,
        help="Maximum words kept in chunk snippet windows.",
    )
    parser.add_argument(
        "--min-words",
        type=int,
        default=45,
        help="Minimum words required for a cleaned chunk snippet.",
    )
    parser.add_argument(
        "--max-chunks-per-source",
        type=int,
        default=1,
        help="Maximum selected chunk snippets per source paper and label.",
    )
    parser.add_argument(
        "--max-neg-additions",
        type=int,
        default=4,
        help="Maximum number of cleaned NEG chunk additions to include in the hybrid draft.",
    )
    return parser.parse_args()


def _normalize_space(text: str) -> str:
    cleaned = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _read_kb_rows(path: Path) -> list[ExampleRow]:
    if not path.exists():
        raise FileNotFoundError(f"Missing KB file: {path}")

    rows: list[ExampleRow] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            label = str(row.get("label") or "").strip().upper()
            source = _normalize_space(str(row.get("source") or ""))
            text = _normalize_space(str(row.get("text") or ""))
            if label not in {"POS", "NEG"}:
                continue
            if not source or not text:
                continue
            rows.append(ExampleRow(label=label, source=source, text=text))
    return rows


def _base_source(source: str) -> str:
    if "|" in source:
        return _normalize_space(source.split("|", 1)[0])
    return _normalize_space(source)


def _word_tokens(text: str) -> list[str]:
    return WORD_PATTERN.findall(text)


def _quality_metrics(text: str) -> dict[str, float | int | bool]:
    words = _word_tokens(text)
    word_count = len(words)
    if word_count == 0:
        return {
            "word_count": 0,
            "short_token_ratio": 1.0,
            "digit_ratio": 0.0,
            "bad_char_ratio": 0.0,
            "signal_hits": 0,
            "reference_like": False,
        }

    short_tokens = sum(1 for token in words if len(token) <= 2)
    chars = text
    digit_count = sum(ch.isdigit() for ch in chars)
    bad_char_count = sum(
        1
        for ch in chars
        if (not ch.isalnum()) and ch not in " .,;:!?()[]{}-_/'\""
    )

    lower = text.lower()
    signal_hits = 0
    for keyword in DOMAIN_KEYWORDS:
        if keyword in lower:
            signal_hits += 1
    exclusion_hits = 0
    for keyword in NEGATIVE_SIGNAL_KEYWORDS:
        if keyword in lower:
            signal_hits += 1
            exclusion_hits += 1

    delivery_hits = sum(1 for keyword in POS_DELIVERY_KEYWORDS if keyword in lower)
    ai_hits = sum(1 for keyword in POS_AI_KEYWORDS if keyword in lower)
    pa_hits = sum(1 for keyword in POS_PA_KEYWORDS if keyword in lower)

    alpha_chars = sum(ch.isalpha() for ch in chars)
    avg_token_len = sum(len(token) for token in words) / max(word_count, 1)

    return {
        "word_count": word_count,
        "short_token_ratio": short_tokens / max(word_count, 1),
        "digit_ratio": digit_count / max(len(chars), 1),
        "bad_char_ratio": bad_char_count / max(len(chars), 1),
        "alpha_ratio": alpha_chars / max(len(chars), 1),
        "avg_token_len": avg_token_len,
        "signal_hits": signal_hits,
        "exclusion_hits": exclusion_hits,
        "delivery_hits": delivery_hits,
        "ai_hits": ai_hits,
        "pa_hits": pa_hits,
        "reference_like": bool(REFERENCE_PATTERN.search(lower)),
        "low_value_like": bool(LOW_VALUE_PATTERN.search(lower)),
    }


def _best_window(text: str, max_words: int) -> tuple[str, float, dict[str, float | int | bool]]:
    normalized = _normalize_space(text)
    matches = list(WORD_PATTERN.finditer(normalized))
    if not matches:
        metrics = _quality_metrics("")
        return "", -999.0, metrics

    if len(matches) <= max_words:
        snippet = normalized
        metrics = _quality_metrics(snippet)
        score = _window_score(metrics)
        return snippet, score, metrics

    step = max(12, max_words // 4)
    best_snippet = ""
    best_score = -999.0
    best_metrics: dict[str, float | int | bool] = _quality_metrics("")

    token_count = len(matches)
    for start in range(0, max(token_count - max_words + 1, 1), step):
        end_token_index = min(start + max_words - 1, token_count - 1)
        start_char = matches[start].start()
        end_char = matches[end_token_index].end()
        snippet = _normalize_space(normalized[start_char:end_char])
        metrics = _quality_metrics(snippet)
        score = _window_score(metrics)
        if score > best_score:
            best_snippet = snippet
            best_score = score
            best_metrics = metrics

    return best_snippet, best_score, best_metrics


def _window_score(metrics: dict[str, float | int | bool]) -> float:
    signal_hits = float(metrics.get("signal_hits", 0) or 0)
    short_token_ratio = float(metrics.get("short_token_ratio", 0.0) or 0.0)
    digit_ratio = float(metrics.get("digit_ratio", 0.0) or 0.0)
    bad_char_ratio = float(metrics.get("bad_char_ratio", 0.0) or 0.0)
    alpha_ratio = float(metrics.get("alpha_ratio", 0.0) or 0.0)
    avg_token_len = float(metrics.get("avg_token_len", 0.0) or 0.0)
    reference_like = bool(metrics.get("reference_like", False))
    low_value_like = bool(metrics.get("low_value_like", False))
    word_count = float(metrics.get("word_count", 0) or 0)

    score = 0.0
    score += signal_hits * 2.0
    score += min(word_count, 160.0) / 80.0
    score -= short_token_ratio * 5.0
    score -= digit_ratio * 2.0
    score -= bad_char_ratio * 30.0
    score += alpha_ratio * 1.5
    score += min(avg_token_len, 7.0) / 4.0
    if reference_like:
        score -= 6.0
    if low_value_like:
        score -= 5.0
    return score


def _build_chunk_candidates(
    rows: list[ExampleRow],
    *,
    max_words: int,
    min_words: int,
) -> tuple[list[ChunkCandidate], dict[str, int]]:
    candidates: list[ChunkCandidate] = []
    skipped = Counter()
    seen: set[tuple[str, str, str]] = set()

    for row in rows:
        source_base = _base_source(row.source)
        snippet, score, metrics = _best_window(row.text, max_words=max_words)

        word_count = int(metrics.get("word_count", 0) or 0)
        short_token_ratio = float(metrics.get("short_token_ratio", 0.0) or 0.0)
        digit_ratio = float(metrics.get("digit_ratio", 0.0) or 0.0)
        bad_char_ratio = float(metrics.get("bad_char_ratio", 0.0) or 0.0)
        alpha_ratio = float(metrics.get("alpha_ratio", 0.0) or 0.0)
        avg_token_len = float(metrics.get("avg_token_len", 0.0) or 0.0)
        signal_hits = int(metrics.get("signal_hits", 0) or 0)
        exclusion_hits = int(metrics.get("exclusion_hits", 0) or 0)
        delivery_hits = int(metrics.get("delivery_hits", 0) or 0)
        ai_hits = int(metrics.get("ai_hits", 0) or 0)
        pa_hits = int(metrics.get("pa_hits", 0) or 0)
        reference_like = bool(metrics.get("reference_like", False))
        low_value_like = bool(metrics.get("low_value_like", False))

        if word_count < min_words:
            skipped["too_short"] += 1
            continue
        if reference_like:
            skipped["reference_like"] += 1
            continue
        if short_token_ratio > 0.22:
            skipped["too_many_short_tokens"] += 1
            continue
        if digit_ratio > 0.08:
            skipped["too_many_digits"] += 1
            continue
        if bad_char_ratio > 0.012:
            skipped["too_many_bad_chars"] += 1
            continue
        if alpha_ratio < 0.68:
            skipped["too_low_alpha_ratio"] += 1
            continue
        if avg_token_len < 3.9:
            skipped["avg_token_len_too_short"] += 1
            continue
        if signal_hits <= 0:
            skipped["no_domain_signals"] += 1
            continue
        if low_value_like:
            skipped["low_value_content"] += 1
            continue

        if row.label == "POS":
            positive_families = int(delivery_hits > 0) + int(ai_hits > 0) + int(pa_hits > 0)
            if positive_families < 2:
                skipped["pos_missing_core_families"] += 1
                continue
        else:
            if exclusion_hits <= 0:
                skipped["neg_missing_exclusion_signals"] += 1
                continue

        key = (row.label, source_base.lower(), snippet.lower())
        if key in seen:
            skipped["duplicate_snippet"] += 1
            continue
        seen.add(key)

        candidates.append(
            ChunkCandidate(
                label=row.label,
                source_base=source_base,
                source_raw=row.source,
                snippet=snippet,
                score=score,
            )
        )

    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates, dict(skipped)


def _select_candidates(
    candidates: list[ChunkCandidate],
    needed: int,
    max_chunks_per_source: int,
) -> list[ChunkCandidate]:
    if needed <= 0:
        return []

    selected: list[ChunkCandidate] = []
    per_source = Counter()
    seen_snippets: set[str] = set()

    for candidate in candidates:
        if len(selected) >= needed:
            break
        signature = candidate.snippet.lower()
        if signature in seen_snippets:
            continue
        if per_source[candidate.source_base] >= max_chunks_per_source:
            continue

        selected.append(candidate)
        per_source[candidate.source_base] += 1
        seen_snippets.add(signature)

    if len(selected) < needed:
        for candidate in candidates:
            if len(selected) >= needed:
                break
            signature = candidate.snippet.lower()
            if signature in seen_snippets:
                continue
            selected.append(candidate)
            seen_snippets.add(signature)

    return selected


def _to_hybrid_rows(candidates: list[ChunkCandidate]) -> list[ExampleRow]:
    rows: list[ExampleRow] = []
    for candidate in candidates:
        if candidate.label == "POS":
            reasoning = (
                "Positive hybrid example from cleaned full-text chunk with explicit smartphone, AI, and physical activity evidence."
            )
        else:
            reasoning = (
                "Negative hybrid example from cleaned full-text chunk preserving exclusion-relevant evidence "
                "(for example review, simulation, non-adult target, or no participant-facing AI intervention)."
            )

        source = f"{candidate.source_base} | hybrid_cleaned_chunk_draft"
        text = f"REASONING: {reasoning} CONTENT: {candidate.snippet}"
        rows.append(ExampleRow(label=candidate.label, source=source, text=text))
    return rows


def _write_csv(path: Path, rows: list[ExampleRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["label", "source", "text"])
        writer.writeheader()
        for row in rows:
            writer.writerow({"label": row.label, "source": row.source, "text": row.text})


def _write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main() -> None:
    args = _parse_args()

    short_rows = _read_kb_rows(args.short_kb)
    chunk_rows = _read_kb_rows(args.chunk_kb)

    short_pos = [row for row in short_rows if row.label == "POS"]
    short_neg = [row for row in short_rows if row.label == "NEG"]

    chunk_candidates_all, skipped = _build_chunk_candidates(
        chunk_rows,
        max_words=max(40, int(args.max_words)),
        min_words=max(20, int(args.min_words)),
    )

    chunk_pos = [item for item in chunk_candidates_all if item.label == "POS"]
    chunk_neg = [item for item in chunk_candidates_all if item.label == "NEG"]

    # Add only a conservative amount of chunk evidence to keep the draft readable and robust.
    base_gap = max(len(short_neg) - len(short_pos), 0)
    need_neg = min(len(chunk_neg), max(0, int(args.max_neg_additions)))
    need_pos = min(len(chunk_pos), need_neg + base_gap)

    selected_pos = _select_candidates(
        chunk_pos,
        needed=need_pos,
        max_chunks_per_source=max(1, int(args.max_chunks_per_source)),
    )
    selected_neg = _select_candidates(
        chunk_neg,
        needed=need_neg,
        max_chunks_per_source=max(1, int(args.max_chunks_per_source)),
    )

    hybrid_rows = short_rows + _to_hybrid_rows(selected_pos) + _to_hybrid_rows(selected_neg)
    _write_csv(args.output_kb, hybrid_rows)

    final_pos = sum(1 for row in hybrid_rows if row.label == "POS")
    final_neg = sum(1 for row in hybrid_rows if row.label == "NEG")

    by_source_pos = Counter(item.source_base for item in selected_pos)
    by_source_neg = Counter(item.source_base for item in selected_neg)

    report = {
        "inputs": {
            "short_kb": str(Path(args.short_kb).resolve()),
            "chunk_kb": str(Path(args.chunk_kb).resolve()),
            "short_rows_total": len(short_rows),
            "short_rows_pos": len(short_pos),
            "short_rows_neg": len(short_neg),
            "chunk_rows_total": len(chunk_rows),
        },
        "cleaning": {
            "chunk_candidates_total": len(chunk_candidates_all),
            "chunk_candidates_pos": len(chunk_pos),
            "chunk_candidates_neg": len(chunk_neg),
            "skipped_reasons": skipped,
            "max_words": int(args.max_words),
            "min_words": int(args.min_words),
            "max_chunks_per_source": int(args.max_chunks_per_source),
            "max_neg_additions": int(args.max_neg_additions),
        },
        "selected_chunk_additions": {
            "pos": len(selected_pos),
            "neg": len(selected_neg),
            "pos_by_source": dict(by_source_pos),
            "neg_by_source": dict(by_source_neg),
        },
        "output": {
            "output_kb": str(Path(args.output_kb).resolve()),
            "output_rows_total": len(hybrid_rows),
            "output_rows_pos": final_pos,
            "output_rows_neg": final_neg,
            "balanced": final_pos == final_neg,
            "existing_input_kb_files_modified": False,
        },
    }
    _write_report(args.output_report, report)

    print(f"[done] Wrote cleaned hybrid KB draft: {args.output_kb}")
    print(f"[done] Wrote draft report: {args.output_report}")
    print(f"[stats] Final rows: total={len(hybrid_rows)} pos={final_pos} neg={final_neg}")


if __name__ == "__main__":
    main()
