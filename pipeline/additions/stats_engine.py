"""Direct run: python -m pipeline.additions.stats_engine

Validation utilities for screening and data extraction outputs.

This module compares AI outputs to human labels (screening) and to the
adjudicated consensus table (data extraction). Outputs include a
readable report, confusion matrix plot, and discrepancy logs.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import unicodedata
from numbers import Real
from pathlib import Path
from typing import Any, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from statsmodels.stats.proportion import proportion_confint

# human readable hint: proportion_confint supplies exact (Clopper-Pearson) binomial CIs (Seabold, S., & Perktold, J., 2010).

from config.user_orchestrator import (
    CURRENT_STAGE,
    DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS,
    DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES,
    DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS,
    DATA_EXTRACTION_VALIDATION_VALUE_ALIASES,
    STUDY_TAGS_IGNORE,
    STUDY_TAGS_INCLUDE,
)
from pipeline.core.extraction_schema import (
    DynamicExtractionSchema,
    ExtractionVariable,
    MISSING_TEXT_VALUE,
)
from pipeline.core.metadata_aliases import metadata_aliases

ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = ROOT / "output" / CURRENT_STAGE
STAGE_PREFIX = f"{CURRENT_STAGE}_"
INPUT_DIR = ROOT / "input"
STAGE_OUTPUT_DIR = OUTPUT_DIR
EXTRACTION_AI_PATH = OUTPUT_DIR / f"{CURRENT_STAGE}_results.jsonl"
EXTRACTION_HUMAN_PATH = ROOT / "input" / "data_extraction_schema.csv"
EXTRACTION_HUMAN_BINARY_SOURCE_NAME = "data_extraction_human_review_qc_sample_binary_scoring.csv"
LOGGER = logging.getLogger(__name__)


def _admin_setting(key: str, default: Any) -> Any:
    """human readable hint: administrative validation labels live in user_orchestrator.py, not pipeline code."""

    if isinstance(DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS, dict):
        return DATA_EXTRACTION_ADMIN_OUTPUT_COLUMNS.get(key, default)
    return default


VALIDATION_PAPER_ID_COLUMN = str(_admin_setting("paper_id_column", "paper_id"))
VALIDATION_VARIABLE_COLUMN = str(_admin_setting("quote_audit_variable_column", "variable"))
VALIDATION_CONSENSUS_COLUMN = str(_admin_setting("quote_audit_consensus_column", "consensus_column"))
VALIDATION_AI_VALUE_COLUMN = str(_admin_setting("quote_audit_value_column", "ai_value"))
VALIDATION_AI_QUOTE_COLUMN = str(_admin_setting("quote_audit_quote_column", "ai_quote"))
VALIDATION_HUMAN_VALUE_COLUMN = "human_value"
VALIDATION_ERROR_TYPE_COLUMN = "error_type"
VALIDATION_ERROR_EFFECT_COLUMN = "error_effect"
EXTRACTION_HUMAN_SCORE_SUFFIX = "__human_score"
EXTRACTION_HUMAN_EVALUABLE_SUFFIX = "__human_evaluable"
VALIDATION_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "of",
    "or",
    "the",
    "to",
    "was",
    "were",
    "with",
}

EXCLUSION_TAGS = [t.lower() for t in STUDY_TAGS_INCLUDE]
IGNORE_TAGS = {t.lower() for t in STUDY_TAGS_IGNORE}


def _stage_file(name: str, suffix: str | None = None) -> Path:
    """Build a stage-prefixed output path under output/<stage>/.

    Validation outputs must include `qc_sample` in the filename to match the QC-only comparison scope.
    """

    base = Path(name)
    needs_qc_token = base.stem.startswith("validation_")
    qc_token = "qc_sample_" if needs_qc_token else ""
    stem_with_suffix = f"{base.stem}_{suffix}" if suffix else base.stem
    filename = f"{STAGE_PREFIX}{qc_token}{stem_with_suffix}{base.suffix}"
    return OUTPUT_DIR / filename


def _find_latest_match(patterns: list[str], search_dirs: list[Path]) -> Optional[Path]:
    """Return the most recently modified file matching any pattern."""

    candidates: list[Path] = []
    for directory in search_dirs:
        if not directory.exists():
            continue
        for pattern in patterns:
            candidates.extend(directory.glob(pattern))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _require_path(value: Optional[str], label: str) -> Path:
    """Require an explicit CSV path to avoid ambiguous auto-search."""

    if not value:
        raise FileNotFoundError(f"Missing required file path for {label}.")
    path = Path(value)
    if not path.exists():
        raise FileNotFoundError(f"File not found for {label}: {path}")
    return path


def _auto_or_require(value: Optional[str], label: str, patterns: list[str]) -> Path:
    """Use explicit path if provided, otherwise auto-detect from input/."""

    if value:
        return _require_path(value, label)
    found = _find_latest_match(patterns, [INPUT_DIR])
    if not found:
        raise FileNotFoundError(
            f"Missing required file for {label}. Looked for {patterns} in {INPUT_DIR}."
        )
    return found


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace from CSV column names."""

    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df


def _normalize_paper_id_value(value: object) -> str:
    """human readable hint: normalize ID formatting so '#250', '250', and '250.0' map to the same key."""

    text = "" if value is None else str(value).strip()
    if not text:
        return ""

    text = text.lstrip("#")
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    return text


def _normalize_id_column(df: pd.DataFrame) -> pd.Series:
    """Find the best ID column and return it as strings."""

    candidates = metadata_aliases("paper_id")
    for col in candidates:
        if col in df.columns:
            return df[col].apply(_normalize_paper_id_value)
    raise KeyError("Could not find an ID column in human export")


def _normalize_tag_text(value: str) -> str:
    text = value.strip().lower().replace("_", " ").replace("-", " ")
    text = " ".join(text.split())
    return text


def _extract_tags(value: Optional[object]) -> list[str]:
    """human readable hint: map explicit human-export tags to the curated include list; ignores notes."""

    if not isinstance(value, str):
        return []

    found: list[str] = []
    seen: set[str] = set()
    parts = re.split(r"[;,|\n]", value)
    for raw in parts:
        norm = _normalize_tag_text(raw)
        if not norm or norm in IGNORE_TAGS:
            continue
        for tag in EXCLUSION_TAGS:
            if _normalize_tag_text(tag) in norm:
                canon = _normalize_tag_text(tag)
                if canon not in seen:
                    seen.add(canon)
                    found.append(tag)
                break
    return found


def _extract_ft_reason(notes_val: Optional[str]) -> str:
    """Extract the full-text exclusion reason from Notes/Tags."""

    if not isinstance(notes_val, str):
        return "Unspecified Reason"
    text = notes_val.strip()
    if text.lower().startswith("exclusion reason:"):
        return text.split(":", 1)[-1].strip() or "Unspecified Reason"
    return text or "Unspecified Reason"


def _parse_human_decision(val: Optional[object]) -> Optional[int]:
    """Normalize human include/exclude values into 1 (include) or 0 (exclude)."""

    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        if int(val) == 1:
            return 1
        if int(val) == 0:
            return 0
        return None
    text = str(val).strip().lower()
    if text in {"1", "yes", "y", "true", "include", "included", "eligible"}:
        return 1
    if text in {"0", "no", "n", "false", "exclude", "excluded", "ineligible"}:
        return 0
    return None


def _load_qc_human_file(path: Path) -> pd.DataFrame:
    """Load a human QC-only file with decisions for the QC sample."""

    df = _clean_cols(pd.read_csv(path))
    df["paper_id"] = _normalize_id_column(df)

    decision_col = None
    for cand in [
        "human_decision",
        "decision",
        "include",
        "included",
        "eligible",
        "is_eligible",
    ]:
        if cand in df.columns:
            decision_col = cand
            break
    if decision_col is None:
        raise KeyError("QC human file must include a decision column (e.g., human_decision).")
    df["human_decision"] = df[decision_col].apply(_parse_human_decision)

    note_col = None
    for cand in ["human_reason", "reason", "notes", "Notes"]:
        if cand in df.columns:
            note_col = cand
            break
    df["human_note"] = df[note_col] if note_col else "QC human review"

    tag_col = None
    for cand in ["human_tag", "Tags", "tags"]:
        if cand in df.columns:
            tag_col = cand
            break
    df["human_tag"] = df[tag_col].apply(lambda v: " | ".join(_extract_tags(v)) if isinstance(v, str) else "") if tag_col else ""

    df = df.dropna(subset=["human_decision"]).copy()
    return df


def _load_human(stage: str, args) -> pd.DataFrame:
    """Load human labels depending on stage."""

    qc_human = _find_latest_match(
        [f"{CURRENT_STAGE}_human_validation_qc_sample_batch_*.csv"],
        [STAGE_OUTPUT_DIR, OUTPUT_DIR, INPUT_DIR],
    )
    if qc_human is not None:
        return _load_qc_human_file(qc_human)

    if stage == "full_text":
        included_path = _auto_or_require(
            args.included, "full_text included CSV", ["*_included_csv_*.csv"]
        )
        excluded_path = _auto_or_require(
            args.excluded, "full_text excluded CSV", ["*_excluded_csv_*.csv"]
        )

        df_inc = _clean_cols(pd.read_csv(included_path))
        df_exc = _clean_cols(pd.read_csv(excluded_path))

        df_inc["human_decision"] = 1
        df_inc["human_note"] = "Included at full text"
        df_inc["human_tag"] = ""

        df_exc["human_decision"] = 0
        note_col = None
        for cand in ["Notes", "notes", "Note", "note"]:
            if cand in df_exc.columns:
                note_col = cand
                break
        tag_col = None
        for cand in ["Tags", "tags", "Tag", "tag"]:
            if cand in df_exc.columns:
                tag_col = cand
                break
        df_exc["human_note"] = df_exc[note_col].apply(_extract_ft_reason) if note_col else "Unspecified Reason"
        df_exc["human_tag"] = df_exc[tag_col].apply(lambda v: " | ".join(_extract_tags(v)) if isinstance(v, str) else "") if tag_col else ""

        df_inc["paper_id"] = _normalize_id_column(df_inc)
        df_exc["paper_id"] = _normalize_id_column(df_exc)

        common_cols = list(set(df_inc.columns) & set(df_exc.columns))
        human = pd.concat([df_inc[common_cols], df_exc[common_cols]], ignore_index=True)
        human = human.drop_duplicates(subset=["paper_id"], keep="first")
        return human

    select_path = _auto_or_require(
        args.select, "title_abstract select CSV", ["*_select_csv_*.csv"]
    )
    irrelevant_path = _auto_or_require(
        args.irrelevant, "title_abstract irrelevant CSV", ["*_irrelevant_csv_*.csv"]
    )

    df_yes = _clean_cols(pd.read_csv(select_path))
    df_no = _clean_cols(pd.read_csv(irrelevant_path))

    df_yes["human_decision"] = 1
    note_col_yes = None
    for cand in ["Notes", "notes", "Note", "note"]:
        if cand in df_yes.columns:
            note_col_yes = cand
            break
    tag_col_yes = None
    for cand in ["Tags", "tags", "Tag", "tag"]:
        if cand in df_yes.columns:
            tag_col_yes = cand
            break
    df_yes["human_note"] = (
        df_yes[note_col_yes].fillna("Included (Yes/Maybe)").astype(str).str.strip().replace("", "Included (Yes/Maybe)")
        if note_col_yes
        else "Included (Yes/Maybe)"
    )
    df_yes["human_tag"] = df_yes[tag_col_yes].apply(lambda v: " | ".join(_extract_tags(v)) if isinstance(v, str) else "") if tag_col_yes else ""

    df_no["human_decision"] = 0
    note_col_no = None
    for cand in ["Notes", "notes", "Note", "note"]:
        if cand in df_no.columns:
            note_col_no = cand
            break
    tag_col_no = None
    for cand in ["Tags", "tags", "Tag", "tag"]:
        if cand in df_no.columns:
            tag_col_no = cand
            break
    df_no["human_note"] = (
        df_no[note_col_no].fillna("Unspecified Reason").astype(str).str.strip().replace("", "Unspecified Reason")
        if note_col_no
        else "Unspecified Reason"
    )
    df_no["human_tag"] = df_no[tag_col_no].apply(lambda v: " | ".join(_extract_tags(v)) if isinstance(v, str) else "") if tag_col_no else ""

    df_yes["paper_id"] = _normalize_id_column(df_yes)
    df_no["paper_id"] = _normalize_id_column(df_no)

    common_cols = list(set(df_yes.columns) & set(df_no.columns))
    human = pd.concat([df_yes[common_cols], df_no[common_cols]], ignore_index=True)
    human = human.drop_duplicates(subset=["paper_id"], keep="first")
    return human


def _parse_ai_decision(val) -> Tuple[Optional[int], str]:
    """Parse the AI decision JSON into a binary include/exclude label."""

    reason = ""
    payload = val

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            lowered = payload.lower()
            if "true" in lowered or "eligible" in lowered:
                return 1, reason
            if "false" in lowered or "ineligible" in lowered:
                return 0, reason
            return None, reason

    if isinstance(payload, dict):
        if "exclusion_reason_category" in payload:
            reason = str(payload.get("exclusion_reason_category", ""))
        elig = payload.get("is_eligible")
        if isinstance(elig, bool):
            return 1 if elig else 0, reason
        if isinstance(elig, str):
            elig_low = elig.lower()
            if elig_low in {"true", "yes", "eligible", "neutral", "maybe"}:
                return 1, reason
            if elig_low in {"false", "no", "ineligible"}:
                return 0, reason

    return None, reason


def _load_ai() -> tuple[pd.DataFrame, Path]:
    """Aggregate AI QC decisions across main and retry runs, keeping latest per paper."""

    empty_columns = ["paper_id", "ai_decision", "ai_reason", "source_path"]

    # human readable hint: gather all QC eligibility JSONL files (main + retries) and prefer newest per paper_id.
    stage_files: list[Path] = []
    patterns = [
        f"{CURRENT_STAGE}_qc_sample_*_eligibility_*.jsonl",  # catches main and retry naming
        f"{CURRENT_STAGE}_eligibility_qc_sample_*.jsonl",    # legacy naming
    ]
    for pat in patterns:
        stage_files.extend(STAGE_OUTPUT_DIR.glob(pat))

    # Exclude split outputs to avoid double-counting per decision type.
    stage_files = [
        p
        for p in stage_files
        if "eligibility_select" not in p.name
        and "eligibility_irrelevant" not in p.name
        and "eligibility_included" not in p.name
        and "eligibility_excluded" not in p.name
    ]

    if not stage_files:
        raise FileNotFoundError(
            "Missing QC AI eligibility file(s). Expected qc_sample eligibility JSONL in "
            f"{STAGE_OUTPUT_DIR}."
        )

    stage_files = sorted(stage_files, key=lambda p: p.stat().st_mtime)

    latest_by_paper: dict[str, dict] = {}
    for path in stage_files:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip() or '"meta":' in line:
                        continue
                    payload = json.loads(line)
                    paper_id = _normalize_paper_id_value(payload.get("paper_id", ""))
                    if not paper_id:
                        continue
                    decision_raw = payload.get("llm_decision")
                    ai_decision, ai_reason = _parse_ai_decision(decision_raw)
                    # human readable hint: ai_reason is always empty string for included, never None or '{}'
                    if ai_decision == 1:
                        ai_reason = ""
                    elif ai_reason is None or ai_reason == "None" or ai_reason == "{}":
                        ai_reason = ""
                    latest_by_paper[paper_id] = {
                        "paper_id": paper_id,
                        "ai_decision": ai_decision,
                        "ai_reason": ai_reason,
                        "source_path": path,
                    }
        except Exception:
            continue

    records = list(latest_by_paper.values())
    if not records:
        # human readable hint: preserve expected columns so downstream merge logic remains stable on empty AI sets.
        return pd.DataFrame(columns=empty_columns), stage_files[-1]
    return pd.DataFrame(records), stage_files[-1]


def _merge(ai: pd.DataFrame, human: pd.DataFrame) -> pd.DataFrame:
    """Merge AI and human labels on the paper ID."""

    human = human.copy()
    ai = ai.copy()
    if "paper_id" not in ai.columns:
        ai["paper_id"] = pd.Series(dtype="string")
    if "ai_decision" not in ai.columns:
        ai["ai_decision"] = pd.Series(dtype="float64")
    if "ai_reason" not in ai.columns:
        ai["ai_reason"] = pd.Series(dtype="string")

    merged = human.merge(ai, on="paper_id", how="inner", suffixes=("_human", "_ai"))
    if merged["paper_id"].duplicated().any():
        dupes = merged[merged["paper_id"].duplicated()]["paper_id"].unique().tolist()
        raise ValueError(f"Duplicate IDs after merge: {dupes}")

    merged["ai_decision"] = merged["ai_decision"].fillna(-1).astype(int)
    merged = merged.sort_values("paper_id").reset_index(drop=True)
    return merged


def _confusion(df: pd.DataFrame) -> tuple[int, int, int, int]:
    """Compute confusion-matrix counts."""

    tp = int(((df["ai_decision"] == 1) & (df["human_decision"] == 1)).sum())
    tn = int(((df["ai_decision"] == 0) & (df["human_decision"] == 0)).sum())
    fp = int(((df["ai_decision"] == 1) & (df["human_decision"] == 0)).sum())
    fn = int(((df["ai_decision"] == 0) & (df["human_decision"] == 1)).sum())
    return tp, tn, fp, fn


def _prop_ci(k: float, n: float, alpha: float = 0.05) -> Tuple[float, float]:
    """human readable hint: exact (Clopper-Pearson) CI via statsmodels (Seabold & Perktold, 2010)."""

    # Guard against array/Series/DataFrame inputs so type checkers see scalars only.
    if (
        not isinstance(k, Real)
        or isinstance(k, bool)
        or not isinstance(n, Real)
        or isinstance(n, bool)
    ):
        raise TypeError("k and n must be real scalar numbers")

    k_val = float(k)
    n_val = float(n)
    if n_val == 0:
        return (math.nan, math.nan)
    lower, upper = proportion_confint(count=k_val, nobs=n_val, alpha=alpha, method="beta")
    return float(lower), float(upper) # type: ignore


def _metrics(tp: int, tn: int, fp: int, fn: int) -> dict:
    """Compute agreement metrics for screening."""

    # human readable hint: PABAK follows Byrt, T., Bishop, J., & Carlin, J. B. (1993). Bias, prevalence and kappa. Journal of Clinical Epidemiology, 46(5), 423–429.

    total = tp + tn + fp + fn
    po = (tp + tn) / total if total else math.nan
    pabak = 2 * po - 1 if total else math.nan
    accuracy = (tp + tn) / total if total else math.nan

    sens_n = tp + fn
    spec_n = tn + fp
    ppv_n = tp + fp
    npv_n = tn + fn
    acc_n = total

    sens = tp / sens_n if sens_n else math.nan
    spec = tn / spec_n if spec_n else math.nan
    ppv = tp / ppv_n if ppv_n else math.nan
    npv = tn / npv_n if npv_n else math.nan

    return {
        "total": total,
        "po": po,
        "pabak": pabak,
        "accuracy": accuracy,
        "sensitivity": sens,
        "specificity": spec,
        "ppv": ppv,
        "npv": npv,
        "sens_ci": _prop_ci(tp, sens_n),
        "spec_ci": _prop_ci(tn, spec_n),
        "ppv_ci": _prop_ci(tp, ppv_n),
        "npv_ci": _prop_ci(tn, npv_n),
        "acc_ci": _prop_ci(tp + tn, acc_n),
    }


def _write_alignment(df: pd.DataFrame, suffix: str | None = None) -> None:
    """human readable hint: single QC alignment file with decisions and reasons."""

    if df.empty:
        path = _stage_file("validation_alignment.csv", suffix)
        if path.exists():
            path.unlink()
        return

    def _norm(val: object) -> str:
        text = "" if val is None else str(val)
        return _normalize_tag_text(text)

    def _tag_list(val: object) -> list[str]:
        return _extract_tags(val)

    out = df.copy()
    out.rename(columns={"paper_id": "ID"}, inplace=True)
    out["decision_match"] = out["ai_decision"] == out["human_decision"]
    # human readable hint: normalize ai_reason to empty string for included and for any None/"None"/"{}"
    out["ai_reason"] = out["ai_reason"].replace(["None", "{}"], "")
    out.loc[out["ai_decision"] == 1, "ai_reason"] = ""
    ai_tags = out["ai_reason"].apply(_tag_list)
    human_tags_series = out["human_tag"].apply(_tag_list) if "human_tag" in out.columns else pd.Series([[]] * len(out))
    out["human_tag"] = human_tags_series.apply(lambda tags: " | ".join(tags))
    # reason_match is True if both tag lists are empty (i.e., both ai_reason and human_tag are empty strings)
    out["reason_match"] = [True if not a and not h else bool(a and h and (a[0] in h)) for a, h in zip(ai_tags, human_tags_series)]

    metadata_cols: list[str] = []
    for alias_key in ("title", "abstract", "authors", "publication_year"):
        for candidate in metadata_aliases(alias_key):
            if candidate in out.columns:
                metadata_cols.append(candidate)
                break

    cols = [
        "ID",
        "human_decision",
        "ai_decision",
        "decision_match",
        "human_note",
        "human_tag",
        "ai_reason",
        "reason_match",
    ] + metadata_cols

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out[[c for c in cols if c in out.columns]].to_csv(
        _stage_file("validation_alignment.csv", suffix), index=False, encoding="utf-8"
    )


def _write_report(stats: dict, tp: int, tn: int, fp: int, fn: int, stage: str, suffix: str | None = None) -> None:
    """Write a readable validation summary report."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append(f"Validation report (AI vs Human) - stage: {stage}")
    lines.append(
        "This study compares the AI screening decisions against human ground truth from configured CSV exports. "
        "Metrics emphasize agreement and error balance, with prevalence-adjusted bias-adjusted kappa (PABAK) "
        "to reduce skew from class imbalance (Byrt et al., 1993)."
    )
    lines.append("")
    lines.append(f"Total papers analyzed (QC sample only): {stats['total']}")
    lines.append(f"Confusion matrix (Human rows, AI columns): TP={tp}, TN={tn}, FP={fp}, FN={fn}")
    lines.append(
        f"Accuracy (overall correct decisions): {stats['accuracy']*100:.1f}% "
        f"(95% CI {stats['acc_ci'][0]*100:.1f}-{stats['acc_ci'][1]*100:.1f}%)"
    )
    lines.append(f"Observed agreement (Po): {stats['po']:.3f}")
    lines.append(f"PABAK: {stats['pabak']:.3f} (PABAK = 2*Po - 1)")
    lines.append(
        f"Sensitivity (recall of human-included): {stats['sensitivity']*100:.1f}% "
        f"(95% CI {stats['sens_ci'][0]*100:.1f}-{stats['sens_ci'][1]*100:.1f}%)"
    )
    lines.append(
        f"Specificity (correctly excluding human-negatives): {stats['specificity']*100:.1f}% "
        f"(95% CI {stats['spec_ci'][0]*100:.1f}-{stats['spec_ci'][1]*100:.1f}%)"
    )
    lines.append(
        f"PPV (precision of AI inclusions): {stats['ppv']*100:.1f}% "
        f"(95% CI {stats['ppv_ci'][0]*100:.1f}-{stats['ppv_ci'][1]*100:.1f}%)"
    )
    lines.append(
        f"NPV (precision of AI exclusions): {stats['npv']*100:.1f}% "
        f"(95% CI {stats['npv_ci'][0]*100:.1f}-{stats['npv_ci'][1]*100:.1f}%)"
    )
    lines.append("")
    lines.append("Interpretation (plain language):")
    lines.append("- Sensitivity: fraction of human-relevant papers the AI kept.")
    lines.append("- Specificity: fraction of human-irrelevant papers the AI excluded.")
    lines.append("- PPV/NPV: how often AI include/exclude calls match human labels.")
    lines.append("- PABAK adjusts kappa for class imbalance, stabilizing agreement estimates.")

    _stage_file("validation_stats_report.txt", suffix).write_text("\n".join(lines), encoding="utf-8")


def _plot_confusion(tp: int, tn: int, fp: int, fn: int, suffix: str | None = None) -> None:
    """Draw and save a confusion-matrix plot."""

    total = tp + tn + fp + fn
    if total == 0:
        return

    data = [[fn, tp], [tn, fp]]  # rows: human 1/0, cols: AI 0/1
    perc = [[v / total for v in row] for row in data]

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(
        data,
        annot=[[f"{data[r][c]}\n({perc[r][c]*100:.1f}%)" for c in range(2)] for r in range(2)],
        fmt="",
        cmap="Blues",
        cbar=False,
        xticklabels=["AI = 0 (exclude)", "AI = 1 (include)"],
        yticklabels=["Human = 1 (include)", "Human = 0 (exclude)"],
        ax=ax,
        annot_kws={"fontsize": 11},
    )
    ax.set_xlabel("AI Decision")
    ax.set_ylabel("Human Decision")
    fig.tight_layout()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(_stage_file("validation_matrix.png", suffix), dpi=300)
    plt.close(fig)


def _extract_timestamp_suffix(path: Path) -> str | None:
    """Extract QC timestamp suffix from legacy and current eligibility filenames.

    Accepted patterns include both:
    - <stage>_eligibility_qc_sample_<YYYYMMDD_HH-MM>[...]
    - <stage>_qc_sample_*_eligibility_<YYYYMMDD_HH-MM>[_<campaign>]
    """

    name = path.stem
    # 1) New naming in run_screening: <stage>_qc_sample_*_eligibility_<timestamp>[_campaign]
    new_match = re.search(r"eligibility_(\d{8}_\d{2}-\d{2})(?:_[A-Fa-f0-9]{8,16})?$", name)
    if new_match:
        return new_match.group(1)

    # 2) Legacy naming: <stage>_eligibility_qc_sample_<timestamp>
    legacy_match = re.search(r"eligibility_(?:select_|irrelevant_|included_|excluded_)?qc_sample_(\d{8}_\d{2}-\d{2})", name)
    if legacy_match:
        return legacy_match.group(1)

    return None


def _load_qc_sample_ids(suffix: str | None) -> set[str] | None:
    """Load QC sample IDs for the matching timestamp suffix."""

    if not suffix:
        return None

    qc_path = STAGE_OUTPUT_DIR / f"{CURRENT_STAGE}_qc_sample_batch_{suffix}.csv"
    if not qc_path.exists():
        fallback = _find_latest_match([f"{CURRENT_STAGE}_qc_sample_batch_*.csv"], [STAGE_OUTPUT_DIR])
        if fallback and fallback.exists():
            print(f"[qc] QC sample file not found at {qc_path}. Using latest QC sample: {fallback}.")
            qc_path = fallback
        else:
            print(f"[qc] QC sample file not found at {qc_path}. Validation will use overlap IDs instead.")
            return None

    try:
        df = pd.read_csv(qc_path)
    except Exception:
        print(f"[qc] QC sample file unreadable at {qc_path}. Validation will use overlap IDs instead.")
        return None

    if "paper_id" not in df.columns:
        print(f"[qc] QC sample missing 'paper_id' column at {qc_path}. Validation will use overlap IDs instead.")
        return None

    normalized = {
        _normalize_paper_id_value(val)
        for val in df["paper_id"].dropna().tolist()
    }
    normalized.discard("")
    return normalized


def validate_screening(stage: str, args) -> None:
    """Validate screening decisions against human labels."""

    ai, ai_path = _load_ai()
    human = _load_human(stage, args)
    merged = _merge(ai, human)

    suffix = _extract_timestamp_suffix(ai_path)
    qc_ids = _load_qc_sample_ids(suffix)
    if qc_ids:
        filtered = merged[merged["paper_id"].isin(qc_ids)].copy()
        if filtered.empty:
            print(
                "[qc] QC-ID filtering produced zero overlap (likely ID-format mismatch across files). "
                "Proceeding with unfiltered AI-human overlap."
            )
        else:
            merged = filtered
    if merged.empty:
        print("[qc] No overlapping IDs after filtering; validation outputs will report zero totals.")

    tp, tn, fp, fn = _confusion(merged)
    stats = _metrics(tp, tn, fp, fn)

    _write_alignment(merged, suffix)
    _write_report(stats, tp, tn, fp, fn, stage, suffix)
    _plot_confusion(tp, tn, fp, fn, suffix)

    print(f"[validation] stage={stage} status=completed")
    print(f"[output] validation_report path={_stage_file('validation_stats_report.txt', suffix).relative_to(ROOT)}")
    print(f"[output] validation_alignment path={_stage_file('validation_alignment.csv', suffix).relative_to(ROOT)}")
    print(f"[output] validation_matrix path={_stage_file('validation_matrix.png', suffix).relative_to(ROOT)}")


def _load_ai_extraction_records(ai_output_dir: Optional[Path] = None) -> list[dict]:
    """Load extraction outputs from run-level or per-paper JSONL files."""

    records: list[dict] = []
    root = Path(ai_output_dir) if ai_output_dir else OUTPUT_DIR
    run_level_path = root / f"{CURRENT_STAGE}_results.jsonl"
    if run_level_path.exists():
        ai_files = [run_level_path]
    elif ai_output_dir is None and EXTRACTION_AI_PATH.exists():
        ai_files = [EXTRACTION_AI_PATH]
    else:
        ai_files = sorted(root.glob("*/data_extraction_results.jsonl"))

    if not ai_files:
        raise FileNotFoundError(
            "Missing AI extraction files. Expected per-paper files at "
            f"{root}/<paper>/data_extraction_results.jsonl"
        )

    for path in ai_files:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                if payload.get("meta") == "extraction_results":
                    continue
                records.append(payload)

    return records


def _value_from_extracted_data(extracted: Any, variable: ExtractionVariable) -> Any:
    """human readable hint: locate the LLM value field generated from one KB variable."""

    if not isinstance(extracted, dict):
        return None
    domain_payload = extracted.get(variable.domain)
    if isinstance(domain_payload, dict):
        return domain_payload.get(variable.value_key)
    return None


def _quote_from_extracted_data(extracted: Any, variable: ExtractionVariable) -> str:
    """human readable hint: locate the LLM quote field generated for audit review."""

    if not isinstance(extracted, dict):
        return ""
    domain_payload = extracted.get(variable.domain)
    if not isinstance(domain_payload, dict):
        return ""
    quote = domain_payload.get(variable.quote_key)
    return "" if quote is None else str(quote)


def _normalization_key(value: Any, variable: ExtractionVariable) -> Any:
    """human readable hint: coerce AI and human-export values into comparable Python values by KB type."""

    if _is_extraction_missing(value, variable):
        return _missing_key(variable)

    if variable.variable_type == "list":
        return frozenset(_normalize_list_items(value))
    if variable.variable_type == "boolean":
        return _normalize_bool(value)
    if variable.variable_type == "integer":
        parsed = _normalize_number(value, integer=True)
        return parsed if parsed is not None else _normalize_scalar(value)
    if variable.variable_type == "float":
        parsed = _normalize_number(value, integer=False)
        return parsed if parsed is not None else _normalize_scalar(value)
    return _normalize_scalar(value)


def _validation_setting(key: str, default: float | int) -> float:
    """human readable hint: read generic validation thresholds from user-editable config."""

    if isinstance(DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS, dict):
        try:
            return float(DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS.get(key, default))
        except Exception:
            return float(default)
    return float(default)


def _validation_bool_setting(key: str, default: bool) -> bool:
    """human readable hint: keep optional fuzzy validation behavior explicit and user-editable."""

    if isinstance(DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS, dict):
        value = DATA_EXTRACTION_VALIDATION_MATCH_SETTINGS.get(key, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().casefold() in {"1", "true", "yes", "y"}
    return default


def _validation_review_note_column_name(consensus_column_name: str) -> str:
    """human readable hint: companion note columns preserve reviewer correction text for quote-aware validation."""

    return f"{consensus_column_name}__human_note"


def _validation_text(value: Any) -> str:
    """human readable hint: normalize prose before factual-congruence matching."""

    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        value = " ".join(str(item) for item in value)
    text = unicodedata.normalize("NFKD", str(value))
    # human readable hint: normalize typography before ASCII folding so semantically identical model names still match.
    text = text.translate(
        str.maketrans(
            {
                "\u00a0": " ",
                "\u2010": "-",
                "\u2011": "-",
                "\u2012": "-",
                "\u2013": "-",
                "\u2014": "-",
                "\u2015": "-",
                "\u2212": "-",
                "\u2043": "-",
                "\u2215": "/",
                "\u2044": "/",
                "\u2018": "'",
                "\u2019": "'",
                "\u201a": "'",
                "\u201b": "'",
                "\u201c": '"',
                "\u201d": '"',
                "\u201e": '"',
                "\u201f": '"',
            }
        )
    )
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.casefold()
    text = text.replace("_", " ")
    text = re.sub(r"[\[\]{}'\"`|;:,()/<>]+", " ", text)
    text = re.sub(r"[^a-z0-9.+%_-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _validation_tokens(value: Any) -> set[str]:
    """human readable hint: derive compact content tokens from prose, lists, and table fragments."""

    text = _validation_text(value)
    tokens = set()
    for token in re.findall(r"[a-z0-9][a-z0-9.+%_-]*", text):
        normalized = token.strip("-_")
        if len(normalized) < 2 or normalized in VALIDATION_STOPWORDS or normalized == "not":
            continue
        tokens.add(normalized)
    return tokens


def _validation_numbers(value: Any) -> list[float]:
    """human readable hint: extract comparable numeric evidence such as percentages, ages, and sample sizes."""

    numbers: list[float] = []
    for match in re.findall(r"(?<![a-z])\d+(?:\.\d+)?", _validation_text(value)):
        try:
            numbers.append(float(match))
        except ValueError:
            continue
    return numbers


def _numbers_overlap(human_value: Any, ai_value: Any) -> bool:
    """human readable hint: allow small rounding differences in reviewer and AI numeric expressions."""

    tolerance = _validation_setting("numeric_relative_tolerance", 0.05)
    human_numbers = _validation_numbers(human_value)
    ai_numbers = _validation_numbers(ai_value)
    for human_number in human_numbers:
        for ai_number in ai_numbers:
            absolute_tolerance = max(0.5, abs(human_number) * tolerance)
            if abs(human_number - ai_number) <= absolute_tolerance:
                return True
    return False


def _token_overlap_ratio(human_value: Any, ai_value: Any) -> float:
    """human readable hint: measure whether AI wording covers the reviewer-derived fact pattern."""

    human_tokens = _validation_tokens(human_value)
    ai_tokens = _validation_tokens(ai_value)
    if not human_tokens or not ai_tokens:
        return 0.0
    return len(human_tokens & ai_tokens) / max(1, min(len(human_tokens), len(ai_tokens)))


def _factual_text_match(human_value: Any, ai_value: Any, variable: ExtractionVariable) -> bool:
    """human readable hint: accept factual prose overlap when exact string equality is too brittle."""

    human_tokens = _validation_tokens(human_value)
    ai_tokens = _validation_tokens(ai_value)
    min_tokens = int(_validation_setting("minimum_token_count_for_fuzzy", 2))
    if len(human_tokens) < min_tokens or len(ai_tokens) < min_tokens:
        return False
    overlap = _token_overlap_ratio(human_value, ai_value)
    if variable.variable_type == "list":
        threshold = _validation_setting("list_token_overlap_threshold", 0.35)
    elif min(len(human_tokens), len(ai_tokens)) <= 4:
        threshold = _validation_setting("short_text_token_overlap_threshold", 0.50)
    else:
        threshold = _validation_setting("free_text_token_overlap_threshold", 0.42)
    if overlap >= threshold:
        return True
    return _numbers_overlap(human_value, ai_value) and overlap >= max(0.25, threshold - 0.15)


def _validation_alias_match(human_value: Any, ai_value: Any, variable: ExtractionVariable) -> bool:
    """human readable hint: apply user-editable semantic equivalence groups after plain normalization."""

    if not isinstance(DATA_EXTRACTION_VALIDATION_VALUE_ALIASES, dict):
        return False
    alias_key = f"{variable.domain}.{variable.variable_name}"
    groups = list(DATA_EXTRACTION_VALIDATION_VALUE_ALIASES.get(alias_key, ()) or [])
    groups.extend(DATA_EXTRACTION_VALIDATION_VALUE_ALIASES.get("*", ()) or [])
    human_text = _validation_text(human_value)
    ai_text = _validation_text(ai_value)
    if not human_text or not ai_text:
        return False
    for group in groups or ():
        terms = [_validation_text(term) for term in group or () if _validation_text(term)]
        human_hit = any(term in human_text or human_text in term for term in terms)
        ai_hit = any(term in ai_text or ai_text in term for term in terms)
        if human_hit and ai_hit:
            return True
    return False


def _extraction_values_match(human_value: Any, ai_value: Any, variable: ExtractionVariable) -> bool:
    """human readable hint: compare AI output against reviewer-derived ground truth."""

    if _normalization_key(human_value, variable) == _normalization_key(ai_value, variable):
        return True
    if _validation_alias_match(human_value, ai_value, variable):
        return True
    if variable.variable_type in {"integer", "float", "boolean", "enum"}:
        return False
    if not _validation_bool_setting("count_fuzzy_matches_in_metrics", False):
        return False
    return _factual_text_match(human_value, ai_value, variable)


def _quote_aware_text_match(human_value: Any, ai_value: Any, variable: ExtractionVariable) -> tuple[bool, str]:
    """human readable hint: compare prose facts for quote-aware validation without changing strict metrics."""

    human_missing = _is_extraction_missing(human_value, variable)
    ai_missing = _is_extraction_missing(ai_value, variable)
    if human_missing or ai_missing:
        return (human_missing and ai_missing, "both_missing" if human_missing and ai_missing else "missing_mismatch")
    if _extraction_values_match(human_value, ai_value, variable):
        return True, "strict_or_alias"
    human_text = _validation_text(human_value)
    ai_text = _validation_text(ai_value)
    if human_text and human_text == ai_text:
        return True, "punctuation_spacing_normalized"
    if human_text and ai_text:
        shorter, longer = sorted((human_text, ai_text), key=len)
        if len(shorter) >= 3 and re.search(rf"(?<![a-z0-9]){re.escape(shorter)}(?![a-z0-9])", longer):
            return True, "short_exact_containment"
    human_tokens = _validation_tokens(human_value)
    ai_tokens = _validation_tokens(ai_value)
    min_tokens = int(_validation_setting("minimum_token_count_for_fuzzy", 2))
    if len(human_tokens) < min_tokens or len(ai_tokens) < min_tokens:
        return False, "too_few_tokens"
    overlap = _token_overlap_ratio(human_value, ai_value)
    if variable.variable_type == "list":
        threshold = _validation_setting("quote_aware_list_token_overlap_threshold", 0.35)
    elif min(len(human_tokens), len(ai_tokens)) <= 4:
        threshold = _validation_setting("quote_aware_short_text_token_overlap_threshold", 0.50)
    else:
        threshold = _validation_setting("quote_aware_free_text_token_overlap_threshold", 0.42)
    if overlap >= threshold:
        return True, f"token_overlap_{overlap:.2f}"
    if _numbers_overlap(human_value, ai_value) and overlap >= max(0.25, threshold - 0.15):
        return True, f"numeric_plus_overlap_{overlap:.2f}"
    return False, f"overlap_{overlap:.2f}"


def _quote_aware_extraction_match(
    human_value: Any,
    ai_value: Any,
    ai_quote: str,
    reviewer_note: Any,
    variable: ExtractionVariable,
) -> tuple[bool, str]:
    """human readable hint: validate values using AI quotes and reviewer correction notes when configured."""

    comparisons: list[tuple[str, Any, Any]] = [("value_vs_truth", human_value, ai_value)]
    if _validation_bool_setting("quote_aware_compare_ai_quote", True):
        comparisons.append(("ai_quote_vs_truth", human_value, ai_quote))
    if reviewer_note and _validation_bool_setting("quote_aware_compare_reviewer_note", True):
        comparisons.append(("value_vs_reviewer_note", reviewer_note, ai_value))
        if _validation_bool_setting("quote_aware_compare_ai_quote", True):
            comparisons.append(("ai_quote_vs_reviewer_note", reviewer_note, ai_quote))

    reasons: list[str] = []
    for label, left, right in comparisons:
        matched, reason = _quote_aware_text_match(left, right, variable)
        if matched:
            reasons.append(f"{label}:{reason}")
    return bool(reasons), "; ".join(reasons)


def _missing_key(variable: ExtractionVariable) -> Any:
    """human readable hint: use type-aware missing values so n/a, Not Available, [], and false align."""

    if variable.variable_type == "list":
        return frozenset()
    if variable.variable_type == "boolean":
        return False
    return MISSING_TEXT_VALUE.casefold()


def _is_extraction_missing(value: Any, variable: ExtractionVariable) -> bool:
    """human readable hint: treat human n/a and LLM missing conventions as the same absence signal."""

    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if variable.variable_type == "list":
        return len(_normalize_list_items(value)) == 0
    text = str(value).strip().casefold()
    return text in {"", "n/a", "na", "not available", "not applicable", "none", "null", "missing"}


def _normalize_scalar(value: Any) -> str:
    """human readable hint: compare scalar text robustly without changing the scientific meaning."""

    text = "" if value is None else str(value)
    text = re.sub(r"\s+", " ", text.strip())
    return text.casefold()


def _normalize_list_items(value: Any) -> list[str]:
    """human readable hint: split human-export comma-separated selections and LLM JSON arrays into comparable sets."""

    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        text = str(value).strip()
        if text.casefold() in {"", "n/a", "na", "not available", "not applicable", "none", "null"}:
            return []
        raw_items = re.split(r"[;,|\n]", text)
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        cleaned = re.sub(r"\s+", " ", str(item).strip()).casefold()
        if not cleaned or cleaned in {"n/a", "na", "not available", "not applicable", "none", "null"}:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def _normalize_bool(value: Any) -> bool:
    """human readable hint: normalize yes/no style human exports and JSON booleans."""

    if isinstance(value, bool):
        return value
    text = str(value).strip().casefold()
    return text in {"true", "1", "yes", "y", "present", "reported", "explicit"}


def _human_score_column_name(consensus_column_name: str) -> str:
    """human readable hint: companion columns can carry human 0/1 judgements without changing schema columns."""

    return f"{consensus_column_name}{EXTRACTION_HUMAN_SCORE_SUFFIX}"


def _parse_human_cell_score(value: Any) -> Optional[bool]:
    """human readable hint: accept common binary reviewer score encodings for data-extraction cells."""

    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().casefold()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "correct", "match", "stimmt"}:
        return True
    if text in {"0", "false", "no", "n", "incorrect", "mismatch", "stimmt nicht"}:
        return False
    return None


def _human_score_columns_present(human_wide: pd.DataFrame, variables: tuple[ExtractionVariable, ...]) -> bool:
    """human readable hint: detect reviewer 0/1 judgement columns in generated human gold-standard tables."""

    for variable in variables:
        if _human_score_column_name(variable.covidence_column_name) in human_wide.columns:
            return True
    return False


def _latest_binary_review_source() -> Path | None:
    """human readable hint: the editable reviewer scoring sheet in input/ is the source of truth when present."""

    exact_path = INPUT_DIR / EXTRACTION_HUMAN_BINARY_SOURCE_NAME
    if exact_path.exists():
        return exact_path
    candidates = sorted(
        INPUT_DIR.glob("data_extraction_human_review_qc_sample_binary_scoring*.csv"),
        key=lambda path: path.stat().st_mtime,
    )
    return candidates[-1] if candidates else None


def _build_gold_standard_frame_from_binary_source(source_path: Path, schema_path: Path) -> pd.DataFrame:
    """human readable hint: convert the editable binary reviewer sheet in memory for validation."""

    from pipeline.additions.human_gold_standard_builder import HumanGoldStandardBuilder

    build = HumanGoldStandardBuilder(
        source_path=source_path,
        schema_path=schema_path,
        output_dir=OUTPUT_DIR,
    ).build()
    return pd.DataFrame(build["wide_rows"])


def _resolve_extraction_human_path(consensus_path: Optional[str], schema_path: Path | None = None) -> Path:
    """human readable hint: resolve explicit legacy consensus files only."""

    if consensus_path:
        return Path(consensus_path)
    raise FileNotFoundError(
        f"Missing editable human review source at {INPUT_DIR / EXTRACTION_HUMAN_BINARY_SOURCE_NAME}"
    )


def _load_extraction_human_wide(
    consensus_path: Optional[str],
    schema_path: Path,
) -> tuple[pd.DataFrame, str]:
    """human readable hint: load human truth from input CSV in memory; do not write derived gold files."""

    if consensus_path:
        path = Path(consensus_path)
        if path.name.startswith("data_extraction_human_review_qc_sample_binary_scoring"):
            return _build_gold_standard_frame_from_binary_source(path, schema_path), str(path)
        return pd.read_csv(path), str(path)

    binary_source = _latest_binary_review_source()
    if binary_source:
        return _build_gold_standard_frame_from_binary_source(binary_source, schema_path), str(binary_source)

    path = _resolve_extraction_human_path(consensus_path, schema_path)
    return pd.read_csv(path), str(path)


def _apply_human_export_aliases(human_wide: pd.DataFrame, variables: tuple[ExtractionVariable, ...]) -> pd.DataFrame:
    """human readable hint: user-configured header aliases adapt old reviewer templates to the active schema."""

    if not isinstance(DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES, dict):
        return human_wide
    human_wide = human_wide.copy()
    for variable in variables:
        target = variable.covidence_column_name
        if target in human_wide.columns:
            continue
        alias_key = f"{variable.domain}.{variable.variable_name}"
        aliases = DATA_EXTRACTION_COVIDENCE_HEADER_ALIASES.get(alias_key, ())
        for alias in aliases or ():
            alias_name = str(alias or "").strip()
            if alias_name and alias_name in human_wide.columns:
                human_wide[target] = human_wide[alias_name]
                break
    return human_wide


def _normalize_number(value: Any, *, integer: bool) -> int | float | None:
    """human readable hint: compare numeric human-export and JSON values even when one side is text."""

    try:
        number = float(str(value).strip())
    except Exception:
        return None
    if math.isnan(number):
        return None
    return int(number) if integer and number.is_integer() else number


def validate_extraction(
    consensus_path: Optional[str] = None,
    ai_output_dir: Optional[str] = None,
    use_human_score_columns: bool = False,
) -> None:
    """Validate extraction outputs against human gold-standard columns mapped by the KB."""

    schema = DynamicExtractionSchema.from_kb()
    ai_root = Path(ai_output_dir) if ai_output_dir else OUTPUT_DIR

    # human readable hint: load the human export once and normalize the paper identifier for AI-human matching.
    human_wide, human_source_label = _load_extraction_human_wide(consensus_path, schema.kb_path)
    human_wide = _clean_cols(human_wide)
    human_wide = _apply_human_export_aliases(human_wide, schema.variables)
    human_wide["paper_id"] = _normalize_id_column(human_wide)
    # human readable hint: data-extraction validation is scoped to the human-reviewed QC batch only.
    qc_ids = _load_qc_sample_ids(None)
    if qc_ids:
        scoped_human = human_wide[human_wide["paper_id"].isin(qc_ids)].copy()
        if not scoped_human.empty:
            human_wide = scoped_human
        else:
            print(
                "[qc] Data-extraction QC-ID filtering produced zero overlap; "
                "validation will use human-reviewed rows from the source file."
            )
    missing_columns = [
        variable.covidence_column_name
        for variable in schema.variables
        if variable.covidence_column_name not in human_wide.columns
    ]
    if missing_columns:
        raise KeyError(
            "Human export is missing KB-mapped column(s): " + ", ".join(sorted(set(missing_columns)))
        )
    # human readable hint: generated human gold-standard sheets carry reviewer-derived values; score columns are audit unless explicitly requested.
    effective_use_human_score_columns = use_human_score_columns
    quote_aware_metrics = _validation_bool_setting("quote_aware_match_in_metrics", True)

    # human readable hint: index AI JSONL records by normalized paper ID and keep the LLM quote beside each value.
    ai_by_id: dict[str, dict[str, Any]] = {}
    for payload in _load_ai_extraction_records(ai_root):
        pid = _normalize_paper_id_value(payload.get("paper_id", ""))
        if not pid:
            continue
        ai_by_id[pid] = payload

    rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, str]] = []
    for _, human_row in human_wide.iterrows():
        paper_id = _normalize_paper_id_value(human_row.get("paper_id", ""))
        ai_payload = ai_by_id.get(paper_id, {})
        extracted = ai_payload.get("extracted_data", {}) if isinstance(ai_payload, dict) else {}

        for variable in schema.variables:
            evaluable_col = f"{variable.covidence_column_name}{EXTRACTION_HUMAN_EVALUABLE_SUFFIX}"
            if evaluable_col in human_wide.columns and str(human_row.get(evaluable_col, "")).strip().casefold() == "false":
                continue
            human_value = human_row.get(variable.covidence_column_name)
            ai_value = _value_from_extracted_data(extracted, variable)
            ai_quote = _quote_from_extracted_data(extracted, variable)
            reviewer_note_col = _validation_review_note_column_name(variable.covidence_column_name)
            reviewer_note = human_row.get(reviewer_note_col, "") if reviewer_note_col in human_wide.columns else ""
            human_missing = _is_extraction_missing(human_value, variable)
            ai_missing = _is_extraction_missing(ai_value, variable)
            score_col = _human_score_column_name(variable.covidence_column_name)
            human_score = (
                _parse_human_cell_score(human_row.get(score_col))
                if effective_use_human_score_columns and score_col in human_wide.columns
                else None
            )
            strict_match = _extraction_values_match(human_value, ai_value, variable)
            quote_aware_match, quote_aware_reason = _quote_aware_extraction_match(
                human_value,
                ai_value,
                ai_quote,
                reviewer_note,
                variable,
            )
            match = human_score if human_score is not None else (quote_aware_match if quote_aware_metrics else strict_match)
            match_source = "human_binary_score" if human_score is not None else (
                "quote_aware_value_or_quote" if quote_aware_metrics else "normalized_exact_value"
            )

            rows.append(
                {
                    "paper_id": paper_id,
                    "variable": variable.variable_name,
                    "covidence_column_name": variable.covidence_column_name,
                    "human_present": not human_missing,
                    "human_missing": human_missing,
                    "ai_missing": ai_missing,
                    "match": match,
                    "strict_match": strict_match,
                    "quote_aware_match": quote_aware_match,
                    "match_source": match_source,
                    "quote_aware_reason": quote_aware_reason,
                    "human_value": "" if human_value is None else str(human_value),
                    "reviewer_note": "" if reviewer_note is None else str(reviewer_note),
                    "ai_value": "" if ai_value is None else str(ai_value),
                    "ai_quote": ai_quote,
                }
            )

            if not match:
                audit_rows.append(
                    {
                        VALIDATION_PAPER_ID_COLUMN: paper_id,
                        VALIDATION_VARIABLE_COLUMN: variable.variable_name,
                        VALIDATION_HUMAN_VALUE_COLUMN: "" if human_value is None else str(human_value),
                        "reviewer_note": "" if reviewer_note is None else str(reviewer_note),
                        VALIDATION_AI_VALUE_COLUMN: "" if ai_value is None else str(ai_value),
                        VALIDATION_AI_QUOTE_COLUMN: ai_quote,
                        "strict_match": str(strict_match).lower(),
                        "quote_aware_match": str(quote_aware_match).lower(),
                        "quote_aware_reason": quote_aware_reason,
                        VALIDATION_ERROR_TYPE_COLUMN: "",
                        VALIDATION_ERROR_EFFECT_COLUMN: "",
                    }
                )

    comparison = pd.DataFrame(rows)
    if comparison.empty:
        raise ValueError("No comparable extraction rows were produced from the KB and human file.")

    # human readable hint: concordance ignores human-missing cells; accuracy counts both exact present matches and correct absences.
    metric_rows: list[dict[str, Any]] = []
    for variable in schema.variables:
        subset = comparison[comparison["variable"] == variable.variable_name]
        present_subset = subset[subset["human_present"]]
        variable_paper_n = int(subset["paper_id"].nunique())
        variable_present_paper_n = int(present_subset["paper_id"].nunique())
        concordance_n = int(len(present_subset))
        concordance_matches = int(present_subset["match"].sum()) if concordance_n else 0
        strict_concordance_matches = int(present_subset["strict_match"].sum()) if concordance_n else 0
        accuracy_n = int(len(subset))
        accuracy_matches = int(subset["match"].sum()) if accuracy_n else 0
        strict_accuracy_matches = int(subset["strict_match"].sum()) if accuracy_n else 0
        concordance = concordance_matches / concordance_n if concordance_n else math.nan
        accuracy = accuracy_matches / accuracy_n if accuracy_n else math.nan
        strict_concordance = strict_concordance_matches / concordance_n if concordance_n else math.nan
        strict_accuracy = strict_accuracy_matches / accuracy_n if accuracy_n else math.nan
        quote_aware_rescued = int(
            ((subset["strict_match"] == False) & (subset["quote_aware_match"] == True)).sum()  # noqa: E712
        )

        if (not math.isnan(concordance) and concordance < 0.80) or (
            not math.isnan(accuracy) and accuracy < 0.90
        ):
            LOGGER.critical(
                "Prompt Refinement Triggered: variable=%s concordance=%s accuracy=%s",
                variable.variable_name,
                "n/a" if math.isnan(concordance) else f"{concordance:.3f}",
                "n/a" if math.isnan(accuracy) else f"{accuracy:.3f}",
            )

        metric_rows.append(
            {
                VALIDATION_VARIABLE_COLUMN: variable.variable_name,
                VALIDATION_CONSENSUS_COLUMN: variable.covidence_column_name,
                "N_Papers": variable_paper_n,
                "N_Papers_Human_Present": variable_present_paper_n,
                "Concordance_Matches": concordance_matches,
                "Concordance_Total_Human_Present": concordance_n,
                "Concordance": concordance,
                "Accuracy_Matches": accuracy_matches,
                "Accuracy_Total_All_Parsed": accuracy_n,
                "Accuracy": accuracy,
                "Strict_Concordance_Matches": strict_concordance_matches,
                "Strict_Concordance": strict_concordance,
                "Strict_Accuracy_Matches": strict_accuracy_matches,
                "Strict_Accuracy": strict_accuracy,
                "Quote_Aware_Rescued": quote_aware_rescued,
            }
        )

    per_variable = pd.DataFrame(metric_rows)
    overall_present = comparison[comparison["human_present"]]
    overall_paper_n = int(comparison["paper_id"].nunique())
    overall_present_paper_n = int(overall_present["paper_id"].nunique())
    overall_concordance = (
        float(overall_present["match"].sum()) / float(len(overall_present)) if len(overall_present) else math.nan
    )
    overall_accuracy = float(comparison["match"].sum()) / float(len(comparison)) if len(comparison) else math.nan
    strict_overall_concordance = (
        float(overall_present["strict_match"].sum()) / float(len(overall_present)) if len(overall_present) else math.nan
    )
    strict_overall_accuracy = (
        float(comparison["strict_match"].sum()) / float(len(comparison)) if len(comparison) else math.nan
    )
    quote_aware_rescued_total = int(
        ((comparison["strict_match"] == False) & (comparison["quote_aware_match"] == True)).sum()  # noqa: E712
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    metrics_path = _stage_file("extraction_accuracy_report.csv")
    audit_path = OUTPUT_DIR / "extraction_error_audit.csv"
    cell_audit_path = _stage_file("extraction_validation_cell_audit.csv")
    report_path = _stage_file("extraction_accuracy_report.txt")

    per_variable.to_csv(metrics_path, index=False, encoding="utf-8")
    comparison.to_csv(cell_audit_path, index=False, encoding="utf-8")
    pd.DataFrame(
        audit_rows,
        columns=[
            VALIDATION_PAPER_ID_COLUMN,
            VALIDATION_VARIABLE_COLUMN,
            VALIDATION_HUMAN_VALUE_COLUMN,
            "reviewer_note",
            VALIDATION_AI_VALUE_COLUMN,
            VALIDATION_AI_QUOTE_COLUMN,
            "strict_match",
            "quote_aware_match",
            "quote_aware_reason",
            VALIDATION_ERROR_TYPE_COLUMN,
            VALIDATION_ERROR_EFFECT_COLUMN,
        ],
    ).to_csv(audit_path, index=False, encoding="utf-8")

    lines = [
        "Extraction validation report (AI vs human gold standard)",
        f"Schema KB: {schema.kb_path}",
        f"Human source: {human_source_label}",
        f"AI output dir: {ai_root}",
        "Validation scope: human-reviewed QC sample rows only",
        f"Human score companion columns used: {effective_use_human_score_columns}",
        f"Quote-aware validation used for metrics: {quote_aware_metrics}",
        f"Variables parsed from KB: {len(schema.variables)}",
        f"n_papers: {overall_paper_n}",
        f"n_papers with human-present values: {overall_present_paper_n}",
        f"Total variable-paper comparisons: {len(comparison)}",
        (
            f"Strict value-only concordance lower bound: {strict_overall_concordance*100:.2f}%"
            if not math.isnan(strict_overall_concordance)
            else "Strict value-only concordance lower bound: n/a"
        ),
        (
            f"Strict value-only accuracy lower bound: {strict_overall_accuracy*100:.2f}%"
            if not math.isnan(strict_overall_accuracy)
            else "Strict value-only accuracy lower bound: n/a"
        ),
        f"Quote-aware rescued comparisons: {quote_aware_rescued_total}",
        (
            f"Overall concordance: {overall_concordance*100:.2f}%"
            if not math.isnan(overall_concordance)
            else "Overall concordance: n/a"
        ),
        (
            f"Overall accuracy: {overall_accuracy*100:.2f}%"
            if not math.isnan(overall_accuracy)
            else "Overall accuracy: n/a"
        ),
        "",
        "Per-variable thresholds: Concordance >= 0.80 and Accuracy >= 0.90.",
    ]
    for _, row in per_variable.iterrows():
        concordance = row["Concordance"]
        accuracy = row["Accuracy"]
        lines.append(
            f"- {row[VALIDATION_VARIABLE_COLUMN]} -> {row[VALIDATION_CONSENSUS_COLUMN]}: "
            f"n_papers={int(row['N_Papers'])}, "
            f"n_papers_human_present={int(row['N_Papers_Human_Present'])}, "
            f"concordance={'n/a' if math.isnan(concordance) else f'{concordance:.3f}'}, "
            f"accuracy={'n/a' if math.isnan(accuracy) else f'{accuracy:.3f}'}"
        )
    report_path.write_text("\n".join(lines), encoding="utf-8")

    print("[validation] stage=data_extraction status=completed")
    print(f"[output] extraction_validation_report path={report_path.relative_to(ROOT)}")
    print(f"[output] extraction_accuracy_csv path={metrics_path.relative_to(ROOT)}")
    print(f"[output] extraction_validation_cell_audit path={cell_audit_path.relative_to(ROOT)}")
    print(f"[output] extraction_error_audit path={audit_path.relative_to(ROOT)}")


def _parse_args():
    """Parse CLI arguments for validation."""

    parser = argparse.ArgumentParser(description="Validate AI screening against configured human exports.")
    parser.add_argument("--select", help="Path to *_select_csv_* (title_abstract stage)")
    parser.add_argument("--irrelevant", help="Path to *_irrelevant_csv_* (title_abstract stage)")
    parser.add_argument("--included", help="Path to *_included_csv_* (full_text stage)")
    parser.add_argument("--excluded", help="Path to *_excluded_csv_* (full_text stage)")
    parser.add_argument("--consensus", help="Path to human gold-standard CSV for data_extraction")
    parser.add_argument("--ai-output-dir", help="Path to data_extraction output folder with per-paper JSONL files")
    parser.add_argument(
        "--use-human-score-columns",
        action="store_true",
        help="Use <covidence_column_name>__human_score companion columns as match decisions.",
    )
    return parser.parse_args()


class ValidationEngine:
    """human readable hint: one-class validation orchestrator for screening and extraction stages."""

    def __init__(self) -> None:
        """human readable hint: validation is intentionally bound to CURRENT_STAGE from config."""

        self.stage = CURRENT_STAGE

    def run(self, args=None) -> None:
        """human readable hint: run the correct validation branch based on the configured stage."""

        args = args or _parse_args()
        if self.stage == "data_extraction":
            validate_extraction(args.consensus, args.ai_output_dir, args.use_human_score_columns)
            return

        validate_screening(self.stage, args)


def run_validation() -> None:
    """Compatibility wrapper for direct execution."""

    ValidationEngine().run()


if __name__ == "__main__":
    run_validation()


