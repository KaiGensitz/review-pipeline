"""Direct run: python -m pipeline.additions.stats_engine

Validation utilities for screening and data extraction outputs.

This module compares AI outputs to human labels (screening) and to the
adjudicated consensus table (data extraction). Outputs include a
readable report, confusion matrix plot, and discrepancy logs.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from statsmodels.stats.proportion import proportion_confint

# human readable hint: proportion_confint supplies exact (Clopper-Pearson) binomial CIs (Seabold, S., & Perktold, J., 2010).

from config.user_orchestrator import CURRENT_STAGE, STUDY_TAGS_IGNORE, STUDY_TAGS_INCLUDE

ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = ROOT / "output" / CURRENT_STAGE
STAGE_PREFIX = f"{CURRENT_STAGE}_"
INPUT_DIR = ROOT / "input"
STAGE_OUTPUT_DIR = OUTPUT_DIR
EXTRACTION_AI_PATH = OUTPUT_DIR / f"{CURRENT_STAGE}_extraction_results.jsonl"
EXTRACTION_HUMAN_PATH = ROOT / "input" / "data_extraction_consensus.csv"

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


def _normalize_id_column(df: pd.DataFrame) -> pd.Series:
    """Find the best ID column and return it as strings."""

    candidates = ["Covidence #", "Covidence#", "paper_id", "id", "ID"]
    for col in candidates:
        if col in df.columns:
            return df[col].astype(str).str.strip().str.lstrip("#")
    raise KeyError("Could not find an ID column in Covidence export")


def _normalize_tag_text(value: str) -> str:
    text = value.strip().lower().replace("_", " ").replace("-", " ")
    text = " ".join(text.split())
    return text


def _extract_tags(value: Optional[object]) -> list[str]:
    """human readable hint: map explicit Covidence tags to the curated include list; ignores notes."""

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
    df["covidence_id"] = _normalize_id_column(df)

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

        df_inc["covidence_id"] = _normalize_id_column(df_inc)
        df_exc["covidence_id"] = _normalize_id_column(df_exc)

        common_cols = list(set(df_inc.columns) & set(df_exc.columns))
        human = pd.concat([df_inc[common_cols], df_exc[common_cols]], ignore_index=True)
        human = human.drop_duplicates(subset=["covidence_id"], keep="first")
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

    df_yes["covidence_id"] = _normalize_id_column(df_yes)
    df_no["covidence_id"] = _normalize_id_column(df_no)

    common_cols = list(set(df_yes.columns) & set(df_no.columns))
    human = pd.concat([df_yes[common_cols], df_no[common_cols]], ignore_index=True)
    human = human.drop_duplicates(subset=["covidence_id"], keep="first")
    return human


def _normalize_text_value(val: Optional[object]) -> str:
    """Normalize values to compare AI vs human extraction consistently."""

    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""

    text = str(val).strip().lower()
    prefixes = ["n=", "n =", "p=", "p =", "p<", "p <", "p-value", "p value"]
    for pref in prefixes:
        if text.startswith(pref):
            text = text[len(pref):].strip()
            break
    text = text.replace("p <", "p<").replace("p-value", "pvalue").replace("p value", "pvalue")
    return text


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

    # human readable hint: gather all QC eligibility JSONL files (main + retries) and prefer newest per paper_id.
    stage_files: list[Path] = []
    patterns = [
        f"{CURRENT_STAGE}_qc_sample_*_eligibility_*.jsonl",  # catches main and retry naming
        f"{CURRENT_STAGE}_eligibility_qc_sample_*.jsonl",    # legacy naming
    ]
    for pat in patterns:
        stage_files.extend(STAGE_OUTPUT_DIR.glob(pat))

    # Exclude split outputs to avoid double-counting per decision type.
    stage_files = [p for p in stage_files if "eligibility_select" not in p.name and "eligibility_irrelevant" not in p.name]

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
                    paper_id = str(payload.get("paper_id", "")).strip().lstrip("#")
                    if not paper_id:
                        continue
                    decision_raw = payload.get("llm_decision")
                    ai_decision, ai_reason = _parse_ai_decision(decision_raw)
                    latest_by_paper[paper_id] = {
                        "paper_id": paper_id,
                        "ai_decision": ai_decision,
                        "ai_reason": ai_reason or str(payload.get("diagnostics", {})),
                        "source_path": path,
                    }
        except Exception:
            continue

    records = list(latest_by_paper.values())
    if not records:
        return pd.DataFrame(), stage_files[-1]
    return pd.DataFrame(records), stage_files[-1]


def _merge(ai: pd.DataFrame, human: pd.DataFrame) -> pd.DataFrame:
    """Merge AI and human labels on the paper ID."""

    human = human.copy()
    ai = ai.copy()
    ai.rename(columns={"paper_id": "covidence_id"}, inplace=True)

    merged = human.merge(ai, on="covidence_id", how="inner", suffixes=("_human", "_ai"))
    if merged["covidence_id"].duplicated().any():
        dupes = merged[merged["covidence_id"].duplicated()]["covidence_id"].unique().tolist()
        raise ValueError(f"Duplicate IDs after merge: {dupes}")

    merged["ai_decision"] = merged["ai_decision"].fillna(-1).astype(int)
    merged = merged.sort_values("covidence_id").reset_index(drop=True)
    return merged


def _confusion(df: pd.DataFrame) -> tuple[int, int, int, int]:
    """Compute confusion-matrix counts."""

    tp = int(((df["ai_decision"] == 1) & (df["human_decision"] == 1)).sum())
    tn = int(((df["ai_decision"] == 0) & (df["human_decision"] == 0)).sum())
    fp = int(((df["ai_decision"] == 1) & (df["human_decision"] == 0)).sum())
    fn = int(((df["ai_decision"] == 0) & (df["human_decision"] == 1)).sum())
    return tp, tn, fp, fn


def _prop_ci(k: int | float, n: int | float, alpha: float = 0.05) -> Tuple[float, float]:
    """human readable hint: exact (Clopper-Pearson) CI via statsmodels (Seabold & Perktold, 2010)."""

    if not isinstance(k, (int, float)) or not isinstance(n, (int, float)):
        raise TypeError("k and n must be scalar numbers")
    if n == 0:
        return (math.nan, math.nan)
    lower, upper = proportion_confint(count=float(k), nobs=float(n), alpha=alpha, method="beta")
    return float(lower), float(upper)


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
    out.rename(columns={"covidence_id": "ID"}, inplace=True)
    out["decision_match"] = out["ai_decision"] == out["human_decision"]
    ai_tags = out["ai_reason"].apply(_tag_list)
    human_tags_series = out["human_tag"].apply(_tag_list) if "human_tag" in out.columns else pd.Series([[]] * len(out))
    out["human_tag"] = human_tags_series.apply(lambda tags: " | ".join(tags))
    out["reason_match"] = [bool(a and h and (a[0] in h)) for a, h in zip(ai_tags, human_tags_series)]

    metadata_cols: list[str] = []
    for variants in [("Title", "title"), ("Abstract", "abstract"), ("Authors", "authors"), ("Year", "year")]:
        for candidate in variants:
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
        "This study compares the AI screening decisions against human ground truth from Covidence exports. "
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
    """Extract the YYYYMMDD_HH-MM suffix from a stage output filename."""

    name = path.stem
    prefix = f"{CURRENT_STAGE}_eligibility_"
    if not name.startswith(prefix):
        return None

    suffix = name.replace(prefix, "")
    # Strip decision-split markers if present to keep validation names clean.
    for token in ["select_", "irrelevant_", "included_", "excluded_"]:
        if suffix.startswith(token):
            suffix = suffix.replace(token, "", 1)
    return suffix


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

    return {str(val) for val in df["paper_id"].dropna().astype(str).tolist()}


def validate_screening(stage: str, args) -> None:
    """Validate screening decisions against human labels."""

    ai, ai_path = _load_ai()
    human = _load_human(stage, args)
    merged = _merge(ai, human)

    suffix = _extract_timestamp_suffix(ai_path)
    qc_ids = _load_qc_sample_ids(suffix)
    if qc_ids:
        merged = merged[merged["covidence_id"].isin(qc_ids)].copy()
    if merged.empty:
        print("[qc] No overlapping IDs after filtering; validation outputs will report zero totals.")

    tp, tn, fp, fn = _confusion(merged)
    stats = _metrics(tp, tn, fp, fn)

    _write_alignment(merged, suffix)
    _write_report(stats, tp, tn, fp, fn, stage, suffix)
    _plot_confusion(tp, tn, fp, fn, suffix)

    print(f"Validation complete (screening stage: {stage}). Outputs:")
    print(f"- {_stage_file('validation_stats_report.txt', suffix).relative_to(ROOT)}")
    print(f"- {_stage_file('validation_alignment.csv', suffix).relative_to(ROOT)}")
    print(f"- {_stage_file('validation_matrix.png', suffix).relative_to(ROOT)}")


def _load_ai_extraction_records() -> list[dict]:
    """Load extraction outputs from per-paper JSONL files."""

    records: list[dict] = []
    if EXTRACTION_AI_PATH.exists():
        ai_files = [EXTRACTION_AI_PATH]
    else:
        ai_files = sorted(OUTPUT_DIR.glob("*/data_extraction_extraction_results.jsonl"))

    if not ai_files:
        raise FileNotFoundError(
            "Missing AI extraction files. Expected per-paper files at output/data_extraction/<paper>/data_extraction_extraction_results.jsonl"
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


def validate_extraction(consensus_path: Optional[str] = None) -> None:
    """Validate extraction outputs against the adjudicated consensus table."""

    human_path = Path(consensus_path) if consensus_path else EXTRACTION_HUMAN_PATH
    if not human_path.exists():
        raise FileNotFoundError(f"Missing human consensus file at {human_path}")

    ai_records = []
    for payload in _load_ai_extraction_records():
        pid = str(payload.get("paper_id", ""))
        extracted = payload.get("extracted_data", {}) or {}
        for field, value in extracted.items():
            ai_records.append(
                {
                    "covidence_id": pid,
                    "field": str(field),
                    "ai_value": "" if value is None else str(value),
                    "ai_norm": _normalize_text_value(value),
                }
            )
    ai_df = pd.DataFrame(ai_records)

    human_wide = _clean_cols(pd.read_csv(human_path))
    human_wide["covidence_id"] = _normalize_id_column(human_wide)
    value_cols = [c for c in human_wide.columns if c != "covidence_id"]
    human_long = human_wide.melt(
        id_vars=["covidence_id"],
        value_vars=value_cols,
        var_name="field",
        value_name="human_value",
    )
    human_long["human_norm"] = human_long["human_value"].apply(_normalize_text_value)

    merged = human_long.merge(ai_df, on=["covidence_id", "field"], how="left")
    merged["ai_norm"] = merged["ai_norm"].fillna("")
    merged["ai_value"] = merged["ai_value"].fillna("")

    eval_mask = merged["human_norm"] != ""
    merged_eval = merged[eval_mask].copy()
    merged_eval["match"] = merged_eval["ai_norm"] == merged_eval["human_norm"]

    per_field = (
        merged_eval.groupby("field").apply(
            lambda g: pd.Series(
                {
                    "matches": int(g["match"].sum()),
                    "total": int(len(g)),
                    "accuracy": (g["match"].sum() / len(g)) if len(g) else math.nan,
                }
            )
        )
    ).reset_index()

    # human readable hint: compute exact binomial CI per field (Clopper-Pearson) for concordance (matches/total).
    if per_field.empty:
        per_field["ci_lower"] = []
        per_field["ci_upper"] = []
    else:
        # Make pandas dtypes explicit for type checkers and then compute Clopper-Pearson CIs.
        per_field = per_field.assign(
            matches=per_field["matches"].astype(float),
            total=per_field["total"].astype(float),
        )
        ci_pairs = [
            _prop_ci(float(row.matches), float(row.total))
            for row in per_field.itertuples(index=False)
        ]
        if ci_pairs:
            per_field["ci_lower"], per_field["ci_upper"] = zip(*ci_pairs)
        else:
            per_field["ci_lower"] = []
            per_field["ci_upper"] = []

    total_matches = int(merged_eval["match"].sum())
    total_items = int(len(merged_eval))
    overall_accuracy = (total_matches / total_items) if total_items else math.nan
    overall_ci = _prop_ci(total_matches, total_items)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("Extraction accuracy report (AI vs adjudicated consensus)")
    lines.append(f"Total items evaluated: {total_items}")
    lines.append(
        f"Overall concordance/accuracy: {overall_accuracy*100:.2f}% (95% CI {overall_ci[0]*100:.2f}-{overall_ci[1]*100:.2f}%)"
        if not math.isnan(overall_accuracy)
        else "Overall concordance/accuracy: n/a"
    )
    lines.append("")
    lines.append("Per-field accuracy (Clopper-Pearson 95% CI):")
    for _, row in per_field.iterrows():
        acc = row["accuracy"]
        if math.isnan(acc):
            lines.append(f"- {row['field']}: n/a (n={row['total']}, matches={row['matches']})")
        else:
            lines.append(
                f"- {row['field']}: {acc*100:.2f}% (95% CI {row['ci_lower']*100:.2f}-{row['ci_upper']*100:.2f}%)"
                f" (n={row['total']}, matches={row['matches']})"
            )

    _stage_file("extraction_accuracy_report.txt").write_text("\n".join(lines), encoding="utf-8")

    with open(_stage_file("validation_stats_report.txt"), "a", encoding="utf-8") as rpt:
        rpt.write("\n\n" + "\n".join(lines))

    discrep = merged_eval[~merged_eval["match"]].copy()
    discrep.rename(
        columns={
            "covidence_id": "PaperID",
            "field": "Field_Name",
            "ai_value": "AI_Value",
            "human_value": "Human_Consensus_Value",
        },
        inplace=True,
    )
    # human readable hint: scaffold columns for manual error typing per Gartlehner et al. (missed/fabricated/misallocated/etc.).
    discrep["Error_Type"] = "unspecified"
    discrep["Error_Impact"] = "unspecified"
    discrep_out_cols = ["PaperID", "Field_Name", "AI_Value", "Human_Consensus_Value", "Error_Type", "Error_Impact"]
    discrep[discrep_out_cols].to_csv(_stage_file("extraction_discrepancies.csv"), index=False, encoding="utf-8")

    print("Validation complete (data extraction stage). Outputs:")
    print(f"- {_stage_file('extraction_accuracy_report.txt').relative_to(ROOT)}")
    print(f"- {_stage_file('extraction_discrepancies.csv').relative_to(ROOT)}")


def _parse_args():
    """Parse CLI arguments for validation."""

    parser = argparse.ArgumentParser(description="Validate AI screening against Covidence exports.")
    parser.add_argument("--select", help="Path to *_select_csv_* (title_abstract stage)")
    parser.add_argument("--irrelevant", help="Path to *_irrelevant_csv_* (title_abstract stage)")
    parser.add_argument("--included", help="Path to *_included_csv_* (full_text stage)")
    parser.add_argument("--excluded", help="Path to *_excluded_csv_* (full_text stage)")
    parser.add_argument("--consensus", help="Path to data_extraction_consensus.csv (data_extraction stage)")
    return parser.parse_args()


def run_validation() -> None:
    """Route validation to the correct stage."""

    args = _parse_args()
    if CURRENT_STAGE == "data_extraction":
        validate_extraction(args.consensus)
        return

    validate_screening(CURRENT_STAGE, args)


if __name__ == "__main__":
    run_validation()
