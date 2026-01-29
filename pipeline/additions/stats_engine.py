"""Validation utilities for screening and data extraction outputs.

This module compares AI outputs to human labels (screening) and to the
adjudicated consensus table (data extraction). Outputs include a
readable report, confusion matrix plot, and discrepancy logs.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from scipy.stats import norm

from config.user_orchestrator import CURRENT_STAGE

ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = ROOT / "output" / CURRENT_STAGE
STAGE_PREFIX = f"{CURRENT_STAGE}_"
INPUT_DIR = ROOT / "input"
STAGE_OUTPUT_DIR = OUTPUT_DIR
EXTRACTION_AI_PATH = OUTPUT_DIR / f"{CURRENT_STAGE}_extraction_results.jsonl"
EXTRACTION_HUMAN_PATH = ROOT / "input" / "data_extraction_consensus.csv"

# Substrings to look for in human exclusion reasons
EXCLUSION_TAGS = [
    "not_adult_population",
    "no smartphone technology",
    "no artificial intelligence",
    "no physical activity",
    "not urban context",
    "wrong publication type",
]


def _stage_file(name: str, suffix: str | None = None) -> Path:
    """Build a stage-prefixed output path under output/<stage>/."""

    if suffix:
        base = Path(name)
        filename = f"{base.stem}_{suffix}{base.suffix}"
        return OUTPUT_DIR / f"{STAGE_PREFIX}{filename}"
    return OUTPUT_DIR / f"{STAGE_PREFIX}{name}"


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
            return df[col].astype(str)
    raise KeyError("Could not find an ID column in Covidence export")


def _extract_reason(value: Optional[str]) -> str:
    """Map free-text reasons into a small set of tags."""

    if not isinstance(value, str):
        return "Unspecified Reason"
    lower = value.lower()
    for tag in EXCLUSION_TAGS:
        if tag.lower() in lower:
            return tag
    return "Unspecified Reason"


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

    reason_col = None
    for cand in ["human_reason", "reason", "notes", "Notes", "Tags", "tags"]:
        if cand in df.columns:
            reason_col = cand
            break
    df["human_reason"] = df[reason_col] if reason_col else "QC human review"

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
        df_inc["human_reason"] = "Included at full text"

        df_exc["human_decision"] = 0
        reason_col = None
        for cand in ["Notes", "notes", "Note", "note", "Tags", "tags", "Tag", "tag"]:
            if cand in df_exc.columns:
                reason_col = cand
                break
        df_exc["human_reason"] = df_exc[reason_col].apply(_extract_ft_reason) if reason_col else "Unspecified Reason"

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
    df_yes["human_reason"] = "Included (Yes/Maybe)"

    df_no["human_decision"] = 0
    reason_col = None
    for cand in ["Tags", "tags", "Tag", "tag", "Notes", "notes", "Note", "note"]:
        if cand in df_no.columns:
            reason_col = cand
            break
    df_no["human_reason"] = df_no[reason_col].apply(_extract_reason) if reason_col else "Unspecified Reason"

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
    """Load AI screening decisions from JSONL and return the file path used."""

    ai_path = _find_latest_match([f"{CURRENT_STAGE}_eligibility_*.jsonl"], [STAGE_OUTPUT_DIR])
    if not ai_path:
        raise FileNotFoundError(
            f"Missing AI eligibility file for {CURRENT_STAGE}. Expected {CURRENT_STAGE}_eligibility_*.jsonl in {STAGE_OUTPUT_DIR}."
        )

    records = []
    with open(ai_path, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            paper_id = str(payload.get("paper_id", ""))
            decision_raw = payload.get("llm_decision")
            ai_decision, ai_reason = _parse_ai_decision(decision_raw)
            records.append(
                {
                    "paper_id": paper_id,
                    "ai_decision": ai_decision,
                    "ai_reason": ai_reason or str(payload.get("diagnostics", {})),
                }
            )

    return pd.DataFrame(records), ai_path


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


def _prop_ci(k: int, n: int, alpha: float = 0.05) -> Tuple[float, float]:
    """Compute a simple proportion confidence interval."""

    if n == 0:
        return (math.nan, math.nan)
    p = k / n
    z = norm.ppf(1 - alpha / 2)
    se = math.sqrt(max(p * (1 - p), 0) / n)
    lower = max(0.0, p - z * se)
    upper = min(1.0, p + z * se)
    return lower, upper


def _metrics(tp: int, tn: int, fp: int, fn: int) -> dict:
    """Compute agreement metrics for screening."""

    total = tp + tn + fp + fn
    po = (tp + tn) / total if total else math.nan
    pabak = 2 * po - 1 if total else math.nan

    sens_n = tp + fn
    spec_n = tn + fp
    ppv_n = tp + fp
    npv_n = tn + fn

    sens = tp / sens_n if sens_n else math.nan
    spec = tn / spec_n if spec_n else math.nan
    ppv = tp / ppv_n if ppv_n else math.nan
    npv = tn / npv_n if npv_n else math.nan

    return {
        "total": total,
        "po": po,
        "pabak": pabak,
        "sensitivity": sens,
        "specificity": spec,
        "ppv": ppv,
        "npv": npv,
        "sens_ci": _prop_ci(tp, sens_n),
        "spec_ci": _prop_ci(tn, spec_n),
        "ppv_ci": _prop_ci(tp, ppv_n),
        "npv_ci": _prop_ci(tn, npv_n),
    }


def _write_discrepancies(df: pd.DataFrame, suffix: str | None = None) -> None:
    """Save the AI vs human disagreements for manual review."""

    discrep = df[df["ai_decision"] != df["human_decision"]].copy()
    if discrep.empty:
        empty_path = _stage_file("discrepancy_log.csv", suffix)
        if empty_path.exists():
            empty_path.unlink()
        return

    metadata_cols: list[str] = []
    for variants in [("Title", "title"), ("Abstract", "abstract"), ("Authors", "authors"), ("Year", "year")]:
        for candidate in variants:
            if candidate in discrep.columns:
                metadata_cols.append(candidate)
                break

    discrep_out = discrep.copy()
    discrep_out.rename(columns={"covidence_id": "ID"}, inplace=True)
    discrep_out["Human_Tag"] = discrep_out.get("human_reason", "")
    discrep_out["AI_Reason"] = discrep_out.get("ai_reason", "")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_cols = ["ID", "Human_Tag", "AI_Reason"] + metadata_cols
    discrep_out[[c for c in output_cols if c in discrep_out.columns]].to_csv(
        _stage_file("discrepancy_log.csv", suffix), index=False, encoding="utf-8"
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
    if name.startswith(prefix):
        return name.replace(prefix, "")
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

    _write_discrepancies(merged, suffix)
    _write_report(stats, tp, tn, fp, fn, stage, suffix)
    _plot_confusion(tp, tn, fp, fn, suffix)

    print(f"Validation complete (screening stage: {stage}). Outputs:")
    print(f"- {_stage_file('validation_stats_report.txt', suffix).relative_to(ROOT)}")
    print(f"- {_stage_file('discrepancy_log.csv', suffix).relative_to(ROOT)}")
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

    total_matches = int(merged_eval["match"].sum())
    total_items = int(len(merged_eval))
    overall_accuracy = (total_matches / total_items) if total_items else math.nan

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("Extraction accuracy report (AI vs adjudicated consensus)")
    lines.append(f"Total items evaluated: {total_items}")
    lines.append(
        f"Overall accuracy: {overall_accuracy*100:.2f}%" if not math.isnan(overall_accuracy) else "Overall accuracy: n/a"
    )
    lines.append("")
    lines.append("Per-field accuracy:")
    for _, row in per_field.iterrows():
        acc = row["accuracy"]
        acc_str = f"{acc*100:.2f}%" if not math.isnan(acc) else "n/a"
        lines.append(f"- {row['field']}: {acc_str} (n={row['total']}, matches={row['matches']})")

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
    discrep_out_cols = ["PaperID", "Field_Name", "AI_Value", "Human_Consensus_Value"]
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
