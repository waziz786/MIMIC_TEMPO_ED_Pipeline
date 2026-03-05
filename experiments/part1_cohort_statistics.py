"""
Part 1 — Cohort Statistics (Table S1)
======================================
Produces the cohort characteristics table for the manuscript.

Outputs:
  - Console: Formatted table
  - artifacts/results/table_s1_cohort.json
  - artifacts/results/table_s1_cohort.csv
"""

import json, warnings, sys
from pathlib import Path
import numpy as np, pandas as pd

warnings.filterwarnings("ignore")

DATA = Path("artifacts/datasets")
OUT  = Path("artifacts/results"); OUT.mkdir(parents=True, exist_ok=True)


def iqr_str(series):
    """Return 'median [Q1–Q3]' string."""
    q = series.dropna().quantile([0.25, 0.5, 0.75])
    return f"{q[0.5]:.1f} [{q[0.25]:.1f}–{q[0.75]:.1f}]"


def pct(n, N):
    return f"{n:,} ({n/N*100:.1f}%)"


def main():
    # ------------------------------------------------------------------
    # 1.  Base cohort from the largest "all" dataset (W6, det24)
    # ------------------------------------------------------------------
    print("Loading base cohort …")
    base = pd.read_csv(DATA / "ed_w6_det24_all.csv",
                       usecols=["stay_id", "subject_id", "hadm_id",
                                "age_at_ed", "gender", "ed_los_hours",
                                "arrival_transport", "race", "y"])
    N = len(base)
    n_patients = base["subject_id"].nunique()
    n_admitted = base["hadm_id"].notna().sum()
    n_female   = (base["gender"] == "F").sum()

    print(f"  Total ED visits:   {N:,}")
    print(f"  Unique patients:   {n_patients:,}")
    print(f"  Admitted:          {pct(n_admitted, N)}")

    stats = {
        "cohort": {
            "n_ed_visits":      int(N),
            "n_unique_patients": int(n_patients),
            "median_age_iqr":   iqr_str(base["age_at_ed"]),
            "pct_female":       round(n_female / N * 100, 1),
            "pct_admitted":     round(n_admitted / N * 100, 1),
            "median_ed_los_h":  iqr_str(base["ed_los_hours"]),
        }
    }

    # ------------------------------------------------------------------
    # 2.  Outcome prevalence
    # ------------------------------------------------------------------
    print("\nOutcome prevalence:")
    outcome_files = {
        "deterioration_24h (all)":      ("ed_w6_det24_all.csv",            "all"),
        "icu_24h (admitted)":           ("ed_w6_icu24_admitted.csv",       "admitted"),
        "death_24h (admitted)":         ("ed_w6_death24_admitted.csv",     "admitted"),
        "vent_24h (admitted)":          ("ed_w6_vent24_admitted.csv",      "admitted"),
        "pressor_24h (admitted)":       ("ed_w6_pressor24_admitted.csv",   "admitted"),
        "cardiac_arrest_hosp (admitted)": ("ed_w6_cardiac_arrest_admitted.csv", "admitted"),
        "acs_hosp (admitted)":          ("ed_w6_acs_admitted.csv",         "admitted"),
    }

    stats["outcomes"] = {}
    for label, (fname, cohort) in outcome_files.items():
        p = DATA / fname
        if not p.exists():
            continue
        y = pd.read_csv(p, usecols=["y"])["y"]
        n_pos = int(y.sum())
        n_tot = len(y)
        prev  = round(n_pos / n_tot * 100, 2)
        stats["outcomes"][label] = {"n": n_tot, "n_pos": n_pos, "prevalence_pct": prev}
        print(f"  {label:40s}  {n_pos:>6,} / {n_tot:>7,}  ({prev:.2f}%)")

    # Also compute deterioration_24h admitted-only
    base_admitted = base[base["hadm_id"].notna()]
    det24_adm = base_admitted["y"]
    n_pos_det = int(det24_adm.sum())
    n_tot_det = len(det24_adm)
    stats["outcomes"]["deterioration_24h (admitted)"] = {
        "n": n_tot_det, "n_pos": n_pos_det,
        "prevalence_pct": round(n_pos_det / n_tot_det * 100, 2)
    }
    print(f"  {'deterioration_24h (admitted)':40s}  {n_pos_det:>6,} / {n_tot_det:>7,}  ({n_pos_det/n_tot_det*100:.2f}%)")

    # ------------------------------------------------------------------
    # 3.  ECG coverage
    # ------------------------------------------------------------------
    print("\nECG coverage:")
    ecg_file = DATA / "ed_w6_cardiac_arrest_ecg_admitted.csv"
    if ecg_file.exists():
        ecg = pd.read_csv(ecg_file,
                          usecols=["ecg_hours_from_ed_w6", "missing_ecg_w6"])
        n_ecg   = len(ecg)
        has_ecg  = (ecg["missing_ecg_w6"] == 0).sum() if "missing_ecg_w6" in ecg.columns else ecg["ecg_hours_from_ed_w6"].notna().sum()
        within_1 = (ecg["ecg_hours_from_ed_w6"].fillna(999) <= 1).sum()
        within_6 = (ecg["ecg_hours_from_ed_w6"].fillna(999) <= 6).sum()

        stats["ecg_coverage"] = {
            "cohort_n": n_ecg,
            "has_ecg":   pct(int(has_ecg), n_ecg),
            "within_1h": pct(int(within_1), n_ecg),
            "within_6h": pct(int(within_6), n_ecg),
        }
        print(f"  ECG available (admitted):  {pct(int(has_ecg), n_ecg)}")
        print(f"  ECG within 1 h:            {pct(int(within_1), n_ecg)}")
        print(f"  ECG within 6 h:            {pct(int(within_6), n_ecg)}")

    # ------------------------------------------------------------------
    # 4.  Feature availability (missingness within 6 h)
    # ------------------------------------------------------------------
    print("\nFeature availability (W6, all ED):")
    w6 = pd.read_csv(DATA / "ed_w6_det24_all.csv",
                     usecols=["lactate_first_6h", "troponin_first_6h",
                              "wbc_first_6h", "creatinine_first_6h",
                              "hemoglobin_first_6h", "platelet_first_6h",
                              "sbp_min_6h", "hr_min_6h"])
    stats["feature_availability_6h"] = {}
    for col in w6.columns:
        avail = w6[col].notna().mean() * 100
        stats["feature_availability_6h"][col] = round(avail, 1)
        print(f"  {col:30s}  {avail:5.1f}% available")

    # ------------------------------------------------------------------
    # 5.  Race / arrival transport distribution
    # ------------------------------------------------------------------
    print("\nRace distribution:")
    race_cts = base["race"].fillna("Unknown").value_counts()
    stats["race"] = {str(k): int(v) for k, v in race_cts.items()}
    for r, c in race_cts.head(8).items():
        print(f"  {r:35s}  {c:>7,}  ({c/N*100:.1f}%)")

    print("\nArrival transport:")
    at = base["arrival_transport"].fillna("Unknown").value_counts()
    stats["arrival_transport"] = {str(k): int(v) for k, v in at.items()}
    for r, c in at.head(6).items():
        print(f"  {r:35s}  {c:>7,}  ({c/N*100:.1f}%)")

    # ------------------------------------------------------------------
    # 6.  Save
    # ------------------------------------------------------------------
    with open(OUT / "table_s1_cohort.json", "w") as f:
        json.dump(stats, f, indent=2, default=str)
    print(f"\nOK Saved -> {OUT / 'table_s1_cohort.json'}")

    # ── Formatted CSV for easy copy-paste into LaTeX ──
    rows = [
        ("N ED visits",               f"{N:,}"),
        ("Unique patients",           f"{n_patients:,}"),
        ("Age, median [IQR]",         iqr_str(base["age_at_ed"])),
        ("Female, %",                 f"{n_female/N*100:.1f}%"),
        ("Admitted, %",               f"{n_admitted/N*100:.1f}%"),
        ("ED LOS (h), median [IQR]",  iqr_str(base["ed_los_hours"])),
    ]
    for label, info in stats["outcomes"].items():
        rows.append((label, f"{info['n_pos']:,} ({info['prevalence_pct']:.2f}%)"))
    for col, avail in stats["feature_availability_6h"].items():
        rows.append((f"{col} available", f"{avail:.1f}%"))

    tbl = pd.DataFrame(rows, columns=["Characteristic", "Value"])
    tbl.to_csv(OUT / "table_s1_cohort.csv", index=False)
    print(f"OK Saved -> {OUT / 'table_s1_cohort.csv'}")


if __name__ == "__main__":
    main()

