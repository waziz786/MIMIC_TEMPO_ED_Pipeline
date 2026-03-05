"""
Part 2 — Option A: Minimal Modeling Validation (IEEE-ready)
============================================================
Produces the main benchmark results for the manuscript.

Design:
  • Outcomes:  deterioration_24h, icu_24h   (+ death_24h optional)
  • Cohorts:   All-ED (composite only), Admitted-only (all outcomes)
  • Windows:   W1, W6, W24  (clinical only)
  • ECG:       W6 vs W6+ECG  — cardiac outcomes ONLY (cardiac_arrest, ACS)
  • Models:    LR (L2, C∈{0.01,0.1,1,10})  +  XGBoost (tuned)
  • Splitting: 5-fold GroupKFold by subject_id
  • Metrics:   AUROC, AUPRC, Brier score, 95% bootstrap CI

Outputs:
  artifacts/results/option_a_results.json
  artifacts/results/table_a1_main.csv      (ready for LaTeX)
  artifacts/results/table_a2_ecg_delta.csv
"""

import json, time, warnings, sys
from pathlib import Path
from collections import OrderedDict

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder, StandardScaler, OneHotEncoder
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import xgboost as xgb

warnings.filterwarnings("ignore")

# ============================================================================
# CONFIG
# ============================================================================
DATA = Path("artifacts/datasets")
OUT  = Path("artifacts/results"); OUT.mkdir(parents=True, exist_ok=True)

N_FOLDS      = 5
RANDOM_STATE  = 42
SUBSAMPLE     = 100_000         # keep runtime sane
BOOT_N        = 1000            # bootstrap iterations for 95 % CI

DROP_COLS = ['stay_id', 'hadm_id', 'ed_intime', 'ed_outtime', 'chiefcomplaint']

# LR grid
LR_C_GRID = [0.01, 0.1, 1.0, 10.0]

# XGB fixed hyper-parameters (tune only max_depth / min_child_weight)
XGB_BASE = dict(
    objective        = "binary:logistic",
    eval_metric      = "logloss",
    n_estimators     = 500,
    learning_rate    = 0.05,
    subsample        = 0.8,
    colsample_bytree = 0.8,
    reg_lambda       = 1.0,
    random_state     = RANDOM_STATE,
    n_jobs           = -1,
    verbosity        = 0,
    early_stopping_rounds = 50,
)
XGB_GRID = [
    {"max_depth": 3, "min_child_weight": 1},
    {"max_depth": 3, "min_child_weight": 5},
    {"max_depth": 5, "min_child_weight": 1},
    {"max_depth": 5, "min_child_weight": 5},
]

ts = lambda: time.strftime("[%H:%M:%S]")


# ============================================================================
# DATA HELPERS
# ============================================================================
def load_csv(fname, subsample=None):
    """Load dataset, return DataFrame (with subject_id kept for grouping).
    
    Args:
        fname: CSV filename
        subsample: Optional target sample size (for full dataset, not after filtering)
    """
    p = DATA / fname
    if not p.exists():
        print(f"  ✗ {fname} not found — skipping"); return None
    df = pd.read_csv(p)
    print(f"  Loaded {len(df):,} rows")
    return df


def prepare(df):
    """
    Returns (X, y, groups, feature_names).
    groups = subject_id array for GroupKFold.
    X is raw (encoders fitted inside CV folds to prevent leakage).
    """
    outcome_cols = [c for c in df.columns if c == 'y' or c.startswith('y_')]
    groups = df['subject_id'].values  # keep for GroupKFold

    drop = [c for c in DROP_COLS + ['subject_id'] if c in df.columns]
    feat = df.drop(columns=drop + outcome_cols)

    # Don't fit encoders here — do it inside CV folds
    y = df['y'].values.astype(int) if 'y' in outcome_cols else df[outcome_cols[0]].values.astype(int)
    return feat.values, y, groups, list(feat.columns), feat


def filter_admitted(df):
    """Return admitted-only rows."""
    return df[df['hadm_id'].notna()].copy()


# ============================================================================
# BOOTSTRAP CI (CLUSTER BOOTSTRAP)
# ============================================================================
def cluster_bootstrap_ci(y_true, y_prob, groups, metric_fn, n_boot=BOOT_N, alpha=0.05):
    """Cluster bootstrap at subject_id level for CI on pooled OOF predictions.
    
    This is the correct approach for grouped (patient-level) data.
    """
    rng = np.random.RandomState(RANDOM_STATE)
    point = metric_fn(y_true, y_prob)
    scores = []
    
    unique_groups = np.unique(groups)
    for _ in range(n_boot):
        # Bootstrap subjects (with replacement)
        boot_groups = rng.choice(unique_groups, len(unique_groups), replace=True)
        boot_idx = np.concatenate([np.where(groups == g)[0] for g in boot_groups])
        try:
            scores.append(metric_fn(y_true[boot_idx], y_prob[boot_idx]))
        except Exception:
            continue
    
    lo = np.percentile(scores, 100 * alpha / 2)
    hi = np.percentile(scores, 100 * (1 - alpha / 2))
    return round(point, 4), round(lo, 4), round(hi, 4)


# ============================================================================
# MODEL TRAINING
# ============================================================================
def _best_lr(Xtr, ytr, Xval, yval):
    """Tune C on validation AUROC, return best model."""
    best_c, best_auc = 0.1, -1
    for c in LR_C_GRID:
        m = LogisticRegression(C=c, penalty='l2', solver='lbfgs',
                               max_iter=1000, class_weight='balanced',
                               random_state=RANDOM_STATE, n_jobs=-1)
        m.fit(Xtr, ytr)
        auc = roc_auc_score(yval, m.predict_proba(Xval)[:, 1])
        if auc > best_auc:
            best_auc, best_c = auc, c
    m = LogisticRegression(C=best_c, penalty='l2', solver='lbfgs',
                           max_iter=1000, class_weight='balanced',
                           random_state=RANDOM_STATE, n_jobs=-1)
    m.fit(Xtr, ytr)
    return m, best_c


def _best_xgb(Xtr, ytr, Xval, yval):
    """Tune max_depth/min_child_weight on validation logloss with early stop."""
    best_params, best_auc = {}, -1
    for hp in XGB_GRID:
        params = {**XGB_BASE, **hp}
        m = xgb.XGBClassifier(**params)
        m.fit(Xtr, ytr, eval_set=[(Xval, yval)], verbose=False)
        auc = roc_auc_score(yval, m.predict_proba(Xval)[:, 1])
        if auc > best_auc:
            best_auc, best_params = auc, hp
    params = {**XGB_BASE, **best_params}
    m = xgb.XGBClassifier(**params)
    m.fit(Xtr, ytr, eval_set=[(Xval, yval)], verbose=False)
    return m, best_params


def evaluate_fold(X_df, y, groups, fold_idx, train_idx, test_idx, model_name, feature_names):
    """
    Within the train split, hold out 20% for validation (LR tuning / XGB early-stop) 
    using GroupKFold to keep subjects disjoint.
    Fit LabelEncoders inside to prevent leakage.
    Returns dict of metrics or None.
    """
    # Extract train and test splits (as DataFrames for cleaner preprocessing)
    Xtr_all_df = X_df.iloc[train_idx].copy()
    ytr_all = y[train_idx]
    groups_tr_all = groups[train_idx]
    
    Xte_df = X_df.iloc[test_idx].copy()
    yte = y[test_idx]
    groups_te = groups[test_idx]

    if ytr_all.sum() < 10 or yte.sum() < 5:
        return None

    # Internal validation split using GroupKFold on training subjects
    gkf = GroupKFold(n_splits=3)  # 66.7-33.3 train-val split by group (approx 70-30)
    inner_splits = list(gkf.split(Xtr_all_df, ytr_all, groups_tr_all))
    if not inner_splits:
        return None
    
    inner_tr_idx, inner_val_idx = inner_splits[0]
    
    Xtr_df = Xtr_all_df.iloc[inner_tr_idx].copy()
    ytr = ytr_all[inner_tr_idx]
    
    Xval_df = Xtr_all_df.iloc[inner_val_idx].copy()
    yval = ytr_all[inner_val_idx]

    # Fit LabelEncoders ONLY on training data, with unknown value handling
    encoders = {}
    for col in Xtr_df.select_dtypes(include=['object']).columns:
        Xtr_df[col] = Xtr_df[col].fillna('_missing_')
        le = LabelEncoder()
        Xtr_df[col] = le.fit_transform(Xtr_df[col].astype(str))
        encoders[col] = le
        
        # Apply to val and test using the same encoder, mapping unknown to -1 or first class
        for df_set in [Xval_df, Xte_df]:
            df_set[col] = df_set[col].fillna('_missing_')
            # Map each value; if unseen, map to first class
            df_set[col] = df_set[col].astype(str).map(
                lambda x: le.transform([x])[0] if x in le.classes_ else 0
            )

    # Impute missing values (median from training set only)
    for col in Xtr_df.columns:
        if Xtr_df[col].isna().any():
            med = Xtr_df[col].median()
            med = med if pd.notna(med) else 0.0
            Xtr_df[col] = Xtr_df[col].fillna(med)
            Xval_df[col] = Xval_df[col].fillna(med)
            Xte_df[col] = Xte_df[col].fillna(med)

    # Scale (using train stats only) - for LR only
    scaler = StandardScaler()
    
    if model_name == "LR":
        Xtr = scaler.fit_transform(Xtr_df)
        Xval = scaler.transform(Xval_df)
        Xte = scaler.transform(Xte_df)
        model, chosen = _best_lr(Xtr, ytr, Xval, yval)
    else:
        # XGB doesn't need scaling; use raw data
        Xtr = Xtr_df.values.astype(np.float64)
        Xval = Xval_df.values.astype(np.float64)
        Xte = Xte_df.values.astype(np.float64)
        model, chosen = _best_xgb(Xtr, ytr, Xval, yval)

    yprob = model.predict_proba(Xte)[:, 1]

    # Use cluster bootstrap on test fold (grouped by subject_id)
    auroc_pt, auroc_lo, auroc_hi = cluster_bootstrap_ci(yte, yprob, groups_te, roc_auc_score)
    auprc_pt, auprc_lo, auprc_hi = cluster_bootstrap_ci(yte, yprob, groups_te, average_precision_score)
    brier = round(brier_score_loss(yte, yprob), 4)

    return {
        "auroc": auroc_pt, "auroc_ci": [auroc_lo, auroc_hi],
        "auprc": auprc_pt, "auprc_ci": [auprc_lo, auprc_hi],
        "brier": brier,
        "n_test": int(len(yte)), "n_pos_test": int(yte.sum()),
        "best_hp": str(chosen),
    }


def run_experiment(X, y, groups, label, feature_names):
    """5-fold GroupKFold with LR + XGB. Returns aggregated results."""
    gkf = GroupKFold(n_splits=N_FOLDS)
    results = {}

    for model_name in ["LR", "XGB"]:
        t0 = time.time()
        fold_results = []
        print(f"    ▸ {model_name} ({label})")

        for fi, (tr_idx, te_idx) in enumerate(gkf.split(X, y, groups), 1):
            r = evaluate_fold(X, y, groups, fi, tr_idx, te_idx, model_name, feature_names)
            if r is None:
                print(f"      Fold {fi}: skipped (insufficient positives)")
                continue
            fold_results.append(r)
            print(f"      Fold {fi}/{N_FOLDS}: AUROC={r['auroc']:.4f} [{r['auroc_ci'][0]:.4f}–{r['auroc_ci'][1]:.4f}]  "
                  f"AUPRC={r['auprc']:.4f}  Brier={r['brier']:.4f}")

        elapsed = time.time() - t0

        if not fold_results:
            results[model_name] = {"auroc_mean": 0, "note": "no valid folds"}
            continue

        aurocs = [r["auroc"] for r in fold_results]
        auprcs = [r["auprc"] for r in fold_results]
        briers = [r["brier"] for r in fold_results]

        # Report mean ± std across folds (standard ML reporting)
        results[model_name] = {
            "auroc_mean": round(np.mean(aurocs), 4),
            "auroc_std":  round(np.std(aurocs), 4),
            "auroc_min":  round(np.min(aurocs), 4),
            "auroc_max":  round(np.max(aurocs), 4),
            "auprc_mean": round(np.mean(auprcs), 4),
            "auprc_std":  round(np.std(auprcs), 4),
            "brier_mean": round(np.mean(briers), 4),
            "n_folds":    len(fold_results),
            "elapsed_s":  round(elapsed, 1),
        }
        a = results[model_name]
        print(f"      ⇒ Mean AUROC={a['auroc_mean']:.4f}±{a['auroc_std']:.4f}  "
              f"[{a['auroc_min']:.4f}–{a['auroc_max']:.4f}]  "
              f"AUPRC={a['auprc_mean']:.4f}  Brier={a['brier_mean']:.4f}  ({elapsed:.0f}s)")

    return results


# ============================================================================
# EXPERIMENT A1: MULTI-WINDOW COMPARISON
# ============================================================================
def exp_a1_multi_window():
    """W1 vs W6 vs W24 -> deterioration_24h on BOTH cohorts."""
    print(f"\n{ts()} ==================================================================")
    print(f"{ts()} A1: Multi-Window Comparison  (deterioration_24h)")
    print(f"{ts()} ==================================================================")

    window_files = OrderedDict([
        ("W1",  "ed_w1_det24_all.csv"),
        ("W6",  "ed_w6_det24_all.csv"),
        ("W24", "ed_w24_det24_all.csv"),
    ])

    results = {}

    for cohort_label in ["all_ed"]:
        results[cohort_label] = {}
        for win, fname in window_files.items():
            print(f"\n  [{win}]")
            df = load_csv(fname)
            if df is None: continue
            # Subsample to target size (no filtering)
            if len(df) > SUBSAMPLE:
                df = df.sample(n=SUBSAMPLE, random_state=RANDOM_STATE, replace=False)
                print(f"  Subsampled to {SUBSAMPLE:,}")
            X_df, y, grp, feat, feat_df = prepare(df)
            print(f"  Final: N={len(X_df):,}, features={len(feat)}, prevalence={y.mean()*100:.2f}%")
            res = run_experiment(feat_df, y, grp, f"{win} -> det_24h", feat)
            results[cohort_label][win] = {
                "n": len(X_df), "n_features": len(feat),
                "prevalence": round(y.mean()*100, 2), **res
            }
    return results


# ============================================================================
# EXPERIMENT A2: MULTI-OUTCOME (admitted-only)
# ============================================================================
def exp_a2_multi_outcome():
    """W6 -> multiple outcomes on admitted cohort."""
    print(f"\n{ts()} ==================================================================")
    print(f"{ts()} A2: Multi-Outcome Evaluation  (W6, admitted)")
    print(f"{ts()} ==================================================================")

    outcome_files = OrderedDict([
        ("deterioration_24h", "ed_w6_det24_all.csv"),      # filter to admitted
        ("icu_24h",           "ed_w6_icu24_admitted.csv"),
        ("death_24h",         "ed_w6_death24_admitted.csv"),
        ("vent_24h",          "ed_w6_vent24_admitted.csv"),
        ("pressor_24h",       "ed_w6_pressor24_admitted.csv"),
    ])

    results = {}
    for outcome, fname in outcome_files.items():
        print(f"\n  [{outcome}]")
        df = load_csv(fname)
        if df is None: continue
        # Subsample to target size (no filtering)
        if len(df) > SUBSAMPLE:
            df = df.sample(n=SUBSAMPLE, random_state=RANDOM_STATE, replace=False)
        X_df, y, grp, feat, feat_df = prepare(df)
        prev = y.mean()*100
        print(f"  N={len(X_df):,}, features={len(feat)}, prevalence={prev:.2f}%")
        if prev < 0.05:
            print(f"  ⚠ Prevalence too low, skipping"); continue
        res = run_experiment(feat_df, y, grp, f"W6 -> {outcome}", feat)
        results[outcome] = {
            "n": len(X_df), "n_features": len(feat),
            "prevalence": round(prev, 2), **res
        }
    return results


# ============================================================================
# EXPERIMENT A3: ECG INCREMENTAL VALUE (cardiac outcomes only)
# ============================================================================
def exp_a3_ecg_cardiac():
    """W6 vs W6+ECG for cardiac_arrest_hosp and acs_hosp (cardiac only)."""
    print(f"\n{ts()} ==================================================================")
    print(f"{ts()} A3: ECG Incremental Value  (cardiac outcomes only)")
    print(f"{ts()} ==================================================================")

    comparisons = OrderedDict([
        ("cardiac_arrest_hosp", ("ed_w6_cardiac_arrest_admitted.csv",
                                  "ed_w6_cardiac_arrest_ecg_admitted.csv")),
        ("acs_hosp",            ("ed_w6_acs_admitted.csv",
                                  "ed_w6_acs_ecg_admitted.csv")),
    ])

    results = {}
    for outcome, (f_noecg, f_ecg) in comparisons.items():
        print(f"\n  [{outcome}]")

        # --- Without ECG ---
        print(f"  [Clinical only]")
        df_no = load_csv(f_noecg)
        if df_no is None: continue
        if len(df_no) > SUBSAMPLE:
            df_no = df_no.sample(n=SUBSAMPLE, random_state=RANDOM_STATE, replace=False)
        X_no_df, y_no, grp_no, feat_no, feat_no_df = prepare(df_no)
        prev = y_no.mean()*100
        print(f"  N={len(X_no_df):,}, features={len(feat_no)}, prevalence={prev:.2f}%")
        if prev < 0.05:
            print(f"  ⚠ too rare"); continue
        res_no = run_experiment(feat_no_df, y_no, grp_no, f"{outcome} (clinical)", feat_no)

        # --- With ECG ---
        print(f"\n  [Clinical + ECG]")
        df_ecg = load_csv(f_ecg)
        if df_ecg is None: continue
        if len(df_ecg) > SUBSAMPLE:
            df_ecg = df_ecg.sample(n=SUBSAMPLE, random_state=RANDOM_STATE, replace=False)
        X_ecg_df, y_ecg, grp_ecg, feat_ecg, feat_ecg_df = prepare(df_ecg)
        print(f"  N={len(X_ecg_df):,}, features={len(feat_ecg)}")
        res_ecg = run_experiment(feat_ecg_df, y_ecg, grp_ecg, f"{outcome} (clinical+ECG)", feat_ecg)

        # Delta
        for mdl in ["LR", "XGB"]:
            d = (res_ecg.get(mdl, {}).get("auroc_mean", 0) -
                 res_no.get(mdl, {}).get("auroc_mean", 0))
            print(f"  Δ AUROC ({mdl}): {d:+.4f}")

        results[outcome] = {
            "clinical":     {"n_features": len(feat_no), **res_no},
            "clinical_ecg": {"n_features": len(feat_ecg), **res_ecg},
            "delta_lr":  round(res_ecg.get("LR",{}).get("auroc_mean",0) -
                               res_no.get("LR",{}).get("auroc_mean",0), 4),
            "delta_xgb": round(res_ecg.get("XGB",{}).get("auroc_mean",0) -
                               res_no.get("XGB",{}).get("auroc_mean",0), 4),
        }
    return results


# ============================================================================
# SAVE LATEX-READY TABLES
# ============================================================================
def save_tables(all_results):
    """Produce CSV tables ready for copy-paste into LaTeX."""

    # ── Table A1: Main results (multi-window, admitted-only) ──
    rows = []
    if "A1_multi_window" in all_results:
        for cohort in ["all_ed", "admitted"]:
            data = all_results["A1_multi_window"].get(cohort, {})
            for win in ["W1", "W6", "W24"]:
                d = data.get(win, {})
                if not d: continue
                for mdl in ["LR", "XGB"]:
                    m = d.get(mdl, {})
                    if not m or m.get("auroc_mean", 0) == 0: continue
                    ci = m.get("auroc_ci", [0, 0])
                    rows.append({
                        "Cohort": cohort,
                        "Window": win,
                        "Model": mdl,
                        "N": d.get("n"),
                        "Features": d.get("n_features"),
                        "Prevalence_%": d.get("prevalence"),
                        "AUROC": f"{m['auroc_mean']:.4f}",
                        "AUROC_95CI": f"[{ci[0]:.4f}–{ci[1]:.4f}]",
                        "AUPRC": f"{m.get('auprc_mean',0):.4f}",
                        "Brier": f"{m.get('brier_mean',0):.4f}",
                        "Outcome": "deterioration_24h",
                    })

    if "A2_multi_outcome" in all_results:
        for outcome, d in all_results["A2_multi_outcome"].items():
            for mdl in ["LR", "XGB"]:
                m = d.get(mdl, {})
                if not m or m.get("auroc_mean", 0) == 0: continue
                ci = m.get("auroc_ci", [0, 0])
                rows.append({
                    "Cohort": "admitted",
                    "Window": "W6",
                    "Outcome": outcome,
                    "Model": mdl,
                    "N": d.get("n"),
                    "Features": d.get("n_features"),
                    "Prevalence_%": d.get("prevalence"),
                    "AUROC": f"{m['auroc_mean']:.4f}",
                    "AUROC_95CI": f"[{ci[0]:.4f}–{ci[1]:.4f}]",
                    "AUPRC": f"{m.get('auprc_mean',0):.4f}",
                    "Brier": f"{m.get('brier_mean',0):.4f}",
                })

    tbl = pd.DataFrame(rows)
    tbl.to_csv(OUT / "table_a1_main.csv", index=False)
    print(f"OK table_a1_main.csv  ({len(tbl)} rows)")

    # ── Table A2: ECG incremental ──
    rows2 = []
    if "A3_ecg_cardiac" in all_results:
        for outcome, d in all_results["A3_ecg_cardiac"].items():
            for mdl in ["LR", "XGB"]:
                clin = d.get("clinical", {}).get(mdl, {})
                ecg  = d.get("clinical_ecg", {}).get(mdl, {})
                if not clin or not ecg: continue
                delta = ecg.get("auroc_mean", 0) - clin.get("auroc_mean", 0)
                rows2.append({
                    "Outcome": outcome,
                    "Model":   mdl,
                    "AUROC_clinical":     f"{clin.get('auroc_mean',0):.4f}",
                    "AUROC_clinical_ECG": f"{ecg.get('auroc_mean',0):.4f}",
                    "Delta_AUROC":        f"{delta:+.4f}",
                    "AUPRC_clinical":     f"{clin.get('auprc_mean',0):.4f}",
                    "AUPRC_clinical_ECG": f"{ecg.get('auprc_mean',0):.4f}",
                })
    tbl2 = pd.DataFrame(rows2)
    tbl2.to_csv(OUT / "table_a2_ecg_delta.csv", index=False)
    print(f"OK table_a2_ecg_delta.csv  ({len(tbl2)} rows)")


# ============================================================================
# MAIN
# ============================================================================
def main():
    t0 = time.time()
    print(f"{ts()} ==================================================================")
    print(f"{ts()} OPTION A -- MINIMAL MODELING VALIDATION (IEEE-READY)")
    print(f"{ts()} GroupKFold . Cluster Bootstrap CI . LR (L2 tuned) + XGBoost")
    print(f"{ts()} ==================================================================")

    all_results = {}

    # A1
    # all_results["A1_multi_window"] = exp_a1_multi_window()

    # A2
    # all_results["A2_multi_outcome"] = exp_a2_multi_outcome()

    # A3
    all_results["A3_ecg_cardiac"] = exp_a3_ecg_cardiac()

    # Save JSON
    with open(OUT / "option_a_results.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nOK option_a_results.json saved")

    # Save tables
    save_tables(all_results)

    elapsed = (time.time() - t0) / 60
    print(f"\n{ts()} ═══ OPTION A COMPLETE — {elapsed:.1f} minutes ═══")


if __name__ == "__main__":
    main()

