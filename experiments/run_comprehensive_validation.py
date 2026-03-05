"""
Comprehensive Empirical Validation Experiments for Manuscript
=============================================================
Six experiment blocks:
  1. Cohort Characterization & Descriptive Statistics
  2. Multi-Window Baseline Comparison (W1/W6/W24) — same outcome
  3. Multi-Outcome Evaluation (ICU, Deterioration, Mortality, Cardiac)
  4. Temporal Leakage Demonstration
  5. ECG Added-Value Analysis
  6. Feature Ablation Study

Datasets are constructed from raw CSVs to ensure each experiment
uses an appropriate, consistently-defined analytic cohort.
"""

import pandas as pd
import numpy as np
import json
import os
import time
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.metrics import (roc_auc_score, average_precision_score,
                             f1_score, precision_score, recall_score)
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.impute import SimpleImputer
import warnings
warnings.filterwarnings('ignore')

# ── paths ─────────────────────────────────────────────────────
DATA_DIR  = r"c:\Users\Lab\Desktop\MIMICc_Deterioration_Pipeline - Copy\artifacts\datasets"
OUT_DIR   = r"c:\Users\Lab\Desktop\MIMICc_Deterioration_Pipeline - Copy\experiments\results"
os.makedirs(OUT_DIR, exist_ok=True)

MAX_ROWS  = 75000
N_SPLITS  = 5

# ── logging ───────────────────────────────────────────────────
def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ── helpers ───────────────────────────────────────────────────
def subsample(df, y_col='y', max_rows=MAX_ROWS, seed=42):
    if len(df) <= max_rows:
        return df
    log(f"  ↓ Subsampling {len(df):,} → {max_rows:,} (stratified on {y_col})")
    _, sub = train_test_split(df, test_size=max_rows, stratify=df[y_col],
                              random_state=seed)
    return sub.reset_index(drop=True)


def encode_categoricals(df):
    """Label-encode object columns in place (returns same df)."""
    for col in df.select_dtypes(include='object').columns:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
    return df


ID_COLS = ['stay_id', 'subject_id', 'hadm_id',
           'ed_intime', 'ed_outtime', 'chiefcomplaint',
           'ecg_time_w1', 'ecg_time_w6',
           'ecg_study_id_w1', 'ecg_study_id_w6',
           'ecg_hours_from_ed_w1', 'ecg_hours_from_ed_w6']

def to_Xy(df, y_col='y', drop_cols=None):
    """Return X (ndarray), y (ndarray), feature_names (list)."""
    if drop_cols is None:
        drop_cols = []
    all_drop = set(ID_COLS + drop_cols)
    # also drop any column starting with 'y' except the target
    y_cols = [c for c in df.columns if c.startswith('y') and c != y_col]
    all_drop.update(y_cols)
    keep = [c for c in df.columns if c not in all_drop and c != y_col]
    feat_df = df[keep].copy()
    feat_df = encode_categoricals(feat_df)
    return feat_df.values, df[y_col].values, list(feat_df.columns)


def run_cv(X, y, model_name='LR', n_splits=N_SPLITS, seed=42):
    """5-fold stratified CV; returns dict of metric arrays."""
    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=seed)
    metrics = {k: [] for k in
               ['auroc', 'auprc', 'f1', 'precision', 'recall', 'specificity']}
    for fi, (tr, te) in enumerate(skf.split(X, y), 1):
        t0 = time.time()
        Xtr, Xte = X[tr], X[te]
        ytr, yte = y[tr], y[te]
        imp = SimpleImputer(strategy='median')
        Xtr = imp.fit_transform(Xtr); Xte = imp.transform(Xte)
        sc  = StandardScaler()
        Xtr = sc.fit_transform(Xtr);  Xte = sc.transform(Xte)
        if model_name == 'LR':
            m = LogisticRegression(max_iter=1000, solver='lbfgs',
                                   random_state=seed, class_weight='balanced')
        else:
            m = GradientBoostingClassifier(n_estimators=100, max_depth=4,
                                            learning_rate=0.1, subsample=0.8,
                                            random_state=seed)
        m.fit(Xtr, ytr)
        yp = m.predict_proba(Xte)[:, 1]
        yh = (yp >= 0.5).astype(int)
        metrics['auroc'].append(roc_auc_score(yte, yp))
        metrics['auprc'].append(average_precision_score(yte, yp))
        metrics['f1'].append(f1_score(yte, yh, zero_division=0))
        metrics['precision'].append(precision_score(yte, yh, zero_division=0))
        metrics['recall'].append(recall_score(yte, yh, zero_division=0))
        tn = ((yh == 0) & (yte == 0)).sum()
        fp = ((yh == 1) & (yte == 0)).sum()
        metrics['specificity'].append(tn / max(tn + fp, 1))
        dt = time.time() - t0
        log(f"    Fold {fi}/{n_splits}: AUROC={metrics['auroc'][-1]:.4f} ({dt:.1f}s)")
    summary = {}
    for k, v in metrics.items():
        summary[f'{k}_mean'] = float(np.mean(v))
        summary[f'{k}_std']  = float(np.std(v))
    return summary

# ── load raw data once ────────────────────────────────────────
log("Loading datasets …")
t_load = time.time()
raw_w1      = pd.read_csv(os.path.join(DATA_DIR, "ed_w1_icu24.csv"))       # 424 952 × 25
raw_w6      = pd.read_csv(os.path.join(DATA_DIR, "ed_w6_det24.csv"))       # 424 952 × 63
raw_w24     = pd.read_csv(os.path.join(DATA_DIR, "ed_w24_det48.csv"))      # 424 952 × 68
raw_cardiac = pd.read_csv(os.path.join(DATA_DIR, "w1_w6_cardiac_with_ecg.csv"))  # 202 990 admitted
raw_mort    = pd.read_csv(os.path.join(DATA_DIR, "w6_w24_multi_mortality.csv"))   # 202 990 admitted
log(f"Loaded 5 files in {time.time()-t_load:.0f}s")

# ══════════════════════════════════════════════════════════════
# EXPERIMENT 0 — Cohort Descriptive Statistics
# ══════════════════════════════════════════════════════════════
log("\n" + "═"*65)
log("EXPERIMENT 0: Cohort Descriptive Statistics")
log("═"*65)

desc = {}
desc['total_ed_visits']    = len(raw_w1)
desc['admitted_visits']    = int(raw_w1['hadm_id'].notna().sum())
desc['admission_rate']     = float(raw_w1['hadm_id'].notna().mean())
desc['age_mean']           = float(raw_w1['age_at_ed'].mean())
desc['age_std']            = float(raw_w1['age_at_ed'].std())
desc['age_median']         = float(raw_w1['age_at_ed'].median())
desc['pct_female']         = float((raw_w1['gender'] == 'F').mean())
desc['ed_los_mean']        = float(raw_w1['ed_los_hours'].mean())
desc['ed_los_median']      = float(raw_w1['ed_los_hours'].median())

# Outcome prevalences (all patients)
desc['icu_24h_prev']       = float(raw_w1['y'].mean())  # y = icu_24h
desc['det_24h_prev']       = float(raw_w6['y'].mean())  # y = det_24h
desc['det_48h_prev']       = float(raw_w24['y'].mean()) # y = det_48h

# Outcome prevalences (admitted)
desc['death_24h_prev']     = float(raw_mort['y_death_24h'].mean())
desc['death_48h_prev']     = float(raw_mort['y_death_48h'].mean())
desc['death_hosp_prev']    = float(raw_mort['y_death_hosp'].mean())
desc['cardiac_arrest_prev']= float(raw_cardiac['y_cardiac_arrest_hosp'].mean())
desc['acs_hosp_prev']      = float(raw_cardiac['y_acs_hosp'].mean())
desc['revasc_hosp_prev']   = float(raw_cardiac['y_revasc_hosp'].mean())

# ECG coverage
desc['ecg_w1_coverage']    = float(1 - raw_cardiac['missing_ecg_w1'].mean())
desc['ecg_w6_coverage']    = float(1 - raw_cardiac['missing_ecg_w6'].mean())

# Vital sign completeness (W6, all patients)
desc['sbp_6h_complete']    = float(raw_w6['sbp_mean_6h'].notna().mean())
desc['hr_6h_complete']     = float(raw_w6['hr_mean_6h'].notna().mean())
desc['lactate_6h_complete']= float(raw_w6['lactate_first_6h'].notna().mean())
desc['troponin_6h_complete'] = float(raw_w6['troponin_first_6h'].notna().mean())
desc['creat_6h_complete']  = float(raw_w6['creatinine_first_6h'].notna().mean())

# Number of features per window
desc['n_features_w1']  = raw_w1.shape[1] - len([c for c in raw_w1.columns if c in ID_COLS or c.startswith('y')])
desc['n_features_w6']  = raw_w6.shape[1] - len([c for c in raw_w6.columns if c in ID_COLS or c.startswith('y')])
desc['n_features_w24'] = raw_w24.shape[1] - len([c for c in raw_w24.columns if c in ID_COLS or c.startswith('y')])

for k, v in desc.items():
    if isinstance(v, float) and v < 1:
        log(f"  {k}: {v:.4f}")
    else:
        log(f"  {k}: {v}")

with open(os.path.join(OUT_DIR, 'cohort_descriptives.json'), 'w') as f:
    json.dump(desc, f, indent=2)

# ══════════════════════════════════════════════════════════════
# BUILD UNIFIED EXPERIMENT DATASETS
# We need same-outcome datasets across windows for fair comparison
# ══════════════════════════════════════════════════════════════
log("\n" + "═"*65)
log("Building unified experiment datasets …")
log("═"*65)

# Dataset A: W1 + W6 + W24 features all predicting SAME outcome (det_24h)
# For W1: we need det_24h outcome. Current W1 file has icu_24h.
# Solution: merge W1 features with det_24h from W6
log("  Building unified det_24h datasets across all windows …")

# W6 has det_24h as 'y'. Extract stay_id + outcome
det24_outcome = raw_w6[['stay_id', 'y']].rename(columns={'y': 'det_24h'})

# W1 features + det_24h outcome
w1_features_only = raw_w1.drop(columns=['y'])
unified_w1 = w1_features_only.merge(det24_outcome, on='stay_id', how='inner')
unified_w1.rename(columns={'det_24h': 'y'}, inplace=True)

# W6 already has det_24h
unified_w6 = raw_w6.copy()

# W24 features + det_24h outcome (W24 currently has det_48h)
w24_features_only = raw_w24.drop(columns=['y'])
unified_w24 = w24_features_only.merge(det24_outcome, on='stay_id', how='inner')
unified_w24.rename(columns={'det_24h': 'y'}, inplace=True)

log(f"  Unified W1:  {unified_w1.shape}")
log(f"  Unified W6:  {unified_w6.shape}")
log(f"  Unified W24: {unified_w24.shape}")
log(f"  det_24h prevalence check — W1:{unified_w1['y'].mean():.4f}  W6:{unified_w6['y'].mean():.4f}  W24:{unified_w24['y'].mean():.4f}")

# Dataset B: Admitted-only with multiple outcomes for multi-outcome eval
# Use W6 features with multiple outcome columns from mortality dataset
w6_cols_common = [c for c in raw_w6.columns if c != 'y']
admitted_mask = raw_w6['hadm_id'].notna()
w6_admitted = raw_w6[admitted_mask].copy()
# Merge mortality outcomes
mort_outcomes = raw_mort[['stay_id', 'y_death_24h', 'y_death_48h', 'y_death_hosp']].copy()
w6_admitted_multi = w6_admitted.merge(mort_outcomes, on='stay_id', how='inner')
# Merge cardiac outcomes
cardiac_outcomes = raw_cardiac[['stay_id', 'y_acs_hosp', 'y_cardiac_arrest_hosp', 'y_revasc_hosp']].copy()
w6_admitted_multi = w6_admitted_multi.merge(cardiac_outcomes, on='stay_id', how='inner')
log(f"  Admitted multi-outcome dataset: {w6_admitted_multi.shape}")

# Dataset C: ECG comparison — admitted patients, W6 clinical + optional ECG
# Extract W6 clinical features from cardiac dataset
ecg_feature_cols = [c for c in raw_cardiac.columns if 'ecg' in c.lower()]
clinical_cols = [c for c in raw_cardiac.columns if c not in ecg_feature_cols]
log(f"  ECG dataset: {raw_cardiac.shape}, {len(ecg_feature_cols)} ECG cols, {len(clinical_cols)} clinical cols")

# ══════════════════════════════════════════════════════════════
# EXPERIMENT 1 — Multi-Window Comparison (SAME outcome: det_24h)
# ══════════════════════════════════════════════════════════════
exp1_start = time.time()
log("\n" + "═"*65)
log("EXPERIMENT 1: Multi-Window Comparison (det_24h across W1/W6/W24)")
log("═"*65)

results_exp1 = {}
for label, df in [("W1", unified_w1), ("W6", unified_w6), ("W24", unified_w24)]:
    df_sub = subsample(df, y_col='y')
    X, y, fnames = to_Xy(df_sub, y_col='y')
    log(f"\n  [{label}] n={len(df_sub):,}  features={X.shape[1]}  prev={y.mean():.4f}")
    for mn in ['LR', 'XGB']:
        log(f"  ▸ {mn}")
        res = run_cv(X, y, model_name=mn)
        results_exp1[f'{label}_{mn}'] = res
        log(f"    AUROC={res['auroc_mean']:.4f}±{res['auroc_std']:.4f}  "
            f"AUPRC={res['auprc_mean']:.4f}±{res['auprc_std']:.4f}  "
            f"F1={res['f1_mean']:.4f}")

log(f"\nExp 1 done in {time.time()-exp1_start:.0f}s")

# ══════════════════════════════════════════════════════════════
# EXPERIMENT 2 — Multi-Outcome Evaluation (W6, admitted patients)
# ══════════════════════════════════════════════════════════════
exp2_start = time.time()
log("\n" + "═"*65)
log("EXPERIMENT 2: Multi-Outcome Evaluation (W6 features, admitted)")
log("═"*65)

outcomes_to_test = {
    'det_24h':            'y',           # composite deterioration
    'death_24h':          'y_death_24h',
    'death_48h':          'y_death_48h',
    'death_hosp':         'y_death_hosp',
    'acs_hosp':           'y_acs_hosp',
    'cardiac_arrest_hosp':'y_cardiac_arrest_hosp',
}

results_exp2 = {}
for outcome_label, y_col in outcomes_to_test.items():
    df_sub = subsample(w6_admitted_multi, y_col=y_col)
    # Use W6 features only (drop mortality/cardiac outcome cols from features)
    extra_drop = [c for c in df_sub.columns if c.startswith('y')]
    X, y_arr, fnames = to_Xy(df_sub, y_col=y_col, drop_cols=extra_drop)
    log(f"\n  [{outcome_label}] n={len(df_sub):,}  prev={y_arr.mean():.4f}")
    for mn in ['LR', 'XGB']:
        log(f"  ▸ {mn}")
        res = run_cv(X, y_arr, model_name=mn)
        results_exp2[f'{outcome_label}_{mn}'] = res
        log(f"    AUROC={res['auroc_mean']:.4f}±{res['auroc_std']:.4f}  "
            f"AUPRC={res['auprc_mean']:.4f}")

log(f"\nExp 2 done in {time.time()-exp2_start:.0f}s")

# ══════════════════════════════════════════════════════════════
# EXPERIMENT 3 — Temporal Leakage Demonstration
# ══════════════════════════════════════════════════════════════
exp3_start = time.time()
log("\n" + "═"*65)
log("EXPERIMENT 3: Temporal Leakage Demonstration")
log("═"*65)
log("  Hypothesis: Using W24 features to predict a 24h outcome inflates")
log("  performance because the feature window overlaps the outcome window.")

results_exp3 = {}

# Proper pipeline — W1 features → det_24h
log("\n  ── Proper (W1 → det_24h) ──")
df_proper = subsample(unified_w1, y_col='y')
X_p, y_p, _ = to_Xy(df_proper, y_col='y')
log(f"  n={len(df_proper):,}  features={X_p.shape[1]}  prev={y_p.mean():.4f}")
for mn in ['LR', 'XGB']:
    log(f"  ▸ {mn}")
    res = run_cv(X_p, y_p, model_name=mn)
    results_exp3[f'proper_W1_det24_{mn}'] = res
    log(f"    AUROC={res['auroc_mean']:.4f}")

# Proper pipeline — W6 features → det_24h
log("\n  ── Proper (W6 → det_24h) ──")
df_proper6 = subsample(unified_w6, y_col='y')
X_p6, y_p6, _ = to_Xy(df_proper6, y_col='y')
log(f"  n={len(df_proper6):,}  features={X_p6.shape[1]}  prev={y_p6.mean():.4f}")
for mn in ['LR', 'XGB']:
    log(f"  ▸ {mn}")
    res = run_cv(X_p6, y_p6, model_name=mn)
    results_exp3[f'proper_W6_det24_{mn}'] = res
    log(f"    AUROC={res['auroc_mean']:.4f}")

# LEAKY — W24 features → det_24h  (24h features overlap 24h outcome!)
log("\n  ── LEAKY (W24 → det_24h — temporal overlap!) ──")
df_leaky = subsample(unified_w24, y_col='y')
X_l, y_l, _ = to_Xy(df_leaky, y_col='y')
log(f"  n={len(df_leaky):,}  features={X_l.shape[1]}  prev={y_l.mean():.4f}")
for mn in ['LR', 'XGB']:
    log(f"  ▸ {mn}")
    res = run_cv(X_l, y_l, model_name=mn)
    results_exp3[f'leaky_W24_det24_{mn}'] = res
    log(f"    AUROC={res['auroc_mean']:.4f}")

# Summarise inflation
log("\n  ── Leakage Inflation Summary ──")
for mn in ['LR', 'XGB']:
    a_w1 = results_exp3[f'proper_W1_det24_{mn}']['auroc_mean']
    a_w6 = results_exp3[f'proper_W6_det24_{mn}']['auroc_mean']
    a_lk = results_exp3[f'leaky_W24_det24_{mn}']['auroc_mean']
    log(f"  {mn}:  W1={a_w1:.4f}  W6={a_w6:.4f}  Leaky_W24={a_lk:.4f}  "
        f"Inflation vs W1: {a_lk-a_w1:+.4f} ({(a_lk-a_w1)/a_w1*100:+.1f}%)  "
        f"Inflation vs W6: {a_lk-a_w6:+.4f} ({(a_lk-a_w6)/a_w6*100:+.1f}%)")

log(f"\nExp 3 done in {time.time()-exp3_start:.0f}s")

# ══════════════════════════════════════════════════════════════
# EXPERIMENT 4 — ECG Added-Value Analysis
# ══════════════════════════════════════════════════════════════
exp4_start = time.time()
log("\n" + "═"*65)
log("EXPERIMENT 4: ECG Added-Value Analysis (admitted patients)")
log("═"*65)

ecg_outcomes = {
    'cardiac_arrest_hosp': 'y_cardiac_arrest_hosp',
    'acs_hosp':            'y_acs_hosp',
    'revasc_hosp':         'y_revasc_hosp',
}

results_exp4 = {}
for olabel, ycol in ecg_outcomes.items():
    df_sub = subsample(raw_cardiac, y_col=ycol)
    
    # WITHOUT ECG
    ecg_drop = [c for c in df_sub.columns if 'ecg' in c.lower()]
    extra_drop_no  = ecg_drop + [c for c in df_sub.columns if c.startswith('y') and c != ycol]
    X_no, y_no, _ = to_Xy(df_sub, y_col=ycol, drop_cols=extra_drop_no)
    
    # WITH ECG
    extra_drop_yes = [c for c in df_sub.columns if c.startswith('y') and c != ycol]
    X_yes, y_yes, _ = to_Xy(df_sub, y_col=ycol, drop_cols=extra_drop_yes)
    
    log(f"\n  [{olabel}]  n={len(df_sub):,}  prev={y_no.mean():.4f}  "
        f"feats_no_ecg={X_no.shape[1]}  feats_w_ecg={X_yes.shape[1]}")
    
    for mn in ['LR', 'XGB']:
        log(f"  ▸ {mn} without ECG")
        r_no = run_cv(X_no, y_no, model_name=mn)
        results_exp4[f'{olabel}_noECG_{mn}'] = r_no
        
        log(f"  ▸ {mn} with ECG")
        r_yes = run_cv(X_yes, y_yes, model_name=mn)
        results_exp4[f'{olabel}_wECG_{mn}'] = r_yes
        
        delta = r_yes['auroc_mean'] - r_no['auroc_mean']
        log(f"    Δ AUROC = {delta:+.4f}")

log(f"\nExp 4 done in {time.time()-exp4_start:.0f}s")

# ══════════════════════════════════════════════════════════════
# EXPERIMENT 5 — Feature Ablation (W6 → det_24h, all patients)
# ══════════════════════════════════════════════════════════════
exp5_start = time.time()
log("\n" + "═"*65)
log("EXPERIMENT 5: Feature Ablation Study (W6 → det_24h)")
log("═"*65)

df_abl = subsample(unified_w6, y_col='y')

# Define feature groups
demo_cols  = ['age_at_ed', 'gender', 'arrival_transport', 'race']
vital_cols = [c for c in df_abl.columns if any(x in c for x in
              ['sbp_', 'hr_', 'rr_', 'spo2_', 'temp_', 'n_vitalsign'])
              and 'missing' not in c]
lab_cols   = [c for c in df_abl.columns if any(x in c for x in
              ['lactate', 'troponin', 'creatinine', 'potassium', 'sodium',
               'bicarbonate', 'wbc', 'hemoglobin', 'platelet', 'is_hs'])
              and 'missing' not in c]
process_cols = [c for c in df_abl.columns if any(x in c for x in
                ['ed_los', 'time_to_first', 'prev_admits', 'prev_ed_visits'])]
missing_cols = [c for c in df_abl.columns if 'missing' in c]

feature_groups = {
    'Demographics only':      demo_cols,
    'Demo + Vitals':          demo_cols + vital_cols,
    'Demo + Vitals + Labs':   demo_cols + vital_cols + lab_cols,
    'Demo + Vitals + Labs + Process': demo_cols + vital_cols + lab_cols + process_cols,
    'All features (full W6)': demo_cols + vital_cols + lab_cols + process_cols + missing_cols,
}

results_exp5 = {}
for group_name, cols in feature_groups.items():
    avail = [c for c in cols if c in df_abl.columns]
    feat_df = df_abl[avail].copy()
    feat_df = encode_categoricals(feat_df)
    X_a = feat_df.values
    y_a = df_abl['y'].values
    log(f"\n  [{group_name}]  features={X_a.shape[1]}")
    for mn in ['XGB']:  # XGB only for ablation (more discriminating)
        log(f"  ▸ {mn}")
        res = run_cv(X_a, y_a, model_name=mn)
        results_exp5[f'{group_name}_{mn}'] = res
        log(f"    AUROC={res['auroc_mean']:.4f}  AUPRC={res['auprc_mean']:.4f}")

log(f"\nExp 5 done in {time.time()-exp5_start:.0f}s")

# ══════════════════════════════════════════════════════════════
# SAVE ALL RESULTS
# ══════════════════════════════════════════════════════════════
all_results = {
    'cohort_descriptives':     desc,
    'exp1_window_comparison':  results_exp1,
    'exp2_multi_outcome':      results_exp2,
    'exp3_leakage_demo':       results_exp3,
    'exp4_ecg_added_value':    results_exp4,
    'exp5_feature_ablation':   results_exp5,
}

out_path = os.path.join(OUT_DIR, 'comprehensive_results.json')
with open(out_path, 'w') as f:
    json.dump(all_results, f, indent=2)

total = time.time() - t_load
log(f"\n{'═'*65}")
log(f"ALL EXPERIMENTS COMPLETE — {total:.0f}s ({total/60:.1f} min)")
log(f"Results → {out_path}")
log(f"{'═'*65}")
