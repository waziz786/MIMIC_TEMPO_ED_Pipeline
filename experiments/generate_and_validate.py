"""
Generate comprehensive datasets via pipeline and run experiments.

This script:
1. Uses the actual pipeline (materialize_dataset_advanced) to generate datasets
2. Creates multiple dataset combinations:
   - Single-outcome datasets across windows (W1, W6, W24)
   - Multi-outcome datasets (combinations of outcomes)
   - Cohort filters (all ED patients vs admitted only)
   - ECG inclusion/exclusion
3. Runs experiments on each dataset
4. Saves results for manuscript

Dataset Strategy:
- PRIMARY: W1/W6/W24 + deterioration_24h (all ED patients)
- SECONDARY: W6 + multiple outcomes (death_24h, cardiac_arrest, ACS, ventilation, ICU)
- CARDIAC: W6 + cardiac outcomes with ECG features
- COMPARATIVE: Same outcome with/without ECG
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, precision_score, recall_score

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import load_yaml, setup_logging
from src.db import get_conn, check_connection
from src.materialize_datasets import materialize_dataset, materialize_dataset_advanced

logger = logging.getLogger(__name__)

# ============================================================================
# DATASET SPECIFICATIONS
# ============================================================================

DATASET_SPECS = [
    # PRIMARY: Multi-window, single outcome (deterioration_24h, all ED patients)
    {
        "name": "ed_w1_det24_all",
        "windows": "W1",
        "outcomes": "deterioration_24h_from_w1",
        "cohort_type": "all",
        "include_ecg": False,
        "description": "1h window, deterioration 1h-25h (aligned), all ED patients"
    },
    {
        "name": "ed_w6_det24_all",
        "windows": "W6",
        "outcomes": "deterioration_24h_from_w6",
        "cohort_type": "all",
        "include_ecg": False,
        "description": "6h window, deterioration 6h-30h (aligned), all ED patients"
    },
    {
        "name": "ed_w24_det24_all",
        "windows": "W24",
        "outcomes": "deterioration_24h_from_w24",
        "cohort_type": "all",
        "include_ecg": False,
        "description": "24h window, deterioration 24h-48h (aligned), all ED patients"
    },
    
    # SECONDARY: W6, multiple individual outcomes (admitted patients)
    {
        "name": "ed_w6_icu24_admitted",
        "windows": "W6",
        "outcomes": "icu_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
        "description": "6h window, ICU 6h-30h (aligned), admitted only"
    },
    {
        "name": "ed_w6_death24_admitted",
        "windows": "W6",
        "outcomes": "death_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
        "description": "6h window, death 6h-30h (aligned), admitted only"
    },
    {
        "name": "ed_w6_vent24_admitted",
        "windows": "W6",
        "outcomes": "vent_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
        "description": "6h window, ventilation 6h-30h (aligned), admitted only"
    },
    {
        "name": "ed_w6_pressor24_admitted",
        "windows": "W6",
        "outcomes": "pressor_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
        "description": "6h window, vasopressor 6h-30h (aligned), admitted only"
    },
    {
        "name": "ed_w6_cardiac_arrest_admitted",
        "windows": "W6",
        "outcomes": "cardiac_arrest_hosp",
        "cohort_type": "admitted",
        "include_ecg": False,
        "description": "6h window, cardiac arrest (hosp-level), admitted only"
    },
    {
        "name": "ed_w6_acs_admitted",
        "windows": "W6",
        "outcomes": "acs_hosp",
        "cohort_type": "admitted",
        "include_ecg": False,
        "description": "6h window, ACS (hosp-level), admitted only"
    },
    
    # CARDIAC: W6 with ECG features (admitted, cardiac outcomes)
    {
        "name": "ed_w6_cardiac_arrest_ecg_admitted",
        "windows": "W6",
        "outcomes": "cardiac_arrest_hosp",
        "cohort_type": "admitted",
        "include_ecg": True,
        "description": "6h+ECG window, cardiac arrest, admitted only"
    },
    {
        "name": "ed_w6_acs_ecg_admitted",
        "windows": "W6",
        "outcomes": "acs_hosp",
        "cohort_type": "admitted",
        "include_ecg": True,
        "description": "6h+ECG window, ACS, admitted only"
    },
    
    # COMPARATIVE: Same outcome with/without ECG
    {
        "name": "ed_w6_det24_all_ecg",
        "windows": "W6",
        "outcomes": "deterioration_24h_from_w6",
        "cohort_type": "all",
        "include_ecg": True,
        "description": "6h+ECG window, deterioration 6h-30h (aligned), all ED patients"
    },
    {
        "name": "ed_w6_det24_admitted_ecg",
        "windows": "W6",
        "outcomes": "deterioration_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": True,
        "description": "6h+ECG window, deterioration 6h-30h (aligned), admitted only"
    },
]

# ============================================================================
# DATASET GENERATION
# ============================================================================

def generate_datasets(
    cfg: Dict[str, Any],
    conn,
    output_dir: str = "artifacts/datasets",
    dataset_names: List[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Generate multiple datasets via pipeline.
    
    Args:
        cfg: Configuration dictionary
        conn: Database connection
        output_dir: Output directory for CSVs
        dataset_names: Specific dataset names to generate (None = all)
        
    Returns:
        Dictionary mapping dataset names to DataFrames
    """
    datasets = {}
    
    # Filter specs if specific names requested
    specs = DATASET_SPECS
    if dataset_names:
        specs = [s for s in specs if s["name"] in dataset_names]
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Generating {len(specs)} datasets...")
    logger.info(f"{'='*70}\n")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    for i, spec in enumerate(specs, 1):
        name = spec["name"]
        logger.info(f"[{i}/{len(specs)}] Generating {name}...")
        logger.info(f"    {spec['description']}")
        
        try:
            out_csv = f"{output_dir}/{name}.csv"
            
            # Use advanced materialization for flexibility
            df = materialize_dataset_advanced(
                conn=conn,
                cfg=cfg,
                windows=spec["windows"],
                outcome_cols=spec["outcomes"],
                out_csv=out_csv,
                cohort_type=spec["cohort_type"],
                include_ecg=spec.get("include_ecg", False),
                add_missing_ind=True,
                missing_threshold=0.50
            )
            
            datasets[name] = df
            
            # Quick summary
            n_rows = len(df)
            n_features = len([c for c in df.columns if not c.startswith('y')])
            outcome_col = f"y_{spec['outcomes']}" if len([c for c in df.columns if c.startswith('y_')]) > 1 else 'y'
            if outcome_col in df.columns:
                outcome_rate = df[outcome_col].mean()
                logger.info(f"    ✓ {n_rows:,} rows × {n_features} features, outcome rate: {outcome_rate*100:.2f}%\n")
            else:
                logger.info(f"    ✓ {n_rows:,} rows × {n_features} features\n")
            
        except Exception as e:
            logger.error(f"    ✗ Failed: {e}\n")
            continue
    
    logger.info(f"{'='*70}")
    logger.info(f"Generated {len(datasets)} datasets successfully")
    logger.info(f"{'='*70}\n")
    
    return datasets

# ============================================================================
# EXPERIMENTATION
# ============================================================================

def run_cv(X: np.ndarray, y: np.ndarray, model_name: str = "LR", n_folds: int = 5) -> Dict[str, Any]:
    """
    Run 5-fold stratified CV and return metrics.
    
    Args:
        X: Feature matrix
        y: Target vector
        model_name: Model name ("LR" or "XGB")
        n_folds: Number of CV folds
        
    Returns:
        Dictionary with AUROC, AUPRC, F1, precision, recall
    """
    skf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=42)
    
    aurocs = []
    auprcs = []
    f1s = []
    precisions = []
    recalls = []
    fold_times = []
    
    for fold_idx, (train_idx, test_idx) in enumerate(skf.split(X, y), 1):
        t0 = time.time()
        
        Xtr, Xte = X[train_idx], X[test_idx]
        ytr, yte = y[train_idx], y[test_idx]
        
        # Skip if outcome too imbalanced in fold
        if ytr.mean() < 0.001 or ytr.mean() > 0.999:
            logger.warning(f"    Fold {fold_idx}: Skipping (outcome rate {ytr.mean():.4f})")
            continue
        
        try:
            if model_name == "LR":
                m = LogisticRegression(solver='lbfgs', max_iter=1000, class_weight='balanced', random_state=42, n_jobs=-1)
            else:  # XGB
                m = GradientBoostingClassifier(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42)
            
            m.fit(Xtr, ytr)
            yhat = m.predict_proba(Xte)[:, 1]
            
            auroc = roc_auc_score(yte, yhat)
            auprc = average_precision_score(yte, yhat)
            
            # For F1/precision/recall, use 0.5 threshold
            ypred = (yhat >= 0.5).astype(int)
            
            # Skip metrics if no positive predictions
            if ypred.sum() > 0:
                f1 = f1_score(yte, ypred, zero_division=0)
                prec = precision_score(yte, ypred, zero_division=0)
                rec = recall_score(yte, ypred, zero_division=0)
            else:
                f1 = 0.0
                prec = 0.0
                rec = 0.0
            
            aurocs.append(auroc)
            auprcs.append(auprc)
            f1s.append(f1)
            precisions.append(prec)
            recalls.append(rec)
            
            elapsed = time.time() - t0
            fold_times.append(elapsed)
            logger.info(f"    Fold {fold_idx}/5: AUROC={auroc:.4f}, AUPRC={auprc:.4f}, F1={f1:.4f} ({elapsed:.1f}s)")
            
        except Exception as e:
            logger.warning(f"    Fold {fold_idx}: {str(e)[:50]}")
            continue
    
    if not aurocs:
        logger.warning(f"    {model_name}: No successful folds!")
        return {
            "auroc_mean": 0.0,
            "auroc_std": 0.0,
            "auprc_mean": 0.0,
            "auprc_std": 0.0,
            "f1_mean": 0.0,
            "precision_mean": 0.0,
            "recall_mean": 0.0,
            "total_time": 0.0,
            "n_folds_completed": 0
        }
    
    return {
        "auroc_mean": float(np.mean(aurocs)),
        "auroc_std": float(np.std(aurocs)),
        "auprc_mean": float(np.mean(auprcs)),
        "auprc_std": float(np.std(auprcs)),
        "f1_mean": float(np.mean(f1s)),
        "precision_mean": float(np.mean(precisions)),
        "recall_mean": float(np.mean(recalls)),
        "total_time": sum(fold_times),
        "n_folds_completed": len(aurocs)
    }

def run_leakage_experiment(
    cfg: Dict[str, Any],
    conn,
    output_dir: str = "artifacts/results",
    subsample_size: int = 50000
) -> Dict[str, Any]:
    """
    Demonstrate temporal leakage: predicting early outcomes with future features.
    
    Comparison:
    - Normal: W6 features → deterioration_24h (ground truth)
    - Leakage: W24 features → deterioration_24h (future information!)
    
    Returns:
        Dictionary with leakage experiment results
    """
    logger.info(f"\n{'='*70}")
    logger.info("LEAKAGE EXPERIMENT: Temporal Information Leakage")
    logger.info(f"{'='*70}\n")
    logger.info("Predicting early deterioration (24h) with:")
    logger.info("  [1] W6 features (appropriate, current info)")
    logger.info("  [2] W24 features (LEAKAGE! future info)\n")
    
    # Generate W6 and W24 datasets with same outcome
    specs_leakage = [
        {
            "name": "ed_w6_det24_all_normal",
            "windows": "W6",
            "outcomes": "deterioration_24h",
            "cohort_type": "all",
            "include_ecg": False,
        },
        {
            "name": "ed_w24_det24_all_leakage",
            "windows": "W24",
            "outcomes": "deterioration_24h",
            "cohort_type": "all",
            "include_ecg": False,
        },
    ]
    
    datasets_leakage = {}
    for spec in specs_leakage:
        name = spec["name"]
        try:
            out_csv = f"{output_dir}/{name}.csv"
            df = materialize_dataset_advanced(
                conn=conn,
                cfg=cfg,
                windows=spec["windows"],
                outcome_cols=spec["outcomes"],
                out_csv=out_csv,
                cohort_type=spec["cohort_type"],
                include_ecg=spec.get("include_ecg", False),
                add_missing_ind=True,
                missing_threshold=0.50
            )
            datasets_leakage[name] = df
            logger.info(f"✓ Generated {name}: {df.shape[0]:,} rows × {df.shape[1]} cols\n")
        except Exception as e:
            logger.error(f"✗ Failed to generate {name}: {e}\n")
    
    leakage_results = {}
    
    for ds_name, df in datasets_leakage.items():
        outcome_col = 'y'
        y = df[outcome_col].values
        X = df.drop(columns=[outcome_col]).values
        
        # Subsample
        if subsample_size and len(X) > subsample_size:
            from sklearn.model_selection import train_test_split
            X, _, y, _ = train_test_split(X, y, train_size=subsample_size, stratify=y, random_state=42)
        
        logger.info(f"{ds_name}")
        logger.info(f"  Shape: {X.shape[0]:,} rows × {X.shape[1]} features")
        logger.info(f"  Outcome rate: {y.mean()*100:.2f}%")
        
        # LR
        logger.info(f"  ▸ Logistic Regression")
        lr_res = run_cv(X, y, model_name="LR", n_folds=5)
        logger.info(f"    AUROC={lr_res['auroc_mean']:.4f}±{lr_res['auroc_std']:.4f}\n")
        
        # XGB
        logger.info(f"  ▸ Gradient Boosting")
        xgb_res = run_cv(X, y, model_name="XGB", n_folds=5)
        logger.info(f"    AUROC={xgb_res['auroc_mean']:.4f}±{xgb_res['auroc_std']:.4f}\n")
        
        leakage_results[ds_name] = {
            "shape": df.shape,
            "outcome_rate": float(y.mean()),
            "n_features": X.shape[1],
            "LR": lr_res,
            "XGB": xgb_res
        }
    
    # Compute leakage inflation
    if "ed_w6_det24_all_normal" in leakage_results and "ed_w24_det24_all_leakage" in leakage_results:
        w6_lr_auroc = leakage_results["ed_w6_det24_all_normal"]["LR"]["auroc_mean"]
        w24_lr_auroc = leakage_results["ed_w24_det24_all_leakage"]["LR"]["auroc_mean"]
        
        w6_xgb_auroc = leakage_results["ed_w6_det24_all_normal"]["XGB"]["auroc_mean"]
        w24_xgb_auroc = leakage_results["ed_w24_det24_all_leakage"]["XGB"]["auroc_mean"]
        
        lr_inflation = ((w24_lr_auroc - w6_lr_auroc) / w6_lr_auroc) * 100
        xgb_inflation = ((w24_xgb_auroc - w6_xgb_auroc) / w6_xgb_auroc) * 100
        
        logger.info(f"\n{'='*70}")
        logger.info("LEAKAGE INFLATION (Future vs Current Features)")
        logger.info(f"{'='*70}")
        logger.info(f"Logistic Regression:")
        logger.info(f"  W6 (normal):  AUROC={w6_lr_auroc:.4f}")
        logger.info(f"  W24 (leakage): AUROC={w24_lr_auroc:.4f}")
        logger.info(f"  Inflation: {lr_inflation:+.1f}%")
        logger.info(f"Gradient Boosting:")
        logger.info(f"  W6 (normal):  AUROC={w6_xgb_auroc:.4f}")
        logger.info(f"  W24 (leakage): AUROC={w24_xgb_auroc:.4f}")
        logger.info(f"  Inflation: {xgb_inflation:+.1f}%")
        logger.info(f"{'='*70}\n")
        
        leakage_results["leakage_inflation"] = {
            "LR_inflation_pct": float(lr_inflation),
            "XGB_inflation_pct": float(xgb_inflation),
            "w6_lr_auroc": float(w6_lr_auroc),
            "w24_lr_auroc": float(w24_lr_auroc),
            "w6_xgb_auroc": float(w6_xgb_auroc),
            "w24_xgb_auroc": float(w24_xgb_auroc),
        }
    
    # Save leakage results
    leakage_file = f"{output_dir}/leakage_results.json"
    with open(leakage_file, 'w') as f:
        json.dump(leakage_results, f, indent=2)
    
    logger.info(f"Leakage results saved to {leakage_file}\n")
    
    return leakage_results

def run_experiments(
    datasets: Dict[str, pd.DataFrame],
    output_dir: str = "artifacts/results",
    subsample_size: int = 50000
) -> Dict[str, Any]:
    """
    Run LR and XGB on each dataset.
    
    Args:
        datasets: Dictionary of dataset DataFrames
        output_dir: Output directory for results
        subsample_size: Subsample size for training (None = all)
        
    Returns:
        Dictionary with results for all datasets
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    all_results = {}
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Running experiments on {len(datasets)} datasets...")
    logger.info(f"{'='*70}\n")
    
    for ds_idx, (ds_name, df) in enumerate(datasets.items(), 1):
        logger.info(f"\n[{ds_idx}/{len(datasets)}] {ds_name}")
        logger.info(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} cols")
        
        # Find outcome column
        outcome_cols = [c for c in df.columns if c.startswith('y')]
        if not outcome_cols:
            logger.warning(f"  No outcome column found, skipping")
            continue
        
        outcome_col = outcome_cols[0]
        y = df[outcome_col].values
        
        # Check outcome prevalence
        outcome_rate = y.mean()
        logger.info(f"  Outcome rate: {outcome_rate*100:.2f}%")
        
        # Skip if too rare or too common
        if outcome_rate < 0.001 or outcome_rate > 0.999:
            logger.warning(f"  Outcome too imbalanced, skipping")
            continue
        
        # Prepare features
        X = df.drop(columns=outcome_cols).values
        
        # Subsample if needed
        if subsample_size and len(X) > subsample_size:
            logger.info(f"  Subsampling {len(X):,} → {subsample_size:,}...")
            from sklearn.model_selection import train_test_split
            X, _, y, _ = train_test_split(X, y, train_size=subsample_size, stratify=y, random_state=42)
        
        logger.info(f"  Features: {X.shape[1]}")
        
        ds_results = {}
        
        # Run LR
        logger.info(f"  ▸ Logistic Regression")
        lr_res = run_cv(X, y, model_name="LR")
        ds_results["LR"] = lr_res
        logger.info(f"    AUROC={lr_res['auroc_mean']:.4f}±{lr_res['auroc_std']:.4f}\n")
        
        # Run XGB
        logger.info(f"  ▸ Gradient Boosting")
        xgb_res = run_cv(X, y, model_name="XGB")
        ds_results["XGB"] = xgb_res
        logger.info(f"    AUROC={xgb_res['auroc_mean']:.4f}±{xgb_res['auroc_std']:.4f}\n")
        
        all_results[ds_name] = {
            "shape": df.shape,
            "outcome_rate": float(outcome_rate),
            "outcome_col": outcome_col,
            "n_features": X.shape[1],
            "results": ds_results
        }
    
    # Save results
    results_file = f"{output_dir}/experimental_results.json"
    with open(results_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Results saved to {results_file}")
    logger.info(f"{'='*70}\n")
    
    return all_results

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main orchestration."""
    # Setup
    setup_logging(verbose=True)
    cfg = load_yaml("config/config.yaml")
    
    # Check connection
    if not check_connection(cfg):
        logger.error("Database connection failed")
        return
    
    conn = get_conn(cfg)
    
    try:
        # Step 1: Generate datasets
        datasets = generate_datasets(cfg, conn)
        
        # Step 2: Run experiments
        results = run_experiments(datasets, subsample_size=50000)
        
        # Step 3: Leakage experiment
        leakage_results = run_leakage_experiment(cfg, conn, subsample_size=50000)
        
        logger.info(f"\n{'='*70}")
        logger.info("SUMMARY")
        logger.info(f"{'='*70}")
        logger.info(f"Total datasets generated: {len(datasets)}")
        logger.info(f"Total experiments run: {len(results)}")
        logger.info(f"Leakage experiment: COMPLETE")
        logger.info(f"{'='*70}\n")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
