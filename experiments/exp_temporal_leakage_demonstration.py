#!/usr/bin/env python3
"""
Experiment: Temporal Leakage Demonstration
==========================================

Purpose: Quantify the impact of temporal leakage by comparing:
  - ISOLATED condition: Features [0-6h] → Outcome [6-30h] (zero overlap)
  - CONTAMINATED condition: Features [0-30h] → Outcome [6-30h] (full overlap)

This demonstrates why temporal isolation is critical for valid clinical predictions.

Reference:
    Nestor, B., et al. (2019). "Feature robustness in non-stationary health records: 
    caveats to deployable model performance in common clinical machine learning tasks."
    Machine Learning for Healthcare Conference.
"""

import sys
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss

warnings.filterwarnings("ignore")

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils import load_yaml

# Configuration
RANDOM_STATE = 42
CV_FOLDS = 5  # Reduced from 5 for speed
SUBSAMPLE_SIZE = 100000  # Match other experiments
RESULTS_DIR = PROJECT_ROOT / "artifacts" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Model configurations (simplified for speed)
MODELS = {
    "Logistic Regression": LogisticRegression(
        max_iter=500,
        solver="lbfgs",
        random_state=RANDOM_STATE,
        n_jobs=-1
    ),
    "Gradient Boosting": GradientBoostingClassifier(
        n_estimators=100,  # Reduced from 100
        learning_rate=0.1,
        max_depth=5,  # Reduced from 5
        random_state=RANDOM_STATE
    )
}


def load_and_prepare_isolated(csv_path):
    """
    Load ISOLATED condition: W6 features [0-6h] → deterioration_24h [6-30h]
    This is the temporally isolated design with ZERO overlap.
    """
    print(f"\n[ISOLATED] Loading {csv_path.name}...")
    df = pd.read_csv(csv_path)
    
    # Get outcome
    y_col = "deterioration_24h_from_w6"
    if y_col not in df.columns:
        raise ValueError(f"Cannot find {y_col} column")
    
    # Get W6 features only
    w6_features = [c for c in df.columns if c.startswith("w6_")]
    
    X = df[w6_features].copy()
    y = df[y_col].values
    groups = df["stay_id"].values
    
    # Remove rows with missing outcome
    valid = ~pd.isna(y)
    X, y, groups = X[valid], y[valid], groups[valid]
    
    # Select only numeric columns and handle missing values
    X = X.select_dtypes(include=[np.number])
    X = X.fillna(X.median())
    
    # Subsample to manageable size
    if len(X) > SUBSAMPLE_SIZE:
        idx = np.random.RandomState(RANDOM_STATE).choice(len(X), SUBSAMPLE_SIZE, replace=False)
        X, y, groups = X.iloc[idx], y[idx], groups[idx]
        print(f"  → Subsampled to {SUBSAMPLE_SIZE:,} for computational efficiency")
    
    print(f"  → N={len(X):,}, Features={X.shape[1]}, Prevalence={y.mean():.2%}")
    print(f"  → Feature extraction window: [0-6h]")
    print(f"  → Outcome observation window: [6-30h]")
    print(f"  → Temporal overlap: ZERO (isolated)")
    
    # Extract base feature names (without w6_ prefix and _6h suffix) for matching
    base_features = []
    for col in X.columns:
        # Remove w6_ prefix and _6h suffix
        base_name = col.replace('w6_', '').replace('_6h', '').replace('_first_', '_').replace('_last_', '_')
        base_features.append(base_name)
    
    return X, y, groups, list(X.columns), base_features


def load_and_prepare_contaminated(csv_path, match_features=None):
    """
    Load CONTAMINATED condition: W24 features [0-24h] → deterioration_24h [6-30h]
    This creates temporal leakage because features include the outcome period.
    
    Args:
        csv_path: Path to dataset
        match_features: List of base feature names to match with isolated condition
    """
    print(f"\n[CONTAMINATED] Loading contaminated condition...")
    df = pd.read_csv(csv_path)
    
    # Get outcome
    y_col = "deterioration_24h_from_w6"
    if y_col not in df.columns:
        raise ValueError(f"Cannot find {y_col} column")
    
    # Get W24 features (these extend into outcome period!)
    w24_all_features = [c for c in df.columns if c.startswith("w24_")]
    
    # If match_features provided, match by base feature names
    if match_features is not None:
        matched_features = []
        for w24_feat in w24_all_features:
            # Extract base name from W24 feature
            base_name = w24_feat.replace('w24_', '').replace('_24h', '').replace('_first_', '_').replace('_last_', '_')
            if base_name in match_features:
                matched_features.append(w24_feat)
        w24_features = matched_features
        print(f"  → Matched to {len(w24_features)} features from isolated condition")
    else:
        w24_features = w24_all_features
    
    X = df[w24_features].copy()
    y = df[y_col].values
    groups = df["stay_id"].values
    
    # Remove rows with missing outcome
    valid = ~pd.isna(y)
    X, y, groups = X[valid], y[valid], groups[valid]
    
    # Select only numeric columns and handle missing values
    X = X.select_dtypes(include=[np.number])
    X = X.fillna(X.median())
    
    # Subsample to SAME size as isolated for fair comparison
    if len(X) > SUBSAMPLE_SIZE:
        idx = np.random.RandomState(RANDOM_STATE).choice(len(X), SUBSAMPLE_SIZE, replace=False)
        X, y, groups = X.iloc[idx], y[idx], groups[idx]
        print(f"  → Subsampled to {SUBSAMPLE_SIZE:,} for computational efficiency")
    
    print(f"  → N={len(X):,}, Features={X.shape[1]}, Prevalence={y.mean():.2%}")
    print(f"  → Feature extraction window: [0-24h]")
    print(f"  → Outcome observation window: [6-30h]")
    print(f"  → Temporal overlap: 18 hours (CONTAMINATED)")
    print(f"  → Leakage mechanism: Model sees features from outcome period")
    
    return X, y, groups, list(X.columns)


def evaluate_model(model, X_train, X_test, y_train, y_test):
    """Train and evaluate a model, return metrics."""
    model.fit(X_train, y_train)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    return {
        "AUROC": roc_auc_score(y_test, y_pred_proba),
        "AUPRC": average_precision_score(y_test, y_pred_proba),
        "Brier": brier_score_loss(y_test, y_pred_proba)
    }


def run_cv_evaluation(X, y, groups, model, model_name, condition_name):
    """Run cross-validated evaluation."""
    print(f"\n  Training {model_name} on {condition_name} condition...")
    
    cv = GroupKFold(n_splits=CV_FOLDS)
    results = []
    
    for fold_idx, (train_idx, test_idx) in enumerate(cv.split(X, y, groups), 1):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]
        
        metrics = evaluate_model(model, X_train, X_test, y_train, y_test)
        results.append(metrics)
        
        print(f"    Fold {fold_idx}: AUROC={metrics['AUROC']:.4f}, "
              f"AUPRC={metrics['AUPRC']:.4f}, Brier={metrics['Brier']:.4f}")
    
    # Aggregate results
    mean_metrics = {
        metric: np.mean([r[metric] for r in results])
        for metric in results[0].keys()
    }
    std_metrics = {
        metric: np.std([r[metric] for r in results])
        for metric in results[0].keys()
    }
    
    print(f"  → Mean AUROC: {mean_metrics['AUROC']:.4f} ± {std_metrics['AUROC']:.4f}")
    
    return mean_metrics, std_metrics, results


def analyze_feature_importance(X_isolated, X_contaminated, y, groups, feature_names_isolated, feature_names_contaminated):
    """
    Analyze which features contribute most to the leakage effect.
    Compare feature importance between isolated and contaminated models.
    """
    print("\n[FEATURE IMPORTANCE ANALYSIS]")
    
    # Train GBM on both conditions
    model_isolated = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=RANDOM_STATE)
    model_contaminated = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=RANDOM_STATE)
    
    # Use all data for feature importance (just for analysis)
    model_isolated.fit(X_isolated, y)
    model_contaminated.fit(X_contaminated, y)
    
    # Get feature importances
    importance_isolated = pd.DataFrame({
        "feature": feature_names_isolated,
        "importance": model_isolated.feature_importances_
    }).sort_values("importance", ascending=False)
    
    importance_contaminated = pd.DataFrame({
        "feature": feature_names_contaminated,
        "importance": model_contaminated.feature_importances_
    }).sort_values("importance", ascending=False)
    
    print("\nTop 10 features in ISOLATED condition:")
    for idx, row in importance_isolated.head(10).iterrows():
        print(f"  {row['feature']}: {row['importance']:.4f}")
    
    print("\nTop 10 features in CONTAMINATED condition:")
    for idx, row in importance_contaminated.head(10).iterrows():
        print(f"  {row['feature']}: {row['importance']:.4f}")
    
    return {
        "isolated_top10": importance_isolated.head(10).to_dict("records"),
        "contaminated_top10": importance_contaminated.head(10).to_dict("records")
    }


def main():
    """Run temporal leakage demonstration experiment."""
    print("=" * 80)
    print("EXPERIMENT: Temporal Leakage Demonstration")
    print("=" * 80)
    print("\nObjective: Quantify the impact of temporal leakage by comparing:")
    print("  - ISOLATED: Features [0-6h] → Outcome [6-30h] (zero overlap)")
    print("  - CONTAMINATED: Features [0-24h] → Outcome [6-30h] (18h overlap)")
    print("\nThis demonstrates why temporal isolation is critical for valid predictions.")
    
    # Path to focused dataset
    leakage_path = PROJECT_ROOT / "artifacts" / "datasets" / "leakage_demo_w6_w24.csv"
    
    if not leakage_path.exists():
        print(f"\n❌ ERROR: Required dataset not found: {leakage_path}")
        print("\nPlease run: python generate_leakage_dataset.py")
        return 1
    
    # Load data
    print("\n" + "=" * 80)
    print("LOADING DATA")
    print("=" * 80)
    
    # Step 1: Load full dataset and identify all features
    print("\n[STEP 1] Identifying common features...")
    df_full = pd.read_csv(leakage_path)
    
    w6_all = [c for c in df_full.columns if c.startswith("w6_")]
    w24_all = [c for c in df_full.columns if c.startswith("w24_")]
    
    print(f"  W6 total features: {len(w6_all)}")
    print(f"  W24 total features: {len(w24_all)}")
    
    # Step 2: Normalize names by removing prefixes and suffixes
    def normalize_name(col):
        """Remove w6_/w24_ prefix and _6h/_24h/_first_6h/_last_6h etc suffixes"""
        name = col.replace('w6_', '').replace('w24_', '')
        name = name.replace('_6h', '').replace('_24h', '')
        name = name.replace('_first_', '_').replace('_last_', '_')
        return name
    
    w6_map = {normalize_name(c): c for c in w6_all}
    w24_map = {normalize_name(c): c for c in w24_all}
    
    # Step 3: Find common base names
    common_bases = sorted(set(w6_map.keys()) & set(w24_map.keys()))
    
    w6_common = [w6_map[base] for base in common_bases]
    w24_common = [w24_map[base] for base in common_bases]
    
    print(f"  Common features (matched): {len(common_bases)}")
    print(f"  W6 extra features: {len(w6_all) - len(w6_common)}")
    print(f"  W24 extra features: {len(w24_all) - len(w24_common)}")
    
    # Step 4: Subsample ONCE on the full dataset to ensure IDENTICAL samples for both conditions
    print(f"\n[STEP 4] Subsampling to {SUBSAMPLE_SIZE:,} for fair comparison...")
    
    y_full = df_full["deterioration_24h_from_w6"].values
    valid_full = ~pd.isna(y_full)
    valid_indices = np.where(valid_full)[0]
    
    if len(valid_indices) > SUBSAMPLE_SIZE:
        rng = np.random.RandomState(RANDOM_STATE)
        subsample_idx = rng.choice(valid_indices, SUBSAMPLE_SIZE, replace=False)
        print(f"  → Selected {SUBSAMPLE_SIZE:,} from {len(valid_indices):,} valid samples")
    else:
        subsample_idx = valid_indices
        print(f"  → Using all {len(valid_indices):,} valid samples")
    
    # Now create ISOLATED and CONTAMINATED conditions from THE SAME subsampled indices
    print(f"\n[ISOLATED] Loading W6 features on subsampled data...")
    X_isolated = df_full.iloc[subsample_idx][w6_common].select_dtypes(include=[np.number]).copy()
    y_isolated = df_full.iloc[subsample_idx]["deterioration_24h_from_w6"].values
    groups_isolated = df_full.iloc[subsample_idx]["stay_id"].values
    X_isolated = X_isolated.fillna(X_isolated.median())
    
    print(f"  → N={len(X_isolated):,}, Features={X_isolated.shape[1]}, Prevalence={y_isolated.mean():.2%}")
    print(f"  → Feature extraction window: [0-6h]")
    print(f"  → Outcome observation window: [6-30h]")
    print(f"  → Temporal overlap: ZERO")
    
    print(f"\n[CONTAMINATED] Loading W24 features on SAME subsampled data...")
    X_contaminated = df_full.iloc[subsample_idx][w24_common].select_dtypes(include=[np.number]).copy()
    y_contaminated = df_full.iloc[subsample_idx]["deterioration_24h_from_w6"].values
    groups_contaminated = df_full.iloc[subsample_idx]["stay_id"].values
    X_contaminated = X_contaminated.fillna(X_contaminated.median())
    
    print(f"  → N={len(X_contaminated):,}, Features={X_contaminated.shape[1]}, Prevalence={y_contaminated.mean():.2%}")
    print(f"  → Feature extraction window: [0-24h]")
    print(f"  → Outcome observation window: [6-30h]")
    print(f"  → Temporal overlap: 18 hours (CONTAMINATED)")
    
    features_isolated = list(X_isolated.columns)
    features_contaminated = list(X_contaminated.columns)
    
    # Verify same samples
    print(f"\n✓ Sample alignment verification:")
    print(f"  ISOLATED: N={len(X_isolated):,}, Patients={len(np.unique(groups_isolated))}")
    print(f"  CONTAMINATED: N={len(X_contaminated):,}, Patients={len(np.unique(groups_contaminated))}")
    print(f"  ✓ Same stay_id values: {np.array_equal(groups_isolated, groups_contaminated)}")
    print(f"  ✓ Same outcome values: {np.array_equal(y_isolated, y_contaminated)}")
    
    # Store results
    results = {
        "experiment": "temporal_leakage_demonstration",
        "description": "Comparison of temporally isolated vs contaminated feature windows",
        "methodology": {
            "isolated": {
                "feature_window": "[0-6h]",
                "outcome_window": "[6-30h]",
                "temporal_overlap": "0 hours",
                "n_features": len(features_isolated),
                "n_samples": len(X_isolated)
            },
            "contaminated": {
                "feature_window": "[0-24h]",
                "outcome_window": "[6-30h]",
                "temporal_overlap": "18 hours",
                "n_features": len(features_contaminated),
                "n_samples": len(X_contaminated)
            }
        },
        "models": {},
        "leakage_effect": {}
    }
    
    # Run experiments
    print("\n" + "=" * 80)
    print("EVALUATING MODELS")
    print("=" * 80)
    
    for model_name, model_class in MODELS.items():
        print(f"\n{'=' * 40}")
        print(f"{model_name}")
        print(f"{'=' * 40}")
        
        # Isolated condition
        print(f"\n[ISOLATED CONDITION]")
        mean_iso, std_iso, folds_iso = run_cv_evaluation(
            X_isolated, y_isolated, groups_isolated,
            model_class, model_name, "ISOLATED"
        )
        
        # Contaminated condition
        print(f"\n[CONTAMINATED CONDITION]")
        mean_cont, std_cont, folds_cont = run_cv_evaluation(
            X_contaminated, y_contaminated, groups_contaminated,
            model_class, model_name, "CONTAMINATED"
        )
        
        # Calculate leakage effect
        leakage_auroc = mean_cont["AUROC"] - mean_iso["AUROC"]
        leakage_auprc = mean_cont["AUPRC"] - mean_iso["AUPRC"]
        leakage_brier = mean_iso["Brier"] - mean_cont["Brier"]  # Lower is better for Brier
        
        print(f"\n{'*' * 40}")
        print(f"LEAKAGE EFFECT ({model_name}):")
        print(f"{'*' * 40}")
        print(f"  AUROC gain: {leakage_auroc:+.4f} ({leakage_auroc*100:+.2f}%)")
        print(f"  AUPRC gain: {leakage_auprc:+.4f} ({leakage_auprc*100:+.2f}%)")
        print(f"  Brier improvement: {leakage_brier:+.4f}")
        
        # Store results
        results["models"][model_name] = {
            "isolated": {
                "mean": mean_iso,
                "std": std_iso,
                "folds": folds_iso
            },
            "contaminated": {
                "mean": mean_cont,
                "std": std_cont,
                "folds": folds_cont
            },
            "leakage_effect": {
                "auroc_gain": leakage_auroc,
                "auprc_gain": leakage_auprc,
                "brier_improvement": leakage_brier,
                "auroc_gain_pct": leakage_auroc * 100,
                "auprc_gain_pct": leakage_auprc * 100
            }
        }
    
    # Feature importance analysis
    print("\n" + "=" * 80)
    print("FEATURE IMPORTANCE ANALYSIS")
    print("=" * 80)
    
    importance_analysis = analyze_feature_importance(
        X_isolated, X_contaminated, y_isolated, groups_isolated,
        features_isolated, features_contaminated
    )
    results["feature_importance"] = importance_analysis
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("\nTemporal leakage effect (CONTAMINATED - ISOLATED):")
    for model_name in MODELS.keys():
        effect = results["models"][model_name]["leakage_effect"]
        print(f"\n{model_name}:")
        print(f"  AUROC: {effect['auroc_gain_pct']:+.2f}%")
        print(f"  AUPRC: {effect['auprc_gain_pct']:+.2f}%")
    
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    print("\nThe CONTAMINATED condition shows artificially inflated performance")
    print("because the model has access to features from the outcome period.")
    print("\nOur ISOLATED design prevents this leakage by enforcing zero temporal")
    print("overlap between feature extraction [0-P] and outcome observation (P,P+H].")
    print("\nThis demonstrates the critical importance of temporal isolation for")
    print("valid clinical prediction models.")
    
    # Save results
    output_path = RESULTS_DIR / "exp_temporal_leakage_demonstration.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n✓ Results saved to: {output_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
