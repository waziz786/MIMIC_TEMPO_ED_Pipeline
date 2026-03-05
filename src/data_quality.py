"""
Data Quality Report Module

Generates comprehensive data quality reports for datasets including:
- Distribution statistics
- Missing value analysis
- Outlier detection
- Feature correlations
- Sanity checks with warnings
"""

import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DataQualityReport:
    """
    Comprehensive data quality analysis for a dataset.
    """
    
    def __init__(self, df: pd.DataFrame, dataset_name: str = "dataset"):
        self.df = df
        self.dataset_name = dataset_name
        self.report = {}
        self.warnings = []
        self.errors = []
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all quality checks and return report."""
        logger.info(f"Running data quality checks on {self.dataset_name}...")
        
        self.report["generated_at"] = datetime.now().isoformat()
        self.report["dataset_name"] = self.dataset_name
        
        # Basic info
        self.report["basic_info"] = self._get_basic_info()
        
        # Column analysis
        self.report["column_analysis"] = self._analyze_columns()
        
        # Missing values
        self.report["missing_values"] = self._analyze_missing()
        
        # Outcome analysis
        self.report["outcome_analysis"] = self._analyze_outcome()
        
        # Outlier detection
        self.report["outliers"] = self._detect_outliers()
        
        # Correlations (top features)
        self.report["correlations"] = self._analyze_correlations()
        
        # Sanity checks
        self.report["sanity_checks"] = self._run_sanity_checks()
        
        # Summary
        self.report["warnings"] = self.warnings
        self.report["errors"] = self.errors
        self.report["quality_score"] = self._compute_quality_score()
        
        return self.report
    
    def _get_basic_info(self) -> Dict[str, Any]:
        """Get basic dataset information."""
        return {
            "n_rows": len(self.df),
            "n_columns": len(self.df.columns),
            "memory_mb": round(self.df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            "columns": list(self.df.columns),
            "dtypes": {col: str(dtype) for col, dtype in self.df.dtypes.items()},
        }
    
    def _analyze_columns(self) -> Dict[str, Dict[str, Any]]:
        """Analyze each column's statistics."""
        analysis = {}
        
        for col in self.df.columns:
            col_data = self.df[col]
            col_info = {
                "dtype": str(col_data.dtype),
                "n_missing": int(col_data.isna().sum()),
                "pct_missing": round(col_data.isna().mean() * 100, 2),
                "n_unique": int(col_data.nunique()),
            }
            
            # Numeric columns
            if pd.api.types.is_numeric_dtype(col_data):
                non_null = col_data.dropna()
                if len(non_null) > 0:
                    col_info.update({
                        "min": float(non_null.min()),
                        "max": float(non_null.max()),
                        "mean": round(float(non_null.mean()), 4),
                        "std": round(float(non_null.std()), 4),
                        "median": float(non_null.median()),
                        "q25": float(non_null.quantile(0.25)),
                        "q75": float(non_null.quantile(0.75)),
                    })
                    
                    # Check for suspicious values
                    if col_info["min"] == col_info["max"] and len(non_null) > 1:
                        self.warnings.append(f"Column '{col}' has constant value: {col_info['min']}")
            
            # Categorical/binary columns
            if col_info["n_unique"] <= 10:
                col_info["value_counts"] = col_data.value_counts().head(10).to_dict()
            
            analysis[col] = col_info
        
        return analysis
    
    def _analyze_missing(self) -> Dict[str, Any]:
        """Analyze missing value patterns."""
        missing_counts = self.df.isna().sum()
        missing_pct = (missing_counts / len(self.df) * 100).round(2)
        
        # Columns with high missingness
        high_missing = missing_pct[missing_pct > 50].sort_values(ascending=False)
        
        if len(high_missing) > 0:
            self.warnings.append(
                f"{len(high_missing)} columns have >50% missing values"
            )
        
        return {
            "total_missing_cells": int(self.df.isna().sum().sum()),
            "total_cells": int(self.df.size),
            "pct_missing_overall": round(self.df.isna().sum().sum() / self.df.size * 100, 2),
            "columns_with_missing": int((missing_counts > 0).sum()),
            "columns_fully_complete": int((missing_counts == 0).sum()),
            "high_missing_columns": high_missing.to_dict(),
        }
    
    def _analyze_outcome(self) -> Dict[str, Any]:
        """Analyze outcome variable (if present)."""
        if 'y' not in self.df.columns:
            return {"present": False}
        
        y = self.df['y']
        outcome_rate = y.mean()
        
        result = {
            "present": True,
            "n_positive": int(y.sum()),
            "n_negative": int((~y.astype(bool)).sum()) if y.dtype == bool else int((y == 0).sum()),
            "outcome_rate": round(outcome_rate * 100, 2),
            "class_balance": round(min(outcome_rate, 1 - outcome_rate) * 2, 4),  # 1.0 = perfect balance
        }
        
        # Warnings for class imbalance
        if outcome_rate < 0.01:
            self.warnings.append(f"[WARN] Very low outcome rate ({outcome_rate*100:.2f}%). Consider oversampling or adjusting threshold.")
        elif outcome_rate < 0.05:
            self.warnings.append(f"Low outcome rate ({outcome_rate*100:.2f}%). May need class balancing.")
        elif outcome_rate > 0.5:
            self.warnings.append(f"High outcome rate ({outcome_rate*100:.2f}%). Check if outcome definition is correct.")
        
        return result
    
    def _detect_outliers(self, threshold: float = 3.0) -> Dict[str, Any]:
        """Detect outliers using IQR and Z-score methods."""
        outliers = {}
        
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        # Exclude binary/indicator columns
        numeric_cols = [c for c in numeric_cols if self.df[c].nunique() > 2 and not c.startswith('missing_')]
        
        for col in numeric_cols[:20]:  # Limit to first 20 numeric columns
            data = self.df[col].dropna()
            if len(data) < 10:
                continue
            
            # IQR method
            q1, q3 = data.quantile(0.25), data.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            n_outliers_iqr = int(((data < lower_bound) | (data > upper_bound)).sum())
            
            # Z-score method
            z_scores = np.abs((data - data.mean()) / data.std())
            n_outliers_zscore = int((z_scores > threshold).sum())
            
            if n_outliers_iqr > 0 or n_outliers_zscore > 0:
                outliers[col] = {
                    "n_outliers_iqr": n_outliers_iqr,
                    "pct_outliers_iqr": round(n_outliers_iqr / len(data) * 100, 2),
                    "n_outliers_zscore": n_outliers_zscore,
                    "iqr_bounds": [round(lower_bound, 2), round(upper_bound, 2)],
                }
        
        return {
            "columns_checked": len(numeric_cols[:20]),
            "columns_with_outliers": len(outliers),
            "details": outliers,
        }
    
    def _analyze_correlations(self, top_n: int = 10) -> Dict[str, Any]:
        """Analyze correlations with outcome and between features."""
        result = {"with_outcome": {}, "high_feature_correlations": []}
        
        if 'y' not in self.df.columns:
            return result
        
        # Correlation with outcome
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        feature_cols = [c for c in numeric_cols if c not in ['y', 'stay_id', 'subject_id', 'hadm_id']]
        
        if len(feature_cols) == 0:
            return result
        
        # Calculate correlations with y
        correlations = {}
        for col in feature_cols:
            try:
                corr = self.df[col].corr(self.df['y'])
                if not pd.isna(corr):
                    correlations[col] = round(corr, 4)
            except:
                pass
        
        # Sort by absolute correlation
        sorted_corrs = sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True)
        result["with_outcome"] = dict(sorted_corrs[:top_n])
        
        # High inter-feature correlations (potential multicollinearity)
        try:
            # Sample for large datasets
            sample_df = self.df[feature_cols[:30]]  # Limit columns
            if len(sample_df) > 10000:
                sample_df = sample_df.sample(10000, random_state=42)
            
            corr_matrix = sample_df.corr()
            
            # Find pairs with |correlation| > 0.8
            high_corrs = []
            for i, col1 in enumerate(corr_matrix.columns):
                for col2 in corr_matrix.columns[i+1:]:
                    corr_val = corr_matrix.loc[col1, col2]
                    if abs(corr_val) > 0.8 and not pd.isna(corr_val):
                        high_corrs.append({
                            "feature1": col1,
                            "feature2": col2,
                            "correlation": round(corr_val, 4)
                        })
            
            result["high_feature_correlations"] = sorted(
                high_corrs, key=lambda x: abs(x["correlation"]), reverse=True
            )[:10]
            
            if len(high_corrs) > 5:
                self.warnings.append(
                    f"{len(high_corrs)} feature pairs have correlation > 0.8 (multicollinearity)"
                )
        except Exception as e:
            logger.debug(f"Could not compute feature correlations: {e}")
        
        return result
    
    def _run_sanity_checks(self) -> List[Dict[str, Any]]:
        """Run sanity checks with pass/warn/fail status."""
        checks = []
        
        # Check 1: Duplicate stay_ids
        if 'stay_id' in self.df.columns:
            n_dups = self.df['stay_id'].duplicated().sum()
            status = "pass" if n_dups == 0 else "warn"
            checks.append({
                "check": "unique_stay_ids",
                "status": status,
                "message": f"{n_dups} duplicate stay_ids found" if n_dups > 0 else "All stay_ids unique",
            })
            if n_dups > 0:
                self.warnings.append(f"Found {n_dups} duplicate stay_ids")
        
        # Check 2: Outcome rate bounds
        if 'y' in self.df.columns:
            outcome_rate = self.df['y'].mean()
            if outcome_rate < 0.001:
                status = "fail"
                msg = f"Outcome rate extremely low ({outcome_rate*100:.3f}%) - check outcome definition"
                self.errors.append(msg)
            elif outcome_rate < 0.01:
                status = "warn"
                msg = f"Outcome rate very low ({outcome_rate*100:.2f}%)"
            elif outcome_rate > 0.5:
                status = "warn"
                msg = f"Outcome rate high ({outcome_rate*100:.2f}%) - verify this is expected"
            else:
                status = "pass"
                msg = f"Outcome rate {outcome_rate*100:.2f}% is reasonable"
            
            checks.append({
                "check": "outcome_rate_bounds",
                "status": status,
                "message": msg,
            })
        
        # Check 3: Age range (if present)
        if 'age' in self.df.columns:
            min_age = self.df['age'].min()
            max_age = self.df['age'].max()
            status = "pass"
            if min_age < 18:
                status = "warn"
                self.warnings.append(f"Found patients younger than 18 (min age: {min_age})")
            if max_age > 120:
                status = "warn"
                self.warnings.append(f"Found implausible ages (max: {max_age})")
            
            checks.append({
                "check": "age_range",
                "status": status,
                "message": f"Age range: {min_age:.0f} - {max_age:.0f}",
            })
        
        # Check 4: Vital signs reasonable ranges
        vital_ranges = {
            "hr_mean_1h": (20, 250),
            "sbp_mean_1h": (40, 280),
            "temp_mean_1h": (30, 43),
            "resp_mean_1h": (4, 60),
            "spo2_mean_1h": (50, 100),
        }
        
        for col, (min_val, max_val) in vital_ranges.items():
            for suffix in ["_1h", "_6h", "_24h", ""]:
                check_col = col.replace("_1h", suffix) if "_1h" in col else col + suffix
                if check_col in self.df.columns:
                    data = self.df[check_col].dropna()
                    if len(data) > 0:
                        out_of_range = ((data < min_val) | (data > max_val)).sum()
                        if out_of_range > 0:
                            pct = out_of_range / len(data) * 100
                            if pct > 5:
                                self.warnings.append(
                                    f"{check_col}: {out_of_range} values ({pct:.1f}%) outside expected range [{min_val}, {max_val}]"
                                )
        
        # Check 5: No all-null columns
        all_null_cols = [col for col in self.df.columns if self.df[col].isna().all()]
        if all_null_cols:
            checks.append({
                "check": "no_all_null_columns",
                "status": "fail",
                "message": f"{len(all_null_cols)} columns are entirely null: {all_null_cols[:5]}",
            })
            self.errors.append(f"Found {len(all_null_cols)} all-null columns")
        else:
            checks.append({
                "check": "no_all_null_columns",
                "status": "pass",
                "message": "No all-null columns found",
            })
        
        return checks
    
    def _compute_quality_score(self) -> float:
        """Compute overall quality score (0-100)."""
        score = 100.0
        
        # Deduct for missing values
        pct_missing = self.report["missing_values"]["pct_missing_overall"]
        score -= min(pct_missing * 0.5, 20)  # Max 20 point deduction
        
        # Deduct for warnings
        score -= len(self.warnings) * 2
        
        # Deduct for errors
        score -= len(self.errors) * 10
        
        # Deduct for low outcome rate
        if "outcome_analysis" in self.report and self.report["outcome_analysis"].get("present"):
            outcome_rate = self.report["outcome_analysis"]["outcome_rate"]
            if outcome_rate < 1:
                score -= 10
            elif outcome_rate < 5:
                score -= 5
        
        return max(0, round(score, 1))
    
    def save_report(self, path: str) -> None:
        """Save report to JSON file."""
        if not self.report:
            self.run_all_checks()
        
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(self.report, f, indent=2, default=str)
        
        logger.info(f"Quality report saved: {path}")
    
    def print_summary(self) -> None:
        """Print a summary of the quality report."""
        if not self.report:
            self.run_all_checks()
        
        print("\n" + "=" * 60)
        print(f"DATA QUALITY REPORT: {self.dataset_name}")
        print("=" * 60)
        
        # Basic info
        info = self.report["basic_info"]
        print(f"\n📊 Basic Info:")
        print(f"   Rows: {info['n_rows']:,}")
        print(f"   Columns: {info['n_columns']}")
        print(f"   Memory: {info['memory_mb']:.2f} MB")
        
        # Missing values
        missing = self.report["missing_values"]
        print(f"Missing Values:")
        print(f"   Overall: {missing['pct_missing_overall']:.1f}%")
        print(f"   Columns with missing: {missing['columns_with_missing']}/{info['n_columns']}")
        
        # Outcome
        outcome = self.report["outcome_analysis"]
        if outcome.get("present"):
            print(f"\n🎯 Outcome:")
            print(f"   Positive: {outcome['n_positive']:,} ({outcome['outcome_rate']:.2f}%)")
            print(f"   Negative: {outcome['n_negative']:,}")
        
        # Correlations
        corrs = self.report.get("correlations", {}).get("with_outcome", {})
        if corrs:
            print(f"Top Correlated Features:")
            for feat, corr in list(corrs.items())[:5]:
                print(f"   {feat}: {corr:+.3f}")
        
        # Sanity checks
        checks = self.report.get("sanity_checks", [])
        if checks:
            print(f"Sanity Checks:")
            for check in checks:
                icon = "[OK]" if check["status"] == "pass" else "[!]" if check["status"] == "warn" else "[X]"
                print(f"   {icon} {check['check']}: {check['message']}")
        
        # Warnings
        if self.warnings:
            print(f"\n[WARN] Warnings ({len(self.warnings)}):")
            for w in self.warnings[:10]:
                print(f"   - {w}")
        
        # Quality score
        print(f"Quality Score: {self.report['quality_score']}/100")
        print("=" * 60)


def generate_quality_report(
    df: pd.DataFrame,
    dataset_name: str,
    output_dir: str = "artifacts/reports"
) -> Dict[str, Any]:
    """
    Generate and save a data quality report.
    
    Args:
        df: DataFrame to analyze
        dataset_name: Name for the dataset
        output_dir: Directory to save report
        
    Returns:
        Quality report dictionary
    """
    reporter = DataQualityReport(df, dataset_name)
    report = reporter.run_all_checks()
    
    # Save report
    report_path = f"{output_dir}/{dataset_name}_quality_report.json"
    reporter.save_report(report_path)
    
    # Print summary
    reporter.print_summary()
    
    return report


def generate_feature_summary(
    df: pd.DataFrame,
    output_path: str
) -> pd.DataFrame:
    """
    Generate a summary CSV of all features.
    
    Args:
        df: DataFrame to summarize
        output_path: Path to save CSV
        
    Returns:
        Summary DataFrame
    """
    summary_data = []
    
    for col in df.columns:
        col_data = df[col]
        row = {
            "feature": col,
            "dtype": str(col_data.dtype),
            "n_missing": col_data.isna().sum(),
            "pct_missing": round(col_data.isna().mean() * 100, 2),
            "n_unique": col_data.nunique(),
        }
        
        # Try to compute numeric stats for all columns
        numeric_data = None
        
        if pd.api.types.is_numeric_dtype(col_data):
            # Already numeric
            numeric_data = col_data.dropna()
        else:
            # Try to convert object columns to numeric
            try:
                # First try direct conversion
                numeric_data = pd.to_numeric(col_data.dropna(), errors='coerce').dropna()
                
                # If we lost too many values in conversion, it's probably categorical
                if len(numeric_data) < 0.5 * len(col_data.dropna()):
                    numeric_data = None
            except:
                numeric_data = None
        
        # Compute numeric statistics if we have numeric data
        if numeric_data is not None and len(numeric_data) > 0:
            row.update({
                "min": float(numeric_data.min()),
                "max": float(numeric_data.max()), 
                "mean": round(float(numeric_data.mean()), 4),
                "std": round(float(numeric_data.std()), 4),
                "median": float(numeric_data.median()),
            })
            
            # Correlation with y if present
            if 'y' in df.columns and col != 'y':
                try:
                    # Use original or converted data for correlation
                    if pd.api.types.is_numeric_dtype(col_data):
                        corr_data = col_data
                    else:
                        corr_data = pd.to_numeric(col_data, errors='coerce')
                    
                    corr_val = corr_data.corr(df['y'])
                    if not pd.isna(corr_val):
                        row["corr_with_y"] = round(corr_val, 4)
                except:
                    pass
        
        # For categorical columns, add most frequent values
        if col_data.dtype == 'object' and col_data.nunique() <= 20:
            non_null = col_data.dropna()
            if len(non_null) > 0:
                top_values = non_null.value_counts().head(3)
                row["top_values"] = "; ".join([f"{val}({count})" for val, count in top_values.items()])
        
        summary_data.append(row)
    
    summary_df = pd.DataFrame(summary_data)
    
    # Save
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(output_path, index=False)
    logger.info(f"Feature summary saved: {output_path}")
    
    return summary_df
