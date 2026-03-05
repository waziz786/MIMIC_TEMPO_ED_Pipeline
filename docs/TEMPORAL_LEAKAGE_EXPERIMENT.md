# Temporal Leakage Demonstration Experiment

## Overview
This experiment quantifies the impact of temporal leakage by comparing two conditions:

### ISOLATED Condition (Leak-Free)
- **Features**: W6 window [0-6 hours from ED arrival]
- **Outcome**: deterioration_24h observed [6-30 hours from ED arrival]
- **Temporal Overlap**: **ZERO** hours
- **Design**: Features extracted BEFORE outcome observation period

### CONTAMINATED Condition (With Leakage)
- **Features**: W24 window [0-24 hours from ED arrival]  
- **Outcome**: deterioration_24h observed [6-30 hours from ED arrival] (same as isolated)
- **Temporal Overlap**: **18 hours** (features from 6-24h overlap with outcome period 6-30h)
- **Leakage Mechanism**: Model has access to features extracted DURING the outcome observation period

## Why This Matters
Temporal leakage occurs when prediction features are extracted from a time period that overlaps with the outcome observation window. In clinical settings, this creates:

1. **Artificially inflated performance** during model development
2. **Impossible deployment** - features wouldn't be available at prediction time
3. **Invalid claims** about model capabilities

## Experimental Design

### Dataset
- **Source**: Combined W6 and W24 feature sets with deterioration_24h_from_w6 outcome
- **Sample Size**: 100,000 (subsampled for computational efficiency)
- **Prevalence**: ~4.00%
- **Features**: 40 matched feature types across both conditions
  - 16 Vitals (sbp, dbp, hr, rr, spo2, temp: min/max/mean)
  - 12 Labs (lactate, troponin, creatinine, wbc, hemoglobin, platelet: first/last)
  - 5 Time/Counts (ed_los_hours, time_to_first_vital, time_to_first_lab, n_vitalsign_sets, prev_ed_visits_1yr)
  - 2 Demographics (age_at_ed, gender)
  - 5 Missing Indicators (missing_*_last for 5 lab analytes)

### Models
1. **Logistic Regression** (L-BFGS solver, 500 iterations)
2. **Gradient Boosting Classifier** (100 trees, max depth 5, learning rate 0.1)

### Evaluation
- **Cross-Validation**: 5-fold GroupKFold (by subject_id)
- **Metrics**: AUROC, AUPRC, Brier Score
- **Comparison**: CONTAMINATED performance - ISOLATED performance = **Leakage Effect**

## Expected Results
The **CONTAMINATED** condition should show:
- **Higher AUROC/AUPRC** (artificially inflated)
- **Lower Brier score** (better calibration, but invalid)
- **Leakage effect** quantified as percentage point improvement

## Reference
This experimental design follows best practices from:
- Nestor, B., et al. (2019). "Feature robustness in non-stationary health records: caveats to deployable model performance in common clinical machine learning tasks." Machine Learning for Healthcare Conference.

## Files
- **Dataset Generator**: `generate_leakage_dataset.py`
- **Experiment Script**: `experiments/exp_temporal_leakage_demonstration.py`
- **Results**: `artifacts/results/exp_temporal_leakage_demonstration.json`

## Our Pipeline's Solution
Our pipeline prevents this leakage through:
1. **Strict temporal windows**: Features [0, P], Outcomes (P, P+H]  
2. **Zero overlap**: Outcome observation starts AFTER feature extraction ends
3. **event_by flags**: Indicators for filtering patients who already had the outcome
4. **Prediction-time alignment**: All outcomes anchored to END of feature window

This experiment **demonstrates** why temporal isolation is critical and **validates** that our design prevents leakage.
