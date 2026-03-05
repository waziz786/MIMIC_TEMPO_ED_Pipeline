"""
Tests for validation functions
"""

import pytest
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.validate import validate_dataset


def test_validate_dataset_empty():
    """Test validation with empty dataset"""
    df = pd.DataFrame()
    
    # Should not raise an error, but return empty metrics
    try:
        metrics = validate_dataset(df, "Empty Dataset")
        assert metrics['n_rows'] == 0
    except Exception as e:
        pytest.fail(f"Validation failed on empty dataset: {e}")


def test_validate_dataset_basic():
    """Test validation with basic dataset"""
    df = pd.DataFrame({
        'stay_id': [1, 2, 3, 4, 5],
        'feature1': [1.0, 2.0, 3.0, 4.0, 5.0],
        'feature2': [10, 20, 30, 40, 50],
        'feature3': [1.0, None, 1.0, None, 1.0],  # 40% missing
        'y': [0, 0, 1, 0, 1]  # 40% positive rate
    })
    
    metrics = validate_dataset(df, "Test Dataset")
    
    assert metrics['n_rows'] == 5
    assert metrics['n_features'] == 4  # Excludes only y, includes stay_id as feature
    assert metrics['outcome_rate'] == 0.4


def test_validate_dataset_missing_features():
    """Test detection of completely missing features"""
    df = pd.DataFrame({
        'stay_id': [1, 2, 3],
        'feature1': [1, 2, 3],
        'feature2': [None, None, None],  # Completely missing
        'y': [0, 1, 0]
    })
    
    metrics = validate_dataset(df, "Missing Test")
    
    assert 'feature2' in metrics['missing_features']
    assert 'feature1' not in metrics['missing_features']


def test_validate_dataset_constant_features():
    """Test detection of constant features"""
    df = pd.DataFrame({
        'stay_id': [1, 2, 3, 4],
        'feature1': [1, 2, 3, 4],  # Variable
        'feature2': [5, 5, 5, 5],  # Constant
        'feature3': [0, 0, 0, 0],  # Constant
        'y': [0, 1, 0, 1]
    })
    
    metrics = validate_dataset(df, "Constant Test")
    
    assert 'feature2' in metrics['constant_features']
    assert 'feature3' in metrics['constant_features']
    assert 'feature1' not in metrics['constant_features']


def test_validate_dataset_extreme_outcomes():
    """Test detection of extreme outcome rates"""
    # Very rare outcome
    df_rare = pd.DataFrame({
        'stay_id': range(1000),
        'feature1': range(1000),
        'y': [1] + [0] * 999  # 0.1% positive rate
    })
    
    metrics_rare = validate_dataset(df_rare, "Rare Outcome")
    assert metrics_rare['outcome_rate'] < 0.01
    
    # Very common outcome
    df_common = pd.DataFrame({
        'stay_id': range(1000),
        'feature1': range(1000),
        'y': [0] + [1] * 999  # 99.9% positive rate
    })
    
    metrics_common = validate_dataset(df_common, "Common Outcome")
    assert metrics_common['outcome_rate'] > 0.99


def test_validate_dataset_no_target():
    """Test validation when target column is missing"""
    df = pd.DataFrame({
        'stay_id': [1, 2, 3],
        'feature1': [1, 2, 3],
    })
    
    try:
        metrics = validate_dataset(df, "No Target")
        # Should handle gracefully
        assert metrics['outcome_rate'] is None
    except Exception as e:
        pytest.fail(f"Should handle missing target gracefully: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
