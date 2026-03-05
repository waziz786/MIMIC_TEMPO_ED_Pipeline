"""
Cardiac Deterioration Pipeline for MIMIC-IV ED
A modular framework for generating deterioration prediction datasets
"""

__version__ = "1.0.0"
__author__ = "MIMIC Research Team"

from .db import get_conn, run_sql, fetch_df
from .utils import load_yaml, render_sql_template, read_sql
from .build_base import build_base
from .build_event_log import build_event_log
from .build_outcomes import build_outcomes
from .build_features import build_features
from .materialize_datasets import materialize_dataset
from .validate import sanity_counts, validate_pipeline

__all__ = [
    "get_conn",
    "run_sql",
    "fetch_df",
    "load_yaml",
    "render_sql_template",
    "read_sql",
    "build_base",
    "build_event_log",
    "build_outcomes",
    "build_features",
    "materialize_dataset",
    "sanity_counts",
    "validate_pipeline",
]
