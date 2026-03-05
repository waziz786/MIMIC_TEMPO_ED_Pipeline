"""
verify_feature_columns.py

Compare expected feature columns (from SQL) with actual columns in the database
and write a small JSON report to `artifacts/reports/feature_columns_report.json`.

Run from repository root:
    python tools/verify_feature_columns.py --config config/config.yaml

"""
import json
import argparse
import sys
import pathlib

# Ensure project root is on sys.path so `from src import ...` works when
# running this script directly (avoids ModuleNotFoundError: No module named 'src').
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils import load_yaml
from src.db import get_conn

EXPECTED = {
    'W1': [
        'stay_id','age_at_ed','gender','arrival_transport','race','chiefcomplaint',
        'temp_w1','hr_w1','rr_w1','spo2_w1','sbp_w1','dbp_w1',
        'triage_pain','triage_acuity','shock_index_w1','map_w1',
        'missing_temp_w1','missing_hr_w1','missing_sbp_w1'
    ],
    'W6': [
        'stay_id','age_at_ed','gender','arrival_transport','race',
        'sbp_min_6h','sbp_max_6h','sbp_mean_6h','sbp_std_6h','dbp_min_6h',
        'hr_min_6h','hr_max_6h','hr_mean_6h','hr_std_6h','rr_max_6h','rr_mean_6h',
        'spo2_min_6h','spo2_mean_6h','temp_max_6h','temp_min_6h','n_vitalsign_measurements_6h',
        'chiefcomplaint','lactate_first_6h','troponin_first_6h','is_hs_troponin_6h',
        'creatinine_first_6h','potassium_first_6h','sodium_first_6h','bicarbonate_first_6h',
        'wbc_first_6h','hemoglobin_first_6h','platelet_first_6h','ed_los_hours',
        'time_to_first_lab_hours','time_to_first_med_hours','prev_admits_1yr','prev_ed_visits_1yr',
        'sbp_cv_6h','hr_range_6h','missing_lactate_6h','missing_troponin_6h'
    ],
    'W24': [
        'stay_id','age_at_ed','gender','arrival_transport','race',
        'sbp_min_24h','sbp_max_24h','sbp_mean_24h','sbp_std_24h',
        'hr_min_24h','hr_max_24h','hr_mean_24h','rr_max_24h','rr_mean_24h',
        'spo2_min_24h','spo2_mean_24h','temp_max_24h','n_vitalsign_measurements_24h',
        'chiefcomplaint','lactate_first_24h','troponin_first_24h','is_hs_troponin_24h',
        'creatinine_first_24h','potassium_first_24h','sodium_first_24h','bicarbonate_first_24h',
        'wbc_first_24h','hemoglobin_first_24h','platelet_first_24h','glucose_first_24h',
        'bun_first_24h','bilirubin_first_24h','inr_first_24h','lactate_max_24h','creatinine_max_24h',
        'ed_los_hours','time_to_first_lab_hours','time_to_first_med_hours',
        'prev_admits_1yr','prev_ed_visits_1yr','bun_creatinine_ratio','lactate_delta_24h'
    ]
}


def get_table_name(cfg, key, default):
    return cfg.get('tables', {}).get(key, default)


def get_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_name = %s ORDER BY ordinal_position""",
            (table_name,)
        )
        return [r[0] for r in cur.fetchall()]


def compare_columns(actual, expected):
    actual_set = set(actual)
    expected_set = set(expected)
    missing = sorted(list(expected_set - actual_set))
    extra = sorted(list(actual_set - expected_set))
    ordered_extra = [c for c in actual if c in extra]
    return {'missing': missing, 'extra': ordered_extra}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/config.yaml')
    parser.add_argument('--out', default='artifacts/reports/feature_columns_report.json')
    args = parser.parse_args()

    cfg = load_yaml(args.config)
    conn = get_conn(cfg)

    tables = {
        'W1': get_table_name(cfg, 'features_w1', 'tmp_features_w1'),
        'W6': get_table_name(cfg, 'features_w6', 'tmp_features_w6'),
        'W24': get_table_name(cfg, 'features_w24', 'tmp_features_w24')
    }

    report = { 'tables': {} }

    for w, table in tables.items():
        cols = get_columns(conn, table)
        comp = compare_columns(cols, EXPECTED[w])
        report['tables'][table] = {
            'window': w,
            'n_columns_actual': len(cols),
            'n_columns_expected': len(EXPECTED[w]),
            'missing_expected_columns': comp['missing'],
            'extra_columns': comp['extra'],
            'actual_columns_sample': cols[:20]
        }

    # Ensure artifacts/reports exists
    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"Report written to: {args.out}")
    # Pretty print summary
    for table, info in report['tables'].items():
        print(f"\nTable: {table} (window {info['window']}):")
        print(f"  actual cols: {info['n_columns_actual']}, expected: {info['n_columns_expected']}")
        if info['missing_expected_columns']:
            print(f"  MISSING: {info['missing_expected_columns']}")
        if info['extra_columns']:
            print(f"  EXTRA: {info['extra_columns']}")

    conn.close()


if __name__ == '__main__':
    main()
