"""
Event-By Flag Statistics & Validation
Queries the outcomes table to report prevalence of all 18 event_by_* flags
and validates logical consistency and monotonicity.
"""
import psycopg2

conn = psycopg2.connect(
    host='localhost', port=5432, dbname='mimiciv',
    user='postgres', password='786786'
)
cur = conn.cursor()

TABLE = 'tmp_ed_outcomes'

# 1. Verify columns exist
cur.execute(f"""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = '{TABLE}' AND column_name LIKE 'event_by_%'
    ORDER BY column_name
""")
cols = [r[0] for r in cur.fetchall()]
print(f"Event-by columns in tmp_outcomes: {len(cols)}")
for c in cols:
    print(f"  {c}")

# 2. Prevalences
cur.execute("""
SELECT
    COUNT(*) as n,
    AVG(event_by_icu_w1::float)*100,
    AVG(event_by_icu_w6::float)*100,
    AVG(event_by_icu_w24::float)*100,
    AVG(event_by_pressor_w1::float)*100,
    AVG(event_by_pressor_w6::float)*100,
    AVG(event_by_pressor_w24::float)*100,
    AVG(event_by_vent_w1::float)*100,
    AVG(event_by_vent_w6::float)*100,
    AVG(event_by_vent_w24::float)*100,
    AVG(event_by_rrt_w1::float)*100,
    AVG(event_by_rrt_w6::float)*100,
    AVG(event_by_rrt_w24::float)*100,
    AVG(event_by_death_w1::float)*100,
    AVG(event_by_death_w6::float)*100,
    AVG(event_by_death_w24::float)*100,
    AVG(event_by_deterioration_w1::float)*100,
    AVG(event_by_deterioration_w6::float)*100,
    AVG(event_by_deterioration_w24::float)*100
FROM tmp_ed_outcomes
""")
r = cur.fetchone()
n = r[0]
print(f"\nTotal rows: {n:,}")
print()
print("EVENT-BY FLAG PREVALENCES (% of ED visits with event WITHIN feature window):")
print("=" * 75)
header = f"{'Event':<30} {'By W1 (<=1h)':>12} {'By W6 (<=6h)':>12} {'By W24 (<=24h)':>14}"
print(header)
print("-" * 75)
labels = ["ICU Admission", "Vasopressor Start", "Ventilation Start", "RRT Start", "Death", "Composite Deterioration"]
for i, label in enumerate(labels):
    w1 = r[1 + i*3]
    w6 = r[2 + i*3]
    w24 = r[3 + i*3]
    print(f"{label:<30} {w1:>11.3f}% {w6:>11.3f}% {w24:>13.3f}%")
print()

# 3. Absolute counts
cur.execute("""
SELECT
    SUM(event_by_icu_w1), SUM(event_by_icu_w6), SUM(event_by_icu_w24),
    SUM(event_by_pressor_w1), SUM(event_by_pressor_w6), SUM(event_by_pressor_w24),
    SUM(event_by_vent_w1), SUM(event_by_vent_w6), SUM(event_by_vent_w24),
    SUM(event_by_rrt_w1), SUM(event_by_rrt_w6), SUM(event_by_rrt_w24),
    SUM(event_by_death_w1), SUM(event_by_death_w6), SUM(event_by_death_w24),
    SUM(event_by_deterioration_w1), SUM(event_by_deterioration_w6), SUM(event_by_deterioration_w24)
FROM tmp_ed_outcomes
""")
c = cur.fetchone()
print("ABSOLUTE COUNTS:")
print("=" * 75)
print(f"{'Event':<30} {'By W1':>10} {'By W6':>10} {'By W24':>12}")
print("-" * 75)
for i, label in enumerate(labels):
    print(f"{label:<30} {c[i*3]:>10,} {c[i*3+1]:>10,} {c[i*3+2]:>12,}")
print()

# 4. Logical consistency: event_by_X_w24 should equal arrival-anchored X_24h (same boundary)
#    event_by_X_w1 and event_by_X_w6 should be subsets of X_24h
cur.execute("""
SELECT
    SUM(CASE WHEN event_by_icu_w1 = 1 AND icu_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_icu_w6 = 1 AND icu_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_icu_w24 = 1 AND icu_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_deterioration_w1 = 1 AND deterioration_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_deterioration_w6 = 1 AND deterioration_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_deterioration_w24 = 1 AND deterioration_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_pressor_w1 = 1 AND pressor_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_pressor_w6 = 1 AND pressor_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_pressor_w24 = 1 AND pressor_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_vent_w1 = 1 AND vent_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_vent_w6 = 1 AND vent_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_vent_w24 = 1 AND vent_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_rrt_w1 = 1 AND rrt_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_rrt_w6 = 1 AND rrt_24h = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_rrt_w24 = 1 AND rrt_24h = 0 THEN 1 ELSE 0 END)
FROM tmp_ed_outcomes
""")
v = cur.fetchone()
print("LOGICAL CONSISTENCY (event_by_X_wN subset of X_24h):")
print("-" * 50)
consistency_labels = ["ICU w1<=24h","ICU w6<=24h","ICU w24=24h","Det w1<=24h","Det w6<=24h","Det w24=24h",
                      "Press w1<=24h","Press w6<=24h","Press w24<=24h","Vent w1<=24h","Vent w6<=24h","Vent w24<=24h",
                      "RRT w1<=24h","RRT w6<=24h","RRT w24<=24h"]
all_ok = True
for i, label in enumerate(consistency_labels):
    status = "PASS" if v[i] == 0 else f"FAIL ({v[i]} violations)"
    if v[i] != 0:
        all_ok = False
    print(f"  {label}: {status}")

# 5. Monotonicity: event_by_X_w1 <= event_by_X_w6 <= event_by_X_w24
cur.execute("""
SELECT
    SUM(CASE WHEN event_by_icu_w1 > event_by_icu_w6 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_icu_w6 > event_by_icu_w24 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_pressor_w1 > event_by_pressor_w6 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_pressor_w6 > event_by_pressor_w24 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_vent_w1 > event_by_vent_w6 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_vent_w6 > event_by_vent_w24 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_rrt_w1 > event_by_rrt_w6 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_rrt_w6 > event_by_rrt_w24 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_death_w1 > event_by_death_w6 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_death_w6 > event_by_death_w24 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_deterioration_w1 > event_by_deterioration_w6 THEN 1 ELSE 0 END),
    SUM(CASE WHEN event_by_deterioration_w6 > event_by_deterioration_w24 THEN 1 ELSE 0 END)
FROM tmp_ed_outcomes
""")
m = cur.fetchone()
print()
print("MONOTONICITY CHECK (w1 <= w6 <= w24 for each event type):")
print("-" * 50)
mono_labels = ["ICU w1>w6","ICU w6>w24","Press w1>w6","Press w6>w24",
               "Vent w1>w6","Vent w6>w24","RRT w1>w6","RRT w6>w24",
               "Death w1>w6","Death w6>w24","Det w1>w6","Det w6>w24"]
for i, label in enumerate(mono_labels):
    status = "PASS" if m[i] == 0 else f"FAIL ({m[i]} violations)"
    if m[i] != 0:
        all_ok = False
    print(f"  {label}: {status}")

# 6. Compare with aligned outcomes (event_by should be disjoint from aligned)
cur.execute("""
SELECT
    -- event_by_icu_w6=1 AND icu_24h_from_w6=1 means same person had ICU both within AND after W6
    SUM(CASE WHEN event_by_icu_w6 = 1 AND icu_24h_from_w6 = 1 THEN 1 ELSE 0 END) as icu_both,
    SUM(CASE WHEN event_by_deterioration_w6 = 1 AND deterioration_24h_from_w6 = 1 THEN 1 ELSE 0 END) as det_both,
    SUM(CASE WHEN event_by_icu_w6 = 1 THEN 1 ELSE 0 END) as icu_by_w6,
    SUM(CASE WHEN icu_24h_from_w6 = 1 THEN 1 ELSE 0 END) as icu_after_w6,
    SUM(CASE WHEN event_by_deterioration_w6 = 1 THEN 1 ELSE 0 END) as det_by_w6,
    SUM(CASE WHEN deterioration_24h_from_w6 = 1 THEN 1 ELSE 0 END) as det_after_w6
FROM tmp_ed_outcomes
""")
d = cur.fetchone()
print()
print("OVERLAP ANALYSIS (event_by vs aligned outcomes at W6):")
print("-" * 60)
print(f"  ICU by W6: {d[2]:,} | ICU 24h after W6: {d[3]:,} | Both: {d[0]:,}")
print(f"  Det by W6: {d[4]:,} | Det 24h after W6: {d[5]:,} | Both: {d[1]:,}")
print()
print(f"ALL VALIDATION CHECKS PASSED: {all_ok}")
conn.close()

