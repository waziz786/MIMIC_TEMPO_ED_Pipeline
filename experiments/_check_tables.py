"""Quick check of raw MIMIC-IV table structures for truncated feature building."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.db import get_conn
from src.utils import load_yaml
import pandas as pd

cfg = load_yaml('config/config.yaml')
conn = get_conn(cfg)

# 1. ed.vitalsign columns
df = pd.read_sql('SELECT * FROM mimiciv_ed.vitalsign LIMIT 2', conn)
print('=== ed.vitalsign columns ===')
print(df.columns.tolist())

# 2. hosp.labevents columns
df2 = pd.read_sql("""SELECT column_name FROM information_schema.columns 
                      WHERE table_schema='mimiciv_hosp' AND table_name='labevents' 
                      ORDER BY ordinal_position""", conn)
print('\n=== hosp.labevents columns ===')
print(df2['column_name'].tolist())

# 3. Cohort size
df3 = pd.read_sql('SELECT COUNT(*) as n FROM tmp_base_ed_cohort', conn)
print(f"\nCohort size: {df3['n'].iloc[0]}")

# 4. Outcome distribution
df4 = pd.read_sql('SELECT deterioration_24h, COUNT(*) as n FROM tmp_ed_outcomes GROUP BY 1 ORDER BY 1', conn)
print('\n=== Outcome distribution ===')
print(df4.to_string())

# 5. Lab itemid mapping - check what itemids we use
print('\n=== Lab itemids for key labs ===')
labs_sql = """
SELECT DISTINCT le.itemid, di.label
FROM mimiciv_hosp.labevents le
JOIN mimiciv_hosp.d_labitems di ON le.itemid = di.itemid
WHERE le.itemid IN (50912, 50813, 50868, 50802, 51006, 50882, 51301, 51222, 51265, 50809, 51003, 50885, 51237)
ORDER BY le.itemid
"""
df5 = pd.read_sql(labs_sql, conn)
print(df5.to_string())

# 6. Check ed.pyxis for medication data
df6 = pd.read_sql('SELECT * FROM mimiciv_ed.pyxis LIMIT 2', conn)
print('\n=== ed.pyxis columns ===')
print(df6.columns.tolist())

conn.close()
print('\nDone.')
