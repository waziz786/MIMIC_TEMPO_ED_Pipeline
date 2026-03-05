# Pipeline Validation Summary

## Latest Pipeline Run: 2026-02-20 (Post SQL Patch)

### Pipeline Completed Successfully
- **Runtime**: 9m 53s
- **Base cohort**: 424,385 ED visits (205,080 unique patients)
- **Events**: 81,930
- **Event types**: 9
- **Deterioration rate (24h from W6)**: 4.05%

---

## SQL Pipeline Patches Applied

### 1. ECG Features W1/W6
- Added defensive `ecg_time IS NOT NULL` filter
- Time window already used `LEAST(ed_outtime, ed_intime + interval)` — confirmed correct

### 2. Feature Baskets W1/W6/W24
- Added `LEAST(ed_outtime, ...)` clamp to vitals, labs, and pyxis time windows
- Prevents features from being pulled after patient left the ED
- **W1**: `LEAST(b.ed_outtime, b.ed_intime + INTERVAL '1 hour')`
- **W6**: `LEAST(b.ed_outtime, b.ed_intime + INTERVAL '6 hours')`
- **W24**: `LEAST(b.ed_outtime, b.ed_intime + INTERVAL '24 hours')`

### 3. Base Cohort
- Added age upper bound: `age_at_ed <= 110` (cap implausible ages)
- Added alive-at-arrival check: `dod IS NULL OR ed_intime <= dod`

### 4. ICD Event SQLs (ACS, Revasc, Cardiac Arrest)
- Already correct: `NULL::timestamp AS event_time` and `'none'::text AS event_time_type`

### 5. Death Event SQL
- Already correct: uses `admissions.deathtime` (not `patients.dod`)

### 6. Event Column Alignment Fix (DISCOVERED DURING QA)
- **Bug found**: Timed event SQLs (ICU, pressor, vent, RRT, death) had 7 columns but event_log table has 8
- Missing `event_time_type` column caused positional shift: `source_table` → `event_time_type`, `event_detail` → `source_table`
- **Fix**: Added `'exact'::text AS event_time_type` to all 5 timed event SQLs

### 7. Outcomes `ev` CTE
- Added `event_time >= b.ed_intime` (no events before ED arrival)
- Added `event_time <= b.ed_intime + INTERVAL '30 days'` (cap at 30 days)

### 8. QA Checks SQL Created (`sql/99_qa_checks.sql`)
Eight automated assertions run after every pipeline build:

| Check | Description | Status |
|-------|-------------|--------|
| events_before_ed | No timed events before ED arrival | PASS |
| icd_events_with_time | ICD events have NULL event_time | PASS |
| event_by_monotonicity | W1 <= W6 <= W24 monotonicity | PASS |
| age_out_of_bounds | All patients 18-110 | PASS |
| dead_before_ed | No deceased patients at ED arrival | PASS |
| ecg_outside_ed_window | ECGs within ED window | PASS |
| label_event_by_overlap | Multi-admission cases (INFO) | 109 cases |
| death_source | Death uses admissions.deathtime | PASS* |

*death_source check identified the column alignment bug (now fixed)

---

## Prior Patches (Still Active)

### ICD Outcomes: Time-Windowed → Hospitalization-Level
| Old Name | New Name | Change |
|----------|----------|--------|
| `cardiac_arrest_24h` | `cardiac_arrest_hosp` | No time window |
| `acs_72h` | `acs_hosp` | No time window |
| `revasc_72h` | `revasc_hosp` | No time window |
| `pci_72h` | `pci_hosp` | No time window |
| `cabg_72h` | `cabg_hosp` | No time window |
| `coronary_event_72h` | `coronary_event_hosp` | No time window |

### Event Log
- **event_time_type**: 'exact' (timed events) or 'none' (ICD events)
- ICD events: event_time = NULL (honest representation)
- Timed events: event_time_type = 'exact' (column alignment now correct)

### Deterioration Composites
- `deterioration_24h` = ICU + pressor + vent + RRT + death (NO cardiac arrest)
- Cardiac arrest: separate `cardiac_arrest_hosp`

---

## Benchmark Methodology Fixes (part2_option_a_benchmarks.py)

| Issue | Fix |
|-------|-----|
| Inner validation: random permutation | Changed to GroupKFold(n_splits=2) |
| Bootstrap CI: row-level | Changed to cluster bootstrap at subject_id level |
| LabelEncoder: fit on full dataset | Moved inside CV folds with unknown handling |
| XGB: unnecessary StandardScaler | Removed scaling for XGB (raw data) |
| Admitted cohort: upsampling duplicates | Removed all filtering, 100k for ALL |

---

## Action: Rerun Pipeline

After the event_time_type column alignment fix, rerun to get clean QA checks:
```powershell
python -m src.main
```
