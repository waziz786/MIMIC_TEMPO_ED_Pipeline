# Quick Start: Loading MIMIC-IV, MIMIC-IV-ED, and ECG Data into PostgreSQL

**For first-time users who have downloaded data from PhysioNet (mkae sure to download the RIGHT dataset)**

---

## Prerequisites

✅ PostgreSQL 18+ installed and running  
✅ Data downloaded and extracted:
- `MIMIC-IV v3.1` → extracted to local folder 
- `MIMIC-IV-ED v2.2` → extracted to local folder  
- `MIMIC-IV-ECG: Diagnostic Electrocardiogram Matched Subset` → folder with `record_list.csv` and `machine_measurements.csv`
ECG data is open access whereas the other two require credential access
---

## Step 1: Set Up Your Environment

Open **PowerShell** in your pipeline directory and set your PostgreSQL password:

```powershell
$env:PGPASSWORD = "your_postgres_password"
```

---

## Step 2: Create Database and Schemas

```powershell
$env:PGPASSWORD = "your_postgres_password"

# Create main database
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d postgres -c "CREATE DATABASE mimiciv;"

# Create schemas
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "CREATE SCHEMA IF NOT EXISTS mimiciv_hosp; CREATE SCHEMA IF NOT EXISTS mimiciv_icu; CREATE SCHEMA IF NOT EXISTS mimiciv_ed; CREATE SCHEMA IF NOT EXISTS mimiciv_ecg;"
```

---

## Step 3: Load MIMIC-IV Hospital Data (v3.1)

### Step 3a: Create Tables

```powershell
$env:PGPASSWORD = "your_postgres_password"

& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -f mimic-code\mimic-iv\buildmimic\postgres\create.sql
```

**Output should show:**
```
CREATE SCHEMA
CREATE TABLE
CREATE TABLE
... (many more)
```

### Step 3b: Load Data from CSV Files

```powershell
# Replace with YOUR data path
$MIMIC_IV_PATH = "C:\Users\YourName\Downloads\mimic-iv-3.1\mimic-iv-3.1"

& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -v ON_ERROR_STOP=1 -v mimic_data_dir="$MIMIC_IV_PATH" -f mimic-code\mimic-iv\buildmimic\postgres\load.sql
```

**This will display:**
```
COPY 546028
COPY 89208
COPY 6364488
... (continues for ~20+ tables)
```

 **Takes 30-60 minutes** (depending on disk speed, may even take longer)

### Step 3c: (Optional) Add Constraints and Indexes

```powershell
# Add data integrity constraints
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -v ON_ERROR_STOP=1 -v mimic_data_dir="$MIMIC_IV_PATH" -f mimic-code\mimic-iv\buildmimic\postgres\constraint.sql

# Create indexes for faster queries
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -v ON_ERROR_STOP=1 -v mimic_data_dir="$MIMIC_IV_PATH" -f mimic-code\mimic-iv\buildmimic\postgres\index.sql
```

---

## Step 4: Load MIMIC-IV-ED Data (v2.2)

### Step 4a: Create ED Tables

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -f mimic-code\mimic-iv-ed\buildmimic\postgres\create.sql
```

### Step 4b: Load ED Data

```powershell
# Replace with YOUR data path
$MIMIC_ED_PATH = "C:\Users\YourName\Downloads\mimic-iv-ed-2.2\mimic-iv-ed-2.2"

& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -v ON_ERROR_STOP=1 -v mimic_data_dir="$MIMIC_ED_PATH" -f mimic-code\mimic-iv-ed\buildmimic\postgres\load.sql
```

 **Takes 10-15 minutes**

---

## Step 5: Load ECG Data

### Step 5a: Create ECG Tables

```powershell
$env:PGPASSWORD = "your_postgres_password"

& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "
CREATE TABLE mimiciv_ecg.record_list (
    subject_id INTEGER,
    study_id INTEGER,
    file_name VARCHAR(255),
    ecg_time TIMESTAMP,
    path VARCHAR(255)
);

CREATE TABLE mimiciv_ecg.machine_measurements (
    subject_id INTEGER,
    study_id INTEGER,
    cart_id INTEGER,
    ecg_time TIMESTAMP,
    report_0 TEXT, report_1 TEXT, report_2 TEXT, report_3 TEXT,
    report_4 TEXT, report_5 TEXT, report_6 TEXT, report_7 TEXT,
    report_8 TEXT, report_9 TEXT, report_10 TEXT, report_11 TEXT,
    report_12 TEXT, report_13 TEXT, report_14 TEXT, report_15 TEXT,
    report_16 TEXT, report_17 TEXT,
    bandwidth TEXT,
    filtering TEXT,
    rr_interval REAL,
    p_onset REAL,
    p_end REAL,
    qrs_onset REAL,
    qrs_end REAL,
    t_end REAL,
    p_axis REAL,
    qrs_axis REAL,
    t_axis REAL
);
"
```

### Step 5b: Load ECG CSV Files

```powershell
$env:PGPASSWORD = "your_postgres_password"

# Replace with YOUR data path
$ECG_PATH = "C:\Users\YourName\Downloads\MIMIC IV ECG DATA"

# Load record_list
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "\COPY mimiciv_ecg.record_list FROM '$ECG_PATH\record_list.csv' WITH (FORMAT csv, HEADER true);"

# Load machine_measurements
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "\COPY mimiciv_ecg.machine_measurements FROM '$ECG_PATH\machine_measurements.csv' WITH (FORMAT csv, HEADER true);"
```

 **Takes 5-7 minutes**

---

## Step 6: Verify All Data Loaded

```powershell
$env:PGPASSWORD = "your_postgres_password"

# Check table counts
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "
SELECT schemaname, COUNT(*) as table_count 
FROM pg_tables 
WHERE schemaname LIKE 'mimiciv%' 
GROUP BY schemaname 
ORDER BY schemaname;
"
```

**Expected output:**
```
  schemaname  | table_count
--------------+-------------
 mimiciv_ed   |           6
 mimiciv_ecg  |           2
 mimiciv_hosp |          22
 mimiciv_icu  |           9
(4 rows)
```

Check key row counts:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "
SELECT 'patients' AS table_name, COUNT(*) FROM mimiciv_hosp.patients
UNION ALL
SELECT 'admissions', COUNT(*) FROM mimiciv_hosp.admissions
UNION ALL
SELECT 'labevents', COUNT(*) FROM mimiciv_hosp.labevents
UNION ALL
SELECT 'icustays', COUNT(*) FROM mimiciv_icu.icustays
UNION ALL
SELECT 'edstays', COUNT(*) FROM mimiciv_ed.edstays
UNION ALL
SELECT 'ecg_records', COUNT(*) FROM mimiciv_ecg.record_list;
"
```

---

## Typical Timings

| Step | Time | Status |
|------|------|--------|
| Create schemas | < 1 min | ✅ Fast |
| Create HOSP tables | < 1 min | ✅ Fast |
| Load HOSP data | 30-60 min | ⏳ Takes time (large) |
| Create ED tables | < 1 min | ✅ Fast |
| Load ED data | 5-10 min | ⏳ Takes time |
| Create ECG tables | < 1 min | ✅ Fast |
| Load ECG data | 2-5 min | ✅ Moderate |
| **TOTAL** | **>1hr** | ✅ One-time setup |

---

## Troubleshooting

### "password authentication failed"
Update the password variable:
```powershell
$env:PGPASSWORD = "your_correct_password"
```

### "No such file or directory"
Make sure paths match your actual download locations:
```powershell
# Verify your paths exist
Test-Path "C:\Users\YourName\Downloads\mimic-iv-3.1"
Test-Path "C:\Users\YourName\Downloads\mimic-iv-ed-2.2"
Test-Path "C:\Users\YourName\Downloads\MIMIC IV ECG DATA"
```

### "relation already exists"
Drop and recreate ( **very careful - deletes data**):
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "DROP SCHEMA mimiciv_hosp CASCADE;"
```

---

## Next Steps

Once data is loaded:

1. **Verify database** → Run verification queries above
2. **Configure pipeline** → Update config.yaml with database credentials
3. **Run pipeline** → Execute your analysis scripts
4. **Query data** → Use DBeaver or SQL clients to explore

---

## Command Reference

**Set password:**
```powershell
$env:PGPASSWORD = "your_password"
```

**Connect to database:**
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv
```

**Run SQL file:**
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -f path\to\file.sql
```

**Run SQL command:**
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -d mimiciv -c "SELECT COUNT(*) FROM table_name;"
```

---

 
**For:** First-time MIMIC-IV users using the pipeline
