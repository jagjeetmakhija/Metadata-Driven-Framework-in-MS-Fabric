# Jagjeet Pipeline Operations Tracker
### A Self-Installing Pipeline Monitoring Framework for Microsoft Fabric
**Version 3.4.3 · Built by Jagjeet · Microsoft Fabric + Power BI**

---

## What Is This?

The **Jagjeet Pipeline Operations Tracker** is a lightweight, self-installing monitoring framework for Microsoft Fabric. Upload one Python file to your notebook, run two lines of code, and you instantly get:

- ✅ A **Lakehouse** with three Delta tables
- ✅ A **Power BI Direct Lake semantic model** with relationships and 8 DAX measures
- ✅ A single method call to log any pipeline run — success or failure

No configuration files. No manual table creation. No separate Power BI setup. Everything is created automatically on first run.

---

## The Three Files

| File | Role | When to Use |
|---|---|---|
| `jagjeet_logging_utils.py` | **Core engine** — the only file required in production | Every notebook that needs tracking |
| `jagjeet_test_data_creation.py` | Sample data generator — populates the framework with realistic dummy data | Once, during setup/testing |
| `jagjeet_usage_examples.py` | 8 copy-paste usage patterns | Reference when building your own notebooks |

---

## What Gets Created

When you run `JagjeetPipelineTracker("MyProject")` for the first time, the following Fabric artifacts are created **automatically**:

```
YOUR FABRIC WORKSPACE
│
├── 📦 LH_MyProject_Jagjeet_PipelineOps        ← Lakehouse
│       ├── 📋 pipeline_activity_log             ← Fact table (empty, fills as you log)
│       ├── 📅 date_dimension                    ← 4 years of dates pre-filled
│       └── ⏰ time_dimension                    ← 1,440 minute slots pre-filled
│
└── 📊 SM_MyProject_Jagjeet_PipelineOps         ← Power BI Semantic Model
        ├── 🔗 pipeline_activity_log → date_dimension   (Many-to-One)
        ├── 🔗 pipeline_activity_log → time_dimension   (Many-to-One)
        └── 📐 8 DAX Measures
                ├── Pipeline Overview:      Total Pipeline Runs, Total Rows Processed
                │                           Active Tables, Active Notebooks
                ├── Performance:            Average Run Duration
                ├── Quality & Reliability:  Failed Runs, Success Rate
                └── Time Intelligence:      Runs Today
```

> **Smart behaviour:** If any of these already exist, they are preserved. Existing data is never overwritten unless you pass `force_recreate=True`.

---

## Prerequisites

Before running anything, make sure you have:

- [ ] A **Microsoft Fabric workspace** with Spark compute enabled
- [ ] A **Fabric notebook** open and attached to a lakehouse (or let the framework create one)
- [ ] The file `jagjeet_logging_utils.py` uploaded to your notebook's **Resources** folder

> **Note:** `semantic-link-labs` (`jags_labs`) is installed automatically if missing — you do not need to pip install anything manually.

---

---

# EXECUTION GUIDE

---

## PHASE 1 — First-Time Setup

### Step 1 · Upload the core file

Upload `jagjeet_logging_utils.py` to your Fabric notebook's **Resources** folder.

```
Notebook → Resources (left panel) → Upload → jagjeet_logging_utils.py
```

This is the **only file required for production**. The other two files are optional tools.

---

### Step 2 · Initialise the tracker

In a new notebook cell, run:

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py  |  Class: JagjeetPipelineTracker
# Lines: 58–142  |  Methods: __init__() + _setup()
# ─────────────────────────────────────────────────────

from builtin.jagjeet_logging_utils import JagjeetPipelineTracker

tracker = JagjeetPipelineTracker("MyProject")
```

**What happens when this runs:**

| Step | What the Framework Does | Code Reference |
|---|---|---|
| 1 | Detects your Fabric workspace ID | `_setup()` → line 93 |
| 2 | Finds or creates the lakehouse `LH_MyProject_Jagjeet_PipelineOps` | `_setup()` → lines 98–128 |
| 3 | Creates `pipeline_activity_log` fact table | `_create_activity_log_table()` → line 187 |
| 4 | Creates `date_dimension` (4 years of dates) | `_create_date_dimension_table()` → line 206 |
| 5 | Creates `time_dimension` (1,440 minute slots) | `_create_time_dimension_table()` → line 246 |
| 6 | Polls until all 3 tables are readable | `_verify_all_tables_ready()` → line 152 |
| 7 | Authenticates using Fabric native token | `_authenticate_for_tom()` → line 176 |
| 8 | Builds the Direct Lake Power BI semantic model | `_build_power_bi_model()` → line 279 |
| 9 | Adds 2 relationships and 8 DAX measures | `_add_model_relationships()` → line 305 |

**Expected output:**
```
============================================================
JAGJEET PIPELINE OPERATIONS TRACKER v3.4.3
============================================================
  • Project:        MyProject
  • Lakehouse:      LH_MyProject_Jagjeet_PipelineOps
  • Semantic Model: SM_MyProject_Jagjeet_PipelineOps

  Workspace: YourWorkspaceName

  Tables:
    pipeline_activity_log: created
    date_dimension: created (4 years)
    time_dimension: created (1,440 slots)

    ✓ pipeline_activity_log: 0 rows
    ✓ date_dimension: 1,461 rows
    ✓ time_dimension: 1,440 rows

  Power BI model created: SM_MyProject_Jagjeet_PipelineOps
  8 DAX measures applied
  Model refreshed

============================================================
Ready. Use: tracker.track_pipeline_run(...)
============================================================
```

> **Second run onwards:** If the lakehouse and tables already exist, the framework detects them and preserves all data. Setup takes only a few seconds on subsequent runs.

---

### Step 3 · Verify setup

Run a health check to confirm everything is working:

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: show_health_check()  |  Line: 470
# ─────────────────────────────────────────────────────

tracker.show_health_check()
```

**Expected output:**
```
============================================================
JAGJEET PIPELINE TRACKER — HEALTH CHECK
============================================================
  Project:   MyProject
  Lakehouse: LH_MyProject_Jagjeet_PipelineOps
  Model:     SM_MyProject_Jagjeet_PipelineOps
  Workspace: YourWorkspaceName

  Tables:
    pipeline_activity_log : 0 records
    date_dimension        : 1,461 dates (up to 2027-XX-XX)
    time_dimension        : 1,440 slots

  Power BI Model: EXISTS
============================================================
```

---

---

## PHASE 2 — Populate with Test Data (Optional)

> Skip this phase if you want to start logging real pipeline runs immediately. Use this phase to verify your Power BI reports before real data exists.

### Step 4 · Upload the test data file

Upload `jagjeet_test_data_creation.py` to your notebook's **Resources** folder.

---

### Step 5 · Run a quick smoke test (5 entries)

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_test_data_creation.py
# Function: quick_test()  |  Line: 158
# Generates: 5 test entries to verify tracker is working
# ─────────────────────────────────────────────────────

from builtin.jagjeet_test_data_creation import quick_test

tracker = quick_test("MyProject")
```

**What it creates:**
```
[OK] INSERT   | customers | Δ+1,000 rows | 5.2s
[OK] UPDATE   | orders    | Δ+100 rows   | 8.7s
[OK] VALIDATE | products  | Δ0 rows      | 3.1s
[OK] MERGE    | sales     | Δ+500 rows   | 12.4s
[OK] DELETE   | inventory | Δ-50 rows    | 2.8s

✅ Quick test complete
```

---

### Step 6 · Run full test environment (optional — 80+ entries)

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_test_data_creation.py
# Function: setup_complete_test_environment()  |  Line: 145
# Generates:
#   - 30 random runs across 8 notebooks / 10 tables    → create_test_data()        line 10
#   - 4-stage daily ETL + quality + merge scenarios     → create_etl_pipeline_scenarios() line 76
#   - 5 realistic failure cases                         → create_failure_scenarios() line 109
#   - 20 performance benchmarks (4 sizes × 5 ops)      → create_performance_benchmarks() line 126
# ─────────────────────────────────────────────────────

from builtin.jagjeet_test_data_creation import setup_complete_test_environment

tracker = setup_complete_test_environment("MyProject")
```

**What it creates:**

| Function | What It Generates | Line |
|---|---|---|
| `create_test_data(tracker, 30)` | 30 random runs — 90% success / 10% failure, spread over 30 days | 10 |
| `create_etl_pipeline_scenarios(tracker)` | Daily ETL (Extract → Transform → Load → Validate) + quality failure + slow aggregation + incremental merge | 76 |
| `create_failure_scenarios(tracker)` | API 429, schema mismatch, OOM, PK violation, DB timeout | 109 |
| `create_performance_benchmarks(tracker)` | SELECT / JOIN / AGGREGATE / SORT / MERGE across 1K → 10M row datasets | 126 |

---

---

## PHASE 3 — Log Real Pipeline Runs

### Step 7 · Log a successful run

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: track_pipeline_run()  |  Line: 369
# Reference: jagjeet_usage_examples.py  |  Example 1  |  Line: 14
# ─────────────────────────────────────────────────────

tracker.track_pipeline_run(
    notebook_name    = "DailyETL",           # which notebook ran
    table_name       = "sales_fact",         # which table was affected
    operation_type   = "INSERT",             # INSERT / UPDATE / DELETE / MERGE / VALIDATE etc.
    rows_before      = 1000000,              # row count before
    rows_after       = 1005000,              # row count after (rows_changed auto-calculated)
    duration_seconds = 67.2,                 # how long it took
    run_message      = "Daily load — 5,000 new rows added"
)
```

**Output:**
```
  [OK] INSERT | sales_fact | Δ+5,000 rows | 67.2s | 2025-09-15
```

---

### Step 8 · Log a failed run

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: track_pipeline_run()  |  Line: 369
# Parameter: error_details  |  Line: 372
# Reference: jagjeet_usage_examples.py  |  Example 1  |  Line: 20
# ─────────────────────────────────────────────────────

tracker.track_pipeline_run(
    notebook_name    = "DataValidation",
    table_name       = "orders",
    operation_type   = "VALIDATE",
    rows_before      = 100000,
    rows_after       = 98500,
    duration_seconds = 25.4,
    error_details    = "1,500 records failed — missing customer_id",   # triggers FAILED status
    run_message      = "Validation flagged data quality issues"
)
```

**Output:**
```
  [FAILED] VALIDATE | orders | Δ-1,500 rows | 25.4s | 2025-09-15
```

> When `error_details` is populated, the entry is marked **FAILED**. The Power BI measures **Failed Runs** and **Success Rate** pick this up automatically.

---

### Step 9 · Auto-time your functions with the decorator

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Function: measure_execution_time  |  Line: 500
# Reference: jagjeet_usage_examples.py  |  Example 3  |  Line: 41
# ─────────────────────────────────────────────────────

from builtin.jagjeet_logging_utils import JagjeetPipelineTracker, measure_execution_time

@measure_execution_time
def load_sales_data():
    # your actual ETL logic here
    df = spark.sql("SELECT * FROM source.sales")
    df.write.format("delta").mode("append").saveAsTable("sales_fact")
    return {"rows_loaded": df.count()}

# Call it — get back (result, duration_seconds, error)
result, duration, error = load_sales_data()

# Log — duration captured automatically even if function failed
tracker.track_pipeline_run(
    notebook_name    = "SalesETL",
    table_name       = "sales_fact",
    operation_type   = "LOAD",
    rows_after       = result["rows_loaded"] if not error else 0,
    duration_seconds = duration,                 # auto-captured
    error_details    = error                     # None if successful
)
```

---

### Step 10 · Backfill historical data

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Parameter: custom_timestamp  |  Line: 373
# Reference: jagjeet_usage_examples.py  |  Example 2  |  Line: 29
# ─────────────────────────────────────────────────────

from datetime import datetime, timedelta

for i in range(7):
    past_date = datetime.now() - timedelta(days=i)
    tracker.track_pipeline_run(
        notebook_name    = "DailyETL",
        table_name       = "sales_fact",
        operation_type   = "REFRESH",
        rows_before      = 100000,
        rows_after       = 100000 + (i * 1000),
        duration_seconds = 15.0 + (i * 2),
        run_message      = f"Daily refresh — {past_date.strftime('%Y-%m-%d')}",
        custom_timestamp = past_date          # overrides the log timestamp
    )
```

> Use `custom_timestamp` when deploying to an existing project that already has a history of pipeline runs you want to represent in Power BI.

---

### Step 11 · Track a multi-stage ETL pipeline

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_usage_examples.py
# Function: run_etl_pipeline()  |  Line: 65
# Pattern: Log each ETL stage separately with staged timestamps
# ─────────────────────────────────────────────────────

from datetime import datetime, timedelta

pipeline_start = datetime.now()

# Stage 1 — Extract
tracker.track_pipeline_run(
    notebook_name="ETL_Pipeline", table_name="source_system",
    operation_type="EXTRACT", rows_before=0, rows_after=50000,
    duration_seconds=45.2, run_message="Extracted from source",
    custom_timestamp=pipeline_start
)

# Stage 2 — Transform (runs 30 min later)
tracker.track_pipeline_run(
    notebook_name="ETL_Pipeline", table_name="staging_area",
    operation_type="TRANSFORM", rows_before=50000, rows_after=49500,
    duration_seconds=120.7, run_message="Business rules applied",
    custom_timestamp=pipeline_start + timedelta(minutes=30)
)

# Stage 3 — Load (runs 45 min after transform)
tracker.track_pipeline_run(
    notebook_name="ETL_Pipeline", table_name="data_warehouse",
    operation_type="LOAD", rows_before=1000000, rows_after=1049500,
    duration_seconds=85.3, run_message="Loaded to warehouse",
    custom_timestamp=pipeline_start + timedelta(minutes=75)
)
```

---

### Step 12 · Track a batch with mixed outcomes

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_usage_examples.py
# Function: track_batch()  |  Line: 115
# Pattern: Loop through tables, log each, then log a batch summary
# ─────────────────────────────────────────────────────

batch_start = datetime.now()
batch_id    = f"BATCH_{batch_start.strftime('%Y%m%d_%H%M%S')}"

tables = [
    {"name": "customers",    "records": 50000,  "success": True,  "duration": 30.5},
    {"name": "orders",       "records": 200000, "success": True,  "duration": 45.2},
    {"name": "products",     "records": 10000,  "success": False, "duration": 5.1},
    {"name": "transactions", "records": 500000, "success": True,  "duration": 120.3}
]

for i, t in enumerate(tables):
    tracker.track_pipeline_run(
        notebook_name="BatchPipeline", table_name=t["name"],
        operation_type="BATCH_LOAD",
        rows_before=0, rows_after=t["records"] if t["success"] else 0,
        duration_seconds=t["duration"],
        run_message=f"[{batch_id}] {t['name']} — {'ok' if t['success'] else 'failed'}",
        error_details=None if t["success"] else f"Schema validation failed for {t['name']}",
        custom_timestamp=batch_start + timedelta(minutes=i * 15)
    )

# Batch summary entry
successful = sum(1 for t in tables if t["success"])
tracker.track_pipeline_run(
    notebook_name="BatchPipeline", table_name="batch_control_log",
    operation_type="BATCH_COMPLETE",
    duration_seconds=sum(t["duration"] for t in tables),
    run_message=f"[{batch_id}] {successful}/{len(tables)} tables succeeded",
    custom_timestamp=batch_start + timedelta(hours=2)
)
```

---

---

## PHASE 4 — Monitor and Maintain

### Step 13 · View recent runs

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: show_recent_runs()  |  Line: 401
# Calls:  fetch_activity_logs()  |  Line: 394
# ─────────────────────────────────────────────────────

tracker.show_recent_runs(10)         # show last 10 runs
```

---

### Step 14 · Get summary statistics

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: show_summary_stats()  |  Line: 405
# ─────────────────────────────────────────────────────

tracker.show_summary_stats()
```

**Output:**
```
Pipeline Activity Summary:
  Total Runs:         1,247
  Unique Notebooks:   8
  Unique Tables:      15
  Unique Operations:  7
```

---

### Step 15 · Query specific logs

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: fetch_activity_logs()  |  Line: 394
# Reference: jagjeet_usage_examples.py  |  Example 4  |  Line: 57
# Returns a Spark DataFrame — fully chainable
# ─────────────────────────────────────────────────────

# Filter by table
df = tracker.fetch_activity_logs(table_name="sales_fact", limit=50)
df.show()

# Filter by operation type
df = tracker.fetch_activity_logs(operation_type="VALIDATE")
df.show()

# All failures
df = tracker.fetch_activity_logs(limit=200)
failures = df.filter(df.error_details.isNotNull())
failures.show()
```

---

### Step 16 · Refresh the Power BI model

Run this after adding new tables, changing schemas, or updating DAX measure definitions:

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: refresh_power_bi_model()  |  Line: 441
# What it does:
#   1. Re-authenticates with Fabric native token  → line 176
#   2. Re-applies 2 relationships                 → line 305
#   3. Re-applies / updates 8 DAX measures        → line 337
#   4. Triggers dataset refresh                   → line 449
# Reference: jagjeet_usage_examples.py  |  Example 8  |  Line: 145
# ─────────────────────────────────────────────────────

tracker.refresh_power_bi_model()
```

---

### Step 17 · Build the Power BI model manually (if setup was skipped)

If the model was not created during initial setup (because tables weren't ready in time), run:

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: build_power_bi_model()  |  Line: 456
# Waits up to 5 minutes for all tables to be readable, then builds
# ─────────────────────────────────────────────────────

tracker.build_power_bi_model(max_wait_minutes=5)
```

---

### Step 18 · Clean up old records

Run this on a schedule to keep the activity log lean:

```python
# ─────────────────────────────────────────────────────
# FILE: jagjeet_logging_utils.py
# Method: purge_old_records()  |  Line: 423
# Uses Delta delete + vacuum(0) to physically remove old data
# ─────────────────────────────────────────────────────

tracker.purge_old_records(days_to_keep=90)    # delete anything older than 90 days
```

---

---

## PHASE 5 — Power BI Reports

After setup, open **SM_MyProject_Jagjeet_PipelineOps** in Power BI. The following measures are pre-built and ready to use:

| Measure | Use In Report | DAX (jagjeet_logging_utils.py line 337) |
|---|---|---|
| `Total Pipeline Runs` | KPI card | `COUNTROWS(pipeline_activity_log)` |
| `Total Rows Processed` | KPI card | `SUM(rows_changed)` |
| `Active Tables` | KPI card | `DISTINCTCOUNT(table_name)` |
| `Active Notebooks` | KPI card | `DISTINCTCOUNT(notebook_name)` |
| `Average Run Duration` | Bar chart by notebook | `AVERAGE(duration_seconds)` |
| `Failed Runs` | Alert card | `CALCULATE(...NOT ISBLANK error_details)` |
| `Success Rate` | Gauge | `DIVIDE(successful rows, total rows)` |
| `Runs Today` | Live KPI | `CALCULATE(...run_date = TODAY)` |

**Suggested report pages:**

```
Page 1 — Overview
  ├── Total Pipeline Runs  (KPI)
  ├── Success Rate          (Gauge)
  ├── Failed Runs           (KPI with alert)
  └── Runs Today            (KPI)

Page 2 — Performance
  ├── Average Run Duration by Notebook  (Bar chart)
  ├── Total Rows Processed over Time    (Line chart)
  └── Duration trend by operation_type  (Column chart)

Page 3 — Failures
  ├── Table: all runs where error_details is not blank
  └── Failure count by notebook         (Bar chart)

Page 4 — Activity Log
  └── Full table: all columns from pipeline_activity_log
      filtered by date_dimension and time_dimension
```

---

---

## Complete Method Reference

All methods below are in `jagjeet_logging_utils.py`.

### `JagjeetPipelineTracker(project_name, force_recreate, workspace_name)`
**Line 58–77** — Class constructor. Triggers full setup sequence.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `project_name` | str | required | Names the lakehouse and semantic model |
| `force_recreate` | bool | `False` | If `True`, drops and rebuilds everything — **data is lost** |
| `workspace_name` | str | `None` | Auto-detected if not provided |

---

### `track_pipeline_run(...)`
**Line 369** — Core logging method. Always appends, never overwrites.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `notebook_name` | str | required | Which notebook ran |
| `table_name` | str | required | Which table was affected |
| `operation_type` | str | required | INSERT / UPDATE / DELETE / MERGE / VALIDATE / EXTRACT / TRANSFORM / LOAD / etc. |
| `rows_before` | int | `0` | Row count before operation |
| `rows_after` | int | `0` | Row count after — `rows_changed` auto-calculated |
| `duration_seconds` | float | `0.0` | Execution time. Use `@measure_execution_time` to auto-capture |
| `run_message` | str | `None` | Free-text description |
| `error_details` | str | `None` | Populate if failed — triggers FAILED status in Power BI |
| `run_by` | str | `None` | Auto-detected from `getpass` if not provided |
| `custom_timestamp` | datetime | `None` | Override run timestamp for historical backfill |

---

### Monitoring Methods

| Method | Line | Description |
|---|---|---|
| `show_recent_runs(limit=10)` | 401 | Print last N runs |
| `show_summary_stats()` | 405 | Print totals: runs, notebooks, tables, operations |
| `fetch_activity_logs(table_name, operation_type, limit)` | 394 | Return filtered Spark DataFrame |
| `show_health_check()` | 470 | Full status: table counts, model existence, available methods |
| `purge_old_records(days_to_keep=90)` | 423 | Delta delete + vacuum for records older than N days |
| `refresh_power_bi_model()` | 441 | Re-apply relationships + measures + trigger refresh |
| `build_power_bi_model(max_wait_minutes=5)` | 456 | Wait for tables, then build model |

---

### Utility Functions

| Function | Line | Description |
|---|---|---|
| `measure_execution_time(func)` | 500 | Decorator — returns `(result, duration_seconds, error)` |
| `resolve_current_user()` | 494 | Auto-detect current Fabric/system user |

---

---

## File Reference Map

Every function, example and pattern in these files — with exact line numbers:

### `jagjeet_logging_utils.py` — 511 lines

```
Line  14  →  ensure_package()                   Auto-install jags_labs if missing
Line  37  →  LOG_SCHEMA                         Shared 13-column Delta schema
Line  58  →  class JagjeetPipelineTracker       Main class
Line  61  →    __init__()                        Constructor — set names, call _setup()
Line  82  →    _date_dim_path (property)         Computed path for date_dimension table
Line  86  →    _time_dim_path (property)         Computed path for time_dimension table
Line  90  →    _setup()                          Bootstrap: workspace → lakehouse → tables → model
Line 145  →    _delta_table_exists()             Check if Delta path is readable
Line 152  →    _verify_all_tables_ready()        Poll until all 3 tables confirm readable
Line 176  →    _authenticate_for_tom()           Get Fabric native token for TOM operations
Line 187  →    _create_activity_log_table()      Create/preserve pipeline_activity_log
Line 206  →    _create_date_dimension_table()    Create/extend 4-year date dimension
Line 246  →    _create_time_dimension_table()    Create 1,440-slot time dimension
Line 272  →    _power_bi_model_exists()          Check if SM already exists in workspace
Line 279  →    _build_power_bi_model()           Build Direct Lake model → relationships → measures
Line 305  →    _add_model_relationships()        Create 2 Many-to-One joins via TOM
Line 337  →    _add_dax_measures()               Create/update 8 DAX measures via TOM
Line 369  →    track_pipeline_run()              ★ Core public method — append one log row
Line 394  →    fetch_activity_logs()             Return filtered Spark DataFrame
Line 401  →    show_recent_runs()                Show last N runs
Line 405  →    show_summary_stats()              Print aggregate counts
Line 423  →    purge_old_records()               Delta delete + vacuum old records
Line 441  →    refresh_power_bi_model()          Re-apply relationships + measures + refresh
Line 456  →    build_power_bi_model()            Poll + build — safe to call any time
Line 470  →    show_health_check()               Full framework status report
Line 494  →  resolve_current_user()              Get current Fabric/system user
Line 500  →  measure_execution_time()            ★ Decorator — auto-time any function
```

---

### `jagjeet_test_data_creation.py` — 180 lines

```
Line   5  →  import JagjeetPipelineTracker       Imports from jagjeet_logging_utils.py
Line  10  →  create_test_data(tracker, 50)        50 random runs — 8 notebooks, 10 tables, 12 ops
Line  76  →  create_etl_pipeline_scenarios()      4-stage ETL + quality failure + merge + backup
Line 109  →  create_failure_scenarios()           5 failure types with realistic error messages
Line 126  →  create_performance_benchmarks()      5 ops × 4 dataset sizes (1K → 10M rows)
Line 145  →  setup_complete_test_environment()    ★ Master function — runs all 4 above
Line 158  →  quick_test()                         ★ 5-entry smoke test — use this first
```

---

### `jagjeet_usage_examples.py` — 168 lines

```
Line   5  →  import JagjeetPipelineTracker        Imports from jagjeet_logging_utils.py
Line  10  →  tracker = JagjeetPipelineTracker(...)  Initialise once
Line  13  →  Example 1: Basic logging              Success and failure with error_details
Line  28  →  Example 2: Historical backfill         custom_timestamp for past dates
Line  40  →  Example 3: Auto-timing decorator       @measure_execution_time pattern
Line  57  →  Example 4: Monitoring                  show_recent_runs, show_summary_stats, fetch_activity_logs
Line  64  →  Example 5: Multi-stage ETL             3 stages with staged timestamps
Line  86  →  Example 6: Failure + retry + recovery  Initial fail → 2 retries → success
Line 114  →  Example 7: Batch processing            4 tables, 1 failure, batch summary entry
Line 145  →  Example 8: Power BI model management   refresh_power_bi_model + TOM inspection
```

---

---

## Execution Order Summary

```
FIRST TIME
  1.  Upload jagjeet_logging_utils.py → Resources folder
  2.  Run: tracker = JagjeetPipelineTracker("MyProject")
  3.  Run: tracker.show_health_check()

POPULATE TEST DATA (optional)
  4.  Upload jagjeet_test_data_creation.py → Resources folder
  5.  Run: quick_test("MyProject")                         ← smoke test (5 entries)
  6.  Run: setup_complete_test_environment("MyProject")    ← full test data (80+ entries)

PRODUCTION LOGGING (in every pipeline notebook)
  7.  from builtin.jagjeet_logging_utils import JagjeetPipelineTracker
  8.  tracker = JagjeetPipelineTracker("MyProject")
  9.  tracker.track_pipeline_run(...)  ← after every operation

MONITOR
  10. tracker.show_recent_runs(10)
  11. tracker.show_summary_stats()
  12. tracker.show_health_check()

MAINTAIN
  13. tracker.refresh_power_bi_model()          ← run after schema changes
  14. tracker.purge_old_records(90)             ← run on a schedule
```

---

---

## What This Framework Does NOT Do

| What | Why |
|---|---|
| Auto-capture ADF pipeline runs | Requires explicit `track_pipeline_run()` call from inside a Fabric notebook activity |
| Monitor Spark job metrics | Not connected to Spark UI or execution plans |
| Create Power BI reports or dashboards | Only the semantic model is created — you build reports on top |
| Run automatically on a schedule | Logging only happens when you call the method |
| Connect to external monitoring tools | Self-contained, no third-party integrations |

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'builtin'` | File not in Resources folder | Upload `jagjeet_logging_utils.py` to notebook Resources |
| `Power BI model not created` | Tables weren't ready in time during setup | Run `tracker.build_power_bi_model()` |
| `TOM auth failed` | `notebookutils` not available | Must run inside a Fabric notebook, not locally |
| `Model not found. Run build_power_bi_model() first` | Model was skipped or deleted | Run `tracker.build_power_bi_model()` |
| `Refresh failed` | Fabric dataset refresh quota | Retry after a few minutes or refresh manually in Power BI |

---

## Built By

**Jagjeet** · Jagjeet Pipeline Operations Tracker v3.4.3

`Microsoft Fabric` · `PySpark` · `Delta Lake` · `Power BI Direct Lake` · `TOM (Tabular Object Model)`
