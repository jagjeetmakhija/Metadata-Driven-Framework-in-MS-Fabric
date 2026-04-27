# jagjeet_usage_examples.py
# Usage patterns for the Jagjeet Pipeline Operations Tracker v3.4.3
# Copy individual examples into your Fabric notebooks as needed

from builtin.jagjeet_logging_utils import JagjeetPipelineTracker, measure_execution_time
from datetime import datetime, timedelta
import time

# Initialise once — creates lakehouse, tables and Power BI model automatically
tracker = JagjeetPipelineTracker("CustomerAnalytics")


# ── Example 1: Basic success and failure logging ──────────────────────────────
tracker.track_pipeline_run(
    notebook_name="DataIngestion", table_name="customers", operation_type="INSERT",
    rows_before=0, rows_after=10000, duration_seconds=12.5,
    run_message="Initial customer load from CRM"
)

tracker.track_pipeline_run(
    notebook_name="DataValidation", table_name="orders", operation_type="VALIDATE",
    rows_before=5000, rows_after=4850, duration_seconds=3.2,
    error_details="150 records failed — missing customer_id",
    run_message="Validation flagged data quality issues"
)


# ── Example 2: Historical backfill with custom timestamps ─────────────────────
for i in range(7):
    past_date = datetime.now() - timedelta(days=i)
    tracker.track_pipeline_run(
        notebook_name="DailyETL", table_name="sales_fact", operation_type="REFRESH",
        rows_before=100000, rows_after=100000 + (i * 1000),
        duration_seconds=15.0 + (i * 2),
        run_message=f"Daily refresh — {past_date.strftime('%Y-%m-%d')}",
        custom_timestamp=past_date
    )


# ── Example 3: Auto-timing with decorator ────────────────────────────────────
@measure_execution_time
def load_sales_data():
    time.sleep(1)  # Simulate processing
    return {"rows_loaded": 5000}

result, duration, error = load_sales_data()

tracker.track_pipeline_run(
    notebook_name="SalesETL", table_name="sales_fact", operation_type="LOAD",
    rows_before=0, rows_after=result["rows_loaded"] if not error else 0,
    duration_seconds=duration,
    run_message="Sales data loaded" if not error else None,
    error_details=error
)


# ── Example 4: Monitoring ─────────────────────────────────────────────────────
tracker.show_recent_runs(5)
tracker.show_summary_stats()
tracker.fetch_activity_logs(operation_type="VALIDATE").show()
tracker.show_health_check()


# ── Example 5: Multi-stage ETL with staged timestamps ────────────────────────
def run_etl_pipeline():
    start = datetime.now() - timedelta(hours=2)
    tracker.track_pipeline_run(
        notebook_name="ETL_Pipeline", table_name="source_system", operation_type="EXTRACT",
        rows_before=0, rows_after=50000, duration_seconds=45.2,
        run_message="Extracted from source", custom_timestamp=start
    )
    tracker.track_pipeline_run(
        notebook_name="ETL_Pipeline", table_name="staging_area", operation_type="TRANSFORM",
        rows_before=50000, rows_after=49500, duration_seconds=120.7,
        run_message="Business rules applied", custom_timestamp=start + timedelta(minutes=30)
    )
    tracker.track_pipeline_run(
        notebook_name="ETL_Pipeline", table_name="data_warehouse", operation_type="LOAD",
        rows_before=1000000, rows_after=1049500, duration_seconds=85.3,
        run_message="Loaded to warehouse", custom_timestamp=start + timedelta(minutes=75)
    )

run_etl_pipeline()


# ── Example 6: Failure, retry and recovery ────────────────────────────────────
def track_failure_and_recovery():
    failure_time = datetime.now() - timedelta(hours=1)
    tracker.track_pipeline_run(
        notebook_name="CriticalPipeline", table_name="core_process", operation_type="EXECUTE",
        duration_seconds=0.5, error_details="Connection timeout",
        run_message="Pipeline failed", custom_timestamp=failure_time
    )
    for attempt in range(3):
        retry_time = failure_time + timedelta(minutes=10 * (attempt + 1))
        if attempt < 2:
            tracker.track_pipeline_run(
                notebook_name="CriticalPipeline", table_name="core_process", operation_type="RETRY",
                duration_seconds=1.0 + attempt,
                error_details=f"Retry {attempt + 1} failed — still unreachable",
                custom_timestamp=retry_time
            )
        else:
            tracker.track_pipeline_run(
                notebook_name="CriticalPipeline", table_name="core_process", operation_type="RETRY",
                rows_before=0, rows_after=25000, duration_seconds=5.2,
                run_message=f"Retry {attempt + 1} succeeded",
                custom_timestamp=retry_time
            )

track_failure_and_recovery()


# ── Example 7: Batch processing with mixed outcomes ───────────────────────────
def track_batch():
    batch_start = datetime.now() - timedelta(hours=3)
    batch_id    = f"BATCH_{batch_start.strftime('%Y%m%d_%H%M%S')}"
    tables = [
        {"name": "customers",    "records": 50000,  "success": True,  "duration": 30.5},
        {"name": "orders",       "records": 200000, "success": True,  "duration": 45.2},
        {"name": "products",     "records": 10000,  "success": False, "duration": 5.1},
        {"name": "transactions", "records": 500000, "success": True,  "duration": 120.3}
    ]
    for i, t in enumerate(tables):
        tracker.track_pipeline_run(
            notebook_name="BatchPipeline", table_name=t["name"], operation_type="BATCH_LOAD",
            rows_before=0, rows_after=t["records"] if t["success"] else 0,
            duration_seconds=t["duration"],
            run_message=f"[{batch_id}] {t['name']} — {'ok' if t['success'] else 'failed'}",
            error_details=None if t["success"] else f"Schema validation failed for {t['name']}",
            custom_timestamp=batch_start + timedelta(minutes=i * 15)
        )
    successful = sum(1 for t in tables if t["success"])
    tracker.track_pipeline_run(
        notebook_name="BatchPipeline", table_name="batch_control_log",
        operation_type="BATCH_COMPLETE",
        duration_seconds=sum(t["duration"] for t in tables),
        run_message=f"[{batch_id}] {successful}/{len(tables)} tables succeeded",
        custom_timestamp=batch_start + timedelta(hours=2)
    )

track_batch()


# ── Example 8: Power BI model management ──────────────────────────────────────
# Refresh existing model (relationships + measures + dataset refresh)
tracker.refresh_power_bi_model()

# Or rebuild from scratch if needed
# tracker.build_power_bi_model()

# Inspect model via TOM
def inspect_model():
    try:
        from jags_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=tracker.semantic_model_name,
                                    readonly=True, workspace=tracker.workspace_name) as tom:
            for table in tom.model.Tables:
                if table.Name == "pipeline_activity_log":
                    folders = {}
                    for m in table.Measures:
                        folders.setdefault(m.DisplayFolder or "Ungrouped", []).append(m.Name)
                    for folder, measures in folders.items():
                        print(f"  📁 {folder}: {', '.join(measures)}")
    except Exception as e:
        print(f"TOM inspection failed: {e}")

inspect_model()
