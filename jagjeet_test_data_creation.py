# jagjeet_test_data_creation.py
# Generate realistic test data for the Jagjeet Pipeline Operations Tracker
# Run once to populate sample data — not for production use

from builtin.jagjeet_logging_utils import JagjeetPipelineTracker
import random
from datetime import datetime, timedelta


def create_test_data(tracker, num_runs=50):
    """Generate N random pipeline runs across notebooks, tables and operations"""
    print(f"Generating {num_runs} test runs...")

    notebooks = ["DataIngestion", "ETL_Pipeline", "DataValidation", "DataCleaning",
                 "Analytics_Prep", "Reporting_ETL", "Incremental_Load", "Batch_Processor"]

    tables = ["customers", "orders", "products", "sales_fact", "inventory",
              "transactions", "user_activity", "financial_data", "order_items", "returns"]

    operations = ["INSERT", "UPDATE", "DELETE", "MERGE", "LOAD", "EXTRACT",
                  "TRANSFORM", "VALIDATE", "AGGREGATE", "QUALITY_CHECK", "REFRESH", "BACKUP"]

    success_messages = ["Run completed successfully", "Data loaded from source",
                        "Transformation rules applied", "Quality validation passed",
                        "Incremental update completed", "Batch processing finished"]

    failure_messages = ["Connection timeout to source system",
                        "Data validation failed — null values found",
                        "Duplicate key constraint violation",
                        "Memory allocation exceeded during join",
                        "Schema mismatch between source and target"]

    complexity = {"INSERT": 2.0, "UPDATE": 3.0, "DELETE": 1.5, "MERGE": 5.0, "LOAD": 4.0,
                  "EXTRACT": 3.0, "TRANSFORM": 6.0, "VALIDATE": 1.0, "AGGREGATE": 3.0,
                  "QUALITY_CHECK": 2.0, "REFRESH": 1.0, "BACKUP": 10.0}

    base_time = datetime.now() - timedelta(days=30)

    for i in range(num_runs):
        run_time  = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(6, 22), minutes=random.randint(0, 59))
        notebook  = random.choice(notebooks)
        table     = random.choice(tables)
        operation = random.choice(operations)

        base_rows = random.randint(50000, 500000) if table in ["orders", "transactions", "sales_fact"] \
                    else random.randint(10000, 100000) if table in ["customers", "products"] \
                    else random.randint(1000, 50000)

        if operation in ["INSERT", "LOAD", "EXTRACT"]:
            rows_before, rows_after = random.randint(0, base_rows // 2), base_rows
        elif operation in ["DELETE", "VALIDATE"]:
            rows_before, rows_after = base_rows, base_rows - random.randint(10, base_rows // 10)
        else:
            rows_before, rows_after = base_rows, base_rows + random.randint(-500, 500)

        rows_after    = max(0, rows_after)
        duration      = round(complexity.get(operation, 2.0) * (rows_after / 100000) * random.uniform(0.5, 2.0), 2)
        is_failure    = random.random() < 0.1
        error_details = random.choice(failure_messages) if is_failure else None
        run_message   = None if is_failure else random.choice(success_messages)

        tracker.track_pipeline_run(
            notebook_name=notebook, table_name=table, operation_type=operation,
            rows_before=rows_before, rows_after=rows_after, duration_seconds=duration,
            run_message=run_message, error_details=error_details,
            run_by=random.choice(["alice.smith", "bob.jones", "carol.davis", "david.wilson"]),
            custom_timestamp=run_time
        )
        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{num_runs} created...")

    print(f"✅ {num_runs} test runs created")
    return num_runs


def create_etl_pipeline_scenarios(tracker):
    """5 hardcoded realistic ETL scenarios"""
    print("\nCreating ETL scenarios...")

    # Daily ETL — 4 sequential stages
    for notebook, table, operation, before, after, duration in [
        ("DataIngestion",  "source_customers",  "EXTRACT",     0,       50000, 45.2),
        ("ETL_Pipeline",   "staging_customers", "TRANSFORM",   50000,   49500, 120.7),
        ("ETL_Pipeline",   "dim_customer",      "LOAD",        45000,   49500, 85.3),
        ("DataValidation", "dim_customer",      "VALIDATE",    49500,   49500, 12.1)
    ]:
        tracker.track_pipeline_run(notebook_name=notebook, table_name=table, operation_type=operation,
                                   rows_before=before, rows_after=after, duration_seconds=duration,
                                   run_message=f"Daily ETL — {operation}")

    # Quality failure
    tracker.track_pipeline_run(notebook_name="DataQuality", table_name="orders", operation_type="QUALITY_CHECK",
                               rows_before=100000, rows_after=98500, duration_seconds=25.4,
                               error_details="1,500 records failed — missing customer_id")

    # Slow aggregation
    tracker.track_pipeline_run(notebook_name="Analytics_Prep", table_name="sales_fact", operation_type="AGGREGATE",
                               rows_before=5000000, rows_after=50000, duration_seconds=450.8,
                               run_message="Monthly aggregation — slower than SLA")

    # Incremental merge
    tracker.track_pipeline_run(notebook_name="Incremental_Load", table_name="transactions", operation_type="MERGE",
                               rows_before=2000000, rows_after=2005000, duration_seconds=67.2,
                               run_message="Incremental merge — 5,000 new transactions")

    print("✅ ETL scenarios created")


def create_failure_scenarios(tracker):
    """5 realistic pipeline failure scenarios"""
    print("\nCreating failure scenarios...")
    failures = [
        ("DataIngestion",  "external_api_data", "EXTRACT",   "API rate limit exceeded — HTTP 429",               5.0),
        ("ETL_Pipeline",   "sales_staging",     "TRANSFORM", "Column 'price' contains non-numeric values",       15.3),
        ("DataValidation", "customer_data",     "VALIDATE",  "Primary key violation — duplicate IDs found",      8.7),
        ("ML_Pipeline",    "feature_store",     "LOAD",      "Out of memory during feature matrix calculation",   45.2),
        ("Reporting",      "dashboard_cache",   "REFRESH",   "Timeout connecting to source database after 120s",  120.0)
    ]
    for notebook, table, operation, error, duration in failures:
        tracker.track_pipeline_run(notebook_name=notebook, table_name=table, operation_type=operation,
                                   duration_seconds=duration, error_details=error,
                                   run_message="Pipeline failed — see error details")
    print(f"✅ {len(failures)} failure scenarios created")


def create_performance_benchmarks(tracker):
    """Benchmark runs across 4 dataset sizes × 5 operations"""
    print("\nCreating performance benchmarks...")
    datasets      = [("bench_small", 1000, 2.1), ("bench_medium", 50000, 15.7),
                     ("bench_large", 1000000, 120.3), ("bench_xlarge", 10000000, 450.8)]
    op_complexity = {"SELECT": 1.0, "JOIN": 3.0, "AGGREGATE": 2.0, "SORT": 2.5, "MERGE": 4.0}

    for table, count, base in datasets:
        for op, factor in op_complexity.items():
            tracker.track_pipeline_run(
                notebook_name="PerformanceBenchmark", table_name=table, operation_type=op,
                rows_before=count, rows_after=count,
                duration_seconds=round(base * factor * random.uniform(0.8, 1.2), 2),
                run_message=f"Benchmark: {op} on {table} ({count:,} records)"
            )
    print("✅ Performance benchmarks created")


# ── Entry points ──────────────────────────────────────────────────────────────
def setup_complete_test_environment(project_name="JagjeetPipelineDemo"):
    """Run all test data generators in sequence"""
    print(f"{'='*60}\nJAGJEET — TEST ENVIRONMENT SETUP\n{'='*60}")
    tracker = JagjeetPipelineTracker(project_name)
    create_test_data(tracker, num_runs=30)
    create_etl_pipeline_scenarios(tracker)
    create_failure_scenarios(tracker)
    create_performance_benchmarks(tracker)
    print(f"\n{'='*60}\nTest environment ready\n{'='*60}")
    tracker.show_summary_stats()
    return tracker


def quick_test(project_name="JagjeetQuickTest"):
    """5-entry smoke test to verify the tracker is working"""
    tracker = JagjeetPipelineTracker(project_name)
    for notebook, table, op, before, after, duration, msg in [
        ("TestNotebook", "customers", "INSERT",   0,     1000,  5.2,  "Test insert"),
        ("TestNotebook", "orders",    "UPDATE",   5000,  5100,  8.7,  "Test update"),
        ("TestNotebook", "products",  "VALIDATE", 2000,  2000,  3.1,  None),
        ("TestNotebook", "sales",     "MERGE",    10000, 10500, 12.4, "Test merge"),
        ("TestNotebook", "inventory", "DELETE",   500,   450,   2.8,  None)
    ]:
        tracker.track_pipeline_run(notebook_name=notebook, table_name=table, operation_type=op,
                                   rows_before=before, rows_after=after, duration_seconds=duration,
                                   run_message=msg)
    print("✅ Quick test complete")
    tracker.show_recent_runs(5)
    return tracker


if __name__ == "__main__":
    # Uncomment to run:
    # tracker = quick_test()
    # tracker = setup_complete_test_environment()
    pass
