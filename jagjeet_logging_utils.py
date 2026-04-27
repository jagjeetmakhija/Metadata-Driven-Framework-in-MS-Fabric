# jagjeet_logging_utils.py
# Jagjeet Pipeline Operations Tracking Framework - Version 3.4.3
# ================================================================================

import subprocess
import sys
import time
import getpass
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Union

def ensure_package(package: str, import_name: str = None) -> None:
    """Install package if missing"""
    import_name = import_name or package
    try:
        __import__(import_name)
    except ImportError:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", package, "--quiet"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"Warning: Could not install {package}: {result.stderr}")

ensure_package("semantic-link-labs", "jags_labs")

import sempy.fabric as jags_fabric
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, year, month, dayofweek, dayofmonth, quarter, weekofyear, date_format, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, TimestampType, IntegerType, BooleanType

try:
    from notebookutils import mssparkutils
    import notebookutils
except ImportError:
    mssparkutils = notebookutils = None

# Single shared schema — used for table creation and log writes
LOG_SCHEMA = StructType([
    StructField("notebook_name",    StringType(),       True),
    StructField("table_name",       StringType(),       True),
    StructField("operation_type",   StringType(),       True),
    StructField("run_by",           StringType(),       True),
    StructField("rows_before",      LongType(),         True),
    StructField("rows_after",       LongType(),         True),
    StructField("rows_changed",     LongType(),         True),
    StructField("duration_seconds", DecimalType(10, 6), True),
    StructField("run_message",      StringType(),       True),
    StructField("error_details",    StringType(),       True),
    StructField("run_date",         StringType(),       True),
    StructField("run_time",         StringType(),       True),
    StructField("run_timestamp",    TimestampType(),    True)
])


class JagjeetPipelineTracker:
    """Jagjeet's Microsoft Fabric pipeline operations tracking framework"""

    def __init__(self, project_name: str, force_recreate: bool = False, workspace_name: str = None):
        self.project_name        = project_name
        self.lakehouse_name      = f"LH_{project_name}_Jagjeet_PipelineOps"
        self.semantic_model_name = f"SM_{project_name}_Jagjeet_PipelineOps"
        self.workspace_id        = None
        self.workspace_name      = workspace_name
        self.lakehouse_id        = None
        self.activity_log_path   = None
        self.force_recreate      = force_recreate
        self.spark               = SparkSession.builder.getOrCreate()

        print(f"\n{'='*60}\nJAGJEET PIPELINE OPERATIONS TRACKER v3.4.3\n{'='*60}")
        print(f"  • Project:        {project_name}")
        print(f"  • Lakehouse:      {self.lakehouse_name}")
        print(f"  • Semantic Model: {self.semantic_model_name}")
        if force_recreate:
            print("  ⚠ Force recreate — existing data will be lost!")
        self._setup()

    # ── Path helpers ──────────────────────────────────────────────────────────
    @property
    def _date_dim_path(self):
        return self.activity_log_path.replace("/pipeline_activity_log", "/date_dimension")

    @property
    def _time_dim_path(self):
        return self.activity_log_path.replace("/pipeline_activity_log", "/time_dimension")

    # ── Setup ─────────────────────────────────────────────────────────────────
    def _setup(self):
        """Bootstrap workspace, lakehouse, tables and Power BI model"""

        # Workspace
        self.workspace_id = jags_fabric.get_notebook_workspace_id()
        try:
            self.workspace_name = self.workspace_name or jags_fabric.resolve_workspace_name(self.workspace_id)
        except:
            self.workspace_name = "MyWorkspace"
        print(f"\n  Workspace: {self.workspace_name}")

        # Lakehouse
        if notebookutils:
            self.lakehouse_id = notebookutils.runtime.context.get('defaultLakehouseId')
            if not self.lakehouse_id:
                try:
                    self.lakehouse_id = mssparkutils.lakehouse.get(self.lakehouse_name)['id']
                    print(f"  Lakehouse: found {self.lakehouse_name}")
                except:
                    try:
                        mssparkutils.lakehouse.create(
                            name=self.lakehouse_name,
                            description=f"Pipeline ops lakehouse for {self.project_name}",
                            workspaceId=self.workspace_id
                        )
                        self.lakehouse_id = mssparkutils.lakehouse.get(self.lakehouse_name)['id']
                        print(f"  Lakehouse: created {self.lakehouse_name}")
                    except Exception as e:
                        print(f"  Lakehouse: failed — {e}. Using current context.")
                        self.lakehouse_id = "current-context"
            else:
                print(f"  Lakehouse: using default ({self.lakehouse_id[:8]}...)")

            self.activity_log_path = (
                f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}/Tables/pipeline_activity_log"
                if self.lakehouse_id != "current-context"
                else "Tables/pipeline_activity_log"
            )

        # Tables
        print("\n  Tables:")
        self._create_activity_log_table()
        self._create_date_dimension_table()
        self._create_time_dimension_table()

        # Power BI model — authenticate once before building
        if self._verify_all_tables_ready():
            self._authenticate_for_tom()
            self._build_power_bi_model()
        else:
            print("  ⚠ Tables not ready — run build_power_bi_model() later")

        print(f"\n{'='*60}\nReady. Use: tracker.track_pipeline_run(...)\n{'='*60}\n")

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _delta_table_exists(self, path: str) -> bool:
        try:
            self.spark.read.format("delta").load(path)
            return True
        except:
            return False

    def _verify_all_tables_ready(self, max_retries=15, wait_seconds=2) -> bool:
        """Poll until all 3 tables are readable"""
        paths = {
            "pipeline_activity_log": self.activity_log_path,
            "date_dimension":        self._date_dim_path,
            "time_dimension":        self._time_dim_path
        }
        all_ready = True
        for name, path in paths.items():
            ready = False
            for attempt in range(max_retries):
                try:
                    count = self.spark.read.format("delta").load(path).count()
                    print(f"    ✓ {name}: {count:,} rows")
                    ready = True
                    break
                except:
                    if attempt < max_retries - 1:
                        time.sleep(wait_seconds)
                    else:
                        print(f"    ✗ {name}: not accessible after {max_retries} attempts")
            all_ready = all_ready and ready
        return all_ready

    def _authenticate_for_tom(self) -> bool:
        """Authenticate once using Fabric native token for all TOM operations"""
        try:
            if notebookutils:
                os.environ['AZURE_ACCESS_TOKEN'] = notebookutils.credentials.getToken('storage')
                return True
        except Exception as e:
            print(f"  Auth failed: {e}")
        return False

    # ── Table creation ────────────────────────────────────────────────────────
    def _create_activity_log_table(self):
        if self._delta_table_exists(self.activity_log_path) and not self.force_recreate:
            try:
                df = self.spark.read.format("delta").load(self.activity_log_path)
                missing = {f.name for f in LOG_SCHEMA} - set(df.columns)
                if missing:
                    print(f"    pipeline_activity_log: {df.count():,} records — missing cols {missing} (schema evolution handles)")
                else:
                    print(f"    pipeline_activity_log: {df.count():,} existing records preserved")
                return
            except Exception as e:
                print(f"    pipeline_activity_log: error — {e}. Recreating...")

        self.spark.createDataFrame([], LOG_SCHEMA).write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite" if self.force_recreate else "ignore") \
            .save(self.activity_log_path)
        print("    pipeline_activity_log: created")

    def _create_date_dimension_table(self):
        if self._delta_table_exists(self._date_dim_path) and not self.force_recreate:
            try:
                df = self.spark.read.format("delta").load(self._date_dim_path)
                max_date   = df.agg({"date_key": "max"}).collect()[0][0]
                days_ahead = (datetime.now() + timedelta(days=365) - datetime.strptime(max_date, "%Y-%m-%d")).days
                if days_ahead <= 0:
                    print(f"    date_dimension: {df.count():,} dates (up to {max_date})")
                    return
                print(f"    date_dimension: extending by {days_ahead} days")
            except:
                pass

        d, end, rows = datetime.now() - timedelta(days=730), datetime.now() + timedelta(days=730), []
        while d <= end:
            rows.append((d.strftime("%Y-%m-%d"),))
            d += timedelta(days=1)

        date_df = self.spark.createDataFrame(rows, ["date_key"]) \
            .withColumn("date_value",  col("date_key").cast("date")) \
            .withColumn("year",        year(col("date_value"))) \
            .withColumn("month",       month(col("date_value"))) \
            .withColumn("day",         dayofmonth(col("date_value"))) \
            .withColumn("quarter",     quarter(col("date_value"))) \
            .withColumn("week_number", weekofyear(col("date_value"))) \
            .withColumn("day_of_week", dayofweek(col("date_value"))) \
            .withColumn("month_name",  date_format(col("date_value"), "MMMM")) \
            .withColumn("day_name",    date_format(col("date_value"), "EEEE")) \
            .withColumn("is_weekend",  (dayofweek(col("date_value")).isin([1, 7])).cast("boolean"))

        if self._delta_table_exists(self._date_dim_path) and not self.force_recreate:
            from delta.tables import DeltaTable
            DeltaTable.forPath(self.spark, self._date_dim_path).alias("t").merge(
                date_df.alias("s"), "t.date_key = s.date_key"
            ).whenNotMatchedInsertAll().execute()
            print("    date_dimension: extended")
        else:
            date_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(self._date_dim_path)
            print("    date_dimension: created (4 years)")

    def _create_time_dimension_table(self):
        if self._delta_table_exists(self._time_dim_path) and not self.force_recreate:
            print(f"    time_dimension: {self.spark.read.format('delta').load(self._time_dim_path).count():,} slots")
            return

        rows = [
            (f"{h:02d}:{m:02d}:00", h, m, f"{h:02d}:00",
             "Night" if h < 6 else "Morning" if h < 12 else "Afternoon" if h < 18 else "Evening",
             9 <= h < 17)
            for h in range(24) for m in range(60)
        ]
        schema = StructType([
            StructField("time_key",          StringType(),  False),
            StructField("hour",              IntegerType(), False),
            StructField("minute",            IntegerType(), False),
            StructField("hour_label",        StringType(),  False),
            StructField("day_period",        StringType(),  False),
            StructField("is_business_hours", BooleanType(), False)
        ])
        self.spark.createDataFrame(rows, schema).write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite" if self.force_recreate else "ignore") \
            .save(self._time_dim_path)
        print("    time_dimension: created (1,440 slots)")

    # ── Power BI model ────────────────────────────────────────────────────────
    def _power_bi_model_exists(self) -> bool:
        try:
            ds = jags_fabric.list_datasets(workspace=self.workspace_name)
            return ds is not None and not ds.empty and self.semantic_model_name in ds['Dataset Name'].tolist()
        except:
            return False

    def _build_power_bi_model(self):
        if self._power_bi_model_exists() and not self.force_recreate:
            print(f"  Power BI model exists. Use refresh_power_bi_model() to update.")
            return True
        try:
            from jags_labs.directlake import generate_direct_lake_semantic_model
            generate_direct_lake_semantic_model(
                dataset=self.semantic_model_name,
                workspace=self.workspace_name,
                lakehouse=self.lakehouse_name,
                lakehouse_tables=["pipeline_activity_log", "date_dimension", "time_dimension"],
                overwrite=True, refresh=False
            )
            print(f"  Power BI model created: {self.semantic_model_name}")
            self._add_model_relationships()
            self._add_dax_measures()
            try:
                jags_fabric.refresh_dataset(dataset=self.semantic_model_name, workspace=self.workspace_name)
                print("  Model refreshed")
            except Exception as e:
                print(f"  Model created — manual refresh needed: {e}")
            return True
        except Exception as e:
            print(f"  Model creation failed: {e}")
            return False

    def _add_model_relationships(self):
        """Create missing Many-to-One relationships"""
        try:
            existing = jags_fabric.list_relationships(dataset=self.semantic_model_name, workspace=self.workspace_name)
            existing_keys = set() if existing.empty else {
                f"{r['From Table']}[{r['From Column']}]->{r['To Table']}[{r['To Column']}]"
                for _, r in existing.iterrows()
            }
            rel_configs = [
                {"from_table": "pipeline_activity_log", "from_column": "run_date",
                 "to_table": "date_dimension",          "to_column": "date_key",
                 "from_cardinality": "Many", "to_cardinality": "One",
                 "cross_filtering_behavior": "OneDirection", "is_active": True},
                {"from_table": "pipeline_activity_log", "from_column": "run_time",
                 "to_table": "time_dimension",          "to_column": "time_key",
                 "from_cardinality": "Many", "to_cardinality": "One",
                 "cross_filtering_behavior": "OneDirection", "is_active": True}
            ]
            from jags_labs.tom import connect_semantic_model
            with connect_semantic_model(dataset=self.semantic_model_name, readonly=False, workspace=self.workspace_name) as tom:
                for rel in rel_configs:
                    key = f"{rel['from_table']}[{rel['from_column']}]->{rel['to_table']}[{rel['to_column']}]"
                    if key not in existing_keys:
                        tom.add_relationship(**rel)
                        print(f"  Relationship created: {key}")
                    else:
                        print(f"  Relationship exists:  {key}")
        except Exception as e:
            print(f"  Relationships failed: {e}")
            print("  Manual: pipeline_activity_log[run_date] -> date_dimension[date_key]")
            print("          pipeline_activity_log[run_time] -> time_dimension[time_key]")

    def _add_dax_measures(self):
        """Create or update all 8 DAX measures"""
        measures = [
            ("Total Pipeline Runs",  "COUNTROWS(pipeline_activity_log)",                                                                                                      "#,##0",            "Pipeline Overview"),
            ("Total Rows Processed", "SUM(pipeline_activity_log[rows_changed])",                                                                                              "#,##0",            "Pipeline Overview"),
            ("Active Tables",        "DISTINCTCOUNT(pipeline_activity_log[table_name])",                                                                                      "#,##0",            "Pipeline Overview"),
            ("Active Notebooks",     "DISTINCTCOUNT(pipeline_activity_log[notebook_name])",                                                                                   "#,##0",            "Pipeline Overview"),
            ("Average Run Duration", "AVERAGE(pipeline_activity_log[duration_seconds])",                                                                                      "#,##0.00 \"sec\"", "Performance"),
            ("Failed Runs",          "CALCULATE(COUNTROWS(pipeline_activity_log), NOT(ISBLANK(pipeline_activity_log[error_details])))",                                       "#,##0",            "Quality & Reliability"),
            ("Success Rate",         "DIVIDE(COUNTROWS(FILTER(pipeline_activity_log, ISBLANK(pipeline_activity_log[error_details]))), COUNTROWS(pipeline_activity_log), 0)",  "0.0%",             "Quality & Reliability"),
            ("Runs Today",           "CALCULATE(COUNTROWS(pipeline_activity_log), pipeline_activity_log[run_date] = FORMAT(TODAY(), \"YYYY-MM-DD\"))",                        "#,##0",            "Time Intelligence"),
        ]
        try:
            from jags_labs.tom import connect_semantic_model
            with connect_semantic_model(dataset=self.semantic_model_name, readonly=False, workspace=self.workspace_name) as tom:
                target = next((t for t in tom.model.Tables if t.Name == "pipeline_activity_log"), None)
                if not target:
                    print("  Table not in model — measures skipped")
                    return
                existing_names = [m.Name for m in target.Measures]
                for name, expr, fmt, folder in measures:
                    if name in existing_names:
                        m = target.Measures[name]
                        m.Expression, m.FormatString, m.DisplayFolder = expr, fmt, folder
                    else:
                        tom.add_measure(table_name="pipeline_activity_log", measure_name=name,
                                        expression=expr, format_string=fmt, display_folder=folder)
                print(f"  {len(measures)} DAX measures applied")
        except Exception as e:
            print(f"  Measures failed: {e}")

    # ── Public API ────────────────────────────────────────────────────────────
    def track_pipeline_run(self, notebook_name: str, table_name: str, operation_type: str,
                           rows_before: int = 0, rows_after: int = 0,
                           duration_seconds: Union[float, Decimal] = 0.0,
                           run_message: str = None, error_details: str = None,
                           run_by: str = None, custom_timestamp: datetime = None):
        """Record a pipeline run — always appends, never overwrites"""
        now      = custom_timestamp or datetime.now()
        run_date = now.strftime("%Y-%m-%d")
        run_time = now.strftime("%H:%M:%S")

        record = [(
            notebook_name, table_name, operation_type,
            run_by or resolve_current_user(),
            int(rows_before), int(rows_after), int(rows_after - rows_before),
            Decimal(str(duration_seconds)),
            run_message, error_details, run_date, run_time, None
        )]

        df = self.spark.createDataFrame(record, LOG_SCHEMA)
        df = df.withColumn("run_timestamp", lit(custom_timestamp) if custom_timestamp else current_timestamp())
        df.write.format("delta").option("mergeSchema", "true").mode("append").save(self.activity_log_path)

        status = "FAILED" if error_details else "OK"
        print(f"  [{status}] {operation_type} | {table_name} | Δ{rows_after - rows_before:+,} rows | {duration_seconds}s | {run_date}")

    def fetch_activity_logs(self, table_name: str = None, operation_type: str = None, limit: int = 100):
        """Return filtered activity log DataFrame"""
        df = self.spark.read.format("delta").load(self.activity_log_path)
        if table_name:     df = df.filter(df.table_name == table_name)
        if operation_type: df = df.filter(df.operation_type == operation_type)
        return df.orderBy(df.run_timestamp.desc()).limit(limit)

    def show_recent_runs(self, limit: int = 10):
        """Display the most recent pipeline runs"""
        self.fetch_activity_logs(limit=limit).show(truncate=False)

    def show_summary_stats(self):
        """Print aggregated pipeline activity stats"""
        try:
            df = self.spark.read.format("delta").load(self.activity_log_path)
            stats = {
                "total_runs":        df.count(),
                "unique_notebooks":  df.select("notebook_name").distinct().count(),
                "unique_tables":     df.select("table_name").distinct().count(),
                "unique_operations": df.select("operation_type").distinct().count()
            }
            print("\nPipeline Activity Summary:")
            for k, v in stats.items():
                print(f"  {k.replace('_', ' ').title()}: {v:,}")
            return stats
        except Exception as e:
            print(f"Could not retrieve stats: {e}")
            return None

    def purge_old_records(self, days_to_keep: int = 90):
        """Delete records older than specified days"""
        try:
            from delta.tables import DeltaTable
            cutoff       = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
            df           = self.spark.read.format("delta").load(self.activity_log_path)
            before_count = df.count()
            old_count    = df.filter(df.run_date < cutoff).count()
            if old_count > 0:
                dt = DeltaTable.forPath(self.spark, self.activity_log_path)
                dt.delete(f"run_date < '{cutoff}'")
                dt.vacuum(0)
                print(f"Purged {old_count:,} records older than {cutoff}. Kept {before_count - old_count:,}.")
            else:
                print(f"No records older than {cutoff}.")
        except Exception as e:
            print(f"Purge failed: {e}")

    def refresh_power_bi_model(self):
        """Re-apply relationships, measures and trigger refresh"""
        if not self._power_bi_model_exists():
            print("Model not found. Run build_power_bi_model() first.")
            return False
        self._authenticate_for_tom()
        self._add_model_relationships()
        self._add_dax_measures()
        try:
            jags_fabric.refresh_dataset(dataset=self.semantic_model_name, workspace=self.workspace_name)
            print("Power BI model refreshed")
        except Exception as e:
            print(f"Refresh failed: {e}")
        return True

    def build_power_bi_model(self, max_wait_minutes: int = 5):
        """Wait for tables to be ready, then build the Power BI model"""
        for attempt in range(max_wait_minutes * 6):
            if self._verify_all_tables_ready(max_retries=3, wait_seconds=1):
                self._authenticate_for_tom()
                return self._build_power_bi_model()
            elif attempt < (max_wait_minutes * 6) - 1:
                print(f"  Tables not ready — retrying in 10s...")
                time.sleep(10)
            else:
                print(f"  Tables not ready after {max_wait_minutes} minutes.")
                return False
        return False

    def show_health_check(self):
        """Display full health status of the framework"""
        print(f"\n{'='*60}\nJAGJEET PIPELINE TRACKER — HEALTH CHECK\n{'='*60}")
        print(f"  Project:   {self.project_name}")
        print(f"  Lakehouse: {self.lakehouse_name}")
        print(f"  Model:     {self.semantic_model_name}")
        print(f"  Workspace: {self.workspace_name}\n  Tables:")
        try:
            print(f"    pipeline_activity_log : {self.spark.read.format('delta').load(self.activity_log_path).count():,} records")
            date_df = self.spark.read.format("delta").load(self._date_dim_path)
            print(f"    date_dimension        : {date_df.count():,} dates (up to {date_df.agg({'date_key':'max'}).collect()[0][0]})")
            print(f"    time_dimension        : {self.spark.read.format('delta').load(self._time_dim_path).count():,} slots")
        except Exception as e:
            print(f"    Error: {e}")
        print(f"\n  Power BI Model: {'EXISTS' if self._power_bi_model_exists() else 'NOT FOUND'}")
        print(f"\n  tracker.track_pipeline_run(...)  — Record a run")
        print(f"  tracker.show_recent_runs(n)       — View recent runs")
        print(f"  tracker.show_summary_stats()      — View stats")
        print(f"  tracker.refresh_power_bi_model()  — Refresh model")
        print(f"  tracker.purge_old_records(days)   — Clean old data")
        print(f"{'='*60}\n")


# ── Utilities ──────────────────────────────────────────────────────────────────
def resolve_current_user() -> str:
    try:
        return getpass.getuser()
    except:
        return "fabric_user"

def measure_execution_time(func):
    """Decorator — returns (result, duration_seconds, error)"""
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = func(*args, **kwargs)
            return result, round(time.time() - start, 3), None
        except Exception as e:
            return None, round(time.time() - start, 3), str(e)
    return wrapper

__version__ = "3.4.3"
