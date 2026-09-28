# Databricks notebook source
# DBTITLE 1,Run ingestion for one table
import json
from pyspark.sql import SparkSession
from watersync.common import quote_sql_string
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings

spark = SparkSession.getActiveSession()

runtime = JdbcRuntimeSettings(
    configuration_fqn=dbutils.widgets.get("configuration_fqn"),
    watermark_fqn=dbutils.widgets.get("watermark_fqn"),
    ingestion_group=dbutils.widgets.get("ingestion_group"),
    source_table_name=dbutils.widgets.get("source_table_name"),
    full_refresh=dbutils.widgets.get("full_refresh").strip().lower() == "true",
)

# ---------------------------------------------------------------------------
# Detect per-table FULL_REFRESH status *before* ingestion clears it.
# If detected (or the global full_refresh flag is set), look up the CDC
# target table so the downstream selective-refresh task can pass it to
# pipelines.start_update(full_refresh_selection=...).
# ---------------------------------------------------------------------------
source_table = runtime.source_table_name
ingestion_group = runtime.ingestion_group
full_refresh_target = ""

needs_full = runtime.full_refresh
if not needs_full and source_table:
    _wm_row = spark.sql(f"""
        SELECT status FROM {runtime.state_table}
        WHERE ingestion_group = '{quote_sql_string(ingestion_group)}'
          AND source_table_name = '{quote_sql_string(source_table)}'
        ORDER BY last_run_timestamp DESC LIMIT 1
    """).first()
    needs_full = _wm_row is not None and _wm_row["status"] == "FULL_REFRESH"

if needs_full and source_table:
    _cfg_row = spark.sql(f"""
        SELECT target_table_fqn FROM {runtime.config_table}
        WHERE ingestion_group = '{quote_sql_string(ingestion_group)}'
          AND source_table_name = '{quote_sql_string(source_table)}'
        LIMIT 1
    """).first()
    if _cfg_row and _cfg_row["target_table_fqn"]:
        full_refresh_target = _cfg_row["target_table_fqn"]

# ---------------------------------------------------------------------------
# Run ingestion
# ---------------------------------------------------------------------------
orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
result = orchestrator.run_selected_ingestion()
print(json.dumps(result, default=str))

# ---------------------------------------------------------------------------
# Signal downstream selective-refresh task via Databricks task values.
# (No-op when running outside a job context.)
# ---------------------------------------------------------------------------
try:
    dbutils.jobs.taskValues.set(key="full_refresh_target", value=full_refresh_target)
except Exception:
    pass  # Not running inside a Databricks job

# COMMAND ----------

