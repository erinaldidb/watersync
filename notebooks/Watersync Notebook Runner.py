# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Watersync Notebook Runner
# MAGIC
# MAGIC Following your preferences, this notebook lives under the project-level `notebooks` folder and lets you execute `watersync` package code directly from a Databricks workspace notebook.
# MAGIC
# MAGIC Usage:
# MAGIC * Choose an `action`
# MAGIC * Set the relevant widgets for that action
# MAGIC * Run the bootstrap cell once
# MAGIC * Run the execution cell
# MAGIC
# MAGIC Supported actions:
# MAGIC * `plan_configs`
# MAGIC * `run_ingestion`
# MAGIC * `create_job`
# MAGIC * `setup_uc`
# MAGIC * `setup_lakebase`
# MAGIC

# COMMAND ----------

# MAGIC %pip install -e ../.
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Bootstrap watersync imports
from pathlib import Path
import json
import sys

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = context.notebookPath().get()
workspace_notebook_path = (
    Path(notebook_path)
    if notebook_path.startswith("/Workspace/")
    else Path("/Workspace") / notebook_path.lstrip("/")
)
notebook_dir = workspace_notebook_path.parent
project_root = notebook_dir.parent
src_root = project_root / "src"

if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))

from watersync.config_planner import IngestionConfigPlanner
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings, JobProvisioningSettings
from watersync.utils.create_ingestion_job import IngestionJobProvisioner
from watersync.utils.lakebase_test_database_setup import LakebaseTestDatabaseSetup
from watersync.utils.uc_setup import UnityCatalogSetup

print(f"Notebook path : {notebook_path}")
print(f"Project root  : {project_root}")
print(f"Source root   : {src_root}")
print("watersync imports loaded successfully")


# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.dropdown(
    "action",
    "plan_configs",
    ["plan_configs", "run_ingestion", "create_job", "setup_uc", "setup_lakebase"],
    "Watersync action",
)

dbutils.widgets.text("catalog", "users", "Target catalog")
dbutils.widgets.text("schema", "emanuele_rinaldi", "Target schema")
dbutils.widgets.text("ingestion_group", "", "Ingestion group")
dbutils.widgets.text("source_table_name", "", "Source table name")
dbutils.widgets.text("jdbc_url", "", "JDBC URL")
dbutils.widgets.text("jdbc_user", "", "JDBC user")
dbutils.widgets.text("jdbc_password", "", "JDBC password")
dbutils.widgets.text("jdbc_secret_scope", "", "JDBC secret scope")
dbutils.widgets.text("jdbc_secret_key", "", "JDBC secret key")
dbutils.widgets.text("watermark_threshold_minutes", "5", "Watermark threshold minutes")
dbutils.widgets.text("fetch_size", "10000", "JDBC fetch size")
dbutils.widgets.text("num_partitions", "8", "JDBC num partitions")

dbutils.widgets.text("wheel_uri", "", "Wheel URI for create_job")
dbutils.widgets.text("cdc_pipeline_id", "", "Existing CDC pipeline id")
dbutils.widgets.text(
    "cdc_pipeline_file_path",
    str(project_root / "src" / "watersync" / "pipeline_bootstrap.py"),
    "CDC pipeline bootstrap file path",
)
dbutils.widgets.text("foreach_concurrency", "4", "ForEach concurrency")

dbutils.widgets.text("project_id", "slalom-jdbc-test", "Lakebase project id")
dbutils.widgets.text("project_display_name", "Slalom JDBC Test DB", "Lakebase project display name")
dbutils.widgets.text("customer_count", "200", "Lakebase customer seed count")
dbutils.widgets.text("product_count", "100", "Lakebase product seed count")
dbutils.widgets.text("order_count", "500", "Lakebase order seed count")
dbutils.widgets.dropdown("simulate_updates", "false", ["false", "true"], "Simulate Lakebase updates")
dbutils.widgets.dropdown("truncate_existing", "false", ["false", "true"], "Truncate existing UC tables")


# COMMAND ----------

# DBTITLE 1,Run watersync action
def _widget(name: str) -> str:
    return dbutils.widgets.get(name).strip()


def _as_bool(name: str) -> bool:
    return _widget(name).lower() == "true"


action = _widget("action")

runtime = JdbcRuntimeSettings(
    catalog=_widget("catalog"),
    schema=_widget("schema"),
    ingestion_group=_widget("ingestion_group"),
    source_table_name=_widget("source_table_name"),
    jdbc_url=_widget("jdbc_url"),
    jdbc_user=_widget("jdbc_user"),
    jdbc_password=_widget("jdbc_password"),
    jdbc_secret_scope=_widget("jdbc_secret_scope"),
    jdbc_secret_key=_widget("jdbc_secret_key"),
    watermark_threshold_minutes=int(_widget("watermark_threshold_minutes") or "5"),
    fetch_size=int(_widget("fetch_size") or "10000"),
    num_partitions=int(_widget("num_partitions") or "8"),
)

if action == "plan_configs":
    if not runtime.ingestion_group:
        raise ValueError("ingestion_group is required for plan_configs")
    result = IngestionConfigPlanner(spark=spark, runtime=runtime).build_for_each_inputs(
        ingestion_group=runtime.ingestion_group
    )
    display(spark.createDataFrame(result))
elif action == "run_ingestion":
    result = JdbcIngestionOrchestrator(spark=spark, runtime=runtime).run_selected_ingestion()
    display(spark.createDataFrame(result))
elif action == "setup_uc":
    setup = UnityCatalogSetup(spark=spark, catalog=runtime.catalog, schema=runtime.schema)
    setup.create_all(truncate_existing=_as_bool("truncate_existing"))
    result = {
        "schema": f"{runtime.catalog}.{runtime.schema}",
        "config_table": setup.config_table,
        "state_table": setup.state_table,
        "truncate_existing": _as_bool("truncate_existing"),
    }
    print(json.dumps(result, indent=2))
elif action == "setup_lakebase":
    setup = LakebaseTestDatabaseSetup(
        project_id=_widget("project_id"),
        project_display_name=_widget("project_display_name"),
    )
    setup.ensure_project()
    setup.create_standard_tables()
    setup.seed_standard_data(
        customer_count=int(_widget("customer_count") or "200"),
        product_count=int(_widget("product_count") or "100"),
        order_count=int(_widget("order_count") or "500"),
    )
    if _as_bool("simulate_updates"):
        setup.simulate_updates()
    result = setup.jdbc_settings()
    print(json.dumps(result, indent=2))
elif action == "create_job":
    wheel_uri = _widget("wheel_uri")
    if not wheel_uri:
        raise ValueError("wheel_uri is required for create_job")
    if not runtime.ingestion_group:
        raise ValueError("ingestion_group is required for create_job")

    job_settings = JobProvisioningSettings(
        ingestion_group=runtime.ingestion_group,
        catalog=runtime.catalog,
        schema=runtime.schema,
        wheel_uri=wheel_uri,
        cdc_pipeline_id=_widget("cdc_pipeline_id"),
        cdc_pipeline_file_path=_widget("cdc_pipeline_file_path"),
        foreach_concurrency=int(_widget("foreach_concurrency") or "4"),
        jdbc_url=runtime.jdbc_url,
        jdbc_user=runtime.jdbc_user,
        jdbc_secret_scope=runtime.jdbc_secret_scope,
        jdbc_secret_key=runtime.jdbc_secret_key,
        watermark_threshold_minutes=str(runtime.watermark_threshold_minutes),
        fetch_size=str(runtime.fetch_size),
        num_partitions=str(runtime.num_partitions),
    )
    result = IngestionJobProvisioner().create_or_update_job(job_settings)
    print(json.dumps(result, indent=2))
else:
    raise ValueError(f"Unsupported action: {action}")


# COMMAND ----------

# DBTITLE 1,Upsert ingestion config rows
# ── Upsert ingestion config rows ─────────────────────────────────────────────
# Edit table_configs below and run this cell to add or update rows.
# Merge key: (ingestion_group, source_table_name)

table_configs = [
    dict(
        ingestion_group   = "epic",
        source_table_name = "epic.patients",
        target_table_name = None,           # None -> auto: staging_<source_table>
        ingestion_type    = "incremental",  # "incremental" | "full"
        key_columns       = "patient_id",
        watermark_column  = "updated_at",
        partition_column  = "patient_id",
        predicate_column  = None,
        epic_csa_enabled  = True,
        enabled           = True,
    ),
    dict(
        ingestion_group   = "epic",
        source_table_name = "epic.encounters",
        target_table_name = None,           # None -> auto: staging_<source_table>
        ingestion_type    = "incremental",  # "incremental" | "full"
        key_columns       = "encounter_id",
        watermark_column  = "modified_at",
        partition_column  = "encounter_id",
        predicate_column  = None,
        epic_csa_enabled  = True,
        enabled           = True,
    ),
    # Add more tables here...
]

# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

_cfg_schema = StructType([
    StructField("ingestion_group",   StringType(),  False),
    StructField("source_table_name", StringType(),  False),
    StructField("target_table_name", StringType(),  True),
    StructField("ingestion_type",    StringType(),  True),
    StructField("key_columns",       StringType(),  True),
    StructField("watermark_column",  StringType(),  True),
    StructField("partition_column",  StringType(),  True),
    StructField("predicate_column",  StringType(),  True),
    StructField("epic_csa_enabled",  BooleanType(), True),
    StructField("enabled",           BooleanType(), False),
])

_rows = [
    (
        r["ingestion_group"],
        r["source_table_name"],
        r.get("target_table_name"),
        r.get("ingestion_type", "incremental"),
        r.get("key_columns"),
        r.get("watermark_column"),
        r.get("partition_column"),
        r.get("predicate_column"),
        r.get("epic_csa_enabled", False),
        r.get("enabled", True),
    )
    for r in table_configs
]

_catalog       = dbutils.widgets.get("catalog").strip()
_schema        = dbutils.widgets.get("schema").strip()
_config_table  = f"{_catalog}.{_schema}.jdbc_ingestion_config"

(
    spark.createDataFrame(_rows, schema=_cfg_schema)
         .withColumn("update_dttm", F.current_timestamp())
         .createOrReplaceTempView("_upsert_configs")
)

spark.sql(f"""
    MERGE INTO {_config_table} AS t
    USING _upsert_configs AS s
    ON  t.ingestion_group   = s.ingestion_group
    AND t.source_table_name = s.source_table_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Upserted {len(table_configs)} row(s) into {_config_table}")
display(
    spark.table(_config_table)
         .where(F.col("ingestion_group").isin([r["ingestion_group"] for r in table_configs]))
         .orderBy("ingestion_group", "source_table_name")
)
