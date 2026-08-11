# Databricks notebook source
# DBTITLE 1,Plan configs and publish task value
from pyspark.sql import SparkSession
from watersync.config_planner import IngestionConfigPlanner
from watersync.models import JdbcRuntimeSettings

spark = SparkSession.getActiveSession()

runtime = JdbcRuntimeSettings(
    configuration_fqn=dbutils.widgets.get("configuration_fqn"),
    watermark_fqn=dbutils.widgets.get("watermark_fqn"),
    ingestion_group=dbutils.widgets.get("ingestion_group"),
)

planner = IngestionConfigPlanner(spark=spark, runtime=runtime)
payload = planner.build_for_each_inputs_json(runtime.ingestion_group)

# Publish task value for downstream ForEach task
dbutils.jobs.taskValues.set(key="table_configs", value=payload)
print(payload)

# COMMAND ----------

