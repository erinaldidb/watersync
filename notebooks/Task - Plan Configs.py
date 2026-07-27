# Databricks notebook source
# DBTITLE 1,Plan configs and publish task value
from pyspark.sql import SparkSession
from watersync.config_planner import IngestionConfigPlanner
from watersync.models import JdbcRuntimeSettings

spark = SparkSession.getActiveSession()

runtime = JdbcRuntimeSettings(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    ingestion_group=dbutils.widgets.get("ingestion_group"),
    jdbc_url=dbutils.widgets.get("jdbc_url"),
    jdbc_user=dbutils.widgets.get("jdbc_user"),
    jdbc_secret_scope=dbutils.widgets.get("jdbc_secret_scope"),
    jdbc_secret_key=dbutils.widgets.get("jdbc_secret_key"),
    watermark_threshold_minutes=int(dbutils.widgets.get("watermark_threshold_minutes") or "5"),
    fetch_size=int(dbutils.widgets.get("fetch_size") or "10000"),
    num_partitions=int(dbutils.widgets.get("num_partitions") or "8"),
)

planner = IngestionConfigPlanner(spark=spark, runtime=runtime)
payload = planner.build_for_each_inputs_json(runtime.ingestion_group)

# Publish task value for downstream ForEach task
dbutils.jobs.taskValues.set(key="table_configs", value=payload)
print(payload)

# COMMAND ----------


