# Databricks notebook source
# DBTITLE 1,Run ingestion for one table
import json
from pyspark.sql import SparkSession
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings

spark = SparkSession.getActiveSession()

runtime = JdbcRuntimeSettings(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    ingestion_group=dbutils.widgets.get("ingestion_group"),
    source_table_name=dbutils.widgets.get("source_table_name"),
    jdbc_url=dbutils.widgets.get("jdbc_url"),
    jdbc_user=dbutils.widgets.get("jdbc_user"),
    jdbc_secret_scope=dbutils.widgets.get("jdbc_secret_scope"),
    jdbc_secret_key=dbutils.widgets.get("jdbc_secret_key"),
    watermark_threshold_minutes=int(dbutils.widgets.get("watermark_threshold_minutes") or "5"),
    fetch_size=int(dbutils.widgets.get("fetch_size") or "10000"),
    num_partitions=int(dbutils.widgets.get("num_partitions") or "8"),
)

orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
result = orchestrator.run_selected_ingestion()
print(json.dumps(result, default=str))

# COMMAND ----------


