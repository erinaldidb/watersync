from __future__ import annotations

import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from watersync.cdc_pipeline import build_pipeline_from_spark_conf

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
build_pipeline_from_spark_conf(spark=spark, dp_module=dp)
