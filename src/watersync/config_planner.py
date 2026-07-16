from __future__ import annotations

import json
from typing import Any, Callable

from watersync.common import quote_sql_string, row_to_dict
from watersync.models import JdbcRuntimeSettings


class JdbcIngestionConfigRepository:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.spark = spark
        self.runtime = runtime

    def load_for_group(self, ingestion_group: str) -> list[dict[str, Any]]:
        if not ingestion_group:
            raise ValueError("ingestion_group is required to plan fanout inputs")

        query = f"""
            SELECT
                ingestion_group,
                source_table_name,
                coalesce(target_table_name, concat('staging_', regexp_extract(source_table_name, '([^\\.]+)', 1))) AS target_table_name,
                lower(coalesce(ingestion_type, 'incremental')) AS ingestion_type
            FROM {self.runtime.config_table}
            WHERE enabled = true
              AND ingestion_group = '{quote_sql_string(ingestion_group)}'
            ORDER BY source_table_name
        """
        configs = [row_to_dict(row) for row in self.spark.sql(query).collect()]
        if not configs:
            raise ValueError(f"No enabled config rows found for ingestion_group={ingestion_group}")
        return configs


class IngestionConfigPlanner:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.repository = JdbcIngestionConfigRepository(spark=spark, runtime=runtime)

    def build_for_each_inputs(self, ingestion_group: str) -> list[dict[str, Any]]:
        return self.repository.load_for_group(ingestion_group=ingestion_group)

    def build_for_each_inputs_json(self, ingestion_group: str) -> str:
        return json.dumps(self.build_for_each_inputs(ingestion_group=ingestion_group))

    def publish_task_value(
        self,
        ingestion_group: str,
        writer: Callable[[str, str], None] | None = None,
        task_value_key: str = "table_configs",
    ) -> str:
        payload = self.build_for_each_inputs_json(ingestion_group=ingestion_group)
        if writer is not None:
            writer(task_value_key, payload)
        return payload
