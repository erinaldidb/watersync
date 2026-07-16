from __future__ import annotations

from typing import Any

from watersync.common import (
    normalize_ingestion_type,
    normalize_text,
    quote_sql_string,
    resolve_target_table_name,
    row_to_dict,
)
from watersync.models import IngestionConfig, JdbcRuntimeSettings
from watersync.workers import (
    EpicCsaIngestionWorker,
    JdbcIngestionWorker,
    TimestampWatermarkIngestionWorker,
)


class JdbcIngestionConfigRepository:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.spark = spark
        self.runtime = runtime

    def load_selected_configs(
        self,
        ingestion_group: str = "",
        source_table_name: str = "",
    ) -> list[IngestionConfig]:
        filters = ["enabled = true"]
        if ingestion_group:
            filters.append(f"ingestion_group = '{quote_sql_string(ingestion_group)}'")
        if source_table_name:
            filters.append(f"source_table_name = '{quote_sql_string(source_table_name)}'")

        query = f"""
            SELECT
                ingestion_group,
                source_table_name,
                target_table_name,
                lower(coalesce(ingestion_type, 'incremental')) AS ingestion_type,
                key_columns,
                watermark_column,
                partition_column,
                predicate_column,
                epic_csa_enabled
            FROM {self.runtime.config_table}
            WHERE {' AND '.join(filters)}
            ORDER BY ingestion_group, source_table_name
        """

        configs: list[IngestionConfig] = []
        for row in self.spark.sql(query).collect():
            row_dict = row_to_dict(row)
            config = IngestionConfig(
                ingestion_group=normalize_text(row_dict.get("ingestion_group")),
                source_table_name=normalize_text(row_dict.get("source_table_name")),
                target_table_name=resolve_target_table_name(
                    row_dict.get("target_table_name"),
                    normalize_text(row_dict.get("source_table_name")),
                ),
                ingestion_type=normalize_ingestion_type(row_dict.get("ingestion_type")),
                key_columns=row_dict.get("key_columns"),
                watermark_column=normalize_text(row_dict.get("watermark_column")),
                partition_column=normalize_text(row_dict.get("partition_column")),
                predicate_column=normalize_text(row_dict.get("predicate_column")),
                epic_csa_enabled=bool(row_dict.get("epic_csa_enabled")),
            )
            if not config.ingestion_group:
                raise ValueError(
                    f"Config row for {config.source_table_name} is missing ingestion_group"
                )
            if (
                config.ingestion_type == "incremental"
                and not config.watermark_column
                and not config.epic_csa_enabled
            ):
                raise ValueError(
                    f"Incremental config row for {config.source_table_name} requires watermark_column"
                )
            configs.append(config)
        return configs


class JdbcIngestionOrchestrator:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.spark = spark
        self.runtime = runtime
        self.repository = JdbcIngestionConfigRepository(spark=spark, runtime=runtime)

    def build_worker(self, config: IngestionConfig) -> JdbcIngestionWorker:
        if config.epic_csa_enabled:
            return EpicCsaIngestionWorker(self.spark, self.runtime, config)
        return TimestampWatermarkIngestionWorker(self.spark, self.runtime, config)

    def run_selected_ingestion(self) -> list[dict[str, Any]]:
        configs = self.repository.load_selected_configs(
            ingestion_group=self.runtime.ingestion_group,
            source_table_name=self.runtime.source_table_name,
        )
        results: list[dict[str, Any]] = []
        for config in configs:
            worker = self.build_worker(config)
            try:
                results.append(worker.process())
            except Exception as exc:
                worker.update_watermark_state(None, "FAILED", str(exc)[:4000])
                results.append(worker._result("FAILED", error=str(exc)))

        failed = [result for result in results if result["status"] == "FAILED"]
        if failed:
            raise RuntimeError(f"{len(failed)} table(s) failed during ingestion")
        return results


__all__ = [
    "EpicCsaIngestionWorker",
    "JdbcIngestionConfigRepository",
    "JdbcIngestionOrchestrator",
    "JdbcIngestionWorker",
    "TimestampWatermarkIngestionWorker",
]
