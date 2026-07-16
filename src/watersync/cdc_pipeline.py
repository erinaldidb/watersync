from __future__ import annotations

from typing import Any

from pyspark.sql.functions import col

from watersync.common import quote_sql_string, row_to_dict

_METADATA_COLUMNS = ["_ingested_at", "_source_table", "_ingestion_group", "_ingestion_type"]
_CSA_CONTROL_COLUMNS = ["_IS_DELETED", "_csa_update_dt"]


class CdcScd2PipelineBuilder:
    def __init__(self, spark: Any, dp_module: Any, catalog: str, schema: str, ingestion_group: str = ""):
        self.spark = spark
        self.dp = dp_module
        self.catalog = catalog
        self.schema = schema
        self.ingestion_group = ingestion_group

    @property
    def config_table(self) -> str:
        return f"{self.catalog}.{self.schema}.jdbc_ingestion_config"

    def load_configs(self) -> list[dict[str, Any]]:
        filters = ["enabled = true"]
        if self.ingestion_group:
            filters.append(f"ingestion_group = '{quote_sql_string(self.ingestion_group)}'")
        rows = self.spark.read.table(self.config_table).filter(" AND ".join(filters)).collect()
        configs = [row_to_dict(row) for row in rows]
        if not configs:
            suffix = f" for ingestion_group='{self.ingestion_group}'" if self.ingestion_group else ""
            raise ValueError(f"No enabled config rows found{suffix}")
        return configs

    def make_snapshot_source(self, staging_fqn: str):
        def next_snapshot_and_version(latest_snapshot_version):
            versions = [
                row["version"]
                for row in self.spark.sql(f"DESCRIBE HISTORY {staging_fqn}").select("version").orderBy("version").collect()
            ]
            if not versions:
                return None
            next_version = versions[0] if latest_snapshot_version is None else next((v for v in versions if v > latest_snapshot_version), None)
            if next_version is None:
                return None
            df = self.spark.read.format("delta").option("versionAsOf", next_version).table(staging_fqn)
            return df, next_version
        return next_snapshot_and_version

    def register_incremental_standard_flow(self, history_table: str, staging_table: str, staging_fqn: str, key_columns: list[str], watermark_column: str) -> None:
        @self.dp.view(name=f"v_{staging_table}", comment=f"Streaming view on {staging_fqn}")
        def _make_view(_tbl=staging_fqn):
            return self.spark.readStream.table(_tbl)

        self.dp.create_auto_cdc_flow(
            target=history_table,
            source=f"v_{staging_table}",
            keys=key_columns,
            sequence_by=col(watermark_column),
            stored_as_scd_type="2",
            except_column_list=_METADATA_COLUMNS,
        )

    def register_incremental_csa_flow(self, history_table: str, staging_table: str, staging_fqn: str, key_columns: list[str]) -> None:
        @self.dp.view(
            name=f"v_{staging_table}_upserts",
            comment=f"Upsert events (_IS_DELETED=0 or NULL full-load rows) from {staging_fqn}",
        )
        def _make_upsert_view(_tbl=staging_fqn):
            return self.spark.readStream.table(_tbl).filter("COALESCE(_IS_DELETED, 0) = 0")

        self.dp.create_auto_cdc_flow(
            name=f"{history_table}_upserts",
            target=history_table,
            source=f"v_{staging_table}_upserts",
            keys=key_columns,
            sequence_by=col("_csa_update_dt"),
            stored_as_scd_type="2",
            except_column_list=_METADATA_COLUMNS + _CSA_CONTROL_COLUMNS,
        )

        history_fqn = f"{self.catalog}.{self.schema}.{history_table}"
        sink_name = f"{history_table}_delete_sink"

        @self.dp.foreach_batch_sink(name=sink_name)
        def _delete_sink(batch_df, batch_id, _hist=history_fqn, _keys=list(key_columns)):
            if batch_df.isEmpty():
                return
            tmp_view = f"_csa_del_{_hist.split('.')[-1]}"
            batch_df.createOrReplaceTempView(tmp_view)
            key_cond = " AND ".join(f"h.`{key}` = d.`{key}`" for key in _keys)
            history_quoted = ".".join(f"`{part}`" for part in _hist.split("."))
            batch_df.sparkSession.sql(f"""
                MERGE INTO {history_quoted} AS h
                USING {tmp_view} AS d
                ON {key_cond} AND h.__END_AT IS NULL
                WHEN MATCHED THEN UPDATE SET h.__END_AT = d._csa_update_dt
            """)

        @self.dp.update_flow(target=sink_name, name=f"{history_table}_close_deletes")
        def _close_deletes_flow(_tbl=staging_fqn, _keys=list(key_columns)):
            return self.spark.readStream.table(_tbl).filter("_IS_DELETED = 1").select(*_keys, "_csa_update_dt")

    def register_snapshot_flow(self, history_table: str, staging_fqn: str, key_columns: list[str]) -> None:
        self.dp.create_auto_cdc_from_snapshot_flow(
            target=history_table,
            source=self.make_snapshot_source(staging_fqn),
            keys=key_columns,
            stored_as_scd_type=2,
            except_column_list=_METADATA_COLUMNS,
        )

    def build(self) -> None:
        for config in self.load_configs():
            source_table = config["source_table_name"]
            staging_table = config["target_table_name"]
            key_columns = [key.strip() for key in (config["key_columns"] or "").split(",") if key.strip()]
            watermark_column = (config["watermark_column"] or "").strip()
            ingestion_type = (config["ingestion_type"] or "incremental").strip().lower()
            epic_csa_enabled = bool(config["epic_csa_enabled"])
            staging_fqn = f"{self.catalog}.{self.schema}.{staging_table}"
            history_table = f"{source_table.split('.')[-1]}_history"

            self.dp.create_streaming_table(
                name=history_table,
                comment=(
                    f"SCD Type 2 history for {source_table} "
                    f"(group: {config['ingestion_group']}, mode: {ingestion_type}). "
                    f"Keys: {key_columns}"
                ),
            )

            if ingestion_type == "incremental":
                if epic_csa_enabled:
                    self.register_incremental_csa_flow(history_table, staging_table, staging_fqn, key_columns)
                else:
                    self.register_incremental_standard_flow(history_table, staging_table, staging_fqn, key_columns, watermark_column)
            else:
                self.register_snapshot_flow(history_table, staging_fqn, key_columns)


def build_pipeline_from_spark_conf(spark: Any, dp_module: Any) -> None:
    builder = CdcScd2PipelineBuilder(
        spark=spark,
        dp_module=dp_module,
        catalog=spark.conf.get("pipeline.catalog", "users"),
        schema=spark.conf.get("pipeline.schema", "default"),
        ingestion_group=spark.conf.get("pipeline.ingestion_group", ""),
    )
    builder.build()
