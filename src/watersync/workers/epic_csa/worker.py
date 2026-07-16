from __future__ import annotations

from pyspark.sql import functions as F

from watersync.common import quote_sql_string
from watersync.models import ReadResult
from watersync.workers.base import JdbcIngestionWorker


class EpicCsaIngestionWorker(JdbcIngestionWorker):
    def derive_csa_table_name(self) -> str:
        return f"epic_util.csa_{self.config.source_table_name.split('.')[-1].lower()}"

    def get_last_watermark(self) -> str:
        row = self.spark.sql(
            f"""
            SELECT last_watermark
            FROM {self.runtime.state_table}
            WHERE ingestion_group = '{quote_sql_string(self.config.ingestion_group)}'
              AND source_table_name = '{quote_sql_string(self.config.source_table_name)}'
              AND target_table_name = '{quote_sql_string(self.config.target_table_name)}'
              AND ingestion_type = '{quote_sql_string(self.config.ingestion_type)}'
              AND last_watermark IS NOT NULL
            ORDER BY last_run_timestamp DESC
            LIMIT 1
            """
        ).first()
        if row and row["last_watermark"] is not None:
            return str(row["last_watermark"])
        return "-1"

    def get_csa_max_watermark(self) -> int | None:
        query = (
            "(SELECT MAX(CAST(_TIMESTAMP_EXTRACT_KEY AS BIGINT)) AS max_csa_wm "
            f"FROM {self.derive_csa_table_name()}) AS csa_max"
        )
        row = self.build_jdbc_reader(query).load().first()
        if row and row["max_csa_wm"] is not None:
            return int(row["max_csa_wm"])
        return None

    def read_full_source_jdbc(self):
        source_query = f"(SELECT * FROM {self.config.source_table_name}) AS source_data"

        if self.config.predicate_column:
            boundaries = self.build_predicate_boundaries()
            predicates = self.build_string_predicates(
                self.config.predicate_column,
                boundaries,
            )
            df = self.build_jdbc_reader_with_predicates(source_query, predicates)
        elif self.config.partition_column:
            lower_bound, upper_bound = self.get_partition_bounds()
            df = self.build_jdbc_reader(
                source_query,
                self.config.partition_column,
                lower_bound,
                upper_bound,
            ).load()
        else:
            df = self.build_jdbc_reader(source_query).load()

        if "_IS_DELETED" not in df.columns:
            df = df.withColumn("_IS_DELETED", F.lit(False))
        if "_csa_update_dt" not in df.columns:
            df = df.withColumn(
                "_csa_update_dt",
                F.lit("1970-01-01 00:00:00").cast("timestamp"),
            )
        return df

    def read_source_jdbc_via_csa(
        self,
        last_csa_watermark: int,
        new_csa_watermark: int,
    ):
        join_keys = self.config.key_column_list
        if not join_keys:
            raise ValueError(
                f"EPIC CSA config row for {self.config.source_table_name} requires key_columns"
            )

        join_condition = " AND ".join(f"csa.{key} = main.{key}" for key in join_keys)
        wm_filter = (
            f"CAST(csa._TIMESTAMP_EXTRACT_KEY AS BIGINT) > {last_csa_watermark} "
            f"AND CAST(csa._TIMESTAMP_EXTRACT_KEY AS BIGINT) <= {new_csa_watermark}"
        )
        csa_key_aliases = ", ".join(
            f"csa.{key} AS _csa_key_{key}" for key in join_keys
        )
        source_query = (
            f"(SELECT csa._IS_DELETED, csa._UPDATE_DT AS _csa_update_dt, {csa_key_aliases}, main.* "
            f"FROM {self.derive_csa_table_name()} csa "
            f"LEFT JOIN {self.config.source_table_name} main ON {join_condition} "
            f"WHERE {wm_filter}) AS csa_source"
        )
        df = self.build_jdbc_reader(source_query).load()
        for key in join_keys:
            df = df.withColumn(key, F.coalesce(F.col(key), F.col(f"_csa_key_{key}")))
        return df.drop(*[f"_csa_key_{key}" for key in join_keys])

    def read_source(self) -> ReadResult:
        last_csa_bigint = int(self.get_last_watermark())
        new_csa_watermark = self.get_csa_max_watermark()
        if new_csa_watermark is None:
            return ReadResult(df=None, skip=True)

        if last_csa_bigint == -1:
            source_df = self.read_full_source_jdbc()
        else:
            if new_csa_watermark <= last_csa_bigint:
                return ReadResult(df=None, skip=True)
            source_df = self.read_source_jdbc_via_csa(
                last_csa_bigint,
                new_csa_watermark,
            )

        return ReadResult(df=source_df, persisted_watermark=str(new_csa_watermark))
