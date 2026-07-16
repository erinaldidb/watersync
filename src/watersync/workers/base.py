from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from watersync.common import quote_sql_string
from watersync.models import IngestionConfig, JdbcRuntimeSettings, ReadResult


class JdbcIngestionWorker(ABC):
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings, config: IngestionConfig):
        self.spark = spark
        self.runtime = runtime
        self.config = config
        self.staging_table_fqn = f"{runtime.catalog}.{runtime.schema}.{config.target_table_name}"

    def process(self) -> dict[str, Any]:
        read_result = self.read_source()
        if read_result.skip:
            self.update_watermark_state(None, "SKIPPED", None)
            return self._result("SKIPPED")

        _, max_staging_watermark = self.write_to_staging(read_result.df)
        watermark_to_store = self.resolve_watermark_to_store(read_result, max_staging_watermark)
        self.update_watermark_state(watermark_to_store, "SUCCESS", None)
        return self._result("SUCCESS", watermark_to_store)

    def _result(
        self,
        status: str,
        watermark: str | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "group": self.config.ingestion_group,
            "table": self.config.source_table_name,
            "target": self.staging_table_fqn,
            "ingestion_type": self.config.ingestion_type,
            "status": status,
        }
        if watermark is not None:
            payload["watermark"] = watermark
        if error is not None:
            payload["error"] = error
        return payload

    @abstractmethod
    def read_source(self) -> ReadResult:
        raise NotImplementedError

    def resolve_watermark_to_store(
        self,
        read_result: ReadResult,
        max_staging_watermark: Any,
    ) -> str | None:
        if read_result.persisted_watermark is not None:
            return read_result.persisted_watermark
        if max_staging_watermark is None:
            return None
        return str(max_staging_watermark)[:19]

    def build_jdbc_reader(
        self,
        dbtable: str,
        partition_column: str | None = None,
        lower_bound: int | None = None,
        upper_bound: int | None = None,
    ):
        reader = (
            self.spark.read.format("jdbc")
            .option("dbtable", dbtable)
            .options(**self.runtime.jdbc_properties)
        )
        if self.runtime.jdbc_url:
            reader = reader.option("url", self.runtime.jdbc_url)
        else:
            reader = reader.option("databricks.connection", self.runtime.connection_name)

        if (
            partition_column
            and lower_bound is not None
            and upper_bound is not None
            and lower_bound != upper_bound
        ):
            reader = (
                reader.option("partitionColumn", partition_column)
                .option("lowerBound", lower_bound)
                .option("upperBound", upper_bound)
                .option("numPartitions", self.runtime.num_partitions)
            )
        return reader

    def build_jdbc_reader_with_predicates(self, dbtable: str, predicates: list[str]):
        if not self.runtime.jdbc_url:
            raise ValueError("Predicate-based parallel reads require runtime.jdbc_url")
        return self.spark.read.jdbc(
            url=self.runtime.jdbc_url,
            table=dbtable,
            predicates=predicates,
            properties=self.runtime.jdbc_properties,
        )

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
        return "1900-01-01 00:00:00"

    def get_partition_bounds(
        self,
        last_watermark: str | None = None,
        cutoff: str | None = None,
    ) -> tuple[int | None, int | None]:
        if not self.config.partition_column:
            return None, None

        if self.config.ingestion_type == "incremental" and last_watermark:
            predicates = [
                f"{self.config.watermark_column} > CAST('{quote_sql_string(last_watermark)}' AS TIMESTAMP)"
            ]
            if cutoff:
                predicates.append(
                    f"{self.config.watermark_column} <= CAST('{quote_sql_string(cutoff)}' AS TIMESTAMP)"
                )
            where_clause = "WHERE " + " AND ".join(predicates)
        else:
            where_clause = ""

        bounds_query = (
            f"(SELECT MIN({self.config.partition_column}) AS min_val, "
            f"MAX({self.config.partition_column}) AS max_val "
            f"FROM {self.config.source_table_name} {where_clause}) AS bounds"
        )
        row = self.build_jdbc_reader(bounds_query).load().first()
        if row is None or row["min_val"] is None or row["max_val"] is None:
            return None, None
        return int(row["min_val"]), int(row["max_val"])

    def build_predicate_boundaries(
        self,
        last_watermark: str | None = None,
        cutoff: str | None = None,
    ) -> list[str]:
        if not self.config.predicate_column:
            return []

        if self.config.ingestion_type == "incremental" and last_watermark:
            wm_filter = [
                f"{self.config.watermark_column} > CAST('{quote_sql_string(last_watermark)}' AS TIMESTAMP)"
            ]
            if cutoff:
                wm_filter.append(
                    f"{self.config.watermark_column} <= CAST('{quote_sql_string(cutoff)}' AS TIMESTAMP)"
                )
            where_clause = "WHERE " + " AND ".join(wm_filter)
        else:
            where_clause = ""

        bounds_query = (
            f"(SELECT MIN({self.config.predicate_column}) AS boundary_val FROM ("
            f"SELECT {self.config.predicate_column}, "
            f"NTILE({self.runtime.num_partitions}) OVER (ORDER BY {self.config.predicate_column}) AS bucket "
            f"FROM {self.config.source_table_name} {where_clause}"
            f") sub WHERE bucket > 1 GROUP BY bucket) AS bounds"
        )
        return sorted(
            row["boundary_val"]
            for row in self.build_jdbc_reader(bounds_query).load().collect()
            if row["boundary_val"] is not None
        )

    @staticmethod
    def build_string_predicates(
        predicate_column: str,
        boundaries: list[str],
    ) -> list[str]:
        if not boundaries:
            return ["1=1"]

        predicates = [
            f"({predicate_column} < '{quote_sql_string(boundaries[0])}' OR {predicate_column} IS NULL)"
        ]
        for idx in range(len(boundaries) - 1):
            lower = quote_sql_string(boundaries[idx])
            upper = quote_sql_string(boundaries[idx + 1])
            predicates.append(
                f"{predicate_column} >= '{lower}' AND {predicate_column} < '{upper}'"
            )
        predicates.append(
            f"{predicate_column} >= '{quote_sql_string(boundaries[-1])}'"
        )
        return predicates

    def read_source_jdbc_standard(self, last_watermark: str | None = None):
        if self.config.ingestion_type == "incremental":
            cutoff = (
                datetime.now()
                - timedelta(minutes=self.runtime.watermark_threshold_minutes)
            ).strftime("%Y-%m-%d %H:%M:%S")
            source_query = (
                f"(SELECT * FROM {self.config.source_table_name} "
                f"WHERE {self.config.watermark_column} > CAST('{quote_sql_string(last_watermark)}' AS TIMESTAMP) "
                f"AND {self.config.watermark_column} <= CAST('{quote_sql_string(cutoff)}' AS TIMESTAMP)) AS source_data"
            )
        else:
            cutoff = None
            source_query = f"(SELECT * FROM {self.config.source_table_name}) AS source_data"

        if self.config.predicate_column:
            boundaries = self.build_predicate_boundaries(last_watermark, cutoff)
            predicates = self.build_string_predicates(
                self.config.predicate_column,
                boundaries,
            )
            return self.build_jdbc_reader_with_predicates(source_query, predicates)

        if self.config.partition_column:
            lower_bound, upper_bound = self.get_partition_bounds(last_watermark, cutoff)
            return self.build_jdbc_reader(
                source_query,
                self.config.partition_column,
                lower_bound,
                upper_bound,
            ).load()

        return self.build_jdbc_reader(source_query).load()

    def write_to_staging(self, df):
        df_with_metadata = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_table", F.lit(self.config.source_table_name))
            .withColumn("_ingestion_group", F.lit(self.config.ingestion_group))
            .withColumn("_ingestion_type", F.lit(self.config.ingestion_type))
        )
        writer = df_with_metadata.write.format("delta").option("clusterByAuto", "true")
        if self.config.ingestion_type == "full":
            writer = writer.mode("overwrite").option("overwriteSchema", "true")
        else:
            writer = writer.mode("append").option("mergeSchema", "true")
        writer.saveAsTable(self.staging_table_fqn)

        max_watermark = None
        if self.config.watermark_column:
            max_watermark = (
                self.spark.table(self.staging_table_fqn)
                .select(F.max(F.col(self.config.watermark_column)).alias("max_wm"))
                .first()["max_wm"]
            )
        return self.staging_table_fqn, max_watermark

    def update_watermark_state(
        self,
        max_watermark: Any,
        status: str,
        error_message: str | None = None,
    ) -> None:
        now = datetime.now()
        state_df = self.spark.createDataFrame(
            [
                (
                    self.config.ingestion_group,
                    self.config.source_table_name,
                    self.config.target_table_name,
                    self.config.ingestion_type,
                    str(max_watermark)[:19] if max_watermark is not None else None,
                    now,
                    status,
                    error_message,
                )
            ],
            schema=(
                "ingestion_group STRING, source_table_name STRING, "
                "target_table_name STRING, ingestion_type STRING, "
                "last_watermark STRING, last_run_timestamp TIMESTAMP, "
                "status STRING, last_error STRING"
            ),
        )
        delta_state = DeltaTable.forName(self.spark, self.runtime.state_table)
        (
            delta_state.alias("target")
            .merge(
                state_df.alias("source"),
                " AND ".join(
                    [
                        "target.ingestion_group = source.ingestion_group",
                        "target.source_table_name = source.source_table_name",
                        "target.target_table_name = source.target_table_name",
                        "target.ingestion_type = source.ingestion_type",
                    ]
                ),
            )
            .whenMatchedUpdate(
                set={
                    "last_watermark": "COALESCE(source.last_watermark, target.last_watermark)",
                    "last_run_timestamp": "source.last_run_timestamp",
                    "status": "source.status",
                    "last_error": "source.last_error",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
