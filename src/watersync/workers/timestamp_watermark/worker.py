from __future__ import annotations

from datetime import datetime, timedelta

from watersync.common import quote_sql_string
from watersync.models import ReadResult
from watersync.workers.base import JdbcIngestionWorker


class TimestampWatermarkIngestionWorker(JdbcIngestionWorker):
    def read_source(self) -> ReadResult:
        if self.config.ingestion_type != "incremental":
            return ReadResult(df=self.read_source_jdbc_standard(None))

        last_watermark = self.get_last_watermark()
        cutoff = (
            datetime.now() - timedelta(minutes=self.runtime.watermark_threshold_minutes)
        ).strftime("%Y-%m-%d %H:%M:%S")
        exists_query = (
            f"(SELECT 1 AS has_rows FROM {self.config.source_table_name} "
            f"WHERE {self.config.watermark_column} > CAST('{quote_sql_string(last_watermark)}' AS TIMESTAMP) "
            f"AND {self.config.watermark_column} <= CAST('{quote_sql_string(cutoff)}' AS TIMESTAMP) LIMIT 1) AS change_check"
        )
        has_rows = self.build_jdbc_reader(exists_query).load().first() is not None
        if not has_rows:
            return ReadResult(df=None, skip=True)
        return ReadResult(df=self.read_source_jdbc_standard(last_watermark))
