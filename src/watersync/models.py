from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

_LOGGING_CONFIGURED = False


def _configure_default_logging() -> None:
    """Configure console logging for the watersync namespace (singleton)."""
    global _LOGGING_CONFIGURED  # noqa: PLW0603
    if _LOGGING_CONFIGURED:
        return
    _LOGGING_CONFIGURED = True

    ws_logger = logging.getLogger("watersync")
    ws_logger.setLevel(logging.INFO)
    if not any(isinstance(h, logging.StreamHandler) for h in ws_logger.handlers):
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(levelname)-8s %(name)s \u2014 %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        ws_logger.addHandler(console)
    ws_logger.propagate = False


@dataclass(frozen=True)
class JdbcRuntimeSettings:
    catalog: str
    schema: str
    ingestion_group: str = ""
    source_table_name: str = ""
    jdbc_url: str = ""
    jdbc_user: str = ""
    jdbc_password: str = ""
    jdbc_secret_scope: str = ""
    jdbc_secret_key: str = ""
    watermark_threshold_minutes: int = 5
    fetch_size: int = 10000
    num_partitions: int = 8
    connection_name: str = "slalom_jdbc_conn"

    def __post_init__(self) -> None:
        _configure_default_logging()

    @property
    def config_table(self) -> str:
        return f"{self.catalog}.{self.schema}.jdbc_ingestion_config"

    @property
    def state_table(self) -> str:
        return f"{self.catalog}.{self.schema}.jdbc_ingestion_watermark"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        properties = {"fetchsize": str(self.fetch_size)}
        if self.jdbc_user:
            properties["user"] = self.jdbc_user
        if self.jdbc_password:
            properties["password"] = self.jdbc_password
        return properties


@dataclass(frozen=True)
class IngestionConfig:
    ingestion_group: str
    source_table_name: str
    target_table_name: str
    ingestion_type: str
    key_columns: str | None
    watermark_column: str
    partition_column: str
    predicate_column: str
    epic_csa_enabled: bool = False

    @property
    def key_column_list(self) -> list[str]:
        return [key.strip() for key in (self.key_columns or "").split(",") if key.strip()]


@dataclass
class ReadResult:
    df: Any | None
    persisted_watermark: str | None = None
    skip: bool = False


@dataclass(frozen=True)
class JobProvisioningSettings:
    ingestion_group: str
    catalog: str
    schema: str
    wheel_uri: str
    package_name: str = "watersync"
    planner_entry_point: str = "watersync-plan-configs"
    worker_entry_point: str = "watersync-run-ingestion"
    foreach_concurrency: int = 4
    jdbc_url: str = ""
    jdbc_user: str = ""
    jdbc_secret_scope: str = ""
    jdbc_secret_key: str = ""
    watermark_threshold_minutes: str = "5"
    fetch_size: str = "10000"
    num_partitions: str = "8"
    cdc_pipeline_id: str = ""
    cdc_pipeline_file_path: str = ""
