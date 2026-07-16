# watersync

Python package for JDBC ingestion with watermark tracking, CDC SCD2 pipelines, and Lakeflow Job provisioning on Databricks.

---

## Architecture

```
jdbc_ingestion_config (Delta)          jdbc_ingestion_watermark (Delta)
        │                                          │
        ▼                                          │
IngestionConfigPlanner                             │
  └─ build fanout inputs per table                 │
        │                                          │
        ▼                                          │
JdbcIngestionOrchestrator                          │
  └─ dispatches one worker per config row          │
        │                                          │
        ├─ TimestampWatermarkIngestionWorker ───────┤ read/write watermark state
        └─ EpicCsaIngestionWorker ─────────────────┘
                │
                ▼
        staging Delta tables  (catalog.schema.staging_<table>)
                │
                ▼
        CDC SCD2 pipeline  (cdc_pipeline.py / pipeline_bootstrap.py)
```

A Lakeflow Job is structured as:

1. **Planner task** — runs `watersync-plan-configs`, publishes `table_configs` task value
2. **For-each task** — runs `watersync-run-ingestion` once per config row (concurrency controlled by `foreach_concurrency`)

---

## Installation

Install in editable mode from a notebook or cluster init script:

```python
%pip install -e /Workspace/Users/<user>/watersync
```

Or build and upload a wheel for production jobs:

```bash
cd watersync
pip install build
python -m build --wheel
# upload dist/watersync-0.1.0-py3-none-any.whl to a UC volume
```

---

## One-Time Setup

Create the Unity Catalog schema and the two metadata tables (`jdbc_ingestion_config` and `jdbc_ingestion_watermark`):

```python
from watersync.utils import UnityCatalogSetup

setup = UnityCatalogSetup(spark, catalog="main", schema="watersync")
setup.create_all()
# Tables created:
#   main.watersync.jdbc_ingestion_config
#   main.watersync.jdbc_ingestion_watermark
```

Or via the CLI entry point:

```bash
watersync-setup-uc --catalog main --schema watersync
```

Pass `--truncate-existing` to reset all state tables (config table is preserved).

---

## Config Table

`jdbc_ingestion_config` drives all ingestion. Each row represents one source table.

| Column | Type | Required | Description |
|---|---|---|---|
| `ingestion_group` | STRING | yes | Logical group — all rows with the same group run in one job |
| `source_table_name` | STRING | yes | Fully qualified source table (e.g. `schema.TableName`) |
| `target_table_name` | STRING | no | Override staging table name; defaults to `staging_<source_table>` |
| `ingestion_type` | STRING | no | `incremental` (default) or `full` |
| `key_columns` | STRING | no | Comma-separated business keys used by the CDC pipeline |
| `watermark_column` | STRING | yes* | Timestamp/sequence column for incremental loads (*required unless `epic_csa_enabled`) |
| `partition_column` | STRING | no | Numeric column for parallel JDBC partitioning |
| `predicate_column` | STRING | no | String column for predicate-based parallel reads |
| `epic_csa_enabled` | BOOLEAN | no | Set `true` to use the EPIC CSA worker |
| `enabled` | BOOLEAN | yes | Set `false` to skip the row without deleting it |

### Adding config rows

Use the **"Upsert ingestion config rows"** cell in the `Watersync Notebook Runner` notebook, or run SQL directly:

```sql
INSERT INTO main.watersync.jdbc_ingestion_config VALUES (
  'epic',                          -- ingestion_group
  'clarity.Clarity_ADT',           -- source_table_name
  NULL,                            -- target_table_name (auto)
  'incremental',                   -- ingestion_type
  'pat_id',                        -- key_columns
  'update_dttm',                   -- watermark_column
  NULL,                            -- partition_column
  NULL,                            -- predicate_column
  false,                           -- epic_csa_enabled
  current_timestamp(),             -- update_dttm
  true                             -- enabled
);
```

---

## Running Ingestion

### From a notebook

Use `JdbcRuntimeSettings` and `JdbcIngestionOrchestrator` directly:

```python
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings

runtime = JdbcRuntimeSettings(
    catalog="main",
    schema="watersync",
    ingestion_group="epic",
    source_table_name="",              # empty = all tables in the group
    jdbc_url="jdbc:postgresql://host:5432/clarity",
    jdbc_user="svc_account",
    jdbc_password="...",               # or use jdbc_secret_scope / jdbc_secret_key
    watermark_threshold_minutes=5,
    fetch_size=10_000,
    num_partitions=8,
)

orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
results = orchestrator.run_selected_ingestion()
```

Pass `source_table_name` to run a single table; leave empty to run every enabled table in the group.

### Via the CLI

```bash
watersync-run-ingestion \
  --catalog main \
  --schema watersync \
  --ingestion-group epic \
  --jdbc-url "jdbc:postgresql://host:5432/clarity" \
  --jdbc-secret-scope my-scope \
  --jdbc-secret-key clarity-password \
  --watermark-threshold-minutes 5 \
  --num-partitions 8
```

### Fan-out planning

The planner reads the config table and returns a JSON list of per-table inputs for the for-each task:

```python
from watersync.config_planner import IngestionConfigPlanner

planner = IngestionConfigPlanner(spark=spark, runtime=runtime)
inputs = planner.build_for_each_inputs(ingestion_group="epic")
# [{"ingestion_group": "epic", "source_table_name": "clarity.Clarity_ADT", ...}, ...]
```

Or with task-value publishing (inside a Lakeflow Job task):

```bash
watersync-plan-configs \
  --catalog main \
  --schema watersync \
  --ingestion-group epic \
  --publish-task-value
```

---

## JDBC Connection Options

Two authentication modes are supported:

**Direct JDBC URL** (username + password or secret scope):

```python
JdbcRuntimeSettings(
    jdbc_url="jdbc:postgresql://host:5432/db",
    jdbc_user="user",
    jdbc_password="password",
    # or:
    jdbc_secret_scope="my-scope",
    jdbc_secret_key="db-password",
    ...
)
```

**Databricks Connection** (Unity Catalog connection object, no URL needed):

```python
JdbcRuntimeSettings(
    connection_name="slalom_jdbc_conn",   # default; UC connection must already exist
    # jdbc_url / jdbc_user / jdbc_password left empty
    ...
)
```

---

## Worker Types

### `TimestampWatermarkIngestionWorker`

Default worker, selected when `epic_csa_enabled = false`.

- **Incremental**: reads rows where `watermark_column > last_watermark` and `<= now() - threshold`; stores the max watermark seen
- **Full**: reads the entire table; supports numeric `partition_column` or string `predicate_column` for parallel reads
- Writes to staging as `CREATE OR REPLACE TABLE` (full) or `APPEND` (incremental)

### `EpicCsaIngestionWorker`

Selected when `epic_csa_enabled = true`. Handles EPIC Clarity-specific change-sequence logic. No `watermark_column` is required.

---

## Job Provisioning

Create or update a Lakeflow Job with the fan-out pattern automatically:

```python
from watersync.models import JobProvisioningSettings
from watersync.utils import IngestionJobProvisioner

settings = JobProvisioningSettings(
    ingestion_group="epic",
    catalog="main",
    schema="watersync",
    wheel_uri="dbfs:/Volumes/main/watersync/wheels/watersync-0.1.0-py3-none-any.whl",
    foreach_concurrency=4,
    jdbc_secret_scope="my-scope",
    jdbc_secret_key="clarity-password",
    watermark_threshold_minutes="5",
    cdc_pipeline_id="<pipeline-uuid>",   # optional: triggers CDC after ingestion
)

provisioner = IngestionJobProvisioner()
result = provisioner.create_or_update_job(settings)
```

Or via CLI:

```bash
watersync-create-job \
  --catalog main \
  --schema watersync \
  --ingestion-group epic \
  --wheel-uri "dbfs:/Volumes/main/watersync/wheels/watersync-0.1.0-py3-none-any.whl" \
  --jdbc-secret-scope my-scope \
  --jdbc-secret-key clarity-password \
  --foreach-concurrency 4
```

---

## Lakebase Test Setup

Provision a Lakebase Postgres project pre-seeded with `customers`, `products`, and `orders` tables to develop and test ingestion pipelines without a real source system:

```python
from watersync.utils import LakebaseTestDatabaseSetup

setup = LakebaseTestDatabaseSetup(
    project_id="slalom-jdbc-test",
    project_display_name="Slalom JDBC Test DB",
)
setup.ensure_project()
setup.create_standard_tables()
setup.seed_standard_data(customer_count=200, product_count=100, order_count=500)

jdbc_settings = setup.jdbc_settings()
# Returns the JDBC URL and credentials to pass to JdbcRuntimeSettings
```

Simulate ongoing updates (useful for testing incremental loads):

```python
setup.simulate_updates()
```

Or via CLI:

```bash
watersync-setup-lakebase \
  --project-id slalom-jdbc-test \
  --customer-count 200 \
  --order-count 500 \
  --simulate-updates
```

---

## Project Layout

```
watersync/
├── pyproject.toml
├── README.md
├── notebooks/
│   └── Watersync Notebook Runner      # interactive runner notebook
└── src/watersync/
    ├── cli.py                         # CLI entry points
    ├── config_planner.py              # fanout input planner
    ├── ingestion.py                   # orchestrator + config repository
    ├── cdc_pipeline.py                # CDC SCD2 pipeline logic
    ├── pipeline_bootstrap.py          # SDP pipeline bootstrap
    ├── models.py                      # dataclasses (JdbcRuntimeSettings, etc.)
    ├── common.py                      # shared helpers
    ├── workers/
    │   ├── base.py                    # JdbcIngestionWorker ABC
    │   ├── timestamp_watermark/
    │   │   └── worker.py              # TimestampWatermarkIngestionWorker
    │   └── epic_csa/
    │       └── worker.py              # EpicCsaIngestionWorker
    └── utils/
        ├── uc_setup.py                # UC schema + table creation
        ├── create_ingestion_job.py    # Lakeflow Job provisioner
        └── lakebase_test_database_setup.py
```
