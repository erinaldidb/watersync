---
name: watersync-row-diff
description: Compares row counts and MD5 content hashes between JDBC source tables (any database — PostgreSQL, Lakebase, MySQL, SQL Server, Oracle) and their Delta target tables in the watersync ingestion pipeline. Use when the user asks to check row differences, verify data parity, validate a full refresh, or run a hash check between source and target.
---

# WaterSync Row-Diff Validation

Run this validation after an ingestion pipeline run to confirm that the source database tables and the Delta final tables are in sync. Works with **any** source database type supported by watersync: PostgreSQL, Lakebase (Databricks Postgres), MySQL, SQL Server, Oracle, or any source reachable via a Unity Catalog connection.

## Prerequisites

- The ingestion config table must be readable at the FQN stored in the job's `configuration_fqn` parameter.
- For JDBC sources: the appropriate Python driver must be installable (`psycopg2-binary` for PostgreSQL/Lakebase, `mysql-connector-python` for MySQL, `pymssql` for SQL Server, `oracledb` for Oracle).
- For UC Connection sources: the connection must be accessible via `spark.read.format("jdbc").option("databricks.connection", connection_name)`.

## What the validation checks

| # | Check | Pass criteria |
|---|-------|---------------|
| 1 | **Row counts** | Source count = Staging count = Final (`__END_AT IS NULL`) count for every table |
| 2 | **Staging cleanliness** | Staging row count equals source count (no accumulated stale data from prior runs) |
| 3 | **Ghost records** | No active (`__END_AT IS NULL`) rows in the final table whose keys were deleted from the source |
| 4 | **MD5 hash** | Per-table aggregate MD5 over all non-timestamp business columns matches between source and final table |

## How to run

### Step 1 — Read the ingestion config

Query the config table to discover all source/staging/target mappings. Both `jdbc_url` and `connection_name` must be read — one or the other will be populated:

```python
config_fqn = "<catalog>.<schema>.jdbc_ingestion_config"  # from job parameter configuration_fqn
ingestion_group = "<group>"  # from job parameter ingestion_group

configs = spark.sql(f"""
    SELECT source_table_name, staging_table_fqn, target_table_fqn,
           key_columns, jdbc_url, jdbc_user, uc_secret_name,
           connection_name
    FROM {config_fqn}
    WHERE ingestion_group = '{ingestion_group}' AND enabled = true
""").collect()
```

### Step 1b — Resolve JDBC credentials

The watersync config table supports three credential sources. Check which fields are populated and resolve in priority order:

| Config fields | Method | Resolution |
|--------------|--------|------------|
| `uc_secret_name` is set | **Unity Catalog secret** | `dbutils.secrets.get(catalog=..., schema=..., key=...)` |
| `jdbc_secret_scope` + `jdbc_secret_key` are set | **Databricks secret scope** | `dbutils.secrets.get(scope=..., key=...)` |
| `connection_name` is set (no direct JDBC) | **UC Connection** | No password needed — Spark handles auth via the connection |

**Unity Catalog secrets** (most common for Lakebase): The `uc_secret_name` field uses the format `catalog.schema.secret_name`. Split on `.` and pass as **keyword arguments**:

```python
# uc_secret_name = "serverless_pixels_catalog.watersync.db_pass"
parts = uc_secret_name.split(".", 2)
password = dbutils.secrets.get(catalog=parts[0], schema=parts[1], key=parts[2])
```

> **Common mistakes:**
> - `secret('catalog.schema', 'key')` SQL function — this is for Databricks secret scopes, not UC secrets.
> - `dbutils.secrets.get(scope='catalog.schema', key='key')` — wrong; UC secrets require the `catalog=` keyword arg.
> - `w.postgres.generate_database_credential(...)` — the SDK `postgres` service may not be available on all workspace versions.

**Databricks secret scopes** (legacy): When `jdbc_secret_scope` and `jdbc_secret_key` are both set:

```python
password = dbutils.secrets.get(scope=jdbc_secret_scope, key=jdbc_secret_key)
```

The resolved password is then passed to `spark.read.format("jdbc").option("password", password)` or to a native Python driver connection.

### Step 2 — Determine the connection method

Each config row uses **one** of two connection methods. Check which fields are populated:

| Field | Method | How to connect |
|-------|--------|----------------|
| `connection_name` is set | **UC Connection** | Use `spark.read.format("jdbc").option("databricks.connection", connection_name)` |
| `jdbc_url` is set (no `connection_name`) | **Direct JDBC** | Parse the URL to determine the database type and connect with a native Python driver |

#### 2a — UC Connection (preferred when available)

When `connection_name` is populated, use Spark's JDBC reader with the UC connection to count source rows:

```python
def source_count_via_uc_connection(connection_name, source_table_name):
    count_query = f"(SELECT COUNT(*) AS cnt FROM {source_table_name}) t"
    row = (spark.read.format("jdbc")
           .option("databricks.connection", connection_name)
           .option("dbtable", count_query)
           .load().first())
    return row["cnt"]
```

For the hash check with UC Connections, push the MD5 computation to the source via a subquery:

```python
def source_hash_via_uc_connection(connection_name, source_table_name, cols):
    cols_expr = " || '|' || ".join([f"COALESCE(CAST({c} AS VARCHAR), '')" for c in cols])
    hash_query = f"(SELECT MD5({cols_expr}) AS row_hash FROM {source_table_name}) t"
    rows = (spark.read.format("jdbc")
            .option("databricks.connection", connection_name)
            .option("dbtable", hash_query)
            .load().collect())
    import hashlib
    row_hashes = sorted([r["row_hash"] for r in rows])
    return hashlib.md5("".join(row_hashes).encode()).hexdigest()
```

#### 2b — Direct JDBC with native Python driver

When `jdbc_url` is set, parse it to detect the database type and install the corresponding driver:

| JDBC URL prefix | Database | Python package | Install command |
|----------------|----------|---------------|----------------|
| `jdbc:postgresql://` | PostgreSQL / Lakebase | `psycopg2-binary` | `%pip install psycopg2-binary -q` |
| `jdbc:mysql://` | MySQL | `mysql-connector-python` | `%pip install mysql-connector-python -q` |
| `jdbc:sqlserver://` | SQL Server | `pymssql` | `%pip install pymssql -q` |
| `jdbc:oracle:thin:@` | Oracle | `oracledb` | `%pip install oracledb -q` |

**Lakebase detection:** If the JDBC URL host matches `*.database.*.cloud.databricks.com`, this is a Lakebase (Databricks Postgres) endpoint. The endpoint hostname pattern is `ep-<name>-<id>.database.<region>.cloud.databricks.com`. Authenticate using the credential resolved in **Step 1b** (typically a UC secret). Use `spark.read.format("jdbc")` with the JDBC URL, `jdbc_user`, and the resolved password — no native driver installation needed.

**All databases:** Resolve the password using **Step 1b** before connecting. The `uc_secret_name` or `jdbc_secret_scope`/`jdbc_secret_key` fields in the config determine which method to use.

**Connection examples by database type:**

```python
# PostgreSQL / Lakebase
import psycopg2
conn = psycopg2.connect(host=host, port=5432, dbname=db, user=user, password=password, sslmode="require")

# MySQL
import mysql.connector
conn = mysql.connector.connect(host=host, port=3306, database=db, user=user, password=password)

# SQL Server
import pymssql
conn = pymssql.connect(server=host, port=1433, database=db, user=user, password=password)

# Oracle
import oracledb
conn = oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service_name}")
```

### Step 3 — Row count comparison

For each config row, count rows in three places:

```python
# Source (via native driver or UC Connection — see Step 2)
source_count = <count from source>

# Delta staging
staging_count = spark.table(staging_table_fqn).count()

# Delta final (active records only)
active_count = spark.sql(
    f"SELECT count(*) FROM {target_table_fqn} WHERE __END_AT IS NULL"
).collect()[0][0]
```

All three counts must be equal for each table.

### Step 4 — Staging cleanliness

Verify that staging contains exactly one ingestion batch (no accumulated rows from prior runs):

```python
batch_count = spark.sql(f"""
    SELECT count(distinct cast(_ingested_at as date))
    FROM {staging_table_fqn}
""").collect()[0][0]
# batch_count should be 1 after a clean full refresh
```

### Step 5 — MD5 hash comparison

Compute a deterministic aggregate hash over business columns. The SQL syntax for MD5 and string aggregation varies by database:

| Database | Row hash expression | Aggregate hash |
|----------|--------------------|-----------------|
| PostgreSQL / Lakebase | `md5(concat_ws('\|', col1, col2, ...))` | `md5(string_agg(row_hash, '' ORDER BY row_hash))` |
| MySQL | `md5(concat_ws('\|', col1, col2, ...))` | `md5(group_concat(row_hash ORDER BY row_hash SEPARATOR ''))` |
| SQL Server | `CONVERT(VARCHAR(32), HASHBYTES('MD5', concat_ws('\|', col1, col2, ...)), 2)` | Collect row hashes and aggregate in Python |
| Oracle | `LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(col1\|\|'\|'\|\|col2...), 2)))` | Collect row hashes and aggregate in Python |

**Delta side (always the same):**

```python
import hashlib
from pyspark.sql.functions import col, concat_ws, md5

df = spark.table(target_table_fqn).filter("__END_AT IS NULL")
df = df.select(*[col(c).cast("string") for c in business_cols])
df = df.withColumn("row_hash", md5(concat_ws("|", *[col(c) for c in business_cols])))
row_hashes = sorted([r["row_hash"] for r in df.select("row_hash").collect()])
delta_hash = hashlib.md5("".join(row_hashes).encode()).hexdigest()
```

The two hashes must match for each table.

### Step 6 — Report results

Present a summary table:

```
Table         Source  Staging  Active  Staging OK  Ghosts  Hash Match
customers        198      198     198  ✅           0 ✅    ✅
products         100      100     100  ✅           0 ✅    ✅
orders          1197     1197    1197  ✅           0 ✅    ✅
```

End with a clear **✅ ALL CHECKS PASSED** or **❌ FAILED** verdict.

## Column selection for hashing

Exclude these columns from the hash:
- SCD2 metadata: `__START_AT`, `__END_AT`
- Ingestion metadata: `_ingested_at`, `_source_table`, `_ingestion_group`, `_ingestion_type`, `_IS_DELETED`, `_csa_update_dt`
- Timestamp columns that differ between source and target due to timezone or precision differences (e.g., `updated_at`, `last_changed`, `modified_at`)

Include all business columns (IDs, names, amounts, statuses, etc.).

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Active count > source count | Staging not truncated on full refresh — ghost records from prior runs replayed by CDC |
| Staging count > source count | Staging in append mode — accumulated rows from multiple ingestion runs |
| Hash mismatch but counts match | Column value drift (e.g., updated timestamps, precision differences) — check which columns differ |
| Source count > active count | Ingestion missed rows — check watermark window, JDBC partitioning bounds, or fetch size |
| UC Connection error | Verify the connection name exists and the current user has USE permission on it |
