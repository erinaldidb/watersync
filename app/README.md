# Watersync Monitor App

A Databricks App built with React + Express (AppKit pattern) for configuring and monitoring the watersync JDBC ingestion pipeline.

## Features

- **Dashboard** — Overview of ingestion groups, watermark staleness, and active jobs
- **Configuration** — View/edit `jdbc_ingestion_config` table (enable/disable tables)
- **Watermarks** — Live watermark state with staleness highlighting
- **Pipelines** — CDC pipeline status and recent updates
- **Logs** — Ingestion log viewer with level filtering
- **Job Runs** — View recent runs and trigger new ones

## Architecture

```
src/
├── client/          # React SPA (Vite + TailwindCSS)
│   ├── App.tsx      # Main app with routing and pages
│   ├── main.tsx     # React entry point
│   └── index.html   # HTML shell
└── server/          # Express API server
    └── index.ts     # REST endpoints (SQL Statement API + Jobs/Pipelines API)
```

## Deployment with Declarative Automation Bundles

### Prerequisites
- Databricks CLI installed and authenticated
- A SQL Warehouse ID (set in `databricks.yml` variables)

### Deploy

```bash
cd watersync/app

# Set your warehouse ID
export WAREHOUSE_ID="your-warehouse-id"

# Validate and deploy
databricks bundle validate
databricks bundle deploy -t dev

# Deploy the app to compute
databricks apps deploy watersync-monitor --source-code-path $(databricks bundle show -t dev | grep root_path | awk '{print $2}')/files
```

### Local Development

```bash
npm install
npm run dev
```

Set environment variables:
```bash
export DATABRICKS_HOST=https://fevm-serverless-pixels-release.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
export DATABRICKS_WAREHOUSE_ID=your-warehouse-id
export WATERSYNC_CATALOG=serverless_pixels_release_catalog
export WATERSYNC_SCHEMA=jdbc_incremental_gh
```

## Configuration

The app reads configuration from the bundle's `databricks.yml`:

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | UC catalog for watersync tables | `serverless_pixels_release_catalog` |
| `schema` | Schema with config/watermark tables | `jdbc_incremental_gh` |
| `warehouse_id` | SQL Warehouse for queries | (required) |

## Tables Used

- `{catalog}.{schema}.jdbc_ingestion_config` — Table ingestion definitions
- `{catalog}.{schema}.jdbc_ingestion_watermark` — Watermark state tracking
- `{catalog}.{schema}.watersync_logs` — Ingestion execution logs
