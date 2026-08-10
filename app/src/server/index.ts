import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = parseInt(process.env.DATABRICKS_APP_PORT || '8000');

// Environment
const CATALOG = process.env.WATERSYNC_CATALOG || 'serverless_pixels_release_catalog';
const SCHEMA = process.env.WATERSYNC_SCHEMA || 'jdbc_incremental_gh';
const WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID || '';
const WORKSPACE_HOST = process.env.DATABRICKS_HOST || 'https://fevm-serverless-pixels-release.cloud.databricks.com';

// Auth: Databricks Apps inject DATABRICKS_TOKEN automatically
const getToken = () => process.env.DATABRICKS_TOKEN || '';

app.use(express.json());
app.use(express.static(path.join(__dirname, '../client')));

// --- Databricks SQL Statement Execution API helper ---
async function executeStatement(sql: string): Promise<any> {
  const resp = await fetch(`${WORKSPACE_HOST}/api/2.0/sql/statements`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${getToken()}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      warehouse_id: WAREHOUSE_ID,
      statement: sql,
      wait_timeout: '30s',
      disposition: 'INLINE',
      format: 'JSON_ARRAY',
    }),
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`SQL execution failed (${resp.status}): ${err}`);
  }
  return resp.json();
}

// --- Databricks Jobs API helper ---
async function fetchJobsAPI(endpoint: string, method = 'GET', body?: any): Promise<any> {
  const opts: RequestInit = {
    method,
    headers: {
      'Authorization': `Bearer ${getToken()}`,
      'Content-Type': 'application/json',
    },
  };
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetch(`${WORKSPACE_HOST}/api/2.1/jobs${endpoint}`, opts);
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Jobs API failed (${resp.status}): ${err}`);
  }
  return resp.json();
}

// --- Pipelines API helper ---
async function fetchPipelinesAPI(endpoint: string): Promise<any> {
  const resp = await fetch(`${WORKSPACE_HOST}/api/2.0/pipelines${endpoint}`, {
    headers: { 'Authorization': `Bearer ${getToken()}` },
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Pipelines API failed (${resp.status}): ${err}`);
  }
  return resp.json();
}

// ============================================================
// API Routes
// ============================================================

// --- Health check ---
app.get('/api/health', (_req, res) => {
  res.json({ status: 'ok', catalog: CATALOG, schema: SCHEMA, timestamp: new Date().toISOString() });
});

// --- Ingestion Config (CRUD) ---
app.get('/api/config', async (_req, res) => {
  try {
    const result = await executeStatement(
      `SELECT * FROM ${CATALOG}.${SCHEMA}.jdbc_ingestion_config ORDER BY ingestion_group, source_table_name`
    );
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/config/groups', async (_req, res) => {
  try {
    const result = await executeStatement(
      `SELECT DISTINCT ingestion_group, 
              count(*) as table_count,
              sum(case when enabled then 1 else 0 end) as enabled_count
       FROM ${CATALOG}.${SCHEMA}.jdbc_ingestion_config 
       GROUP BY ingestion_group ORDER BY ingestion_group`
    );
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/config/toggle', async (req, res) => {
  try {
    const { source_table_name, ingestion_group, enabled } = req.body;
    await executeStatement(
      `UPDATE ${CATALOG}.${SCHEMA}.jdbc_ingestion_config 
       SET enabled = ${enabled} 
       WHERE source_table_name = '${source_table_name}' 
         AND ingestion_group = '${ingestion_group}'`
    );
    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/config/update', async (req, res) => {
  try {
    const { source_table_name, ingestion_group, field, value } = req.body;
    await executeStatement(
      `UPDATE ${CATALOG}.${SCHEMA}.jdbc_ingestion_config 
       SET ${field} = '${value}' 
       WHERE source_table_name = '${source_table_name}' 
         AND ingestion_group = '${ingestion_group}'`
    );
    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// --- Watermark State ---
app.get('/api/watermarks', async (_req, res) => {
  try {
    const result = await executeStatement(
      `SELECT w.*, 
              TIMESTAMPDIFF(MINUTE, w.last_ingestion_timestamp, current_timestamp()) as minutes_since_last
       FROM ${CATALOG}.${SCHEMA}.jdbc_ingestion_watermark w
       ORDER BY w.last_ingestion_timestamp DESC`
    );
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/watermarks/summary', async (_req, res) => {
  try {
    const result = await executeStatement(
      `SELECT ingestion_group,
              count(*) as table_count,
              min(last_ingestion_timestamp) as oldest_watermark,
              max(last_ingestion_timestamp) as newest_watermark,
              avg(TIMESTAMPDIFF(MINUTE, last_ingestion_timestamp, current_timestamp())) as avg_staleness_minutes
       FROM ${CATALOG}.${SCHEMA}.jdbc_ingestion_watermark
       GROUP BY ingestion_group
       ORDER BY ingestion_group`
    );
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// --- Jobs / Pipeline Runs ---
app.get('/api/jobs', async (req, res) => {
  try {
    const nameFilter = req.query.name || 'watersync';
    const result = await fetchJobsAPI(`/list?limit=25&name=${nameFilter}`);
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/jobs/:jobId/runs', async (req, res) => {
  try {
    const result = await fetchJobsAPI(`/runs/list?job_id=${req.params.jobId}&limit=10`);
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/jobs/:jobId/run-now', async (req, res) => {
  try {
    const result = await fetchJobsAPI('/run-now', 'POST', { job_id: parseInt(req.params.jobId) });
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pipelines', async (_req, res) => {
  try {
    const result = await fetchPipelinesAPI('?filter=name+LIKE+%27watersync%25%27&max_results=25');
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pipelines/:pipelineId', async (req, res) => {
  try {
    const result = await fetchPipelinesAPI(`/${req.params.pipelineId}`);
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pipelines/:pipelineId/updates', async (req, res) => {
  try {
    const result = await fetchPipelinesAPI(`/${req.params.pipelineId}/updates?max_results=10`);
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// --- Logs (ZeroBus test logs) ---
app.get('/api/logs', async (req, res) => {
  try {
    const limit = req.query.limit || '100';
    const level = req.query.level || '';
    const levelFilter = level ? `AND level = '${level}'` : '';
    const result = await executeStatement(
      `SELECT * FROM ${CATALOG}.${SCHEMA}.watersync_logs 
       WHERE 1=1 ${levelFilter}
       ORDER BY log_timestamp DESC 
       LIMIT ${limit}`
    );
    res.json(result);
  } catch (e: any) {
    // Table might not exist yet
    res.json({ result: { data_array: [] }, manifest: { schema: { columns: [] } } });
  }
});

// --- SPA fallback ---
app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, '../client/index.html'));
});

app.listen(PORT, () => {
  console.log(`[watersync-monitor] Server running on port ${PORT}`);
  console.log(`[watersync-monitor] Catalog: ${CATALOG}, Schema: ${SCHEMA}`);
});
