import React, { useState, useEffect, useCallback } from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';

// ============================================================
// Types
// ============================================================
interface SQLResult {
  status?: string;
  manifest?: { schema: { columns: Array<{ name: string; type_name: string }> } };
  result?: { data_array: any[][] };
}

interface JobRun {
  run_id: number;
  state: { result_state: string; life_cycle_state: string };
  start_time: number;
  end_time?: number;
  run_name?: string;
}

// ============================================================
// Hooks
// ============================================================
function useFetch<T>(url: string, interval?: number): { data: T | null; loading: boolean; error: string | null; refetch: () => void } {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json();
      setData(json);
      setError(null);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [url]);

  useEffect(() => {
    fetchData();
    if (interval) {
      const timer = setInterval(fetchData, interval);
      return () => clearInterval(timer);
    }
  }, [fetchData, interval]);

  return { data, loading, error, refetch: fetchData };
}

// ============================================================
// Utility Components
// ============================================================
function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    SUCCESS: 'bg-green-100 text-green-800',
    RUNNING: 'bg-blue-100 text-blue-800',
    FAILED: 'bg-red-100 text-red-800',
    TERMINATED: 'bg-yellow-100 text-yellow-800',
    PENDING: 'bg-gray-100 text-gray-600',
    COMPLETED: 'bg-green-100 text-green-800',
  };
  const color = colors[status?.toUpperCase()] || 'bg-gray-100 text-gray-600';
  return <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${color}`}>{status || 'UNKNOWN'}</span>;
}

function Card({ title, children, className = '' }: { title: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`bg-white rounded-lg shadow-sm border border-gray-200 ${className}`}>
      <div className="px-4 py-3 border-b border-gray-100">
        <h3 className="text-sm font-semibold text-gray-700">{title}</h3>
      </div>
      <div className="p-4">{children}</div>
    </div>
  );
}

function MetricCard({ label, value, subtext, status }: { label: string; value: string | number; subtext?: string; status?: 'healthy' | 'warning' | 'error' }) {
  const statusColor = status === 'healthy' ? 'text-green-600' : status === 'warning' ? 'text-yellow-600' : status === 'error' ? 'text-red-600' : 'text-gray-900';
  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${statusColor}`}>{value}</p>
      {subtext && <p className="text-xs text-gray-400 mt-1">{subtext}</p>}
    </div>
  );
}

function DataTable({ result }: { result: SQLResult | null }) {
  if (!result?.manifest?.schema?.columns || !result?.result?.data_array) {
    return <p className="text-gray-400 text-sm">No data</p>;
  }
  const columns = result.manifest.schema.columns;
  const rows = result.result.data_array;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="bg-gray-50">
            {columns.map((col, i) => (
              <th key={i} className="px-3 py-2 text-left font-medium text-gray-600 uppercase tracking-wider">
                {col.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {rows.map((row, ri) => (
            <tr key={ri} className="hover:bg-gray-50">
              {row.map((cell, ci) => (
                <td key={ci} className="px-3 py-2 text-gray-800 whitespace-nowrap">
                  {cell === null ? <span className="text-gray-300">null</span> : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ============================================================
// Pages
// ============================================================
function DashboardPage() {
  const { data: health } = useFetch<any>('/api/health');
  const { data: groups } = useFetch<SQLResult>('/api/config/groups', 30000);
  const { data: wmSummary } = useFetch<SQLResult>('/api/watermarks/summary', 30000);
  const { data: jobs } = useFetch<any>('/api/jobs', 60000);

  const groupRows = groups?.result?.data_array || [];
  const wmRows = wmSummary?.result?.data_array || [];
  const jobList = jobs?.jobs || [];

  const totalTables = groupRows.reduce((acc: number, r: any[]) => acc + parseInt(r[1] || '0'), 0);
  const totalEnabled = groupRows.reduce((acc: number, r: any[]) => acc + parseInt(r[2] || '0'), 0);
  const avgStaleness = wmRows.length > 0 ? Math.round(wmRows.reduce((acc: number, r: any[]) => acc + parseFloat(r[4] || '0'), 0) / wmRows.length) : 0;

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <MetricCard label="Ingestion Groups" value={groupRows.length} subtext={`${totalTables} total tables`} />
        <MetricCard label="Enabled Tables" value={totalEnabled} status={totalEnabled > 0 ? 'healthy' : 'warning'} />
        <MetricCard label="Avg Staleness" value={`${avgStaleness}m`} status={avgStaleness < 60 ? 'healthy' : avgStaleness < 240 ? 'warning' : 'error'} />
        <MetricCard label="Active Jobs" value={jobList.length} subtext={health?.catalog || ''} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card title="Ingestion Groups">
          <DataTable result={groups} />
        </Card>
        <Card title="Watermark Summary">
          <DataTable result={wmSummary} />
        </Card>
      </div>

      <Card title="Watersync Jobs">
        {jobList.length === 0 ? (
          <p className="text-gray-400 text-sm">No watersync jobs found</p>
        ) : (
          <div className="space-y-2">
            {jobList.slice(0, 10).map((job: any) => (
              <div key={job.job_id} className="flex items-center justify-between p-2 rounded border border-gray-100 hover:bg-gray-50">
                <div>
                  <p className="text-sm font-medium text-gray-800">{job.settings?.name || `Job ${job.job_id}`}</p>
                  <p className="text-xs text-gray-400">ID: {job.job_id}</p>
                </div>
                <Link to={`/jobs/${job.job_id}`} className="text-xs text-blue-600 hover:underline">View Runs →</Link>
              </div>
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}

function ConfigPage() {
  const { data: config, refetch } = useFetch<SQLResult>('/api/config', 30000);
  const [toggling, setToggling] = useState<string | null>(null);

  const handleToggle = async (row: any[], columns: any[]) => {
    const getCol = (name: string) => {
      const idx = columns.findIndex((c: any) => c.name === name);
      return idx >= 0 ? row[idx] : null;
    };
    const src = getCol('source_table_name');
    const grp = getCol('ingestion_group');
    const enabled = getCol('enabled');
    const newEnabled = enabled === 'true' || enabled === true ? false : true;
    setToggling(`${grp}:${src}`);
    await fetch('/api/config/toggle', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source_table_name: src, ingestion_group: grp, enabled: newEnabled }),
    });
    setToggling(null);
    refetch();
  };

  const columns = config?.manifest?.schema?.columns || [];
  const rows = config?.result?.data_array || [];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-800">Ingestion Configuration</h2>
        <button onClick={refetch} className="px-3 py-1.5 text-xs bg-blue-600 text-white rounded hover:bg-blue-700">Refresh</button>
      </div>
      <Card title={`${rows.length} table configurations`}>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="bg-gray-50">
                {columns.map((col: any, i: number) => (
                  <th key={i} className="px-3 py-2 text-left font-medium text-gray-600">{col.name}</th>
                ))}
                <th className="px-3 py-2 text-left font-medium text-gray-600">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {rows.map((row: any[], ri: number) => {
                const enabledIdx = columns.findIndex((c: any) => c.name === 'enabled');
                const grpIdx = columns.findIndex((c: any) => c.name === 'ingestion_group');
                const srcIdx = columns.findIndex((c: any) => c.name === 'source_table_name');
                const isEnabled = row[enabledIdx] === 'true' || row[enabledIdx] === true;
                const key = `${row[grpIdx]}:${row[srcIdx]}`;
                return (
                  <tr key={ri} className="hover:bg-gray-50">
                    {row.map((cell: any, ci: number) => (
                      <td key={ci} className="px-3 py-2 text-gray-800">
                        {ci === enabledIdx ? (
                          <span className={`px-2 py-0.5 rounded-full text-xs ${isEnabled ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                            {isEnabled ? 'enabled' : 'disabled'}
                          </span>
                        ) : cell === null ? <span className="text-gray-300">null</span> : String(cell)}
                      </td>
                    ))}
                    <td className="px-3 py-2">
                      <button
                        onClick={() => handleToggle(row, columns)}
                        disabled={toggling === key}
                        className={`px-2 py-1 rounded text-xs ${isEnabled ? 'bg-red-50 text-red-700 hover:bg-red-100' : 'bg-green-50 text-green-700 hover:bg-green-100'}`}
                      >
                        {toggling === key ? '...' : isEnabled ? 'Disable' : 'Enable'}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

function WatermarksPage() {
  const { data: watermarks, refetch } = useFetch<SQLResult>('/api/watermarks', 15000);
  const columns = watermarks?.manifest?.schema?.columns || [];
  const rows = watermarks?.result?.data_array || [];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-800">Watermark State</h2>
        <button onClick={refetch} className="px-3 py-1.5 text-xs bg-blue-600 text-white rounded hover:bg-blue-700">Refresh</button>
      </div>
      <Card title={`${rows.length} tracked tables`}>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="bg-gray-50">
                {columns.map((col: any, i: number) => (
                  <th key={i} className="px-3 py-2 text-left font-medium text-gray-600">{col.name}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {rows.map((row: any[], ri: number) => {
                const stalenessIdx = columns.findIndex((c: any) => c.name === 'minutes_since_last');
                const staleness = stalenessIdx >= 0 ? parseFloat(row[stalenessIdx] || '0') : 0;
                const rowClass = staleness > 240 ? 'bg-red-50' : staleness > 60 ? 'bg-yellow-50' : '';
                return (
                  <tr key={ri} className={`hover:bg-gray-50 ${rowClass}`}>
                    {row.map((cell: any, ci: number) => (
                      <td key={ci} className="px-3 py-2 text-gray-800 whitespace-nowrap">
                        {ci === stalenessIdx ? (
                          <span className={staleness > 240 ? 'text-red-700 font-medium' : staleness > 60 ? 'text-yellow-700' : 'text-green-700'}>
                            {Math.round(staleness)}m
                          </span>
                        ) : cell === null ? <span className="text-gray-300">null</span> : String(cell)}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

function PipelinesPage() {
  const { data: pipelines } = useFetch<any>('/api/pipelines', 30000);
  const pipelineList = pipelines?.statuses || [];

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold text-gray-800">CDC Pipelines</h2>
      {pipelineList.length === 0 ? (
        <Card title="No Pipelines">
          <p className="text-gray-400 text-sm">No watersync pipelines found. Create one via the CLI or notebook runner.</p>
        </Card>
      ) : (
        pipelineList.map((p: any) => (
          <Card key={p.pipeline_id} title={p.name || p.pipeline_id}>
            <div className="flex items-center gap-4 mb-3">
              <StatusBadge status={p.state} />
              <span className="text-xs text-gray-500">ID: {p.pipeline_id}</span>
              {p.latest_updates && (
                <span className="text-xs text-gray-500">
                  Last update: {new Date(p.latest_updates[0]?.creation_time).toLocaleString()}
                </span>
              )}
            </div>
            {p.latest_updates && (
              <div className="space-y-1">
                {p.latest_updates.slice(0, 5).map((u: any) => (
                  <div key={u.update_id} className="flex items-center gap-3 text-xs p-1.5 rounded bg-gray-50">
                    <StatusBadge status={u.state} />
                    <span className="text-gray-600">{new Date(u.creation_time).toLocaleString()}</span>
                    <span className="text-gray-400">{u.update_id?.slice(0, 8)}...</span>
                  </div>
                ))}
              </div>
            )}
          </Card>
        ))
      )}
    </div>
  );
}

function JobRunsPage() {
  const jobId = window.location.pathname.split('/jobs/')[1];
  const { data: runs } = useFetch<any>(`/api/jobs/${jobId}/runs`, 15000);
  const [triggering, setTriggering] = useState(false);
  const runList: JobRun[] = runs?.runs || [];

  const triggerRun = async () => {
    setTriggering(true);
    await fetch(`/api/jobs/${jobId}/run-now`, { method: 'POST' });
    setTriggering(false);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-800">Job {jobId} — Recent Runs</h2>
        <button
          onClick={triggerRun}
          disabled={triggering}
          className="px-3 py-1.5 text-xs bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50"
        >
          {triggering ? 'Triggering...' : 'Run Now'}
        </button>
      </div>
      <Card title={`${runList.length} recent runs`}>
        <div className="space-y-2">
          {runList.map((run) => (
            <div key={run.run_id} className="flex items-center justify-between p-2 rounded border border-gray-100">
              <div className="flex items-center gap-3">
                <StatusBadge status={run.state?.result_state || run.state?.life_cycle_state} />
                <span className="text-xs text-gray-600">{run.run_name || `Run ${run.run_id}`}</span>
              </div>
              <div className="text-xs text-gray-400">
                {new Date(run.start_time).toLocaleString()}
                {run.end_time && ` — ${Math.round((run.end_time - run.start_time) / 1000)}s`}
              </div>
            </div>
          ))}
        </div>
      </Card>
      <Link to="/" className="text-sm text-blue-600 hover:underline">← Back to Dashboard</Link>
    </div>
  );
}

function LogsPage() {
  const [level, setLevel] = useState('');
  const { data: logs, refetch } = useFetch<SQLResult>(`/api/logs?limit=100&level=${level}`, 10000);
  const columns = logs?.manifest?.schema?.columns || [];
  const rows = logs?.result?.data_array || [];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-800">Ingestion Logs</h2>
        <div className="flex gap-2">
          {['', 'INFO', 'WARNING', 'ERROR'].map((l) => (
            <button
              key={l}
              onClick={() => setLevel(l)}
              className={`px-2 py-1 text-xs rounded ${level === l ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
            >
              {l || 'ALL'}
            </button>
          ))}
          <button onClick={refetch} className="px-3 py-1 text-xs bg-blue-600 text-white rounded hover:bg-blue-700">Refresh</button>
        </div>
      </div>
      <Card title={`${rows.length} log entries`}>
        {rows.length === 0 ? (
          <p className="text-gray-400 text-sm">No logs found. Logs appear after ingestion runs complete.</p>
        ) : (
          <DataTable result={logs} />
        )}
      </Card>
    </div>
  );
}

// ============================================================
// App Shell
// ============================================================
function App() {
  const location = useLocation();
  const navItems = [
    { path: '/', label: 'Dashboard', icon: '■' },
    { path: '/config', label: 'Configuration', icon: '⚙' },
    { path: '/watermarks', label: 'Watermarks', icon: '⏱' },
    { path: '/pipelines', label: 'Pipelines', icon: '▶' },
    { path: '/logs', label: 'Logs', icon: '▣' },
  ];

  return (
    <div className="min-h-screen flex">
      {/* Sidebar */}
      <aside className="w-56 bg-[#1B3139] text-white flex flex-col">
        <div className="p-4 border-b border-gray-700">
          <h1 className="text-lg font-bold tracking-tight">⚡ Watersync</h1>
          <p className="text-xs text-gray-400 mt-0.5">Pipeline Monitor</p>
        </div>
        <nav className="flex-1 p-3 space-y-1">
          {navItems.map((item) => (
            <Link
              key={item.path}
              to={item.path}
              className={`flex items-center gap-2 px-3 py-2 rounded text-sm transition-colors ${
                location.pathname === item.path
                  ? 'bg-white/10 text-white font-medium'
                  : 'text-gray-300 hover:bg-white/5 hover:text-white'
              }`}
            >
              <span>{item.icon}</span>
              {item.label}
            </Link>
          ))}
        </nav>
        <div className="p-4 border-t border-gray-700 text-xs text-gray-500">
          <p>Catalog: {'{env}'}</p>
          <p>v1.0.0</p>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-6 overflow-auto">
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/config" element={<ConfigPage />} />
          <Route path="/watermarks" element={<WatermarksPage />} />
          <Route path="/pipelines" element={<PipelinesPage />} />
          <Route path="/jobs/:jobId" element={<JobRunsPage />} />
          <Route path="/logs" element={<LogsPage />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;
