import { createBrowserRouter, NavLink, Outlet, RouterProvider, useOutletContext } from 'react-router';
import { useEffect, useMemo, useState } from 'react';
import type { FormEvent } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Checkbox,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Skeleton,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import {
  Activity,
  ArrowLeft,
  ArrowRight,
  CheckCircle2,
  Clock3,
  Database,
  Droplets,
  ExternalLink,
  Layers3,
  Play,
  Plus,
  RefreshCw,
  Settings2,
  TableProperties,
  Trash2,
  TriangleAlert,
  Workflow,
} from 'lucide-react';

type Location = { catalog: string; schema: string };
type Context = { location: Location; setLocation: (location: Location) => void; revision: number; refresh: () => void };
type ConfigRow = {
  ingestion_group: string;
  source_table_name: string;
  staging_table_fqn: string | null;
  target_table_fqn: string;
  ingestion_type: string;
  key_columns: string | null;
  watermark_column: string | null;
  partition_column: string | null;
  predicate_column: string | null;
  epic_csa_enabled: boolean;
  jdbc_url: string | null;
  jdbc_user: string | null;
  jdbc_secret_scope: string | null;
  jdbc_secret_key: string | null;
  connection_name: string | null;
  watermark_threshold_minutes: number;
  fetch_size: number;
  num_partitions: number;
  update_dttm: string | null;
  enabled: boolean;
};
type WatermarkRow = {
  ingestion_group: string;
  source_table_name: string;
  staging_table_fqn: string | null;
  ingestion_type: string;
  last_watermark: string | null;
  last_run_timestamp: string | null;
  status: string | null;
  last_error: string | null;
};
type JobRun = {
  run_id?: number;
  run_url?: string;
  run_name?: string;
  start_time?: number;
  end_time?: number;
  setup_duration?: number;
  execution_duration?: number;
  cleanup_duration?: number;
  state?: { life_cycle_state?: string; result_state?: string; state_message?: string };
};
type JobSchedule = {
  quartz_cron_expression: string;
  timezone_id: string;
  pause_status?: 'PAUSED' | 'UNPAUSED';
};
type JobRow = {
  job_id?: number;
  settings?: {
    name?: string;
    schedule?: JobSchedule;
  };
  creator_user_name?: string;
  created_time?: number;
  workspace_url?: string;
  runs: JobRun[];
};
type SourceTable = { table_schema: string; table_name: string };
type SourceColumn = {
  column_name: string;
  data_type: string;
  is_nullable: string;
  ordinal_position: string;
  is_primary_key: string;
};
type TableDraft = {
  table: SourceTable;
  columns: SourceColumn[];
  keyColumn: string;
  watermarkColumn: string;
  partitionColumn: string;
  predicateColumn: string;
  targetFqn: string;
  stagingFqn: string;
};
type InferenceProgress = { completed: number; total: number };

const api = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const response = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...init });
  const body: unknown = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail =
      typeof body === 'object' && body !== null && 'error' in body && typeof body.error === 'string'
        ? body.error
        : undefined;
    throw new Error(detail ?? `Request failed (${response.status})`);
  }
  return body as T;
};
const errorMessage = (value: unknown) => (value instanceof Error ? value.message : 'Unexpected error');
const formString = (form: FormData, name: string, fallback: string) => {
  const value = form.get(name);
  return typeof value === 'string' && value ? value : fallback;
};
const defaultLocation: Location = {
  catalog: 'serverless_pixels_release_catalog',
  schema: 'jdbc_incremental_gh',
};
const savedLocation = (): Location => {
  const raw = localStorage.getItem('watersync-location');
  if (!raw) return defaultLocation;
  try {
    const value: unknown = JSON.parse(raw);
    if (
      typeof value === 'object' &&
      value !== null &&
      'catalog' in value &&
      'schema' in value &&
      typeof value.catalog === 'string' &&
      typeof value.schema === 'string'
    ) {
      if (value.catalog === 'main' && value.schema === 'watersync') return defaultLocation;
      return { catalog: value.catalog, schema: value.schema };
    }
  } catch {
    /* Fall through to defaults. */
  }
  return defaultLocation;
};
const fqn = (location: Location, table: string) => `${location.catalog}.${location.schema}.${table}`;
const displayTime = (value: string | number | null | undefined) =>
  value ? new Date(typeof value === 'number' ? value : value).toLocaleString() : 'Never';
const runStatus = (run?: JobRun) => run?.state?.result_state ?? run?.state?.life_cycle_state ?? 'NEVER_RUN';
const statusVariant = (status: string): 'default' | 'secondary' | 'destructive' | 'outline' => {
  if (status === 'SUCCESS') return 'default';
  if (['FAILED', 'TIMEDOUT', 'CANCELED', 'INTERNAL_ERROR'].includes(status)) return 'destructive';
  if (['RUNNING', 'PENDING', 'QUEUED', 'TERMINATING'].includes(status)) return 'secondary';
  return 'outline';
};
const runDuration = (run: JobRun) => {
  const duration =
    run.end_time && run.start_time
      ? run.end_time - run.start_time
      : (run.setup_duration ?? 0) + (run.execution_duration ?? 0) + (run.cleanup_duration ?? 0);
  if (!duration) return 'Duration unavailable';
  if (duration < 60_000) return `${Math.max(1, Math.round(duration / 1000))}s`;
  return `${Math.floor(duration / 60_000)}m ${Math.round((duration % 60_000) / 1000)}s`;
};

function Layout() {
  const [location, setLocationState] = useState<Location>(savedLocation);
  const [revision, setRevision] = useState(0);
  const [draft, setDraft] = useState(location);
  const setLocation = (next: Location) => {
    setLocationState(next);
    setDraft(next);
    localStorage.setItem('watersync-location', JSON.stringify(next));
    setRevision((v) => v + 1);
  };
  const context: Context = { location, setLocation, revision, refresh: () => setRevision((v) => v + 1) };
  const links = [
    ['/', 'Overview', Activity],
    ['/config', 'Configuration', Settings2],
    ['/watermarks', 'Watermarks', Droplets],
    ['/jobs', 'Jobs', Workflow],
  ] as const;
  return (
    <div className="app-shell min-h-screen bg-background text-foreground">
      <header className="app-header">
        <div className="mx-auto flex max-w-screen-2xl flex-wrap items-center gap-4 px-6 py-4">
          <div className="brand-lockup mr-auto">
            <div className="brand-mark">
              <Droplets className="h-5 w-5" />
            </div>
            <div>
              <div className="brand-name">
                WaterSync <span>Control Plane</span>
              </div>
              <div className="brand-subtitle">JDBC ingestion control plane</div>
            </div>
          </div>
          <Badge variant="outline" className="hidden sm:flex">
            <span className="status-dot" /> Connected
          </Badge>
          <Dialog>
            <DialogTrigger asChild>
              <Button variant="outline" className="context-button">
                <Database className="mr-2 h-4 w-4" />
                <span className="max-w-64 truncate">
                  {location.catalog}.{location.schema}
                </span>
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Metadata location</DialogTitle>
                <DialogDescription>
                  Select the catalog and schema containing the WaterSync configuration and watermark tables.
                </DialogDescription>
              </DialogHeader>
              <div className="grid gap-4 py-2">
                <div className="space-y-1.5">
                  <Label htmlFor="catalog">Catalog</Label>
                  <Input
                    id="catalog"
                    value={draft.catalog}
                    onChange={(e) => setDraft({ ...draft, catalog: e.target.value })}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="schema">Schema</Label>
                  <Input
                    id="schema"
                    value={draft.schema}
                    onChange={(e) => setDraft({ ...draft, schema: e.target.value })}
                  />
                </div>
              </div>
              <DialogFooter>
                <Button onClick={() => setLocation(draft)} disabled={!draft.catalog || !draft.schema}>
                  Use location
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
        <div className="nav-strip">
          <nav className="mx-auto flex max-w-screen-2xl gap-1 overflow-x-auto px-6">
            {links.map(([to, label, Icon]) => (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                className={({ isActive }) => `nav-link ${isActive ? 'nav-link-active' : ''}`}
              >
                <Icon className="h-4 w-4" />
                {label}
              </NavLink>
            ))}
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-screen-2xl p-6 lg:py-8">
        <Outlet context={context} />
      </main>
    </div>
  );
}
const useControl = () => useOutletContext<Context>();

function PageTitle({ title, description, action }: { title: string; description: string; action?: React.ReactNode }) {
  return (
    <div className="page-title mb-6 flex flex-wrap items-start justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{description}</p>
      </div>
      {action}
    </div>
  );
}
function ErrorState({ message }: { message: string }) {
  return (
    <Alert variant="destructive">
      <AlertTitle>Data could not be loaded</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  );
}

function OverviewPage() {
  const { location, revision, refresh } = useControl();
  const params = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      watermark_table: sql.string(fqn(location, 'jdbc_ingestion_watermark')),
      refresh_token: sql.int(revision),
    }),
    [location, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('config_summary', params);
  const row = data?.[0];
  return (
    <>
      <PageTitle
        title="Pipeline health at a glance"
        description={`Actual state from ${location.catalog}.${location.schema}; refreshes on demand.`}
        action={
          <Button variant="outline" onClick={refresh}>
            <RefreshCw className="mr-2 h-4 w-4" />
            Refresh
          </Button>
        }
      />
      {loading ? (
        <div className="grid gap-4 md:grid-cols-4">
          {[1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="h-32" />
          ))}
        </div>
      ) : error ? (
        <ErrorState message={error} />
      ) : !row ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>No metadata found</EmptyTitle>
            <EmptyDescription>Choose a catalog and schema containing WaterSync tables.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <>
          <Card className="overview-hero mb-7 overflow-hidden">
            <CardContent className="flex flex-wrap items-center justify-between gap-5 p-6">
              <div>
                <div className="eyebrow">Active metadata environment</div>
                <h2 className="mt-1 text-xl font-semibold">{location.catalog}</h2>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Badge variant="secondary">Schema · {location.schema}</Badge>
                  <Badge variant="outline">SQL warehouse live</Badge>
                  <Badge variant="outline">{row.enabled_count} sources enabled</Badge>
                </div>
              </div>
              <div className="hero-health">
                <CheckCircle2 className="h-5 w-5" />
                <div className="mode-field space-y-1.5">
                  <div className="font-semibold">Control plane ready</div>
                  <div className="text-xs opacity-80">Configuration loaded successfully</div>
                </div>
              </div>
            </CardContent>
          </Card>
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-lg font-semibold">Pipeline health</h2>
            <span className="text-xs text-muted-foreground">Actual · refreshed on demand</span>
          </div>
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <Metric
              icon={TableProperties}
              tone="info"
              title="Configured sources"
              value={row.config_count}
              note="rows · current location"
            />
            <Metric
              icon={Layers3}
              tone="success"
              title="Enabled sources"
              value={row.enabled_count}
              note="actual · current state"
            />
            <Metric
              icon={TriangleAlert}
              title="Failed sources"
              value={row.failed_count}
              note="actual status · investigate"
              destructive={Number(row.failed_count) > 0}
            />
            <Metric
              icon={Clock3}
              title="Latest pipeline activity"
              value={displayTime(row.last_run_timestamp)}
              note={`source: ${location.catalog}.${location.schema}`}
            />
          </div>
        </>
      )}
      <Card className="mt-8 overflow-hidden">
        <CardHeader>
          <CardTitle>WaterSync workflow</CardTitle>
          <CardDescription>
            Configure ingestion sources, inspect or reset incremental state, then create and run framework jobs.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-0 p-0 md:grid-cols-3">
          <Hint
            number="01"
            icon={Settings2}
            title="Configuration"
            text="Maintain JDBC source mappings and enablement."
          />
          <Hint number="02" icon={Droplets} title="Watermarks" text="Correct state or force an incremental reload." />
          <Hint number="03" icon={Workflow} title="Jobs" text="Create, monitor, and trigger Lakeflow Jobs." />
        </CardContent>
      </Card>
    </>
  );
}
function Metric({
  icon: Icon,
  title,
  value,
  note,
  destructive = false,
  tone,
}: {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  value: string | number;
  note: string;
  destructive?: boolean;
  tone?: 'primary' | 'info' | 'success';
}) {
  return (
    <Card
      className={`metric-card ${
        destructive
          ? 'metric-card-danger'
          : tone === 'primary'
            ? 'metric-card-primary'
            : tone === 'info'
              ? 'metric-card-info'
              : tone === 'success'
                ? 'metric-card-success'
                : ''
      }`}
    >
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardDescription className="font-medium uppercase tracking-wide">{title}</CardDescription>
          <div className="metric-icon">
            <Icon className="h-4 w-4" />
          </div>
        </div>
        <CardTitle className={`metric-value ${destructive ? 'text-destructive' : ''}`}>{value}</CardTitle>
      </CardHeader>
      <CardContent className="text-xs text-muted-foreground">{note}</CardContent>
    </Card>
  );
}
function Hint({
  number,
  icon: Icon,
  title,
  text,
}: {
  number: string;
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  text: string;
}) {
  return (
    <div className="workflow-step">
      <div className="workflow-number">{number}</div>
      <div className="workflow-icon">
        <Icon className="h-5 w-5" />
      </div>
      <div>
        <div className="font-medium">{title}</div>
        <div className="mt-1 text-sm text-muted-foreground">{text}</div>
      </div>
    </div>
  );
}

function ConfigPage() {
  const { location, revision, refresh } = useControl();
  const [search, setSearch] = useState('');
  const [editing, setEditing] = useState<ConfigRow | null>(null);
  const [open, setOpen] = useState(false);
  const [discoveryOpen, setDiscoveryOpen] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const params = useMemo(
    () => ({
      table_name: sql.string(fqn(location, 'jdbc_ingestion_config')),
      search: sql.string(search),
      page_size: sql.int(100),
      page_offset: sql.int(0),
      refresh_token: sql.int(revision),
    }),
    [location, search, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('config_entries', params);
  const remove = async (row: ConfigRow) => {
    if (!confirm(`Delete ${row.ingestion_group} / ${row.source_table_name}?`)) return;
    try {
      await api<unknown>('/api/config', {
        method: 'DELETE',
        body: JSON.stringify({
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
        }),
      });
      refresh();
    } catch (e) {
      setMessage(errorMessage(e));
    }
  };
  return (
    <>
      <PageTitle
        title="Ingestion configuration"
        description="Exact WaterSync source mappings; first 100 filtered rows."
        action={
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={() => setDiscoveryOpen(true)}>
              <Database className="mr-2 h-4 w-4" /> Discover tables
            </Button>
            <Button
              onClick={() => {
                setEditing(null);
                setOpen(true);
              }}
            >
              <Plus className="mr-2 h-4 w-4" /> Add entry
            </Button>
          </div>
        }
      />
      <div className="mb-4 flex gap-2">
        <Input
          aria-label="Filter configurations"
          placeholder="Filter group or source table"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <Button variant="outline" onClick={refresh}>
          <RefreshCw className="h-4 w-4" />
        </Button>
      </div>
      {message && <ErrorState message={message} />}{' '}
      {loading ? (
        <Skeleton className="h-80" />
      ) : error ? (
        <ErrorState message={error} />
      ) : !data?.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>No configuration entries</EmptyTitle>
            <EmptyDescription>Add a JDBC source or change the selected metadata location.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Group</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Target</TableHead>
                <TableHead>Mode</TableHead>
                <TableHead>Watermark</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((row) => (
                <TableRow key={`${row.ingestion_group}/${row.source_table_name}`}>
                  <TableCell>{row.ingestion_group}</TableCell>
                  <TableCell className="font-mono text-xs">{row.source_table_name}</TableCell>
                  <TableCell className="font-mono text-xs">{row.target_table_fqn}</TableCell>
                  <TableCell>{row.ingestion_type}</TableCell>
                  <TableCell>{row.watermark_column ?? '—'}</TableCell>
                  <TableCell>
                    <Badge variant={row.enabled ? 'default' : 'secondary'}>
                      {row.enabled ? 'Enabled' : 'Disabled'}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        setEditing(row);
                        setOpen(true);
                      }}
                    >
                      Edit
                    </Button>
                    <Button
                      size="icon"
                      variant="ghost"
                      aria-label="Delete configuration"
                      onClick={() => void remove(row)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
      <ConfigDialog
        open={open}
        onOpenChange={setOpen}
        row={editing}
        location={location}
        onSaved={() => {
          setOpen(false);
          refresh();
        }}
      />
      <DiscoveryDialog
        open={discoveryOpen}
        onOpenChange={setDiscoveryOpen}
        location={location}
        onSaved={() => {
          setDiscoveryOpen(false);
          refresh();
        }}
      />
    </>
  );
}

const numericTypes = [
  'smallint',
  'integer',
  'int',
  'bigint',
  'decimal',
  'numeric',
  'number',
  'real',
  'float',
  'double',
];
const temporalTypes = [
  'date',
  'datetime',
  'datetime2',
  'timestamp',
  'timestamp without time zone',
  'timestamp with time zone',
];
const stringTypes = ['char', 'varchar', 'nvarchar', 'text', 'string'];
const isType = (column: SourceColumn, types: string[]) =>
  types.some((type) => column.data_type.toLowerCase().includes(type));
const safeTargetName = (value: string) => value.replace(/[^A-Za-z0-9_]/g, '_').toLowerCase();

function DiscoveryDialog({
  open,
  onOpenChange,
  location,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (value: boolean) => void;
  location: Location;
  onSaved: () => void;
}) {
  const [step, setStep] = useState(1);
  const [busy, setBusy] = useState(false);
  const [inferenceProgress, setInferenceProgress] = useState<InferenceProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [group, setGroup] = useState('');
  const [databaseType, setDatabaseType] = useState('sqlserver');
  const [connectionMode, setConnectionMode] = useState('uc');
  const [connectionName, setConnectionName] = useState('');
  const [database, setDatabase] = useState('');
  const [jdbcUrl, setJdbcUrl] = useState('');
  const [jdbcUser, setJdbcUser] = useState('');
  const [secretScope, setSecretScope] = useState('');
  const [secretKey, setSecretKey] = useState('');
  const [tables, setTables] = useState<SourceTable[]>([]);
  const [tableFilter, setTableFilter] = useState('');
  const [selectedTables, setSelectedTables] = useState<SourceTable[]>([]);
  const [ingestionType, setIngestionType] = useState('incremental');
  const [epicCsa, setEpicCsa] = useState(false);
  const [drafts, setDrafts] = useState<TableDraft[]>([]);
  const [baseTargetFqn, setBaseTargetFqn] = useState(`${location.catalog}.${location.schema}`);
  const [baseStagingFqn, setBaseStagingFqn] = useState(`${location.catalog}.${location.schema}`);
  const [threshold, setThreshold] = useState('5');
  const [fetchSize, setFetchSize] = useState('10000');
  const [partitions, setPartitions] = useState('8');

  const reset = () => {
    setStep(1);
    setError(null);
    setTables([]);
    setSelectedTables([]);
    setDrafts([]);
    setTableFilter('');
    setBaseTargetFqn(`${location.catalog}.${location.schema}`);
    setBaseStagingFqn(`${location.catalog}.${location.schema}`);
    setInferenceProgress(null);
  };
  const changeOpen = (value: boolean) => {
    if (!value) reset();
    onOpenChange(value);
  };
  const loadTables = async () => {
    setBusy(true);
    setInferenceProgress(null);
    setError(null);
    setSelectedTables([]);
    try {
      const result = await api<{ tables: SourceTable[] }>('/api/source-tables', {
        method: 'POST',
        body: JSON.stringify({
          connectionName: connectionMode === 'uc' ? connectionName : '',
          jdbcUrl: connectionMode === 'jdbc' ? jdbcUrl : '',
          jdbcUser: connectionMode === 'jdbc' ? jdbcUser : '',
          jdbcSecretScope: connectionMode === 'jdbc' ? secretScope : '',
          jdbcSecretKey: connectionMode === 'jdbc' ? secretKey : '',
          database,
          databaseType,
        }),
      });
      setTables(result.tables);
    } catch (value) {
      setError(errorMessage(value));
    } finally {
      setBusy(false);
    }
  };
  const inferColumns = (next: SourceColumn[]) => {
    const primary = next.filter((column) => column.is_primary_key === '1').map((column) => column.column_name);
    const idFallback = next.find((column) => /(^id$|_id$)/i.test(column.column_name) && column.is_nullable === 'NO');
    const key = primary[0] ?? idFallback?.column_name ?? next[0]?.column_name ?? 'none';
    const watermark =
      next.find(
        (column) => isType(column, temporalTypes) && /(update|modified|change|timestamp|date)/i.test(column.column_name)
      ) ?? next.find((column) => isType(column, temporalTypes));
    const partition =
      next.find((column) => column.column_name === key && isType(column, numericTypes)) ??
      next.find((column) => isType(column, numericTypes) && column.is_nullable === 'NO');
    const predicate = partition
      ? undefined
      : (next.find((column) => column.column_name === key && isType(column, stringTypes)) ??
        next.find((column) => isType(column, stringTypes) && column.is_nullable === 'NO'));
    return {
      keyColumn: primary.length ? primary.join(',') : key,
      watermarkColumn: watermark?.column_name ?? 'none',
      partitionColumn: partition?.column_name ?? 'none',
      predicateColumn: predicate?.column_name ?? 'none',
    };
  };
  const continueToSettings = async () => {
    if (!selectedTables.length) return;
    setBusy(true);
    setError(null);
    setInferenceProgress({ completed: 0, total: selectedTables.length });
    try {
      const discovered: Array<{ sourceSchema: string; table: string; columns: SourceColumn[] }> = [];
      for (let offset = 0; offset < selectedTables.length; offset += 10) {
        const page = selectedTables.slice(offset, offset + 10);
        const result = await api<{
          tables: Array<{ sourceSchema: string; table: string; columns: SourceColumn[] }>;
        }>('/api/source-columns-batch', {
          method: 'POST',
          body: JSON.stringify({
            connectionName: connectionMode === 'uc' ? connectionName : '',
            jdbcUrl: connectionMode === 'jdbc' ? jdbcUrl : '',
            jdbcUser: connectionMode === 'jdbc' ? jdbcUser : '',
            jdbcSecretScope: connectionMode === 'jdbc' ? secretScope : '',
            jdbcSecretKey: connectionMode === 'jdbc' ? secretKey : '',
            database,
            databaseType,
            tables: page.map((table) => ({ sourceSchema: table.table_schema, table: table.table_name })),
          }),
        });
        discovered.push(...result.tables);
        setInferenceProgress({
          completed: Math.min(offset + page.length, selectedTables.length),
          total: selectedTables.length,
        });
      }
      setDrafts(
        discovered.map((resultTable) => {
          const table = { table_schema: resultTable.sourceSchema, table_name: resultTable.table };
          const target = safeTargetName(resultTable.table);
          return {
            table,
            columns: resultTable.columns,
            ...inferColumns(resultTable.columns),
            targetFqn: `${baseTargetFqn.replace(/\.$/, '')}.${target}`,
            stagingFqn: ingestionType === 'incremental' ? `${baseStagingFqn.replace(/\.$/, '')}.staging_${target}` : '',
          };
        })
      );
      setStep(2);
      setInferenceProgress(null);
    } catch (value) {
      setError(errorMessage(value));
    } finally {
      setBusy(false);
    }
  };
  const save = async () => {
    if (!drafts.length) return;
    setBusy(true);
    setError(null);
    try {
      for (const draft of drafts) {
        await api('/api/config', {
          method: 'POST',
          body: JSON.stringify({
            ...location,
            ingestionGroup: group,
            sourceTableName: `${draft.table.table_schema}.${draft.table.table_name}`,
            stagingTableFqn: draft.stagingFqn || null,
            targetTableFqn: draft.targetFqn,
            ingestionType,
            keyColumns: draft.keyColumn === 'none' ? null : draft.keyColumn,
            watermarkColumn: draft.watermarkColumn === 'none' ? null : draft.watermarkColumn,
            partitionColumn: draft.partitionColumn === 'none' ? null : draft.partitionColumn,
            predicateColumn: draft.predicateColumn === 'none' ? null : draft.predicateColumn,
            epicCsaEnabled: epicCsa,
            jdbcUrl: connectionMode === 'jdbc' ? jdbcUrl : null,
            jdbcUser: connectionMode === 'jdbc' ? jdbcUser : null,
            jdbcSecretScope: connectionMode === 'jdbc' ? secretScope : null,
            jdbcSecretKey: connectionMode === 'jdbc' ? secretKey : null,
            connectionName: connectionMode === 'uc' ? connectionName : null,
            watermarkThresholdMinutes: Number(threshold),
            fetchSize: Number(fetchSize),
            numPartitions: Number(partitions),
            enabled: true,
          }),
        });
      }
      onSaved();
      reset();
    } catch (value) {
      setError(errorMessage(value));
    } finally {
      setBusy(false);
    }
  };
  const filteredTables = tables.filter((table) =>
    `${table.table_schema}.${table.table_name}`.toLowerCase().includes(tableFilter.toLowerCase())
  );
  const canContinue = Boolean(group && selectedTables.length);
  const canLoadTables =
    connectionMode === 'uc'
      ? Boolean(connectionName)
      : Boolean(database && jdbcUrl && jdbcUser && secretScope && secretKey);
  const updateDraft = (index: number, changes: Partial<TableDraft>) =>
    setDrafts((current) =>
      current.map((draft, draftIndex) => (draftIndex === index ? { ...draft, ...changes } : draft))
    );
  const updateBaseTarget = (value: string) => {
    setBaseTargetFqn(value);
    const base = value.replace(/\.$/, '');
    setDrafts((current) =>
      current.map((draft) => ({ ...draft, targetFqn: `${base}.${safeTargetName(draft.table.table_name)}` }))
    );
  };
  const updateBaseStaging = (value: string) => {
    setBaseStagingFqn(value);
    const base = value.replace(/\.$/, '');
    setDrafts((current) =>
      current.map((draft) => ({
        ...draft,
        stagingFqn: ingestionType === 'incremental' ? `${base}.staging_${safeTargetName(draft.table.table_name)}` : '',
      }))
    );
  };
  const draftsValid =
    Boolean(baseTargetFqn && (ingestionType === 'full' || baseStagingFqn)) &&
    drafts.every(
      (draft) =>
        draft.targetFqn &&
        (ingestionType === 'full' || (draft.keyColumn !== 'none' && (epicCsa || draft.watermarkColumn !== 'none')))
    );

  return (
    <Dialog open={open} onOpenChange={changeOpen}>
      <DialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] overflow-y-auto sm:max-w-7xl 2xl:max-w-[90rem]">
        <DialogHeader>
          <DialogTitle>Discover and configure source tables</DialogTitle>
          <DialogDescription>
            Step {step} of 2 ·{' '}
            {step === 1
              ? 'Connect, discover, and choose the replication mode.'
              : 'Review inferred columns and runtime settings.'}
          </DialogDescription>
        </DialogHeader>
        <div className="wizard-progress" aria-label={`Step ${step} of 2`}>
          <span style={{ width: `${step * 50}%` }} />
        </div>
        {error && <ErrorState message={error} />}
        {inferenceProgress && (
          <Alert>
            <RefreshCw className={`h-4 w-4 ${busy ? 'animate-spin' : ''}`} />
            <AlertTitle>Inspecting table metadata</AlertTitle>
            <AlertDescription>
              {inferenceProgress.completed} of {inferenceProgress.total} tables inspected · pages of 10, four concurrent
              queries per page.
            </AlertDescription>
          </Alert>
        )}
        {step === 1 ? (
          <div className="space-y-5">
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Connection and ingestion group</CardTitle>
                <CardDescription>
                  Use an existing UC connection or discover directly with a JDBC URL and Databricks secret.
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                <ControlledField
                  id="discover-group"
                  label="Ingestion group"
                  value={group}
                  onChange={setGroup}
                  required
                />
                <div className="space-y-1.5">
                  <Label htmlFor="connection-mode">Connection method</Label>
                  <Select value={connectionMode} onValueChange={setConnectionMode}>
                    <SelectTrigger id="connection-mode">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="uc">Existing UC connection</SelectItem>
                      <SelectItem value="jdbc">Direct JDBC URL</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="database-type">Database type</Label>
                  <Select value={databaseType} onValueChange={setDatabaseType}>
                    <SelectTrigger id="database-type">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="sqlserver">SQL Server</SelectItem>
                      <SelectItem value="postgresql">PostgreSQL</SelectItem>
                      <SelectItem value="mysql">MySQL</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {connectionMode === 'uc' ? (
                  <>
                    <div className="md:col-span-2 xl:col-span-2">
                      <ControlledField
                        id="connection-name"
                        label="UC connection name"
                        value={connectionName}
                        onChange={setConnectionName}
                        required
                      />
                    </div>
                    <p className="self-end pb-2 text-xs text-muted-foreground md:col-span-2 xl:col-span-1">
                      The source database is defined by the UC connection and cannot be overridden here.
                    </p>
                  </>
                ) : (
                  <>
                    <ControlledField
                      id="source-database"
                      label="Database"
                      value={database}
                      onChange={setDatabase}
                      required
                    />
                    <ControlledField
                      id="discover-jdbc-user"
                      label="JDBC user"
                      value={jdbcUser}
                      onChange={setJdbcUser}
                      required
                    />
                    <ControlledField
                      id="discover-secret-scope"
                      label="Password secret scope"
                      value={secretScope}
                      onChange={setSecretScope}
                      required
                    />
                    <ControlledField
                      id="discover-secret-key"
                      label="Password secret key"
                      value={secretKey}
                      onChange={setSecretKey}
                      required
                    />
                    <div className="md:col-span-2 xl:col-span-2">
                      <ControlledField
                        id="discover-jdbc-url"
                        label="JDBC URL"
                        value={jdbcUrl}
                        onChange={setJdbcUrl}
                        required
                      />
                    </div>
                    <Alert className="md:col-span-2 xl:col-span-3">
                      <AlertTitle>Secret-backed authentication</AlertTitle>
                      <AlertDescription>
                        Do not include a password in the JDBC URL. Discovery creates and removes a temporary UC
                        connection using this secret reference.
                      </AlertDescription>
                    </Alert>
                  </>
                )}
                <div className="flex justify-end border-t pt-4 md:col-span-2 xl:col-span-3">
                  <Button type="button" onClick={() => void loadTables()} disabled={busy || !canLoadTables}>
                    {busy ? 'Loading tables…' : 'Load available tables'}
                  </Button>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Available tables</CardTitle>
                <CardDescription>Select every table you want to replicate.</CardDescription>
              </CardHeader>
              <CardContent>
                {busy && !tables.length ? (
                  <Skeleton className="h-56" />
                ) : !tables.length ? (
                  <Empty>
                    <EmptyHeader>
                      <EmptyTitle>No tables loaded</EmptyTitle>
                      <EmptyDescription>
                        Configure a source connection, then load its available tables.
                      </EmptyDescription>
                    </EmptyHeader>
                  </Empty>
                ) : (
                  <>
                    <div className="mb-4 flex flex-wrap items-center gap-3">
                      <Input
                        aria-label="Filter available tables"
                        placeholder="Filter schema or table"
                        value={tableFilter}
                        onChange={(event) => setTableFilter(event.target.value)}
                        className="min-w-64 flex-1"
                      />
                      <Badge variant="secondary">{selectedTables.length} selected</Badge>
                      <div className="flex gap-1">
                        <Button
                          type="button"
                          size="sm"
                          variant="ghost"
                          onClick={() =>
                            setSelectedTables((current) => {
                              const keyed = new Map(
                                current.map((table) => [`${table.table_schema}.${table.table_name}`, table])
                              );
                              for (const table of filteredTables) {
                                keyed.set(`${table.table_schema}.${table.table_name}`, table);
                              }
                              return [...keyed.values()];
                            })
                          }
                        >
                          Select filtered
                        </Button>
                        <Button type="button" size="sm" variant="ghost" onClick={() => setSelectedTables([])}>
                          Clear
                        </Button>
                      </div>
                    </div>
                    <div className="source-table-list">
                      {filteredTables.map((table) => {
                        const selected = selectedTables.some(
                          (selectedTable) =>
                            selectedTable.table_schema === table.table_schema &&
                            selectedTable.table_name === table.table_name
                        );
                        return (
                          <button
                            type="button"
                            key={`${table.table_schema}.${table.table_name}`}
                            className={`source-table-option ${selected ? 'source-table-option-selected' : ''}`}
                            onClick={() =>
                              setSelectedTables((current) =>
                                selected
                                  ? current.filter(
                                      (selectedTable) =>
                                        selectedTable.table_schema !== table.table_schema ||
                                        selectedTable.table_name !== table.table_name
                                    )
                                  : [...current, table]
                              )
                            }
                          >
                            <Checkbox
                              checked={selected}
                              aria-label={`Select ${table.table_schema}.${table.table_name}`}
                            />
                            <span className="min-w-0 truncate">
                              {table.table_schema}.<strong>{table.table_name}</strong>
                            </span>
                            {selected && <CheckCircle2 className="ml-auto h-4 w-4" />}
                          </button>
                        );
                      })}
                    </div>
                  </>
                )}
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Replication mode</CardTitle>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-2">
                <div className="mode-field space-y-1.5">
                  <Label htmlFor="discover-type">Load type</Label>
                  <Select
                    value={ingestionType}
                    onValueChange={(value) => {
                      setIngestionType(value);
                      if (value === 'full') setEpicCsa(false);
                    }}
                  >
                    <SelectTrigger id="discover-type">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="full">Initial / full load</SelectItem>
                      <SelectItem value="incremental">Incremental</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <label className={`mode-toggle ${ingestionType === 'full' ? 'opacity-50' : ''}`}>
                  <Checkbox
                    checked={epicCsa}
                    disabled={ingestionType === 'full'}
                    onCheckedChange={(checked) => setEpicCsa(checked === true)}
                  />
                  <span>
                    <strong>Use EPIC CSA</strong>
                    <small>Uses change-sequence tracking instead of a timestamp watermark.</small>
                  </span>
                </label>
              </CardContent>
            </Card>
          </div>
        ) : (
          <div className="space-y-5">
            <Alert>
              <CheckCircle2 className="h-4 w-4" />
              <AlertTitle>Metadata inferred</AlertTitle>
              <AlertDescription>
                Primary-key constraints are preferred. Review every suggestion before saving.
              </AlertDescription>
            </Alert>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Review {drafts.length} table configurations</CardTitle>
                <CardDescription>
                  Each table keeps its own inferred key, watermark, partitioning, and target.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid gap-4 rounded-lg border bg-muted/30 p-4 md:grid-cols-2">
                  <ControlledField
                    id="base-target-fqn"
                    label="Base target FQN"
                    value={baseTargetFqn}
                    onChange={updateBaseTarget}
                    required
                  />
                  <ControlledField
                    id="base-staging-fqn"
                    label="Base staging FQN"
                    value={baseStagingFqn}
                    onChange={updateBaseStaging}
                    required={ingestionType === 'incremental'}
                  />
                  <p className="text-xs text-muted-foreground md:col-span-2">
                    WaterSync appends each normalized table name. Staging tables also receive the staging_ prefix.
                  </p>
                </div>
                {drafts.map((draft, index) => (
                  <div className="rounded-lg border p-4" key={`${draft.table.table_schema}.${draft.table.table_name}`}>
                    <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
                      <div className="font-mono text-sm font-semibold">
                        {draft.table.table_schema}.{draft.table.table_name}
                      </div>
                      <Badge variant="outline">{draft.columns.length} columns</Badge>
                    </div>
                    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                      <ColumnSelect
                        id={`key-column-${index}`}
                        label="Key column(s)"
                        value={draft.keyColumn}
                        columns={draft.columns}
                        onChange={(value) => updateDraft(index, { keyColumn: value })}
                        allowCombined
                      />
                      <ColumnSelect
                        id={`watermark-column-${index}`}
                        label="Watermark column"
                        value={draft.watermarkColumn}
                        columns={draft.columns.filter((column) => isType(column, temporalTypes))}
                        onChange={(value) => updateDraft(index, { watermarkColumn: value })}
                        disabled={ingestionType === 'full' || epicCsa}
                      />
                      <ColumnSelect
                        id={`partition-column-${index}`}
                        label="Numeric partition column"
                        value={draft.partitionColumn}
                        columns={draft.columns.filter((column) => isType(column, numericTypes))}
                        onChange={(value) =>
                          updateDraft(index, {
                            partitionColumn: value,
                            ...(value !== 'none' ? { predicateColumn: 'none' } : {}),
                          })
                        }
                      />
                      <ColumnSelect
                        id={`predicate-column-${index}`}
                        label="String predicate column"
                        value={draft.predicateColumn}
                        columns={draft.columns.filter((column) => isType(column, stringTypes))}
                        onChange={(value) =>
                          updateDraft(index, {
                            predicateColumn: value,
                            ...(value !== 'none' ? { partitionColumn: 'none' } : {}),
                          })
                        }
                      />
                      <ControlledField
                        id={`target-fqn-${index}`}
                        label="Final target FQN"
                        value={draft.targetFqn}
                        onChange={(value) => updateDraft(index, { targetFqn: value })}
                        required
                      />
                      <ControlledField
                        id={`staging-fqn-${index}`}
                        label="Staging table FQN"
                        value={draft.stagingFqn}
                        onChange={(value) => updateDraft(index, { stagingFqn: value })}
                      />
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Optional runtime settings</CardTitle>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-3">
                <ControlledField
                  id="discover-threshold"
                  label="Watermark delay (minutes)"
                  value={threshold}
                  onChange={setThreshold}
                  type="number"
                />
                <ControlledField
                  id="discover-fetch-size"
                  label="JDBC fetch size"
                  value={fetchSize}
                  onChange={setFetchSize}
                  type="number"
                />
                <ControlledField
                  id="discover-partitions"
                  label="JDBC partitions"
                  value={partitions}
                  onChange={setPartitions}
                  type="number"
                />
              </CardContent>
            </Card>
          </div>
        )}
        <DialogFooter className="gap-2">
          <Button type="button" variant="outline" onClick={() => (step === 1 ? changeOpen(false) : setStep(1))}>
            <ArrowLeft className="mr-2 h-4 w-4" />
            {step === 1 ? 'Cancel' : 'Back'}
          </Button>
          {step === 1 ? (
            <Button type="button" onClick={() => void continueToSettings()} disabled={busy || !canContinue}>
              {busy && inferenceProgress
                ? `Inspecting ${inferenceProgress.completed}/${inferenceProgress.total}`
                : 'Review inferred settings'}
              <ArrowRight className="ml-2 h-4 w-4" />
            </Button>
          ) : (
            <Button type="button" onClick={() => void save()} disabled={busy || !draftsValid}>
              {busy ? 'Saving…' : `Save ${drafts.length} configuration${drafts.length === 1 ? '' : 's'}`}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ControlledField({
  id,
  label,
  value,
  onChange,
  required = false,
  type = 'text',
}: {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  required?: boolean;
  type?: string;
}) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Input id={id} type={type} value={value} onChange={(event) => onChange(event.target.value)} required={required} />
    </div>
  );
}
function ColumnSelect({
  id,
  label,
  value,
  columns,
  onChange,
  disabled = false,
  allowCombined = false,
}: {
  id: string;
  label: string;
  value: string;
  columns: SourceColumn[];
  onChange: (value: string) => void;
  disabled?: boolean;
  allowCombined?: boolean;
}) {
  const hasCombined = allowCombined && value.includes(',');
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Select value={value} onValueChange={onChange} disabled={disabled}>
        <SelectTrigger id={id}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="none">None</SelectItem>
          {hasCombined && <SelectItem value={value}>{value} (composite primary key)</SelectItem>}
          {columns.map((column) => (
            <SelectItem key={column.column_name} value={column.column_name}>
              {column.column_name} · {column.data_type}
              {column.is_primary_key === '1' ? ' · primary key' : ''}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

function ConfigDialog({
  open,
  onOpenChange,
  row,
  location,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (v: boolean) => void;
  row: ConfigRow | null;
  location: Location;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    const f = new FormData(e.currentTarget);
    try {
      await api('/api/config', {
        method: 'POST',
        body: JSON.stringify({
          ...location,
          originalIngestionGroup: row?.ingestion_group,
          originalSourceTableName: row?.source_table_name,
          ingestionGroup: f.get('group'),
          sourceTableName: f.get('source'),
          stagingTableFqn: f.get('staging_fqn') || null,
          targetTableFqn: f.get('target_fqn'),
          ingestionType: f.get('type'),
          keyColumns: f.get('keys') || null,
          watermarkColumn: f.get('watermark') || null,
          partitionColumn: f.get('partition') || null,
          predicateColumn: f.get('predicate') || null,
          epicCsaEnabled: f.get('epic') === 'on',
          jdbcUrl: f.get('jdbc_url') || null,
          jdbcUser: f.get('jdbc_user') || null,
          jdbcSecretScope: f.get('jdbc_secret_scope') || null,
          jdbcSecretKey: f.get('jdbc_secret_key') || null,
          connectionName: f.get('connection_name') || null,
          watermarkThresholdMinutes: Number(f.get('watermark_threshold_minutes') || 5),
          fetchSize: Number(f.get('fetch_size') || 10000),
          numPartitions: Number(f.get('num_partitions') || 8),
          enabled: f.get('enabled') === 'on',
        }),
      });
      onSaved();
    } catch (x) {
      setError((x as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] max-w-4xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{row ? 'Edit' : 'Add'} configuration</DialogTitle>
          <DialogDescription>Values map directly to jdbc_ingestion_config.</DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          <Tabs defaultValue="mapping" className="w-full">
            <TabsList className="grid w-full grid-cols-3">
              <TabsTrigger value="mapping">Source mapping</TabsTrigger>
              <TabsTrigger value="incremental">Incremental settings</TabsTrigger>
              <TabsTrigger value="connection">Connection &amp; runtime</TabsTrigger>
            </TabsList>
            <TabsContent value="mapping" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <Field name="group" label="Ingestion group" value={row?.ingestion_group} />
                <Field name="source" label="Source table" value={row?.source_table_name} />
                <Field name="target_fqn" label="Final target FQN" value={row?.target_table_fqn} required />
                <Field name="staging_fqn" label="Staging table FQN" value={row?.staging_table_fqn} />
                <div>
                  <Label htmlFor="type">Ingestion type</Label>
                  <Select name="type" defaultValue={row?.ingestion_type ?? 'incremental'}>
                    <SelectTrigger id="type" className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="incremental">Incremental</SelectItem>
                      <SelectItem value="full">Full</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Toggle name="enabled" label="Enabled" checked={row?.enabled ?? true} />
              </div>
            </TabsContent>
            <TabsContent value="incremental" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <Field name="keys" label="Key columns" value={row?.key_columns} />
                <Field name="watermark" label="Watermark column" value={row?.watermark_column} />
                <Field name="partition" label="Partition column" value={row?.partition_column} />
                <Field name="predicate" label="Predicate column" value={row?.predicate_column} />
                <Field
                  name="watermark_threshold_minutes"
                  label="Watermark delay (minutes)"
                  value={String(row?.watermark_threshold_minutes ?? 5)}
                />
                <Toggle name="epic" label="EPIC CSA mode" checked={row?.epic_csa_enabled ?? false} />
              </div>
            </TabsContent>
            <TabsContent value="connection" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="md:col-span-2">
                  <Field name="jdbc_url" label="JDBC URL" value={row?.jdbc_url} />
                </div>
                <Field name="jdbc_user" label="JDBC user" value={row?.jdbc_user} />
                <Field name="connection_name" label="UC connection name" value={row?.connection_name} />
                <Field name="jdbc_secret_scope" label="JDBC secret scope" value={row?.jdbc_secret_scope} />
                <Field name="jdbc_secret_key" label="JDBC secret key" value={row?.jdbc_secret_key} />
                <Field name="fetch_size" label="JDBC fetch size" value={String(row?.fetch_size ?? 10000)} />
                <Field name="num_partitions" label="JDBC partitions" value={String(row?.num_partitions ?? 8)} />
              </div>
            </TabsContent>
          </Tabs>
          <DialogFooter>
            <Button type="submit" disabled={busy}>
              {busy ? 'Saving…' : 'Save configuration'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
function Field({
  name,
  label,
  value,
  required = false,
}: {
  name: string;
  label: string;
  value?: string | null;
  required?: boolean;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={name}>{label}</Label>
      <Input
        id={name}
        name={name}
        defaultValue={value ?? ''}
        required={required || name === 'group' || name === 'source'}
      />
    </div>
  );
}
function Toggle({ name, label, checked }: { name: string; label: string; checked: boolean }) {
  return (
    <label className="flex items-center gap-3 rounded-md border p-3 text-sm">
      <Switch name={name} defaultChecked={checked} />
      {label}
    </label>
  );
}

function WatermarksPage() {
  const { location, revision, refresh } = useControl();
  const [search, setSearch] = useState('');
  const [editing, setEditing] = useState<WatermarkRow | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const params = useMemo(
    () => ({
      table_name: sql.string(fqn(location, 'jdbc_ingestion_watermark')),
      search: sql.string(search),
      page_size: sql.int(100),
      page_offset: sql.int(0),
      refresh_token: sql.int(revision),
    }),
    [location, search, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('watermark_entries', params);
  const reset = async (row: WatermarkRow) => {
    if (
      !confirm(
        `Delete the watermark for ${row.source_table_name}? The next incremental run will perform a full refresh.`
      )
    )
      return;
    try {
      await api('/api/watermark', {
        method: 'DELETE',
        body: JSON.stringify({
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
        }),
      });
      refresh();
    } catch (e) {
      setMessage((e as Error).message);
    }
  };
  return (
    <>
      <PageTitle
        title="Incremental watermark state"
        description="Actual persisted state; deleting a row forces the framework’s next incremental read to start fresh."
      />
      <div className="mb-4 flex gap-2">
        <Input
          aria-label="Filter watermarks"
          placeholder="Filter group or source table"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <Button variant="outline" onClick={refresh}>
          <RefreshCw className="h-4 w-4" />
        </Button>
      </div>
      {message && <ErrorState message={message} />}{' '}
      {loading ? (
        <Skeleton className="h-80" />
      ) : error ? (
        <ErrorState message={error} />
      ) : !data?.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>No watermark rows</EmptyTitle>
            <EmptyDescription>No incremental state exists at this location.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Group</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Last watermark</TableHead>
                <TableHead>Last run</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Error</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((row) => (
                <TableRow key={`${row.ingestion_group}/${row.source_table_name}`}>
                  <TableCell>{row.ingestion_group}</TableCell>
                  <TableCell className="font-mono text-xs">{row.source_table_name}</TableCell>
                  <TableCell>{row.last_watermark ?? '—'}</TableCell>
                  <TableCell>{displayTime(row.last_run_timestamp)}</TableCell>
                  <TableCell>
                    <Badge variant={row.status === 'FAILED' ? 'destructive' : 'secondary'}>
                      {row.status ?? 'UNKNOWN'}
                    </Badge>
                  </TableCell>
                  <TableCell className="max-w-64 truncate" title={row.last_error ?? ''}>
                    {row.last_error ?? '—'}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button size="sm" variant="ghost" onClick={() => setEditing(row)}>
                      Update
                    </Button>
                    <Button size="sm" variant="destructive" onClick={() => void reset(row)}>
                      Force full refresh
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
      <WatermarkDialog
        row={editing}
        location={location}
        onClose={() => setEditing(null)}
        onSaved={() => {
          setEditing(null);
          refresh();
        }}
      />
    </>
  );
}
function WatermarkDialog({
  row,
  location,
  onClose,
  onSaved,
}: {
  row: WatermarkRow | null;
  location: Location;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!row) return;
    const f = new FormData(e.currentTarget);
    try {
      await api('/api/watermark', {
        method: 'PATCH',
        body: JSON.stringify({
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
          lastWatermark: f.get('watermark') || null,
          status: f.get('status'),
        }),
      });
      onSaved();
    } catch (x) {
      setError((x as Error).message);
    }
  };
  return (
    <Dialog open={!!row} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Update watermark</DialogTitle>
          <DialogDescription>
            {row?.ingestion_group} / {row?.source_table_name}
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          <Field name="watermark" label="Last watermark" value={row?.last_watermark} />
          <Field name="status" label="Status" value={row?.status ?? 'SUCCESS'} />
          <DialogFooter>
            <Button type="submit">Save state</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function JobsPage() {
  const { location, revision } = useControl();
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [runningJob, setRunningJob] = useState<number | null>(null);
  const [scheduleJob, setScheduleJob] = useState<JobRow | null>(null);
  const groupParams = useMemo(
    () => ({
      table_name: sql.string(fqn(location, 'jdbc_ingestion_config')),
      refresh_token: sql.int(revision),
    }),
    [location, revision]
  );
  const {
    data: groups,
    loading: groupsLoading,
    error: groupsError,
  } = useAnalyticsQuery('ingestion_groups', groupParams);
  const load = () => {
    setLoading(true);
    setError(null);
    api<{ jobs: JobRow[] }>('/api/jobs')
      .then((x) => setJobs(x.jobs))
      .catch((e) => setError(errorMessage(e)))
      .finally(() => setLoading(false));
  };
  useEffect(() => {
    const timer = window.setTimeout(load, 0);
    return () => window.clearTimeout(timer);
  }, []);
  const run = async (id: number) => {
    setRunningJob(id);
    try {
      const x = await api<{ runId: number }>(`/api/jobs/${id}/run`, { method: 'POST' });
      alert(`Run ${x.runId} started`);
      load();
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setRunningJob(null);
    }
  };
  return (
    <>
      <PageTitle
        title="WaterSync Lakeflow Jobs"
        description="Monitor recent run health, trigger ingestion, or open the full job in Databricks."
        action={
          <Button onClick={() => setOpen(true)}>
            <Plus className="mr-2 h-4 w-4" />
            Create job
          </Button>
        }
      />
      {error && <ErrorState message={error} />}{' '}
      {loading ? (
        <Skeleton className="h-72" />
      ) : !jobs.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>No jobs visible</EmptyTitle>
            <EmptyDescription>
              Grant the app service principal workspace access or create the first job.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          {jobs.map((job) => (
            <Card key={job.job_id} className="job-card">
              <CardHeader>
                <div className="flex justify-between gap-4">
                  <div>
                    <CardTitle>{job.settings?.name ?? `Job ${job.job_id}`}</CardTitle>
                    <CardDescription>
                      ID {job.job_id} · created {displayTime(job.created_time)}
                    </CardDescription>
                  </div>
                  <Badge variant={statusVariant(runStatus(job.runs[0]))}>
                    {runStatus(job.runs[0]).replaceAll('_', ' ')}
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="space-y-5">
                <div className="rounded-md border bg-muted/30 p-3">
                  <div className="mb-1 flex items-center justify-between gap-3">
                    <div className="flex min-w-0 items-center gap-3">
                      <span className="text-sm font-medium">Schedule</span>
                      {job.settings?.schedule && (
                        <span className="truncate font-mono text-xs text-foreground">
                          {job.settings.schedule.quartz_cron_expression}
                        </span>
                      )}
                    </div>
                    <Badge variant={job.settings?.schedule?.pause_status === 'UNPAUSED' ? 'secondary' : 'outline'}>
                      {!job.settings?.schedule
                        ? 'Manual only'
                        : job.settings.schedule.pause_status === 'UNPAUSED'
                          ? 'Active'
                          : 'Paused'}
                    </Badge>
                  </div>
                  {job.settings?.schedule ? (
                    <div className="text-xs text-muted-foreground">{job.settings.schedule.timezone_id}</div>
                  ) : (
                    <p className="text-xs text-muted-foreground">Runs only when manually triggered.</p>
                  )}
                </div>
                <div>
                  <div className="mb-2 flex items-center justify-between gap-3 text-xs text-muted-foreground">
                    <span className="font-medium text-foreground">Last 10 runs</span>
                    <span>Newest on the right</span>
                  </div>
                  {job.runs.length ? (
                    <div
                      className="run-timeline"
                      aria-label={`Recent run status for ${job.settings?.name ?? `job ${job.job_id}`}`}
                    >
                      {[...job.runs].reverse().map((recentRun) => {
                        const status = runStatus(recentRun);
                        const label = `${status.replaceAll('_', ' ')} · ${displayTime(recentRun.start_time)} · ${runDuration(recentRun)}`;
                        return (
                          <a
                            key={recentRun.run_id}
                            className={`run-mark run-mark-${status.toLowerCase()}`}
                            title={label}
                            aria-label={label}
                            href={recentRun.run_url}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {status === 'RUNNING' ? (
                              <RefreshCw className="h-4 w-4 animate-spin" aria-hidden="true" />
                            ) : (
                              <span>{status === 'SUCCESS' ? '✓' : '!'}</span>
                            )}
                          </a>
                        );
                      })}
                    </div>
                  ) : (
                    <p className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">
                      No runs recorded yet.
                    </p>
                  )}
                  <p className="mt-2 text-xs text-muted-foreground">
                    Latest:{' '}
                    {job.runs[0] ? `${displayTime(job.runs[0].start_time)} · ${runDuration(job.runs[0])}` : 'Never run'}
                  </p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    disabled={!job.job_id || runningJob === job.job_id}
                    onClick={() => {
                      if (job.job_id) void run(job.job_id);
                    }}
                  >
                    <Play className="mr-2 h-4 w-4" />
                    {runningJob === job.job_id ? 'Starting…' : 'Run now'}
                  </Button>
                  <Button variant="outline" asChild>
                    <a href={job.workspace_url} target="_blank" rel="noreferrer">
                      Open in Databricks
                      <ExternalLink className="ml-2 h-4 w-4" />
                    </a>
                  </Button>
                  <Button variant="outline" onClick={() => setScheduleJob(job)} disabled={!job.job_id}>
                    <Clock3 className="mr-2 h-4 w-4" />
                    Edit schedule
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
      <JobDialog
        open={open}
        onOpenChange={setOpen}
        location={location}
        groups={groups ?? []}
        groupsLoading={groupsLoading}
        groupsError={groupsError}
        onSaved={() => {
          setOpen(false);
          load();
        }}
      />
      <ScheduleDialog
        job={scheduleJob}
        onClose={() => setScheduleJob(null)}
        onSaved={() => {
          setScheduleJob(null);
          load();
        }}
      />
    </>
  );
}
function JobDialog({
  open,
  onOpenChange,
  location,
  groups,
  groupsLoading,
  groupsError,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (v: boolean) => void;
  location: Location;
  groups: Array<{
    ingestion_group: string;
    source_count: number;
    enabled_source_count: number;
    enabled_incremental_source_count: number;
  }>;
  groupsLoading: boolean;
  groupsError: string | null;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [ingestionGroup, setIngestionGroup] = useState('');
  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [scheduleActive, setScheduleActive] = useState(true);
  const [gitUrl, setGitUrl] = useState('https://github.com/erinaldidb/watersync');
  const [gitBranch, setGitBranch] = useState('main');
  const [computeMode, setComputeMode] = useState<'SERVERLESS' | 'JOB_CLUSTER'>('SERVERLESS');
  const [performanceTarget, setPerformanceTarget] = useState<'STANDARD' | 'PERFORMANCE_OPTIMIZED'>('STANDARD');
  useEffect(() => {
    if (open && !ingestionGroup && groups[0]) setIngestionGroup(groups[0].ingestion_group);
  }, [open, ingestionGroup, groups]);
  const selectedGroup = groups.find((group) => group.ingestion_group === ingestionGroup);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const form = new FormData(e.currentTarget);
      const result = await api<{ jobId: number; action: 'created' | 'updated' }>('/api/jobs', {
        method: 'POST',
        body: JSON.stringify({
          ...location,
          ingestionGroup,
          gitUrl: form.get('gitUrl'),
          gitBranch: form.get('gitBranch'),
          foreachConcurrency: Number(form.get('foreachConcurrency')),
          cdcPipelineId: selectedGroup?.enabled_incremental_source_count ? form.get('cdcPipelineId') : null,
          schedule: {
            enabled: scheduleEnabled,
            quartzCronExpression: formString(form, 'quartzCronExpression', '0 0 8 * * ?'),
            timezoneId: formString(form, 'timezoneId', 'America/New_York'),
            pauseStatus: scheduleActive ? 'UNPAUSED' : 'PAUSED',
          },
          compute: {
            mode: computeMode,
            performanceTarget,
            sparkVersion: formString(form, 'sparkVersion', ''),
            driverNodeTypeId: formString(form, 'driverNodeTypeId', ''),
            workerNodeTypeId: formString(form, 'workerNodeTypeId', ''),
            minWorkers: Number(form.get('minWorkers') ?? 1),
            maxWorkers: Number(form.get('maxWorkers') ?? 4),
          },
        }),
      });
      alert(`Job ${result.jobId} ${result.action}`);
      onSaved();
    } catch (x) {
      setError((x as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[calc(100vw-2rem)] sm:max-w-5xl">
        <DialogHeader>
          <DialogTitle>Create or update a WaterSync job</DialogTitle>
          <DialogDescription>
            Select a configured ingestion group. WaterSync generates the parameters and task graph automatically.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          {groupsError && <ErrorState message={groupsError} />}
          <Tabs defaultValue="job" className="w-full">
            <TabsList className={`grid w-full ${computeMode === 'JOB_CLUSTER' ? 'grid-cols-5' : 'grid-cols-4'}`}>
              <TabsTrigger value="job">Job configuration</TabsTrigger>
              <TabsTrigger value="source">Git source &amp; execution</TabsTrigger>
              <TabsTrigger value="compute">Compute</TabsTrigger>
              {computeMode === 'JOB_CLUSTER' && <TabsTrigger value="cluster">Job cluster</TabsTrigger>}
              <TabsTrigger value="schedule">Schedule</TabsTrigger>
            </TabsList>
            <TabsContent value="job" forceMount className="mt-4 space-y-4 data-[state=inactive]:hidden">
              <div className="space-y-2">
                <Label htmlFor="ingestion-group">Ingestion group</Label>
                {groupsLoading ? (
                  <Skeleton className="h-10" />
                ) : groups.length ? (
                  <Select value={ingestionGroup} onValueChange={setIngestionGroup}>
                    <SelectTrigger id="ingestion-group" className="w-full">
                      <SelectValue placeholder="Select an ingestion group" />
                    </SelectTrigger>
                    <SelectContent>
                      {groups.map((group) => (
                        <SelectItem key={group.ingestion_group} value={group.ingestion_group}>
                          {group.ingestion_group} · {group.enabled_source_count} enabled sources
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : (
                  <Empty>
                    <EmptyHeader>
                      <EmptyTitle>No ingestion groups found</EmptyTitle>
                      <EmptyDescription>Add a row to the configuration table before creating a job.</EmptyDescription>
                    </EmptyHeader>
                  </Empty>
                )}
              </div>
              {selectedGroup && (
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Generated job</CardTitle>
                    <CardDescription>
                      [{selectedGroup.ingestion_group}] Ingestion Pipeline · {selectedGroup.source_count} configured
                      sources
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="grid gap-4 text-sm text-muted-foreground md:grid-cols-2">
                    <div className="min-w-0 space-y-1">
                      <div>configuration_fqn</div>
                      <div className="break-all font-mono text-xs text-foreground">
                        {fqn(location, 'jdbc_ingestion_config')}
                      </div>
                    </div>
                    <div className="min-w-0 space-y-1">
                      <div>watermark_fqn</div>
                      <div className="break-all font-mono text-xs text-foreground">
                        {fqn(location, 'jdbc_ingestion_watermark')}
                      </div>
                    </div>
                    <div className="min-w-0 space-y-1">
                      <div>ingestion_group</div>
                      <div className="break-all font-mono text-xs text-foreground">{selectedGroup.ingestion_group}</div>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>
            <TabsContent value="source" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2 md:col-span-2">
                  <Label htmlFor="git-url">GitHub repository URL</Label>
                  <Input
                    id="git-url"
                    name="gitUrl"
                    type="url"
                    value={gitUrl}
                    onChange={(event) => setGitUrl(event.target.value)}
                    placeholder="https://github.com/organization/repository"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="git-branch">Branch</Label>
                  <Input
                    id="git-branch"
                    name="gitBranch"
                    value={gitBranch}
                    onChange={(event) => setGitBranch(event.target.value)}
                    placeholder="main"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="concurrency">Parallel tables</Label>
                  <Input
                    id="concurrency"
                    name="foreachConcurrency"
                    type="number"
                    min="1"
                    max="100"
                    defaultValue="4"
                    required
                  />
                </div>
                <div className="space-y-2 md:col-span-2">
                  <Label htmlFor="pipeline-id">Existing CDC pipeline ID (optional)</Label>
                  <Input
                    id="pipeline-id"
                    name="cdcPipelineId"
                    placeholder={
                      selectedGroup?.enabled_incremental_source_count
                        ? 'Leave blank to create or reuse the group CDC pipeline'
                        : 'Not required: this group has no enabled incremental tables'
                    }
                    disabled={!selectedGroup?.enabled_incremental_source_count}
                  />
                  {!selectedGroup?.enabled_incremental_source_count && (
                    <p className="text-xs text-muted-foreground">
                      The CDC declarative pipeline task will be omitted for this ingestion group.
                    </p>
                  )}
                  {Boolean(selectedGroup?.enabled_incremental_source_count) && (
                    <p className="text-xs text-muted-foreground">
                      When blank, WaterSync automatically provisions a serverless CDC SCD2 pipeline for this group.
                    </p>
                  )}
                </div>
                <div className="space-y-2 md:col-span-2">
                  <Label>Task library</Label>
                  <div className="break-all rounded-md border bg-muted/30 p-3 font-mono text-xs">
                    watersync@git+{gitUrl.replace(/\.git$/, '')}.git@{gitBranch || 'main'}
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Installed on the planner and every ingestion worker task from the selected branch.
                  </p>
                </div>
              </div>
            </TabsContent>
            <TabsContent value="schedule" forceMount className="mt-4 data-[state=inactive]:hidden">
              <ScheduleFields
                enabled={scheduleEnabled}
                onEnabledChange={setScheduleEnabled}
                active={scheduleActive}
                onActiveChange={setScheduleActive}
              />
            </TabsContent>
            <TabsContent value="compute" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="compute-mode">Compute mode</Label>
                  <Select value={computeMode} onValueChange={(value) => setComputeMode(value as typeof computeMode)}>
                    <SelectTrigger id="compute-mode" className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="SERVERLESS">Serverless</SelectItem>
                      <SelectItem value="JOB_CLUSTER">Shared autoscaling job cluster</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {computeMode === 'SERVERLESS' && (
                  <div className="space-y-2">
                    <Label htmlFor="performance-target">Serverless performance mode</Label>
                    <Select
                      value={performanceTarget}
                      onValueChange={(value) => setPerformanceTarget(value as typeof performanceTarget)}
                    >
                      <SelectTrigger id="performance-target" className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="STANDARD">Standard</SelectItem>
                        <SelectItem value="PERFORMANCE_OPTIMIZED">Performance optimized</SelectItem>
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-muted-foreground">
                      Standard favors cost efficiency; performance optimized favors faster startup and execution.
                    </p>
                  </div>
                )}
              </div>
            </TabsContent>
            {computeMode === 'JOB_CLUSTER' && (
              <TabsContent value="cluster" forceMount className="mt-4 data-[state=inactive]:hidden">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2 md:col-span-2">
                    <Label htmlFor="spark-version">Databricks Runtime</Label>
                    <Input
                      id="spark-version"
                      name="sparkVersion"
                      defaultValue="18.x-scala2.13"
                      placeholder="18.x-scala2.13"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="driver-node-type">Driver node type</Label>
                    <Input
                      id="driver-node-type"
                      name="driverNodeTypeId"
                      placeholder="For example: i3.xlarge"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="worker-node-type">Worker node type</Label>
                    <Input
                      id="worker-node-type"
                      name="workerNodeTypeId"
                      placeholder="For example: i3.xlarge"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="min-workers">Minimum workers</Label>
                    <Input id="min-workers" name="minWorkers" type="number" min="0" defaultValue="1" required />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="max-workers">Maximum workers</Label>
                    <Input id="max-workers" name="maxWorkers" type="number" min="1" defaultValue="4" required />
                  </div>
                  <p className="text-xs text-muted-foreground md:col-span-2">
                    The planner and ingestion workers share this autoscaling job cluster. The CDC pipeline task uses its
                    own pipeline compute.
                  </p>
                </div>
              </TabsContent>
            )}
          </Tabs>
          <p className="text-xs text-muted-foreground">
            If a job with the generated name already exists, its definition is updated in place.
          </p>
          <DialogFooter>
            <Button type="submit" disabled={busy || !ingestionGroup || groupsLoading}>
              {busy ? 'Saving…' : 'Create or update job'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function ScheduleFields({
  enabled,
  onEnabledChange,
  active,
  onActiveChange,
  schedule,
}: {
  enabled: boolean;
  onEnabledChange: (value: boolean) => void;
  active: boolean;
  onActiveChange: (value: boolean) => void;
  schedule?: JobSchedule;
}) {
  return (
    <div className="space-y-4">
      <label className="flex items-center justify-between gap-4 rounded-md border p-3 text-sm">
        <span>
          <span className="block font-medium">Scheduled execution</span>
          <span className="text-xs text-muted-foreground">Run automatically using a Quartz cron schedule.</span>
        </span>
        <Switch checked={enabled} onCheckedChange={onEnabledChange} aria-label="Scheduled execution" />
      </label>
      <div className="grid gap-4 md:grid-cols-2">
        <div className="space-y-2 md:col-span-2">
          <Label htmlFor="quartz-cron">Quartz cron expression</Label>
          <Input
            id="quartz-cron"
            name="quartzCronExpression"
            defaultValue={schedule?.quartz_cron_expression ?? '0 0 8 * * ?'}
            placeholder="0 0 8 * * ?"
            required
            disabled={!enabled}
          />
          <p className="text-xs text-muted-foreground">Seconds, minutes, hours, day of month, month, day of week.</p>
        </div>
        <div className="space-y-2">
          <Label htmlFor="schedule-timezone">Timezone</Label>
          <Input
            id="schedule-timezone"
            name="timezoneId"
            defaultValue={schedule?.timezone_id ?? 'America/New_York'}
            placeholder="America/New_York"
            required
            disabled={!enabled}
          />
        </div>
        <label className="flex items-center justify-between gap-4 rounded-md border p-3 text-sm">
          <span>
            <span className="block font-medium">Schedule active</span>
            <span className="text-xs text-muted-foreground">Turn off to save the schedule in a paused state.</span>
          </span>
          <Switch checked={active} onCheckedChange={onActiveChange} aria-label="Schedule active" disabled={!enabled} />
        </label>
      </div>
    </div>
  );
}

function ScheduleDialog({ job, onClose, onSaved }: { job: JobRow | null; onClose: () => void; onSaved: () => void }) {
  const schedule = job?.settings?.schedule;
  const [enabled, setEnabled] = useState(false);
  const [active, setActive] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setEnabled(Boolean(schedule));
    setActive(schedule?.pause_status !== 'PAUSED');
    setError(null);
  }, [job, schedule]);
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!job?.job_id) return;
    const form = new FormData(event.currentTarget);
    setBusy(true);
    setError(null);
    try {
      await api(`/api/jobs/${job.job_id}/schedule`, {
        method: 'PATCH',
        body: JSON.stringify({
          enabled,
          quartzCronExpression: formString(form, 'quartzCronExpression', '0 0 8 * * ?'),
          timezoneId: formString(form, 'timezoneId', 'America/New_York'),
          pauseStatus: active ? 'UNPAUSED' : 'PAUSED',
        }),
      });
      onSaved();
    } catch (value) {
      setError(errorMessage(value));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={Boolean(job)} onOpenChange={(value) => !value && onClose()}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Edit job schedule</DialogTitle>
          <DialogDescription>{job?.settings?.name ?? `Job ${job?.job_id}`}</DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          <ScheduleFields
            key={`${job?.job_id}-${schedule?.quartz_cron_expression ?? 'manual'}`}
            enabled={enabled}
            onEnabledChange={setEnabled}
            active={active}
            onActiveChange={setActive}
            schedule={schedule}
          />
          <DialogFooter>
            <Button type="submit" disabled={busy}>
              {busy ? 'Saving…' : enabled ? 'Save schedule' : 'Remove schedule'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <OverviewPage /> },
      { path: '/config', element: <ConfigPage /> },
      { path: '/watermarks', element: <WatermarksPage /> },
      { path: '/jobs', element: <JobsPage /> },
    ],
  },
]);
export default function App() {
  return <RouterProvider router={router} />;
}
