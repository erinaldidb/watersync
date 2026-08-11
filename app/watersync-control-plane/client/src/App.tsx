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
  Textarea,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { Activity, Database, Droplets, Play, Plus, RefreshCw, Settings2, Trash2, Workflow } from 'lucide-react';

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
type JobRow = { job_id?: number; settings?: { name?: string }; creator_user_name?: string; created_time?: number };

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
    <div className="min-h-screen bg-background text-foreground">
      <header className="border-b bg-card">
        <div className="mx-auto flex max-w-screen-2xl flex-wrap items-center gap-4 px-5 py-3">
          <div className="mr-4 flex items-center gap-2 font-semibold">
            <Database className="h-5 w-5" /> WaterSync Control Plane
          </div>
          <nav className="flex flex-1 flex-wrap gap-1">
            {links.map(([to, label, Icon]) => (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                className={({ isActive }) =>
                  `flex items-center gap-2 rounded-md px-3 py-2 text-sm ${isActive ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-muted'}`
                }
              >
                <Icon className="h-4 w-4" />
                {label}
              </NavLink>
            ))}
          </nav>
          <Dialog>
            <DialogTrigger asChild>
              <Button variant="outline" size="sm">
                <Database className="mr-2 h-4 w-4" />
                {location.catalog}.{location.schema}
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
                <div>
                  <Label htmlFor="catalog">Catalog</Label>
                  <Input
                    id="catalog"
                    value={draft.catalog}
                    onChange={(e) => setDraft({ ...draft, catalog: e.target.value })}
                  />
                </div>
                <div>
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
      </header>
      <main className="mx-auto max-w-screen-2xl p-5">
        <Outlet context={context} />
      </main>
    </div>
  );
}
const useControl = () => useOutletContext<Context>();

function PageTitle({ title, description, action }: { title: string; description: string; action?: React.ReactNode }) {
  return (
    <div className="mb-6 flex flex-wrap items-start justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold">{title}</h1>
        <p className="text-sm text-muted-foreground">{description}</p>
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
        <div className="grid gap-4 md:grid-cols-4">
          <Metric title="Configured sources" value={row.config_count} note="rows · current location" />
          <Metric title="Enabled sources" value={row.enabled_count} note="actual · current period" />
          <Metric
            title="Failed sources"
            value={row.failed_count}
            note="actual status · investigate"
            destructive={Number(row.failed_count) > 0}
          />
          <Metric
            title="Latest pipeline activity"
            value={displayTime(row.last_run_timestamp)}
            note={`source: ${location.catalog}.${location.schema}`}
          />
        </div>
      )}
      <Card className="mt-6">
        <CardHeader>
          <CardTitle>Operating model</CardTitle>
          <CardDescription>
            Configure ingestion sources, inspect or reset incremental state, then create and run framework jobs.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-3">
          <Hint title="1. Configuration" text="Maintain JDBC source mappings and enablement." />
          <Hint title="2. Watermarks" text="Correct state or delete a row to force a full incremental reload." />
          <Hint title="3. Jobs" text="Create, replace, monitor, and trigger Lakeflow Jobs." />
        </CardContent>
      </Card>
    </>
  );
}
function Metric({
  title,
  value,
  note,
  destructive = false,
}: {
  title: string;
  value: string | number;
  note: string;
  destructive?: boolean;
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{title}</CardDescription>
        <CardTitle className={destructive ? 'text-destructive' : ''}>{value}</CardTitle>
      </CardHeader>
      <CardContent className="text-xs text-muted-foreground">{note}</CardContent>
    </Card>
  );
}
function Hint({ title, text }: { title: string; text: string }) {
  return (
    <div className="rounded-md border p-4">
      <div className="font-medium">{title}</div>
      <div className="mt-1 text-sm text-muted-foreground">{text}</div>
    </div>
  );
}

function ConfigPage() {
  const { location, revision, refresh } = useControl();
  const [search, setSearch] = useState('');
  const [editing, setEditing] = useState<ConfigRow | null>(null);
  const [open, setOpen] = useState(false);
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
          <Button
            onClick={() => {
              setEditing(null);
              setOpen(true);
            }}
          >
            <Plus className="mr-2 h-4 w-4" />
            Add entry
          </Button>
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
    </>
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
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{row ? 'Edit' : 'Add'} configuration</DialogTitle>
          <DialogDescription>Values map directly to jdbc_ingestion_config.</DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="grid gap-3 md:grid-cols-2">
          {error && (
            <div className="md:col-span-2">
              <ErrorState message={error} />
            </div>
          )}
          <Field name="group" label="Ingestion group" value={row?.ingestion_group} />
          <Field name="source" label="Source table" value={row?.source_table_name} />
          <Field name="target_fqn" label="Final target FQN" value={row?.target_table_fqn} required />
          <Field name="staging_fqn" label="Staging table FQN" value={row?.staging_table_fqn} />
          <Field name="keys" label="Key columns" value={row?.key_columns} />
          <Field name="watermark" label="Watermark column" value={row?.watermark_column} />
          <Field name="partition" label="Partition column" value={row?.partition_column} />
          <Field name="predicate" label="Predicate column" value={row?.predicate_column} />
          <Field name="jdbc_url" label="JDBC URL" value={row?.jdbc_url} />
          <Field name="jdbc_user" label="JDBC user" value={row?.jdbc_user} />
          <Field name="jdbc_secret_scope" label="JDBC secret scope" value={row?.jdbc_secret_scope} />
          <Field name="jdbc_secret_key" label="JDBC secret key" value={row?.jdbc_secret_key} />
          <Field name="connection_name" label="UC connection name" value={row?.connection_name} />
          <Field
            name="watermark_threshold_minutes"
            label="Watermark delay (minutes)"
            value={String(row?.watermark_threshold_minutes ?? 5)}
          />
          <Field name="fetch_size" label="JDBC fetch size" value={String(row?.fetch_size ?? 10000)} />
          <Field name="num_partitions" label="JDBC partitions" value={String(row?.num_partitions ?? 8)} />
          <div>
            <Label htmlFor="type">Ingestion type</Label>
            <Select name="type" defaultValue={row?.ingestion_type ?? 'incremental'}>
              <SelectTrigger id="type">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="incremental">Incremental</SelectItem>
                <SelectItem value="full">Full</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Toggle name="enabled" label="Enabled" checked={row?.enabled ?? true} />
          <Toggle name="epic" label="EPIC CSA mode" checked={row?.epic_csa_enabled ?? false} />
          <DialogFooter className="md:col-span-2">
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
    <div>
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
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [jobId, setJobId] = useState<number | undefined>();
  const load = () => {
    setLoading(true);
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
    try {
      const x = await api<{ runId: number }>(`/api/jobs/${id}/run`, { method: 'POST' });
      alert(`Run ${x.runId} started`);
    } catch (e) {
      setError(errorMessage(e));
    }
  };
  return (
    <>
      <PageTitle
        title="WaterSync Lakeflow Jobs"
        description="Jobs visible to the app service principal. Create or replace definitions, then trigger runs."
        action={
          <Button
            onClick={() => {
              setJobId(undefined);
              setOpen(true);
            }}
          >
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
            <Card key={job.job_id}>
              <CardHeader>
                <div className="flex justify-between gap-4">
                  <div>
                    <CardTitle>{job.settings?.name ?? `Job ${job.job_id}`}</CardTitle>
                    <CardDescription>
                      ID {job.job_id} · created {displayTime(job.created_time)}
                    </CardDescription>
                  </div>
                  <Badge variant="outline">Lakeflow Job</Badge>
                </div>
              </CardHeader>
              <CardContent className="flex gap-2">
                <Button
                  disabled={!job.job_id}
                  onClick={() => {
                    if (job.job_id) void run(job.job_id);
                  }}
                >
                  <Play className="mr-2 h-4 w-4" />
                  Run now
                </Button>
                <Button
                  variant="outline"
                  onClick={() => {
                    setJobId(job.job_id);
                    setOpen(true);
                  }}
                >
                  Replace definition
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
      <JobDialog
        open={open}
        onOpenChange={setOpen}
        jobId={jobId}
        onSaved={() => {
          setOpen(false);
          load();
        }}
      />
    </>
  );
}
const defaultJob = JSON.stringify(
  {
    name: '[group] Ingestion Pipeline',
    max_concurrent_runs: 1,
    parameters: [
      {
        name: 'configuration_fqn',
        default: 'serverless_pixels_release_catalog.jdbc_incremental_gh.jdbc_ingestion_config',
      },
      {
        name: 'watermark_fqn',
        default: 'serverless_pixels_release_catalog.jdbc_incremental_gh.jdbc_ingestion_watermark',
      },
      { name: 'ingestion_group', default: 'group' },
    ],
    tasks: [
      {
        task_key: 'ingestion_configs',
        notebook_task: { notebook_path: '/Workspace/path/Task - Plan Configs.py', source: 'WORKSPACE' },
      },
      {
        task_key: 'ingestion_worker',
        depends_on: [{ task_key: 'ingestion_configs' }],
        for_each_task: {
          inputs: '{{tasks.ingestion_configs.values.table_configs}}',
          concurrency: 4,
          task: {
            task_key: 'ingestion_worker_iteration',
            notebook_task: {
              notebook_path: '/Workspace/path/Task - Run Ingestion.py',
              source: 'WORKSPACE',
              base_parameters: { source_table_name: '{{input.source_table_name}}' },
            },
          },
        },
      },
    ],
  },
  null,
  2
);
function JobDialog({
  open,
  onOpenChange,
  jobId,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (v: boolean) => void;
  jobId?: number;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const raw = new FormData(e.currentTarget).get('settings');
      if (typeof raw !== 'string') throw new Error('Job settings JSON is required');
      const settings: unknown = JSON.parse(raw);
      await api<unknown>('/api/jobs', { method: 'POST', body: JSON.stringify({ jobId, settings }) });
      onSaved();
    } catch (x) {
      setError((x as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle>{jobId ? `Replace job ${jobId}` : 'Create WaterSync job'}</DialogTitle>
          <DialogDescription>
            Provide a Jobs API 2.1 settings object. Secret values must reference Databricks secret scopes; never paste
            credentials here.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          <Label htmlFor="settings">Job settings JSON</Label>
          <Textarea id="settings" name="settings" className="min-h-96 font-mono text-xs" defaultValue={defaultJob} />
          <DialogFooter>
            <Button type="submit" disabled={busy}>
              {busy ? 'Saving…' : jobId ? 'Replace job' : 'Create job'}
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
