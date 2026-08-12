import { analytics, createApp, server } from '@databricks/appkit';
import { WorkspaceClient, jobs, sql as dbsql } from '@databricks/sdk-experimental';
import { z } from 'zod';

const identifierPart = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]*$/);
const locationSchema = z.object({ catalog: identifierPart, schema: identifierPart });
const configSchema = locationSchema.extend({
  originalIngestionGroup: z.string().optional(),
  originalSourceTableName: z.string().optional(),
  ingestionGroup: z.string().min(1),
  sourceTableName: z.string().min(1),
  stagingTableFqn: z.string().nullable().optional(),
  targetTableFqn: z.string().regex(/^[^.]+\.[^.]+\.[^.]+$/),
  ingestionType: z.enum(['incremental', 'full']),
  keyColumns: z.string().nullable().optional(),
  watermarkColumn: z.string().nullable().optional(),
  partitionColumn: z.string().nullable().optional(),
  predicateColumn: z.string().nullable().optional(),
  epicCsaEnabled: z.boolean(),
  jdbcUrl: z.string().nullable().optional(),
  jdbcUser: z.string().nullable().optional(),
  jdbcSecretScope: z.string().nullable().optional(),
  jdbcSecretKey: z.string().nullable().optional(),
  connectionName: z.string().nullable().optional(),
  watermarkThresholdMinutes: z.number().int().nonnegative(),
  fetchSize: z.number().int().positive(),
  numPartitions: z.number().int().positive(),
  enabled: z.boolean(),
});
const keySchema = locationSchema.extend({ ingestionGroup: z.string().min(1), sourceTableName: z.string().min(1) });
const watermarkSchema = keySchema.extend({ lastWatermark: z.string().nullable(), status: z.string().min(1) });
const jobPayloadSchema = locationSchema.extend({
  ingestionGroup: z.string().min(1),
  gitUrl: z.url().refine((value) => new URL(value).hostname === 'github.com', 'Repository must be hosted on GitHub'),
  gitBranch: z.string().trim().min(1),
  foreachConcurrency: z.number().int().min(1).max(100),
  cdcPipelineId: z.string().trim().optional(),
});

const plannerNotebookPath = 'notebooks/Task - Plan Configs';
const workerNotebookPath = 'notebooks/Task - Run Ingestion';

const workspace = new WorkspaceClient({});
const requiredEnv = (name: string) => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
};
const warehouseId = requiredEnv('DATABRICKS_WAREHOUSE_ID');

const tableName = (catalog: string, schema: string, table: string) => `\`${catalog}\`.\`${schema}\`.\`${table}\``;
const parameter = (
  name: string,
  value: string | null | undefined,
  type = 'STRING'
): dbsql.StatementParameterListItem => ({
  name,
  type,
  ...(value == null ? {} : { value }),
});

async function execute(statement: string, parameters: dbsql.StatementParameterListItem[] = []) {
  const response = await workspace.statementExecution.executeStatement({
    warehouse_id: warehouseId,
    statement,
    parameters,
    wait_timeout: '50s',
    on_wait_timeout: 'CANCEL',
  });
  if (response.status?.state !== 'SUCCEEDED') {
    throw new Error(
      response.status?.error?.message ?? `SQL statement ended in ${response.status?.state ?? 'unknown state'}`
    );
  }
  return response;
}

const handleError = (res: { status(code: number): { json(value: unknown): void } }, error: unknown) => {
  const message = error instanceof Error ? error.message : 'Unexpected error';
  res.status(error instanceof z.ZodError ? 400 : 500).json({ error: message });
};

await createApp({
  plugins: [analytics(), server()],
  onPluginsReady(appkit) {
    appkit.server.extend((app) => {
      app.post('/api/config', async (req, res) => {
        try {
          const body = configSchema.parse(req.body);
          const target = tableName(body.catalog, body.schema, 'jdbc_ingestion_config');
          await execute(
            `MERGE INTO ${target} t USING (SELECT :ingestion_group ingestion_group, :source_table_name source_table_name) s
             ON t.ingestion_group = coalesce(:original_ingestion_group, s.ingestion_group)
             AND t.source_table_name = coalesce(:original_source_table_name, s.source_table_name)
             WHEN MATCHED THEN UPDATE SET ingestion_group=s.ingestion_group, source_table_name=s.source_table_name,
               staging_table_fqn=:staging_table_fqn, target_table_fqn=:target_table_fqn,
               ingestion_type=:ingestion_type, key_columns=:key_columns,
               watermark_column=:watermark_column, partition_column=:partition_column,
               predicate_column=:predicate_column, epic_csa_enabled=:epic_csa_enabled,
               jdbc_url=:jdbc_url, jdbc_user=:jdbc_user, jdbc_secret_scope=:jdbc_secret_scope,
               jdbc_secret_key=:jdbc_secret_key, connection_name=:connection_name,
               watermark_threshold_minutes=:watermark_threshold_minutes, fetch_size=:fetch_size,
               num_partitions=:num_partitions,
               update_dttm=current_timestamp(), enabled=:enabled
             WHEN NOT MATCHED THEN INSERT (ingestion_group, source_table_name, staging_table_fqn, target_table_fqn,
               ingestion_type, key_columns, watermark_column, partition_column, predicate_column, epic_csa_enabled,
               jdbc_url, jdbc_user, jdbc_secret_scope, jdbc_secret_key, connection_name,
               watermark_threshold_minutes, fetch_size, num_partitions, update_dttm, enabled)
             VALUES (s.ingestion_group, s.source_table_name, :staging_table_fqn, :target_table_fqn,
               :ingestion_type, :key_columns, :watermark_column, :partition_column, :predicate_column, :epic_csa_enabled,
               :jdbc_url, :jdbc_user, :jdbc_secret_scope, :jdbc_secret_key, :connection_name,
               :watermark_threshold_minutes, :fetch_size, :num_partitions, current_timestamp(), :enabled)`,
            [
              parameter('original_ingestion_group', body.originalIngestionGroup),
              parameter('original_source_table_name', body.originalSourceTableName),
              parameter('ingestion_group', body.ingestionGroup),
              parameter('source_table_name', body.sourceTableName),
              parameter('staging_table_fqn', body.stagingTableFqn),
              parameter('target_table_fqn', body.targetTableFqn),
              parameter('ingestion_type', body.ingestionType),
              parameter('key_columns', body.keyColumns),
              parameter('watermark_column', body.watermarkColumn),
              parameter('partition_column', body.partitionColumn),
              parameter('predicate_column', body.predicateColumn),
              parameter('epic_csa_enabled', String(body.epicCsaEnabled), 'BOOLEAN'),
              parameter('jdbc_url', body.jdbcUrl),
              parameter('jdbc_user', body.jdbcUser),
              parameter('jdbc_secret_scope', body.jdbcSecretScope),
              parameter('jdbc_secret_key', body.jdbcSecretKey),
              parameter('connection_name', body.connectionName),
              parameter('watermark_threshold_minutes', String(body.watermarkThresholdMinutes), 'INT'),
              parameter('fetch_size', String(body.fetchSize), 'INT'),
              parameter('num_partitions', String(body.numPartitions), 'INT'),
              parameter('enabled', String(body.enabled), 'BOOLEAN'),
            ]
          );
          res.json({ ok: true });
        } catch (error) {
          handleError(res, error);
        }
      });

      app.delete('/api/config', async (req, res) => {
        try {
          const body = keySchema.parse(req.body);
          await execute(
            `DELETE FROM ${tableName(body.catalog, body.schema, 'jdbc_ingestion_config')} WHERE ingestion_group=:ingestion_group AND source_table_name=:source_table_name`,
            [parameter('ingestion_group', body.ingestionGroup), parameter('source_table_name', body.sourceTableName)]
          );
          res.json({ ok: true });
        } catch (error) {
          handleError(res, error);
        }
      });

      app.patch('/api/watermark', async (req, res) => {
        try {
          const body = watermarkSchema.parse(req.body);
          await execute(
            `UPDATE ${tableName(body.catalog, body.schema, 'jdbc_ingestion_watermark')} SET last_watermark=:last_watermark, status=:status, last_run_timestamp=current_timestamp(), last_error=NULL WHERE ingestion_group=:ingestion_group AND source_table_name=:source_table_name`,
            [
              parameter('last_watermark', body.lastWatermark),
              parameter('status', body.status),
              parameter('ingestion_group', body.ingestionGroup),
              parameter('source_table_name', body.sourceTableName),
            ]
          );
          res.json({ ok: true });
        } catch (error) {
          handleError(res, error);
        }
      });

      app.delete('/api/watermark', async (req, res) => {
        try {
          const body = keySchema.parse(req.body);
          await execute(
            `DELETE FROM ${tableName(body.catalog, body.schema, 'jdbc_ingestion_watermark')} WHERE ingestion_group=:ingestion_group AND source_table_name=:source_table_name`,
            [parameter('ingestion_group', body.ingestionGroup), parameter('source_table_name', body.sourceTableName)]
          );
          res.json({ ok: true });
        } catch (error) {
          handleError(res, error);
        }
      });

      app.get('/api/jobs', async (_req, res) => {
        try {
          const visibleJobs = [];
          for await (const job of workspace.jobs.list({ limit: 100, expand_tasks: false })) visibleJobs.push(job);
          const workspaceHost = requiredEnv('DATABRICKS_HOST').replace(/\/$/, '');
          const workspaceUrl = /^https?:\/\//i.test(workspaceHost) ? workspaceHost : `https://${workspaceHost}`;
          const result = [];
          for (let offset = 0; offset < visibleJobs.length; offset += 8) {
            const batch = visibleJobs.slice(offset, offset + 8);
            const summaries = await Promise.all(
              batch.map(async (job) => {
                const recentRuns = [];
                if (job.job_id) {
                  for await (const run of workspace.jobs.listRuns({
                    job_id: job.job_id,
                    limit: 10,
                    expand_tasks: false,
                  })) {
                    recentRuns.push({
                      run_id: run.run_id,
                      run_name: run.run_name,
                      start_time: run.start_time,
                      end_time: run.end_time,
                      setup_duration: run.setup_duration,
                      execution_duration: run.execution_duration,
                      cleanup_duration: run.cleanup_duration,
                      state: run.state,
                    });
                  }
                }
                return {
                  ...job,
                  workspace_url: job.job_id ? `${workspaceUrl}/jobs/${job.job_id}` : workspaceUrl,
                  runs: recentRuns,
                };
              })
            );
            result.push(...summaries);
          }
          res.json({ jobs: result });
        } catch (error) {
          handleError(res, error);
        }
      });

      app.post('/api/jobs', async (req, res) => {
        try {
          const body = jobPayloadSchema.parse(req.body);
          const name = `[${body.ingestionGroup}] Ingestion Pipeline`;
          const configurationFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_config`;
          const watermarkFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_watermark`;
          const tasks: jobs.Task[] = [
            {
              task_key: 'ingestion_configs',
              notebook_task: { notebook_path: plannerNotebookPath, source: 'GIT' },
            },
            {
              task_key: 'ingestion_worker',
              depends_on: [{ task_key: 'ingestion_configs' }],
              for_each_task: {
                inputs: '{{tasks.ingestion_configs.values.table_configs}}',
                concurrency: body.foreachConcurrency,
                task: {
                  task_key: 'ingestion_worker_iteration',
                  notebook_task: {
                    notebook_path: workerNotebookPath,
                    source: 'GIT',
                    base_parameters: { source_table_name: '{{input.source_table_name}}' },
                  },
                },
              },
            },
          ];
          if (body.cdcPipelineId) {
            tasks.push({
              task_key: 'cdc_scd2_pipeline',
              depends_on: [{ task_key: 'ingestion_worker' }],
              pipeline_task: { pipeline_id: body.cdcPipelineId, full_refresh: false },
            });
          }
          const settings: jobs.JobSettings = {
            name,
            max_concurrent_runs: 1,
            parameters: [
              { name: 'configuration_fqn', default: configurationFqn },
              { name: 'watermark_fqn', default: watermarkFqn },
              { name: 'ingestion_group', default: body.ingestionGroup },
            ],
            tasks,
            git_source: {
              git_url: body.gitUrl,
              git_provider: 'gitHub',
              git_branch: body.gitBranch,
            },
          };
          const existing = [];
          for await (const job of workspace.jobs.list({ name, limit: 25, expand_tasks: false })) {
            if (job.settings?.name === name) existing.push(job);
          }
          if (existing[0]?.job_id) {
            await workspace.jobs.reset({ job_id: existing[0].job_id, new_settings: settings });
            res.json({ jobId: existing[0].job_id, action: 'updated' });
          } else {
            const created = await workspace.jobs.create(settings as jobs.CreateJob);
            res.json({ jobId: created.job_id, action: 'created' });
          }
        } catch (error) {
          handleError(res, error);
        }
      });

      app.post('/api/jobs/:jobId/run', async (req, res) => {
        try {
          const jobId = z.coerce.number().int().positive().parse(req.params.jobId);
          const run = await workspace.jobs.runNow({ job_id: jobId });
          res.json({ runId: run.run_id });
        } catch (error) {
          handleError(res, error);
        }
      });
    });
  },
});
