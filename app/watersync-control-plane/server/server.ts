import { analytics, createApp, server } from '@databricks/appkit';
import { WorkspaceClient, jobs, pipelines, sql as dbsql } from '@databricks/sdk-experimental';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';

const identifierPart = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]*$/);
const locationSchema = z.object({ catalog: identifierPart, schema: identifierPart });
const configSchema = locationSchema
  .extend({
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
  })
  .superRefine((value, context) => {
    if (value.ingestionType !== 'incremental') return;
    if (!value.keyColumns?.trim()) {
      context.addIssue({
        code: 'custom',
        path: ['keyColumns'],
        message: 'Incremental ingestion requires a key column',
      });
    }
    if (!value.epicCsaEnabled && !value.watermarkColumn?.trim()) {
      context.addIssue({
        code: 'custom',
        path: ['watermarkColumn'],
        message: 'Incremental ingestion requires a watermark column unless EPIC CSA is enabled',
      });
    }
  });
const sourceDatabaseType = z.enum(['postgresql', 'mysql', 'sqlserver', 'oracle']);
const sourceDiscoverySchema = z
  .object({
    connectionName: z.string().trim().optional().default(''),
    jdbcUrl: z.string().trim().optional().default(''),
    jdbcUser: z.string().trim().optional().default(''),
    jdbcSecretScope: z.string().trim().optional().default(''),
    jdbcSecretKey: z.string().trim().optional().default(''),
    database: z.string().trim().optional().default(''),
    databaseType: sourceDatabaseType,
  })
  .superRefine((value, context) => {
    if (value.connectionName) return;
    for (const field of ['database', 'jdbcUrl', 'jdbcUser', 'jdbcSecretScope', 'jdbcSecretKey'] as const) {
      if (!value[field]) {
        context.addIssue({ code: 'custom', path: [field], message: 'Required for direct JDBC discovery' });
      }
    }
    if (/(?:password|pwd)\s*=/i.test(value.jdbcUrl)) {
      context.addIssue({
        code: 'custom',
        path: ['jdbcUrl'],
        message: 'Do not put passwords in the JDBC URL; use a Databricks secret scope and key',
      });
    }
  });
const sourceColumnsSchema = sourceDiscoverySchema.extend({
  sourceSchema: z.string().trim().min(1),
  table: z.string().trim().min(1),
});
const sourceColumnsBatchSchema = sourceDiscoverySchema.extend({
  tables: z
    .array(z.object({ sourceSchema: z.string().trim().min(1), table: z.string().trim().min(1) }))
    .min(1)
    .max(10),
});
const keySchema = locationSchema.extend({ ingestionGroup: z.string().min(1), sourceTableName: z.string().min(1) });
const watermarkSchema = keySchema.extend({ lastWatermark: z.string().nullable(), status: z.string().min(1) });
const cronExpression = z
  .string()
  .trim()
  .refine((value) => value.split(/\s+/).length >= 6, 'Use a Quartz cron expression with at least 6 fields');
const scheduleSchema = z.object({
  enabled: z.boolean(),
  quartzCronExpression: cronExpression,
  timezoneId: z.string().trim().min(1),
  pauseStatus: z.enum(['PAUSED', 'UNPAUSED']),
});
const computeSchema = z
  .object({
    mode: z.enum(['SERVERLESS', 'JOB_CLUSTER']),
    performanceTarget: z.enum(['STANDARD', 'PERFORMANCE_OPTIMIZED']),
    sparkVersion: z.string().trim(),
    driverNodeTypeId: z.string().trim(),
    workerNodeTypeId: z.string().trim(),
    minWorkers: z.number().int().min(0),
    maxWorkers: z.number().int().min(1),
  })
  .superRefine((value, context) => {
    if (value.mode !== 'JOB_CLUSTER') return;
    for (const [field, fieldValue] of [
      ['sparkVersion', value.sparkVersion],
      ['driverNodeTypeId', value.driverNodeTypeId],
      ['workerNodeTypeId', value.workerNodeTypeId],
    ] as const) {
      if (!fieldValue) context.addIssue({ code: 'custom', path: [field], message: 'Required for a job cluster' });
    }
    if (value.maxWorkers < value.minWorkers) {
      context.addIssue({
        code: 'custom',
        path: ['maxWorkers'],
        message: 'Maximum workers must be at least the minimum',
      });
    }
  });
const jobPayloadSchema = locationSchema.extend({
  ingestionGroup: z.string().min(1),
  gitUrl: z.url().refine((value) => new URL(value).hostname === 'github.com', 'Repository must be hosted on GitHub'),
  gitBranch: z.string().trim().min(1),
  foreachConcurrency: z.number().int().min(1).max(100),
  cdcPipelineId: z.string().trim().nullable().optional(),
  schedule: scheduleSchema,
  compute: computeSchema,
});
const jobSchedulePayloadSchema = scheduleSchema.extend({ jobId: z.number().int().positive() });

const plannerNotebookPath = 'notebooks/Task - Plan Configs';
const workerNotebookPath = 'notebooks/Task - Run Ingestion';

const watersyncDependency = (gitUrl: string, gitBranch: string) =>
  `watersync@git+${gitUrl.replace(/\.git$/, '')}.git@${gitBranch}`;

const workspace = new WorkspaceClient({});
const requiredEnv = (name: string) => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
};
const workspaceBaseUrl = () => {
  const host = requiredEnv('DATABRICKS_HOST').replace(/\/$/, '');
  return /^https?:\/\//i.test(host) ? host : `https://${host}`;
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

const sqlLiteral = (value: string) => `'${value.replace(/'/g, "''")}'`;
const responseRows = (response: Awaited<ReturnType<typeof execute>>) => {
  const names = response.manifest?.schema?.columns?.map((column) => column.name ?? '') ?? [];
  return (response.result?.data_array ?? []).map((values) =>
    Object.fromEntries(names.map((name, index) => [name, values[index] ?? null]))
  );
};
const remoteQuery = async (
  connectionName: string,
  database: string | undefined,
  query: string,
  databaseType: z.infer<typeof sourceDatabaseType>
) => {
  const parameters = [parameter('connection_name', connectionName), parameter('remote_sql', query)];
  if (!database) {
    return execute(`SELECT * FROM remote_query(:connection_name, query => :remote_sql)`, parameters);
  }
  const databaseOption = databaseType === 'oracle' ? 'service_name' : 'database';
  return execute(`SELECT * FROM remote_query(:connection_name, ${databaseOption} => :database, query => :remote_sql)`, [
    ...parameters,
    parameter('database', database),
  ]);
};

type SourceDiscovery = z.infer<typeof sourceDiscoverySchema>;
const jdbcEndpoint = (databaseType: SourceDiscovery['databaseType'], jdbcUrl: string) => {
  const expectedPrefix = `jdbc:${databaseType === 'sqlserver' ? 'sqlserver' : databaseType}:`;
  if (!jdbcUrl.toLowerCase().startsWith(expectedPrefix)) {
    throw new Error(`JDBC URL must start with ${expectedPrefix}`);
  }
  if (databaseType === 'sqlserver') {
    const match = /^jdbc:sqlserver:\/\/([^:;]+)(?::(\d+))?(?:;.*)?$/i.exec(jdbcUrl);
    if (!match) throw new Error('Use a SQL Server JDBC URL such as jdbc:sqlserver://host:1433;databaseName=mydb');
    return { host: match[1], port: match[2] ?? '1433' };
  }
  if (databaseType === 'oracle') {
    const serviceMatch = /^jdbc:oracle:thin:@\/\/([^:/]+)(?::(\d+))?\/[^/?;]+(?:[?;].*)?$/i.exec(jdbcUrl);
    const sidMatch = /^jdbc:oracle:thin:@([^:/]+)(?::(\d+))?:[^:;/?]+(?:[?;].*)?$/i.exec(jdbcUrl);
    const match = serviceMatch ?? sidMatch;
    if (!match) {
      throw new Error('Use an Oracle JDBC URL such as jdbc:oracle:thin:@//host:1521/service_name');
    }
    return { host: match[1], port: match[2] ?? '1521' };
  }
  const match = /^jdbc:(?:postgresql|mysql):\/\/([^/:?;]+)(?::(\d+))?\/[^?;]+(?:[?;].*)?$/i.exec(jdbcUrl);
  if (!match) throw new Error(`Use a ${databaseType} JDBC URL containing a host and database`);
  return { host: match[1], port: match[2] ?? (databaseType === 'postgresql' ? '5432' : '3306') };
};

async function withDiscoveryConnection<T>(body: SourceDiscovery, action: (connectionName: string) => Promise<T>) {
  if (body.connectionName) return action(body.connectionName);

  const endpoint = jdbcEndpoint(body.databaseType, body.jdbcUrl);
  const connectionName = `_watersync_discovery_${randomUUID().replace(/-/g, '')}`;
  const connectionType = body.databaseType === 'sqlserver' ? 'SQLSERVER' : body.databaseType.toUpperCase();
  await execute(`CREATE CONNECTION \`${connectionName}\` TYPE ${connectionType} OPTIONS (
    host ${sqlLiteral(endpoint.host)},
    port ${sqlLiteral(endpoint.port)},
    user ${sqlLiteral(body.jdbcUser)},
    password secret(${sqlLiteral(body.jdbcSecretScope)}, ${sqlLiteral(body.jdbcSecretKey)})
  )`);
  try {
    return await action(connectionName);
  } finally {
    try {
      await execute(`DROP CONNECTION IF EXISTS \`${connectionName}\``);
    } catch (error) {
      console.error(`Failed to remove temporary discovery connection ${connectionName}`, error);
    }
  }
}

const listTablesSql = (databaseType: z.infer<typeof sourceDatabaseType>) => {
  if (databaseType === 'oracle') {
    return `SELECT OWNER AS table_schema, TABLE_NAME AS table_name
      FROM ALL_TABLES
      ORDER BY OWNER, TABLE_NAME
      FETCH FIRST 1000 ROWS ONLY`;
  }
  const base = `SELECT ${databaseType === 'sqlserver' ? 'TOP 1000 ' : ''}TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE IN ('BASE TABLE', 'TABLE')
    ORDER BY TABLE_SCHEMA, TABLE_NAME`;
  return databaseType === 'sqlserver' ? base : `${base} LIMIT 1000`;
};

const columnsSql = (databaseType: z.infer<typeof sourceDatabaseType>, sourceSchema: string, table: string) => {
  const schema = sqlLiteral(sourceSchema);
  const tableNameValue = sqlLiteral(table);
  if (databaseType === 'oracle') {
    return `SELECT c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type,
        CASE c.NULLABLE WHEN 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
        c.COLUMN_ID AS ordinal_position,
        CASE WHEN EXISTS (
          SELECT 1
          FROM ALL_CONSTRAINTS tc
          JOIN ALL_CONS_COLUMNS k ON tc.OWNER = k.OWNER AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
          WHERE tc.CONSTRAINT_TYPE = 'P' AND tc.OWNER = c.OWNER
            AND tc.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME
        ) THEN 1 ELSE 0 END AS is_primary_key
      FROM ALL_TAB_COLUMNS c
      WHERE c.OWNER = UPPER(${schema}) AND c.TABLE_NAME = UPPER(${tableNameValue})
      ORDER BY c.COLUMN_ID`;
  }
  const concat =
    databaseType === 'mysql'
      ? 'GROUP_CONCAT(k.COLUMN_NAME ORDER BY k.ORDINAL_POSITION)'
      : databaseType === 'sqlserver'
        ? "STRING_AGG(CAST(k.COLUMN_NAME AS VARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY k.ORDINAL_POSITION)"
        : "STRING_AGG(k.COLUMN_NAME, ',' ORDER BY k.ORDINAL_POSITION)";
  return `SELECT c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type,
      c.IS_NULLABLE AS is_nullable, c.ORDINAL_POSITION AS ordinal_position,
      CASE WHEN pk.primary_keys IS NOT NULL AND CONCAT(',', pk.primary_keys, ',') LIKE CONCAT('%,', c.COLUMN_NAME, ',%')
        THEN 1 ELSE 0 END AS is_primary_key
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN (
      SELECT ${concat} AS primary_keys
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA AND tc.TABLE_NAME = k.TABLE_NAME
      WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        AND tc.TABLE_SCHEMA = ${schema} AND tc.TABLE_NAME = ${tableNameValue}
    ) pk ON 1 = 1
    WHERE c.TABLE_SCHEMA = ${schema} AND c.TABLE_NAME = ${tableNameValue}
    ORDER BY c.ORDINAL_POSITION`;
};

async function groupHasEnabledIncrementalSource(catalog: string, schema: string, ingestionGroup: string) {
  const response = await execute(
    `SELECT count_if(coalesce(enabled, true) AND lower(ingestion_type) = 'incremental') > 0
     FROM ${tableName(catalog, schema, 'jdbc_ingestion_config')}
     WHERE ingestion_group = :ingestion_group`,
    [parameter('ingestion_group', ingestionGroup)]
  );
  return response.result?.data_array?.[0]?.[0]?.toLowerCase() === 'true';
}

const pipelineBootstrap = `from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from watersync.cdc_pipeline import build_pipeline_from_spark_conf

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
build_pipeline_from_spark_conf(spark=spark, dp_module=dp)
`;

async function ensureCdcPipeline({
  pipelineId,
  catalog,
  schema,
  ingestionGroup,
  gitUrl,
  gitBranch,
}: {
  pipelineId?: string | null;
  catalog: string;
  schema: string;
  ingestionGroup: string;
  gitUrl: string;
  gitBranch: string;
}) {
  if (pipelineId) return pipelineId;

  const pipelineName = `[${ingestionGroup}] CDC SCD2 Pipeline`;
  const safeNamePrefix = `[${ingestionGroup}] CDC SCD2`.replace(/'/g, "''");
  let existingPipelineId: string | undefined;
  for await (const pipeline of workspace.pipelines.listPipelines({ filter: `name LIKE '${safeNamePrefix}%'` })) {
    if (pipeline.name === pipelineName && pipeline.pipeline_id) {
      existingPipelineId = pipeline.pipeline_id;
      break;
    }
  }

  const safeGroup = ingestionGroup.replace(/[^A-Za-z0-9_-]/g, '_');
  const bootstrapDirectory = '/Shared/watersync-generated-pipelines';
  const bootstrapWorkspacePath = `${bootstrapDirectory}/${safeGroup}_cdc_pipeline.py`;
  await workspace.workspace.mkdirs({ path: bootstrapDirectory });
  await workspace.workspace.import({
    path: bootstrapWorkspacePath,
    format: 'RAW',
    overwrite: true,
    content: Buffer.from(pipelineBootstrap).toString('base64'),
  });

  const configurationFqn = `${catalog}.${schema}.jdbc_ingestion_config`;
  const watermarkFqn = `${catalog}.${schema}.jdbc_ingestion_watermark`;
  const pipelineSettings: pipelines.CreatePipeline = {
    name: pipelineName,
    catalog,
    target: schema,
    configuration: {
      'pipeline.configuration_fqn': configurationFqn,
      'pipeline.watermark_fqn': watermarkFqn,
      'pipeline.ingestion_group': ingestionGroup,
    },
    libraries: [{ file: { path: `/Workspace${bootstrapWorkspacePath}` } }],
    environment: { dependencies: [watersyncDependency(gitUrl, gitBranch)] },
    serverless: true,
    channel: 'CURRENT',
  };

  if (existingPipelineId) {
    await workspace.pipelines.update({ pipeline_id: existingPipelineId, ...pipelineSettings });
    return existingPipelineId;
  }
  const created = await workspace.pipelines.create(pipelineSettings);
  if (!created.pipeline_id) throw new Error('Databricks created the CDC pipeline without returning an ID');
  return created.pipeline_id;
}

const handleError = (res: { status(code: number): { json(value: unknown): void } }, error: unknown) => {
  const message = error instanceof Error ? error.message : 'Unexpected error';
  res.status(error instanceof z.ZodError ? 400 : 500).json({ error: message });
};

await createApp({
  plugins: [analytics(), server()],
  onPluginsReady(appkit) {
    appkit.server.extend((app) => {
      app.post('/api/source-tables', async (req, res) => {
        try {
          const body = sourceDiscoverySchema.parse(req.body);
          const response = await withDiscoveryConnection(body, (connectionName) =>
            remoteQuery(
              connectionName,
              body.connectionName ? undefined : body.database,
              listTablesSql(body.databaseType),
              body.databaseType
            )
          );
          res.json({ tables: responseRows(response) });
        } catch (error) {
          handleError(res, error);
        }
      });

      app.post('/api/source-columns', async (req, res) => {
        try {
          const body = sourceColumnsSchema.parse(req.body);
          const response = await withDiscoveryConnection(body, (connectionName) =>
            remoteQuery(
              connectionName,
              body.connectionName ? undefined : body.database,
              columnsSql(body.databaseType, body.sourceSchema, body.table),
              body.databaseType
            )
          );
          res.json({ columns: responseRows(response) });
        } catch (error) {
          handleError(res, error);
        }
      });

      app.post('/api/source-columns-batch', async (req, res) => {
        try {
          const body = sourceColumnsBatchSchema.parse(req.body);
          const tables = await withDiscoveryConnection(body, async (connectionName) => {
            const results = [];
            for (let offset = 0; offset < body.tables.length; offset += 4) {
              const batch = body.tables.slice(offset, offset + 4);
              const batchResults = await Promise.all(
                batch.map(async (table) => {
                  const response = await remoteQuery(
                    connectionName,
                    body.connectionName ? undefined : body.database,
                    columnsSql(body.databaseType, table.sourceSchema, table.table),
                    body.databaseType
                  );
                  return { ...table, columns: responseRows(response) };
                })
              );
              results.push(...batchResults);
            }
            return results;
          });
          res.json({ tables });
        } catch (error) {
          handleError(res, error);
        }
      });

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
          const workspaceUrl = workspaceBaseUrl();
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
                      run_url:
                        job.job_id && run.run_id
                          ? `${workspaceBaseUrl()}/jobs/${job.job_id}/runs/${run.run_id}`
                          : undefined,
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
          const hasIncrementalSources = await groupHasEnabledIncrementalSource(
            body.catalog,
            body.schema,
            body.ingestionGroup
          );
          const cdcPipelineId = hasIncrementalSources
            ? await ensureCdcPipeline({
                pipelineId: body.cdcPipelineId,
                catalog: body.catalog,
                schema: body.schema,
                ingestionGroup: body.ingestionGroup,
                gitUrl: body.gitUrl,
                gitBranch: body.gitBranch,
              })
            : undefined;
          const name = `[${body.ingestionGroup}] Ingestion Pipeline`;
          const configurationFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_config`;
          const watermarkFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_watermark`;
          const environmentKey = 'watersync_environment';
          const jobClusterKey = 'watersync_cluster';
          const dependency = watersyncDependency(body.gitUrl, body.gitBranch);
          const taskCompute =
            body.compute.mode === 'SERVERLESS'
              ? { environment_key: environmentKey }
              : {
                  job_cluster_key: jobClusterKey,
                  libraries: [{ pypi: { package: dependency } }],
                };
          const tasks: jobs.Task[] = [
            {
              task_key: 'ingestion_configs',
              notebook_task: { notebook_path: plannerNotebookPath, source: 'GIT' },
              ...taskCompute,
            },
            {
              task_key: 'ingestion_worker',
              depends_on: [{ task_key: 'ingestion_configs' }],
              for_each_task: {
                inputs: '{{tasks.ingestion_configs.values.table_configs}}',
                concurrency: body.foreachConcurrency,
                task: {
                  task_key: 'ingestion_worker_iteration',
                  ...taskCompute,
                  notebook_task: {
                    notebook_path: workerNotebookPath,
                    source: 'GIT',
                    base_parameters: { source_table_name: '{{input.source_table_name}}' },
                  },
                },
              },
            },
          ];
          if (cdcPipelineId) {
            tasks.push({
              task_key: 'cdc_scd2_pipeline',
              depends_on: [{ task_key: 'ingestion_worker' }],
              pipeline_task: { pipeline_id: cdcPipelineId, full_refresh: false },
            });
          }
          const settings: jobs.JobSettings = {
            name,
            max_concurrent_runs: 1,
            ...(body.compute.mode === 'SERVERLESS'
              ? {
                  performance_target: body.compute.performanceTarget,
                  environments: [
                    {
                      environment_key: environmentKey,
                      spec: { environment_version: '4', dependencies: [dependency] },
                    },
                  ],
                }
              : {
                  job_clusters: [
                    {
                      job_cluster_key: jobClusterKey,
                      new_cluster: {
                        spark_version: body.compute.sparkVersion,
                        driver_node_type_id: body.compute.driverNodeTypeId,
                        node_type_id: body.compute.workerNodeTypeId,
                        autoscale: {
                          min_workers: body.compute.minWorkers,
                          max_workers: body.compute.maxWorkers,
                        },
                      },
                    },
                  ],
                }),
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
            ...(body.schedule.enabled
              ? {
                  schedule: {
                    quartz_cron_expression: body.schedule.quartzCronExpression,
                    timezone_id: body.schedule.timezoneId,
                    pause_status: body.schedule.pauseStatus,
                  },
                }
              : {}),
          };
          const existing = [];
          for await (const job of workspace.jobs.list({ name, limit: 25, expand_tasks: false })) {
            if (job.settings?.name === name) existing.push(job);
          }
          if (existing[0]?.job_id) {
            await workspace.jobs.reset({ job_id: existing[0].job_id, new_settings: settings });
            res.json({ jobId: existing[0].job_id, pipelineId: cdcPipelineId, action: 'updated' });
          } else {
            const created = await workspace.jobs.create(settings as jobs.CreateJob);
            res.json({ jobId: created.job_id, pipelineId: cdcPipelineId, action: 'created' });
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

      app.patch('/api/jobs/:jobId/schedule', async (req, res) => {
        try {
          const body = jobSchedulePayloadSchema.parse({
            ...req.body,
            jobId: z.coerce.number().parse(req.params.jobId),
          });
          if (body.enabled) {
            await workspace.jobs.update({
              job_id: body.jobId,
              new_settings: {
                schedule: {
                  quartz_cron_expression: body.quartzCronExpression,
                  timezone_id: body.timezoneId,
                  pause_status: body.pauseStatus,
                },
              },
            });
          } else {
            await workspace.jobs.update({ job_id: body.jobId, fields_to_remove: ['schedule'] });
          }
          res.json({ ok: true });
        } catch (error) {
          handleError(res, error);
        }
      });
    });
  },
});
