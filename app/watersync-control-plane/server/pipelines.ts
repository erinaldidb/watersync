import { pipelines } from '@databricks/sdk-experimental';
import { execute, parameter, tableName, workspace } from './sql.js';
import { logger } from './logging.js';

export const plannerNotebookPath = 'notebooks/Task - Plan Configs';
export const workerNotebookPath = 'notebooks/Task - Run Ingestion';
export const selectiveRefreshNotebookPath = 'src/watersync/utils/selective_pipeline_refresh';
export const pipelineBootstrapPath = 'src/watersync/pipeline_bootstrap';

export const watersyncDependency = (gitUrl: string, gitBranch: string) =>
  `watersync@git+${gitUrl.replace(/\.git$/, '')}.git@${gitBranch}`;

export async function groupNeedsCdcPipeline(catalog: string, schema: string, ingestionGroup: string) {
  const response = await execute(
    `SELECT count_if(coalesce(enabled, true) AND (
       lower(ingestion_type) = 'incremental' OR coalesce(auto_cdc_from_snapshot, false)
     )) > 0
     FROM ${tableName(catalog, schema, 'jdbc_ingestion_config')}
     WHERE ingestion_group = :ingestion_group`,
    [parameter('ingestion_group', ingestionGroup)]
  );
  return response.result?.data_array?.[0]?.[0]?.toLowerCase() === 'true';
}

/**
 * Resolves the workspace-absolute path of the Git repo that contains the
 * watersync project.  Pipeline libraries require `/Workspace/…` paths
 * (they don’t support `source: 'GIT'` like job tasks).
 */
async function resolveRepoWorkspacePath(gitUrl: string): Promise<string> {
  const repoName = new URL(gitUrl).pathname.split('/').pop()?.replace(/\.git$/, '') ?? 'watersync';
  const me = await workspace.currentUser.me();
  return `/Workspace/Users/${me.userName}/${repoName}`;
}

export async function ensureCdcPipeline(opts: {
  pipelineId?: string | null;
  catalog: string;
  schema: string;
  ingestionGroup: string;
  gitUrl: string;
  gitBranch: string;
}) {
  if (opts.pipelineId) return opts.pipelineId;

  const pipelineName = `[${opts.ingestionGroup}] CDC SCD2 Pipeline`;
  const safeNamePrefix = `[${opts.ingestionGroup}] CDC SCD2`.replace(/'/g, "''");
  let existingPipelineId: string | undefined;
  for await (const p of workspace.pipelines.listPipelines({ filter: `name LIKE '${safeNamePrefix}%'` })) {
    if (p.name === pipelineName && p.pipeline_id) {
      existingPipelineId = p.pipeline_id;
      break;
    }
  }

  const repoRoot = await resolveRepoWorkspacePath(opts.gitUrl);
  const bootstrapPath = `${repoRoot}/${pipelineBootstrapPath}.py`;

  const configurationFqn = `${opts.catalog}.${opts.schema}.jdbc_ingestion_config`;
  const watermarkFqn = `${opts.catalog}.${opts.schema}.jdbc_ingestion_watermark`;
  const settings: pipelines.CreatePipeline = {
    name: pipelineName,
    catalog: opts.catalog,
    target: opts.schema,
    configuration: {
      'pipeline.configuration_fqn': configurationFqn,
      'pipeline.watermark_fqn': watermarkFqn,
      'pipeline.ingestion_group': opts.ingestionGroup,
    },
    libraries: [{ file: { path: bootstrapPath } }],
    environment: { dependencies: [watersyncDependency(opts.gitUrl, opts.gitBranch)] },
    serverless: true,
    channel: 'CURRENT',
  };

  if (existingPipelineId) {
    await workspace.pipelines.update({ pipeline_id: existingPipelineId, ...settings });
    logger.info('pipeline.updated', { pipelineId: existingPipelineId, pipelineName, ingestionGroup: opts.ingestionGroup });
    return existingPipelineId;
  }
  const created = await workspace.pipelines.create(settings);
  if (!created.pipeline_id) throw new Error('Databricks created the CDC pipeline without returning an ID');
  logger.info('pipeline.created', { pipelineId: created.pipeline_id, pipelineName, ingestionGroup: opts.ingestionGroup });
  return created.pipeline_id;
}
