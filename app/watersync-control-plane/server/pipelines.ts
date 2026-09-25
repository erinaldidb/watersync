import { pipelines } from '@databricks/sdk-experimental';
import { execute, parameter, tableName, workspace } from './sql.js';
import { logger } from './logging.js';

export const plannerNotebookPath = 'notebooks/Task - Plan Configs';
export const workerNotebookPath = 'notebooks/Task - Run Ingestion';

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

const pipelineBootstrap = `from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from watersync.cdc_pipeline import build_pipeline_from_spark_conf

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
build_pipeline_from_spark_conf(spark=spark, dp_module=dp)
`;

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

  const safeGroup = opts.ingestionGroup.replace(/[^A-Za-z0-9_-]/g, '_');
  const bootstrapDir = '/Shared/watersync-generated-pipelines';
  const bootstrapPath = `${bootstrapDir}/${safeGroup}_cdc_pipeline.py`;
  await workspace.workspace.mkdirs({ path: bootstrapDir });
  await workspace.workspace.import({
    path: bootstrapPath,
    format: 'RAW',
    overwrite: true,
    content: Buffer.from(pipelineBootstrap).toString('base64'),
  });

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
    libraries: [{ file: { path: `/Workspace${bootstrapPath}` } }],
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
