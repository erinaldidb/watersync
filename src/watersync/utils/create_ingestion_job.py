from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as pl
from databricks.sdk.service.compute import AutoScale, ClusterSpec, DataSecurityMode, Environment, Library
from databricks.sdk.service.jobs import (
    ForEachTask,
    JobCluster,
    JobEnvironment,
    JobParameterDefinition,
    JobSettings,
    NotebookTask,
    PipelineTask,
    Source,
    Task,
    TaskDependency,
)

from watersync.models import JobProvisioningSettings


class IngestionJobProvisioner:
    def __init__(self, workspace_client: WorkspaceClient | None = None):
        self.w = workspace_client or WorkspaceClient()

    def ensure_pipeline(self, settings: JobProvisioningSettings) -> str:
        if settings.cdc_pipeline_id:
            return settings.cdc_pipeline_id
        if not settings.cdc_pipeline_file_path:
            raise ValueError("cdc_pipeline_file_path is required when cdc_pipeline_id is not provided")

        pipeline_name = f"[{settings.ingestion_group}] CDC SCD2 Pipeline"
        safe_prefix = f"[{settings.ingestion_group}] CDC SCD2".replace("'", "''")
        existing = [
            pipeline
            for pipeline in self.w.pipelines.list_pipelines(filter=f"name LIKE '{safe_prefix}%'")
            if pipeline.name == pipeline_name
        ]
        if existing:
            return existing[0].pipeline_id

        created = self.w.pipelines.create(
            name=pipeline_name,
            catalog=settings.catalog,
            target=settings.schema,
            configuration={
                "pipeline.catalog": settings.catalog,
                "pipeline.schema": settings.schema,
                "pipeline.ingestion_group": settings.ingestion_group,
            },
            libraries=[pl.PipelineLibrary(file=pl.FileLibrary(path=settings.cdc_pipeline_file_path))],
            environment=pl.PipelinesEnvironment(dependencies=[settings.wheel_uri]),
            serverless=True,
            channel="CURRENT",
        )
        return created.pipeline_id

    def build_job_settings(self, settings: JobProvisioningSettings, pipeline_id: str) -> JobSettings:
        env_key = "Default"
        environments = [
            JobEnvironment(
                environment_key=env_key,
                spec=Environment(client="1", dependencies=[settings.wheel_uri]),
            )
        ]

        planner_task = Task(
            task_key="ingestion_configs",
            description="Build one For each item per enabled source table.",
            notebook_task=NotebookTask(
                notebook_path=settings.planner_notebook_path,
                source=Source.WORKSPACE,
            ),
            environment_key=env_key,
        )

        worker_iteration = Task(
            task_key="ingestion_worker_iteration",
            notebook_task=NotebookTask(
                notebook_path=settings.worker_notebook_path,
                base_parameters={
                    "source_table_name": "{{input.source_table_name}}",
                },
                source=Source.WORKSPACE,
            ),
            environment_key=env_key,
        )

        ingestion_worker = Task(
            task_key="ingestion_worker",
            description="Fan-out JDBC ingestion worker.",
            depends_on=[TaskDependency(task_key="ingestion_configs")],
            for_each_task=ForEachTask(
                inputs="{{tasks.ingestion_configs.values.table_configs}}",
                concurrency=settings.foreach_concurrency,
                task=worker_iteration,
            ),
        )

        cdc_task = Task(
            task_key="cdc_scd2_pipeline",
            description="Run the Lakeflow Spark Declarative Pipeline SCD2 step.",
            depends_on=[TaskDependency(task_key="ingestion_worker")],
            pipeline_task=PipelineTask(pipeline_id=pipeline_id, full_refresh=False),
        )

        return JobSettings(
            name=f"[{settings.ingestion_group}] Ingestion Pipeline",
            parameters=[
                JobParameterDefinition(name="ingestion_group", default=settings.ingestion_group),
                JobParameterDefinition(name="catalog", default=settings.catalog),
                JobParameterDefinition(name="schema", default=settings.schema),
                JobParameterDefinition(name="jdbc_url", default=settings.jdbc_url),
                JobParameterDefinition(name="jdbc_user", default=settings.jdbc_user),
                JobParameterDefinition(name="jdbc_secret_scope", default=settings.jdbc_secret_scope),
                JobParameterDefinition(name="jdbc_secret_key", default=settings.jdbc_secret_key),
                JobParameterDefinition(name="watermark_threshold_minutes", default=settings.watermark_threshold_minutes),
                JobParameterDefinition(name="fetch_size", default=settings.fetch_size),
                JobParameterDefinition(name="num_partitions", default=settings.num_partitions),
            ],
            tasks=[planner_task, ingestion_worker, cdc_task],
            max_concurrent_runs=1,
            environments=environments,
        )

    def create_or_update_job(self, settings: JobProvisioningSettings) -> dict[str, str | int]:
        pipeline_id = self.ensure_pipeline(settings)
        job_settings = self.build_job_settings(settings, pipeline_id)
        existing_jobs = [job for job in self.w.jobs.list(name=job_settings.name) if job.settings and job.settings.name == job_settings.name]
        if existing_jobs:
            job_id = existing_jobs[0].job_id
            self.w.jobs.reset(job_id=job_id, new_settings=job_settings)
        else:
            result = self.w.jobs.create(
                name=job_settings.name,
                parameters=job_settings.parameters,
                tasks=job_settings.tasks,
                max_concurrent_runs=job_settings.max_concurrent_runs,
                environments=job_settings.environments,
            )
            job_id = result.job_id
        return {
            "job_id": job_id,
            "pipeline_id": pipeline_id,
            "job_name": job_settings.name,
        }
