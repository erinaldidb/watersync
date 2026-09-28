# Databricks notebook source
"""Selective pipeline refresh — runs as a Databricks job task.

Reads `cdc_pipeline_id` from a notebook widget and the
`full_refresh_targets` list published by the planner
(``ingestion_configs`` task) via ``dbutils.jobs.taskValues.get``.
When the target list is non-empty, calls
``pipelines.start_update(full_refresh_selection=...)`` and polls
until the update completes.  When the list is empty, exits immediately.
"""
from __future__ import annotations

import json
import time

from databricks.sdk import WorkspaceClient


def run_selective_refresh(pipeline_id: str, raw_targets: str) -> list[str]:
    """Trigger a selective full-refresh on *pipeline_id* for the given targets.

    Parameters
    ----------
    pipeline_id:
        The Databricks pipeline ID to refresh.
    raw_targets:
        A JSON-encoded array of fully-qualified target table names.  Empty
        strings are silently filtered out.

    Returns
    -------
    list[str]
        The list of tables that were actually refreshed (empty if none).
    """
    targets = [t for t in json.loads(raw_targets) if t.strip()] if raw_targets else []

    if not targets:
        print("No tables require selective full refresh.")
        return []

    print(f"Triggering selective pipeline refresh for {len(targets)} table(s): {targets}")

    w = WorkspaceClient()
    response = w.pipelines.start_update(
        pipeline_id=pipeline_id,
        full_refresh_selection=targets,
    )
    update_id = response.update_id
    print(f"Started pipeline update: {update_id}")

    while True:
        info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
        state = (
            str(info.update.state.value)
            if info.update and info.update.state
            else "UNKNOWN"
        )
        print(f"  Pipeline update state: {state}")
        if state == "COMPLETED":
            break
        if state in ("FAILED", "CANCELED"):
            raise RuntimeError(f"Pipeline update {update_id} ended with state {state}")
        time.sleep(15)

    print(f"Selective pipeline refresh completed for: {targets}")
    return targets


# -- notebook entry-point (runs when executed as a Databricks notebook task) --
try:
    _pipeline_id = dbutils.widgets.get("cdc_pipeline_id").strip()  # type: ignore[name-defined]

    # Read the full_refresh_targets list set by the planner (ingestion_configs task).
    # The planner detects all FULL_REFRESH watermark statuses in one query.
    try:
        _targets_list = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]
            taskKey="ingestion_configs",
            key="full_refresh_targets",
            default=[],
            debugValue=[],
        )
        if isinstance(_targets_list, str):
            _targets_list = json.loads(_targets_list)
        if not isinstance(_targets_list, list):
            _targets_list = []
    except Exception:
        _targets_list = []

    _raw = json.dumps(_targets_list)
    _refreshed = run_selective_refresh(_pipeline_id, _raw)
    dbutils.notebook.exit(json.dumps({"refreshed": _refreshed}))  # type: ignore[name-defined]
except NameError:
    pass  # Not running as a notebook — importable as a library
