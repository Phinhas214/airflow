from __future__ import annotations

from mini_airflow.db.metadata_db import MetadataDB
from mini_airflow.executor.base import BaseExecutor
from mini_airflow.models.taskinstance import TaskInstance
from mini_airflow.sdk.task import BaseOperator
from mini_airflow.worker.worker import run_task_instance


class SequentialExecutor(BaseExecutor):
    """Runs one task at a time, synchronously, in the caller's thread — the
    same role as real Airflow's SequentialExecutor: no parallelism, useful
    when deterministic ordering matters more than speed (e.g. SQLite-backed
    local testing)."""

    def submit(self, task: BaseOperator, ti: TaskInstance, db: MetadataDB) -> None:
        run_task_instance(task, ti, db)
