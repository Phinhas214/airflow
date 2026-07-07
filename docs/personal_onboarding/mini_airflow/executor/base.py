from __future__ import annotations

from mini_airflow.db.metadata_db import MetadataDB
from mini_airflow.models.taskinstance import TaskInstance
from mini_airflow.sdk.task import BaseOperator


class BaseExecutor:
    """Decides *how/where* a queued task instance actually runs. The
    Scheduler only decides *that* it should run — same split as real
    Airflow's `executors/` interface."""

    def submit(self, task: BaseOperator, ti: TaskInstance, db: MetadataDB) -> None:
        raise NotImplementedError

    def sync(self) -> None:
        """Reap finished work. Called once per scheduler loop tick."""

    def shutdown(self) -> None:
        pass
