from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from mini_airflow.models.state import DagRunState
from mini_airflow.models.taskinstance import TaskInstance


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DagRun:
    """One execution of a Dag. Compare to `models/dagrun.py` in real Airflow:
    same shape (a logical_date plus one TaskInstance per task), minus the
    scheduling metadata (data interval, run_type, etc.) that isn't needed to
    demonstrate the dependency-resolution loop."""

    dag_id: str
    run_id: str = field(default_factory=lambda: uuid4().hex[:8])
    logical_date: datetime = field(default_factory=_utcnow)
    state: DagRunState = DagRunState.RUNNING
    task_instances: dict[str, TaskInstance] = field(default_factory=dict)

    def get_task_instance(self, task_id: str) -> TaskInstance:
        return self.task_instances[task_id]
