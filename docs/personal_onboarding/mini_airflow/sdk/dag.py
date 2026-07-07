from __future__ import annotations

from datetime import datetime
from typing import Any

# Module-level singletons, mirroring how real Airflow's DagBag discovers Dag
# objects: user code declares a DAG (optionally as a context manager), and
# whatever gets constructed while a Dag file is imported is what the system
# sees. Real Airflow's `dag_processing/dagbag.py` does the equivalent scan
# after exec'ing the file.
_CONTEXT_STACK: list["DAG"] = []
DAG_REGISTRY: dict[str, "DAG"] = {}


class DAG:
    def __init__(
        self,
        dag_id: str,
        schedule: str | None = None,
        start_date: datetime | None = None,
    ) -> None:
        self.dag_id = dag_id
        self.schedule = schedule
        self.start_date = start_date
        self.tasks: dict[str, Any] = {}
        DAG_REGISTRY[dag_id] = self

    def add_task(self, task: Any) -> None:
        if task.task_id in self.tasks:
            raise ValueError(f"Task {task.task_id!r} already exists in dag {self.dag_id!r}")
        self.tasks[task.task_id] = task

    def get_task(self, task_id: str) -> Any:
        return self.tasks[task_id]

    def topological_order(self) -> list[str]:
        """Kahn's algorithm — the same shape as real Airflow's dependency
        traversal, just computed fresh each time instead of once at
        serialization time."""
        in_degree = {task_id: len(t.upstream_task_ids) for task_id, t in self.tasks.items()}
        ready = [task_id for task_id, degree in in_degree.items() if degree == 0]
        order: list[str] = []
        while ready:
            task_id = ready.pop(0)
            order.append(task_id)
            for downstream_id in self.tasks[task_id].downstream_task_ids:
                in_degree[downstream_id] -= 1
                if in_degree[downstream_id] == 0:
                    ready.append(downstream_id)
        if len(order) != len(self.tasks):
            raise ValueError(f"Dag {self.dag_id!r} has a cycle")
        return order

    def __enter__(self) -> "DAG":
        _CONTEXT_STACK.append(self)
        return self

    def __exit__(self, *exc_info: Any) -> None:
        _CONTEXT_STACK.pop()

    def __repr__(self) -> str:
        return f"<DAG: {self.dag_id}>"
