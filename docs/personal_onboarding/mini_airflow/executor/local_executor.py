from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor

from mini_airflow.db.metadata_db import MetadataDB
from mini_airflow.executor.base import BaseExecutor
from mini_airflow.models.taskinstance import TaskInstance
from mini_airflow.sdk.task import BaseOperator
from mini_airflow.worker.worker import run_task_instance


class LocalExecutor(BaseExecutor):
    """Runs task instances concurrently in a thread pool. Stands in for real
    Airflow's LocalExecutor, which forks worker subprocesses rather than
    threads — threads are enough here since example task callables are
    plain Python, and it keeps this project dependency-free."""

    def __init__(self, parallelism: int = 4) -> None:
        self._pool = ThreadPoolExecutor(max_workers=parallelism)
        self._futures: dict[tuple[str, str, str], Future] = {}

    def submit(self, task: BaseOperator, ti: TaskInstance, db: MetadataDB) -> None:
        self._futures[ti.key] = self._pool.submit(run_task_instance, task, ti, db)

    def sync(self) -> None:
        for key in [k for k, f in self._futures.items() if f.done()]:
            future = self._futures.pop(key)
            future.result()  # re-raise anything that escaped run_task_instance itself

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)
