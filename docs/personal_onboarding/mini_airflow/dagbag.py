from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from mini_airflow.sdk.dag import DAG, DAG_REGISTRY


class DagBag:
    """Loads a Dag file in its own module namespace and collects whatever
    `DAG` objects got registered as a side effect of importing it. This is
    the one place user-authored code actually executes — mirroring
    `dag_processing/dagbag.py`, which real Airflow runs in an isolated
    subprocess specifically because it is executing arbitrary user code.
    """

    def __init__(self) -> None:
        self.dags: dict[str, DAG] = {}

    def process_file(self, filepath: str | Path) -> list[str]:
        path = Path(filepath).resolve()
        before = set(DAG_REGISTRY)

        module_name = f"mini_airflow_dagfile__{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load dag file {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        new_dag_ids = sorted(set(DAG_REGISTRY) - before)
        for dag_id in new_dag_ids:
            self.dags[dag_id] = DAG_REGISTRY[dag_id]
        return new_dag_ids

    def get_dag(self, dag_id: str) -> DAG:
        return self.dags[dag_id]
