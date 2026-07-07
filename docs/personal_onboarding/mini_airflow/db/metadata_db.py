# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import threading
from typing import Any

from mini_airflow.models.dagrun import DagRun
from mini_airflow.models.state import TaskInstanceState
from mini_airflow.models.taskinstance import TaskInstance
from mini_airflow.sdk.dag import DAG


class MetadataDB:
    """The single source of truth every other component reads/writes through.

    In real Airflow, workers never touch the metadata DB directly — they go
    through the Execution API (`api_fastapi/execution_api/`), which is what
    lets a worker be untrusted and network-isolated from the database. This
    toy collapses that boundary (`worker.py` calls this class directly) to
    keep the line count down, but keeping it as its own module is deliberate:
    it marks exactly the seam where a real deployment draws a hard line.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._dag_runs: dict[tuple[str, str], DagRun] = {}
        self._xcom: dict[tuple[str, str, str], Any] = {}

    def create_dag_run(self, dag: DAG) -> DagRun:
        with self._lock:
            run = DagRun(dag_id=dag.dag_id)
            for task_id in dag.tasks:
                run.task_instances[task_id] = TaskInstance(
                    dag_id=dag.dag_id, task_id=task_id, run_id=run.run_id
                )
            self._dag_runs[(dag.dag_id, run.run_id)] = run
            return run

    def get_dag_run(self, dag_id: str, run_id: str) -> DagRun:
        with self._lock:
            return self._dag_runs[(dag_id, run_id)]

    def list_dag_runs(self, dag_id: str | None = None) -> list[DagRun]:
        with self._lock:
            runs = list(self._dag_runs.values())
        if dag_id is not None:
            runs = [r for r in runs if r.dag_id == dag_id]
        return runs

    def set_task_instance_state(self, ti: TaskInstance, state: TaskInstanceState) -> None:
        with self._lock:
            ti.state = state

    def xcom_push(self, dag_id: str, task_id: str, run_id: str, value: Any) -> None:
        with self._lock:
            self._xcom[(dag_id, task_id, run_id)] = value

    def xcom_pull(self, dag_id: str, task_id: str, run_id: str) -> Any:
        with self._lock:
            return self._xcom.get((dag_id, task_id, run_id))
