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

"""Runs every example dag end-to-end and asserts the expected outcome.

Usage: `python3 run_examples.py` from inside docs/personal_onboarding/.

Doubles as a smoke test that exercises every component wired together:
sdk -> dagbag -> scheduler -> executor -> worker -> metadata db.
"""

from __future__ import annotations

from pathlib import Path

from mini_airflow.dagbag import DagBag
from mini_airflow.db.metadata_db import MetadataDB
from mini_airflow.executor.base import BaseExecutor
from mini_airflow.executor.local_executor import LocalExecutor
from mini_airflow.executor.sequential_executor import SequentialExecutor
from mini_airflow.models.dagrun import DagRun
from mini_airflow.models.state import DagRunState, TaskInstanceState
from mini_airflow.scheduler.scheduler import Scheduler

EXAMPLES_DIR = Path(__file__).parent / "mini_airflow" / "examples"


def run(dagfile: str, dag_id: str, executor: BaseExecutor) -> DagRun:
    bag = DagBag()
    bag.process_file(EXAMPLES_DIR / dagfile)
    dag = bag.get_dag(dag_id)

    db = MetadataDB()
    scheduler = Scheduler(db, executor)

    dag_run = scheduler.trigger_dag(dag)
    scheduler.run_to_completion(dag, dag_run)
    executor.shutdown()

    print(f"\n{dag_id} ({type(executor).__name__}): {dag_run.state.value}")
    for task_id, ti in dag_run.task_instances.items():
        print(f"  {task_id:<20} {ti.state.value:<16} try={ti.try_number}")
    return dag_run


def main() -> None:
    etl_run = run("example_dag.py", "example_etl", LocalExecutor())
    assert etl_run.state == DagRunState.SUCCESS
    assert etl_run.get_task_instance("load").state == TaskInstanceState.SUCCESS

    retry_run = run("example_retry_dag.py", "example_retry", SequentialExecutor())
    assert retry_run.get_task_instance("download").state == TaskInstanceState.SUCCESS
    assert retry_run.get_task_instance("download").try_number == 3
    assert retry_run.get_task_instance("broken").state == TaskInstanceState.FAILED
    assert retry_run.get_task_instance("cleanup").state == TaskInstanceState.SUCCESS
    assert retry_run.state == DagRunState.FAILED

    sensor_run = run("example_sensor_dag.py", "example_sensor", LocalExecutor())
    assert sensor_run.state == DagRunState.SUCCESS

    print("\nAll example dags behaved as expected.")


if __name__ == "__main__":
    main()
