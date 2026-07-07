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

import logging
from datetime import datetime, timezone

from mini_airflow.db.metadata_db import MetadataDB
from mini_airflow.models.state import TaskInstanceState
from mini_airflow.models.taskinstance import TaskInstance
from mini_airflow.sdk.task import BaseOperator

logger = logging.getLogger("mini_airflow.worker")


class TaskInstanceContext:
    """The only handle a task callable gets on shared state — stands in for
    the real Execution API client (`RuntimeTaskInstance` / `comms.py` in
    `task-sdk/src/airflow/sdk/execution_time/`). A task can read/write XCom
    through here; it never sees the `MetadataDB` object itself."""

    def __init__(self, ti: TaskInstance, db: MetadataDB) -> None:
        self._ti = ti
        self._db = db

    def xcom_pull(self, task_id: str) -> object:
        return self._db.xcom_pull(self._ti.dag_id, task_id, self._ti.run_id)

    def xcom_push(self, value: object) -> None:
        self._db.xcom_push(self._ti.dag_id, self._ti.task_id, self._ti.run_id, value)


def run_task_instance(task: BaseOperator, ti: TaskInstance, db: MetadataDB) -> None:
    """Execute one task instance and update its state in place. Mirrors what
    `task-sdk`'s `task_runner.py` does inside a worker process: run the
    user's code, push its return value to XCom, and translate the outcome
    (success / retry / permanent failure) into a state transition."""

    ti.state = TaskInstanceState.RUNNING
    ti.try_number += 1
    ti.start_date = datetime.now(timezone.utc)
    context = {"ti": TaskInstanceContext(ti, db), "task_id": ti.task_id, "run_id": ti.run_id}

    try:
        result = task.execute(context)
        db.xcom_push(ti.dag_id, ti.task_id, ti.run_id, result)
        ti.state = TaskInstanceState.SUCCESS
        ti.error = None
    except Exception as exc:
        logger.warning("Task %s/%s failed on try %d: %s", ti.dag_id, ti.task_id, ti.try_number, exc)
        ti.error = str(exc)
        if ti.try_number <= task.retries:
            ti.state = TaskInstanceState.UP_FOR_RETRY
            ti.next_retry_at = datetime.now(timezone.utc) + task.retry_delay
        else:
            ti.state = TaskInstanceState.FAILED
    finally:
        ti.end_date = datetime.now(timezone.utc)
