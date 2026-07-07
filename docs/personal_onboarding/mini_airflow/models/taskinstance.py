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

from dataclasses import dataclass
from datetime import datetime

from mini_airflow.models.state import TaskInstanceState


@dataclass
class TaskInstance:
    """One task, in one dag run. Compare to `airflow-core/src/airflow/models/
    taskinstance.py` — same identity (dag_id, task_id, run_id), same state
    machine, same retry bookkeeping, minus persistence and the composite key
    also including `map_index`."""

    dag_id: str
    task_id: str
    run_id: str
    state: TaskInstanceState = TaskInstanceState.NONE
    try_number: int = 0
    start_date: datetime | None = None
    end_date: datetime | None = None
    next_retry_at: datetime | None = None
    error: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.dag_id, self.task_id, self.run_id)

    def is_ready_for_retry(self, now: datetime) -> bool:
        if self.state != TaskInstanceState.UP_FOR_RETRY:
            return False
        return self.next_retry_at is None or now >= self.next_retry_at
