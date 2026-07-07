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
import time
from datetime import datetime, timezone

from mini_airflow.db.metadata_db import MetadataDB
from mini_airflow.executor.base import BaseExecutor
from mini_airflow.models.dagrun import DagRun
from mini_airflow.models.state import DagRunState, TaskInstanceState
from mini_airflow.models.taskinstance import TaskInstance
from mini_airflow.sdk.dag import DAG

logger = logging.getLogger("mini_airflow.scheduler")

_UPSTREAM_FAILURE_STATES = (TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED)


class Scheduler:
    """Owns the dependency graph: decides which task instances are ready to
    run and hands them to the executor. Mirrors `jobs/scheduler_job_runner.py`
    minus persistence across restarts and minus handling many concurrent
    dag runs — one `run_to_completion()` call is one Dag run's lifecycle.
    """

    def __init__(self, db: MetadataDB, executor: BaseExecutor) -> None:
        self.db = db
        self.executor = executor

    def trigger_dag(self, dag: DAG) -> DagRun:
        run = self.db.create_dag_run(dag)
        logger.info("Triggered %s run_id=%s", dag.dag_id, run.run_id)
        return run

    def run_to_completion(self, dag: DAG, run: DagRun, poll_interval: float = 0.02) -> DagRun:
        while not self._is_dag_run_finished(run):
            self._propagate_upstream_failures(dag, run)
            for ti in self._get_ready_task_instances(dag, run):
                ti.state = TaskInstanceState.QUEUED
                self.executor.submit(dag.get_task(ti.task_id), ti, self.db)

            self.executor.sync()
            if not self._is_dag_run_finished(run):
                time.sleep(poll_interval)

        run.state = (
            DagRunState.SUCCESS
            if all(ti.state == TaskInstanceState.SUCCESS for ti in run.task_instances.values())
            else DagRunState.FAILED
        )
        logger.info("Dag run %s/%s finished: %s", dag.dag_id, run.run_id, run.state)
        return run

    def _get_ready_task_instances(self, dag: DAG, run: DagRun) -> list[TaskInstance]:
        now = datetime.now(timezone.utc)
        ready = []
        for ti in run.task_instances.values():
            if ti.state == TaskInstanceState.UP_FOR_RETRY:
                if not ti.is_ready_for_retry(now):
                    continue
            elif ti.state != TaskInstanceState.NONE:
                continue

            task = dag.get_task(ti.task_id)
            upstream_states = [run.get_task_instance(uid).state for uid in task.upstream_task_ids]
            if self._trigger_rule_met(task.trigger_rule, upstream_states):
                ready.append(ti)
        return ready

    def _trigger_rule_met(self, trigger_rule: str, upstream_states: list[TaskInstanceState]) -> bool:
        if not upstream_states:
            return True
        if not all(state.is_terminal for state in upstream_states):
            return False
        if trigger_rule == "all_success":
            return all(state == TaskInstanceState.SUCCESS for state in upstream_states)
        if trigger_rule == "all_done":
            return True
        if trigger_rule == "one_failed":
            return any(state in _UPSTREAM_FAILURE_STATES for state in upstream_states)
        if trigger_rule == "none_failed":
            return not any(state in _UPSTREAM_FAILURE_STATES for state in upstream_states)
        raise ValueError(f"Unknown trigger rule {trigger_rule!r}")

    def _propagate_upstream_failures(self, dag: DAG, run: DagRun) -> None:
        """A task with the default `all_success` rule can never become ready
        once an upstream permanently fails; mark it UPSTREAM_FAILED so the
        dag run can reach a terminal state instead of waiting forever."""
        for ti in run.task_instances.values():
            if ti.state != TaskInstanceState.NONE:
                continue
            task = dag.get_task(ti.task_id)
            if task.trigger_rule != "all_success":
                continue
            upstream_states = [run.get_task_instance(uid).state for uid in task.upstream_task_ids]
            if any(state in _UPSTREAM_FAILURE_STATES for state in upstream_states):
                ti.state = TaskInstanceState.UPSTREAM_FAILED

    def _is_dag_run_finished(self, run: DagRun) -> bool:
        return all(ti.state.is_terminal for ti in run.task_instances.values())
