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

import argparse
import sys

from mini_airflow.dagbag import DagBag
from mini_airflow.db.metadata_db import MetadataDB
from mini_airflow.executor.local_executor import LocalExecutor
from mini_airflow.executor.sequential_executor import SequentialExecutor
from mini_airflow.models.dagrun import DagRun
from mini_airflow.models.state import DagRunState
from mini_airflow.scheduler.scheduler import Scheduler

EXECUTORS = {"local": LocalExecutor, "sequential": SequentialExecutor}


def _print_run_summary(run: DagRun) -> None:
    width = max(len(task_id) for task_id in run.task_instances) + 2
    print(f"\nDag run {run.dag_id}/{run.run_id}: {run.state.value}")
    for task_id, ti in run.task_instances.items():
        tries = f"(try {ti.try_number})" if ti.try_number > 1 else ""
        error = f" - {ti.error}" if ti.error else ""
        print(f"  {task_id:<{width}} {ti.state.value:<16} {tries}{error}")


def trigger(args: argparse.Namespace) -> int:
    bag = DagBag()
    bag.process_file(args.dagfile)
    dag = bag.get_dag(args.dag_id)

    db = MetadataDB()
    executor = EXECUTORS[args.executor]()
    scheduler = Scheduler(db, executor)

    run = scheduler.trigger_dag(dag)
    scheduler.run_to_completion(dag, run)
    executor.shutdown()

    _print_run_summary(run)
    return 0 if run.state == DagRunState.SUCCESS else 1


def show(args: argparse.Namespace) -> int:
    bag = DagBag()
    bag.process_file(args.dagfile)
    dag = bag.get_dag(args.dag_id)

    print(f"{dag.dag_id} (schedule={dag.schedule!r})")
    for task_id in dag.topological_order():
        task = dag.get_task(task_id)
        upstream = ", ".join(sorted(task.upstream_task_ids)) or "-"
        print(f"  {task_id:<20} upstream=[{upstream}] trigger_rule={task.trigger_rule}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mini-airflow")
    sub = parser.add_subparsers(dest="command", required=True)

    trigger_parser = sub.add_parser("trigger", help="Load a dag file and run it to completion")
    trigger_parser.add_argument("dagfile")
    trigger_parser.add_argument("dag_id")
    trigger_parser.add_argument("--executor", choices=EXECUTORS, default="local")
    trigger_parser.set_defaults(func=trigger)

    show_parser = sub.add_parser("show", help="Print a dag's tasks in topological order")
    show_parser.add_argument("dagfile")
    show_parser.add_argument("dag_id")
    show_parser.set_defaults(func=show)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
