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

from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mini_airflow.sdk.dag import DAG

TriggerRule = str  # "all_success" | "all_done" | "one_failed" | "none_failed"


class BaseOperator:
    """One node in the dependency graph. Compare to `airflow.sdk.BaseOperator`
    in real Airflow: same `>>`/`<<` dependency-declaration operators, same
    retries/trigger_rule fields, minus templating, pools, and SLAs."""

    def __init__(
        self,
        task_id: str,
        dag: DAG | None = None,
        retries: int = 0,
        retry_delay: timedelta = timedelta(seconds=0),
        trigger_rule: TriggerRule = "all_success",
    ) -> None:
        from mini_airflow.sdk.dag import _CONTEXT_STACK

        self.task_id = task_id
        self.retries = retries
        self.retry_delay = retry_delay
        self.trigger_rule = trigger_rule
        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()

        self.dag = dag or (_CONTEXT_STACK[-1] if _CONTEXT_STACK else None)
        if self.dag is None:
            raise ValueError(f"Task {task_id!r} must be created inside `with DAG(...)` or passed dag=")
        self.dag.add_task(self)

    def set_downstream(self, other: BaseOperator) -> BaseOperator:
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other

    def set_upstream(self, other: BaseOperator) -> BaseOperator:
        other.set_downstream(self)
        return other

    def __rshift__(self, other: BaseOperator) -> BaseOperator:
        return self.set_downstream(other)

    def __lshift__(self, other: BaseOperator) -> BaseOperator:
        return self.set_upstream(other)

    def execute(self, context: dict[str, Any]) -> Any:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"


class XComArg:
    """A reference to another task's return value, resolved at execution
    time. Mirrors real Airflow's `airflow.sdk.XComArg` — what lets the
    TaskFlow API (`@task`) turn `transform(extract())` into both a
    dependency edge *and* a data-passing channel, without the caller ever
    touching the metadata DB or Execution API directly."""

    def __init__(self, operator: BaseOperator) -> None:
        self.operator = operator

    def resolve(self, context: dict[str, Any]) -> Any:
        return context["ti"].xcom_pull(task_id=self.operator.task_id)


class PythonOperator(BaseOperator):
    def __init__(
        self,
        task_id: str,
        python_callable: Callable[..., Any],
        op_args: tuple[Any, ...] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}

    def execute(self, context: dict[str, Any]) -> Any:
        args = [self._resolve(value, context) for value in self.op_args]
        kwargs = {key: self._resolve(value, context) for key, value in self.op_kwargs.items()}
        return self.python_callable(*args, **kwargs)

    @staticmethod
    def _resolve(value: Any, context: dict[str, Any]) -> Any:
        return value.resolve(context) if isinstance(value, XComArg) else value


def task(_func: Callable[..., Any] | None = None, **task_kwargs: Any) -> Callable[..., Any]:
    """`@task` decorator — the TaskFlow API. Decorating a plain function
    returns a factory; calling the factory builds a `PythonOperator`, wires
    up dependencies for any `XComArg` arguments automatically, and returns a
    new `XComArg` pointing at this task's own future return value."""

    def decorator(func: Callable[..., Any]) -> Callable[..., XComArg]:
        def factory(*op_args: Any, **op_kwargs: Any) -> XComArg:
            task_id = task_kwargs.get("task_id", func.__name__)
            extra = {key: value for key, value in task_kwargs.items() if key != "task_id"}
            operator = PythonOperator(
                task_id=task_id, python_callable=func, op_args=op_args, op_kwargs=op_kwargs, **extra
            )
            for arg in (*op_args, *op_kwargs.values()):
                if isinstance(arg, XComArg):
                    arg.operator.set_downstream(operator)
            return XComArg(operator)

        return factory

    if _func is not None:
        return decorator(_func)
    return decorator
