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

import time
from collections.abc import Callable
from typing import Any

from mini_airflow.sdk.task import BaseOperator


class BaseSensorOperator(BaseOperator):
    """Polls `poke()` until it returns True or `timeout` elapses.

    Real Airflow lets a sensor `defer()` so the Triggerer evaluates the wait
    condition asynchronously and the worker's slot is freed in the meantime;
    this toy just blocks the worker thread instead. That gap is the single
    biggest thing this project simplifies away — see README.md.
    """

    def __init__(
        self, task_id: str, poke_interval: float = 1.0, timeout: float = 60.0, **kwargs: Any
    ) -> None:
        super().__init__(task_id, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout

    def poke(self, context: dict[str, Any]) -> bool:
        raise NotImplementedError

    def execute(self, context: dict[str, Any]) -> Any:
        waited = 0.0
        while not self.poke(context):
            if waited >= self.timeout:
                raise TimeoutError(f"Sensor {self.task_id!r} timed out after {self.timeout}s")
            time.sleep(self.poke_interval)
            waited += self.poke_interval
        return True


class PythonSensor(BaseSensorOperator):
    def __init__(self, task_id: str, python_callable: Callable[[], bool], **kwargs: Any) -> None:
        super().__init__(task_id, **kwargs)
        self.python_callable = python_callable

    def poke(self, context: dict[str, Any]) -> bool:
        return bool(self.python_callable())
