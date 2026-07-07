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
from datetime import datetime, timezone

from mini_airflow.sdk.dag import DAG
from mini_airflow.sdk.sensor import PythonSensor
from mini_airflow.sdk.task import PythonOperator

_ready_at = time.monotonic() + 0.3


def is_condition_met() -> bool:
    return time.monotonic() >= _ready_at


def notify() -> None:
    print("condition met, proceeding")


with DAG("example_sensor", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=timezone.utc)):
    wait_for_condition = PythonSensor(
        "wait_for_condition", python_callable=is_condition_met, poke_interval=0.1, timeout=5
    )
    notify_task = PythonOperator("notify", python_callable=notify)

    wait_for_condition.set_downstream(notify_task)
