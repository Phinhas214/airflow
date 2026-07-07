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

from datetime import datetime, timedelta, timezone

from mini_airflow.sdk.dag import DAG
from mini_airflow.sdk.task import PythonOperator

_attempts: dict[str, int] = {}


def flaky_download() -> str:
    _attempts["download"] = _attempts.get("download", 0) + 1
    if _attempts["download"] < 3:
        raise ConnectionError("upstream service unavailable")
    return "payload.csv"


def always_fails() -> None:
    raise ValueError("this task is broken on purpose")


def cleanup() -> None:
    print("cleanup ran regardless of upstream outcome")


with DAG("example_retry", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=timezone.utc)):
    download = PythonOperator(
        "download", python_callable=flaky_download, retries=3, retry_delay=timedelta(seconds=0)
    )
    broken = PythonOperator("broken", python_callable=always_fails)
    cleanup_task = PythonOperator("cleanup", python_callable=cleanup, trigger_rule="all_done")

    download.set_downstream(cleanup_task)
    broken.set_downstream(cleanup_task)
