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
