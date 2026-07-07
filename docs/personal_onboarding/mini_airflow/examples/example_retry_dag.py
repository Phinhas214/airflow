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
