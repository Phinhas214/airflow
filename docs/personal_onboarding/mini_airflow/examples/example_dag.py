from __future__ import annotations

from datetime import datetime, timezone

from mini_airflow.sdk.dag import DAG
from mini_airflow.sdk.task import task

with DAG("example_etl", schedule="@daily", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc)):

    @task
    def extract() -> dict[str, int]:
        return {"rows": 100}

    @task
    def transform(data: dict[str, int]) -> dict[str, int]:
        return {"rows": data["rows"] * 2}

    @task
    def load(data: dict[str, int]) -> None:
        print(f"Loaded {data['rows']} rows")

    # `transform(extract())` builds the dependency edges AND the XCom data
    # channel between tasks purely from these function calls - no explicit
    # `>>` needed. See XComArg in sdk/task.py.
    load(transform(extract()))
