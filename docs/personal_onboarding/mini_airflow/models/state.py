from __future__ import annotations

from enum import Enum


class TaskInstanceState(str, Enum):
    NONE = "none"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UPSTREAM_FAILED = "upstream_failed"

    @property
    def is_terminal(self) -> bool:
        return self in (
            TaskInstanceState.SUCCESS,
            TaskInstanceState.FAILED,
            TaskInstanceState.UPSTREAM_FAILED,
        )


class DagRunState(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
