# Airflow as a Black Box

At the highest level of abstraction, ignore every internal component (scheduler,
executor, workers, metadata DB, API server) and just look at what goes in and what
comes out.

- **Primary input**: a **Dag** (Python code authored with `airflow.sdk` declaring tasks
  and dependencies) plus a **trigger signal** (a schedule interval, a manual trigger, or
  an asset/event-driven trigger) telling Airflow when to run it.
- **Primary output**: **executed task runs** — the real side effects performed against
  external systems — plus the observable **run state** (success/failed/retry, logs,
  timing) exposed via the UI, REST API, and CLI.

```mermaid
flowchart LR
    subgraph INPUT["Input"]
        A["Dag definition\n(Python code, tasks + dependencies)"]
        B["Trigger signal\n(schedule / manual / event)"]
    end

    A --> AF
    B --> AF

    AF["Airflow"]

    AF --> C["Executed task runs\n(side effects in external systems)"]
    AF --> D["Run state\n(status, logs, timing via UI / API / CLI)"]

    subgraph OUTPUT["Output"]
        C
        D
    end
```
