<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Airflow: A Guided Walkthrough for New Engineers

This is a mental-map document, not a spec. It points at real files/folders so you can
go verify anything yourself. Follow up in conversation to go deeper on any section.

## 1. What does this app actually do?

Apache Airflow is a platform for authoring, scheduling, and monitoring **workflows as
code**. A workflow is called a **Dag** (Directed Acyclic Graph) — a Python file that
declares a set of tasks and the dependencies between them (`task_a >> task_b`, etc.).

What a user actually does with it:

- **Author**: write a Python file using `from airflow.sdk import DAG` (or the `@dag`/`@task`
  decorators) describing tasks and their order. Example dags live in
  `airflow-core/src/airflow/example_dags/`.
- **Schedule**: Airflow decides when a Dag's next run is due (cron-like schedule, a
  data interval, or asset/dataset-driven triggers) and creates a `DagRun`.
- **Execute**: each task in a run becomes a `TaskInstance` that gets handed to a worker
  process to execute.
- **Monitor & operate**: a web UI (Grid view, Graph view, logs, etc.) shows run history,
  lets you retry/clear/mark tasks, inspect logs, and manage connections/variables/pools.
- **Extend**: "providers" (`providers/`) are plugin packages that add integrations —
  operators/hooks/sensors for AWS, GCP, Databricks, Slack, etc. (88 provider packages
  today) — so a Dag can orchestrate work in other systems rather than doing the work itself.

Airflow's opinion (see `README.md`): workflows should be mostly static and slowly
changing, tasks should be idempotent, and large data shouldn't be passed directly
between tasks (small metadata can go through XCom) — it's an orchestrator, not a data
processing engine.

## 2. How is the codebase structured?

This is a **uv workspace monorepo** — many independently-versioned Python packages
sharing one lockfile (`uv.lock`) and one dependency resolution. The workspace members
are listed in the root `pyproject.toml` (`apache-airflow-core`, `apache-airflow-task-sdk`,
`apache-airflow-ctl`, `apache-airflow-providers`, the `shared/*` libraries, etc.).

Top-level layout that matters day to day:

| Path | What it is |
|---|---|
| `airflow-core/src/airflow/` | The core product: scheduler, metadata DB models, REST API, CLI, web UI |
| `task-sdk/` | The SDK users import to author Dags (`airflow.sdk`), plus the task execution runtime |
| `providers/` | 88 integration packages (`providers/amazon`, `providers/google`, …), each its own distribution |
| `airflow-ctl/` | A separate management CLI (talks to a running Airflow over the API, not the DB) |
| `chart/` | Helm chart for Kubernetes deployment |
| `shared/` | Small libraries (logging, serialization, secrets-masker, …) symlinked into multiple distributions so they share one implementation across versions |
| `dev/`, `scripts/`, `devel-common/` | Dev tooling: Breeze (the dev/CI container environment), CI helper scripts, shared test fixtures |
| `registry/` | The provider registry static site (airflow.apache.org/registry) — a separate 11ty/vanilla-JS project, not part of the runtime product |
| `clients/`, `go-sdk/`, `java-sdk/`, `ts-sdk/` | Generated/typed clients for the REST API in other languages |

Inside `airflow-core/src/airflow/`, the components that make up the running system:

- `models/` — SQLAlchemy ORM models: `DagModel`, `DagRun`, `Connection`, `Pool`,
  `SerializedDagModel`, `Backfill`, `HITL` (human-in-the-loop), etc. This is the
  metadata database schema.
- `dag_processing/` — parses Dag Python files (in separate processes, via
  `dag_processing/manager.py` and `processor.py`) and stores the *serialized* Dag
  structure in the DB. This is the only place raw user Dag-file code executes on the
  control plane.
- `jobs/` — the long-running control-plane loops: `scheduler_job_runner.py`,
  `dag_processor_job_runner.py`, `triggerer_job_runner.py`. `JOB_LIFECYCLE.md` in that
  folder documents how these are started/stopped/health-checked.
- `api_fastapi/` — two distinct FastAPI apps:
  - `core_api/` — the public REST API v2 and the API the React UI calls (auth, CRUD on
    dags/runs/connections/pools/etc.)
  - `execution_api/` — the narrow, JWT-authenticated API workers/triggerer/Dag
    processor use to talk to the metadata DB indirectly.
- `cli/` — the `airflow` command-line tool (`cli_parser.py`, `commands/`).
- `ui/` — the React/TypeScript frontend (Vite-built), served by `core_api`.
- `executors/`, `triggers/`, `sensors/`, `operators/` — the base classes and built-in
  implementations for how/where task work actually runs.

### Architecture diagram

```
                              ┌─────────────────────────┐
   Dag author  ── writes ──▶ │  Dag file (Python)       │
                              │  uses `airflow.sdk` DAG  │
                              └────────────┬─────────────┘
                                           │ parsed by
                                           ▼
                          ┌───────────────────────────────┐
                          │   Dag File Processor          │  (airflow-core: dag_processing/)
                          │   runs user code in isolated  │
                          │   subprocesses, NEVER the      │
                          │   scheduler itself             │
                          └───────────────┬────────────────┘
                                          │ writes serialized Dag
                                          ▼
                          ┌───────────────────────────────┐
                          │        Metadata Database       │  (Postgres/MySQL/SQLite)
                          │  DagModel, DagRun, TaskInstance │
                          └───────────────┬────────────────┘
                       reads/writes only  │  ▲
                       via Execution API  │  │ reads serialized Dags,
                       (workers/trigger.) │  │ never runs user code
                                          ▼  │
   ┌───────────────────┐        ┌──────────────────────┐        ┌─────────────────────┐
   │     Scheduler      │◀──────▶│     API Server        │◀──────▶│      React UI        │
   │  jobs/scheduler_    │  DB    │  api_fastapi/core_api │  HTTP  │  ui/ (Vite build)     │
   │  job_runner.py      │        │  + execution_api      │        │                       │
   └─────────┬───────────┘        └──────────────────────┘        └───────────────────────┘
             │ creates TaskInstances,
             │ assigns to executor
             ▼
   ┌────────────────────┐        JWT-scoped to a single
   │      Executor        │      task-instance-id
   │ (Local/Celery/K8s/…) │───────────────┐
   └─────────┬─────────────┘               │
             │ launches                    ▼
             ▼                    ┌──────────────────────┐
   ┌────────────────────┐         │   Execution API        │
   │       Worker          │◀───────│  (execution_api/)      │
   │  task-sdk runtime:    │  talks │  the ONLY way workers, │
   │  execution_time/      │  only  │  triggerer, and DFP    │
   │  task_runner.py        │  here  │  reach the DB          │
   └────────────────────┘         └──────────────────────┘

   ┌────────────────────┐
   │     Triggerer        │  evaluates deferred tasks/sensors (async),
   │ jobs/triggerer_       │  also goes through the Execution API
   │ job_runner.py         │
   └────────────────────┘
```

This mirrors the **Architecture Boundaries** section in the repo's `CLAUDE.md` — that's
the authoritative, terser version of this diagram, along with
`airflow-core/docs/security/security_model.rst` for the trust boundaries between these
components (e.g. why a worker only gets a JWT scoped to one task instance, not DB
credentials).

## 3. Tech stack and what each piece is responsible for

**Backend (`airflow-core/`, `task-sdk/`)**

- **Python 3.10–3.14** — the implementation language for the whole control plane, SDK,
  and providers.
- **SQLAlchemy (2.0-style ORM)** — every metadata table (`DagModel`, `TaskInstance`,
  `Connection`, …) is a SQLAlchemy model in `models/`. The project explicitly bans the
  old `sqlalchemy.ext.declarative` import path (see ruff config in root `pyproject.toml`).
- **Alembic** (`airflow-core/src/airflow/migrations/`, driven by `alembic.ini`) — schema
  migrations for the metadata DB.
- **FastAPI** — both `core_api` (public REST + UI backend) and `execution_api` (worker/
  triggerer/DFP-facing API) are FastAPI apps. Pydantic models under
  `api_fastapi/*/datamodels/` define request/response schemas.
- **Gunicorn** (`api_fastapi/gunicorn_app.py`, `gunicorn_config.py`) — the production
  ASGI/WSGI process manager serving the FastAPI apps.
- **JWT** — short-lived tokens scope a worker to exactly one task instance when it calls
  the Execution API (see `airflow-core/docs/security/jwt_token_authentication.rst`).
- **Pluggable executors** (`executors/`) — Local, Celery, Kubernetes, etc. — decide
  *where/how* a task instance actually runs; the scheduler only decides *that* it should run.
- **Providers** (`providers/`) — each is its own installable distribution
  (`apache-airflow-providers-amazon`, etc.) contributing operators/hooks/sensors/triggers
  for external systems, discovered at runtime via `providers_manager.py`.

**Frontend (`airflow-core/src/airflow/ui/`)**

- **React + TypeScript, built with Vite** — the whole web UI.
- **Chakra UI** (`@chakra-ui/react`) — component library/design system.
- **TanStack Query** — server-state fetching/caching against the `core_api` REST API
  (the client is generated from the OpenAPI spec, see `ui/openapi-gen/`).
- **TanStack Table**, **@xyflow/react** + **elkjs** — the Grid view table and the Graph
  view Dag-dependency diagram/auto-layout, respectively.
- **Monaco Editor** — in-browser code viewing (e.g. viewing Dag source, config).
- **i18next** — UI translations (see the `airflow-translations` skill and
  `airflow-core/src/airflow/ui/public/i18n/locales/`).
- **Zustand** — local client-side state management.
- **Playwright** + **Vitest** — e2e and unit tests for the UI.

**Dev/CI tooling**

- **uv** — package/dependency manager and workspace tool for the whole monorepo
  (`uv.lock`, `uv sync`, `uv run`).
- **Breeze** (`dev/breeze/`) — the containerized dev/CI environment; almost everything
  (tests, `airflow` CLI runs, mypy for providers) is expected to run through it rather
  than the bare host.
- **prek** — the pre-commit-hook runner (ruff lint/format, mypy, license headers,
  commit-message conventions, etc.).
- **ruff** — linter + formatter for all Python code.
- **mypy** — static typing, run per-distribution via dedicated prek hooks.

## 4. How does data flow through the system? (end-to-end request)

Two different "requests" matter here — a **user viewing/triggering a Dag in the UI**,
and a **task actually executing**. Both are worth tracing.

### A. User clicks "Trigger Dag" in the UI

1. React UI (`ui/src/…`) sends an authenticated HTTP request to `core_api`
   (`api_fastapi/core_api/routes/`).
2. The route handler validates the request (Pydantic datamodel), calls into
   domain/service code (`api_fastapi/core_api/services/`), and writes a new `DagRun`
   row via SQLAlchemy directly to the metadata DB — the API server is one of the few
   components allowed direct DB access.
3. Response (serialized `DagRun`) flows back to the UI; TanStack Query updates the
   cached view, and the Grid/Graph view re-renders.
4. Separately (not from this request), the **Scheduler** (`jobs/scheduler_job_runner.py`)
   is continuously polling the DB, notices the new `DagRun`, and for each task in the
   already-serialized Dag (produced earlier by the Dag File Processor) creates a
   `TaskInstance` row and asks the configured **Executor** to run it.

### B. A task instance actually executing

1. The Executor (Local/Celery/Kubernetes/…) launches a **worker** process for the
   task instance, injecting a short-lived JWT scoped to that specific task instance ID.
2. The worker runs the **task-sdk** runtime (`task-sdk/src/airflow/sdk/execution_time/`):
   `supervisor.py` spawns and supervises `task_runner.py`, which imports the user's task
   callable/operator and executes it.
3. Anything the task needs from the control plane — its rendered template fields,
   connections, XCom read/write, marking itself success/failed/deferred — goes out over
   HTTP to the **Execution API** (`api_fastapi/execution_api/`), authenticated with that
   task-scoped JWT. The worker **never** talks to the metadata DB directly
   (`comms.py`/`request_handlers.py` in execution_time implement this protocol).
4. If the task calls `defer()` (e.g. a sensor waiting on an external event), execution
   moves to the **Triggerer** (`jobs/triggerer_job_runner.py`), which evaluates the
   trigger asynchronously and, on completion, tells the scheduler (again through the
   Execution API) to resume the task.
5. Final state (success/failed/up_for_retry) is written back to the DB via the
   Execution API; the UI picks this up on its next poll/query and the Grid/Graph view
   updates.

The throughline in both flows: **only the API server and the Scheduler/Dag-processor's
own writes touch the DB directly; workers, the triggerer, and the Dag file processor are
steered through the Execution API** (with the DFP/triggerer's direct-DB-access being a
documented, intentional current limitation, not a violation — see
`airflow-core/docs/security/security_model.rst`).

## 5. Key dependencies and integration points — where to be careful

- **`airflow.sdk` is the user-facing contract.** Anything in `task-sdk/` is effectively
  a public API surface — changing behavior here can break every existing Dag file in
  the wild. Check `contributing-docs/19_execution_api_versioning.rst` before touching
  the Execution API's request/response shapes; it's versioned deliberately because
  workers and the API server can be on different Airflow versions during upgrades.
- **Serialization is a hard coupling point.** `models/serialized_dag.py` and
  `serialization/` are what let the scheduler avoid running user code — if a Dag
  attribute isn't serializable, the scheduler simply can't see it. Changes to operator/
  Dag attributes need to stay serialization-compatible.
- **`shared/` libraries are symlinked, not copied**, into multiple distributions
  (`airflow-core`, `task-sdk`, providers). Editing a file under `shared/logging/` (for
  example) changes behavior everywhere it's linked — verify which distributions consume
  it before assuming a change is local.
- **`session` handling in `airflow-core`**: functions that take a `session` parameter
  must never call `session.commit()` themselves (per `CLAUDE.md`) — commit boundaries
  are owned by the caller. Violating this is an easy way to introduce subtle
  transaction/locking bugs in scheduler-loop code.
- **Bulk DB writes in the scheduler loop must be batched** (`utils/db_cleanup.py` is the
  reference pattern). An unbounded `DELETE`/`UPDATE` against a user-driven table can
  stall the scheduler main loop for every Dag in the deployment.
- **Providers are loosely coupled by design** (each is its own distribution with its own
  `pyproject.toml`, tested independently), but `providers_manager.py` /
  `providers_manager_runtime.py` are the tight coupling point — they discover and index
  every provider's operators/hooks/connections at runtime, so changes there can affect
  all 88 providers at once.
- **The React UI's OpenAPI client is generated** (`ui/openapi-gen/`) from the `core_api`
  FastAPI schema — don't hand-edit generated client code; regenerate it after changing
  a route/datamodel.
- **`registry/` is a separate product** (static site, own `CLAUDE.md`, no framework,
  no shared code with the running Airflow app beyond `dev/registry/` extraction
  scripts) — don't assume conventions from `airflow-core/` apply there.

## Where to go next

- `CLAUDE.md` (repo root) — the terse, authoritative dev-workflow reference (commands,
  coding standards, PR process). This document is the narrative companion to it.
- `airflow-core/docs/security/security_model.rst` — the full trust-boundary model
  referenced throughout section 4/5 above.
- `contributing-docs/03a_contributors_quick_start_beginners.rst` — environment setup
  from scratch.
- `dev/breeze/doc/` — how the containerized dev environment and CI selective-checks work.
