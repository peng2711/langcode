# DAG and Agent workflow load tests

The full workload model and metric definitions are documented in
[`docs/zh/dag-workflow-load-test.md`](../../docs/zh/dag-workflow-load-test.md).

This test drives the same `WITH ... FOR UPDATE SKIP LOCKED ... UPDATE ...
RETURNING` statement used by `DAGScheduler.claim_next_available_task`.

Two scenarios are available:

- `claim` isolates atomic task-claim capacity against a flat ready queue.
- `workflow` exercises the repository-owned Agent control path: diamond DAG
  publication, `NOTIFY`, inbox consumption, atomic claim, simulated execution,
  heartbeat renewal, completion, dependency unlock, and lease recovery. It
  does not call an LLM or execute tools, so model latency/cost is excluded.

## Run

Start PostgreSQL and install the Python dependencies, then run from Git Bash,
WSL, Linux, or macOS:

```bash
docker compose up -d postgres
python -m pip install -r requirements.txt
./scripts/load_test_dag.sh
```

Run the end-to-end control-plane scenario with 200 simulated agents:

```bash
LOAD_SCENARIO=workflow USERS=202 SPAWN_RATE=20 RUN_TIME=5m \
  WORKFLOW_SEED_DAGS=20000 ./scripts/load_test_dag.sh
```

Two users are reserved for the Lead Agent publisher and lease monitor, so
`USERS=202` produces 200 simulated Sub Agents. Each seeded DAG defaults to the
realistic shape `root -> 4 parallel workers -> join` (six tasks and eight
dependency edges). Component rows in the report have these meanings:

- `LeadAgent/publish_dag`, `LeadAgent/notify_agents`, and
  `LeadAgent/publish_dag_end_to_end`: publish/fan-out path.
- `MessageHub/consume_inbox`: idle Agent message-consumption path.
- `DAGScheduler/claim_next_task`: atomic scheduling path.
- `DAGScheduler/renew_lease`: heartbeat path.
- `DAGScheduler/complete_and_unlock`: completion and downstream release.
- `DAGScheduler/reclaim_expired_leases`: crash recovery path.
- `Workflow/claim_execute_complete`: end-to-end control-plane task latency.

The workflow scenario also writes `workflow_<run>_summary_<pid>.json` with task
status counts, dependency-edge counts, unread messages, and the
`invalid_completed_tasks` consistency check.

Set `WORKFLOW_NOTIFY_FANOUT=0` (the default) to reproduce the repository's
broadcast-to-all-agents behavior. Set a positive value to cap notification
fan-out during focused scheduler tests. Simulated task execution defaults to
10-50 ms and can be changed with `WORKFLOW_EXECUTION_MIN_MS` and
`WORKFLOW_EXECUTION_MAX_MS`.

Defaults:

- 2,000 concurrent Locust users
- 100 users spawned per second
- 10-minute headless run
- up to 5 seconds for in-flight claims to finish during shutdown
- 1,000,000 ready tasks seeded with one set-based PostgreSQL insert
- PostgreSQL connection pool (10 to 80 connections for one Locust process)
- automatic 200,000-task refill when fewer than 200,000 ready tasks remain

HTML and CSV reports are written to `reports/load/`. Test rows are tagged by
`thread_id = LOAD_THREAD_ID`; use a unique `LOAD_RUN_ID` for each run.

Useful overrides:

```bash
USERS=200 SPAWN_RATE=20 RUN_TIME=1m LOAD_SEED_TASKS=100000 \
  ./scripts/load_test_dag.sh

LOCUST_PROCESSES=4 LOAD_TOTAL_POSTGRES_POOL_MAX_SIZE=120 \
  ./scripts/load_test_dag.sh
```

For a multi-process run, the total pool cap is divided across workers. An
advisory lock ensures only one worker seeds or refills a run at a time.

The test deliberately leaves its rows in PostgreSQL so results can be audited.
After the run, remove only that run's rows with:

```sql
DELETE FROM tasks WHERE thread_id = '<LOAD_THREAD_ID>';
```
