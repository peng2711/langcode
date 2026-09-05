# LangCode - LangChain based Claude Code like coding agent

## Local dependencies

Start PostgreSQL, Redis, and a single-node RocketMQ broker with:

```bash
docker compose up -d
```

The services are available only on `127.0.0.1` and store their data in named
Docker volumes. The default local connection settings are:

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_DB=langcode
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

Redis is exposed on `127.0.0.1:6379`; RocketMQ NameServer and Broker are
exposed on `127.0.0.1:9876` and `127.0.0.1:10911`, respectively. Override any
published port or PostgreSQL setting with the corresponding environment
variable when running `docker compose`.

## DAG scheduler load test

The repository includes a headless Locust test for PostgreSQL atomic task
claims. It seeds one million ready tasks by default and runs 2,000 users for
10 minutes:

```bash
./scripts/load_test_dag.sh
```

See [`tests/load/README.md`](tests/load/README.md) for tuning, reports, and
cleanup instructions.

Use `LOAD_SCENARIO=workflow` to exercise the full repository-owned control
path (DAG publish, Message Hub, claim, heartbeat, completion/unlock, and lease
recovery) without including external LLM latency.

## completed

- agent loop
- tool use
- permission check: (permission middleware (customized), deny/allow/ask)
- hooks: (middleware-based)
- todo write: (todo middleware)
- context compact + in-session memory (short-term memory): (async postgres checkpointer + context compression middleware (customized))
- memory: sematic (user preferences) + procedural (behavioral guidelines) + episodic (past experience), LLM-based retrieval, use files as indices for retrieval (long-term memory)
- system prompt: real-time assembly by the middleware sequence
- skill-loading：hot-pluggable, requiring no restart
- error recovery

## in_progress

- subagent
- task system
- background tasks
- agent teams

## pending

- cron scheduler
- team protocols
- autonomous agents
- worktree isolation
- mcp plugin
