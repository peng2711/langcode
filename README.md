# LangCode

LangCode 是一个基于 LangChain / LangGraph 构建的终端 Coding Agent 与 Agent
Harness 项目，参考 Claude Code 的交互方式，重点探索上下文工程、工具与权限控制、
长期记忆，以及多个 Agent 通过 DAG 和独立 Worker 协作执行复杂任务。

本分支已经实现以 PostgreSQL 为状态与审计事实源、RocketMQ 为异步传输层的
Multi-Agent 执行闭环。当前定位是可运行、可测试的工程实现，不代表已经完成生产环境的
容量验证或真实 LLM 大规模基准。

## 主要能力

- Agent Loop：流式输出、工具调用、Todo、Hooks 与错误恢复。
- Context Engineering：动态 Prompt Pipeline、上下文压缩和 PostgreSQL Checkpoint。
- Memory：语义、程序性与情景记忆，以及基于 LLM 的记忆召回。
- Tool Governance：工具注册与 allow / deny / ask 权限中间件。
- Skills：运行时发现与加载，无需重启主进程。
- DAG Task System：依赖管理、并发安全认领、下游解锁和过期租约回收。
- Worker Runtime：独立通用 Worker 注册、心跳、任务竞争和 Agent Card 按需加载。
- Execution Lifecycle：使用 `execution_id + attempt` 跟踪认领、执行、重试、完成、
  失败、取消和过期。
- Reliable Messaging：RocketMQ MessageBus、PostgreSQL Transactional Outbox、
  显式 ACK/Retry、消费去重与投递审计。
- Observability：结构化运行日志和有序的 execution lifecycle events。

## 执行链路

```text
Lead Agent 发布 DAG
  -> PostgreSQL 在业务事务中写入任务、依赖和 Outbox 事件
  -> Outbox Dispatcher 将已提交事件发布到 RocketMQ
  -> 通用 Worker 从共享消费组接收就绪信号
  -> PostgreSQL 原子认领任务并创建 execution attempt
  -> 解析并冻结本次执行使用的 Agent Card 版本
  -> LLM 与工具执行，期间持续心跳续租
  -> 完成或失败状态与 execution event 同事务提交
  -> ACK；可重试失败使用同一 execution_id 创建下一 attempt
```

PostgreSQL 保存任务、执行状态、Agent Card、Outbox 和审计事件，是系统事实源。
RocketMQ 负责异步通知与跨进程分发。旧 attempt 的迟到心跳或结果不能覆盖当前有效
attempt，并会记录 `execution.stale_update_rejected`。

## 本地基础设施

启动 PostgreSQL、Redis、RocketMQ NameServer、Broker 和 Dashboard：

```bash
docker compose up -d
```

服务只绑定到 `127.0.0.1`，数据保存在 Docker named volumes。默认配置：

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_DB=langcode
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export ROCKETMQ_NAMESRV_ADDR=127.0.0.1:9876
```

Redis 默认监听 `127.0.0.1:6379`；RocketMQ NameServer、Broker 和 Proxy 默认监听
`127.0.0.1:9876`、`127.0.0.1:10911` 和 `127.0.0.1:8080`。端口及 PostgreSQL
配置均可通过对应环境变量覆盖。

## Worker Runtime

安装依赖后可以启动独立通用 Runtime：

```bash
python -m pip install -r requirements.txt
python -m worker --runtime-id runtime-001
```

Runtime 会注册自身、启动 Outbox Dispatcher，并从共享队列竞争符合 task type 和 shard
的可执行任务。认领成功后，它加载任务冻结的 Agent Card，执行一个 attempt，再卸载
Card 并回到空闲状态。Lead CLI 离线时，独立 Runtime 的 Dispatcher 仍会继续投递已经
提交到 Outbox 的事件。

模型调用通过 `MODEL_NAME`、`LIGHT_MODEL_NAME`、`API_KEY` 和 `BASE_URL`
配置。启动真实 Worker 会调用模型 API，并受供应商限流和费用约束。

## 一致性与失败处理

- DAG 任务使用 PostgreSQL 原子认领，避免多个 Worker 同时获得同一执行权。
- 每次逻辑执行使用一个 `execution_id`；自动重试递增 `attempt`，不复用 attempt。
- 完成、失败、取消、续租均校验当前 execution、attempt 和 Runtime 所有权。
- 业务状态与 Outbox / execution event 在同一 PostgreSQL 事务中提交。
- RocketMQ 使用至少一次投递；消费者通过 `event_id` 去重并显式 ACK 或 Retry。
- Dispatcher 发布失败后采用有界指数退避，超过阈值进入失败状态并保留审计记录。
- Runtime 崩溃后由租约回收重新调度；旧 attempt 的迟到结果会被拒绝。
- Agent Card 内容发布后不可变，每个 attempt 保存使用版本及摘要，支持审计与复现。

## 测试

纯内存 MessageBus、消息路由和 shard 测试：

```bash
pytest tests/test_message_bus.py
```

PostgreSQL execution lifecycle 集成测试：

```bash
LANGCODE_RUN_DB_TESTS=1 pytest tests/test_dag_scheduler_lifecycle.py
```

PostgreSQL + RocketMQ Outbox、投递与 ACK 集成测试：

```bash
LANGCODE_RUN_MQ_TESTS=1 pytest tests/test_rocketmq_integration.py
```

仓库还保留 PostgreSQL DAG 原子认领 Locust 压测：

```bash
./scripts/load_test_dag.sh
```

测试结果只能按覆盖范围描述。原子认领或控制面 QPS 不等于真实 LLM 编码任务吞吐；
真实端到端结果还受到模型延迟、RPM/TPM、Token 数、工具执行和重试策略影响。

## 当前状态

已实现并有代码或测试覆盖：

- Agent Loop、工具调用、权限控制、Hooks、Todo 与错误恢复。
- 上下文压缩、Checkpoint、长期记忆、动态 Prompt 和 Skill 热加载。
- Lead Agent、Sub Agent、DAG 任务系统及基础团队协作。
- 独立 Worker Runtime、注册心跳、租约回收与任务接管。
- Task Execution Lifecycle、Retry、迟到结果拒绝和 execution events。
- Agent Card 版本化、不可变约束、按需加载与执行快照。
- RocketMQ MessageBus、Transactional Outbox、ACK/Retry 和投递审计。
- 结构化日志及任务、执行、消息和权限关联字段。

仍需生产化验证或继续建设：

- 大规模多机 Worker 和 RocketMQ 故障演练。
- 接入真实 LLM 后的限流、Token 成本、质量评估和端到端容量基准。
- 监控仪表盘、告警、审计数据归档和运维工具。
- 更完整的 Team Protocol、自治策略和人工介入流程。
- Cron 调度、Worktree 隔离与 MCP 插件。

## 文档

- [消息、执行闭环与审计改造说明](消息与log改造.md)
- [DAG Scheduler 使用说明](lib/README_dag_scheduler.md)
- [DAG 原子认领压测](tests/load/README.md)

`docs/zh/`、`docs/en/` 和 `docs/ja/` 记录了从 Agent Loop 到 Agent Teams、
自治调度与 Worktree 隔离的设计过程。部分章节是教程或目标设计，最终运行行为以当前
代码和本 README 的“当前状态”为准。
