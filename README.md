# LangCode

LangCode 是一个基于 LangChain / LangGraph 构建的终端 Coding Agent 与 Agent
Harness 实验项目，参考 Claude Code 的交互方式，重点探索上下文工程、工具与权限控制、
长期记忆，以及多个 Agent 通过 DAG 协作执行复杂任务。

项目目前已经具备单 Agent 执行闭环和实验性的 Multi-Agent 控制链路。DAG 调度、消息、
租约与运行状态以 PostgreSQL 为中心；真实 LLM 的延迟、限流和成本不包含在仓库现有的
控制面压测结果中。

## 主要能力

- Agent Loop：流式输出、工具调用与错误恢复。
- Context Engineering：系统提示词动态组装、上下文压缩、短期 Checkpoint。
- Memory：语义、程序性和情景记忆，以及基于 LLM 的记忆召回。
- Tool Governance：工具注册、权限中间件、allow / deny / ask 策略和 Hooks。
- Skills：运行时发现和加载，无需重启主进程。
- Multi-Agent：Lead Agent、Sub Agent、消息收件箱和协作工具。
- DAG Scheduler：任务分解、依赖管理、原子认领、心跳续租、幂等完成、下游解锁与
  过期租约回收。
- Load Testing：可复现的 Locust 原子认领基线和 Agent 控制面端到端压测。

## Multi-Agent 控制链路

```text
Lead Agent 发布 DAG
  -> PostgreSQL 持久化任务与依赖
  -> Message Hub 批量写入独立收件箱并广播一次 NOTIFY
  -> Sub Agent 使用 FOR UPDATE SKIP LOCKED 原子认领
  -> 执行期间心跳续租
  -> 幂等完成任务并原子解锁下游
  -> Reaper 回收崩溃 Agent 的过期租约
```

消息通知保留每个 Agent 的独立持久化记录，同时将原来的逐 Agent 事务和重复
`NOTIFY` 改为一次集合写入与一次广播唤醒。PostgreSQL 仍然是当前实现的事实源。

## 本地基础设施

启动 PostgreSQL、Redis 和单节点 RocketMQ：

```bash
docker compose up -d
```

服务只绑定到 `127.0.0.1`，数据保存在 Docker named volumes。PostgreSQL 默认配置：

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_DB=langcode
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

Redis 默认监听 `127.0.0.1:6379`；RocketMQ NameServer 和 Broker 默认监听
`127.0.0.1:9876` 和 `127.0.0.1:10911`。所有端口和 PostgreSQL 配置均可通过对应
环境变量覆盖。

> 当前 Agent 消息链路使用 PostgreSQL `LISTEN/NOTIFY + agent_messages`。
> Compose 中的 RocketMQ 用于后续 Outbox、可靠投递和独立 Worker 改造，目前尚未接管
> Agent 通信。设计方案见[消息与日志改造](消息与log改造.md)。

## DAG 与 Agent 控制面压测

默认场景用于隔离测试 PostgreSQL 原子任务认领：

```bash
./scripts/load_test_dag.sh
```

完整控制面场景覆盖 DAG 发布、消息通知、任务认领、模拟执行、心跳、完成与依赖解锁、
租约回收：

```bash
LOAD_SCENARIO=workflow USERS=102 SPAWN_RATE=25 RUN_TIME=5m \
  WORKFLOW_SEED_DAGS=50000 ./scripts/load_test_dag.sh
```

`USERS=102` 包含 100 个 Sub Agent、1 个 Lead Agent 和 1 个租约回收器。在本地同参数
5 分钟测试中：

| 指标 | 结果 |
|---|---:|
| 控制面任务吞吐 | 621.00 tasks/s |
| 控制面任务周期平均延迟 | 125.26 ms |
| 控制面任务周期 P95 / P99 | 170 / 200 ms |
| 100 Agent 通知平均延迟 | 64.63 ms |
| 100 Agent 通知 P95 / P99 | 93 / 110 ms |
| DAG 发布加通知 P95 / P99 | 230 / 270 ms |
| Locust Failure Count | 0 |

结束后的数据库一致性检查中，重复任务 ID 和违反依赖顺序的完成任务均为 0。以上测试用
10–50 ms 等待模拟任务执行，不调用外部 LLM，也不执行真实工具，因此 `621 tasks/s`
表示 Agent 调度控制面的吞吐，不代表真实编码任务或模型推理吞吐。

详细负载模型、指标口径、结果对比和清理方式：

- [压测设计与已验证结果](docs/zh/dag-workflow-load-test.md)
- [Locust 运行参数](tests/load/README.md)

## 当前状态

已实现：

- Agent Loop、工具调用、权限控制、Hooks 和 Todo 中间件。
- 上下文压缩、短期 Checkpoint、长期记忆和动态 Prompt Pipeline。
- Skill 热加载和基础错误恢复。
- 实验性 Lead / Sub Agent、DAG 任务看板、PostgreSQL Message Hub。
- 并发安全的任务认领、续租、完成解锁、故障回收和批量通知。
- 原子认领与完整控制面 Locust 压测。

规划或改造中：

- RocketMQ MessageBus、PostgreSQL Outbox、消费幂等和死信处理。
- 独立 Worker 池、`execution_id + attempt` 执行审计闭环和结构化日志。
- 后台任务、Cron 调度、Worktree 隔离和 MCP 插件。
- 接入真实 LLM 后的限流、Token 成本与端到端任务基准。

## 文档

`docs/zh/`、`docs/en/` 和 `docs/ja/` 记录了从 Agent Loop 到 Agent Teams、自治调度
与 Worktree 隔离的设计过程。部分章节是教程或目标设计，最终运行行为以当前代码和本
README 的“当前状态”为准。
