# Agent 通信、执行闭环与审计改造方案

## 总结

将现有 PostgreSQL `LISTEN/NOTIFY + agent_messages` 改为 RocketMQ 通信，同时保留 PostgreSQL 作为任务、执行状态和审计事实源。

每次 sub task 被认领都创建独立执行实例。`task_id` 只表示逻辑任务，执行审计、状态更新和消息关联统一使用 `(execution_id, attempt)`，避免重试、租约过期和迟到结果混淆。

核心原则：

- PostgreSQL 事务负责状态变更与 Outbox 事件写入。
- RocketMQ 负责异步分发、唤醒和跨进程通信。
- 消费采用至少一次投递，使用 `event_id` 幂等去重。
- Sub Agent 改为独立 worker 进程，由共享 worker 池执行。
- 任务、通信和权限请求均形成可查询的审计闭环。
- 旧 `agent_messages` 表停止写入，仅保留历史查询。

## 执行模型

新增 `task_executions`：

```text
execution_id UUID
attempt INT
task_id TEXT
thread_id TEXT
agent_id TEXT
worker_id TEXT
status: claimed/running/completed/failed/expired/cancelled
claimed_at
started_at
ended_at
lease_expires_at
last_heartbeat_at
result_summary
result_path
failure_reason

PRIMARY KEY (execution_id, attempt)
UNIQUE (task_id, execution_id, attempt)
```

规则：

- 第一次执行任务时生成新的 `execution_id`，并以 `attempt = 1` 创建 `task_execution`。
- 因失败、租约过期或其他可重试原因再次执行时，沿用同一个 `execution_id`，仅递增 `attempt` 并创建新的 `task_execution`。
- 用户主动重新运行已经结束的逻辑任务时，生成新的 `execution_id`，并重新从 `attempt = 1` 开始。
- 所有 execution 日志、心跳、完成、失败、回收、权限与通信事件均携带 `task_id`、`execution_id`、`attempt`。
- 一次 execution attempt 只能以 `completed`、`failed`、`expired` 或 `cancelled` 之一结束。
- 租约过期时，调度器将当前 attempt 关闭为 `expired`，再允许新 worker 使用同一 `execution_id` 创建下一 attempt。
- 旧 attempt 的迟到心跳、完成或失败请求不得修改任务当前状态；记录 `execution.stale_update_rejected` 审计事件。
- 任务最终状态由最新有效的 `(execution_id, attempt)` 决定；逻辑任务完成后不再创建新 attempt。

将 `tasks.metadata.retry_count` 迁移为明确的 `current_attempt`、`current_execution_id` 等列；保留旧 metadata 只作兼容读取。

## 通信与一致性

新增统一 `MessageBus` 接口，替代 `AsyncPostgresMessageHub`。消息 envelope 增加：

```json
{
  "event_id": "uuid",
  "event_type": "task.completed",
  "thread_id": "session-id",
  "task_id": "task-id",
  "execution_id": "uuid",
  "attempt": 2,
  "sender": "worker-1",
  "target": "lead",
  "correlation_id": "uuid",
  "payload": {}
}
```

`execution_id` 与 `attempt` 对执行期消息为必填；任务发布、agent 启动等执行前事件可为空。

```python
class MessageBus(Protocol):
    async def send(
        self,
        *,
        sender: str,
        target: str | None,
        event_type: str,
        payload: dict,
        thread_id: str | None = None,
        task_id: str | None = None,
        execution_id: str | None = None,
        attempt: int | None = None,
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """返回 event_id"""

    async def receive(
        self,
        consumer: str,
        timeout: float = 5,
    ) -> list[MessageEnvelope]:
        ...

    async def ack(self, event_id: str) -> None:
        ...

    async def retry(self, event_id: str, reason: str) -> None:
        ...
```

### RocketMQ 主题

- `agent-command`
  - `task_available`
  - `permission_response`
  - `agent_start`
  - `agent_shutdown`
- `task-event`
  - `task.published`
  - `execution.claimed`
  - `execution.started`
  - `execution.heartbeat`
  - `execution.completed`
  - `execution.failed`
  - `execution.expired`
  - `execution.retried`
- `permission-event`
  - `permission.requested`
  - `permission.decided`
- `agent-message`
  - 普通 lead/sub 通信，默认持久化并可按类型配置 TTL

消息类型使用 RocketMQ tag 过滤；消息 key 优先使用 `task_id`、`execution_id`、`request_id` 或 `agent_id`，保证同一业务对象的顺序性。

### PostgreSQL Outbox

新增 `message_outbox` 表，所有需要发送的事件必须在业务事务内写入：

```text
message_outbox
- event_id UUID PRIMARY KEY
- topic
- tag
- message_key
- envelope JSONB
- status: pending/published/failed
- retry_count
- next_retry_at
- last_error
- created_at
- published_at
```

事务边界：

- 发布 DAG：任务、依赖关系和 `task.published` 事件同一事务提交。
- 认领任务：任务状态更新为 `in_progress`，同时创建或递增 execution attempt 并写入 `execution.claimed`。
- 开始执行：execution 更新为 `running`，写入 `execution.started`。
- 心跳续租：更新 `last_heartbeat` 和租约，写入 `execution.heartbeat`。
- 完成/失败/回收：状态变更和对应生命周期事件同一事务提交。
- 权限请求：权限审计记录与 `permission.requested` 同一事务提交。
- 权限决定：权限审计更新与 `permission.decided` 同一事务提交。

Outbox dispatcher 独立运行，负责：

- 批量读取待发送事件。
- 发布成功后更新 `published_at`。
- 失败后指数退避重试。
- 超过最大重试次数后进入失败状态并触发告警。
- RocketMQ 暂不可用时不得影响已经提交的 PostgreSQL 业务事务。

### 消费幂等与失败处理

新增 `message_consumer_inbox` 表：

```text
message_consumer_inbox
- consumer_name
- event_id
- consumed_at
- result
PRIMARY KEY (consumer_name, event_id)
```

消费流程：

1. 收到消息。
2. 插入消费记录。
3. 如果记录已存在，直接确认消息。
4. 执行业务处理。
5. 处理成功后 ACK。
6. 处理失败时重试；超过阈值进入死信队列。

所有消费者必须支持重复消息，不能依赖“只消费一次”。

## Worker 与任务接口

Sub Agent 改为独立 worker 进程，worker 注册心跳、能力、版本和租约到 PostgreSQL；共享 worker 池竞争可执行工作。

新增 `agent_workers`：

```text
agent_workers
- worker_id
- status: starting/idle/busy/stopped
- capabilities JSONB
- version
- current_agent_id
- last_heartbeat
- lease_expires_at
- started_at
```

Worker 启动入口：

```bash
python -m worker --worker-id worker-001
```

Worker 行为：

- 注册并定期续租。
- 从共享 worker 池竞争可执行任务。
- 通过 PostgreSQL 原子认领任务并创建或递增 execution attempt。
- 监听 `agent-command` 获取唤醒、权限响应和生命周期命令。
- 通过逻辑 `agent_id` 的订阅组消费定向消息；worker 接管 agent 后继续消费该 agent 的未确认消息。
- Worker 崩溃后由租约过期机制释放任务和 agent，其他 worker 可接管。
- Worker 定期扫描 PostgreSQL 可执行任务，作为 RocketMQ 通知丢失或暂时不可用时的最终补偿机制。

调度接口调整：

```python
claim_next_available_task(
    thread_id: str,
    worker_id: str,
    agent_id: str,
) -> ClaimedExecution

renew_lease(
    task_id: str,
    execution_id: str,
    attempt: int,
    worker_id: str,
) -> bool

complete_execution(
    task_id: str,
    execution_id: str,
    attempt: int,
    worker_id: str,
    result: ExecutionResult,
) -> bool

fail_execution(
    task_id: str,
    execution_id: str,
    attempt: int,
    worker_id: str,
    error: str,
) -> RetryDecision
```

完成、失败和续租 SQL 必须同时校验：

- `task_id`
- `current_execution_id`
- `current_attempt`
- `worker_id` 或当前 owner
- execution 尚未终止

`spawn_sub_agent` 改为持久化逻辑 agent 实例并发送 `agent_start` Outbox 事件，不再以 CLI 内的 `asyncio.create_task()` 作为运行方式。

提供任务执行查询能力：

```python
get_task_execution_history(task_id) -> list[ExecutionRecord]
get_execution_events(execution_id, attempt) -> list[ExecutionEvent]
```

用于审计、回放和后续可视化。

## 审计与日志

区分两类日志。

### 结构化运行日志

替换当前文件格式日志为结构化输出，至少包含：

```text
timestamp
level
logger
service
worker_id
agent_id
thread_id
task_id
execution_id
attempt
event_id
correlation_id
message
exception
```

命令内容、权限参数和模型上下文不得无条件写入日志，敏感字段需要脱敏。

### 业务审计事件

新增 `task_execution_events`，每条记录必须关联 `(execution_id, attempt)`：

- `execution.claimed`
- `execution.started`
- `execution.heartbeat`
- `execution.completed`
- `execution.failed`
- `execution.expired`
- `execution.cancelled`
- `execution.retried`
- `execution.stale_update_rejected`

新增消息投递审计，记录 `event_id`、消费者、投递/消费状态、重试次数和错误信息。

保留 `permission_audit_log`，并使每次权限申请与决定关联对应 execution；无执行上下文的 lead 权限操作允许 execution 字段为空。

## 迁移策略

- 初始化 RocketMQ topic、消费组、Outbox、消费去重、worker 注册和 execution 表。
- 停止写入旧 `agent_messages`；旧表只读保留用于历史查询。
- 不做长期双写。
- 不将旧消息重新投递，避免重复执行。
- 提供旧消息表到新审计表的只读查询兼容层。
- RocketMQ 不可用时，Outbox 保留待投递事件；worker 的 PostgreSQL 周期扫描保证任务最终可被执行。

## 测试计划

必须覆盖：

1. 同一 `task_id` 的重试沿用同一个 `execution_id`，并生成递增且不复用的 `attempt`。
2. 每个 execution 从认领到终态形成完整事件链。
3. 过期 execution 的迟到完成不会覆盖新 attempt。
4. 多 worker 并发认领同一任务时只有一个成功。
5. 重复 RocketMQ 消息只被业务处理一次。
6. Outbox 与任务/execution 状态同事务提交或回滚。
7. worker 崩溃、租约过期、回收、重试和接管。
8. 权限请求和决定能够关联至对应 execution。
9. RocketMQ 暂不可用与恢复后不丢失任务或执行审计事件。
10. 任务、通信和权限日志能够通过 `event_id`、`task_id`、`execution_id`、`attempt` 关联查询。

验收标准：

- 不再依赖 PostgreSQL `LISTEN/NOTIFY` 唤醒 agent。
- 不再通过本地 asyncio task 作为独立 worker 的唯一启动方式。
- 任意任务都能从发布追踪到最终状态。
- 每次 attempt 都有唯一且完整的 execution 闭环。
- 消息重复、延迟、重试和 worker 崩溃不会导致重复执行或永久丢失。
- PostgreSQL、RocketMQ、Worker 均可独立重启。
- 迟到的旧 execution 结果不会覆盖当前有效 execution。
- 日志可通过 `event_id/task_id/execution_id/attempt` 关联完整执行链路。

## 默认假设

- `task_id` 是逻辑 sub task 的稳定标识，不代表一次执行。
- `execution_id` 标识一次逻辑执行；同一逻辑执行的重试沿用该 ID。
- `(execution_id, attempt)` 是一次执行 attempt 的审计关联键和闭环主键。
- `attempt` 从 1 开始，在同一个 `execution_id` 下递增且不复用。
- 用户主动重新运行已结束任务时生成新的 `execution_id`。
- 任务和执行状态以 PostgreSQL 为准；RocketMQ 为可靠异步分发层。
- 使用至少一次投递 + 消费幂等，不把 RocketMQ 事务消息作为跨 PostgreSQL 的一致性主机制。
- v1 记录任务、通信和权限审计，不保存完整模型上下文及全部工具输出。
- Worker 池由预先启动的独立进程组成；`spawn_sub_agent` 负责创建逻辑 agent，不负责直接创建容器。
- 默认消息保留期和重试次数通过环境变量配置，任务生命周期事件的保留期长于普通通信消息。
