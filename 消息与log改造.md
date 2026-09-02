# Agent 通信、执行闭环与审计改造方案

## 总结

将现有 PostgreSQL `LISTEN/NOTIFY + agent_messages` 改为 RocketMQ 通信，同时保留 PostgreSQL 作为任务、执行状态和审计事实源。

每次 sub task 被认领都创建独立执行实例。`task_id` 只表示逻辑任务，执行审计、状态更新和消息关联统一使用 `(execution_id, attempt)`，避免重试、租约过期和迟到结果混淆。

核心原则：

- PostgreSQL 事务负责状态变更与 Outbox 事件写入。
- RocketMQ 负责异步分发、唤醒和跨进程通信。
- 消费采用至少一次投递，使用 `event_id` 幂等去重。
- 所有 Agent Runtime 使用同一基础运行时，由共享 worker 池执行。
- Agent Runtime 根据任务动态加载和卸载 Agent Card，而非预置固定身份或能力。
- 任务、通信和权限请求均形成可查询的审计闭环。
- 旧 `agent_messages` 表停止写入，仅保留历史查询。

## 执行模型

新增 `task_executions`：

```text
execution_id UUID
attempt INT
task_id TEXT
thread_id TEXT
runtime_id TEXT
agent_card_id TEXT
agent_card_version TEXT
agent_card_digest TEXT
agent_card_config_hash TEXT
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
- 租约过期时，调度器将当前 attempt 关闭为 `expired`，再允许新的 Runtime 使用同一 `execution_id` 创建下一 attempt。
- 旧 attempt 的迟到心跳、完成或失败请求不得修改任务当前状态；记录 `execution.stale_update_rejected` 审计事件。
- 任务最终状态由最新有效的 `(execution_id, attempt)` 决定；逻辑任务完成后不再创建新 attempt。
- 同一个 `execution_id` 的所有 attempt 必须使用完全相同的 Agent Card 快照，保证重试结果可审计和可复现。
- 用户主动重新运行已经结束的逻辑任务时，才重新解析可用 Agent Card，并生成新的 `execution_id`。

将 `tasks.metadata.retry_count` 迁移为明确的 `current_attempt`、`current_execution_id` 等列；保留旧 metadata 只作兼容读取。

## Agent Card 模型

本文中的 Agent Runtime 和 Agent Card 是逻辑概念，不假定 Kubernetes、容器或镜像替换等具体部署架构。

Agent Runtime 是所有 worker 共用的基础执行环境，只提供任务调度、模型调用、隔离、权限拦截、日志和消息能力。它不携带固定角色、system prompt、业务工具或技能。

Agent Card 是可版本化、可校验的能力包，定义一次任务执行中 agent 的身份和可见能力：

```text
agent_card_id
version
status: active/deprecated/revoked
task_types
system_prompt
tool_allowlist
skill_allowlist
runtime_config
bundle_ref
bundle_digest
config_hash
created_at
```

约束：

- 已发布 Card 的 `version`、内容和 `bundle_digest` 不可修改；变更必须发布新版本。
- Runtime 采用 load on claim：不预加载业务 Card；只有 PostgreSQL 原子认领成功并提交后，才为该 attempt 拉取、校验和装载已冻结的 Card。
- Runtime 只加载已批准、`active` 且与自身 `runtime_config` 兼容的 Card。
- Card 只能缩小 Runtime 的默认权限边界，不能自行授予绕过全局权限中间件、文件隔离或密钥策略的能力。
- Card 内的 system prompt、工具白名单、skill 白名单和运行参数只在当前 execution attempt 可见。
- Card 加载失败、校验失败或已被撤销时，当前 attempt 以明确原因失败，不得静默降级到其他 Card。

### 任务选卡与快照

DAG 中每个任务新增：

```text
task_type TEXT NOT NULL
work_shard INT NOT NULL
card_selector JSONB
```

- `task_type` 是任务能力分类，例如 `code.implementation`、`code.review` 或 `research`.
- `work_shard` 是任务就绪 LiteTopic 的稳定分片号；发布 DAG 时按该 `task_type` 的分片数计算并持久化，用于消息路由和 PostgreSQL 原子认领。
- `card_selector` 可显式指定 `agent_card_id + version`，或声明所需能力；未显式指定时，Card Registry 按 `task_type` 解析默认 Card。
- Registry 对每个 selector 必须解析出唯一的 `active` Card；零个或多个匹配均为 `card_resolution_failed`，任务不得进入执行。
- 任务认领事务内只解析并冻结 Card 快照：将 `agent_card_id`、`agent_card_version`、`agent_card_digest` 和 `agent_card_config_hash` 写入首次 attempt；该事务不得拉取 bundle、修改 Runtime prompt 或装载工具、skill。
- 认领事务提交成功后，认领到任务的 Runtime 才进入 `loading`，按冻结的 `bundle_ref` 拉取 Card，校验 `bundle_digest` 和 `config_hash`，再注入 system prompt、工具、skill 和运行参数。
- Card 装载成功前不得开始模型调用、工具调用或向下游声明 `execution.started`；装载成功后才将 attempt 转为 `running`。
- Card 装载失败时，当前 attempt 以 `card_load_failed` 原因结束；必须先清理任何部分装载状态，再按重试策略创建下一 attempt 或将任务终结，不能改选其他 Card。
- 同一 `execution_id` 的重试沿用首次 attempt 的 Card 快照，不因默认 Card 更新而漂移。
- 新的用户重跑生成新的 `execution_id`，重新解析 Card；如果旧 Card 已撤销，不允许继续重试该 execution。

Card 的解析、加载、卸载和异常状态必须写入 execution 审计事件，以支持回放时恢复对应的身份、prompt、工具和 skill 视图。

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
  "sender": "runtime-1",
  "target": "lead-or-execution-id",
  "correlation_id": "uuid",
  "payload": {}
}
```

`execution_id` 与 `attempt` 对执行期消息为必填；任务发布、Runtime 唤醒等执行前事件可为空。定向执行期消息使用 `execution_id` 作为目标，不依赖固定 sub agent 名称。

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
  - 任务就绪 LiteTopic：`ready.<task_type>.s<work_shard>`。
  - `task_available`：表示对应 `task_type + work_shard` 中可能存在可认领任务。
  - 定向控制 LiteTopic：`runtime.<runtime_id>`。
  - `permission_response`、`runtime_wakeup`、`execution_cancel`：只发送至对应 Runtime 的定向控制 LiteTopic。
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

消息类型使用 RocketMQ tag 过滤；消息 key 优先使用 `task_id`、`execution_id`、`request_id` 或 `runtime_id`，保证同一业务对象的顺序性。

### LiteTopic 任务分片与订阅

所有 worker 都是可执行任意 `task_type` 的通用 Runtime，因此采用一个共享消费池，而非按角色为 Runtime 静态分配 LiteTopic。

- 有 `M` 种 `task_type`，每种类型配置相同或独立的 `N` 个 `work_shard`；`agent-command` 下共有 `M x N` 个任务就绪 LiteTopic。
- 一个任务的分片由稳定规则确定，例如 `work_shard = hash(task_id) % N`。任务进入 `pending` 且依赖满足时，Lead 通过 Outbox 向 `ready.<task_type>.s<work_shard>` 发送 `task_available`。
- 所有 Runtime 的工作消费者使用同一个消费组 `GID-agent-runtime-pool`，统一绑定 `agent-command` 的任务就绪 LiteTopic 集合；不为每个 Runtime 手工维护 `M x N` 条订阅。
- 同一 LiteTopic 中的 `task_available` 只由消费组内一个消费者处理，不会广播给所有 idle Runtime；不同 LiteTopic 可由不同 Runtime 并行处理。
- 一个 `task_available` 是“该分片有工作”的提示，不是对某个 Runtime 的任务所有权授予。消息可携带 `task_id` 作为去重和观测线索，但 Runtime 必须以 PostgreSQL 原子认领结果为准。
- 定向控制消息与任务就绪消息使用独立消费者：每个 Runtime 额外使用仅订阅 `runtime.<runtime_id>` 的控制消费者，避免共享工作消费组错误消费其他 Runtime 的权限响应或取消命令。
- `N` 决定同一 `task_type` 的最大消息分片并行度，不决定实际同时执行数。实际执行并发受 idle Runtime 数、全局配额和每类任务配额共同限制。
- `N` 应按预期峰值并发、顺序约束和运维成本配置；v1 以 `64` 为默认值，并允许按 `task_type` 覆盖。

`task_available` envelope 示例：

```json
{
  "event_id": "uuid",
  "event_type": "task_available",
  "thread_id": "session-id",
  "task_id": "task-id",
  "execution_id": null,
  "attempt": null,
  "sender": "lead",
  "target": null,
  "correlation_id": "uuid",
  "payload": {
    "task_type": "code.implementation",
    "work_shard": 3,
    "task_version": 1
  }
}
```

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

- 发布 DAG：任务、依赖关系、稳定计算的 `work_shard` 与 `task.published` 事件同一事务提交；当任务已满足依赖时，同时写入对应 `task_available` Outbox 事件。
- 上游任务完成：下游任务的 `blocked_by_count` 变为零时，在同一事务内写入对应 `task_available` Outbox 事件。
- 认领任务：Runtime 收到 `task_available` 后，在 PostgreSQL 原子认领事务中选择同一 `thread_id + task_type + work_shard` 的一个 ready task。事务内将任务更新为 `in_progress`、冻结 Agent Card 快照、创建或递增 execution attempt、将 Runtime 更新为 `loading`，并写入 `execution.claimed`、`card.resolved` 及对应 Outbox 事件；事务提交成功才算认领成功。
- 加载 Card：认领提交后，Runtime 以独立步骤写入 `card.load_started`，拉取并校验冻结的 Card，再完成换装。加载成功后，以独立事务将 execution 更新为 `running`，并写入 `card.loaded`、`execution.started`。
- 加载失败：Runtime 将当前 attempt 更新为 `failed`，写入 `card.load_failed`、`execution.failed` 和重试或终结所需 Outbox 事件；随后卸载部分状态。该 attempt 不得开始执行用户任务。
- 心跳续租：更新 `last_heartbeat` 和租约，写入 `execution.heartbeat`。
- 完成/失败/回收：任务与 execution 状态变更、`card.unload_started` 和对应生命周期事件同一事务提交。
- Card 卸载：Runtime 以独立步骤卸载 Card；成功后写入 `card.unloaded` 并转为 `idle`，失败后写入 `card.unload_failed` 并将 Runtime 隔离，直至清理成功或人工处置。
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

任务就绪消息的确认规则：

1. Runtime 收到 `task_available` 后，在 PostgreSQL 中原子认领同一分片的一个任务。
2. 认领成功并提交后确认该消息；后续 Card 装载和执行失败由 execution 状态机和新的 Outbox 事件处理，不重新投递已确认的原始就绪消息。
3. 分片中已无可认领任务，或消息指向的任务已被其他 Runtime 认领、取消或完成时，直接确认消息。
4. 数据库暂时不可用或认领事务无法确定是否提交时，不确认消息并按消费重试处理。

## Runtime 与任务接口

所有 worker 都运行同一种通用 Agent Runtime。Runtime 注册其健康状态、运行时版本和当前执行，但不注册固定业务能力；任意空闲 Runtime 都能认领任务，并采用 load on claim，在认领成功后按该任务冻结的 Card 快照动态装载所需身份与能力。

新增 `agent_runtimes`：

```text
agent_runtimes
- runtime_id
- status: starting/idle/loading/running/unloading/stopped
- runtime_version
- current_execution_id
- current_attempt
- last_heartbeat
- lease_expires_at
- started_at
```

Runtime 启动入口：

```bash
python -m worker --runtime-id runtime-001
```

Runtime 行为：

- 注册并定期续租。
- 使用共享消费组 `GID-agent-runtime-pool` 消费 `agent-command` 的任务就绪 LiteTopic；同一 LiteTopic 只分配给一个 Runtime，不向所有 idle Runtime 广播同一就绪消息。
- 使用独立控制消费者订阅自身的 `runtime.<runtime_id>` LiteTopic，接收权限响应、唤醒和取消命令。
- 收到就绪消息后，从对应 `task_type + work_shard` 在 PostgreSQL 原子认领一个任务，解析或读取固定的 Card 快照，并创建或递增 execution attempt。
- 原子认领事务将 Runtime 置为 `loading`，该状态只在事务提交成功后生效；随后拉取、校验并加载 Card 定义的 system prompt、工具集合、skill 集合和运行参数，加载完成后才可开始模型调用。
- Card 加载属于认领之后的独立步骤，不占用认领数据库事务，也不会在 Runtime 空闲时预加载任何业务 Card。
- 仅将 Card 允许的工具和 skill 暴露给当前 attempt；全局权限、隔离和密钥策略始终由 Runtime 强制执行。
- 监听 `agent-command` 获取唤醒、权限响应和取消命令。
- 定向执行期消息按 `execution_id` 路由；Runtime 接管未完成 execution 后继续消费其未确认消息。
- 在 attempt 结束后将 Runtime 置为 `unloading`，卸载 Card 并清理 Card 注入的 prompt、工具、skill 和临时状态；只有卸载成功后才可转为 `idle` 并认领下一任务。
- Runtime 崩溃后由租约过期机制释放任务；其他 Runtime 可接管并按 Card 快照恢复下一 attempt。
- Runtime 定期扫描 PostgreSQL 可执行任务，作为 RocketMQ 通知丢失或暂时不可用时的最终补偿机制。

调度接口调整：

```python
claim_next_available_task(
    thread_id: str,
    runtime_id: str,
    task_type: str,
    work_shard: int,
) -> ClaimedExecution

renew_lease(
    task_id: str,
    execution_id: str,
    attempt: int,
    runtime_id: str,
) -> bool

complete_execution(
    task_id: str,
    execution_id: str,
    attempt: int,
    runtime_id: str,
    result: ExecutionResult,
) -> bool

fail_execution(
    task_id: str,
    execution_id: str,
    attempt: int,
    runtime_id: str,
    error: str,
) -> RetryDecision
```

完成、失败和续租 SQL 必须同时校验：

- `task_id`
- `current_execution_id`
- `current_attempt`
- `runtime_id` 或当前 owner
- execution 尚未终止

`spawn_sub_agent` 改为创建或激活通用 Runtime 的工作请求，不再绑定固定角色或固定能力；Runtime 在每次任务认领成功后按冻结的 Card 快照加载相应能力。

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
runtime_id
agent_card_id
agent_card_version
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
- `card.resolved`
- `card.load_started`
- `card.loaded`
- `card.load_failed`
- `card.unloaded`
- `card.unload_failed`

新增消息投递审计，记录 `event_id`、消费者、投递/消费状态、重试次数和错误信息。

保留 `permission_audit_log`，并使每次权限申请与决定关联对应 execution；无执行上下文的 lead 权限操作允许 execution 字段为空。

## 迁移策略

- 初始化 RocketMQ topic、消费组、Outbox、消费去重、Runtime 注册、Card Registry 和 execution 表。
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
4. 多 Runtime 并发认领同一任务时只有一个成功。
5. 重复 RocketMQ 消息只被业务处理一次。
6. Outbox 与任务/execution 状态同事务提交或回滚。
7. Runtime 崩溃、租约过期、回收、重试和接管。
8. 权限请求和决定能够关联至对应 execution。
9. RocketMQ 暂不可用与恢复后不丢失任务或执行审计事件。
10. 任务、通信和权限日志能够通过 `event_id`、`task_id`、`execution_id`、`attempt` 关联查询。
11. 任意空闲 Runtime 均能认领不同 `task_type` 的任务，并动态加载对应 Card。
12. Runtime 只能看到当前 Card 白名单内的 system prompt、工具和 skill。
13. 同一 execution 重试时使用相同的 Card 版本、digest 和配置哈希；人工重跑才重新选卡。
14. Card 校验或加载失败时不执行任务，并记录可审计失败原因。
15. attempt 结束后 Card 已卸载，下一任务不会继承前一任务的 prompt、工具、skill 或临时状态。
16. 同一 `task_available` 不会广播给所有 idle Runtime；同一 LiteTopic 同时只被共享消费组中的一个 Runtime 消费。
17. 同一 `task_type` 的不同 `work_shard` 可被不同 Runtime 并行认领，且每个 Runtime 都可认领任意 `task_type`。
18. Runtime 只在 PostgreSQL 认领事务提交成功后开始加载 Card；认领失败或回滚时不得修改本地 prompt、工具、skill 或临时状态。
19. Card 加载失败后，原始 `task_available` 已确认但当前 attempt 被正确终结或重试；不会开始模型调用，也不会改选其他 Card。

验收标准：

- 不再依赖 PostgreSQL `LISTEN/NOTIFY` 唤醒 agent。
- 不再通过本地 asyncio task 作为独立 worker 的唯一启动方式。
- 任意任务都能从发布追踪到最终状态。
- 每次 attempt 都有唯一且完整的 execution 闭环。
- Runtime 不携带固定业务身份或能力，且可在不同任务间动态切换 Agent Card。
- 所有通用 Runtime 共享任务就绪 LiteTopic 消费池；同一任务就绪消息不会广播给所有 idle Runtime。
- Agent Card 仅在 PostgreSQL 认领提交成功后加载，加载成功前不得执行任务。
- Card 的解析、版本、内容摘要、加载和卸载结果均可关联至 execution attempt。
- 消息重复、延迟、重试和 Runtime 崩溃不会导致重复执行或永久丢失。
- PostgreSQL、RocketMQ、Runtime 均可独立重启。
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
- Runtime 池由预先启动的通用执行单元组成；具体进程、线程或其他部署形态不属于本方案约束。
- Agent Card 是运行时可加载的逻辑能力包，不等同于容器、Pod 或镜像。
- Runtime 不预加载业务 Card；Card 快照在认领事务内冻结，实际加载在认领提交后的 `loading` 状态完成。
- Runtime 每次只执行一个 attempt，且在 attempt 结束后完成 Card 卸载；并发通过多个 Runtime 扩展。
- `agent-command` 的任务就绪 LiteTopic 命名为 `ready.<task_type>.s<work_shard>`；每种 `task_type` 默认 `64` 个分片，任务以 `hash(task_id) % work_shard_count` 稳定路由。
- 任务就绪 LiteTopic 由所有 Runtime 的共享消费组消费；每个 Runtime 另行订阅自身 `runtime.<runtime_id>` 控制 LiteTopic。
- 默认消息保留期和重试次数通过环境变量配置，任务生命周期事件的保留期长于普通通信消息。
