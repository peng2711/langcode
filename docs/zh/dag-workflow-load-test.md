# DAG 与 Agent 控制链路压测设计

## 目标与边界

本方案衡量 LangCode 自己实现的 Agent 控制链路：

```text
Lead Agent 发布菱形 DAG
  -> Message Hub 持久化消息并发送 PostgreSQL NOTIFY
  -> Sub Agent 空闲时消费通知
  -> FOR UPDATE SKIP LOCKED 原子认领
  -> 模拟任务执行并按比例续租
  -> 完成任务并原子解锁下游
  -> 定时回收模拟崩溃 Agent 的过期租约
```

压测不调用外部 LLM，也不执行文件或 Shell 工具。因此报告中的
`Workflow/claim_execute_complete` 是控制面任务周期，不是完整 AI 任务耗时，不能表述为
“Agent Team 每秒完成多少真实编码任务”。

## 负载模型

- 两个固定 Locust User 分别模拟 Lead Agent 和 CLI 租约监控器。
- 其余 User 模拟 Sub Agent；例如 `USERS=102` 表示 100 个 Sub Agent。
- 初始 DAG 为 `root -> 4 parallel workers -> join`，共 6 个任务和 8 条边。
- Agent 首先认领任务，等待 10–50 ms 模拟执行，再完成并解锁下游。
- 每 20 个完成任务执行一次 heartbeat；生产代码中真实周期为 25 秒。
- Lead Agent 默认每 5 秒发布一个新 DAG；通知使用一次集合插入为每个 Agent
  保留独立持久消息，并通过一次广播 `NOTIFY` 唤醒监听者。
- 回收器默认每 10 秒注入并回收 10 个过期租约，且只操作本次 `thread_id`。

## 场景与命令

原子认领容量基线：

```bash
LOAD_SCENARIO=claim USERS=2000 SPAWN_RATE=100 RUN_TIME=10m \
  LOAD_SEED_TASKS=1000000 ./scripts/load_test_dag.sh
```

端到端控制链路冒烟（10 个 Sub Agent）：

```bash
LOAD_SCENARIO=workflow USERS=12 SPAWN_RATE=6 RUN_TIME=30s \
  WORKFLOW_SEED_DAGS=100 ./scripts/load_test_dag.sh
```

端到端控制链路基线（100 个 Sub Agent）：

```bash
LOAD_SCENARIO=workflow USERS=102 SPAWN_RATE=25 RUN_TIME=5m \
  WORKFLOW_SEED_DAGS=50000 ./scripts/load_test_dag.sh
```

端到端控制链路压力测试（500 个 Sub Agent）：

```bash
LOAD_SCENARIO=workflow USERS=502 SPAWN_RATE=50 RUN_TIME=10m \
  WORKFLOW_SEED_DAGS=100000 ./scripts/load_test_dag.sh
```

2000 Agent 极限测试应使用 `USERS=2002`。批量通知已经消除逐 Agent 事务和重复
`NOTIFY`，但该档位仍需要确认 PostgreSQL 连接容量、监听连接数量与磁盘空间。

## 指标口径

- `LeadAgent/publish_dag`：任务和依赖边落库。
- `LeadAgent/notify_agents`：批量持久化每个 Agent 的独立消息并广播一次 NOTIFY。
- `LeadAgent/publish_dag_end_to_end`：一次发布加完整通知扇出。
- `MessageHub/consume_inbox`：Agent 空闲后的消费式收件箱读取。
- `DAGScheduler/claim_next_task`：原子认领成功请求。
- `DAGScheduler/claim_next_task/empty`：当前没有 ready task；它不是系统错误。
- `DAGScheduler/renew_lease`：心跳续租。
- `DAGScheduler/complete_and_unlock`：完成任务并更新下游计数。
- `DAGScheduler/reclaim_expired_leases`：过期租约回收。
- `Workflow/claim_execute_complete`：认领、模拟执行、完成与解锁的控制面周期。

不要使用 Locust 的 `Aggregated` QPS 作为任务吞吐，因为一个任务周期会产生多条组件指标。
任务吞吐应读取 `Workflow/claim_execute_complete`，调度容量应读取
`DAGScheduler/claim_next_task`。

## 正确性门槛

- `invalid_completed_tasks` 必须为 0。
- Locust 组件 Failure Count 必须为 0；容量探索时可单独记录 PoolTimeout。
- `completed + pending + in_progress + failed` 必须等于 summary 的 `total_tasks`。
- DAG 耗尽后的 `claim_next_task/empty` 属于数据供给不足，应增加 `WORKFLOW_SEED_DAGS`，
  不能当成调度吞吐。
- 对比不同规模时保持 DAG 宽度、模拟执行时间、连接池和通知 fan-out 配置一致。

## 已验证结果

同一台本地机器、100 个 Sub Agent、5 分钟、5 万个初始菱形 DAG 的前后对比：

| 指标 | 串行通知基线 | 批量通知 | 变化 |
|---|---:|---:|---:|
| 控制面任务吞吐 | 596.84 tasks/s | 621.00 tasks/s | +4.0% |
| 控制面周期平均延迟 | 130.93 ms | 125.26 ms | -4.3% |
| 控制面周期 P95 / P99 | 170 / 210 ms | 170 / 200 ms | P99 -4.8% |
| 100 Agent 通知平均延迟 | 5708.27 ms | 64.63 ms | -98.9% |
| 100 Agent 通知 P95 / P99 | 6800 / 6900 ms | 93 / 110 ms | P95 约快 73 倍 |
| 发布加通知 P95 / P99 | 6900 / 7100 ms | 230 / 270 ms | P95 约快 30 倍 |

优化后共记录 186,145 个完整控制面任务周期，Failure Count 为 0；停止后的数据库
快照包含 300,588 个任务，`invalid_completed_tasks=0`，重复任务 ID 为 0。测试使用
10–50 ms 模拟执行时间，不含 LLM 和工具执行，因此这些数字只代表 Agent 控制面能力。

每次 workflow 测试还会输出 `workflow_<run>_summary_<pid>.json`，用于核对任务状态、
依赖边、未读消息和依赖一致性。
