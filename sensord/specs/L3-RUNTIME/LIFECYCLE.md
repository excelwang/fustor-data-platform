# L3: [workflow] [sensord] Runtime Lifecycle & Self-Healing

> Type: workflow | decision
> Layer: Stability Layer (SCP Implementation)

---

## 1. 状态机 (SensordPipe State Machine)

每一个数据流动管道（SensordPipe）在 **Sensord** 内部拥有独立的生命周期：

```mermaid
state_machine
    [*] --> INITIALIZING
    INITIALIZING --> CONNECTING: Config Loaded
    CONNECTING --> SYNCING: SCP Handshake Success
    SYNCING --> RECONNECTING: Network Error / Timeout
    RECONNECTING --> CONNECTING: Backoff Wait Done
    SYNCING --> CLOSING: SIGTERM / SIGHUP Removed
    CLOSING --> [*]
```

---

## 2. 心跳与生存 (Heartbeat & Survival)

**Sensord** 与消费者之间的 SCP 心跳是生存决策的核心机制：

### 2.1 核心不变量 (Invariant)
**心跳循环 (Heartbeat Tunnel) 必须永续运行**。只要进程存活且任务启用，心跳循环永远不应因业务 Task (如 Audit/Scan) 的崩溃而终止。

### 2.2 脐带功能 (Umbilical Functions)
1.  **生存权置换**: 消费者在心跳响应中下发指令。若心跳中断超过 `timeout_seconds`，Sensord **必须** 停止该 Session 下的数据推送并进入 `RECONNECTING` 状态。
2.  **Audit 触发器**: 长期运行的 Session 如果缺失心跳对账，其产生的数据被视为“非审计数据”，消费者有权在审计周期结束后将其丢弃。
3.  **状态隐式上报**: 通过 `can_realtime` 等健康位隐式告知 Consumer 当前数据面的就绪状态。
4.  **退避策略**: 发生连接错误时必须执行指数级退避（Exponential Backoff），严禁因重连失败而彻底退出。

---

## 3. 审计上下文 (Audit Context)

`Audit Context` (mtime cache) 是 sensord 进程内的物理缓存，用于“True Silence”优化。

- **不持久化**: 此缓存仅存在于内存，sensord 重启后首轮执行物理全量审计是设计的鲁棒性边界。
- **强制失效**: 当 Session 重建或发生角色晋升时，Audit Context 必须被显式清空，以防止不同会话间的时间轴由于 Clock Drift 出现空隙。

---

## 4. 故障隔离与自愈 (Self-Healing)

### 4.1 核心原则
- **控制面优先**: 即使数据驱动（Data Plane）抛出 `RuntimeError`，**Sensord** 的 `Stability Layer` 必须接管该异常，将 Pipe 切回 `RECONNECTING` 或 `INITIALIZING` 状态，尝试重建。
- **环境隔离**: 审计过程（Audit）产生的物理 IO 密集型操作必须在独立的 `AuditContext` 中运行，不得阻塞实时事件。

### 4.2 角色切换响应
- **晋升 (Promotion -> Leader)**:
    1. **清空 Audit 缓存**: 确保当选后的首轮审计是物理上的全量对账。
    2. **触发 Snapshot**: 补齐作为 Follower 期间可能缺失的资产全貌。
- **降级 (Demotion -> Follower)**:
    - 立即停止正在进行的物理扫描任务（Snapshot/Audit），仅维持 inotify 实时监听。

### 4.3 故障分级处理表

| 故障层级 | 影响范围 | 自愈逻辑 |
|---------|----------|----------|
| **Task 级** (Scan/Audit) | 仅影响该周期的对账 | **Task 重启**: 控制循环捕获异常，经 Backoff 后在下一周期重新拉起。 |
| **信道级** (Transport/Auth) | 影响该 Pipe 的数据推送 | **Re-Session**: 自动断开当前会话并执行完整握手重连。 |
| **资源级** (inotify 耗尽) | 影响本节点所有相关 Pipe | **Component Reset**: 标记驱动为 Stale，触发 Driver 实例的重新初始化。 |
| **进程级** (OOM/Crash) | 影响整个 sensord 容器 | **Process Guard**: 依赖外部宿主环境 (systemd/k8s) 重新启动。 |

---

## 5. 远程任务调度 (Command Execution)

sensord 必须能够在任何状态下（包含 Follower 或 初始化中）响应来自 Consumer 的突发控制任务：

- **On-Command Find (`scan`)**: 执行特定路径的深层遍历。
- **结果分发**: 该类任务产生的事件通常直接交由 `Sender` 发送，可绕过内部 EventBus 环形缓冲区，以保证管控指令的执行不被积压的数据包阻塞。

---

## 6. 健康度观测 (Observability)

所有正在运行的组件必须能够实时导出其健康统计摘要：
- **心跳延时 (Heartbeat RTT)**: 衡量控制链路质量。
- **积压深度 (Bus Backlog)**: 衡量数据面处理瓶颈。
- **连续失败计数 (Error Count)**: 触发自动退避与保护性熔断。


