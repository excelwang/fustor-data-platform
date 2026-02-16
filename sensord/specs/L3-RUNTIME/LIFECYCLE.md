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

### 2.3 Session Timeout Negotiation (超时协商)

Session 超时时间由 **Client-Hint + Server-Default** 共同决定：

1. **Client Hint**: sensord 在 `CreateSessionRequest` 中携带 `session_timeout_seconds` (通常来自本地配置)。
2. **Server Decision**: fustord 取 `max(client_hint, server_default)` 作为最终超时时间。
3. **Acknowledgment**: fustord 在响应中返回最终决定的 `session_timeout_seconds`，sensord **必须** 采纳此值作为心跳间隔的基准。

### 2.4 Error Recovery Strategy (错误恢复策略)

控制循环针对不同类型的异常采取差异化的恢复策略：

| 异常类型 | 策略 | 原因 |
|---------|------|------|
| `SessionObsoletedError` | **Immediate Retry** | 会话被服务端主动终结 (如 Leader Failover)，应立即重连以竞选新 Leader。 |
| `RuntimeError` | **Exponential Backoff** | 连接超时、配置错误等环境问题，快速重试会加重系统负担。 |
| `CancelledError` (+ STOPPING) | **Break** | 正常的停止流程。 |
| `CancelledError` (- STOPPING) | **Continue** | 单个 Task (如 Snapshot) 被取消，但 SensordPipe 仍需运行 (见 §2)。 |
| Background Task Crash | **Count & Retry** | 记录连续错误计数，触发 Backoff，等待下一轮循环重启 Task。

---

## 3. 审计上下文 (Audit Context)

`Audit Context` (mtime cache) 是 sensord 进程内的物理缓存，用于“True Silence”优化。

- **不持久化**: 此缓存仅存在于内存，sensord 重启后首轮执行物理全量审计是设计的鲁棒性边界。
- **强制失效**: 当 Session 重建或发生角色晋升时，Audit Context 必须被显式清空，以防止不同会话间的时间轴由于 Clock Drift 出现空隙。

---

## 4. 热文件监控 (Hot Watch Set)

### 4.1 逻辑保留
尽管 FS 驱动经过了重构，但 Hot Watch Set 的逻辑被严格保留：

- **扫描**: `FSScanner` 返回所有目录及其 mtime。
- **排序**: `FSDriver` 根据 mtime 倒序排列。
- **截断**: 仅对前 N 个（配置限制）活跃目录建立 inotify watch。
- **漂移计算**: 计算 NFS Server 与 sensord 本地时间的 `drift`，并在设置 Watch 时修正时间戳。

### 4.2 架构分离
- **Audit Interval**: 12 hours (Long cycle)
- **Sentinel Interval**: 5 minutes (Short cycle)
- **FSScanner**: 负责 IO 和遍历 (无状态)。
- **FSDriver**: 负责策略 (Policy)、排序 (Sorting) 和 调度 (Scheduling)。

---

## 5. 故障隔离与自愈 (Self-Healing)

### 5.1 核心原则
- **控制面优先**: 即使数据驱动（Data Plane）抛出 `RuntimeError`，**Sensord** 的 `Stability Layer` 必须接管该异常，将 Pipe 切回 `RECONNECTING` 或 `INITIALIZING` 状态，尝试重建。
- **环境隔离**: 审计过程（Audit）产生的物理 IO 密集型操作必须在独立的 `AuditContext` 中运行，不得阻塞实时事件。

### 5.2 角色切换响应
- **晋升 (Promotion -> Leader)**:
    1. **清空 Audit 缓存**: 确保当选后的首轮审计是物理上的全量对账。
    2. **触发 Snapshot**: 补齐作为 Follower 期间可能缺失的资产全貌。
- **降级 (Demotion -> Follower)**:
    - 立即停止正在进行的物理扫描任务（Snapshot/Audit），仅维持 inotify 实时监听。

### 5.3 故障分级处理表

| 故障层级 | 影响范围 | 自愈逻辑 |
|---------|----------|----------|
| **Task 级** (Scan/Audit) | 仅影响该周期的对账 | **Task 重启**: 控制循环捕获异常，经 Backoff 后在下一周期重新拉起。 |
| **信道级** (Transport/Auth) | 影响该 Pipe 的数据推送 | **Re-Session**: 自动断开当前会话并执行完整握手重连。 |
| **资源级** (inotify 耗尽) | 影响本节点所有相关 Pipe | **Component Reset**: 标记驱动为 Stale，触发 Driver 实例的重新初始化。 |
| **进程级** (OOM/Crash) | 影响整个 sensord 容器 | **Process Guard**: 依赖外部宿主环境 (systemd/k8s) 重新启动。 |

### 5.4 Degraded Mode (降级模式)

组件健康管理采用 **分层级联故障** 模型：

#### 最小粒度组件 (Health Unit)

以下组件是健康维护的最小粒度单元：

| 组件 | 层面 | 说明 |
|------|------|------|
| **Source** | sensord | 数据源驱动（如 FSDriver） |
| **SensordPipe** | sensord | 代理管道 |
| **Sender** | sensord | 上行发送通道 |

#### 级联故障规则 (Cascading Failure Rules)

- **Pipe 失败不影响其他 Pipe**: 每个 Pipe 独立运行，某个 Pipe 崩溃不影响同进程内的其他 Pipe
- **Source/Sender 失败 → 关联 SensordPipe 全部失败**: Source 或 Sender 是共享组件，其故障影响所有依赖它的 SensordPipe
- 失败后，组件应尝试通过自行重启恢复运行，并记录错误日志。健康状态中记录失败重启次数。

#### 可观测性要求

所有组件的健康状态必须提供观测手段：
- Heartbeat 响应中携带组件健康摘要
- API 接口提供组件级别的健康检查 (`/health/components`)
- 日志中明确标记组件状态转换事件

---

## 6. 远程任务调度 (Command Execution)

sensord 必须能够在任何状态下（包含 Follower 或 初始化中）响应来自 Consumer 的突发控制任务：

- **On-Command Find (`scan`)**: 执行特定路径的深层遍历。
- **结果分发**: 该类任务产生的事件通常直接交由 `Sender` 发送，可绕过内部 EventBus 环形缓冲区，以保证管控指令的执行不被积压的数据包阻塞。

---

## 7. 健康度观测 (Observability)

所有正在运行的组件必须能够实时导出其健康统计摘要：
- **心跳延时 (Heartbeat RTT)**: 衡量控制链路质量。
- **积压深度 (Bus Backlog)**: 衡量数据面处理瓶颈。
- **连续失败计数 (Error Count)**: 触发自动退避与保护性熔断。
