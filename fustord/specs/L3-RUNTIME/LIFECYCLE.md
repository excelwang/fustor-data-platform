---
version: 1.0.0
---

# L3: [workflow] Fustor 运行时行为与生命周期 (Runtime Behavior)

> Type: workflow | decision
> Layer: Domain Layer

> 版本: 1.0.0
> 日期: 2026-02-04
> 补充: CONSISTENCY.md

本文档记录了系统在运行时的动态行为细节，包括 Leader 选举、角色切换、审计缓存生命周期等。

## 1. Leader 选举与角色切换

### 1.1 选举策略: 先到先得 (First-Come-First-Serve)
Leader 的选举完全由 fustord 端控制，采用非抢占式的锁机制。

- **获取锁**: 第一个连接到 View 的 Session 会立即获得 Leader 锁。
- **排队**: 后续连接的 Session 只能成为 Follower (Standby)。
- **释放锁**: 仅当 Leader Session 断开连接或显式销毁时，锁才会被释放。

### 1.2 故障转移 (Failover Promotion)
当 Leader 掉线时，fustord 会执行 **First Response Promotion**：

1. 检测到 Leader Session 终止。
2. 释放 View 的 Leader 锁。
3. 选择**第一个成功获取锁**的 Follower（基于 `try_become_leader` 返回顺序）。
4. 立即将其提拔为新的 Leader。

> **设计理由**：响应快的节点通常意味着网络延迟低、负载轻，更适合承担 Leader 的 Snapshot/Audit 任务。此策略实现简单且自然地实现了负载均衡。

### 1.3 前置依赖：Sensord 角色切换行为 (Prerequisite: Sensord Role Switching)

当 sensord 从 Follower 晋升为 Leader 时：

| 动作 | 行为 | 原因 |
|------|------|------|
| **Pre-scan** | **Skip** | 实时消息同步 (`_message_sync`) 在 Follower 期间已启动，监控已就绪。 |
| **Snapshot Sync** | **Trigger** | 确保捕获 Follower 期间可能遗漏的存量状态（仅限 Session 内首次当选）。 |
| **Audit Cache** | **Clear** | **强制清空**审计缓存，确保当选后的第一次审计是**全量扫描** (建立新基准)。 |
| **Sentinel** | **Start** | 启动后台巡检任务。 |

### 1.4 状态标志语义: 瞬态 vs 单调

| 标志类型 | 示例 | 语义 | 适用场景 |
|---------|------|------|----------|
| **瞬态标志** (Transient) | `PipeState.MESSAGE_SYNC` | 表示某个 Task **当前正在运行**。Task 完成、异常退出或重建时，标志会短暂清除。 | 内部控制逻辑 (如是否需要重启 task) |
| **单调标志** (Monotonic) | `is_realtime_ready` | 表示记录 SensordPipe **曾经成功连接过** EventBus 并处于就绪状态。一旦设为 `True`，除非 SensordPipe 重启不然不会变回 `False`。 | 外部状态判断 (如 Heartbeat `can_realtime`) |

> [!CAUTION]
> 避免在外部监控或测试中使用瞬态标志作为"服务就绪"的判据，这会导致因 Task 重启窗口期引发的 Flaky Test。

### 1.5 Session Timeout Negotiation (超时协商)

Session 超时时间由 **Client-Hint + Server-Default** 共同决定：

1. **Client Hint**: sensord 在 `CreateSessionRequest` 中携带 `session_timeout_seconds` (通常来自本地配置)。
2. **Server Decision**: fustord 取 `max(client_hint, server_default)` 作为最终超时时间。
3. **Acknowledgment**: fustord 在响应中返回最终决定的 `session_timeout_seconds`，sensord **必须** 采纳此值作为心跳间隔的基准。



---

## 2. 状态机 (Session State Machine)

fustord 为每一个 **Sensord** 的租赁请求维护一个 Session 状态：

```mermaid
state_machine
    [*] --> ESTABLISHING: POST /session
    ESTABLISHING --> ACTIVE: Handshake success
    ACTIVE --> STALE: Heartbeat missed once
    STALE --> ACTIVE: Heartbeat recovered
    STALE --> EXPIRED: Heartbeat timeout exceeded
    ACTIVE --> TERMINATED: DELETE /session
    EXPIRED --> TERMINATED: Cleanup routine
    TERMINATED --> [*]
```

---

## 3. 心跳与脐带 (SCP Umbilical Cord)

1.  **心跳检测**: fustord 采用被动等候模式。如果距离上次心跳超过 `timeout_seconds`，将 Session 切换为 `EXPIRED`。
2.  **指令下发**: 所有的业务指令（如 `start_audit`, `do_scan`, `upgrade`）都通过 **SCP** 心跳的 HTTP Response 进行“搭载”分发。

---

## 4. 资源清理原则 (Cleanup)

- **非 Live 视图**: Session 过期后，View 内部的数据条目**不得**物理删除，除非收到明确的 `on_closed` 事件。
- **Live 视图**: Session 过期即视为源不可达，物理清理内存中与该源相关的全部实时影子。

---

## 5. 故障隔离模型 (Fault Tolerance Model)

### 5.1 级联故障规则 (Cascading Failure Rules)

组件健康管理采用 **分层级联故障** 模型：

- **Receiver 失败 → 关联 FustordPipe 全部失败**: Receiver 是共享组件，其故障影响所有依赖它的 FustordPipe。
- **FustordPipe 失败 → 中止下游视图**: FustordPipe 故障应中止其下游关联的 View 的数据写入。
    - **Live View**: 一旦任一 FustordPipe 失败则不可用（保留应急 on-demand 查询服务）。
    - **Persisted View**: 默认继续可用，标记数据源为 Stale。

### 5.2 组件自愈 (Component Self-Healing)

1.  **重启尝试**: 失败后，Receiver/Pipe 组件应尝试通过自行重启恢复运行。
2.  **错误记录**: 必须记录错误日志，并在健康状态中统计失败重启次数。
3.  **僵尸预防 (Zombie Prevention)**: Control Loop 应主动检测卡死 (Stuck) 的 Task 并执行 `cancel()` 重启。


