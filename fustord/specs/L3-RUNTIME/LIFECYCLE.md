# L3: [runtime] Fustord Session Lifecycle

> Type: runtime_state_machine
> Layer: Stability Layer (SCP Implementation)

---

## 1. 状态机 (Session State Machine)

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

## 2. Leader 选举与故障转移

### 2.1 选举策略: 先到先得 (First-Come-First-Serve)
Leader 的选举完全由 fustord 端控制，采用非抢占式的锁机制。

- **获取锁**: 第一个连接到 View 的 Session 会立即获得 Leader 锁。
- **排队**: 后续连接的 Session 只能成为 Follower (Standby)。
- **释放锁**: 仅当 Leader Session 断开连接或显式销毁时，锁才会被释放。

### 2.2 故障转移 (Failover Promotion)
当 Leader 掉线时，fustord 会执行 **First Response Promotion**：

1. 检测到 Leader Session 终止。
2. 释放 View 的 Leader 锁。
3. 选择**第一个成功获取锁**的 Follower（基于 `try_become_leader` 返回顺序）。
4. 立即将其提拔为新的 Leader。

> **设计理由**：响应快的节点通常意味着网络延迟低、负载轻，更适合承担 Leader 的 Snapshot/Audit 任务。

### 2.3 Session Timeout Negotiation (超时协商)

Session 超时时间由 **Client-Hint + Server-Default** 共同决定：

1. **Client Hint**: sensord 在 `CreateSessionRequest` 中携带 `session_timeout_seconds` (通常来自本地配置)。
2. **Server Decision**: fustord 取 `max(client_hint, server_default)` 作为最终超时时间。
3. **Acknowledgment**: fustord 在响应中返回最终决定的 `session_timeout_seconds`，sensord **必须** 采纳此值作为心跳间隔的基准。

---

## 3. SCP 交互原则 (Interactions)

1.  **心跳检测**: fustord 采用被动等候模式。如果距离上次心跳超过 `timeout_seconds`，将 Session 切换为 `EXPIRED`。
2.  **指令下发**: 所有的业务指令（如 `start_audit`, `do_scan`, `upgrade`）都通过 **SCP** 心跳的 HTTP Response 进行“搭载”分发。

---

## 4. 资源清理原则 (Cleanup)

- **非 Live 视图**: Session 过期后，View 内部的数据条目**不得**物理删除，除非收到明确的 `on_closed` 事件。
- **Live 视图**: Session 过期即视为源不可达，物理清理内存中与该源相关的全部实时影子。

---

## 5. 故障隔离与降级 (Fault Tolerance)

### 5.1 级联故障规则 (Cascading Failure Rules)

- **Pipe 失败不影响其他 Pipe**: 每个 Pipe 独立运行，某个 Pipe 崩溃不影响同进程内的其他 Pipe
- **Receiver 失败 → 关联 FustordPipe 全部失败**: Receiver 是共享组件，其故障影响所有依赖它的 FustordPipe
- **FustordPipe 失败 → 中止下游视图**: FustordPipe 故障应中止其下游关联的 View 的数据写入。live 类型的视图一旦任一 FustordPipe 失败则不可用（保留应急 on-demand 查询服务），其他类型的默认继续可用。

### 5.2 Zombie Prevention (僵尸任务预防)

- **Liveness Probe**: Control Loop 不应仅检查 `task.done()`，还应检查 Task 是否在更新 `statistics` 或 `heartbeat` timestamp。
- **Timeout Kill**: 对于卡死 (Stuck) 的 Task，Control Loop 应主动 `cancel()` 并重启。
