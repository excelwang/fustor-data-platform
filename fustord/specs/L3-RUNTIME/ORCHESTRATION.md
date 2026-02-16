---
version: 1.0.0
---

# L3: [pattern] Fustord Command Orchestration (SCP)

> Type: design_pattern
> Layer: Management Layer (SCP Dispatcher)

---

## [model] Resource_Renting_Model_Primitives

**Rationale**: Abstract complex management tasks into simple protocol-level "rents" for unicast or broadcast delivery.

在 **fustord** 中，所有的管理行为（升级、扫描、重启）统一抽象为对 **SCP (Datacast Control Protocol)** 寻址原语的租用。

### 1.1 广播寻址 (Broadcast)
- **Mechanism**: `ViewDriver.trigger_on_demand_scan(path)` -> `JobManager.create_job()` -> `PipeSessionStore.queue_command()`
- **语义**: 将 payload（通常是 `scan` 指令）下发给所有订阅了该 `view_id` 的 **Datacast**。
- **场景**: 强制全网扫描（On-Command Find）。

### 1.2 单播寻址 (Unicast)
- **Mechanism**: `PipeSessionStore.queue_command(session_id, payload)`
- **语义**: 精准触达指定的某个 **Datacast** 正在运行的特定 Session。
- **场景**: 灰度升级、特定节点的配置重载。

---

## [interface] Job_and_Bridge_Coordination

**Rationale**: Decouple task initiation from the delivery mechanism.

### 2.1 Job Manager
`JobManager` 负责管理跨 Session 的异步任务状态（如扫描进度）。
```python
class JobManager:
    async def create_job(self, view_id: str, path: str, sessions: List[str]) -> str: ...
    async def complete_job_for_session(self, view_id: str, session_id: str, path: str): ...
```

### 2.2 Pipe Session Bridge
`PipeSessionBridge` 负责维护单次 Session 的指令队列。
```python
class PipeSessionBridge:
    # 指令入队
    def queue_command(self, session_id: str, cmd: Dict):
        self.store.queue_command(session_id, cmd)
    
    # 心跳带出指令 (搭載响应模式)
    async def keep_alive(self, session_id: str) -> Dict:
        commands = self.store.get_and_clear_commands(session_id)
        return {"status": "ok", "commands": commands}
```

---

## [mechanism] Command_Piggyback_Dispatch_Model

由于 **Datacast** 与 **fustord** 之间基于 HTTP 的 Pull 模型（心跳由 Datacast 发起），指令分发采用 **搭载响应模式**：
1. **Producer**: 管理逻辑（如 `FSViewDriver`）调用 `pipe.session_bridge.store.queue_command()`。
2. **Buffer**: 通用 `session_id` 寻址，指令存在于 `PipeSessionStore` 的内存队列中。
3. **Consumer**: 当 **Datacast** 下一次 POST 心跳时，`keep_alive` 逻辑取出队列中的所有 Payload，作为响应体返回给 Datacast 执行。
