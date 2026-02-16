---
version: 1.0.0
---

# L3: [pattern] Fustord Command Orchestration (SCP)

> Type: design_pattern
> Layer: Management Layer (SCP Dispatcher)

---

## [model] Resource_Renting_Model_Primitives

**Rationale**: Abstract complex management tasks into simple protocol-level "rents" for unicast or broadcast delivery.

在 **fustord** 中，所有的管理行为（升级、扫描、重启）统一抽象为对 **SCP (Sensord Control Protocol)** 寻址原语的租用。

### 1.1 广播寻址 (Broadcast)
- **API**: `SessionManager.broadcast(payload, view_id=None)`
- **语义**: 将 payload 下发给所有关联了该 `view_id` 的 **Sensord**。若未指定 `view_id`，则广播至全量活跃节点。
- **场景**: 强制全网扫描（On-Command Find）。

### 1.2 单播寻址 (Unicast)
- **API**: `SessionManager.unicast(payload, sensord_id)`
- **语义**: 精准触达指定的某个 **Sensord**。
- **场景**: 灰度升级、特定节点的配置重载。

---

## [interface] Task_Orchestrator_Interface_Definition

**Rationale**: Provide a clean service boundary for the management layer to interact with the stability layer's session pool.

```python
# Task Orchestrator interface
class ITaskOrchestrator:
    async def view_broadcast(self, view_id: str, cmd: Dict): ...
    async def sensord_targeted_dispatch(self, sensord_id: str, cmd: Dict): ...
```

建议封装通用的 `TaskOrchestrator` 服务，隔离分发细节：

```python
class TaskOrchestrator:
    async def view_broadcast(self, view_id: str, cmd: Dict) -> List[Dict]:
        """
        全量广播逻辑：用于回退扫描。
        1. 确定 ViewID 关联的所有 Session
        2. Stability.broadcast()
        3. 汇聚结果
        """
        pass

    async def sensord_targeted_dispatch(self, sensord_id: str, cmd: Dict) -> Dict:
        """
        精准选路逻辑：用于升级/停止。
        1. 确定 sensordID 关联的 Session (优先 Leader)
        2. Stability.unicast()
        """
        pass
```

---

## [mechanism] Command_Piggyback_Dispatch_Model

由于 **Sensord** 与 **fustord** 之间通常是基于 HTTP 的 Pull 模型（心跳由 Sensord 发起），指令分发采用 **搭载响应模式**：
1. `SessionManager` 将待发指令存入目标 Session 的指令队列。
2. 当 **Sensord** 下一次 POST 心跳时，指令队列中的 Payload 被作为响应体返回。
