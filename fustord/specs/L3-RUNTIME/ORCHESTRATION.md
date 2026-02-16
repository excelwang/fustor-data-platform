---
version: 1.0.0
---

# L3: [pattern] Fustord Command Orchestration (SCP)

> Type: design_pattern
> Layer: Management Layer (SCP Dispatcher)

---

## 1. 核心原语: Renting Model

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

## 2. 搭载模型 (Piggyback)

由于 **Sensord** 与 **fustord** 之间通常是基于 HTTP 的 Pull 模型（心跳由 Sensord 发起），指令分发采用 **搭载响应模式**：
1. `SessionManager` 将待发指令存入目标 Session 的指令队列。
2. 当 **Sensord** 下一次 POST 心跳时，指令队列中的 Payload 被作为响应体返回。
