---
version: 1.0.0
---

# L3: [algorithm] Task Orchestration & Dispatch

> Type: algorithm | interface

## 1. 概述

本文档定义了任务编排服务 (`TaskOrchestrator`) 的实现细节，负责将高层业务意图（如"升级"、"扫描"）转换为 L1 层的中立寻址原语。

## 2. 接口定义

建议封装通用的 `TaskOrchestrator` 服务，隔离分发细节：

```python
class TaskOrchestrator:
    async def view_broadcast(self, view_id: str, cmd: Dict) -> List[Dict]:
        """
        全量广播逻辑：用于回退扫描。
        1. 确定 ViewID 关联的所有 Session
        2. L1.broadcast()
        3. 汇聚结果
        """
        pass

    async def agent_targeted_dispatch(self, agent_id: str, cmd: Dict) -> Dict:
        """
        精准选路逻辑：用于升级/停止。
        1. 确定 AgentID 关联的 Session (优先 Leader)
        2. L1.unicast()
        """
        pass
```

## 3. 寻址策略

### 3.1 广播 (Broadcast)
- **目标**: View ID
- **原语**: `L1.broadcast(view_id)`
- **场景**: 数据补全 (Scan), 配置推送 (Reload)

### 3.2 单播 (Unicast)
- **目标**: Agent ID
- **原语**: `L1.unicast(agent_id)`
- **场景**: 进程管控 (Upgrade, Stop, Restart)
