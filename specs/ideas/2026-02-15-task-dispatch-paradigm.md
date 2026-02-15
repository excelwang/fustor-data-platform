# 任务分发范式：广播 (Broadcast) vs 精准 (Targeted)

在 Fustor 的 L1-L3 架构中，远程指令（Tasks）的下发存在两种截然不同的逻辑语义。为了避免架构耦合和重复重启/扫描，必须在逻辑上对它们进行严格区分。

## 1. 核心维度对比

| 维度 | 广播式任务 (L2/Data-Compensatory) | 精准式任务 (L3/Administrative) |
| :--- | :--- | :--- |
| **典型示例** | `scan` (On-Demand Find) | `upgrade`, `reload`, `stop` |
| **逻辑目标** | **View** (数据视图) | **Agent** (物理进程) |
| **寻址映射** | `ViewID -> List[SessionID]` | `AgentID -> List[SessionID]` |
| **成功准则** | **Quorum/Full** (集齐所有源) | **Any/Single** (命中即生效) |
| **失败影响** | 数据盲区 (Partial Data) | 管控失效 (Control Loss) |
| **执行原子性** | 数据分片级 | 进程级 |

---

## 2. 逻辑分层定义

### 2.1 视图广播任务 (Broadcast Task)
这种任务的目的是**内容补偿**。当 L1 数据路径不满足查询需求时，L2 启动广播以获取全量视图。

*   **逻辑流程**:
    1.  确定 `ViewID` 关联的所有活跃 `Pipe`。
    2.  确定每个 `Pipe` 关联的所有活跃 `Session`。
    3.  **并发分发**: 向所有选定的 `Session` 发送指令。
    4.  **结果汇聚**: 收集所有响应，原样交付给 `ViewDriver.apply_fallback_results()` 汇总。

### 2.2 代理控制任务 (Targeted Task)
这种任务的目的是**状态变更**。它是对 L2 连接载体的生命周期进行运维操作。

*   **逻辑流程**:
    1.  解析 Agent 的 `task_id` 获取 `AgentID`。
    2.  在全局 Session 表中过滤出属于该 `AgentID` 的所有 `Session`。
    3.  **选路逻辑**:
        *   优先选择 `Leader` Session。
        *   若无 Leader，选择第一个探测到的活跃 Session（Heartbeat 最新的）。
    4.  **单点分发**: 只向选中的**一个** Session 发送指令。
    5.  **原子执行**: 利用 Agent 进程的继承关系，实现全局重启或加载。

---

## 3. 实现建议：任务编排器 (TaskOrchestrator)

在架构上建议封装通用的 `TaskOrchestrator` 服务，隔离分发细节：

```python
class TaskOrchestrator:
    async def view_broadcast(self, view_id: str, cmd: Dict) -> List[Dict]:
        """
        全量广播逻辑：用于回退扫描。
        保证覆盖所有物理源，解决数据盲区。
        """
        pass

    async def agent_targeted_dispatch(self, agent_id: str, cmd: Dict) -> Dict:
        """
        精准选路逻辑：用于升级/停止。
        保证进程级操作的原子性，避免冲突。
        """
        pass
```

## 4. 结论

将“广播”与“精准”在寻址空间上进行拆解，是实现“控制面与数据面解耦”这一生存 mandate 的技术基石。
*   **广播** 是为了数据的 **完整性**。
*   **精准** 是为了控制的 **幂等性与一致性**。
