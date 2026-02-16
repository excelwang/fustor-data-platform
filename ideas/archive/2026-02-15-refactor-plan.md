# Fustor 架构重构：层级修正与原语中立化

本项目遵循 **“下沉稳定性，上行扩展性”** 的原则，将架构层级调整为正确的次序。

## 1. 重新定义架构层级 (Corrected Hierarchy)

| 层级 | 名称 | 职责核心 | 状态感知度 |
| :--- | :--- | :--- | :--- |
| **Layer 1** | **稳定性层 (Stability)** | 连接维持、心跳隧道、指令分发容器 | **业务色盲**：不感知 `scan` 或 `upgrade` |
| **Layer 2** | **领域层 (Domain)** | 数据同步逻辑、API 响应、回退策略、快照合并 | **业务敏感**：实现核心产品价值 |
| **Layer 3** | **插件层 (Management)** | 远程升级、配置重载、运维 UI | **可选插件**：对 L1/L2 的标准化借用 |

---

## 2. 指令原语化 (Command Neutralization)

Layer 1 (SessionManager) 不再提供特定业务接口，仅提供寻址原语：

*   **`L1.broadcast(selector, payload)`**: 针对逻辑组（如 ViewID）的全量覆盖。
*   **`L1.unicast(target, payload)`**: 针对物理点（如 SessionID/datacaststID）的精准触达。
*   **`L1.heartbeat_tunnel`**: 仅负责负载的可靠传输，不关心负载内容。

---

## 3. 任务下发的层级归属：统一租用模式 (Unified Renting Model)

我们不再区分“管理任务”和“回退任务”，它们都是 L2 领域的**标准行为**。

### 3.1 补偿性扫描 (Fallback Scan) —— L2 领域行为
*   **语义**：当 L2 发现数据不完整时，向 L1 租用 `broadcast` 原语。
*   **流程**：`QueryService (L2)` -> `L1.broadcast(view_id, scan_cmd)` -> `Aggregation (L2)`。

### 3.2 运维管理行为 (Fleet Management) —— L2 领域行为
*   **语义**：将“管理”视为一种特殊的 **Management View**。
    *   **入向**：datacastst 状态通过 `MgmtEvent` 进入 L2，构建“集群资产视图”。
    *   **出向**：当需要操作（如升级）时，Management View 向 L1 租用 `unicast` 原语。
*   **流程**：`MgmtView (L2)` -> `L1.unicast(target, upgrade_cmd)`。

---

## 4. 关键动作 (Action Items)

1.  **[L1 清洗]**: 彻底切除 `SessionManager` 中关于 `scan`, `path`, `job_id` 的所有代码。实现纯净的 `broadcast/unicast` 容器。
2.  **[L2 固化]**: 将 Fallback 策略从 Mgmt 包迁移到 View 查询核心流程。定义通用的 `merge_snapshots` 驱动契约。
3.  **[L3 降级]**: 将 Mgmt 修改为 L1 接口的一个普通借用者，不再享有任何 L1/L2 内部状态的特殊优待。包名调整为 `fustor-view-mgmt` / `fustor-source-mgmt`。
4.  **[L3 极简化]**: `fustor-view-mgmt` 退化为 L2 视图的纯 API 转发层。

---

## 5. 架构收益
*   **安全性**: 核心 L1 逻辑变简单、变通用，降低崩溃概率。
*   **扩展性**: 未来管理功能只需在 L3/L2 注入 Payload，无需修改核心。
*   **鲁棒性**: 实现了“API 永不 503”与“进程级原子升级”的完美兼容。
