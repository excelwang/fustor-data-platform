---
version: 1.0.0
---

# L3: [pattern] Fustord View Driver Lifecycle

> Type: design_pattern
> Layer: Domain Layer (View Management)

---

## 1. 核心模式: Multi-Tenant View

在 **fustord** 内部，对于每一个逻辑视图（由 `ViewID` 标识），承载着来自多个 **Sensord** 节点的数据流。

### 1.1 视图隔离 (Isolation)
- **规则**: 每个 View 拥有独立的 `ViewStateManager` 和 `SessionManager`。
- **目的**: 确保不同业务域的数据互不干扰，且锁粒度控制在视图级别。

### 1.2 行为约束 (Lifecycle Constraints)
1.  **视图持久化**: 除非显式配置 `live_mode: true`，否则 Session 断开后，View 的内存状态（树结构、时钟水位）应被保留。
2.  **优雅降级**: 若底层存储驱动失效，View 应标记自己为 `Stale` 并告知 Sensord 降低同步频率。
3.  **重载一致性**: 在 `SIGHUP` 触发配置重载时，若 ViewID 未变，内存实例必须保持存活。

---

## 2. 交互 (Interactions)

View 通过 **SCP** 控制面响应向 **Sensord** 下发 `scan` 或 `audit` 指令。

### 2.3 生命周期约束

-   **引用计数**: Driver 内部**不**维护引用计数（简化设计）。
-   **显式销毁**: 必须调用 `driver.close()` 或 `FSDriver.invalidate(uri, cred)` 才能从缓存中移除。
