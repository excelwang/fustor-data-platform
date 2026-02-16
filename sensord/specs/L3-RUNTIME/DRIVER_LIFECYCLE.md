# L3: [pattern] [Sensord] Driver Lifecycle (Singleton)

> Type: design_pattern
> Layer: Domain Layer (Driver Management)

---

## 1. 核心模式: Per-URI Singleton

在 **Sensord** 内部，对于每一个唯一的物理资源（由 `URI` 标识），**必须** 保证全局仅有一个 Driver 实例。

### 1.1 唯一标识符决定论 (UDID)
- **规则**: `UDID = Hash(URI + DriverType)`。
- **目的**: 防止对同一个 NFS 挂载点或数据库表进行重复扫描，节省系统句柄与内存。

### 1.2 行为约束 (Lifecycle Constraints)
1.  **共享引用**: 即使多个 `SensordPipe` 租用了同一个源，它们共享同一个底层 Driver 实例。
2.  **显式关闭**: 只有当所有租用该 Driver 的 `SensordPipe` 全部断开，且经过 `linger_timeout` 后，Driver 才执行 `close()`。
3.  **重载一致性**: 在 `SIGHUP` 触发配置重载时，若 URI 未变，Driver 实例必须保持存活，仅更新内部业务参数（如 `throttle_interval`）。

---

## 2. 线程隔离 (Thread Isolation)

每一个 Driver 允许拥有自己的独立采集线程，但必须通过 **Sensord-core** 提供的 `Thread Bridge` 模式与主 asyncio 循环通信。
### 1.3 资源复用模型 (Resource Multiplexing)

**机制**: 当多个 SensordPipe 配置指向同一物理 URI 且使用相同凭证时，系统只会创建一个 Driver 实例，并为其分配独立的订阅者视图。
-   **资源互斥 (Resource Pooling)**: 共享实例意味着底层的 WatchManager（如 inotify 监听器）、扫描线程池以及 EventQueue (EventBus) 在物理层上是共用的。
-   **订阅隔离**: 虽然底层资源共享，但每个 SensordPipe 拥有独立的 `last_consumed_index`，确保数据消费进度的独立性。

### 2.3 生命周期约束 (Constraints)

-   **重载一致性 (Reload Consistency)**: 在配置热重载（Hot Reload）场景下，若 URI 保持不变但驱动参数（如 `throttle_interval` 或排除规则）发生变更，ConfigReloader 必须显式执行 `invalidate` 以强制销毁旧实例并创建应用新参数的新实例。
-   **健康监测**: Driver 周期性向 `SensordPipe` 报告其 Watcher 存活状态。若底层监控链条断裂且无法自愈，Driver 将被标记为 `Stale` 并由 `PipeManager` 触发重启。
