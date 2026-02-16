# L3: [pattern] [Datacast] Driver Lifecycle (Singleton)

> Type: design_pattern
> Layer: Domain Layer (Driver Management)

---

## [pattern] Per_URI_Singleton_Pattern

在 **Datacast** 内部，对于每一个唯一的物理资源（由 `URI` 标识），**必须** 保证全局仅有一个 Driver 实例。

### 1.1 唯一标识符决定论 (UDID)
- **规则**: `UDID = Hash(URI + DriverType)`。
- **目的**: 防止对同一个 NFS 挂载点或数据库表进行重复扫描，节省系统句柄与内存。

### 1.2 行为约束 (Lifecycle Constraints)
1.  **共享引用**: 即使多个 `DatacastPipe` 租用了同一个源，它们共享同一个底层 Driver 实例。
2.  **显式关闭**: 只有当所有租用该 Driver 的 `DatacastPipe` 全部断开，且经过 `linger_timeout` 后，Driver 才执行 `close()`。
3.  **重载一致性**: 在 `SIGHUP` 触发配置重载时，若 URI **和** 驱动参数均未变更，Driver 实例保持存活。若驱动参数发生变更（如 `throttle_interval`、排除规则等），则必须执行 `invalidate` 强制销毁旧实例并创建新实例（见 §3）。

### 1.3 资源复用模型 (Resource Multiplexing)

**机制**: 当多个 DatacastPipe 配置指向同一物理 URI 且使用相同凭证时，系统只会创建一个 Driver 实例，并为其分配独立的订阅者视图。
-   **资源互斥 (Resource Pooling)**: 共享实例意味着底层的 WatchManager（如 inotify 监听器）、扫描线程池以及 EventQueue (EventBus) 在物理层上是共用的。
-   **订阅隔离**: 虽然底层资源共享，但每个 DatacastPipe 拥有独立的 `last_consumed_index`，确保数据消费进度的独立性。

---

## [isolation] Thread_Isolation_Strategy

每一个 Driver 允许拥有自己的独立采集线程，但必须通过 **Datacast-core** 提供的 `Thread Bridge` 模式与主 asyncio 循环通信。

---

## [strategy] Hot_Reload_Behavior_Strategy

配置热重载时 Driver 的处理策略：

| 条件 | 动作 | 原因 |
|------|------|------|
| URI 未变 + 参数未变 | **保持存活** | 无需任何操作 |
| URI 未变 + 参数变更 | **销毁重建 (Invalidate)** | 确保 `exclude_patterns` 等敏感参数完全生效 |
| URI 变更 | **销毁重建** | 全新的物理资源，必须重新初始化 |

-   **健康监测**: Driver 周期性向 `DatacastPipe` 报告其 Watcher 存活状态。若底层监控链条断裂且无法自愈，Driver 将被标记为 `Stale` 并由 `PipeManager` 触发重启。
