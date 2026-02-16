---
version: 1.0.0
---

# L3: [algorithm] [Sensord] Shadow Reference Frame (Clock)

> Type: algorithm | safety_spec
> Layer: Domain Layer (Event Sequencing)

## 1. 问题背景: 时钟偏斜 (Clock Skew)

在分布式系统中，**Sensord**（采集端）与 **Consumer**（存储端）的服务器时钟可能完全不同步。
- 如果直接透传 `mtime`，一旦采集端时钟超前，存储端会因“来自未来的文件”导致索引崩溃。
- 如果将时钟同步延迟作为判定标准，会导致数据顺序错乱。

---

## 2. 算法方案: Shadow Reference Frame

**Sensord** 采用 **影子参考系 (Logical Clock)** 算法，实现局部保护：

### 2.1 本地隔离保护 (Local Shield)
- **原理**: Sensord 记录本地启动时的系统偏斜量。所有向外发送的事件，必须经过 `ShadowClock` 的修正。
- **目标**: 确保在一个 Session 生命周期内，发送给消费者的 `watermark` 严格单调递增，且绝不超前于消费者的已知全局水位线。

### 2.2 漂移采样与补偿 (Sampling & Compensation)

Sensord 不盲目信任本机 `time.time()`，而是在启动或执行全量扫描时通过采样建立物理参考对齐。

**采样逻辑 (P99 Stability)**：
1. **Sampling**: 收集目标存储路径中所有目录的 `mtime`。
2. **Filtering**: 排序并选取 P99 分位点作为 `latest_mtime_stable`（有效排除未来的极端跳变干扰）。
3. **Drift Calc**: 计算物理漂移值：`drift = latest_mtime_stable - time.time()`。
4. **Correction**: 此后所有外发事件的逻辑时间轴均基于 `Local_Time + drift`。

```python
# 1. P99 漂移采样——过滤前 1% 异常值
mtimes = sorted(dir_mtime_map.values())
p99_idx = max(0, int(len(mtimes) * 0.99) - 1)
latest_mtime_stable = mtimes[p99_idx]

# 2. 计算漂移
self.drift_from_nfs = latest_mtime_stable - time.time()
self.watch_manager.drift_from_nfs = self.drift_from_nfs

# 3. 归一化调度 (LRU 使用归一化时间)
lru_timestamp = server_mtime - drift_from_nfs
self.watch_manager.schedule(path, lru_timestamp)
```

### 2.3 事件索引生成契约 (Index Generation Contract)

所有产生数据的组件必须统一使用补偿后的物理时间戳作为事件 `index`。

| 组件 | 逻辑 | index 含义 |
|------|------|------------|
| **Realtime Handler** | `int((time.time() + drift) * 1000)` | 实时发生的补偿时间戳（对齐存储面） |
| **Scanner (Snapshot/Audit)** | `int((time.time() + drift) * 1000)` | 扫描发现时的补偿时间戳 |

- **有序性**: 确保 `realtime` 消息与 `audit` 补偿消息在同一逻辑时间轨道上竞争。

**禁止行为 (Split-Brain Timer)**:
- 禁止任何组件使用未补偿的 `time.time()` 生成 `index`
- **违反的后果**: 如果 sensord 时钟滞后于 NFS (`Drift > 0`)，未补偿的时间戳会导致 EventBus 检测到 Index Regression，根据单调递增原则**丢弃**合法的实时事件

---

## 3. 分层时钟模型 (Hierarchical Clock Model)

| 轨道 | 定义 | 核心用途 |
| :--- | :--- | :--- |
| **Physical Time** | 本地物理流逝参考 (Monotonic) | 内部超时判定、重连退避、速率控制。 |
| **Shadow Time** | 对齐存储面的参考时间 (Corrected) | 事件 `index` 生成、上行进度同步。 |
| **Data Time** | 文件系统的逻辑 `mtime` | 增量裁决、Suspect 判定、一致性真相。 |

---

## 4. 异常处理策略 (Handling Anomalies)

| 场景 | sensord 行为策略 |
| :--- | :--- |
| **存储时间跳向未来** | 影子参考系将 `latest_mtime_stable` 截断在 P99，防止局部“未来文件”毁掉全局任务的 LRU 优先级。 |
| **长期无写入 (Silence)** | 停止采样，`drift` 维持最后一次计算的静态值。控制链路心跳频率不受时钟漂移影响。 |
| **冷启动 (Cold Start)** | 由于尚未建立 Skew 样本，系统回退到物理时间，直至第一波扫描或实时事件完成校准。 |

---

## 5. 对外协议影响 (Protocol Impact)

Sensord 发送的消息包必须严格解耦物理时间与逻辑时间：
- `mtime`: 原始数据域时间，用于 **Consumer** 执行最终一致性对账。
- `index`: 补偿后的物理时间轴，用于 **Consumer** 判定消息流的顺序连续性（Liveness）。
