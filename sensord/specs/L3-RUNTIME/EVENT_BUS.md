# L3: [pattern] [Sensord] MemoryEventBus (Async Dispatch)

> Type: design_pattern
> Layer: Stability Layer (SDP Handling)

---

## 1. 设计初衷

为了将 **Domain Layer** (扫描产生的事件) 与 **Stability Layer** (网络推送逻辑) 完全解耦，**Sensord** 内部核心使用了一个有界环形缓冲区 `MemoryEventBus`。

---

## 2. 核心特性

### 2.1 自动分裂 (Bus Splitting)
- **场景**: 当某个消费者的网络极慢，导致缓冲区积压超过 80% 时。
- **动作**: Sensord 会为该“慢消费者”自动复制一份独立的订阅流，并启动背压保护。防止单个消费者的延迟导致全局数据一致性算法（Leader Audit）挂起。

### 2.2 投影透传 (Projection Passthrough)
- **逻辑**: 如果 `fields_mapping` 规则被触发，EventBus 会在内存中执行字段投影。发送端最终看到的事件体是经过最小化处理后的数据，以节省带宽。

### 2.3 生命周期对齐 (Lifecycle Alignment)
- **规则**: `EventBus` 的生命周期随 **Sensord** 进程启动而建立，随最后一条 `SensordPipe` 销毁而释放资源。
- **背压保护 (Backpressure)**: 当缓冲区达到 100% 满位时，生产者（Source）将被暂停异步执行，直到至少有一个活跃订阅者释放其锁定的槽位。

### 2.2 快慢消费者自动分裂 (Automatic Bus Splitting)

为防止因网络瞬态拥塞导致的慢消费者（Slow Consumer）拖慢整个节点的实时同步性能，Bus 实现了自动分裂逻辑。
- **触发条件**: 任意订阅者执行 `commit` 指令且总负载超过其处理阈值。
- **分裂算法 (Threshold 95%)**:
  1. 监测最快订阅者（Head）与最慢订阅者（Tail）的索引位置差。
  2. 若 `(Head_Index - Tail_Index) >= Capacity * 0.95`，判定发生由于慢消费者导致的 Head-of-Line Blocking。
  3. **分裂执行**: 系统将最快订阅者通过“接球（Handoff）”流程迁移至一个新的、空的 `EventBus` 实例，使其恢复极速推送。

### 2.3 消息投影 (Event Projection)

- **字段最小化**: Bus 会动态聚合所有订阅者的 `required_fields` 集合。
- **采集优化**: 若所有活跃订阅者都不需要某类大体积字段（如图片二进制流），Source 驱动可在采集层直接跳过该字段的分封。

## 3. 生命周期与资源管理

- **低水位线 (Low Watermark)**: 维护所有订阅者中最小的 `last_seen_index`。
- **垃圾回收 (GC)**: 物理索引标号小于低水位线的事件会被立即从内存中 Trim 掉。
- **单例回收**: 当 Bus 的订阅者清零且缓冲区排空后，该 Bus 实例会被 `BusManager` 自动销毁以释放内存。
