---
version: 1.0.0
---

# L3: [pattern] Fustord Receiver EventBus (Async Ingestion)

> Type: design_pattern
> Layer: Stability Layer (SDP Ingestion)

---

## [overview] Event_Bus_Design_Intent

为了将 **Receiver** (接收网络请求) 与 **View Processing** (复杂的业务一致性逻辑) 解耦，**fustord** 内部使用消息队列缓冲区。

---

## [feature] Event_Bus_Core_Features

### 2.1 生产者隔离 (Producer Isolation)
- **逻辑**: 每个 FustordPipe 拥有独立的 `asyncio.Queue`。
- **目的**: 确保 A 传感器的写入高峰不会阻塞 B 传感器的实时响应。

### 2.2 批量入库 (Batch Ingestion)
- **优化**: 队列消费者采用 Batch 扫描模式。如果队列中有积压，一次性取出多个事件交由 `View.process_batch()`，以减少锁竞争。

### 2.3 背压传递 (Backpressure)
- **规则**: 当 `FustordPipe` 队列长度超过容量阈值时，Receiver 在 HTTP 响应中返回 `429 Too Many Requests`。
- **自愈**: **Sensord** 收到 429 后应启动退避重试，从而将接收端的压力反向传递给产生端。

## [lifecycle] Event_Bus_Resource_Management_Lifecycle回收
- **低水位线 (Low Watermark)**: 取所有活跃订阅者中最小的 `last_seen_position`。
- **Buffer Trimming**: 定期删除索引小于等于低水位线的事件，释放内存。
- **自动销毁**: 当所有订阅者均已 `unsubscribe` 且缓冲区已清空时，Bus 实例由 `BusService` 回收。
