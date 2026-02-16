---
version: 1.0.0
---

# L3: [pattern] Fustor MemoryEventBus 消息同步机制

> Type: pattern | algorithm
> Layer: Domain Layer

## 1. 概述

`MemoryEventBus` 是 sensord 内部的高吞吐、低延迟消息同步核心。它通过生产者-消费者模型实现数据读取 (Source) 与数据推送 (Sender) 的解耦。

## 2. 核心机制

### 2.1 环形缓冲区 (Ring Buffer)
- **固定容量**: 默认为 1000 个事件，由 `max_queue_size` 配置。
- **背压机制**: 当缓冲区满时，生产者将进入异步等待状态，直到消费者 `commit` 释放空间。

### 2.2 自动分裂阈值 (Splitting Threshold)
为了防止慢消费者（Slow Consumer）拖慢整个 Bus 的回收进度，系统实施自动分裂逻辑：

- **触发条件**: 任意订阅者完成 `commit` 时触发。
- **分裂阈值**: **95%** 的缓冲区容量。
- **判断逻辑**:
  1. 计算当前最快订阅者与最慢订阅者的位置差距。
  2. 若 `(最快 index - 最慢 index) >= capacity * 0.95`，则触发分裂。
- **执行动作**: 最快订阅者被从当前 Bus 移除，并迁移至一个新的、空的 `EventBus` 实例中，从而允许其继续高速推送，同时慢订阅者可以在旧 Bus 中继续追赶。

### 2.3 字段过滤与投影
- **按需加载**: Bus 跟踪所有订阅者所需的字段集合。
- **Recalculated Required Fields**: 仅当所有活跃订阅者都不需要的字段时，Source 驱动才可跳过该字段的采集。

## 3. 生命周期与资源回收
- **低水位线 (Low Watermark)**: 取所有活跃订阅者中最小的 `last_seen_position`。
- **Buffer Trimming**: 定期删除索引小于等于低水位线的事件，释放内存。
- **自动销毁**: 当所有订阅者均已 `unsubscribe` 且缓冲区已清空时，Bus 实例由 `BusService` 回收。
