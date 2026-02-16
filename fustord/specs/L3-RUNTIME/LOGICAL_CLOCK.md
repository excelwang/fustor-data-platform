---
version: 1.0.0
---

# L3: [algorithm] Fustord Logical Watermark Arbitration

> Type: algorithm | safety_spec
> Layer: Domain Layer (SDP Consistency)

---

## 1. 问题场景: 多源写冲突

当多个 **Sensord** 节点同时向一个 View 推送数据时，由于各自机器时钟的 Skew, View 的全局水位线可能产生震荡：
- A 节点时钟慢 10s, B 节点时钟快 10s。
- 如果简单取 Max(mtime), View 的水位线会瞬间跳跃并阻塞 A 节点的合法旧事件。

---

## 2. 算法方案: Shadow Watermark

**fustord** 实现了基于 **影子水位线 (Logical Watermark)** 的仲裁逻辑，也就是所谓的“真相之钟”。

### 2.1 局部单调性 (Local Per-Session Monotonicity)
- 对于来自同一个 Session 的事件流, fustord 强制要求版本号单调递增。

### 2.2 全局聚合准则 (Global View Aggregation)
- **View_Watermark = Min(All_Active_Sessions_Watermarks)**。
- **目的**: 确保只有当所有活跃的Sensord都推进过某个物理时间点后，该时间点之前的数据才被视为“审计完备”的对象。

---

## 3. 简化逻辑时钟算法 (Simplified Logical Clock)

fustord 采用 **纯物理时间驱动的水位线**，完全免疫 mtime 异常。

**公式**：

```python
Watermark = fustord_Physical_Time - Mode_Skew
```

### 3.1 Skew 采样

对于每个 Realtime 事件，计算 fustord 本地时间与 mtime 的差异：

```python
if mtime and can_sample_skew:
    reference_time = time.time()  # fustord Local Time
    diff = int(reference_time - mtime)
    self._global_buffer.append(diff)
    self._global_histogram[diff] += 1
```

- **免疫力**: 使用 fustord Local Time 作为参考系，免疫 sensord 时钟偏差

### 3.2 Mode Skew 选举

选取出现频率最高 (Mode) 的差异值作为权威偏差。即使某个 sensord 时钟彻底错乱，fustord 通过全局 Mode 选举将其样本识别为异常并剔除。

### 3.3 Watermark 计算

```python
def get_watermark(self) -> float:
    skew = self._get_global_skew_locked() or 0.0
    return time.time() - skew
```

- **简洁性**: 纯函数，无状态依赖。
- **免疫性**: 恶意 mtime（如 `touch -d 2050`）完全无法推进 Watermark。
- **一致性**: Watermark 始终与物理时间同步推进，不受写入流量影响。

---

## 4. 确定性保护 (Safety)

1.  **疑似排除**: 水位线附近的事件被标记为 `Suspect`。
2.  **强制校准**: 在接收到 `audit_end` 信号后, View 会强制同步一次该 Session 的影子时钟到汇报者的物理时间点。

---

## 5. 已知局限性: 冷启动问题 (Cold Start)

**场景描述**：
当 fustord 刚启动或 View 刚创建时，若第一批事件为 Snapshot/Audit（而非 Realtime），`_global_histogram` 可能为空，导致 Skew 采样不足，Watermark 回退到物理时间。

**影响**：
本应被标记为 Suspect 的热文件可能被误判为"已冷却"。

**缓解措施**：
```python
def get_watermark(self) -> float:
    baseline = time.time() - self._compute_mode_skew()
    return max(self._watermark, baseline)
```
即使 `_watermark` 未被有效推进，也会回退到当前物理时间作为保底，确保 Age 不会为负值。

---

## 6. 优势总结

1.  **层级隔离**: sensord 保护本地资源，fustord 保护全局一致性。
2.  **免疫单点故障**: 全局 Mode 选举过滤个别异常节点。
3.  **自愈能力**: 长期无写入时，Watermark 随物理时间自动推进，避免 Suspect 永久滞留。
