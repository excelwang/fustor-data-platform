# L3: [algorithm] [fustord] View FS Logical Clock (Watermark)

> Type: algorithm
> Layer: Domain Layer (Global Consistency)

---

## [overview] Distributed_Clock_Skew_Overview

fustord 必须维护一个全局一致的 **逻辑水位线 (Watermark)**，用于：
1. **Suspect 判定**: 判断文件是否“足够旧”以被标记为可疑。
2. **Tombstone 仲裁**: 判定新写入是否覆盖了墓碑。
3. **Age 计算**: 提供稳定的文件年龄，屏蔽 NFS 时钟跳变。

---

## [model] Hierarchical_Clock_Domain_Model

fustord 采用 **被动接收 + 主动校准** 的策略：

| 组件 | 职责 |
| :--- | :--- |
| **Sensord (Source)** | **Guarantee**: 负责屏蔽 NFS 抖动，提供经过补偿的 `index` 时间戳。 |
| **Fustord (View)** | **Arbitration**: 负责维护全局 Watermark，免疫单个 Sensord 的时钟异常。 |

### [strategy] Sensord_Contract_and_Expectations

fustord **假设** Sensord 发送的事件满足以下契约：
1. **Compensated Index**: `event.index` 是经过漂移补偿的逻辑时间戳，近似于物理时间。
2. **Monotonicity**: 在单个 Session 内，`index` 严格单调递增。

> **注意**: fustord **不关心** Sensord 内部如何计算 Drift (P99 算法等)，只关心收到的 `index` 是否平滑。

---

## [algorithm] Shadow_Reference_Frame_Consensus_Algorithm

**Rationale**: Provide a stable logical time domain that masks physical clock skew between distributed nodes without requiring global clock sync.

```python
# Consensus Algorithm Pseudocode
def calculate_global_watermark(samples: List[Sample]) -> Watermark:
    skew = calculate_mode_skew(samples)
    return time.time() - skew
```

### 3.1 算法输入
- **`fustord_local_time`**: fustord 本机物理时间 (Trust Anchor)。
- **`event.mtime`**: 来自不同 Sensord 的原始文件时间。

## [algorithm] Skew_Sampling_Algorithm

**Rationale**: Collect samples of physical vs. logical time offset across all connected sensors.

```python
if mtime and can_sample_skew:
    reference_time = time.time()  # fustord Local Time
    diff = int(reference_time - mtime)
    self._global_buffer.append(diff)
    self._global_histogram[diff] += 1
```

- **免疫力**: 使用 fustord Local Time 作为参考系，免疫 sensord 时钟偏差

## [algorithm] Mode_Skew_Calculation_Algorithm

**Rationale**: Use a consensus-based approach (Mode) to filter out individual node clock anomalies.

```python
def _get_global_skew_locked(self) -> float:
    mode_key = self._global_histogram.most_common(1)[0][0]
    return float(mode_key)
```

- 选取出现频率最高 (Mode) 的差异值作为权威偏差

## [algorithm] Watermark_Calculation_Algorithm

**Rationale**: Update the authoritative system watermark by combining the consensus skew with local time.

```python
def get_watermark(self) -> float:
    baseline = time.time() - self._compute_mode_skew()
    # 冷启动保护: 即使 _watermark 未被有效推进，也会回退到当前物理时间作为保底
    return max(self._watermark, baseline)
```

---

## [strategy] Clock_Anomaly_Remediation_Strategy

**Rationale**: Define robust behavior for edge cases to maintain system stability.

| 场景 | fustord 行为 |
| :--- | :--- |
| **Normal** | Watermark 随物理时间线性推进。 |
| **Future Mtime (Abnormal)** | 拒绝推进 Watermark。该异常文件会被标记为 `Suspect` (因 `mtime > watermark` 极大)。 |
| **Cold Start** | **Safe Fallback**: 在收集足够样本前，回退使用物理时间。可能导致暂时性的 Suspect 判定宽松。 |

---

## [impact] System_Protocol_Time_Impact_Analysis

**Rationale**: Illustrate the practical effects of the logical clock on consistency checks.

| 用途 | 公式 | 说明 |
| :--- | :--- | :--- |
| **Suspect Age** | `Watermark - mtime` | 文件已存在多久 (免疫 NFS 跳变) |
| **Tombstone Check** | `mtime > tombstone_ts` | 墓碑覆盖判定 |

### 3.3 免疫特性 (Immunity)
- **免疫 Future Timestamp**: 即使某个 Sensord 发来 2050 年的 `mtime`，由于它是个例，不会成为 Mode (众数)，因此会被算法忽略。
- **免疫 Stagnation**: Watermark 随 fustord 本地时间流逝$而自动推进，不依赖 Sensord 持续发送事件。
