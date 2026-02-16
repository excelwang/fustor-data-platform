---
version: 1.0.0
---

# L3: [algorithm] [Fustord] View FS Logical Clock (Watermark)

> Type: algorithm
> Layer: Domain Layer (Global Consistency)

## 1. 背景与挑战

fustord 必须维护一个全局一致的 **逻辑水位线 (Watermark)**，用于：
1. **Suspect 判定**: 判断文件是否“足够旧”以被标记为可疑。
2. **Tombstone 仲裁**: 判定新写入是否覆盖了墓碑。
3. **Age 计算**: 提供稳定的文件年龄，屏蔽 NFS 时钟跳变。

---

## 2. 核心架构

fustord 采用 **被动接收 + 主动校准** 的策略：

| 组件 | 职责 |
| :--- | :--- |
| **Sensord (Source)** | **Guarantee**: 负责屏蔽 NFS 抖动，提供经过补偿的 `index` 时间戳。 |
| **Fustord (View)** | **Arbitration**: 负责维护全局 Watermark，免疫单个 Sensord 的时钟异常。 |

### 2.1 对 Sensord 的契约要求 (Contract with Sensord)

fustord **假设** Sensord 发送的事件满足以下契约：
1. **Compensated Index**: `event.index` 是经过漂移补偿的逻辑时间戳，近似于物理时间。
2. **Monotonicity**: 在单个 Session 内，`index` 严格单调递增。

> **注意**: fustord **不关心** Sensord 内部如何计算 Drift (P99 算法等)，只关心收到的 `index` 是否平滑。

---

## 3. Watermark 算法 (The Truth)

fustord 采用 **Mode Skew (众数偏斜)** 算法来计算全局水位线。

### 3.1 算法输入
- **`fustord_local_time`**: fustord 本机物理时间 (Trust Anchor)。
- **`event.mtime`**: 来自不同 Sensord 的原始文件时间。

### 3.2 算法逻辑
1. **Sampling**: 对每个 Realtime 事件，计算 `diff = fustord_time - event.mtime`。
2. **Voting**: 统计 `diff` 的直方图，选取出现频率最高的值 (Mode) 作为 **Global Skew**。
3. **Calculation**:
   ```python
   Watermark = fustord_Physical_Time - Global_Skew
   ```

### 3.3 免疫特性 (Immunity)
- **免疫 Future Timestamp**: 即使某个 Sensord 发来 2050 年的 `mtime`，由于它是个例，不会成为 Mode (众数)，因此会被算法忽略。
- **免疫 Stagnation**: Watermark 随 fustord 本地时间流逝而自动推进，不依赖 Sensord 持续发送事件。

---

## 4. 关键场景处理

| 场景 | fustord 行为 |
| :--- | :--- |
| **Normal** | Watermark 随物理时间线性推进。 |
| **Future Mtime (Abnormal)** | 拒绝推进 Watermark。该异常文件会被标记为 `Suspect` (因 `mtime > watermark` 极大)。 |
| **Cold Start** | 在收集足够样本前，回退使用物理时间 (Safe Fallback)。可能导致暂时性的 Suspect 判定宽松。 |

---

## 5. 时间用途总结

| 用途 | 公式 | 说明 |
| :--- | :--- | :--- |
| **Suspect Age** | `Watermark - mtime` | 文件已存在多久 (免疫 NFS 跳变) |
| **Tombstone Check** | `mtime > tombstone_ts` | 墓碑覆盖判定 |

> 完整算法细节见各个实现的源码。
