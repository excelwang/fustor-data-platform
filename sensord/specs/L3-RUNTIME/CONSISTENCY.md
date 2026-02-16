---
version: 1.0.0
---

# L3: [algorithm] [Sensord] Data Consistency & Sync

> Type: algorithm | design_decision
> Layer: Domain Layer (SDP Implementation)

---

## 1. 核心模型 (Core Model)

**Sensord** 采用 **Leader/Follower** 会话对齐模型。在特定数据的同步视图中，同一时间仅有一个 **Sensord** 实例被指派为 Leader 以执行 Snapshot 和 Audit 对账任务，其余实例作为 Follower 仅同步 realtime 实时增量。

---

## 2. 数据采集轨道 (Sync Tracks)

Sensord 通过三种互补的时间轨道确保数据的一致性：

### 2.1 实时轨道 (Realtime Track)
- **机制**: 使用 `inotify` 捕获文件系统的即时变更。
- **特点**: 低延迟（亚秒级），但不保证 100% 可达（受内核事件丢包影响）。
- **字段标记**: `is_realtime=True`。

### 2.2 快照轨道 (Snapshot Track)
- **机制**: 建立 Session 时立即执行物理全量扫描。
- **角色**: 仅 **Leader** 执行。
- **目的**: 建立业务对齐的基准水位。

### 2.3 审计轨道 (Audit Track)
- **机制**: 周期性执行深层物理扫描。
- **角色**: 仅 **Leader** 执行。
- **目的**: 补全 inotify 丢失的“盲区”文件并清理不存在的“幽灵条目”。

---

## 3. 扫描算法 (Scan Algorithms)

### 3.1 Audit 快速扫描算法

利用 POSIX 语义：创建/删除文件通常会更新直接父目录的 `mtime`。

**True Silence (静默跳过)** 机制：
1. 递归进入目录，获取当前目录 `mtime`。
2. 比对缓存：若 `current_mtime == cached_mtime`，判定该目录在存储面“静默”。
3. **静默标记**: 即使静默，sensord 仍发送 `audit_skipped=True` 的空数据帧，告知 Consumer “此处无恙”，保护子项不被误删。
4. **子目录递归**: 无论当前层是否静默，必须始终递归检查子目录。

### 3.2 Path Normalization Contract (路径归一化契约)

为确保多 sensord (尤其是 Shared Storage 场景) 视图的一致合并，所有 Source Driver **必须** 遵循以下路径生成规则：

1.  **Relative Path**: 输出路径必须相对于配置的 `root_path` / `uri`。
2.  **Leading Slash**: 归一化后的路径必须以 `/` 开头 (例如 `/foo/bar.txt`)。
    - 示例：`uri=/mnt/data`，文件 `/mnt/data/foo/bar.txt` -> 归一化为 `/foo/bar.txt`。
3.  **Consistency**: Realtime, Snapshot, Audit 三种模式生成的路径必须完全一致。

---

## 4. 交互原语 (Interaction Primitives)

| 原语 | 协议层 | 描述 |
|------|--------|------|
| **`on_modified`** | SDP | 文件内容发生变更，`is_atomic_write=False`。 |
| **`on_closed`** | SDP | 文件写关闭，`is_atomic_write=True`（视为最终状态）。 |
| **`audit_start / end`** | SDP | 标记一个物理对账周期的开始与结束。 |
| **`can_realtime`** | SCP | 健康状态位，表示实时监听是否健康。 |

---

## 5. 哨兵巡检 (Sentinel Sweep)

Sensord 响应 Consumer 下发的 `suspect_check` 任务：

- **Interval**: 由 Consumer 决定 (通常 ~5min)。
- **Action**: 对指定的 path 列表执行 `os.stat`。
- **Feedback**: 返回最新的 mtime 和存在状态。
