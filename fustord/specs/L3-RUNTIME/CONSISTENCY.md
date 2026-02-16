---
version: 1.0.0
---

# L3: [algorithm] Fustor Consistency & Arbitration

> Type: algorithm | decision
> Layer: Domain Layer (SDP Arbitration)

---

## 1. 核心模型 (Core Model)

**Fustord** 采用 **Leader/Follower** 会话对齐模型。对于特定的 View，同一时间仅有一个 **Sensord** 示例被指派为 Leader 以执行 Snapshot 和 Audit 对账任务，其余实例作为 Follower 仅同步 realtime 实时增量。

---

## 2. 数据采集轨道 (Sync Tracks)

通过三种互补的时间轨道确保数据的一致性：

### 2.1 实时轨道 (Realtime Track)
- **来源**: sensord 推送的 `inotify` 变更。
- **特点**: 低延迟（亚秒级），具有最高优先级。
- **字段标记**: `is_realtime=True`。

### 2.2 快照轨道 (Snapshot Track)
- **机制**: 建立 Session 时 Leader 执行的物理全量扫描。
- **目的**: 建立业务对齐的基准水位。

### 2.3 审计轨道 (Audit Track)
- **机制**: 周期性执行的深层物理扫描。
- **目的**: 补全 blind-spot 并清理 ghost entries。

---

## 3. 状态管理 (State Management)

fustord 通过 `FSState` 类维护以下核心状态：

### 3.1 内存树 (Memory Tree)

存储文件/目录的最新元数据状态，经过仲裁后的"真相"。

| 字段 | 描述 |
|------|------|
| `last_updated_at` | fustord 本地物理时间戳，记录最后确认时刻，用于陈旧证据保护 |
| `integrity_suspect` | `bool`，是否为可疑热文件 |
| `known_by_sensord` | `bool`，监控质量证明 (Monitoring Quality Attestation) |

### 3.2 墓碑表 (Tombstone List)

- **用途**：记录被 Realtime 删除的文件，防止滞后的 Snapshot/Audit 使其复活。
- **结构**：`Dict[Path, Tuple[LogicalTime, PhysicalTime]]`。
- **生命周期**：
  - **创建**：Realtime Delete 时记录。
  - **转世 (Reincarnation)**：当新事件 `mtime > tombstone_logical_ts` 时清除墓碑。
  - **清理**：Audit-End 时清理物理时间超过 1 小时的墓碑。

### 3.3 可疑名单 (Suspect List)

- **用途**：标记可能处于不稳定状态的文件（如正在写入、NFS 缓存未刷新）。
- **来源**：
  - `is_atomic_write=False` 的事件 (Modify)。
  - Snapshot/Audit 发现的 `(Watermark - mtime) < hot_threshold` 的热文件。
- **清除**：
  - 收到 `is_atomic_write=True` (Close/Create) 事件。
  - TTL 到期且 mtime 保持稳定。

### 3.4 盲区名单 (Blind-spot List)

- **用途**：标记仅由 Audit/Snapshot 发现，未经 Realtime 确认的文件。
- **清除**：收到该文件的 Realtime 事件。

---

## 4. 仲裁算法 (Arbitration Algorithms)

核心原则：**Realtime 优先，Mtime 仲裁，墓碑防复活**。

### 4.1 Realtime 消息处理
- **Update/Insert**: 立即更新内存树，更新 `last_updated_at`，清除 Suspect/Blind-spot 标记。若 `is_atomic_write=False`，加入 Suspect List。
- **Delete**: 从内存树删除，创建 Tombstone。

### 4.2 Snapshot/Audit 消息处理

**规则 1: 墓碑保护 (Tombstone Protection)**
若路径存在于 Tombstone List，仅当 `event.mtime > tombstone.logical_ts` 时才允许复活（转世），否则丢弃。

**规则 2: Mtime 仲裁 (Mtime Arbitration)**
若内存树中已存在该节点：
- 若 `existing.mtime >= event.mtime`，视为旧数据丢弃。
- 若 `existing.mtime < event.mtime`，更新节点。

**规则 3: 陈旧证据保护 (Stale Evidence Protection)**
在 Audit End 执行 Missing Detection 时：
- 若内存节点的 `last_updated_at > last_audit_start`，说明在审计期间有实时更新，**禁止删除**该节点，即使它未出现在审计列表中。

---

## 5. 交互原语 (SDP Primitives)

| 原语 | 说明 |
|------|------|
| **`on_modified`** | 文件内容变更，`is_atomic_write=False`，触发 Suspect。 |
| **`on_closed`** | 文件写关闭，`is_atomic_write=True`，清除 Suspect。 |
| **`audit_start`** | 标记审计周期开始，记录 `last_audit_start` 物理时间。 |
| **`audit_end`** | 标记审计结束，触发 Missing Detection 和 Tombstone 清理。 |

---

## 6. 特殊场景处理

### 6.1 就地更新 (In-Place Updates)
对于 `cp -p` 或 `rsync -a` 保留旧 mtime 的场景：
- fustord 依赖 `last_updated_at` 识别这是新写入的（尽管 mtime 很旧）。
- Missing Detection 会跳过 `last_updated_at > audit_start` 的节点，防止误删。

### 6.2 审计跳过 (Audit Skipped)
若 sensord 报告某目录 `audit_skipped=True`（静默优化），fustord 在执行 Missing Detection 时必须**跳过**该目录下的所有子孙节点检查。

---

## 7. API 反馈

| 状态 | API 字段 |
|------|----------|
| **Blind-spot** | `/stats` 返回 `has_blind_spot: true` |
| **Suspect** | 节点字段 `integrity_suspect: true` |
| **Missing Monitoring** | 节点字段 `sensord_missing: true` (即 `not known_by_sensord`) |
