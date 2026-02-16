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

利用 POSIX 语义：创建/删除文件只更新**直接父目录**的 mtime。

**True Silence (静默跳过)** 机制：
1. 递归进入目录，获取当前目录 `mtime`。
2. 比对缓存：若 `current_mtime == cached_mtime`，判定该目录在存储面“静默”。
3. **静默标记**: 即使静默，sensord 仍发送 `audit_skipped=True` 的空数据帧，告知 Consumer “此处无恙”，保护子项不被误删。
4. **子目录递归**: 无论当前层是否静默，必须始终递归检查子目录。

```python
def audit_worker(root):
    current_dir_mtime = os.stat(root).st_mtime
    cached_mtime = mtime_cache.get(root)
    
    # 判定静默：如果 mtime 未变，说明当前目录下的**直接文件列表**未变
    is_silent = (cached_mtime is not None and cached_mtime == current_dir_mtime)
    
    # 无论是静默还是非静默，必须 scandir 获取子目录以便递归
    # (注意：os.scandir 是 generator，不消耗大量内存)
    with os.scandir(root) as it:
        for entry in it:
            if entry.is_dir():
                # 递归：放入队列继续深度优先或广度优先扫描
                work_queue.put(entry.path)
            
            elif entry.is_file():
                if is_silent:
                    continue  # 静默优化：跳过文件 stat 和 event 发送
                
                # 非静默：完整发送文件事件
                send_audit_event(entry.path, parent_path=root, parent_mtime=current_dir_mtime)

    # 如果静默，发送特定的 audit_skipped 标记，告知 Consumer "这个目录我看过了，没变"
    if is_silent:
        send_audit_event(root, audit_skipped=True)
    else:
        # 更新缓存
        mtime_cache[root] = current_dir_mtime
```

### 3.2 Audit Message Format (审计消息格式)

Audit 消息复用标准 Event 结构，但包含用于一致性裁决的额外字段：

```json
{
  "message_source": "audit",
  "event_type": "UPDATE",
  "index": 1706000000000,
  "rows": [
    {
      "path": "/data/file.txt",
      "modified_time": 1706000123.0,
      "size": 10240,
      "parent_path": "/data",           // Required for Parent Mtime Check
      "parent_mtime": 1706000100.0,     // Required for Parent Mtime Check
      "audit_skipped": false
    },
    {
      "path": "/data/silent_dir",
      "audit_skipped": true             // Directory skipped
    }
  ]
}
```

### 3.3 NFS Clock Drift Compensation (NFS 时钟漂移补偿)

由于 sensord 运行在物理机上的时钟可能与 NFS Server 的时钟（即文件 mtime 的来源）存在偏差，为了保证物理时间戳（index）与逻辑时间戳（mtime）的可比性，Source Driver 必须执行漂移补偿。

**Mechanism (Shadow Reference Frame)**:
- **Sampling**: SensordPipe 启动时执行 Pre-scan，收集所有目录的 recursive mtime。
- **Reference Selection**: 选取 P99 分位的 mtime 作为 `latest_mtime_stable` (排除未来时间或极端异常值)。

**Implementation Detail**:

```python
# 1. P99 漂移采样——过滤前 1% 异常值
mtimes = sorted(dir_mtime_map.values())
p99_idx = max(0, int(len(mtimes) * 0.99) - 1)
latest_mtime_stable = mtimes[p99_idx]

# 2. 计算漂移
self.drift_from_nfs = latest_mtime_stable - time.time()

# 3. 修正事件时间戳
event.index = int((time.time() + self.drift_from_nfs) * 1000)
```

- **Purpose**: 确保 Consumer 收到的事件 `index` 大致对齐到 NFS 的时间轴，防止因 SensordPipe 时钟大幅落后导致事件被误判为"陈旧"而被丢弃。

### 3.4 Path Normalization Contract (路径归一化契约)

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
