# L0: [fustord] Project Vision

> **Core Purpose**: Define the strategic vision for **fustord**, the centralized aggregator and synchronization arbitrator of the Fustor ecosystem.
> All spec statements must trace back to items defined here.

## VISION.SCOPE

**fustord** is a centralized data aggregation and consistency arbitration engine. It serves as the "truth anchor" for distributed data states collected by autonomous **Sensord** nodes.

### In-Scope
- **AGGREGATION**: Unified indexing of metadata from multiple heterogeneous **Sensord** sources.
- **CONSISTENCY**: Multi-source arbitration using Tombstone, Suspect, and Blind-spot mechanisms.
- **AVAILABILITY**: "Presence is Service" — The API must never return 503; it performs On-Command Fallback scans if data is missing.
- **RESILIENCE**: fustord and its View Engine must stay operational regardless of individual Sensord failures or network partitions.
- **ORCHESTRATION**: Centralized fleet management (upgrades, config reloads) of connected Sensord nodes via SCP.
- **EXTENSIBILITY**: Support for pluggable View Drivers (FS, Forest, Search, etc.) to project data into different business views.

### Out-of-Scope
- **STORAGE**: fustord does NOT store user file contents; it persists and serves metadata views.
- **AUTH_PROXY**: fustord does NOT act as a general authentication proxy; it uses API key-based pipe authorization for backend service-to-service communication.

## VISION.STABILITY

> "The View must survive the Source."

The fundamental architectural goal of **fustord** is the **absolute decoupling of View State from Source Reliability**.

- **FUSTORD_SURVIVAL**: Once the fustord process starts, it **MUST NOT** terminate due to ingestion errors, invalid data frames, or malformed protocol packets. It is the indestructible anchor of the system.
- **VIEW_ISOLATION**: Failure in one View (e.g., a memory leak in a specific plugin) must not affect the stability of the Management API or other unrelated Views.
- **UMBILICAL_CONTROL**: fustord maintains the "Umbilical Cord" (SCP Tunnel) to every Sensord.

## VISION.PROTOCOL

**fustord** 官方定义了控制链路 (SCP) 与数据链路 (SDP) 的交互契约。

- **Idempotency**: 无论底层网络重传多少次，数据的变更在视图中必须具备幂等性。
- **Contract Enforcement**: 强制执行 Schema 校验，确保非法格式无法进入领域层。

## VISION.DOMAIN

**fustord** 领域层负责核心的一致性裁决逻辑。通过对 Watermark、Tombstone 及 Suspect 状态的数学模拟，重建分布式存储的真实视图。

## VISION.LIFECYCLE

管理 Sensord 集群的完整生命周期，包括灰度升级、原子配置分发及僵尸节点检测。

---

## VISION.AUTONOMY

**fustord** 采用 **"中心化协调，去中心化执行"** 的自治模型：

- **COORDINATOR_MINDSET**: fustord 不是 Sensord 的"主人"，而是"仲裁者"。它尊重每一个 Sensord 上报的本地事实，仅在多个事实冲突时根据一致性代数进行裁决。
- **ON_DEMAND_DRIVE**: fustord 的数据抓取是"按需驱动"的。如果缓存中没有数据，Domain Layer 会自动通过 Stability Layer 触发广播扫描。
- **PEER_NEUTRABILITY**: fustord 对接入的 Sensord 保持中立。

---

## VISION.CONCURRENCY

**fustord** 核心引擎采用单线程事件循环确保状态机变更的确定性，同时支持并发查询。

---

## VISION.LAYER_INDEPENDENCE

遵循严格的分层隔离原则，确保稳定性层与业务域逻辑解耦。


## VISION.SUCCESS_CRITERIA

- **ZERO_503**: `fustord` 的查询 API 在高并发和断连场景下依然保持极高可用性。
- **CONSISTENCY_QUORUM**: 只要集群中存在一个健康的 Leader Sensord，fustord 的视图即可保证最终一致。
- **FLEET_CONTROL**: 100% 的 Sensord 在线维护（升级、配置、重启）均通过 fustord 控制台完成。

---

## VISION.UBIQUITOUS_LANGUAGE

| Term | Definition |
|------|------------|
| fustord | 中央聚合与协调服务进程 |
| Sensord | 外部自主传感器节点 |
| Pipe | fustord 与 Sensord 之间的逻辑数据通道 |
| Source | 数据产出驱动（sensord 侧驱动） |
| View | 数据汇聚驱动（fustord 侧驱动） |
| Session | 基于 Pipe 建立的带租约的业务会话 |
| SCP | Sensord Control Protocol (控制流协议，用于生存与管理) |
| SDP | Sensord Data Protocol (数据流协议，用于事件传输) |
| Watermark | 逻辑时钟水位线，用于判断数据时效性 |
| Leader | 在多源场景下，负责执行补偿扫描的任务承担者 |
| Follower | 仅执行增量同步，作为 Leader 的热备 |
| Sentinel | 周期性的一致性健康检查机制 |
| Tombstone | 已删除文件的逻辑标记，防止旧快照导致数据"回魂" |
| Suspect | 疑似不完整写入的文件标记 |
| Blind-spot | inotify 未覆盖区域发现的文件标记 |
| Receiver | fustord 侧的协议接收组件 (HTTP/gRPC) |
| Forest View | 多源聚合视图，将多个 Source 映射为统一的业务目录树 |
