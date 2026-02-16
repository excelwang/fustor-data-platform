# L0: Fustor Vision

> **Core Purpose**: Define the strategic vision that guides all lower layers (L1-L3).
> All spec statements must trace back to items defined here.

## VISION.SCOPE

Fustor is a distributed data synchronization framework that ensures data consistency across heterogeneous storage endpoints via a real-time event-driven pipeline using **Sensord** as the autonomous sensor engine.

### In-Scope
- **SYNC**: Real-time and periodic data synchronization across distributed storage (NFS, OSS, etc.)
- **CONSISTENCY**: Multi-source consistency arbitration with Tombstone, Suspect, and Blind-spot mechanisms
- **AVAILABILITY**: API must never return 503 — "Presence is Service" (On-Command Fallback)
- **RESILIENCE**: Sensord process runs indefinitely; the control plane is immune to data plane failures.
- **MANAGEMENT**: Remote fleet management (upgrade, config reload) as optional plugins
- **EXTENSIBILITY**: Schema-based driver architecture supporting third-party data sources and views

### Out-of-Scope
- **STORAGE**: Fustor does NOT store or persist user data; it indexes and synchronizes metadata
- **REPLICATION**: Fustor does NOT replicate file contents between nodes; it synchronizes state views
- **AUTH**: Fustor does NOT implement end-user authentication; it uses API key-based pipe authorization

## VISION.SURVIVAL

> "The Control Plane must survive the Data Plane."

Fustor 的根本架构目标是 **控制流 (SCP) 与数据流 (SDP) 的绝对解耦**。

- **Sensord_SURVIVAL**: Sensord 进程启动后，**绝不因**业务逻辑错误、数据损坏或插件故障而终止。它本质上是一个不朽守护进程，唯一任务是维持到 fustord 的生命线。
- **FUSTORD_SURVIVAL**: fustord 必须保持对心跳和管理指令的响应，不受数据处理管道或视图一致性逻辑状态的影响。
- **UMBILICAL_CORD**: 只要控制面（SCP 心跳、任务分发、状态上报）保持完整，系统就具备无限的修复潜力：
  1. **自我修复 (Self-Repair)**: 控制面可远程重启、重置或重新配置崩溃的数据面。
  2. **热升级 (Hot Upgrades)**: Sensord 接收软件版本更新，实现在线自置换。
  3. **配置热重载 (Config Hot-Reload)**: 动态更新业务逻辑，无需重启即可修复故障。

### Architecture of Separation (SCP/SDP)

```mermaid
graph TD
    subgraph "Indestructible Shell (Stability Layer - SCP)"
        SensordDaemon[Sensord Daemon] <-->|SCP Heartbeat / Tasks| fustordCore[fustord Core]
    end

    subgraph "Volatile Payload (Domain Layer - SDP)"
        Scanner[File Scanner]
        Syncer[Data Syncer]
        Parser[Format Parser]
    end

    SensordDaemon -.->|Spawns/Monitors| Scanner
    SensordDaemon -.->|Restarts| Syncer
    fustordCore -.->|Configures| Parser
```

- **The Shell (SCP)**: Responsible ONLY for authentication, network connectivity (Heartbeat), and process orchestration.
- **The Payload (SDP)**: Responsible for the actual "work" (FileSystem watching, Database querying). If the Payload crashes, the Shell detects it and awaits instructions via SCP.

## VISION.EXPECTED_EFFECTS

远程操作必须保证 Sensord 作为一个整体的原子性与一致性。

### Hot Upgrade
- **单点触发，进程重启**: 对于多连接 (Multi-Pipe) 的 Sensord 进程，升级指令必须具备**精准下发 (Targeted)** 能力。无论多少个 Session 活跃，fustord 仅触发其中一个，由 Sensord 完成全局自置换。
- **透明恢复**: 升级后，Sensord 自动恢复所有业务连接。

### Config Hot-Reload
- **全局生效 (Process-Wide)**: 新配置必须在 Sensord 所有组件中同步生效。
- **无感应用**: 配置更新秒级完成，不中断长连接或上传任务。

### On-Command Find (SDP Compensation)
- **全量广播 (Broadcast)**: 内存视图失效时，fustord 通过 SCP 向所有活跃源同时下发扫描指令，确保零数据盲区。
- **语义汇聚 (SDP Aggregation)**: 不同 Source 返回的 SDP 数据交由 View Driver 汇聚处理。

## VISION.LAYER_MODEL

Fustor 采用 **"下沉稳定性，上行扩展性"** 的三层垂直模型，严禁次序颠倒：

### Stability Layer (SCP Focused)
- **职责**: 纯粹的连接维持。负责物理链接 (Pipes)、SCP 心跳隧道 (Umbilical Cord)、生存状态监控。
- **中立原则**: Stability Layer 只提供**寻址原语** (Unicast / Broadcast)。
- **愿景定位**: 系统的"生存地基"。

### Domain Layer (SDP Focused)
- **职责**: 定义数据的"血肉"。包括数据驱动 (Source/View)、快照合并方案 (SDP)、API 核心查询逻辑。
- **自治原则**: API 的"永不 503"保障属于 Domain Layer 的**核心本能**。

### Management Layer (Operations & Plugins)
- **职责**: 非实时、非关键路径的管理工作（升级、迁移、UI 服务）。

## VISION.AUTONOMY

Fustor 拒绝"主从"式命令模型，推崇 **"感知驱动，按需对齐"** 的自主模型：

- **INTRINSIC_DRIVE**: Sensord 绝非被动等待 fustord 命令的傀儡。它是一个**主动的、有状态的传感器**。
- **INDEPENDENT_LIFECYCLE**: Sensord 的生存不依赖于 fustord。断网或 fustord 崩溃时，Sensord 感知逻辑 (Domain) 全速运行。
- **MULTI_TARGET_RENTING**: Sensord 可同时向多个 Receiver (fustord、三方工具) 租用 Stability 管道推送数据。
- **UNIVERSAL_ADDRESSING**: Stability Layer 仅提供 `broadcast` 和 `unicast` 两种寻址原语。

## VISION.SUCCESS_CRITERIA

- **UPTIME**: `Sensord` 进程运行时间以**月**计量。
- **ZOMBIE_RECOVERY**: fustord 可通过 SCP 诊断"僵尸 Sensord"（数据面卡死）并远程发出修复命令。
- **ZERO_TOUCH**: 无需 SSH。fustord 可推送新配置，Sensord Shell 用新配置重启数据面任务。

## VISION.UBIQUITOUS_LANGUAGE

| Term | Definition |
|------|------------|
| Sensord | 部署在数据节点上的自主传感器进程 (SCP/SDP Producer) |
| fustord | 中央聚合与协调服务 (SCP/SDP Consumer) |
| Pipe | Sensord/fustord 之间的逻辑数据隧道 |
| Source | 数据产出驱动（Sensord 侧） |
| View | 数据消费/汇聚驱动（fustord 侧） |
| Session | SensordPipe 与 FustordPipe 之间的业务会话 (Lease) |
| SCP | Sensord Control Protocol (Survival & Orchestration) |
| SDP | Sensord Data Protocol (Data Contract & Consistency) |
| Heartbeat | SCP 下的生存检测与指令隧道 |
| Schema | SDP 数据契约/格式标识（如 `fs`） |
| Tombstone | 已删除文件的逻辑标记 |
| Suspect | 疑似不完整写入的文件标记 |
| Blind-spot | inotify 未覆盖区域发现的文件标记 |
| Leader | 负责 Snapshot/Audit 同步的选举角色 |
| Follower | 仅负责 Realtime 同步的非选举角色 |
| Sentinel | 周期性哨兵巡检机制 |
| Watermark | 逻辑时钟水位线 |
