# L2: [sensord] System Architecture

> This document defines the high-level component structure of **sensord**.
> **Subject**: Role (Active) | Component (Passive)
> - Role: Observes / Decides / Acts
> - Component: Input / Output

---

## COMPONENTS.LAYER_MODEL

### 三层垂直模型 (SCP/SDP Separation)

```mermaid
graph TD
    subgraph "External Consumer / Aggregator"
        fustord["Consumer (Arbitration/Merge)"]
        mgmt["Fleet Management (Optional)"]
    end

    subgraph "Sensord Domain Layer (核心业务 - SDP)"
        sensordSource["Source Drivers (FS/SQL/etc.)"]
        sensordCore["Sensord-core (ABCs/Events)"]
        SDP["Sensord Data Protocol"]
    end

    subgraph "Sensord Stability Layer (稳定性层 - SCP)"
        SensordPipe["sensordPipe (Lifecycle/Heartbeat)"]
        EventBus["EventBus (Memory Buffer)"]
        Sender["Sender Drivers (HTTP/gRPC)"]
        SCP["Sensord Control Protocol"]
    end

    %% Dependencies
    SensordPipe <==>|SCP (Umbilical Cord)| fustord
    SensordPipe ---|Spawns/Monitors| sensordSource
    sensordSource ---|SDP (Data Contract)| EventBus
    EventBus ---|Drained by| SensordPipe
    SensordPipe ---|Uses| Sender
    mgmt -.->|SCP Control Commands| SensordPipe
```

---

### 2.2 Package Topology (Sub-system Mapping)

```mermaid
graph TD
    subgraph "Sensord (Standalone Project)"
        Core["Sensord-core (Foundation)"]
        Daemon["Sensord (The Shell)"]
        SrcFS["Sensord-source-fs (Scanner Driver)"]
        SendHTTP["Sensord-sender-http (Transport)"]
        SchemaFS["Sensord-schema-fs (FS Data Contract)"]
    end

    subgraph "External Consumer (e.g., fustord)"
        Aggregator["Aggregator System"]
        ViewFS["View-FS Driver"]
    end

    %% Internal Dependencies
    Daemon --> Core
    SrcFS --> Core
    SrcFS --> SchemaFS
    SendHTTP --> Core

    %% External Interaction
    Aggregator ..> Core : Implements SCP Handshake
    ViewFS ..> SchemaFS : Imports SDP Contract
    Daemon <==>|SDP Flow / SCP Heartbeat| Aggregator
```

### 2.3 Component Mapping
| Original Package | New Standalone Identity | Role |
|------------------|-------------------------|------|
| `fustor-core` | `sensord-core` | Foundation ABCs & Models |
| `sensord` | `sensord` | Process Guardian (SCP/SDP Orchestrator) |
| `fustor-source-fs` | `sensord-source-fs` | FS Scanner Driver |
| `fustor-schema-fs` | `sensord-schema-fs` | FS Event Schema (SDP Implementation) |
| `fustor-sender-*` | `sensord-sender-*` | Network Transporters |

---

## COMPONENTS.SYMMETRY

### 术语对应表

| sensord 概念 | 职责 | 对应消费者概念 (Aggregator) |
|-----------|------|------------|
| **Source** | 本地数据读取实现 | **View** |
| **Sender** | 传输通道（协议+凭证） | **Receiver** |
| **SensordPipe** | 运行时绑定 (Source→Sender) | **ConsumerPipe** |
| **task_id** | 传感器任务唯一标识 | **session_id** |
| **SCP** | 控制流协议 | **Control Channel** |
| **SDP** | 数据流协议 | **Data Channel** |

---

## COMPONENTS.CORE

Core components that implement primary functionality.

### COMPONENTS.CORE.PACKAGES

#### Package Structure

**Component**: sensord engine package organization.

```
sensord/                             # sensord 守护进程主包
├── src/sensord/
│   ├── boot/                        # 引导与环境初始化
│   ├── core/                        # 运行时调度 (PipeManager)
│   └── cmd/                         # CLI 指令 (reload, start, version)

sensord-core/                         # 核心抽象层 (SDK)
├── src/sensord_core/
│   ├── common/                      # 通用工具 (logging, daemon, paths)
│   ├── event/                       # 统一事件模型 (EventBase, EventType)
│   ├── pipe/                        # 管道与 Handler ABC
│   ├── transport/                   # Sender ABC
│   ├── clock/                       # 影子参考系时钟算法
│   └── config/                      # Pydantic 配置模型
```

### COMPONENTS.CORE.DRIVERS

#### Source Driver Packages

**Component**: Data extraction implementation packages.

```
sensord-source-fs/                   # 文件系统 Source Driver
sensord-source-sql/                  # 数据库 Source Driver (待选)
sensord-sender-http/                 # HTTP 协议发送驱动
sensord-sender-grpc/                 # gRPC 协议发送驱动
```

---

## COMPONENTS.TOPOLOGY

### COMPONENTS.TOPOLOGY.INGESTION

#### sensord 侧数据流向 (SDP Transmission)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                      │
│   sensord Node                                                                       │
│   ───────────                                                                        │
│                                                                                      │
│   Source-A ──┬── sensordPipe-1 ──┬── Sender (HTTP) ──▶ Consumer A (SDP)              │
│   Source-B ──┘                   └── Sender (HTTP) ──▶ Consumer B (SDP)              │
│                                                                                      │
│   约束: <source, sender> 组合唯一 (由 sensordPipe 负责生命周期维护)                     │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### COMPONENTS.TOPOLOGY.EVENT_BUS (SDP Buffer)

#### sensord 异步采集架构

**Component**: EventBus-based message synchronization.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              sensord 消息同步架构                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌──────────────┐         ┌─────────────────┐         ┌──────────────┐             │
│   │  Local Watch │────────▶│    EventBus     │────────▶│  sensordPipe   │──▶ Consumer  │
│   │   Thread     │  put()  │   (MemoryBus)   │get()    │  Broadcaster   │    (SDP)     │
│   └──────────────┘         └─────────────────┘         └──────────────┘             │
│         │                         │                                                  │
│         │                    ┌────┴────┐                                ▶  SCP Task │
│         │               subscriber1  subscriber2                        │  (Scan..) │
│       异步入队              (Pipe-1)   (Pipe-2)                         │           │
│       (不阻塞)                                                         ◀── SCP HB   │
│                                                                                      │
│   特性:                                                                              │
│   1. 生产者-消费者完全解耦: 数据读取线程不被网络推送延迟阻塞                                │
│   2. 全局/按源共享 EventBus: 节省内存与文件系统句柄资源                                    │
│   3. 反向命令隧道 (SCP): 消费者通过 Heartbeat 响应下发控制指令                             │
│   4. 背压控制: 内部环形缓冲区防止 OOM，支持订阅者落后自动分裂                              │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## COMPONENTS.SESSION (SCP Protocol)

### Session (Lease) 定义

Session 是 **sensord** 与 **Consumer** 之间通过 **SCP** 建立的业务租赁关系。

### Session 生命周期 (Standalone Perspective)

```
sensord 启动
    │
    ├── SCP Handshake ───────────────────────▶ Consumer 验证端点与凭证
    │   POST /api/v1/pipe/sessions/              │
    │   {task_id, schema, ...}                   ▼
    │                                       Consumer 锁定资源，颁发 Lease
    │                                            │
    │◀── 200 {session_id, timeout_seconds} ─────┤
    │                                            │
    ▼                                            ▼
SDP 事件推送 (携带 session_id)                 Consumer 处理数据
SCP 心跳 (间隔 = timeout_seconds / 2)          Consumer 刷新 Lease 有效期
    │                                            │
    ▼                                            ▼
sensord 停止 或 网络断开                      Lease 过期处理
    │                                            │
    └── DELETE /sessions/{id} ──────────────────▶│ Consumer 处理收尾逻辑
                                                 │ 
```

---

## COMPONENTS.CONFIG

### sensord 目录结构与配置

**Root Path**: `$SENSORD_HOME` (default: `/etc/sensord`)

```
$SENSORD_HOME/
├── sources-config.yaml              # 存储源定义 (SDP Provider)
├── senders-config.yaml              # 发送器定义 (SCP Transport)
└── pipes-config/                    # 绑定关系 (source ID <-> sender ID)
    └── pipe-*.yaml
```

#### sources-config.yaml
```yaml
research-data:
  driver: fs
  uri: /mnt/nfs/research
  enabled: true
  driver_params:
    throttle_interval_sec: 1.0
```

#### senders-config.yaml
```yaml
main-aggregator:
  driver: http
  endpoint: http://aggregator.corp:8102
  credential:
    key: "sk_prod_12345"
```

#### pipes-config/pipe-research.yaml
```yaml
id: pipe-research
source: research-data
sender: main-aggregator
enabled: true
audit_interval_sec: 600
```

---

## COMPONENTS.PROTOCOL (SCP & SDP)

sensord 期望消费端实现的最小符合性接口：

| Path | Method | Protocol | Description |
|--------|--------|----------|-------------|
| `/api/v1/pipe/session/` | POST | SCP | 创建会话，获取 `session_id` |
| `/api/v1/pipe/{id}/events` | POST | SDP | 推送数据事件流 (Batched) |
| `/api/v1/pipe/session/{id}` | DELETE | SCP | 主动关闭会话 |
| `/api/v1/mgmt/upgrade` | POST | SCP | 执行远程升级指令 |

### SCP Heartbeat Payload
Consumer 在对 sensord 心跳的 HTTP 响应中，可以包含待执行的任务：

```json
{
  "status": "ok",
  "commands": [
    {
      "type": "scan",
      "params": {"path": "/data/subdir", "job_id": "job_99"}
    }
  ]
}
```

---

## COMPONENTS.AUTONOMY (Peer-to-Peer Model)

sensord 采用 **Peer-to-Peer** 协作模型，而非简单的 Agent 模型：

*   **租用模型 (Renting)**: sensord 视外部系统为“信道提供方”。当需要发布数据时，通过 SCP 启动一个租约进程（Session）。
*   **生存隔离**: 底层 `Stability Layer` 必须在逻辑上隔离各 Session。
*   **控制与数据解耦**: SCP 维持生存，SDP 维持契约。两者在协议层面互不干扰。
