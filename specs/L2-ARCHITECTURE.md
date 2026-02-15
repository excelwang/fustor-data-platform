---
version: 1.0.0
---

# L2: Fustor Architecture

> This document defines the high-level component structure.
> Each component traces to L1 contracts and decomposes into L3 items.

> **Subject**: Role (Active) | Component (Passive)
> - Role: Observes / Decides / Acts (Agent-driven)
> - Component: Input / Output (Script-driven)

---

## COMPONENTS.LAYER_MODEL

### 三层垂直模型

```mermaid
graph TD
    subgraph "L3: Management Plane (运维层/可选)"
        L3_Fusion["fustor-view-mgmt (Orchestration/UI)"]
        L3_Agent["fustor-source-mgmt (Command Support)"]
    end

    subgraph "L2: Session & Control Plane (会话层/核心)"
        L2_Fusion["SessionManager (Presence Tracking)"]
        L2_Agent["AgentPipe (Heartbeat/Umbilical Cord)"]
    end

    subgraph "L1: Data Plane (数据层/执行)"
        L1_Source["Source Drivers (FS/SQL/etc.)"]
        L1_View["View Drivers (Arbitration/Merge)"]
        L1_Sender["Sender/Receiver Drivers"]
    end

    %% Dependencies
    L3_Fusion -.->|Injects Admin Cmd| L2_Fusion
    L2_Agent <==>|L2 Heartbeat Tunnel| L2_Fusion
    L2_Agent ---|Spawns/Monitors| L1_Source
    L1_Source ---|Produces Events| L2_Agent
    L1_View ---|Queries/Updates| L2_Fusion
```

**Implements**:
- (Ref: CONTRACTS.LAYER_INDEPENDENCE)

---

## COMPONENTS.SYMMETRY

### 术语对称表

| Agent 概念 | 职责 | Fusion 对应 | 职责 |
|-----------|------|------------|------|
| **Source** | 数据读取实现 | **View** | 数据处理实现 |
| **Sender** | 传输通道（协议+凭证） | **Receiver** | 传输通道（协议+凭证） |
| **AgentPipe** | 运行时绑定 (Source→Sender) | **FusionPipe** | 运行时绑定 (Receiver→View) |

### 设计原则

1. **完全松耦合**: Agent 和 Fusion 完全独立，第三方可单独使用任一端
2. **对称架构**: Agent 与 Fusion 的概念一一对应
3. **分层清晰**: 参考 Netty 架构，职责分离
4. **可扩展**: 支持多协议、多 Schema

---

## COMPONENTS.CORE

Core components that implement primary functionality.

### COMPONENTS.CORE.PACKAGES

#### Package Structure

**Component**: Fustor monorepo package organization.

```
extensions/
├── fustor-core/                     # 核心抽象层
│   └── src/fustor_core/
│       ├── common/                  # 通用工具 (原 fustor-common)
│       │   ├── logging.py
│       │   ├── daemon.py
│       │   ├── paths.py
│       │   └── utils.py
│       ├── event/                   # 事件模型 (原 fustor-event-model)
│       │   ├── base.py              # EventBase
│       │   └── types.py             # EventType, MessageSource
│       ├── pipe/                    # Pipe 抽象
│       │   ├── pipe.py              # FustorPipe ABC
│       │   ├── context.py           # PipeContext
│       │   └── handler.py           # Handler ABC
│       ├── transport/               # 传输抽象
│       │   ├── sender.py            # Sender ABC
│       │   └── receiver.py          # Receiver ABC
│       ├── clock/                   # 时钟 (通用，不依赖特定 Schema)
│       │   └── logical_clock.py
│       ├── config/                  # 配置模型
│       │   └── models.py
│       └── exceptions.py
│
├── fustor-agent-sdk/                # Agent 开发 SDK
├── fustor-fusion-sdk/               # Fusion 开发 SDK
```

### COMPONENTS.CORE.SCHEMA

#### fustor-schema-fs

**Component**: File system data contract definition.

```
extensions/
├── fustor-schema-fs/                # 文件系统 Schema
│   └── src/fustor_schema_fs/
│       ├── __init__.py
│       ├── event.py                 # FSEventRow (Pydantic 模型)
│       └── version.py               # SCHEMA_NAME, SCHEMA_VERSION
```

**Implements**:
- (Ref: CONTRACTS.DATA_ROUTING.SEMANTIC_SCHEMA)

### COMPONENTS.CORE.HANDLERS

#### Source/View Handler Packages

**Component**: Data handler implementation packages.

```
extensions/
├── fustor-source-fs/                # FS Source Driver
├── fustor-source-oss/               # OSS Source Driver
├── fustor-view-fs/                  # FS View Driver (含一致性逻辑)
│   └── src/fustor_view_fs/
│       ├── handler.py               # FSViewHandler
│       ├── arbitrator.py            # 一致性仲裁 (fs 特有)
│       ├── state.py                 # Suspect, Blind-spot, Tombstone
│       └── nodes.py                 # 内存树节点
```

### COMPONENTS.CORE.TRANSPORT

#### Transport Packages

**Component**: Protocol-specific transport drivers.

```
extensions/
├── fustor-sender-http/              # HTTP Sender (原 pusher-fusion)
├── fustor-sender-grpc/              # gRPC Sender (新增)
├── fustor-receiver-http/            # HTTP Receiver (从 fusion 抽取)
├── fustor-receiver-grpc/            # gRPC Receiver (新增)
```

### COMPONENTS.CORE.APPLICATION

#### Application Packages

```
agent/                               # fustor-agent
fusion/                              # fustor-fusion
```

---

## COMPONENTS.TOPOLOGY

### COMPONENTS.TOPOLOGY.AGENT

#### Agent 侧关系

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                      │
│   Agent 侧                                                                           │
│   ─────────                                                                          │
│                                                                                      │
│   Source ──┬── AgentPipe ──┬── Sender                                          │
│   Source ──┘               └── Sender                                          │
│                                                                                      │
│   约束: <source, sender> 组合唯一 (同一组合只能启动一个 AgentPipe)                 │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### COMPONENTS.TOPOLOGY.FUSION

#### Fusion 侧关系

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                      │
│   Receiver : FusionPipe = 1 : N                                                  │
│   (一个 Receiver 可服务多个 FusionPipe)                                          │
│                                                                                      │
│   ┌──────────────────┐                                                              │
│   │ Receiver (HTTP)  │───┬──▶ FusionPipe-A ──▶ View-X                           │
│   │ Port: 8102       │   │                                                          │
│   │ API Key: fk_xxx  │   └──▶ FusionPipe-B ──┬──▶ View-X                       │
│   └──────────────────┘                       └──▶ View-Y                       │
│                                                                                      │
│   ─────────────────────────────────────────────────────────────────────────────────  │
│                                                                                      │
│   FusionPipe : View = 1 : N                                                      │
│                                                                                      │
│   View : FusionPipe = N : M                   
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### COMPONENTS.TOPOLOGY.MESSAGE_SYNC

#### Agent 消息同步架构

**Component**: EventBus-based message synchronization.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Agent 消息同步架构                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌──────────────┐         ┌─────────────────┐         ┌──────────────┐             │
│   │  FS Watch    │────────▶│    EventBus     │────────▶│  AgentPipe   │──▶ Fusion   │
│   │   Thread     │  put()  │   (MemoryBus)   │get()    │  Consumer    │             │
│   └──────────────┘         └─────────────────┘         └──────────────┘             │
│         │                         │                                                  │
│         │                    ┌────┴────┐                                ▶  Run      │
│         │               subscriber1  subscriber2                        │  Command  │
│       异步入队              (Pipe-A)  (Pipe-B)                          │  (Scan..) │
│       (不阻塞)                                                         ◀── Heartbeat│
│                                                                                      │
│   特性:                                                                              │
│   1. 生产者-消费者完全解耦 (Source 产生事件不被推送阻塞)                                │
│   2. 200ms 轮询超时 (低负载时延迟 ~0ms, 最坏 200ms)                                   │
│   3. 批量获取已有事件 (有多少取多少, 不等待凑满 batch)                                  │
│   4. 同源 AgentPipe 共享 Bus (节省资源, 减少重复读取)                                  │
│   5. 反向命令通道: Fusion 通过 Heartbeat 响应下发指令 (如 Real-Time Scan)                │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Implements**:
- (Ref: CONTRACTS.CONCURRENCY.QUEUE_ISOLATION)
- (Ref: CONTRACTS.STABILITY.EVENTBUS_RING_BUFFER)

#### EventBus 共享机制

同源的多个 AgentPipe 可共享同一个 EventBus：

```
Source Signature = (driver, uri, credential)

Pipe-A (source=fs-research) ──┐
                                  ├──▶ EventBus-1 (signature=fs:/data/research)
Pipe-B (source=fs-research) ──┘

Pipe-C (source=fs-archive)  ────▶ EventBus-2 (signature=fs:/data/archive)
```

每个订阅者独立跟踪消费进度：
- `last_consumed_index`: 已消费的最后一个事件索引
- `low_watermark`: 所有订阅者中最慢的位置 (用于缓冲区清理)

---

## COMPONENTS.SESSION

### Session 定义

Session 是 **AgentPipe** 和 **FusionPipe** 之间的业务会话。

### Session 数据结构

```python
@dataclass
class Session:
    session_id: str                    # 唯一会话 ID
    agent_task_id: str                 # AgentPipe 的 task_id
    fusion_pipe_id: str                # FusionPipe ID
    
    # 生命周期
    created_at: datetime
    last_active_at: datetime
    timeout_seconds: int               # 从 Pipe 配置获取
    
    # 状态追踪
    latest_event_index: int            # 断点续传 (Pipe 级别)
    
    # 认证
    receiver_id: str                   # 使用的 Receiver
    client_ip: str
```

### Session 生命周期

```
AgentPipe 启动
    │
    ├── Sender.connect() ────────────────────▶ Receiver 验证 API Key
    │   POST /api/v1/pipe/sessions/              │
    │   {task_id: "..."}                         ▼
    │                                       FusionPipe 创建 Session
    │                                            │
    │◀── 200 {session_id, timeout_seconds} ─────┤
    │                                            │
    ▼                                            ▼
事件推送 (携带 session_id)                    事件处理
心跳 (间隔 = timeout_seconds / 2)            刷新 last_active_at
    │                                            │
    ▼                                            ▼
Pipe 停止 或 网络断开                     Session 超时检测
    │                                            │
    └── DELETE /sessions/{id} ──────────────────▶│ View.on_session_close()
                                                 │ View 自行决定状态处理
                                                 │ (live 类型清空，否则保留)
```

---

## COMPONENTS.CONSISTENCY

### 组件层级

| 组件 | 层级 | 说明 |
|------|------|------|
| **LogicalClock** | View 级别 | 通用时间仲裁，不依赖特定 Schema |
| **Leader/Follower** | View 级别 | fs 特有，仅 view-fs 实现 |
| **审计周期** | View 级别 | fs 特有，由一致性方案决定哪个 Session 审计 |
| **Suspect/Blind-spot/Tombstone** | View 级别 | fs 特有，仅 view-fs 实现 |

### 多 Session 并发写入

同一 View 接收多个 Session 的事件时，使用 LogicalClock 仲裁：
- 比较事件的 mtime
- 更新的事件覆盖旧事件
- fs 特有的 Leader/Follower 逻辑在 view-fs 中实现

**Implements**:
- (Ref: CONTRACTS.CONCURRENCY.RW_LOCK_DISCIPLINE)
- (Ref: CONTRACTS.STABILITY.SUSPECT_ON_PARTIAL)

---

## COMPONENTS.CONFIG

### Agent 配置结构

```
$FUSTOR_AGENT_HOME/
├── sources-config.yaml              # Source 定义
├── senders-config.yaml              # Sender 定义 (原 pushers-config.yaml)
└── agent-pipes-config/              # AgentPipe 定义
    └── pipe-*.yaml
```

#### sources-config.yaml
```yaml
fs-research:
  driver: fs
  uri: /data/research
  enabled: true
  driver_params:
    throttle_interval_sec: 1.0
```

#### senders-config.yaml
```yaml
fusion-http:
  driver: http
  endpoint: http://fusion.local:8102
  credential:
    key: fk_research_key
  driver_params:
    batch_size: 100
```

#### agent-pipes-config/pipe-research.yaml
```yaml
id: pipe-research
source: fs-research
sender: fusion-http
enabled: true
audit_interval_sec: 600
sentinel_interval_sec: 120
```

### Fusion 配置结构

```
$FUSTOR_FUSION_HOME/
├── receivers-config.yaml            # Receiver 定义
├── views-config/                    # View 定义
│   └── view-*.yaml
└── fusion-pipes-config/             # FusionPipe 定义
    └── pipe-*.yaml
```

#### receivers-config.yaml
```yaml
http-receiver:
  driver: http
  bind: 0.0.0.0
  port: 8102
  credential:
    key: fk_research_key
  driver_params:
    max_request_size_mb: 16
```

#### views-config/fs-research.yaml
```yaml
id: fs-research
driver: fs
enabled: true
live_mode: false                     # false: Session 关闭保留状态
driver_params:
  hot_file_threshold_sec: 300
  blind_spot_style: detect
```

#### fusion-pipes-config/pipe-http.yaml
```yaml
id: pipe-http
receiver: http-receiver
views:                               # 1:N 关系
  - fs-research
  - fs-archive
enabled: true
session_timeout_seconds: 30
```

---

## COMPONENTS.API

### API 路径

| 路径 | 用途 |
|--------|--------|
| `/api/v1/pipe/session/` | Session 管理（创建/心跳/关闭） |
| `/api/v1/pipe/{session_id}/events` | 事件推送 |
| `/api/v1/pipe/consistency/*` | 一致性信号（audit_start/end, snapshot_end） |
| `/api/v1/pipe/pipes` | FusionPipe 管理（列表/详情） |
| `/api/v1/views/*` | 数据视图查询 |

### Session 创建响应

```json
{
  "session_id": "sess_xxx",
  "timeout_seconds": 30,
  "view_ids": ["fs-research", "fs-archive"]
}
```

Agent 收到响应后，设置心跳间隔为 `timeout_seconds / 2`。

---

## COMPONENTS.DEPENDENCIES

### 包依赖图

```
                              fustor-core
                    ┌────────────────┼────────────────┐
                    │                │                │
                    ▼                ▼                ▼
           fustor-agent-sdk   fustor-schema-*   fustor-fusion-sdk
                    │                │                │
          ┌─────────┼────────────────┼────────────────┼─────────┐
          │         │                │                │         │
          ▼         ▼                ▼                ▼         ▼
   fustor-source-*  fustor-sender-*       fustor-receiver-*  fustor-view-*
          │              │                     │              │
          └──────────────┴──────────┬──────────┴──────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼                               ▼
              fustor-agent                    fustor-fusion
```

## COMPONENTS.ROLES

### Peer-to-Peer 自主模型

Fustor 将 Agent 和 Fusion 视为 L1 稳定性层的 **平等租户 (Peer Tenants)**，而非主从关系：

*   **主动感知 (Proactive)**: Agent 拥有原生的领域冲动（L2），会根据配置自主启动监听并租用 L1 管道推送数据。不需要 Fusion 的"启动命令"。
*   **对等对称**: 双方使用相同的 L1 原语进行通信。区别仅在于 L2 驱动的类型：一端是 **感知源 (Source)**，另一端是 **聚合视图 (View)**。
*   **生存隔离**: 管理行为（L3）的失效不应影响数据面（L2）的自主同步与生命体征（L1）。

**Implements**:
- (Ref: CONTRACTS.LAYER_INDEPENDENCE)
- (Ref: CONTRACTS.COMMAND_DISPATCH.UNIFIED_RENTING)

### COMPONENTS.ROLES.FSDRIVER

#### FSDriver Singleton Lifecycle

为节省系统资源（如 inotify watch 描述符），FSDriver 实现了 **Per-URI Singleton** 模式。

-   **唯一标识**: `signature = f"{uri}#{hash(credential)}"`
-   **行为**:
    -   不同 AgentPipe 配置若指向同一 URI 且凭证相同，将共享同一个 Driver 实例。
    -   共享实例意味着共享底层的 WatchManager 和 EventQueue。
-   **生命周期约束**:
    -   **引用计数**: Driver 内部不维护引用计数（简化设计）。
    -   **显式销毁**: 必须调用 `driver.close()` 或 `FSDriver.invalidate(uri, cred)` 才能从缓存中移除。
    -   **热重载**: 修改配置（如排除列表）但 URI 不变时，ConfigReloader 必须显式 `invalidate` 旧实例。

---

## COMPONENTS.FIELD_MAPPING

### 字段映射与投影

**Component**: Field-level data transformation between Source and View.

#### 配置格式

```yaml
# Agent 端配置示例
pipes:
  my-agent-pipe:
    source: shared-fs
    sender: fusion-main
    fields_mapping:
      - to: "path"                    # 目标字段名
        source: ["path:string"]       # 源字段名[:类型转换]
      - to: "modified_time"
        source: ["modified_time:number"]
      - to: "custom_size"
        source: ["size:integer"]      # size → custom_size (重命名)
      - to: "label"
        hardcoded_value: "production" # 硬编码常量
```

**投影语义**:
- 已配置 `fields_mapping`（列表非空）：输出中**仅包含映射规则显式声明的字段**
- 未配置 `fields_mapping`（列表为空或缺省）：透明直通，所有字段原样传输

**Implements**:
- (Ref: CONTRACTS.DATA_ROUTING.FIELD_MAPPING_PROJECTION)
