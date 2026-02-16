# L2: Fustord Architecture

> Type: architecture | structure
> Layer: L2

---

## 0. Ecosystem Algebra (The Universal Laws)

> **Binding**: All components (Datacast, Fustord, SDKs).

### 0.1 The Relational Pair (Duality)
The Fustor ecosystem is defined by the mapping between **Sources** and **Views**.
$$ \text{Datacast(Source)} \xrightarrow{SDP \mid SCP} \text{Fustord(View)} $$

### 0.2 Structural Algebra (n:m Mapping)
1. **Receiver : Pipe = 1 : N** (Demultiplexing)
2. **View : Pipe = N : M** (Aggregation)

### 0.3 Layer Algebra (Nested Invariants)
Every node MUST adhere to the **Stability $\succ$ Domain $\succ$ Management** hierarchy.
- **Stability (The Shell)**: MUST be neutral, mechanical, and indestructible.
- **Domain (The Payload)**: MUST be autonomous and logical.
- **Management (The Plugin)**: MUST be optional and operational.

### 0.4 Protocol Algebra (The Umbilical Cord)
- **Control Plane (SCP)**: Unicast/Broadcast semantics for Survival & Orchestration.
- **Data Plane (SDP)**: Idempotent event streams for Consistency.
- **Survival Rule**: SCP MUST persist even if the Data Plane is crashing or corrupted.

---

## 1. System Overview

**Fustord** is a centralized aggregator and synchronization engine. It acts as the "Server" in the Fustor peer-to-peer model, responsible for:
- Accepting connections from multiple autonomous **Datacast** nodes.
- Detecting consistency issues (skew, drift, missing data) across time and space.
- Serving unified, arbitrated views of the data via API.

## 2. Component Model

### 2.1 Three-Layer Model

| Layer | Responsibility | Components |
|-------|----------------|------------|
| **Management** | Orchestration, API, Config | `SessionManager`, `APIHandler`, `ConfigReloader` |
| **Domain** | Consistency, State, View | `FSViewDriver`, `LogicalClock`, `Arbitrator` |
| **Stability** | Transport, Ingestion, Queuing | `Receiver`, `FustordPipe`, `MemoryEventBus` |

#### Layer Definitions

**Stability Layer (Ingestion & Session)**
- **Strategic Intent**: Ensure fustord is always "visible" and "reachable".
- **Responsibility**: Maintains physical links (Pipes) and business leases (Sessions).
- **Neutrality**: Only guarantees mechanical delivery of SCP/SDP packets. Agnostic to business logic.

**Domain Layer (Arbitration & Views)**
- **Strategic Intent**: Achieve eventual consistency of distributed data.
- **Responsibility**: The Core Brain. Handles logical clocks (Watermark), multi-source reconciliation (Leader Election), and View implementations.
- **Autonomy**: Even if Management plugins are unloaded, the Domain Layer can still maintain consistency based on real-time data.

**Management Layer (Fleet Ops & UI)**
- **Strategic Intent**: Provide a human-operable control plane.
- **Responsibility**: External interaction and policy dispatch.
- **Pluggability**: All UI interfaces and ops tools are optional plugins.

### 2.2 Peer-to-Peer Symmetry

Fustord mirrors Datacast's architecture:

| Concept | Datacast Side (Source) | Fustord Side (View) |
|---------|-----------------------|---------------------|
| **Pipeline** | `DatacastPipe` | `FustordPipe` |
| **Flow** | Source → Sender | Receiver → View |
| **Stability** | Active Push (Client) | Passive Ingestion (Server) |
| **Protocol Carrier** | `SenderHandler` | `Receiver` |

---

## 3. Lexicon Symmetry (Internal Mapping)

| Datacast Concept | Responsibility | Fustord Concept (Aggregator) |
|-----------------|----------------|-----------------------------|
| **Source** | Local data extraction | **View** |
| **Sender** | Transport channel | **Receiver** |
| **DatacastPipe** | Source $\rightarrow$ Sender binding | **FustordPipe** |
| **task_id** | Datacast process/task ID | **session_id** |
| **SCP** | Control flow | **Control Channel** |
| **SDP** | Data flow | **Data Channel** |

---



## 4. Ingestion Topology

Data flows from the network into the View memory state through a strictly decoupled pipeline:

```mermaid
graph LR
    Net[Network Request] --> Receiver[Receiver (HTTP)]
    Receiver -->|Put Batch| Queue[AsyncIO Queue]
    Queue -->|Pop Batch| Pipe[FustordPipe]
    Pipe -->|Dispatch| View[View Driver]
    View -->|Update| State[Memory State]
```

### 4.1 Receiver & Backpressure
- **Receiver**: Terminates HTTP/TCP connections.
- **Isolation**: Each `FustordPipe` has a dedicated `asyncio.Queue`.
- **Backpressure**: If a Queue is full (Consumer slow), Receiver replies with `429 Too Many Requests`. This forces Datacast to slow down (Per-Pipe backpressure).

### 4.2 Pipeline & Views
- **1-to-N Mapping**: One `FustordPipe` can dispatch events to multiple `Views`.
- **Example**: `pipe-http` receiving data can feed both `view-hot` (In-Memory) and `view-archive` (SQL Database) simultaneously.

---

## 5. Session Lifecycle

Fustord manages the lifecycle of connected Datacast nodes via `SessionManager`.

### 5.1 Handshake (PUT /session)
- Datacast initiates connection.
- Fustord validates `task_id` and credentials.
- Fustord assigns a `session_id` and negotiates `timeout`.

### 5.2 Heartbeat (POST /heartbeat)
- Use as "Keep-Alive".
- **Piggyback Command**: Fustord includes "Commands" (like `scan`, `audit`) in the heartbeat response body.

### 5.3 Termination
- **Explicit**: Datacast sends `DELETE /session`.
- **Timeout**: If no heartbeat for `timeout` seconds, Fustord marks Session as `stale` or `closed` (depending on View policy).

---

## 6. Unified Renting Model & Orchestration

Domain/Management services utilize the Stability Layer as a generic "renting" platform.

### 6.1 Task Types

#### 1. Broadcast Task (Content Compensation)
- **Purpose**: Data Compensation (e.g., `scan` / On-Demand Find).
- **Target**: All active sessions for a specific `View`.
- **Primitive**: `Stability.broadcast(view_id)`
- **Success Criteria**: Quorum/Full (Aggregate data from all sources).
- **Failure**: Partial Data (Blind spots).

#### 2. Targeted Task (State Mutation)
- **Purpose**: Control & Lifecycle (e.g., `upgrade`, `reload`, `stop`).
- **Target**: Specific `Datacast` instance (via `task_id`).
- **Primitive**: `Stability.unicast(datacast_id)`
- **Success Criteria**: Ack from target.
- **Failure**: Control Loss.

### 6.2 TaskOrchestrator
A dedicated service (`TaskOrchestrator`) isolates dispatch complexity:
- **Input**: High-level Intent (`scan view:research`)
- **Resolution**: Resolves `view_id` to list of active `session_ids`.
- **Dispatch**: Uses `SessionManager` to push commands via Heartbeat response.

---

## 7. API Surface

fustord 对外提供两类 API 接口：

| 功能域 | 协议 | 方向 |
|--------|------|------|
| Session 管理 (创建/心跳/销毁) | SCP | Datacast ↔ Fustord |
| 数据事件批量接收 | SDP | Datacast → Fustord |
| View 查询 (目录树/统计) | HTTP | Client → Fustord |

> 具体 API Path、Payload 格式及哨兵巡检接口见 [PROTOCOL_CARRIER.md](./L3-RUNTIME/PROTOCOL_CARRIER.md) 和 [CONSISTENCY.md](./L3-RUNTIME/CONSISTENCY.md)。

