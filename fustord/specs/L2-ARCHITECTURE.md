# L2: Fustord Architecture

> Type: architecture | structure
> Layer: L2

---

## 0. Ecosystem Algebra (The Universal Laws)

> **Binding**: All components (Sensord, Fustord, SDKs).

### 0.1 The Relational Pair (Duality)
The Fustor ecosystem is defined by the mapping between **Sources** and **Views**.
$$ \text{Sensord(Source)} \xrightarrow{SDP \mid SCP} \text{Fustord(View)} $$

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
- Accepting connections from multiple autonomous **Sensord** nodes.
- Detecting consistency issues (skew, drift, missing data) across time and space.
- Serving unified, arbitrated views of the data via API.

## 2. Component Model

### 2.1 Three-Layer Model

| Layer | Responsibility | Components |
|-------|----------------|------------|
| **Management** | Orchestration, API, Config | `SessionManager`, `APIHandler`, `ConfigReloader` |
| **Domain** | Consistency, State, View | `FSViewDriver`, `LogicalClock`, `Arbitrator` |
| **Stability** | Transport, Ingestion, Queuing | `Receiver`, `FustordPipe`, `MemoryEventBus` |

### 2.2 Peer-to-Peer Symmetry

Fustord mirrors Sensord's architecture:

| Concept | Sensord Side | Fustord Side |
|---------|--------------|--------------|
| **Pipeline** | `SensordPipe` | `FustordPipe` |
| **Flow** | Source → Sender | Receiver → View |
| **Stability** | Active Push (Client) | Passive Ingestion (Server) |
| **Protocol Carrier** | `SenderHandler` (sensord) | `Receiver` (fustord) ([spec]) |

---

## 3. Package Structure

The fustord codebase is organized into core and extension packages:

### 3.1 Core Packages

- `fustord`: The main daemon entry point, CLI, and lifecycle management.
- `fustord-core`: Shared libraries and interfaces.
    - `clock/`: LogicalClock and Skew detection logic.
    - `session/`: SessionManager and Session state machine.
    - `view/`: Abstract base classes for View Drivers.
    - `stability/`: (Shared) BasePipeManager and common Mixins from `sensord-core`.

### 3.2 Extension Packages

- `fustor-receiver-*`: Transport protocol implementations.
    - `fustor-receiver-http`: HTTP/REST receiver (FastAPI/Aiohttp).
    - `fustor-receiver-grpc`: gRPC receiver (Future).
- `fustor-view-*`: Domain specific view drivers.
    - `fustor-view-fs`: File System Consistency View (Tombstones, Suspects).
    - `fustor-view-fs-forest`: Multi-tree Aggregation View.

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
- **Backpressure**: If a Queue is full (Consumer slow), Receiver replies with `429 Too Many Requests`. This forces Sensord to slow down (Per-Pipe backpressure).

### 4.2 Pipeline & Views
- **1-to-N Mapping**: One `FustordPipe` can dispatch events to multiple `Views`.
- **Example**: `pipe-http` receiving data can feed both `view-hot` (In-Memory) and `view-archive` (SQL Database) simultaneously.

---

## 5. Session Lifecycle

Fustord manages the lifecycle of connected Sensord nodes via `SessionManager`.

### 5.1 Handshake (PUT /session)
- Sensord initiates connection.
- Fustord validates `task_id` and credentials.
- Fustord assigns a `session_id` and negotiates `timeout`.

### 5.2 Heartbeat (POST /heartbeat)
- Use as "Keep-Alive".
- **Piggyback Command**: Fustord includes "Commands" (like `scan`, `audit`) in the heartbeat response body.

### 5.3 Termination
- **Explicit**: Sensord sends `DELETE /session`.
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
- **Target**: Specific `Sensord` instance (via `task_id`).
- **Primitive**: `Stability.unicast(sensord_id)`
- **Success Criteria**: Ack from target.
- **Failure**: Control Loss.

### 6.2 TaskOrchestrator
A dedicated service (`TaskOrchestrator`) isolates dispatch complexity:
- **Input**: High-level Intent (`scan view:research`)
- **Resolution**: Resolves `view_id` to list of active `session_ids`.
- **Dispatch**: Uses `SessionManager` to push commands via Heartbeat response.

---

## 7. Configuration Structure

Settings are loaded from `$FUSTORD_HOME`.

```yaml
# receivers.yaml - 定义监听端口
http-main:
  driver: http
  port: 18881
  api_keys: ["key-1", "key-2"]

# views/main-view.yaml - 定义业务视图
main-fs-view:
  driver: fs-view
  driver_params:
      hot_file_threshold: 60.0

# pipes/pipe-1.yaml - 定义绑定关系
pipe-1:
  receiver: http-main
  views: [main-fs-view]
```

---

## 8. API Surface

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/api/v1/pipe/session` | Create/Renew Session (SCP) |
| POST | `/api/v1/pipe/{sess_id}/events` | Ingest Event Batch (SDP) |
| POST | `/api/v1/pipe/{sess_id}/heartbeat` | Keep-Alive & Get Commands (SCP) |
| DELETE | `/api/v1/pipe/{sess_id}` | Close Session (SCP) |
| GET | `/api/v1/views/{view_id}/stats` | Query View Statistics |
| GET | `/api/v1/views/{view_id}/tree` | Browse View Directory Tree |

