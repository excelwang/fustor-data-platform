# L2: Fustord Architecture

> Type: architecture | structure
> Layer: L2

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

---

## 3. Package Structure

The fustord codebase is organized into core and extension packages:

### 3.1 Core Packages

- `fustord`: The main daemon entry point, CLI, and lifecycle management.
- `fustord-core`: Shared libraries and interfaces.
    - `clock/`: LogicalClock and Skew detection logic.
    - `session/`: SessionManager and Session state machine.
    - `view/`: Abstract base classes for View Drivers.

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

## 6. Configuration Structure

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

## 7. API Surface

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/api/v1/pipe/session` | Create/Renew Session |
| POST | `/api/v1/pipe/{sess_id}/events` | Ingest Event Batch |
| POST | `/api/v1/pipe/{sess_id}/heartbeat` | Keep-Alive & Get Commands |
| DELETE | `/api/v1/pipe/{sess_id}` | Close Session |
| GET | `/api/v1/views/{view_id}/stats` | Query View Statistics |
| GET | `/api/v1/views/{view_id}/tree` | Browse View Directory Tree |
