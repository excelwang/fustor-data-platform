# L3: Protocol Carrier (SCP & SDP)

> **Subject**: SCP | SDP | Carrier
> **Layer**: Stability Layer (Shared Infrastructure)

---

## [overview] Protocol_Carrier_Layer_Overview

The Protocol Carrier layer is the **True Symmetry Point** of the Datacast/Fustord ecosystem. It defines the binary/textual wire formats and state machines used to maintain the "Umbilical Cord" and the "Data Stream".

## [protocol] SCP_Datacast_Control_Protocol_Definition

SCP is responsible for **Presence**, **Survival**, and **Orchestration**.

### 2.1 Handshake (Session Creation)
- **Symmetry**: Client proposes, Server disposes (with limits).
- **Contract**: Negotiation of `session_timeout` and `task_id`.

### 2.2 Heartbeat (The Umbilical Cord)
- **Carrier**: Bi-directional frame (usually HTTP Request/Response or WebSocket Ping/Pong).
- **Payload**:
    - **Upstream**: Health metrics, Current Role (L/F), Task Acknowledgments.
    - **Downstream**: Role assignment, Command dispatch (`scan`, `reload`, `upgrade`).

## [protocol] SDP_Datacast_Data_Protocol_Definition

SDP is responsible for **Event Delivery** and **Consistency Alignment**.

### 3.1 Batching & Idempotency
- **Carrier**: Unidirected Batch streams.
- **Contract**: Each batch MUST contain a sequence pointer or watermark to enable server-side deduplication.

### 3.2 Payload Structure
- **Frame**: Metadata (SourceID, ViewID, Schema) + Event Body.
- **Separation**: SDP frames MUST NOT be parsed by the Stability Layer; they are passed directly to the Domain Layer (ViewHandlers).

---

## [model] Architectural_Duality_Pipes_vs_Carriers

### 1.1 The Asymmetry of Pipes
**Pipes** are directional orchestrators. They define "how data moves through a node."
- **DatacastPipe**: Active orchestrator (Source $\rightarrow$ EventBus $\rightarrow$ Sender). Handles local drift, scanning, and pushing.
- **FustordPipe**: Passive orchestrator (Receiver $\rightarrow$ Queue $\rightarrow$ ViewHandlers). Handles multi-source merging and backpressure.

> [!IMPORTANT]
> **Pipes MUST NOT be merged.** Their responsibilities are polar opposites (Client/Push vs. Server/Pull). Merging them would violate the Single Responsibility Principle.

### 4.2 The Symmetry of Decoupled Carriers
Symmetry is found in the **Protocol Carrier layer**, not the Pipe layer. The carrier handles the "mechanical delivery" of SCP/SDP frames.

| Symmetric Point | Responsibility | Implementation Carrier |
|-----------------|----------------|------------------------|
| **Frame Definition** | Shared models for SCP/SDP packets | `datacast_core.protocol` |
| **Negotiation Logic** | Logic for `session_timeout` and `role` | `datacast_core.protocol.scp` |
| **Reliability Primitives** | Retries (Client) / Backpressure (Server) | `Stability Layer` |

By extracting the Protocol Carrier into `Datacast-core`, we achieve ecosystem-wide alignment without creating a monolithic, over-complicated Pipe component.
