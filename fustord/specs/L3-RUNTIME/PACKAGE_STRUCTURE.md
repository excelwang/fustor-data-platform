# L3: [structure] [Fustord] Package Topology

> Type: structure
> Layer: Implementation Layer (Code Organization)

---

## 1. Core Packages

- `fustord`: The main daemon entry point, CLI, and lifecycle management.
- `fustord-core`: Shared libraries and interfaces.
    - `clock/`: LogicalClock and Skew detection logic.
    - `session/`: SessionManager and Session state machine.
    - `view/`: Abstract base classes for View Drivers.
    - `stability/`: (Shared) BasePipeManager and common Mixins from `sensord-core`.

## 2. Extension Packages

- `fustor-receiver-*`: Transport protocol implementations.
    - `fustor-receiver-http`: HTTP/REST receiver (FastAPI/Aiohttp).
    - `fustor-receiver-grpc`: gRPC receiver (Future).
- `fustor-view-*`: Domain specific view drivers.
    - `fustor-view-fs`: File System Consistency View (Tombstones, Suspects).
    - `fustor-view-fs-forest`: Multi-tree Aggregation View.
