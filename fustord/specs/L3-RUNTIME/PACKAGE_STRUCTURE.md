# L3: [structure] [Fustord] Package Topology

> Type: structure
> Layer: Implementation Layer (Code Organization)

---

## [definition] Fustord_Core_Package_Organization

- `fustord`: The main daemon entry point, CLI, and lifecycle management.
- `fustord-core`: Shared libraries and interfaces.
    - `clock/`: LogicalClock and Skew detection logic.
    - `session/`: SessionManager and Session state machine.
    - `view/`: Abstract base classes for View Drivers.
    - `stability/`: (Shared) BasePipeManager and common Mixins from `sensord-core`.

## [definition] Fustord_Driver_Extension_Packages

- `fustord-receiver-*`: Transport protocol implementations.
    - `fustord-receiver-http`: HTTP/REST receiver.
    - `fustord-receiver-grpc`: gRPC receiver (Future).
- `fustord-view-*`: Domain specific view drivers.
    - `fustord-view-fs`: File System Consistency View (Tombstones, Suspects).
    - `fustord-view-fs-forest`: Multi-tree Aggregation View.
