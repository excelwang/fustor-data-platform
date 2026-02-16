# L3: [structure] [Fustord] Package Topology

> Type: structure
> Layer: Implementation Layer (Code Organization)

---

## [definition] Fustord_Core_Package_Organization

- `fustord`: The main daemon entry point, CLI, and lifecycle management.
- `core`: (datacast-core) Shared libraries and interfaces.
    - `clock/`: LogicalClock and Skew detection logic.
    - `session/`: SessionManager and Session state machine.
    - `view/`: Abstract base classes for View Drivers.
    - `stability/`: (Shared) BasePipeManager and common Mixins from `datacast-core`.

- `fustor-mgmt`: Unified control plane.
    - `protocol/`: Shared command models.
    - `agent/`: Remote command executor.
    - `server/`: Orchestration and monitoring API.
