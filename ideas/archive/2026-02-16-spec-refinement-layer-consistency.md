# Idea: Spec Layer Consistency Refinement

> Date: 2026-02-16
> Status: Draft
> Context: Post-migration cleanup

## 1. Context

Following the migration to Vibespec L0-L3, we performed a deep audit of the content distribution. While the general structure is correct, several items invade the wrong abstraction layers. This document identifies these anomalies and proposes a remediation plan.

## 2. Analysis & Findings

### 2.1 L1-CONTRACTS: Implementation Leakage (Magic Numbers)

**Observation**: `CONTRACTS.STABILITY.EVENTBUS_RING_BUFFER` mandates specific default values:
> "System MUST use a fixed-size ring buffer (**default 1000**)... triggered at **95%** capacity gap."

**Violation**: L1 should define *Invariants* and *Behaviors*, not *Magic Numbers*. "1000" and "95%" are implementation tuning parameters (L3), not architectural contracts.

**Proposal**:
- **L1**: "System MUST use a bounded ring buffer with configurable capacity and backpressure."
- **L3 (EventBus)**: "Default capacity is 1000. Backpressure triggers at 95% utilization."

### 2.2 L2-ARCHITECTURE: Code & Algorithm Leakage

**Observation A**: `COMPONENTS.ORCHESTRATION` contains a Python class definition:
```python
class TaskOrchestrator:
    async def view_broadcast(...)
```
**Violation**: L2 is for Component/Role definitions and Topology. Code-level interface definitions belong in L3.

**Observation B**: `COMPONENTS.ROLES.FSDRIVER` defines the "Per-URI Singleton" implementation pattern:
> "signature = f'{uri}#{hash(credential)}'"

**Violation**: This is a specific *instantiation strategy* (Algorithm/Pattern), which belongs in L3 (Implementation), not L2 (Architecture). L2 should only state "Drivers are resource-constrained and managed by the Loader."

**Proposal**:
- Move `TaskOrchestrator` code to `specs/L3-RUNTIME/ORCHESTRATION.md` (New file).
- Move Singleton logic to `specs/L3-RUNTIME/DRIVER_PATTERNS.md` (New file) or merge into `LIFECYCLE.md`.

### 2.3 L0 & L3: Mostly Clean

- **L0** correctly stays high-level (Vision, Philosophy).
- **L3** correctly contains dense implementation details (`CONSISTENCY.md`, `LOGICAL_CLOCK.md`).

## 3. Refinement Plan

### 3.1 New Files
- `specs/L3-RUNTIME/ORCHESTRATION.md`: For Task Dispatch implementation details.
- `specs/L3-RUNTIME/DRIVER_LIFECYCLE.md`: For Driver instantiation patterns (Singleton) and Lifecycle.

### 3.2 Moves
1. **L1 -> L3**: Move magic numbers (1000, 95%, 5.0s) from Checks to L3 defaults.
2. **L2 -> L3**: Move `TaskOrchestrator` Python code to `L3-RUNTIME/ORCHESTRATION.md`.
3. **L2 -> L3**: Move FSDriver Singleton pattern to `L3-RUNTIME/DRIVER_LIFECYCLE.md`.
