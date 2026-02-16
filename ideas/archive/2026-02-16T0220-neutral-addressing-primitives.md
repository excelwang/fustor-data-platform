---
version: 1.0.0
---

# Universal Addressing Primitives: The "Neutral L1" Paradigm

## Context
During the Fustor architecture refactoring and L0-L3 reorganization, we discovered that the Stability Layer (L1) was contaminated with business-specific logic (e.g., `scan` commands, `job_id` tracking). To achieve the vision of an **Indestructible Control Plane**, L1 must be strictly decoupled from Domain (L2) and Management (L3) semantics.

This idea formalizes the shift from a "Command-Response" model to a "Neutral Addressing" model where the protocol backbone provides only routing primitives.

## Proposal

### L0: Vision / Scope
- **Vision**: L1 is a "dumb pipe" for smart endpoints. It provides the **Umbilical Cord** that survives any business failure.
- **Scope**: L1 manages only connection state, heartbeats, and addressing. It does not inspect payloads or understand "tasks".

### L1: Contracts (Rules)
- **NEUTRALITY**: The `SessionManager` MUST NOT contain string literals or logic related to `scan`, `snapshot`, `job_id`, or `path`.
- **ADDRESSING_ONLY**: Every packet sent from fustord to sensord MUST be either `unicast(target_id)` or `broadcast(view_id)`.
- **PAYLOAD_OPACITY**: L1 MUST treat all command payloads as opaque dictionaries.

### L2: Architecture (Components)
- **Role: Primitive Borrower**: L2 Services (like `FallbackScanner`) rent L1's `broadcast` primitive to discover data.
- **Role: View Manager**: L2 coordinates the merging of data returned via neutral pipes, providing the "illusion" of a unified command.

### L3: Implementation (Details)
- Refactor `SessionManager.queue_command` to `SessionManager.dispatch(header, payload)`.
- Remove `pending_scans` and `_path_to_job_id` from `SessionManager`.
- Implement `fustor-core/pipe/addressing.py` to define the binary header format for neutral routing.

## Rationale
This approach aligns with **VISION.LAYER_MODEL** (Stability Underneath). By stripping L1 of business awareness, we reduce its complexity and bugs. If a new data type (e.g., SQL sync) is added, L1 remains unchanged. If the management UI crashes, the "Addressing Primitives" continue to facilitate data sync.

## Verification Plan

### Automated Validation
- [x] `vibespec validate` passes without new errors.
- [x] Unit test: `test_l1_neutrality` verifies no business keywords in `session_manager.py`.

### Manual Review Criteria
- [x] **Alignment**: Supports the "Indestructible Shell" philosophy in L0-VISION.
- [x] **Traceability**: All L2 data sync logic is anchored to neutral L1 primitives.
- [x] **Testability**: Can we simulate a "Management Failure" while "Data Sync" continues? Yes.
