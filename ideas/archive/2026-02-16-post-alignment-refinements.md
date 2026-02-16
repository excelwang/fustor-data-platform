# Idea: Post-Alignment Refinements & Core Consolidation

> **Context**: Following the successful Phase 7 alignment of `sensord` with `fustord`, several opportunities for further consolidation and refinement have emerged. These aim to reduce code duplication, standardize testing patterns, and clarify runtime states.

## Proposed Changes

### 1. Consolidate Pipe Lifecycle Logic
With `sensord/stability/pipe_manager.py` now mirroring `fustord`'s structure, the shared lifecycle logic (start/stop/restart/error handling) should be extracted into `sensord-core` (formerly `fustor-core`).

- **Action**: Create `sensord_core.stability.base_manager.BasePipeManager`.
- **Action**: Extract common mixins from `sensord/stability/mixins` and `fustord/stability/mixins` to `sensord_core`.

### 2. Formalize Global Configuration Pattern
During Phase 7, we shifted `sensord` to use the global `sensord_config` loader for mutations. This pattern simplifies service signatures but requires rigorous testing support.

- **Action**: Update `L3-IMPLEMENTATION` to explicitly sanction the **Global Config Singleton Pattern** for mutation operations.
- **Action**: Deprecate passing `ConfigService` instances where the global loader is the source of truth.

### 3. Refine Pipe State Machine
The current `PipeState` enum lacks a clear `RUNNING` or `HEALTHY` state, forcing tests and monitoring tools to infer health from transient states like `SNAPSHOT_SYNC` or `MESSAGE_SYNC`.

- **Action**: Add `PipeState.RUNNING` (or `ACTIVE`) as a primary state.
- **Action**: Treat `SNAPSHOT_SYNC`, `MESSAGE_SYNC` as sub-states or flags within the running lifecycle.

### 4. Standardize Test Fixtures
Tests for `PipeManager` required complex mocking of `EventBusManager` and `sensord_config`.

- **Action**: Create a unified `sensord_test_fixture` in `tests/conftest.py` that pre-mocks the global config and event bus with sensible defaults.
- **Benefit**: Reduces boilerplate in `test_pipe_service_*.py` and prevents regression when internal dependencies change.

## Rationale
- **DRY**: Both daemons now share 80% of their lifecycle logic. Duplication breeds divergence.
- **Clarity**: A `RUNNING` state is essential for operator visibility.
- **Stability**: Standardized test fixtures ensure that refactorings don't break tests due to brittle mock setups.
