# L1: Fustor Contracts (Fustord Perspective)

> **Subject**: System | Fustord
> **Relation**: `Fustord MUST [action]` AND `Fustord RELIES ON [Sensord action]`

## 1. CONTRACTS.SYSTEM (Universal)

Contracts that apply to **ALL** Python processes (both fustord and sensord).

### Stability

- **NO_CRASH_PRINCIPLE**: Process MUST NOT crash on any business logic exception.
  > Verification: Zero process terminations in logs due to unhandled `Exception`.

- **SINGLE_EVENT_LOOP**: System MUST run on a single `asyncio` event loop.
  > Verification: No `threading.Thread` spawning without `run_in_executor`.

- **NO_SHARED_MUTABLE_IN_EXECUTOR**: Thread pool tasks MUST be pure functions without shared mutable state.
  > Verification: No `Lock` usage in executor tasks.

### Threading Bridge

- **THREAD_BRIDGE_PATTERN**: Sync IO (DB/Disk) MUST use the Producer(Thread)-Queue-Consumer(Async) pattern.
  > Verification: No blocking IO calls in main loop.

---

## 2. CONTRACTS.FUSTORD (Aggregator Obligations)

Contracts specific to the **Fustord** process.

### Reliability

- **ZOMBIE_DETECTION**: Fustord MUST detect Sensords with active Heartbeat but zero Data flow for > `threshold`.
  > Responsibility: Detect "brain-dead" sensors.

- **REMOTE_REMEDIATION**: Fustord MUST be able to Unicast a "Restart" command to a specific Zombie Sensord.
  > Responsibility: Self-healing control plane.

### Concurrency & Locking

- **PER_VIEW_LOCK**: ViewStateManager MUST use per-view `asyncio.Lock`. Global lock is FORBIDDEN.
  > Responsibility: Parallelism.

- **RW_LOCK_DISCIPLINE**: Views MUST use `AsyncRWLock` (Read-Shared, Write-Exclusive).
  > Responsibility: High concurrent read throughput.

- **QUEUE_ISOLATION**: Each `FustordPipe` MUST have an independent `asyncio.Queue`.
  > Responsibility: Slow pipe isolation.

- **QUEUE_DRAIN_SIGNAL**: Queue workers MUST use `asyncio.Event` for shutdown, not polling.
  > Responsibility: Efficiency.

### Data Routing

- **TWO_TIER_ROUTING**: Events MUST be routed first by `FustordPipe` (Schema match), then by `ViewManager`.
  > Responsibility: Prevent schema pollution.

- **PAYLOAD_OPACITY**: Stability Layer (SessionManager) MUST treat payloads as opaque dicts.
  > Responsibility: Layer independence.

---

## 3. CONTRACTS.PREREQUISITES (Sensord Obligations)

Fustord's correctness **DEPENDS** on Sensord fulfilling these contracts.

### Connection & Transport

- **RETRY_BACKOFF**: Sensord MUST implement exponential backoff.
- **NEVER_STOP_RETRY**: Sensord MUST retry connection indefinitely.
- **BATCH_SEND**: Sensord MUST batch events (configurable size).
- **THROTTLE_EVENTS**: Sensord MUST merge rapid `IN_MODIFY` events.

### Data Integrity (SDP)

- **ATOMIC_WRITE_MARKING**: Sensord MUST mark `is_atomic_write=True` ONLY on `close_write`.
- **EVENT_LIFECYCLE**: `on_modified` -> `atomic=False`; `on_closed` -> `atomic=True`.
- **SEMANTIC_SCHEMA**: Sensord MUST use logical schema names (e.g. "fs"), not paths.

### Lifecycle

- **HOT_UPGRADE_ATOMICITY**: Sensord upgrade MUST NOT drop active pipe connections > 1 retry interval.
- **CONFIG_RELOAD_ATOMICITY**: Config reload MUST apply atomically.
- **INTRINSIC_DRIVE**: Sensord MUST initiate scanning without Fustord commands.

---

## 4. CONTRACTS.LAYER_INDEPENDENCE

- **STABILITY_NEUTRALITY**: SessionManager MUST NOT contain words: `scan`, `path`, `snapshot`.
- **DOMAIN_SELF_SUFFICIENT**: API MUST work even if Management Layer is removed.
- **MANAGEMENT_TRUE_PLUGIN**: System MUST boot without Management packages.
