---
version: 1.0.0
invariants:
  - id: INV_CONTROL_SURVIVES_DATA
    statement: "The Control Plane must survive the Data Plane. Business logic errors must never terminate the Agent or Fusion process."
  - id: INV_API_NEVER_503
    statement: "The /fs/tree API endpoint must always return a valid response, never 503."
  - id: INV_LAYER_ORDER
    statement: "L1/L2 must never depend on L3. L3 is an optional plugin."
---

# L1: Fustor Contracts

> **Subject**: Agent | System. Pattern: `[Agent|System] MUST [action]`
> - Responsibility: WHO is accountable
> - Verification: HOW to measure compliance

---

## CONTRACTS.STABILITY

> Source: 05-Stability.md

### Connection Retry

- **RETRY_BACKOFF**: System MUST implement exponential backoff for connection retry.
  > Responsibility: Reliability — ensure automatic reconnection after transient failures.
  > Verification: Initial interval = 5.0s, max interval = 60.0s, alert threshold = 5 consecutive errors.
  (Ref: VISION.SURVIVAL)

- **NEVER_STOP_RETRY**: System MUST NOT stop retrying after reaching the alert threshold; it MUST continue retrying at max interval indefinitely.
  > Responsibility: Survival — ensure upstream recovery triggers automatic reconnection.
  > Verification: AgentPipe remains in RECONNECTING state and retries persist beyond alert threshold.
  (Ref: VISION.SURVIVAL.AGENT_SURVIVAL)

### Exception Isolation

- **SINGLE_FILE_ISOLATION**: System MUST isolate single-file processing failures from affecting the entire task.
  > Responsibility: Fault isolation — prevent individual file errors from cascading.
  > Verification: `PermissionError`, `FileNotFoundError` are logged and skipped; subsequent files continue processing.
  (Ref: VISION.SURVIVAL)

- **NO_CRASH_PRINCIPLE**: System MUST NOT crash on any exception. Agent and Fusion MUST log errors and continue operation.
  > Responsibility: Indestructibility — maintain process uptime.
  > Verification: Zero process terminations due to business logic exceptions in production.
  (Ref: VISION.SURVIVAL)

### Data Integrity

- **ATOMIC_WRITE_MARKING**: System MUST mark events with `is_atomic_write` field at the data source.
  > Responsibility: Correctness — distinguish partial writes from complete writes.
  > Verification: `IN_CLOSE_WRITE` events marked `is_atomic_write=True`; `IN_MODIFY` events marked `False`.
  (Ref: VISION.SCOPE.CONSISTENCY)

- **SUSPECT_ON_PARTIAL**: System MUST mark files as `integrity_suspect=True` when receiving `is_atomic_write=False` events.
  > Responsibility: Data quality — flag potentially incomplete data.
  > Verification: Suspect flag set on partial write, cleared on atomic write confirmation.
  (Ref: VISION.SCOPE.CONSISTENCY)

- **EVENT_LIFECYCLE**: System MUST follow file event lifecycle rules:
  > - File creation: only `on_closed` (`IN_CLOSE_WRITE`) sends metadata, marked `is_atomic_write=True`.
  > - File modification: `on_modified` sends `is_atomic_write=False`, `on_closed` sends `is_atomic_write=True`.
  > - Known limitation: `cp -p`, `rsync` without `IN_CLOSE_WRITE` require Audit for discovery.
  > Responsibility: Protocol compliance — ensure consistent event semantics.
  > Verification: Event flow matches lifecycle rules in integration tests.
  (Ref: VISION.SCOPE.CONSISTENCY)

### Resource Protection

- **BATCH_SEND**: System MUST aggregate events using `batch_size` (default 100) before sending.
  > Responsibility: Efficiency — reduce network overhead.
  > Verification: Network packets contain batched events.
  (Ref: VISION.SCOPE.SYNC)

- **THROTTLE_EVENTS**: System MUST use `throttle_interval` (default 1.0s) to merge frequent `IN_MODIFY` events.
  > Responsibility: Protection — prevent event storms.
  > Verification: Rapid `IN_MODIFY` events merged within throttle window.
  (Ref: VISION.SCOPE.SYNC)

- **EVENTBUS_RING_BUFFER**: System MUST use a fixed-size ring buffer (default 1000) for EventBus, with automatic fast/slow consumer splitting triggered at **95%** capacity gap.
  > Responsibility: Memory safety — prevent OOM and head-of-line blocking.
  > Verification: EventBus never exceeds buffer size; splitting occurs when (fastest_index - slowest_index) >= capacity * 0.95.
  (Ref: VISION.SCOPE.RESILIENCE)

---

## CONTRACTS.CONCURRENCY

> Source: 06-CONCURRENCY_PERFORMANCE.md

### Threading Model

- **SINGLE_EVENT_LOOP**: System MUST run on asyncio single-threaded event loop.
  > Responsibility: Correctness — ensure deterministic execution order.
  > Verification: No `threading.Lock` usage on shared state; all coordination via asyncio.
  (Ref: VISION.SCOPE.SYNC)

- **NO_SHARED_MUTABLE_IN_EXECUTOR**: System MUST NOT access shared mutable state from `run_in_executor` threads.
  > Responsibility: Safety — prevent data races.
  > Verification: Zero `threading.Lock` on shared state; executor-only for pure computation.
  (Ref: VISION.SCOPE.RESILIENCE)

- **THREAD_BRIDGE_PATTERN**: System MUST use Thread Bridge pattern for synchronous IO iterators (e.g., `os.scandir`, MinIO SDK):
  > 1. Producer Thread: dedicated thread for sync iterator
  > 2. `asyncio.Queue` as backpressure buffer
  > 3. `threading.Event` (`stop_event`) for Consumer exit signaling
  > 4. Consumer MUST drain queue before exit to unblock Producer
  > Responsibility: Correctness — prevent deadlocks in sync-to-async bridging.
  > Verification: Queue drained on shutdown; no hung threads.
  (Ref: VISION.SCOPE.SYNC)

### Lock Strategy

- **PER_VIEW_LOCK**: System MUST use per-view `asyncio.Lock` granularity for ViewStateManager and SessionManager.
  > Responsibility: Parallelism — different views must never block each other.
  > Verification: Concurrent requests to different views proceed without mutual blocking.
  (Ref: VISION.SCOPE.AVAILABILITY)

- **NO_GLOBAL_LOCK_REGRESSION**: System MUST NOT degrade per-view/per-pipe locks to global locks.
  > Responsibility: Performance — prevent serialization bottleneck.
  > Verification: No single lock guards multiple views/pipes.
  (Ref: VISION.SCOPE.AVAILABILITY)

- **RW_LOCK_DISCIPLINE**: View-FS MUST use `AsyncRWLock`:
  > - Read Lock: `process_event`, `query` (concurrent reads allowed)
  > - Write Lock: `rebuild` only (exclusive, blocks all reads/writes)
  > - MUST NOT substitute Read Lock with Write Lock "for safety"
  > Responsibility: Throughput — maintain read concurrency under load.
  > Verification: Multiple concurrent queries do not serialize.
  (Ref: VISION.SCOPE.AVAILABILITY)

### Queue & Backpressure

- **QUEUE_ISOLATION**: Each FusionPipe MUST have an independent event queue and processing loop.
  > Responsibility: Isolation — different views process independently.
  > Verification: No cross-pipe shared queues exist.
  (Ref: VISION.SURVIVAL)

- **QUEUE_DRAIN_SIGNAL**: System MUST use `asyncio.Event` for queue drain signaling, NOT busy-wait polling.
  > Responsibility: Efficiency — eliminate `while not empty: sleep(N)` patterns.
  > Verification: `wait_for_drain()` method used instead of polling.
  (Ref: VISION.SCOPE.SYNC)

### Cache Strategy

- **CONFIG_CACHE_TTL**: System MUST cache YAML config reads with TTL (default 5s). MUST NOT execute `yaml.safe_load()` on every request.
  > Responsibility: Performance — avoid hot-path file IO.
  > Verification: Config reads served from cache within TTL.
  (Ref: VISION.SCOPE.AVAILABILITY)

- **LEADER_CACHE_VERIFY**: System MUST periodically verify leader cache validity (every 5 heartbeats). MUST NOT permanently trust leader cache.
  > Responsibility: Consistency — prevent stale leader state.
  > Verification: Cache invalidation on mismatch detected.
  (Ref: VISION.SCOPE.CONSISTENCY)

### Performance Targets

| Metric | Target | Rationale |
|--------|--------|-----------|
| Event throughput | 1,000 events/sec (peak) | Single pipe serial processing sufficient |
| Concurrent sessions | 100+ | Per-view/per-pipe locks |
| Query concurrency | Unlimited | Lock-free reads |

---

## CONTRACTS.DATA_ROUTING

> Source: 07-DATA_ROUTING_AND_CONTRACTS.md

- **SEMANTIC_SCHEMA**: `event_schema` MUST represent "data format" (e.g., `"fs"`), NOT physical path or source ID.
  > Responsibility: Normalization — enable heterogeneous path merging.
  > Verification: All Source drivers set `event_schema` to logical schema name; never to physical path.
  (Ref: VISION.SCOPE.SYNC)

- **SOURCE_SCHEMA_CONSISTENCY**: Source driver MUST set `event_schema` identically for realtime, snapshot, and audit events.
  > Responsibility: Consistency — single schema per data contract.
  > Verification: All event types from same source share identical `event_schema`.
  (Ref: VISION.SCOPE.CONSISTENCY)

- **TWO_TIER_ROUTING**: System MUST implement two-tier event routing:
  > 1. **Tier 1 (FusionPipe)**: Route by `Handler.schema_name` matching `Event.event_schema`
  > 2. **Tier 2 (ViewManager)**: Route by `Driver.target_schema` matching `Event.event_schema`
  > Responsibility: Safety — prevent cross-schema data pollution.
  > Verification: DB events never reach FS memory tree; FS events never reach DB handlers.
  (Ref: VISION.SCOPE.EXTENSIBILITY)

- **FIELD_MAPPING_PROJECTION**: `fields_mapping` MUST use projection semantics:
  > - Non-empty mapping: output contains ONLY explicitly mapped fields
  > - Empty/absent mapping: transparent passthrough (no transformation)
  > Invariant: `len(fields_mapping) == 0 ⟹ event_out ≡ event_in`
  > Responsibility: Data security — prevent accidental field leakage.
  > Verification: Unmapped fields absent from output events.
  (Ref: VISION.SCOPE.EXTENSIBILITY)

---

## CONTRACTS.COMMAND_DISPATCH

> Source: 01-ARCHITECTURE.md §12

- **UNIFIED_RENTING**: Command dispatch MUST use "Unified Renting" model — L2/L3 rent L1 neutral primitives.
  > Responsibility: Layer independence — L1 must never know command semantics.
  > Verification: L1 code contains zero references to `scan`, `upgrade`, or `stop`.
  (Ref: VISION.LAYER_MODEL)

- **BROADCAST_FOR_SCAN**: Fallback scan MUST use L1 `broadcast(view_id, payload)` primitive targeting all sources of a view.
  > Responsibility: Completeness — zero data blind spots.
  > Verification: All active sessions receive scan command simultaneously.
  (Ref: VISION.EXPECTED_EFFECTS)

- **UNICAST_FOR_MGMT**: Management operations (upgrade, stop) MUST use L1 `unicast(target, payload)` primitive targeting specific agent/session.
  > Responsibility: Atomicity — process-level precision for operations.
  > Verification: Only the targeted agent receives the management command.
  (Ref: VISION.EXPECTED_EFFECTS)

- **AUTO_RECONNECT**: All remote command-induced disconnections (upgrade, restart) MUST be auto-recovered by L2 heartbeat reconnection.
  > Responsibility: Resilience — umbilical cord stays ready when physically reachable.
  > Verification: Agent reconnects within 2× backoff interval after process restart.
  (Ref: VISION.SURVIVAL.UMBILICAL_CORD)

---

## CONTRACTS.TESTING

> Source: 11-TESTING_AND_REVIEW_BEST_PRACTICES.md

- **NO_SURVIVAL_BIAS**: Test fixtures MUST strictly simulate real object creation processes. MUST NOT manually inject attributes to bypass errors.
  > Responsibility: Authenticity — tests must expose real bugs, not mask them.
  > Verification: Zero `setattr` on business objects in test code (except for explicit mocking).
  (Ref: VISION.SCOPE)

- **BASE_CLASS_TRUTH**: Base class attribute names are the single source of truth. Derived classes MUST NOT guess or invent attribute names.
  > Responsibility: Contract integrity — prevent `AttributeError` in production.
  > Verification: All `self.xxx` accesses verified against base class definition in code review.
  (Ref: VISION.SCOPE)

- **KWARGS_PASSTHROUGH**: Adapter pattern implementations MUST transparently pass `**kwargs` end-to-end.
  > Responsibility: Extensibility — ensure Forest View and future drivers receive contextual parameters.
  > Verification: Test with non-standard kwargs; verify no `TypeError` raised.
  (Ref: VISION.SCOPE.EXTENSIBILITY)

- **REVIEW_CHECKLIST**: Code review MUST check:
  > 1. Mock audit: No `setattr` on business logic classes
  > 2. Attribute consistency: `super().__init__` matches base class
  > 3. Terminology: Comments match latest architecture naming
  > 4. Shadow errors: Test failures show real interface mismatches, not framework artifacts
  > Responsibility: Quality gate — systematic error prevention.
  > Verification: Checklist items verified in every PR review.
  (Ref: VISION.SCOPE)

---

## CONTRACTS.LAYER_INDEPENDENCE

> Source: L0-VISION §1.4

- **L1_NEUTRALITY**: L1 (SessionManager) MUST provide only addressing primitives (`broadcast`, `unicast`, `heartbeat_tunnel`). MUST NOT contain business logic.
  > Responsibility: Stability — keep the survival layer simple and generic.
  > Verification: Zero business terms (`scan`, `upgrade`, `path`, `job_id`) in L1 code.
  (Ref: VISION.LAYER_MODEL)

- **L2_SELF_SUFFICIENT**: L2 MUST implement API fallback (On-Command Find) independently. MUST NOT depend on L3 for data completeness.
  > Responsibility: Availability — core API survives L3 removal.
  > Verification: Disable all L3 plugins; `/fs/tree` still returns valid data.
  (Ref: VISION.LAYER_MODEL)

- **L3_TRUE_PLUGIN**: Removing all L3 packages MUST NOT affect L1/L2 functionality.
  > Responsibility: Independence — management is optional.
  > Verification: System boots and serves API without `fustor-view-mgmt` or `fustor-source-mgmt`.
  (Ref: VISION.LAYER_MODEL)
