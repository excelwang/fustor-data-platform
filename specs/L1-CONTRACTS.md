---
version: 1.0.0
invariants:
  - id: INV_CONTROL_SURVIVES_DATA
    statement: "The Control Plane must survive the Data Plane. Business logic errors must never terminate the Agent or Fusion process."
  - id: INV_API_NEVER_503
    statement: "The /fs/tree API endpoint must always return a valid response, never 503."
  - id: INV_LAYER_ORDER
    statement: "Stability/Domain Layer must never depend on Management Layer. Management Layer is an optional plugin."
---

# L1: Fustor Contracts

> **Subject**: Agent | System. Pattern: `[Agent|System] MUST [action]`
> - Responsibility: WHO is accountable
> - Verification: HOW to measure compliance

---

## CONTRACTS.STABILITY


### Connection Retry

- **RETRY_BACKOFF**: System MUST implement exponential backoff for connection retry.
  > Responsibility: Reliability — ensure automatic reconnection after transient failures.
  > Verification: Backoff interval increases exponentially up to a configured maximum.

- **NEVER_STOP_RETRY**: System MUST NOT stop retrying after reaching the alert threshold; it MUST continue retrying at max interval indefinitely.
  > Responsibility: Survival — ensure upstream recovery triggers automatic reconnection.
  > Verification: AgentPipe remains in RECONNECTING state and retries persist beyond alert threshold.

### Exception Isolation

- **SINGLE_FILE_ISOLATION**: System MUST isolate single-file processing failures from affecting the entire task.
  > Responsibility: Fault isolation — prevent individual file errors from cascading.
  > Verification: `PermissionError`, `FileNotFoundError` are logged and skipped; subsequent files continue processing.

- **NO_CRASH_PRINCIPLE**: System MUST NOT crash on any exception. Agent and Fusion MUST log errors and continue operation.
  > Responsibility: Indestructibility — maintain process uptime.
  > Verification: Zero process terminations due to business logic exceptions in production.

### Data Integrity

- **ATOMIC_WRITE_MARKING**: System MUST mark events with `is_atomic_write` field at the data source.
  > Responsibility: Correctness — distinguish partial writes from complete writes.
  > Verification: `IN_CLOSE_WRITE` events marked `is_atomic_write=True`; `IN_MODIFY` events marked `False`.

- **SUSPECT_ON_PARTIAL**: System MUST mark files as `integrity_suspect=True` when receiving `is_atomic_write=False` events.
  > Responsibility: Data quality — flag potentially incomplete data.
  > Verification: Suspect flag set on partial write, cleared on atomic write confirmation.

- **EVENT_LIFECYCLE**: System MUST follow file event lifecycle rules:
  > - File creation: only `on_closed` (`IN_CLOSE_WRITE`) sends metadata, marked `is_atomic_write=True`.
  > - File modification: `on_modified` sends `is_atomic_write=False`, `on_closed` sends `is_atomic_write=True`.
  > - Known limitation: `cp -p`, `rsync` without `IN_CLOSE_WRITE` require Audit for discovery.
  > Responsibility: Protocol compliance — ensure consistent event semantics.
  > Verification: Event flow matches lifecycle rules in integration tests.

### Resource Protection

- **BATCH_SEND**: System MUST aggregate events using configurable batch size before sending.
  > Responsibility: Efficiency — reduce network overhead.
  > Verification: Network packets contain batched events based on configuration.

- **THROTTLE_EVENTS**: System MUST use configurable throttle interval to merge frequent `IN_MODIFY` events.
  > Responsibility: Protection — prevent event storms.
  > Verification: Rapid `IN_MODIFY` events merged within throttle window.

- **EVENTBUS_RING_BUFFER**: System MUST use a bounded ring buffer for EventBus, with automatic fast/slow consumer splitting triggered at high capacity usage.
  > Responsibility: Memory safety — prevent OOM and head-of-line blocking.
  > Verification: EventBus never exceeds configured buffer size; splitting occurs when lag exceeds threshold.

---

## CONTRACTS.CONCURRENCY


### Threading Model

- **SINGLE_EVENT_LOOP**: System MUST run on asyncio single-threaded event loop.
  > Responsibility: Correctness — ensure deterministic execution order.
  > Verification: No `threading.Lock` usage on shared state; all coordination via asyncio.

- **NO_SHARED_MUTABLE_IN_EXECUTOR**: System MUST NOT access shared mutable state from `run_in_executor` threads.
  > Responsibility: Safety — prevent data races.
  > Verification: Zero `threading.Lock` on shared state; executor-only for pure computation.

- **THREAD_BRIDGE_PATTERN**: System MUST use Thread Bridge pattern for synchronous IO iterators (e.g., `os.scandir`, MinIO SDK):
  > 1. Producer Thread: dedicated thread for sync iterator
  > 2. `asyncio.Queue` as backpressure buffer
  > 3. `threading.Event` (`stop_event`) for Consumer exit signaling
  > 4. Consumer MUST drain queue before exit to unblock Producer
  > Responsibility: Correctness — prevent deadlocks in sync-to-async bridging.
  > Verification: Queue drained on shutdown; no hung threads.

### Lock Strategy

- **PER_VIEW_LOCK**: System MUST use per-view `asyncio.Lock` granularity for ViewStateManager and SessionManager.
  > Responsibility: Parallelism — different views must never block each other.
  > Verification: Concurrent requests to different views proceed without mutual blocking.

- **NO_GLOBAL_LOCK_REGRESSION**: System MUST NOT degrade per-view/per-pipe locks to global locks.
  > Responsibility: Performance — prevent serialization bottleneck.
  > Verification: No single lock guards multiple views/pipes.

- **RW_LOCK_DISCIPLINE**: View-FS MUST use `AsyncRWLock`:
  > - Read Lock: `process_event`, `query` (concurrent reads allowed)
  > - Write Lock: `rebuild` only (exclusive, blocks all reads/writes)
  > - MUST NOT substitute Read Lock with Write Lock "for safety"
  > Responsibility: Throughput — maintain read concurrency under load.
  > Verification: Multiple concurrent queries do not serialize.

### Queue & Backpressure

- **QUEUE_ISOLATION**: Each FusionPipe MUST have an independent event queue and processing loop.
  > Responsibility: Isolation — different views process independently.
  > Verification: No cross-pipe shared queues exist.

- **QUEUE_DRAIN_SIGNAL**: System MUST use `asyncio.Event` for queue drain signaling, NOT busy-wait polling.
  > Responsibility: Efficiency — eliminate `while not empty: sleep(N)` patterns.
  > Verification: `wait_for_drain()` method used instead of polling.

### Cache Strategy

- **CONFIG_CACHE_TTL**: System MUST cache YAML config reads with TTL (default 5s). MUST NOT execute `yaml.safe_load()` on every request.
  > Responsibility: Performance — avoid hot-path file IO.
  > Verification: Config reads served from cache within TTL.

- **LEADER_CACHE_VERIFY**: System MUST periodically verify leader cache validity (every 5 heartbeats). MUST NOT permanently trust leader cache.
  > Responsibility: Consistency — prevent stale leader state.
  > Verification: Cache invalidation on mismatch detected.

### Performance Targets

| Metric | Target | Rationale |
|--------|--------|-----------|
| Event throughput | 1,000 events/sec (peak) | Single pipe serial processing sufficient |
| Concurrent sessions | 100+ | Per-view/per-pipe locks |
| Query concurrency | Unlimited | Lock-free reads |

---

## CONTRACTS.DATA_ROUTING


- **SEMANTIC_SCHEMA**: `event_schema` MUST represent "data format" (e.g., `"fs"`), NOT physical path or source ID.
  > Responsibility: Normalization — enable heterogeneous path merging.
  > Verification: All Source drivers set `event_schema` to logical schema name; never to physical path.

- **SOURCE_SCHEMA_CONSISTENCY**: Source driver MUST set `event_schema` identically for realtime, snapshot, and audit events.
  > Responsibility: Consistency — single schema per data contract.
  > Verification: All event types from same source share identical `event_schema`.

- **TWO_TIER_ROUTING**: System MUST implement two-tier event routing:
  > 1. **Tier 1 (FusionPipe)**: Route by `Handler.schema_name` matching `Event.event_schema`
  > 2. **Tier 2 (ViewManager)**: Route by `Driver.target_schema` matching `Event.event_schema`
  > Responsibility: Safety — prevent cross-schema data pollution.
  > Verification: DB events never reach FS memory tree; FS events never reach DB handlers.

- **FIELD_MAPPING_PROJECTION**: `fields_mapping` MUST use projection semantics:
  > - Non-empty mapping: output contains ONLY explicitly mapped fields
  > - Empty/absent mapping: transparent passthrough (no transformation)
  > Invariant: `len(fields_mapping) == 0 ⟹ event_out ≡ event_in`
  > Responsibility: Data security — prevent accidental field leakage.
  > Verification: Unmapped fields absent from output events.

---

## CONTRACTS.ADDRESSING

> Source: STABILITY_NEUTRALITY, 2026-02-16T0220-neutral-addressing-primitives.md

- **ADDRESSING_ONLY**: Every packet sent from Fusion to Agent MUST be either `unicast(target_id)` or `broadcast(view_id)`.
  > Responsibility: Routing — purely mechanical packet delivery.
  > Verification: Stability Layer code contains only `dispatch(header, payload)`.

- **PAYLOAD_OPACITY**: Stability Layer MUST treat all command payloads as opaque dictionaries.
  > Responsibility: Stability — Stability Layer survives invalid payloads.
  > Verification: Zero inspections of `payload.type` or `payload.body` in `SessionManager`.

- **NEUTRALITY**: The `SessionManager` MUST NOT contain string literals or logic related to `scan`, `snapshot`, `job_id`, or `path`.
  > Responsibility: Decoupling — prevent business logic leakage.
  > Verification: Grep check passes.

- **AUTO_RECONNECT**: All remote operations (even destructive ones like restart) MUST be auto-recovered by Domain heartbeat reconnection.
  > Responsibility: Resilience — umbilical cord stays ready when physically reachable.
  > Verification: Agent reconnects within 2× backoff interval after process restart.

---

## CONTRACTS.TESTING


- **NO_SURVIVAL_BIAS**: Test fixtures MUST strictly simulate real object creation processes. MUST NOT manually inject attributes to bypass errors.
  > Responsibility: Authenticity — tests must expose real bugs, not mask them.
  > Verification: Zero `setattr` on business objects in test code (except for explicit mocking).

- **BASE_CLASS_TRUTH**: Base class attribute names are the single source of truth. Derived classes MUST NOT guess or invent attribute names.
  > Responsibility: Contract integrity — prevent `AttributeError` in production.
  > Verification: All `self.xxx` accesses verified against base class definition in code review.

- **KWARGS_PASSTHROUGH**: Adapter pattern implementations MUST transparently pass `**kwargs` end-to-end.
  > Responsibility: Extensibility — ensure Forest View and future drivers receive contextual parameters.
  > Verification: Test with non-standard kwargs; verify no `TypeError` raised.

- **REVIEW_CHECKLIST**: Code review MUST check:
  > 1. Mock audit: No `setattr` on business logic classes
  > 2. Attribute consistency: `super().__init__` matches base class
  > 3. Terminology: Comments match latest architecture naming
  > 4. Shadow errors: Test failures show real interface mismatches, not framework artifacts
  > Responsibility: Quality gate — systematic error prevention.
  > Verification: Checklist items verified in every PR review.

---

## CONTRACTS.LAYER_INDEPENDENCE

> Source: L0-VISION §1.4

- **STABILITY_NEUTRALITY**: Stability Layer (SessionManager) MUST provide only addressing primitives (`broadcast`, `unicast`, `heartbeat_tunnel`). MUST NOT contain business logic.
  > Responsibility: Stability — keep the survival layer simple and generic.
  > Verification: Zero business terms (`scan`, `upgrade`, `path`, `job_id`) in Stability Layer code.

- **DOMAIN_SELF_SUFFICIENT**: Domain Layer MUST implement API fallback (On-Command Find) independently. MUST NOT depend on Management Layer for data completeness.
  > Responsibility: Availability — core API survives Management Layer removal.
  > Verification: Disable all Management plugins; `/fs/tree` still returns valid data.

- **MANAGEMENT_TRUE_PLUGIN**: Removing all Management packages MUST NOT affect Stability/Domain functionality.
  > Responsibility: Independence — management is optional.
  > Verification: System boots and serves API without `fustor-view-mgmt` or `fustor-source-mgmt`.
