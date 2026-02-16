# L1: [Sensord] System Contracts

> **Subject**: Sensord | Consumer | System
> Pattern: `[Subject] MUST [action]`
> - Responsibility: WHO is accountable
> - Verification: HOW to measure compliance

---

## CONTRACTS.STABILITY (SCP Foundations)

### Connection Retry

- **RETRY_BACKOFF**: System MUST implement exponential backoff for connection retry to external consumers.
  > Responsibility: Reliability — ensure automatic reconnection after transient failures.
  > Verification: Backoff interval increases exponentially up to a configured maximum.

- **NEVER_STOP_RETRY**: Sensord MUST NOT stop retrying after reaching the alert threshold; it MUST continue retrying at max interval indefinitely.
  > Responsibility: Survival — ensure upstream recovery triggers automatic reconnection.
  > Verification: SensordPipe remains in RECONNECTING state and retries persist beyond alert threshold.

### Exception Isolation

- **SINGLE_FILE_ISOLATION**: Sensord MUST isolate single-file processing failures from affecting the entire task.
  > Responsibility: Fault isolation — prevent individual file errors from cascading.
  > Verification: `PermissionError`, `FileNotFoundError` are logged and skipped; subsequent files continue processing.

- **NO_CRASH_PRINCIPLE**: Sensord MUST NOT crash on any exception. It MUST log errors and continue operation.
  > Responsibility: Indestructibility — maintain process uptime.
  > Verification: Zero process terminations due to business logic exceptions.

### Data Integrity (SDP Foundations)

- **ATOMIC_WRITE_MARKING**: Source drivers MUST mark events with `is_atomic_write` field.
  > Responsibility: Correctness — distinguish partial writes from complete writes.
  > Verification: `IN_CLOSE_WRITE` events marked `is_atomic_write=True`; `IN_MODIFY` events marked `False`.

- **EVENT_LIFECYCLE**: Sensord MUST follow file event lifecycle rules:
  > - File creation: only `on_closed` (`IN_CLOSE_WRITE`) sends metadata, marked `is_atomic_write=True`.
  > - File modification: `on_modified` sends `is_atomic_write=False`, `on_closed` sends `is_atomic_write=True`.
  > Responsibility: Protocol compliance — ensure consistent event semantics.
  > Verification: Event flow matches lifecycle rules in protocol tests.

### Resource Protection

- **BATCH_SEND**: Sensord MUST aggregate events using configurable batch size before sending.
  > Responsibility: Efficiency — reduce network overhead.
  > Verification: Network packets contain batched events based on configuration.

- **THROTTLE_EVENTS**: Sensord MUST use configurable throttle interval to merge frequent `IN_MODIFY` events.
  > Responsibility: Protection — prevent event storms.
  > Verification: Rapid `IN_MODIFY` events merged within throttle window.

- **BOUNDED_BUFFERING**: Sensord MUST use bounded memory buffers for all internal event queues, enabling backpressure or drop-policies at capacity.
  > Responsibility: Memory safety — prevent OOM and head-of-line blocking.
  > Verification: EventBus and Queues have explicit `maxsize` configuration.

---

## CONTRACTS.LIFECYCLE (SCP Domains)

### Hot Upgrade & Config

- **HOT_UPGRADE_ATOMICITY**: Sensord MUST guarantee atomic, in-place replacement of its process logic.
  > Responsibility: Survival — Ensure zero downtime during upgrades.
  > Verification: Process PID may change, but active sessions MUST reconnect within 1 retry interval.

- **TARGETED_DELIVERY**: Sensord MUST be able to identify and respond to upgrade commands targeted specifically at its instance ID.
  > Responsibility: Operations — Canary deployments.
  > Verification: Only the targeted Sensord responds to the upgrade trigger.

- **CONFIG_RELOAD_ATOMICITY**: Configuration changes MUST apply to the entire Sensord process state atomically.
  > Responsibility: Consistency — No partial configuration states.
  > Verification: All components switch to new config revision effectively simultaneously.

### Health & Remediation

- **ZOMBIE_REMEDIATION**: Sensord MUST respond to "Kill & Restart" or "Clean Slate Config" commands if its data plane becomes non-responsive.
  > Responsibility: Survival — Remote fix for stuck data planes.
  > Verification: Sensord receives command, terminates, restarts, and resumes normal operation.

---

## CONTRACTS.AUTONOMY

### Intrinsic Drive

- **INTRINSIC_DRIVE**: Sensord Domain Layer MUST initiate data scanning and synchronization based on local configuration, WITHOUT waiting for consumer commands.
  > Responsibility: Autonomy — Sensord is a proactive sensor.
  > Verification: Sensord starts scanning immediately upon boot/config load, even if no consumer is reachable.

---

## CONTRACTS.CONCURRENCY

### Execution Model

- **NON_BLOCKING_IO**: Core orchestration logic MUST NOT be blocked by I/O operations (File/Network).
  > Responsibility: Liveness — Heartbeats and control commands must be processed immediately.
  > Verification: Main event loop latency stays within defined SLA (e.g., <100ms).

- **DETERMINISTIC_ORDERING**: State mutations within a single component MUST occur sequentially to guarantee consistency without race conditions.
  > Responsibility: Correctness — prevent concurrent state modification bugs.
  > Verification: No usage of `threading.Lock` for business state; separate threads used only for raw I/O isolation.

---

## CONTRACTS.DATA_ROUTING (SDP Routing)

- **SEMANTIC_SCHEMA**: `event_schema` MUST represent "data format" (e.g., `"fs"`), NOT physical path or source ID.
  > Responsibility: Normalization — enable heterogeneous path merging.
  > Verification: All Source drivers set `event_schema` to logical schema name.

- **SOURCE_SCHEMA_CONSISTENCY**: Source driver MUST set `event_schema` identically for realtime, snapshot, and audit events.
  > Responsibility: Consistency — single schema per data contract.
  > Verification: All event types from same source share identical `event_schema`.

- **FIELD_MAPPING_PROJECTION**: `fields_mapping` MUST use projection semantics:
  > - Non-empty mapping: output contains ONLY explicitly mapped fields
  > - Empty/absent mapping: transparent passthrough (no transformation)
  > Responsibility: Data security — prevent accidental field leakage.
  > Verification: Unmapped fields absent from output events.

---

## CONTRACTS.ADDRESSING (SCP Primitives)

- **ADDRESSING_PRIMITIVES**: Every control packet MUST follow `unicast(target_id)` or `broadcast(view_id)` semantics.
  > Responsibility: Routing — mechanical packet delivery.
  > Verification: Stability Layer code contains only `dispatch(header, payload)`.

- **PAYLOAD_OPACITY**: Stability Layer MUST treat all SCP command payloads as opaque dictionaries.
  > Responsibility: Stability — Stability Layer survives invalid payloads.
  > Verification: Zero inspections of payload content in the session management code.

- **NEUTRALITY**: The `SessionManager` MUST NOT contain string literals or logic related to specific business commands like `scan` or `path`.
  > Responsibility: Decoupling — prevent business logic leakage.
  > Verification: Grep check passes.

---

## CONTRACTS.TESTING

- **NO_SURVIVAL_BIAS**: Test fixtures MUST strictly simulate real object creation processes. MUST NOT manually inject attributes to bypass errors.
  > Responsibility: Authenticity — tests must expose real bugs.
  > Verification: Zero `setattr` on business objects in test code.

- **BASE_CLASS_TRUTH**: Base class attribute names are the single source of truth. Derived classes MUST NOT guess or invent attribute names.
  > Responsibility: Contract integrity.
  > Verification: All `self.xxx` accesses verified against base class definitions.

- **KWARGS_PASSTHROUGH**: Adapter pattern implementations MUST transparently pass `**kwargs` end-to-end.
  > Responsibility: Extensibility — ensure future drivers receive contextual parameters.
  > Verification: Test with non-standard kwargs; verify no `TypeError` raised.

---

## CONTRACTS.LAYER_INDEPENDENCE

- **STABILITY_NEUTRALITY**: Stability Layer MUST provide only addressing primitives. MUST NOT contain business logic.
  > Responsibility: Stability — keep the survival layer simple and generic.
  > Verification: Zero business terms (`scan`, `upgrade`, `path`) in Stability Layer code.

- **MANAGEMENT_OPTIONALITY**: Removing management packages MUST NOT affect core Stability/Domain functionality.
  > Responsibility: Independence — management is optional.
  > Verification: Sensord boots and functions normally without `Sensord-mgmt`.
