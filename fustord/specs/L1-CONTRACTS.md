---
version: 1.0.0
invariants:
  - id: INV_FUSTORD_SURVIVAL
    statement: "The fustord process must survive all non-fatal business logic exceptions."
  - id: INV_API_NEVER_503
    statement: "The API MUST always return a valid response, using On-Command fallback if necessary."
  - id: INV_DATA_ISOLATION
    statement: "Failure in one Pipe or View must not block ingestion or queries for other Pipes/Views."
---

# L1: [fustord] System Contracts

> **Subject**: fustord | System. Pattern: `[fustord|System] MUST [action]`
> - Responsibility: WHO is accountable
> - Verification: HOW to measure compliance

---

## CONTRACTS.PROTOCOL (SCP & SDP Interface)

- **IDEMPOTENT_INGESTION**: fustord MUST ensure SDP event ingestion is idempotent. Duplicate batches from datacaststs MUST NOT create duplicate tree entries.
  > Responsibility: Correctness.
  > Verification: Sending the same event batch twice results in a single state change.

- **TIMEOUT_ACKNOWLEDGMENT**: fustord MUST include the negotiated `session_timeout` in the SCP handshake response.
  > Responsibility: Stability.
  > Verification: Response body contains `session_timeout_seconds`.

---

## CONTRACTS.STABILITY (Pipe & Session)

- **INGESTION_REJECTION**: fustord Stability Layer MUST reject new events with `429 Too Many Requests` when the internal pipe queue reaches capacity.
  > Responsibility: Protection.
  > Verification: Receiver returns 429 when buffer is full.

- **RETRY_BACKOFF**: System MUST implement exponential backoff for connection retry to external storage or management services.
  > Responsibility: Reliability.
  > Verification: Backoff interval increases exponentially up to a configured maximum.

- **NO_CRASH_PRINCIPLE**: fustord MUST NOT crash on any exception. malformed SDP packets or SCP state errors must be caught and logged.
  > Responsibility: Indestructibility.

---

## CONTRACTS.DOMAIN (Consistency & Arbitration)

### API Availability

- **INV_API_NEVER_503**: The `/fs/tree` API endpoint MUST always return a valid response, never 503.
  > Responsibility: Availability — "Presence is Service".
  > Verification: API returns data (even if marked as blind-spot) during network partitions or datacastst downtime.

- **ON_COMMAND_FALLBACK**: fustord MUST trigger an on-demand SCP `scan` command if a query hits a path while the memory tree is still being built.
  > Responsibility: Correctness.

- **AUTHORITY_GRADIENT**: fustord MUST prioritize REALTIME events over SNAPSHOT/AUDIT events during arbitration.
  > Responsibility: Truth determination.

- **POSITIVE_ROUTING**: fustord MUST ensure events are routed to **all** Views that match the event's schema and pipe binding.
  > Responsibility: Completeness.

---

## CONTRACTS.LIFECYCLE (Orchestration)

- **TARGETED_UPGRADE**: fustord MUST be able to target a specific `datacastst_id` for upgrade commands via SCP unicast.
  > Responsibility: Operations.

- **ZOMBIE_DETECTION**: fustord MUST detect Datacast nodes that maintain SCP heartbeats but fail to send SDP data events for a configured threshold period.
  > Responsibility: Health detection.

---

## CONTRACTS.CONCURRENCY (Safety)

- **DETERMINISTIC_CORE**: fustord core MUST use a single-threaded event loop for state transitions to avoid race conditions.
  > Responsibility: Safety.

- **STATE_ISOLATION**: fustord MUST ensure that concurrent queries and updates to different Views proceed without serialization.
  > Responsibility: Performance.

---

## CONTRACTS.LAYER_INDEPENDENCE

- **STABILITY_NEUTRALITY**: Stability Layer MUST provide only addressing primitives. It MUST NOT contain knowledge of path structures or logical clocks.
  > Responsibility: Stability.
  > Verification: Zero business terms (`scan`, `path`, `logical_clock`) in Stability Layer code.

- **MANAGEMENT_OPTIONALITY**: Removing Management plugins MUST NOT affect basic ingestion and consistency arbitration.
  > Responsibility: Independence.
  > Verification: System boots and solves queries without `fustord-mgmt`.
