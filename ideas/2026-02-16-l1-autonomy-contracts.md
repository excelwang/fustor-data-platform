# Idea: L1 Autonomy Contracts Gap Fill

> **Context**: Filling gaps identified in `vibespec review L0` audit.

## Proposed Changes
Add the following contracts to `specs/L1-CONTRACTS.md` under `## CONTRACTS.AUTONOMY` (new section).

### Intrinsic Drive
- **INTRINSIC_DRIVE**: Agent Domain Layer MUST initiate data scanning and synchronization based on local configuration, WITHOUT waiting for Fusion commands.
  > Responsibility: Autonomy — Agent is a proactive sensor, not a passive remote hook.
  > Verification: Agent starts scanning immediately upon boot/config load, even if Fusion is unreachable.

### Multi-Target Renting
- **MULTI_TARGET_RENTING**: Agent Domain Layer MUST be able to push data to multiple independent Receivers (Fusion, Local-Log, 3rd-Party) simultaneously using the same Stability primitives.
  > Responsibility: Decoupling — Data ownership belongs to Agent, not Fusion.
  > Verification: One source event replicated to multiple configured pipes/senders.
