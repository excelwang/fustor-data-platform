# Idea: L1 Survival Contracts Gap Fill

> **Context**: Filling gaps identified in `vibespec review L0` audit.

## Proposed Changes
Add the following contracts to `specs/L1-CONTRACTS.md` under `## CONTRACTS.LIFECYCLE` (new section).

### Hot Upgrade
- **HOT_UPGRADE_ATOMICITY**: System MUST guarantee atomic, in-place replacement of the sensord process logic.
  > Responsibility: Survival — Ensure zero downtime during upgrades.
  > Verification: Process PID may change, but pipe connections MUST remain active or reconnect within 1 retry interval.
  
- **TARGETED_DELIVERY**: fustord MUST be able to target a specific sensord instance for upgrade, ignoring others sharing the same Session.
  > Responsibility: Operations — Canary deployments.
  > Verification: Only the targeted sensordID receives the upgrade command.

### Config Hot-Reload
- **CONFIG_RELOAD_ATOMICITY**: Configuration changes MUST apply to the entire sensord process state atomically.
  > Responsibility: Consistency — No partial configuration states (e.g., half-old, half-new).
  > Verification: All components switch to new config revision effectively simultaneously.

### Zombie Recovery & Zero Touch
- **ZOMBIE_DETECTION**: fustord MUST detect sensords that maintain Heartbeat but fail to send Data events for > `threshold` period.
  > Responsibility: Health — Detect "brain-dead" sensords.
  > Verification: Alert triggered when heartbeat ok but data flow zero for X minutes.

- **REMOTE_REMEDIATION**: fustord MUST be able to push a "Kill & Restart" or "Clean Slate Config" command to a Zombie sensord.
  > Responsibility: Survival — Remote fix for non-responsive data planes.
  > Verification: Zombie sensord receives command, terminates, restarts, and resumes normal operation.
