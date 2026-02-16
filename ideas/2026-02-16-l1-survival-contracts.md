# Idea: L1 Survival Contracts Gap Fill

> **Context**: Filling gaps identified in `vibespec review L0` audit.

## Proposed Changes
Add the following contracts to `specs/L1-CONTRACTS.md` under `## CONTRACTS.LIFECYCLE` (new section).

### Hot Upgrade
- **HOT_UPGRADE_ATOMICITY**: System MUST guarantee atomic, in-place replacement of the Agent process logic.
  > Responsibility: Survival — Ensure zero downtime during upgrades.
  > Verification: Process PID may change, but pipe connections MUST remain active or reconnect within 1 retry interval.
  
- **TARGETED_DELIVERY**: Fusion MUST be able to target a specific Agent instance for upgrade, ignoring others sharing the same Session.
  > Responsibility: Operations — Canary deployments.
  > Verification: Only the targeted AgentID receives the upgrade command.

### Config Hot-Reload
- **CONFIG_RELOAD_ATOMICITY**: Configuration changes MUST apply to the entire Agent process state atomically.
  > Responsibility: Consistency — No partial configuration states (e.g., half-old, half-new).
  > Verification: All components switch to new config revision effectively simultaneously.

### Zombie Recovery & Zero Touch
- **ZOMBIE_DETECTION**: Fusion MUST detect Agents that maintain Heartbeat but fail to send Data events for > `threshold` period.
  > Responsibility: Health — Detect "brain-dead" agents.
  > Verification: Alert triggered when heartbeat ok but data flow zero for X minutes.

- **REMOTE_REMEDIATION**: Fusion MUST be able to push a "Kill & Restart" or "Clean Slate Config" command to a Zombie Agent.
  > Responsibility: Survival — Remote fix for non-responsive data planes.
  > Verification: Zombie agent receives command, terminates, restarts, and resumes normal operation.
