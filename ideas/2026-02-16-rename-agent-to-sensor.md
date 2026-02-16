# Idea: Rename 'Agent' to 'Sensor' (sensord)

> **Context**: User feedback indicates "Agent" implies dependency. "Sensor" (sensord) is selected to emphasize autonomous detection role and alignment with L0 Vision.

## Proposed Changes

### Global Terminology Update
Replace **"Agent"** with **"Sensor"** across all layers (L0-VISION, L1-CONTRACTS, L2-ARCHITECTURE).

- **Concept**: `Agent` → `Sensor`
- **Binary/Service**: `fustor-agent` → `fustor-sensor` (or `sensord` in context)
- **Component**: `AgentPipe` → `SensorPipe`
- **Config**: `AgentConfig` → `SensorConfig`
- **Symbology**: "Sovereign Sensor" instead of "Autonomous Sensor".

### Impact Analysis
- **L0-VISION**: Update `UBIQUITOUS_LANGUAGE` and all narrative descriptions.
- **L1-CONTRACTS**: Update all `[Agent]` subjects to `[Relay]`. Update `CONTRACTS.STABILITY` references.
- **L2-ARCHITECTURE**: Update Component/Topology diagrams. Rename packages `fustor-agent-sdk` → `fustor-relay-sdk`. 
- **Codebase**: Recursive rename of `fustor_agent` packages and classes.

### Rationale
- **Alignment**: L0 Vision already defines Agent as an "autonomous sensor".
- **Function**: Accurately describes the core function of detecting data changes.
- **Clarity**: Removes ambiguity with LLM Agents.
