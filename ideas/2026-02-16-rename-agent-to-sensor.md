# Idea: Rename 'sensord' to 'Sensor' (sensord)

> **Context**: User feedback indicates "sensord" implies dependency. "Sensor" (sensord) is selected to emphasize autonomous detection role and alignment with L0 Vision.

## Proposed Changes

### Global Terminology Update
Replace **"sensord"** with **"Sensor"** across all layers (L0-VISION, L1-CONTRACTS, L2-ARCHITECTURE).

- **Concept**: `sensord` → `Sensor`
- **Binary/Service**: `fustor-sensord` → `fustor-sensor` (or `sensord` in context)
- **Component**: `sensordPipe` → `SensorPipe`
- **Config**: `sensordConfig` → `SensorConfig`
- **Symbology**: "Sovereign Sensor" instead of "Autonomous Sensor".

### Impact Analysis
- **L0-VISION**: Update `UBIQUITOUS_LANGUAGE` and all narrative descriptions.
- **L1-CONTRACTS**: Update all `[sensord]` subjects to `[Relay]`. Update `CONTRACTS.STABILITY` references.
- **L2-ARCHITECTURE**: Update Component/Topology diagrams. Rename packages `fustor-sensord-sdk` → `fustor-relay-sdk`. 
- **Codebase**: Recursive rename of `fustor_sensord` packages and classes.

### Rationale
- **Alignment**: L0 Vision already defines sensord as an "autonomous sensor".
- **Function**: Accurately describes the core function of detecting data changes.
- **Clarity**: Removes ambiguity with LLM sensords.
