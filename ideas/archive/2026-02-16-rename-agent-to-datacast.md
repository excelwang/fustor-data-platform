# Idea: Rename 'datacast' to 'Datacast' (datacast)

> **Context**: User feedback indicates "datacast" implies dependency. "Datacast" (datacast) is selected to emphasize autonomous detection role and alignment with L0 Vision.

## Proposed Changes

### Global Terminology Update
Replace **"datacast"** with **"Datacast"** across all layers (L0-VISION, L1-CONTRACTS, L2-ARCHITECTURE).

- **Concept**: `datacast` → `Datacast`
- **Binary/Service**: `datacast` → `fustor-datacast` (or `datacast` in context)
- **Component**: `DatacastPipe` → `DatacastPipe`
- **Config**: `DatacastConfig` → `DatacastConfig`
- **Symbology**: "Sovereign Datacast" instead of "Autonomous Datacast".

### Impact Analysis
- **L0-VISION**: Update `UBIQUITOUS_LANGUAGE` and all narrative descriptions.
- **L1-CONTRACTS**: Update all `[datacast]` subjects to `[Relay]`. Update `CONTRACTS.STABILITY` references.
- **L2-ARCHITECTURE**: Update Component/Topology diagrams. Rename packages `datacast-sdk` → `fustor-relay-sdk`. 
- **Codebase**: Recursive rename of `fustor_datacast` packages and classes.

### Rationale
- **Alignment**: L0 Vision already defines datacast as an "autonomous datacast".
- **Function**: Accurately describes the core function of detecting data changes.
- **Clarity**: Removes ambiguity with LLM datacasts.
