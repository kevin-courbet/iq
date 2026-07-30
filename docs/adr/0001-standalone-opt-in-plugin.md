# ADR 0001: Standalone Opt-In Plugin

- Status: Accepted
- Date: 2026-07-30
- Decider: Kevin Courbet

## Decision

IQ is an independent public repository and optional plugin. Consumer applications must work without IQ installed. Consumers may pin and install an exact IQ revision, but IQ source is not embedded into their build or test graph.

IQ owns generic queue execution and durability. Consumers own repository-specific validation, signoff, credentials, communication, installation policy, and any future control-plane presentation.

## Consequences

- IQ has an independent release and validation lifecycle.
- Consumer repositories pin immutable IQ source or artifacts for opt-in installation.
- Absence of IQ is a supported state, not an application error.
- Future UI integrations consume IQ contracts without becoming IQ execution authority.
