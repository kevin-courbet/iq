# ADR 0011: Inherited System Configuration Descriptors

- Status: Accepted
- Date: 2026-09-10
- Decider: Kevin Courbet
- References: ADR 0010, dotfiles ADR `2026-09-06-allow-optional-iq-composition-for-sisyphus`

## Decision

IQ accepts its system configuration from a canonical regular file or an exact inherited Linux `/proc/self/fd/<n>` descriptor.

An inherited system configuration descriptor must reference a non-empty, owner-only, read-only regular file within the configuration size bound.

The descriptor must reference an anonymous memfd with all write, growth, shrink, and seal seals.

IQ validates the descriptor identity and seals before and after it reads the configuration bytes.

## Consequences

- Sisyphus can give IQ a sealed snapshot of the approved system configuration.
- IQ does not read a mutable configuration path during a guarded operation.
- Non-Linux systems continue to use canonical regular configuration files.
