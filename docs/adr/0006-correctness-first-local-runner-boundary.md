# ADR 0006: Correctness-First Local Runner Boundary

- Status: Accepted
- Date: 2026-08-13
- Decider: Kevin Courbet

## Decision

IQ is solo local development tooling. It uses basic OS process isolation and resource bounds for its integration runner. Direct access to the configured model credential is accepted.

Exact runtime closure, credential proxying or isolation from child tools, Btrfs qgroups, persistent filesystem quotas, and hostile sandbox hardening are out of scope.

Lifecycle, data, and Git correctness remain required. This includes crash recovery.

## Consequences

- Practical local deployment does not require runtime command or path manifests.
- The runner uses basic process isolation and bounded resources, not a security boundary against a malicious repository or integration agent.
- The lifecycle authority and exact evidence decisions in ADR 0005 remain in force.
