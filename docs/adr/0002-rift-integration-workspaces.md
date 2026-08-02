# ADR 0002: Rift Integration Workspaces

- Status: Accepted
- Date: 2026-08-02
- Decider: Kevin Courbet

## Decision

IQ uses Rift as its only integration-workspace backend. Each repository managed by IQ must already be a Rift root. IQ creates item workspaces with copy-on-write reuse of the complete source workspace, including dependency and build artifacts, then resets tracked state to the exact target base before merging.

Blocked and otherwise recoverable items retain their Rift. Integrated and cancelled items retain durable queue history but no workspace. Under the repository lease, IQ reconciles terminal workspaces and unreferenced IQ-owned Rifts before processing more queue work, so interrupted cleanup is retried after restart.

After removing IQ-owned Rifts, IQ runs global Rift garbage collection to reclaim physical storage immediately. This permanently purges all previously removed Rift trash visible to the same Rift installation, including trash not created by IQ.

## Consequences

- Rift is a required IQ runtime dependency; there is no Git-worktree fallback.
- Workspace roots must be outside their source repository and dedicated to one repository target.
- Rift creation skips repository post-create hooks because repository validation policy remains IQ's execution authority.
- Copy-on-write snapshots reuse existing dependencies and build output without eagerly duplicating their physical blocks.
- Unknown, untracked, or identity-mismatched workspace paths fail closed instead of being deleted directly.
- Operators must not treat removed Rift trash as recoverable while IQ is running.
