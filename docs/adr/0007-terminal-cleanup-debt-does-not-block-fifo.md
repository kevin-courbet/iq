# ADR 0007: Terminal Cleanup Debt Does Not Block FIFO

- Status: Accepted
- Date: 2026-08-13
- Decider: Kevin Courbet
- Supersedes: ADR 0002, terminal-workspace cleanup clause

## Decision

Dirty or active-Git terminal integration workspaces are preserved as durable
cleanup debt. This debt is separate from active queue FIFO, so safe preservation
does not block later nonterminal work. Nonterminal FIFO remains strict.

Rift root, path, source, identity, generation, owner, lease, observation, and
mutation authority failures remain fail-closed errors and still block the
operation. Unknown occupancy, failed observations, and uncertain removal are
errors. Explicit cleanup may retry preserved workspaces without backoff.
