# ADR 0010: Exact Job Targets and Candidate Review

- Status: Accepted
- Date: 2026-09-07
- Decider: Kevin Courbet
- Supersedes: ADR 0005 candidate-classification, review, and orchestration-probe clauses
- Supersedes: ADR 0009 target-authority clauses

## Decision

Every job has one immutable full target ref in the canonical repository. The repository policy target branch is only the default for new jobs.

IQ pins the exact target SHA, source SHA, and candidate SHA for each integration attempt. Review, validation, and landing authority use these exact identities.

IQ derives candidate classification only from composition evidence. A conflict or an accepted agent tree change makes the candidate semantic.

A candidate without these changes is mechanical. Only a semantic candidate requires review before IQ mutates the canonical target.

If the target moves, IQ supersedes the open review and the old candidate. IQ creates a new candidate and review identity for the new target.

An old review cannot authorize the new candidate or target.

Each review response creates an exact durable receipt. An exact replay returns that receipt after workspace cleanup and terminal-history cleanup.

The read-only `sisyphus-backend/v1` probe binds an available result to the canonical database path and durable database ID.

Sisyphus supplies both values to direct IQ CLI operations. These operations are `workspace create`, `workspace list`, `workspace status`, `workspace remove`, and `submit`.

Each direct operation requires `--expected-database-id`. IQ rejects a different database before state access.

The global `--rift-executable` authority can use a canonical path or an exact inherited Linux `/proc/self/fd/<n>` descriptor.

An inherited descriptor must be read-only and reference a same-user executable regular file. An anonymous memfd must have all required write and size seals.

IQ binds descriptor authority to device, inode, owner, mode, size, SHA-256, and seals. IQ validates this authority before and after each Rift operation.

IQ retains the descriptor for Rift operations. The descriptor does not remain open in Rift, Git, provider, runner, or agent processes.

## Consequences

- A job target remains unchanged after the repository default changes.
- Target movement cannot make an old review authorize a new candidate.
- Cleanup does not remove exact review-replay authority.
- Sisyphus cannot authorize a direct operation with a database path alone.
- A sealed in-memory Rift executable does not require a durable executable path.
