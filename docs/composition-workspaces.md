# Composition Workspaces

## Repository Registration

`iq repo init` accepts only a clean primary checkout attached to the exact target branch and equal to its fetched remote SHA. It validates the target as a Git branch, resolves the configured remote, and persists its canonical fetch and push URL identities before any fetch or ref mutation. Every later registered operation resolves the remote again and rejects a changed name or URL identity before policy loading, fetch, or push. The target SHA must contain valid version 1 `.iq/config.json` policy. IQ records the target SHA, canonical policy snapshot, SHA-256 digest, seed creation intent, and exact managed paths before it creates the seed.

The seed is an IQ-owned detached Rift at the exact target SHA. IQ records its Rift and source identities. Seed refresh preserves ignored build artifacts, resets tracked state exactly, and is durable cleanup debt.

## Development

`iq dev-workspace create` refreshes the seed and creates a direct child Rift from it. IQ records creation intent before Rift mutation and records the exact Rift identity before the workspace becomes active. The returned branch, base SHA, path, and workspace ID are authoritative.

Development workspace names are permanently allocated within a registered repository. A removed name cannot be reused; use a new task name.

The registered checkout is integration-only. Commit all producer work in the development workspace. Do not push the development branch.

## Submission

`iq submit --workspace <id>` requires the exact IQ Rift, branch, clean state, no active Git operation, and base ancestry. IQ persists the base SHA and submission SHA before it stages the commit. A retry uses this intent without reading mutable workspace `HEAD`. Restart reconciliation verifies exact staging and private refs before one transaction publishes the queue source and submitted workspace state.

Use `iq submit --workspace <id> --replace <item-id>` only for the same local item and workspace after a `needs_agent_fix` block. IQ persists the new immutable source before old integration cleanup. If the old integration Rift is dirty, IQ preserves it and leaves retryable replacement cleanup debt.

## Integration

Local submissions never fetch a mutable source branch. IQ resolves the private ref and verifies it against the persisted commit. The integration Rift starts at the exact fetched target base. IQ applies only the exact three-tree change from the persisted development base to the immutable submission commit. It blocks empty changes and verifies that the candidate has exactly one parent: the target base.

Run `iq integrate --next --repo-path <path> --repo-key <key>` or let the daemon process the queue. `--next` and `--resume` are mutually exclusive. Resume accepts only the oldest active item.

Validation uses only the command from the exact target-base policy. Each attempt stores the target SHA, policy JSON, and SHA-256 digest. Required signoff runs after validation and must return successful evidence for the exact candidate SHA and every configured context.

If the target moves, IQ first persists the new base, source candidate, policy snapshot, and reconciliation intent while it clears prior validation and signoff evidence. It then imports the exact new base into the integration Rift and replays the candidate. Restart resumes from that durable intent. No earlier evidence can authorize the new candidate.

A PR/MR supplies provider metadata, exact head/base observations, and provider gates. Every provider landing requires the post-signoff snapshot gate to pass before target mutation. For a registered repository, IQ also fetches the target after signoff; the final snapshot head and base must equal the queued source and fetched target immediately before IQ pushes the exact candidate with a compare-and-set lease. Provider merge APIs can mutate targets only for legacy unregistered queues, after the same final gate check.

## Landing And Cleanup

IQ verifies the exact fetched remote result and records registered-checkout reconciliation intent before mutation. It requires the clean integration-only target branch, then resets the registered checkout to the exact remote target SHA. This operation is restart-safe and does not require target history to be a fast-forward. IQ then marks the candidate integrated and records the same remote target SHA for seed refresh, with development-workspace cleanup debt, in one SQLite transaction. A process stop cannot lose the checkout intent, landed result, or cleanup obligations.

Landing state is durable and exact. `ready` permits cancellation. `uncertain` records the candidate and expected target while a push can have an unknown result and blocks cancellation until reconciliation. `landed` records both the validated candidate and exact landed commit. A definite compare-and-set rejection returns to `ready`, invalidates candidate evidence, rebuilds on the fetched target, and revalidates. Transport or process failures remain `uncertain` until the remote proves whether the candidate landed.

`iq cleanup` retries replacement cleanup, registered-checkout reconciliation, seed refresh, and development Rift removal. Removal resolves a moved Rift by its persisted Rift ID. The current path can be outside the former IQ root only after exact Rift and source identity, direct ancestry, real-directory type, childlessness, clean Git state, and absence of active Git operations are verified. Cleanliness always includes all untracked files, even when repository configuration hides them. IQ repeats these checks immediately before mutation. IQ records garbage-collection debt before Rift removal and clears it only after global Rift GC succeeds. Dirty or active Git work is never deleted.

Cancellation is source-aware. A cancelled local submission returns its development workspace to reusable state, records the submission as cancelled, and keeps any integration Rift as explicit terminal cleanup debt. A local agent fix uses immutable replacement only; remote branch sources use requeue.

All lifecycle operations first bind the opaque repository key to one canonical checkout and target in code and SQLite. They then use a scoped repository operation lease, one process-lock order, short Rift-root and registry lock scopes, bounded SQLite waits, and supervised process groups. An unexpired lease cannot be stolen. Parent loss closes an authority pipe that terminates the complete external command group.
