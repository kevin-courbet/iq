# Composition Workspaces

## Repository Registration

`iq repo init` accepts a primary checkout as bootstrap input and requires `--storage-root` on a filesystem that supports Linux copy-on-write reflinks. The storage root is independent of the queue database location. IQ validates `main` or `master` and records the absolute lexical request identity and storage root before it resolves the checkout. A retry uses that durable request before touching the input path and rejects a changed storage root. Multiple request paths can bind the same repository UUID when they resolve the same remote and target. IQ then resolves the configured remote and durably records provisioning intent before it creates the IQ-owned full checkout. Every later operation uses the owned checkout and rejects a changed remote identity before fetch or push. Registration copies optional untracked policy but does not create an attempt snapshot.

The owned checkout is an independent Rift root at the exact target SHA. It has no Git alternates or Rift ancestors. Development and integration roots are its direct children. SQLite records the exact root, remote, target, Rift, child-root, generation, provisioning, and cleanup identities.

## Development

`iq dev-workspace create` refreshes the owned root and creates a direct child Rift from it. IQ records creation intent and a pending child-root generation before it publishes the generation marker or mutates Rift state. Restart reconciles only the exact current or pending marker. IQ records the exact Rift identity before the workspace becomes active. The returned branch, base SHA, path, and workspace ID are authoritative.

Development workspace names are permanently allocated within a registered repository. A removed name cannot be reused; use a new task name.

The owned checkout is integration-only. Commit all producer work in the development workspace. Do not push the development branch.

## Submission

`iq submit --workspace <id>` requires the exact IQ Rift, branch, clean state, no active Git operation, and base ancestry. IQ persists the base SHA and submission SHA before it stages the commit. A retry uses this intent without reading mutable workspace `HEAD`. Restart reconciliation verifies exact staging and private refs before one transaction publishes the queue source and submitted workspace state.

Use `iq submit --workspace <id> --replace <item-id>` only for the same local item and workspace after a `needs_agent_fix` block. IQ persists the new immutable source before old integration cleanup. If the old integration Rift is dirty, IQ preserves it and leaves retryable replacement cleanup debt. After cleanup, the one existing effort enters `replacement_pending`; composition binds that effort to the new attempt and resets its failed-cycle count without deleting its history.

## Integration

Local submissions never fetch a mutable source branch. IQ resolves the private ref and verifies it against the persisted commit. The integration Rift starts at the exact fetched target base. IQ applies only the exact three-tree change from the persisted development base to the immutable submission commit. It blocks empty changes and verifies that the candidate has exactly one parent: the target base.

Run `iq integrate --next --repo-key <key>` or let the daemon process the queue. `--next` and `--resume` are mutually exclusive. Resume accepts only the oldest active item.

When a new attempt starts under the repository lease, IQ reads optional local `.iq/config.json` only from the owned root and atomically stores its canonical policy snapshot and SHA-256 digest on the attempt. A missing file is the explicit no-validation policy. IQ accepts the exact candidate SHA without running validation and records the `no_validation` disposition with that digest. A present file is strict version 2 JSON. Required signoff runs after validation and must return successful evidence for the exact candidate SHA, policy digest, and every configured context. Doctor can inspect the current file under the repository lease, but that inspection is not persisted and cannot authorize landing.

Daemon policy is not a second authority. Registered repositories reject daemon validation commands and daemon signoff.

Initial target resolution records the exact `ls-remote` SHA before fetch. IQ fetches only that SHA into an attempt-private ref, verifies its commit object, and then publishes the remote-tracking ref under pending checkout authority. A remote move during this interval cannot change the attempt base. The normal target-movement boundary later observes the new SHA and recomposes.

If the target moves, IQ first persists the new base, source candidate, and reconciliation intent while it invalidates prior validation and signoff evidence. Every command validation keeps an immutable invocation row with its exact target and candidate SHA; invalidated rows remain audit history and cannot authorize landing. A successful command must leave `HEAD` at that candidate SHA before IQ records success. IQ keeps the attempt's policy snapshot, imports the exact new base into the integration Rift, and replays the candidate. Restart resumes from that durable intent.

A PR/MR supplies provider metadata, exact head/base observations, and provider gates. Every provider landing requires the post-signoff snapshot gate to pass before target mutation. The final snapshot head and base must equal the queued source and fetched target before IQ requests the provider merge. IQ records integration only after the provider reports the exact landed commit.

## Landing And Cleanup

IQ rejects tracked `.iq/config.json` before landing. It verifies the exact fetched remote result and records owned-checkout reconciliation intent before mutation. It requires the clean integration-only target branch, then resets the owned checkout to the exact remote target SHA. This operation is restart-safe and does not require target history to be a fast-forward. A process stop cannot lose the checkout intent, landed result, or cleanup obligations.

Landing state is durable and exact. `ready` permits cancellation. `uncertain` records the candidate and expected target while a push can have an unknown result and blocks cancellation until reconciliation. `landed` records both the validated candidate and exact landed commit. A definite compare-and-set rejection returns to `ready`, invalidates candidate evidence, rebuilds on the fetched target, and revalidates. Transport or process failures remain `uncertain` until the remote proves whether the candidate landed.

`iq cleanup` retries replacement cleanup, owned-checkout reconciliation, and development Rift removal. Removal resolves a moved Rift by its persisted Rift ID. IQ verifies exact Rift and source identity, direct ancestry, real-directory type, childlessness, clean Git state, and absence of active Git operations before mutation. IQ records garbage-collection debt before removal and clears it only after global Rift GC succeeds. Dirty or active Git work is never deleted.

`iq dev-workspace remove <id> --discard-residue` is an explicit recovery operation for a `cleanup_pending` or `cleanup_failed` development workspace. It requires the durable Rift ID to be absent from the exact source inventory and the durable old path to equal the expected direct child under the registered workspace root. A Rift that still exists or moved must use normal verified Rift removal. IQ records an unpredictable same-root quarantine name and the inspected root identity and tree digest in the existing cleanup state, then atomically renames the exact residue root without replacement. It reinspects that quarantine through directory descriptors, accepts only no-follow real directories and regular files, and rejects every symlink, special entry, `.git`, and `.rift` marker at any depth. Each child is moved without replacement to a unique quarantine name and its durable identity is verified before deletion. A changed entry or newly occupied original path is preserved as explicit retryable cleanup debt. IQ marks the workspace removed only after the durable path and quarantine are absent and Rift garbage collection succeeds.

Cancellation is source-aware. A cancelled local submission returns its development workspace to reusable state, records the submission as cancelled, and keeps any integration Rift as explicit terminal cleanup debt. A local agent fix uses immutable replacement only; remote branch sources use requeue.

All lifecycle operations first bind the opaque repository key to one canonical checkout and target in code and SQLite. They then acquire the kernel repository process lock, replace the durable heartbeat row, verify exact root authority, and use short Rift-root and registry lock scopes, bounded SQLite waits, and supervised process groups. The kernel lock prevents a live owner from being replaced and permits immediate recovery after process death. Parent loss closes an authority pipe that terminates the complete external command group.
