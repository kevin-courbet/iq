# ADR 0003: Standalone Composition Workspaces

- Status: Accepted
- Date: 2026-08-10
- Decider: Kevin Courbet

## Decision

ADR 0008 supersedes this ADR's repository-root, path-derived key, and
multiple-target clauses. Its immutable source, candidate,
evidence, landing, and lifecycle decisions remain in force.

IQ owns the complete standalone composition lifecycle. The IQ-owned checkout is the independent Rift root and Git authority. IQ creates development and integration Rifts directly from that root and imports immutable exact-HEAD local submissions into private refs. A local candidate has one parent and applies only the persisted development-base-to-submission change to the current target.

Integration policy is attempt-owned and exact. ADR 0004 supersedes this ADR's former target-tree policy source and defines optional local policy, attempt snapshots, and policy behavior during target movement.

IQ persists the registered remote name and canonical fetch and push URL identities before its first fetch. Every registered operation verifies these identities before policy loading or remote mutation. IQ marks an item integrated only after it verifies the landed candidate and reconciles the owned checkout to the exact fetched remote target. Every provider landing requires its post-signoff snapshot gate to pass before target mutation and uses the provider merge API. Reconciliation records intent before an exact reset and supports rewritten target history. SQLite lifecycle state and exact Rift identity make every boundary restart-safe. Cleanup preserves dirty or active Git work, including all untracked files regardless of repository status configuration, and resolves relocated Rifts by identity.

Landing outcome is a durable variant. `ready` means no target mutation is unresolved, `uncertain` records an exact candidate and expected target while push success is unknown, and `landed` records the validated candidate and exact landed commit. A definite compare-and-set rejection clears uncertain authority, invalidates evidence, and starts moved-base recovery. Only transport or process failures retain uncertain reconciliation. Cancellation is permitted whenever the outcome is `ready`.

Repository operation authority is scoped and RAII-owned. Integrator and composition validate one canonical checkout and target binding in code and SQLite, use the same repository process lock before the non-stealing SQLite lease, then use short Rift-root and registry lock scopes. All external mutations run in supervised process groups whose authority pipe terminates the group after owner loss.

Queue source and landing policy are exact variants enforced by SQLite. Local submissions use durable creation intents and immutable replacement. Cancellation returns local development work to reusable state and retains explicit terminal integration cleanup debt. Remote sources alone can requeue.

IQ owns its default state namespace. The state root is absolute and non-empty. Incompatible existing state is rejected without mutation.

## Consequences

- Producers do not edit or push from registered target checkouts.
- Local sources are immutable commits, not mutable branch names.
- Rift remains the only workspace and cleanup backend.
- Registration copies policy input but does not create attempt policy authority.
- Git-worktree integration, inferred policy, metadata-less adoption, and consumer runtime assumptions are outside this design.
