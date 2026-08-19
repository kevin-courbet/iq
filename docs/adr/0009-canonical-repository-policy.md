# ADR 0009: Canonical Repository Policy

- Status: Accepted
- Date: 2026-08-15
- Decider: Kevin Courbet
- Supersedes: ADR 0008 canonical-target and owned-root authority clauses; ADR 0003 landing-policy clauses

## Decision

Every repository has exactly one designated canonical Git repository and one
canonical target ref. The canonical target is the only ref consulted when new
work starts and the only ref that landing can advance. Local and remote are
transport relationships, not authority classes.

Repository policy stores separate values for IQ operation state, canonical
repository identity, target branch, integration policy, and replication
policy. CLI arguments can request an operation but cannot select or authorize
policy. The operation state is enabled, draining with an exact captured set of
existing obligations, or disabled. Disabled repositories reject all new IQ
mutations and permit only reads, cancellation, and safe cleanup.
New registration accepts only enabled policy. Draining and disabled are reached
through normal state transitions or assigned by offline migration.
Before registration creates a reservation, intent, ownership row, or filesystem
root, it verifies the canonical repository and every replica. Physical ownership
keys come only from this verified policy. Any identity failure leaves registration
with no durable or filesystem effect.

The canonical repository is either a securely identified local bare Git
repository or an accessible Git repository with exact fetch, push, provider identity,
and immutable Git object format. Object format is exactly SHA-1 or SHA-256.
Every replica uses the canonical format. Local verification uses hardened
`git rev-parse --show-object-format`; provider verification must report the
format before any effect and fails closed when that capability is absent.
GitHub uses `GET /repos/{owner}/{repo}/hash-algorithm`; GitLab uses the
project API `repository_object_format` field.
Accessible repositories record the provider repository
host, path, and immutable provider ID used to validate merge-request URLs and
provider responses. The accessible repository ID must equal the provider ID.
The provider ID is revalidated before every provider or Git effect; an adapter
that cannot verify it blocks the effect. Local bare identity includes its canonical non-symlink
path, bare-repository status, device, and inode. Replicas
are separate destinations. Replica observations can never change the canonical
target or reconcile the IQ-owned root.
GitHub identity uses the repository node ID for both registration and
merge-request admission. Provider identity and snapshot commands at item release
gates run through the timeout-controlled, cancellation-aware process supervisor
with bounded stdout and stderr. Provider command failures remain typed
infrastructure outcomes.
Physical ownership is role-independent, target-independent, and
transport-independent. Provider ownership uses provider, normalized host, and
immutable repository ID. Local bare ownership uses device and inode while its
canonical path is verified separately. SQLite enforces one owner across all
canonical and replica roles and leases effects by physical identity.

The IQ-owned root is a local materialization of the exact canonical target. It
is the independent Rift root but is not a second Git authority. Workspace
creation resolves the current canonical target, records that exact SHA,
reconciles the owned root to it, and creates a direct child Rift. Failure to
resolve or fetch the canonical target fails closed.

Integration policy is exactly direct or merge-request-required. Direct policy
permits immutable local workspace submission and explicit direct-branch
admission. Landing uses an exact compare-and-set update of the canonical target.
Merge-request-required policy rejects direct admission. The coding agent pushes
the source branch, creates and describes the merge request, and admits its exact
provider, repository, target, identity, and head SHA to IQ. IQ does not create
merge requests.

IQ captures immutable source SHA, canonical target SHA, resulting candidate,
policy snapshot, and validation evidence for every attempt. Source or target
movement invalidates prior evidence and requires recomposition and revalidation.
Direct landing advances the exact validated candidate. Provider landing requires
one provider operation that atomically pins the admitted head and validated base.
If an adapter cannot guarantee both values, IQ blocks before provider mutation.
The current GitHub and GitLab CLI adapters are blocked for this reason.
Landing first records prepared authority. The same transaction that opens the
process gate records released authority. Failed preflight records no release and
remains retryable. An unknown released direct push remains uncertain. Target observation proves success only
when it contains the exact candidate. A different target is not proof of lease
rejection and cannot authorize recomposition or a second landing attempt.
Only an exact `git push --porcelain` stale-lease rejection record for the target
authorizes recomposition.
Released or completed external landing authority remains present when an effort
is wrapped by an infrastructure or provider blocker. Cancellation, migration,
candidate rejection, target recomposition, and any other state transition cannot
erase that authority before exact reconciliation resolves it.

Canonical landing and replication are separate durable lifecycles. A successful
canonical landing is never rolled back because a replica is stale or
unavailable. Replication records exact source, expected destination, operation,
outcome, application identity, uncertainty evidence, and retry debt. Restart
reconciles pending and in-flight debt against the exact destination ref.
Debt uses the item's exact landed SHA, not a later canonical observation. Each
debt publishes a durable source pin before it becomes pending and retains that
pin through all non-terminal and applied recovery states. The pin is removed
only after exact application is durable. FIFO and uniqueness use immutable
physical destination identity, independent of fetch transport.

The public coding-agent workspace commands are `iq workspace create`, `list`,
`status`, and `remove`. IQ's retained integration workspace operator commands
are `iq integration status` and `reset`. The old command names are removed.
Merge-request admission uses an explicit `iq admit mr` command. Presence or
absence of an MR URL never selects landing policy.
Submission is new work. A draining repository rejects submission before it can
create a local-submission intent or queue item; IQ does not transfer a workspace
obligation into a queue-item obligation during draining.

User checkouts and IQ-owned roots are never synchronized through filesystem
copy operations. Git objects and refs move only through verified Git operations.
Every IQ Git process uses one explicit canonical working directory, sets
`GIT_NO_REPLACE_OBJECTS=1`, and clears ambient Git, SSH, askpass, credential, namespace,
object-store, discovery, and configuration controls. Validation, provider, Rift,
and agent processes that can start Git inherit the same object-resolution control. IQ disables system and global
Git configuration; rejects repository URL, credential, helper, and transport
overrides; and uses only explicit IQ identity and non-interactive settings.
The host, its same-user processes, and installed Git, provider, Rift, and
systemd executables are trusted. IQ does not try to defend against a local user
who replaces a host executable or races a host filesystem path.

Agent work remains untrusted. It runs in the existing Bubblewrap sandbox with a
bounded overlay, no provider access, and a read-only runner and Git executable.
One transient `iq-agent-<cycle-id>.service` owns one agent cycle. No command
broker, sealed executable copy, or per-Git-command systemd service is used.

IQ resolves host executables to absolute paths and runs normal Git, provider,
and Rift commands directly. Git uses non-interactive settings, disabled hooks,
no replacement objects, explicit repository bindings, and controlled
configuration for agent-facing export work. The verified binding persists the
object format. IQ initializes owned roots with that format and validates object
IDs against it.

Durable effect ordering remains a correctness requirement. IQ commits exact
landing or replication intent before the external mutation starts. A crash
after that commit remains an uncertain effect and is resolved by observing the
canonical repository. Direct landing uses an exact compare-and-set update. MR
landing remains blocked unless the provider adapter can pin both the admitted
head and validated base.

Integration effort creation and conflict projection require the queue item to
remain in `merging` with the exact active unfinished attempt. Cancellation and
effort creation serialize on one database write boundary. A winning
cancellation remains terminal and cannot be changed back to `merging`.
The public cancellation command does not report success until the exact prepared
systemd unit and cgroup are confirmed terminated. Cancellation
state and exact termination debt remain durable when confirmation fails. Daemon
startup and a repeated cancellation use the same reconciler.
Prepared launch authority has an explicit durable handoff. Cancellation before
handoff closes spawn authority and prevents `systemd-run`. Cancellation after
handoff keeps termination debt until process-start acknowledgement or failed
spawn closes that authority. A missing systemd unit is terminal only after spawn
authority is closed.

Schema 3 is replaced by schema 4 through one explicit offline migration that
preserves repository UUIDs, queue and attempt history, evidence, events,
notifications, and cleanup obligations. Normal runtime has no compatibility
path for the old schema or old CLI. Deployment policy and repository inventory
remain external inputs; IQ contains no repository-specific policy.
Every active schema-3 MR gets its admitted base only from migration inventory.
An active item that is incompatible with its assigned integration policy must
have an explicit cancellation disposition; migration never continues it under
an incompatible runtime policy.
Historical complete provider identity and base come from explicit inventory when
schema 3 has no durable proof. The legacy URL is validation input only. Active MR
continuation also requires the exact provider-derived source ref. Migration does
not fabricate identity sentinels. Invalid JSON or semantic workspace and runner
identity requires explicit checked repair. Dispositions are globally unique and
must name an item owned by their policy assignment.
Version-3 migration inventory models ready repositories and every interrupted
provisioning lifecycle as distinct variants. Interrupted provisioning is
explicitly preserved or cancelled and never converted into an invented ready
root. Operator-generated Git bindings are required for each lifecycle with a
live repository, every active development Rift, and every retained integration
workspace repair. Migration rechecks Git admin structure, live Git identity,
linked-worktree backlinks, object format, and expected HEAD/base before backup publication or
primary database mutation. It verifies every canonical and replica provider or
local-bare policy identity before database path resolution, copying, backup, or
mutation. An unavailable or changed repository rejects migration.
An active schema-3 runner must be cancelled. Its inventory disposition must
include the exact unit, cgroup, PID, and process-start identity. Migration checks
this authority against systemd and `/proc` before backup publication. It does
not create launcher authority from the migration process or legacy payload. A
schema-3 `iq-agent-<cycle-id>.scope` is explicit legacy termination authority;
migration never interprets it as a current `.service` authority.
Current provisioning persists the same Git binding when Git first exists,
verifies it on every resume step, and replaces it only through verified root
relocation. A replacement Git directory cannot continue an interrupted plan.

Replication debt is FIFO for each immutable physical destination. CLI retry and
daemon recovery use one reconciler. It publishes and verifies the item's exact
landed source pin before pending, reconciles all non-terminal and applied states,
and verifies the exact destination ref after each push. A later canonical
landing does not change the source object of older debt. Applied and superseded
source-pin cleanup compare-and-delete only the recorded source SHA under the
repository binding and lease, then verify absence. A mismatch preserves the
pending cleanup state and pin and returns an invariant error.
FIFO uses a transactional positive sequence unique to physical destination and
target, never wall-clock time. A completed later sequence prevents an older
retry from writing the destination backwards. IQ commits an explicit
supersession-cleanup-pending state, removes only the exact expected local source
pin under the repository binding and lease, and then finalizes supersession.
Restart resumes cleanup before later debt.

During `pin_source`, replication can run only exact source-object verification
and compare-and-update of `refs/iq/replication/<debt-id>`. The durable operation
changes before IQ observes or mutates the destination. A crash after ref
publication resumes the same idempotent comparison.

IQ private repository-target and landing refs have durable cleanup debt. Cleanup
discovers existing refs under the repository lease, retains refs required by a
pending checkout, uncertain landing, or replication source-pin transition, and
records exact ref/SHA debt before deletion. Cleanup uses compare-and-delete,
verifies absence before finalization, rejects drift without changing the ref or
debt, and resumes after restart. Terminal landing and ready checkout refs do not
accumulate.

Physical ownership, canonical repository, target branch, integration
policy, and replication policy are immutable after registration. SQLite rejects
raw authority mutation. Revisioned operation state permits only enabled to
draining and draining to disabled transitions.

## Consequences

- The canonical target answers workspace freshness without reference to
  `origin`, local branch state, or replica freshness.
- Repository policy is checked before workspace creation, admission,
  integration, landing, replication, retry, and external mutation.
- Coding agents own MR creation and context; IQ owns admission and integration.
- MR comments are projections and authorized answer inputs. SQLite remains
  lifecycle authority.
- Direct integration can trigger CI or deployment and must be enabled explicitly
  in deployment policy.
- A new agent starts from current truth by creating a new IQ workspace. Existing
  workspaces keep their recorded base.
- Draining rejects new work while exact existing obligations finish.
- Branch protection and credential scope remain final provider-side barriers.
