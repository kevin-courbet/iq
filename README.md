# IQ

IQ is a durable Git integration coordinator. SQLite owns lifecycle state. One explicit repository policy owns Git authority.

## Repository Policy

Repository policy stores five separate concepts:

- operation state: `enabled`, `draining`, or `disabled`
- canonical repository: a local bare Git path or an accessible Git repository
- target branch: `main` or `master`
- integration policy: `direct` or `merge_request_required`
- replication policy: no replicas or an exact list of replica destinations

The canonical target is the only source for new workspace bases and the only target that IQ can land. A bootstrap checkout, `origin`, a local branch, the IQ-owned root, and replicas are not authority. The owned root is only a canonical materialization and the independent Rift seed.

See [Repository Policy](docs/repository-policy.md) for strict JSON formats and state behavior.

## Registration

Registration requires a bootstrap checkout, an owned storage root, and an explicit policy file. It does not derive authority from the bootstrap remote.
Registration accepts only enabled policy. Use state-transition commands after registration; draining and disabled assignments are migration-only.

```sh
iq repo init \
  --path /work/bootstrap \
  --storage-root /var/lib/iq \
  --policy /etc/iq/repository-policy.json
```

`.iq/config.json` remains optional, untracked validation and signoff configuration. It cannot define canonical repository, operation, integration, or replication policy.

## Coding Workspaces

Coding agents use only these workspace commands:

```sh
iq workspace create --repo-key <repository-uuid> --name feature
iq workspace list --repo-key <repository-uuid>
iq workspace status <workspace-id>
iq workspace remove <workspace-id>
```

Create resolves the exact current canonical target, fails closed if it cannot resolve or fetch it, reconciles the owned root, creates a direct child Rift, and records the exact base SHA. Existing workspaces keep their recorded base.

The retained internal integration Rift has separate operator commands:

```sh
iq integration status --repo-key <repository-uuid>
iq integration reset --repo-key <repository-uuid> --system-config /etc/iq/system.yaml
```

The old `dev-workspace` command and the old `workspace status/reset` operator paths do not exist.

## Admission

Direct policy permits exact branch admission and immutable local workspace submission:

```sh
iq admit direct --repo-key <repository-uuid> --source agent/feature --head <full-sha>
iq submit --workspace <workspace-id>
```

Direct landing uses compare-and-set against the exact validated canonical target. Direct canonical mutation can start CI or deployment, so deployment policy must enable it explicitly.

Merge-request-required policy rejects both direct forms. The coding agent pushes its source branch, creates and describes the MR, then admits it:

```sh
iq admit mr https://github.com/owner/repository/pull/123 --repo-key <repository-uuid>
```

IQ pins provider, canonical repository identity, target branch, MR identity, exact head, and exact current base. IQ queries and validates the admitted MR. IQ never creates an MR. A cross-repository MR fails. Source or target movement makes evidence stale and requires exact recomposition and validation.

Provider landing requires one provider operation that atomically pins both the admitted head and validated base. The current GitHub and GitLab CLI adapters cannot supply that guarantee, so IQ blocks before provider mutation.

No credentialed provider test project is available in this repository. A real-provider sandbox is required before an adapter can enable landing and prove atomic base/head pinning plus post-landing evidence.

## Replication

Canonical landing and replication are separate. A replica cannot change canonical truth or owned-root reconciliation. Failed replication does not roll back canonical state. It records exact durable debt:

```sh
iq replication status --repo-key <repository-uuid>
iq replication retry <debt-id>
```

Each debt records the item's exact landed SHA, immutable physical destination identity, target, transactional destination sequence, expected destination SHA, compare-and-set operation, outcome, and failure. CLI retry and daemon recovery use this sequence, not timestamps, for FIFO. A completed later sequence first moves an older debt to durable supersession-cleanup-pending state. IQ then deletes the exact expected source pin and finalizes supersession, without a remote write. Restart resumes either cleanup boundary. A durable internal ref pins the source before debt becomes pending. The reconciler retains it through non-terminal and applied recovery, retries it after canonical advances, verifies the exact destination after push, and removes it only after application or safe supersession is durable.

Never use `rsync`, `scp`, or manual filesystem copies to move data from a user checkout or IQ-owned root. Move Git objects and refs only through verified Git operations.

## Operation State

- `enabled` permits new authorized work.
- `draining` contains an exact captured set of existing workspace, queue, and replication obligations. It rejects new work.
- `disabled` rejects new mutation, integration, retry, landing, and replication. Reads, cancellation, and safe cleanup remain available.

Policy authorization runs before operation arguments are validated.

## Schema Migration

Normal runtime accepts schema 4 only and rejects schema 3. Migration is explicit and offline:

```sh
iq migrate inspect-git-binding --path /var/lib/iq/repositories/<uuid>/root
iq --queue-db /var/lib/iq/queues.db migrate schema3 \
  --policy-inventory /etc/iq/schema3-policy-inventory.json
```

Version-2 inventory uses distinct ready-repository and interrupted-provisioning lifecycle variants. Each interrupted lifecycle is explicitly preserved or cancelled; migration never invents a ready root. The inventory contains generated live Git bindings for each lifecycle that has a repository, every active development Rift, and each supplied retained integration workspace. Before database path resolution or copying, migration verifies every canonical and replica provider or local-bare identity. It then takes the exclusive database authority lease, requires one policy assignment for every repository UUID, verifies every binding and expected HEAD/base before backup publication or primary mutation, validates exact schema 3, creates a durable backup, preserves queue/audit/evidence/event/notification/cleanup data, creates exact admissions, and validates all schema-4 authority before commit. Every active MR base comes from inventory. An active admission that is incompatible with the assigned integration policy must be explicitly cancelled. Failure leaves all schema-3 database-family files unchanged and usable. Migration and binding inspection dispatch before normal schema open. There is no runtime compatibility path.

## Queue And Control Plane

```sh
iq list
iq events <item-id>
iq cancel <item-id>
iq integrate --system-config /etc/iq/system.yaml --next --repo-key <repository-uuid>
iq cleanup --repo-key <repository-uuid> --system-config /etc/iq/system.yaml
iq daemon --config /etc/iq/iq.yaml --system-config /etc/iq/system.yaml
```

Cancellation reports success only after IQ confirms that the exact prepared service and complete cgroup terminated. A failure keeps durable termination debt for retry by the command or daemon startup.

The daemon and CLI use shared validated database identity leases. Repository operation leases serialize mutation for one repository without holding an idle global exclusion.

## Development

```sh
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
RUSTFLAGS="-D warnings" cargo build --locked --release
```
