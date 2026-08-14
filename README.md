# IQ

IQ is a standalone durable Git integration coordinator. It serializes work for one physical target, uses a sandboxed local integration agent for every item, builds and validates the exact candidate, applies explicit signoff policy, lands with an exact lease, and reconciles all external effects from SQLite state.

IQ owns one full checkout and independent Rift root for each registered remote. Development and integration Rifts are direct children of that root.

Registration persists the configured remote name and canonical fetch and push URL identities before the first fetch. Later registered operations reject any remote name, fetch URL, or push URL change before remote mutation.

## Composition

`.iq/config.json` is optional local control-plane configuration in the owned root. It must not be tracked. When it is absent, IQ skips validation, requires no signoff, and integrates the exact candidate SHA. When it is present, it remains strict versioned JSON:

```json
{
  "version": 2,
  "integration": {
    "validation": {"command": "task validate"},
    "signoff": {"mode": "none"},
    "agent": {"model": "openai/gpt-5.6-sol"}
  },
  "state_repository": {"kind": "local"}
}
```

Required signoff is explicit:

```json
{
  "version": 2,
  "integration": {
    "validation": {"command": "task validate"},
    "signoff": {
      "mode": "required",
      "command": "./ci/iq-signoff",
      "contexts": ["linux", "macos"]
    }
  },
  "state_repository": {
    "kind": "gitlab_issue",
    "visibility": "full",
    "repository": "group/project",
    "allowed_responders": ["maintainer"]
  }
}
```

IQ rejects malformed, blank, unknown, unsupported, symlinked, or tracked policy. It does not infer commands from Cargo, Taskfile, Make, package managers, repository languages, or tools. The signoff command receives `IQ_SIGNOFF_SHA` and must print `{"sha":"<exact-sha>","contexts":{"<context>":"success"}}`.

IQ copies optional untracked policy from the bootstrap checkout into the owned root during registration. It creates an authoritative policy snapshot only when a new attempt starts under the repository lease. It atomically stores the canonical snapshot and SHA-256 digest on that attempt. Resume, retry, and target movement keep the stored snapshot. A new attempt reads the owned-root file. Development and integration Rifts receive the file, but IQ trusts only the attempt snapshot. IQ rejects a tracked copy before landing.

Registered repositories reject daemon validation commands and daemon signoff because owned-root policy is authoritative. `iq doctor` reports `owned_root` or `none` as the validation authority.

```sh
iq repo init --path /path/to/repo --storage-root /path/to/reflink-storage --target main --remote origin
iq repo list
iq dev-workspace create --repo-key <repository-uuid> --name feature
iq submit --workspace <workspace-id>
iq integrate --system-config /etc/iq/system.yaml --next --repo-key <repository-uuid>
iq cleanup --repo-key <repository-uuid> --system-config /etc/iq/system.yaml
```

Normal cleanup preserves non-empty residue. If a terminal cleanup workspace's exact Rift is absent, `iq dev-workspace remove <workspace-id> --discard-residue` deletes only the residue at its exact IQ-owned path. It rejects symlinks, special entries, and `.git` or `.rift` markers at any depth.

Local submission refs under `refs/iq/submissions/` are immutable. Local items apply the exact persisted development-base-to-submission change as a one-parent candidate. Target movement creates a new candidate from that same change and invalidates validation and signoff evidence. Empty changes are blocked.

Every PR/MR landing requires a passing post-signoff provider snapshot before target mutation. IQ fetches the final target and requires the snapshot to contain the queued head and that exact base. It then requests the provider merge and records integration only after the provider reports the exact landed commit.

`--next` and `--resume` are mutually exclusive. Explicit resume accepts only the oldest active item for that repository queue.

See [Composition Workspaces](docs/composition-workspaces.md) for the lifecycle and recovery contract.

## Control Plane

The daemon serves version 1 framed JSON only on the configured Unix socket. `inbox`, `show`, `answer`, and `watch` use this API. Local answers use `SO_PEERCRED`; callers cannot supply actor identity.

```sh
iq inbox --config /etc/iq/system.yaml
iq show <item-id> --config /etc/iq/system.yaml
iq answer --config /etc/iq/system.yaml --external-id <id> --request <id> --effort <id> --attempt <id> --cycle <id> --target-sha <sha> --source-sha <sha> --answer '<text>'
iq watch --json --config /etc/iq/system.yaml --cursor 0
```

System configuration selects the exact OpenCode executable, credential environment name, agent, default model, cycle timeout, process, memory, CPU, writable-byte, protocol, log, and API bounds. It does not require runtime command or path manifests. The automatic failed-cycle limit is always 10. Project policy can override only the model.

The Linux runner uses an unprivileged mount namespace, Bubblewrap, a user-systemd scope, and a size-bounded tmpfs overlay over the retained Rift. Normal runtime trees are read-only. The configured model credential is passed directly to OpenCode. Repository remote credentials are not deliberately mounted. IQ imports only a bounded staged patch whose paths and staged-tree digest match the typed result. Btrfs qgroups and persistent filesystem quotas are not required.

## Queue Commands

```sh
iq enqueue --repo-key <repository-uuid> --source feature --head <sha>
iq list
iq events <item-id>
iq retry <item-id>
iq requeue <item-id> --head <sha>
iq cancel <item-id>
iq daemon --config /path/to/iq.yaml --system-config /etc/iq/system.yaml
iq doctor --config /path/to/iq.yaml --system-config /etc/iq/system.yaml
```

Queue state is host-local. IQ creates only the current schema. An incompatible existing database is rejected without mutation and must be removed before IQ is initialized again.

The daemon, communication, forced-command, and config-reconciliation interfaces remain available. Repository operation leases and filesystem locks are scoped to one operation, so an idle daemon does not exclude composition commands.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
