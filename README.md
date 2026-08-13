# IQ

IQ is a standalone durable Git integration coordinator. It serializes work for one physical target, uses a sandboxed local integration agent for every item, builds and validates the exact candidate, applies explicit signoff policy, lands with an exact lease, and reconciles all external effects from SQLite state.

IQ uses Rift for integration, seed, and development workspaces. Registered target checkouts are integration-only. A repository must be a primary Rift root on a supported same-filesystem layout.

Registration persists the configured remote name and canonical fetch and push URL identities before the first fetch. Later registered operations reject any remote name, fetch URL, or push URL change before remote mutation.

## Composition

`.iq/config.json` is optional local control-plane configuration in the registered integration checkout. It must not be tracked. When it is absent, IQ skips validation, requires no signoff, and integrates the exact candidate SHA. When it is present, it remains strict versioned JSON:

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

IQ creates an authoritative local-policy snapshot only when a new attempt starts under the repository lease. It atomically stores the canonical snapshot and SHA-256 digest on that attempt. Resume, retry, and target movement keep the stored snapshot. A new attempt reads the current local file. Registration and seed refresh do not read policy. Doctor can inspect current local policy under the same repository lease, but it does not persist or authorize evidence. Rift copies the ignored file into seed, development, and integration workspaces, but IQ trusts only the attempt snapshot from the registered integration checkout. IQ rejects a tracked copy before landing.

Daemon validation is explicit. Use `validation: {mode: none}` or `validation: {mode: command, command: "..."}`. IQ rejects omitted validation, legacy `auto`, and legacy `validation_command`. Registered repositories also reject daemon validation commands and daemon signoff because local integration-checkout policy is authoritative. `iq doctor` reports `local_integration_checkout`, `daemon`, or `none` as the validation authority.

```sh
iq repo init --path /path/to/repo --target main --remote origin
iq repo list
iq dev-workspace create --repo-key '/path/to/repo::main' --name feature
iq submit --workspace <workspace-id>
iq integrate --system-config /etc/iq/system.yaml --next --repo-path /path/to/repo --repo-key '/path/to/repo::main'
iq cleanup --repo-key '/path/to/repo::main'
```

Normal cleanup preserves non-empty residue. If a terminal cleanup workspace's exact Rift is absent, `iq dev-workspace remove <workspace-id> --discard-residue` deletes only the residue at its exact IQ-owned path. It rejects symlinks, special entries, and `.git` or `.rift` markers at any depth.

Local submission refs under `refs/iq/submissions/` are immutable. Local items apply the exact persisted development-base-to-submission change as a one-parent candidate. Target movement creates a new candidate from that same change and invalidates validation and signoff evidence. Empty changes are blocked.

Every PR/MR landing requires a passing post-signoff provider snapshot before target mutation. For registered repositories, a PR/MR URL remains provider metadata and a provider gate. IQ fetches the final target and then requires the snapshot to contain the queued head and that exact base. IQ lands the exact validated candidate itself with a compare-and-set Git push; it does not delegate target mutation to the provider merge API.

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
iq enqueue --repo-path /path/to/repo --source feature --head <sha>
iq list
iq events <item-id>
iq retry <item-id>
iq requeue <item-id> --head <sha>
iq cancel <item-id>
iq daemon --config /path/to/iq.yaml --system-config /etc/iq/system.yaml
iq doctor --config /path/to/iq.yaml --system-config /etc/iq/system.yaml
```

Queue state is host-local. Runtime accepts schema v9 only. Upgrade schema v8 explicitly with `iq migrate --system-config /etc/iq/system.yaml`. IQ first creates a mode-0600 SQLite online backup in the same protected state directory, syncs and verifies it, and then converts active items in one immediate transaction. Migration requires no active repository or daemon lease and does not infer missing runner or repository identity.

The daemon, communication, forced-command, and config-reconciliation interfaces remain available. Repository operation leases and filesystem locks are scoped to one operation, so an idle daemon does not exclude composition commands.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
