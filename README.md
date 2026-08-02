# IQ

IQ is a durable, repository-native integration queue. It serializes completed branches into a target branch, runs repository-owned validation and signoff policy, preserves conflicted integration workspaces, and lands only the exact validated candidate.

IQ uses Rift copy-on-write snapshots for integration workspaces. Managed repositories must be initialized Rift roots before IQ starts. Terminal and orphan IQ workspaces are removed automatically; IQ then runs global Rift garbage collection so removed Rift trash is not recoverable.

Provision each repository once and configure an external workspace root on the same filesystem:

```sh
rift init --here /path/to/repo
```

IQ snapshots with Rift's complete copy-on-write mode so dependencies and build artifacts are physically reused, skips repository Rift hooks, and resets tracked state to the exact queued base SHA before integration.

IQ is standalone and opt-in. Consumers such as Threadmill and Spindle provide repository paths, validation commands, signoff policy, communication transports, and service installation policy. They do not embed IQ source.

## Development

```sh
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

## CLI

```sh
iq enqueue --repo-path /path/to/repo --source feature --head <sha>
iq list
iq events <item-id>
iq retry <item-id>
iq requeue <item-id> --head <sha>
iq cancel <item-id>
iq daemon --config /path/to/iq.yaml
iq doctor --config /path/to/iq.yaml
```

Threadmill can stage app-owned daemon configuration without implementing IQ's YAML
format:

```sh
iq config reconcile \
  --current-config /path/to/iq.yaml \
  --desired-inventory /path/to/threadmill-inventory.json \
  --current-manager-state /path/to/threadmill-manager-state.json \
  --staged-directory /path/to/generation-uuid \
  --reconcile-lock /path/to/threadmill-reconcile.lock \
  --workspace-root /path/to/stable/iq-workspaces \
  --bootstrap
```

`--current-manager-state` may be absent only with explicit `--bootstrap`. Bootstrap is
for first reconciliation; host installer persists its own initialized marker. Reconcile
refuses an existing staged directory unless it is a complete digest-identical retry. It
publishes one generation directory containing exactly `iq.yaml`,
`threadmill-manager-state.json`, `action`, and strict machine-readable `manifest.json`.
`action` contains
exactly `start\n` when effective config has repositories, or `stop\n` when effective
config is empty. Empty `repos: []` plus empty manager state is valid reconciliation
output; only daemon's standalone non-empty-config invariant is skipped for that result.
The advisory lock is held from input reads through generation publication.

Shell consumers can verify a generation without parsing its manifest:

```sh
iq config verify-generation \
  --generation /path/to/generation-uuid \
  --current-config /path/to/iq.yaml \
  --current-manager-state /path/to/threadmill-manager-state.json \
  --reconcile-lock /path/to/threadmill-reconcile.lock
```

`verify-generation` acquires same symlink-safe advisory lock, validates strict manifest
version/file inventory/digests, and compares exact current config/state presence and
bytes against manifest CAS bases. Success emits `{ "verified": true }`; any manifest,
file, lock, or input drift exits nonzero with no publication side effect.

`manifest.json` contains `version: 1`, tagged `current_config` and
`current_manager_state` snapshots (`present` with exact SHA-256 bytes, or `absent` with
the fixed absent-input digest `SHA-256("iq-reconcile:absent-input:v1")`), and SHA-256
digests for each other generation file.
Threadmill host installation must compare manifest base snapshots immediately before
immutable release/current publication (CAS); IQ's reconcile lock serializes staging but
does not replace that host install lock. A complete matching destination is idempotent;
any mismatch is rejected. File and temporary-directory fsync failures before rename fail
without publication. Parent fsync after successful rename is best-effort and reported as
a warning because publication already committed.

The desired inventory is strict JSON:

```json
{
  "manager_id": "threadmill",
  "repositories": [
    {
      "repo_path": "/workspaces/project",
      "target": "main",
      "validation": {"mode": "explicit", "command": "task validate"}
    },
    {
      "repo_path": "/workspaces/other",
      "target": "main",
      "validation": {"mode": "auto"}
    }
  ]
}
```

Manager state is strict JSON and records only Threadmill-owned logical boundaries:

```json
{
  "manager_id": "threadmill",
  "boundaries": [
    {
      "repo_path": "/workspaces/project",
      "target": "main",
      "repo_key": "/workspaces/project::main",
      "ownership": {
        "kind": "adopted",
        "original_validation": {"kind": "auto"},
        "last_applied_validation": {"kind": "explicit", "command": "task validate"}
      }
    }
  ]
}
```

`repo_path` is required absolute and canonicalized before matching; relative paths are
rejected consistently by daemon, doctor, and reconcile. New entries receive a repo key
derived from canonical path and target plus a fixed-length SHA-256 workspace directory
below supplied workspace root. Workspace identity resolves from deepest existing canonical
ancestor and rejects `.`/`..` aliases. Existing matching entries retain `repo_key`,
`workspace_root`, `remote`, `signoff`, and `communication`; only `validation_command`
follows desired intent.

Each boundary is either `adopted` or `created`:

- `adopted` records pre-existing validation as `original_validation` and tracks
  `last_applied_validation`. Removing boundary restores original validation and retains
  config entry.
- `created` records exact non-validation `baseline` plus `last_applied_validation`.
  Removing boundary deletes entry only when baseline and app validation still match.

For both variants, externally changing app-owned validation fails reconciliation instead
of overwriting it. Created-entry policy changes also fail because deletion could erase
consumer policy. Adopted-entry policy changes remain untouched. `auto` removes
`validation_command` so daemon derives safe default. Config, manager state, and action
outputs publish atomically after one source snapshot race check. Desired validation is
strictly tagged JSON: `{ "mode": "auto" }` or `{ "mode": "explicit", "command": "..." }`;
aliases and string shorthand are rejected. Manager IDs, targets, and commands must not
have surrounding whitespace.

Queue state is host-local. Run commands on the host that owns the target repository queue.
