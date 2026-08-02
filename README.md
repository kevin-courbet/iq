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

Queue state is host-local. Run commands on the host that owns the target repository queue.
