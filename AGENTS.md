# IQ Agent Conventions

IQ is a standalone, opt-in durable Git integration coordinator.

- Keep repository-specific validation, signoff, credentials, and installation policy in consumer repositories or host configuration.
- SQLite is current host-local durable authority. External Git, provider, process, and filesystem effects require exact identity checks and restart-safe reconciliation.
- Preserve strict FIFO per physical repository target. Oldest blocked work prevents later integration.
- Preserve exact source, base, candidate, signoff, and landed SHA identity.
- Registered target checkouts are integration-only. Development occurs only in IQ-owned Rift workspaces created from the exact detached IQ seed.
- Local submissions are immutable exact-HEAD private refs and always produce one-parent squash candidates. Empty submissions never land.
- Read strict versioned `.iq/config.json` only from the exact target-base SHA. Persist that SHA, the canonical policy snapshot, and its SHA-256 digest for every attempt.
- Signoff policy is exactly `none` or `required`. Required signoff has an explicit command and non-empty explicit contexts; target movement invalidates all evidence.
- Record integration and cleanup debt atomically before seed or workspace cleanup. Cleanup must preserve dirty work.
- Rift is the only integration-workspace backend. Retain active/blocked Rifts, reconcile terminal/orphan Rifts under the repo lease, and run approved global Rift GC after removal.
- Never weaken landing leases, target containment verification, process cancellation, or responder authorization.
- Use one-off Red-Green-Refactor smoke validation by default. Commit tests only for durable behavior and stable public contracts.

Validation:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
