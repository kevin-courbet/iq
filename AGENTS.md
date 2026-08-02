# IQ Agent Conventions

IQ is a standalone, opt-in durable Git integration coordinator.

- Keep repository-specific validation, signoff, credentials, and installation policy in consumer repositories or host configuration.
- SQLite is current host-local durable authority. External Git, provider, process, and filesystem effects require exact identity checks and restart-safe reconciliation.
- Preserve strict FIFO per physical repository target. Oldest blocked work prevents later integration.
- Preserve exact source, base, candidate, signoff, and landed SHA identity.
- Rift is the only integration-workspace backend. Retain active/blocked Rifts, reconcile terminal/orphan Rifts under the repo lease, and run approved global Rift GC after removal.
- Never weaken landing leases, target containment verification, process cancellation, or responder authorization.
- Use one-off Red-Green-Refactor smoke validation by default. Commit tests only for durable behavior and stable public contracts.

Validation:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
