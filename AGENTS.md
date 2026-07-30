# IQ Agent Conventions

IQ is a standalone, opt-in durable Git integration coordinator.

- Keep repository-specific validation, signoff, credentials, and installation policy in consumer repositories or host configuration.
- SQLite is current host-local durable authority. External Git, provider, process, and filesystem effects require exact identity checks and restart-safe reconciliation.
- Preserve strict FIFO per physical repository target. Oldest blocked work prevents later integration.
- Preserve exact source, base, candidate, signoff, and landed SHA identity.
- Never weaken landing leases, target containment verification, process cancellation, or responder authorization.
- Use one-off Red-Green-Refactor smoke validation by default. Commit tests only for durable behavior and stable public contracts.

Validation:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
