# IQ Agent Conventions

IQ is a standalone, opt-in durable Git integration coordinator.

- Keep validation and signoff in owned-root policy. Do not accept daemon policy as a second authority.
- SQLite is current host-local durable authority. External Git, provider, process, and filesystem effects require exact identity checks and restart-safe reconciliation.
- Preserve strict FIFO per physical repository target. Oldest blocked work prevents later integration.
- Preserve exact source, base, candidate, signoff, and landed SHA identity.
- The IQ-owned checkout is integration-only. Development occurs only in direct child Rifts created from that root.
- Local submissions are immutable exact-HEAD private refs and always produce one-parent squash candidates. Empty submissions never land.
- Treat `.iq/config.json` as untracked local control-plane configuration in the owned root. Its absence means no validation and no signoff. Reject tracked policy.
- At attempt start under the repository lease, persist the canonical local policy snapshot and SHA-256 digest atomically. Retries and target movement keep that snapshot; a new attempt reads current local policy.
- Policy is explicitly no validation or a validation command with signoff exactly `none` or `required`. Required signoff has an explicit command and non-empty explicit contexts.
- Rift copies of ignored local policy are allowed for tools and agents, but they are never IQ policy authority.
- Record integration and cleanup debt atomically before workspace cleanup. Cleanup must preserve dirty work.
- Rift is the only integration-workspace backend. Retain active/blocked Rifts, reconcile terminal/orphan Rifts under the repo lease, and run approved global Rift GC after removal.
- Never weaken landing leases, target containment verification, process cancellation, or responder authorization.
- Use one-off Red-Green-Refactor smoke validation by default. Commit tests only for durable behavior and stable public contracts.

Validation:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
