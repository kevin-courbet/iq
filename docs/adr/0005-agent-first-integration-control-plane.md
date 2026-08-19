# ADR 0005: Agent-First Integration Control Plane

- Status: Accepted
- Date: 2026-08-12
- Decider: Kevin Courbet

## Decision

Every queue item participates in one durable integration effort. IQ mechanically composes the target and source in a retained integration Rift. A local integration agent then performs or approves semantic integration for every item, including clean composition. For clean composition, the agent can return `resolved` without edits.

The agent stages an integrated tree and returns a typed result. It never commits or authorizes landing. After each accepted resolution or repair, one IQ-owned candidate builder first records durable build intent, creates the candidate shape required by the source and landing variant, reconciles its exact SHA, and invalidates earlier validation and signoff evidence. IQ remains authority for queue order, attempts, Rifts, candidate shape, validation, signoff, provider gates, leases, landing, reconciliation, and cleanup debt.

OpenCode is the first OS-sandboxed local runner. The runner boundary permits future local runner implementations without changing integration-effort, candidate, state-repository, local API, notification, or landing contracts. Runner, agent, and default model are system configuration. A project can override only the model. Ten failed automatic cycles is a fixed limit; it is not configurable.

ADR 0006 supersedes the runner isolation and threat boundary in this decision. The lifecycle authority and correctness decisions in this ADR remain unchanged.

SQLite is the sole durable queue, integration-effort, decision, event, projection-debt, and notification-delivery authority. Each project uses exactly one tagged state repository: `local`, `github_issue`, or `gitlab_issue`; the default is `local`. GitHub and GitLab use one issue per queue item. Full visibility projects every durable lifecycle transition. Minimal visibility projects every typed integration blocker.

The local API and notifications are separate contracts. The local API uses a verified Unix-domain socket and peer credentials for reads, answers, and the durable event stream. Notifications are bounded, best-effort attention signals over durable events. They cannot accept answers or become state authority.

Whole-conflict `use source` and `use target` prompts are removed from normal integration. Semantic ambiguity creates a typed guidance request. Mechanical agent failures retry automatically up to the fixed limit. Infrastructure, provider, signoff, and landing uncertainty stay distinct typed states.

## Consequences

- Producer work can finish after immutable submission while IQ handles later fan-in.
- Clean and conflicted composition use one semantic integration path.
- Candidate identity and all evidence remain under IQ authority.
- One repository variant prevents simultaneous or contradictory projections.
- SQLite remains the sole lifecycle authority for active effort identities.
- OpenCode needs an OS-enforced sandbox; prompt permissions are not a security boundary.
- Local API availability and notification availability can fail independently.
- Notification delivery with an unknown external outcome is not retried automatically.
- Released landing authority, including authority inside a blocked resume state, cannot be cancelled or replaced before reconciliation proves the outcome.
- A future runner must implement the same typed protocol and sandbox boundary; it cannot change queue or landing semantics.
