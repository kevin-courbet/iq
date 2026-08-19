# Composition Workspaces

## Authority

Repository policy, not location, defines the canonical repository and target. A canonical repository is either a local bare path or an accessible Git repository with exact fetch, push, and provider identity.

The IQ-owned root is a materialization and independent Rift root. Development and retained integration Rifts are direct children. Replicas are destinations only.

## Registration

`iq repo init` requires `--policy`. The bootstrap checkout supplies only bootstrap objects and optional untracked `.iq/config.json`. IQ does not inspect its remote to select authority.

Provisioning persists the repository UUID, explicit policy, canonical target observation, owned paths, exact Rift registry identity, and restart-safe lifecycle before external effects. Local bare and accessible canonical repositories use the same verified Git fetch and compare-and-set mechanisms.

## Workspace Creation

`iq workspace create` first authorizes new work. It then validates the name, resolves the exact current canonical target, records pending checkout authority, fetches that exact object, reconciles the owned root, records the workspace base, and creates one direct child Rift. Any canonical resolution or fetch failure fails closed.

`list` and `status` are reads. `remove` is safe cleanup and remains available when disabled. Residue discard accepts only an absent exact Rift and an exact IQ-owned path with no symlink, special file, `.git`, or `.rift` marker.

## Direct Integration

Direct policy permits `iq admit direct` and `iq submit`. Local submissions are immutable exact-HEAD private refs. Composition applies the recorded development-base-to-submission tree change to the exact current target and creates one-parent squash candidates.

Source, target, candidate, policy snapshot, validation invocation, and signoff evidence use exact SHAs. Target movement invalidates old evidence, records movement, recomposes, and revalidates. Source movement rejects the admission or requires an explicit direct requeue where legal.

Landing prepares durable authority before process preflight and records release only when the command gate opens. It pushes the validated candidate with `--force-with-lease=<target>:<expected-sha>`. Only the exact target's structured porcelain stale-lease rejection permits recomposition. An uncertain released result keeps exact landing reconciliation authority.

Direct canonical mutation can start CI or deployment.

## Merge-Request Integration

Merge-request-required policy rejects direct admission and local submit. The coding agent owns branch push, MR creation, MR description, and source updates. `iq admit mr <url>` records provider, canonical repository identity, target, MR identity, head, and current canonical base.

IQ fetches the provider MR ref at the admitted head. A changed head is stale. A changed base causes target movement handling. IQ never pushes an MR source update and never creates an MR. If conflict resolution changes the candidate, IQ blocks for the coding agent to update and readmit the MR.

Before provider mutation, IQ requires one provider operation that atomically pins the admitted head and validated base. The current GitHub and GitLab CLI adapters cannot supply this guarantee, so they block without mutation. A future adapter must also verify the landed tree, first parent, admitted-head ancestry, and canonical target containment.

## Replication

After canonical integration is durable, IQ creates one exact replication lifecycle per configured replica. One global physical-identity registry prevents any canonical or replica from being owned by another policy. Physical identity leases serialize effects. Replica advancement uses its own target observation and compare-and-set push. Failure stores retryable debt and does not change the integrated canonical outcome.

Replicas never participate in workspace freshness, target movement, owned-root reconciliation, candidate construction, validation, or landing decisions.

Do not use `rsync`, `scp`, or manual copies from owned roots. Only verified Git object and ref operations can move repository state.

## Operation States

Enabled allows new authorized work. Draining stores the exact workspace, queue-item, and replication obligations that can finish. Disabled blocks mutation, integration, retry, landing, and replication. Reads, cancellation, and safe cleanup remain legal.

Authorization occurs before request argument validation and before every external mutation boundary.

## Internal Integration Workspace

`iq integration status` and `iq integration reset` operate only on retained internal integration Rifts. They do not refer to coding-agent workspaces. Terminal cleanup debt remains durable and does not permit deletion of dirty work.
