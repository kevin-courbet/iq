# Composition Workspaces

## Authority

Repository policy defines the canonical repository and the default target branch. Each job stores one immutable full target ref. A canonical repository is either a local bare path or an accessible Git repository with exact fetch, push, and provider identity.

The IQ-owned root is a materialization and independent Rift root. Development and retained integration Rifts are direct children. Replicas are destinations only.

## Registration

`iq repo init` requires `--policy`. The bootstrap checkout supplies only bootstrap objects and optional untracked `.iq/config.json`. IQ does not inspect its remote to select authority.

Provisioning persists the repository UUID, explicit policy, canonical target observation, owned paths, exact Rift registry identity, and restart-safe lifecycle before external effects. Local bare and accessible canonical repositories use the same verified Git fetch and compare-and-set mechanisms.

## Workspace Creation

`iq workspace create` first authorizes new work. `--target-branch` selects a branch and requires `--expected-target-sha`. Without these options, IQ uses the policy default and current target. IQ resolves the selected target once under repository authority. An explicit target must equal the expected SHA before IQ records an intent or changes workspace state. IQ stores the full `refs/heads/*` ref and expected target SHA. It then fetches that object and creates one direct child Rift. A moved explicit target returns `WorkspaceTargetMovedError` without workspace mutation.

A durable creation intent becomes the workspace target authority. A matching retry does not observe the current remote target. It returns an active workspace or resumes the exact `Creating` workspace. Remote movement cannot block replay, status, cancellation, or cleanup. A changed name, target ref, or expected target SHA fails closed. Mutating resume steps still require the captured workspace obligation during draining.

`list` and `status` are reads. `remove` is safe cleanup and remains available when disabled. Residue discard accepts only an absent exact Rift and an exact IQ-owned path with no symlink, special file, `.git`, or `.rift` marker.

## Direct Integration

Direct policy permits `iq admit direct` and `iq submit`. `iq admit direct --target-branch` selects a target. A submission uses its workspace target. Local submissions are immutable exact-HEAD private refs. Composition applies the source tree change to that exact target.

If a submission receipt is lost, repeat `iq submit --workspace <id>`. IQ resumes an incomplete intent or reconstructs the original ready receipt. Replay remains valid while the item is ready, integrating, waiting for review, landing, or integrated. Replay also remains valid during cleanup and after workspace removal. IQ requires the exact workspace, submission, source SHA, target ref, item, and immutable private ref. Identity disagreement or multiple active submissions fails closed. `--replace` keeps its separate replacement rules.

Source, target, candidate, policy snapshot, validation invocation, and signoff evidence use exact SHAs. Target movement invalidates old evidence, records movement, recomposes, and revalidates. Source movement rejects the admission or requires an explicit direct requeue where legal.

Landing prepares durable authority before process preflight and records release only when the command gate opens. It pushes the validated candidate with `--force-with-lease=<target>:<expected-sha>`. Only the exact target's structured porcelain stale-lease rejection permits recomposition. An uncertain released result keeps exact landing reconciliation authority.

Direct canonical mutation can start CI or deployment.

IQ computes candidate classification after composition. A clean composition with no agent tree change is mechanical. A conflict or agent tree change is semantic. Semantic candidates require an exact authorized review. Approval continues validation. A request for changes starts the next agent cycle with the review text.

## Merge-Request Integration

Merge-request-required policy rejects direct admission and local submit. The coding agent owns branch push, MR creation, MR description, and source updates. `iq admit mr <url>` records provider, canonical repository identity, target, MR identity, head, and current canonical base.

IQ fetches the provider MR ref at the admitted head. A changed head is stale. A changed base causes target movement handling. IQ never pushes an MR source update and never creates an MR. If conflict resolution changes the candidate, IQ blocks for the coding agent to update and readmit the MR.

Before provider mutation, IQ requires one provider operation that atomically pins the admitted head and validated base. The current GitHub and GitLab CLI adapters cannot supply this guarantee, so they block without mutation. A future adapter must also verify the landed tree, first parent, admitted-head ancestry, and canonical target containment.

## Replication

After canonical integration is durable, IQ creates one exact replication lifecycle per configured replica. Replication uses the item target ref. One global physical-identity registry prevents any canonical or replica from being owned by another policy. Physical identity leases serialize effects. Failure stores retryable debt and does not change the integrated canonical outcome.

Queue FIFO is strict for each repository and target ref. Work for one blocked target does not block a different target. One repository lease still serializes repository mutations.

[ADR 0010](adr/0010-exact-job-targets-and-candidate-review.md) defines exact job-target and candidate-review authority.

Replicas never participate in workspace freshness, target movement, owned-root reconciliation, candidate construction, validation, or landing decisions.

Do not use `rsync`, `scp`, or manual copies from owned roots. Only verified Git object and ref operations can move repository state.

## Operation States

Enabled allows new authorized work. Draining stores the exact workspace, queue-item, and replication obligations that can finish. Disabled blocks mutation, integration, retry, landing, and replication. Reads, cancellation, and safe cleanup remain legal.

Authorization occurs before request argument validation and before every external mutation boundary.

## Internal Integration Workspace

`iq integration status` and `iq integration reset` operate only on retained internal integration Rifts. They do not refer to coding-agent workspaces. Terminal cleanup debt remains durable and does not permit deletion of dirty work.
