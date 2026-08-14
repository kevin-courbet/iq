# ADR 0008: IQ-Owned Repository Roots

- Status: Accepted
- Date: 2026-08-14
- Decider: Kevin Courbet
- Supersedes: ADR 0002 and ADR 0003 repository-root, key, and target clauses; ADR 0004 policy-location clauses

## Decision

IQ supports exactly one target branch for each remote repository. The target is
`main` or `master`. Exact fetch and push URL identity has one durable owner row.
That row owns one repository UUID and one immutable target before provisioning
starts and after the repository becomes ready.

IQ owns one full checkout for each remote repository. This checkout is the Git
and ref authority and the independent Rift root. All development and
integration Rifts are direct children of this root.

A user checkout is registration input only. IQ does not persist
or open it during normal operation. Its branch, worktree state, path, rename,
or deletion does not affect IQ. Registration first stores the absolute lexical
request path and options. A retry uses that request identity before it resolves
the bootstrap checkout, so the same spelling remains valid after rename or
deletion. Multiple request paths can bind one repository UUID when they resolve
the same remote and target. After secure remote identity resolution, an
existing remote owner is authoritative before any new target observation. A
ready repository returns directly, and an active intent resumes its durable
plan without reading the bootstrap target or remote target again unless its
recorded object is missing at the fetch phase.

The fetched remote target ref is the target source of truth. A fetched or
imported source SHA is immutable. IQ records each target observation before it
fetches that exact SHA into a private ref. Under durable pending checkout
authority, retries fetch the recorded SHA without making a new observation. IQ
verifies the object and publishes the remote-tracking ref at that SHA. A later
observation is permitted only after checkout reconciliation is ready. Target
movement invalidates old candidate, validation, and signoff evidence. The
attempt records pending recomposition and the exact replacement candidate after
recomposition. SQLite is the lifecycle authority. Each command validation has
an immutable invocation row with its exact target and candidate SHA. Target
movement marks prior invocation evidence invalid instead of erasing its
history. A successful validation command cannot change candidate `HEAD`; a
changed `HEAD` records an invocation with no success evidence before repair is
attempted, then blocks as a validation infrastructure failure. IQ classifies
post-command worktree dirtiness only after candidate `HEAD` identity matches.

Repository keys are opaque UUIDs. Repository identity is not derived from a
path.

Local `.iq/config.json` policy is optional untracked input. IQ copies it into
the owned root. A tracked policy is invalid. The owned root is the runtime
policy location. IQ opens the policy directory, policy file, and owner marker
without following symbolic links. It accepts only bounded regular files.

Provisioning has these durable states: reserved, staging directory, Git
initialized, remote configured, target fetched, target checked out, root
published, policy published, Rift initialized, Rift verified, owner published,
child roots published, and ready. Restart reconciles each state against the
exact external effect before it advances. Existing-identical file and database
publication recovery validates and synchronizes the destination and its parent
directory before it reports success.

Each development and integration child root has current and optional pending
generation authority. Workspace creation stores the pending generation before
it publishes the marker. Restart accepts only the exact current or pending
marker and completes the generation as part of the same creation recovery.

The versioned owner marker records the queue database ID, repository UUID,
owned-root path, root Rift ID, Rift registry path and file identity, and
generation. The database records the same identity for the repository and both
child roots. Child markers also record the exact role and path. Every repository
operation acquires the kernel process lock, replaces its durable heartbeat row,
and verifies this identity before it can read or mutate repository state. A
dead process cannot delay recovery until heartbeat expiry.

IQ installs only its current schema, currently marker `3`. An existing database
with an incompatible marker or shape, including an empty file, is rejected
without mutation. For a missing destination, IQ creates and validates one
private sibling database, syncs it, publishes it with an atomic no-replace
rename, and syncs the parent directory.

## Consequences

- The user checkout is never runtime authority.
- The owned checkout has no Git alternates or shared object store.
- The owned Rift root has no ancestors.
- The development and integration roots are exact direct children of the owned
  root and use the same persisted Rift registry file identity.
- SQLite records exact owned-root, remote, target, Rift, child-root, generation,
  provisioning, and cleanup identities.
- A repository row cannot exist without its exact remote-owner row, and one
  remote owner cannot have both a provisioning intent and a ready repository.
- Queue items refer to a repository key and do not store repository paths or
  target branches.
