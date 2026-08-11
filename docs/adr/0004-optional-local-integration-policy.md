# ADR 0004: Optional Local Integration Policy

- Status: Accepted
- Date: 2026-08-11
- Decider: Kevin Courbet

## Decision

`.iq/config.json` is optional local control-plane configuration in a registered integration checkout. It must not be committed. IQ rejects a tracked copy. IQ never loads policy from target, source, or candidate Git trees.

An absent file is the explicit no-validation policy: IQ accepts the exact candidate SHA, reports validation skipped, and requires no signoff. A present file remains strict versioned JSON and defines a validation command with signoff exactly `none` or `required`. Malformed present policy is an error. IQ does not infer validation from repository files or tools.

IQ creates authoritative policy only when a new integration attempt starts under the repository lease. Attempt creation atomically persists the canonical policy snapshot and its SHA-256 digest. Resume, retry, and target movement retain that snapshot. A new attempt reads current local policy. Registration and seed refresh do not require or snapshot policy. Doctor can inspect current local policy under the repository lease, but inspection is not persisted and cannot authorize evidence.

Rift copies local policy into seed, development, and integration workspaces so tools and agents can use it. These copies are not policy authority. IQ checks again for tracked policy before landing. Provider gates do not change.

Registered repositories reject daemon validation and signoff policy. Unregistered daemon policy uses explicit `none` or `command` validation states; legacy `auto`, omitted validation, and legacy command fields are rejected. Doctor output identifies local integration checkout, daemon, or no policy authority.

## Consequences

- Repository history and candidate content cannot change or supply IQ policy.
- Local policy changes affect only attempts that start after the change.
- Repositories can integrate without validation configuration.
- Hosts must ignore `.iq/config.json` outside repository history; IQ does not add a repository `.gitignore` rule.
