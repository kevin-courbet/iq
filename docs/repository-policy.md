# Repository Policy

## Policy File

Registration consumes one strict JSON object:

```json
{
  "operation_state": {"state": "enabled"},
  "canonical_repository": {
    "kind": "accessible",
    "object_format": "sha1",
    "fetch_url": "https://github.com/owner/repository.git",
    "push_url": "https://github.com/owner/repository.git",
    "repository_id": "<immutable-provider-id>",
    "provider": {
      "provider": "github",
      "host": "github.com",
      "repository": "owner/repository",
      "repository_id": "<immutable-provider-id>"
    }
  },
  "target_branch": "main",
  "integration_policy": "direct",
  "replication_policy": {"mode": "none"}
}
```

A local canonical repository uses an absolute bare path:

```json
{
  "operation_state": {"state": "enabled"},
  "canonical_repository": {
    "kind": "local_bare",
    "object_format": "sha1",
    "path": "/srv/git/repository.git",
    "device": 2049,
    "inode": 123456
  },
  "target_branch": "main",
  "integration_policy": "direct",
  "replication_policy": {
    "mode": "replicate",
    "targets": [
      {
        "kind": "accessible",
        "object_format": "sha1",
        "fetch_url": "https://gitlab.example/group/repository.git",
        "push_url": "https://gitlab.example/group/repository.git",
        "repository_id": "<immutable-repository-id>",
        "provider": {
          "provider": "gitlab",
          "host": "gitlab.example",
          "repository": "group/repository",
          "repository_id": "<immutable-repository-id>"
        }
      }
    ]
  }
}
```

Every canonical repository and replica records exactly one Git object format: `sha1` or `sha256`. Replicas must use the canonical format. The policy `target_branch` is `main` or `master` and is the default for new jobs. A workspace or direct admission can select another valid Git branch. IQ stores each job target as a full `refs/heads/*` ref. Every accessible repository needs provider identity. Provider values are `github` and `gitlab`. The accessible and provider repository IDs must be equal. Before an effect, GitHub reports `hash_algorithm` through `GET /repos/{owner}/{repo}/hash-algorithm`, and GitLab reports `repository_object_format` through its project API. The reported format must match policy; a provider without this capability is rejected. One global registry reserves every canonical and replica physical identity. It rejects cross-policy and cross-role reuse, even when transport, path spelling, or target differs.

New registration requires `enabled`. Before opening or creating the queue database, IQ verifies the bootstrap root and Git binding, object format, target ref, local policy bytes, canonical repository, and every replica, including local device/inode and bare Git identity or immutable provider identity. Ownership and duplicate checks use only the verified policy. IQ rechecks these identities before reservation. A failed preflight creates no reservation, intent, ownership row, policy row, root, database mutation, or storage mutation. Draining and disabled policy assignments are valid only for schema migration. Normal operation reaches them through `iq repo drain` and `iq repo disable`.

IQ starts Git with system and global configuration disabled, non-interactive transport, an explicit IQ commit identity, and `GIT_NO_REPLACE_OBJECTS=1`. Validation, provider, Rift, and agent processes that can start Git inherit the same object-resolution control. IQ clears ambient Git, SSH, askpass, credential, object-store, namespace, discovery, and configuration controls. Before every command or effect release gate, it rejects replacement refs, legacy grafts, shallow history, and alternate object databases in the verified git-dir and common-dir. The verified binding includes hardened `git rev-parse --show-object-format`; every stored or observed object ID must match that format. IQ initializes owned roots with the exact canonical format and derives zero object IDs from it. It rejects URL rewrites and repository credential or transport overrides. Every Git process has one explicit, canonical working directory. Accessible repositories accept HTTPS, SSH, or SCP syntax. Only `local_bare` accepts an absolute path or file transport. Raw remote configuration, the exact effect destination, object format, and immutable provider or local identity are checked at the process release gate. Tests use separate physical bootstrap, canonical, and replica repositories.

## Draining

Draining is an exact state, not a boolean:

```json
{
  "state": "draining",
  "obligations": [
    {"kind": "workspace", "id": "<workspace-id>"},
    {"kind": "queue_item", "id": "<item-id>"},
    {"kind": "replication", "id": "<debt-id>"}
  ]
}
```

The schema-3 migration captures obligations from current non-terminal work. New work is not added to the set.

## Migration Inventory

The offline migration accepts only strict inventory version 4:

```json
{
  "version": 4,
  "repositories": [
    {
      "repo_key": "<repository-uuid>",
      "repository": {
        "state": "ready",
        "git_binding": {
          "top_level": "/absolute/path/to/repository",
          "git_dir": "/absolute/path/to/repository/.git",
          "common_dir": "/absolute/path/to/repository/.git",
          "object_format": "sha1",
          "bare": false
        }
      },
      "policy": {
        "operation_state": {"state": "enabled"},
        "canonical_repository": {
          "kind": "accessible",
          "object_format": "sha1",
          "fetch_url": "https://github.com/owner/repository.git",
          "push_url": "https://github.com/owner/repository.git",
          "repository_id": "<immutable-provider-id>",
          "provider": {
            "provider": "github",
            "host": "github.com",
            "repository": "owner/repository",
            "repository_id": "<immutable-provider-id>"
          }
        },
        "target_branch": "main",
        "integration_policy": "direct",
        "replication_policy": {"mode": "none"}
      },
      "development_workspaces": [],
      "item_dispositions": []
    }
  ]
}
```

Generate each `git_binding` with `iq migrate inspect-git-binding --path <path>`. Do not write binding fields manually.

Migration checks Git layout, object format, linked-worktree backlinks, and required commits. Device, inode, and mount identity stays in process memory.

Each interrupted lifecycle requires an explicit `preserve` or `cancel` disposition. Unavailable or changed repositories reject migration before primary mutation.

Before migration resolves or copies the database, it verifies every inventory policy effect identity. Accessible canonical and replica identities must match their provider repository ID and object format. Local-bare canonical and replica identities must match the exact path, device, inode, bare state, and object format. An active runner requires explicit unit, cgroup, PID, and process-start authority. Migration checks this authority against systemd and `/proc`. Failure leaves every source database-family file unchanged.

Object format is required migration inventory authority. Operator-generated bindings prove the format of every live root and workspace. All schema-3 object IDs and existing refs must match that format; current schema-3 SHA-1 data remains SHA-1 unless the inventory and every live binding prove SHA-256.

Migration tests copy `tests/fixtures/schema3-2a69e24.db`, which was generated by the released schema installer at commit `2a69e24`. They run the real `iq migrate schema3` CLI and verify exact queue, attempt, invocation, event, prompt, notification, state-repository, cleanup, repository, admission, rollback, and backup values.

## Deployment Inventory

The deployment inventory supplies all repository UUIDs, operation states, canonical identities, integration policies, replication targets, and active-item dispositions. IQ product code and migration logic contain no project UUID, project name, repository count, or deployment-policy special case.

## Safety

Direct canonical target mutation can start CI or deployment. Replication starts only after canonical landing is durable. Migration validates all schema-5 content before commit. The schema-5 backup must match the exact source digest and database identity. The schema-3 migration keeps a durable backup. The schema-4 migration also keeps a durable backup. A failed migration leaves the source database usable. Do not copy user checkouts or IQ-owned roots with `rsync`, `scp`, or manual filesystem operations.

Replication uses one CLI/daemon reconciler and a transactional monotonic sequence for each immutable physical destination and target. Wall-clock timestamps do not order debt. A completed later sequence prevents an older retry from writing. Applied and superseded cleanup compare-and-delete only the exact recorded source SHA under the repository binding and lease, then verify that the pin is absent. A mismatch is an invariant error that preserves the pending cleanup state and the drifted pin. IQ first commits `superseded_cleanup_pending`, performs this cleanup, then commits `superseded`. Restart accepts and resumes the cleanup-pending state before other debt for that destination. Old debt always uses that item's exact landed SHA, even when the canonical target advances before observation. IQ publishes and verifies a durable internal source ref before debt becomes pending. The ref survives pinning, pending, applying, uncertain, failed, applied, and supersession cleanup recovery. Pin publication and cleanup are restart-safe.

Direct landing is prepared before process preflight and becomes released atomically with the command gate. A preflight failure remains not released and retryable. An unknown released push result remains `landing_uncertain`, including inside infrastructure and provider blocker resume states. Cancellation, migration, candidate rejection, target movement, and other state replacement cannot erase released or completed external landing authority. A later third target does not prove compare-and-set rejection and cannot authorize recomposition or a second push. IQ can complete the item when exact observation proves the candidate landed, or recompose only when the `git push --porcelain` record for the exact target reports `[rejected] (stale info)`.

`iq cancel` persists cancellation first, then synchronously reconciles exact runner termination. It returns failure while termination debt remains. Daemon startup retries the same debt. Private `refs/iq/repository-targets/*` and `refs/iq/landings/*` are retained only while durable checkout, uncertain landing, or replication-pin authority requires them. Safe cleanup records exact debt, compare-and-deletes the expected SHA, verifies absence, and resumes after restart.

Physical ownership, canonical repository, default target branch, integration policy, and replication policy are immutable after registration. Each job target is also immutable. SQLite triggers reject raw authority updates. Queue FIFO is strict for each target ref. Physical identity leases serialize canonical and replica effects for the repository. Operation state changes only from enabled to draining and from draining to disabled with a matching revision transition.
