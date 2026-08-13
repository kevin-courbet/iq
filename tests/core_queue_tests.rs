use iq::core::{BlockedPhase, BlockedReason, QueueStatus};
use iq::issue_backends::{issue_adapter_for_provider, IssueProvider, IssueSyncTarget};
use iq::providers::{provider_for_url, ProviderGate};
use iq::sqlite::{EnqueueRequest, SqliteQueue};
use rusqlite::{params, Connection};
use std::fs;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn sqlite_enqueue_is_idempotent_but_rejects_active_head_changes() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();

    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let repeated = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let changed = queue.enqueue(EnqueueRequest {
        repo_key: "repo::main".into(),
        repo_path: "/repo".into(),
        source_branch: "agent/one".into(),
        target_branch: "main".into(),
        current_head_sha: "222".into(),
        pr_url: None,
        producer_metadata: serde_json::json!({"worker":"W001","attempt":2}),
        state_repository: iq::control_domain::StateRepositorySnapshot::Local,
    });

    assert_eq!(first.id, repeated.id);
    assert!(changed.is_err());
    assert_eq!(queue.get_item(&first.id).unwrap().current_head_sha, "111");
    assert_eq!(queue.list_items().unwrap().len(), 1);
}

#[test]
fn sqlite_read_fails_on_corrupt_persisted_json_fields() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    queue
        .set_conflict_metadata(
            &item.id,
            &serde_json::json!({"files":["a.txt"]}),
            "target",
            "source",
        )
        .unwrap();
    let conn = Connection::open(&db).unwrap();

    conn.execute(
        "UPDATE queue_items SET validation_evidence_json=?1 WHERE id=?2",
        params!["{not-json", item.id],
    )
    .unwrap();
    let error = format!("{:#}", queue.get_item(&item.id).unwrap_err());
    assert!(error.contains("validation_evidence_json"), "{error}");

    conn.execute(
        "UPDATE queue_items SET validation_evidence_json='{}', producer_metadata_json=?1 WHERE id=?2",
        params!["{not-json", item.id],
    )
    .unwrap();
    let error = format!("{:#}", queue.get_item(&item.id).unwrap_err());
    assert!(error.contains("producer_metadata_json"), "{error}");

    conn.execute(
        "UPDATE queue_items SET producer_metadata_json='{}', conflict_json=?1 WHERE id=?2",
        params!["{not-json", item.id],
    )
    .unwrap();
    let error = format!("{:#}", queue.get_item(&item.id).unwrap_err());
    assert!(error.contains("conflict_json"), "{error}");
}

#[test]
fn schema_v9_migration_preserves_terminal_attempt_policy_as_opaque_history() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue.claim_next_ready("repo::main").unwrap().unwrap();
    queue
        .transition_item(&item.id, QueueStatus::Cancelled)
        .unwrap();
    drop(queue);

    let connection = Connection::open(&db).unwrap();
    mark_database_as_v8(&connection);
    connection
        .execute(
            "UPDATE integration_attempts SET policy_snapshot_json='opaque legacy policy',policy_digest='opaque legacy digest' WHERE id=?1",
            params![attempt.id],
        )
        .unwrap();
    drop(connection);
    let system_config = write_system_config(temp.path());

    let migrated = SqliteQueue::migrate_v8(&db, &system_config).unwrap();
    let migrated_attempt = migrated.get_attempt(&attempt.id).unwrap();
    assert_eq!(
        migrated_attempt.policy_snapshot_json.as_deref(),
        Some("opaque legacy policy")
    );
    assert_eq!(
        migrated_attempt.policy_digest.as_deref(),
        Some("opaque legacy digest")
    );

    let connection = Connection::open(&db).unwrap();
    let version: String = connection
        .query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "9");
    assert!(temp.path().join("queues.db.schema-v8.backup").exists());
}

#[test]
fn schema_v9_migration_rejects_ambiguous_active_items() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    drop(queue);

    let connection = Connection::open(&db).unwrap();
    mark_database_as_v8(&connection);
    drop(connection);
    let system_config = write_system_config(temp.path());

    let error = match SqliteQueue::migrate_v8(&db, &system_config) {
        Ok(_) => panic!("nonterminal queue migrated"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("lacks attempt identity"), "{error}");
    let connection = Connection::open(&db).unwrap();
    let version: String = connection
        .query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "8");
}

#[test]
fn schema_v9_migration_rejects_active_repository_leases() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    assert!(queue.acquire_repo_lease("repo::main", "owner", 60).unwrap());
    drop(queue);

    let connection = Connection::open(&db).unwrap();
    mark_database_as_v8(&connection);
    drop(connection);
    let system_config = write_system_config(temp.path());

    let error = match SqliteQueue::migrate_v8(&db, &system_config) {
        Ok(_) => panic!("active lease migrated"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("no active repository-operation or daemon lease"));
    let connection = Connection::open(&db).unwrap();
    let version: String = connection
        .query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "8");
}

fn mark_database_as_v8(connection: &Connection) {
    connection
        .execute_batch(
            "CREATE TABLE communication_bindings(id TEXT PRIMARY KEY);
             CREATE TABLE communication_response_receipts(id TEXT PRIMARY KEY);
             UPDATE queue_metadata SET value='8' WHERE key='workspace_schema_version';",
        )
        .unwrap();
}

fn write_system_config(root: &std::path::Path) -> std::path::PathBuf {
    let path = root.join("system.yaml");
    fs::write(
        &path,
        format!(
            "integration_agent:\n  runner: opencode\n  executable: /bin/true\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 10\n  max_log_bytes: 4096\n  max_result_bytes: 4096\n  max_processes: 4\n  memory_bytes: 67108864\n  cpu_seconds: 10\n  writable_bytes: 1048576\n  open_files: 64\n  credential_env: TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
            root.display()
        ),
    )
    .unwrap();
    path
}

#[test]
fn sqlite_repo_lease_prevents_concurrent_integrators_until_expiry() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();

    assert!(queue
        .acquire_repo_lease("repo::main", "owner-a", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("repo::main", "owner-b", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("repo::main", "owner-a", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("expired::main", "owner-a", -1)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("expired::main", "owner-b", 60)
        .unwrap());
}

#[test]
fn sqlite_repo_lease_owner_check_rejects_stale_integrator() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();

    assert!(queue
        .acquire_repo_lease("repo::main", "owner-a", -1)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("repo::main", "owner-b", 60)
        .unwrap());

    assert!(!queue
        .ensure_repo_lease_owner("repo::main", "owner-a", 60)
        .unwrap());
    assert!(queue
        .ensure_repo_lease_owner("repo::main", "owner-b", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("repo::main", "owner-a", 60)
        .unwrap());

    assert!(queue
        .acquire_repo_lease("renewed::main", "owner-a", -1)
        .unwrap());
    assert!(!queue
        .ensure_repo_lease_owner("renewed::main", "owner-a", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("renewed::main", "owner-a", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("renewed::main", "owner-b", 60)
        .unwrap());
}

#[test]
fn claim_next_ready_holds_queue_behind_oldest_active_item() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    let repo_key = "repo::main";

    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let second = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/two".into(),
            target_branch: "main".into(),
            current_head_sha: "222".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let (claimed, _) = queue.claim_next_ready(repo_key).unwrap().unwrap();
    assert_eq!(claimed.id, first.id);
    queue
        .block_item(
            &first.id,
            BlockedPhase::Merging,
            BlockedReason::Infra,
            "resolve first before later work starts",
        )
        .unwrap();

    assert!(queue.claim_next_ready(repo_key).unwrap().is_none());
    assert_eq!(
        queue.get_item(&second.id).unwrap().status,
        QueueStatus::Ready
    );
}

#[test]
fn github_provider_adapter_maps_cli_checks_to_gate_state() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-gh");
    fs::write(
        &fake,
        "#!/bin/sh\nif [ \"$1 $2\" = \"pr view\" ]; then\n  printf '%s' '{\"headRefOid\":\"head1\",\"baseRefOid\":\"base1\",\"reviewDecision\":\"APPROVED\",\"mergeStateStatus\":\"CLEAN\",\"statusCheckRollup\":[{\"status\":\"COMPLETED\",\"conclusion\":\"SUCCESS\"}]}'\nelse\n  exit 0\nfi\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake);

    let provider = provider_for_url("https://github.com/org/repo/pull/7").unwrap();
    let snapshot = provider
        .snapshot("https://github.com/org/repo/pull/7")
        .unwrap();

    assert_eq!(snapshot.head_sha, "head1");
    assert_eq!(snapshot.base_sha, "base1");
    assert_eq!(snapshot.gate, ProviderGate::Pass);
    std::env::remove_var("IQ_GITHUB_CLI");
}

#[test]
fn provider_selection_rejects_malformed_supported_host_urls() {
    assert!(provider_for_url("https://github.com/org/repo").is_err());
    assert!(provider_for_url("https://gitlab.com/group/project/merge_requests/7").is_err());
    assert!(provider_for_url("ssh://github.com/org/repo/pull/7").is_err());
}

#[test]
fn github_issue_backend_syncs_projection() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-gh");
    let log = temp.path().join("gh.log");
    fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> {log}
if [ "$1 $2" = "issue create" ]; then
  printf '%s' '{{"number":42,"url":"https://github.com/org/repo/issues/42"}}'
  exit 0
fi
if [ "$1 $2" = "issue view" ]; then
  printf '%s' '{{"comments":[{{"body":"Looks good\n\n```\niq answer prompt-1 use source\n```","author":{{"login":"octo"}}}}]}}'
  exit 0
fi
exit 0
"#,
            log = log.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake);

    let projection = iq::issue_backends::IssueProjection {
        title: "Integration queue: agent/one → main".into(),
        labels: vec!["iq:queue".into(), "iq:status:blocked".into()],
        body: "<!-- iq:item:item-1 -->\nbody".into(),
        comments: vec!["<!-- iq:prompt:prompt-1 -->\nPrompt".into()],
    };
    let adapter = issue_adapter_for_provider(IssueProvider::GitHub).unwrap();
    let synced = adapter
        .sync_projection(
            &IssueSyncTarget {
                repo: "org/repo".into(),
                issue: None,
            },
            &projection,
        )
        .unwrap();
    let captured = fs::read_to_string(&log).unwrap();
    assert!(captured.contains("issue create"), "{captured}");
    assert!(captured.contains("issue comment 42"), "{captured}");
    assert_eq!(synced.url, "https://github.com/org/repo/issues/42");
    std::env::remove_var("IQ_GITHUB_CLI");
}

#[test]
fn github_issue_backend_updates_managed_labels_and_skips_existing_marker_comments() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-gh");
    let log = temp.path().join("gh.log");
    fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> {log}
if [ "$1 $2" = "issue view" ]; then
  printf '%s' '{{"labels":[{{"name":"iq:queue"}},{{"name":"iq:status:ready"}},{{"name":"external"}}],"comments":[{{"body":"<!-- iq:prompt:prompt-1 -->\nold prompt","author":{{"login":"octo"}}}}]}}'
  exit 0
fi
exit 0
"#,
            log = log.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake);

    let projection = iq::issue_backends::IssueProjection {
        title: "Integration queue: agent/one → main".into(),
        labels: vec!["iq:queue".into(), "iq:status:blocked".into()],
        body: "<!-- iq:item:item-1 -->\nbody".into(),
        comments: vec![
            "<!-- iq:prompt:prompt-1 -->\nPrompt".into(),
            "<!-- iq:event:event-1 -->\nEvent".into(),
        ],
    };
    let adapter = issue_adapter_for_provider(IssueProvider::GitHub).unwrap();
    adapter
        .sync_projection(
            &IssueSyncTarget {
                repo: "org/repo".into(),
                issue: Some("42".into()),
            },
            &projection,
        )
        .unwrap();

    let captured = fs::read_to_string(&log).unwrap();
    assert!(captured.contains("issue view 42"), "{captured}");
    assert!(
        captured.contains("--add-label iq:status:blocked"),
        "{captured}"
    );
    assert!(
        captured.contains("--remove-label iq:status:ready"),
        "{captured}"
    );
    assert!(!captured.contains("--remove-label external"), "{captured}");
    assert!(
        captured.contains("issue comment 42 --repo org/repo --body <!-- iq:event:event-1 -->"),
        "{captured}"
    );
    assert!(
        !captured.contains("issue comment 42 --repo org/repo --body <!-- iq:prompt:prompt-1 -->"),
        "{captured}"
    );
    std::env::remove_var("IQ_GITHUB_CLI");
}

#[test]
fn gitlab_issue_backend_updates_managed_labels_and_skips_existing_marker_notes() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-glab");
    let log = temp.path().join("glab.log");
    fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> {log}
if [ "$1 $2" = "issue view" ]; then
  printf '%s' '{{"labels":["iq:queue","iq:status:blocked","team"],"comments":[{{"body":"<!-- iq:prompt:prompt-2 -->\nold prompt","author":{{"username":"maintainer"}}}}]}}'
  exit 0
fi
exit 0
"#,
            log = log.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITLAB_CLI", &fake);

    let projection = iq::issue_backends::IssueProjection {
        title: "Integration queue: agent/two → main".into(),
        labels: vec!["iq:queue".into(), "iq:status:validating".into()],
        body: "<!-- iq:item:item-2 -->\nbody".into(),
        comments: vec![
            "<!-- iq:prompt:prompt-2 -->\nPrompt".into(),
            "<!-- iq:event:event-2 -->\nEvent".into(),
        ],
    };
    let adapter = issue_adapter_for_provider(IssueProvider::GitLab).unwrap();
    adapter
        .sync_projection(
            &IssueSyncTarget {
                repo: "org/repo".into(),
                issue: Some("9".into()),
            },
            &projection,
        )
        .unwrap();

    let captured = fs::read_to_string(&log).unwrap();
    assert!(captured.contains("issue view 9"), "{captured}");
    assert!(
        captured.contains("--label iq:status:validating"),
        "{captured}"
    );
    assert!(
        captured.contains("--unlabel iq:status:blocked"),
        "{captured}"
    );
    assert!(!captured.contains("--unlabel team"), "{captured}");
    assert!(
        captured.contains("issue note 9 --repo org/repo --message <!-- iq:event:event-2 -->"),
        "{captured}"
    );
    assert!(
        !captured.contains("issue note 9 --repo org/repo --message <!-- iq:prompt:prompt-2 -->"),
        "{captured}"
    );
    std::env::remove_var("IQ_GITLAB_CLI");
}
