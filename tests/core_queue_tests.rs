use iq::core::{BlockedPhase, BlockedReason, QueueStatus};
use iq::issue_backends::{issue_adapter_for_provider, IssueProvider, IssueSyncTarget};
use iq::providers::{provider_for_url, ProviderGate};
use iq::sqlite::{
    EnqueueRequest, ReplacementState, SqliteQueue, WorkspaceIdentity, WorkspaceState,
};
use rusqlite::{params, types::Value, Connection};
use std::fs;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn register(queue: &SqliteQueue, repo_key: &str, path: &str) {
    queue
        .register_control_plane_fixture_repository(repo_key, path, "main")
        .unwrap();
}

#[test]
fn sqlite_enqueue_is_idempotent_but_rejects_active_head_changes() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    register(&queue, "00000000-0000-4000-8000-000000000001", "/repo");

    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: "00000000-0000-4000-8000-000000000001".into(),
            source_branch: "agent/one".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let repeated = queue
        .enqueue(EnqueueRequest {
            repo_key: "00000000-0000-4000-8000-000000000001".into(),
            source_branch: "agent/one".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let changed = queue.enqueue(EnqueueRequest {
        repo_key: "00000000-0000-4000-8000-000000000001".into(),
        source_branch: "agent/one".into(),
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
fn cleanup_debt_sql_boundary_rejects_invalid_targets_and_alert_authority() {
    let mutations = [
        (
            "unknown_key_insert",
            "INSERT INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,json_object('path','/workspaces/row','rift_id','rift','source_rift_id','source','hostile',1),'retained','pending',NULL,0,'2026-08-13T00:00:00Z',NULL,'2026-08-13T00:00:00Z','2026-08-13T00:00:00Z')",
        ),
        (
            "empty_path_insert",
            "INSERT INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,json_object('path','','rift_id','rift','source_rift_id','source'),'retained','pending',NULL,0,'2026-08-13T00:00:00Z',NULL,'2026-08-13T00:00:00Z','2026-08-13T00:00:00Z')",
        ),
        (
            "empty_rift_insert",
            "INSERT INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,json_object('path','/workspaces/row','rift_id','','source_rift_id','source'),'retained','pending',NULL,0,'2026-08-13T00:00:00Z',NULL,'2026-08-13T00:00:00Z','2026-08-13T00:00:00Z')",
        ),
        (
            "empty_source_insert",
            "INSERT INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,json_object('path','/workspaces/row','rift_id','rift','source_rift_id',''),'retained','pending',NULL,0,'2026-08-13T00:00:00Z',NULL,'2026-08-13T00:00:00Z','2026-08-13T00:00:00Z')",
        ),
    ];
    for (name, sql) in mutations {
        let temp = tempdir().unwrap();
        let db = temp.path().join(format!("{name}.db"));
        let queue = SqliteQueue::open(&db).unwrap();
        let item = enqueue_current_item(&queue, 0);
        queue.transition_item(&item, QueueStatus::Merging).unwrap();
        queue
            .set_workspace_intent(&item, "/workspaces/row")
            .unwrap();
        queue
            .set_workspace_identity(&item, "/workspaces/row", "rift", "source")
            .unwrap();
        drop(queue);
        let connection = Connection::open(&db).unwrap();
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .unwrap();
        assert!(connection.execute(sql, [&item]).is_err(), "case {name}");
    }

    for (name, event_item, event_type, alert) in [
        ("missing_event", None, "terminal_workspace_preserved", 1),
        (
            "wrong_item",
            Some("other"),
            "terminal_workspace_preserved",
            1,
        ),
        ("wrong_type", None, "hostile", 1),
        ("wrong_alert", None, "terminal_workspace_preserved", 0),
    ] {
        let temp = tempdir().unwrap();
        let db = temp.path().join(format!("{name}.db"));
        let queue = SqliteQueue::open(&db).unwrap();
        let item = enqueue_current_item(&queue, 0);
        let other = enqueue_current_item(&queue, 1);
        queue.transition_item(&item, QueueStatus::Merging).unwrap();
        queue
            .set_workspace_intent(&item, "/workspaces/row")
            .unwrap();
        queue
            .set_workspace_identity(&item, "/workspaces/row", "rift", "source")
            .unwrap();
        drop(queue);
        let connection = Connection::open(&db).unwrap();
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .unwrap();
        if name != "missing_event" {
            connection
                .execute(
                    "INSERT INTO durable_events(id,item_id,event_type,payload_json,alert,created_at) VALUES('alert',?1,?2,'{}',?3,'2026-08-13T00:00:00Z')",
                    params![event_item.map_or(item.as_str(), |_| other.as_str()), event_type, alert],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,json_object('path','/workspaces/row','rift_id','rift','source_rift_id','source'),'retained','pending',NULL,0,'2026-08-13T00:00:00Z',NULL,'2026-08-13T00:00:00Z','2026-08-13T00:00:00Z')",
                [&item],
            )
            .unwrap();
        assert!(connection
            .execute(
                "UPDATE terminal_workspace_cleanup_debt SET state='preserved',reason='dirty',observation_count=1,alert_event_id='alert' WHERE item_id=?1",
                [&item],
            )
            .is_err(), "case {name}");
    }

    let temp = tempdir().unwrap();
    let db = temp.path().join("invalid-updates.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = enqueue_current_item(&queue, 0);
    queue.transition_item(&item, QueueStatus::Merging).unwrap();
    queue
        .set_workspace_intent(&item, "/workspaces/row")
        .unwrap();
    queue
        .set_workspace_identity(&item, "/workspaces/row", "rift", "source")
        .unwrap();
    drop(queue);
    let connection = Connection::open(&db).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    connection
        .execute(
            "INSERT INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,json_object('path','/workspaces/row','rift_id','rift','source_rift_id','source'),'retained','pending',NULL,0,'2026-08-13T00:00:00Z',NULL,'2026-08-13T00:00:00Z','2026-08-13T00:00:00Z')",
            [&item],
        )
        .unwrap();
    for mutation in [
        "workspace_json=json_set(workspace_json,'$.hostile',1)",
        "workspace_json=json_set(workspace_json,'$.path','')",
        "workspace_json=json_set(workspace_json,'$.rift_id','')",
        "workspace_json=json_set(workspace_json,'$.source_rift_id','')",
    ] {
        assert!(
            connection
                .execute(
                    &format!(
                        "UPDATE terminal_workspace_cleanup_debt SET {mutation} WHERE item_id=?1"
                    ),
                    [&item],
                )
                .is_err(),
            "mutation {mutation}"
        );
    }
    connection
        .execute(
            "INSERT INTO durable_events(id,item_id,event_type,payload_json,alert,created_at) VALUES('alert',?1,'terminal_workspace_preserved','{}',1,'2026-08-13T00:00:00Z')",
            [&item],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE terminal_workspace_cleanup_debt SET state='preserved',reason='dirty',observation_count=1,alert_event_id='alert' WHERE item_id=?1",
            [&item],
        )
        .unwrap();
    for mutation in ["event_type='hostile'", "alert=0"] {
        assert!(
            connection
                .execute(
                    &format!("UPDATE durable_events SET {mutation} WHERE id='alert'"),
                    [],
                )
                .is_err(),
            "event mutation {mutation}"
        );
    }
    assert!(connection
        .execute("DELETE FROM durable_events WHERE id='alert'", [])
        .is_err());
}

#[test]
fn malformed_current_rows_are_rejected_without_mutation() {
    let cases = [
        "unknown_json_key",
        "missing_json_key",
        "empty_path",
        "empty_rift_id",
        "empty_source_rift_id",
        "unknown_target_kind",
        "unknown_state",
        "unknown_reason",
        "count_overflow",
        "invalid_retry_timestamp",
        "active_parent",
        "cleaned_parent",
        "wrong_alert_item",
        "wrong_alert_type",
        "wrong_alert_flag",
    ];
    for name in cases {
        let temp = tempdir().unwrap();
        let db = temp.path().join(format!("{name}.db"));
        let queue = SqliteQueue::open(&db).unwrap();
        let item = enqueue_current_item(&queue, 0);
        let other = enqueue_current_item(&queue, 1);
        queue.transition_item(&item, QueueStatus::Merging).unwrap();
        queue
            .set_workspace_intent(&item, "/workspaces/row")
            .unwrap();
        queue
            .set_workspace_identity(&item, "/workspaces/row", "rift", "source")
            .unwrap();
        queue
            .transition_item(&item, QueueStatus::Cancelled)
            .unwrap();
        drop(queue);
        let connection = Connection::open(&db).unwrap();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON;")
            .unwrap();
        let alert_item = if name == "wrong_alert_item" {
            other.as_str()
        } else {
            item.as_str()
        };
        let alert_type = if name == "wrong_alert_type" {
            "wrong_type"
        } else {
            "terminal_workspace_preserved"
        };
        let alert_flag = if name == "wrong_alert_flag" { 0 } else { 1 };
        connection
            .execute_batch(
                "DROP TRIGGER terminal_cleanup_debt_update_guard;
                 DROP TRIGGER terminal_cleanup_debt_queue_update;
                 DROP TRIGGER terminal_cleanup_debt_cleaned;
                 DROP TRIGGER terminal_cleanup_debt_exact_target_insert;
                 DROP TRIGGER terminal_cleanup_debt_exact_target_update;
                 DROP TRIGGER terminal_cleanup_debt_alert_insert_guard;
                 DROP TRIGGER terminal_cleanup_debt_alert_update_guard;",
            )
            .unwrap();
        if name.starts_with("wrong_alert_") {
            connection
                .execute(
                    "INSERT INTO durable_events(id,item_id,event_type,payload_json,alert,created_at) VALUES('alert',?1,?2,'{}',?3,'2026-08-13T00:00:00Z')",
                    params![alert_item, alert_type, alert_flag],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE terminal_workspace_cleanup_debt SET state='preserved',reason='dirty',observation_count=1,alert_event_id='alert' WHERE item_id=?1",
                    [&item],
                )
                .unwrap();
        } else {
            let mutation = match name {
                "unknown_json_key" => "workspace_json=json_set(workspace_json,'$.extra',1)",
                "missing_json_key" => "workspace_json=json_remove(workspace_json,'$.rift_id')",
                "empty_path" => "workspace_json=json_set(workspace_json,'$.path','')",
                "empty_rift_id" => "workspace_json=json_set(workspace_json,'$.rift_id','')",
                "empty_source_rift_id" => {
                    "workspace_json=json_set(workspace_json,'$.source_rift_id','')"
                }
                "unknown_target_kind" => "target_kind='unknown'",
                "unknown_state" => "state='unknown'",
                "unknown_reason" => "state='preserved',reason='unknown',observation_count=1",
                "count_overflow" => "observation_count=256",
                "invalid_retry_timestamp" => "next_retry_at='not-a-timestamp'",
                "active_parent" => "state=state",
                "cleaned_parent" => "state=state",
                _ => unreachable!(),
            };
            connection
                .execute(
                    &format!(
                        "UPDATE terminal_workspace_cleanup_debt SET {mutation} WHERE item_id=?1"
                    ),
                    [&item],
                )
                .unwrap();
            if name == "active_parent" {
                connection
                    .execute(
                        "UPDATE queue_items SET status='merging' WHERE id=?1",
                        [&item],
                    )
                    .unwrap();
            } else if name == "cleaned_parent" {
                connection
                    .execute(
                        "UPDATE queue_items SET integration_workspace_path=NULL,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,integration_workspace_cleaned_at='2026-08-13T00:00:00Z' WHERE id=?1",
                        [&item],
                    )
                    .unwrap();
            }
        }
        iq::control_store::reinstall_cleanup_triggers_for_test(&connection).unwrap();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=OFF;")
            .unwrap();
        let before = database_snapshot(&connection);
        drop(connection);

        assert!(SqliteQueue::open(&db).is_err(), "case {name} opened");
        let connection = Connection::open(&db).unwrap();
        assert_eq!(
            database_snapshot(&connection),
            before,
            "case {name} mutated"
        );
    }
}

fn enqueue_current_item(queue: &SqliteQueue, index: usize) -> String {
    let repo_key = format!("00000000-0000-4000-8000-{index:012x}");
    register(queue, &repo_key, &format!("/repo-{index}"));
    queue
        .enqueue(EnqueueRequest {
            repo_key,
            source_branch: format!("agent/{index}"),
            current_head_sha: format!("{index:040x}"),
            pr_url: None,
            producer_metadata: serde_json::json!({"index":index}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap()
        .id
}

fn table_rows(connection: &Connection, table: &str) -> Vec<Vec<Value>> {
    let mut statement = connection
        .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY rowid"))
        .unwrap();
    let column_count = statement.column_count();
    statement
        .query_map([], |row| {
            (0..column_count)
                .map(|column| row.get(column))
                .collect::<Result<Vec<Value>, _>>()
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn database_snapshot(connection: &Connection) -> Vec<(String, Vec<Vec<Value>>)> {
    let mut names = connection
        .prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    names.insert(0, "sqlite_schema".into());
    names
        .into_iter()
        .map(|name| {
            let rows = table_rows(connection, &name);
            (name, rows)
        })
        .collect()
}

#[test]
fn replacement_cleanup_preserves_terminal_attempt_and_clears_matching_debt() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "00000000-0000-4000-8000-000000000001";
    register(&queue, repo_key, "/repo");
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/one".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue
        .claim_next_ready_control_fixture(repo_key)
        .unwrap()
        .unwrap();
    let identity = WorkspaceIdentity {
        path: "/workspaces/old".into(),
        rift_id: "old-rift".into(),
        source_rift_id: "source-rift".into(),
    };
    queue
        .set_workspace_intent(&item.id, &identity.path)
        .unwrap();
    queue
        .set_workspace_identity(
            &item.id,
            &identity.path,
            &identity.rift_id,
            &identity.source_rift_id,
        )
        .unwrap();

    let terminal_at = "2026-08-13T00:00:00Z";
    let replacement = ReplacementState::CleanupPending {
        old_attempt_id: attempt.id.clone(),
        old_workspace: identity,
    };
    let connection = Connection::open(&db).unwrap();
    connection
        .execute(
            "UPDATE integration_attempts SET result='cancelled',finished_at=?1 WHERE id=?2",
            params![terminal_at, attempt.id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET status='blocked',blocked_phase='merging',blocked_reason='needs_agent_fix',blocked_message='replace source',replacement_json=?1 WHERE id=?2",
            params![serde_json::to_string(&replacement).unwrap(), item.id],
        )
        .unwrap();
    drop(connection);
    assert!(queue.acquire_repo_lease(repo_key, owner_id, 60).unwrap());

    let cleaned = queue
        .finish_replacement_cleanup(repo_key, owner_id, &item.id, &attempt.id)
        .unwrap();

    assert_eq!(cleaned.status, QueueStatus::Ready);
    assert_eq!(cleaned.current_attempt_id, None);
    assert_eq!(cleaned.workspace, WorkspaceState::NotCreated);
    assert_eq!(cleaned.replacement, ReplacementState::None);
    assert_eq!(cleaned.blocked_phase, None);
    assert_eq!(cleaned.blocked_reason, None);
    let connection = Connection::open(&db).unwrap();
    let terminal: (String, String) = connection
        .query_row(
            "SELECT result,finished_at FROM integration_attempts WHERE id=?1",
            params![attempt.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(terminal, ("cancelled".into(), terminal_at.into()));
    let replacement_json: Option<String> = connection
        .query_row(
            "SELECT replacement_json FROM queue_items WHERE id=?1",
            params![item.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(replacement_json, None);
}

#[test]
fn replacement_cleanup_clears_stale_debt_without_reopening_integrated_item() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "00000000-0000-4000-8000-000000000001";
    register(&queue, repo_key, "/repo");
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/one".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue
        .claim_next_ready_control_fixture(repo_key)
        .unwrap()
        .unwrap();
    let identity = WorkspaceIdentity {
        path: "/workspaces/old".into(),
        rift_id: "old-rift".into(),
        source_rift_id: "source-rift".into(),
    };
    let replacement = ReplacementState::CleanupPending {
        old_attempt_id: attempt.id.clone(),
        old_workspace: identity,
    };
    let landed_sha = "2222222222222222222222222222222222222222";
    let terminal_at = "2026-08-13T00:00:00Z";
    let connection = Connection::open(&db).unwrap();
    connection
        .execute(
            "UPDATE integration_attempts SET result='integrated',finished_at=?1,landed_commit_sha=?2 WHERE id=?3",
            params![terminal_at, landed_sha, attempt.id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET status='integrated',landed_commit_sha=?1,landing_state_json=?2,integration_workspace_cleaned_at=?3,replacement_json=?4 WHERE id=?5",
            params![landed_sha, serde_json::json!({"state":"landed","candidate_sha":landed_sha,"commit_sha":landed_sha}).to_string(), terminal_at, serde_json::to_string(&replacement).unwrap(), item.id],
        )
        .unwrap();
    drop(connection);
    assert!(queue.acquire_repo_lease(repo_key, owner_id, 60).unwrap());

    let cleaned = queue
        .finish_replacement_cleanup(repo_key, owner_id, &item.id, &attempt.id)
        .unwrap();

    assert_eq!(cleaned.status, QueueStatus::Integrated);
    assert_eq!(
        cleaned.current_attempt_id.as_deref(),
        Some(attempt.id.as_str())
    );
    assert_eq!(cleaned.landed_commit_sha.as_deref(), Some(landed_sha));
    assert_eq!(cleaned.replacement, ReplacementState::None);
    let connection = Connection::open(&db).unwrap();
    let terminal: (String, String) = connection
        .query_row(
            "SELECT result,finished_at FROM integration_attempts WHERE id=?1",
            params![attempt.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(terminal, ("integrated".into(), terminal_at.into()));
}

#[test]
fn replacement_cleanup_rejects_unknown_terminal_attempt_without_clearing_debt() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "00000000-0000-4000-8000-000000000001";
    register(&queue, repo_key, "/repo");
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/one".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue
        .claim_next_ready_control_fixture(repo_key)
        .unwrap()
        .unwrap();
    let replacement = ReplacementState::CleanupPending {
        old_attempt_id: attempt.id.clone(),
        old_workspace: WorkspaceIdentity {
            path: "/workspaces/old".into(),
            rift_id: "old-rift".into(),
            source_rift_id: "source-rift".into(),
        },
    };
    let connection = Connection::open(&db).unwrap();
    connection
        .execute(
            "UPDATE integration_attempts SET result='unknown',finished_at='2026-08-13T00:00:00Z' WHERE id=?1",
            params![attempt.id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET status='blocked',blocked_phase='merging',blocked_reason='needs_agent_fix',blocked_message='replace source',replacement_json=?1 WHERE id=?2",
            params![serde_json::to_string(&replacement).unwrap(), item.id],
        )
        .unwrap();
    drop(connection);
    assert!(queue.acquire_repo_lease(repo_key, owner_id, 60).unwrap());

    assert!(queue
        .finish_replacement_cleanup(repo_key, owner_id, &item.id, &attempt.id)
        .is_err());
    let unchanged = queue.get_item(&item.id).unwrap();
    assert_eq!(unchanged.status, QueueStatus::Blocked);
    assert_eq!(unchanged.replacement, replacement);
}

#[test]
fn replacement_cleanup_rejects_partial_terminal_attempt_without_clearing_debt() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "00000000-0000-4000-8000-000000000001";
    register(&queue, repo_key, "/repo");
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/one".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue
        .claim_next_ready_control_fixture(repo_key)
        .unwrap()
        .unwrap();
    let replacement = ReplacementState::CleanupPending {
        old_attempt_id: attempt.id.clone(),
        old_workspace: WorkspaceIdentity {
            path: "/workspaces/old".into(),
            rift_id: "old-rift".into(),
            source_rift_id: "source-rift".into(),
        },
    };
    let connection = Connection::open(&db).unwrap();
    connection
        .execute(
            "UPDATE integration_attempts SET result='cancelled',finished_at=NULL WHERE id=?1",
            params![attempt.id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET status='blocked',blocked_phase='merging',blocked_reason='needs_agent_fix',blocked_message='replace source',replacement_json=?1 WHERE id=?2",
            params![serde_json::to_string(&replacement).unwrap(), item.id],
        )
        .unwrap();
    drop(connection);
    assert!(queue.acquire_repo_lease(repo_key, owner_id, 60).unwrap());

    assert!(queue
        .finish_replacement_cleanup(repo_key, owner_id, &item.id, &attempt.id)
        .is_err());
    let unchanged = queue.get_item(&item.id).unwrap();
    assert_eq!(unchanged.status, QueueStatus::Blocked);
    assert_eq!(unchanged.replacement, replacement);
}

#[test]
fn sqlite_read_fails_on_corrupt_persisted_json_fields() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    register(&queue, "00000000-0000-4000-8000-000000000001", "/repo");
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "00000000-0000-4000-8000-000000000001".into(),
            source_branch: "agent/one".into(),
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
fn sqlite_repo_lease_prevents_concurrent_integrators_until_expiry() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    register(&queue, "00000000-0000-4000-8000-000000000001", "/repo");

    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000001", "owner-a", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000001", "owner-b", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000001", "owner-a", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000002", "owner-a", -1)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000002", "owner-b", 60)
        .unwrap());
}

#[test]
fn sqlite_repo_lease_owner_check_rejects_stale_integrator() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    register(&queue, "00000000-0000-4000-8000-000000000001", "/repo");

    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000001", "owner-a", -1)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000001", "owner-b", 60)
        .unwrap());

    assert!(!queue
        .ensure_repo_lease_owner("00000000-0000-4000-8000-000000000001", "owner-a", 60)
        .unwrap());
    assert!(queue
        .ensure_repo_lease_owner("00000000-0000-4000-8000-000000000001", "owner-b", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000001", "owner-a", 60)
        .unwrap());

    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000003", "owner-a", -1)
        .unwrap());
    assert!(!queue
        .ensure_repo_lease_owner("00000000-0000-4000-8000-000000000003", "owner-a", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000003", "owner-a", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("00000000-0000-4000-8000-000000000003", "owner-b", 60)
        .unwrap());
}

#[test]
fn claim_next_ready_holds_queue_behind_oldest_active_item() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    register(&queue, "00000000-0000-4000-8000-000000000001", "/repo");
    let repo_key = "00000000-0000-4000-8000-000000000001";

    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/one".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let second = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/two".into(),
            current_head_sha: "222".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let (claimed, _) = queue
        .claim_next_ready_control_fixture(repo_key)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, first.id);
    queue
        .block_item(
            &first.id,
            BlockedPhase::Merging,
            BlockedReason::Infra,
            "resolve first before later work starts",
        )
        .unwrap();

    assert!(queue
        .claim_next_ready_control_fixture(repo_key)
        .unwrap()
        .is_none());
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
