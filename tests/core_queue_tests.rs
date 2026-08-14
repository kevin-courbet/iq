use iq::core::{BlockedPhase, BlockedReason, QueueStatus};
use iq::issue_backends::{issue_adapter_for_provider, IssueProvider, IssueSyncTarget};
use iq::providers::{provider_for_url, ProviderGate};
use iq::sqlite::{
    EnqueueRequest, ReplacementState, SqliteQueue, SqliteQueueReader, WorkspaceIdentity,
    WorkspaceState,
};
use rusqlite::{params, types::Value, Connection};
use sha2::Digest;
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, OnceLock,
};
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

static RUNTIME_HANDOFF_BLOCKED: AtomicBool = AtomicBool::new(false);

fn recorded_snapshot_path() -> &'static Mutex<Option<std::path::PathBuf>> {
    static PATH: OnceLock<Mutex<Option<std::path::PathBuf>>> = OnceLock::new();
    PATH.get_or_init(|| Mutex::new(None))
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
fn direct_v9_to_v10_backfills_exact_terminal_cleanup_debt_and_preserves_queue() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let items = (0..7)
        .map(|index| enqueue_migration_item(&queue, index))
        .collect::<Vec<_>>();
    drop(queue);

    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    connection
        .execute_batch(
            "DROP TRIGGER queue_items_workspace_state_update;
             DROP TRIGGER queue_items_landing_state_update;",
        )
        .unwrap();
    let retained = |index: usize| {
        format!(
            "UPDATE queue_items SET integration_workspace_path='/workspaces/{index}',integration_workspace_rift_id='rift-{index}',integration_workspace_source_rift_id='source-{index}' WHERE id='{}';",
            items[index]
        )
    };
    connection
        .execute_batch(&format!(
            "UPDATE queue_items SET status='cancelled',integration_workspace_path='/workspaces/intent' WHERE id='{intent}';
             UPDATE queue_items SET status='cancelled' WHERE id='{cancelled}';
             {cancelled_retained}
             UPDATE queue_items SET status='integrated',landed_commit_sha='{sha}',landing_state_json='{{\"state\":\"landed\",\"candidate_sha\":\"{sha}\",\"commit_sha\":\"{sha}\"}}' WHERE id='{integrated}';
             {integrated_retained}
             UPDATE queue_items SET status='cancelled',integration_workspace_cleaned_at='2026-08-13T00:00:00Z' WHERE id='{cleaned}';
             UPDATE queue_items SET status='cancelled' WHERE id='{not_created}';
             UPDATE queue_items SET status='merging' WHERE id='{active}';
             {active_retained}",
            intent = items[0],
            cancelled = items[1],
            cancelled_retained = retained(1),
            integrated = items[2],
            integrated_retained = retained(2),
            cleaned = items[3],
            not_created = items[4],
            active = items[5],
            active_retained = retained(5),
            sha = "1".repeat(40),
        ))
        .unwrap();
    mark_database_as_v9(&connection);
    let database_id: String = metadata_value(&connection, "database_id");
    let queue_before = table_rows(&connection, "queue_items");
    drop(connection);

    drop(SqliteQueue::migrate_v9(&db).unwrap());

    let connection = Connection::open(&db).unwrap();
    assert_eq!(
        metadata_value(&connection, "workspace_schema_version"),
        "10"
    );
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
    assert_eq!(table_rows(&connection, "queue_items"), queue_before);
    let debts = connection
        .prepare(
            "SELECT item_id,target_kind,workspace_json FROM terminal_workspace_cleanup_debt ORDER BY item_id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(2)?).unwrap(),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut expected_debts = vec![
        (
            items[0].clone(),
            "creation_intent".into(),
            serde_json::json!({"path":"/workspaces/intent"}),
        ),
        (
            items[1].clone(),
            "retained".into(),
            serde_json::json!({"path":"/workspaces/1","rift_id":"rift-1","source_rift_id":"source-1"}),
        ),
        (
            items[2].clone(),
            "retained".into(),
            serde_json::json!({"path":"/workspaces/2","rift_id":"rift-2","source_rift_id":"source-2"}),
        ),
    ];
    expected_debts.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(debts, expected_debts);
    let backups = schema_backups(temp.path(), "queues.db.schema-v9");
    assert_eq!(backups.len(), 1);
    let backup = Connection::open(&backups[0]).unwrap();
    assert_eq!(metadata_value(&backup, "workspace_schema_version"), "9");
    assert_eq!(metadata_value(&backup, "database_id"), database_id);
    assert_eq!(table_rows(&backup, "queue_items"), queue_before);
}

#[test]
fn malformed_v9_partial_identity_rolls_back_and_corrected_retry_uses_unique_backup() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = enqueue_migration_item(&queue, 0);
    drop(queue);
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    connection
        .execute_batch("DROP TRIGGER queue_items_workspace_state_update;")
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET status='cancelled',integration_workspace_path='/workspaces/partial',integration_workspace_rift_id='rift-only' WHERE id=?1",
            [&item],
        )
        .unwrap();
    mark_database_as_v9(&connection);
    let malformed = database_snapshot(&connection);
    drop(connection);

    assert!(SqliteQueue::migrate_v9(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(database_snapshot(&connection), malformed);
    connection
        .execute(
            "UPDATE queue_items SET integration_workspace_source_rift_id='source' WHERE id=?1",
            [&item],
        )
        .unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_first_v10_metadata
             BEFORE UPDATE OF value ON queue_metadata
             WHEN OLD.key='workspace_schema_version' AND NEW.value='10'
             BEGIN SELECT RAISE(ABORT,'first v10 conversion rejected'); END;",
        )
        .unwrap();
    drop(connection);

    assert!(SqliteQueue::migrate_v9(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    connection
        .execute_batch("DROP TRIGGER reject_first_v10_metadata;")
        .unwrap();
    drop(connection);
    assert_eq!(schema_backups(temp.path(), "queues.db.schema-v9").len(), 0);

    drop(SqliteQueue::migrate_v9(&db).unwrap());
    assert_eq!(schema_backups(temp.path(), "queues.db.schema-v9").len(), 1);
    let connection = Connection::open(&db).unwrap();
    assert_eq!(
        metadata_value(&connection, "workspace_schema_version"),
        "10"
    );
}

#[test]
fn top_level_v8_to_v10_migration_creates_distinct_version_backups() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v8(&connection);
    let database_id: String = metadata_value(&connection, "database_id");
    drop(connection);

    drop(SqliteQueue::migrate(&db, Some(&write_system_config(temp.path()))).unwrap());

    let connection = Connection::open(&db).unwrap();
    assert_eq!(
        metadata_value(&connection, "workspace_schema_version"),
        "10"
    );
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
    assert_eq!(schema_backups(temp.path(), "queues.db.schema-v8").len(), 1);
    assert_eq!(schema_backups(temp.path(), "queues.db.schema-v9").len(), 1);
}

#[test]
fn malformed_existing_v10_schema_is_rejected_without_repair_or_mutation() {
    let cases = [
        ("missing_table", "DROP TABLE terminal_workspace_cleanup_debt;"),
        (
            "wrong_index_order",
            "DROP INDEX terminal_workspace_cleanup_debt_due; CREATE INDEX terminal_workspace_cleanup_debt_due ON terminal_workspace_cleanup_debt(next_retry_at,state);",
        ),
        (
            "missing_index_column",
            "DROP INDEX terminal_workspace_cleanup_debt_due; CREATE INDEX terminal_workspace_cleanup_debt_due ON terminal_workspace_cleanup_debt(state);",
        ),
        (
            "missing_foreign_key",
            "DROP TABLE terminal_workspace_cleanup_debt; CREATE TABLE terminal_workspace_cleanup_debt(item_id TEXT NOT NULL PRIMARY KEY REFERENCES queue_items(id),workspace_json TEXT NOT NULL,target_kind TEXT NOT NULL,state TEXT NOT NULL,reason TEXT,observation_count INTEGER NOT NULL,next_retry_at TEXT NOT NULL,alert_event_id TEXT UNIQUE,created_at TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE INDEX terminal_workspace_cleanup_debt_due ON terminal_workspace_cleanup_debt(state,next_retry_at);",
        ),
        (
            "wrong_column_shape",
            "DROP TABLE terminal_workspace_cleanup_debt; CREATE TABLE terminal_workspace_cleanup_debt(item_id TEXT PRIMARY KEY REFERENCES queue_items(id) ON DELETE CASCADE,workspace_json BLOB NOT NULL,target_kind TEXT NOT NULL,state TEXT NOT NULL,reason TEXT,observation_count INTEGER NOT NULL,next_retry_at TEXT NOT NULL,alert_event_id TEXT UNIQUE REFERENCES durable_events(id),created_at TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE INDEX terminal_workspace_cleanup_debt_due ON terminal_workspace_cleanup_debt(state,next_retry_at);",
        ),
        ("extra_table", "CREATE TABLE hostile_table(value TEXT);"),
        (
            "extra_index",
            "CREATE INDEX hostile_index ON queue_items(updated_at);",
        ),
        (
            "extra_trigger",
            "CREATE TRIGGER hostile_trigger AFTER INSERT ON queue_events BEGIN SELECT 1; END;",
        ),
    ];
    for (name, mutation) in cases {
        let temp = tempdir().unwrap();
        let db = temp.path().join(format!("{name}.db"));
        drop(SqliteQueue::open(&db).unwrap());
        let connection = Connection::open(&db).unwrap();
        connection.execute_batch(mutation).unwrap();
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .unwrap();
        let before = database_snapshot(&connection);
        let journal_before = journal_mode(&connection);
        drop(connection);
        let files_before = directory_files(temp.path());

        assert!(SqliteQueue::open(&db).is_err(), "case {name} opened");
        let connection = Connection::open(&db).unwrap();
        assert_eq!(
            database_snapshot(&connection),
            before,
            "case {name} mutated"
        );
        assert_eq!(journal_mode(&connection), journal_before);
        drop(connection);
        assert_eq!(directory_files(temp.path()), files_before);
    }
}

#[test]
fn malformed_existing_v10_wal_is_rejected_without_authoritative_directory_mutation() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    connection
        .pragma_update(None, "wal_autocheckpoint", 0)
        .unwrap();
    connection
        .execute(
            "UPDATE queue_metadata SET value='malformed' WHERE key='workspace_schema_version'",
            [],
        )
        .unwrap();
    assert!(fs::metadata(db.with_extension("db-wal")).unwrap().len() > 32);
    let mut lock_name = db.file_name().unwrap().to_os_string();
    lock_name.push(".control.lock");
    fs::remove_file(db.with_file_name(lock_name)).unwrap();
    let before = authoritative_directory_state(temp.path());

    assert!(SqliteQueue::open(&db).is_err());

    assert_eq!(authoritative_directory_state(temp.path()), before);
    drop(connection);
}

#[test]
fn valid_existing_v10_wal_validation_uses_committed_wal_content() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    connection
        .pragma_update(None, "wal_autocheckpoint", 0)
        .unwrap();
    connection
        .execute(
            "UPDATE queue_metadata SET value='9' WHERE key='workspace_schema_version'",
            [],
        )
        .unwrap();
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    connection
        .execute(
            "UPDATE queue_metadata SET value='10' WHERE key='workspace_schema_version'",
            [],
        )
        .unwrap();
    assert!(fs::metadata(db.with_extension("db-wal")).unwrap().len() > 32);
    let immutable = Connection::open_with_flags(
        format!("file:{}?immutable=1", db.display()),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap();
    assert_eq!(metadata_value(&immutable, "workspace_schema_version"), "9");
    drop(immutable);

    drop(SqliteQueue::open(&db).unwrap());
    drop(connection);
}

#[test]
fn migration_backup_validation_leaves_no_wal_or_shm_sidecars() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    drop(connection);

    drop(SqliteQueue::migrate_v9(&db).unwrap());

    let backups = schema_backups(temp.path(), "queues.db.schema-v9");
    assert_eq!(backups.len(), 1);
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = backups[0].as_os_str().to_os_string();
        sidecar.push(suffix);
        assert!(!std::path::Path::new(&sidecar).exists());
    }
}

#[test]
fn schema_literal_case_change_is_rejected_without_mutation() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("literal.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    let trigger_sql: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='terminal_cleanup_debt_insert_guard'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let hostile_sql = trigger_sql.replacen("'retained'", "'RETAINED'", 1);
    assert_ne!(hostile_sql, trigger_sql);
    connection
        .execute_batch("DROP TRIGGER terminal_cleanup_debt_insert_guard;")
        .unwrap();
    connection.execute_batch(&hostile_sql).unwrap();
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    let before = database_snapshot(&connection);
    let journal_before = journal_mode(&connection);
    drop(connection);
    let files_before = directory_files(temp.path());

    assert!(SqliteQueue::open(&db).is_err());

    let connection = Connection::open(&db).unwrap();
    assert_eq!(database_snapshot(&connection), before);
    assert_eq!(journal_mode(&connection), journal_before);
    drop(connection);
    assert_eq!(directory_files(temp.path()), files_before);
}

#[test]
fn schema_escaped_literal_whitespace_change_is_rejected() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("escaped-literal.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER terminal_cleanup_debt_delete_guard;
             CREATE TRIGGER terminal_cleanup_debt_delete_guard
             BEFORE DELETE ON terminal_workspace_cleanup_debt
             WHEN EXISTS (
               SELECT 1 FROM queue_items item WHERE item.id=OLD.item_id
                 AND item.status IN ('integrated','cancelled')
                 AND item.integration_workspace_cleaned_at IS NULL
                 AND item.integration_workspace_path IS NOT NULL
             )
             BEGIN
               SELECT RAISE(ABORT,'terminal queue  workspace requires cleanup debt''s authority');
             END;",
        )
        .unwrap();
    let before = database_snapshot(&connection);
    drop(connection);

    assert!(SqliteQueue::open(&db).is_err());
    assert_eq!(database_snapshot(&Connection::open(&db).unwrap()), before);
}

#[test]
fn v9_missing_notification_deliveries_is_rejected_without_recreation() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    connection
        .execute_batch("DROP TABLE notification_deliveries;")
        .unwrap();
    let before = database_snapshot(&connection);
    drop(connection);

    assert!(SqliteQueue::migrate_v9(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(database_snapshot(&connection), before);
}

#[test]
fn v10_missing_required_non_debt_trigger_is_rejected_without_recreation() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    connection
        .execute_batch("DROP TRIGGER queue_items_landing_state_update;")
        .unwrap();
    let before = database_snapshot(&connection);
    drop(connection);

    assert!(SqliteQueue::open(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(database_snapshot(&connection), before);
}

#[test]
fn v10_missing_required_non_debt_table_is_rejected_without_recreation() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    connection
        .execute_batch("DROP TABLE notification_deliveries;")
        .unwrap();
    let before = database_snapshot(&connection);
    drop(connection);

    assert!(SqliteQueue::open(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(database_snapshot(&connection), before);
}

#[test]
fn v8_missing_required_table_index_or_trigger_is_rejected_without_migration() {
    let cases = [
        ("table", "DROP TABLE queue_events;"),
        ("index", "DROP INDEX queue_items_active_identity;"),
        (
            "trigger",
            "DROP TRIGGER queue_items_workspace_state_update;",
        ),
    ];
    for (name, mutation) in cases {
        let temp = tempdir().unwrap();
        let db = temp.path().join(format!("v8-missing-{name}.db"));
        let connection = Connection::open(&db).unwrap();
        iq::sqlite::initialize_test_schema(&connection, "8").unwrap();
        connection.execute_batch(mutation).unwrap();
        let before = database_snapshot(&connection);
        drop(connection);

        assert!(SqliteQueue::migrate_v8(&db, &write_system_config(temp.path())).is_err());
        let connection = Connection::open(&db).unwrap();
        assert_eq!(metadata_value(&connection, "workspace_schema_version"), "8");
        assert_eq!(database_snapshot(&connection), before);
    }
}

#[test]
fn malformed_v9_source_index_is_rejected_without_migration() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    connection
        .execute_batch(
            "DROP INDEX one_original_notification_delivery;
             CREATE UNIQUE INDEX one_original_notification_delivery ON notification_deliveries(event_id) WHERE redelivery_of IS NULL;",
        )
        .unwrap();
    let before = database_snapshot(&connection);
    drop(connection);

    assert!(SqliteQueue::migrate_v9(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(database_snapshot(&connection), before);
}

#[test]
fn v10_cleanup_table_without_checks_is_rejected() {
    assert_malformed_cleanup_table_rejected(
        "without-checks",
        "item_id TEXT NOT NULL PRIMARY KEY REFERENCES queue_items(id) ON DELETE CASCADE,workspace_json TEXT NOT NULL,target_kind TEXT NOT NULL,state TEXT NOT NULL,reason TEXT,observation_count INTEGER NOT NULL,next_retry_at TEXT NOT NULL,alert_event_id TEXT UNIQUE REFERENCES durable_events(id),created_at TEXT NOT NULL,updated_at TEXT NOT NULL",
    );
}

#[test]
fn v10_cleanup_table_without_alert_unique_is_rejected() {
    assert_malformed_cleanup_table_rejected(
        "without-alert-unique",
        "item_id TEXT NOT NULL PRIMARY KEY REFERENCES queue_items(id) ON DELETE CASCADE,workspace_json TEXT NOT NULL CHECK(json_valid(workspace_json) AND json_type(workspace_json,'$.path')='text' AND (target_kind='creation_intent' OR (json_type(workspace_json,'$.rift_id')='text' AND json_type(workspace_json,'$.source_rift_id')='text'))),target_kind TEXT NOT NULL CHECK(target_kind IN ('creation_intent','retained')),state TEXT NOT NULL CHECK(state IN ('pending','preserved')),reason TEXT CHECK(reason IN ('dirty','active_git_operation','both') OR reason IS NULL),observation_count INTEGER NOT NULL CHECK(observation_count BETWEEN 0 AND 255),next_retry_at TEXT NOT NULL,alert_event_id TEXT REFERENCES durable_events(id),created_at TEXT NOT NULL,updated_at TEXT NOT NULL,CHECK((state='pending' AND reason IS NULL AND observation_count=0 AND alert_event_id IS NULL) OR (state='preserved' AND reason IS NOT NULL AND observation_count>0 AND alert_event_id IS NOT NULL))",
    );
}

#[test]
fn old_style_terminal_write_after_v10_migration_fails_at_sql_boundary() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = enqueue_migration_item(&queue, 0);
    drop(queue);
    let old_connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&old_connection);
    drop(SqliteQueue::migrate_v9(&db).unwrap());

    let changed = old_connection.execute(
        "UPDATE queue_items SET status='cancelled',integration_workspace_path='/old-writer' WHERE id=?1",
        [&item],
    );
    assert!(changed.is_err());
    drop(old_connection);
    drop(SqliteQueue::open(&db).unwrap());
}

#[test]
fn database_id_changing_version_trigger_rolls_back_migration() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    let database_id = metadata_value(&connection, "database_id");
    connection
        .execute_batch(
            "CREATE TRIGGER change_database_id_during_v10
             AFTER UPDATE OF value ON queue_metadata
             WHEN OLD.key='workspace_schema_version' AND NEW.value='10'
             BEGIN UPDATE queue_metadata SET value='changed' WHERE key='database_id'; END;",
        )
        .unwrap();
    drop(connection);

    assert!(SqliteQueue::migrate_v9(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
}

#[test]
fn backup_same_bytes_replacement_inode_is_rejected_before_commit() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    let database_id = metadata_value(&connection, "database_id");
    drop(connection);
    iq::control_store::set_migration_backup_test_hook(&db, Some(replace_backup_inode));

    assert!(SqliteQueue::migrate_v9(&db).is_err());
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
}

#[test]
fn backup_path_replacement_after_validation_rolls_back_before_v10_commit() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    let database_id = metadata_value(&connection, "database_id");
    drop(connection);
    iq::control_store::set_migration_backup_precommit_test_hook(&db, Some(replace_backup_inode));

    assert!(SqliteQueue::migrate_v9(&db).is_err());

    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='terminal_workspace_cleanup_debt'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn backup_sidecar_creation_after_validation_rolls_back_before_v10_commit() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    let database_id = metadata_value(&connection, "database_id");
    drop(connection);
    iq::control_store::set_migration_backup_precommit_test_hook(
        &db,
        Some(create_backup_wal_sidecar),
    );

    assert!(SqliteQueue::migrate_v9(&db).is_err());

    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='terminal_workspace_cleanup_debt'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn runtime_open_handoff_blocks_extra_schema_wal_commit() {
    let _guard = env_lock().lock().unwrap();
    for reader in [false, true] {
        let temp = tempdir().unwrap();
        let db = temp.path().join("queues.db");
        let queue = SqliteQueue::open(&db).unwrap();
        let item = enqueue_migration_item(&queue, 0);
        drop(queue);
        RUNTIME_HANDOFF_BLOCKED.store(false, Ordering::SeqCst);
        iq::control_store::set_runtime_open_handoff_test_hook(
            &db,
            Some(attempt_extra_schema_wal_commit),
        );

        if reader {
            let opened = SqliteQueueReader::open(&db).unwrap();
            assert_eq!(opened.get_item(&item).unwrap().id, item);
        } else {
            let opened = SqliteQueue::open(&db).unwrap();
            assert_eq!(opened.get_item(&item).unwrap().id, item);
        }

        assert!(RUNTIME_HANDOFF_BLOCKED.load(Ordering::SeqCst));
        let connection = Connection::open(&db).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name='hostile_handoff'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        drop(connection);

        let writer = SqliteQueue::open(&db).unwrap();
        let reader = SqliteQueueReader::open(&db).unwrap();
        let _writer_lease = iq::control_store::DatabaseProcessLease::acquire(&db).unwrap();
        let _reader_lease = iq::control_store::DatabaseProcessLease::acquire(&db).unwrap();
        let concurrent_item = enqueue_migration_item(&writer, 1);
        assert_eq!(
            reader.get_item(&concurrent_item).unwrap().id,
            concurrent_item
        );
    }
}

#[test]
fn private_validation_snapshots_are_removed_after_success_and_failure() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());

    for malformed in [false, true] {
        if malformed {
            Connection::open(&db)
                .unwrap()
                .execute_batch("CREATE TABLE hostile_snapshot(value TEXT);")
                .unwrap();
        }
        *recorded_snapshot_path().lock().unwrap() = None;
        iq::control_store::set_database_snapshot_test_hook(&db, Some(record_snapshot_path));

        let result = SqliteQueueReader::open(&db);

        assert_eq!(result.is_err(), malformed);
        let snapshot = recorded_snapshot_path().lock().unwrap().take().unwrap();
        assert!(!snapshot.exists());
    }
}

#[test]
fn primary_database_path_replacement_is_rejected_before_v9_commit() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    let source = fs::metadata(&db).unwrap();
    let database_id = metadata_value(&connection, "database_id");
    drop(connection);
    iq::control_store::set_migration_primary_test_hook(&db, Some(replace_primary_inode));

    assert!(SqliteQueue::migrate_v9(&db).is_err());

    let replaced = fs::metadata(&db).unwrap();
    assert_ne!(
        (source.dev(), source.ino()),
        (replaced.dev(), replaced.ino())
    );
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "9");
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
}

#[test]
fn primary_database_path_replacement_is_rejected_before_v8_commit() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v8(&connection);
    let source = fs::metadata(&db).unwrap();
    let database_id = metadata_value(&connection, "database_id");
    drop(connection);
    iq::control_store::set_migration_primary_test_hook(&db, Some(replace_primary_inode));

    assert!(SqliteQueue::migrate_v8(&db, &write_system_config(temp.path())).is_err());

    let replaced = fs::metadata(&db).unwrap();
    assert_ne!(
        (source.dev(), source.ino()),
        (replaced.dev(), replaced.ino())
    );
    let connection = Connection::open(&db).unwrap();
    assert_eq!(metadata_value(&connection, "workspace_schema_version"), "8");
    assert_eq!(metadata_value(&connection, "database_id"), database_id);
}

fn assert_malformed_cleanup_table_rejected(name: &str, columns: &str) {
    let temp = tempdir().unwrap();
    let db = temp.path().join(format!("{name}.db"));
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    connection
        .execute_batch("DROP TABLE terminal_workspace_cleanup_debt;")
        .unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TABLE terminal_workspace_cleanup_debt({columns}); CREATE INDEX terminal_workspace_cleanup_debt_due ON terminal_workspace_cleanup_debt(state,next_retry_at);"
        ))
        .unwrap();
    let before = database_snapshot(&connection);
    drop(connection);
    assert!(SqliteQueue::open(&db).is_err());
    assert_eq!(database_snapshot(&Connection::open(&db).unwrap()), before);
}

fn replace_backup_inode(path: &std::path::Path) {
    let original = fs::metadata(path).unwrap();
    let replacement = path.with_extension("replacement");
    fs::copy(path, &replacement).unwrap();
    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
    fs::rename(&replacement, path).unwrap();
    let replaced = fs::metadata(path).unwrap();
    assert_ne!(
        (original.dev(), original.ino()),
        (replaced.dev(), replaced.ino())
    );
}

fn create_backup_wal_sidecar(path: &std::path::Path) {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push("-wal");
    fs::write(std::path::PathBuf::from(sidecar), b"hostile wal").unwrap();
}

fn attempt_extra_schema_wal_commit(path: &std::path::Path) {
    let lease = match iq::control_store::DatabaseProcessLease::acquire(path) {
        Ok(lease) => lease,
        Err(error) => {
            assert_eq!(
                error
                    .downcast_ref::<std::io::Error>()
                    .and_then(std::io::Error::raw_os_error),
                Some(libc::EWOULDBLOCK)
            );
            RUNTIME_HANDOFF_BLOCKED.store(true, Ordering::SeqCst);
            return;
        }
    };
    let _lease = lease;
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch("CREATE TABLE hostile_handoff(value TEXT);")
        .unwrap();
}

fn record_snapshot_path(path: &std::path::Path) {
    *recorded_snapshot_path().lock().unwrap() = Some(path.to_path_buf());
}

fn replace_primary_inode(path: &std::path::Path) {
    let replacement = path.with_extension("replacement");
    fs::copy(path, &replacement).unwrap();
    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
    fs::rename(&replacement, path).unwrap();
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
        let item = enqueue_migration_item(&queue, 0);
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
        let item = enqueue_migration_item(&queue, 0);
        let other = enqueue_migration_item(&queue, 1);
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
    let item = enqueue_migration_item(&queue, 0);
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
fn non_utf8_database_migration_and_current_writer_share_one_raw_byte_fence() {
    let temp = tempdir().unwrap();
    let mut name = b"queue-".to_vec();
    name.push(0xff);
    name.extend_from_slice(b".db");
    let db = temp.path().join(std::ffi::OsString::from_vec(name));
    drop(SqliteQueue::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    mark_database_as_v9(&connection);
    drop(connection);
    let writer_fence = iq::control_store::DatabaseProcessLease::acquire(&db).unwrap();

    assert!(SqliteQueue::migrate_v9(&db).is_err());
    assert_eq!(
        metadata_value(&Connection::open(&db).unwrap(), "workspace_schema_version"),
        "9"
    );
    drop(writer_fence);

    drop(SqliteQueue::migrate_v9(&db).unwrap());
    assert_eq!(
        metadata_value(&Connection::open(&db).unwrap(), "workspace_schema_version"),
        "10"
    );
}

#[test]
fn malformed_existing_v10_rows_are_rejected_without_mutation() {
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
        let item = enqueue_migration_item(&queue, 0);
        let other = enqueue_migration_item(&queue, 1);
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
        iq::control_store::reinstall_v10_triggers_for_test(&connection).unwrap();
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

fn enqueue_migration_item(queue: &SqliteQueue, index: usize) -> String {
    queue
        .enqueue(EnqueueRequest {
            repo_key: format!("repo-{index}::main"),
            repo_path: format!("/repo-{index}"),
            source_branch: format!("agent/{index}"),
            target_branch: "main".into(),
            current_head_sha: format!("{index:040x}"),
            pr_url: None,
            producer_metadata: serde_json::json!({"index":index}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap()
        .id
}

fn mark_database_as_v9(connection: &Connection) {
    iq::sqlite::force_test_schema_version(connection, "9").unwrap();
}

fn metadata_value(connection: &Connection, key: &str) -> String {
    connection
        .query_row(
            "SELECT value FROM queue_metadata WHERE key=?1",
            [key],
            |row| row.get(0),
        )
        .unwrap()
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

fn journal_mode(connection: &Connection) -> String {
    connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap()
}

fn directory_files(path: &std::path::Path) -> Vec<std::ffi::OsString> {
    let mut files = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[derive(Debug, Eq, PartialEq)]
struct AuthoritativeEntryState {
    mode: u32,
    uid: u32,
    gid: u32,
    device: u64,
    inode: u64,
    links: u64,
    size: u64,
    accessed_seconds: i64,
    accessed_nanoseconds: i64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
    sha256: String,
}

fn authoritative_directory_state(
    path: &std::path::Path,
) -> BTreeMap<std::ffi::OsString, AuthoritativeEntryState> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let entry_path = entry.path();
            let metadata = fs::symlink_metadata(&entry_path).unwrap();
            assert!(
                metadata.is_file(),
                "unexpected entry {}",
                entry_path.display()
            );
            let mut file = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NOATIME)
                .open(&entry_path)
                .unwrap();
            let mut digest = sha2::Sha256::new();
            std::io::copy(&mut file, &mut digest).unwrap();
            (
                entry.file_name(),
                AuthoritativeEntryState {
                    mode: metadata.mode(),
                    uid: metadata.uid(),
                    gid: metadata.gid(),
                    device: metadata.dev(),
                    inode: metadata.ino(),
                    links: metadata.nlink(),
                    size: metadata.len(),
                    accessed_seconds: metadata.atime(),
                    accessed_nanoseconds: metadata.atime_nsec(),
                    modified_seconds: metadata.mtime(),
                    modified_nanoseconds: metadata.mtime_nsec(),
                    changed_seconds: metadata.ctime(),
                    changed_nanoseconds: metadata.ctime_nsec(),
                    sha256: format!("{:x}", digest.finalize()),
                },
            )
        })
        .collect()
}

#[test]
fn replacement_cleanup_preserves_terminal_attempt_and_clears_matching_debt() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "repo::main";
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue.claim_next_ready(repo_key).unwrap().unwrap();
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
    let repo_key = "repo::main";
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue.claim_next_ready(repo_key).unwrap().unwrap();
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
    let repo_key = "repo::main";
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue.claim_next_ready(repo_key).unwrap().unwrap();
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
    let repo_key = "repo::main";
    let owner_id = "owner";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, attempt) = queue.claim_next_ready(repo_key).unwrap().unwrap();
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
fn schema_v8_to_v10_migration_preserves_terminal_attempt_policy_as_opaque_history() {
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
    assert_eq!(version, "10");
    assert_eq!(schema_backups(temp.path(), "queues.db.schema-v8").len(), 1);
}

#[test]
fn schema_v8_to_v10_migration_enables_read_only_queue_access() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let item_id = "terminal-v8-item";
    let attempt_id = "terminal-v8-attempt";
    let connection = Connection::open(&db).unwrap();
    iq::sqlite::initialize_test_schema(&connection, "8").unwrap();
    connection
        .execute(
            "INSERT INTO queue_items(id,repo_key,repo_path,source_branch,target_branch,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,landing_state_json,source_kind,source_ref,landing_policy,created_at,updated_at) VALUES(?1,'fixture::main','/repo','agent/terminal','main','{}','[]','cancelled','111',?2,'{\"state\":\"ready\"}','remote_branch','agent/terminal','direct','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            params![item_id, attempt_id],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO integration_attempts(id,item_id,attempt_number,source_head_sha,started_at) VALUES(?1,?2,1,'111','2026-01-01T00:00:00Z')",
            params![attempt_id, item_id],
        )
        .unwrap();
    drop(connection);

    let error = match SqliteQueueReader::open(&db) {
        Ok(_) => panic!("schema v8 reader opened without migration"),
        Err(error) => format!("{error:#}"),
    };
    assert!(
        error.contains(
            "IQ schema version 8 requires explicit migration with a verified system configuration path"
        ),
        "{error}"
    );

    let system_config = write_system_config(temp.path());
    drop(SqliteQueue::migrate_v8(&db, &system_config).unwrap());
    let reader = SqliteQueueReader::open(&db).unwrap();

    let items = reader.list_items().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, item_id);
    assert_eq!(reader.get_item(item_id).unwrap().id, item_id);
    assert_eq!(reader.get_attempt(attempt_id).unwrap().id, attempt_id);
}

#[test]
fn schema_v8_to_v10_migration_preserves_unclaimed_ready_items_without_efforts() {
    let temp = tempdir().unwrap();
    let repository = temp.path().join("repo");
    fs::create_dir(&repository).unwrap();
    iq::integrator::git(&repository, ["init"]).unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: repository.to_string_lossy().into_owned(),
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

    let migrated = SqliteQueue::migrate_v8(&db, &system_config).unwrap();
    let migrated_item = migrated.get_item(&item.id).unwrap();
    assert_eq!(migrated_item.status, QueueStatus::Ready);
    assert!(migrated_item.current_attempt_id.is_none());
    assert!(iq::control_store::ControlStore::open(&db)
        .unwrap()
        .effort_for_item(&item.id)
        .unwrap()
        .is_none());
    let connection = Connection::open(&db).unwrap();
    let version: String = connection
        .query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "10");
}

#[test]
fn schema_v8_to_v10_migration_rejects_active_repository_leases() {
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
    iq::sqlite::force_test_schema_version(connection, "8").unwrap();
}

fn schema_backups(root: &std::path::Path, prefix: &str) -> Vec<std::path::PathBuf> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(prefix) && name.ends_with(".backup"))
        })
        .collect()
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
