use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use iq::control_domain::{CompositionEvidence, IntegrationEffortState};
use iq::core::QueueStatus;
use iq::repository_policy::{
    GitRepository, IntegrationPolicy, OperationState, ReplicationPolicy, RepositoryPolicy,
};
use iq::sqlite::{LandingState, SqliteQueue};
use rusqlite::{params, Connection};
use tempfile::{tempdir, TempDir};

const REPOSITORY_KEY: &str = "00000000-0000-4000-8000-000000000001";
const TARGET_SHA: &str = "1111111111111111111111111111111111111111";
const SOURCE_SHA: &str = "2222222222222222222222222222222222222222";
const CANDIDATE_SHA: &str = "3333333333333333333333333333333333333333";

struct Schema5Fixture {
    _temp: TempDir,
    database: PathBuf,
}

#[derive(Clone, Copy)]
enum Schema5EffortState {
    CandidateReady,
    Landing,
    LandingUncertain,
    Integrated,
}

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/schema5-db0dd7d.db")
}

fn file_sha256(path: &Path) -> String {
    format!("{:x}", Sha256::digest(fs::read(path).unwrap()))
}

fn seeded_schema5_fixture() -> Schema5Fixture {
    seeded_schema5_fixture_with_state(Schema5EffortState::CandidateReady)
}

fn seeded_schema5_fixture_with_state(effort_state: Schema5EffortState) -> Schema5Fixture {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    fs::copy(fixture_path(), &database).unwrap();
    fs::set_permissions(&database, fs::Permissions::from_mode(0o600)).unwrap();
    let reservation = temp.path().join("repositories").join(REPOSITORY_KEY);
    fs::create_dir_all(&reservation).unwrap();
    let root = reservation.join("root");
    assert!(Command::new("/usr/bin/git")
        .args(["init", "--bare", root.to_str().unwrap()])
        .status()
        .unwrap()
        .success());
    let root_metadata = fs::metadata(&root).unwrap();
    let canonical = GitRepository::LocalBare {
        path: root.clone(),
        device: root_metadata.dev(),
        inode: root_metadata.ino(),
        object_format: iq::git_object::GitObjectFormat::Sha1,
    };
    let policy = RepositoryPolicy {
        operation_state: OperationState::Enabled,
        canonical_repository: canonical.clone(),
        target_branch: "main".into(),
        integration_policy: IntegrationPolicy::Direct,
        replication_policy: ReplicationPolicy::None,
    };
    let ownership_key = canonical.canonical_ownership_key().unwrap();
    let development = reservation.join("development");
    let integration = reservation.join("integration");
    fs::create_dir(&development).unwrap();
    fs::create_dir(&integration).unwrap();
    let workspace_path = integration.join("candidate");
    assert!(Command::new("/usr/bin/git")
        .args(["init", workspace_path.to_str().unwrap()])
        .status()
        .unwrap()
        .success());
    let registry = temp.path().join("rift.sqlite");
    fs::write(&registry, b"schema-5 registry identity\n").unwrap();
    let registry_metadata = fs::metadata(&registry).unwrap();
    let binding = iq::git_command::RepositoryBinding::capture(&root).unwrap();
    let workspace_binding = iq::git_command::RepositoryBinding::capture(&workspace_path).unwrap();
    let workspace = serde_json::json!({
        "path": workspace_path.clone(),
        "rift_id": "INTEGRATIONRIFT000000001",
        "source_rift_id": "ROOTRIFT000000000000000001",
    });
    let runner = serde_json::json!({
        "kind": "opencode",
        "executable": {"path":"/bin/true","device":1,"inode":1,"sha256":"a".repeat(64)},
        "agent": "iq-integration",
        "model": "fixture/model",
        "cycle_timeout_seconds": 60,
        "bounds": {"max_log_bytes":4096,"max_result_bytes":4096,"max_processes":4,"memory_bytes":67108864,"cpu_seconds":60,"writable_bytes":1048576,"open_files":64},
        "sandbox": {
            "implementation":"fixture",
            "bubblewrap":{"path":"/bin/true","device":1,"inode":1,"sha256":"b".repeat(64)},
            "unshare":{"path":"/bin/true","device":1,"inode":1,"sha256":"c".repeat(64)},
            "systemd_run":{"path":"/bin/true","device":1,"inode":1,"sha256":"d".repeat(64)},
            "systemctl":{"path":"/bin/true","device":1,"inode":1,"sha256":"e".repeat(64)}
        },
        "credential_env":"FIXTURE_MODEL_KEY"
    });
    let (state_name, state, item_status, landing_state, landed_commit_sha) = match effort_state {
        Schema5EffortState::CandidateReady => (
            "candidate_ready",
            serde_json::json!({
                "state":"candidate_ready",
                "payload":{
                    "operation_id":"builder-1",
                    "cycle_id":"cycle-1",
                    "candidate_sha":CANDIDATE_SHA,
                    "staged_tree_sha256":"f".repeat(64)
                }
            }),
            "merged",
            serde_json::json!({"state":"ready"}),
            None,
        ),
        Schema5EffortState::Landing => (
            "landing",
            serde_json::json!({
                "state":"landing",
                "payload":{
                    "candidate_sha":CANDIDATE_SHA,
                    "expected_target_sha":TARGET_SHA,
                    "lease_id":"landing-lease-1",
                    "signoff":{
                        "kind":"no_validation",
                        "policy_digest":"a".repeat(64)
                    }
                }
            }),
            "integrating",
            serde_json::json!({"state":"ready"}),
            None,
        ),
        Schema5EffortState::LandingUncertain => (
            "landing_uncertain",
            serde_json::json!({
                "state":"landing_uncertain",
                "payload":{
                    "candidate_sha":CANDIDATE_SHA,
                    "expected_target_sha":TARGET_SHA,
                    "command_id":"landing-command-1",
                    "evidence":"command_gate_released"
                }
            }),
            "integrating",
            serde_json::json!({
                "state":"uncertain",
                "candidate_sha":CANDIDATE_SHA,
                "expected_target_sha":TARGET_SHA
            }),
            None,
        ),
        Schema5EffortState::Integrated => (
            "integrated",
            serde_json::json!({
                "state":"integrated",
                "payload":{
                    "candidate_sha":CANDIDATE_SHA,
                    "landed_sha":CANDIDATE_SHA,
                    "attempt_id":"attempt-1",
                    "event_id":"event-integrated-1"
                }
            }),
            "integrated",
            serde_json::json!({
                "state":"landed",
                "candidate_sha":CANDIDATE_SHA,
                "commit_sha":CANDIDATE_SHA
            }),
            Some(CANDIDATE_SHA),
        ),
    };
    let mut connection = Connection::open(&database).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    let transaction = connection.transaction().unwrap();
    transaction.execute(
        "INSERT INTO repository_policies(repo_key,revision,operation_state_json,canonical_repository_json,canonical_ownership_key,target_branch,integration_policy,replication_policy_json,created_at,updated_at) VALUES(?1,1,?2,?3,?4,'main','direct',?5,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        params![REPOSITORY_KEY,serde_json::to_string(&policy.operation_state).unwrap(),serde_json::to_string(&canonical).unwrap(),ownership_key,serde_json::to_string(&policy.replication_policy).unwrap()],
    ).unwrap();
    transaction.execute(
        "INSERT INTO physical_repository_ownership(identity_key,repo_key,role,ordinal,repository_json,created_at) VALUES(?1,?2,'canonical',0,?3,'2026-01-01T00:00:00Z')",
        params![ownership_key,REPOSITORY_KEY,serde_json::to_string(&canonical).unwrap()],
    ).unwrap();
    transaction.execute(
        "INSERT INTO registered_repositories(repo_key,owned_root_path,git_binding_json,root_rift_id,registry_identity,registry_device,registry_inode,generation,source_sha,checkout_json,development_root_path,integration_root_path,provisioning_json,created_at,updated_at) VALUES(?1,?2,?3,'ROOTRIFT000000000000000001',?4,?5,?6,0,?7,?8,?9,?10,'{\"state\":\"ready\"}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        params![REPOSITORY_KEY,root.as_os_str().as_encoded_bytes(),serde_json::to_string(&binding).unwrap(),registry.as_os_str().as_encoded_bytes(),registry_metadata.dev(),registry_metadata.ino(),TARGET_SHA,serde_json::json!({"state":"ready","target_sha":TARGET_SHA}).to_string(),development.as_os_str().as_encoded_bytes(),integration.as_os_str().as_encoded_bytes()],
    ).unwrap();
    for (kind, path) in [("development", &development), ("integration", &integration)] {
        transaction.execute(
            "INSERT INTO workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode,generation) VALUES(?1,?2,?3,?4,'ROOTRIFT000000000000000001',?5,?6,?7,0)",
            params![REPOSITORY_KEY,kind,path.as_os_str().as_encoded_bytes(),root.as_os_str().as_encoded_bytes(),registry.as_os_str().as_encoded_bytes(),registry_metadata.dev(),registry_metadata.ino()],
        ).unwrap();
    }
    transaction.execute(
        "INSERT INTO queue_items(id,repo_key,producer_metadata_json,validation_evidence_json,status,current_attempt_id,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,target_sha,source_sha,landed_commit_sha,landing_state_json,created_at,updated_at) VALUES('item-1',?1,'{\"fixture\":\"schema5\"}','[]',?2,'attempt-1',?3,'INTEGRATIONRIFT000000001','ROOTRIFT000000000000000001',?4,?5,?6,?7,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        params![REPOSITORY_KEY,item_status,workspace_path.to_str().unwrap(),TARGET_SHA,SOURCE_SHA,landed_commit_sha,landing_state.to_string()],
    ).unwrap();
    transaction.execute(
        "INSERT INTO workspace_git_bindings(owner_kind,owner_id,top_level,binding_json,created_at) VALUES('integration','item-1',?1,?2,'2026-01-01T00:00:00Z')",
        params![workspace_path.as_os_str().as_encoded_bytes(),serde_json::to_string(&workspace_binding).unwrap()],
    ).unwrap();
    transaction.execute(
        "INSERT INTO queue_admissions(item_id,kind,source_branch,head_sha,admitted_at) VALUES('item-1','direct','agent/schema5',?1,'2026-01-01T00:00:00Z')",
        [SOURCE_SHA],
    ).unwrap();
    transaction.execute(
        "INSERT INTO integration_attempts(id,item_id,attempt_number,source_head_sha,target_base_sha,merge_commit_sha,started_at) VALUES('attempt-1','item-1',1,?1,?2,?3,'2026-01-01T00:00:00Z')",
        params![SOURCE_SHA,TARGET_SHA,CANDIDATE_SHA],
    ).unwrap();
    transaction.execute(
        "INSERT INTO integration_efforts(id,item_id,attempt_id,target_sha,source_sha,source_variant,landing_variant,workspace_json,runner_snapshot_json,state_repository_json,failed_cycles,state,state_json,created_at,updated_at) VALUES('effort-1','item-1','attempt-1',?1,?2,'remote_branch','direct',?3,?4,'{\"kind\":\"local\"}',0,?5,?6,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        params![TARGET_SHA,SOURCE_SHA,workspace.to_string(),runner.to_string(),state_name,state.to_string()],
    ).unwrap();
    transaction.execute(
        "INSERT INTO integration_cycles(id,effort_id,cycle_number,status,created_at,finished_at) VALUES('cycle-1','effort-1',1,'resolved','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z')",
        [],
    ).unwrap();
    transaction.execute(
        "INSERT INTO candidate_evidence(effort_id,cycle_id,candidate_sha,builder_operation_id,created_at) VALUES('effort-1','cycle-1',?1,'builder-1','2026-01-01T00:00:01Z')",
        [CANDIDATE_SHA],
    ).unwrap();
    if matches!(effort_state, Schema5EffortState::Integrated) {
        transaction
            .execute(
                "UPDATE integration_attempts SET landed_commit_sha=?1,finished_at='2026-01-01T00:00:02Z',result='integrated' WHERE id='attempt-1'",
                [CANDIDATE_SHA],
            )
            .unwrap();
        transaction
            .execute(
                "UPDATE queue_items SET integration_workspace_path=NULL,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,integration_workspace_cleaned_at='2026-01-01T00:00:02Z' WHERE id='item-1'",
                [],
            )
            .unwrap();
        transaction
            .execute(
                "DELETE FROM workspace_git_bindings WHERE owner_id='item-1'",
                [],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);
    Schema5Fixture {
        _temp: temp,
        database,
    }
}

#[test]
fn frozen_schema5_migration_preserves_data_review_backup_and_restart() {
    let fixture = seeded_schema5_fixture();
    let source_bytes = fs::read(&fixture.database).unwrap();
    let source_sha256 = file_sha256(&fixture.database);

    let report = SqliteQueue::migrate_schema5(&fixture.database).unwrap();

    assert_eq!(report.from_schema, 5);
    assert_eq!(report.to_schema, 6);
    assert_eq!(fs::read(&report.backup_path).unwrap(), source_bytes);
    let queue = SqliteQueue::open(&fixture.database).unwrap();
    let item = queue.get_item("item-1").unwrap();
    assert_eq!(item.target_ref.as_str(), "refs/heads/main");
    assert_eq!(item.producer_metadata["fixture"], "schema5");
    let store = iq::control_store::ControlStore::open(&fixture.database).unwrap();
    let effort = store.effort_for_item("item-1").unwrap().unwrap();
    assert_eq!(
        effort.composition,
        CompositionEvidence::MigratedUnknown { source_schema: 5 }
    );
    let IntegrationEffortState::ReviewRequired(review) = effort.state else {
        panic!("schema-5 candidate did not become review_required")
    };
    assert_eq!(review.candidate_sha, CANDIDATE_SHA);
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT classification FROM candidate_evidence WHERE effort_id='effort-1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "semantic"
    );
    let (event_alert, event_review, event_target_ref, event_candidate): (
        i64,
        String,
        String,
        String,
    ) = connection
        .query_row(
            "SELECT alert,json_extract(payload_json,'$.review_id'),json_extract(payload_json,'$.target_ref'),json_extract(payload_json,'$.candidate_sha') FROM durable_events WHERE effort_id='effort-1' AND event_type='review_required'",
            [],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        (event_alert, event_review, event_target_ref, event_candidate),
        (
            1,
            review.review_id,
            "refs/heads/main".into(),
            CANDIDATE_SHA.into()
        )
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM queue_metadata WHERE key='schema5_migration_source_sha256'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        source_sha256
    );
    drop(connection);
    drop(queue);

    let restarted = SqliteQueue::migrate_schema5(&fixture.database).unwrap();
    assert_eq!(restarted.backup_path, report.backup_path);
    SqliteQueue::open(&fixture.database).unwrap();
}

#[test]
fn frozen_schema5_migration_pauses_unreleased_landing_for_exact_review() {
    let fixture = seeded_schema5_fixture_with_state(Schema5EffortState::Landing);

    SqliteQueue::migrate_schema5(&fixture.database).unwrap();

    let queue = SqliteQueue::open(&fixture.database).unwrap();
    let item = queue.get_item("item-1").unwrap();
    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.landing, LandingState::Ready);
    let store = iq::control_store::ControlStore::open(&fixture.database).unwrap();
    let effort = store.effort_for_item("item-1").unwrap().unwrap();
    let IntegrationEffortState::ReviewRequired(review) = effort.state else {
        panic!("unreleased schema-5 landing did not pause for review")
    };
    assert_eq!(review.cycle_id, "cycle-1");
    assert_eq!(review.candidate_sha, CANDIDATE_SHA);
    let connection = Connection::open(&fixture.database).unwrap();
    let review_identity: (String, String, String, String, String, String, String) = connection
        .query_row(
            "SELECT id,effort_id,attempt_id,cycle_id,target_sha,source_sha,candidate_sha FROM candidate_reviews WHERE effort_id='effort-1'",
            [],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?)),
        )
        .unwrap();
    assert_eq!(
        review_identity,
        (
            review.review_id.clone(),
            "effort-1".into(),
            "attempt-1".into(),
            "cycle-1".into(),
            TARGET_SHA.into(),
            SOURCE_SHA.into(),
            CANDIDATE_SHA.into(),
        )
    );
    let event: serde_json::Value = connection
        .query_row(
            "SELECT payload_json FROM durable_events WHERE effort_id='effort-1' AND event_type='review_required'",
            [],
            |row| row.get::<_, String>(0),
        )
        .map(|payload| serde_json::from_str(&payload).unwrap())
        .unwrap();
    assert_eq!(event["review_id"], review.review_id);
    assert_eq!(event["attempt_id"], "attempt-1");
    assert_eq!(event["cycle_id"], "cycle-1");
    assert_eq!(event["target_ref"], "refs/heads/main");
    assert_eq!(event["target_sha"], TARGET_SHA);
    assert_eq!(event["source_sha"], SOURCE_SHA);
    assert_eq!(event["candidate_sha"], CANDIDATE_SHA);
}

#[test]
fn frozen_schema5_migration_preserves_landing_uncertain_authority() {
    let fixture = seeded_schema5_fixture_with_state(Schema5EffortState::LandingUncertain);

    SqliteQueue::migrate_schema5(&fixture.database).unwrap();

    let queue = SqliteQueue::open(&fixture.database).unwrap();
    let item = queue.get_item("item-1").unwrap();
    assert_eq!(item.status, QueueStatus::Integrating);
    assert_eq!(
        item.landing,
        LandingState::Uncertain {
            candidate_sha: CANDIDATE_SHA.into(),
            expected_target_sha: TARGET_SHA.into(),
        }
    );
    let store = iq::control_store::ControlStore::open(&fixture.database).unwrap();
    let effort = store.effort_for_item("item-1").unwrap().unwrap();
    assert_eq!(
        effort.composition,
        CompositionEvidence::MigratedPostRelease { source_schema: 5 }
    );
    let IntegrationEffortState::LandingUncertain(landing) = effort.state else {
        panic!("schema-5 landing_uncertain authority changed during migration")
    };
    assert_eq!(landing.candidate_sha, CANDIDATE_SHA);
    assert_eq!(landing.expected_target_sha, TARGET_SHA);
    assert_eq!(landing.command_id, "landing-command-1");
    assert_eq!(landing.evidence, "command_gate_released");
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM candidate_reviews WHERE effort_id='effort-1'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM durable_events WHERE effort_id='effort-1' AND event_type='review_required'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn frozen_schema5_migration_preserves_integrated_authority() {
    let fixture = seeded_schema5_fixture_with_state(Schema5EffortState::Integrated);

    SqliteQueue::migrate_schema5(&fixture.database).unwrap();

    let queue = SqliteQueue::open(&fixture.database).unwrap();
    let item = queue.get_item("item-1").unwrap();
    assert_eq!(item.status, QueueStatus::Integrated);
    let store = iq::control_store::ControlStore::open(&fixture.database).unwrap();
    let effort = store.effort_for_item("item-1").unwrap().unwrap();
    assert_eq!(
        effort.composition,
        CompositionEvidence::MigratedPostRelease { source_schema: 5 }
    );
    let IntegrationEffortState::Integrated(integrated) = effort.state else {
        panic!("schema-5 integrated authority changed during migration")
    };
    assert_eq!(integrated.candidate_sha, CANDIDATE_SHA);
    assert_eq!(integrated.landed_sha, CANDIDATE_SHA);
    assert_eq!(integrated.attempt_id, "attempt-1");
    assert_eq!(integrated.event_id, "event-integrated-1");
}

#[test]
fn frozen_schema5_migration_preserves_blocked_landing_uncertain_authority() {
    let fixture = seeded_schema5_fixture_with_state(Schema5EffortState::LandingUncertain);
    let connection = Connection::open(&fixture.database).unwrap();
    let blocker = serde_json::json!({
        "kind":"infrastructure",
        "component":"filesystem",
        "operation":"reconcile landing",
        "cause":{"kind":"unavailable","detail":"fixture"}
    });
    let state = serde_json::json!({
        "state":"infrastructure_blocked",
        "payload":{
            "blocker":blocker,
            "resume":{
                "state":"landing_uncertain",
                "payload":{
                    "candidate_sha":CANDIDATE_SHA,
                    "expected_target_sha":TARGET_SHA,
                    "command_id":"landing-command-1",
                    "evidence":"command_gate_released"
                }
            }
        }
    });
    connection
        .execute(
            "UPDATE integration_efforts SET state='infrastructure_blocked',state_json=?1,blocker_kind='infrastructure' WHERE id='effort-1'",
            [state.to_string()],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET status='blocked',blocked_phase='integrating',blocked_reason='infra',blocked_message=?1 WHERE id='item-1'",
            [blocker.to_string()],
        )
        .unwrap();
    drop(connection);

    SqliteQueue::migrate_schema5(&fixture.database).unwrap();

    SqliteQueue::open(&fixture.database).unwrap();
    let store = iq::control_store::ControlStore::open(&fixture.database).unwrap();
    let effort = store.effort_for_item("item-1").unwrap().unwrap();
    assert_eq!(
        effort.composition,
        CompositionEvidence::MigratedPostRelease { source_schema: 5 }
    );
    let IntegrationEffortState::InfrastructureBlocked(blocked) = effort.state else {
        panic!("schema-5 blocked landing authority changed during migration")
    };
    let iq::control_domain::ResumeState::LandingUncertain(landing) = blocked.resume else {
        panic!("schema-5 blocked landing resume authority changed during migration")
    };
    assert_eq!(landing.candidate_sha, CANDIDATE_SHA);
    assert_eq!(landing.command_id, "landing-command-1");
}

#[test]
fn schema5_migration_rejects_same_database_backup_with_different_content() {
    let fixture = seeded_schema5_fixture();
    let mut backup = fixture.database.as_os_str().to_os_string();
    backup.push(".schema5-backup");
    let backup = PathBuf::from(backup);
    fs::copy(&fixture.database, &backup).unwrap();
    Connection::open(&backup)
        .unwrap()
        .execute(
            "INSERT INTO queue_metadata(key,value) VALUES('stale-backup','different')",
            [],
        )
        .unwrap();

    let error = SqliteQueue::migrate_schema5(&fixture.database).unwrap_err();

    assert!(
        format!("{error:#}").contains("differs from the exact migration source"),
        "{error:#}"
    );
}
