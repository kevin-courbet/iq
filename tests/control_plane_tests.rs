use iq::agent_protocol::{
    parse_result, AgentInput, AgentResult, LandingVariant, ProtocolLimits, RepositoryIdentity,
    RiftIdentity, SourceVariant,
};
use iq::control_api::{request, ApiRequest, ControlApiServer};
use iq::control_domain::{
    AgentRunning, AtomicResultState, EncodedPath, ExactEffortIdentity, ExecutableIdentity,
    InfrastructureCause, InfrastructureComponent, IntegrationEffortState, IssueRepositorySnapshot,
    IssueVisibility, ProviderGateKind, ProviderGateStatus, ProviderSignoffBlocker, RunnerBounds,
    RunnerKind, RunnerSnapshot, SandboxIdentity, StateRepositorySnapshot,
};
use iq::control_store::ControlStore;
use sha2::Digest;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn sha(byte: char) -> String {
    std::iter::repeat_n(byte, 40).collect()
}

fn input() -> AgentInput {
    AgentInput {
        version: 1,
        identity: ExactEffortIdentity {
            effort_id: "effort-1".into(),
            item_id: "item-1".into(),
            attempt_id: "attempt-1".into(),
            cycle_id: "cycle-1".into(),
            target_sha: sha('1'),
            source_sha: sha('2'),
            candidate_sha: None,
        },
        repository: RepositoryIdentity {
            repo_key: "/repo::main".into(),
            target_branch: "main".into(),
        },
        source: SourceVariant::RemoteBranch {
            branch: "feature".into(),
            sha: sha('2'),
        },
        landing: LandingVariant::Direct,
        base_sha: sha('0'),
        rift: RiftIdentity {
            rift_id: "rift-1".into(),
            source_rift_id: "rift-source".into(),
            relative_path: EncodedPath::from_bytes(b"item-1").unwrap(),
        },
        conflicts: Vec::new(),
        prior_outcomes: Vec::new(),
        validation_evidence: Vec::new(),
        instructions: Vec::new(),
        limits: ProtocolLimits {
            max_result_bytes: 4096,
            max_text_bytes: 1024,
            max_paths: 10,
            max_evidence_entries: 10,
        },
    }
}

#[test]
fn protocol_rejects_unknown_fields_and_identity_changes() {
    let input = input();
    let unknown = br#"{"outcome":"resolved","version":1,"identity":{"effort_id":"effort-1","item_id":"item-1","attempt_id":"attempt-1","cycle_id":"cycle-1","target_sha":"1111111111111111111111111111111111111111","source_sha":"2222222222222222222222222222222222222222","candidate_sha":null},"staged_tree_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","changed_paths":[],"checks":[],"unknown":true}"#;
    assert!(parse_result(unknown, &input).is_err());

    let mut identity = serde_json::to_value(&input.identity).unwrap();
    identity["item_id"] = serde_json::Value::String("other".into());
    let wrong_identity = serde_json::json!({
        "outcome": "resolved",
        "version": 1,
        "identity": identity,
        "staged_tree_sha256": "a".repeat(64),
        "changed_paths": [],
        "checks": []
    });
    assert!(parse_result(&serde_json::to_vec(&wrong_identity).unwrap(), &input).is_err());
}

#[test]
fn interrupted_protocol_cycle_delete_retries_exact_quarantine_only() {
    let temp = tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let protocol = iq::agent_protocol::protocol_directory(&workspace, "cycle-1").unwrap();
    let blocked = protocol.join("blocked");
    std::fs::create_dir(&blocked).unwrap();
    std::fs::write(blocked.join("result"), "result").unwrap();
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let unrelated = workspace
        .join(".iq-agent-protocol")
        .join(".remove-cycle-1-not-a-uuid");
    std::fs::create_dir(&unrelated).unwrap();
    std::fs::set_permissions(&unrelated, std::fs::Permissions::from_mode(0o700)).unwrap();

    assert!(iq::agent_protocol::remove_protocol_cycle(&workspace, "cycle-1").is_err());
    let quarantine = workspace.join(".iq-agent-protocol/.remove-cycle-1");
    assert!(!protocol.exists());
    assert!(quarantine.is_dir());
    std::fs::set_permissions(
        quarantine.join("blocked"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();

    iq::agent_protocol::remove_protocol_cycle(&workspace, "cycle-1").unwrap();
    iq::agent_protocol::remove_protocol_cycle(&workspace, "cycle-1").unwrap();
    assert!(!quarantine.exists());
    assert!(unrelated.is_dir());
}

#[test]
fn terminal_cleanup_rejects_replacement_rift_before_artifact_removal() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("workspaces");
    let workspace = root.join("item-1");
    let sandbox = root.join(".iq-agent-sandbox-cycle-1");
    std::fs::create_dir_all(workspace.join(".iq-agent-protocol/cycle-1")).unwrap();
    std::fs::write(workspace.join(".rift"), "BBBBBBBBBBBBBBBBBBBBBBBBBB\n").unwrap();
    std::fs::create_dir_all(sandbox.join("export")).unwrap();
    let artifacts = iq::control_store::TerminalCycleArtifacts {
        item_id: "item-1".into(),
        cycle_id: "cycle-1".into(),
        workspace: iq::sqlite::WorkspaceIdentity {
            path: workspace.to_string_lossy().into_owned(),
            rift_id: "AAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
            source_rift_id: "source-rift".into(),
        },
    };

    let error = iq::agent_runner::cleanup_terminal_cycle_artifacts(
        &iq::sqlite::WorkspaceRootIdentity {
            path: root,
            source: Path::new("/source").to_path_buf(),
            source_rift_id: "source-rift".into(),
            scope: "fixture::main".into(),
            registry_identity: "registry".into(),
            generation: 0,
        },
        &artifacts,
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("Rift identity differs"));
    assert!(sandbox.is_dir());
    assert!(workspace.join(".iq-agent-protocol/cycle-1").is_dir());
}

#[test]
fn terminal_cleanup_removes_exact_sandbox_when_original_rift_is_absent() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("workspaces");
    std::fs::create_dir(&root).unwrap();
    let sandbox = root.join(".iq-agent-sandbox-cycle-1");
    let unrelated = root.join(".iq-agent-sandbox-other-cycle");
    std::fs::create_dir_all(sandbox.join("export")).unwrap();
    std::fs::create_dir_all(unrelated.join("export")).unwrap();
    let artifacts = iq::control_store::TerminalCycleArtifacts {
        item_id: "item-1".into(),
        cycle_id: "cycle-1".into(),
        workspace: iq::sqlite::WorkspaceIdentity {
            path: root.join("item-1").to_string_lossy().into_owned(),
            rift_id: "AAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
            source_rift_id: "source-rift".into(),
        },
    };
    let root_identity = iq::sqlite::WorkspaceRootIdentity {
        path: root,
        source: Path::new("/source").to_path_buf(),
        source_rift_id: "source-rift".into(),
        scope: "fixture::main".into(),
        registry_identity: "registry".into(),
        generation: 0,
    };

    iq::agent_runner::cleanup_terminal_cycle_artifacts(&root_identity, &artifacts).unwrap();
    iq::agent_runner::cleanup_terminal_cycle_artifacts(&root_identity, &artifacts).unwrap();

    assert!(!sandbox.exists());
    assert!(unrelated.is_dir());
}

#[test]
fn encoded_paths_preserve_non_utf8_and_reject_escape() {
    let path = EncodedPath::from_bytes(b"src/\xffname").unwrap();
    assert_eq!(path.to_bytes().unwrap(), b"src/\xffname");
    assert!(EncodedPath::from_bytes(b"../secret").is_err());
    assert!(EncodedPath::from_bytes(b"/absolute").is_err());
}

#[test]
fn repository_policy_is_one_strict_variant() {
    let repository = StateRepositorySnapshot::GitlabIssue(IssueRepositorySnapshot {
        repository: "group/project".into(),
        visibility: IssueVisibility::Full,
        allowed_responders: vec!["maintainer".into()],
    });
    repository.validate().unwrap();
    let duplicate = StateRepositorySnapshot::GithubIssue(IssueRepositorySnapshot {
        repository: "org/repo".into(),
        visibility: IssueVisibility::Minimal,
        allowed_responders: vec!["Octo".into(), "octo".into()],
    });
    assert!(duplicate.validate().is_err());
}

#[test]
fn sql_rejects_missing_or_mismatched_blocker_identity() {
    let fixture = effort_fixture();
    fixture
        .store
        .block_infrastructure(
            &fixture.effort_id,
            iq::control_domain::InfrastructureBlocker {
                component: InfrastructureComponent::Database,
                operation: "persist state".into(),
                cause: InfrastructureCause::Unavailable {
                    detail: "database unavailable".into(),
                },
            },
        )
        .unwrap();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    assert!(connection
        .execute(
            "UPDATE integration_efforts SET blocker_kind=NULL WHERE id=?1",
            [&fixture.effort_id],
        )
        .is_err());
    assert!(connection
        .execute(
            "UPDATE integration_efforts SET blocker_kind='provider_signoff' WHERE id=?1",
            [&fixture.effort_id],
        )
        .is_err());
    assert!(connection
        .execute(
            "UPDATE integration_efforts SET state_json='{\"state\":\"infrastructure_blocked\",\"payload\":{}}' WHERE id=?1",
            [&fixture.effort_id],
        )
        .is_err());
}

#[test]
fn sql_rejects_extra_and_missing_effort_payload_fields() {
    let fixture = effort_fixture();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    assert!(connection
        .execute(
            "UPDATE integration_efforts SET state_json='{\"state\":\"agent_ready\",\"payload\":{\"next_cycle\":1,\"candidate_sha\":\"1111111111111111111111111111111111111111\"}}' WHERE id=?1",
            [&fixture.effort_id],
        )
        .is_err());
    assert!(connection
        .execute(
            "UPDATE integration_efforts SET state_json='{\"state\":\"agent_ready\",\"payload\":{}}' WHERE id=?1",
            [&fixture.effort_id],
        )
        .is_err());
    assert!(connection
        .execute(
            "UPDATE integration_efforts SET state_json='{\"state\":\"agent_ready\",\"payload\":{\"next_cycle\":1},\"extra\":true}' WHERE id=?1",
            [&fixture.effort_id],
        )
        .is_err());
}

#[test]
fn valid_resolved_result_parses_as_typed_variant() {
    let input = input();
    let result = serde_json::json!({
        "outcome": "resolved",
        "version": 1,
        "identity": input.identity,
        "staged_tree_sha256": "a".repeat(64),
        "changed_paths": [],
        "checks": []
    });
    assert!(matches!(
        parse_result(&serde_json::to_vec(&result).unwrap(), &input).unwrap(),
        AgentResult::Resolved(_)
    ));
}

#[test]
fn unix_socket_api_enforces_private_modes_and_serves_durable_inbox() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let store = ControlStore::open_test_database(&database).unwrap();
    let socket = temp.path().join("control/control.sock");
    let config = iq::agent_config::ControlPlaneConfig {
        unix_socket: socket.clone(),
        max_request_bytes: 4096,
        max_free_text_bytes: 1024,
        max_response_bytes: 4096,
        max_concurrent_clients: 2,
        max_client_queue_bytes: 4096,
        max_stream_backlog_events: 100,
        client_idle_seconds: 5,
    };
    let server = ControlApiServer::bind(config, store).unwrap();
    assert_eq!(
        std::fs::metadata(socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let thread = std::thread::spawn(move || server.serve_one().unwrap());
    let response = request(&socket, &ApiRequest::Inbox { limit: 10 }, 4096).unwrap();
    assert!(response.ok);
    assert_eq!(response.result, serde_json::json!([]));
    thread.join().unwrap();
}

#[test]
fn unix_socket_event_stream_resumes_from_last_durable_cursor() {
    let fixture = effort_fixture();
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "blocked",
            serde_json::json!({"reason":"first"}),
        )
        .unwrap();
    let socket = fixture.temp.path().join("control/control.sock");
    let config = iq::agent_config::ControlPlaneConfig {
        unix_socket: socket.clone(),
        max_request_bytes: 4096,
        max_free_text_bytes: 1024,
        max_response_bytes: 64 * 1024,
        max_concurrent_clients: 2,
        max_client_queue_bytes: 64 * 1024,
        max_stream_backlog_events: 100,
        client_idle_seconds: 1,
    };
    let server = ControlApiServer::bind(config.clone(), fixture.store.clone()).unwrap();
    let thread = std::thread::spawn(move || server.serve_one().unwrap());
    let mut event_ids = Vec::new();
    let cursor = iq::control_api::watch(&socket, 0, 100, 64 * 1024, |response| {
        if response.result["kind"] == "events" {
            event_ids.extend(
                response.result["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|event| event["id"].as_str().unwrap().to_string()),
            );
        }
        Ok(())
    })
    .unwrap();
    thread.join().unwrap();
    assert!(!event_ids.is_empty());

    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "blocked_again",
            serde_json::json!({"reason":"second"}),
        )
        .unwrap();
    let server = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
    let thread = std::thread::spawn(move || server.serve_one().unwrap());
    let mut resumed = Vec::new();
    let resumed_cursor = iq::control_api::watch(&socket, cursor, 100, 64 * 1024, |response| {
        if response.result["kind"] == "events" {
            resumed.extend(
                response.result["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|event| event["id"].as_str().unwrap().to_string()),
            );
        }
        Ok(())
    })
    .unwrap();
    thread.join().unwrap();
    assert_eq!(resumed.len(), 1);
    assert!(!event_ids.contains(&resumed[0]));
    assert!(resumed_cursor > cursor);
}

#[test]
fn unix_socket_event_stream_reports_backpressure_without_advancing_cursor() {
    let fixture = effort_fixture();
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "blocked",
            serde_json::json!({"reason":"x".repeat(1024)}),
        )
        .unwrap();
    let socket = fixture.temp.path().join("control/control.sock");
    let config = iq::agent_config::ControlPlaneConfig {
        unix_socket: socket.clone(),
        max_request_bytes: 4096,
        max_free_text_bytes: 1024,
        max_response_bytes: 4096,
        max_concurrent_clients: 2,
        max_client_queue_bytes: 256,
        max_stream_backlog_events: 100,
        client_idle_seconds: 1,
    };
    let server = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
    let thread = std::thread::spawn(move || server.serve_one().unwrap());
    let mut responses = Vec::new();
    let cursor = iq::control_api::watch(&socket, 0, 100, 4096, |response| {
        responses.push(response.clone());
        Ok(())
    })
    .unwrap();
    thread.join().unwrap();

    assert_eq!(cursor, 0);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].result["kind"], "disconnect");
    assert_eq!(responses[0].result["reason"], "backpressure");
    assert_eq!(responses[0].result["cursor"], 0);
}

#[test]
fn unix_socket_event_stream_reports_expired_cursor() {
    let fixture = effort_fixture();
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "blocked",
            serde_json::json!({"reason":"first"}),
        )
        .unwrap();
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "blocked_again",
            serde_json::json!({"reason":"second"}),
        )
        .unwrap();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute("DELETE FROM durable_events WHERE sequence<3", [])
        .unwrap();
    let oldest: u64 = connection
        .query_row("SELECT MIN(sequence) FROM durable_events", [], |row| {
            row.get(0)
        })
        .unwrap();
    drop(connection);
    assert!(oldest > 1);
    let socket = fixture.temp.path().join("control/control.sock");
    let config = iq::agent_config::ControlPlaneConfig {
        unix_socket: socket.clone(),
        max_request_bytes: 4096,
        max_free_text_bytes: 1024,
        max_response_bytes: 4096,
        max_concurrent_clients: 2,
        max_client_queue_bytes: 4096,
        max_stream_backlog_events: 100,
        client_idle_seconds: 1,
    };
    let server = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
    let thread = std::thread::spawn(move || server.serve_one().unwrap());
    let response = request(
        &socket,
        &ApiRequest::Watch {
            cursor: 1,
            limit: 100,
        },
        4096,
    )
    .unwrap();
    thread.join().unwrap();

    assert!(!response.ok);
    assert_eq!(response.result["kind"], "cursor_expired");
    assert_eq!(response.result["oldest_cursor"], oldest - 1);
}

#[test]
fn explicit_v8_migration_creates_verified_private_backup() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    drop(iq::sqlite::SqliteQueue::open(&database).unwrap());
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute(
            "UPDATE queue_metadata SET value='8' WHERE key='workspace_schema_version'",
            [],
        )
        .unwrap();
    connection
        .execute_batch(
            "CREATE TABLE communication_bindings(id TEXT PRIMARY KEY); CREATE TABLE communication_response_receipts(id TEXT PRIMARY KEY);",
        )
        .unwrap();
    drop(connection);
    let system = temp.path().join("system.yaml");
    std::fs::write(&system, system_config(temp.path())).unwrap();

    drop(iq::sqlite::SqliteQueue::migrate_v8(&database, &system).unwrap());

    let backup = temp.path().join("queue.db.schema-v8.backup");
    let metadata = std::fs::metadata(&backup).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    let backup_connection = rusqlite::Connection::open(&backup).unwrap();
    let backup_version: String = backup_connection
        .query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(backup_version, "8");
    let migrated = rusqlite::Connection::open(&database).unwrap();
    let migrated_version: String = migrated
        .query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(migrated_version, "9");
}

#[test]
fn v8_ready_local_submission_and_claimed_conflict_migrate_with_exact_authority() {
    let temp = tempdir().unwrap();
    let repository = temp.path().join("repo");
    std::fs::create_dir(&repository).unwrap();
    git(&repository, ["init"]);
    let database = temp.path().join("queue.db");
    let target_sha = sha('1');
    let source_sha = sha('2');
    let ready_sha = sha('3');
    let ready_base_sha = sha('4');
    let ready_item_id = "4b6d66b7-d46d-442d-8704-fc909e110478";
    let ready_workspace_id = "workspace-v8-ready";
    let ready_submission_id = "submission-v8-ready";
    let ready_private_ref = "refs/iq/submissions/v8-ready";
    let item_id = "arbitrary-eight-conflict-item";
    let attempt_id = "attempt-v8";
    let prompt_id = "prompt-v8";
    let retained = temp.path().join("retained");
    std::fs::create_dir(&retained).unwrap();
    let conflict_paths = (1..=8)
        .map(|index| format!("conflict-{index}.txt"))
        .collect::<Vec<_>>();
    let conflicts = serde_json::json!({
        "files":conflict_paths,
        "target_sha":target_sha,
        "source_sha":source_sha
    });
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch(include_str!("fixtures/schema-v8-active.sql"))
        .unwrap();
    connection.execute(
        "INSERT INTO registered_repositories(repo_key,integration_path,target_branch,remote,seed_path,workspace_root,checkout_reconciliation_json,seed_refresh_json,created_at,updated_at) VALUES('fixture::main',?1,'main','origin',?2,?3,?4,?5,'2025-12-31T00:00:00Z','2025-12-31T00:00:00Z')",
        rusqlite::params![repository.as_os_str().as_bytes(),temp.path().join("seed").as_os_str().as_bytes(),temp.path().join("development-workspaces").as_os_str().as_bytes(),serde_json::json!({"state":"ready","target_sha":ready_base_sha}).to_string(),serde_json::json!({"state":"ready","target_sha":ready_base_sha}).to_string()],
    ).unwrap();
    connection.execute(
        "INSERT INTO development_workspaces(id,repo_key,name,path,branch,base_sha,status,cleanup_json,created_at,updated_at) VALUES(?1,'fixture::main','ready-local',?2,'iq-ready-local',?3,'submitted','{\"state\":\"pending\"}','2025-12-31T00:00:00Z','2025-12-31T00:00:00Z')",
        rusqlite::params![ready_workspace_id,temp.path().join("ready-development-workspace").as_os_str().as_bytes(),ready_base_sha],
    ).unwrap();
    connection.execute(
        "INSERT INTO local_submissions(id,queue_item_id,repo_key,workspace_id,base_sha,commit_sha,private_ref,staging_ref,state,created_at) VALUES(?1,?2,'fixture::main',?3,?4,?5,?6,'refs/iq/staging/v8-ready','queued','2025-12-31T00:00:00Z')",
        rusqlite::params![ready_submission_id,ready_item_id,ready_workspace_id,ready_base_sha,ready_sha,ready_private_ref],
    ).unwrap();
    connection.execute(
        "INSERT INTO queue_items(id,repo_key,repo_path,source_branch,target_branch,producer_metadata_json,validation_evidence_json,status,current_head_sha,landing_state_json,source_kind,source_ref,submission_id,landing_policy,created_at,updated_at) VALUES(?1,'fixture::main',?2,?3,'main','{\"worker\":\"W-ready\"}','[]','ready',?4,'{\"state\":\"ready\"}','local_submission',?3,?5,'squash','2025-12-31T00:00:00Z','2025-12-31T00:00:00Z')",
        rusqlite::params![ready_item_id,repository.to_str().unwrap(),ready_private_ref,ready_sha,ready_submission_id],
    ).unwrap();
    connection
        .execute(
            "INSERT INTO queue_items(id,repo_key,repo_path,source_branch,target_branch,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,blocked_phase,blocked_reason,blocked_message,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,target_sha,source_sha,landing_state_json,source_kind,source_ref,landing_policy,created_at,updated_at) VALUES(?1,'fixture::main',?2,'agent/conflict','main','{\"worker\":\"W001\"}','[]','blocked',?3,?4,'merging','needs_user_input','legacy conflict',?5,?6,?7,'rift-1','source-rift-1',?8,?3,'{\"state\":\"ready\"}','remote_branch','agent/conflict','direct','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            rusqlite::params![item_id,repository.to_str().unwrap(),source_sha,attempt_id,prompt_id,conflicts.to_string(),retained.to_str().unwrap(),target_sha],
        )
        .unwrap();
    connection.execute(
        "INSERT INTO integration_attempts(id,item_id,attempt_number,source_head_sha,target_base_sha,started_at) VALUES(?1,?2,1,?3,?4,'2026-01-01T00:00:00Z')",
        rusqlite::params![attempt_id,item_id,source_sha,target_sha],
    ).unwrap();
    connection.execute(
        "INSERT INTO prompts(id,item_id,attempt_id,blocked_phase,status,question,created_by,created_at) VALUES(?1,?2,?3,'merging','open','Resolve conflict','iq','2026-01-01T00:00:00Z')",
        rusqlite::params![prompt_id,item_id,attempt_id],
    ).unwrap();
    drop(connection);
    let system = temp.path().join("system.yaml");
    std::fs::write(&system, system_config(temp.path())).unwrap();

    let migrated = iq::sqlite::SqliteQueue::migrate_v8(&database, &system).unwrap();
    let store = ControlStore::open(migrated.path()).unwrap();
    assert!(store.effort_for_item(ready_item_id).unwrap().is_none());
    let ready = migrated.get_item(ready_item_id).unwrap();
    assert_eq!(ready.id, ready_item_id);
    assert_eq!(ready.status, iq::core::QueueStatus::Ready);
    assert_eq!(ready.current_attempt_id, None);
    assert_eq!(ready.target_sha, None);
    assert_eq!(ready.source_sha, None);
    assert!(ready.workspace.identity().is_none());
    let migrated_connection = rusqlite::Connection::open(&database).unwrap();
    let ready_created_at: String = migrated_connection
        .query_row(
            "SELECT created_at FROM queue_items WHERE id=?1",
            [ready_item_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(ready_created_at, "2025-12-31T00:00:00Z");
    let submission = migrated.local_submission(ready_submission_id).unwrap();
    assert_eq!(submission.queue_item_id, ready_item_id);
    assert_eq!(submission.workspace_id, ready_workspace_id);
    assert_eq!(submission.commit_sha, ready_sha);
    assert_eq!(submission.private_ref, ready_private_ref);
    assert_eq!(submission.state, iq::sqlite::LocalSubmissionState::Queued);
    let workspace = migrated.workspace(ready_workspace_id).unwrap();
    assert_eq!(
        workspace.status,
        iq::sqlite::DevelopmentWorkspaceStatus::Submitted
    );

    let effort = store.effort_for_item(item_id).unwrap().unwrap();

    assert_eq!(effort.attempt_id, attempt_id);
    assert_eq!(effort.target_sha, target_sha);
    assert_eq!(effort.source_sha, source_sha);
    assert_eq!(effort.source_variant, "remote_branch");
    assert_eq!(effort.landing_variant, "direct");
    assert_eq!(effort.workspace.path, retained.to_str().unwrap());
    assert_eq!(effort.workspace.rift_id, "rift-1");
    assert_eq!(effort.workspace.source_rift_id, "source-rift-1");
    assert_eq!(effort.item_id, item_id);
    assert!(matches!(
        effort.state,
        IntegrationEffortState::AgentReady(_)
    ));
    let migrated_item = migrated.get_item(item_id).unwrap();
    assert_eq!(migrated_item.status, iq::core::QueueStatus::Merging);
    assert_eq!(
        migrated_item.conflict.unwrap()["files"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
    assert_eq!(migrated.get_prompt(prompt_id).unwrap().status, "superseded");
    assert_eq!(store.inbox(10).unwrap().len(), 1);
}

#[test]
fn omitted_notifications_use_non_zero_defaults() {
    let temp = tempdir().unwrap();
    let system = temp.path().join("system.yaml");
    let configuration = system_config(temp.path());
    let without_notifications = configuration.split("notifications:\n").next().unwrap();
    std::fs::write(&system, without_notifications).unwrap();

    let loaded = iq::agent_config::SystemConfig::load(&system).unwrap();

    assert!(loaded.notifications.max_attempts > 0);
    assert!(loaded.notifications.max_event_age_seconds > 0);
    assert!(loaded.notifications.projection_debt_alert_seconds > 0);
    assert!(loaded.notifications.backends.is_empty());
}

#[test]
fn queue_landing_projection_rejects_drift_after_effort_creation() {
    let fixture = effort_fixture();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();

    let error = connection
        .execute(
            "UPDATE queue_items SET landing_state_json=json_object('state','uncertain','candidate_sha',?1,'expected_target_sha',?2) WHERE id='item-1'",
            rusqlite::params![sha('3'), sha('1')],
        )
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("queue lifecycle is a projection of integration_effort"));
    let landing: String = connection
        .query_row(
            "SELECT landing_state_json FROM queue_items WHERE id='item-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(landing, r#"{"state":"ready"}"#);
}

#[test]
fn api_retry_resumes_provider_gate_without_consuming_an_agent_cycle() {
    let fixture = effort_fixture_with_repository(StateRepositorySnapshot::GitlabIssue(
        IssueRepositorySnapshot {
            repository: "group/project".into(),
            visibility: IssueVisibility::Full,
            allowed_responders: vec!["maintainer".into()],
        },
    ));
    let effort = fixture.store.effort_for_item("item-1").unwrap().unwrap();
    let cycle = running_cycle("cycle-1", 1);
    start_cycle(&fixture.store, &effort.id, &cycle);
    let intent = iq::control_store::CandidateIntent {
        operation_id: "builder-1".into(),
        cycle_id: cycle.cycle_id,
        staged_tree_sha256: "a".repeat(64),
        tree_sha: sha('4'),
        parents: vec![sha('1'), sha('2')],
        author_name: "IQ Test".into(),
        author_email: "iq@example.test".into(),
        author_timestamp: "2026-01-01T00:00:00Z".into(),
        committer_name: "IQ Test".into(),
        committer_email: "iq@example.test".into(),
        committer_timestamp: "2026-01-01T00:00:00Z".into(),
        message: "candidate".into(),
        operation_ref: "refs/iq/candidate-operations/builder-1".into(),
    };
    fixture
        .store
        .accept_resolved_cycle(&effort.id, &intent)
        .unwrap();
    let candidate_sha = sha('3');
    fixture
        .store
        .record_candidate(
            &effort.id,
            &iq::control_store::CandidateObservation {
                operation_id: intent.operation_id.clone(),
                candidate_sha: candidate_sha.clone(),
                tree_sha: intent.tree_sha.clone(),
                parent_shas: intent.parents.clone(),
                author_name: intent.author_name.clone(),
                author_email: intent.author_email.clone(),
                author_timestamp: intent.author_timestamp.clone(),
                committer_name: intent.committer_name.clone(),
                committer_email: intent.committer_email.clone(),
                committer_timestamp: intent.committer_timestamp.clone(),
                message: intent.message.clone(),
                operation_ref: intent.operation_ref.clone(),
            },
        )
        .unwrap();
    fixture
        .store
        .start_validation(&effort.id, "policy-digest")
        .unwrap();
    fixture
        .store
        .complete_validation(&effort.id, &candidate_sha)
        .unwrap();
    fixture
        .store
        .block_provider(
            &effort.id,
            ProviderSignoffBlocker {
                gate: ProviderGateKind::Provider,
                repository: "org/repo".into(),
                context: "required-check".into(),
                candidate_sha,
                status: ProviderGateStatus::Pending,
                evidence: "check is pending".into(),
            },
        )
        .unwrap();

    let socket = fixture.temp.path().join("control/control.sock");
    let config = iq::agent_config::ControlPlaneConfig {
        unix_socket: socket.clone(),
        max_request_bytes: 4096,
        max_free_text_bytes: 1024,
        max_response_bytes: 64 * 1024,
        max_concurrent_clients: 2,
        max_client_queue_bytes: 64 * 1024,
        max_stream_backlog_events: 100,
        client_idle_seconds: 5,
    };
    let server = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
    let thread = std::thread::spawn(move || server.serve_one().unwrap());
    let response = request(
        &socket,
        &ApiRequest::Retry {
            item_id: effort.item_id,
        },
        64 * 1024,
    )
    .unwrap();
    thread.join().unwrap();

    assert!(response.ok);
    assert_eq!(response.result["failed_cycles"], 0);
    assert_eq!(response.result["state"]["state"], "validating");
    assert_eq!(response.result["state"]["payload"]["stage"], "gates");
}

#[test]
fn failed_landing_recomposition_preserves_one_recoverable_uncertain_state() {
    let fixture = effort_fixture();
    let effort = fixture.store.effort_for_item("item-1").unwrap().unwrap();
    let cycle = running_cycle("cycle-1", 1);
    start_cycle(&fixture.store, &effort.id, &cycle);
    let intent = iq::control_store::CandidateIntent {
        operation_id: "builder-1".into(),
        cycle_id: cycle.cycle_id,
        staged_tree_sha256: "a".repeat(64),
        tree_sha: sha('4'),
        parents: vec![sha('1'), sha('2')],
        author_name: "IQ Test".into(),
        author_email: "iq@example.test".into(),
        author_timestamp: "2026-01-01T00:00:00Z".into(),
        committer_name: "IQ Test".into(),
        committer_email: "iq@example.test".into(),
        committer_timestamp: "2026-01-01T00:00:00Z".into(),
        message: "candidate".into(),
        operation_ref: "refs/iq/candidate-operations/builder-1".into(),
    };
    fixture
        .store
        .accept_resolved_cycle(&effort.id, &intent)
        .unwrap();
    let candidate_sha = sha('3');
    fixture
        .store
        .record_candidate(
            &effort.id,
            &iq::control_store::CandidateObservation {
                operation_id: intent.operation_id.clone(),
                candidate_sha: candidate_sha.clone(),
                tree_sha: intent.tree_sha.clone(),
                parent_shas: intent.parents.clone(),
                author_name: intent.author_name.clone(),
                author_email: intent.author_email.clone(),
                author_timestamp: intent.author_timestamp.clone(),
                committer_name: intent.committer_name.clone(),
                committer_email: intent.committer_email.clone(),
                committer_timestamp: intent.committer_timestamp.clone(),
                message: intent.message.clone(),
                operation_ref: intent.operation_ref.clone(),
            },
        )
        .unwrap();
    fixture
        .store
        .start_validation(&effort.id, "policy-digest")
        .unwrap();
    fixture
        .store
        .complete_validation(&effort.id, &candidate_sha)
        .unwrap();
    rusqlite::Connection::open(&fixture.database)
        .unwrap()
        .execute(
            "UPDATE integration_attempts SET validated_commit_sha=?1 WHERE id='attempt-1'",
            [&candidate_sha],
        )
        .unwrap();
    fixture
        .store
        .begin_landing(
            &effort.id,
            &sha('1'),
            "lease-1",
            "command-1",
            iq::control_domain::SignoffDisposition::NotRequired,
        )
        .unwrap();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_recomposition_projection BEFORE UPDATE OF conflict_json ON queue_items BEGIN SELECT RAISE(ABORT,'injected recomposition failure'); END;",
        )
        .unwrap();

    assert!(fixture
        .store
        .recompose_after_target_move(
            &effort.id,
            &sha('5'),
            &serde_json::json!({"target_sha":sha('5')})
        )
        .is_err());

    let effort_after_failure = fixture.store.effort_for_item("item-1").unwrap().unwrap();
    assert!(matches!(
        effort_after_failure.state,
        IntegrationEffortState::LandingUncertain(ref landing)
            if landing.candidate_sha == candidate_sha
                && landing.expected_target_sha == sha('1')
    ));
    let (landing, target_sha, validated_sha): (String, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT item.landing_state_json,item.target_sha,attempt.validated_commit_sha FROM queue_items item JOIN integration_attempts attempt ON attempt.id=item.current_attempt_id WHERE item.id='item-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&landing).unwrap(),
        serde_json::json!({
            "state":"uncertain",
            "candidate_sha":candidate_sha,
            "expected_target_sha":sha('1')
        })
    );
    assert_eq!(target_sha.as_deref(), None);
    assert_eq!(validated_sha.as_deref(), Some(candidate_sha.as_str()));

    connection
        .execute_batch("DROP TRIGGER fail_recomposition_projection")
        .unwrap();
    fixture
        .store
        .recompose_after_target_move(
            &effort.id,
            &sha('5'),
            &serde_json::json!({"target_sha":sha('5')}),
        )
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .effort_for_item("item-1")
            .unwrap()
            .unwrap()
            .state,
        IntegrationEffortState::AgentReady(_)
    ));
    let (landing, target_sha, validated_sha): (String, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT item.landing_state_json,item.target_sha,attempt.validated_commit_sha FROM queue_items item JOIN integration_attempts attempt ON attempt.id=item.current_attempt_id WHERE item.id='item-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(landing, r#"{"state":"ready"}"#);
    assert_eq!(target_sha.as_deref(), Some(sha('5').as_str()));
    assert_eq!(validated_sha, None);
}

#[test]
fn effort_cancellation_restores_local_submission_workspace() {
    let fixture = effort_fixture();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute(
            "INSERT INTO development_workspaces(id,status,cleanup_json,updated_at) VALUES('workspace-1','submitted','{\"state\":\"pending\"}','test')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO local_submissions(id,workspace_id,state) VALUES('submission-1','workspace-1','queued')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET source_kind='local_submission',submission_id='submission-1' WHERE id='item-1'",
            [],
        )
        .unwrap();
    drop(connection);

    fixture
        .store
        .cancel(&fixture.effort_id, "test", "operator_cancelled")
        .unwrap();

    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let submission: String = connection
        .query_row(
            "SELECT state FROM local_submissions WHERE id='submission-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let workspace: String = connection
        .query_row(
            "SELECT status FROM development_workspaces WHERE id='workspace-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let queue: String = connection
        .query_row(
            "SELECT status FROM queue_items WHERE id='item-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(submission, "cancelled");
    assert_eq!(workspace, "active");
    assert_eq!(queue, "cancelled");
}

#[test]
fn cancelled_running_cycle_is_terminated_from_restart_debt() {
    let fixture = effort_fixture();
    let effort = fixture.store.effort_for_item("item-1").unwrap().unwrap();
    let mut child = Command::new("sleep")
        .arg("30")
        .process_group(0)
        .spawn()
        .unwrap();
    let pid = child.id();
    let start = iq::agent_runner::process_start_ticks(pid).unwrap();
    let group = unsafe { libc::getpgid(pid as i32) };
    let mut running = running_cycle("cycle-restart-cancel", 1);
    running.pid = pid;
    running.process_start_ticks = start;
    running.process_group_id = group;
    start_cycle(&fixture.store, &effort.id, &running);

    fixture
        .store
        .cancel(&effort.id, "test", "operator_cancelled")
        .unwrap();
    assert!(child.try_wait().unwrap().is_none());
    let debt: i64 = rusqlite::Connection::open(&fixture.database)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(debt, 1);

    assert_eq!(
        fixture
            .store
            .reconcile_cancelled_runner_terminations(true)
            .unwrap(),
        1
    );
    let _ = child.wait();
    assert!(iq::agent_runner::process_start_ticks(pid).is_err());
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let debt: i64 = connection
        .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(debt, 0);
}

#[test]
fn local_repository_creates_no_external_artifact() {
    let fixture = effort_fixture();
    iq::state_repository::project_item(&fixture.store, "item-1").unwrap();
    assert!(fixture
        .store
        .repository_artifact(&fixture.effort_id)
        .unwrap()
        .is_none());
}

#[test]
fn gitlab_issue_comments_get_one_durable_disposition_and_only_exact_authority_resumes() {
    let _guard = env_lock().lock().unwrap();
    let fixture = effort_fixture_with_repository(StateRepositorySnapshot::GitlabIssue(
        IssueRepositorySnapshot {
            repository: "group/project".into(),
            visibility: IssueVisibility::Full,
            allowed_responders: vec!["maintainer".into()],
        },
    ));
    let effort = fixture.store.effort_for_item("item-1").unwrap().unwrap();
    let cycle = running_cycle("cycle-1", 1);
    start_cycle(&fixture.store, &effort.id, &cycle);
    let identity = ExactEffortIdentity {
        effort_id: effort.id.clone(),
        item_id: effort.item_id.clone(),
        attempt_id: effort.attempt_id.clone(),
        cycle_id: cycle.cycle_id.clone(),
        target_sha: effort.target_sha.clone(),
        source_sha: effort.source_sha.clone(),
        candidate_sha: None,
    };
    fixture
        .store
        .require_guidance(
            &effort.id,
            iq::control_domain::SemanticGuidanceBlocker {
                request_id: "request-1".into(),
                question: "Which contract applies?".into(),
                affected_contracts: vec!["contract".into()],
                affected_paths: vec![EncodedPath::from_bytes(b"file.txt").unwrap()],
                alternatives: iq::control_domain::GuidanceAlternatives::FreeText,
                evidence: "conflict".into(),
                identity: identity.clone(),
            },
        )
        .unwrap();
    let fake = fixture.temp.path().join("glab");
    let log = fixture.temp.path().join("glab.log");
    let exact_answer = serde_json::json!({
        "version": 1,
        "request_id": "request-1",
        "effort_id": identity.effort_id,
        "attempt_id": identity.attempt_id,
        "cycle_id": identity.cycle_id,
        "target_sha": identity.target_sha,
        "source_sha": identity.source_sha,
        "candidate_sha": null,
        "answer": "preserve both contracts"
    });
    let mut unknown_version = exact_answer.clone();
    unknown_version["version"] = serde_json::json!(2);
    let mut stale_effort = exact_answer.clone();
    stale_effort["effort_id"] = serde_json::json!("unknown-effort");
    let mut unauthorized = exact_answer.clone();
    unauthorized["answer"] = serde_json::json!("unauthorized answer");
    let comments = serde_json::json!([
        {"id": 94, "body": exact_answer.to_string(), "author": null},
        {"id": 95, "body": "not JSON", "author": {"username": "maintainer"}},
        {"id": 96, "body": unknown_version.to_string(), "author": {"username": "maintainer"}},
        {"id": 97, "body": stale_effort.to_string(), "author": {"username": "maintainer"}},
        {"id": 98, "body": unauthorized.to_string(), "author": {"username": "outsider"}},
        {"id": 99, "body": exact_answer.to_string(), "author": {"username": "maintainer"}}
    ]);
    std::fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
if [ "$1 $2" = "repo view" ]; then printf '%s' '{{}}'; exit 0; fi
if [ "$1 $2" = "issue create" ]; then printf '%s' '{{"number":7,"url":"https://gitlab.com/group/project/-/issues/7"}}'; exit 0; fi
if [ "$1" = "api" ]; then printf '%s' '{comments}'; exit 0; fi
exit 0
"#,
            log = log.display(),
            comments = comments,
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::env::set_var("IQ_GITLAB_CLI", &fake);

    iq::state_repository::project_item(&fixture.store, "item-1").unwrap();
    let dispositions = iq::state_repository::ingest_answers(&fixture.store, "item-1").unwrap();
    let duplicate_poll = iq::state_repository::ingest_answers(&fixture.store, "item-1").unwrap();
    std::env::remove_var("IQ_GITLAB_CLI");

    assert_eq!(
        dispositions,
        vec![
            iq::control_store::AnswerDisposition::Malformed,
            iq::control_store::AnswerDisposition::Malformed,
            iq::control_store::AnswerDisposition::Malformed,
            iq::control_store::AnswerDisposition::Stale,
            iq::control_store::AnswerDisposition::Unauthorized,
            iq::control_store::AnswerDisposition::Applied,
        ]
    );
    assert!(duplicate_poll.is_empty());
    assert!(matches!(
        fixture
            .store
            .effort_for_item("item-1")
            .unwrap()
            .unwrap()
            .state,
        IntegrationEffortState::AgentReady(_)
    ));
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("issue create"))
            .count(),
        1
    );
}

#[test]
fn failed_terminal_issue_close_waits_for_due_retry_and_closes_same_issue() {
    let _guard = env_lock().lock().unwrap();
    let fixture = effort_fixture_with_repository(StateRepositorySnapshot::GitlabIssue(
        IssueRepositorySnapshot {
            repository: "group/project".into(),
            visibility: IssueVisibility::Full,
            allowed_responders: vec!["maintainer".into()],
        },
    ));
    let fake = fixture.temp.path().join("glab");
    let log = fixture.temp.path().join("glab.log");
    let fail_close = fixture.temp.path().join("fail-close");
    std::fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
if [ "$1 $2" = "repo view" ]; then printf '%s' '{{}}'; exit 0; fi
if [ "$1 $2" = "issue create" ]; then printf '%s' '{{"number":7,"url":"https://gitlab.com/group/project/-/issues/7"}}'; exit 0; fi
if [ "$1 $2" = "issue view" ]; then printf '%s' '{{"labels":[],"comments":[]}}'; exit 0; fi
if [ "$1" = "api" ]; then printf '%s' '[]'; exit 0; fi
if [ "$1 $2" = "issue close" ] && [ -e '{fail_close}' ]; then printf '%s' 'close unavailable' >&2; exit 1; fi
exit 0
"#,
            log = log.display(),
            fail_close = fail_close.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::env::set_var("IQ_GITLAB_CLI", &fake);

    iq::state_repository::project_item(&fixture.store, "item-1").unwrap();
    fixture
        .store
        .cancel(&fixture.effort_id, "test", "cancelled")
        .unwrap();
    std::fs::write(&fail_close, "fail").unwrap();
    assert!(iq::state_repository::project_item(&fixture.store, "item-1").is_err());
    assert!(fixture.store.projection_items(10).unwrap().is_empty());

    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute(
            "UPDATE projection_debt SET next_attempt_at='2000-01-01T00:00:00Z' WHERE effort_id=?1",
            [&fixture.effort_id],
        )
        .unwrap();
    drop(connection);
    std::fs::remove_file(fail_close).unwrap();
    assert_eq!(fixture.store.projection_items(10).unwrap(), vec!["item-1"]);
    iq::state_repository::project_item(&fixture.store, "item-1").unwrap();
    std::env::remove_var("IQ_GITLAB_CLI");

    let artifact = fixture
        .store
        .repository_artifact(&fixture.effort_id)
        .unwrap()
        .unwrap();
    assert_eq!(artifact.artifact_id, "7");
    assert_eq!(artifact.state, "closed");
    assert!(fixture.store.projection_items(10).unwrap().is_empty());
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("issue close 7"))
            .count(),
        2
    );
}

#[test]
fn full_issue_is_reserved_once_at_enqueue_and_transferred_to_effort() {
    let _guard = env_lock().lock().unwrap();
    let fixture = bare_store_fixture();
    let repository = StateRepositorySnapshot::GitlabIssue(IssueRepositorySnapshot {
        repository: "group/project".into(),
        visibility: IssueVisibility::Full,
        allowed_responders: vec!["maintainer".into()],
    });
    let fake = fixture.temp.path().join("glab");
    let log = fixture.temp.path().join("glab.log");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1 $2\" = \"repo view\" ]; then printf '%s' '{{}}'; exit 0; fi\nif [ \"$1 $2\" = \"issue create\" ]; then printf '%s' '{{\"number\":7,\"url\":\"https://gitlab.com/group/project/-/issues/7\"}}'; exit 0; fi\nif [ \"$1 $2\" = \"issue view\" ]; then printf '%s' '{{\"labels\":[],\"comments\":[]}}'; exit 0; fi\nif [ \"$1\" = \"api\" ]; then printf '%s' '[]'; exit 0; fi\nexit 0\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::env::set_var("IQ_GITLAB_CLI", &fake);

    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection.execute(
        "INSERT INTO item_state_repository_bindings(item_id,snapshot_json,provider,repository,visibility,reservation_state,created_at) VALUES('item-1',?1,'gitlab','group/project','full','pending','2026-01-01T00:00:00Z')",
        [serde_json::to_string(&repository).unwrap()],
    ).unwrap();
    drop(connection);
    iq::state_repository::reserve_full_issue(&fixture.store, "item-1").unwrap();
    let workspace = iq::sqlite::WorkspaceIdentity {
        path: fixture
            .temp
            .path()
            .join("rift")
            .to_string_lossy()
            .to_string(),
        rift_id: "rift-1".into(),
        source_rift_id: "source-rift".into(),
    };
    let effort = fixture
        .store
        .create_effort(iq::control_store::NewEffort {
            item_id: "item-1",
            attempt_id: "attempt-1",
            target_sha: &sha('1'),
            source_sha: &sha('2'),
            source_variant: "remote_branch",
            landing_variant: "direct",
            workspace: &workspace,
            runner: &runner_snapshot(),
            state_repository: &repository,
        })
        .unwrap();
    std::env::remove_var("IQ_GITLAB_CLI");

    assert!(fixture
        .store
        .item_repository_reservation("item-1")
        .unwrap()
        .is_none());
    assert_eq!(
        fixture
            .store
            .repository_artifact(&effort.id)
            .unwrap()
            .unwrap()
            .artifact_id,
        "7"
    );
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("issue create"))
            .count(),
        1
    );
}

#[test]
fn reservation_outbox_recovers_pending_enqueue_once() {
    let _guard = env_lock().lock().unwrap();
    let fixture = bare_store_fixture();
    let repository = StateRepositorySnapshot::GitlabIssue(IssueRepositorySnapshot {
        repository: "group/project".into(),
        visibility: IssueVisibility::Full,
        allowed_responders: vec!["maintainer".into()],
    });
    let fake = fixture.temp.path().join("glab");
    let log = fixture.temp.path().join("glab.log");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1 $2\" = \"repo view\" ]; then printf '%s' '{{}}'; exit 0; fi\nif [ \"$1 $2\" = \"issue create\" ]; then printf '%s' '{{\"number\":7,\"url\":\"https://gitlab.com/group/project/-/issues/7\"}}'; exit 0; fi\nexit 0\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::env::set_var("IQ_GITLAB_CLI", &fake);
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection.execute(
        "INSERT INTO item_state_repository_bindings(item_id,snapshot_json,provider,repository,visibility,reservation_state,created_at) VALUES('item-1',?1,'gitlab','group/project','full','pending','2026-01-01T00:00:00Z')",
        [serde_json::to_string(&repository).unwrap()],
    ).unwrap();
    drop(connection);

    assert_eq!(
        iq::state_repository::process_issue_reservation_outbox(&fixture.store, 10).unwrap(),
        1
    );
    assert_eq!(
        iq::state_repository::process_issue_reservation_outbox(&fixture.store, 10).unwrap(),
        0
    );
    std::env::remove_var("IQ_GITLAB_CLI");

    assert_eq!(
        fixture
            .store
            .item_repository_reservation("item-1")
            .unwrap()
            .unwrap()
            .artifact_id,
        "7"
    );
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("issue create"))
            .count(),
        1
    );
}

#[test]
fn exhausted_projection_debt_creates_one_alert_and_one_delivery() {
    let fixture = effort_fixture();
    let fake = fixture.temp.path().join("notify-send");
    std::fs::write(&fake, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = notification_dispatcher(&fixture, fake, 2, 60);
    dispatcher.configure().unwrap();
    for _ in 0..10 {
        fixture
            .store
            .record_projection_debt(&fixture.effort_id, &anyhow::anyhow!("provider unavailable"))
            .unwrap();
    }
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute(
            "UPDATE projection_debt SET created_at='2000-01-01T00:00:00Z' WHERE effort_id=?1",
            [&fixture.effort_id],
        )
        .unwrap();
    drop(connection);

    assert_eq!(
        fixture.store.alert_exhausted_projection_debt(60).unwrap(),
        1
    );
    assert_eq!(
        fixture.store.alert_exhausted_projection_debt(60).unwrap(),
        0
    );
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let alerts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM durable_events WHERE effort_id=?1 AND event_type='projection_debt_exhausted' AND alert=1",
            [&fixture.effort_id],
            |row| row.get(0),
        )
        .unwrap();
    let deliveries: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM notification_deliveries delivery JOIN durable_events event ON event.id=delivery.event_id WHERE event.effort_id=?1 AND event.event_type='projection_debt_exhausted'",
            [&fixture.effort_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!((alerts, deliveries), (1, 1));
}

#[test]
fn minimal_issue_binding_creates_nothing_until_blocked_then_reuses_one_issue() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let queue = iq::sqlite::SqliteQueue::open(&database).unwrap();
    let repository = StateRepositorySnapshot::GitlabIssue(IssueRepositorySnapshot {
        repository: "group/project".into(),
        visibility: IssueVisibility::Minimal,
        allowed_responders: vec!["maintainer".into()],
    });
    let item = queue
        .enqueue(iq::sqlite::EnqueueRequest {
            repo_key: "fixture::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/minimal".into(),
            target_branch: "main".into(),
            current_head_sha: sha('2'),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"test"}),
            state_repository: repository.clone(),
        })
        .unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute(
            "INSERT INTO integration_attempts(id,item_id,attempt_number,source_head_sha,started_at) VALUES('attempt-1',?1,1,?2,'2026-01-01T00:00:00Z')",
            rusqlite::params![item.id, sha('2')],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET current_attempt_id='attempt-1' WHERE id=?1",
            [&item.id],
        )
        .unwrap();
    drop(connection);
    let store = ControlStore::open(&database).unwrap();
    let workspace = iq::sqlite::WorkspaceIdentity {
        path: temp.path().join("rift").to_string_lossy().to_string(),
        rift_id: "rift-1".into(),
        source_rift_id: "source-rift".into(),
    };
    let effort = store
        .create_effort(iq::control_store::NewEffort {
            item_id: &item.id,
            attempt_id: "attempt-1",
            target_sha: &sha('1'),
            source_sha: &sha('2'),
            source_variant: "remote_branch",
            landing_variant: "direct",
            workspace: &workspace,
            runner: &runner_snapshot(),
            state_repository: &repository,
        })
        .unwrap();
    let fake = temp.path().join("glab");
    let log = temp.path().join("glab.log");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1 $2\" = \"repo view\" ]; then printf '%s' '{{}}'; exit 0; fi\nif [ \"$1 $2\" = \"issue create\" ]; then printf '%s' '{{\"number\":7,\"url\":\"https://gitlab.com/group/project/-/issues/7\"}}'; exit 0; fi\nif [ \"$1 $2\" = \"issue view\" ]; then printf '%s' '{{\"labels\":[],\"comments\":[]}}'; exit 0; fi\nif [ \"$1\" = \"api\" ]; then printf '%s' '[]'; exit 0; fi\nexit 0\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::env::set_var("IQ_GITLAB_CLI", &fake);

    iq::state_repository::project_item(&store, &item.id).unwrap();
    assert!(store.repository_artifact(&effort.id).unwrap().is_none());
    store
        .block_infrastructure(
            &effort.id,
            iq::control_domain::InfrastructureBlocker {
                component: InfrastructureComponent::Sandbox,
                operation: "admit runner".into(),
                cause: InfrastructureCause::Unavailable {
                    detail: "sandbox unavailable".into(),
                },
            },
        )
        .unwrap();
    iq::state_repository::project_item(&store, &item.id).unwrap();
    store
        .retry_blocked(
            &effort.id,
            &iq::control_store::ResponderIdentity::LocalPeer {
                uid: unsafe { libc::geteuid() },
            },
            unsafe { libc::geteuid() },
        )
        .unwrap();
    iq::state_repository::project_item(&store, &item.id).unwrap();
    start_cycle(&store, &effort.id, &running_cycle("cycle-1", 1));
    std::env::remove_var("IQ_GITLAB_CLI");

    assert_eq!(
        store
            .repository_artifact(&effort.id)
            .unwrap()
            .unwrap()
            .artifact_id,
        "7"
    );
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("issue create"))
            .count(),
        1
    );
    assert!(store.projection_items(10).unwrap().is_empty());
}

#[test]
fn notification_fake_receives_one_bounded_deduplicated_delivery() {
    let fixture = effort_fixture();
    let fake = fixture.temp.path().join("notify-send");
    let log = fixture.temp.path().join("notify.log");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\nprintf 'invocation\\n' >> '{}'\nprintf '%s' \"$*\" > '{}.payload'\n",
            log.display(),
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = iq::notifications::NotificationDispatcher::new(
        &fixture.database,
        iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Wslg { executable: fake }],
            max_attempts: 2,
            max_event_age_seconds: 60,
            projection_debt_alert_seconds: 60,
        },
    );
    dispatcher.configure().unwrap();
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "integration_blocked",
            serde_json::json!({
                "repository":"fixture::main",
                "blocker_kind":"infrastructure",
                "reason":"sandbox unavailable"
            }),
        )
        .unwrap();
    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    assert_eq!(dispatcher.dispatch_once().unwrap(), 0);
    let delivered = std::fs::read_to_string(log).unwrap();
    assert_eq!(delivered.lines().count(), 1);
    let payload = std::fs::read_to_string(fixture.temp.path().join("notify.log.payload")).unwrap();
    assert!(payload.len() <= 2048);
    assert!(payload.contains("iq show item-1"));
}

#[test]
fn restarted_notification_becomes_unknown_and_runs_only_as_attributed_redelivery() {
    let fixture = effort_fixture();
    let fake = fixture.temp.path().join("notify-send");
    let log = fixture.temp.path().join("notify.log");
    std::fs::write(
        &fake,
        format!("#!/bin/sh\nprintf 'invocation\\n' >> '{}'\n", log.display()),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = iq::notifications::NotificationDispatcher::new(
        &fixture.database,
        iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Wslg { executable: fake }],
            max_attempts: 2,
            max_event_age_seconds: 60,
            projection_debt_alert_seconds: 60,
        },
    );
    dispatcher.configure().unwrap();
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "integration_blocked",
            serde_json::json!({"reason":"sandbox unavailable"}),
        )
        .unwrap();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let delivery_id: i64 = connection
        .query_row(
            "SELECT id FROM notification_deliveries WHERE state='pending'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "UPDATE notification_deliveries SET state='running',claim_id='claim-before-crash',claimed_at='2026-01-01T00:00:00Z' WHERE id=?1",
            [delivery_id],
        )
        .unwrap();
    drop(connection);

    assert_eq!(dispatcher.mark_started_unknown_after_restart().unwrap(), 1);
    assert_eq!(dispatcher.dispatch_once().unwrap(), 0);
    assert!(!log.exists());
    let redelivery_id = dispatcher
        .redeliver(delivery_id, "operator@example.test")
        .unwrap();
    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    assert_eq!(std::fs::read_to_string(log).unwrap(), "invocation\n");

    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let original_state: String = connection
        .query_row(
            "SELECT state FROM notification_deliveries WHERE id=?1",
            [delivery_id],
            |row| row.get(0),
        )
        .unwrap();
    let redelivery: (String, i64, String) = connection
        .query_row(
            "SELECT state,redelivery_of,redelivery_actor FROM notification_deliveries WHERE id=?1",
            [redelivery_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(original_state, "delivery_unknown");
    assert_eq!(
        redelivery,
        (
            "delivered".into(),
            delivery_id,
            "operator@example.test".into()
        )
    );
}

#[test]
fn notification_retry_then_delivery_preserves_attempt_count() {
    let fixture = effort_fixture();
    let fake = fixture.temp.path().join("notify-send");
    let counter = fixture.temp.path().join("attempts");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\ncount=0\ntest ! -f '{0}' || count=$(cat '{0}')\ncount=$((count + 1))\nprintf '%s' \"$count\" > '{0}'\ntest \"$count\" -gt 1\n",
            counter.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = notification_dispatcher(&fixture, fake, 3, 60);
    dispatcher.configure().unwrap();
    record_notification_alert(&fixture);

    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let first: (String, i64) = connection
        .query_row(
            "SELECT state,attempt_count FROM notification_deliveries",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(first, ("pending".into(), 1));
    connection
        .execute(
            "UPDATE notification_deliveries SET next_attempt_at='2000-01-01T00:00:00Z'",
            [],
        )
        .unwrap();
    drop(connection);

    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let final_state: (String, i64) = connection
        .query_row(
            "SELECT state,attempt_count FROM notification_deliveries",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(final_state, ("delivered".into(), 2));
}

#[test]
fn notification_exhaustion_and_expiry_are_terminal_without_extra_invocation() {
    let failed = effort_fixture();
    let failing_fake = failed.temp.path().join("notify-send");
    let failed_log = failed.temp.path().join("failed.log");
    std::fs::write(
        &failing_fake,
        format!(
            "#!/bin/sh\nprintf 'attempt\\n' >> '{}'\nexit 1\n",
            failed_log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&failing_fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = notification_dispatcher(&failed, failing_fake, 2, 60);
    dispatcher.configure().unwrap();
    record_notification_alert(&failed);
    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    let connection = rusqlite::Connection::open(&failed.database).unwrap();
    connection
        .execute(
            "UPDATE notification_deliveries SET next_attempt_at='2000-01-01T00:00:00Z'",
            [],
        )
        .unwrap();
    drop(connection);
    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    let connection = rusqlite::Connection::open(&failed.database).unwrap();
    let exhausted: (String, i64) = connection
        .query_row(
            "SELECT state,attempt_count FROM notification_deliveries",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(exhausted, ("failed".into(), 2));
    assert_eq!(
        std::fs::read_to_string(failed_log).unwrap().lines().count(),
        2
    );

    let expired = effort_fixture();
    let expiry_fake = expired.temp.path().join("notify-send");
    let expiry_log = expired.temp.path().join("expired.log");
    std::fs::write(
        &expiry_fake,
        format!(
            "#!/bin/sh\nprintf 'called\\n' > '{}'\n",
            expiry_log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&expiry_fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = notification_dispatcher(&expired, expiry_fake, 2, 1);
    dispatcher.configure().unwrap();
    record_notification_alert(&expired);
    let connection = rusqlite::Connection::open(&expired.database).unwrap();
    connection
        .execute(
            "UPDATE durable_events SET created_at='2000-01-01T00:00:00Z'",
            [],
        )
        .unwrap();
    drop(connection);
    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    let connection = rusqlite::Connection::open(&expired.database).unwrap();
    let expired_state: String = connection
        .query_row("SELECT state FROM notification_deliveries", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(expired_state, "expired");
    assert!(!expiry_log.exists());
}

#[test]
fn notification_restart_recovers_claimed_before_start() {
    let fixture = effort_fixture();
    let fake = fixture.temp.path().join("notify-send");
    std::fs::write(&fake, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = notification_dispatcher(&fixture, fake, 2, 60);
    dispatcher.configure().unwrap();
    record_notification_alert(&fixture);
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute(
            "UPDATE notification_deliveries SET state='claimed',claim_id='claim-before-start',claimed_at='2026-01-01T00:00:00Z'",
            [],
        )
        .unwrap();
    drop(connection);

    assert_eq!(dispatcher.mark_started_unknown_after_restart().unwrap(), 0);
    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    let state: String = connection
        .query_row("SELECT state FROM notification_deliveries", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(state, "delivered");
}

#[test]
fn windows_notification_uses_fixed_arguments_and_payload_environment() {
    let fixture = effort_fixture();
    let fake = fixture.temp.path().join("powershell.exe");
    let arguments = fixture.temp.path().join("arguments");
    let payload = fixture.temp.path().join("payload");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '%s' \"$IQ_NOTIFICATION_PAYLOAD\" > '{}'\n",
            arguments.display(),
            payload.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dispatcher = iq::notifications::NotificationDispatcher::new(
        &fixture.database,
        iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Windows {
                executable: fake,
            }],
            max_attempts: 2,
            max_event_age_seconds: 60,
            projection_debt_alert_seconds: 60,
        },
    );
    dispatcher.configure().unwrap();
    record_notification_alert(&fixture);
    assert_eq!(dispatcher.dispatch_once().unwrap(), 1);

    let arguments = std::fs::read_to_string(arguments).unwrap();
    let lines = arguments.lines().collect::<Vec<_>>();
    assert_eq!(&lines[..3], ["-NoProfile", "-NonInteractive", "-Command"]);
    assert_eq!(lines.len(), 4);
    assert!(!arguments.contains("sandbox unavailable"));
    let payload = std::fs::read_to_string(payload).unwrap();
    assert!(payload.contains("sandbox unavailable"));
    assert!(payload.contains("iq show item-1"));
}

#[test]
fn unavailable_notification_backend_reports_degraded_health() {
    let fixture = effort_fixture();
    let unavailable = fixture.temp.path().join("missing-notify-send");
    let dispatcher = notification_dispatcher(&fixture, unavailable, 2, 60);
    assert_eq!(
        dispatcher.health(),
        vec![iq::notifications::BackendHealth {
            backend: "wslg",
            available: false,
            detail: format!(
                "unavailable: {}",
                fixture.temp.path().join("missing-notify-send").display()
            ),
        }]
    );
}

#[test]
fn systemd_unit_lookup_distinguishes_loaded_missing_and_failed_queries() {
    let temp = tempdir().unwrap();
    let loaded = temp.path().join("systemctl-loaded");
    std::fs::write(
        &loaded,
        "#!/bin/sh\nprintf 'LoadState=loaded\\nMainPID=42\\n'\n",
    )
    .unwrap();
    std::fs::set_permissions(&loaded, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        iq::agent_runner::systemd_unit_state(&loaded, "iq-agent-cycle-1").unwrap(),
        iq::agent_runner::SystemdUnitState::Loaded { main_pid: Some(42) }
    );

    let missing = temp.path().join("systemctl-missing");
    std::fs::write(
        &missing,
        "#!/bin/sh\nprintf 'LoadState=not-found\\nMainPID=0\\n'\n",
    )
    .unwrap();
    std::fs::set_permissions(&missing, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        iq::agent_runner::systemd_unit_state(&missing, "iq-agent-cycle-1").unwrap(),
        iq::agent_runner::SystemdUnitState::Missing
    );

    let failed = temp.path().join("systemctl-failed");
    std::fs::write(&failed, "#!/bin/sh\nprintf 'bus unavailable' >&2\nexit 1\n").unwrap();
    std::fs::set_permissions(&failed, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = iq::agent_runner::systemd_unit_state(&failed, "iq-agent-cycle-1")
        .unwrap_err()
        .to_string();
    assert!(error.contains("inspect prepared systemd unit failed"));
}

#[test]
fn exact_process_termination_rejects_changed_process_group() {
    let mut child = Command::new("sleep")
        .arg("30")
        .process_group(0)
        .spawn()
        .unwrap();
    let pid = child.id();
    let start_ticks = iq::agent_runner::process_start_ticks(pid).unwrap();
    let process_group = unsafe { libc::getpgid(pid as i32) };
    assert!(process_group > 0);

    let error = iq::agent_runner::terminate_exact_process(pid, start_ticks, process_group + 1)
        .unwrap_err()
        .to_string();
    assert!(error.contains("different process group"));
    assert!(child.try_wait().unwrap().is_none());

    iq::agent_runner::terminate_exact_process(pid, start_ticks, process_group).unwrap();
    child.wait().unwrap();
}

#[test]
fn staged_result_import_reproduces_exact_tree_without_commit() {
    let temp = tempdir().unwrap();
    let retained = temp.path().join("retained");
    git(temp.path(), ["init", retained.to_str().unwrap()]);
    git(&retained, ["config", "user.name", "IQ Test"]);
    git(&retained, ["config", "user.email", "iq@example.test"]);
    git(&retained, ["config", "commit.gpgsign", "false"]);
    let hooks = temp.path().join("hooks");
    std::fs::create_dir(&hooks).unwrap();
    git(
        &retained,
        ["config", "core.hooksPath", hooks.to_str().unwrap()],
    );
    std::fs::write(retained.join("file.txt"), "base\n").unwrap();
    git(&retained, ["add", "file.txt"]);
    git(&retained, ["commit", "-m", "base"]);
    let sandbox = temp.path().join("sandbox");
    git(
        temp.path(),
        [
            "clone",
            retained.to_str().unwrap(),
            sandbox.to_str().unwrap(),
        ],
    );
    std::fs::write(sandbox.join("file.txt"), "integrated\n").unwrap();
    git(&sandbox, ["add", "file.txt"]);
    let export = temp.path().join("export");
    std::fs::create_dir(&export).unwrap();
    std::fs::set_permissions(&export, std::fs::Permissions::from_mode(0o700)).unwrap();
    let patch = git_bytes(&sandbox, ["diff", "--cached", "--binary", "--full-index"]);
    std::fs::write(export.join("staged.patch"), patch).unwrap();
    std::fs::write(export.join("staged.paths"), b"file.txt\0").unwrap();
    std::fs::write(export.join("unstaged.paths"), b"").unwrap();
    let tree = git_text(&sandbox, ["write-tree"]);
    std::fs::write(export.join("staged.tree"), &tree).unwrap();
    let digest = format!("{:x}", sha2::Sha256::digest(tree.trim().as_bytes()));

    iq::agent_runner::import_staged_result(
        &export,
        &retained,
        &digest,
        &[EncodedPath::from_bytes(b"file.txt").unwrap()],
    )
    .unwrap();

    assert_eq!(git_text(&retained, ["write-tree"]), tree);
    assert_eq!(
        git_text(&retained, ["rev-parse", "HEAD"]),
        git_text(&sandbox, ["rev-parse", "HEAD"])
    );
}

#[test]
fn fake_opencode_runs_in_bounded_sandbox_and_exports_typed_result() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let retained = temp.path().join("retained");
    git(temp.path(), ["init", retained.to_str().unwrap()]);
    git(&retained, ["config", "user.name", "IQ Test"]);
    git(&retained, ["config", "user.email", "iq@example.test"]);
    git(&retained, ["config", "commit.gpgsign", "false"]);
    std::fs::write(retained.join("file.txt"), "base\n").unwrap();
    git(&retained, ["add", "file.txt"]);
    git(&retained, ["commit", "-m", "base"]);
    let fake = temp.path().join("opencode");
    std::fs::write(
        &fake,
        r##"#!/bin/sh
set -eu
printf 'integrated\n' > file.txt
git add file.txt
tree=$(git write-tree)
digest=$(printf '%s' "$tree" | sha256sum | cut -d' ' -f1)
result=/iq-protocol/result.json
cat > "$result.tmp" <<EOF
{"outcome":"resolved","version":1,"identity":{"effort_id":"effort-1","item_id":"item-1","attempt_id":"attempt-1","cycle_id":"cycle-1","target_sha":"1111111111111111111111111111111111111111","source_sha":"2222222222222222222222222222222222222222","candidate_sha":null},"staged_tree_sha256":"$digest","changed_paths":[[{"hex":"66696c652e747874"}]],"checks":[]}
EOF
mv "$result.tmp" "$result"
"##,
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let executable = iq::agent_config::executable_identity(&fake).unwrap();
    let mut snapshot = runner_snapshot();
    snapshot.executable = executable;
    snapshot.bounds.max_processes = 16;
    snapshot.bounds.memory_bytes = 256 * 1024 * 1024;
    snapshot.bounds.writable_bytes = 16 * 1024 * 1024;
    snapshot.bounds.open_files = 128;
    let config = iq::agent_config::IntegrationAgentConfig {
        runner: RunnerKind::Opencode,
        executable: fake,
        agent: "iq-integration".into(),
        model: "test/model".into(),
        cycle_timeout_seconds: 30,
        max_log_bytes: 1024 * 1024,
        max_result_bytes: 4096,
        max_processes: 16,
        memory_bytes: 256 * 1024 * 1024,
        cpu_seconds: 30,
        writable_bytes: 16 * 1024 * 1024,
        open_files: 128,
        credential_env: "IQ_TEST_MODEL_KEY".into(),
    };
    std::env::set_var("IQ_TEST_MODEL_KEY", "not-logged");
    let outcome = iq::agent_runner::OpenCodeRunner::new(config, snapshot)
        .unwrap()
        .run(
            &retained,
            &input(),
            &[],
            iq::agent_runner::RunnerLifecycle {
                on_prepared: |_: &str, _: &Path| Ok(()),
                on_started: |_: u32, _: u64, _: i32, _: &str, _: &Path| Ok(()),
                on_writing: |_: &AtomicResultState| Ok(()),
                authority_active: || Ok(true),
            },
        )
        .unwrap();
    std::env::remove_var("IQ_TEST_MODEL_KEY");
    let iq::agent_runner::RunnerOutcome::Complete { result, log, .. } = outcome else {
        panic!("fake runner did not return a complete result: {outcome:?}")
    };
    assert!(matches!(*result, AgentResult::Resolved(_)));
    assert!(!String::from_utf8_lossy(&log).contains("not-logged"));
    assert_eq!(
        std::fs::read_to_string(retained.join("file.txt")).unwrap(),
        "base\n"
    );
}

fn system_config(root: &Path) -> String {
    format!(
        "integration_agent:\n  runner: opencode\n  executable: /bin/true\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 10\n  max_log_bytes: 4096\n  max_result_bytes: 4096\n  max_processes: 4\n  memory_bytes: 67108864\n  cpu_seconds: 10\n  writable_bytes: 1048576\n  open_files: 64\n  credential_env: TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
        root.display()
    )
}

fn notification_dispatcher(
    fixture: &EffortFixture,
    executable: std::path::PathBuf,
    max_attempts: u8,
    max_event_age_seconds: u64,
) -> iq::notifications::NotificationDispatcher {
    iq::notifications::NotificationDispatcher::new(
        &fixture.database,
        iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Wslg { executable }],
            max_attempts,
            max_event_age_seconds,
            projection_debt_alert_seconds: 60,
        },
    )
}

fn record_notification_alert(fixture: &EffortFixture) {
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "integration_blocked",
            serde_json::json!({
                "repository":"fixture::main",
                "blocker_kind":"infrastructure",
                "reason":"sandbox unavailable"
            }),
        )
        .unwrap();
}

struct EffortFixture {
    temp: tempfile::TempDir,
    database: std::path::PathBuf,
    store: ControlStore,
    effort_id: String,
}

fn bare_store_fixture() -> EffortFixture {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let store = ControlStore::open_test_database(&database).unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute(
            "INSERT INTO queue_items(id,current_attempt_id) VALUES('item-1','attempt-1')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO integration_attempts(id,item_id) VALUES('attempt-1','item-1')",
            [],
        )
        .unwrap();
    drop(connection);
    EffortFixture {
        temp,
        database,
        store,
        effort_id: String::new(),
    }
}

fn effort_fixture() -> EffortFixture {
    effort_fixture_with_repository(StateRepositorySnapshot::Local)
}

fn effort_fixture_with_repository(state_repository: StateRepositorySnapshot) -> EffortFixture {
    let fixture = bare_store_fixture();
    let temp = fixture.temp;
    let database = fixture.database;
    let store = fixture.store;
    let workspace = iq::sqlite::WorkspaceIdentity {
        path: temp.path().join("rift").to_string_lossy().to_string(),
        rift_id: "rift-1".into(),
        source_rift_id: "source-rift".into(),
    };
    let effort = store
        .create_effort(iq::control_store::NewEffort {
            item_id: "item-1",
            attempt_id: "attempt-1",
            target_sha: &sha('1'),
            source_sha: &sha('2'),
            source_variant: "remote_branch",
            landing_variant: "direct",
            workspace: &workspace,
            runner: &runner_snapshot(),
            state_repository: &state_repository,
        })
        .unwrap();
    EffortFixture {
        temp,
        database,
        store,
        effort_id: effort.id,
    }
}

fn runner_snapshot() -> RunnerSnapshot {
    RunnerSnapshot {
        kind: RunnerKind::Opencode,
        executable: ExecutableIdentity {
            path: "/bin/true".into(),
            device: 1,
            inode: 1,
            sha256: "a".repeat(64),
        },
        agent: "iq-integration".into(),
        model: "test/model".into(),
        cycle_timeout_seconds: 10,
        bounds: RunnerBounds {
            max_log_bytes: 4096,
            max_result_bytes: 4096,
            max_processes: 4,
            memory_bytes: 64 * 1024 * 1024,
            cpu_seconds: 10,
            writable_bytes: 1024 * 1024,
            open_files: 64,
        },
        sandbox: SandboxIdentity {
            implementation: "linux_userns_tmpfs_overlay_v1".into(),
            bubblewrap: "/usr/bin/bwrap".into(),
            unshare: "/usr/bin/unshare".into(),
            systemd_run: "/usr/bin/systemd-run".into(),
            systemctl: "/usr/bin/systemctl".into(),
        },
        credential_env: "IQ_TEST_MODEL_KEY".into(),
    }
}

fn running_cycle(id: &str, number: u8) -> AgentRunning {
    AgentRunning {
        launch_operation_id: format!("launch-{id}"),
        unit_name: format!("iq-agent-{id}"),
        cycle_id: id.into(),
        cycle_number: number,
        pid: 1,
        process_start_ticks: 1,
        process_group_id: 1,
        authority_lease_id: "lease".into(),
        sandbox_id: "sandbox".into(),
        input_sha256: "a".repeat(64),
        result: AtomicResultState::Absent,
        started_at: chrono::Utc::now().to_rfc3339(),
    }
}

fn launching_cycle(running: &AgentRunning) -> iq::control_domain::AgentLaunching {
    iq::control_domain::AgentLaunching {
        launch_operation_id: running.launch_operation_id.clone(),
        unit_name: running.unit_name.clone(),
        cycle_id: running.cycle_id.clone(),
        cycle_number: running.cycle_number,
        authority_lease_id: running.authority_lease_id.clone(),
        input_sha256: running.input_sha256.clone(),
        protocol_directory: std::path::PathBuf::from("/test/protocol"),
        prepared_at: running.started_at.clone(),
    }
}

fn start_cycle(store: &ControlStore, effort_id: &str, running: &AgentRunning) {
    store
        .prepare_cycle_launch(effort_id, &launching_cycle(running))
        .unwrap();
    store.record_cycle_started(effort_id, running).unwrap();
}

fn git<const N: usize>(cwd: &Path, args: [&str; N]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_text<const N: usize>(cwd: &Path, args: [&str; N]) -> String {
    String::from_utf8(git_bytes(cwd, args)).unwrap()
}

fn git_bytes<const N: usize>(cwd: &Path, args: [&str; N]) -> Vec<u8> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
