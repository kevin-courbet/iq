use iq::agent_protocol::{
    parse_result, AgentInput, AgentResult, LandingVariant, ProtocolLimits, RepositoryIdentity,
    RiftIdentity, SourceVariant,
};
use iq::control_api::{request, ApiEnvelope, ApiRequest, ControlApiServer};
use iq::control_domain::{
    AgentRunning, AtomicResultState, EncodedPath, ExactEffortIdentity, ExecutableIdentity,
    InfrastructureCause, InfrastructureComponent, IntegrationEffortState, IssueRepositorySnapshot,
    IssueVisibility, ProviderGateKind, ProviderGateStatus, ProviderSignoffBlocker, RunnerBounds,
    RunnerKind, RunnerSnapshot, SandboxIdentity, StateRepositorySnapshot,
};
use iq::control_store::ControlStore;
use iq::git_object::GitObjectFormat;
use iq::sqlite::SqliteQueue;
use sha2::Digest;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tempfile::tempdir;
mod support;
use support::{direct_policy, Command, RepositoryFixture};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(debug_assertions)]
struct DatabaseSnapshotPause {
    ready: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}

#[cfg(debug_assertions)]
fn database_snapshot_pause() -> &'static Mutex<Option<DatabaseSnapshotPause>> {
    static PAUSE: OnceLock<Mutex<Option<DatabaseSnapshotPause>>> = OnceLock::new();
    PAUSE.get_or_init(|| Mutex::new(None))
}

#[cfg(debug_assertions)]
fn pause_database_snapshot(_temporary: &Path) {
    let Some(pause) = database_snapshot_pause().lock().unwrap().take() else {
        return;
    };
    pause.ready.send(()).unwrap();
    pause.release.recv().unwrap();
}

#[cfg(debug_assertions)]
fn database_maintenance_blocked() -> &'static Mutex<Option<mpsc::SyncSender<()>>> {
    static BLOCKED: OnceLock<Mutex<Option<mpsc::SyncSender<()>>>> = OnceLock::new();
    BLOCKED.get_or_init(|| Mutex::new(None))
}

#[cfg(debug_assertions)]
fn signal_database_maintenance_blocked(_database: &Path) {
    database_maintenance_blocked()
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .send(())
        .unwrap();
}

fn sha(byte: char) -> String {
    std::iter::repeat_n(byte, 40).collect()
}

fn input() -> AgentInput {
    AgentInput {
        version: 2,
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
            repo_key: "00000000-0000-4000-8000-000000000001".into(),
            target_branch: "main".into(),
            object_format: GitObjectFormat::Sha1,
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
    let unknown = br#"{"outcome":"resolved","version":2,"identity":{"effort_id":"effort-1","item_id":"item-1","attempt_id":"attempt-1","cycle_id":"cycle-1","target_sha":"1111111111111111111111111111111111111111","source_sha":"2222222222222222222222222222222222222222","candidate_sha":null},"staged_tree_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","changed_paths":[],"checks":[],"unknown":true}"#;
    assert!(parse_result(unknown, &input).is_err());

    let mut identity = serde_json::to_value(&input.identity).unwrap();
    identity["item_id"] = serde_json::Value::String("other".into());
    let wrong_identity = serde_json::json!({
        "outcome": "resolved",
        "version": 2,
        "identity": identity,
        "staged_tree_sha256": "a".repeat(64),
        "changed_paths": [],
        "checks": []
    });
    assert!(parse_result(&serde_json::to_vec(&wrong_identity).unwrap(), &input).is_err());
}

#[test]
fn protocol_rejects_object_ids_from_a_different_repository_format() {
    let mut input = input();
    input.repository.object_format = GitObjectFormat::Sha256;
    input.identity.target_sha = "1".repeat(64);
    input.identity.source_sha = "2".repeat(64);
    input.base_sha = "0".repeat(64);
    input.source = SourceVariant::RemoteBranch {
        branch: "feature".into(),
        sha: "2".repeat(64),
    };
    input.validate().unwrap();

    input.identity.target_sha = "1".repeat(40);
    assert!(input.validate().is_err());
}

#[test]
fn interrupted_protocol_cycle_delete_retries_exact_cycle_only() {
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
    assert!(protocol.is_dir());
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();

    iq::agent_protocol::remove_protocol_cycle(&workspace, "cycle-1").unwrap();
    iq::agent_protocol::remove_protocol_cycle(&workspace, "cycle-1").unwrap();
    assert!(!protocol.exists());
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
            scope: "00000000-0000-4000-8000-000000000001".into(),
            registry_identity: "registry".into(),
            generation: 0,
            pending_generation: None,
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
    iq::agent_runner::write_test_sandbox_ownership(&sandbox, "cycle-1").unwrap();
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
        scope: "00000000-0000-4000-8000-000000000001".into(),
        registry_identity: "registry".into(),
        generation: 0,
        pending_generation: None,
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
fn signoff_disposition_requires_one_exact_evidence_variant() {
    use iq::control_domain::SignoffDisposition;

    for valid in [
        serde_json::json!({"kind":"no_validation","policy_digest":"a".repeat(64)}),
        serde_json::json!({"kind":"validation_without_signoff","policy_digest":"b".repeat(64)}),
        serde_json::json!({
            "kind":"evidence",
            "evidence_id":"evidence-1",
            "candidate_sha":"c".repeat(40),
            "policy_digest":"d".repeat(64)
        }),
    ] {
        serde_json::from_value::<SignoffDisposition>(valid).unwrap();
    }
    for invalid in [
        serde_json::json!({"kind":"no_validation"}),
        serde_json::json!({"kind":"validation_without_signoff","policy_digest":"a".repeat(64),"host_policy":true}),
        serde_json::json!({"kind":"not_required","policy_digest":"a".repeat(64)}),
        serde_json::json!({"kind":"evidence","evidence_id":"evidence-1","candidate_sha":"c".repeat(40)}),
    ] {
        assert!(serde_json::from_value::<SignoffDisposition>(invalid).is_err());
    }
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
        "version": 2,
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
    let store = SqliteQueue::open(&database)
        .unwrap()
        .validated_control_store()
        .unwrap();
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
    let (_lifetime, server) = ControlApiServer::bind(config, store).unwrap();
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
#[cfg(debug_assertions)]
fn daemon_lifetime_fences_outlive_api_server_failure() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let store = SqliteQueue::open(&database)
        .unwrap()
        .validated_control_store()
        .unwrap();
    let socket = temp.path().join("control/control.sock");
    let config = iq::agent_config::ControlPlaneConfig {
        unix_socket: socket,
        max_request_bytes: 4096,
        max_free_text_bytes: 1024,
        max_response_bytes: 4096,
        max_concurrent_clients: 2,
        max_client_queue_bytes: 4096,
        max_stream_backlog_events: 100,
        client_idle_seconds: 5,
    };
    let (lifetime, server) = ControlApiServer::bind(config.clone(), store.clone()).unwrap();

    let error = format!("{:#}", server.serve_failure_for_test().unwrap_err());

    assert!(error.contains("simulated IQ control API failure"));
    let second_daemon = match ControlApiServer::bind(config.clone(), store.clone()) {
        Ok(_) => panic!("second daemon acquired live lifetime fences"),
        Err(error) => error,
    };
    assert!(format!("{second_daemon:#}").contains("acquire exclusive IQ daemon lease"));
    let second_database = SqliteQueue::open(&database)
        .expect("validated queue open must coexist with the daemon database lease");
    drop(second_database);

    drop(lifetime);
    let (next_lifetime, next_server) = ControlApiServer::bind(config, store).unwrap();
    drop(next_server);
    drop(next_lifetime);
    drop(SqliteQueue::open(&database).unwrap());
}

#[test]
fn exclusive_database_maintenance_lease_blocks_validated_queue_open() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    drop(SqliteQueue::open(&database).unwrap());
    let maintenance = iq::control_store::DatabaseProcessLease::acquire_exclusive(&database)
        .expect("acquire database maintenance lease");

    let blocked = match SqliteQueue::open(&database) {
        Ok(_) => panic!("validated queue open ignored exclusive database maintenance lease"),
        Err(error) => error,
    };

    assert!(format!("{blocked:#}").contains("acquire shared IQ database process lease"));
    drop(maintenance);
    drop(SqliteQueue::open(&database).unwrap());
}

#[test]
#[cfg(debug_assertions)]
fn first_control_lock_handoff_fences_exclusive_maintenance() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    drop(SqliteQueue::open(&database).unwrap());
    let control_lock = temp.path().join("queue.db.control.lock");
    std::fs::remove_file(&control_lock).unwrap();
    let (snapshot_ready, snapshot_ready_receiver) = mpsc::sync_channel(1);
    let (snapshot_release, snapshot_release_receiver) = mpsc::sync_channel(1);
    *database_snapshot_pause().lock().unwrap() = Some(DatabaseSnapshotPause {
        ready: snapshot_ready,
        release: snapshot_release_receiver,
    });
    iq::control_store::set_database_snapshot_test_hook(&database, Some(pause_database_snapshot));
    let runtime_database = database.clone();
    let runtime = std::thread::spawn(move || SqliteQueue::open(&runtime_database));
    snapshot_ready_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert!(!control_lock.exists());

    let (maintenance_ready, maintenance_ready_receiver) = mpsc::sync_channel(1);
    let (maintenance_release, maintenance_release_receiver) = mpsc::sync_channel(1);
    let (maintenance_blocked, maintenance_blocked_receiver) = mpsc::sync_channel(1);
    *database_maintenance_blocked().lock().unwrap() = Some(maintenance_blocked);
    iq::control_store::set_database_lease_blocked_test_hook(
        &database,
        Some(signal_database_maintenance_blocked),
    );
    let maintenance_database = database.clone();
    let maintenance = std::thread::spawn(move || {
        let lease =
            iq::control_store::DatabaseProcessLease::acquire_exclusive(&maintenance_database)
                .unwrap();
        maintenance_ready.send(()).unwrap();
        maintenance_release_receiver.recv().unwrap();
        drop(lease);
    });
    maintenance_blocked_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert!(matches!(
        maintenance_ready_receiver.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    snapshot_release.send(()).unwrap();
    drop(runtime.join().unwrap().unwrap());
    iq::control_store::set_database_snapshot_test_hook(&database, None);
    iq::control_store::set_database_lease_blocked_test_hook(&database, None);
    maintenance_ready_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert!(control_lock.is_file());
    maintenance_release.send(()).unwrap();
    maintenance.join().unwrap();
}

#[test]
fn api_shutdown_closes_silent_and_watch_clients_and_joins_producer() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let store = SqliteQueue::open(&database)
        .unwrap()
        .validated_control_store()
        .unwrap();
    let socket = temp.path().join("control/control.sock");
    let config = test_control_api_config(socket.clone(), 60, 2);
    let (_lifetime, server) = ControlApiServer::bind(config, store).unwrap();
    let (stop_sender, stop_receiver) = mpsc::channel();
    let (done_sender, done_receiver) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let _ = done_sender.send(server.serve(stop_receiver));
    });
    let mut silent = UnixStream::connect(&socket).unwrap();
    let mut watch_client = UnixStream::connect(&socket).unwrap();
    write_watch_request(&mut watch_client);
    wait_for_control_api_saturation(&socket);

    let started = Instant::now();
    stop_sender.send(()).unwrap();
    let result = done_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("control API shutdown did not finish promptly");
    assert!(result.is_ok(), "{result:?}");
    thread.join().unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_stream_closed(&mut silent);
    assert_stream_closed(&mut watch_client);
}

#[test]
fn api_worker_failure_shuts_down_and_joins_silent_worker() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let store = SqliteQueue::open(&database)
        .unwrap()
        .validated_control_store()
        .unwrap();
    let socket = temp.path().join("control/control.sock");
    let config = test_control_api_config(socket.clone(), 60, 2);
    let (_lifetime, server) = ControlApiServer::bind(config, store).unwrap();
    let (_stop_sender, stop_receiver) = mpsc::channel();
    let (done_sender, done_receiver) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let _ = done_sender.send(server.serve(stop_receiver));
    });
    let mut silent = UnixStream::connect(&socket).unwrap();
    let mut malformed = UnixStream::connect(&socket).unwrap();
    use std::io::Write;
    malformed.write_all(&10_u32.to_be_bytes()).unwrap();
    malformed.write_all(b"x").unwrap();
    malformed.flush().unwrap();
    wait_for_control_api_saturation(&socket);

    malformed.shutdown(std::net::Shutdown::Write).unwrap();
    let result = done_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("worker failure did not stop the control API");
    assert!(result.is_err());
    thread.join().unwrap();
    assert_stream_closed(&mut silent);
}

#[test]
fn api_watch_producer_failure_shuts_down_and_joins_silent_worker() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let store = SqliteQueue::open(&database)
        .unwrap()
        .validated_control_store()
        .unwrap();
    let socket = temp.path().join("control/control.sock");
    let config = test_control_api_config(socket.clone(), 60, 2);
    let (_lifetime, server) = ControlApiServer::bind(config, store).unwrap();
    let (_stop_sender, stop_receiver) = mpsc::channel();
    let (done_sender, done_receiver) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let _ = done_sender.send(server.serve(stop_receiver));
    });
    let mut silent = UnixStream::connect(&socket).unwrap();
    let mut watch_client = UnixStream::connect(&socket).unwrap();
    write_watch_request(&mut watch_client);
    wait_for_control_api_saturation(&socket);
    std::fs::rename(&database, temp.path().join("validated.db")).unwrap();
    std::fs::write(&database, b"replacement\n").unwrap();

    let result = done_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("producer failure did not stop the control API");
    let error = format!("{:#}", result.unwrap_err());
    assert!(
        error.contains("queue database identity changed while IQ was running"),
        "{error}"
    );
    thread.join().unwrap();
    assert_stream_closed(&mut silent);
    assert_stream_closed(&mut watch_client);
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
    let (lifetime, server) = ControlApiServer::bind(config.clone(), fixture.store.clone()).unwrap();
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
    drop(lifetime);

    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "blocked_again",
            serde_json::json!({"reason":"second"}),
        )
        .unwrap();
    let (_lifetime, server) = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
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
    let (_lifetime, server) = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
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
    let (_lifetime, server) = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
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
        .start_validation(&effort.id, &"a".repeat(64))
        .unwrap();
    rusqlite::Connection::open(&fixture.database)
        .unwrap()
        .execute(
            "UPDATE integration_attempts SET validated_commit_sha=?1 WHERE id='attempt-1'",
            [sha('9')],
        )
        .unwrap();
    assert!(fixture
        .store
        .complete_validation(&effort.id, &candidate_sha)
        .is_err());
    rusqlite::Connection::open(&fixture.database)
        .unwrap()
        .execute(
            "UPDATE integration_attempts SET validated_commit_sha=?1 WHERE id='attempt-1'",
            [&candidate_sha],
        )
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
    let (_lifetime, server) = ControlApiServer::bind(config, fixture.store.clone()).unwrap();
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
#[cfg(debug_assertions)]
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
        .start_validation(&effort.id, &"a".repeat(64))
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
        .complete_validation(&effort.id, &candidate_sha)
        .unwrap();
    fixture
        .store
        .begin_landing(
            &effort.id,
            &sha('1'),
            "lease-1",
            "command-1",
            iq::control_domain::SignoffDisposition::NoValidation {
                policy_digest: "a".repeat(64),
            },
        )
        .unwrap();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    fixture
        .store
        .begin_target_move(&effort.id, &sha('5'))
        .unwrap();
    fixture
        .store
        .prepare_target_recomposition(&effort.id, &sha('5'))
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    connection
        .execute("DELETE FROM private_ref_cleanup_debt", [])
        .unwrap();
    iq::control_store::set_recomposition_projection_failure_test_hook(&fixture.database, true);

    assert!(fixture
        .store
        .complete_target_recomposition(
            &effort.id,
            &sha('5'),
            &serde_json::json!({"target_sha":sha('5')})
        )
        .is_err());

    let effort_after_failure = fixture.store.effort_for_item("item-1").unwrap().unwrap();
    assert!(matches!(
        effort_after_failure.state,
        IntegrationEffortState::TargetMovePending(ref pending)
            if pending.target_sha == sha('5')
                && pending.source_sha == sha('2')
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
        serde_json::json!({"state":"ready"})
    );
    assert_eq!(target_sha.as_deref(), Some(sha('5').as_str()));
    assert_eq!(validated_sha.as_deref(), Some(candidate_sha.as_str()));
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM candidate_evidence", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );

    fixture
        .store
        .complete_target_recomposition(
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
fn released_landing_authority_rejects_every_destructive_runtime_transition() {
    let fixture = released_landing_fixture();
    let before = fixture.store.effort_for_item("item-1").unwrap().unwrap();

    for error in [
        fixture
            .store
            .cancel(&fixture.effort_id, "operator", "operator_cancelled")
            .unwrap_err(),
        fixture
            .store
            .begin_target_move(&fixture.effort_id, &sha('5'))
            .unwrap_err(),
        fixture
            .store
            .prepare_target_recomposition(&fixture.effort_id, &sha('5'))
            .unwrap_err(),
        fixture
            .store
            .reject_candidate(&fixture.effort_id, "candidate defect")
            .unwrap_err(),
    ] {
        assert!(
            format!("{error:#}").contains("landing authority"),
            "{error:#}"
        );
    }
    assert_eq!(
        fixture.store.effort_for_item("item-1").unwrap().unwrap(),
        before
    );
}

#[test]
fn cancellation_detects_released_landing_authority_inside_valid_blocked_states() {
    for blocker in ["infrastructure", "provider"] {
        let fixture = released_landing_fixture();
        match blocker {
            "infrastructure" => fixture
                .store
                .block_infrastructure(
                    &fixture.effort_id,
                    iq::control_domain::InfrastructureBlocker {
                        component: InfrastructureComponent::Filesystem,
                        operation: "landing".into(),
                        cause: InfrastructureCause::Interrupted {
                            detail: "restart required".into(),
                        },
                    },
                )
                .unwrap(),
            "provider" => fixture
                .store
                .block_provider(
                    &fixture.effort_id,
                    ProviderSignoffBlocker {
                        gate: ProviderGateKind::Provider,
                        repository: "org/repository".into(),
                        context: "landing".into(),
                        candidate_sha: sha('3'),
                        status: ProviderGateStatus::Pending,
                        evidence: "provider response is pending".into(),
                    },
                )
                .unwrap(),
            _ => unreachable!(),
        }
        let before = fixture.store.effort_for_item("item-1").unwrap().unwrap();

        let error = fixture
            .store
            .cancel(&fixture.effort_id, "operator", "operator_cancelled")
            .unwrap_err();

        assert!(
            format!("{error:#}").contains("landing authority"),
            "{error:#}"
        );
        assert_eq!(
            fixture.store.effort_for_item("item-1").unwrap().unwrap(),
            before,
            "cancellation changed {blocker} landing authority"
        );
    }
}

struct TestService {
    wrapper: std::process::Child,
    unit_name: String,
    control_group: String,
    pid: u32,
    process_start_ticks: u64,
}

impl TestService {
    #[allow(clippy::zombie_processes)]
    fn start(cycle_id: &str, script: &str) -> Self {
        let unit_name = iq::control_domain::systemd_unit_name(cycle_id).unwrap();
        let mut wrapper = Command::new("/usr/bin/systemd-run")
            .args([
                "--user",
                "--quiet",
                "--collect",
                "--wait",
                "--pipe",
                "--property=Type=exec",
                &format!("--unit={unit_name}"),
                "--",
                "/bin/sh",
                "-c",
                script,
            ])
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let output = Command::new("/usr/bin/systemctl")
                .args([
                    "--user",
                    "show",
                    &unit_name,
                    "--property=LoadState,ActiveState,MainPID,ControlGroup",
                    "--no-pager",
                ])
                .output()
                .unwrap();
            let properties = String::from_utf8(output.stdout).unwrap();
            let control_group = properties
                .lines()
                .find_map(|line| line.strip_prefix("ControlGroup="))
                .filter(|value| !value.is_empty());
            if let (true, Some(control_group)) =
                (properties.contains("ActiveState=active"), control_group)
            {
                let control_group = control_group.to_string();
                let pid = properties
                    .lines()
                    .find_map(|line| line.strip_prefix("MainPID="))
                    .and_then(|value| value.parse::<u32>().ok())
                    .filter(|pid| *pid != 0);
                if let Some(pid) = pid {
                    return Self {
                        wrapper,
                        unit_name,
                        control_group,
                        pid,
                        process_start_ticks: iq::agent_runner::process_start_ticks(pid).unwrap(),
                    };
                }
            }
            if Instant::now() >= deadline {
                let _ = Command::new("/usr/bin/systemctl")
                    .args(["--user", "stop", &unit_name])
                    .status();
                let _ = wrapper.wait();
                panic!("systemd service did not become active");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait(&mut self) {
        self.wrapper.wait().unwrap();
    }
}

impl Drop for TestService {
    fn drop(&mut self) {
        if self.wrapper.try_wait().ok().flatten().is_none() {
            let _ = Command::new("/usr/bin/systemctl")
                .args(["--user", "stop", &self.unit_name])
                .status();
        }
        let _ = self.wrapper.wait();
    }
}

#[test]
fn cancelled_running_cycle_is_terminated_from_restart_debt() {
    let cycle_id = format!("cancel-{}", uuid::Uuid::new_v4());
    let mut service = TestService::start(&cycle_id, "exec /bin/sleep 30");
    let fixture = effort_fixture();
    let effort = fixture.store.effort_for_item("item-1").unwrap().unwrap();
    let mut running = running_cycle(&cycle_id, 1);
    running.pid = service.pid;
    running.process_start_ticks = service.process_start_ticks;
    running.control_group = service.control_group.clone();
    start_cycle(&fixture.store, &effort.id, &running);

    fixture
        .store
        .cancel(&effort.id, "test", "operator_cancelled")
        .unwrap();
    assert_eq!(
        fixture
            .store
            .reconcile_cancelled_runner_terminations()
            .unwrap(),
        1
    );
    service.wait();
    assert!(
        !iq::agent_runner::exact_process_is_alive(service.pid, service.process_start_ticks)
            .unwrap()
    );
    let debt: i64 = rusqlite::Connection::open(&fixture.database)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(debt, 0);
}

#[test]
fn cancelled_running_cycle_kills_descendant_in_a_different_process_group() {
    let root = tempdir().unwrap();
    let descendant_pid_path = root.path().join("descendant.pid");
    let cycle_id = format!("cancel-{}", uuid::Uuid::new_v4());
    let script = format!(
        "setsid /bin/sleep 30 & printf '%s' \"$!\" > '{}'; exec /bin/sleep 30",
        descendant_pid_path.display()
    );
    let mut service = TestService::start(&cycle_id, &script);
    let deadline = Instant::now() + Duration::from_secs(2);
    while !descendant_pid_path.is_file() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    let descendant_pid = std::fs::read_to_string(&descendant_pid_path)
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let descendant_start = iq::agent_runner::process_start_ticks(descendant_pid).unwrap();
    let fixture = effort_fixture();
    let mut running = running_cycle(&cycle_id, 1);
    running.pid = service.pid;
    running.process_start_ticks = service.process_start_ticks;
    running.control_group = service.control_group.clone();
    start_cycle(&fixture.store, &fixture.effort_id, &running);
    fixture
        .store
        .cancel(&fixture.effort_id, "test", "operator_cancelled")
        .unwrap();

    assert!(fixture
        .store
        .reconcile_cancelled_runner_termination(&fixture.effort_id)
        .unwrap());
    service.wait();
    assert!(!iq::agent_runner::exact_process_is_alive(descendant_pid, descendant_start).unwrap());
}

#[test]
fn cancel_cli_fails_explicitly_and_retains_unconfirmed_launch_termination_debt() {
    let helper_root = tempdir().unwrap();
    let systemctl = helper_root.path().join("systemctl");
    let fail = helper_root.path().join("fail");
    let stopped = helper_root.path().join("stopped");
    std::fs::write(
        &systemctl,
        format!(
            "#!/bin/sh\ncase \"$2\" in\nshow) if [ -f '{}' ]; then printf 'LoadState=not-found\\nMainPID=0\\n'; else printf 'LoadState=loaded\\nActiveState=active\\nMainPID=0\\n'; fi ;;\nstop) [ ! -f '{}' ] || exit 7; : > '{}' ;;\n*) exit 8 ;;\nesac\n",
            stopped.display(),
            fail.display(),
            stopped.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(&fail, b"fail\n").unwrap();
    let mut runner = runner_snapshot();
    runner.sandbox.systemctl = iq::agent_config::executable_identity(&systemctl).unwrap();
    let fixture = effort_fixture_with_repository_and_runner(StateRepositorySnapshot::Local, runner);
    let running = running_cycle("cycle-cli-cancel", 1);
    fixture
        .store
        .prepare_cycle_launch(&fixture.effort_id, &launching_cycle(&running))
        .unwrap();
    assert!(fixture
        .store
        .surrender_cycle_spawn_authority(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap());

    let failed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&fixture.database)
        .args(["cancel", "item-1"])
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert!(
        String::from_utf8_lossy(&failed.stderr).contains("stop prepared systemd unit failed"),
        "{}",
        String::from_utf8_lossy(&failed.stderr)
    );
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT status FROM queue_items WHERE id='item-1'",
                [],
                |row| { row.get::<_, String>(0) }
            )
            .unwrap(),
        "cancelled"
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );

    let early_acknowledgement = fixture
        .store
        .acknowledge_cycle_spawn_failed(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap_err();
    assert!(
        format!("{early_acknowledgement:#}").contains("still active"),
        "{early_acknowledgement:#}"
    );
    std::fs::remove_file(fail).unwrap();
    let retried = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&fixture.database)
        .args(["cancel", "item-1"])
        .output()
        .unwrap();
    assert!(!retried.status.success());
    assert!(
        String::from_utf8_lossy(&retried.stderr).contains("durable termination debt remains"),
        "{}",
        String::from_utf8_lossy(&retried.stderr)
    );
    fixture
        .store
        .acknowledge_cycle_spawn_failed(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap();
    let completed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&fixture.database)
        .args(["cancel", "item-1"])
        .output()
        .unwrap();
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn cancellation_after_spawn_surrender_rejects_started_until_launcher_closes_authority() {
    let fixture = effort_fixture();
    let running = running_cycle("cycle-cancelled-before-start-record", 1);
    fixture
        .store
        .prepare_cycle_launch(&fixture.effort_id, &launching_cycle(&running))
        .unwrap();
    assert!(fixture
        .store
        .surrender_cycle_spawn_authority(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap());

    fixture
        .store
        .cancel(&fixture.effort_id, "test", "operator_cancelled")
        .unwrap();
    assert!(!fixture
        .store
        .record_cycle_started(
            &fixture.effort_id,
            &running,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap());
    assert!(!fixture
        .store
        .reconcile_cancelled_runner_termination(&fixture.effort_id)
        .unwrap());
    assert_eq!(
        rusqlite::Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );

    fixture
        .store
        .acknowledge_cycle_spawn_failed(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap();
    assert!(fixture
        .store
        .reconcile_cancelled_runner_termination(&fixture.effort_id)
        .unwrap());
    assert_eq!(
        rusqlite::Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn live_paused_launcher_retains_spawn_authority_until_exact_process_death() {
    let fixture = effort_fixture();
    let mut launcher = Command::new("sleep").arg("30").spawn().unwrap();
    let mut running = running_cycle("cycle-launcher-liveness", 1);
    running.launcher.pid = launcher.id();
    running.launcher.process_start_ticks =
        iq::agent_runner::process_start_ticks(launcher.id()).unwrap();
    fixture
        .store
        .prepare_cycle_launch(&fixture.effort_id, &launching_cycle(&running))
        .unwrap();
    fixture
        .store
        .surrender_cycle_spawn_authority(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap();
    unsafe { libc::kill(launcher.id() as i32, libc::SIGSTOP) };

    assert!(fixture
        .store
        .authorize_cycle_spawn(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap());
    assert!(fixture
        .store
        .reset_prepared_launch(&fixture.effort_id, &running.cycle_id)
        .unwrap_err()
        .to_string()
        .contains("live launcher"));

    unsafe {
        libc::kill(launcher.id() as i32, libc::SIGCONT);
        libc::kill(launcher.id() as i32, libc::SIGKILL);
    }
    launcher.wait().unwrap();
    assert!(!fixture
        .store
        .authorize_cycle_spawn(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap());
    fixture
        .store
        .reset_prepared_launch(&fixture.effort_id, &running.cycle_id)
        .unwrap();
}

#[test]
fn spawn_authorization_rejects_stale_launcher_token_and_pid_reuse_identity() {
    let fixture = effort_fixture();
    let running = running_cycle("cycle-stale-launcher", 1);
    fixture
        .store
        .prepare_cycle_launch(&fixture.effort_id, &launching_cycle(&running))
        .unwrap();
    fixture
        .store
        .surrender_cycle_spawn_authority(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap();
    let mut stale_token = running.launcher.clone();
    stale_token.token = "launcher-stale-token".into();

    assert!(fixture
        .store
        .authorize_cycle_spawn(
            &fixture.effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &stale_token,
        )
        .unwrap_err()
        .to_string()
        .contains("differs from surrendered authority"));
    assert!(!iq::agent_runner::exact_process_is_alive(
        running.launcher.pid,
        running.launcher.process_start_ticks + 1,
    )
    .unwrap());
}

#[test]
fn cancellation_before_spawn_keeps_debt_while_exact_launcher_is_paused() {
    let helper_root = tempdir().unwrap();
    let systemctl = helper_root.path().join("systemctl");
    let log = helper_root.path().join("systemctl.log");
    std::fs::write(
        &systemctl,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nprintf 'LoadState=not-found\\nMainPID=0\\n'\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut runner = runner_snapshot();
    runner.sandbox.systemctl = iq::agent_config::executable_identity(&systemctl).unwrap();
    let fixture = effort_fixture_with_repository_and_runner(StateRepositorySnapshot::Local, runner);
    let mut launcher = Command::new("sleep").arg("30").spawn().unwrap();
    let mut running = running_cycle("cycle-close-race", 1);
    running.launcher.pid = launcher.id();
    running.launcher.process_start_ticks =
        iq::agent_runner::process_start_ticks(launcher.id()).unwrap();
    fixture
        .store
        .prepare_cycle_launch(&fixture.effort_id, &launching_cycle(&running))
        .unwrap();
    unsafe { libc::kill(launcher.id() as i32, libc::SIGSTOP) };
    fixture
        .store
        .cancel(&fixture.effort_id, "test", "cancel-before-spawn")
        .unwrap();

    assert!(!fixture
        .store
        .reconcile_cancelled_runner_termination(&fixture.effort_id)
        .unwrap());
    assert_eq!(
        rusqlite::Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    unsafe {
        libc::kill(launcher.id() as i32, libc::SIGCONT);
        libc::kill(launcher.id() as i32, libc::SIGKILL);
    }
    launcher.wait().unwrap();
    assert!(fixture
        .store
        .reconcile_cancelled_runner_termination(&fixture.effort_id)
        .unwrap());
    assert_eq!(
        rusqlite::Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    assert!(std::fs::read_to_string(log).unwrap().contains(
        "--user show --property=LoadState --property=ActiveState --property=MainPID --property=ControlGroup -- iq-agent-cycle-close-race.service"
    ));
}

#[test]
fn schema4_open_rejects_corrupt_termination_cycle_and_unit_pair() {
    let fixture = effort_fixture();
    let running = running_cycle("cycle-corrupt-debt", 1);
    fixture
        .store
        .prepare_cycle_launch(&fixture.effort_id, &launching_cycle(&running))
        .unwrap();
    fixture
        .store
        .cancel(&fixture.effort_id, "test", "create-debt")
        .unwrap();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute(
            "UPDATE runner_termination_debt SET authority_json=json_set(authority_json,'$.payload.unit_name','iq-agent-other-cycle.service') WHERE effort_id=?1",
            [&fixture.effort_id],
        )
        .unwrap();
    drop(connection);

    let error = match iq::sqlite::SqliteQueue::open(&fixture.database) {
        Ok(_) => panic!("corrupt termination authority opened successfully"),
        Err(error) => error,
    };
    assert!(
        format!("{error:#}").contains("exact cycle authority"),
        "{error:#}"
    );
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
// Controlled CLI scripts verify IQ's legacy state-repository contract, not provider service behavior.
fn controlled_gitlab_cli_contract_records_one_disposition_and_exact_resume_authority() {
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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Gitlab,
        &fake,
    )
    .unwrap();

    iq::state_repository::project_item(&fixture.store, "item-1").unwrap();
    let dispositions = iq::state_repository::ingest_answers(&fixture.store, "item-1").unwrap();
    let duplicate_poll = iq::state_repository::ingest_answers(&fixture.store, "item-1").unwrap();

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
fn controlled_gitlab_cli_contract_retries_terminal_issue_close() {
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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Gitlab,
        &fake,
    )
    .unwrap();

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
fn controlled_gitlab_cli_contract_reserves_full_issue_once() {
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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Gitlab,
        &fake,
    )
    .unwrap();

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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Gitlab,
        &fake,
    )
    .unwrap();
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
fn controlled_gitlab_cli_contract_reuses_minimal_blocked_issue() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let fixture = RepositoryFixture::new(temp.path(), &database);
    let head = fixture.create_branch("agent/minimal");
    let repository = StateRepositorySnapshot::GitlabIssue(IssueRepositorySnapshot {
        repository: "group/project".into(),
        visibility: IssueVisibility::Minimal,
        allowed_responders: vec!["maintainer".into()],
    });
    let item = fixture
        .manager
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: fixture.repository.key.clone(),
            source_branch: "agent/minimal".into(),
            current_head_sha: head,
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
            "UPDATE queue_items SET status='merging',current_attempt_id='attempt-1' WHERE id=?1",
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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Gitlab,
        &fake,
    )
    .unwrap();

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
    let dispatcher = SqliteQueue::open(&fixture.database)
        .unwrap()
        .notification_dispatcher(iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Wslg { executable: fake }],
            max_attempts: 2,
            max_event_age_seconds: 60,
            projection_debt_alert_seconds: 60,
        })
        .unwrap();
    dispatcher.configure().unwrap();
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "integration_blocked",
            serde_json::json!({
                "repository":"00000000-0000-4000-8000-000000000001",
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
fn notification_dispatcher_rejects_replaced_database_before_access() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("queue.db");
    let queue = SqliteQueue::open(&database).unwrap();
    let dispatcher = queue
        .notification_dispatcher(iq::agent_config::NotificationConfig::default())
        .unwrap();
    drop(queue);
    std::fs::rename(&database, temp.path().join("validated.db")).unwrap();
    let replacement = b"replacement is not an IQ database\n";
    std::fs::write(&database, replacement).unwrap();

    let configure = format!("{:#}", dispatcher.configure().unwrap_err());
    let dispatch = format!("{:#}", dispatcher.dispatch_once().unwrap_err());

    assert!(configure.contains("queue database identity changed while IQ was running"));
    assert!(dispatch.contains("queue database identity changed while IQ was running"));
    assert_eq!(std::fs::read(&database).unwrap(), replacement);
    assert!(!database.with_extension("db-wal").exists());
    assert!(!database.with_extension("db-shm").exists());
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
    let dispatcher = SqliteQueue::open(&fixture.database)
        .unwrap()
        .notification_dispatcher(iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Wslg { executable: fake }],
            max_attempts: 2,
            max_event_age_seconds: 60,
            projection_debt_alert_seconds: 60,
        })
        .unwrap();
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
    let dispatcher = SqliteQueue::open(&fixture.database)
        .unwrap()
        .notification_dispatcher(iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Windows {
                executable: fake,
            }],
            max_attempts: 2,
            max_event_age_seconds: 60,
            projection_debt_alert_seconds: 60,
        })
        .unwrap();
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
    let publish_executable = |path: &Path, body: &str| {
        let staged = path.with_extension("new");
        std::fs::write(&staged, body).unwrap();
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::rename(staged, path).unwrap();
    };
    let loaded = temp.path().join("systemctl-loaded");
    let arguments = temp.path().join("systemctl-arguments");
    publish_executable(
        &loaded,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$2\" = show ]; then printf 'LoadState=loaded\\nActiveState=active\\nMainPID=42\\n'; fi\n",
            arguments.display()
        ),
    );
    let loaded_identity = iq::agent_config::executable_identity(&loaded).unwrap();
    assert_eq!(
        iq::agent_runner::systemd_unit_state(&loaded_identity, "iq-agent-cycle-1.service").unwrap(),
        iq::agent_runner::SystemdUnitState::Active { main_pid: Some(42) }
    );
    iq::agent_runner::stop_systemd_unit(&loaded_identity, "iq-agent-cycle-1.service").unwrap();
    assert_eq!(
        std::fs::read_to_string(&arguments).unwrap(),
        "--user show --property=LoadState --property=ActiveState --property=MainPID --property=ControlGroup -- iq-agent-cycle-1.service\n--user stop --no-block -- iq-agent-cycle-1.service\n"
    );
    let before = std::fs::read(&arguments).unwrap();
    for unit in [
        "iq-agent-cycle-*.service",
        "iq-agent--cycle.service",
        "unrelated.service",
    ] {
        assert!(iq::agent_runner::systemd_unit_state(&loaded_identity, unit).is_err());
    }
    assert_eq!(std::fs::read(&arguments).unwrap(), before);

    let missing = temp.path().join("systemctl-missing");
    publish_executable(
        &missing,
        "#!/bin/sh\nprintf 'LoadState=not-found\\nMainPID=0\\n'\n",
    );
    let missing_identity = iq::agent_config::executable_identity(&missing).unwrap();
    assert_eq!(
        iq::agent_runner::systemd_unit_state(&missing_identity, "iq-agent-cycle-1.service")
            .unwrap(),
        iq::agent_runner::SystemdUnitState::Missing
    );

    let failed = temp.path().join("systemctl-failed");
    publish_executable(&failed, "#!/bin/sh\nprintf 'bus unavailable' >&2\nexit 1\n");
    let failed_identity = iq::agent_config::executable_identity(&failed).unwrap();
    let error = iq::agent_runner::systemd_unit_state(&failed_identity, "iq-agent-cycle-1.service")
        .unwrap_err()
        .to_string();
    assert!(error.contains("inspect prepared systemd unit failed"));
}

#[test]
fn executable_identity_rejects_content_and_inode_replacement() {
    let temp = tempdir().unwrap();
    let executable = temp.path().join("tool");
    std::fs::write(&executable, b"#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let identity = iq::agent_config::executable_identity(&executable).unwrap();

    std::fs::write(&executable, b"#!/bin/sh\nexit 1\n").unwrap();
    let content_error = iq::agent_config::verify_executable(&identity).unwrap_err();
    assert!(content_error
        .to_string()
        .contains("approved executable identity changed"));

    let identity = iq::agent_config::executable_identity(&executable).unwrap();
    let replacement = temp.path().join("replacement");
    std::fs::write(&replacement, b"#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(replacement, &executable).unwrap();
    let inode_error = iq::agent_config::verify_executable(&identity).unwrap_err();
    assert!(inode_error
        .to_string()
        .contains("approved executable identity changed"));
}

#[test]
fn sandbox_cleanup_rejects_replacement_and_symlink_roots() {
    let temp = tempdir().unwrap();
    let root = temp.path().join(".iq-agent-sandbox-cycle-replacement");
    let export = root.join("export");
    std::fs::create_dir_all(&export).unwrap();
    iq::agent_runner::write_test_sandbox_ownership(&root, "cycle-replacement").unwrap();
    let original = temp.path().join("original");
    std::fs::rename(&root, &original).unwrap();
    std::fs::create_dir_all(&export).unwrap();
    std::fs::copy(
        original.join(".iq-sandbox-owner.json"),
        root.join(".iq-sandbox-owner.json"),
    )
    .unwrap();
    std::fs::write(root.join("replacement-data"), b"keep\n").unwrap();

    let replacement_error = iq::agent_runner::remove_sandbox_export(&export).unwrap_err();
    assert!(replacement_error
        .to_string()
        .contains("ownership manifest differs"));
    assert_eq!(
        std::fs::read(root.join("replacement-data")).unwrap(),
        b"keep\n"
    );

    std::fs::remove_dir_all(&root).unwrap();
    std::os::unix::fs::symlink(&original, &root).unwrap();
    let symlink_error = iq::agent_runner::remove_sandbox_export(&export).unwrap_err();
    assert!(format!("{symlink_error:#}").contains("open sandbox root"));
    assert!(original.join("export").is_dir());
}

#[test]
fn staged_result_import_reproduces_exact_tree_without_commit() {
    let temp = tempdir().unwrap();
    let retained = temp.path().join("retained");
    git(temp.path(), ["init", retained.to_str().unwrap()]);
    git(&retained, ["config", "user.name", "IQ Test"]);
    git(&retained, ["config", "user.email", "iq@example.test"]);
    git(&retained, ["config", "commit.gpgsign", "false"]);
    std::fs::write(retained.join("file.txt"), "base\n").unwrap();
    git(&retained, ["add", "file.txt"]);
    git(&retained, ["commit", "-m", "base"]);
    iq::git_command::authorize_current(&retained).unwrap();
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
    iq::git_command::authorize_current(&retained).unwrap();
    let fake = temp.path().join("opencode");
    std::fs::write(
        &fake,
        r##"#!/bin/sh
set -eu
prompt=
for argument in "$@"; do prompt=$argument; done
version=$(expr "$prompt" : '.*protocol version \([0-9][0-9]*\) JSON')
test -n "$version"
test "$IQ_GIT_EXECUTABLE" = /iq-git
test "$(command -v git)" = /iq-bin/git
printf 'integrated\n' > file.txt
git add file.txt
tree=$(git write-tree)
digest=$(printf '%s' "$tree" | sha256sum | cut -d' ' -f1)
result=/iq-protocol/result.json
cat > "$result.tmp" <<EOF
    {"outcome":"resolved","version":$version,"identity":{"effort_id":"effort-1","item_id":"item-1","attempt_id":"attempt-1","cycle_id":"cycle-1","target_sha":"1111111111111111111111111111111111111111","source_sha":"2222222222222222222222222222222222222222","candidate_sha":null},"staged_tree_sha256":"$digest","changed_paths":[[{"hex":"66696c652e747874"}]],"checks":[]}
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
    let hostile_marker = temp.path().join("sandbox-helper-inherited-git-env");
    let hostile_diff = temp.path().join("hostile-diff");
    std::fs::write(
        &hostile_diff,
        format!("#!/bin/sh\n: > '{}'\n", hostile_marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&hostile_diff, std::fs::Permissions::from_mode(0o755)).unwrap();
    let hostile_path = temp.path().join("hostile-path");
    std::fs::create_dir(&hostile_path).unwrap();
    let hostile_git_marker = temp.path().join("hostile-path-git-executed");
    let hostile_git = hostile_path.join("git");
    std::fs::write(
        &hostile_git,
        format!(
            "#!/bin/sh\n: > '{}'\nexit 91\n",
            hostile_git_marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&hostile_git, std::fs::Permissions::from_mode(0o755)).unwrap();
    let original_path = std::env::var_os("PATH");
    std::env::set_var("IQ_TEST_MODEL_KEY", "not-logged");
    std::env::set_var("GIT_EXTERNAL_DIFF", &hostile_diff);
    std::env::set_var("PATH", format!("{}:/usr/bin:/bin", hostile_path.display()));
    let outcome = iq::agent_runner::OpenCodeRunner::new(config, snapshot)
        .unwrap()
        .run(
            &retained,
            &input(),
            &[],
            iq::agent_runner::RunnerLifecycle {
                on_prepared: |_: &str, _: &Path| Ok(()),
                on_spawn_surrender: || Ok(true),
                recheck_spawn_authority: || Ok(true),
                on_spawn_failed: || Ok(()),
                on_started: |_: u32, _: u64, _: &str, _: &str, _: &Path| Ok(true),
                on_writing: |_: &AtomicResultState| Ok(()),
                authority_active: || Ok(true),
            },
        )
        .unwrap();
    std::env::remove_var("IQ_TEST_MODEL_KEY");
    std::env::remove_var("GIT_EXTERNAL_DIFF");
    match original_path {
        Some(path) => std::env::set_var("PATH", path),
        None => std::env::remove_var("PATH"),
    }
    let iq::agent_runner::RunnerOutcome::Complete { result, log, .. } = outcome else {
        panic!("fake runner did not return a complete result: {outcome:?}")
    };
    assert!(matches!(*result, AgentResult::Resolved(_)));
    assert!(!String::from_utf8_lossy(&log).contains("not-logged"));
    assert!(!hostile_marker.exists());
    assert!(!hostile_git_marker.exists());
    assert_eq!(
        std::fs::read_to_string(retained.join("file.txt")).unwrap(),
        "base\n"
    );
}

#[test]
fn failed_post_spawn_start_record_stops_service_and_closes_surrendered_authority() {
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
    iq::git_command::authorize_current(&retained).unwrap();
    let fake = temp.path().join("opencode");
    std::fs::write(&fake, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let executable = iq::agent_config::executable_identity(&fake).unwrap();
    let mut snapshot = runner_snapshot();
    snapshot.executable = executable;
    snapshot.bounds.max_processes = 16;
    snapshot.bounds.memory_bytes = 256 * 1024 * 1024;
    let config = iq::agent_config::IntegrationAgentConfig {
        runner: RunnerKind::Opencode,
        executable: fake,
        agent: "iq-integration".into(),
        model: "test/model".into(),
        cycle_timeout_seconds: 30,
        max_log_bytes: 4096,
        max_result_bytes: 4096,
        max_processes: 16,
        memory_bytes: 256 * 1024 * 1024,
        cpu_seconds: 30,
        writable_bytes: 16 * 1024 * 1024,
        open_files: 128,
        credential_env: "IQ_TEST_MODEL_KEY".into(),
    };
    let spawn_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = spawn_failed.clone();
    std::env::set_var("IQ_TEST_MODEL_KEY", "not-logged");

    let error = iq::agent_runner::OpenCodeRunner::new(config, snapshot.clone())
        .unwrap()
        .run(
            &retained,
            &input(),
            &[],
            iq::agent_runner::RunnerLifecycle {
                on_prepared: |_: &str, _: &Path| Ok(()),
                on_spawn_surrender: || Ok(true),
                recheck_spawn_authority: || Ok(true),
                on_spawn_failed: move || {
                    observed.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                },
                on_started: |_: u32, _: u64, _: &str, _: &str, _: &Path| {
                    anyhow::bail!("injected start-record failure")
                },
                on_writing: |_: &AtomicResultState| Ok(()),
                authority_active: || Ok(true),
            },
        )
        .unwrap_err();
    std::env::remove_var("IQ_TEST_MODEL_KEY");

    assert!(format!("{error:#}").contains("injected start-record failure"));
    assert!(
        spawn_failed.load(std::sync::atomic::Ordering::SeqCst),
        "{error:#}"
    );
    assert!(matches!(
        iq::agent_runner::systemd_unit_state(
            &snapshot.sandbox.systemctl,
            "iq-agent-cycle-1.service"
        )
        .unwrap(),
        iq::agent_runner::SystemdUnitState::Missing
            | iq::agent_runner::SystemdUnitState::Inactive { .. }
    ));
}

fn test_control_api_config(
    unix_socket: std::path::PathBuf,
    client_idle_seconds: u64,
    max_concurrent_clients: u32,
) -> iq::agent_config::ControlPlaneConfig {
    iq::agent_config::ControlPlaneConfig {
        unix_socket,
        max_request_bytes: 4096,
        max_free_text_bytes: 1024,
        max_response_bytes: 4096,
        max_concurrent_clients,
        max_client_queue_bytes: 4096,
        max_stream_backlog_events: 100,
        client_idle_seconds,
    }
}

fn write_watch_request(stream: &mut UnixStream) {
    use std::io::Write;
    let bytes = serde_json::to_vec(&ApiEnvelope {
        version: 1,
        request: ApiRequest::Watch {
            cursor: 0,
            limit: 100,
        },
    })
    .unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
    stream.flush().unwrap();
}

fn wait_for_control_api_saturation(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(response) = request(socket, &ApiRequest::Inbox { limit: 1 }, 4096) {
            if response.result["error"] == "too_many_clients" {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "control API did not register all expected clients"
        );
    }
}

fn assert_stream_closed(stream: &mut UnixStream) {
    use std::io::Read;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Ok(read) => panic!("control API client remained open and returned {read} byte(s)"),
        Err(error)
            if !matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) => {}
        Err(error) => panic!("control API client remained open: {error}"),
    }
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
    SqliteQueue::open(&fixture.database)
        .unwrap()
        .notification_dispatcher(iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Wslg { executable }],
            max_attempts,
            max_event_age_seconds,
            projection_debt_alert_seconds: 60,
        })
        .unwrap()
}

fn record_notification_alert(fixture: &EffortFixture) {
    fixture
        .store
        .record_alert(
            &fixture.effort_id,
            "integration_blocked",
            serde_json::json!({
                "repository":"00000000-0000-4000-8000-000000000001",
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
    let repository_key = "00000000-0000-4000-8000-000000000001";
    let reservation = temp.path().join("repositories").join(repository_key);
    std::fs::create_dir_all(&reservation).unwrap();
    let remote = reservation.join("root");
    assert!(Command::new("/usr/bin/git")
        .args([
            "init",
            "--bare",
            "--object-format=sha1",
            remote.to_str().unwrap(),
        ])
        .status()
        .unwrap()
        .success());
    let policy = direct_policy(&remote);
    let ownership_key = policy
        .canonical_repository
        .canonical_ownership_key()
        .unwrap();
    iq::sqlite::SqliteQueue::open(&database).unwrap();
    let store = ControlStore::open(&database).unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute(
            "INSERT INTO repository_policies(repo_key,revision,operation_state_json,canonical_repository_json,canonical_ownership_key,target_branch,integration_policy,replication_policy_json,created_at,updated_at) VALUES(?1,1,?2,?3,?4,?5,?6,?7,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            rusqlite::params![repository_key,serde_json::to_string(&policy.operation_state).unwrap(),serde_json::to_string(&policy.canonical_repository).unwrap(),ownership_key,policy.target_branch,policy.integration_policy.to_string(),serde_json::to_string(&policy.replication_policy).unwrap()],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO physical_repository_ownership(identity_key,repo_key,role,ordinal,repository_json,created_at) VALUES(?1,?2,'canonical',0,?3,'2026-01-01T00:00:00Z')",
            rusqlite::params![ownership_key,repository_key,serde_json::to_string(&policy.canonical_repository).unwrap()],
    )
    .unwrap();
    let development = reservation.join("development");
    let integration = reservation.join("integration");
    let registry = temp.path().join("rift.sqlite");
    std::fs::create_dir(&development).unwrap();
    std::fs::create_dir(&integration).unwrap();
    std::fs::write(&registry, b"test registry\n").unwrap();
    let registry_metadata = std::fs::metadata(&registry).unwrap();
    let binding = iq::git_command::RepositoryBinding::capture(&remote).unwrap();
    let mut connection = connection;
    let transaction = connection.transaction().unwrap();
    transaction
        .execute(
            "INSERT INTO registered_repositories(repo_key,owned_root_path,git_binding_json,root_rift_id,registry_identity,registry_device,registry_inode,generation,source_sha,checkout_json,development_root_path,integration_root_path,provisioning_json,created_at,updated_at) VALUES(?1,?2,?3,'ROOTRIFT000000000000000001',?4,?5,?6,0,?7,?8,?9,?10,'{\"state\":\"ready\"}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            rusqlite::params![repository_key,remote.as_os_str().as_encoded_bytes(),serde_json::to_string(&binding).unwrap(),registry.as_os_str().as_encoded_bytes(),registry_metadata.dev(),registry_metadata.ino(),sha('1'),serde_json::json!({"state":"ready","target_sha":sha('1')}).to_string(),development.as_os_str().as_encoded_bytes(),integration.as_os_str().as_encoded_bytes()],
        )
        .unwrap();
    for (kind, root) in [("development", &development), ("integration", &integration)] {
        transaction
            .execute(
                "INSERT INTO workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode,generation) VALUES(?1,?2,?3,?4,'ROOTRIFT000000000000000001',?5,?6,?7,0)",
                rusqlite::params![repository_key,kind,root.as_os_str().as_encoded_bytes(),remote.as_os_str().as_encoded_bytes(),registry.as_os_str().as_encoded_bytes(),registry_metadata.dev(),registry_metadata.ino()],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute(
            "INSERT INTO queue_items(id,repo_key,producer_metadata_json,validation_evidence_json,status,current_attempt_id,created_at,updated_at) VALUES('item-1',?1,'{}','[]','merging','attempt-1','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            [repository_key],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO queue_admissions(item_id,kind,source_branch,head_sha,admitted_at) VALUES('item-1','direct','agent/test','1111111111111111111111111111111111111111','2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO integration_attempts(id,item_id,attempt_number,source_head_sha,started_at) VALUES('attempt-1','item-1',1,'1111111111111111111111111111111111111111','2026-01-01T00:00:00Z')",
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

#[test]
fn cancellation_winning_effort_creation_race_cannot_restore_merging() {
    let fixture = bare_store_fixture();
    let queue = iq::sqlite::SqliteQueue::open(&fixture.database).unwrap();
    let mut cancellation = rusqlite::Connection::open(&fixture.database).unwrap();
    let cancellation = cancellation
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    cancellation
        .execute(
            "UPDATE integration_attempts SET result='cancelled',finished_at='2026-01-01T00:00:01Z' WHERE id='attempt-1'",
            [],
        )
        .unwrap();
    cancellation
        .execute(
            "UPDATE queue_items SET status='cancelled' WHERE id='item-1'",
            [],
        )
        .unwrap();
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
    let store = fixture.store.clone();
    let creator = std::thread::spawn(move || {
        store.create_effort(iq::control_store::NewEffort {
            item_id: "item-1",
            attempt_id: "attempt-1",
            target_sha: &sha('1'),
            source_sha: &sha('2'),
            source_variant: "remote_branch",
            landing_variant: "direct",
            workspace: &workspace,
            runner: &runner_snapshot(),
            state_repository: &StateRepositorySnapshot::Local,
        })
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(!creator.is_finished());
    cancellation.commit().unwrap();

    let error = creator.join().unwrap().unwrap_err();

    assert!(matches!(
        error.downcast_ref::<iq::control_store::EffortCreationError>(),
        Some(iq::control_store::EffortCreationError::Cancelled { item_id }) if item_id == "item-1"
    ));
    assert_eq!(
        queue.get_item("item-1").unwrap().status,
        iq::core::QueueStatus::Cancelled
    );
    assert!(fixture.store.effort_for_item("item-1").unwrap().is_none());
}

fn effort_fixture() -> EffortFixture {
    effort_fixture_with_repository(StateRepositorySnapshot::Local)
}

fn effort_fixture_with_repository(state_repository: StateRepositorySnapshot) -> EffortFixture {
    effort_fixture_with_repository_and_runner(state_repository, runner_snapshot())
}

fn effort_fixture_with_repository_and_runner(
    state_repository: StateRepositorySnapshot,
    runner: RunnerSnapshot,
) -> EffortFixture {
    let fixture = bare_store_fixture();
    let temp = fixture.temp;
    let database = fixture.database;
    let store = fixture.store;
    let workspace_path = temp.path().join("rift");
    std::fs::create_dir(&workspace_path).unwrap();
    assert!(Command::new("/usr/bin/git")
        .args([
            "init",
            "--object-format=sha1",
            workspace_path.to_str().unwrap(),
        ])
        .status()
        .unwrap()
        .success());
    let binding = iq::git_command::authorize_current(&workspace_path).unwrap();
    let workspace = iq::sqlite::WorkspaceIdentity {
        path: workspace_path.to_string_lossy().to_string(),
        rift_id: "rift-1".into(),
        source_rift_id: "source-rift".into(),
    };
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute(
            "UPDATE queue_items SET integration_workspace_path=?1,integration_workspace_rift_id=?2,integration_workspace_source_rift_id=?3 WHERE id='item-1'",
            rusqlite::params![workspace.path,workspace.rift_id,workspace.source_rift_id],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO workspace_git_bindings(owner_kind,owner_id,top_level,binding_json,created_at) VALUES('integration','item-1',?1,?2,'2026-01-01T00:00:00Z')",
            rusqlite::params![workspace_path.as_os_str().as_encoded_bytes(),serde_json::to_string(&binding).unwrap()],
        )
        .unwrap();
    let effort = store
        .create_effort(iq::control_store::NewEffort {
            item_id: "item-1",
            attempt_id: "attempt-1",
            target_sha: &sha('1'),
            source_sha: &sha('2'),
            source_variant: "remote_branch",
            landing_variant: "direct",
            workspace: &workspace,
            runner: &runner,
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

fn released_landing_fixture() -> EffortFixture {
    let fixture = effort_fixture();
    let running = running_cycle("cycle-landing", 1);
    start_cycle(&fixture.store, &fixture.effort_id, &running);
    let intent = iq::control_store::CandidateIntent {
        operation_id: "candidate-operation".into(),
        cycle_id: running.cycle_id,
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
        operation_ref: "refs/iq/candidate-operations/candidate-operation".into(),
    };
    fixture
        .store
        .accept_resolved_cycle(&fixture.effort_id, &intent)
        .unwrap();
    fixture
        .store
        .record_candidate(
            &fixture.effort_id,
            &iq::control_store::CandidateObservation {
                operation_id: intent.operation_id.clone(),
                candidate_sha: sha('3'),
                tree_sha: intent.tree_sha,
                parent_shas: intent.parents,
                author_name: intent.author_name,
                author_email: intent.author_email,
                author_timestamp: intent.author_timestamp,
                committer_name: intent.committer_name,
                committer_email: intent.committer_email,
                committer_timestamp: intent.committer_timestamp,
                message: intent.message,
                operation_ref: intent.operation_ref,
            },
        )
        .unwrap();
    fixture
        .store
        .start_validation(&fixture.effort_id, &"a".repeat(64))
        .unwrap();
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute(
            "UPDATE integration_attempts SET merge_commit_sha=?1,validated_commit_sha=?1 WHERE id='attempt-1'",
            [sha('3')],
        )
        .unwrap();
    drop(connection);
    fixture
        .store
        .complete_validation(&fixture.effort_id, &sha('3'))
        .unwrap();
    fixture
        .store
        .begin_landing(
            &fixture.effort_id,
            &sha('1'),
            "lease-1",
            "command-1",
            iq::control_domain::SignoffDisposition::NoValidation {
                policy_digest: "a".repeat(64),
            },
        )
        .unwrap();
    let uncertain = iq::control_domain::IntegrationEffortState::LandingUncertain(
        iq::control_domain::LandingUncertain {
            candidate_sha: sha('3'),
            expected_target_sha: sha('1'),
            command_id: "command-1".into(),
            evidence: "command_gate_released".into(),
        },
    );
    let connection = rusqlite::Connection::open(&fixture.database).unwrap();
    connection
        .execute(
            "UPDATE integration_efforts SET state='landing_uncertain',state_json=?1,updated_at='2026-01-01T00:00:02Z' WHERE id=?2",
            rusqlite::params![serde_json::to_string(&uncertain).unwrap(), fixture.effort_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE queue_items SET status='integrating',target_sha=?1,source_sha=?2,landing_state_json=json_object('state','uncertain','candidate_sha',?3,'expected_target_sha',?1),updated_at='2026-01-01T00:00:02Z' WHERE id='item-1'",
            rusqlite::params![sha('1'), sha('2'), sha('3')],
        )
        .unwrap();
    drop(connection);
    fixture
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
            bubblewrap: iq::agent_config::executable_identity(Path::new("/usr/bin/bwrap")).unwrap(),
            unshare: iq::agent_config::executable_identity(Path::new("/usr/bin/unshare")).unwrap(),
            systemd_run: iq::agent_config::executable_identity(Path::new("/usr/bin/systemd-run"))
                .unwrap(),
            systemctl: iq::agent_config::executable_identity(Path::new("/usr/bin/systemctl"))
                .unwrap(),
        },
        credential_env: "IQ_TEST_MODEL_KEY".into(),
    }
}

fn running_cycle(id: &str, number: u8) -> AgentRunning {
    let unit_name = format!("iq-agent-{id}.service");
    AgentRunning {
        launch_operation_id: format!("launch-{id}"),
        unit_name: unit_name.clone(),
        cycle_id: id.into(),
        cycle_number: number,
        pid: 1,
        process_start_ticks: 1,
        control_group: format!(
            "/user.slice/user-1000.slice/user@1000.service/app.slice/{unit_name}"
        ),
        authority_lease_id: "lease".into(),
        launcher: iq::control_domain::LauncherAuthority {
            pid: std::process::id(),
            process_start_ticks: iq::agent_runner::process_start_ticks(std::process::id()).unwrap(),
            token: "00000000-0000-4000-8000-000000000001".into(),
        },
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
        launcher: running.launcher.clone(),
        input_sha256: running.input_sha256.clone(),
        protocol_directory: std::path::PathBuf::from("/test/protocol"),
        prepared_at: running.started_at.clone(),
        spawn_authority: iq::control_domain::SpawnAuthority::Open,
    }
}

fn start_cycle(store: &ControlStore, effort_id: &str, running: &AgentRunning) {
    store
        .prepare_cycle_launch(effort_id, &launching_cycle(running))
        .unwrap();
    assert!(store
        .surrender_cycle_spawn_authority(
            effort_id,
            &running.launch_operation_id,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap());
    assert!(store
        .record_cycle_started(
            effort_id,
            running,
            &running.authority_lease_id,
            &running.launcher,
        )
        .unwrap());
}

fn git<const N: usize>(cwd: &Path, args: [&str; N]) {
    let mut command = Command::new("git");
    if args.first() == Some(&"init")
        && !args
            .iter()
            .any(|argument| argument.starts_with("--object-format="))
    {
        command
            .arg("init")
            .arg("--object-format=sha1")
            .args(&args[1..]);
    } else {
        command.args(args);
    }
    let output = command.current_dir(cwd).output().unwrap();
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
