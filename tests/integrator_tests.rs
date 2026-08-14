use iq::composition::{
    load_local_policy, RepositoryInitOptions, RepositoryManager, SignoffPolicy, ValidationPolicy,
};
use iq::control_store::ControlStore;
use iq::core::{BlockedPhase, BlockedReason, QueueStatus};
use iq::integrator::{
    git, git_output, HostSignoffPolicy, IntegrationPolicy, Integrator, IntegratorOptions,
};
use iq::sqlite::{EnqueueRequest, SqliteQueue};
use std::fs;
use std::io::Read;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use wait_timeout::ChildExt;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct FixtureEnvironment {
    _guard: MutexGuard<'static, ()>,
    model_key: Option<std::ffi::OsString>,
}

impl FixtureEnvironment {
    fn acquire() -> Self {
        let guard = env_lock().lock().unwrap();
        let model_key = std::env::var_os("IQ_TEST_MODEL_KEY");
        std::env::set_var("IQ_TEST_MODEL_KEY", "fixture-model-key");
        Self {
            _guard: guard,
            model_key,
        }
    }
}

impl Drop for FixtureEnvironment {
    fn drop(&mut self) {
        match self.model_key.take() {
            Some(value) => std::env::set_var("IQ_TEST_MODEL_KEY", value),
            None => std::env::remove_var("IQ_TEST_MODEL_KEY"),
        }
    }
}

fn track_ignored_file(repo: &Path, path: &str) {
    let object = git_output(repo, ["hash-object", "-w", "--", path]).unwrap();
    git(
        repo,
        [
            "update-index",
            "--add",
            "--cacheinfo",
            "100644",
            object.as_str(),
            path,
        ],
    )
    .unwrap();
}

#[test]
fn direct_landing_integrates_only_after_remote_target_contains_landed_commit() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/one", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/one"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Integrated);
    let remote_main = git_output(&fixture.repo, ["rev-parse", "refs/remotes/origin/main"]).unwrap();
    assert_eq!(
        item.landed_commit_sha.as_deref(),
        Some(remote_main.as_str())
    );
    git(
        &fixture.repo,
        [
            "merge-base",
            "--is-ancestor",
            item.landed_commit_sha.as_ref().unwrap(),
            "refs/remotes/origin/main",
        ],
    )
    .unwrap();
}

#[test]
fn migrated_ready_item_creates_exact_attempt_effort_and_runner_when_claimed() {
    let fixture = GitFixture::new(false);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let setup_db = fixture.temp.path().join("setup.db");
    let queue = SqliteQueue::open(&setup_db).unwrap();
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
                seed_path: Some(fixture.temp.path().join("seed-root/seed")),
                workspace_root: Some(fixture.temp.path().join("development-workspaces")),
            },
        )
        .unwrap();
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let target_head = git_output(&fixture.repo, ["rev-parse", "HEAD"]).unwrap();
    let workspace = manager
        .create_workspace(&repository.key, "migrated-ready")
        .unwrap();
    fs::write(
        workspace.path.join("migrated-ready.txt"),
        "migrated ready\n",
    )
    .unwrap();
    git(&workspace.path, ["add", "migrated-ready.txt"]).unwrap();
    git(&workspace.path, ["commit", "-m", "migrated ready"]).unwrap();
    let (submission, item) = manager.submit(&workspace.id, None).unwrap();
    let source_head = submission.commit_sha.clone();
    drop(queue);
    let db = fixture.temp.path().join("queues.db");
    let connection = rusqlite::Connection::open(&db).unwrap();
    iq::sqlite::initialize_test_schema(&connection, "8").unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER registered_repository_remote_insert;
             DROP TRIGGER queue_items_local_source_insert;",
        )
        .unwrap();
    connection
        .execute("ATTACH DATABASE ?1 AS setup", [setup_db.to_str().unwrap()])
        .unwrap();
    connection
        .execute_batch(
            "INSERT INTO registered_remote_identities SELECT * FROM setup.registered_remote_identities;\
             INSERT INTO registered_repositories SELECT * FROM setup.registered_repositories;\
             INSERT INTO development_workspaces SELECT * FROM setup.development_workspaces;\
             INSERT INTO local_submissions SELECT * FROM setup.local_submissions;\
             INSERT INTO queue_items SELECT * FROM setup.queue_items;\
             INSERT INTO queue_events SELECT * FROM setup.queue_events;\
             UPDATE queue_metadata SET value=(SELECT value FROM setup.queue_metadata WHERE key='database_id') WHERE key='database_id';\
             DETACH DATABASE setup;",
        )
        .unwrap();
    iq::sqlite::force_test_schema_version(&connection, "8").unwrap();
    drop(connection);
    let system_path = fixture.temp.path().join("system.yaml");
    fs::write(
        &system_path,
        format!(
            "integration_agent:\n  runner: opencode\n  executable: {}\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 30\n  max_log_bytes: 1048576\n  max_result_bytes: 1048576\n  max_processes: 16\n  memory_bytes: 268435456\n  cpu_seconds: 30\n  writable_bytes: 16777216\n  open_files: 128\n  credential_env: IQ_TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
            fixture.runner.display(),
            fixture.temp.path().display()
        ),
    )
    .unwrap();

    let migrated = SqliteQueue::migrate_v8(&db, &system_path).unwrap();
    let store = ControlStore::open(&db).unwrap();
    assert!(store.effort_for_item(&item.id).unwrap().is_none());
    assert!(migrated
        .get_item(&item.id)
        .unwrap()
        .current_attempt_id
        .is_none());
    let expected_runner = fixture.system_config().runner_snapshot(None).unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "test-migrated-ready".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let claimed = integrator.run_once().unwrap().unwrap();
    let attempt_id = claimed.current_attempt_id.as_deref().unwrap();
    let attempt = migrated.get_attempt(attempt_id).unwrap();
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();

    assert_eq!(attempt.item_id, item.id);
    assert_eq!(attempt.source_head_sha, source_head);
    assert_eq!(effort.attempt_id, attempt.id);
    assert_eq!(effort.item_id, item.id);
    assert_eq!(effort.target_sha, target_head);
    assert_eq!(effort.source_sha, attempt.source_head_sha);
    assert_eq!(effort.source_variant, "local_submission");
    assert_eq!(effort.landing_variant, "squash");
    assert_eq!(effort.runner, expected_runner);
    let connection = rusqlite::Connection::open(migrated.path()).unwrap();
    let effort_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM integration_efforts WHERE item_id=?1",
            [&item.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(effort_count, 1);
    std::env::remove_var("IQ_RIFT_DATABASE");
}

#[test]
fn no_validation_accepts_exact_candidate_and_reports_skipped_policy() {
    let fixture = GitFixture::new(false);
    let source_head =
        fixture.create_source_branch("agent/no-validation", "feature.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/no-validation"],
    )
    .unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/no-validation".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Integrated);
    let attempt = queue
        .get_attempt(item.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(attempt.validated_commit_sha, item.landed_commit_sha);
    assert_eq!(attempt.validation_command, None);
    assert_eq!(
        iq::composition::verify_policy_snapshot(
            attempt.policy_snapshot_json.as_deref().unwrap(),
            attempt.policy_digest.as_deref().unwrap(),
        )
        .unwrap()
        .policy,
        ValidationPolicy::None
    );
    let event_types = queue
        .events(&item.id)
        .unwrap()
        .into_iter()
        .map(|event| event.event_type)
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"validation_skipped".into()));
    assert!(event_types.contains(&"signoff_not_required".into()));
}

#[test]
fn local_policy_is_optional_strict_and_untracked() {
    let temp = tempdir().unwrap();
    git(temp.path(), ["init"]).unwrap();

    let (absent, _, _) = load_local_policy(temp.path()).unwrap();
    assert_eq!(absent.policy, ValidationPolicy::None);

    fs::create_dir(temp.path().join(".iq")).unwrap();
    fs::write(temp.path().join(".iq/config.json"), b"{}").unwrap();
    assert!(load_local_policy(temp.path()).is_err());

    fs::write(
        temp.path().join(".iq/config.json"),
        br#"{"version":2,"integration":{"validation":{"command":"git diff --check"},"signoff":{"mode":"none"}}}"#,
    )
    .unwrap();
    let (configured, snapshot, digest) = load_local_policy(temp.path()).unwrap();
    assert_eq!(
        configured.policy,
        ValidationPolicy::Command {
            command: "git diff --check".into(),
            signoff: SignoffPolicy::None,
        }
    );
    assert_eq!(
        iq::composition::verify_policy_snapshot(&snapshot, &digest).unwrap(),
        configured
    );

    track_ignored_file(temp.path(), ".iq/config.json");
    let error = format!("{:#}", load_local_policy(temp.path()).unwrap_err());
    assert!(error.contains("must not be tracked"), "{error}");
}

#[test]
fn local_policy_rejects_oversized_files_and_symlinked_directories() {
    let temp = tempdir().unwrap();
    git(temp.path(), ["init"]).unwrap();
    fs::create_dir(temp.path().join(".iq")).unwrap();
    fs::write(
        temp.path().join(".iq/config.json"),
        vec![b' '; 1024 * 1024 + 1],
    )
    .unwrap();
    let oversized = format!("{:#}", load_local_policy(temp.path()).unwrap_err());
    assert!(oversized.contains("exceeds"), "{oversized}");

    fs::remove_dir_all(temp.path().join(".iq")).unwrap();
    let external = temp.path().join("external-policy");
    fs::create_dir(&external).unwrap();
    fs::write(external.join("config.json"), b"{}").unwrap();
    symlink(&external, temp.path().join(".iq")).unwrap();
    let symlinked = format!("{:#}", load_local_policy(temp.path()).unwrap_err());
    assert!(symlinked.contains("non-symlink directory"), "{symlinked}");
}

#[test]
fn tracked_local_policy_is_rejected_before_landing() {
    let fixture = GitFixture::new(false);
    let source_head = fixture.create_source_branch(
        "agent/tracked-policy",
        ".iq/config.json",
        r#"{"version":2}"#,
    );
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/tracked-policy".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();

    assert_eq!(blocked.id, item.id);
    let effort = iq::control_store::ControlStore::open(queue.path())
        .unwrap()
        .effort_for_item(&item.id)
        .unwrap()
        .unwrap();
    assert!(matches!(
        effort.state,
        iq::control_domain::IntegrationEffortState::InfrastructureBlocked(_)
    ));
    assert!(blocked.landed_commit_sha.is_none());
}

#[test]
fn registered_attempt_snapshots_local_policy_once() {
    let fixture = GitFixture::new(false);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let first_sha = fixture.create_source_branch("agent/first", "first.txt", "first\n");
    let second_sha = fixture.create_source_branch("agent/second", "second.txt", "second\n");
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
                seed_path: Some(fixture.temp.path().join("seed-root/seed")),
                workspace_root: Some(fixture.temp.path().join("development-workspaces")),
            },
        )
        .unwrap();
    assert!(!Path::new(repository.seed.path().unwrap())
        .join(".iq/config.json")
        .exists());
    let host_policy_result = Integrator::new_with_policy(
        IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "host-policy-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("host-policy-workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        },
        IntegrationPolicy::Validation {
            command: "git diff --check".into(),
            signoff: HostSignoffPolicy::None,
        },
    );
    let error = match host_policy_result {
        Ok(_) => panic!("registered repository accepted host validation"),
        Err(error) => format!("{error:#}"),
    };
    assert!(
        error.contains("local integration-checkout policy"),
        "{error}"
    );

    fs::write(fixture.repo.join(".git/info/exclude"), ".iq/config.json\n").unwrap();
    fs::create_dir_all(fixture.repo.join(".iq")).unwrap();
    let config_path = fixture.repo.join(".iq/config.json");
    let command = format!(
        "test -f .iq/config.json && printf '{{malformed' > '{}' && git diff --check",
        config_path.display()
    );
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "integration": {
                "validation": {"command": command},
                "signoff": {"mode": "none"}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/first".into(),
            target_branch: "main".into(),
            current_head_sha: first_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let second = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/second".into(),
            target_branch: "main".into(),
            current_head_sha: second_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repository.key.clone(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("integration-workspaces"),
        rift_database: Some(fixture.rift_database.clone()),
        system_config: fixture.system_config(),
    })
    .unwrap();

    let integrated = integrator.run_once().unwrap().unwrap();
    assert_eq!(integrated.id, first.id);
    assert_eq!(integrated.status, QueueStatus::Integrated);
    let store = ControlStore::open(&db).unwrap();
    let artifacts = store
        .terminal_cycle_artifacts(&repository.key)
        .unwrap()
        .into_iter()
        .find(|cycle| cycle.item_id == integrated.id)
        .unwrap();
    assert!(!Path::new(&artifacts.workspace.path).exists());
    let unrelated_development = manager
        .create_workspace(&repository.key, "unrelated-active")
        .unwrap();
    let development_sentinel = unrelated_development.path.join("unrelated.txt");
    fs::write(&development_sentinel, "unrelated\n").unwrap();
    let integration_root = fixture.temp.path().join("integration-workspaces");
    let sandbox = integration_root.join(format!(".iq-agent-sandbox-{}", artifacts.cycle_id));
    fs::create_dir_all(sandbox.join("export")).unwrap();
    let unknown = integration_root.join("unrelated-unknown-entry");

    integrator.reset_workspaces().unwrap();
    assert!(!sandbox.exists());
    assert_eq!(
        fs::read_to_string(&development_sentinel).unwrap(),
        "unrelated\n"
    );
    integrator.reset_workspaces().unwrap();
    assert_eq!(
        fs::read_to_string(&development_sentinel).unwrap(),
        "unrelated\n"
    );
    fs::create_dir_all(sandbox.join("export")).unwrap();
    fs::create_dir(&unknown).unwrap();
    let connection = rusqlite::Connection::open(&db).unwrap();
    let persisted_root: (String, String, String, String, i64) = connection
        .query_row(
            "SELECT source_path,source_rift_id,workspace_root,registry_identity,generation FROM workspace_roots WHERE repo_key=?1",
            [&repository.key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    assert_eq!(Path::new(&persisted_root.2), integration_root);
    connection
        .execute(
            "DELETE FROM workspace_roots WHERE repo_key=?1",
            [&repository.key],
        )
        .unwrap();
    let absent_row_error = format!("{:#}", integrator.reset_workspaces().unwrap_err());
    assert!(
        absent_row_error.contains("no persisted workspace root authority"),
        "{absent_row_error}"
    );
    assert!(sandbox.is_dir());
    assert!(unknown.is_dir());
    connection
        .execute(
            "INSERT INTO workspace_roots(repo_key,source_path,source_rift_id,workspace_root,registry_identity,generation) VALUES(?1,?2,?3,?4,?5,?6)",
            rusqlite::params![repository.key,persisted_root.0,persisted_root.1,persisted_root.2,persisted_root.3,persisted_root.4],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE workspace_roots SET source_rift_id='mismatched-source' WHERE repo_key=?1",
            [&repository.key],
        )
        .unwrap();
    let mismatched_row_error = format!("{:#}", integrator.reset_workspaces().unwrap_err());
    assert!(
        mismatched_row_error.contains("owner differs from durable authority"),
        "{mismatched_row_error}"
    );
    assert!(sandbox.is_dir());
    assert!(unknown.is_dir());
    connection
        .execute(
            "UPDATE workspace_roots SET source_rift_id=?1 WHERE repo_key=?2",
            rusqlite::params![persisted_root.1, repository.key],
        )
        .unwrap();
    let owner_marker = integration_root.join(".iq-workspace-owner.json");
    let owner_bytes = fs::read(&owner_marker).unwrap();
    fs::remove_file(&owner_marker).unwrap();
    let unowned_error = format!("{:#}", integrator.reset_workspaces().unwrap_err());
    assert!(
        unowned_error.contains("IQ workspace owner marker"),
        "{unowned_error}"
    );
    assert!(sandbox.is_dir());
    assert!(unknown.is_dir());

    fs::write(&owner_marker, &owner_bytes).unwrap();
    let mut mismatched_owner: serde_json::Value = serde_json::from_slice(&owner_bytes).unwrap();
    mismatched_owner["repo_key"] = serde_json::json!("other::main");
    fs::write(
        &owner_marker,
        serde_json::to_vec(&mismatched_owner).unwrap(),
    )
    .unwrap();
    let mismatched_error = format!("{:#}", integrator.reset_workspaces().unwrap_err());
    assert!(
        mismatched_error.contains("owner differs from durable authority"),
        "{mismatched_error}"
    );
    assert!(sandbox.is_dir());
    assert!(unknown.is_dir());

    fs::write(&owner_marker, owner_bytes).unwrap();
    fs::remove_dir(&unknown).unwrap();
    integrator.reset_workspaces().unwrap();
    assert!(!sandbox.exists());
    let deleted_cycle_id = uuid::Uuid::new_v4().to_string();
    let deleted_sandbox = integration_root.join(format!(".iq-agent-sandbox-{deleted_cycle_id}"));
    fs::create_dir_all(deleted_sandbox.join("export")).unwrap();
    let durable_cycle_id = artifacts.cycle_id.clone();
    connection
        .execute(
            "UPDATE integration_cycles SET status='starting',failure_json=NULL,finished_at=NULL WHERE id=?1",
            [&durable_cycle_id],
        )
        .unwrap();
    let durable_sandbox = integration_root.join(format!(".iq-agent-sandbox-{durable_cycle_id}"));
    fs::create_dir_all(durable_sandbox.join("export")).unwrap();
    let malformed_sandbox = integration_root.join(".iq-agent-sandbox-not-a-uuid");
    fs::create_dir_all(malformed_sandbox.join("export")).unwrap();
    fs::create_dir(&unknown).unwrap();
    assert!(unknown.is_dir());
    let malformed_error = format!("{:#}", manager.cleanup_repo(&repository.key).unwrap_err());
    assert!(
        malformed_error.contains("malformed cycle identity"),
        "{malformed_error}"
    );
    assert!(deleted_sandbox.is_dir());
    assert!(durable_sandbox.is_dir());
    fs::remove_dir_all(&malformed_sandbox).unwrap();

    manager.cleanup_repo(&repository.key).unwrap();
    assert!(!deleted_sandbox.exists());
    assert!(durable_sandbox.is_dir());
    assert!(unknown.is_dir());
    let repair_key = format!(
        "deleted_cycle_sandbox_repair:{}:{}",
        repository.key, deleted_cycle_id
    );
    let repair: String = connection
        .query_row(
            "SELECT value FROM queue_metadata WHERE key=?1",
            [&repair_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&repair).unwrap()["state"],
        "authorized"
    );
    manager.cleanup_repo(&repository.key).unwrap();
    assert!(!deleted_sandbox.exists());
    assert!(durable_sandbox.is_dir());
    connection
        .execute(
            "UPDATE integration_cycles SET status='failed',failure_json=json_object('kind','interrupted'),finished_at='2026-01-01T00:00:00Z' WHERE id=?1",
            [&durable_cycle_id],
        )
        .unwrap();
    fs::remove_dir(&unknown).unwrap();
    fs::remove_dir_all(&durable_sandbox).unwrap();
    let attempt = queue
        .get_attempt(integrated.current_attempt_id.as_deref().unwrap())
        .unwrap();
    let snapshot = attempt.policy_snapshot_json.unwrap();
    let digest = attempt.policy_digest.unwrap();
    assert!(matches!(
        iq::composition::verify_policy_snapshot(&snapshot, &digest)
            .unwrap()
            .policy,
        ValidationPolicy::Command { .. }
    ));

    let error = format!("{:#}", integrator.run_once().unwrap_err());
    assert!(
        error.contains("parse strict versioned local policy"),
        "{error}"
    );
    assert_eq!(
        queue.get_item(&second.id).unwrap().status,
        QueueStatus::Ready
    );
    assert!(queue
        .get_item(&second.id)
        .unwrap()
        .current_attempt_id
        .is_none());
    std::env::remove_var("IQ_RIFT_DATABASE");
}

#[test]
fn integrator_refuses_to_transition_after_lease_owner_changes() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    fixture.set_validation_command(&format!(
        "sqlite3 -cmd '.timeout 5000' '{}' \"UPDATE repo_leases SET owner_id='owner-b' WHERE repo_key='fixture::main'\" && sleep 0.1 && git diff --check",
        db.display()
    ));
    let source_head = fixture.create_source_branch("agent/stale-owner", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/stale-owner"]).unwrap();
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let enqueued = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/stale-owner".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "owner-a".into(),
            lease_ttl_seconds: 1,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let result = integrator.run_once();

    match result {
        Err(_) => {}
        Ok(Some(item)) => {
            assert_eq!(item.status, QueueStatus::Blocked);
            assert_eq!(item.blocked_reason, Some(BlockedReason::Infra));
        }
        Ok(None) => panic!("lease loss returned no queue state"),
    }
    let item = queue.get_item(&enqueued.id).unwrap();
    assert!(matches!(
        item.status,
        QueueStatus::Ready
            | QueueStatus::Merging
            | QueueStatus::Merged
            | QueueStatus::Validating
            | QueueStatus::Blocked
    ));
    assert_eq!(item.landed_commit_sha, None);
}

#[test]
fn source_branch_head_mismatch_blocks_without_integrating_moved_code() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/moved", "feature.txt", "accepted\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/moved"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/moved".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    fs::write(fixture.repo.join("moved.txt"), "not accepted\n").unwrap();
    git(&fixture.repo, ["add", "moved.txt"]).unwrap();
    git(&fixture.repo, ["commit", "-m", "move source branch"]).unwrap();
    let moved_head = git_output(&fixture.repo, ["rev-parse", "HEAD"]).unwrap();
    git(&fixture.repo, ["push", "origin", "agent/moved"]).unwrap();

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Merging));
    assert_eq!(item.blocked_reason, Some(BlockedReason::NeedsAgentFix));
    let remote_main = git_output(&fixture.repo, ["rev-parse", "refs/remotes/origin/main"]).unwrap();
    assert_ne!(remote_main, moved_head);
    assert!(git(
        &fixture.repo,
        [
            "merge-base",
            "--is-ancestor",
            &moved_head,
            "refs/remotes/origin/main",
        ],
    )
    .is_err());
}

#[test]
fn daemon_run_holds_later_ready_item_behind_oldest_blocked_item() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/conflict", "conflict.txt", "source\n");
    fixture.commit_on_main("conflict.txt", "target\n");
    let later_head = fixture.create_source_branch("agent/later", "later.txt", "later\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/conflict".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let later = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/later".into(),
            target_branch: "main".into(),
            current_head_sha: later_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let blocked = integrator.run_once().unwrap().unwrap();
    assert_eq!(blocked.id, first.id);
    let effort = iq::control_store::ControlStore::open(&db)
        .unwrap()
        .effort_for_item(&first.id)
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            effort.state,
            iq::control_domain::IntegrationEffortState::GuidanceRequired(_)
        ),
        "{:?}",
        effort.state
    );

    let held = integrator.run_once().unwrap().unwrap();

    assert_eq!(held.id, first.id);
    assert_eq!(
        iq::control_store::ControlStore::open(queue.path())
            .unwrap()
            .inbox(10)
            .unwrap()[0]
            .item_id,
        first.id
    );
    assert_eq!(
        queue.get_item(&later.id).unwrap().status,
        QueueStatus::Ready
    );
}

#[test]
fn guidance_answer_starts_new_agent_process_and_lands_exact_validated_candidate() {
    let fixture = GitFixture::new(true);
    let provider = fixture.temp.path().join("fail-provider");
    let provider_log = fixture.temp.path().join("provider-called");
    std::fs::write(
        &provider,
        format!(
            "#!/bin/sh\nprintf 'called\\n' > '{}'\nexit 1\n",
            provider_log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&provider, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::env::set_var("IQ_GITHUB_CLI", &provider);
    std::env::set_var("IQ_GITLAB_CLI", &provider);
    let source_head =
        fixture.create_source_branch("agent/guidance", "contract.txt", "source behavior\n");
    fixture.commit_on_main("contract.txt", "target behavior\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/guidance".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W-guidance"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();
    assert_eq!(blocked.status, QueueStatus::Blocked);
    let store = iq::control_store::ControlStore::open(&db).unwrap();
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    let iq::control_domain::IntegrationEffortState::GuidanceRequired(blocked) = &effort.state
    else {
        panic!(
            "first agent cycle did not request guidance: effort={effort:?} events={:?}",
            store.events_after(0, 100)
        )
    };
    let iq::control_domain::IntegrationBlocker::SemanticGuidance(guidance) = &blocked.blocker
    else {
        panic!("guidance state has the wrong blocker")
    };
    let mut control_config = fixture.system_config().control_plane;
    let control_temp = tempfile::Builder::new()
        .prefix("iq-api-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(control_temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    control_config.unix_socket = control_temp.path().join("control.sock");
    let socket = control_config.unix_socket.clone();
    let (_lifetime, server) = iq::control_api::ControlApiServer::bind(
        control_config.clone(),
        iq::control_store::ControlStore::open(&db).unwrap(),
    )
    .unwrap();
    let thread = std::thread::spawn(move || server.serve_one().unwrap());
    let response = iq::control_api::request(
        &socket,
        &iq::control_api::ApiRequest::Answer {
            answer: iq::control_store::AnswerCommand {
                external_id: "local-guidance-answer-1".into(),
                request_id: guidance.request_id.clone(),
                effort_id: effort.id.clone(),
                attempt_id: guidance.identity.attempt_id.clone(),
                cycle_id: guidance.identity.cycle_id.clone(),
                target_sha: guidance.identity.target_sha.clone(),
                source_sha: guidance.identity.source_sha.clone(),
                candidate_sha: guidance.identity.candidate_sha.clone(),
                answer: "preserve target and source behavior".into(),
            },
        },
        control_config.max_response_bytes,
    )
    .unwrap();
    thread.join().unwrap();
    assert!(response.ok, "{response:?}");
    assert_eq!(response.result, serde_json::json!("applied"));

    let candidate = integrator.run_once().unwrap().unwrap();
    assert_eq!(candidate.status, QueueStatus::Merged);
    let integrated = match integrator.run_once() {
        Ok(Some(item)) => item,
        outcome => panic!(
            "landing outcome={outcome:?} queue={:?} effort={:?} events={:?}",
            queue.get_item(&item.id),
            store.effort_for_item(&item.id),
            store.events_after(0, 100)
        ),
    };
    assert_eq!(
        integrated.status,
        QueueStatus::Integrated,
        "queue={integrated:?} effort={:?} events={:?}",
        store.effort_for_item(&item.id).unwrap(),
        store.events_after(0, 100).unwrap()
    );
    let attempt = queue
        .get_attempt(integrated.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(attempt.validated_commit_sha, integrated.landed_commit_sha);
    assert_eq!(
        integrated.landed_commit_sha.as_deref(),
        Some(
            git_output(&fixture.repo, ["rev-parse", "refs/remotes/origin/main"])
                .unwrap()
                .as_str()
        )
    );
    let landed = integrated.landed_commit_sha.as_deref().unwrap();
    assert_eq!(
        git_output(&fixture.repo, ["show", &format!("{landed}:contract.txt")]).unwrap(),
        "target behavior\nsource behavior"
    );
    assert_eq!(
        git_output(
            &fixture.repo,
            [
                "for-each-ref",
                "--format=%(refname)",
                "refs/iq/candidate-operations",
            ],
        )
        .unwrap(),
        ""
    );
    let connection = rusqlite::Connection::open(&db).unwrap();
    let cycles: Vec<(String, String, i64, i64)> = connection
        .prepare(
            "SELECT id,status,json_extract(process_json,'$.pid'),json_extract(process_json,'$.process_start_ticks') FROM integration_cycles WHERE effort_id=?1 ORDER BY cycle_number",
        )
        .unwrap()
        .query_map([&effort.id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(cycles.len(), 2);
    assert_eq!(cycles[0].1, "guidance_required");
    assert_eq!(cycles[1].1, "resolved");
    assert_ne!(cycles[0].0, cycles[1].0);
    assert_ne!((cycles[0].2, cycles[0].3), (cycles[1].2, cycles[1].3));
    std::env::remove_var("IQ_GITHUB_CLI");
    std::env::remove_var("IQ_GITLAB_CLI");
    assert!(!provider_log.exists());
}

#[test]
fn ten_invalid_runner_processes_create_one_cycle_limit_and_never_start_eleven() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch(
        "agent/invalid-output",
        "force-invalid-agent",
        "invalid identity\n",
    );
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/invalid-output".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W-invalid"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();
    assert_eq!(blocked.status, QueueStatus::Blocked);
    assert_eq!(blocked.blocked_reason, Some(BlockedReason::NeedsAgentFix));
    let store = iq::control_store::ControlStore::open(&db).unwrap();
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    assert_eq!(effort.failed_cycles, 10);
    assert!(matches!(
        effort.state,
        iq::control_domain::IntegrationEffortState::CycleLimitBlocked(_)
    ));
    let connection = rusqlite::Connection::open(&db).unwrap();
    let processes: Vec<(i64, i64, i64, String)> = connection
        .prepare(
            "SELECT cycle_number,json_extract(process_json,'$.pid'),json_extract(process_json,'$.process_start_ticks'),status FROM integration_cycles WHERE effort_id=?1 ORDER BY cycle_number",
        )
        .unwrap()
        .query_map([&effort.id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(processes.len(), 10);
    assert_eq!(
        processes
            .iter()
            .map(|process| process.0)
            .collect::<Vec<_>>(),
        (1..=10).collect::<Vec<_>>()
    );
    assert!(processes.iter().all(|process| process.3 == "failed"));
    let identities = processes
        .iter()
        .map(|process| (process.1, process.2))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(identities.len(), 10);
    assert_eq!(
        store
            .events_after(0, 100)
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == "cycle_limit")
            .count(),
        1
    );
    let held = integrator.run_once().unwrap().unwrap();
    assert_eq!(held.id, item.id);
    let cycle_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM integration_cycles WHERE effort_id=?1",
            [&effort.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cycle_count, 10);
}

#[test]
fn launching_restart_removes_cycle_artifacts_before_replacement_and_integration() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/launch-restart", "feature.txt", "feature\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/launch-restart".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W-launch-restart"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let workspace_root = fixture.temp.path().join("workspaces");
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: "fixture::main".into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-launch-restart".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: workspace_root.clone(),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    std::env::remove_var("IQ_TEST_MODEL_KEY");
    let error = integrator.run_once().unwrap_err();
    assert!(
        format!("{error:#}").contains("required model credential IQ_TEST_MODEL_KEY is unavailable")
    );
    let store = ControlStore::open(&db).unwrap();
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    let iq::control_domain::IntegrationEffortState::AgentLaunching(launch) = &effort.state else {
        panic!("failed pre-start launch did not retain launch authority")
    };
    let interrupted_cycle_id = launch.cycle_id.clone();
    let retained = Path::new(&effort.workspace.path);
    let sandbox = retained
        .parent()
        .unwrap()
        .join(format!(".iq-agent-sandbox-{}", launch.cycle_id));
    let protocol = launch.protocol_directory.clone();
    assert!(sandbox.is_dir());
    assert!(protocol.is_dir());

    std::env::set_var("IQ_TEST_MODEL_KEY", "fixture-model-key");
    let candidate = integrator.resume_item(&item.id).unwrap();
    assert_eq!(candidate.status, QueueStatus::Merged);
    assert!(!sandbox.exists());
    assert!(!protocol.exists());
    let interrupted: (String, String) = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row(
            "SELECT status,failure_json FROM integration_cycles WHERE id=?1",
            [&interrupted_cycle_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(interrupted.0, "failed");
    assert_eq!(
        serde_json::from_str::<iq::control_domain::CycleFailure>(&interrupted.1).unwrap(),
        iq::control_domain::CycleFailure::Interrupted
    );

    let integrated = integrator.run_once().unwrap().unwrap();
    assert_eq!(integrated.status, QueueStatus::Integrated);
    assert!(store
        .terminal_cycle_artifacts("fixture::main")
        .unwrap()
        .iter()
        .any(|cycle| cycle.cycle_id == interrupted_cycle_id));
    iq::integrator::verify_rift_workspace_config(
        &fixture.repo,
        &workspace_root,
        "fixture::main",
        Some(&fixture.rift_database),
        &db,
    )
    .unwrap();
}

#[test]
fn target_moved_merge_conflict_blocks_with_conflict_metadata() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/late-conflict", "conflict.txt", "source\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/late-conflict"],
    )
    .unwrap();
    let moved_branch = "target/late-conflict";
    fixture.set_validation_command(&format!(
        "git --git-dir={} update-ref refs/heads/main $(git --git-dir={} rev-parse refs/heads/{moved_branch})",
        fixture.remote.display(),
        fixture.remote.display(),
    ));
    let moved_sha =
        fixture.create_unpublished_target_change(moved_branch, "conflict.txt", "target\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/late-conflict".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Merging));
    assert_eq!(item.blocked_reason, Some(BlockedReason::NeedsUserInput));
    assert_eq!(item.conflict.as_ref().unwrap()["files"][0], "conflict.txt");
    assert_eq!(item.target_sha.as_deref(), Some(moved_sha.as_str()));
}

#[test]
fn target_movement_keeps_the_attempt_validation_policy() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/missing-revalidation", "feature.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/missing-revalidation"],
    )
    .unwrap();
    let moved_branch = "target/no-validation";
    fixture.set_validation_command(&format!(
        "git --git-dir={} update-ref refs/heads/main $(git --git-dir={} rev-parse refs/heads/{moved_branch})",
        fixture.remote.display(),
        fixture.remote.display(),
    ));
    fixture.create_unpublished_target_change(moved_branch, "target.txt", "target\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/missing-revalidation".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W005"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Integrated);
    let attempt = queue
        .get_attempt(item.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert!(attempt.policy_snapshot_json.is_none());
    assert!(attempt.validation_command.is_some());
}

#[test]
fn direct_landing_fetch_failure_persists_integrating_block() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/fetch-fails", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/fetch-fails"]).unwrap();
    fixture.set_validation_command("git remote set-url origin /missing/iq-remote");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/fetch-fails".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W006"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Integrating));
    assert_eq!(item.blocked_reason, Some(BlockedReason::Infra));
}

#[test]
fn unsupported_provider_url_blocks_the_authoritative_effort() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/unsupported-provider", "feature.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/unsupported-provider"],
    )
    .unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/unsupported-provider".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: Some("https://code.example.test/change/8".into()),
            producer_metadata: serde_json::json!({"worker":"W007"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();

    assert_eq!(blocked.id, item.id);
    assert_eq!(blocked.status, QueueStatus::Blocked);
    assert_eq!(blocked.blocked_phase, Some(BlockedPhase::Integrating));
    assert_eq!(blocked.blocked_reason, Some(BlockedReason::Infra));
    let effort = iq::control_store::ControlStore::open(&db)
        .unwrap()
        .effort_for_item(&item.id)
        .unwrap()
        .unwrap();
    let iq::control_domain::IntegrationEffortState::InfrastructureBlocked(blocked) = effort.state
    else {
        panic!("unsupported provider URL did not block the effort")
    };
    let iq::control_domain::IntegrationBlocker::Infrastructure(blocker) = blocked.blocker else {
        panic!("unsupported provider URL used the wrong blocker")
    };
    let iq::control_domain::ResumeState::Validating(resume) = blocked.resume else {
        panic!("unsupported provider URL lost its landing-gate resume state")
    };
    assert_eq!(
        blocker.component,
        iq::control_domain::InfrastructureComponent::Configuration
    );
    assert_eq!(blocker.operation, "select_provider_adapter");
    assert!(matches!(
        blocker.cause,
        iq::control_domain::InfrastructureCause::Unavailable { ref detail }
            if detail.contains("https://code.example.test/change/8")
                && detail.contains(&resume.candidate_sha)
    ));
    assert_eq!(resume.stage, iq::control_domain::ValidationStage::Gates);
}

fn registered_terminal_fixture() -> (GitFixture, SqliteQueue, iq::sqlite::RegisteredRepository) {
    let fixture = GitFixture::new(false);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
                seed_path: Some(fixture.temp.path().join("seed-root/seed")),
                workspace_root: Some(fixture.temp.path().join("development-workspaces")),
            },
        )
        .unwrap();
    (fixture, queue, repository)
}

fn enqueue_fixture_item(
    fixture: &GitFixture,
    queue: &SqliteQueue,
    repository: &iq::sqlite::RegisteredRepository,
    branch: &str,
) -> iq::sqlite::QueueItem {
    let head = fixture.create_source_branch(branch, &format!("{branch}.txt"), "feature\n");
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: branch.into(),
            target_branch: "main".into(),
            current_head_sha: head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"terminal-test"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap()
}

fn cancelled_retained_integrator_fixture() -> (
    GitFixture,
    SqliteQueue,
    iq::sqlite::RegisteredRepository,
    iq::sqlite::QueueItem,
    std::path::PathBuf,
) {
    let (fixture, queue, repository) = registered_terminal_fixture();
    let first = enqueue_fixture_item(&fixture, &queue, &repository, "agent/terminal-first");
    let db = fixture.temp.path().join("queues.db");
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "terminal-first".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("integration-workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    std::env::remove_var("IQ_TEST_MODEL_KEY");
    assert!(integrator.run_once().is_err());
    std::env::set_var("IQ_TEST_MODEL_KEY", "fixture-model-key");
    let store = ControlStore::open(&db).unwrap();
    let effort = store.effort_for_item(&first.id).unwrap().unwrap();
    let retained = std::path::PathBuf::from(&effort.workspace.path);
    store
        .cancel(&effort.id, "test", "terminal fixture")
        .unwrap();
    (fixture, queue, repository, first, retained)
}

#[test]
fn dirty_cancelled_terminal_rift_does_not_block_later_ready_item() {
    let (fixture, queue, repository, _first, retained) = cancelled_retained_integrator_fixture();
    fs::write(retained.join("dirty.txt"), "preserve\n").unwrap();
    let later = enqueue_fixture_item(&fixture, &queue, &repository, "agent/terminal-later");
    let db = fixture.temp.path().join("queues.db");
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "terminal-later".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("integration-workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let processed = integrator.run_once().unwrap().unwrap();
    assert_eq!(processed.id, later.id);
    assert_eq!(processed.status, QueueStatus::Integrated);
    assert!(retained.exists());
}

#[test]
fn active_bisect_in_clean_cancelled_terminal_rift_is_preserved_and_fifo_progresses() {
    let (fixture, queue, repository, _first, retained) = cancelled_retained_integrator_fixture();
    git(&retained, ["reset", "--hard"]).unwrap();
    git(&retained, ["clean", "-fd"]).unwrap();
    git(&retained, ["bisect", "start", "HEAD", "HEAD~1"]).unwrap();
    let status = git_output(
        &retained,
        ["status", "--porcelain=v1", "--untracked-files=all"],
    )
    .unwrap();
    assert!(status.is_empty(), "{status}");
    let bisect_start = git_output(&retained, ["rev-parse", "--git-path", "BISECT_START"]).unwrap();
    let bisect_start = Path::new(&bisect_start);
    assert!(if bisect_start.is_absolute() {
        bisect_start.exists()
    } else {
        retained.join(bisect_start).exists()
    });
    let later = enqueue_fixture_item(&fixture, &queue, &repository, "agent/terminal-git-later");
    let db = fixture.temp.path().join("queues.db");
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "terminal-git-later".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("integration-workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let processed = integrator.run_once().unwrap().unwrap();
    assert_eq!(processed.id, later.id);
    assert_eq!(processed.status, QueueStatus::Integrated);
    assert!(retained.exists());
}

#[test]
fn landed_dirty_terminal_rift_is_preserved_while_later_item_progresses_then_removed_clean() {
    let (fixture, queue, repository) = registered_terminal_fixture();
    let first = enqueue_fixture_item(&fixture, &queue, &repository, "agent/integrated-first");
    let db = fixture.temp.path().join("queues.db");
    let options = IntegratorOptions {
        repo_key: repository.key.clone(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "integrated-terminal-cleanup".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("integration-workspaces"),
        rift_database: Some(fixture.rift_database.clone()),
        system_config: fixture.system_config(),
    };
    let retained = options.workspace_root.join(&first.id);
    let post_receive = fixture.remote.join("hooks/post-receive");
    git(&fixture.remote, ["config", "core.hooksPath", "hooks"]).unwrap();
    fs::write(
        &post_receive,
        format!(
            "#!/bin/sh\nprintf 'preserve\\n' > '{}'\n",
            retained.join("dirty-after-landing.txt").display()
        ),
    )
    .unwrap();
    fs::set_permissions(
        &post_receive,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    let integrated = fixture
        .integrator(options.clone())
        .unwrap()
        .run_once()
        .unwrap()
        .unwrap();
    assert_eq!(integrated.id, first.id);
    assert_eq!(integrated.status, QueueStatus::Integrated);
    fs::remove_file(&post_receive).unwrap();
    assert!(retained.join("dirty-after-landing.txt").exists());
    let later = enqueue_fixture_item(&fixture, &queue, &repository, "agent/integrated-later");

    let progressed = fixture
        .integrator(options.clone())
        .unwrap()
        .run_once()
        .unwrap()
        .unwrap();

    assert_eq!(progressed.id, later.id);
    assert_eq!(progressed.status, QueueStatus::Integrated);
    assert!(retained.exists());
    assert!(ControlStore::open(&db)
        .unwrap()
        .terminal_workspace_cleanup_debt(&first.id)
        .unwrap()
        .is_some_and(|debt| debt.is_preserved()));

    fs::remove_file(retained.join("dirty-after-landing.txt")).unwrap();
    git(&retained, ["reset", "--hard"]).unwrap();
    let report = fixture
        .integrator(options)
        .unwrap()
        .reset_workspaces()
        .unwrap();
    assert!(report.outcomes.iter().any(|outcome| matches!(
        outcome,
        iq::integrator::TerminalCleanupOutcome::Removed { path } if path == &retained
    )));
    assert!(!retained.exists());
    assert!(matches!(
        queue.get_item(&first.id).unwrap().workspace,
        iq::sqlite::WorkspaceState::Cleaned { .. }
    ));
}

#[test]
fn operator_reset_bypasses_due_time_and_removes_cleaned_terminal_rift() {
    let (fixture, _queue, repository, _first, retained) = cancelled_retained_integrator_fixture();
    fs::write(retained.join("dirty.txt"), "preserve\n").unwrap();
    let db = fixture.temp.path().join("queues.db");
    let options = IntegratorOptions {
        repo_key: repository.key.clone(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "operator-reset-real".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("integration-workspaces"),
        rift_database: Some(fixture.rift_database.clone()),
        system_config: fixture.system_config(),
    };
    let integrator = fixture.integrator(options.clone()).unwrap();
    let preserved = integrator.reset_workspaces().unwrap();
    assert!(preserved.outcomes.iter().any(|outcome| matches!(
        outcome,
        iq::integrator::TerminalCleanupOutcome::Preserved { .. }
    )));
    fs::remove_file(retained.join("dirty.txt")).unwrap();
    git(&retained, ["reset", "--hard"]).unwrap();
    let merge_head = git_output(&retained, ["rev-parse", "--git-path", "MERGE_HEAD"]).unwrap();
    if Path::new(&merge_head).exists() {
        fs::remove_file(merge_head).unwrap();
    }
    let removed = fixture
        .integrator(options)
        .unwrap()
        .reset_workspaces()
        .unwrap();
    assert!(
        removed.outcomes.iter().any(|outcome| matches!(
            outcome,
            iq::integrator::TerminalCleanupOutcome::Removed { .. }
        )),
        "report={removed:?} path_exists={}",
        retained.exists()
    );
    assert!(!retained.exists());
    let store = ControlStore::open(&db).unwrap();
    assert!(store
        .terminal_workspace_cleanup_debt(&_first.id)
        .unwrap()
        .is_none());
    assert!(!store
        .effort_for_item(&_first.id)
        .unwrap()
        .unwrap()
        .workspace
        .path
        .is_empty());
}

#[test]
fn iq_cleanup_cli_retries_terminal_integration_cleanup_and_reports_structure() {
    let (fixture, _queue, repository, first, retained) = cancelled_retained_integrator_fixture();
    let db = fixture.temp.path().join("queues.db");
    fs::write(retained.join("dirty.txt"), "preserve\n").unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "cli-preserve".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("integration-workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let preserved = integrator.reset_workspaces().unwrap();
    assert!(preserved.outcomes.iter().any(|outcome| matches!(
        outcome,
        iq::integrator::TerminalCleanupOutcome::Preserved { .. }
    )));
    fs::remove_file(retained.join("dirty.txt")).unwrap();
    git(&retained, ["reset", "--hard"]).unwrap();
    let merge_head = git_output(&retained, ["rev-parse", "--git-path", "MERGE_HEAD"]).unwrap();
    if Path::new(&merge_head).exists() {
        fs::remove_file(merge_head).unwrap();
    }
    let config_path = fixture.temp.path().join("system.yaml");
    let config = fixture.system_config();
    let yaml = format!(
        "integration_agent:\n  runner: opencode\n  executable: {}\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 30\n  max_log_bytes: 1048576\n  max_result_bytes: 1048576\n  max_processes: 16\n  memory_bytes: 268435456\n  cpu_seconds: 30\n  writable_bytes: 16777216\n  open_files: 128\n  credential_env: IQ_TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
        config.integration_agent.executable.display(),
        config.control_plane.unix_socket.display()
    );
    fs::write(&config_path, yaml).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "cleanup",
            "--repo-key",
            repository.key.as_str(),
            "--system-config",
            config_path.to_str().unwrap(),
            "--repo-path",
            fixture.repo.to_str().unwrap(),
            "--workspace-root",
            fixture
                .temp
                .path()
                .join("integration-workspaces")
                .to_str()
                .unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["terminal"]["mode"], "operator_requested");
    assert_eq!(
        report["terminal"]["outcomes"][0]["Removed"]["path"],
        retained.to_str().unwrap()
    );
    assert!(report["development"].is_array());
    assert!(!retained.exists());
    let store = ControlStore::open(&db).unwrap();
    assert!(store
        .terminal_workspace_cleanup_debt(&first.id)
        .unwrap()
        .is_none());
    let item = SqliteQueue::open(&db).unwrap().get_item(&first.id).unwrap();
    assert!(matches!(
        item.workspace,
        iq::sqlite::WorkspaceState::Cleaned { .. }
    ));
}

#[test]
fn iq_cleanup_cli_removes_one_workspace_without_repository_arguments() {
    let (fixture, queue, repository) = registered_terminal_fixture();
    let manager = RepositoryManager::new(queue.clone());
    let workspace = manager
        .create_workspace(&repository.key, "workspace-only-cleanup")
        .unwrap();
    fs::write(workspace.path.join("workspace-only.txt"), "cleanup\n").unwrap();
    git(&workspace.path, ["add", "workspace-only.txt"]).unwrap();
    git(&workspace.path, ["commit", "-m", "workspace-only"]).unwrap();
    let (_, item) = manager.submit(&workspace.id, None).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let integrated = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "workspace-only-integration".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("integration-workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap()
        .run_once()
        .unwrap()
        .unwrap();
    assert_eq!(integrated.id, item.id);
    assert_eq!(integrated.status, QueueStatus::Integrated);

    let output = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "cleanup",
            "--workspace",
            workspace.id.as_str(),
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let removed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(removed["id"], workspace.id);
    assert_eq!(removed["status"], "removed");
    assert!(!workspace.path.exists());
}

#[test]
fn daemon_once_opens_valid_v10_database_and_completes_cycle() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let (daemon_config, system_config, _control_directory) = write_daemon_runtime_config(&fixture);

    let output = run_daemon_once(&fixture, &db, &daemon_config, &system_config);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!([null])
    );
}

#[test]
fn validated_integrator_queue_rejects_different_configured_database_before_mutation() {
    let fixture = GitFixture::new(false);
    let database_a = fixture.temp.path().join("queue-a.db");
    let database_b = fixture.temp.path().join("queue-b.db");
    let queue_a = SqliteQueue::open(&database_a).unwrap();
    drop(SqliteQueue::open(&database_b).unwrap());
    let before_a = fs::read(&database_a).unwrap();
    let before_b = fs::read(&database_b).unwrap();
    let workspace_root = fixture.temp.path().join("validated-queue-workspaces");
    let options = IntegratorOptions {
        repo_key: format!("{}::main", fixture.repo.display()),
        repo_path: fixture.repo.clone(),
        queue_db: database_b.clone(),
        owner_id: "validated-queue-test".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: workspace_root.clone(),
        rift_database: Some(fixture.rift_database.clone()),
        system_config: fixture.system_config(),
    };

    let mismatch = Integrator::new_with_policy_and_validated_queue(
        options.clone(),
        IntegrationPolicy::NoValidation,
        queue_a.clone(),
    );

    let error = match mismatch {
        Ok(_) => panic!("integrator accepted a different configured queue database"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("validated queue authority path"), "{error}");
    assert!(error.contains("does not match configured integrator queue database"));
    assert_eq!(fs::read(&database_a).unwrap(), before_a);
    assert_eq!(fs::read(&database_b).unwrap(), before_b);
    assert!(!workspace_root.exists());

    let matching = Integrator::new_with_policy_and_validated_queue(
        IntegratorOptions {
            queue_db: database_a,
            ..options
        },
        IntegrationPolicy::NoValidation,
        queue_a,
    );
    if let Err(error) = matching {
        panic!("matching validated queue was rejected: {error:#}");
    }
}

#[test]
fn daemon_once_rejects_independent_database_process_lease_holder() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let (daemon_config, system_config, _control_directory) = write_daemon_runtime_config(&fixture);
    let mut holder = spawn_database_process_lease_holder(&db, fixture.temp.path());

    let blocked = run_daemon_once(&fixture, &db, &daemon_config, &system_config);

    assert!(!blocked.status.success());
    let stderr = String::from_utf8_lossy(&blocked.stderr);
    assert!(stderr.contains("acquire exclusive IQ database process lease"));
    assert!(stderr.contains("Resource temporarily unavailable"));
    drop(holder.stdin.take());
    assert!(holder.wait().unwrap().success());

    let released = run_daemon_once(&fixture, &db, &daemon_config, &system_config);
    assert!(
        released.status.success(),
        "{}",
        String::from_utf8_lossy(&released.stderr)
    );
}

#[test]
fn daemon_api_failure_stops_daemon_before_lifetime_fences_release() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let (daemon_config, system_config, control_directory) = write_daemon_runtime_config(&fixture);
    let mut system = iq::agent_config::SystemConfig::load(&system_config).unwrap();
    system.control_plane.max_concurrent_clients = 1;
    fs::write(&system_config, serde_yaml::to_string(&system).unwrap()).unwrap();
    let ready = fixture.temp.path().join("daemon-ready");
    let failure = fixture.temp.path().join("api-failure");
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_CONTROL_API_FAILURE_TRIGGER", &failure)
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "daemon",
            "--config",
            daemon_config.to_str().unwrap(),
            "--system-config",
            system_config.to_str().unwrap(),
            "--ready-file",
            ready.to_str().unwrap(),
            "--interval-seconds",
            "60",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        assert!(
            daemon.try_wait().unwrap().is_none(),
            "daemon exited before readiness"
        );
        assert!(Instant::now() < deadline, "daemon readiness timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut silent = UnixStream::connect(control_directory.path().join("control.sock")).unwrap();
    let saturation_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(response) = iq::control_api::request(
            &control_directory.path().join("control.sock"),
            &iq::control_api::ApiRequest::Inbox { limit: 1 },
            4096,
        ) {
            if response.result["error"] == "too_many_clients" {
                break;
            }
        }
        assert!(
            Instant::now() < saturation_deadline,
            "daemon API did not register the silent client"
        );
    }

    let blocked = run_daemon_once(&fixture, &db, &daemon_config, &system_config);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr)
        .contains("acquire exclusive IQ database process lease"));

    fs::write(&failure, b"fail\n").unwrap();
    let status = daemon
        .wait_timeout(Duration::from_secs(5))
        .unwrap()
        .expect("daemon did not stop after API failure");
    let mut stderr = String::new();
    daemon
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(!status.success());
    assert!(stderr.contains("IQ control API stopped unexpectedly"));
    assert!(stderr.contains("simulated IQ control API failure"));
    silent
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut byte = [0_u8; 1];
    assert!(!matches!(
        silent.read(&mut byte),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            )
    ));

    let released = run_daemon_once(&fixture, &db, &daemon_config, &system_config);
    assert!(
        released.status.success(),
        "{}",
        String::from_utf8_lossy(&released.stderr)
    );
}

#[test]
fn database_process_lease_holder_process() {
    let Some(database) = std::env::var_os("IQ_TEST_DATABASE_LEASE_HOLDER") else {
        return;
    };
    let ready = std::env::var_os("IQ_TEST_DATABASE_LEASE_READY").unwrap();
    let _lease = iq::control_store::DatabaseProcessLease::acquire(Path::new(&database)).unwrap();
    fs::write(ready, b"ready\n").unwrap();
    std::io::stdin().read_to_end(&mut Vec::new()).unwrap();
}

fn write_daemon_runtime_config(
    fixture: &GitFixture,
) -> (std::path::PathBuf, std::path::PathBuf, tempfile::TempDir) {
    let daemon_config = fixture.temp.path().join("daemon.yaml");
    let system_config = fixture.temp.path().join("system.yaml");
    let control_directory = tempfile::Builder::new()
        .prefix("iq-control-")
        .tempdir_in("/tmp")
        .unwrap();
    fs::set_permissions(control_directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        &daemon_config,
        serde_json::to_vec(&serde_json::json!({
            "repos": [{
                "repo_path": fixture.repo,
                "target": "main",
                "remote": "origin",
                "workspace_root": fixture.temp.path().join("daemon-workspaces"),
                "validation": {"mode": "none"}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let mut system = fixture.system_config();
    system.control_plane.unix_socket = control_directory.path().join("control.sock");
    fs::write(&system_config, serde_yaml::to_string(&system).unwrap()).unwrap();
    (daemon_config, system_config, control_directory)
}

fn run_daemon_once(
    fixture: &GitFixture,
    database: &Path,
    daemon_config: &Path,
    system_config: &Path,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "daemon",
            "--once",
            "--config",
            daemon_config.to_str().unwrap(),
            "--system-config",
            system_config.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

fn spawn_database_process_lease_holder(database: &Path, root: &Path) -> Child {
    let ready = root.join("database-lease-ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "database_process_lease_holder_process",
            "--nocapture",
        ])
        .env("IQ_TEST_DATABASE_LEASE_HOLDER", database)
        .env("IQ_TEST_DATABASE_LEASE_READY", &ready)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "database lease holder exited before readiness"
        );
        assert!(Instant::now() < deadline, "database lease holder timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
    child
}

#[test]
fn creation_intent_terminal_preservation_keeps_authoritative_target_and_alert_payload() {
    let fixture = GitFixture::new(false);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = format!("{}::main", fixture.repo.display());
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.clone(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/creation-intent".into(),
            target_branch: "main".into(),
            current_head_sha: fixture.create_source_branch(
                "agent/creation-intent",
                "creation-intent.txt",
                "feature\n",
            ),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let _ = queue.claim_next_ready(&repo_key).unwrap().unwrap();
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
                seed_path: Some(fixture.temp.path().join("seed-root/seed")),
                workspace_root: Some(fixture.temp.path().join("development-workspaces")),
            },
        )
        .unwrap();
    fs::create_dir_all(fixture.repo.join(".iq")).unwrap();
    fs::write(
        fixture.repo.join(".iq/config.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "integration": {
                "validation": {"command": "git diff --check"},
                "signoff": {"mode": "none"}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let workspace_root = fixture.temp.path().join("integration-workspaces");
    let expected = workspace_root.join(&item.id);
    queue
        .set_workspace_intent(&item.id, expected.to_str().unwrap())
        .unwrap();
    let integrator_options = IntegratorOptions {
        repo_key: repository.key.clone(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "creation-intent-preserve".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: workspace_root.clone(),
        rift_database: Some(fixture.rift_database.clone()),
        system_config: fixture.system_config(),
    };
    drop(fixture.integrator(integrator_options.clone()).unwrap());
    let output = Command::new("rift")
        .args([
            "--database",
            fixture.rift_database.to_str().unwrap(),
            "create",
            "--copy-all",
            "--no-hooks",
            "--name",
            item.id.as_str(),
            "--into",
            workspace_root.to_str().unwrap(),
            fixture.repo.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        Path::new(String::from_utf8_lossy(&output.stdout).trim()),
        expected
    );
    queue
        .transition_item(&item.id, QueueStatus::Cancelled)
        .unwrap();
    fs::write(expected.join("dirty.txt"), "preserve\n").unwrap();
    let integrator = fixture.integrator(integrator_options).unwrap();
    let report = integrator.reset_workspaces().unwrap();
    assert!(report.outcomes.iter().any(|outcome| matches!(
        outcome,
        iq::integrator::TerminalCleanupOutcome::Preserved { .. }
    )));
    assert!(expected.exists());
    let store = ControlStore::open(&db).unwrap();
    let debt = store
        .terminal_workspace_cleanup_debt(&item.id)
        .unwrap()
        .unwrap();
    assert_eq!(
        debt.target,
        iq::control_store::TerminalWorkspaceTarget::CreationIntent {
            path: expected.to_str().unwrap().into()
        }
    );
    let connection = rusqlite::Connection::open(&db).unwrap();
    let payload: serde_json::Value = connection
        .query_row(
            "SELECT payload_json FROM durable_events WHERE item_id=?1 AND event_type='terminal_workspace_preserved'",
            [&item.id],
            |row| row.get::<_, String>(0),
        )
        .map(|raw| serde_json::from_str(&raw).unwrap())
        .unwrap();
    assert_eq!(payload["repository"], repository.key);
    assert_eq!(payload["blocker_kind"], "workspace_cleanup");
    assert_eq!(payload["reason"], "dirty");
    assert_eq!(payload["target"]["path"], expected.to_str().unwrap());
    assert!(payload["target"].get("rift_id").is_none());
    let deliveries: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM notification_deliveries WHERE event_id=(SELECT id FROM durable_events WHERE item_id=?1 AND event_type='terminal_workspace_preserved') AND redelivery_of IS NULL",
            [&item.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(deliveries, 0);
}

#[test]
fn automatic_creation_intent_retry_before_due_keeps_observation_and_processes_later_item() {
    let fixture = GitFixture::new(false);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = format!("{}::main", fixture.repo.display());
    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.clone(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/intent-backoff".into(),
            target_branch: "main".into(),
            current_head_sha: fixture.create_source_branch(
                "agent/intent-backoff",
                "intent-backoff.txt",
                "feature\n",
            ),
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (_, _) = queue.claim_next_ready(&repo_key).unwrap().unwrap();
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
                seed_path: Some(fixture.temp.path().join("seed-root/seed")),
                workspace_root: Some(fixture.temp.path().join("development-workspaces")),
            },
        )
        .unwrap();
    let workspace_root = fixture.temp.path().join("integration-workspaces");
    let expected = workspace_root.join(&first.id);
    queue
        .set_workspace_intent(&first.id, expected.to_str().unwrap())
        .unwrap();
    let options = IntegratorOptions {
        repo_key: repository.key.clone(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "creation-intent-backoff".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: workspace_root.clone(),
        rift_database: Some(fixture.rift_database.clone()),
        system_config: fixture.system_config(),
    };
    drop(fixture.integrator(options.clone()).unwrap());
    let output = Command::new("rift")
        .args([
            "--database",
            fixture.rift_database.to_str().unwrap(),
            "create",
            "--copy-all",
            "--no-hooks",
            "--name",
            first.id.as_str(),
            "--into",
            workspace_root.to_str().unwrap(),
            fixture.repo.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    queue
        .transition_item(&first.id, QueueStatus::Cancelled)
        .unwrap();
    fs::write(expected.join("dirty.txt"), "preserve\n").unwrap();
    fixture
        .integrator(options.clone())
        .unwrap()
        .reset_workspaces()
        .unwrap();
    let store = ControlStore::open(&db).unwrap();
    let before = store
        .terminal_workspace_cleanup_debt(&first.id)
        .unwrap()
        .unwrap();
    let later = enqueue_fixture_item(&fixture, &queue, &repository, "agent/intent-later");

    let processed = fixture
        .integrator(options)
        .unwrap()
        .run_once()
        .unwrap()
        .unwrap();

    assert_eq!(processed.id, later.id);
    assert_eq!(processed.status, QueueStatus::Integrated);
    assert_eq!(
        ControlStore::open(&db)
            .unwrap()
            .terminal_workspace_cleanup_debt(&first.id)
            .unwrap()
            .unwrap(),
        before
    );
    assert!(expected.exists());
    let item = queue.get_item(&first.id).unwrap();
    assert!(matches!(
        item.workspace,
        iq::sqlite::WorkspaceState::CreationIntent { .. }
    ));
}

#[test]
fn validated_open_rejects_mismatched_cleanup_debt_authority() {
    let (fixture, queue, repository, first, retained) = cancelled_retained_integrator_fixture();
    let db = fixture.temp.path().join("queues.db");
    fs::write(retained.join("dirty.txt"), "preserve\n").unwrap();
    let options = IntegratorOptions {
        repo_key: repository.key.clone(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "authority-validation".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("integration-workspaces"),
        rift_database: Some(fixture.rift_database.clone()),
        system_config: fixture.system_config(),
    };
    let integrator = fixture.integrator(options).unwrap();
    let preserved = integrator.reset_workspaces().unwrap();
    assert!(preserved.outcomes.iter().any(|outcome| matches!(
        outcome,
        iq::integrator::TerminalCleanupOutcome::Preserved { .. }
    )));
    let later = enqueue_fixture_item(&fixture, &queue, &repository, "agent/authority-later");
    let connection = rusqlite::Connection::open(&db).unwrap();
    connection
        .execute_batch("DROP TRIGGER terminal_cleanup_debt_update_guard;")
        .unwrap();
    connection
        .execute(
            "UPDATE terminal_workspace_cleanup_debt SET workspace_json=json_set(workspace_json,'$.path',?1) WHERE item_id=?2",
            rusqlite::params![retained.with_file_name("moved-by-external-input").to_string_lossy(), first.id],
        )
        .unwrap();
    iq::control_store::reinstall_v10_triggers_for_test(&connection).unwrap();
    drop(connection);
    let error = match SqliteQueue::open(&db) {
        Ok(_) => panic!("mismatched cleanup debt passed validated open"),
        Err(error) => error,
    };
    let error = format!("{error:#}");
    assert!(
        error.contains("schema v10 cleanup debt target set differs from queue authority"),
        "{error}"
    );
    assert!(retained.exists());
    let later_status: String = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row(
            "SELECT status FROM queue_items WHERE id=?1",
            [&later.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        later_status, "ready",
        "validated-open rejection changed the later queue item"
    );
}

#[test]
fn repeated_operator_retry_reopen_keeps_one_alert_and_caps_backoff() {
    let (fixture, queue, repository, first, retained) = cancelled_retained_integrator_fixture();
    let db = fixture.temp.path().join("queues.db");
    let log = fixture.temp.path().join("notification.log");
    let notify = fixture.temp.path().join("notify");
    fs::write(
        &notify,
        format!("#!/bin/sh\nprintf 'invoked\\n' >> '{}'\n", log.display()),
    )
    .unwrap();
    fs::set_permissions(&notify, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let notification = queue
        .notification_dispatcher(iq::agent_config::NotificationConfig {
            backends: vec![iq::agent_config::NotificationBackendConfig::Wslg {
                executable: notify.clone(),
            }],
            max_attempts: 3,
            max_event_age_seconds: 3600,
            projection_debt_alert_seconds: 60,
        })
        .unwrap();
    notification.configure().unwrap();
    fs::write(retained.join("dirty.txt"), "preserve\n").unwrap();
    let make_integrator = || {
        fixture
            .integrator(IntegratorOptions {
                repo_key: repository.key.clone(),
                repo_path: fixture.repo.clone(),
                queue_db: db.clone(),
                owner_id: "retry-reopen".into(),
                lease_ttl_seconds: 30,
                base_remote: "origin".into(),
                workspace_root: fixture.temp.path().join("integration-workspaces"),
                rift_database: Some(fixture.rift_database.clone()),
                system_config: fixture.system_config(),
            })
            .unwrap()
    };
    let before_tenth = chrono::Utc::now();
    for _ in 0..10 {
        make_integrator().reset_workspaces().unwrap();
    }
    let after_tenth = chrono::Utc::now();
    let store = ControlStore::open(&db).unwrap();
    let debt = store
        .terminal_workspace_cleanup_debt(&first.id)
        .unwrap()
        .unwrap();
    let (count, next_retry_at) = match debt.state {
        iq::control_store::TerminalWorkspaceCleanupState::Preserved {
            observation_count,
            next_retry_at,
            ..
        } => (observation_count, next_retry_at),
        iq::control_store::TerminalWorkspaceCleanupState::Pending => {
            panic!("missing preserved debt")
        }
    };
    assert_eq!(count, 10);
    assert!(next_retry_at >= before_tenth + chrono::Duration::hours(1));
    assert!(next_retry_at <= after_tenth + chrono::Duration::hours(1));
    let connection = rusqlite::Connection::open(&db).unwrap();
    let event_count: i64 = connection.query_row("SELECT COUNT(*) FROM durable_events WHERE item_id=?1 AND event_type='terminal_workspace_preserved'", [&first.id], |row| row.get(0)).unwrap();
    let delivery_count: i64 = connection.query_row("SELECT COUNT(*) FROM notification_deliveries WHERE event_id=(SELECT alert_event_id FROM terminal_workspace_cleanup_debt WHERE item_id=?1) AND redelivery_of IS NULL", [&first.id], |row| row.get(0)).unwrap();
    assert_eq!((event_count, delivery_count), (1, 1));
    assert_eq!(notification.dispatch_once().unwrap(), 1);
    assert_eq!(fs::read_to_string(&log).unwrap().lines().count(), 1);
    let before_retry = chrono::Utc::now();
    make_integrator().reset_workspaces().unwrap();
    let after_retry = chrono::Utc::now();
    let debt = ControlStore::open(&db)
        .unwrap()
        .terminal_workspace_cleanup_debt(&first.id)
        .unwrap()
        .unwrap();
    let next_retry_at = match debt.state {
        iq::control_store::TerminalWorkspaceCleanupState::Preserved { next_retry_at, .. } => {
            next_retry_at
        }
        iq::control_store::TerminalWorkspaceCleanupState::Pending => panic!("missing debt"),
    };
    assert!(next_retry_at >= before_retry + chrono::Duration::hours(1));
    assert!(next_retry_at <= after_retry + chrono::Duration::hours(1));
    assert!(queue
        .get_item(&first.id)
        .unwrap()
        .workspace
        .path()
        .is_some());
}

#[test]
fn pr_provider_landing_blocks_when_provider_does_not_land_queued_head() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/not-landed", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/not-landed"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/not-landed".into(),
            target_branch: "main".into(),
            current_head_sha: source_head.clone(),
            pr_url: Some("https://github.com/org/repo/pull/8".into()),
            producer_metadata: serde_json::json!({"worker":"W004"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let fake_gh = fixture.temp.path().join("fake-gh-noop");
    let remote = fixture.remote.clone();
    fs::write(
        &fake_gh,
        format!(
            r#"#!/bin/sh
if [ "$1 $2" = "pr view" ]; then
  head=$(git --git-dir={remote} rev-parse refs/heads/agent/not-landed)
  base=$(git --git-dir={remote} rev-parse refs/heads/main)
  printf '{{"headRefOid":"%s","baseRefOid":"%s","reviewDecision":"APPROVED","mergeStateStatus":"CLEAN","statusCheckRollup":[{{"status":"COMPLETED","conclusion":"SUCCESS"}}]}}' "$head" "$base"
  exit 0
fi
if [ "$1 $2" = "pr merge" ]; then
  exit 0
fi
exit 2
"#,
            remote = remote.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake_gh).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_gh, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake_gh);

    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "test-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Integrating));
    assert_eq!(item.blocked_reason, Some(BlockedReason::Provider));
    assert_eq!(item.landed_commit_sha, None);
    let store = iq::control_store::ControlStore::open(&db).unwrap();
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    let iq::control_domain::IntegrationEffortState::ProviderBlocked(blocked) = &effort.state else {
        panic!("provider result did not transition the effort: {effort:?}")
    };
    let iq::control_domain::IntegrationBlocker::ProviderSignoff(provider) = &blocked.blocker else {
        panic!("provider-blocked effort has the wrong blocker")
    };
    assert_eq!(provider.repository, "https://github.com/org/repo/pull/8");
    assert_eq!(provider.candidate_sha, attempt_candidate(&queue, &item));
    assert!(git(
        &fixture.repo,
        [
            "merge-base",
            "--is-ancestor",
            source_head.as_str(),
            "refs/remotes/origin/main",
        ],
    )
    .is_err());
    let retried = integrator.run_once().unwrap().unwrap();
    assert_eq!(retried.status, QueueStatus::Blocked);
    assert_eq!(retried.blocked_reason, Some(BlockedReason::Provider));
    let connection = rusqlite::Connection::open(db).unwrap();
    let cycle_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM integration_cycles", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(cycle_count, 1);
    std::env::remove_var("IQ_GITHUB_CLI");
}

fn attempt_candidate(queue: &SqliteQueue, item: &iq::sqlite::QueueItem) -> String {
    queue
        .get_attempt(item.current_attempt_id.as_deref().unwrap())
        .unwrap()
        .validated_commit_sha
        .unwrap()
}

struct GitFixture {
    _environment: FixtureEnvironment,
    temp: tempfile::TempDir,
    remote: std::path::PathBuf,
    repo: std::path::PathBuf,
    rift_database: std::path::PathBuf,
    runner: std::path::PathBuf,
    validation_command: Mutex<Option<String>>,
}

impl GitFixture {
    fn new(_include_validation: bool) -> Self {
        let environment = FixtureEnvironment::acquire();
        let temp = tempfile::Builder::new()
            .prefix(".iq-integrator-test-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .unwrap();
        let remote = temp.path().join("remote.git");
        git(temp.path(), ["init", "--bare", remote.to_str().unwrap()]).unwrap();
        let repo = temp.path().join("repo");
        git(
            temp.path(),
            ["clone", remote.to_str().unwrap(), repo.to_str().unwrap()],
        )
        .unwrap();
        git(&repo, ["config", "user.email", "iq@example.test"]).unwrap();
        git(&repo, ["config", "user.name", "IQ Test"]).unwrap();
        git(&repo, ["config", "commit.gpgsign", "false"]).unwrap();
        let hooks = temp.path().join("empty-hooks");
        fs::create_dir(&hooks).unwrap();
        git(&repo, ["config", "core.hooksPath", hooks.to_str().unwrap()]).unwrap();
        git(&repo, ["checkout", "-b", "main"]).unwrap();
        fs::write(repo.join("README.md"), "base\n").unwrap();
        git(&repo, ["add", "."]).unwrap();
        git(&repo, ["commit", "-m", "base"]).unwrap();
        git(&repo, ["push", "-u", "origin", "main"]).unwrap();
        let rift_database = temp.path().join("rift.sqlite");
        let output = Command::new("rift")
            .arg("--database")
            .arg(&rift_database)
            .args(["init", "--here"])
            .arg(&repo)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "rift init failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let runner = temp.path().join("fake-opencode");
        fs::write(
            &runner,
            r##"#!/usr/bin/python3
import hashlib
import json
import os
import subprocess

prompt = os.sys.argv[-1]
with open("/iq-protocol/input.json", "r", encoding="utf-8") as source:
    request = json.load(source)
result_path = "/iq-protocol/result.json"
answers = [
    entry["text"]
    for entry in request["validation_evidence"]
    if entry["kind"].startswith("guidance_answer:")
]
if request["conflicts"] and not answers:
    result = {
        "outcome": "guidance_required",
        "version": 1,
        "identity": request["identity"],
        "question": "Resolve the semantic conflict",
        "affected_contracts": ["preserve target and source behavior"],
        "affected_paths": [request["conflicts"][0]["path"]],
        "alternatives": {"kind": "free_text"},
        "evidence": "automatic fixture agent does not choose one conflict side",
    }
else:
    if request["conflicts"]:
        for conflict in request["conflicts"]:
            path = b"/".join(bytes.fromhex(part["hex"]) for part in conflict["path"])
            target = subprocess.run(
                ["git", "show", b":2:" + path], capture_output=True
            ).stdout
            source = subprocess.run(
                ["git", "show", b":3:" + path], capture_output=True
            ).stdout
            combined = target
            if source and source not in combined:
                combined += source
            with open(path, "wb") as output:
                output.write(combined)
            subprocess.check_call(["git", "add", "--", path])
    tree = subprocess.check_output(["git", "write-tree"], text=True).strip()
    names = subprocess.check_output(
        ["git", "diff", "--cached", "--name-only", "-z"]
    ).split(b"\0")
    paths = [
        [{"hex": component.hex()} for component in name.split(b"/")]
        for name in names
        if name
    ]
    result = {
        "outcome": "resolved",
        "version": 1,
        "identity": request["identity"],
        "staged_tree_sha256": hashlib.sha256(tree.encode()).hexdigest(),
        "changed_paths": paths,
        "checks": [],
    }
    if os.path.exists("force-invalid-agent"):
        result["identity"]["cycle_id"] = "invalid-cycle"
temporary = result_path + ".tmp"
with open(temporary, "w", encoding="utf-8") as output:
    json.dump(result, output, separators=(",", ":"))
os.replace(temporary, result_path)
"##,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&runner).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&runner, permissions).unwrap();
        }
        Self {
            _environment: environment,
            temp,
            remote,
            repo,
            rift_database,
            runner,
            validation_command: Mutex::new(None),
        }
    }

    fn system_config(&self) -> iq::agent_config::SystemConfig {
        iq::agent_config::SystemConfig {
            integration_agent: iq::agent_config::IntegrationAgentConfig {
                runner: iq::control_domain::RunnerKind::Opencode,
                executable: self.runner.clone(),
                agent: "iq-integration".into(),
                model: "test/model".into(),
                cycle_timeout_seconds: 30,
                max_log_bytes: 1024 * 1024,
                max_result_bytes: 1024 * 1024,
                max_processes: 16,
                memory_bytes: 256 * 1024 * 1024,
                cpu_seconds: 30,
                writable_bytes: 16 * 1024 * 1024,
                open_files: 128,
                credential_env: "IQ_TEST_MODEL_KEY".into(),
            },
            control_plane: iq::agent_config::ControlPlaneConfig {
                unix_socket: self.temp.path().join("control.sock"),
                max_request_bytes: 4096,
                max_free_text_bytes: 1024,
                max_response_bytes: 4096,
                max_concurrent_clients: 2,
                max_client_queue_bytes: 4096,
                max_stream_backlog_events: 100,
                client_idle_seconds: 5,
            },
            notifications: Default::default(),
        }
    }

    fn create_source_branch(&self, branch: &str, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "-b", branch, "main"]).unwrap();
        if let Some(parent) = self.repo.join(Path::new(path)).parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        if path == ".iq/config.json" {
            track_ignored_file(&self.repo, path);
        } else {
            git(&self.repo, ["add", path]).unwrap();
        }
        git(&self.repo, ["commit", "-m", "feature"]).unwrap();
        let sha = git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap();
        git(
            &self.repo,
            ["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        )
        .unwrap();
        sha
    }

    fn set_validation_command(&self, command: &str) {
        *self.validation_command.lock().unwrap() = Some(command.into());
    }

    fn integrator(&self, options: IntegratorOptions) -> anyhow::Result<Integrator> {
        let policy = self
            .validation_command
            .lock()
            .unwrap()
            .clone()
            .map(|command| IntegrationPolicy::Validation {
                command,
                signoff: HostSignoffPolicy::None,
            })
            .unwrap_or(IntegrationPolicy::NoValidation);
        Integrator::new_with_policy(options, policy)
    }

    fn create_unpublished_target_change(&self, branch: &str, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "-b", branch, "main"]).unwrap();
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        git(&self.repo, ["add", path]).unwrap();
        git(&self.repo, ["commit", "-m", "target moved"]).unwrap();
        let sha = git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap();
        git(
            &self.repo,
            ["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        )
        .unwrap();
        sha
    }

    fn commit_on_main(&self, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "main"]).unwrap();
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        git(&self.repo, ["add", path]).unwrap();
        git(&self.repo, ["commit", "-m", "target change"]).unwrap();
        git(&self.repo, ["push", "origin", "main"]).unwrap();
        git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap()
    }
}
