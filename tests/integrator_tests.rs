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
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::tempdir;

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
    connection
        .execute_batch(include_str!("fixtures/schema-v8-active.sql"))
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
    let server = iq::control_api::ControlApiServer::bind(
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
