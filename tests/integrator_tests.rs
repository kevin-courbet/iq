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
#[cfg(debug_assertions)]
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, PermissionsExt};
#[cfg(debug_assertions)]
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};
use tempfile::tempdir;
#[cfg(debug_assertions)]
use wait_timeout::ChildExt;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn open_queue(path: &std::path::Path) -> SqliteQueue {
    SqliteQueue::open(path).unwrap()
}

fn normalized_database_bytes(database: &Path, snapshot: &Path) -> Vec<u8> {
    let connection = rusqlite::Connection::open(database).unwrap();
    connection
        .execute("VACUUM INTO ?1", [snapshot.to_str().unwrap()])
        .unwrap();
    fs::read(snapshot).unwrap()
}

#[cfg(debug_assertions)]
fn rift_inventory(database: &Path) -> Vec<std::path::PathBuf> {
    let connection = rusqlite::Connection::open(database).unwrap();
    let mut statement = connection
        .prepare("SELECT path FROM rift ORDER BY path")
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(0).map(Into::into))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[cfg(debug_assertions)]
fn rift_ancestors(database: &Path, path: &Path) -> Vec<std::path::PathBuf> {
    let output = Command::new("rift")
        .arg("--database")
        .arg(database)
        .arg("ancestors")
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(Into::into)
        .collect()
}

fn provision_fixture_repository(
    queue: &SqliteQueue,
    fixture: &GitFixture,
) -> iq::sqlite::RegisteredRepository {
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
            },
        )
        .unwrap()
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
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
            },
        )
        .unwrap();
    let repo_key = repository.key.as_str();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/one".into(),
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
    let remote_main = git_output(
        &repository.owned_root_path,
        ["ls-remote", "iq-target", "refs/heads/main"],
    )
    .unwrap()
    .split_whitespace()
    .next()
    .unwrap()
    .to_string();
    assert_eq!(
        item.landed_commit_sha.as_deref(),
        Some(remote_main.as_str())
    );
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
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
            },
        )
        .unwrap();
    let repo_key = repository.key.as_str();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/no-validation".into(),
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/tracked-policy".into(),
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
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
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
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
            },
        )
        .unwrap();
    assert!(repository.owned_root_path.join(".iq/config.json").exists());
    let host_policy_result = Integrator::new_with_policy_and_validated_queue(
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
        queue.clone(),
    );
    let error = match host_policy_result {
        Ok(_) => panic!("registered repository accepted host validation"),
        Err(error) => format!("{error:#}"),
    };
    assert!(
        error.contains("local integration-checkout policy"),
        "{error}"
    );

    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/first".into(),
            current_head_sha: first_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let _second = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/second".into(),
            current_head_sha: second_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
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
    let integration_root = queue.workspace_root_path(&repository.key).unwrap().unwrap();
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
    let persisted_root: (String, String, String, String, u64, u64, i64) = connection
        .query_row(
            "SELECT CAST(source_path AS TEXT),source_rift_id,CAST(root_path AS TEXT),CAST(registry_identity AS TEXT),registry_device,registry_inode,generation FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
            [&repository.key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .unwrap();
    assert_eq!(Path::new(&persisted_root.2), integration_root);
    assert!(connection
        .execute(
            "DELETE FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
            [&repository.key],
        )
        .is_err());
    assert!(sandbox.is_dir());
    assert!(unknown.is_dir());
    assert!(connection
        .execute(
            "UPDATE workspace_roots SET source_rift_id='mismatched-source' WHERE repo_key=?1",
            [&repository.key],
        )
        .is_err());
    assert!(sandbox.is_dir());
    assert!(unknown.is_dir());
    let owner_marker = integration_root.join(".iq-workspace-owner.json");
    let owner_bytes = fs::read(&owner_marker).unwrap();
    fs::remove_file(&owner_marker).unwrap();
    let unowned_error = format!("{:#}", integrator.reset_workspaces().unwrap_err());
    assert!(
        unowned_error.contains("owner marker is missing"),
        "{unowned_error}"
    );
    assert!(sandbox.is_dir());
    assert!(unknown.is_dir());

    fs::write(&owner_marker, &owner_bytes).unwrap();
    let mut mismatched_owner: serde_json::Value = serde_json::from_slice(&owner_bytes).unwrap();
    mismatched_owner["repo_key"] = serde_json::json!("00000000-0000-4000-8000-000000000002");
    fs::write(
        &owner_marker,
        serde_json::to_vec(&mismatched_owner).unwrap(),
    )
    .unwrap();
    let mismatched_error = format!("{:#}", integrator.reset_workspaces().unwrap_err());
    assert!(
        mismatched_error.contains("owned by incompatible configuration"),
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

    let unknown_error = format!("{:#}", manager.cleanup_repo(&repository.key).unwrap_err());
    assert!(unknown_error.contains("unknown entry"), "{unknown_error}");
    assert!(deleted_sandbox.is_dir());
    fs::remove_dir(&unknown).unwrap();
    manager.cleanup_repo(&repository.key).unwrap();
    assert!(!deleted_sandbox.exists());
    assert!(durable_sandbox.is_dir());
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

    let second = integrator.run_once().unwrap().unwrap();
    assert_eq!(second.status, QueueStatus::Integrated);
    let second_attempt = queue
        .get_attempt(second.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert!(matches!(
        iq::composition::verify_policy_snapshot(
            second_attempt.policy_snapshot_json.as_deref().unwrap(),
            second_attempt.policy_digest.as_deref().unwrap(),
        )
        .unwrap()
        .policy,
        ValidationPolicy::Command { .. }
    ));
    std::env::remove_var("IQ_RIFT_DATABASE");
}

#[test]
fn integrator_refuses_to_transition_after_lease_owner_changes() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    let source_head = fixture.create_source_branch("agent/stale-owner", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/stale-owner"]).unwrap();
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    fixture.set_validation_command(&format!(
        "sqlite3 -cmd '.timeout 5000' '{}' \"UPDATE repo_leases SET owner_id='owner-b' WHERE repo_key='{repo_key}'\" && sleep 0.1 && git diff --check",
        db.display()
    ));
    fs::create_dir_all(repository.owned_root_path.join(".iq")).unwrap();
    fs::copy(
        fixture.repo.join(".iq/config.json"),
        repository.owned_root_path.join(".iq/config.json"),
    )
    .unwrap();
    let enqueued = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/stale-owner".into(),
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    git(&fixture.repo, ["checkout", "agent/moved"]).unwrap();
    let repo_key = repository.key.as_str();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/moved".into(),
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
    git(&fixture.repo, ["checkout", "main"]).unwrap();

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

    assert_eq!(
        item.status,
        QueueStatus::Blocked,
        "item={item:?} events={:?}",
        queue.events(&item.id).unwrap()
    );
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Merging));
    assert_eq!(item.blocked_reason, Some(BlockedReason::NeedsAgentFix));
    let remote_main = git_output(
        &repository.owned_root_path,
        ["rev-parse", "refs/remotes/iq-target/main"],
    )
    .unwrap();
    assert_ne!(remote_main, moved_head);
    assert!(git(
        &repository.owned_root_path,
        [
            "merge-base",
            "--is-ancestor",
            &moved_head,
            "refs/remotes/iq-target/main",
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/conflict".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let later = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/later".into(),
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/guidance".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W-guidance"}),
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
        .tempdir_in(std::env::temp_dir())
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
    let remote_main = git_output(
        &repository.owned_root_path,
        ["ls-remote", "iq-target", "refs/heads/main"],
    )
    .unwrap()
    .split_whitespace()
    .next()
    .unwrap()
    .to_string();
    assert_eq!(
        integrated.landed_commit_sha.as_deref(),
        Some(remote_main.as_str())
    );
    let landed = integrated.landed_commit_sha.as_deref().unwrap();
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            ["show", &format!("{landed}:contract.txt")],
        )
        .unwrap(),
        "target behavior\nsource behavior"
    );
    assert_eq!(
        git_output(
            &repository.owned_root_path,
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/invalid-output".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W-invalid"}),
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/launch-restart".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W-launch-restart"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let workspace_root = queue.workspace_root_path(repo_key).unwrap().unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repo_key.into(),
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
        .terminal_cycle_artifacts(repo_key)
        .unwrap()
        .iter()
        .any(|cycle| cycle.cycle_id == interrupted_cycle_id));
    iq::integrator::verify_rift_workspace_config(
        &repository.owned_root_path,
        &workspace_root,
        repo_key,
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
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
            },
        )
        .unwrap();
    let repo_key = repository.key.as_str();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/late-conflict".into(),
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
    let moved_sha =
        fixture.create_unpublished_target_change(moved_branch, "target.txt", "target\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
            },
        )
        .unwrap();
    let initial_target_sha = repository.source_sha.clone();
    let repo_key = repository.key.as_str();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/missing-revalidation".into(),
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
    let landed_sha = item.landed_commit_sha.as_deref().unwrap();
    let remote_sha = git_output(
        &repository.owned_root_path,
        ["ls-remote", "iq-target", "refs/heads/main"],
    )
    .unwrap()
    .split_whitespace()
    .next()
    .unwrap()
    .to_string();
    assert_eq!(remote_sha, landed_sha);
    assert!(git(
        &repository.owned_root_path,
        ["merge-base", "--is-ancestor", &moved_sha, landed_sha],
    )
    .is_ok());
    let attempt = queue
        .get_attempt(item.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(attempt.target_base_sha.as_deref(), Some(moved_sha.as_str()));
    assert!(
        matches!(
            &attempt.moved_base,
            iq::sqlite::MovedBaseState::Applied {
                target_sha,
                candidate_sha,
                ..
            } if target_sha == &moved_sha && candidate_sha == landed_sha
        ),
        "moved base: {:?}, landed: {landed_sha}",
        attempt.moved_base
    );
    let attempt = queue
        .get_attempt(item.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert!(matches!(
        iq::composition::verify_policy_snapshot(
            attempt.policy_snapshot_json.as_deref().unwrap(),
            attempt.policy_digest.as_deref().unwrap(),
        )
        .unwrap()
        .policy,
        ValidationPolicy::Command { .. }
    ));
    assert!(attempt.validation_command.is_some());
    let connection = rusqlite::Connection::open(queue.path()).unwrap();
    let mut statement = connection
        .prepare(
            "SELECT target_base_sha,candidate_sha,validated_commit_sha,invalidated_at FROM validation_invocations WHERE attempt_id=?1 ORDER BY invocation_number",
        )
        .unwrap();
    let invocations = statement
        .query_map([attempt.id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(invocations.len(), 2);
    assert_eq!(invocations[0].0, initial_target_sha);
    assert_ne!(invocations[0].1, invocations[1].1);
    assert!(invocations[0].3.is_some());
    assert_eq!(invocations[1].0, moved_sha);
    assert_eq!(invocations[1].1, landed_sha);
    assert_eq!(invocations[1].2.as_deref(), Some(landed_sha));
    assert!(invocations[1].3.is_none());
}

#[test]
fn direct_landing_push_failure_persists_integrating_block() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/fetch-fails", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/fetch-fails"]).unwrap();
    fixture.set_validation_command("git status --short");
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let hook = fixture.remote.join("hooks/pre-receive");
    fs::write(&hook, "#!/bin/sh\nexit 1\n").unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    git(
        &fixture.remote,
        [
            "config",
            "core.hooksPath",
            fixture.remote.join("hooks").to_str().unwrap(),
        ],
    )
    .unwrap();
    let repo_key = repository.key.as_str();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/fetch-fails".into(),
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

    assert_eq!(
        item.status,
        QueueStatus::Blocked,
        "item={item:?} events={:?}",
        queue.events(&item.id).unwrap()
    );
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/unsupported-provider".into(),
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
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                target_branch: "main".into(),
                remote: "origin".into(),
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
            source_branch: branch.into(),
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
    let error = integrator.run_once().unwrap_err();
    assert!(
        error.to_string().contains("credential"),
        "unexpected integration error: {error:#}"
    );
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
    let owned_status = git_output(
        &repository.owned_root_path,
        ["status", "--porcelain=v1", "--untracked-files=all"],
    )
    .unwrap();
    assert!(owned_status.is_empty(), "{owned_status}");
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
    let retained = queue
        .workspace_root_path(&repository.key)
        .unwrap()
        .unwrap()
        .join(&first.id);
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
    let registered_after_landing = queue.repository(&repository.key).unwrap();
    assert!(
        matches!(
            registered_after_landing.checkout_reconciliation,
            iq::sqlite::CheckoutReconciliationState::Ready(_)
        ),
        "{registered_after_landing:?}"
    );
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
fn daemon_once_opens_current_database_and_completes_cycle() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let source_sha = fixture.create_source_branch("agent/daemon-once", "daemon.txt", "daemon\n");
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key,
            source_branch: "agent/daemon-once".into(),
            current_head_sha: source_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (daemon_config, system_config, _control_directory) =
        write_daemon_runtime_config(&fixture, &db);

    let output = run_daemon_once(&fixture, &db, &daemon_config, &system_config);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output[0]["status"], "integrated");
    let completed = queue.get_item(&item.id).unwrap();
    assert_eq!(completed.status, QueueStatus::Integrated);
    let landed = completed.landed_commit_sha.unwrap();
    assert_eq!(
        git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap(),
        landed
    );
    let connection = rusqlite::Connection::open(&db).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT result FROM integration_attempts WHERE item_id=?1",
                [&item.id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "integrated"
    );
}

#[test]
fn validated_integrator_queue_rejects_different_configured_database_before_mutation() {
    let fixture = GitFixture::new(false);
    let database_a = fixture.temp.path().join("queue-a.db");
    let database_b = fixture.temp.path().join("queue-b.db");
    let queue_a = SqliteQueue::open(&database_a).unwrap();
    drop(SqliteQueue::open(&database_b).unwrap());
    let repository = provision_fixture_repository(&queue_a, &fixture);
    let before_a = fs::read(&database_a).unwrap();
    let before_b = fs::read(&database_b).unwrap();
    let workspace_root = fixture.temp.path().join("validated-queue-workspaces");
    let options = IntegratorOptions {
        repo_key: repository.key,
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
fn daemon_once_rejects_missing_generation_marker_without_any_durable_effect() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let source_sha = fixture.create_source_branch(
        "agent/missing-generation",
        "missing-generation.txt",
        "generation\n",
    );
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/missing-generation".into(),
            current_head_sha: source_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let generation = repository
        .integration_root_path
        .join(".iq-workspace-generation");
    let (daemon_config, system_config, _control) = write_daemon_runtime_config(&fixture, &db);
    fs::remove_file(&generation).unwrap();
    let git_head = git_output(&repository.owned_root_path, ["rev-parse", "HEAD"]).unwrap();
    let git_refs = git_output(&repository.owned_root_path, ["show-ref"]).unwrap();
    let git_status = git_output(&repository.owned_root_path, ["status", "--porcelain=v1"]).unwrap();
    let database_before = normalized_database_bytes(&db, &fixture.temp.path().join("before.db"));
    let rift_before = normalized_database_bytes(
        &fixture.rift_database,
        &fixture.temp.path().join("rift-before.db"),
    );

    let rejected = run_daemon_once(&fixture, &db, &daemon_config, &system_config);

    assert!(!rejected.status.success());
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("inspect IQ workspace generation"),
        "{stderr}"
    );
    assert!(stderr.contains(generation.to_str().unwrap()), "{stderr}");
    assert!(!generation.exists());
    assert_eq!(
        git_output(&repository.owned_root_path, ["rev-parse", "HEAD"]).unwrap(),
        git_head
    );
    assert_eq!(
        git_output(&repository.owned_root_path, ["show-ref"]).unwrap(),
        git_refs
    );
    assert_eq!(
        git_output(&repository.owned_root_path, ["status", "--porcelain=v1"]).unwrap(),
        git_status
    );
    assert_eq!(
        normalized_database_bytes(&db, &fixture.temp.path().join("after.db")),
        database_before
    );
    assert_eq!(
        normalized_database_bytes(
            &fixture.rift_database,
            &fixture.temp.path().join("rift-after.db")
        ),
        rift_before
    );
    assert_eq!(queue.get_item(&item.id).unwrap().status, QueueStatus::Ready);
}

#[test]
fn daemon_once_rejects_independent_database_process_lease_holder() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let (daemon_config, system_config, _control_directory) =
        write_daemon_runtime_config(&fixture, &db);
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
#[cfg(debug_assertions)]
fn daemon_api_failure_stops_daemon_before_lifetime_fences_release() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let (daemon_config, system_config, control_directory) =
        write_daemon_runtime_config(&fixture, &db);
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
        if daemon.try_wait().unwrap().is_some() {
            let mut error = String::new();
            daemon
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut error)
                .unwrap();
            panic!("daemon exited before readiness: {error}");
        }
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
    database: &Path,
) -> (std::path::PathBuf, std::path::PathBuf, tempfile::TempDir) {
    let queue = open_queue(database);
    let repository = provision_fixture_repository(&queue, fixture);
    let daemon_config = fixture.temp.path().join("daemon.yaml");
    let system_config = fixture.temp.path().join("system.yaml");
    let control_directory = tempfile::Builder::new()
        .prefix("iq-control-")
        .tempdir_in(std::env::temp_dir())
        .unwrap();
    fs::set_permissions(control_directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        &daemon_config,
        serde_json::to_vec(&serde_json::json!({
            "repos": [{
                "repo_key": repository.key
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

#[test]
#[cfg(debug_assertions)]
fn integration_generation_crashes_reconcile_pending_marker_and_single_workspace() {
    for boundary in ["integration_recorded", "integration_marker"] {
        let fixture = GitFixture::new(false);
        fixture.set_validation_command("false");
        let database = fixture.temp.path().join("queues.db");
        let queue = open_queue(&database);
        let repository = provision_fixture_repository(&queue, &fixture);
        let inventory_before = rift_inventory(&fixture.rift_database);
        let source_sha = fixture.create_source_branch(
            "agent/generation-crash",
            "generation.txt",
            "generation\n",
        );
        git(
            &fixture.repo,
            ["push", "-u", "origin", "agent/generation-crash"],
        )
        .unwrap();
        let item = queue
            .enqueue(EnqueueRequest {
                repo_key: repository.key.clone(),
                source_branch: "agent/generation-crash".into(),
                current_head_sha: source_sha,
                pr_url: None,
                producer_metadata: serde_json::json!({}),
                state_repository: iq::control_domain::StateRepositorySnapshot::Local,
            })
            .unwrap();
        let (daemon_config, system_config, _control) =
            write_daemon_runtime_config(&fixture, &database);
        let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_MODEL_KEY", "fixture-model-key")
            .env("IQ_TEST_WORKSPACE_GENERATION_STOP_AFTER", boundary)
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
            .unwrap();
        assert_eq!(interrupted.status.code(), Some(84), "boundary {boundary}");
        let connection = rusqlite::Connection::open(&database).unwrap();
        let (root, current, pending): (Vec<u8>, i64, Option<i64>) = connection
            .query_row(
                "SELECT root_path,generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
                [&repository.key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((current, pending), (0, Some(1)));
        let root = std::path::PathBuf::from(std::ffi::OsString::from_vec(root));
        assert_eq!(
            fs::read_to_string(root.join(".iq-workspace-generation"))
                .unwrap()
                .trim()
                .parse::<i64>()
                .unwrap(),
            if boundary == "integration_marker" {
                1
            } else {
                0
            }
        );
        let (status, workspace): (String, Option<String>) = connection
            .query_row(
                "SELECT status,integration_workspace_path FROM queue_items WHERE id=?1",
                [&item.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "merging");
        assert!(workspace.is_some());
        assert_eq!(rift_inventory(&fixture.rift_database), inventory_before);
        assert_eq!(
            rusqlite::Connection::open(&fixture.rift_database)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM trash", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(connection);

        let resumed = run_daemon_once(&fixture, &database, &daemon_config, &system_config);
        assert!(
            resumed.status.success(),
            "boundary {boundary}: {}",
            String::from_utf8_lossy(&resumed.stderr)
        );

        let connection = rusqlite::Connection::open(&database).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
                    [&repository.key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
                )
                .unwrap(),
            (1, None)
        );
        let (status, workspace, rift_id, source_rift_id): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = connection
            .query_row(
                "SELECT status,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id FROM queue_items WHERE id=?1",
                [&item.id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(status, "merging");
        let workspace = std::path::PathBuf::from(workspace.unwrap());
        let rift_id = rift_id.unwrap();
        assert_eq!(
            source_rift_id.as_deref(),
            Some(repository.root_rift_id.as_str())
        );
        assert_eq!(
            fs::read_to_string(workspace.join(".rift")).unwrap().trim(),
            rift_id
        );
        assert_eq!(
            rift_ancestors(&fixture.rift_database, &workspace),
            [repository.owned_root_path]
        );
        let mut expected_inventory = inventory_before;
        expected_inventory.push(workspace.clone());
        expected_inventory.sort();
        assert_eq!(rift_inventory(&fixture.rift_database), expected_inventory);
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM queue_items WHERE integration_workspace_path=?1 AND integration_workspace_rift_id=?2",
                    [workspace.to_str().unwrap(), rift_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            fs::read_dir(&root)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .count(),
            1
        );
        assert_eq!(
            rusqlite::Connection::open(&fixture.rift_database)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM trash", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(!fs::read_dir(&root).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".iq-workspace-generation-")));
    }
}

#[test]
#[cfg(debug_assertions)]
fn initial_target_fetch_keeps_observed_sha_when_remote_moves_before_restart() {
    for boundary in ["observation", "fetch"] {
        let fixture = GitFixture::new(false);
        fixture.set_validation_command("git diff --check");
        let database = fixture.temp.path().join("queues.db");
        let queue = open_queue(&database);
        let repository = provision_fixture_repository(&queue, &fixture);
        let observed_target =
            git_output(&repository.owned_root_path, ["rev-parse", "HEAD"]).unwrap();
        let source_sha = fixture.create_source_branch(
            "agent/target-fetch-crash",
            "target-fetch.txt",
            "target fetch\n",
        );
        git(
            &fixture.repo,
            ["push", "-u", "origin", "agent/target-fetch-crash"],
        )
        .unwrap();
        let item = queue
            .enqueue(EnqueueRequest {
                repo_key: repository.key.clone(),
                source_branch: "agent/target-fetch-crash".into(),
                current_head_sha: source_sha,
                pr_url: None,
                producer_metadata: serde_json::json!({}),
                state_repository: iq::control_domain::StateRepositorySnapshot::Local,
            })
            .unwrap();
        let (daemon_config, system_config, _control) =
            write_daemon_runtime_config(&fixture, &database);
        let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_MODEL_KEY", "fixture-model-key")
            .env("IQ_TEST_TARGET_FETCH_STOP_AFTER", boundary)
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
            .unwrap();
        assert_eq!(interrupted.status.code(), Some(83), "boundary {boundary}");
        let connection = rusqlite::Connection::open(&database).unwrap();
        let (attempt_id, target): (String, Option<String>) = connection
            .query_row(
                "SELECT id,target_base_sha FROM integration_attempts WHERE item_id=?1",
                [&item.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let checkout: String = connection
            .query_row(
                "SELECT checkout_json FROM registered_repositories WHERE repo_key=?1",
                [&repository.key],
                |row| row.get(0),
            )
            .unwrap();
        let checkout: serde_json::Value = serde_json::from_str(&checkout).unwrap();
        assert_eq!(target.as_deref(), Some(observed_target.as_str()));
        assert_eq!(checkout["state"], "pending");
        assert_eq!(checkout["target_sha"], observed_target);
        drop(connection);

        let moved_sha = fixture.create_unpublished_target_change(
            &format!("target/after-{boundary}"),
            "moved-target.txt",
            "moved target\n",
        );
        git(
            &fixture.remote,
            ["update-ref", "refs/heads/main", moved_sha.as_str()],
        )
        .unwrap();

        let resumed = run_daemon_once(&fixture, &database, &daemon_config, &system_config);
        assert!(
            resumed.status.success(),
            "boundary {boundary}: {}",
            String::from_utf8_lossy(&resumed.stderr)
        );
        let reconciled = queue.get_item(&item.id).unwrap();
        assert_eq!(reconciled.status, QueueStatus::Merging);
        assert!(queue
            .repository(&repository.key)
            .unwrap()
            .checkout_reconciliation
            .is_ready_for(&observed_target));
        assert_eq!(
            git_output(
                &repository.owned_root_path,
                ["rev-parse", "refs/remotes/iq-target/main"]
            )
            .unwrap(),
            observed_target
        );
        let resumed = run_daemon_once(&fixture, &database, &daemon_config, &system_config);
        assert!(
            resumed.status.success(),
            "boundary {boundary}: {}",
            String::from_utf8_lossy(&resumed.stderr)
        );
        let completed = queue.get_item(&item.id).unwrap();
        assert_eq!(completed.status, QueueStatus::Integrated);
        let attempt = queue
            .get_attempt(completed.current_attempt_id.as_deref().unwrap())
            .unwrap();
        assert_eq!(attempt.id, attempt_id);
        assert_eq!(attempt.target_base_sha.as_deref(), Some(moved_sha.as_str()));
        assert_eq!(
            git_output(
                &repository.owned_root_path,
                ["rev-parse", &format!("refs/iq/targets/{attempt_id}")],
            )
            .unwrap(),
            observed_target
        );
        let landed = completed.landed_commit_sha.as_deref().unwrap();
        git(
            &repository.owned_root_path,
            [
                "merge-base",
                "--is-ancestor",
                observed_target.as_str(),
                landed,
            ],
        )
        .unwrap();
        git(
            &repository.owned_root_path,
            ["merge-base", "--is-ancestor", moved_sha.as_str(), landed],
        )
        .unwrap();
        let connection = rusqlite::Connection::open(&database).unwrap();
        let invocations: Vec<(String, String, Option<String>, Option<String>)> = connection
            .prepare(
                "SELECT target_base_sha,candidate_sha,validated_commit_sha,invalidated_at FROM validation_invocations WHERE attempt_id=?1 ORDER BY invocation_number",
            )
            .unwrap()
            .query_map([&attempt_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[0].0, observed_target);
        assert!(invocations[0].3.is_some());
        assert_eq!(invocations[1].0, moved_sha);
        assert_eq!(invocations[1].1, landed);
        assert_eq!(invocations[1].2.as_deref(), Some(landed));
        assert!(invocations[1].3.is_none());
        assert_eq!(
            git_output(
                &repository.owned_root_path,
                ["ls-remote", "iq-target", "refs/heads/main"],
            )
            .unwrap()
            .split_whitespace()
            .next(),
            completed.landed_commit_sha.as_deref()
        );
    }
}

#[test]
#[cfg(debug_assertions)]
fn supervised_target_refresh_resumes_observation_before_observing_later_remote_move() {
    let fixture = GitFixture::new(false);
    fixture.set_validation_command("git diff --check");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let observed_a = git_output(&repository.owned_root_path, ["rev-parse", "HEAD"]).unwrap();
    let source_sha = fixture.create_source_branch(
        "agent/supervised-target-refresh",
        "supervised-target.txt",
        "target refresh\n",
    );
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/supervised-target-refresh"],
    )
    .unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/supervised-target-refresh".into(),
            current_head_sha: source_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let (daemon_config, system_config, _control) = write_daemon_runtime_config(&fixture, &database);
    let run_at = |boundary: &str| {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_MODEL_KEY", "fixture-model-key")
            .env("IQ_TEST_SUPERVISED_TARGET_STOP_AFTER", boundary)
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
    };

    assert_eq!(run_at("observation").status.code(), Some(84));
    let connection = rusqlite::Connection::open(&database).unwrap();
    let attempt_id: String = connection
        .query_row(
            "SELECT id FROM integration_attempts WHERE item_id=?1",
            [&item.id],
            |row| row.get(0),
        )
        .unwrap();
    let checkout: String = connection
        .query_row(
            "SELECT checkout_json FROM registered_repositories WHERE repo_key=?1",
            [&repository.key],
            |row| row.get(0),
        )
        .unwrap();
    let checkout: serde_json::Value = serde_json::from_str(&checkout).unwrap();
    assert_eq!(checkout["state"], "pending");
    assert_eq!(checkout["target_sha"], observed_a);
    drop(connection);

    let observed_b = fixture.create_unpublished_target_change(
        "target/after-supervised-observation",
        "moved-supervised-target.txt",
        "moved target\n",
    );
    git(
        &fixture.remote,
        ["update-ref", "refs/heads/main", observed_b.as_str()],
    )
    .unwrap();

    assert_eq!(run_at("reconciled").status.code(), Some(84));
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            ["rev-parse", "refs/remotes/iq-target/main"]
        )
        .unwrap(),
        observed_a
    );
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            [
                "rev-parse",
                &format!("refs/iq/supervised-targets/{attempt_id}/{observed_a}")
            ]
        )
        .unwrap(),
        observed_a
    );
    let checkout: String = rusqlite::Connection::open(&database)
        .unwrap()
        .query_row(
            "SELECT checkout_json FROM registered_repositories WHERE repo_key=?1",
            [&repository.key],
            |row| row.get(0),
        )
        .unwrap();
    let checkout: serde_json::Value = serde_json::from_str(&checkout).unwrap();
    assert_eq!(checkout["state"], "ready");
    assert_eq!(checkout["target_sha"], observed_a);

    let resumed = run_daemon_once(&fixture, &database, &daemon_config, &system_config);
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let completed = queue.get_item(&item.id).unwrap();
    assert_eq!(completed.status, QueueStatus::Integrated);
    git(
        &fixture.remote,
        [
            "merge-base",
            "--is-ancestor",
            observed_b.as_str(),
            completed.landed_commit_sha.as_deref().unwrap(),
        ],
    )
    .unwrap();
}

#[test]
#[cfg(debug_assertions)]
fn pending_target_is_reconciled_before_new_item_observes_remote_movement() {
    let fixture = GitFixture::new(false);
    fixture.set_validation_command("git diff --check");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let observed_a = git_output(&repository.owned_root_path, ["rev-parse", "HEAD"]).unwrap();
    let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_MODEL_KEY", "fixture-model-key")
        .env("IQ_TEST_COMPOSITION_TARGET_STOP_AFTER", "observation")
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "dev-workspace",
            "create",
            "--repo-key",
            &repository.key,
            "--name",
            "pending-target",
        ])
        .output()
        .unwrap();
    assert_eq!(interrupted.status.code(), Some(82));

    let source_sha =
        fixture.create_source_branch("agent/pending-target", "pending-target.txt", "source\n");
    let observed_b = fixture.create_unpublished_target_change(
        "target/after-pending",
        "moved-after-pending.txt",
        "target moved\n",
    );
    git(
        &fixture.remote,
        ["update-ref", "refs/heads/main", observed_b.as_str()],
    )
    .unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/pending-target".into(),
            current_head_sha: source_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: repository.owned_root_path.clone(),
            queue_db: database.clone(),
            owner_id: "pending-target-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "iq-target".into(),
            workspace_root: repository.integration_root_path,
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let reconciled = integrator.run_once().unwrap().unwrap();

    assert_eq!(reconciled.status, QueueStatus::Merging);
    let attempt = queue
        .get_attempt(reconciled.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert!(attempt.target_base_sha.is_none());
    let registered = queue.repository(&repository.key).unwrap();
    assert!(registered.checkout_reconciliation.is_ready_for(&observed_a));
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            ["rev-parse", "refs/remotes/iq-target/main"]
        )
        .unwrap(),
        observed_a
    );
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            [
                "rev-parse",
                &format!(
                    "refs/iq/repository-targets/{}/{}",
                    repository.key, observed_a
                )
            ]
        )
        .unwrap(),
        observed_a
    );

    let mut completed = integrator.run_once().unwrap().unwrap();
    for _ in 0..3 {
        if completed.status != QueueStatus::Merging {
            break;
        }
        completed = integrator.run_once().unwrap().unwrap();
    }

    assert_eq!(completed.status, QueueStatus::Integrated);
    let attempt = queue
        .get_attempt(completed.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(
        attempt.target_base_sha.as_deref(),
        Some(observed_b.as_str())
    );
    git(
        &fixture.remote,
        [
            "merge-base",
            "--is-ancestor",
            observed_b.as_str(),
            completed.landed_commit_sha.as_deref().unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        queue.get_item(&item.id).unwrap().status,
        QueueStatus::Integrated
    );
}

#[test]
fn successful_validation_that_changes_head_is_blocked_without_success_evidence() {
    let fixture = GitFixture::new(false);
    fixture.set_validation_command("git checkout --detach HEAD^");
    let source_head =
        fixture.create_source_branch("agent/validation-head", "feature.txt", "feature\n");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/validation-head".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: repository.owned_root_path,
            queue_db: database.clone(),
            owner_id: "validation-head-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "iq-target".into(),
            workspace_root: repository.integration_root_path,
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();

    assert_eq!(blocked.status, QueueStatus::Blocked);
    assert_eq!(blocked.blocked_phase, Some(BlockedPhase::Validating));
    assert_eq!(blocked.blocked_reason, Some(BlockedReason::Infra));
    let attempt = queue
        .get_attempt(blocked.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert!(attempt.validated_commit_sha.is_none());
    let connection = rusqlite::Connection::open(&database).unwrap();
    let invocation: (String, Option<String>) = connection
        .query_row(
            "SELECT candidate_sha,validated_commit_sha FROM validation_invocations WHERE attempt_id=?1",
            [&attempt.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(invocation.1.is_none());
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM durable_events WHERE item_id=?1 AND event_type='validation_succeeded'",
                [&item.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let state: String = connection
        .query_row(
            "SELECT state FROM integration_efforts WHERE item_id=?1",
            [&item.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "infrastructure_blocked");
    let workspace = Path::new(blocked.workspace.path().unwrap());
    assert_eq!(
        git_output(workspace, ["rev-parse", "HEAD"]).unwrap(),
        invocation.0
    );
    let store = ControlStore::open(&database).unwrap();
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    let uid = unsafe { libc::geteuid() };
    store
        .retry_blocked(
            &effort.id,
            &iq::control_store::ResponderIdentity::LocalPeer { uid },
            uid,
        )
        .unwrap();
    let retried = integrator.run_once().unwrap().unwrap();
    assert_eq!(retried.status, QueueStatus::Blocked);
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM validation_invocations WHERE attempt_id=?1",
                [&attempt.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
}

#[test]
fn validation_changed_head_precedes_dirty_worktree_classification() {
    let fixture = GitFixture::new(false);
    fixture.set_validation_command("IQ_VALIDATION_TEST=1 git checkout --detach HEAD^");
    let source_head =
        fixture.create_source_branch("agent/validation-head-dirty", "feature.txt", "feature\n");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let hooks = fixture.temp.path().join("validation-hooks");
    fs::create_dir(&hooks).unwrap();
    let post_checkout = hooks.join("post-checkout");
    fs::write(
        &post_checkout,
        "#!/bin/sh\nif [ \"$IQ_VALIDATION_TEST\" = 1 ]; then touch validation-dirty.txt; fi\n",
    )
    .unwrap();
    fs::set_permissions(&post_checkout, fs::Permissions::from_mode(0o755)).unwrap();
    git(
        &repository.owned_root_path,
        ["config", "core.hooksPath", hooks.to_str().unwrap()],
    )
    .unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/validation-head-dirty".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: repository.owned_root_path,
            queue_db: database.clone(),
            owner_id: "validation-head-dirty-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "iq-target".into(),
            workspace_root: repository.integration_root_path,
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let mut blocked = integrator.run_once().unwrap().unwrap();
    for _ in 0..3 {
        if blocked.status != QueueStatus::Merging {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
        blocked = integrator.run_once().unwrap().unwrap();
    }

    assert_eq!(blocked.status, QueueStatus::Blocked);
    assert_eq!(blocked.blocked_phase, Some(BlockedPhase::Validating));
    assert_eq!(blocked.blocked_reason, Some(BlockedReason::Infra));
    let attempt = queue
        .get_attempt(blocked.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert!(attempt.validated_commit_sha.is_none());
    let connection = rusqlite::Connection::open(&database).unwrap();
    let invocation: (String, Option<String>) = connection
        .query_row(
            "SELECT candidate_sha,validated_commit_sha FROM validation_invocations WHERE attempt_id=?1",
            [&attempt.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(invocation.1.is_none());
    assert_eq!(
        connection
            .query_row(
                "SELECT state FROM integration_efforts WHERE item_id=?1",
                [&item.id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "infrastructure_blocked"
    );
    let workspace = Path::new(blocked.workspace.path().unwrap());
    assert_eq!(
        git_output(workspace, ["rev-parse", "HEAD"]).unwrap(),
        invocation.0
    );
    assert!(workspace.join("validation-dirty.txt").is_file());
}

#[test]
fn changed_head_with_failed_repair_keeps_invocation_and_retryable_block() {
    let fixture = GitFixture::new(false);
    fixture.set_validation_command(
        "git checkout --detach HEAD^ && touch \"$(git rev-parse --git-path index.lock)\"",
    );
    let source_head =
        fixture.create_source_branch("agent/validation-repair", "feature.txt", "feature\n");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/validation-repair".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: repository.owned_root_path,
            queue_db: database.clone(),
            owner_id: "validation-repair-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "iq-target".into(),
            workspace_root: repository.integration_root_path,
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();

    assert_eq!(blocked.status, QueueStatus::Blocked);
    assert_eq!(blocked.blocked_phase, Some(BlockedPhase::Validating));
    assert_eq!(blocked.blocked_reason, Some(BlockedReason::Infra));
    let attempt_id = blocked.current_attempt_id.as_deref().unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    let blocked_message: String = connection
        .query_row(
            "SELECT blocked_message FROM queue_items WHERE id=?1",
            [&item.id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(blocked_message.contains("candidate repair failed"));
    let (candidate_sha, validated_sha, count): (String, Option<String>, i64) = connection
        .query_row(
            "SELECT candidate_sha,validated_commit_sha,(SELECT COUNT(*) FROM validation_invocations WHERE attempt_id=?1) FROM validation_invocations WHERE attempt_id=?1",
            [attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(validated_sha.is_none());
    assert_eq!(count, 1);
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM durable_events WHERE item_id=?1 AND event_type='validation_succeeded'",
                [&item.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT state FROM integration_efforts WHERE item_id=?1",
                [&item.id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "infrastructure_blocked"
    );
    let workspace = Path::new(blocked.workspace.path().unwrap());
    assert_ne!(
        git_output(workspace, ["rev-parse", "HEAD"]).unwrap(),
        candidate_sha
    );
}

#[test]
fn changed_head_revalidation_with_failed_repair_is_recorded_before_retryable_block() {
    let fixture = GitFixture::new(false);
    let moved_sha = fixture.create_unpublished_target_change(
        "target/revalidation-repair",
        "moved-target.txt",
        "moved target\n",
    );
    let first_validation = fixture.temp.path().join("first-validation-complete");
    fixture.set_validation_command(&format!(
        "if test ! -e '{flag}'; then touch '{flag}'; git --git-dir='{remote}' update-ref refs/heads/main {moved_sha}; git diff --check; else git checkout --detach HEAD^ && touch revalidation-dirty.txt && touch \"$(git rev-parse --git-path index.lock)\"; fi",
        flag = first_validation.display(),
        remote = fixture.remote.display(),
    ));
    let source_head =
        fixture.create_source_branch("agent/revalidation-repair", "feature.txt", "feature\n");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/revalidation-repair".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: repository.owned_root_path,
            queue_db: database.clone(),
            owner_id: "revalidation-repair-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "iq-target".into(),
            workspace_root: repository.integration_root_path,
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let mut blocked = integrator.run_once().unwrap().unwrap();
    for _ in 0..3 {
        if blocked.status != QueueStatus::Merging {
            break;
        }
        blocked = integrator.run_once().unwrap().unwrap();
    }

    assert_eq!(blocked.status, QueueStatus::Blocked);
    assert_eq!(blocked.blocked_phase, Some(BlockedPhase::Validating));
    assert_eq!(blocked.blocked_reason, Some(BlockedReason::Infra));
    let attempt_id = blocked.current_attempt_id.as_deref().unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    let blocked_message: String = connection
        .query_row(
            "SELECT blocked_message FROM queue_items WHERE id=?1",
            [&item.id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(blocked_message.contains("candidate repair failed"));
    let invocations: Vec<(String, Option<String>, Option<String>)> = connection
        .prepare(
            "SELECT candidate_sha,validated_commit_sha,invalidated_at FROM validation_invocations WHERE attempt_id=?1 ORDER BY invocation_number",
        )
        .unwrap()
        .query_map([attempt_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(invocations.len(), 2);
    assert!(invocations[0].1.is_some());
    assert!(invocations[0].2.is_some());
    assert!(invocations[1].1.is_none());
    assert!(invocations[1].2.is_none());
    assert_eq!(
        connection
            .query_row(
                "SELECT state FROM integration_efforts WHERE item_id=?1",
                [&item.id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "infrastructure_blocked"
    );
    let workspace = Path::new(blocked.workspace.path().unwrap());
    assert_ne!(
        git_output(workspace, ["rev-parse", "HEAD"]).unwrap(),
        invocations[1].0
    );
    assert!(workspace.join("revalidation-dirty.txt").is_file());
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key;
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.clone(),
            source_branch: "agent/creation-intent".into(),
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
    let _ = queue
        .claim_next_ready_control_fixture(&repo_key)
        .unwrap()
        .unwrap();
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let repository = queue.repository(&repo_key).unwrap();
    fs::create_dir_all(repository.owned_root_path.join(".iq")).unwrap();
    fs::write(
        repository.owned_root_path.join(".iq/config.json"),
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
    let workspace_root = queue.workspace_root_path(&repo_key).unwrap().unwrap();
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
            repository.owned_root_path.to_str().unwrap(),
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.clone();
    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.clone(),
            source_branch: "agent/intent-backoff".into(),
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
    let (_, _) = queue
        .claim_next_ready_control_fixture(&repo_key)
        .unwrap()
        .unwrap();
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let workspace_root = queue.workspace_root_path(&repo_key).unwrap().unwrap();
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
            repository.owned_root_path.to_str().unwrap(),
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
    iq::control_store::reinstall_cleanup_triggers_for_test(&connection).unwrap();
    drop(connection);
    let error = match SqliteQueue::open(&db) {
        Ok(_) => panic!("mismatched cleanup debt passed validated open"),
        Err(error) => error,
    };
    let error = format!("{error:#}");
    assert!(
        error.contains("IQ local state is incompatible; remove it and reinitialize IQ"),
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
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/not-landed".into(),
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

    assert_eq!(
        item.status,
        QueueStatus::Blocked,
        "item={item:?} events={:?}",
        queue.events(&item.id).unwrap()
    );
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
            [
                "push",
                self.active_remote(),
                &format!("HEAD:refs/heads/{branch}"),
            ],
        )
        .unwrap();
        sha
    }

    fn set_validation_command(&self, command: &str) {
        fs::create_dir_all(self.repo.join(".iq")).unwrap();
        fs::write(
            self.repo.join(".iq/config.json"),
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
    }

    fn integrator(&self, options: IntegratorOptions) -> anyhow::Result<Integrator> {
        let queue = open_queue(&options.queue_db);
        Integrator::new_with_policy_and_validated_queue(
            options,
            IntegrationPolicy::NoValidation,
            queue,
        )
    }

    fn create_unpublished_target_change(&self, branch: &str, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "-b", branch, "main"]).unwrap();
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        git(&self.repo, ["add", path]).unwrap();
        git(&self.repo, ["commit", "-m", "target moved"]).unwrap();
        let sha = git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap();
        git(
            &self.repo,
            [
                "push",
                self.active_remote(),
                &format!("HEAD:refs/heads/{branch}"),
            ],
        )
        .unwrap();
        sha
    }

    fn commit_on_main(&self, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "main"]).unwrap();
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        git(&self.repo, ["add", path]).unwrap();
        git(&self.repo, ["commit", "-m", "target change"]).unwrap();
        git(&self.repo, ["push", self.active_remote(), "main"]).unwrap();
        git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap()
    }

    fn active_remote(&self) -> &str {
        if Command::new("git")
            .args(["remote", "get-url", "iq-target"])
            .current_dir(&self.repo)
            .output()
            .unwrap()
            .status
            .success()
        {
            "iq-target"
        } else {
            "origin"
        }
    }
}
