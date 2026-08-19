use iq::composition::{
    load_local_policy, RepositoryInitOptions, RepositoryManager, SignoffPolicy, ValidationPolicy,
};
use iq::control_store::ControlStore;
use iq::core::{BlockedPhase, BlockedReason, QueueStatus};
use iq::integrator::{HostSignoffPolicy, IntegrationPolicy, Integrator, IntegratorOptions};
use iq::sqlite::SqliteQueue;
mod support;
use std::fs;
use std::io::Read;
#[cfg(debug_assertions)]
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
#[cfg(debug_assertions)]
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};
use support::{direct_policy, managed_test_tempdir, Command};
use tempfile::tempdir;
use wait_timeout::ChildExt;

fn git<I, S>(cwd: &Path, args: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let args = args
        .into_iter()
        .map(|argument| argument.as_ref().to_os_string())
        .collect::<Vec<_>>();
    let mut command = Command::new("/usr/bin/git");
    if args.first().is_some_and(|argument| argument == "init")
        && !args.iter().any(|argument| {
            argument
                .to_str()
                .is_some_and(|argument| argument.starts_with("--object-format="))
        })
    {
        command
            .arg("init")
            .arg("--object-format=sha1")
            .args(&args[1..]);
    } else {
        command.args(args);
    }
    let output = command.current_dir(cwd).output()?;
    if !output.status.success() {
        anyhow::bail!("{}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(())
}

fn git_output<I, S>(cwd: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("/usr/bin/git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        anyhow::bail!("{}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

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

fn with_hostile_git_environment<T>(root: &Path, operation: impl FnOnce() -> T) -> T {
    let hostile = [
        ("GIT_DIR", root.join("hostile-git-dir").into_os_string()),
        (
            "GIT_WORK_TREE",
            root.join("hostile-work-tree").into_os_string(),
        ),
        (
            "GIT_INDEX_FILE",
            root.join("hostile-index").into_os_string(),
        ),
        (
            "GIT_OBJECT_DIRECTORY",
            root.join("hostile-objects").into_os_string(),
        ),
        (
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            root.join("hostile-alternates").into_os_string(),
        ),
        ("GIT_NAMESPACE", "hostile".into()),
        ("GIT_REPLACE_REF_BASE", "refs/hostile/replace/".into()),
        (
            "GIT_COMMON_DIR",
            root.join("hostile-common").into_os_string(),
        ),
        (
            "GIT_CEILING_DIRECTORIES",
            root.join("hostile-ceiling").into_os_string(),
        ),
        ("GIT_DISCOVERY_ACROSS_FILESYSTEM", "1".into()),
        (
            "GIT_CONFIG_GLOBAL",
            root.join("hostile-global-config").into_os_string(),
        ),
        (
            "GIT_CONFIG_SYSTEM",
            root.join("hostile-system-config").into_os_string(),
        ),
        ("GIT_CONFIG_COUNT", "1".into()),
        ("GIT_CONFIG_KEY_0", "core.bare".into()),
        ("GIT_CONFIG_VALUE_0", "true".into()),
        ("GIT_SSH_COMMAND", "false".into()),
        ("GIT_ASKPASS", "false".into()),
        ("SSH_AUTH_SOCK", root.join("hostile-agent").into_os_string()),
        ("SSH_ASKPASS", "false".into()),
        ("SSH_ASKPASS_REQUIRE", "force".into()),
        ("ASKPASS_PROGRAM", "false".into()),
        ("GCM_INTERACTIVE", "always".into()),
    ];
    let previous = hostile
        .iter()
        .map(|(key, _)| (*key, std::env::var_os(key)))
        .collect::<Vec<_>>();
    for (key, value) in hostile {
        std::env::set_var(key, value);
    }
    let result = operation();
    for (key, value) in previous {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
    result
}

struct SshGitServer {
    child: Child,
    port: u16,
    user: String,
    previous_home: Option<std::ffi::OsString>,
    previous_identity: Option<std::ffi::OsString>,
    previous_known_hosts: Option<std::ffi::OsString>,
}

impl SshGitServer {
    fn start(root: &Path, repository: &str) -> Self {
        let ssh = root.join("ssh-server");
        fs::create_dir(&ssh).unwrap();
        let host_key = ssh.join("host-key");
        let client_key = ssh.join("client-key");
        for key in [&host_key, &client_key] {
            let status = Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(key)
                .status()
                .unwrap();
            assert!(status.success());
        }
        let authorized_keys = ssh.join("authorized-keys");
        fs::copy(client_key.with_extension("pub"), &authorized_keys).unwrap();
        fs::set_permissions(&authorized_keys, fs::Permissions::from_mode(0o600)).unwrap();
        let user = git_output(Path::new("/"), ["--version"])
            .and_then(|_| {
                let output = Command::new("id").arg("-un").output()?;
                anyhow::ensure!(output.status.success());
                Ok(String::from_utf8(output.stdout)?.trim().to_string())
            })
            .unwrap();
        let forced = ssh.join("git-service");
        fs::write(
            &forced,
            format!(
                "#!/bin/sh\ncase \"$SSH_ORIGINAL_COMMAND\" in\n  \"git-upload-pack '/{repository}'\") exec /usr/bin/git-upload-pack '{path}' ;;\n  \"git-receive-pack '/{repository}'\") exec /usr/bin/git-receive-pack '{path}' ;;\n  *) exit 126 ;;\nesac\n",
                path = root.join(repository).display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&forced, fs::Permissions::from_mode(0o755)).unwrap();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let config = ssh.join("sshd-config");
        fs::write(
            &config,
            format!(
                "Port {port}\nListenAddress 127.0.0.1\nHostKey {host_key}\nPidFile {pid}\nAuthorizedKeysFile {authorized_keys}\nStrictModes no\nPasswordAuthentication no\nChallengeResponseAuthentication no\nUsePAM no\nPermitRootLogin yes\nAllowUsers {user}\nForceCommand {forced}\nLogLevel ERROR\n",
                host_key = host_key.display(),
                pid = ssh.join("sshd.pid").display(),
                authorized_keys = authorized_keys.display(),
                forced = forced.display(),
            ),
        )
        .unwrap();
        let mut child = Command::new("/usr/sbin/sshd")
            .args(["-D", "-e", "-f"])
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                let mut stderr = String::new();
                child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();
                panic!("sshd exited before readiness with {status}: {stderr}");
            }
            assert!(Instant::now() < deadline, "sshd readiness timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
        let home = ssh.join("home");
        let client_directory = home.join(".ssh");
        fs::create_dir_all(&client_directory).unwrap();
        fs::copy(&client_key, client_directory.join("id_ed25519")).unwrap();
        fs::set_permissions(
            client_directory.join("id_ed25519"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let host_public = fs::read_to_string(host_key.with_extension("pub")).unwrap();
        let mut fields = host_public.split_whitespace();
        let algorithm = fields.next().unwrap();
        let key = fields.next().unwrap();
        let known_hosts = client_directory.join("known_hosts");
        fs::write(
            &known_hosts,
            format!("[127.0.0.1]:{port} {algorithm} {key}\n"),
        )
        .unwrap();
        let previous_home = std::env::var_os("HOME");
        let previous_identity = std::env::var_os("IQ_TEST_SSH_IDENTITY_FILE");
        let previous_known_hosts = std::env::var_os("IQ_TEST_SSH_KNOWN_HOSTS");
        std::env::set_var("HOME", &home);
        std::env::set_var(
            "IQ_TEST_SSH_IDENTITY_FILE",
            client_directory.join("id_ed25519"),
        );
        std::env::set_var("IQ_TEST_SSH_KNOWN_HOSTS", known_hosts);
        Self {
            child,
            port,
            user,
            previous_home,
            previous_identity,
            previous_known_hosts,
        }
    }

    fn url(&self, repository: &str) -> String {
        format!("ssh://{}@127.0.0.1:{}/{repository}", self.user, self.port)
    }
}

impl Drop for SshGitServer {
    fn drop(&mut self) {
        match self.previous_home.take() {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
        match self.previous_identity.take() {
            Some(value) => std::env::set_var("IQ_TEST_SSH_IDENTITY_FILE", value),
            None => std::env::remove_var("IQ_TEST_SSH_IDENTITY_FILE"),
        }
        match self.previous_known_hosts.take() {
            Some(value) => std::env::set_var("IQ_TEST_SSH_KNOWN_HOSTS", value),
            None => std::env::remove_var("IQ_TEST_SSH_KNOWN_HOSTS"),
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap()
}

struct FixtureEnvironment {
    _guard: MutexGuard<'static, ()>,
    model_key: Option<std::ffi::OsString>,
    path: Option<std::ffi::OsString>,
    rift_database: Option<std::ffi::OsString>,
}

impl FixtureEnvironment {
    fn acquire() -> Self {
        let guard = env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model_key = std::env::var_os("IQ_TEST_MODEL_KEY");
        let path = std::env::var_os("PATH");
        let rift_database = std::env::var_os("IQ_RIFT_DATABASE");
        std::env::set_var("IQ_TEST_MODEL_KEY", "fixture-model-key");
        Self {
            _guard: guard,
            model_key,
            path,
            rift_database,
        }
    }
}

impl Drop for FixtureEnvironment {
    fn drop(&mut self) {
        match self.model_key.take() {
            Some(value) => std::env::set_var("IQ_TEST_MODEL_KEY", value),
            None => std::env::remove_var("IQ_TEST_MODEL_KEY"),
        }
        match self.path.take() {
            Some(value) => std::env::set_var("PATH", value),
            None => std::env::remove_var("PATH"),
        }
        match self.rift_database.take() {
            Some(value) => std::env::set_var("IQ_RIFT_DATABASE", value),
            None => std::env::remove_var("IQ_RIFT_DATABASE"),
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
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap();
    let repo_key = repository.key.as_str();
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/one".into(),
            current_head_sha: source_head,
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

    assert_eq!(
        item.status,
        QueueStatus::Integrated,
        "{item:#?}\n{:#?}",
        queue.events(&item.id).unwrap()
    );
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
    assert!(git_output(
        &repository.owned_root_path,
        [
            "for-each-ref",
            "--format=%(refname)",
            "refs/iq/landings/",
            &format!("refs/iq/repository-targets/{repo_key}/"),
        ],
    )
    .unwrap()
    .is_empty());
    assert_eq!(
        rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[cfg(debug_assertions)]
#[test]
fn private_ref_cleanup_recovers_exact_debt_and_rejects_drift() {
    let fixture = GitFixture::new(false);
    let drift_sha = fixture.create_source_branch(
        "agent/private-ref-drift",
        "private-ref.txt",
        "drift object\n",
    );
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let database = fixture.temp.path().join("private-ref-cleanup.db");
    let queue = open_queue(&database);
    let repository = RepositoryManager::new(queue)
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap();
    git(
        &repository.owned_root_path,
        ["fetch", "iq-target", drift_sha.as_str()],
    )
    .unwrap();
    let target_sha = repository.source_sha.clone();
    let stale_ref = format!(
        "refs/iq/repository-targets/{}/{}",
        repository.key, target_sha
    );
    git(
        &repository.owned_root_path,
        ["update-ref", stale_ref.as_str(), target_sha.as_str()],
    )
    .unwrap();
    let system_config = fixture.temp.path().join("system.yaml");
    fs::write(
        &system_config,
        serde_yaml::to_string(&fixture.system_config()).unwrap(),
    )
    .unwrap();
    let cleanup = |stop_after: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iq"));
        command
            .arg("--queue-db")
            .arg(&database)
            .args(["cleanup", "--repo-key", &repository.key, "--system-config"])
            .arg(&system_config);
        if let Some(stop_after) = stop_after {
            command.env("IQ_TEST_PRIVATE_REF_STOP_AFTER", stop_after);
        }
        command.output().unwrap()
    };

    let debt_recorded = cleanup(Some("debt_recorded"));
    assert_eq!(debt_recorded.status.code(), Some(92));
    let connection = rusqlite::Connection::open(&database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    git(
        &repository.owned_root_path,
        ["update-ref", stale_ref.as_str(), drift_sha.as_str()],
    )
    .unwrap();
    let drifted = cleanup(None);
    assert!(!drifted.status.success());
    assert!(String::from_utf8_lossy(&drifted.stderr).contains("drifted"));
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            ["rev-parse", stale_ref.as_str()]
        )
        .unwrap(),
        drift_sha
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );

    git(
        &repository.owned_root_path,
        ["update-ref", stale_ref.as_str(), target_sha.as_str()],
    )
    .unwrap();
    let ref_deleted = cleanup(Some("ref_deleted"));
    assert_eq!(ref_deleted.status.code(), Some(93));
    assert!(git_output(
        &repository.owned_root_path,
        ["for-each-ref", "--format=%(refname)", stale_ref.as_str()],
    )
    .unwrap()
    .is_empty());
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    let absence_verified = cleanup(Some("absence_verified"));
    assert_eq!(absence_verified.status.code(), Some(94));
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    let restarted = cleanup(None);
    assert!(
        restarted.status.success(),
        "{}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn active_runner_cancellation_commits_without_repository_lease_and_stops_monitor() {
    let fixture = GitFixture::new(false);
    fs::write(&fixture.runner, "#!/bin/sh\nsleep 30\n").unwrap();
    fs::set_permissions(&fixture.runner, fs::Permissions::from_mode(0o755)).unwrap();
    let source_head = fixture.create_source_branch(
        "agent/cancel-active-runner",
        "README.md",
        "source conflict\n",
    );
    fixture.commit_on_main("README.md", "target conflict\n");
    let database = fixture.temp.path().join("cancel-active-runner.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/cancel-active-runner".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: database.clone(),
            owner_id: "active-runner-integrator".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let integration = std::thread::spawn(move || integrator.run_once());
    let store = ControlStore::open(&database).unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    let effort = loop {
        if let Some(effort) = store.effort_for_item(&item.id).unwrap() {
            if matches!(
                effort.state,
                iq::control_domain::IntegrationEffortState::AgentRunning(_)
            ) {
                break effort;
            }
        }
        assert!(
            Instant::now() < deadline,
            "runner did not reach active command state"
        );
        std::thread::sleep(Duration::from_millis(10));
    };

    let started = Instant::now();
    let cancellation = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&database)
        .args(["cancel", &item.id])
        .output()
        .unwrap();
    assert!(
        cancellation.status.success(),
        "{}",
        String::from_utf8_lossy(&cancellation.stderr)
    );
    let cancelled = queue.get_item(&item.id).unwrap();

    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(cancelled.status, QueueStatus::Cancelled);
    let connection = rusqlite::Connection::open(&database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM runner_termination_debt WHERE effort_id=?1",
                [&effort.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM durable_events WHERE effort_id=?1 AND event_type='cancelled'",
                [&effort.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let monitored = integration.join().unwrap().unwrap().unwrap();
    assert_eq!(monitored.status, QueueStatus::Cancelled);
    assert!(Path::new(monitored.workspace.path().unwrap()).exists());
    assert_eq!(
        connection
            .query_row(
                "SELECT state FROM terminal_workspace_cleanup_debt WHERE item_id=?1",
                [&item.id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "pending"
    );
}

#[test]
fn canonical_landing_survives_replication_failure_with_exact_debt() {
    let fixture = GitFixture::new(false);
    let source_head =
        fixture.create_source_branch("agent/replication-debt", "replica.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/replication-debt"],
    )
    .unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let replica = fixture.temp.path().join("unavailable-replica.git");
    git(
        fixture.temp.path(),
        ["init", "--bare", replica.to_str().unwrap()],
    )
    .unwrap();
    let replica = replica.canonicalize().unwrap();
    let replica_metadata = replica.metadata().unwrap();
    let mut policy = direct_policy(&fixture.remote);
    policy.replication_policy = iq::repository_policy::ReplicationPolicy::Replicate {
        targets: vec![iq::repository_policy::GitRepository::LocalBare {
            object_format: iq::git_object::GitObjectFormat::Sha1,
            path: replica.clone(),
            device: replica_metadata.dev(),
            inode: replica_metadata.ino(),
        }],
    };
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy,
            },
        )
        .unwrap();
    let offline_replica = fixture.temp.path().join("offline-replica.git");
    fs::rename(&replica, &offline_replica).unwrap();
    let first_item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/replication-debt".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({"worker":"replication"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "replication-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    std::env::set_var("IQ_TEST_CONTROL_NOW", "2100-01-01T00:00:00Z");
    let first_integrated = integrator.run_once().unwrap().unwrap();
    std::env::remove_var("IQ_TEST_CONTROL_NOW");
    assert_eq!(
        first_integrated.status,
        QueueStatus::Integrated,
        "{first_integrated:?}"
    );
    let first_landed = first_integrated.landed_commit_sha.clone().unwrap();
    let first_debt = queue
        .replication_debts(Some(&repository.key))
        .unwrap()
        .remove(0);
    assert_eq!(first_debt.item_id, first_item.id);
    assert_eq!(first_debt.canonical_source_sha, first_landed);
    assert_eq!(first_debt.outcome, "failed");
    assert!(git_output(
        &repository.owned_root_path,
        [
            "for-each-ref",
            "--format=%(refname)",
            &format!(
                "refs/iq/landings/{}",
                first_integrated.current_attempt_id.as_deref().unwrap()
            ),
        ],
    )
    .unwrap()
    .is_empty());
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            [
                "rev-parse",
                &format!("refs/iq/replication/{}", first_debt.id),
            ],
        )
        .unwrap(),
        first_landed
    );
    assert!(first_debt
        .failure
        .as_deref()
        .unwrap()
        .contains("verify replica identity"));
    let malformed_database = fixture.temp.path().join("malformed-replication-debt.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute("VACUUM INTO ?1", [malformed_database.to_str().unwrap()])
        .unwrap();
    let malformed = rusqlite::Connection::open(&malformed_database).unwrap();
    malformed
        .execute_batch(
            "DROP TRIGGER replication_debt_identity_immutable;
             UPDATE replication_debt SET destination_key='malformed-policy-binding';
             CREATE TRIGGER replication_debt_identity_immutable
             BEFORE UPDATE OF id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,created_at
             ON replication_debt
             BEGIN SELECT RAISE(ABORT,'replication debt identity is immutable'); END;",
        )
        .unwrap();
    drop(malformed);
    let malformed_error = match SqliteQueue::open(&malformed_database) {
        Ok(_) => panic!("malformed replication debt opened"),
        Err(error) => error,
    };
    assert!(
        format!("{malformed_error:#}").contains("destination identity is inconsistent"),
        "{malformed_error:#}"
    );
    let hostile_marker = fixture
        .temp
        .path()
        .join("replication-hostile-config-executed");
    let hostile_command = fixture.temp.path().join("replication-hostile-command");
    fs::write(
        &hostile_command,
        format!("#!/bin/sh\n: > '{}'\nexit 1\n", hostile_marker.display()),
    )
    .unwrap();
    fs::set_permissions(&hostile_command, fs::Permissions::from_mode(0o755)).unwrap();
    for (key, value) in [
        ("http.proxy", "http://127.0.0.1:9"),
        ("http.sslVerify", "false"),
        ("filter.hostile.smudge", hostile_command.to_str().unwrap()),
        ("core.hooksPath", hostile_command.to_str().unwrap()),
        ("core.fsmonitor", hostile_command.to_str().unwrap()),
    ] {
        git(
            &repository.owned_root_path,
            ["config", "--local", key, value],
        )
        .unwrap();
    }
    let hostile_retry = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "retry",
            &first_debt.id,
        ])
        .output()
        .unwrap();
    assert!(!hostile_retry.status.success());
    assert_eq!(
        queue.replication_debt(&first_debt.id).unwrap().outcome,
        "failed"
    );
    assert!(!hostile_marker.exists());
    for key in [
        "http.proxy",
        "http.sslVerify",
        "filter.hostile.smudge",
        "core.hooksPath",
        "core.fsmonitor",
    ] {
        git(
            &repository.owned_root_path,
            ["config", "--local", "--unset-all", key],
        )
        .unwrap();
    }
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute(
            "UPDATE replication_debt SET operation='pin_source',outcome='pinning',expected_destination_sha=NULL,application_id=NULL,failure=NULL WHERE id=?1",
            [&first_debt.id],
        )
        .unwrap();
    let source_pin = format!("refs/iq/replication/{}", first_debt.id);
    git(
        &repository.owned_root_path,
        ["update-ref", "-d", source_pin.as_str()],
    )
    .unwrap();
    let interrupted_pin = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_TEST_REPLICATION_STOP_AFTER", "source_pin")
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "retry",
            &first_debt.id,
        ])
        .output()
        .unwrap();
    assert_eq!(interrupted_pin.status.code(), Some(89));
    assert_eq!(
        queue.replication_debt(&first_debt.id).unwrap().operation,
        "pin_source"
    );
    assert!(!replica.exists());
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            ["rev-parse", source_pin.as_str()]
        )
        .unwrap(),
        first_landed
    );
    let recovered_pin = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "retry",
            &first_debt.id,
        ])
        .output()
        .unwrap();
    assert!(recovered_pin.status.success());
    let recovered_pin = queue.replication_debt(&first_debt.id).unwrap();
    assert_eq!(recovered_pin.operation, "resolve_destination");
    assert_eq!(recovered_pin.outcome, "failed");
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            [
                "rev-parse",
                &format!("refs/iq/replication/{}", first_debt.id),
            ],
        )
        .unwrap(),
        first_landed
    );

    let second_head = fixture.create_source_branch(
        "agent/replication-debt-two",
        "replica-two.txt",
        "feature two\n",
    );
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/replication-debt-two"],
    )
    .unwrap();
    let second_item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/replication-debt-two".into(),
            current_head_sha: second_head,
            producer_metadata: serde_json::json!({"worker":"replication-two"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let draining = RepositoryManager::new(queue.clone())
        .begin_draining(&repository.key)
        .unwrap();
    assert!(matches!(
        draining.policy.operation_state,
        iq::repository_policy::OperationState::Draining { ref obligations }
            if obligations.contains(&iq::repository_policy::Obligation::QueueItem { id: second_item.id.clone() })
                && obligations.contains(&iq::repository_policy::Obligation::Replication { id: first_debt.id.clone() })
    ));
    std::env::set_var("IQ_TEST_CONTROL_NOW", "1900-01-01T00:00:00Z");
    let second_integrated = integrator.run_once().unwrap().unwrap();
    std::env::remove_var("IQ_TEST_CONTROL_NOW");
    assert_eq!(
        second_integrated.status,
        QueueStatus::Integrated,
        "{second_integrated:?}"
    );
    let second_landed = second_integrated.landed_commit_sha.clone().unwrap();
    assert_eq!(
        git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap(),
        second_landed
    );
    let debts = queue.replication_debts(Some(&repository.key)).unwrap();
    assert_eq!(
        debts.iter().map(|debt| debt.sequence).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let equal_timestamp_database = fixture.temp.path().join("equal-timestamp-replication.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute(
            "VACUUM INTO ?1",
            [equal_timestamp_database.to_str().unwrap()],
        )
        .unwrap();
    let equal_connection = rusqlite::Connection::open(&equal_timestamp_database).unwrap();
    let immutable_trigger: String = equal_connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='replication_debt_identity_immutable'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    equal_connection
        .execute_batch("DROP TRIGGER replication_debt_identity_immutable")
        .unwrap();
    equal_connection
        .execute(
            "UPDATE replication_debt SET created_at='2000-01-01T00:00:00Z'",
            [],
        )
        .unwrap();
    equal_connection.execute_batch(&immutable_trigger).unwrap();
    drop(equal_connection);
    assert_eq!(
        SqliteQueue::open(&equal_timestamp_database)
            .unwrap()
            .replication_debts(Some(&repository.key))
            .unwrap()
            .iter()
            .map(|debt| debt.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(debts.len(), 2);
    assert_eq!(debts[0].canonical_source_sha, first_landed);
    assert_eq!(debts[1].canonical_source_sha, second_landed);
    assert_eq!(debts[0].outcome, "failed");
    assert_eq!(debts[1].outcome, "pending");
    drop(integrator);

    let status = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "status",
            "--repo-key",
            &repository.key,
        ])
        .output()
        .unwrap();
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status[0]["outcome"], "failed");
    assert_eq!(status[1]["outcome"], "pending");

    fs::rename(&offline_replica, &replica).unwrap();
    let replica_ref_before = Command::new("git")
        .args([
            "--git-dir",
            replica.to_str().unwrap(),
            "show-ref",
            "--verify",
            "refs/heads/main",
        ])
        .output()
        .unwrap();
    let newer_first = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "retry",
            &debts[1].id,
        ])
        .output()
        .unwrap();
    assert_eq!(newer_first.status.code(), Some(1));
    assert!(newer_first.stdout.is_empty());
    assert_eq!(
        String::from_utf8(newer_first.stderr).unwrap(),
        format!(
            "Error: replication debt {} is blocked by older debt {} for the same target\n",
            debts[1].id, debts[0].id
        )
    );
    let replica_ref_after = Command::new("git")
        .args([
            "--git-dir",
            replica.to_str().unwrap(),
            "show-ref",
            "--verify",
            "refs/heads/main",
        ])
        .output()
        .unwrap();
    assert_eq!(
        replica_ref_after.status.code(),
        replica_ref_before.status.code()
    );
    assert_eq!(replica_ref_after.stdout, replica_ref_before.stdout);
    let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_TEST_REPLICATION_STOP_AFTER", "applying")
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "retry",
            &debts[0].id,
        ])
        .status()
        .unwrap();
    assert_eq!(interrupted.code(), Some(88));
    let applying = queue.replication_debt(&debts[0].id).unwrap();
    assert_eq!(applying.outcome, "applying");
    let application_id = applying.application_id.clone().unwrap();
    let expected_destination_sha = applying.expected_destination_sha.clone().unwrap();
    let base = git_output(&fixture.remote, ["rev-parse", &format!("{first_landed}^")]).unwrap();
    git(
        &fixture.repo,
        [
            "push",
            replica.to_str().unwrap(),
            &format!("{base}:refs/heads/main"),
        ],
    )
    .unwrap();
    let uncertain = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "retry",
            &debts[0].id,
        ])
        .output()
        .unwrap();
    assert!(uncertain.status.success());
    let uncertain_debt = queue.replication_debt(&debts[0].id).unwrap();
    assert_eq!(uncertain_debt.outcome, "uncertain");
    assert_eq!(
        uncertain_debt.application_id.as_deref(),
        Some(application_id.as_str())
    );
    assert_eq!(
        uncertain_debt.expected_destination_sha.as_deref(),
        Some(expected_destination_sha.as_str())
    );
    git(
        &repository.owned_root_path,
        [
            "push",
            replica.to_str().unwrap(),
            &format!("{second_landed}:refs/heads/main"),
        ],
    )
    .unwrap();
    git(
        &repository.owned_root_path,
        [
            "update-ref",
            "-d",
            &format!("refs/iq/replication/{}", debts[1].id),
        ],
    )
    .unwrap();
    assert_eq!(
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute(
                "UPDATE replication_debt SET outcome='succeeded',updated_at=created_at WHERE id=?1 AND outcome='pending'",
                [&debts[1].id],
            )
            .unwrap(),
        1
    );
    let reverse_push_marker = fixture.temp.path().join("reverse-replication-push");
    let pre_receive = replica.join("hooks/pre-receive");
    fs::write(
        &pre_receive,
        format!("#!/bin/sh\n: > '{}'\n", reverse_push_marker.display()),
    )
    .unwrap();
    fs::set_permissions(&pre_receive, fs::Permissions::from_mode(0o755)).unwrap();
    let retry_older = |stop_after: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iq"));
        command
            .env("GIT_DIR", &offline_replica)
            .env("GIT_WORK_TREE", &fixture.repo)
            .env("GIT_OBJECT_DIRECTORY", &offline_replica)
            .env("GIT_ALTERNATE_OBJECT_DIRECTORIES", &offline_replica)
            .env("GIT_NAMESPACE", "hostile")
            .env("GIT_REPLACE_REF_BASE", "refs/hostile/replace/")
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "protocol.ext.allow")
            .env("GIT_CONFIG_VALUE_0", "always")
            .env("GIT_SSH_COMMAND", "false")
            .env("SSH_ASKPASS", "false")
            .args([
                "--queue-db",
                db.to_str().unwrap(),
                "replication",
                "retry",
                &debts[0].id,
            ]);
        if let Some(stop_after) = stop_after {
            command.env("IQ_TEST_REPLICATION_STOP_AFTER", stop_after);
        }
        command.output().unwrap()
    };
    let recorded = retry_older(Some("supersession_recorded"));
    assert_eq!(recorded.status.code(), Some(90));
    assert_eq!(
        queue.replication_debt(&debts[0].id).unwrap().outcome,
        "superseded_cleanup_pending"
    );
    assert!(Command::new("/usr/bin/git")
        .args([
            "show-ref",
            "--verify",
            &format!("refs/iq/replication/{}", debts[0].id),
        ])
        .current_dir(&repository.owned_root_path)
        .output()
        .unwrap()
        .status
        .success());
    SqliteQueue::open(&db).unwrap();

    let preserved_ref = format!("refs/iq/replication/{}", debts[0].id);
    git(
        &repository.owned_root_path,
        ["update-ref", &preserved_ref, &second_landed, &first_landed],
    )
    .unwrap();
    let drifted = retry_older(None);
    assert!(!drifted.status.success());
    assert!(
        String::from_utf8_lossy(&drifted.stderr)
            .contains("replication source pin drifted from canonical source authority"),
        "{}",
        String::from_utf8_lossy(&drifted.stderr)
    );
    assert_eq!(
        queue.replication_debt(&debts[0].id).unwrap().outcome,
        "superseded_cleanup_pending"
    );
    assert_eq!(
        git_output(&repository.owned_root_path, ["rev-parse", &preserved_ref]).unwrap(),
        second_landed
    );
    git(
        &repository.owned_root_path,
        ["update-ref", &preserved_ref, &first_landed, &second_landed],
    )
    .unwrap();

    let pin_deleted = retry_older(Some("supersession_pin_deleted"));
    assert_eq!(pin_deleted.status.code(), Some(91));
    assert_eq!(
        queue.replication_debt(&debts[0].id).unwrap().outcome,
        "superseded_cleanup_pending"
    );
    assert!(!Command::new("/usr/bin/git")
        .args([
            "show-ref",
            "--verify",
            &format!("refs/iq/replication/{}", debts[0].id),
        ])
        .current_dir(&repository.owned_root_path)
        .output()
        .unwrap()
        .status
        .success());
    SqliteQueue::open(&db).unwrap();

    let older_retry = retry_older(None);
    assert!(
        older_retry.status.success(),
        "{}",
        String::from_utf8_lossy(&older_retry.stderr)
    );
    let older_retry: serde_json::Value = serde_json::from_slice(&older_retry.stdout).unwrap();
    assert_eq!(older_retry["outcome"], "superseded");
    assert_eq!(older_retry["superseded_by_id"], debts[1].id);
    assert!(!reverse_push_marker.exists());
    assert!(!Command::new("/usr/bin/git")
        .args([
            "show-ref",
            "--verify",
            &format!("refs/iq/replication/{}", debts[0].id),
        ])
        .current_dir(&repository.owned_root_path)
        .output()
        .unwrap()
        .status
        .success());
    assert_eq!(
        git_output(&replica, ["rev-parse", "refs/heads/main"]).unwrap(),
        second_landed
    );
    assert!(matches!(
        RepositoryManager::new(queue.clone())
            .disable_drained(&repository.key)
            .unwrap()
            .policy
            .operation_state,
        iq::repository_policy::OperationState::Disabled
    ));
    let denied_retry = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "replication",
            "retry",
            &debts[0].id,
        ])
        .output()
        .unwrap();
    assert!(!denied_retry.status.success());
    assert!(String::from_utf8_lossy(&denied_retry.stderr).contains("repository is disabled"));
    assert_eq!(
        git_output(&replica, ["rev-parse", "refs/heads/main"]).unwrap(),
        second_landed
    );
}

#[test]
fn sha256_local_submission_lands_and_recovers_replication_to_empty_target() {
    let fixture = GitFixture::new_with_object_format(iq::git_object::GitObjectFormat::Sha256);
    let database = fixture.temp.path().join("sha256-lifecycle.db");
    let queue = open_queue(&database);
    let replica = fixture.temp.path().join("sha256-replica.git");
    git(
        fixture.temp.path(),
        [
            "init",
            "--bare",
            "--object-format=sha256",
            replica.to_str().unwrap(),
        ],
    )
    .unwrap();
    let replica = replica.canonicalize().unwrap();
    let replica_metadata = replica.metadata().unwrap();
    let mut policy = direct_policy(&fixture.remote);
    policy.replication_policy = iq::repository_policy::ReplicationPolicy::Replicate {
        targets: vec![iq::repository_policy::GitRepository::LocalBare {
            path: replica.clone(),
            device: replica_metadata.dev(),
            inode: replica_metadata.ino(),
            object_format: iq::git_object::GitObjectFormat::Sha256,
        }],
    };
    let manager = RepositoryManager::new(queue.clone());
    let repository = manager
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy,
            },
        )
        .unwrap();
    assert_eq!(
        iq::git_command::RepositoryBinding::capture(&repository.owned_root_path)
            .unwrap()
            .object_format,
        iq::git_object::GitObjectFormat::Sha256
    );

    let workspace = manager
        .create_workspace(&repository.key, "sha256-direct")
        .unwrap();
    assert_eq!(workspace.base_sha.len(), 64);
    fs::write(workspace.path.join("sha256.txt"), "sha256 lifecycle\n").unwrap();
    git(&workspace.path, ["add", "sha256.txt"]).unwrap();
    git(
        &workspace.path,
        [
            "-c",
            "user.name=IQ Test",
            "-c",
            "user.email=iq@example.test",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "sha256 direct submission",
        ],
    )
    .unwrap();
    let (_, item) = manager.submit(&workspace.id, None).unwrap();

    let unavailable_replica = fixture.temp.path().join("sha256-replica-unavailable.git");
    fs::rename(&replica, &unavailable_replica).unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: database,
            owner_id: "sha256-lifecycle".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: repository.integration_root_path.clone(),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let mut integrated = None;
    for _ in 0..10 {
        let current = integrator.run_once().unwrap().unwrap();
        let complete = current.status == QueueStatus::Integrated;
        integrated = Some(current);
        if complete {
            break;
        }
    }
    let integrated = integrated.unwrap();
    assert_eq!(integrated.id, item.id);
    assert_eq!(
        integrated.status,
        QueueStatus::Integrated,
        "{integrated:#?}"
    );
    assert_eq!(integrated.landed_commit_sha.as_ref().unwrap().len(), 64);

    let debt = queue
        .replication_debts(Some(&repository.key))
        .unwrap()
        .remove(0);
    assert_eq!(debt.outcome, "failed");
    let source_pin = format!("refs/iq/replication/{}", debt.id);
    assert_eq!(
        git_output(&repository.owned_root_path, ["rev-parse", &source_pin]).unwrap(),
        integrated.landed_commit_sha.clone().unwrap()
    );

    fs::rename(&unavailable_replica, &replica).unwrap();
    let completed = manager.retry_replication(&debt.id).unwrap();
    assert_eq!(completed.outcome, "succeeded");
    assert_eq!(
        git_output(&replica, ["rev-parse", "refs/heads/main"]).unwrap(),
        integrated.landed_commit_sha.unwrap()
    );
    assert_eq!(
        Command::new("/usr/bin/git")
            .current_dir(&repository.owned_root_path)
            .args(["rev-parse", "--verify", "--quiet", &source_pin])
            .status()
            .unwrap()
            .code(),
        Some(1)
    );
}

#[test]
fn retained_rift_fixture_is_removed_during_failure_unwind() {
    let mut fixture_root = None;
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let fixture = GitFixture::new(false);
        fixture_root = Some(fixture.temp.path().to_path_buf());
        let queue = open_queue(&fixture.temp.path().join("failure-cleanup.db"));
        let repository = provision_fixture_repository(&queue, &fixture);
        RepositoryManager::new(queue)
            .create_workspace(&repository.key, "failure-cleanup")
            .unwrap();
        panic!("exercise fixture cleanup during failure unwind");
    }));

    assert!(failed.is_err());
    assert!(!fixture_root.unwrap().exists());
}

#[test]
fn accessible_replica_rejects_non_default_fetch_and_push_ports() {
    let fixture = GitFixture::new(false);
    let mut policy = direct_policy(&fixture.remote);
    policy.replication_policy = iq::repository_policy::ReplicationPolicy::Replicate {
        targets: vec![iq::repository_policy::GitRepository::Accessible {
            object_format: iq::git_object::GitObjectFormat::Sha1,
            fetch_url: "ssh://github.com:2222/org/replica.git".into(),
            push_url: "ssh://github.com:2222/org/replica.git".into(),
            repository_id: "replica-repository-id".into(),
            provider: iq::repository_policy::ProviderRepository {
                provider: iq::repository_policy::Provider::Github,
                host: "github.com".into(),
                repository: "org/replica".into(),
                repository_id: "replica-repository-id".into(),
            },
        }],
    };
    let error = policy.validate().unwrap_err();
    assert!(format!("{error:#}").contains("non-default endpoint port"));
}

#[test]
fn replication_debt_pins_item_landing_when_canonical_advances_after_push() {
    let fixture = GitFixture::new(false);
    let source_head =
        fixture.create_source_branch("agent/replication-race", "race-replica.txt", "source\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/replication-race"],
    )
    .unwrap();
    let hook = fixture.remote.join("hooks/post-receive");
    fs::write(
        &hook,
        "#!/bin/sh\nwhile read old new ref; do\n  [ \"$ref\" = refs/heads/main ] || continue\n  tree=$(/usr/bin/git rev-parse \"$new^{tree}\") || exit $?\n  child=$(printf 'advance after IQ push\\n' | GIT_AUTHOR_NAME=Race GIT_AUTHOR_EMAIL=race@example.test GIT_COMMITTER_NAME=Race GIT_COMMITTER_EMAIL=race@example.test /usr/bin/git commit-tree \"$tree\" -p \"$new\") || exit $?\n  /usr/bin/git update-ref refs/heads/main \"$child\" \"$new\" || exit $?\ndone\n",
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    let replica = fixture.temp.path().join("race-replica.git");
    git(
        fixture.temp.path(),
        ["init", "--bare", replica.to_str().unwrap()],
    )
    .unwrap();
    let replica = replica.canonicalize().unwrap();
    let metadata = replica.metadata().unwrap();
    let db = fixture.temp.path().join("replication-race.db");
    let queue = open_queue(&db);
    let mut policy = direct_policy(&fixture.remote);
    policy.replication_policy = iq::repository_policy::ReplicationPolicy::Replicate {
        targets: vec![iq::repository_policy::GitRepository::LocalBare {
            object_format: iq::git_object::GitObjectFormat::Sha1,
            path: replica.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
        }],
    };
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy,
            },
        )
        .unwrap();
    let offline = fixture.temp.path().join("race-replica-offline.git");
    fs::rename(&replica, &offline).unwrap();
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/replication-race".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrated = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "replication-race".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("race-workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap()
        .run_once()
        .unwrap()
        .unwrap();
    let landed = integrated.landed_commit_sha.unwrap();
    let canonical_head = git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap();
    assert_ne!(canonical_head, landed);
    assert!(Command::new("/usr/bin/git")
        .args([
            "--git-dir",
            fixture.remote.to_str().unwrap(),
            "merge-base",
            "--is-ancestor",
            &landed,
            &canonical_head,
        ])
        .status()
        .unwrap()
        .success());
    let debt = queue
        .replication_debts(Some(&repository.key))
        .unwrap()
        .remove(0);
    assert_eq!(debt.item_id, item.id);
    assert_eq!(debt.canonical_source_sha, landed);
    assert_eq!(
        git_output(
            &repository.owned_root_path,
            ["rev-parse", &format!("refs/iq/replication/{}", debt.id)],
        )
        .unwrap(),
        landed
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
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap();
    let repo_key = repository.key.as_str();
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/no-validation".into(),
            current_head_sha: source_head,
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

    assert_eq!(item.status, QueueStatus::Integrated, "{item:#?}");
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
    iq::git_command::authorize_current(temp.path()).unwrap();

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
    iq::git_command::authorize_current(temp.path()).unwrap();
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/tracked-policy".into(),
            current_head_sha: source_head,
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
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
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

    let first = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/first".into(),
            current_head_sha: first_sha,
            producer_metadata: serde_json::json!({"worker":"W001"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let _second = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/second".into(),
            current_head_sha: second_sha,
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
    iq::agent_runner::write_test_sandbox_ownership(&sandbox, &artifacts.cycle_id).unwrap();
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
    iq::agent_runner::write_test_sandbox_ownership(&sandbox, &artifacts.cycle_id).unwrap();
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
    iq::agent_runner::write_test_sandbox_ownership(&deleted_sandbox, &deleted_cycle_id).unwrap();
    let durable_cycle_id = artifacts.cycle_id.clone();
    connection
        .execute(
            "UPDATE integration_cycles SET status='starting',failure_json=NULL,finished_at=NULL WHERE id=?1",
            [&durable_cycle_id],
        )
        .unwrap();
    let durable_sandbox = integration_root.join(format!(".iq-agent-sandbox-{durable_cycle_id}"));
    fs::create_dir_all(durable_sandbox.join("export")).unwrap();
    iq::agent_runner::write_test_sandbox_ownership(&durable_sandbox, &durable_cycle_id).unwrap();
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
    let enqueued = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/stale-owner".into(),
            current_head_sha: source_head,
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
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/moved".into(),
            current_head_sha: source_head,
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
    let first = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/conflict".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({"worker":"W002"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let later = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/later".into(),
            current_head_sha: later_head,
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
    let _github_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Github,
        &provider,
    )
    .unwrap();
    let _gitlab_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Gitlab,
        &provider,
    )
    .unwrap();
    let source_head =
        fixture.create_source_branch("agent/guidance", "contract.txt", "source behavior\n");
    fixture.commit_on_main("contract.txt", "target behavior\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/guidance".into(),
            current_head_sha: source_head,
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
    assert_eq!(blocked.status, QueueStatus::Blocked, "{blocked:#?}");
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/invalid-output".into(),
            current_head_sha: source_head,
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
fn prelaunch_credential_failure_creates_no_launch_authority_or_artifacts() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/launch-restart", "feature.txt", "feature\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let repo_key = repository.key.as_str();
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/launch-restart".into(),
            current_head_sha: source_head,
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
    assert!(matches!(
        effort.state,
        iq::control_domain::IntegrationEffortState::AgentReady(_)
    ));
    let retained = Path::new(&effort.workspace.path);
    assert!(!retained.join(".iq-agent-protocol").exists());
    assert!(
        !retained.parent().unwrap().read_dir().unwrap().any(|entry| {
            entry.is_ok_and(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".iq-agent-sandbox-")
            })
        })
    );

    std::env::set_var("IQ_TEST_MODEL_KEY", "fixture-model-key");
    let candidate = integrator.resume_item(&item.id).unwrap();
    assert_eq!(candidate.status, QueueStatus::Merged);

    let integrated = integrator.run_once().unwrap().unwrap();
    assert_eq!(integrated.status, QueueStatus::Integrated);
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
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap();
    let repo_key = repository.key.as_str();
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/late-conflict".into(),
            current_head_sha: source_head,
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
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap();
    let initial_target_sha = repository.source_sha.clone();
    let repo_key = repository.key.as_str();
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/missing-revalidation".into(),
            current_head_sha: source_head,
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
#[cfg(debug_assertions)]
fn target_move_commit_crash_resumes_oldest_item_before_later_fifo_work() {
    let fixture = GitFixture::new(true);
    let first_head =
        fixture.create_source_branch("agent/target-move-crash-first", "first.txt", "first\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/target-move-crash-first"],
    )
    .unwrap();
    let second_head =
        fixture.create_source_branch("agent/target-move-crash-second", "second.txt", "second\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/target-move-crash-second"],
    )
    .unwrap();
    let moved_branch = "target/target-move-crash";
    fixture.set_validation_command(&format!(
        "git --git-dir={} update-ref refs/heads/main $(git --git-dir={} rev-parse refs/heads/{moved_branch})",
        fixture.remote.display(),
        fixture.remote.display(),
    ));
    let moved_sha =
        fixture.create_unpublished_target_change(moved_branch, "target.txt", "target\n");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    git(&fixture.repo, ["checkout", "main"]).unwrap();
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap();
    let manager = RepositoryManager::new(queue.clone());
    let first = manager
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/target-move-crash-first".into(),
            current_head_sha: first_head,
            producer_metadata: serde_json::json!({"worker":"first"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let second = manager
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/target-move-crash-second".into(),
            current_head_sha: second_head,
            producer_metadata: serde_json::json!({"worker":"second"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: repository.owned_root_path,
            queue_db: database.clone(),
            owner_id: "target-move-crash-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "iq-target".into(),
            workspace_root: repository.integration_root_path,
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    iq::control_store::set_target_move_commit_failure_test_hook(&database, true);

    let interrupted = integrator.run_once().unwrap_err();

    assert!(
        format!("{interrupted:#}").contains("durable target-move commit"),
        "{interrupted:#}"
    );
    let pending = ControlStore::open(&database)
        .unwrap()
        .effort_for_item(&first.id)
        .unwrap()
        .unwrap();
    assert!(matches!(
        pending.state,
        iq::control_domain::IntegrationEffortState::TargetMovePending(ref movement)
            if movement.target_sha == moved_sha
    ));
    assert_eq!(
        queue.get_item(&second.id).unwrap().status,
        QueueStatus::Ready
    );

    let system_config = fixture.temp.path().join("target-move-crash-system.yaml");
    fs::write(
        &system_config,
        serde_yaml::to_string(&fixture.system_config()).unwrap(),
    )
    .unwrap();
    let candidate_cleared = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_RECOMPOSITION_STOP_AFTER", "candidate_cleared")
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "integrate",
            "--next",
            "--repo-key",
            &repository.key,
            "--system-config",
            system_config.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(candidate_cleared.status.code(), Some(94));
    let connection = rusqlite::Connection::open(&database).unwrap();
    let (candidate_evidence, validated_sha): (i64, Option<String>) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM candidate_evidence WHERE effort_id=effort.id),attempt.validated_commit_sha FROM integration_efforts effort JOIN integration_attempts attempt ON attempt.id=effort.attempt_id WHERE effort.item_id=?1",
            [&first.id],
            |row| Ok((row.get(0)?,row.get(1)?)),
        )
        .unwrap();
    assert_eq!(candidate_evidence, 1);
    assert!(validated_sha.is_some());

    let recovered = integrator.run_once().unwrap().unwrap();

    assert_eq!(recovered.id, first.id);
    assert_eq!(recovered.status, QueueStatus::Integrated);
    assert_eq!(
        queue.get_item(&second.id).unwrap().status,
        QueueStatus::Ready
    );
}

#[test]
fn mr_required_cli_blocks_before_mutation_and_cancels_hung_provider_gate() {
    let fixture = GitFixture::new(false);
    let provider_root = fixture.temp.path().join("provider-root");
    let canonical = provider_root.join("org/repo.git");
    fs::create_dir_all(canonical.parent().unwrap()).unwrap();
    git(
        fixture.temp.path(),
        ["init", "--bare", canonical.to_str().unwrap()],
    )
    .unwrap();
    git(
        &fixture.repo,
        [
            "push",
            canonical.to_str().unwrap(),
            "refs/heads/main:refs/heads/main",
        ],
    )
    .unwrap();
    let source_head = fixture.create_source_branch(
        "agent/provider-blocked",
        "provider.txt",
        "provider source\n",
    );
    git(
        &fixture.repo,
        [
            "push",
            canonical.to_str().unwrap(),
            "refs/heads/agent/provider-blocked:refs/heads/agent/provider-blocked",
        ],
    )
    .unwrap();
    git(
        &canonical,
        ["update-ref", "refs/pull/8/head", source_head.as_str()],
    )
    .unwrap();
    let base_sha = git_output(&canonical, ["rev-parse", "refs/heads/main"]).unwrap();
    let provider = fixture.temp.path().join("controlled-gh");
    let provider_log = fixture.temp.path().join("controlled-gh.log");
    let merge_marker = fixture.temp.path().join("provider-merge-called");
    fs::write(
        &provider,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
if [ "$1 $2" = "api --hostname" ]; then
  case "$4" in
    repos/org/repo) printf '%s' '{{"node_id":"provider-repository-id","full_name":"org/repo"}}' ;;
    repos/org/repo/hash-algorithm) printf '%s' '{{"hash_algorithm":"sha1"}}' ;;
    *) exit 3 ;;
  esac
  exit 0
fi
if [ "$1 $2" = "pr view" ]; then
  printf '%s' '{{"headRefOid":"{head}","baseRefOid":"{base}","baseRefName":"main","baseRepository":{{"id":"provider-repository-id","nameWithOwner":"org/repo"}},"reviewDecision":"APPROVED","mergeStateStatus":"CLEAN","statusCheckRollup":[{{"status":"COMPLETED","conclusion":"SUCCESS"}}]}}'
  exit 0
fi
if [ "$1 $2" = "pr merge" ]; then
  touch '{marker}'
  exit 99
fi
exit 2
"#,
            log = provider_log.display(),
            head = source_head,
            base = base_sha,
            marker = merge_marker.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&provider, fs::Permissions::from_mode(0o755)).unwrap();
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Github,
        &provider,
    )
    .unwrap();
    let daemon = SshGitServer::start(&provider_root, "org/repo.git");
    let canonical_url = daemon.url("org/repo.git");
    let database = fixture.temp.path().join("provider-queues.db");
    let queue = open_queue(&database);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy: iq::repository_policy::RepositoryPolicy {
                    operation_state: iq::repository_policy::OperationState::Enabled,
                    canonical_repository: iq::repository_policy::GitRepository::Accessible {
                        object_format: iq::git_object::GitObjectFormat::Sha1,
                        fetch_url: canonical_url.clone(),
                        push_url: canonical_url,
                        repository_id: "provider-repository-id".into(),
                        provider: iq::repository_policy::ProviderRepository {
                            provider: iq::repository_policy::Provider::Github,
                            host: "127.0.0.1".into(),
                            repository: "org/repo".into(),
                            repository_id: "provider-repository-id".into(),
                        },
                    },
                    target_branch: "main".into(),
                    integration_policy:
                        iq::repository_policy::IntegrationPolicy::MergeRequestRequired,
                    replication_policy: iq::repository_policy::ReplicationPolicy::None,
                },
            },
        )
        .unwrap();
    let workspace = RepositoryManager::new(queue.clone())
        .create_workspace(&repository.key, "provider-submit-rejected")
        .unwrap();
    git(&workspace.path, ["config", "user.name", "IQ Test"]).unwrap();
    git(&workspace.path, ["config", "user.email", "iq@example.test"]).unwrap();
    git(&workspace.path, ["config", "commit.gpgsign", "false"]).unwrap();
    fs::write(workspace.path.join("rejected.txt"), "rejected\n").unwrap();
    git(&workspace.path, ["add", "rejected.txt"]).unwrap();
    git(&workspace.path, ["commit", "-m", "rejected submit"]).unwrap();
    let submit = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .arg("--test-github-executable")
        .arg(&provider)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "submit",
            "--workspace",
            &workspace.id,
        ])
        .output()
        .unwrap();
    assert_eq!(submit.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(submit.stderr).unwrap(),
        "Error: repository policy rejects local direct submission\n"
    );
    let direct = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .arg("--test-github-executable")
        .arg(&provider)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "admit",
            "direct",
            "--repo-key",
            &repository.key,
            "--source",
            "agent/provider-blocked",
            "--head",
            &source_head,
        ])
        .output()
        .unwrap();
    assert_eq!(direct.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(direct.stderr).unwrap(),
        "Error: repository policy rejects direct admission\n"
    );

    let mr_url = "https://127.0.0.1/org/repo/pull/8";
    let admitted = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .arg("--test-github-executable")
        .arg(&provider)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "admit",
            "mr",
            mr_url,
            "--repo-key",
            &repository.key,
        ])
        .output()
        .unwrap();
    assert!(
        admitted.status.success(),
        "{}",
        String::from_utf8_lossy(&admitted.stderr)
    );
    let admitted: serde_json::Value = serde_json::from_slice(&admitted.stdout).unwrap();
    assert_eq!(admitted["admission"]["head_sha"], source_head);
    assert_eq!(admitted["admission"]["base_sha"], base_sha);
    assert_eq!(
        admitted["admission"]["repository_id"],
        "provider-repository-id"
    );
    assert_eq!(admitted["admission"]["url"], mr_url);
    let system_config = fixture.temp.path().join("provider-system.yaml");
    fs::write(
        &system_config,
        serde_yaml::to_string(&fixture.system_config()).unwrap(),
    )
    .unwrap();
    let mut integrated = serde_json::Value::Null;
    for _ in 0..3 {
        let output = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .arg("--test-github-executable")
            .arg(&provider)
            .env("IQ_TEST_MODEL_KEY", "fixture-model-key")
            .args([
                "--queue-db",
                database.to_str().unwrap(),
                "integrate",
                "--next",
                "--repo-key",
                &repository.key,
                "--system-config",
                system_config.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        integrated = serde_json::from_slice(&output.stdout).unwrap();
        if integrated["status"] != "merging" {
            break;
        }
    }
    assert_eq!(integrated["status"], "blocked");
    assert_eq!(integrated["blocked_phase"], "integrating");
    assert_eq!(integrated["blocked_reason"], "provider");
    let blocked_message: String = rusqlite::Connection::open(&database)
        .unwrap()
        .query_row(
            "SELECT blocked_message FROM queue_items WHERE id=?1",
            [integrated["id"].as_str().unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(blocked_message.contains("provider landing is unsupported before mutation"));
    assert!(blocked_message.contains("cannot atomically pin the validated base"));
    let provider_calls = fs::read_to_string(&provider_log).unwrap();
    assert!(
        provider_calls
            .lines()
            .filter(|line| line.starts_with("pr view "))
            .count()
            >= 3
    );
    assert!(!provider_calls.contains("pr merge"), "{provider_calls}");
    assert!(!merge_marker.exists());
    assert_eq!(
        git_output(&canonical, ["rev-parse", "refs/heads/main"]).unwrap(),
        base_sha
    );

    let item_id = integrated["id"].as_str().unwrap();
    let store = ControlStore::open(&database).unwrap();
    let effort = store.effort_for_item(item_id).unwrap().unwrap();
    let uid = unsafe { libc::geteuid() };
    store
        .retry_blocked(
            &effort.id,
            &iq::control_store::ResponderIdentity::LocalPeer { uid },
            uid,
        )
        .unwrap();
    let hung_marker = fixture.temp.path().join("hung-provider-started");
    fs::write(
        &provider,
        format!(
            "#!/bin/sh\nif [ \"$1 $2\" = \"api --hostname\" ]; then touch '{}'; sleep 60; fi\nexit 2\n",
            hung_marker.display()
        ),
    )
    .unwrap();
    let started = Instant::now();
    let mut resumed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .arg("--test-github-executable")
        .arg(&provider)
        .env("IQ_TEST_MODEL_KEY", "fixture-model-key")
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "integrate",
            "--resume",
            item_id,
            "--repo-key",
            &repository.key,
            "--owner",
            "release-boundary",
            "--system-config",
            system_config.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let marker_deadline = Instant::now() + Duration::from_secs(10);
    while !hung_marker.exists() {
        assert!(
            Instant::now() < marker_deadline,
            "hung provider did not start"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let cancelled = RepositoryManager::new(queue.clone())
        .cancel_item(item_id, "provider-timeout-test")
        .unwrap();

    assert_eq!(cancelled.status, QueueStatus::Cancelled);
    let status = resumed
        .wait_timeout(Duration::from_secs(5))
        .unwrap()
        .expect("hung provider process did not stop after cancellation");
    assert!(status.success());
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(
        queue.get_item(item_id).unwrap().status,
        QueueStatus::Cancelled
    );
    assert_eq!(
        rusqlite::Connection::open(&database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM repo_leases", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

fn migrated_provider_landing_recovery_fixture(
    provider_kind: iq::repository_policy::Provider,
    history_contract: &str,
) {
    let fixture = GitFixture::new(false);
    let provider_root = fixture
        .temp
        .path()
        .join(format!("{history_contract}-provider"));
    let canonical = provider_root.join("org/repo.git");
    fs::create_dir_all(canonical.parent().unwrap()).unwrap();
    git(
        fixture.temp.path(),
        ["init", "--bare", canonical.to_str().unwrap()],
    )
    .unwrap();
    git(
        &fixture.repo,
        [
            "push",
            canonical.to_str().unwrap(),
            "refs/heads/main:refs/heads/main",
        ],
    )
    .unwrap();
    let base_sha = git_output(&canonical, ["rev-parse", "refs/heads/main"]).unwrap();
    let source_head = fixture.create_source_branch(
        &format!("agent/{history_contract}"),
        &format!("{history_contract}.txt"),
        "provider source\n",
    );
    let provider_source_ref = match provider_kind {
        iq::repository_policy::Provider::Github => "refs/pull/8/head",
        iq::repository_policy::Provider::Gitlab => "refs/merge-requests/8/head",
    };
    git(
        &fixture.repo,
        [
            "push",
            canonical.to_str().unwrap(),
            &format!("{source_head}:{provider_source_ref}"),
        ],
    )
    .unwrap();
    let provider_cli = fixture
        .temp
        .path()
        .join(format!("{history_contract}-provider-cli"));
    let initial_provider_script = match provider_kind {
        iq::repository_policy::Provider::Github => format!(
            "#!/bin/sh\nif [ \"$1 $2\" = \"api --hostname\" ]; then case \"$4\" in repos/org/repo) printf '%s' '{{\"node_id\":\"provider-repository-id\",\"full_name\":\"org/repo\"}}' ;; repos/org/repo/hash-algorithm) printf '%s' '{{\"hash_algorithm\":\"sha1\"}}' ;; *) exit 3 ;; esac; exit 0; fi\nif [ \"$1 $2\" = \"pr view\" ]; then printf '%s' '{{\"headRefOid\":\"{source_head}\",\"baseRefOid\":\"{base_sha}\",\"baseRefName\":\"main\",\"baseRepository\":{{\"id\":\"provider-repository-id\",\"nameWithOwner\":\"org/repo\"}},\"reviewDecision\":\"APPROVED\",\"mergeStateStatus\":\"CLEAN\",\"statusCheckRollup\":[]}}'; exit 0; fi\nexit 2\n"
        ),
        iq::repository_policy::Provider::Gitlab => format!(
            "#!/bin/sh\nif [ \"$1\" = \"api\" ]; then printf '%s' '{{\"id\":\"provider-repository-id\",\"path_with_namespace\":\"org/repo\",\"repository_object_format\":\"sha1\"}}'; exit 0; fi\nif [ \"$1 $2\" = \"mr view\" ]; then case \"$*\" in *'--repo https://127.0.0.1/org/repo'*) ;; *) exit 3 ;; esac; printf '%s' '{{\"head_sha\":\"{source_head}\",\"base_sha\":\"{base_sha}\",\"target_branch\":\"main\",\"target_project_id\":\"provider-repository-id\",\"state\":\"opened\",\"pipeline_status\":\"success\",\"approved\":true}}'; exit 0; fi\nexit 2\n"
        ),
    };
    fs::write(&provider_cli, initial_provider_script).unwrap();
    fs::set_permissions(&provider_cli, fs::Permissions::from_mode(0o755)).unwrap();
    let _provider_executable =
        iq::providers::inject_test_provider_executable(provider_kind, &provider_cli).unwrap();
    let daemon = SshGitServer::start(&provider_root, "org/repo.git");
    let canonical_url = daemon.url("org/repo.git");
    let provider_identity = iq::repository_policy::ProviderRepository {
        provider: provider_kind,
        host: "127.0.0.1".into(),
        repository: "org/repo".into(),
        repository_id: "provider-repository-id".into(),
    };
    let database = fixture
        .temp
        .path()
        .join(format!("{history_contract}-provider.db"));
    let queue = open_queue(&database);
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy: iq::repository_policy::RepositoryPolicy {
                    operation_state: iq::repository_policy::OperationState::Enabled,
                    canonical_repository: iq::repository_policy::GitRepository::Accessible {
                        object_format: iq::git_object::GitObjectFormat::Sha1,
                        fetch_url: canonical_url.clone(),
                        push_url: canonical_url,
                        repository_id: "provider-repository-id".into(),
                        provider: provider_identity,
                    },
                    target_branch: "main".into(),
                    integration_policy:
                        iq::repository_policy::IntegrationPolicy::MergeRequestRequired,
                    replication_policy: iq::repository_policy::ReplicationPolicy::None,
                },
            },
        )
        .unwrap();
    let mr_url = match provider_kind {
        iq::repository_policy::Provider::Github => "https://127.0.0.1/org/repo/pull/8",
        iq::repository_policy::Provider::Gitlab => "https://127.0.0.1/org/repo/-/merge_requests/8",
    };
    let item = RepositoryManager::new(queue.clone())
        .admit_merge_request(&repository.key, mr_url, &serde_json::json!({}))
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: database.clone(),
            owner_id: format!("{history_contract}-provider-recovery"),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let blocked = (0..3)
        .find_map(|_| {
            let current = integrator.run_once().unwrap().unwrap();
            (current.status == QueueStatus::Blocked).then_some(current)
        })
        .expect("provider item did not reach its pre-mutation block");
    assert_eq!(blocked.blocked_reason, Some(BlockedReason::Provider));
    let merge_method = if history_contract == "preserve_head" {
        "merge"
    } else {
        "squash"
    };
    let migrated_database = fixture
        .temp
        .path()
        .join(format!("{history_contract}-migrated-provider.db"));
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute("VACUUM INTO ?1", [migrated_database.to_str().unwrap()])
        .unwrap();
    rusqlite::Connection::open(&migrated_database)
        .unwrap()
        .execute_batch(&format!(
            "DROP TRIGGER queue_admission_identity_immutable;
             UPDATE queue_admissions SET provider_merge_method='{merge_method}' WHERE item_id='{}';
             CREATE TRIGGER queue_admission_identity_immutable
             BEFORE UPDATE ON queue_admissions
             BEGIN SELECT RAISE(ABORT,'queue admission identity is immutable'); END;",
            item.id
        ))
        .unwrap();
    let database = migrated_database;
    let queue = open_queue(&database);
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: database.clone(),
            owner_id: format!("{history_contract}-migrated-provider-recovery"),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let store = ControlStore::open(&database).unwrap();
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    let (candidate_sha, policy_digest) = match &effort.state {
        iq::control_domain::IntegrationEffortState::ProviderBlocked(blocked) => {
            match &blocked.resume {
                iq::control_domain::ResumeState::Validating(validating) => (
                    validating.candidate_sha.clone(),
                    validating.policy_digest.clone(),
                ),
                state => panic!("unexpected provider recovery state: {state:?}"),
            }
        }
        state => panic!("unexpected provider blocked effort: {state:?}"),
    };
    let workspace = effort.workspace.path.clone();
    git(
        &fixture.repo,
        ["fetch", workspace.as_str(), candidate_sha.as_str()],
    )
    .unwrap();
    let candidate_tree = git_output(
        &fixture.repo,
        ["rev-parse", &format!("{candidate_sha}^{{tree}}")],
    )
    .unwrap();
    let mut commit_tree = Command::new("/usr/bin/git");
    commit_tree.current_dir(&fixture.repo).args([
        "commit-tree",
        candidate_tree.as_str(),
        "-p",
        &base_sha,
    ]);
    if history_contract == "preserve_head" {
        commit_tree.args(["-p", source_head.as_str()]);
    }
    commit_tree.args(["-m", "provider landing"]);
    let landing_output = commit_tree.output().unwrap();
    assert!(
        landing_output.status.success(),
        "{}",
        String::from_utf8_lossy(&landing_output.stderr)
    );
    let landing_sha = String::from_utf8(landing_output.stdout)
        .unwrap()
        .trim()
        .to_string();
    git(
        &fixture.repo,
        [
            "push",
            canonical.to_str().unwrap(),
            &format!("{landing_sha}:refs/heads/main"),
        ],
    )
    .unwrap();
    let gitlab_landing_fields = if history_contract == "preserve_head" {
        format!("\"merge_commit_sha\":\"{landing_sha}\",\"squash_commit_sha\":\"{source_head}\"")
    } else {
        format!("\"squash_commit_sha\":\"{landing_sha}\"")
    };
    let final_provider_script = match provider_kind {
        iq::repository_policy::Provider::Github => format!(
            "#!/bin/sh\nif [ \"$1 $2\" = \"api --hostname\" ]; then case \"$4\" in repos/org/repo) printf '%s' '{{\"node_id\":\"provider-repository-id\",\"full_name\":\"org/repo\"}}' ;; repos/org/repo/hash-algorithm) printf '%s' '{{\"hash_algorithm\":\"sha1\"}}' ;; *) exit 3 ;; esac; exit 0; fi\nif [ \"$1 $2\" = \"pr view\" ]; then case \"$*\" in *mergeCommit*) printf '%s' '{{\"headRefOid\":\"{source_head}\",\"mergeCommit\":{{\"oid\":\"{landing_sha}\"}}}}' ;; *) printf '%s' '{{\"headRefOid\":\"{source_head}\",\"baseRefOid\":\"{landing_sha}\",\"baseRefName\":\"main\",\"baseRepository\":{{\"id\":\"provider-repository-id\",\"nameWithOwner\":\"org/repo\"}},\"reviewDecision\":\"APPROVED\",\"mergeStateStatus\":\"CLEAN\",\"statusCheckRollup\":[]}}' ;; esac; exit 0; fi\nexit 2\n"
        ),
        iq::repository_policy::Provider::Gitlab => format!(
            "#!/bin/sh\nif [ \"$1\" = \"api\" ]; then printf '%s' '{{\"id\":\"provider-repository-id\",\"path_with_namespace\":\"org/repo\",\"repository_object_format\":\"sha1\"}}'; exit 0; fi\nif [ \"$1 $2\" = \"mr view\" ]; then case \"$*\" in *'--repo https://127.0.0.1/org/repo'*) ;; *) exit 3 ;; esac; printf '%s' '{{\"head_sha\":\"{source_head}\",\"base_sha\":\"{landing_sha}\",\"target_branch\":\"main\",\"target_project_id\":\"provider-repository-id\",{gitlab_landing_fields},\"state\":\"merged\",\"pipeline_status\":\"success\",\"approved\":true}}'; exit 0; fi\nexit 2\n"
        ),
    };
    fs::write(&provider_cli, final_provider_script).unwrap();
    let _updated_provider_executable =
        iq::providers::inject_test_provider_executable(provider_kind, &provider_cli).unwrap();
    let uid = unsafe { libc::geteuid() };
    store
        .retry_blocked(
            &effort.id,
            &iq::control_store::ResponderIdentity::LocalPeer { uid },
            uid,
        )
        .unwrap();
    store
        .begin_landing(
            &effort.id,
            &base_sha,
            "migrated-provider-lease",
            "migrated-provider-command",
            iq::control_domain::SignoffDisposition::NoValidation { policy_digest },
        )
        .unwrap();
    let uncertain = iq::control_domain::IntegrationEffortState::LandingUncertain(
        iq::control_domain::LandingUncertain {
            candidate_sha,
            expected_target_sha: base_sha.clone(),
            command_id: "migrated-provider-command".into(),
            evidence: "schema3_migration".into(),
        },
    );
    let migrated_connection = rusqlite::Connection::open(&database).unwrap();
    migrated_connection
        .execute(
            "UPDATE integration_efforts SET state='landing_uncertain',state_json=?1,updated_at=?2 WHERE id=?3",
            rusqlite::params![
                serde_json::to_string(&uncertain).unwrap(),
                chrono::Utc::now().to_rfc3339(),
                effort.id
            ],
        )
        .unwrap();
    migrated_connection
        .execute(
            "UPDATE queue_items SET landing_state_json=json_object('state','uncertain','candidate_sha',?1,'expected_target_sha',?2),updated_at=?3 WHERE id=?4",
            rusqlite::params![
                uncertain.candidate_sha().unwrap(),
                base_sha,
                chrono::Utc::now().to_rfc3339(),
                item.id
            ],
        )
        .unwrap();
    drop(migrated_connection);
    let staged = queue.get_item(&item.id).unwrap();
    assert!(
        staged.landing.is_uncertain(),
        "migrated provider fixture did not retain uncertain landing: {staged:?}"
    );

    let integrated = integrator.run_once().unwrap().unwrap();

    assert_eq!(
        integrated.status,
        QueueStatus::Integrated,
        "item={integrated:?} events={:?}",
        queue.events(&item.id).unwrap()
    );
    assert_eq!(
        integrated.landed_commit_sha.as_deref(),
        Some(landing_sha.as_str())
    );
    let guarantee: (String, i64, String, String) = rusqlite::Connection::open(database)
        .unwrap()
        .query_row(
            "SELECT history_contract,contains_admitted_head,first_parent_sha,validated_tree_sha FROM provider_landing_guarantees WHERE item_id=?1",
            [&item.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(guarantee.0, history_contract);
    assert_eq!(guarantee.1, i64::from(history_contract == "preserve_head"));
    assert_eq!(guarantee.2, base_sha);
    assert_eq!(guarantee.3, candidate_tree);
}

#[test]
fn migrated_github_merge_recovery_preserves_admitted_head_history() {
    migrated_provider_landing_recovery_fixture(
        iq::repository_policy::Provider::Github,
        "preserve_head",
    );
}

#[test]
fn migrated_github_squash_recovery_uses_inventory_history_contract() {
    migrated_provider_landing_recovery_fixture(iq::repository_policy::Provider::Github, "squash");
}

#[test]
fn migrated_gitlab_squash_recovery_uses_tree_and_first_parent_contract() {
    migrated_provider_landing_recovery_fixture(iq::repository_policy::Provider::Gitlab, "squash");
}

#[test]
fn migrated_gitlab_both_commit_fields_use_merge_commit_contained_by_target() {
    migrated_provider_landing_recovery_fixture(
        iq::repository_policy::Provider::Gitlab,
        "preserve_head",
    );
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
    let repo_key = repository.key.as_str();
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repo_key.into(),
            source_branch: "agent/fetch-fails".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({"worker":"W006"}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let wrapper_directory = fixture.temp.path().join("failed-push-git");
    fs::create_dir(&wrapper_directory).unwrap();
    let wrapper = wrapper_directory.join("git");
    fs::write(
        &wrapper,
        "#!/bin/sh\nif [ \"$1\" = push ]; then exit 1; fi\nexec /usr/bin/git \"$@\"\n",
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let _git_executable = iq::git_command::inject_test_git_executable(&wrapper).unwrap();

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
    assert_eq!(
        item.blocked_phase,
        Some(BlockedPhase::Integrating),
        "item={item:?} events={:?}",
        queue.events(&item.id).unwrap()
    );
    assert_eq!(item.blocked_reason, Some(BlockedReason::Infra));
    let landing_ref = format!(
        "refs/iq/landings/{}",
        item.current_attempt_id.as_deref().unwrap()
    );
    assert!(!git_output(
        &repository.owned_root_path,
        ["for-each-ref", "--format=%(refname)", landing_ref.as_str()],
    )
    .unwrap()
    .is_empty());
    assert_eq!(
        rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn provider_delay_with_lease_expiry_blocks_before_push_or_release_record() {
    let fixture = GitFixture::new(false);
    let provider_root = fixture.temp.path().join("release-provider-root");
    let canonical = provider_root.join("org/repo.git");
    fs::create_dir_all(canonical.parent().unwrap()).unwrap();
    git(
        fixture.temp.path(),
        ["init", "--bare", canonical.to_str().unwrap()],
    )
    .unwrap();
    git(
        &fixture.repo,
        [
            "push",
            canonical.to_str().unwrap(),
            "refs/heads/main:refs/heads/main",
        ],
    )
    .unwrap();
    let source_head = fixture.create_source_branch(
        "agent/provider-release-race",
        "provider-release.txt",
        "provider release\n",
    );
    git(
        &fixture.repo,
        [
            "push",
            canonical.to_str().unwrap(),
            "refs/heads/agent/provider-release-race:refs/heads/agent/provider-release-race",
        ],
    )
    .unwrap();
    let base_sha = git_output(&canonical, ["rev-parse", "refs/heads/main"]).unwrap();
    let database = fixture.temp.path().join("provider-release.db");
    let provider = fixture.temp.path().join("release-gh");
    let provider_log = fixture.temp.path().join("release-gh.log");
    fs::write(
        &provider,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
case "$4" in
  repos/org/repo) printf '%s' '{{"node_id":"provider-repository-id","full_name":"org/repo"}}' ;;
  repos/org/repo/hash-algorithm)
    state=$(/usr/bin/python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("SELECT COALESCE((SELECT state FROM integration_efforts LIMIT 1),\"\")").fetchone()[0])' "$IQ_TEST_PROVIDER_DATABASE")
    if [ "$state" = landing ]; then
      /bin/sleep 0.2
      /usr/bin/python3 -c 'import sqlite3,sys; db=sqlite3.connect(sys.argv[1]); db.execute("UPDATE repo_leases SET expires_at=\"1970-01-01T00:00:00Z\""); db.commit()' "$IQ_TEST_PROVIDER_DATABASE"
    fi
    printf '%s' '{{"hash_algorithm":"sha1"}}'
    ;;
  *) exit 3 ;;
esac
"#,
            log = provider_log.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&provider, fs::Permissions::from_mode(0o755)).unwrap();
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Github,
        &provider,
    )
    .unwrap();
    std::env::set_var("IQ_TEST_PROVIDER_DATABASE", &database);
    let daemon = SshGitServer::start(&provider_root, "org/repo.git");
    let canonical_url = daemon.url("org/repo.git");
    let queue = open_queue(&database);
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy: iq::repository_policy::RepositoryPolicy {
                    operation_state: iq::repository_policy::OperationState::Enabled,
                    canonical_repository: iq::repository_policy::GitRepository::Accessible {
                        object_format: iq::git_object::GitObjectFormat::Sha1,
                        fetch_url: canonical_url.clone(),
                        push_url: canonical_url,
                        repository_id: "provider-repository-id".into(),
                        provider: iq::repository_policy::ProviderRepository {
                            provider: iq::repository_policy::Provider::Github,
                            host: "127.0.0.1".into(),
                            repository: "org/repo".into(),
                            repository_id: "provider-repository-id".into(),
                        },
                    },
                    target_branch: "main".into(),
                    integration_policy: iq::repository_policy::IntegrationPolicy::Direct,
                    replication_policy: iq::repository_policy::ReplicationPolicy::None,
                },
            },
        )
        .unwrap();
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/provider-release-race".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: repository.owned_root_path,
            queue_db: database.clone(),
            owner_id: "provider-release-race".into(),
            lease_ttl_seconds: 30,
            base_remote: "iq-target".into(),
            workspace_root: repository.integration_root_path,
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let error = integrator.run_once().unwrap_err();

    assert!(format!("{error:#}").contains("lease is no longer owned"));
    assert_ne!(
        queue.get_item(&item.id).unwrap().status,
        QueueStatus::Integrated
    );
    assert_eq!(
        git_output(&canonical, ["rev-parse", "refs/heads/main"]).unwrap(),
        base_sha
    );
    assert_eq!(
        rusqlite::Connection::open(&database)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM durable_events WHERE item_id=?1 AND event_type='landing_released'",
                [&item.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert!(fs::read_to_string(provider_log).unwrap().lines().count() >= 2);
}

#[test]
fn validation_git_config_mutation_is_denied_before_landing_release() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/preflight-fails", "preflight.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/preflight-fails"],
    )
    .unwrap();
    fixture.set_validation_command(
        "git config url.ssh://attacker.invalid/.insteadOf file:///does-not-match",
    );
    let db = fixture.temp.path().join("preflight-fails.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/preflight-fails".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "preflight-fails".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();

    assert_eq!(blocked.status, QueueStatus::Merging, "{blocked:#?}");
    assert_eq!(blocked.blocked_phase, None);
    assert!(matches!(blocked.landing, iq::sqlite::LandingState::Ready));
    assert!(git_output(
        Path::new(blocked.workspace.path().unwrap()),
        [
            "config",
            "--local",
            "--get",
            "url.ssh://attacker.invalid/.insteadOf"
        ]
    )
    .is_err());
    let released: i64 = rusqlite::Connection::open(queue.path())
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM durable_events WHERE item_id=?1 AND event_type='landing_released'",
            [&item.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(released, 0);
}

#[test]
fn validation_replacement_refs_and_grafts_cannot_change_landing_semantics() {
    for (name, validation, expected) in [
        (
            "replace",
            r#"base=$(git rev-parse HEAD^) && tree=$(git rev-parse "$base^{tree}") && git update-ref "refs/replace/$base" HEAD && test "$(git rev-parse "$base^{tree}")" = "$tree""#,
            "replacement refs",
        ),
        (
            "grafts",
            r#"common=$(git rev-parse --git-common-dir) && mkdir -p "$common/info" && git rev-parse HEAD > "$common/info/grafts""#,
            "legacy grafts",
        ),
    ] {
        let fixture = GitFixture::new(true);
        let marker = fixture.temp.path().join(format!("{name}-validation-ran"));
        fixture.set_validation_command(&format!(
            "test \"$GIT_NO_REPLACE_OBJECTS\" = 1 && {validation} && : > '{}'",
            marker.display()
        ));
        let branch = format!("agent/{name}-object-resolution");
        let source_head = fixture.create_source_branch(
            &branch,
            &format!("{name}-object-resolution.txt"),
            "feature\n",
        );
        git(&fixture.repo, ["push", "-u", "origin", &branch]).unwrap();
        let initial_target = git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap();
        let database = fixture
            .temp
            .path()
            .join(format!("{name}-object-resolution.db"));
        let queue = open_queue(&database);
        let repository = provision_fixture_repository(&queue, &fixture);
        let item = RepositoryManager::new(queue.clone())
            .admit_direct(iq::sqlite::DirectAdmissionRequest {
                repo_key: repository.key.clone(),
                source_branch: branch,
                current_head_sha: source_head,
                producer_metadata: serde_json::json!({}),
                state_repository: iq::control_domain::StateRepositorySnapshot::Local,
            })
            .unwrap();
        let integrator = fixture
            .integrator(IntegratorOptions {
                repo_key: repository.key,
                repo_path: fixture.repo.clone(),
                queue_db: database,
                owner_id: format!("{name}-object-resolution"),
                lease_ttl_seconds: 30,
                base_remote: "origin".into(),
                workspace_root: fixture.temp.path().join(format!("{name}-workspaces")),
                rift_database: Some(fixture.rift_database.clone()),
                system_config: fixture.system_config(),
            })
            .unwrap();

        let blocked = integrator.run_once().unwrap().unwrap();

        assert!(marker.is_file(), "{name} validation did not run");
        assert_eq!(blocked.status, QueueStatus::Blocked);
        assert_eq!(blocked.blocked_phase, Some(BlockedPhase::Validating));
        assert!(matches!(blocked.landing, iq::sqlite::LandingState::Ready));
        let connection = rusqlite::Connection::open(queue.path()).unwrap();
        let blocked_message: String = connection
            .query_row(
                "SELECT blocked_message FROM queue_items WHERE id=?1",
                [&item.id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(blocked_message.contains(expected), "{name}: {blocked:?}");
        assert_eq!(
            git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap(),
            initial_target
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM durable_events WHERE item_id=?1 AND event_type='landing_released'",
                    [&item.id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }
}

#[test]
#[cfg(debug_assertions)]
fn direct_landing_release_commit_precedes_spawn_and_crash_recovery_avoids_hooks() {
    let fixture = GitFixture::new(true);
    let hook_marker = fixture.temp.path().join("local-pre-push-ran");
    fixture.set_validation_command(&format!(
        "common=$(git rev-parse --git-common-dir) && hook=\"$common/hooks/pre-push\" && mkdir -p \"$(dirname \"$hook\")\" && printf '#!/bin/sh\\n: > \"{}\"\\n' > \"$hook\" && chmod +x \"$hook\"",
        hook_marker.display()
    ));
    let source_head =
        fixture.create_source_branch("agent/release-boundary", "boundary.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/release-boundary"],
    )
    .unwrap();
    let initial_target = git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap();
    let database = fixture.temp.path().join("release-boundary.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/release-boundary".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let wrapper_directory = fixture.temp.path().join("release-boundary-git");
    fs::create_dir(&wrapper_directory).unwrap();
    let push_log = fixture.temp.path().join("release-boundary-pushes");
    let wrapper = wrapper_directory.join("git");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexec /usr/bin/git \"$@\"\n",
            push_log.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::set_var(
        "PATH",
        format!("{}:{}", wrapper_directory.display(), path.to_string_lossy()),
    );
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: database.clone(),
            owner_id: "release-boundary".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    std::env::set_var("IQ_TEST_LANDING_FAIL_BEFORE_RELEASE_COMMIT", "1");
    let before_commit = integrator.run_once().unwrap().unwrap();
    std::env::remove_var("IQ_TEST_LANDING_FAIL_BEFORE_RELEASE_COMMIT");
    let push_count = || {
        fs::read_to_string(&push_log)
            .ok()
            .map(|contents| {
                contents
                    .lines()
                    .filter(|line| line.starts_with("push "))
                    .count()
            })
            .unwrap_or(0)
    };
    assert_eq!(before_commit.status, QueueStatus::Blocked);
    assert!(matches!(
        before_commit.landing,
        iq::sqlite::LandingState::Ready
    ));
    assert_eq!(push_count(), 0);
    assert_eq!(
        git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap(),
        initial_target
    );
    let release_count = || {
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .pragma_update(None, "recursive_triggers", "ON")
            .unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA recursive_triggers", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        connection
            .query_row(
                "SELECT COUNT(*) FROM durable_events WHERE item_id=?1 AND event_type='landing_released'",
                [&item.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    assert_eq!(release_count(), 0);

    let store = ControlStore::open(&database).unwrap();
    let uid = unsafe { libc::geteuid() };
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    store
        .retry_blocked(
            &effort.id,
            &iq::control_store::ResponderIdentity::LocalPeer { uid },
            uid,
        )
        .unwrap();
    let workspace = Path::new(before_commit.workspace.path().unwrap());
    let hostile_marker = fixture.temp.path().join("hostile-git-config-executed");
    let hostile_command = fixture.temp.path().join("hostile-git-command");
    fs::write(
        &hostile_command,
        format!("#!/bin/sh\n: > '{}'\nexit 1\n", hostile_marker.display()),
    )
    .unwrap();
    fs::set_permissions(&hostile_command, fs::Permissions::from_mode(0o755)).unwrap();
    let hostile_hooks = fixture.temp.path().join("hostile-hooks");
    fs::create_dir(&hostile_hooks).unwrap();
    fs::copy(&hostile_command, hostile_hooks.join("pre-push")).unwrap();
    for (key, value) in [
        ("http.proxy", "http://127.0.0.1:9"),
        ("http.sslVerify", "false"),
        ("filter.hostile.process", hostile_command.to_str().unwrap()),
        ("core.hooksPath", hostile_hooks.to_str().unwrap()),
        ("core.fsmonitor", hostile_command.to_str().unwrap()),
    ] {
        git(workspace, ["config", "--local", key, value]).unwrap();
    }
    let hostile = integrator.run_once().unwrap_err();
    assert!(
        format!("{hostile:#}").contains("Git configuration is not allowed"),
        "{hostile:#}"
    );
    assert_eq!(release_count(), 0);
    assert_eq!(push_count(), 0);
    assert!(!hostile_marker.exists());
    for key in [
        "http.proxy",
        "http.sslVerify",
        "filter.hostile.process",
        "core.hooksPath",
        "core.fsmonitor",
    ] {
        git(workspace, ["config", "--local", "--unset-all", key]).unwrap();
    }
    let system_config = fixture.temp.path().join("release-boundary-system.yaml");
    fs::write(
        &system_config,
        serde_yaml::to_string(&fixture.system_config()).unwrap(),
    )
    .unwrap();
    let crashed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_LANDING_STOP_AFTER_RELEASE_COMMIT", "1")
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "integrate",
            "--next",
            "--repo-key",
            &repository.key,
            "--system-config",
            system_config.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(crashed.status.code(), Some(91));
    assert_eq!(push_count(), 0);
    assert_eq!(release_count(), 1);
    let effort = store.effort_for_item(&item.id).unwrap().unwrap();
    assert!(matches!(
        effort.state,
        iq::control_domain::IntegrationEffortState::LandingUncertain(_)
    ));

    let reconciled = integrator.run_once().unwrap().unwrap();
    assert_eq!(reconciled.status, QueueStatus::Blocked);
    assert_eq!(reconciled.blocked_phase, Some(BlockedPhase::Integrating));
    let pushes = fs::read_to_string(&push_log).unwrap_or_default();
    assert_eq!(push_count(), 0, "{pushes}");
    assert_eq!(release_count(), 1);
    assert_eq!(
        git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap(),
        initial_target
    );
    assert!(!hook_marker.exists());
}

#[test]
fn direct_landing_does_not_execute_repository_pre_push_hook() {
    let fixture = GitFixture::new(true);
    let hook_marker = fixture.temp.path().join("local-pre-push-ran");
    fixture.set_validation_command(&format!(
        "common=$(git rev-parse --git-common-dir) && hook=\"$common/hooks/pre-push\" && mkdir -p \"$(dirname \"$hook\")\" && printf '#!/bin/sh\\n: > \"{}\"\\n' > \"$hook\" && chmod +x \"$hook\"",
        hook_marker.display()
    ));
    let source_head = fixture.create_source_branch(
        "agent/disabled-pre-push",
        "disabled-pre-push.txt",
        "feature\n",
    );
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/disabled-pre-push"],
    )
    .unwrap();
    let database = fixture.temp.path().join("disabled-pre-push.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    RepositoryManager::new(queue)
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/disabled-pre-push".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: fixture.repo.clone(),
            queue_db: database,
            owner_id: "disabled-pre-push".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let integrated = integrator.run_once().unwrap().unwrap();

    assert_eq!(integrated.status, QueueStatus::Integrated);
    assert!(!hook_marker.exists());
}

#[test]
fn direct_landing_compare_and_set_preserves_target_moved_during_push() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/cas-race", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/cas-race"]).unwrap();

    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    fs::write(fixture.repo.join("race.txt"), "race\n").unwrap();
    git(&fixture.repo, ["add", "race.txt"]).unwrap();
    git(
        &fixture.repo,
        ["commit", "-m", "move target during landing"],
    )
    .unwrap();
    let raced_target = git_output(&fixture.repo, ["rev-parse", "HEAD"]).unwrap();
    git(
        &fixture.repo,
        [
            "push",
            "origin",
            &format!("{raced_target}:refs/iq-test/raced-target"),
        ],
    )
    .unwrap();
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/cas-race".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    let wrapper_directory = fixture.temp.path().join("cas-git");
    fs::create_dir(&wrapper_directory).unwrap();
    let wrapper = wrapper_directory.join("git");
    let marker = fixture.temp.path().join("cas-race-applied");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  case \"$argument\" in\n    --force-with-lease=refs/heads/main:*)\n      if [ ! -e '{marker}' ]; then\n        : > '{marker}'\n        /usr/bin/git --git-dir='{remote}' update-ref refs/heads/main '{raced_target}' || exit $?\n      fi\n      ;;\n  esac\ndone\nexec /usr/bin/git \"$@\"\n",
            marker = marker.display(),
            remote = fixture.remote.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let _git_executable = iq::git_command::inject_test_git_executable(&wrapper).unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "cas-race".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let mut item = integrator.run_once().unwrap().unwrap();
    for _ in 0..2 {
        if item.status == QueueStatus::Integrated {
            break;
        }
        item = integrator.run_once().unwrap().unwrap();
    }
    assert_eq!(item.status, QueueStatus::Integrated, "{item:?}");
    assert!(marker.is_file());
    let landed = git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap();
    assert!(Command::new("/usr/bin/git")
        .args([
            "--git-dir",
            fixture.remote.to_str().unwrap(),
            "merge-base",
            "--is-ancestor",
            &raced_target,
            &landed,
        ])
        .status()
        .unwrap()
        .success());
}

#[test]
fn unknown_push_followed_by_third_target_stays_uncertain_without_second_landing() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/unknown-third", "unknown.txt", "source\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/unknown-third"],
    )
    .unwrap();
    let db = fixture.temp.path().join("unknown-third.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    git(&fixture.repo, ["switch", "main"]).unwrap();
    fs::write(fixture.repo.join("third.txt"), "third\n").unwrap();
    git(&fixture.repo, ["add", "third.txt"]).unwrap();
    git(&fixture.repo, ["commit", "-m", "third target"]).unwrap();
    let third_target = git_output(&fixture.repo, ["rev-parse", "HEAD"]).unwrap();
    git(
        &fixture.repo,
        [
            "push",
            "origin",
            &format!("{third_target}:refs/iq-test/third-target"),
        ],
    )
    .unwrap();
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/unknown-third".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let wrapper_directory = fixture.temp.path().join("unknown-git");
    fs::create_dir(&wrapper_directory).unwrap();
    let wrapper = wrapper_directory.join("git");
    let marker = fixture.temp.path().join("unknown-push-count");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nif {{ [ \"$1\" = ls-remote ] || [ \"$1\" = fetch ]; }} && [ -e '{marker}' ] && [ ! -e '{marker}.moved' ]; then\n  /usr/bin/git --git-dir='{remote}' update-ref refs/heads/main '{third_target}' || exit $?\n  : > '{marker}.moved'\nfi\nfor argument in \"$@\"; do\n  case \"$argument\" in\n    --force-with-lease=refs/heads/main:*)\n      /usr/bin/git \"$@\" || exit $?\n      printf 'push\\n' >> '{marker}'\n      printf '[rejected] (stale info)\\n' >&2\n      exit 75\n      ;;\n  esac\ndone\nexec /usr/bin/git \"$@\"\n",
            marker = marker.display(),
            remote = fixture.remote.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let _git_executable = iq::git_command::inject_test_git_executable(&wrapper).unwrap();
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key,
            repo_path: fixture.repo.clone(),
            queue_db: db,
            owner_id: "unknown-third".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();

    let blocked = integrator.run_once().unwrap().unwrap();
    assert_eq!(
        blocked.status,
        QueueStatus::Blocked,
        "item={blocked:?} remote={} third={third_target} marker={:?} events={:?}",
        git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap(),
        fs::read_to_string(&marker),
        queue.events(&item.id).unwrap()
    );
    assert!(
        matches!(blocked.landing, iq::sqlite::LandingState::Uncertain { .. }),
        "{blocked:?} events={:?}",
        queue.events(&item.id).unwrap()
    );
    git(
        &fixture.remote,
        ["update-ref", "refs/heads/main", &third_target],
    )
    .unwrap();
    assert_eq!(
        git_output(&fixture.remote, ["rev-parse", "refs/heads/main"]).unwrap(),
        third_target
    );
    let reconciled = with_hostile_git_environment(fixture.temp.path(), || {
        integrator.resume_item(&item.id).unwrap()
    });
    assert_eq!(reconciled.status, QueueStatus::Blocked);
    assert!(matches!(
        reconciled.landing,
        iq::sqlite::LandingState::Uncertain { .. }
    ));
    assert_eq!(fs::read_to_string(marker).unwrap(), "push\n");
}

#[test]
fn direct_policy_rejects_mr_before_url_validation() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    let repository = provision_fixture_repository(&queue, &fixture);
    let error = RepositoryManager::new(queue)
        .admit_merge_request(
            &repository.key,
            "not a provider URL",
            &serde_json::json!({"worker":"W007"}),
        )
        .unwrap_err();
    assert!(format!("{error:#}").contains("policy rejects merge-request admission"));
}

#[test]
fn disabled_repository_rejects_new_workspace_and_direct_admission_before_arguments() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    let queue = open_queue(&db);
    std::env::set_var("IQ_RIFT_DATABASE", &fixture.rift_database);
    let repository = RepositoryManager::new(queue.clone())
        .init(
            &fixture.repo,
            RepositoryInitOptions {
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
            },
        )
        .unwrap();
    let manager = RepositoryManager::new(queue.clone());
    manager.begin_draining(&repository.key).unwrap();
    manager.disable_drained(&repository.key).unwrap();
    let canonical_before = git_output(&fixture.remote, ["show-ref"]).unwrap();
    for arguments in [
        vec![
            "workspace",
            "create",
            "--repo-key",
            repository.key.as_str(),
            "--name",
            "invalid/name",
        ],
        vec![
            "admit",
            "direct",
            "--repo-key",
            repository.key.as_str(),
            "--source",
            "not a branch",
            "--head",
            "not a sha",
        ],
    ] {
        let rejected = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .arg("--queue-db")
            .arg(&db)
            .args(arguments)
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(1));
        assert_eq!(
            String::from_utf8(rejected.stderr).unwrap(),
            "Error: repository is disabled\n"
        );
    }

    let workspace = manager
        .create_workspace(&repository.key, "invalid/name")
        .unwrap_err();
    assert!(format!("{workspace:#}").contains("repository is disabled"));
    let admission = manager
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "not a branch".into(),
            current_head_sha: "not a sha".into(),
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap_err();
    assert!(format!("{admission:#}").contains("repository is disabled"));
    let merge_request = manager
        .admit_merge_request(
            &repository.key,
            "https://github.com/acme/repo/pull/1",
            &serde_json::json!({}),
        )
        .unwrap_err();
    assert!(format!("{merge_request:#}").contains("repository is disabled"));
    let integrator = fixture
        .integrator(IntegratorOptions {
            repo_key: repository.key.clone(),
            repo_path: fixture.repo.clone(),
            queue_db: db.clone(),
            owner_id: "disabled-test".into(),
            lease_ttl_seconds: 30,
            base_remote: "origin".into(),
            workspace_root: fixture.temp.path().join("workspaces"),
            rift_database: Some(fixture.rift_database.clone()),
            system_config: fixture.system_config(),
        })
        .unwrap();
    let integration = integrator.run_once().unwrap_err();
    assert!(format!("{integration:#}").contains("repository is disabled"));
    assert!(manager.status(&repository.key).is_ok());
    assert!(manager
        .workspaces(Some(&repository.key))
        .unwrap()
        .is_empty());
    assert!(queue.list_items().unwrap().is_empty());
    assert_eq!(
        git_output(&fixture.remote, ["show-ref"]).unwrap(),
        canonical_before
    );
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
                storage_root: fixture.temp.path().to_path_buf(),
                policy: direct_policy(&fixture.remote),
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
    RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: branch.into(),
            current_head_sha: head,
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
    let wrapper_directory = fixture.temp.path().join("dirty-after-push-git");
    fs::create_dir(&wrapper_directory).unwrap();
    let wrapper = wrapper_directory.join("git");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n/usr/bin/git \"$@\"\nstatus=$?\nif [ $status -eq 0 ] && [ \"$1\" = push ]; then printf preserve > '{}'; fi\nexit $status\n",
            retained.join("dirty-after-landing.txt").display()
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let _git_executable = iq::git_command::inject_test_git_executable(&wrapper).unwrap();
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key,
            source_branch: "agent/daemon-once".into(),
            current_head_sha: source_sha,
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/missing-generation".into(),
            current_head_sha: source_sha,
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
fn daemon_once_allows_independent_shared_database_process_lease_holder() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    drop(SqliteQueue::open(&db).unwrap());
    let (daemon_config, system_config, _control_directory) =
        write_daemon_runtime_config(&fixture, &db);
    let mut holder = spawn_database_process_lease_holder(&db, fixture.temp.path());

    let concurrent = run_daemon_once(&fixture, &db, &daemon_config, &system_config);

    assert!(
        concurrent.status.success(),
        "{}",
        String::from_utf8_lossy(&concurrent.stderr)
    );
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
fn live_daemon_allows_cli_development_workspace_lifecycle() {
    let fixture = GitFixture::new(false);
    let database = fixture.temp.path().join("queues.db");
    let (daemon_config, system_config, _control_directory) =
        write_daemon_runtime_config(&fixture, &database);
    let daemon: serde_json::Value =
        serde_json::from_slice(&fs::read(&daemon_config).unwrap()).unwrap();
    let repo_key = daemon["repos"][0]["repo_key"].as_str().unwrap();
    let ready = fixture.temp.path().join("daemon-ready");
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_MODEL_KEY", "fixture-model-key")
        .args([
            "--queue-db",
            database.to_str().unwrap(),
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
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() {
        if let Some(status) = daemon.try_wait().unwrap() {
            let mut stderr = String::new();
            daemon
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("daemon exited before readiness with {status}: {stderr}");
        }
        assert!(Instant::now() < deadline, "daemon readiness timed out");
        std::thread::sleep(Duration::from_millis(10));
    }

    let create_deadline = Instant::now() + Duration::from_secs(10);
    let created = loop {
        let output = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("RUST_BACKTRACE", "1")
            .args([
                "--queue-db",
                database.to_str().unwrap(),
                "workspace",
                "create",
                "--repo-key",
                repo_key,
                "--name",
                "daemon-cli-live",
            ])
            .output()
            .unwrap();
        if output.status.success()
            || !String::from_utf8_lossy(&output.stderr).contains("has an active operation")
            || Instant::now() >= create_deadline
        {
            break output;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let created_json: serde_json::Value = if created.status.success() {
        serde_json::from_slice(&created.stdout).unwrap()
    } else {
        serde_json::Value::Null
    };
    let workspace_id = created_json["id"].as_str().unwrap_or_default();
    let workspace_path = created_json["path"].as_str().unwrap_or_default();
    let observed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "workspace",
            "status",
            workspace_id,
        ])
        .output()
        .unwrap();
    let listed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "workspace",
            "list",
            "--repo-key",
            repo_key,
        ])
        .output()
        .unwrap();
    let removed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "workspace",
            "remove",
            workspace_id,
        ])
        .output()
        .unwrap();
    let integration_status = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "integration",
            "status",
            "--repo-key",
            repo_key,
        ])
        .output()
        .unwrap();
    let integration_reset = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .args([
            "--queue-db",
            database.to_str().unwrap(),
            "integration",
            "reset",
            "--repo-key",
            repo_key,
            "--system-config",
            system_config.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    daemon.kill().unwrap();
    daemon.wait().unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(
        observed.status.success(),
        "{}",
        String::from_utf8_lossy(&observed.stderr)
    );
    let observed: serde_json::Value = serde_json::from_slice(&observed.stdout).unwrap();
    assert_eq!(observed["workspace"]["status"], "active");
    assert_eq!(observed["exists"], true);
    assert_eq!(observed["clean"], true);
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let listed: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert!(
        removed.status.success(),
        "{}",
        String::from_utf8_lossy(&removed.stderr)
    );
    let removed: serde_json::Value = serde_json::from_slice(&removed.stdout).unwrap();
    assert_eq!(removed["status"], "removed");
    assert!(!Path::new(workspace_path).exists());
    assert!(
        integration_status.status.success(),
        "{}",
        String::from_utf8_lossy(&integration_status.stderr)
    );
    assert!(
        integration_reset.status.success(),
        "{}",
        String::from_utf8_lossy(&integration_reset.stderr)
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
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("acquire exclusive IQ daemon lease"));

    fs::write(&failure, b"fail\n").unwrap();
    let status = daemon
        .wait_timeout(Duration::from_secs(30))
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
        let item = RepositoryManager::new(queue.clone())
            .admit_direct(iq::sqlite::DirectAdmissionRequest {
                repo_key: repository.key.clone(),
                source_branch: "agent/generation-crash".into(),
                current_head_sha: source_sha,
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
            (1, None),
            "boundary {boundary}: stdout={} stderr={}",
            String::from_utf8_lossy(&resumed.stdout),
            String::from_utf8_lossy(&resumed.stderr)
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
        let item = RepositoryManager::new(queue.clone())
            .admit_direct(iq::sqlite::DirectAdmissionRequest {
                repo_key: repository.key.clone(),
                source_branch: "agent/target-fetch-crash".into(),
                current_head_sha: source_sha,
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/supervised-target-refresh".into(),
            current_head_sha: source_sha,
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
            "workspace",
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/pending-target".into(),
            current_head_sha: source_sha,
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/validation-head".into(),
            current_head_sha: source_head,
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
    fixture.set_validation_command("touch validation-dirty.txt && git checkout --detach HEAD^");
    let source_head =
        fixture.create_source_branch("agent/validation-head-dirty", "feature.txt", "feature\n");
    let database = fixture.temp.path().join("queues.db");
    let queue = open_queue(&database);
    let repository = provision_fixture_repository(&queue, &fixture);
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/validation-head-dirty".into(),
            current_head_sha: source_head,
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/validation-repair".into(),
            current_head_sha: source_head,
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
    let item = RepositoryManager::new(queue.clone())
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: repository.key.clone(),
            source_branch: "agent/revalidation-repair".into(),
            current_head_sha: source_head,
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
        error.contains("IQ local state is incompatible; control authority is invalid"),
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
        Self::new_with_object_format(iq::git_object::GitObjectFormat::Sha1)
    }

    fn new_with_object_format(object_format: iq::git_object::GitObjectFormat) -> Self {
        let environment = FixtureEnvironment::acquire();
        let temp = managed_test_tempdir(".iq-integrator-test-");
        let remote = temp.path().join("remote.git");
        git(
            temp.path(),
            [
                "init",
                "--bare",
                &format!("--object-format={object_format}"),
                remote.to_str().unwrap(),
            ],
        )
        .unwrap();
        let repo = temp.path().join("repo");
        git(
            temp.path(),
            [
                "init",
                &format!("--object-format={object_format}"),
                repo.to_str().unwrap(),
            ],
        )
        .unwrap();
        git(&repo, ["remote", "add", "origin", remote.to_str().unwrap()]).unwrap();
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
        git(&repo, ["config", "--unset", "core.hooksPath"]).unwrap();
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
        std::env::set_var("IQ_RIFT_DATABASE", &rift_database);
        let runner = temp.path().join("fake-opencode");
        fs::write(
            &runner,
            r##"#!/usr/bin/python3
import hashlib
import json
import os
import re
import subprocess

prompt = os.sys.argv[-1]
version_match = re.search(r"protocol version ([0-9]+) JSON", prompt)
if version_match is None:
    raise RuntimeError("runner prompt has no protocol version")
protocol_version = int(version_match.group(1))
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
        "version": protocol_version,
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
        "version": protocol_version,
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
