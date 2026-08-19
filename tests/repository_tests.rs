use iq::sqlite::{CheckoutReconciliationState, SqliteQueue};
mod support;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Read;
#[cfg(debug_assertions)]
use std::io::Seek;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
#[cfg(debug_assertions)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use support::{direct_policy, managed_test_tempdir, Command};
use tempfile::tempdir;
#[cfg(debug_assertions)]
use wait_timeout::ChildExt;

fn git(path: &Path, args: &[&str]) -> String {
    let mut command = std::process::Command::new("git");
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
    let output = command.current_dir(path).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

struct CliFixture {
    _temporary: tempfile::TempDir,
    root: PathBuf,
    remote: PathBuf,
    bootstrap: PathBuf,
    database: PathBuf,
    rift_database: PathBuf,
}

impl CliFixture {
    fn new(target: &str) -> Self {
        let temporary = managed_test_tempdir(".iq-repository-test-");
        let root = temporary.path().to_path_buf();
        let remote = root.join("remote.git");
        let bootstrap = root.join("bootstrap");
        std::fs::create_dir(&remote).unwrap();
        git(&remote, &["init", "--bare"]);
        git(
            &root,
            &[
                "clone",
                remote.to_str().unwrap(),
                bootstrap.to_str().unwrap(),
            ],
        );
        git(&bootstrap, &["config", "user.name", "IQ Test"]);
        git(&bootstrap, &["config", "user.email", "iq@example.test"]);
        git(&bootstrap, &["config", "commit.gpgsign", "false"]);
        std::fs::write(bootstrap.join("README.md"), format!("{target}\n")).unwrap();
        git(&bootstrap, &["add", "README.md"]);
        git(&bootstrap, &["commit", "-m", "initial"]);
        git(&bootstrap, &["branch", "-M", target]);
        git(&bootstrap, &["push", "-u", "origin", target]);
        git(&bootstrap, &["switch", "-c", "dirty-bootstrap"]);
        std::fs::write(bootstrap.join("dirty.txt"), "not committed\n").unwrap();
        Self {
            database: root.join("queues.db"),
            rift_database: root.join("rift.sqlite"),
            _temporary: temporary,
            root,
            remote,
            bootstrap,
        }
    }

    fn iq_command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iq"));
        command
            .env("IQ_RIFT_DATABASE", &self.rift_database)
            .env("IQ_TEST_MODEL_KEY", "repository-test-model-key")
            .arg("--queue-db")
            .arg(&self.database)
            .args(args);
        command
    }

    fn iq(&self, args: &[&str]) -> Output {
        self.iq_command(args).output().unwrap()
    }

    #[cfg(debug_assertions)]
    fn bounded_iq(&self, args: &[&str], context: &str) -> Output {
        bounded_cli_output(&mut self.iq_command(args), context)
    }

    fn init(&self, target: &str) -> Output {
        let policy = self.policy(target);
        self.iq(&[
            "repo",
            "init",
            "--path",
            self.bootstrap.to_str().unwrap(),
            "--storage-root",
            self.root.to_str().unwrap(),
            "--policy",
            policy.to_str().unwrap(),
        ])
    }

    #[cfg(debug_assertions)]
    fn bounded_init(&self, target: &str, context: &str) -> Output {
        let policy = self.policy(target);
        self.bounded_iq(
            &[
                "repo",
                "init",
                "--path",
                self.bootstrap.to_str().unwrap(),
                "--storage-root",
                self.root.to_str().unwrap(),
                "--policy",
                policy.to_str().unwrap(),
            ],
            context,
        )
    }

    fn policy(&self, target: &str) -> PathBuf {
        let path = self.root.join(format!("repository-policy-{target}.json"));
        let mut policy = direct_policy(&self.remote);
        policy.target_branch = target.to_string();
        std::fs::write(&path, serde_json::to_vec_pretty(&policy).unwrap()).unwrap();
        path
    }
}

fn successful_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[cfg(debug_assertions)]
fn bounded_cli_output(command: &mut Command, context: &str) -> Output {
    let mut stdout = tempfile::tempfile().unwrap();
    let mut stderr = tempfile::tempfile().unwrap();
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout.try_clone().unwrap()))
        .stderr(Stdio::from(stderr.try_clone().unwrap()))
        .process_group(0);
    let mut child = command.spawn().unwrap();
    let process_group = i32::try_from(child.id()).unwrap();
    let timed_out = child
        .wait_timeout(std::time::Duration::from_secs(20))
        .unwrap()
        .is_none();
    if timed_out {
        // SAFETY: The negative PID targets only the process group created above.
        let result = unsafe { libc::kill(-process_group, libc::SIGKILL) };
        if result == -1 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            panic!("failed to terminate timed-out process group for {context}");
        }
    }
    let status = child.wait().unwrap();
    // SAFETY: Signal zero checks for descendants in the process group without changing them.
    let group_outlived_leader = unsafe { libc::kill(-process_group, 0) } == 0;
    if group_outlived_leader {
        // SAFETY: The negative PID targets only the process group created above.
        unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }
    stdout.rewind().unwrap();
    stderr.rewind().unwrap();
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    stdout.read_to_end(&mut stdout_bytes).unwrap();
    stderr.read_to_end(&mut stderr_bytes).unwrap();
    assert!(
        !timed_out,
        "{context} timed out: {}",
        String::from_utf8_lossy(&stderr_bytes)
    );
    assert!(
        !group_outlived_leader,
        "{context} left a running descendant"
    );
    Output {
        status,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
    }
}

#[cfg(debug_assertions)]
fn wait_for_reservation_barrier(barrier: &Path, parties: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while std::fs::read_dir(barrier).unwrap().count() != parties {
        assert!(
            std::time::Instant::now() < deadline,
            "reservation barrier timed out"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn assert_parser_rejection(output: Output, diagnostic: &str, usage: &str) {
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.lines().next(), Some(diagnostic), "{stderr}");
    assert!(stderr.contains(usage), "{stderr}");
    assert!(
        stderr.ends_with("For more information, try '--help'.\n"),
        "{stderr}"
    );
}

fn run_until_item_leaves_merging(
    fixture: &CliFixture,
    args: &[&str],
    item_id: &str,
) -> (Value, String) {
    for _ in 0..3 {
        let output = fixture.iq(args);
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let value = successful_json(output);
        let status: String = Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT status FROM queue_items WHERE id=?1",
                [item_id],
                |row| row.get(0),
            )
            .unwrap();
        if status != "merging" {
            return (value, stdout);
        }
    }
    panic!("queue item remained merging after three daemon operations")
}

#[cfg(debug_assertions)]
fn interrupt_init(fixture: &CliFixture, boundary: &str) -> Output {
    let policy = fixture.policy("main");
    Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_PROVISION_STOP_AFTER", boundary)
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            fixture.bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            policy.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

#[cfg(debug_assertions)]
fn interrupt_init_after_effect(fixture: &CliFixture, boundary: &str) -> Output {
    let policy = fixture.policy("main");
    Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_PROVISION_STOP_AFTER_EFFECT", boundary)
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            fixture.bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            policy.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

#[cfg(debug_assertions)]
fn bounded_interrupt_init_after_effect(fixture: &CliFixture, boundary: &str) -> Output {
    let policy = fixture.policy("main");
    let mut command = fixture.iq_command(&[
        "repo",
        "init",
        "--path",
        fixture.bootstrap.to_str().unwrap(),
        "--storage-root",
        fixture.root.to_str().unwrap(),
        "--policy",
        policy.to_str().unwrap(),
    ]);
    command.env("IQ_TEST_PROVISION_STOP_AFTER_EFFECT", boundary);
    bounded_cli_output(
        &mut command,
        &format!("provisioning effect boundary {boundary}"),
    )
}

fn directory_bytes(path: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            let bytes = entry
                .file_type()
                .unwrap()
                .is_file()
                .then(|| std::fs::read(entry.path()).unwrap());
            (name, bytes)
        })
        .collect()
}

#[test]
fn operation_lock_holder_process() {
    let Some(lock_path) = std::env::var_os("IQ_TEST_OPERATION_LOCK_HOLDER") else {
        return;
    };
    let ready = PathBuf::from(std::env::var_os("IQ_TEST_OPERATION_LOCK_READY").unwrap());
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);
    std::fs::write(ready, b"ready\n").unwrap();
    std::io::stdin().read_to_end(&mut Vec::new()).unwrap();
}

fn spawn_operation_lock_holder(lock: &Path, root: &Path, label: &str) -> Child {
    let ready = root.join(format!("operation-lock-{label}-ready"));
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "operation_lock_holder_process", "--nocapture"])
        .env("IQ_TEST_OPERATION_LOCK_HOLDER", lock)
        .env("IQ_TEST_OPERATION_LOCK_READY", &ready)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !ready.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "operation lock holder exited before readiness"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "operation lock holder timed out"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    child
}

fn release_operation_lock_holder(mut holder: Child) {
    drop(holder.stdin.take());
    assert!(holder.wait().unwrap().success());
}

fn rift_ancestors(database: &Path, path: &Path) -> Vec<PathBuf> {
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
        .map(PathBuf::from)
        .collect()
}

fn rift_inventory(database: &Path) -> Vec<PathBuf> {
    let connection = Connection::open(database).unwrap();
    let mut statement = connection
        .prepare("SELECT path FROM rift ORDER BY path")
        .unwrap();
    let paths = statement
        .query_map([], |row| row.get::<_, String>(0).map(PathBuf::from))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    paths
}

fn assert_policy_rejection_left_no_repository(fixture: &CliFixture) {
    let rejected = fixture.init("main");
    assert!(
        !rejected.status.success(),
        "unsafe policy was accepted: {}",
        String::from_utf8_lossy(&rejected.stdout)
    );
    assert!(!fixture.rift_database.exists());
    assert!(!fixture.root.join("repositories").exists());
    if fixture.database.exists() {
        let connection = Connection::open(&fixture.database).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM repository_policies", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }
}

#[test]
fn registration_identity_preflight_creates_no_database_or_storage_paths() {
    let fixture = CliFixture::new("main");
    let provider = fixture.root.join("preflight-gh");
    std::fs::write(
        &provider,
        "#!/bin/sh\ncase \"$4\" in\nrepos/org/repo) printf '%s' '{\"node_id\":\"wrong-id\",\"full_name\":\"org/repo\"}' ;;\nrepos/org/repo/hash-algorithm) printf '%s' '{\"hash_algorithm\":\"sha1\"}' ;;\n*) exit 3 ;;\nesac\n",
    )
    .unwrap();
    std::fs::set_permissions(&provider, std::fs::Permissions::from_mode(0o755)).unwrap();

    let provider_policy = iq::repository_policy::RepositoryPolicy {
        operation_state: iq::repository_policy::OperationState::Enabled,
        canonical_repository: iq::repository_policy::GitRepository::Accessible {
            object_format: iq::git_object::GitObjectFormat::Sha1,
            fetch_url: "https://github.com/org/repo.git".into(),
            push_url: "https://github.com/org/repo.git".into(),
            repository_id: "expected-id".into(),
            provider: iq::repository_policy::ProviderRepository {
                provider: iq::repository_policy::Provider::Github,
                host: "github.com".into(),
                repository: "org/repo".into(),
                repository_id: "expected-id".into(),
            },
        },
        target_branch: "main".into(),
        integration_policy: iq::repository_policy::IntegrationPolicy::MergeRequestRequired,
        replication_policy: iq::repository_policy::ReplicationPolicy::None,
    };
    let mut local_policy = direct_policy(&fixture.remote);
    let iq::repository_policy::GitRepository::LocalBare { inode, .. } =
        &mut local_policy.canonical_repository
    else {
        unreachable!()
    };
    *inode += 1;

    for (name, policy, provider_cli) in [
        ("provider", provider_policy, Some(provider.as_path())),
        ("local", local_policy, None),
    ] {
        let database_parent = fixture.root.join(format!("absent-{name}-database"));
        let database = database_parent.join("queues.db");
        let storage = fixture.root.join(format!("absent-{name}-storage"));
        let policy_path = fixture.root.join(format!("{name}-preflight-policy.json"));
        std::fs::write(&policy_path, serde_json::to_vec_pretty(&policy).unwrap()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_iq"));
        command
            .arg("--queue-db")
            .arg(&database)
            .args(["repo", "init", "--path"])
            .arg(&fixture.bootstrap)
            .arg("--storage-root")
            .arg(&storage)
            .arg("--policy")
            .arg(&policy_path);
        if let Some(provider_cli) = provider_cli {
            command.arg("--test-github-executable").arg(provider_cli);
        }
        let rejected = command.output().unwrap();
        assert!(
            !rejected.status.success(),
            "{name} identity unexpectedly registered"
        );
        assert!(!database_parent.exists(), "{name} created database parent");
        assert!(!database.exists(), "{name} created database");
        assert!(!storage.exists(), "{name} created storage root");
        assert!(
            !PathBuf::from(format!("{}.control.lock", database.display())).exists(),
            "{name} created database lock"
        );
    }
}

#[test]
fn failed_registration_preflight_preserves_existing_database_and_corrected_retry_succeeds() {
    let fixture = CliFixture::new("main");
    let initialized = fixture.iq(&["repo", "list"]);
    assert!(initialized.status.success());
    let storage = fixture.root.join("existing-database-storage");
    std::fs::create_dir(&storage).unwrap();
    let policy_path = fixture.root.join("existing-database-policy.json");
    let mut bad_policy = direct_policy(&fixture.remote);
    let iq::repository_policy::GitRepository::LocalBare { inode, .. } =
        &mut bad_policy.canonical_repository
    else {
        unreachable!()
    };
    *inode += 1;
    std::fs::write(
        &policy_path,
        serde_json::to_vec_pretty(&bad_policy).unwrap(),
    )
    .unwrap();
    let database_before = std::fs::read(&fixture.database).unwrap();
    let logical_state = |database: &Path| {
        let connection = Connection::open(database).unwrap();
        [
            "registered_repositories",
            "repository_provisioning_intents",
            "repository_bootstrap_requests",
            "repository_policies",
        ]
        .map(|table| {
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        })
    };
    let logical_before = logical_state(&fixture.database);
    let init = |policy: &Path| {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .arg("--queue-db")
            .arg(&fixture.database)
            .args(["repo", "init", "--path"])
            .arg(&fixture.bootstrap)
            .arg("--storage-root")
            .arg(&storage)
            .arg("--policy")
            .arg(policy)
            .output()
            .unwrap()
    };

    let rejected = init(&policy_path);
    assert!(!rejected.status.success());
    assert_eq!(std::fs::read(&fixture.database).unwrap(), database_before);
    assert_eq!(logical_state(&fixture.database), logical_before);
    assert!(std::fs::read_dir(&storage).unwrap().next().is_none());

    std::fs::write(
        &policy_path,
        serde_json::to_vec_pretty(&direct_policy(&fixture.remote)).unwrap(),
    )
    .unwrap();
    let retried = init(&policy_path);
    assert!(
        retried.status.success(),
        "{}",
        String::from_utf8_lossy(&retried.stderr)
    );
    assert_eq!(logical_state(&fixture.database)[0], 1);
}

#[test]
fn executable_environment_overrides_fail_before_database_open() {
    let root = managed_test_tempdir(".iq-provider-environment-test-");
    let database = root.path().join("queue.db");
    let rejected = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&database)
        .args(["repo", "list"])
        .env("IQ_GITHUB_CLI", "/tmp/untrusted-gh")
        .output()
        .unwrap();

    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr)
        .contains("provider executable environment overrides are forbidden"));
    assert!(!database.exists());
    assert!(!PathBuf::from(format!("{}.control.lock", database.display())).exists());

    let rejected = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&database)
        .args(["repo", "list"])
        .env("IQ_RIFT_CLI", "/tmp/untrusted-rift")
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr)
        .contains("Rift executable environment overrides are forbidden"));
    assert!(!database.exists());
}

#[test]
fn test_artifact_cleanup_preserves_similarly_named_repository_directory() {
    let unrelated = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        ".iq-unrelated-test-survival-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&unrelated).unwrap();
    std::fs::write(unrelated.join("user-data"), b"preserve\n").unwrap();

    let managed = managed_test_tempdir(".iq-cleanup-scope-test-");

    assert!(managed
        .path()
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("iq-test-artifacts-"));
    assert_eq!(
        std::fs::read(unrelated.join("user-data")).unwrap(),
        b"preserve\n"
    );
    std::fs::remove_dir_all(unrelated).unwrap();
}

#[test]
fn repo_init_returns_canonical_uuid_and_cli_rejects_malformed_key() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let key = repository["key"].as_str().unwrap();
    assert_eq!(uuid::Uuid::parse_str(key).unwrap().to_string(), key);

    let rejected = fixture.iq(&["repo", "status", "--repo-key", "not-a-repository-key"]);

    assert!(!rejected.status.success());
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(stderr.contains("repository key"), "{stderr}");
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM registered_repositories", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
}

#[test]
fn incompatible_database_is_rejected_without_mutation() {
    for marker in ["other", "1"] {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("queues.db");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE queue_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                 CREATE TABLE unrelated_state(value TEXT);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO queue_metadata(key,value) VALUES('workspace_schema_version',?1)",
                [marker],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO queue_metadata(key,value) VALUES('database_id','foreign')",
                [],
            )
            .unwrap();
        drop(connection);
        let before = std::fs::read(&database).unwrap();

        let error = match SqliteQueue::open(&database) {
            Ok(_) => panic!("incompatible database opened"),
            Err(error) => format!("{error:#}"),
        };

        assert!(error.contains("remove it and reinitialize IQ"), "{error}");
        assert_eq!(std::fs::read(&database).unwrap(), before);
        assert!(!database.with_extension("db-wal").exists());
        assert!(!database.with_extension("db-shm").exists());
    }
}

#[test]
fn preexisting_empty_database_is_rejected_without_directory_or_sidecar_mutation() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    std::fs::File::create(&database).unwrap();
    let before = directory_bytes(temporary.path());

    let rejected = SqliteQueue::open(&database);

    assert!(rejected.is_err());
    assert_eq!(directory_bytes(temporary.path()), before);
    assert_eq!(std::fs::read(&database).unwrap(), b"");
    assert!(!database.with_file_name("queues.db-wal").exists());
    assert!(!database.with_file_name("queues.db-shm").exists());
}

#[test]
#[cfg(debug_assertions)]
fn fresh_database_crashes_publish_no_destination_and_only_one_private_temp() {
    for boundary in ["temp_created", "temp_validated"] {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("queues.db");
        let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_TEST_DATABASE_STOP_AFTER", boundary)
            .arg("--queue-db")
            .arg(&database)
            .args(["repo", "list"])
            .output()
            .unwrap();
        assert_eq!(interrupted.status.code(), Some(87), "boundary {boundary}");
        assert!(!database.exists());
        assert!(!database.with_file_name("queues.db-wal").exists());
        assert!(!database.with_file_name("queues.db-shm").exists());
        let entries = std::fs::read_dir(temporary.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 1, "boundary {boundary}: {entries:?}");
        assert!(entries[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".queues.db.iq-new-"));
        std::fs::remove_file(&entries[0]).unwrap();
        drop(SqliteQueue::open(&database).unwrap());
    }
}

#[test]
#[cfg(debug_assertions)]
fn fresh_database_restart_resyncs_published_file_and_parent_before_success() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    let run = |boundary: &str| {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_TEST_DATABASE_STOP_AFTER", boundary)
            .arg("--queue-db")
            .arg(&database)
            .args(["repo", "list"])
            .output()
            .unwrap()
    };

    assert_eq!(run("published").status.code(), Some(87));
    assert!(database.is_file());
    assert_eq!(run("open_resynced").status.code(), Some(87));
    let resumed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&database)
        .args(["repo", "list"])
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let connection = Connection::open(&database).unwrap();
    assert!(!iq::sqlite::validate_existing_schema_identity(&connection)
        .unwrap()
        .is_empty());
    assert!(!std::fs::read_dir(temporary.path())
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".queues.db.iq-new-")));
}

#[test]
#[cfg(debug_assertions)]
fn concurrent_fresh_database_publications_resync_success_and_already_exists_paths() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    let barrier = temporary.path().join("database-publish-barrier");
    std::fs::create_dir(&barrier).unwrap();
    let spawn = || {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_TEST_DATABASE_PUBLISH_BARRIER", &barrier)
            .env("IQ_TEST_DATABASE_PUBLISH_BARRIER_PARTIES", "2")
            .env("IQ_TEST_DATABASE_STOP_AFTER", "resynced")
            .arg("--queue-db")
            .arg(&database)
            .args(["repo", "list"])
            .spawn()
            .unwrap()
    };
    let first = spawn();
    let second = spawn();

    assert_eq!(first.wait_with_output().unwrap().status.code(), Some(87));
    assert_eq!(second.wait_with_output().unwrap().status.code(), Some(87));
    assert!(database.is_file());
    let connection = Connection::open(&database).unwrap();
    assert!(!iq::sqlite::validate_existing_schema_identity(&connection)
        .unwrap()
        .is_empty());
    assert!(!std::fs::read_dir(temporary.path())
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".queues.db.iq-new-")));
}

#[test]
#[cfg(debug_assertions)]
fn policy_restart_resyncs_existing_identical_file_and_parent_before_success() {
    let fixture = CliFixture::new("main");
    std::fs::create_dir(fixture.bootstrap.join(".iq")).unwrap();
    let policy = b"{\"version\":1}\n";
    std::fs::write(fixture.bootstrap.join(".iq/config.json"), policy).unwrap();
    let repository_policy = fixture.policy("main");
    let run = |boundary: &str| {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_FILE_PUBLICATION_LABEL", "owned repository policy")
            .env("IQ_TEST_FILE_PUBLICATION_STOP_AFTER", boundary)
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "repo",
                "init",
                "--path",
                fixture.bootstrap.to_str().unwrap(),
                "--storage-root",
                fixture.root.to_str().unwrap(),
                "--policy",
                repository_policy.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    };

    assert_eq!(run("renamed").status.code(), Some(88));
    assert_eq!(run("resynced").status.code(), Some(88));
    let repository = successful_json(fixture.init("main"));
    let root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
    assert_eq!(std::fs::read(root.join(".iq/config.json")).unwrap(), policy);
    assert!(!std::fs::read_dir(root.join(".iq"))
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".config.json.")));
}

#[test]
#[cfg(debug_assertions)]
fn fresh_schema_installation_recovers_as_one_transaction() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_TEST_SCHEMA_STOP_AFTER_OBJECTS", "1")
        .arg("--queue-db")
        .arg(&database)
        .args(["repo", "list"])
        .output()
        .unwrap();
    assert_eq!(
        interrupted.status.code(),
        Some(86),
        "{}",
        String::from_utf8_lossy(&interrupted.stderr)
    );

    let resumed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(&database)
        .args(["repo", "list"])
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let connection = Connection::open(&database).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    assert!(!iq::sqlite::validate_existing_schema_identity(&connection)
        .unwrap()
        .is_empty());
}

#[test]
#[cfg(debug_assertions)]
fn provisioning_resume_requires_valid_preflight_after_reservation() {
    let fixture = CliFixture::new("main");
    let interrupted = interrupt_init(&fixture, "reservation");
    assert_eq!(interrupted.status.code(), Some(86));
    let renamed_bootstrap = fixture.root.join("renamed-bootstrap");
    std::fs::rename(&fixture.bootstrap, &renamed_bootstrap).unwrap();

    let rejected = fixture.init("main");

    assert!(
        !rejected.status.success(),
        "{}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("resolve bootstrap checkout"));
    std::fs::rename(renamed_bootstrap, &fixture.bootstrap).unwrap();
    successful_json(fixture.init("main"));

    let fixture = CliFixture::new("main");
    let policy = fixture.policy("main");
    let interrupted = interrupt_init(&fixture, "fetch");
    assert_eq!(interrupted.status.code(), Some(86));
    std::fs::rename(&fixture.bootstrap, fixture.root.join("deleted-bootstrap")).unwrap();
    std::fs::rename(&fixture.remote, fixture.root.join("unavailable-remote.git")).unwrap();

    let resumed = fixture.iq(&[
        "repo",
        "init",
        "--path",
        fixture.bootstrap.to_str().unwrap(),
        "--storage-root",
        fixture.root.to_str().unwrap(),
        "--policy",
        policy.to_str().unwrap(),
    ]);

    assert!(
        !resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert!(String::from_utf8_lossy(&resumed.stderr).contains("local bare repository"));
}

#[test]
#[cfg(debug_assertions)]
fn provisioning_resume_rejects_replaced_git_directory_and_accepts_restored_binding() {
    let fixture = CliFixture::new("main");
    let interrupted = interrupt_init(&fixture, "git_init");
    assert_eq!(interrupted.status.code(), Some(86));
    let connection = Connection::open(&fixture.database).unwrap();
    let staging: Vec<u8> = connection
        .query_row(
            "SELECT staging_root_path FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let staging = PathBuf::from(std::ffi::OsString::from_vec(staging));
    let original = staging.with_file_name("original-root.tmp");
    std::fs::rename(&staging, &original).unwrap();
    git(
        staging.parent().unwrap(),
        &["init", staging.to_str().unwrap()],
    );

    let rejected = fixture.init("main");
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("Git repository binding changed"),
        "{}",
        String::from_utf8_lossy(&rejected.stderr)
    );

    std::fs::remove_dir_all(&staging).unwrap();
    std::fs::rename(original, &staging).unwrap();
    let resumed = fixture.init("main");
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
}

#[test]
#[cfg(debug_assertions)]
fn second_bootstrap_resumes_fetched_remote_owner_without_remote_access() {
    let fixture = CliFixture::new("main");
    let second_bootstrap = fixture.root.join("second-bootstrap");
    git(
        &fixture.root,
        &[
            "clone",
            fixture.remote.to_str().unwrap(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    let interrupted = interrupt_init(&fixture, "fetch");
    assert_eq!(interrupted.status.code(), Some(86));
    let connection = Connection::open(&fixture.database).unwrap();
    let expected_key: String = connection
        .query_row("SELECT repo_key FROM repository_policies", [], |row| {
            row.get(0)
        })
        .unwrap();
    drop(connection);
    let wrapper_directory = fixture.root.join("unavailable-remote-bin");
    std::fs::create_dir(&wrapper_directory).unwrap();
    let git_wrapper = wrapper_directory.join("git");
    std::fs::write(
        &git_wrapper,
        "#!/bin/sh\nif [ \"$1\" = ls-remote ]; then printf 'remote unavailable\\n' >&2; exit 99; fi\nexec /usr/bin/git \"$@\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&git_wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let repository_policy = fixture.policy("main");
    let resumed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env(
            "PATH",
            format!(
                "{}:{}",
                wrapper_directory.display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            second_bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            repository_policy.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    let repository = successful_json(resumed);
    assert_eq!(repository["key"], expected_key);
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM repository_bootstrap_requests WHERE repo_key=?1",
                [&expected_key],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
}

#[test]
#[cfg(debug_assertions)]
fn remote_owner_rejects_a_different_storage_root_without_binding_the_request() {
    let fixture = CliFixture::new("main");
    let second_bootstrap = fixture.root.join("second-bootstrap");
    git(
        &fixture.root,
        &[
            "clone",
            fixture.remote.to_str().unwrap(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    assert_eq!(
        interrupt_init(&fixture, "reservation").status.code(),
        Some(86)
    );
    let different_storage = fixture.root.join("different-storage");
    std::fs::create_dir(&different_storage).unwrap();
    let policy = fixture.policy("main");
    let register_second = |storage: &Path| {
        fixture.iq(&[
            "repo",
            "init",
            "--path",
            second_bootstrap.to_str().unwrap(),
            "--storage-root",
            storage.to_str().unwrap(),
            "--policy",
            policy.to_str().unwrap(),
        ])
    };

    let request_count = || {
        Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM repository_bootstrap_requests",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let rejected_active = register_second(&different_storage);
    assert!(!rejected_active.status.success());
    assert!(String::from_utf8_lossy(&rejected_active.stderr)
        .contains("already bound to owned storage root"));
    assert_eq!(request_count(), 1);

    successful_json(fixture.init("main"));
    let rejected_ready = register_second(&different_storage);
    assert!(!rejected_ready.status.success());
    assert!(String::from_utf8_lossy(&rejected_ready.stderr)
        .contains("already bound to owned storage root"));
    assert_eq!(request_count(), 1);

    let registered = successful_json(register_second(&fixture.root));
    assert!(Path::new(registered["owned_root_path"].as_str().unwrap())
        .starts_with(fixture.root.join("repositories")));
    assert_eq!(request_count(), 2);
}

#[test]
#[cfg(debug_assertions)]
fn bootstrap_request_identity_requires_live_relative_and_symlink_spellings() {
    for symlink_spelling in [false, true] {
        let fixture = CliFixture::new("main");
        let nested = fixture.root.join("nested");
        std::fs::create_dir(&nested).unwrap();
        let spelling = if symlink_spelling {
            symlink("bootstrap", fixture.root.join("bootstrap-link")).unwrap();
            "bootstrap-link"
        } else {
            "nested/../bootstrap"
        };
        let repository_policy = fixture.policy("main");
        let run = |stop: bool| {
            let mut command = Command::new(env!("CARGO_BIN_EXE_iq"));
            command
                .current_dir(&fixture.root)
                .env("IQ_RIFT_DATABASE", &fixture.rift_database)
                .arg("--queue-db")
                .arg(&fixture.database)
                .args([
                    "repo",
                    "init",
                    "--path",
                    spelling,
                    "--storage-root",
                    fixture.root.to_str().unwrap(),
                    "--policy",
                    repository_policy.to_str().unwrap(),
                ]);
            if stop {
                command.env("IQ_TEST_PROVISION_STOP_AFTER", "reservation");
            }
            command.output().unwrap()
        };
        assert_eq!(run(true).status.code(), Some(86));
        if symlink_spelling {
            std::fs::remove_file(fixture.root.join("bootstrap-link")).unwrap();
        }
        let deleted_bootstrap = fixture.root.join("deleted-bootstrap");
        std::fs::rename(&fixture.bootstrap, &deleted_bootstrap).unwrap();

        let rejected = run(false);

        assert!(
            !rejected.status.success(),
            "{}",
            String::from_utf8_lossy(&rejected.stderr)
        );
        assert!(String::from_utf8_lossy(&rejected.stderr).contains("resolve bootstrap checkout"));
        std::fs::rename(deleted_bootstrap, &fixture.bootstrap).unwrap();
        if symlink_spelling {
            symlink("bootstrap", fixture.root.join("bootstrap-link")).unwrap();
        }
        successful_json(run(false));
    }
}

#[test]
#[cfg(debug_assertions)]
fn partial_git_and_rift_initialization_are_reconciled() {
    let fixture = CliFixture::new("main");
    assert_eq!(
        interrupt_init(&fixture, "staging_directory").status.code(),
        Some(86)
    );
    let connection = Connection::open(&fixture.database).unwrap();
    let staging: Vec<u8> = connection
        .query_row(
            "SELECT staging_root_path FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let staging = PathBuf::from(std::ffi::OsString::from_vec(staging));
    std::fs::create_dir(staging.join(".git")).unwrap();
    std::fs::write(staging.join(".git/HEAD"), b"ref: refs/heads/main\n").unwrap();
    let resumed = fixture.init("main");
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );

    let fixture = CliFixture::new("main");
    assert_eq!(interrupt_init(&fixture, "policy").status.code(), Some(86));
    let connection = Connection::open(&fixture.database).unwrap();
    let root: Vec<u8> = connection
        .query_row(
            "SELECT owned_root_path FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let root = PathBuf::from(std::ffi::OsString::from_vec(root));
    std::fs::write(root.join(".rift"), b"incomplete").unwrap();
    let resumed = fixture.init("main");
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );

    let fixture = CliFixture::new("main");
    assert_eq!(
        interrupt_init_after_effect(&fixture, "rift_init")
            .status
            .code(),
        Some(85)
    );
    let connection = Connection::open(&fixture.database).unwrap();
    let root: Vec<u8> = connection
        .query_row(
            "SELECT owned_root_path FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let root = PathBuf::from(std::ffi::OsString::from_vec(root));
    std::fs::remove_file(root.join(".rift")).unwrap();
    let resumed = fixture.init("main");
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
}

#[test]
#[cfg(debug_assertions)]
fn checkout_phase_replaces_corrupt_index_and_worktree_before_publication() {
    let fixture = CliFixture::new("main");
    let interrupted = interrupt_init(&fixture, "fetch");
    assert_eq!(interrupted.status.code(), Some(86));
    let connection = Connection::open(&fixture.database).unwrap();
    let (staging, source_sha): (Vec<u8>, String) = connection
        .query_row(
            "SELECT staging_root_path,source_sha FROM repository_provisioning_intents",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let staging = PathBuf::from(std::ffi::OsString::from_vec(staging));
    git(&staging, &["checkout", "-B", "main", &source_sha]);
    std::fs::write(staging.join("README.md"), "corrupt\n").unwrap();
    git(&staging, &["add", "README.md"]);

    let repository = successful_json(fixture.init("main"));
    let owned_root = Path::new(repository["owned_root_path"].as_str().unwrap());

    assert_eq!(git(owned_root, &["rev-parse", "HEAD"]), source_sha);
    assert_eq!(git(owned_root, &["status", "--porcelain=v1"]), "");
    assert_eq!(
        std::fs::read_to_string(owned_root.join("README.md")).unwrap(),
        "main\n"
    );
}

#[test]
#[cfg(debug_assertions)]
fn completed_policy_owner_and_child_phases_reject_tampering() {
    let fixture = CliFixture::new("main");
    std::fs::create_dir(fixture.bootstrap.join(".iq")).unwrap();
    std::fs::write(fixture.bootstrap.join(".iq/config.json"), "{}\n").unwrap();
    assert_eq!(interrupt_init(&fixture, "policy").status.code(), Some(86));
    let connection = Connection::open(&fixture.database).unwrap();
    let root: Vec<u8> = connection
        .query_row(
            "SELECT owned_root_path FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let root = PathBuf::from(std::ffi::OsString::from_vec(root));
    std::fs::write(root.join(".iq/config.json"), "tampered\n").unwrap();
    assert!(!fixture.init("main").status.success());

    let fixture = CliFixture::new("main");
    assert_eq!(interrupt_init(&fixture, "owner").status.code(), Some(86));
    let connection = Connection::open(&fixture.database).unwrap();
    let root: Vec<u8> = connection
        .query_row(
            "SELECT owned_root_path FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let root = PathBuf::from(std::ffi::OsString::from_vec(root));
    let marker = root.join(".git/iq-owner.json");
    let mut owner: Value = serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    owner["generation"] = Value::from(1);
    std::fs::write(marker, serde_json::to_vec(&owner).unwrap()).unwrap();
    assert!(!fixture.init("main").status.success());

    let fixture = CliFixture::new("main");
    assert_eq!(
        interrupt_init(&fixture, "child_roots").status.code(),
        Some(86)
    );
    let connection = Connection::open(&fixture.database).unwrap();
    let development: Vec<u8> = connection
        .query_row(
            "SELECT owned_root_path FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let development =
        PathBuf::from(std::ffi::OsString::from_vec(development)).with_file_name("development");
    std::fs::remove_file(development.join(".iq-workspace-owner.json")).unwrap();
    assert!(!fixture.init("main").status.success());
}

#[test]
#[cfg(debug_assertions)]
fn provisioning_restarts_after_every_external_effect_boundary() {
    let workers = [
        ("reservation", Some("reserved")),
        ("staging_directory", Some("staging_directory")),
        ("git_init", Some("git_initialized")),
        ("remote", Some("remote_configured")),
        ("fetch", Some("target_fetched")),
        ("checkout", Some("target_checked_out")),
        ("root", Some("root_published")),
        ("policy", Some("policy_published")),
        ("rift_init", Some("rift_initialized")),
        ("rift_proof", Some("rift_verified")),
        ("owner", Some("owner_published")),
        ("child_roots", Some("child_roots_published")),
        ("ready", None),
    ]
    .into_iter()
    .map(|(boundary, expected_phase)| {
        std::thread::spawn(move || {
            verify_provisioning_restart_boundary(boundary, expected_phase);
        })
    })
    .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
}

#[cfg(debug_assertions)]
fn verify_provisioning_restart_boundary(boundary: &str, expected_phase: Option<&str>) {
    let fixture = CliFixture::new("main");
    let repository_policy = fixture.policy("main");
    let mut interrupted_command = fixture.iq_command(&[
        "repo",
        "init",
        "--path",
        fixture.bootstrap.to_str().unwrap(),
        "--storage-root",
        fixture.root.to_str().unwrap(),
        "--policy",
        repository_policy.to_str().unwrap(),
    ]);
    interrupted_command
        .env("IQ_TEST_PROVISION_STOP_AFTER", boundary)
        .env("IQ_TEST_MODEL_KEY", "repository-test-model-key");
    let interrupted = bounded_cli_output(
        &mut interrupted_command,
        &format!("provisioning boundary {boundary}"),
    );
    assert_eq!(
        interrupted.status.code(),
        Some(86),
        "boundary {boundary} did not stop: {}",
        String::from_utf8_lossy(&interrupted.stderr)
    );
    let connection = Connection::open(&fixture.database).unwrap();
    let phase = connection
        .query_row(
            "SELECT json_extract(lifecycle_json,'$.state') FROM repository_provisioning_intents",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok();
    assert_eq!(phase.as_deref(), expected_phase, "boundary {boundary}");

    let retried = fixture.bounded_init("main", &format!("provisioning boundary {boundary} retry"));
    assert!(
        retried.status.success(),
        "boundary {boundary} retry failed: {}",
        String::from_utf8_lossy(&retried.stderr)
    );
    let repository: Value = serde_json::from_slice(&retried.stdout).unwrap();
    let repo_key = repository["key"].as_str().unwrap();
    let status = successful_json(fixture.bounded_iq(
        &["repo", "status", "--repo-key", repo_key],
        &format!("provisioning boundary {boundary} status"),
    ));
    assert_eq!(status["repository"]["key"], repo_key);
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM registered_repositories", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM workspace_roots", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM repository_provisioning_intents",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    let root = Path::new(status["repository"]["owned_root_path"].as_str().unwrap());
    assert!(!root.with_file_name(".root.tmp").exists());
}

#[test]
#[cfg(debug_assertions)]
fn provisioning_recovers_when_each_effect_precedes_its_lifecycle_record() {
    let cases = [
        ("staging_directory", "reserved"),
        ("git_init", "staging_directory"),
        ("remote", "git_initialized"),
        ("fetch", "remote_configured"),
        ("checkout", "target_fetched"),
        ("root", "target_checked_out"),
        ("policy", "root_published"),
        ("rift_init", "policy_published"),
        ("rift_proof", "rift_initialized"),
        ("owner", "rift_verified"),
        ("child_roots", "owner_published"),
        ("ready", "child_roots_published"),
    ];
    for cases in cases.chunks(4) {
        std::thread::scope(|scope| {
            for &(boundary, prior_phase) in cases {
                scope.spawn(move || {
                    verify_provisioning_effect_recovery(boundary, prior_phase);
                });
            }
        });
    }
}

#[cfg(debug_assertions)]
fn verify_provisioning_effect_recovery(boundary: &str, prior_phase: &str) {
    let fixture = CliFixture::new("main");
    let interrupted = bounded_interrupt_init_after_effect(&fixture, boundary);
    assert_eq!(
        interrupted.status.code(),
        Some(85),
        "boundary {boundary}: {}",
        String::from_utf8_lossy(&interrupted.stderr)
    );
    let connection = Connection::open(&fixture.database).unwrap();
    let (root, staging, phase): (Vec<u8>, Vec<u8>, String) = connection
        .query_row(
            "SELECT owned_root_path,staging_root_path,json_extract(lifecycle_json,'$.state') FROM repository_provisioning_intents",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(phase, prior_phase, "boundary {boundary}");
    let root = PathBuf::from(std::ffi::OsString::from_vec(root));
    let staging = PathBuf::from(std::ffi::OsString::from_vec(staging));
    if matches!(
        boundary,
        "staging_directory" | "git_init" | "remote" | "fetch" | "checkout"
    ) {
        assert!(staging.is_dir(), "boundary {boundary}");
        assert!(!root.exists(), "boundary {boundary}");
    } else {
        assert!(root.is_dir(), "boundary {boundary}");
        assert!(!staging.exists(), "boundary {boundary}");
    }
    if matches!(boundary, "git_init" | "remote" | "fetch" | "checkout") {
        assert!(staging.join(".git").is_dir(), "boundary {boundary}");
    }
    if matches!(
        boundary,
        "rift_init" | "rift_proof" | "owner" | "child_roots" | "ready"
    ) {
        assert!(root.join(".rift").is_file(), "boundary {boundary}");
    }
    if matches!(boundary, "owner" | "child_roots" | "ready") {
        assert!(
            root.join(".git/iq-owner.json").is_file(),
            "boundary {boundary}"
        );
    }
    if matches!(boundary, "child_roots" | "ready") {
        assert!(root.with_file_name("development").is_dir());
        assert!(root.with_file_name("integration").is_dir());
    }
    drop(connection);

    let resumed = fixture.bounded_init(
        "main",
        &format!("provisioning effect boundary {boundary} retry"),
    );
    assert!(
        resumed.status.success(),
        "boundary {boundary}: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let repository: Value = serde_json::from_slice(&resumed.stdout).unwrap();
    let final_root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM registered_repositories", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM repository_provisioning_intents",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    let inventory = rift_inventory(&fixture.rift_database);
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0], final_root);
    assert!(!final_root.with_file_name(".root.tmp").exists());
}

#[test]
fn repository_status_rejects_owner_marker_identity_changes() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let owned_root = Path::new(repository["owned_root_path"].as_str().unwrap());
    let marker_path = owned_root.join(".git/iq-owner.json");
    let mut marker: Value = serde_json::from_slice(&std::fs::read(&marker_path).unwrap()).unwrap();
    let registry = fixture.rift_database.canonicalize().unwrap();
    let registry_metadata = std::fs::metadata(&registry).unwrap();

    assert_eq!(marker["version"], 2);
    assert_eq!(marker["repo_key"], repo_key);
    assert_eq!(marker["owned_root_path"], owned_root.to_str().unwrap());
    assert_eq!(
        marker["root_rift_id"],
        std::fs::read_to_string(owned_root.join(".rift"))
            .unwrap()
            .trim()
    );
    assert_eq!(marker["registry_identity"], registry.to_str().unwrap());
    assert_eq!(marker["registry_device"], registry_metadata.dev());
    assert_eq!(marker["registry_inode"], registry_metadata.ino());
    assert_eq!(marker["generation"], 0);

    marker["registry_inode"] = Value::from(registry_metadata.ino() + 1);
    std::fs::write(&marker_path, serde_json::to_vec_pretty(&marker).unwrap()).unwrap();
    let rejected = fixture.iq(&["repo", "status", "--repo-key", repo_key]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr)
        .contains("owned repository marker differs from database authority"));
}

#[test]
fn repository_status_rejects_extra_fetch_and_push_urls() {
    for push in [false, true] {
        let fixture = CliFixture::new("main");
        let repository = successful_json(fixture.init("main"));
        let repo_key = repository["key"].as_str().unwrap();
        let root = Path::new(repository["owned_root_path"].as_str().unwrap());
        let extra = fixture.root.join(if push {
            "extra-push.git"
        } else {
            "extra-fetch.git"
        });
        std::fs::create_dir(&extra).unwrap();
        git(&extra, &["init", "--bare"]);
        let mut arguments = vec!["remote", "set-url"];
        if push {
            arguments.push("--push");
        }
        arguments.extend(["--add", "iq-target", extra.to_str().unwrap()]);
        git(root, &arguments);

        let rejected = fixture.iq(&["repo", "status", "--repo-key", repo_key]);

        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stderr)
            .contains("owned repository remote identity changed"));
    }
}

#[test]
fn database_open_rejects_contradictory_child_root_content() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let connection = Connection::open(&fixture.database).unwrap();
    let trigger: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='workspace_root_exact_identity_update'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute_batch("DROP TRIGGER workspace_root_exact_identity_update;")
        .unwrap();
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .unwrap();
    connection
        .execute(
            "UPDATE workspace_roots SET source_rift_id='different' WHERE repo_key=?1 AND kind='development'",
            [repo_key],
        )
        .unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    connection.execute_batch(&trigger).unwrap();
    drop(connection);

    let Err(rejected) = SqliteQueue::open(&fixture.database) else {
        panic!("contradictory child-root content was accepted");
    };
    assert!(format!("{rejected:#}").starts_with("IQ local state is incompatible;"));
}

#[test]
fn malformed_checkout_states_reject_in_domain_sql_and_database_open() {
    assert!(serde_json::from_str::<CheckoutReconciliationState>(
        r#"{"state":"pending","target_sha":"bad"}"#
    )
    .is_err());
    assert!(
        serde_json::from_str::<CheckoutReconciliationState>(&format!(
            r#"{{"state":"failed","target_sha":"{}","message":" "}}"#,
            "a".repeat(40)
        ))
        .is_err()
    );

    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let connection = Connection::open(&fixture.database).unwrap();
    assert!(connection
        .execute(
            "UPDATE registered_repositories SET checkout_json='{\"state\":\"pending\",\"target_sha\":\"bad\"}' WHERE repo_key=?1",
            [repo_key],
        )
        .is_err());
    let trigger: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='registered_repository_checkout_update'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute_batch("DROP TRIGGER registered_repository_checkout_update;")
        .unwrap();
    connection
        .execute(
            "UPDATE registered_repositories SET checkout_json='{\"state\":\"failed\",\"target_sha\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"message\":\"\"}' WHERE repo_key=?1",
            [repo_key],
        )
        .unwrap();
    connection.execute_batch(&trigger).unwrap();
    drop(connection);

    let Err(rejected) = SqliteQueue::open(&fixture.database) else {
        panic!("malformed checkout content was accepted");
    };
    assert!(format!("{rejected:#}").starts_with("IQ local state is incompatible;"));
}

#[test]
fn main_and_master_register_but_one_remote_cannot_own_both_targets() {
    for target in ["main", "master"] {
        let fixture = CliFixture::new(target);
        let repository = successful_json(fixture.init(target));
        assert_eq!(repository["target_branch"], target);
        assert_eq!(
            git(
                Path::new(repository["owned_root_path"].as_str().unwrap()),
                &["branch", "--show-current"]
            ),
            target
        );
    }

    let fixture = CliFixture::new("main");
    git(
        &fixture.root,
        &[
            "--git-dir",
            fixture.remote.to_str().unwrap(),
            "branch",
            "master",
            "main",
        ],
    );
    let mut main = Command::new(env!("CARGO_BIN_EXE_iq"));
    let main_policy = fixture.policy("main");
    let master_policy = fixture.policy("master");
    main.env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            fixture.bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            main_policy.to_str().unwrap(),
        ]);
    let mut master = Command::new(env!("CARGO_BIN_EXE_iq"));
    master
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            fixture.bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            master_policy.to_str().unwrap(),
        ]);
    let main = main.spawn().unwrap();
    let master = master.spawn().unwrap();
    let main_output = main.wait_with_output().unwrap();
    let master_output = master.wait_with_output().unwrap();
    assert_ne!(
        main_output.status.success(),
        master_output.status.success(),
        "main stderr: {}\nmaster stderr: {}",
        String::from_utf8_lossy(&main_output.stderr),
        String::from_utf8_lossy(&master_output.stderr)
    );

    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM repository_policies", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM workspace_roots", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM repository_provisioning_intents",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        std::fs::read_dir(fixture.root.join("repositories"))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM registered_repositories", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    let (owned, development, integration): (Vec<u8>, Vec<u8>, Vec<u8>) = connection
        .query_row(
            "SELECT owned_root_path,development_root_path,integration_root_path FROM registered_repositories",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let owned = PathBuf::from(std::ffi::OsString::from_vec(owned));
    let development = PathBuf::from(std::ffi::OsString::from_vec(development));
    let integration = PathBuf::from(std::ffi::OsString::from_vec(integration));
    assert!(development.is_dir());
    assert!(integration.is_dir());
    assert_eq!(rift_inventory(&fixture.rift_database), [owned]);
    let registry = Connection::open(&fixture.rift_database).unwrap();
    assert_eq!(
        registry
            .query_row("SELECT COUNT(*) FROM trash", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
    let registered_target: String = connection
        .query_row("SELECT target_branch FROM repository_policies", [], |row| {
            row.get(0)
        })
        .unwrap();
    let rejected_target = if registered_target == "main" {
        "master"
    } else {
        "main"
    };
    let rejected = fixture.init(rejected_target);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("immutable target"));
}

#[test]
fn concurrent_same_target_registration_at_reservation_barrier_returns_one_repository_identity() {
    let fixture = CliFixture::new("main");
    let barrier = fixture.root.join("reservation-barrier");
    std::fs::create_dir(&barrier).unwrap();
    let second_bootstrap = fixture.root.join("second-bootstrap");
    let repository_policy = fixture.policy("main");
    git(
        &fixture.root,
        &[
            "clone",
            fixture.remote.to_str().unwrap(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    let spawn = |path: &Path| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iq"));
        command
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_RESERVATION_BARRIER", &barrier)
            .env("IQ_TEST_RESERVATION_BARRIER_PARTIES", "2")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "repo",
                "init",
                "--path",
                path.to_str().unwrap(),
                "--storage-root",
                fixture.root.to_str().unwrap(),
                "--policy",
                repository_policy.to_str().unwrap(),
            ]);
        command.spawn().unwrap()
    };
    let first = spawn(&fixture.bootstrap);
    let second = spawn(&second_bootstrap);
    #[cfg(debug_assertions)]
    wait_for_reservation_barrier(&barrier, 2);
    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    assert!(
        first.status.success(),
        "first: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        second.status.success(),
        "second: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    let second: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(first["key"], second["key"]);
    assert_eq!(first["owned_root_path"], second["owned_root_path"]);

    let connection = Connection::open(&fixture.database).unwrap();
    for (table, count) in [
        ("repository_policies", 1),
        ("repository_bootstrap_requests", 2),
        ("registered_repositories", 1),
    ] {
        assert_eq!(
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            count,
            "table {table}"
        );
    }
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM repository_provisioning_intents",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    git(&fixture.remote, &["branch", "master", "main"]);
    let third_bootstrap = fixture.root.join("third-bootstrap");
    git(
        &fixture.root,
        &[
            "clone",
            fixture.remote.to_str().unwrap(),
            third_bootstrap.to_str().unwrap(),
        ],
    );
    let rejected = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            third_bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            fixture.policy("master").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("immutable target"));
    assert_eq!(rift_inventory(&fixture.rift_database).len(), 1);
    let rift = Connection::open(&fixture.rift_database).unwrap();
    assert_eq!(
        rift.query_row("SELECT COUNT(*) FROM trash", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
#[cfg(debug_assertions)]
fn concurrent_registration_uses_one_winning_storage_root_and_one_fence() {
    let fixture = CliFixture::new("main");
    let barrier = fixture.root.join("different-storage-reservation-barrier");
    std::fs::create_dir(&barrier).unwrap();
    let second_bootstrap = fixture.root.join("second-bootstrap");
    git(
        &fixture.root,
        &[
            "clone",
            fixture.remote.to_str().unwrap(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    let second_storage = fixture.root.join("second-storage");
    std::fs::create_dir(&second_storage).unwrap();
    let repository_policy = fixture.policy("main");
    let spawn = |path: &Path, storage: &Path| {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_RESERVATION_BARRIER", &barrier)
            .env("IQ_TEST_RESERVATION_BARRIER_PARTIES", "2")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "repo",
                "init",
                "--path",
                path.to_str().unwrap(),
                "--storage-root",
                storage.to_str().unwrap(),
                "--policy",
                repository_policy.to_str().unwrap(),
            ])
            .spawn()
            .unwrap()
    };
    let first = spawn(&fixture.bootstrap, &fixture.root);
    let second = spawn(&second_bootstrap, &second_storage);
    wait_for_reservation_barrier(&barrier, 2);
    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    assert_ne!(first.status.success(), second.status.success());

    let (winner, loser, loser_bootstrap) = if first.status.success() {
        (&first, &second, &second_bootstrap)
    } else {
        (&second, &first, &fixture.bootstrap)
    };
    let winner: Value = serde_json::from_slice(&winner.stdout).unwrap();
    let winner_root = PathBuf::from(winner["owned_root_path"].as_str().unwrap());
    let winner_storage = winner_root
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    assert!(String::from_utf8_lossy(&loser.stderr).contains("already bound to owned storage root"));
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM repository_bootstrap_requests",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );

    let retried = successful_json(fixture.iq(&[
        "repo",
        "init",
        "--path",
        loser_bootstrap.to_str().unwrap(),
        "--storage-root",
        winner_storage.to_str().unwrap(),
        "--policy",
        repository_policy.to_str().unwrap(),
    ]));
    assert_eq!(retried["key"], winner["key"]);
    assert_eq!(retried["owned_root_path"], winner["owned_root_path"]);
}

#[test]
#[cfg(debug_assertions)]
fn concurrent_registration_uses_one_winning_rift_registry() {
    let fixture = CliFixture::new("main");
    let barrier = fixture.root.join("different-registry-reservation-barrier");
    std::fs::create_dir(&barrier).unwrap();
    let second_bootstrap = fixture.root.join("second-bootstrap");
    git(
        &fixture.root,
        &[
            "clone",
            fixture.remote.to_str().unwrap(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    let second_registry = fixture.root.join("second-rift.sqlite");
    let repository_policy = fixture.policy("main");
    let spawn = |path: &Path, registry: &Path| {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", registry)
            .env("IQ_TEST_RESERVATION_BARRIER", &barrier)
            .env("IQ_TEST_RESERVATION_BARRIER_PARTIES", "2")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "repo",
                "init",
                "--path",
                path.to_str().unwrap(),
                "--storage-root",
                fixture.root.to_str().unwrap(),
                "--policy",
                repository_policy.to_str().unwrap(),
            ])
            .spawn()
            .unwrap()
    };
    let first = spawn(&fixture.bootstrap, &fixture.rift_database);
    let second = spawn(&second_bootstrap, &second_registry);
    wait_for_reservation_barrier(&barrier, 2);
    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    assert_ne!(first.status.success(), second.status.success());

    let (winner, loser, loser_bootstrap, winner_registry) = if first.status.success() {
        (&first, &second, &second_bootstrap, &fixture.rift_database)
    } else {
        (&second, &first, &fixture.bootstrap, &second_registry)
    };
    let winner: Value = serde_json::from_slice(&winner.stdout).unwrap();
    assert!(String::from_utf8_lossy(&loser.stderr).contains("and Rift registry"));
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM repository_bootstrap_requests",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );

    let retried = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", winner_registry)
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            loser_bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            repository_policy.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let retried = successful_json(retried);
    assert_eq!(retried["key"], winner["key"]);
    assert_eq!(retried["owned_root_path"], winner["owned_root_path"]);
}

#[test]
fn registrations_for_two_remotes_get_distinct_repository_uuids() {
    let fixture = CliFixture::new("main");
    let first = successful_json(fixture.init("main"));
    let second_remote = fixture.root.join("second-remote.git");
    let second_bootstrap = fixture.root.join("second-remote-bootstrap");
    std::fs::create_dir(&second_remote).unwrap();
    git(&second_remote, &["init", "--bare"]);
    git(
        &fixture.root,
        &[
            "clone",
            second_remote.to_str().unwrap(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    git(&second_bootstrap, &["config", "user.name", "IQ Test"]);
    git(
        &second_bootstrap,
        &["config", "user.email", "iq@example.test"],
    );
    git(&second_bootstrap, &["config", "commit.gpgsign", "false"]);
    std::fs::write(second_bootstrap.join("README.md"), "second\n").unwrap();
    git(&second_bootstrap, &["add", "README.md"]);
    git(&second_bootstrap, &["commit", "-m", "second remote"]);
    git(&second_bootstrap, &["branch", "-M", "main"]);
    git(&second_bootstrap, &["push", "-u", "origin", "main"]);

    let second_policy_path = fixture.root.join("second-repository-policy.json");
    std::fs::write(
        &second_policy_path,
        serde_json::to_vec_pretty(&direct_policy(&second_remote)).unwrap(),
    )
    .unwrap();
    let second = successful_json(fixture.iq(&[
        "repo",
        "init",
        "--path",
        second_bootstrap.to_str().unwrap(),
        "--storage-root",
        fixture.root.to_str().unwrap(),
        "--policy",
        second_policy_path.to_str().unwrap(),
    ]));

    assert_ne!(first["key"], second["key"]);
    assert_ne!(first["owned_root_path"], second["owned_root_path"]);
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM registered_repositories", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        2
    );
}

#[test]
fn explicit_storage_root_is_independent_of_queue_database_location() {
    let fixture = CliFixture::new("main");
    let storage_root = fixture.root.join("rift-storage");
    std::fs::create_dir(&storage_root).unwrap();
    let policy = fixture.policy("main");

    let repository = successful_json(fixture.iq(&[
        "repo",
        "init",
        "--path",
        fixture.bootstrap.to_str().unwrap(),
        "--storage-root",
        storage_root.to_str().unwrap(),
        "--policy",
        policy.to_str().unwrap(),
    ]));

    let owned_root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
    assert!(owned_root.starts_with(storage_root.join("repositories")));
    assert!(!owned_root.starts_with(fixture.database.parent().unwrap().join("repositories")));
}

#[test]
fn noncanonical_rift_registry_path_has_one_durable_identity() {
    let fixture = CliFixture::new("main");
    let registry_directory = fixture.root.join("registry-directory");
    std::fs::create_dir(&registry_directory).unwrap();
    let registry = registry_directory.join("../alternate-rift.sqlite");
    let repository_policy = fixture.policy("main");
    let register = || {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &registry)
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "repo",
                "init",
                "--path",
                fixture.bootstrap.to_str().unwrap(),
                "--storage-root",
                fixture.root.to_str().unwrap(),
                "--policy",
                repository_policy.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    };

    let first = successful_json(register());
    let retried = successful_json(register());

    assert_eq!(retried["key"], first["key"]);
    assert_eq!(retried["owned_root_path"], first["owned_root_path"]);
}

#[test]
#[cfg(debug_assertions)]
fn owned_root_refresh_keeps_durable_observation_until_a_later_refresh() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let owned_root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
    let observed_a = git(&fixture.remote, &["rev-parse", "refs/heads/main"]);
    let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_MODEL_KEY", "repository-test-model-key")
        .env("IQ_TEST_COMPOSITION_TARGET_STOP_AFTER", "observation")
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "workspace",
            "create",
            "--repo-key",
            repo_key,
            "--name",
            "observed-a",
        ])
        .output()
        .unwrap();
    assert_eq!(interrupted.status.code(), Some(82));
    let connection = Connection::open(&fixture.database).unwrap();
    let checkout_json: String = connection
        .query_row(
            "SELECT checkout_json FROM registered_repositories WHERE repo_key=?1",
            [repo_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&checkout_json).unwrap()["target_sha"],
        observed_a
    );
    drop(connection);

    git(&fixture.bootstrap, &["switch", "main"]);
    std::fs::write(fixture.bootstrap.join("remote-moved.txt"), "moved\n").unwrap();
    git(&fixture.bootstrap, &["add", "remote-moved.txt"]);
    git(
        &fixture.bootstrap,
        &["commit", "-m", "move remote after observation"],
    );
    git(&fixture.bootstrap, &["push", "origin", "main"]);
    let observed_b = git(&fixture.remote, &["rev-parse", "refs/heads/main"]);
    assert_ne!(observed_a, observed_b);

    let workspace_a = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "observed-a",
    ]));
    assert_eq!(
        git(&owned_root, &["rev-parse", "refs/remotes/iq-target/main"]),
        observed_a
    );
    assert_eq!(
        git(
            Path::new(workspace_a["path"].as_str().unwrap()),
            &["rev-parse", "HEAD"]
        ),
        observed_a
    );
    assert!(git(
        &owned_root,
        &[
            "for-each-ref",
            "--format=%(refname)",
            &format!("refs/iq/repository-targets/{repo_key}/{observed_a}")
        ]
    )
    .is_empty());
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM private_ref_cleanup_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );

    let workspace_b = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "observed-b",
    ]));
    assert_eq!(
        git(&owned_root, &["rev-parse", "refs/remotes/iq-target/main"]),
        observed_b
    );
    assert_eq!(
        git(
            Path::new(workspace_b["path"].as_str().unwrap()),
            &["rev-parse", "HEAD"]
        ),
        observed_b
    );
}

#[test]
#[cfg(debug_assertions)]
fn linked_bootstrap_request_requires_live_checkout_before_resuming_active_intent() {
    let fixture = CliFixture::new("main");
    let second_bootstrap = fixture.root.join("second-bootstrap");
    git(
        &fixture.root,
        &[
            "clone",
            fixture.remote.to_str().unwrap(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    assert_eq!(
        interrupt_init(&fixture, "reservation").status.code(),
        Some(86)
    );
    let repository_policy = fixture.policy("main");
    let linked = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_PROVISION_STOP_AFTER_EFFECT", "staging_directory")
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            second_bootstrap.to_str().unwrap(),
            "--storage-root",
            fixture.root.to_str().unwrap(),
            "--policy",
            repository_policy.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(linked.status.code(), Some(85));
    std::fs::rename(
        &second_bootstrap,
        fixture.root.join("removed-second-bootstrap"),
    )
    .unwrap();

    let resume = || {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "repo",
                "init",
                "--path",
                second_bootstrap.to_str().unwrap(),
                "--storage-root",
                fixture.root.to_str().unwrap(),
                "--policy",
                repository_policy.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    };

    let rejected = resume();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("resolve bootstrap checkout"));
    std::fs::rename(
        fixture.root.join("removed-second-bootstrap"),
        &second_bootstrap,
    )
    .unwrap();
    let repository = successful_json(resume());
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(DISTINCT repo_key) FROM repository_bootstrap_requests",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM repository_bootstrap_requests",
                [],
                |row| { row.get::<_, i64>(0) }
            )
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .query_row("SELECT repo_key FROM registered_repositories", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        repository["key"].as_str().unwrap()
    );
}

#[test]
fn ready_registration_acquires_operation_lock_before_filesystem_verification() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
    let operation_lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join(".git/iq-operation.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(operation_lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    std::fs::write(root.join("README.md"), "tampered while locked\n").unwrap();

    let rejected = fixture.init("main");

    assert!(!rejected.status.success());
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(stderr.contains("active operation"), "{stderr}");
    assert!(!stderr.contains("worktree"), "{stderr}");
    assert_eq!(
        unsafe { libc::flock(operation_lock.as_raw_fd(), libc::LOCK_UN) },
        0
    );
    git(&root, &["reset", "--hard", "HEAD"]);
    assert!(fixture.init("main").status.success());
}

#[test]
#[cfg(debug_assertions)]
fn killed_operation_holder_is_recovered_before_lease_ttl_expires() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let ready = fixture.root.join("repository-operation-ready");
    let mut holder = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .env("IQ_TEST_REPOSITORY_OPERATION_READY", &ready)
        .arg("--queue-db")
        .arg(&fixture.database)
        .args(["repo", "status", "--repo-key", repo_key])
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !ready.exists() {
        assert!(holder.try_wait().unwrap().is_none());
        assert!(
            std::time::Instant::now() < deadline,
            "operation holder timed out"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let connection = Connection::open(&fixture.database).unwrap();
    let expires_at: String = connection
        .query_row(
            "SELECT expires_at FROM repo_leases WHERE repo_key=?1",
            [repo_key],
            |row| row.get(0),
        )
        .unwrap();
    assert!(expires_at > chrono::Utc::now().to_rfc3339());
    drop(connection);
    holder.kill().unwrap();
    holder.wait().unwrap();

    let recovered = fixture.iq(&["repo", "status", "--repo-key", repo_key]);

    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
}

#[test]
fn operation_lock_cli_matrix_rejects_live_holder_and_succeeds_after_holder_exit() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let owned_root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
    let operation_lock = owned_root.join(".git/iq-operation.lock");

    let holder = spawn_operation_lock_holder(&operation_lock, &fixture.root, "status");
    let blocked = fixture.iq(&["repo", "status", "--repo-key", repo_key]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("active operation"));
    release_operation_lock_holder(holder);
    successful_json(fixture.iq(&["repo", "status", "--repo-key", repo_key]));

    let holder = spawn_operation_lock_holder(&operation_lock, &fixture.root, "create");
    let blocked = fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "lock-submit",
    ]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("active operation"));
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM development_workspaces WHERE name='lock-submit'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    drop(connection);
    release_operation_lock_holder(holder);
    let submit_workspace = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "lock-submit",
    ]));

    let cleanup_workspace = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "lock-cleanup",
    ]));
    let cleanup_id = cleanup_workspace["id"].as_str().unwrap();
    let cleanup_path = PathBuf::from(cleanup_workspace["path"].as_str().unwrap());
    let holder = spawn_operation_lock_holder(&operation_lock, &fixture.root, "cleanup");
    let blocked = fixture.iq(&["workspace", "remove", cleanup_id]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("active operation"));
    assert!(cleanup_path.is_dir());
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT status FROM development_workspaces WHERE id=?1",
                [cleanup_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "active"
    );
    release_operation_lock_holder(holder);
    successful_json(fixture.iq(&["workspace", "remove", cleanup_id]));
    assert!(!cleanup_path.exists());

    let submit_id = submit_workspace["id"].as_str().unwrap();
    let submit_path = PathBuf::from(submit_workspace["path"].as_str().unwrap());
    git(&submit_path, &["config", "user.name", "IQ Test"]);
    git(&submit_path, &["config", "user.email", "iq@example.test"]);
    git(&submit_path, &["config", "commit.gpgsign", "false"]);
    std::fs::write(submit_path.join("operation-lock.txt"), "submitted\n").unwrap();
    git(&submit_path, &["add", "operation-lock.txt"]);
    git(&submit_path, &["commit", "-m", "operation lock matrix"]);
    let holder = spawn_operation_lock_holder(&operation_lock, &fixture.root, "submit");
    let blocked = fixture.iq(&["submit", "--workspace", submit_id]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("active operation"));
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM local_submissions", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM queue_items", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    drop(connection);
    release_operation_lock_holder(holder);
    let submitted = successful_json(fixture.iq(&["submit", "--workspace", submit_id]));
    let item_id = submitted[1]["id"].as_str().unwrap();

    let daemon_config = fixture.root.join("operation-lock-daemon.yaml");
    std::fs::write(
        &daemon_config,
        format!("repos:\n  - repo_key: {repo_key}\n"),
    )
    .unwrap();
    let control = tempdir().unwrap();
    std::fs::set_permissions(control.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let system_config = fixture.root.join("operation-lock-system.yaml");
    std::fs::write(
        &system_config,
        format!(
            "integration_agent:\n  runner: opencode\n  executable: /bin/true\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 10\n  max_log_bytes: 4096\n  max_result_bytes: 4096\n  max_processes: 16\n  memory_bytes: 67108864\n  cpu_seconds: 10\n  writable_bytes: 1048576\n  open_files: 64\n  credential_env: IQ_TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
            control.path().display()
        ),
    )
    .unwrap();
    let daemon_args = [
        "daemon",
        "--config",
        daemon_config.to_str().unwrap(),
        "--system-config",
        system_config.to_str().unwrap(),
        "--once",
    ];
    let holder = spawn_operation_lock_holder(&operation_lock, &fixture.root, "daemon");
    let blocked = fixture.iq(&daemon_args);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("active operation"));
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT status FROM queue_items WHERE id=?1",
                [item_id],
                |row| { row.get::<_, String>(0) }
            )
            .unwrap(),
        "ready"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM integration_attempts WHERE item_id=?1",
                [item_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    drop(connection);
    release_operation_lock_holder(holder);
    let released = fixture.iq(&daemon_args);
    assert!(
        released.status.success(),
        "{}",
        String::from_utf8_lossy(&released.stderr)
    );
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT status FROM queue_items WHERE id=?1",
                [item_id],
                |row| { row.get::<_, String>(0) }
            )
            .unwrap(),
        "blocked"
    );
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM integration_attempts WHERE item_id=?1",
                [item_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn swapped_child_directories_are_rejected_by_exact_role_markers() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let connection = Connection::open(&fixture.database).unwrap();
    let (development, integration): (Vec<u8>, Vec<u8>) = connection
        .query_row(
            "SELECT development_root_path,integration_root_path FROM registered_repositories WHERE repo_key=?1",
            [repo_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let development = PathBuf::from(std::ffi::OsString::from_vec(development));
    let integration = PathBuf::from(std::ffi::OsString::from_vec(integration));
    let temporary = development.with_file_name("child-swap");
    std::fs::rename(&development, &temporary).unwrap();
    std::fs::rename(&integration, &development).unwrap();
    std::fs::rename(&temporary, &integration).unwrap();

    let rejected = fixture.iq(&["repo", "status", "--repo-key", repo_key]);

    assert!(!rejected.status.success());
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("owned by incompatible configuration"),
        "{stderr}"
    );
}

#[test]
fn invalid_target_leaves_no_iq_state_or_rift_residue() {
    let fixture = CliFixture::new("main");
    let rejected = fixture.init("develop");
    assert!(!rejected.status.success());
    assert!(!fixture.database.exists());
    assert!(!fixture.rift_database.exists());
    assert!(!fixture.root.join("repositories").exists());
}

#[test]
fn provisioning_rejects_unsafe_or_unverifiable_policy_entries() {
    let fixture = CliFixture::new("main");
    let external = fixture.root.join("external-policy");
    std::fs::create_dir(&external).unwrap();
    symlink(&external, fixture.bootstrap.join(".iq")).unwrap();
    assert_policy_rejection_left_no_repository(&fixture);

    let fixture = CliFixture::new("main");
    std::fs::create_dir(fixture.bootstrap.join(".iq")).unwrap();
    let external = fixture.root.join("external-config.json");
    std::fs::write(&external, "{}\n").unwrap();
    symlink(&external, fixture.bootstrap.join(".iq/config.json")).unwrap();
    assert_policy_rejection_left_no_repository(&fixture);

    let fixture = CliFixture::new("main");
    std::fs::create_dir(fixture.bootstrap.join(".iq")).unwrap();
    let fifo = std::ffi::CString::new(fixture.bootstrap.join(".iq/config.json").to_str().unwrap())
        .unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    assert_policy_rejection_left_no_repository(&fixture);

    let fixture = CliFixture::new("main");
    std::fs::create_dir(fixture.bootstrap.join(".iq")).unwrap();
    std::fs::write(fixture.bootstrap.join(".iq/config.json"), "{}\n").unwrap();
    let policy_object = git(
        &fixture.bootstrap,
        &["hash-object", "-w", ".iq/config.json"],
    );
    git(
        &fixture.bootstrap,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "100644",
            &policy_object,
            ".iq/config.json",
        ],
    );
    git(
        &fixture.bootstrap,
        &["commit", "-m", "track forbidden policy"],
    );
    assert_policy_rejection_left_no_repository(&fixture);

    let fixture = CliFixture::new("main");
    std::fs::write(fixture.bootstrap.join(".git/index"), "invalid index\n").unwrap();
    assert_policy_rejection_left_no_repository(&fixture);
}

#[test]
fn cli_lifecycle_uses_only_repository_key_after_bootstrap_deletion() {
    let fixture = CliFixture::new("main");
    let initial_sha = git(&fixture.bootstrap, &["rev-parse", "HEAD"]);
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap().to_string();
    std::fs::rename(&fixture.bootstrap, fixture.root.join("deleted-bootstrap")).unwrap();

    successful_json(fixture.iq(&["repo", "status", "--repo-key", &repo_key]));
    let workspace = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        &repo_key,
        "--name",
        "bootstrap-deleted",
    ]));
    let workspace_id = workspace["id"].as_str().unwrap();
    let workspace_path = PathBuf::from(workspace["path"].as_str().unwrap());
    git(&workspace_path, &["config", "user.name", "IQ Test"]);
    git(
        &workspace_path,
        &["config", "user.email", "iq@example.test"],
    );
    git(&workspace_path, &["config", "commit.gpgsign", "false"]);
    std::fs::write(workspace_path.join("feature.txt"), "from child\n").unwrap();
    git(&workspace_path, &["add", "feature.txt"]);
    git(&workspace_path, &["commit", "-m", "feature"]);
    let submitted = successful_json(fixture.iq(&["submit", "--workspace", workspace_id]));
    let item_id = submitted[1]["id"].as_str().unwrap().to_string();

    let daemon_config = fixture.root.join("daemon.yaml");
    std::fs::write(
        &daemon_config,
        format!("repos:\n  - repo_key: {repo_key}\n"),
    )
    .unwrap();
    let system_config = fixture.root.join("system.yaml");
    let runner = fixture.root.join("fake-opencode");
    std::fs::write(
        &runner,
        r#"#!/usr/bin/python3
import hashlib
import json
import os
import subprocess

with open("/iq-protocol/input.json", "r", encoding="utf-8") as source:
    request = json.load(source)
tree = subprocess.check_output(["git", "write-tree"], text=True).strip()
result = {
    "outcome": "resolved",
    "version": 2,
    "identity": request["identity"],
    "staged_tree_sha256": hashlib.sha256(tree.encode()).hexdigest(),
    "changed_paths": [[{"hex": "666561747572652e747874"}]],
    "checks": [],
}

with open("/iq-protocol/result.json.tmp", "w", encoding="utf-8") as output:
    json.dump(result, output, separators=(",", ":"))
os.replace("/iq-protocol/result.json.tmp", "/iq-protocol/result.json")
"#,
    )
    .unwrap();
    std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
    let control_temporary = tempdir().unwrap();
    let control_directory = control_temporary.path();
    std::fs::set_permissions(control_directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        &system_config,
        format!(
            "integration_agent:\n  runner: opencode\n  executable: {}\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 10\n  max_log_bytes: 4096\n  max_result_bytes: 4096\n  max_processes: 16\n  memory_bytes: 67108864\n  cpu_seconds: 10\n  writable_bytes: 1048576\n  open_files: 64\n  credential_env: IQ_TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
            runner.display(),
            control_directory.display()
        ),
    )
    .unwrap();
    successful_json(fixture.iq(&[
        "doctor",
        "--config",
        daemon_config.to_str().unwrap(),
        "--system-config",
        system_config.to_str().unwrap(),
    ]));
    let daemon_args = [
        "daemon",
        "--config",
        daemon_config.to_str().unwrap(),
        "--system-config",
        system_config.to_str().unwrap(),
        "--once",
    ];
    let (_, daemon_stdout) = run_until_item_leaves_merging(&fixture, &daemon_args, &item_id);

    let landed_sha = git(&fixture.remote, &["rev-parse", "refs/heads/main"]);
    let conflict_summary = Connection::open(&fixture.database)
        .unwrap()
        .query_row(
            "SELECT coalesce(json_extract(conflict_json,'$.summary'),'') FROM queue_items LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    let cycle_failure = Connection::open(&fixture.database)
        .unwrap()
        .query_row(
            "SELECT coalesce(failure_json,'') || char(10) || coalesce(CAST(log_blob AS TEXT),'') FROM integration_cycles ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert_ne!(
        landed_sha, initial_sha,
        "daemon output: {daemon_stdout}\nconflict summary: {conflict_summary}\ncycle failure: {cycle_failure}"
    );
    assert_eq!(
        git(&fixture.remote, &["show", "refs/heads/main:feature.txt"]),
        "from child"
    );
    assert_eq!(
        git(
            &fixture.remote,
            &["merge-base", "--is-ancestor", &initial_sha, &landed_sha]
        ),
        ""
    );
    let cleanup = successful_json(fixture.iq(&[
        "cleanup",
        "--repo-key",
        &repo_key,
        "--system-config",
        system_config.to_str().unwrap(),
    ]));
    assert!(!workspace_path.exists());
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM development_workspaces WHERE status!='removed'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0,
        "cleanup output: {cleanup}"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM terminal_workspace_cleanup_debt",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn removed_cli_surfaces_fail_in_parser_without_state_or_filesystem_effects() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let before = directory_bytes(&fixture.root);

    let cases: Vec<(Vec<&str>, &str, &str)> = vec![
        (
            vec!["dev-workspace", "list"],
            "error: unrecognized subcommand 'dev-workspace'",
            "Usage: iq [OPTIONS] <COMMAND>",
        ),
        (
            vec!["workspace", "status", "--repo-key", repo_key],
            "error: unexpected argument '--repo-key' found",
            "Usage: iq workspace status [OPTIONS] <ID>",
        ),
        (
            vec!["workspace", "reset", "--repo-key", repo_key],
            "error: unrecognized subcommand 'reset'",
            "Usage: iq workspace [OPTIONS] <COMMAND>",
        ),
        (
            vec![
                "integrate",
                "--next",
                "--repo-key",
                repo_key,
                "--system-config",
                "/unused",
                "--repo-path",
                "/unused",
            ],
            "error: unexpected argument '--repo-path' found",
            "Usage: iq integrate --system-config <SYSTEM_CONFIG> --repo-key <REPO_KEY>",
        ),
        (
            vec![
                "integrate",
                "--next",
                "--repo-key",
                repo_key,
                "--system-config",
                "/unused",
                "--base-remote",
                "origin",
            ],
            "error: unexpected argument '--base-remote' found",
            "Usage: iq integrate --system-config <SYSTEM_CONFIG> --repo-key <REPO_KEY>",
        ),
        (
            vec![
                "integrate",
                "--next",
                "--repo-key",
                repo_key,
                "--system-config",
                "/unused",
                "--workspace-root",
                "/unused",
            ],
            "error: unexpected argument '--workspace-root' found",
            "Usage: iq integrate --system-config <SYSTEM_CONFIG> --repo-key <REPO_KEY>",
        ),
        (
            vec![
                "integrate",
                "--next",
                "--repo-key",
                repo_key,
                "--system-config",
                "/unused",
                "--rift-database",
                "/unused",
            ],
            "error: unexpected argument '--rift-database' found",
            "Usage: iq integrate --system-config <SYSTEM_CONFIG> --repo-key <REPO_KEY>",
        ),
        (
            vec!["cleanup", "--workspace", "not-authorized"],
            "error: unexpected argument '--workspace' found",
            "Usage: iq cleanup [OPTIONS] --repo-key <REPO_KEY> --system-config <SYSTEM_CONFIG>",
        ),
    ];
    for (arguments, diagnostic, usage) in cases {
        assert_parser_rejection(fixture.iq(&arguments), diagnostic, usage);
        assert_eq!(directory_bytes(&fixture.root), before);
    }
}

#[test]
fn submit_replace_creates_a_new_immutable_item_and_lands_the_replacement_commit() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let workspace = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "replace-flow",
    ]));
    let workspace_id = workspace["id"].as_str().unwrap();
    let workspace_path = PathBuf::from(workspace["path"].as_str().unwrap());
    git(&workspace_path, &["config", "user.name", "IQ Test"]);
    git(
        &workspace_path,
        &["config", "user.email", "iq@example.test"],
    );
    git(&workspace_path, &["config", "commit.gpgsign", "false"]);
    std::fs::write(workspace_path.join("feature.txt"), "first\n").unwrap();
    git(&workspace_path, &["add", "feature.txt"]);
    git(&workspace_path, &["commit", "-m", "first candidate"]);
    let submitted = successful_json(fixture.iq(&["submit", "--workspace", workspace_id]));
    let item_id = submitted[1]["id"].as_str().unwrap().to_string();

    let daemon_config = fixture.root.join("replace-daemon.yaml");
    std::fs::write(
        &daemon_config,
        format!("repos:\n  - repo_key: {repo_key}\n"),
    )
    .unwrap();
    let control = tempdir().unwrap();
    std::fs::set_permissions(control.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let blocked_system = fixture.root.join("replace-blocked-system.yaml");
    std::fs::write(
        &blocked_system,
        format!(
            "integration_agent:\n  runner: opencode\n  executable: /bin/false\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 10\n  max_log_bytes: 4096\n  max_result_bytes: 4096\n  max_processes: 16\n  memory_bytes: 67108864\n  cpu_seconds: 10\n  writable_bytes: 1048576\n  open_files: 64\n  credential_env: IQ_TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
            control.path().display()
        ),
    )
    .unwrap();
    let blocked_args = [
        "daemon",
        "--config",
        daemon_config.to_str().unwrap(),
        "--system-config",
        blocked_system.to_str().unwrap(),
        "--once",
    ];
    run_until_item_leaves_merging(&fixture, &blocked_args, &item_id);
    let connection = Connection::open(&fixture.database).unwrap();
    let blocked: (String, String) = connection
        .query_row(
            "SELECT status,blocked_reason FROM queue_items WHERE id=?1",
            [&item_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(blocked, ("blocked".into(), "needs_agent_fix".into()));

    std::fs::write(workspace_path.join("feature.txt"), "replacement\n").unwrap();
    git(&workspace_path, &["add", "feature.txt"]);
    git(&workspace_path, &["commit", "-m", "replacement candidate"]);
    let replacement = successful_json(fixture.iq(&[
        "submit",
        "--workspace",
        workspace_id,
        "--replace",
        &item_id,
    ]));
    let replacement_workspace_sha = git(&workspace_path, &["rev-parse", "HEAD"]);
    std::fs::write(workspace_path.join("feature.txt"), "third unsubmitted\n").unwrap();
    git(&workspace_path, &["add", "feature.txt"]);
    git(
        &workspace_path,
        &["commit", "-m", "third unsubmitted candidate"],
    );
    let third_workspace_sha = git(&workspace_path, &["rev-parse", "HEAD"]);
    assert_ne!(third_workspace_sha, replacement_workspace_sha);
    let replacement_item_id = replacement[1]["id"].as_str().unwrap().to_string();
    assert_ne!(replacement_item_id, item_id);
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM local_submissions WHERE queue_item_id=?1",
                [&item_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let old_state: (String, String, String) = connection
        .query_row(
            "SELECT item.status,effort.state,submission.state FROM queue_items item JOIN integration_efforts effort ON effort.item_id=item.id JOIN queue_admissions admission ON admission.item_id=item.id JOIN local_submissions submission ON submission.id=admission.submission_id WHERE item.id=?1",
            [&item_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        old_state,
        ("cancelled".into(), "cancelled".into(), "replaced".into())
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT replaces_item_id FROM local_submissions WHERE queue_item_id=?1",
                [&replacement_item_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        item_id
    );

    let runner = fixture.root.join("replace-runner");
    std::fs::write(
        &runner,
        r#"#!/usr/bin/python3
import hashlib
import json
import os
import subprocess

with open("/iq-protocol/input.json", "r", encoding="utf-8") as source:
    request = json.load(source)
tree = subprocess.check_output(["git", "write-tree"], text=True).strip()
result = {
    "outcome": "resolved",
    "version": 2,
    "identity": request["identity"],
    "staged_tree_sha256": hashlib.sha256(tree.encode()).hexdigest(),
    "changed_paths": [[{"hex": "666561747572652e747874"}]],
    "checks": [],
}
with open("/iq-protocol/result.json.tmp", "w", encoding="utf-8") as output:
    json.dump(result, output, separators=(",", ":"))
os.replace("/iq-protocol/result.json.tmp", "/iq-protocol/result.json")
"#,
    )
    .unwrap();
    std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
    let valid_system = fixture.root.join("replace-valid-system.yaml");
    let mut valid = std::fs::read_to_string(&blocked_system).unwrap();
    valid = valid.replace(
        "executable: /bin/false",
        &format!("executable: {}", runner.display()),
    );
    std::fs::write(&valid_system, valid).unwrap();

    let daemon = successful_json(fixture.iq(&[
        "daemon",
        "--config",
        daemon_config.to_str().unwrap(),
        "--system-config",
        valid_system.to_str().unwrap(),
        "--once",
    ]));
    let status: (String, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT status,blocked_reason,blocked_message FROM queue_items WHERE id=?1",
            [&replacement_item_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status.0, "integrated", "status={status:?} daemon={daemon}");
    let completed_effort: String = connection
        .query_row(
            "SELECT state FROM integration_efforts WHERE item_id=?1",
            [&replacement_item_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(completed_effort, "integrated");
    assert_eq!(
        git(&fixture.remote, &["show", "refs/heads/main:feature.txt"]),
        "replacement"
    );
    assert!(!Command::new("git")
        .args([
            "--git-dir",
            fixture.remote.to_str().unwrap(),
            "merge-base",
            "--is-ancestor",
            &third_workspace_sha,
            "refs/heads/main",
        ])
        .output()
        .unwrap()
        .status
        .success());
    let preserved = fixture.iq(&[
        "cleanup",
        "--repo-key",
        repo_key,
        "--system-config",
        valid_system.to_str().unwrap(),
    ]);
    assert!(!preserved.status.success());
    assert!(workspace_path.is_dir());
    assert_eq!(
        git(&workspace_path, &["rev-parse", "HEAD"]),
        third_workspace_sha
    );
    git(
        &workspace_path,
        &["reset", "--hard", &replacement_workspace_sha],
    );
    successful_json(fixture.iq(&[
        "cleanup",
        "--repo-key",
        repo_key,
        "--system-config",
        valid_system.to_str().unwrap(),
    ]));
    assert!(!workspace_path.exists());
}

#[test]
#[cfg(debug_assertions)]
fn development_generation_crashes_reconcile_pending_marker_and_single_workspace() {
    for boundary in ["development_recorded", "development_marker"] {
        let fixture = CliFixture::new("main");
        let repository = successful_json(fixture.init("main"));
        let repo_key = repository["key"].as_str().unwrap();
        let owned_root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
        let inventory_before = rift_inventory(&fixture.rift_database);
        let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_MODEL_KEY", "repository-test-model-key")
            .env("IQ_TEST_WORKSPACE_GENERATION_STOP_AFTER", boundary)
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "workspace",
                "create",
                "--repo-key",
                repo_key,
                "--name",
                "generation-crash",
            ])
            .output()
            .unwrap();
        assert_eq!(interrupted.status.code(), Some(84), "boundary {boundary}");
        let connection = Connection::open(&fixture.database).unwrap();
        let (root, current, pending): (Vec<u8>, i64, Option<i64>) = connection
            .query_row(
                "SELECT root_path,generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind='development'",
                [repo_key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((current, pending), (0, Some(1)));
        let root = PathBuf::from(std::ffi::OsString::from_vec(root));
        let marker = std::fs::read_to_string(root.join(".iq-workspace-generation")).unwrap();
        assert_eq!(
            marker.trim().parse::<i64>().unwrap(),
            if boundary == "development_marker" {
                1
            } else {
                0
            }
        );
        assert_eq!(rift_inventory(&fixture.rift_database), inventory_before);
        assert_eq!(
            Connection::open(&fixture.rift_database)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM trash", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(connection);

        let resumed = successful_json(fixture.iq(&[
            "workspace",
            "create",
            "--repo-key",
            repo_key,
            "--name",
            "generation-crash",
        ]));

        let connection = Connection::open(&fixture.database).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind='development'",
                    [repo_key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
                )
                .unwrap(),
            (1, None)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM development_workspaces WHERE repo_key=?1",
                    [repo_key],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        let workspace_path = PathBuf::from(resumed["path"].as_str().unwrap());
        let rift_id = resumed["identity"]["rift_id"].as_str().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT rift_id FROM development_workspaces WHERE repo_key=?1",
                    [repo_key],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            rift_id
        );
        assert!(workspace_path.is_dir());
        assert_eq!(
            std::fs::read_to_string(workspace_path.join(".rift"))
                .unwrap()
                .trim(),
            rift_id
        );
        assert_eq!(
            rift_ancestors(&fixture.rift_database, &workspace_path),
            [owned_root]
        );
        let mut expected_inventory = inventory_before;
        expected_inventory.push(workspace_path);
        expected_inventory.sort();
        assert_eq!(rift_inventory(&fixture.rift_database), expected_inventory);
        assert_eq!(
            Connection::open(&fixture.rift_database)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM trash", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            std::fs::read_to_string(root.join(".iq-workspace-generation"))
                .unwrap()
                .trim()
                .parse::<i64>()
                .unwrap(),
            1
        );
        assert!(!std::fs::read_dir(&root).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".iq-workspace-generation-")));
    }
}

#[test]
fn workspace_root_rejects_owner_marker_database_id_mismatch() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let root = PathBuf::from(repository["development_root_path"].as_str().unwrap());
    let marker_path = root.join(".iq-workspace-owner.json");
    let mut marker: Value = serde_json::from_slice(&std::fs::read(&marker_path).unwrap()).unwrap();
    marker["queue_database_id"] = Value::String(uuid::Uuid::new_v4().to_string());
    std::fs::write(&marker_path, serde_json::to_vec(&marker).unwrap()).unwrap();

    let rejected = fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "wrong-database-owner",
    ]);

    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("owned by incompatible configuration")
    );
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM development_workspaces", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn workspace_root_rejects_generation_two_steps_ahead_of_authority() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let root = PathBuf::from(repository["development_root_path"].as_str().unwrap());
    std::fs::write(root.join(".iq-workspace-generation"), b"2\n").unwrap();

    let rejected = fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "future-generation",
    ]);

    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains(
        "current/pending generation authority differs from IQ workspace root generation 2"
    ));
    assert_eq!(
        Connection::open(&fixture.database)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM development_workspaces", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn workspace_remove_needs_no_system_configuration() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let workspace = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "cleanup-only",
    ]));
    let removed =
        successful_json(fixture.iq(&["workspace", "remove", workspace["id"].as_str().unwrap()]));
    assert_eq!(removed["status"], "removed");
    assert!(!Path::new(workspace["path"].as_str().unwrap()).exists());
}

#[test]
fn cleanup_preserves_dirty_workspace_then_removes_clean_workspace_and_authority() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let workspace = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repository["key"].as_str().unwrap(),
        "--name",
        "dirty-cleanup",
    ]));
    let workspace_id = workspace["id"].as_str().unwrap();
    let workspace_path = PathBuf::from(workspace["path"].as_str().unwrap());
    std::fs::write(workspace_path.join("dirty.txt"), "preserve me\n").unwrap();

    let rejected = fixture.iq(&["workspace", "remove", workspace_id]);

    assert!(!rejected.status.success());
    assert!(workspace_path.join("dirty.txt").is_file());
    let connection = Connection::open(&fixture.database).unwrap();
    let (status, cleanup): (String, String) = connection
        .query_row(
            "SELECT status,cleanup_json FROM development_workspaces WHERE id=?1",
            [workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "cleanup_failed");
    assert_eq!(
        serde_json::from_str::<Value>(&cleanup).unwrap()["state"],
        "operator_failed"
    );
    drop(connection);

    std::fs::remove_file(workspace_path.join("dirty.txt")).unwrap();
    successful_json(fixture.iq(&["workspace", "remove", workspace_id]));

    assert!(!workspace_path.exists());
    let connection = Connection::open(&fixture.database).unwrap();
    let (status, cleanup): (String, String) = connection
        .query_row(
            "SELECT status,cleanup_json FROM development_workspaces WHERE id=?1",
            [workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "removed");
    assert_eq!(
        serde_json::from_str::<Value>(&cleanup).unwrap()["state"],
        "complete"
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM workspace_gc_debt", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    assert_eq!(rift_inventory(&fixture.rift_database).len(), 1);
}

#[test]
fn development_and_integration_rifts_are_direct_isolated_children_of_owned_root() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let owned_root = PathBuf::from(repository["owned_root_path"].as_str().unwrap());
    let mut exclude = std::fs::read_to_string(owned_root.join(".git/info/exclude")).unwrap();
    exclude.push_str("warm-cache/\n");
    std::fs::write(owned_root.join(".git/info/exclude"), exclude).unwrap();
    std::fs::create_dir(owned_root.join("warm-cache")).unwrap();
    std::fs::write(owned_root.join("warm-cache/build.bin"), "root artifact\n").unwrap();

    let workspace = successful_json(fixture.iq(&[
        "workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "cow-children",
    ]));
    let development = PathBuf::from(workspace["path"].as_str().unwrap());
    assert_eq!(
        std::fs::read_to_string(development.join("warm-cache/build.bin")).unwrap(),
        "root artifact\n"
    );
    std::fs::write(
        development.join("warm-cache/build.bin"),
        "development artifact\n",
    )
    .unwrap();
    std::fs::write(development.join("feature.txt"), "feature\n").unwrap();
    git(&development, &["config", "user.name", "IQ Test"]);
    git(&development, &["config", "user.email", "iq@example.test"]);
    git(&development, &["config", "commit.gpgsign", "false"]);
    git(&development, &["add", "feature.txt"]);
    git(&development, &["commit", "-m", "feature"]);
    let submitted =
        successful_json(fixture.iq(&["submit", "--workspace", workspace["id"].as_str().unwrap()]));
    let item_id = submitted[1]["id"].as_str().unwrap().to_string();

    let daemon = fixture.root.join("blocked-daemon.yaml");
    std::fs::write(&daemon, format!("repos:\n  - repo_key: {repo_key}\n")).unwrap();
    let control = tempdir().unwrap();
    std::fs::set_permissions(control.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let system = fixture.root.join("blocked-system.yaml");
    std::fs::write(
        &system,
        format!(
            "integration_agent:\n  runner: opencode\n  executable: /bin/false\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 10\n  max_log_bytes: 4096\n  max_result_bytes: 4096\n  max_processes: 16\n  memory_bytes: 67108864\n  cpu_seconds: 10\n  writable_bytes: 1048576\n  open_files: 64\n  credential_env: IQ_TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
            control.path().display()
        ),
    )
    .unwrap();
    let daemon_args = [
        "daemon",
        "--config",
        daemon.to_str().unwrap(),
        "--system-config",
        system.to_str().unwrap(),
        "--once",
    ];
    let (blocked, _) = run_until_item_leaves_merging(&fixture, &daemon_args, &item_id);
    let integration = PathBuf::from(
        blocked[0]["workspace"]["identity"]["path"]
            .as_str()
            .unwrap(),
    );
    std::fs::write(
        integration.join("warm-cache/build.bin"),
        "integration artifact\n",
    )
    .unwrap();

    assert_eq!(
        rift_ancestors(&fixture.rift_database, &development),
        std::slice::from_ref(&owned_root)
    );
    assert_eq!(
        rift_ancestors(&fixture.rift_database, &integration),
        std::slice::from_ref(&owned_root)
    );
    assert_eq!(
        std::fs::read_to_string(owned_root.join("warm-cache/build.bin")).unwrap(),
        "root artifact\n"
    );
    assert_eq!(
        std::fs::read_to_string(development.join("warm-cache/build.bin")).unwrap(),
        "development artifact\n"
    );
    assert_eq!(
        std::fs::read_to_string(integration.join("warm-cache/build.bin")).unwrap(),
        "integration artifact\n"
    );
}

#[test]
fn daemon_cli_rejects_all_repository_authority_fields_before_database_open() {
    let fixture = CliFixture::new("main");
    let system = fixture.root.join("system.yaml");
    let control = tempdir().unwrap();
    std::fs::set_permissions(control.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        &system,
        format!(
            "integration_agent:\n  runner: opencode\n  executable: /bin/true\n  agent: iq-integration\n  model: test/model\n  cycle_timeout_seconds: 10\n  max_log_bytes: 4096\n  max_result_bytes: 4096\n  max_processes: 16\n  memory_bytes: 67108864\n  cpu_seconds: 10\n  writable_bytes: 1048576\n  open_files: 64\n  credential_env: IQ_TEST_MODEL_KEY\ncontrol_plane:\n  unix_socket: {}/control.sock\n  max_request_bytes: 4096\n  max_free_text_bytes: 1024\n  max_response_bytes: 4096\n  max_concurrent_clients: 2\n  max_client_queue_bytes: 4096\n  max_stream_backlog_events: 100\n  client_idle_seconds: 5\nnotifications:\n  backends: []\n  max_attempts: 2\n  max_event_age_seconds: 60\n  projection_debt_alert_seconds: 60\n",
            control.path().display()
        ),
    )
    .unwrap();
    for field in [
        "path",
        "repo_path",
        "target",
        "target_branch",
        "remote",
        "workspace",
        "workspace_root",
        "policy",
    ] {
        let config = fixture.root.join(format!("forbidden-{field}.yaml"));
        std::fs::write(
            &config,
            format!(
                "repos:\n  - repo_key: 00000000-0000-4000-8000-000000000001\n    {field}: forbidden\n"
            ),
        )
        .unwrap();
        let database = fixture.root.join(format!("forbidden-{field}.db"));
        let rejected = Command::new(env!("CARGO_BIN_EXE_iq"))
            .arg("--queue-db")
            .arg(&database)
            .args([
                "daemon",
                "--config",
                config.to_str().unwrap(),
                "--system-config",
                system.to_str().unwrap(),
                "--once",
            ])
            .output()
            .unwrap();
        assert!(!rejected.status.success(), "field {field} was accepted");
        assert!(!database.exists(), "field {field} mutated the database");
    }
}
