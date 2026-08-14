use iq::repository::ProvisionOptions;
use iq::sqlite::{CheckoutReconciliationState, EnqueueRequest, SqliteQueue};
use rusqlite::Connection;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use tempfile::tempdir;

fn git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .unwrap();
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
        let temporary = tempfile::Builder::new()
            .prefix(".iq-repository-test-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .unwrap();
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

    fn iq(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &self.rift_database)
            .env("IQ_TEST_MODEL_KEY", "repository-test-model-key")
            .arg("--queue-db")
            .arg(&self.database)
            .args(args)
            .output()
            .unwrap()
    }

    fn init(&self, target: &str) -> Output {
        self.iq(&[
            "repo",
            "init",
            "--path",
            self.bootstrap.to_str().unwrap(),
            "--target",
            target,
        ])
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
            "--target",
            "main",
        ])
        .output()
        .unwrap()
}

#[cfg(debug_assertions)]
fn interrupt_init_after_effect(fixture: &CliFixture, boundary: &str) -> Output {
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
            "--target",
            "main",
        ])
        .output()
        .unwrap()
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
                .query_row("SELECT COUNT(*) FROM repository_remote_owners", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }
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
                "--target",
                "main",
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
fn provisioning_is_independent_of_bootstrap_state_and_retries_by_identity() {
    let temporary = tempfile::Builder::new()
        .prefix("iq-owned-root-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let remote = temporary.path().join("remote.git");
    let bootstrap = temporary.path().join("bootstrap");
    std::fs::create_dir(&remote).unwrap();
    git(&remote, &["init", "--bare"]);
    git(
        temporary.path(),
        &[
            "clone",
            remote.to_str().unwrap(),
            bootstrap.to_str().unwrap(),
        ],
    );
    git(&bootstrap, &["config", "user.name", "IQ Test"]);
    git(&bootstrap, &["config", "user.email", "iq@example.test"]);
    git(&bootstrap, &["config", "commit.gpgsign", "false"]);
    std::fs::write(bootstrap.join("README.md"), "main\n").unwrap();
    git(&bootstrap, &["add", "README.md"]);
    git(&bootstrap, &["commit", "-m", "main"]);
    git(&bootstrap, &["branch", "-M", "main"]);
    git(&bootstrap, &["push", "-u", "origin", "main"]);
    let source_sha = git(&bootstrap, &["rev-parse", "HEAD"]);
    std::fs::create_dir(bootstrap.join(".iq")).unwrap();
    std::fs::write(bootstrap.join(".iq/config.json"), b"{\"version\":1}\n").unwrap();
    git(&bootstrap, &["switch", "-c", "dirty-bootstrap"]);
    std::fs::write(bootstrap.join("README.md"), "dirty\n").unwrap();

    let database = temporary.path().join("queues.db");
    let queue = SqliteQueue::open(&database).unwrap();
    std::fs::create_dir(temporary.path().join("owned")).unwrap();
    let options = ProvisionOptions {
        storage_root: temporary.path().join("owned"),
        bootstrap_path: bootstrap.clone(),
        target: "main".into(),
        remote_name: "origin".into(),
        rift_database: Some(temporary.path().join("rift.sqlite")),
    };

    let owned = queue.provision_repository(&options).unwrap();
    let retried = queue.provision_repository(&options).unwrap();

    assert_eq!(retried.repo_key(), owned.repo_key());
    assert_eq!(retried.path(), owned.path());
    assert_ne!(owned.path(), bootstrap);
    assert_eq!(git(owned.path(), &["rev-parse", "HEAD"]), source_sha);
    assert_eq!(git(owned.path(), &["branch", "--show-current"]), "main");
    assert!(!owned.path().join(".git/objects/info/alternates").exists());
    assert_eq!(
        std::fs::read(owned.path().join(".iq/config.json")).unwrap(),
        b"{\"version\":1}\n"
    );
    let ancestors = std::process::Command::new("rift")
        .arg("--database")
        .arg(temporary.path().join("rift.sqlite"))
        .args(["ancestors", owned.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(ancestors.status.success());
    assert!(ancestors.stdout.is_empty());
    assert_eq!(owned.children().development.parent(), owned.path().parent());
    assert_eq!(owned.children().integration.parent(), owned.path().parent());

    let connection = Connection::open(&database).unwrap();
    let bootstrap_references: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM registered_repositories WHERE owned_root_path=?1 OR development_root_path=?1 OR integration_root_path=?1",
            [bootstrap.as_os_str().as_encoded_bytes()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bootstrap_references, 0);
    let pending: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM repository_provisioning_intents",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);
}

#[test]
#[cfg(debug_assertions)]
fn provisioning_resume_uses_only_the_durable_plan_after_reservation() {
    let fixture = CliFixture::new("main");
    let interrupted = interrupt_init(&fixture, "reservation");
    assert_eq!(interrupted.status.code(), Some(86));
    std::fs::rename(&fixture.bootstrap, fixture.root.join("renamed-bootstrap")).unwrap();

    let resumed = fixture.init("main");

    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );

    let fixture = CliFixture::new("main");
    let interrupted = interrupt_init(&fixture, "fetch");
    assert_eq!(interrupted.status.code(), Some(86));
    std::fs::rename(&fixture.bootstrap, fixture.root.join("deleted-bootstrap")).unwrap();
    std::fs::rename(&fixture.remote, fixture.root.join("unavailable-remote.git")).unwrap();

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
        .query_row("SELECT repo_key FROM repository_remote_owners", [], |row| {
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
            "--target",
            "main",
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
fn bootstrap_request_identity_survives_relative_dotdot_and_deleted_symlink_spellings() {
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
        let run = |stop: bool| {
            let mut command = Command::new(env!("CARGO_BIN_EXE_iq"));
            command
                .current_dir(&fixture.root)
                .env("IQ_RIFT_DATABASE", &fixture.rift_database)
                .arg("--queue-db")
                .arg(&fixture.database)
                .args(["repo", "init", "--path", spelling, "--target", "main"]);
            if stop {
                command.env("IQ_TEST_PROVISION_STOP_AFTER", "reservation");
            }
            command.output().unwrap()
        };
        assert_eq!(run(true).status.code(), Some(86));
        if symlink_spelling {
            std::fs::remove_file(fixture.root.join("bootstrap-link")).unwrap();
        }
        std::fs::rename(&fixture.bootstrap, fixture.root.join("deleted-bootstrap")).unwrap();

        let resumed = run(false);

        assert!(
            resumed.status.success(),
            "{}",
            String::from_utf8_lossy(&resumed.stderr)
        );
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
    assert!(fixture.init("main").status.success());

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
    assert!(fixture.init("main").status.success());

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
    assert!(fixture.init("main").status.success());
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
    for (boundary, expected_phase) in [
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
    ] {
        let fixture = CliFixture::new("main");
        let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
            .env("IQ_RIFT_DATABASE", &fixture.rift_database)
            .env("IQ_TEST_PROVISION_STOP_AFTER", boundary)
            .arg("--queue-db")
            .arg(&fixture.database)
            .args([
                "repo",
                "init",
                "--path",
                fixture.bootstrap.to_str().unwrap(),
                "--target",
                "main",
            ])
            .output()
            .unwrap();
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

        let retried = fixture.init("main");
        assert!(
            retried.status.success(),
            "boundary {boundary} retry failed: {}",
            String::from_utf8_lossy(&retried.stderr)
        );
        let repository: Value = serde_json::from_slice(&retried.stdout).unwrap();
        let repo_key = repository["key"].as_str().unwrap();
        let status = successful_json(fixture.iq(&["repo", "status", "--repo-key", repo_key]));
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
}

#[test]
#[cfg(debug_assertions)]
fn provisioning_recovers_when_each_effect_precedes_its_lifecycle_record() {
    for (boundary, prior_phase) in [
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
    ] {
        let fixture = CliFixture::new("main");
        let interrupted = interrupt_init_after_effect(&fixture, boundary);
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

        let resumed = fixture.init("main");
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
fn sql_rejects_contradictory_child_roots_and_cross_role_paths() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let connection = Connection::open(&fixture.database).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();

    assert!(connection
        .execute(
            "UPDATE workspace_roots SET source_rift_id='different' WHERE repo_key=?1 AND kind='development'",
            [repo_key],
        )
        .is_err());
    assert!(connection
        .execute(
            "UPDATE workspace_roots SET kind='integration' WHERE repo_key=?1 AND kind='development'",
            [repo_key],
        )
        .is_err());

    let transaction = connection.unchecked_transaction().unwrap();
    let other = "00000000-0000-4000-8000-000000000002";
    transaction
        .execute(
            "INSERT INTO repository_remote_owners(repo_key,fetch_url,push_url,target_branch,created_at) VALUES(?1,'other-fetch','other-push','main','now')",
            [other],
        )
        .unwrap();
    let cross_role = transaction.execute(
        "INSERT INTO registered_repositories(repo_key,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode,generation,remote_name,fetch_url,push_url,target_branch,source_sha,checkout_json,development_root_path,integration_root_path,provisioning_json,created_at,updated_at)
         SELECT ?1,development_root_path,root_rift_id,registry_identity,registry_device,registry_inode,0,'iq-target','other-fetch','other-push','main',source_sha,checkout_json,owned_root_path,integration_root_path,'{\"state\":\"ready\"}','now','now'
         FROM registered_repositories WHERE repo_key=?2",
        [other, repo_key],
    );
    assert!(cross_role.is_err());
    transaction.rollback().unwrap();

    assert!(connection
        .execute(
            "INSERT INTO repository_provisioning_intents(repo_key,bootstrap_path,owned_root_path,staging_root_path,rift_registry_path,target_branch,fetch_url,push_url,source_sha,policy_bytes,lifecycle_json,created_at,updated_at)
             SELECT repo_key,CAST('/tmp/bootstrap' AS BLOB),CAST('/tmp/root' AS BLOB),CAST('/tmp/staging' AS BLOB),CAST('/tmp/rift' AS BLOB),target_branch,fetch_url,push_url,source_sha,NULL,'{\"state\":\"reserved\"}','now','now' FROM registered_repositories WHERE repo_key=?1",
            [repo_key],
        )
        .is_err());

    let transaction = connection.unchecked_transaction().unwrap();
    let parent_without_children = "00000000-0000-4000-8000-000000000003";
    transaction
        .execute(
            "INSERT INTO repository_remote_owners(repo_key,fetch_url,push_url,target_branch,created_at) VALUES(?1,'third-fetch','third-push','main','now')",
            [parent_without_children],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO registered_repositories(repo_key,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode,generation,remote_name,fetch_url,push_url,target_branch,source_sha,checkout_json,development_root_path,integration_root_path,provisioning_json,created_at,updated_at)
             VALUES(?1,CAST('/tmp/third-root' AS BLOB),'third-rift',CAST('/tmp/third-registry' AS BLOB),1,2,0,'iq-target','third-fetch','third-push','main','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{\"state\":\"ready\",\"target_sha\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}',CAST('/tmp/third-development' AS BLOB),CAST('/tmp/third-integration' AS BLOB),'{\"state\":\"ready\"}','now','now')",
            [parent_without_children],
        )
        .unwrap();
    assert!(transaction.commit().is_err());

    let transaction = connection.unchecked_transaction().unwrap();
    let parent_with_one_child = "00000000-0000-4000-8000-000000000004";
    transaction
        .execute(
            "INSERT INTO repository_remote_owners(repo_key,fetch_url,push_url,target_branch,created_at) VALUES(?1,'fourth-fetch','fourth-push','main','now')",
            [parent_with_one_child],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO registered_repositories(repo_key,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode,generation,remote_name,fetch_url,push_url,target_branch,source_sha,checkout_json,development_root_path,integration_root_path,provisioning_json,created_at,updated_at)
             VALUES(?1,CAST('/tmp/fourth-root' AS BLOB),'fourth-rift',CAST('/tmp/fourth-registry' AS BLOB),1,2,0,'iq-target','fourth-fetch','fourth-push','main','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{\"state\":\"ready\",\"target_sha\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}',CAST('/tmp/fourth-development' AS BLOB),CAST('/tmp/fourth-integration' AS BLOB),'{\"state\":\"ready\"}','now','now')",
            [parent_with_one_child],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode,generation,pending_generation)
             VALUES(?1,'development',CAST('/tmp/fourth-development' AS BLOB),CAST('/tmp/fourth-root' AS BLOB),'fourth-rift',CAST('/tmp/fourth-registry' AS BLOB),1,2,0,NULL)",
            [parent_with_one_child],
        )
        .unwrap();
    assert!(transaction.commit().is_err());
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
    assert_eq!(
        format!("{rejected:#}"),
        "IQ local state is incompatible; remove it and reinitialize IQ"
    );
}

#[test]
fn registered_host_claim_rejects_without_creating_an_attempt() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let source_sha = git(&fixture.bootstrap, &["rev-parse", "main"]);
    let queue = SqliteQueue::open(&fixture.database).unwrap();
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            source_branch: "feature".into(),
            current_head_sha: source_sha,
            pr_url: None,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();

    assert!(queue.claim_next_ready(repo_key).is_err());
    let connection = Connection::open(&fixture.database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM integration_attempts", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
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
    assert_eq!(
        format!("{rejected:#}"),
        "IQ local state is incompatible; remove it and reinitialize IQ"
    );
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
            "--target",
            "main",
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
            "--target",
            "master",
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
            .query_row("SELECT COUNT(*) FROM repository_remote_owners", [], |row| {
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
        .query_row(
            "SELECT target_branch FROM repository_remote_owners",
            [],
            |row| row.get(0),
        )
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
                "--target",
                "main",
            ]);
        command.spawn().unwrap()
    };
    let first = spawn(&fixture.bootstrap);
    let second = spawn(&second_bootstrap);
    #[cfg(debug_assertions)]
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::fs::read_dir(&barrier).unwrap().count() != 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "reservation barrier timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
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
        ("repository_remote_owners", 1),
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
            "--target",
            "master",
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

    let second = successful_json(fixture.iq(&[
        "repo",
        "init",
        "--path",
        second_bootstrap.to_str().unwrap(),
        "--target",
        "main",
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
            "dev-workspace",
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
        "dev-workspace",
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
    assert_eq!(
        git(
            &owned_root,
            &[
                "rev-parse",
                &format!("refs/iq/repository-targets/{repo_key}/{observed_a}")
            ]
        ),
        observed_a
    );

    let workspace_b = successful_json(fixture.iq(&[
        "dev-workspace",
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
fn linked_bootstrap_request_resumes_active_intent_without_reopening_checkout() {
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
            "--target",
            "main",
        ])
        .output()
        .unwrap();
    assert_eq!(linked.status.code(), Some(85));
    std::fs::rename(
        &second_bootstrap,
        fixture.root.join("removed-second-bootstrap"),
    )
    .unwrap();

    let resumed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_RIFT_DATABASE", &fixture.rift_database)
        .arg("--queue-db")
        .arg(&fixture.database)
        .args([
            "repo",
            "init",
            "--path",
            second_bootstrap.to_str().unwrap(),
            "--target",
            "main",
        ])
        .output()
        .unwrap();

    let repository = successful_json(resumed);
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
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
        "dev-workspace",
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
        "dev-workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "lock-submit",
    ]));

    let cleanup_workspace = successful_json(fixture.iq(&[
        "dev-workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "lock-cleanup",
    ]));
    let cleanup_id = cleanup_workspace["id"].as_str().unwrap();
    let cleanup_path = PathBuf::from(cleanup_workspace["path"].as_str().unwrap());
    let holder = spawn_operation_lock_holder(&operation_lock, &fixture.root, "cleanup");
    let blocked = fixture.iq(&["cleanup", "--workspace", cleanup_id]);
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
    successful_json(fixture.iq(&["cleanup", "--workspace", cleanup_id]));
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
        "dev-workspace",
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
    "version": 1,
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
fn submit_replace_reuses_the_item_and_lands_the_replacement_commit() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let workspace = successful_json(fixture.iq(&[
        "dev-workspace",
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
    assert_eq!(replacement[1]["id"], item_id);
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM local_submissions WHERE queue_item_id=?1",
                [&item_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
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
    "version": 1,
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

    let old_attempt_id = replacement[1]["replacement"]["old_attempt_id"]
        .as_str()
        .unwrap();
    let old_integration = PathBuf::from(
        replacement[1]["replacement"]["old_workspace"]["path"]
            .as_str()
            .unwrap(),
    );
    let preserved = fixture.iq(&[
        "cleanup",
        "--repo-key",
        repo_key,
        "--system-config",
        valid_system.to_str().unwrap(),
    ]);
    assert!(!preserved.status.success());
    assert!(old_integration.exists());
    git(&old_integration, &["reset", "--hard"]);
    git(&old_integration, &["clean", "-ffd"]);
    successful_json(fixture.iq(&[
        "cleanup",
        "--repo-key",
        repo_key,
        "--system-config",
        valid_system.to_str().unwrap(),
    ]));
    assert!(!old_integration.exists());
    let pending_effort: (String, String, String, i64) = connection
        .query_row(
            "SELECT id,attempt_id,state,failed_cycles FROM integration_efforts WHERE item_id=?1",
            [&item_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(pending_effort.1, old_attempt_id);
    assert_eq!(pending_effort.2, "replacement_pending");
    assert_eq!(pending_effort.3, 0);
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
            [&item_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status.0, "integrated", "status={status:?} daemon={daemon}");
    let completed_effort: (String, String, String) = connection
        .query_row(
            "SELECT id,attempt_id,state FROM integration_efforts WHERE item_id=?1",
            [&item_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(completed_effort.0, pending_effort.0);
    assert_ne!(completed_effort.1, old_attempt_id);
    assert_eq!(completed_effort.2, "integrated");
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
                "dev-workspace",
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
            "dev-workspace",
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
        "dev-workspace",
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
        "dev-workspace",
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
fn cleanup_cli_supports_workspace_only_without_system_configuration() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let repo_key = repository["key"].as_str().unwrap();
    let workspace = successful_json(fixture.iq(&[
        "dev-workspace",
        "create",
        "--repo-key",
        repo_key,
        "--name",
        "cleanup-only",
    ]));
    let removed =
        successful_json(fixture.iq(&["cleanup", "--workspace", workspace["id"].as_str().unwrap()]));
    assert_eq!(removed["status"], "removed");
    assert!(!Path::new(workspace["path"].as_str().unwrap()).exists());
}

#[test]
fn cleanup_preserves_dirty_workspace_then_removes_clean_workspace_and_authority() {
    let fixture = CliFixture::new("main");
    let repository = successful_json(fixture.init("main"));
    let workspace = successful_json(fixture.iq(&[
        "dev-workspace",
        "create",
        "--repo-key",
        repository["key"].as_str().unwrap(),
        "--name",
        "dirty-cleanup",
    ]));
    let workspace_id = workspace["id"].as_str().unwrap();
    let workspace_path = PathBuf::from(workspace["path"].as_str().unwrap());
    std::fs::write(workspace_path.join("dirty.txt"), "preserve me\n").unwrap();

    let rejected = fixture.iq(&["cleanup", "--workspace", workspace_id]);

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
    successful_json(fixture.iq(&["cleanup", "--workspace", workspace_id]));

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
        "dev-workspace",
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
