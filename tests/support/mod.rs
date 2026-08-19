pub fn rift_executable_path() -> std::path::PathBuf {
    let path = std::env::var_os("PATH").expect("PATH is required for the test Rift executable");
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join("rift");
        if candidate.is_file() {
            return candidate
                .canonicalize()
                .expect("resolve test Rift executable");
        }
    }
    panic!("Rift executable is unavailable for integration tests");
}

pub fn initialize_rift_executable() {
    static INITIALIZE: std::sync::Once = std::sync::Once::new();
    INITIALIZE.call_once(|| {
        iq::agent_config::initialize_rift_executable_authority(&rift_executable_path()).unwrap();
    });
}

pub struct Command(std::process::Command);

impl Command {
    pub fn new(program: impl AsRef<std::ffi::OsStr>) -> Self {
        let executable = if program.as_ref() == std::ffi::OsStr::new("rift") {
            rift_executable_path().into_os_string()
        } else {
            program.as_ref().to_os_string()
        };
        let mut command = std::process::Command::new(executable);
        if program.as_ref() == std::ffi::OsStr::new(env!("CARGO_BIN_EXE_iq")) {
            command.arg("--rift-executable").arg(rift_executable_path());
        }
        Self(command)
    }
}

impl std::ops::Deref for Command {
    type Target = std::process::Command;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Command {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub fn managed_test_tempdir(prefix: &str) -> tempfile::TempDir {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};

    initialize_rift_executable();
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open("/tmp/iq-test-fixture-cleanup.lock")
        .unwrap();
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);
    let repository = std::fs::canonicalize(env!("CARGO_MANIFEST_DIR")).unwrap();
    let repository_digest = {
        use sha2::Digest;
        format!(
            "{:x}",
            sha2::Sha256::digest(repository.as_os_str().as_encoded_bytes())
        )
    };
    let root = repository
        .parent()
        .unwrap()
        .join(format!("iq-test-artifacts-{}", &repository_digest[..16]));
    let root_manifest_path = root.join(".iq-test-root.json");
    let root_id = if root.exists() {
        let metadata = std::fs::symlink_metadata(&root).unwrap();
        assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
        assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        let manifest_metadata = std::fs::symlink_metadata(&root_manifest_path).unwrap();
        assert!(
            manifest_metadata.is_file()
                && !manifest_metadata.file_type().is_symlink()
                && manifest_metadata.len() <= 4096
        );
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&root_manifest_path).unwrap()).unwrap();
        assert_eq!(manifest["version"], 1);
        assert_eq!(
            manifest["repository"],
            repository.to_string_lossy().as_ref()
        );
        let root_id = manifest["root_id"].as_str().unwrap();
        assert_eq!(uuid::Uuid::parse_str(root_id).unwrap().to_string(), root_id);
        root_id.to_string()
    } else {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let root_id = uuid::Uuid::new_v4().to_string();
        let mut manifest = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&root_manifest_path)
            .unwrap();
        serde_json::to_writer(
            &mut manifest,
            &serde_json::json!({
                "version": 1,
                "repository": repository,
                "root_id": root_id,
            }),
        )
        .unwrap();
        std::io::Write::write_all(&mut manifest, b"\n").unwrap();
        std::io::Write::flush(&mut manifest).unwrap();
        root_id
    };
    for entry in std::fs::read_dir(&root).unwrap() {
        let entry = entry.unwrap();
        if entry.path() == root_manifest_path {
            continue;
        }
        let Ok(metadata) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        let marker = entry.path().join(".iq-test-owner.json");
        let Ok(marker_metadata) = std::fs::symlink_metadata(&marker) else {
            continue;
        };
        if !marker_metadata.is_file()
            || marker_metadata.file_type().is_symlink()
            || marker_metadata.len() > 4096
        {
            continue;
        }
        let Ok(owner) = std::fs::read(&marker)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .ok_or(())
        else {
            continue;
        };
        if owner["version"] != 1 || owner["root_id"] != root_id {
            continue;
        }
        if test_fixture_owner_is_active(&owner) {
            continue;
        }
        let Ok(current) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        if (current.dev(), current.ino()) != (metadata.dev(), metadata.ino()) {
            continue;
        }
        if let Err(error) = std::fs::remove_dir_all(entry.path()) {
            assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        }
    }
    let temporary = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(&root)
        .unwrap();
    std::fs::write(
        temporary.path().join(".iq-test-owner.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "root_id": root_id,
            "pid": std::process::id(),
            "process_start_ticks": process_start_ticks(std::process::id()).unwrap(),
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_UN) }, 0);
    temporary
}

fn test_fixture_owner_is_active(owner: &serde_json::Value) -> bool {
    let Some(pid) = owner["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
    else {
        return false;
    };
    let Some(expected_start) = owner["process_start_ticks"].as_str() else {
        return false;
    };
    process_start_ticks(pid).as_deref() == Some(expected_start)
}

fn process_start_ticks(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(") ")?
        .1
        .split_whitespace()
        .nth(19)
        .map(str::to_string)
}

pub fn direct_policy(canonical: &std::path::Path) -> iq::repository_policy::RepositoryPolicy {
    use std::os::unix::fs::MetadataExt;
    let canonical = std::fs::canonicalize(canonical).unwrap();
    let metadata = std::fs::metadata(&canonical).unwrap();
    let object_format = iq::git_command::RepositoryBinding::capture(&canonical)
        .unwrap()
        .object_format;
    iq::repository_policy::RepositoryPolicy {
        operation_state: iq::repository_policy::OperationState::Enabled,
        canonical_repository: iq::repository_policy::GitRepository::LocalBare {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            object_format,
        },
        target_branch: "main".into(),
        integration_policy: iq::repository_policy::IntegrationPolicy::Direct,
        replication_policy: iq::repository_policy::ReplicationPolicy::None,
    }
    .validate()
    .unwrap()
}

#[allow(dead_code)]
pub struct RepositoryFixture {
    pub queue: iq::sqlite::SqliteQueue,
    pub manager: iq::composition::RepositoryManager,
    pub repository: iq::sqlite::RegisteredRepository,
    pub working: std::path::PathBuf,
    _storage: tempfile::TempDir,
}

#[allow(dead_code)]
impl RepositoryFixture {
    pub fn new(root: &std::path::Path, database: &std::path::Path) -> Self {
        let remote = root.join("canonical.git");
        let working = root.join("bootstrap");
        let storage = managed_test_tempdir(".iq-shared-fixture-test-");
        let rift_database = storage.path().join("rift.sqlite");
        run(
            root,
            &[
                "init",
                "--bare",
                "--initial-branch=main",
                remote.to_str().unwrap(),
            ],
        );
        run(
            root,
            &["init", "--initial-branch=main", working.to_str().unwrap()],
        );
        run(&working, &["config", "user.name", "IQ Test"]);
        run(&working, &["config", "user.email", "iq@example.test"]);
        run(&working, &["config", "commit.gpgsign", "false"]);
        std::fs::write(working.join("README.md"), "fixture\n").unwrap();
        run(&working, &["add", "README.md"]);
        run(&working, &["commit", "-m", "fixture"]);
        run(
            &working,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        run(&working, &["push", "-u", "origin", "main"]);
        let queue = iq::sqlite::SqliteQueue::open(database).unwrap();
        let manager = iq::composition::RepositoryManager::new(queue.clone());
        let options = iq::composition::RepositoryInitOptions {
            storage_root: storage.path().to_path_buf(),
            policy: direct_policy(&remote),
        }
        .preflight_for_test(&working, rift_database)
        .unwrap();
        let repository = manager.init_preflighted(options).unwrap();
        Self {
            queue,
            manager,
            repository,
            working,
            _storage: storage,
        }
    }

    pub fn create_branch(&self, branch: &str) -> String {
        run(&self.working, &["switch", "main"]);
        run(&self.working, &["switch", "-C", branch]);
        let marker = self.working.join("fixture-branch.txt");
        std::fs::write(&marker, format!("{branch}\n")).unwrap();
        run(&self.working, &["add", marker.to_str().unwrap()]);
        run(&self.working, &["commit", "-m", branch]);
        run(&self.working, &["push", "--force", "origin", branch]);
        output(&self.working, &["rev-parse", "HEAD"])
    }
}

fn run(path: &std::path::Path, arguments: &[&str]) {
    let mut command = std::process::Command::new("git");
    if arguments.first() == Some(&"init")
        && !arguments
            .iter()
            .any(|argument| argument.starts_with("--object-format="))
    {
        command
            .arg("init")
            .arg("--object-format=sha1")
            .args(&arguments[1..]);
    } else {
        command.args(arguments);
    }
    let result = command.current_dir(path).output().unwrap();
    assert!(
        result.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn output(path: &std::path::Path, arguments: &[&str]) -> String {
    let result = std::process::Command::new("git")
        .args(arguments)
        .current_dir(path)
        .output()
        .unwrap();
    assert!(result.status.success());
    String::from_utf8(result.stdout).unwrap().trim().to_string()
}
