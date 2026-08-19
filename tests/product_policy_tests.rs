use iq::composition::{RepositoryInitOptions, RepositoryManager};
use iq::git_object::GitObjectFormat;
use iq::repository_policy::{
    GitRepository, IntegrationPolicy, OperationState, Provider, ProviderRepository,
    ReplicationPolicy, RepositoryPolicy,
};
use iq::sqlite::SqliteQueue;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
mod support;
use std::sync::{Mutex, MutexGuard, OnceLock};
use support::Command;

fn environment_lock() -> MutexGuard<'static, ()> {
    support::initialize_rift_executable();
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn git(path: &Path, args: &[&str]) -> String {
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
    let output = command.current_dir(path).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn local_bare_identity(path: &Path) -> GitRepository {
    let path = std::fs::canonicalize(path).unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let object_format = iq::git_command::RepositoryBinding::capture(&path)
        .unwrap()
        .object_format;
    GitRepository::LocalBare {
        path,
        device: metadata.dev(),
        inode: metadata.ino(),
        object_format,
    }
}

fn assert_registration_has_no_effects(queue: &SqliteQueue, storage_root: &Path) {
    let connection = rusqlite::Connection::open(queue.path()).unwrap();
    for table in [
        "repository_bootstrap_requests",
        "repository_provisioning_intents",
        "repository_policies",
        "physical_repository_ownership",
        "registered_repositories",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "registration changed {table}");
    }
    assert!(storage_root.is_dir());
    assert!(storage_root.read_dir().unwrap().next().is_none());
}

struct Fixture {
    _environment: MutexGuard<'static, ()>,
    _temporary: tempfile::TempDir,
    queue: SqliteQueue,
    manager: RepositoryManager,
    repository_key: String,
    bootstrap: PathBuf,
    owned_root: PathBuf,
    canonical: PathBuf,
    replica: PathBuf,
}

impl Fixture {
    fn new(replication: bool) -> Self {
        let environment = environment_lock();
        let temporary = tempfile::Builder::new()
            .prefix(".iq-product-policy-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .unwrap();
        let root = temporary.path();
        let bootstrap_remote = root.join("bootstrap.git");
        let canonical = root.join("canonical.git");
        let replica = root.join("replica.git");
        for bare in [&bootstrap_remote, &canonical, &replica] {
            std::fs::create_dir(bare).unwrap();
            git(bare, &["init", "--bare"]);
        }
        let bootstrap = root.join("bootstrap");
        git(
            root,
            &[
                "clone",
                bootstrap_remote.to_str().unwrap(),
                bootstrap.to_str().unwrap(),
            ],
        );
        git(&bootstrap, &["config", "user.name", "IQ Test"]);
        git(&bootstrap, &["config", "user.email", "iq@example.test"]);
        git(&bootstrap, &["config", "commit.gpgsign", "false"]);
        std::fs::write(bootstrap.join("README.md"), "canonical\n").unwrap();
        git(&bootstrap, &["add", "README.md"]);
        git(&bootstrap, &["commit", "-m", "canonical"]);
        git(&bootstrap, &["branch", "-M", "main"]);
        git(&bootstrap, &["push", "origin", "main"]);
        git(
            &bootstrap,
            &["remote", "add", "canonical", canonical.to_str().unwrap()],
        );
        git(&bootstrap, &["push", "canonical", "main"]);

        std::fs::write(bootstrap.join("README.md"), "bootstrap-only\n").unwrap();
        git(&bootstrap, &["commit", "-am", "bootstrap only"]);
        git(&bootstrap, &["push", "origin", "main"]);

        let identity = |path: &Path| {
            let path = std::fs::canonicalize(path).unwrap();
            let metadata = std::fs::metadata(&path).unwrap();
            let object_format = iq::git_command::RepositoryBinding::capture(&path)
                .unwrap()
                .object_format;
            GitRepository::LocalBare {
                path,
                device: metadata.dev(),
                inode: metadata.ino(),
                object_format,
            }
        };
        let policy = RepositoryPolicy {
            operation_state: OperationState::Enabled,
            canonical_repository: identity(&canonical),
            target_branch: "main".into(),
            integration_policy: IntegrationPolicy::Direct,
            replication_policy: if replication {
                ReplicationPolicy::Replicate {
                    targets: vec![identity(&replica)],
                }
            } else {
                ReplicationPolicy::None
            },
        }
        .validate()
        .unwrap();
        let queue = SqliteQueue::open(&root.join("queues.db")).unwrap();
        std::env::set_var("IQ_RIFT_DATABASE", root.join("rift.sqlite"));
        let manager = RepositoryManager::new(queue.clone());
        let repository = manager
            .init(
                &bootstrap,
                RepositoryInitOptions {
                    storage_root: root.to_path_buf(),
                    policy,
                },
            )
            .unwrap();
        Self {
            _environment: environment,
            _temporary: temporary,
            queue,
            manager,
            repository_key: repository.key,
            bootstrap,
            owned_root: repository.owned_root_path,
            canonical,
            replica,
        }
    }
}

#[test]
fn canonical_policy_beats_stale_bootstrap_and_workspace_lifecycle_is_public() {
    let fixture = Fixture::new(false);
    let canonical_sha = git(&fixture.canonical, &["rev-parse", "main"]);
    let canonical_tree = git(&fixture.canonical, &["rev-parse", "main^{tree}"]);
    let repository = fixture.queue.repository(&fixture.repository_key).unwrap();
    assert_eq!(repository.source_sha, canonical_sha);
    assert_eq!(
        git(&fixture.owned_root, &["rev-parse", "HEAD"]),
        canonical_sha
    );
    assert_eq!(
        git(&fixture.owned_root, &["rev-parse", "HEAD^{tree}"]),
        canonical_tree
    );
    assert_eq!(
        std::fs::read_to_string(fixture.owned_root.join("README.md")).unwrap(),
        "canonical\n"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.bootstrap.join("README.md")).unwrap(),
        "bootstrap-only\n"
    );

    let workspace = fixture
        .manager
        .create_workspace(&fixture.repository_key, "lifecycle")
        .unwrap();
    assert_eq!(workspace.base_sha, canonical_sha);
    assert_eq!(git(&workspace.path, &["rev-parse", "HEAD"]), canonical_sha);
    assert_eq!(
        git(&workspace.path, &["rev-parse", "HEAD^{tree}"]),
        canonical_tree
    );
    assert_eq!(
        std::fs::read_to_string(workspace.path.join("README.md")).unwrap(),
        "canonical\n"
    );
    assert_eq!(
        fixture
            .manager
            .workspaces(Some(&fixture.repository_key))
            .unwrap()
            .len(),
        1
    );
    assert!(
        fixture
            .manager
            .workspace_status(&workspace.id)
            .unwrap()
            .exists
    );
    let removed = fixture.manager.remove_workspace(&workspace.id).unwrap();
    assert_eq!(removed.status.to_string(), "removed");
}

#[test]
fn draining_captures_workspace_and_disabled_repository_remains_readable() {
    let fixture = Fixture::new(false);
    let workspace = fixture
        .manager
        .create_workspace(&fixture.repository_key, "drain")
        .unwrap();
    git(&fixture.bootstrap, &["fetch", "canonical", "main"]);
    git(
        &fixture.bootstrap,
        &["switch", "-C", "agent/drain", "canonical/main"],
    );
    std::fs::write(fixture.bootstrap.join("drain.txt"), "captured\n").unwrap();
    git(&fixture.bootstrap, &["add", "drain.txt"]);
    git(&fixture.bootstrap, &["commit", "-m", "captured queue item"]);
    git(&fixture.bootstrap, &["push", "canonical", "agent/drain"]);
    let head = git(&fixture.bootstrap, &["rev-parse", "HEAD"]);
    let admitted = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            fixture.queue.path().to_str().unwrap(),
            "admit",
            "direct",
            "--repo-key",
            &fixture.repository_key,
            "--source",
            "agent/drain",
            "--head",
            &head,
        ])
        .output()
        .unwrap();
    assert!(
        admitted.status.success(),
        "{}",
        String::from_utf8_lossy(&admitted.stderr)
    );
    let captured_item: iq::sqlite::QueueItem = serde_json::from_slice(&admitted.stdout).unwrap();
    let drain = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            fixture.queue.path().to_str().unwrap(),
            "repo",
            "drain",
            "--repo-key",
            &fixture.repository_key,
        ])
        .output()
        .unwrap();
    assert!(
        drain.status.success(),
        "{}",
        String::from_utf8_lossy(&drain.stderr)
    );
    let draining: iq::sqlite::RegisteredRepository = serde_json::from_slice(&drain.stdout).unwrap();
    assert!(matches!(
        draining.policy.operation_state,
        OperationState::Draining { .. }
    ));
    let canonical_before = git(&fixture.canonical, &["show-ref"]);
    assert!(fixture
        .manager
        .create_workspace(&fixture.repository_key, "rejected")
        .is_err());
    let direct = fixture
        .manager
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: fixture.repository_key.clone(),
            source_branch: "agent/valid".into(),
            current_head_sha: "1111111111111111111111111111111111111111".into(),
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap_err();
    assert!(format!("{direct:#}").contains("draining"));
    let merge_request = fixture
        .manager
        .admit_merge_request(
            &fixture.repository_key,
            "https://github.com/acme/repo/pull/1",
            &serde_json::json!({}),
        )
        .unwrap_err();
    assert!(format!("{merge_request:#}").contains("draining"));
    assert_eq!(fixture.queue.list_items().unwrap().len(), 1);
    assert_eq!(git(&fixture.canonical, &["show-ref"]), canonical_before);
    assert_eq!(
        fixture
            .manager
            .cancel_item(&captured_item.id, "test")
            .unwrap()
            .status,
        iq::core::QueueStatus::Cancelled
    );
    fixture.manager.remove_workspace(&workspace.id).unwrap();
    let disable = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            fixture.queue.path().to_str().unwrap(),
            "repo",
            "disable",
            "--repo-key",
            &fixture.repository_key,
        ])
        .output()
        .unwrap();
    assert!(
        disable.status.success(),
        "{}",
        String::from_utf8_lossy(&disable.stderr)
    );
    let disabled: iq::sqlite::RegisteredRepository =
        serde_json::from_slice(&disable.stdout).unwrap();
    assert_eq!(disabled.policy.operation_state, OperationState::Disabled);
    assert!(fixture.manager.status(&fixture.repository_key).is_ok());
    assert_eq!(
        fixture
            .manager
            .cancel_item(&captured_item.id, "test")
            .unwrap()
            .status,
        iq::core::QueueStatus::Cancelled
    );
    assert_eq!(
        fixture
            .manager
            .remove_workspace(&workspace.id)
            .unwrap()
            .status,
        iq::sqlite::DevelopmentWorkspaceStatus::Removed
    );
    assert!(fixture
        .manager
        .create_workspace(&fixture.repository_key, "still-rejected")
        .is_err());
    let unavailable = fixture._temporary.path().join("canonical-unavailable.git");
    std::fs::rename(&fixture.canonical, &unavailable).unwrap();
    let cleanup = fixture.manager.cleanup_repo(&fixture.repository_key);
    assert!(cleanup.is_ok(), "{cleanup:?}");
    std::fs::rename(unavailable, &fixture.canonical).unwrap();
}

#[test]
fn replica_is_not_used_as_canonical_freshness_authority() {
    let fixture = Fixture::new(true);
    let canonical_sha = git(&fixture.canonical, &["rev-parse", "main"]);
    assert!(!Command::new("git")
        .args(["show-ref", "--verify", "refs/heads/main"])
        .current_dir(&fixture.replica)
        .output()
        .unwrap()
        .status
        .success());
    let workspace = fixture
        .manager
        .create_workspace(&fixture.repository_key, "canonical-source")
        .unwrap();
    assert_eq!(workspace.base_sha, canonical_sha);
    assert_eq!(git(&workspace.path, &["rev-parse", "HEAD"]), canonical_sha);
    assert_eq!(
        std::fs::read_to_string(workspace.path.join("README.md")).unwrap(),
        "canonical\n"
    );
}

#[test]
fn local_git_url_rewrite_is_rejected_before_external_or_database_effects() {
    let fixture = Fixture::new(true);
    let canonical_before = git(&fixture.canonical, &["rev-parse", "main"]);
    let replica_before = Command::new("git")
        .args(["show-ref", "--verify", "refs/heads/main"])
        .current_dir(&fixture.replica)
        .output()
        .unwrap();
    let key = format!("url.{}.insteadOf", fixture.replica.display());
    git(
        &fixture.owned_root,
        &["config", &key, fixture.canonical.to_str().unwrap()],
    );

    let error = fixture
        .manager
        .create_workspace(&fixture.repository_key, "rewrite-denied")
        .unwrap_err();

    assert!(
        format!("{error:#}").contains("repository-local Git configuration is not allowed: url."),
        "{error:#}"
    );
    assert!(fixture
        .manager
        .workspaces(Some(&fixture.repository_key))
        .unwrap()
        .is_empty());
    assert_eq!(
        git(&fixture.canonical, &["rev-parse", "main"]),
        canonical_before
    );
    let replica_after = Command::new("git")
        .args(["show-ref", "--verify", "refs/heads/main"])
        .current_dir(&fixture.replica)
        .output()
        .unwrap();
    assert_eq!(replica_after.status.code(), replica_before.status.code());
}

#[test]
fn global_git_url_rewrite_is_not_loaded_by_iq_git_operations() {
    let fixture = Fixture::new(true);
    let config = fixture._temporary.path().join("hostile-global-gitconfig");
    let key = format!("url.{}.insteadOf", fixture.replica.display());
    let configured = Command::new("/usr/bin/git")
        .args(["config", "--file"])
        .arg(&config)
        .args([&key, fixture.canonical.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(configured.success());
    let previous = std::env::var_os("GIT_CONFIG_GLOBAL");
    std::env::set_var("GIT_CONFIG_GLOBAL", &config);

    let workspace = fixture
        .manager
        .create_workspace(&fixture.repository_key, "global-rewrite-disabled")
        .unwrap();

    match previous {
        Some(value) => std::env::set_var("GIT_CONFIG_GLOBAL", value),
        None => std::env::remove_var("GIT_CONFIG_GLOBAL"),
    }
    assert_eq!(
        workspace.base_sha,
        git(&fixture.canonical, &["rev-parse", "main"])
    );
    assert!(!Command::new("/usr/bin/git")
        .args(["show-ref", "--verify", "refs/heads/main"])
        .current_dir(&fixture.replica)
        .output()
        .unwrap()
        .status
        .success());
}

#[test]
fn ambient_git_repository_and_transport_controls_do_not_change_iq_authority() {
    let fixture = Fixture::new(false);
    let repository = fixture.queue.repository(&fixture.repository_key).unwrap();
    let canonical_sha = git(&fixture.canonical, &["rev-parse", "main"]);
    let hostile = [
        ("GIT_DIR", fixture.replica.as_os_str()),
        ("GIT_WORK_TREE", fixture.bootstrap.as_os_str()),
        ("GIT_OBJECT_DIRECTORY", fixture.replica.as_os_str()),
        ("GIT_INDEX_FILE", fixture.replica.as_os_str()),
        ("GIT_NAMESPACE", std::ffi::OsStr::new("hostile")),
        (
            "GIT_REPLACE_REF_BASE",
            std::ffi::OsStr::new("refs/hostile/replace/"),
        ),
        ("GIT_ALLOW_PROTOCOL", std::ffi::OsStr::new("https")),
        ("GIT_SSH_COMMAND", std::ffi::OsStr::new("false")),
        ("GIT_ASKPASS", std::ffi::OsStr::new("false")),
        ("GIT_CONFIG_COUNT", std::ffi::OsStr::new("1")),
        ("GIT_CONFIG_KEY_0", std::ffi::OsStr::new("core.bare")),
        ("GIT_CONFIG_VALUE_0", std::ffi::OsStr::new("true")),
    ];
    let previous = hostile
        .iter()
        .map(|(key, _)| (*key, std::env::var_os(key)))
        .collect::<Vec<_>>();
    for (key, value) in hostile {
        std::env::set_var(key, value);
    }

    let status = fixture.manager.status(&fixture.repository_key);
    let verified = fixture
        .manager
        .verify_runtime_repository(&fixture.repository_key, &repository.integration_root_path);

    for (key, value) in previous {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
    assert_eq!(status.unwrap().owned_root_head, canonical_sha);
    assert!(verified.is_ok(), "{verified:?}");
}

#[test]
fn provider_ownership_key_ignores_transport_and_target_details() {
    let provider = ProviderRepository {
        provider: Provider::Github,
        host: "github.com".into(),
        repository: "acme/repository".into(),
        repository_id: "R_immutable".into(),
    };
    let https = GitRepository::Accessible {
        object_format: GitObjectFormat::Sha1,
        fetch_url: "https://github.com/acme/repository.git".into(),
        push_url: "https://github.com/acme/repository.git".into(),
        repository_id: "R_immutable".into(),
        provider: provider.clone(),
    }
    .validate("canonical")
    .unwrap();
    let ssh = GitRepository::Accessible {
        object_format: GitObjectFormat::Sha1,
        fetch_url: "git@github.com:acme/repository.git".into(),
        push_url: "ssh://git@github.com/acme/repository.git".into(),
        repository_id: "R_immutable".into(),
        provider,
    }
    .validate("canonical")
    .unwrap();

    assert_eq!(
        https.canonical_ownership_key().unwrap(),
        ssh.canonical_ownership_key().unwrap()
    );
}

#[test]
fn accessible_repository_requires_provider_verified_identity() {
    let value = serde_json::json!({
        "kind": "accessible",
        "fetch_url": "https://github.com/acme/repository.git",
        "push_url": "https://github.com/acme/repository.git",
        "repository_id": "operator-asserted"
    });

    assert!(serde_json::from_value::<GitRepository>(value).is_err());
}

#[test]
fn canonical_repository_cannot_claim_another_policy_replica() {
    let fixture = Fixture::new(true);
    let replica = std::fs::canonicalize(&fixture.replica).unwrap();
    let metadata = std::fs::metadata(&replica).unwrap();
    git(
        &fixture.bootstrap,
        &["push", replica.to_str().unwrap(), "main"],
    );
    let second_bootstrap = fixture._temporary.path().join("second-bootstrap");
    let bootstrap_origin = git(&fixture.bootstrap, &["remote", "get-url", "origin"]);
    git(
        fixture._temporary.path(),
        &[
            "clone",
            bootstrap_origin.as_str(),
            second_bootstrap.to_str().unwrap(),
        ],
    );
    let error = fixture
        .manager
        .init(
            &second_bootstrap,
            RepositoryInitOptions {
                storage_root: fixture._temporary.path().to_path_buf(),
                policy: RepositoryPolicy {
                    operation_state: OperationState::Enabled,
                    canonical_repository: GitRepository::LocalBare {
                        object_format: GitObjectFormat::Sha1,
                        path: replica,
                        device: metadata.dev(),
                        inode: metadata.ino(),
                    },
                    target_branch: "main".into(),
                    integration_policy: IntegrationPolicy::Direct,
                    replication_policy: ReplicationPolicy::None,
                },
            },
        )
        .unwrap_err();

    assert!(
        format!("{error:#}").contains("canonical repository is already reserved as a replica"),
        "{error:#}"
    );
}

#[test]
fn replica_destination_identity_rejects_duplicate_physical_streams_and_unsafe_transports() {
    let provider = ProviderRepository {
        provider: Provider::Github,
        host: "github.com".into(),
        repository: "acme/repository".into(),
        repository_id: "R_same".into(),
    };
    let canonical = GitRepository::Accessible {
        object_format: GitObjectFormat::Sha1,
        fetch_url: "https://github.com/acme/repository.git".into(),
        push_url: "https://github.com/acme/repository.git".into(),
        repository_id: "R_same".into(),
        provider: provider.clone(),
    };
    let same_provider_over_ssh = GitRepository::Accessible {
        object_format: GitObjectFormat::Sha1,
        fetch_url: "git@github.com:acme/repository.git".into(),
        push_url: "ssh://git@github.com/acme/repository.git".into(),
        repository_id: "R_same".into(),
        provider,
    };
    let canonical_duplicate = RepositoryPolicy {
        operation_state: OperationState::Enabled,
        canonical_repository: canonical,
        target_branch: "main".into(),
        integration_policy: IntegrationPolicy::Direct,
        replication_policy: ReplicationPolicy::Replicate {
            targets: vec![same_provider_over_ssh],
        },
    }
    .validate()
    .unwrap_err();
    assert!(canonical_duplicate
        .to_string()
        .contains("canonical repository cannot also be a replica"));

    let replica_provider = ProviderRepository {
        provider: Provider::Github,
        host: "github.com".into(),
        repository: "acme/replica".into(),
        repository_id: "R_replica".into(),
    };
    let generic = || GitRepository::Accessible {
        object_format: GitObjectFormat::Sha1,
        fetch_url: "https://github.com/acme/replica.git".into(),
        push_url: "ssh://git@github.com/acme/replica.git".into(),
        repository_id: "R_replica".into(),
        provider: replica_provider.clone(),
    };
    let duplicate_push = RepositoryPolicy {
        operation_state: OperationState::Enabled,
        canonical_repository: GitRepository::Accessible {
            object_format: GitObjectFormat::Sha1,
            fetch_url: "https://github.com/acme/canonical.git".into(),
            push_url: "ssh://git@github.com/acme/canonical.git".into(),
            repository_id: "R_canonical".into(),
            provider: ProviderRepository {
                provider: Provider::Github,
                host: "github.com".into(),
                repository: "acme/canonical".into(),
                repository_id: "R_canonical".into(),
            },
        },
        target_branch: "main".into(),
        integration_policy: IntegrationPolicy::Direct,
        replication_policy: ReplicationPolicy::Replicate {
            targets: vec![generic(), generic()],
        },
    }
    .validate()
    .unwrap_err();
    assert!(duplicate_push.to_string().contains("duplicate target"));

    for transport in [
        "git://git.example/acme/repository.git",
        "ext::sh -c exploit",
        "hg::https://git.example/acme/repository",
    ] {
        let error = GitRepository::Accessible {
            object_format: GitObjectFormat::Sha1,
            fetch_url: transport.into(),
            push_url: transport.into(),
            repository_id: "R_unsafe".into(),
            provider: ProviderRepository {
                provider: Provider::Github,
                host: "github.com".into(),
                repository: "acme/repository".into(),
                repository_id: "R_unsafe".into(),
            },
        }
        .validate("repository")
        .unwrap_err();
        assert!(error.to_string().contains("HTTPS, SSH, or SCP"));
    }

    for transport in [
        "https://github.com:8443/acme/repository.git",
        "ssh://git@github.com:2222/acme/repository.git",
    ] {
        let error = GitRepository::Accessible {
            object_format: GitObjectFormat::Sha1,
            fetch_url: transport.into(),
            push_url: transport.into(),
            repository_id: "R_port".into(),
            provider: ProviderRepository {
                provider: Provider::Github,
                host: "github.com".into(),
                repository: "acme/repository".into(),
                repository_id: "R_port".into(),
            },
        }
        .validate("repository")
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("non-default endpoint port"),
            "{error:#}"
        );
    }

    let contradictory = GitRepository::Accessible {
        object_format: GitObjectFormat::Sha1,
        fetch_url: "https://github.com/acme/repository.git".into(),
        push_url: "https://github.com/acme/repository.git".into(),
        repository_id: "outer".into(),
        provider: ProviderRepository {
            provider: Provider::Github,
            host: "github.com".into(),
            repository: "acme/repository".into(),
            repository_id: "provider".into(),
        },
    }
    .validate("repository")
    .unwrap_err();
    assert!(contradictory.to_string().contains("must equal"));
}

#[test]
fn sql_cannot_mutate_registered_repository_authority() {
    let fixture = Fixture::new(false);
    let connection = rusqlite::Connection::open(fixture.queue.path()).unwrap();
    connection
        .pragma_update(None, "recursive_triggers", "ON")
        .unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA recursive_triggers", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    for sql in [
        "UPDATE repository_policies SET canonical_ownership_key='other'",
        "UPDATE repository_policies SET canonical_repository_json='{}'",
        "UPDATE repository_policies SET target_branch='master'",
        "UPDATE repository_policies SET integration_policy='merge_request_required'",
        "UPDATE repository_policies SET replication_policy_json='{\"mode\":\"replicate\",\"targets\":[]}'",
        "UPDATE repository_policies SET revision=revision+1",
        "UPDATE repository_policies SET operation_state_json='{\"state\":\"disabled\"}',revision=revision+1",
    ] {
        assert!(connection.execute(sql, []).is_err(), "raw SQL succeeded: {sql}");
    }
    for sql in [
        "INSERT OR REPLACE INTO repository_policies SELECT * FROM repository_policies",
        "INSERT INTO repository_policies SELECT * FROM repository_policies ON CONFLICT(repo_key) DO UPDATE SET canonical_repository_json='{}'",
        "INSERT OR REPLACE INTO physical_repository_ownership SELECT * FROM physical_repository_ownership",
        "INSERT INTO physical_repository_ownership SELECT * FROM physical_repository_ownership ON CONFLICT(identity_key) DO UPDATE SET repository_json='{}'",
    ] {
        assert!(connection.execute(sql, []).is_err(), "raw SQL replaced authority: {sql}");
    }
    let draining = fixture
        .manager
        .begin_draining(&fixture.repository_key)
        .unwrap();
    assert!(matches!(
        draining.policy.operation_state,
        OperationState::Draining { .. }
    ));
    let disabled = fixture
        .manager
        .disable_drained(&fixture.repository_key)
        .unwrap();
    assert_eq!(disabled.policy.operation_state, OperationState::Disabled);
}

#[test]
fn sql_cannot_delete_or_replace_queue_admission_identity() {
    let fixture = Fixture::new(false);
    git(
        &fixture.bootstrap,
        &["push", "canonical", "HEAD:refs/heads/agent/sql-authority"],
    );
    let source_head = git(&fixture.bootstrap, &["rev-parse", "HEAD"]);
    let item = fixture
        .manager
        .admit_direct(iq::sqlite::DirectAdmissionRequest {
            repo_key: fixture.repository_key.clone(),
            source_branch: "agent/sql-authority".into(),
            current_head_sha: source_head,
            producer_metadata: serde_json::json!({}),
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        })
        .unwrap();
    let connection = rusqlite::Connection::open(fixture.queue.path()).unwrap();
    connection
        .pragma_update(None, "recursive_triggers", "ON")
        .unwrap();
    assert!(connection
        .execute(
            "INSERT INTO queue_item_purge_authority(item_id,authorized_at) VALUES(?1,'2026-01-01T00:00:00Z')",
            [&item.id],
        )
        .is_err());

    for sql in [
        format!("DELETE FROM queue_admissions WHERE item_id='{}'", item.id),
        format!("DELETE FROM queue_items WHERE id='{}'", item.id),
        format!(
            "INSERT OR REPLACE INTO queue_admissions SELECT * FROM queue_admissions WHERE item_id='{}'",
            item.id
        ),
        format!(
            "INSERT INTO queue_admissions SELECT * FROM queue_admissions WHERE item_id='{}' ON CONFLICT(item_id) DO UPDATE SET head_sha='0000000000000000000000000000000000000000'",
            item.id
        ),
    ] {
        assert!(
            connection.execute(&sql, []).is_err(),
            "raw SQL changed queue admission authority: {sql}"
        );
    }

    fixture.manager.cancel_item(&item.id, "test").unwrap();
    fixture.queue.purge_terminal_item(&item.id).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM queue_items WHERE id=?1",
                [&item.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn accessible_https_transport_rejects_embedded_credentials() {
    for transport in [
        "https://token@github.com/acme/repository.git",
        "https://user:secret@github.com/acme/repository.git",
    ] {
        let error = GitRepository::Accessible {
            object_format: GitObjectFormat::Sha1,
            fetch_url: transport.into(),
            push_url: transport.into(),
            repository_id: "R_credentials".into(),
            provider: ProviderRepository {
                provider: Provider::Github,
                host: "github.com".into(),
                repository: "acme/repository".into(),
                repository_id: "R_credentials".into(),
            },
        }
        .validate("repository")
        .unwrap_err();
        assert!(format!("{error:#}").contains("credentials"), "{error:#}");
    }
}

#[test]
fn draining_repository_rejects_workspace_submission_without_creating_intent_or_item() {
    let fixture = Fixture::new(false);
    let workspace = fixture
        .manager
        .create_workspace(&fixture.repository_key, "draining-submit")
        .unwrap();
    git(&workspace.path, &["config", "user.name", "IQ Test"]);
    git(
        &workspace.path,
        &["config", "user.email", "iq@example.test"],
    );
    git(&workspace.path, &["config", "commit.gpgsign", "false"]);
    std::fs::write(workspace.path.join("change.txt"), "change\n").unwrap();
    git(&workspace.path, &["add", "change.txt"]);
    git(&workspace.path, &["commit", "-m", "change"]);
    fixture
        .manager
        .begin_draining(&fixture.repository_key)
        .unwrap();

    let error = fixture.manager.submit(&workspace.id, None).unwrap_err();

    assert!(error.to_string().contains("repository is draining"));
    let connection = rusqlite::Connection::open(fixture.queue.path()).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM local_submissions", [], |row| row
                .get::<_, i64>(0))
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
}

#[test]
#[cfg(debug_assertions)]
fn existing_workspace_creation_intent_finishes_before_canonical_refresh() {
    let fixture = Fixture::new(false);
    let original = git(&fixture.canonical, &["rev-parse", "main"]);
    let interrupted = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_TEST_WORKSPACE_CREATION_STOP_AFTER", "rift_created")
        .args([
            "--queue-db",
            fixture.queue.path().to_str().unwrap(),
            "workspace",
            "create",
            "--repo-key",
            &fixture.repository_key,
            "--name",
            "crash-recovery",
        ])
        .status()
        .unwrap();
    assert_eq!(interrupted.code(), Some(85));
    let intent = fixture
        .manager
        .workspaces(Some(&fixture.repository_key))
        .unwrap()
        .remove(0);
    assert_eq!(intent.status.to_string(), "creating");
    assert!(intent.identity.is_none());
    assert!(intent.path.is_dir());
    assert_eq!(intent.base_sha, original);

    let mover = fixture._temporary.path().join("canonical-mover");
    git(
        fixture._temporary.path(),
        &[
            "clone",
            fixture.canonical.to_str().unwrap(),
            mover.to_str().unwrap(),
        ],
    );
    git(&mover, &["config", "user.name", "IQ Test"]);
    git(&mover, &["config", "user.email", "iq@example.test"]);
    git(&mover, &["config", "commit.gpgsign", "false"]);
    git(&mover, &["switch", "main"]);
    std::fs::write(mover.join("moved.txt"), "moved\n").unwrap();
    git(&mover, &["add", "moved.txt"]);
    git(&mover, &["commit", "-m", "move canonical"]);
    git(&mover, &["push", "origin", "main"]);
    let moved = git(&fixture.canonical, &["rev-parse", "main"]);

    let resumed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            fixture.queue.path().to_str().unwrap(),
            "workspace",
            "create",
            "--repo-key",
            &fixture.repository_key,
            "--name",
            "crash-recovery",
        ])
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let resumed: iq::sqlite::DevelopmentWorkspace =
        serde_json::from_slice(&resumed.stdout).unwrap();
    assert_eq!(resumed.id, intent.id);
    assert_eq!(resumed.base_sha, original);
    assert_eq!(git(&fixture.owned_root, &["rev-parse", "HEAD"]), original);
    assert_ne!(original, moved);
}

#[test]
fn registration_rejects_non_enabled_policy_before_database_or_filesystem_mutation() {
    let environment = environment_lock();
    let temporary = tempfile::Builder::new()
        .prefix(".iq-registration-state-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let root = temporary.path();
    let canonical = root.join("canonical.git");
    let bootstrap = root.join("bootstrap");
    std::fs::create_dir(&canonical).unwrap();
    git(&canonical, &["init", "--bare"]);
    git(
        root,
        &[
            "clone",
            canonical.to_str().unwrap(),
            bootstrap.to_str().unwrap(),
        ],
    );
    let canonical = canonical.canonicalize().unwrap();
    let metadata = canonical.metadata().unwrap();
    let database = root.join("queues.db");
    let queue = SqliteQueue::open(&database).unwrap();
    let before = std::fs::read(&database).unwrap();
    for state in [
        OperationState::Draining {
            obligations: Default::default(),
        },
        OperationState::Disabled,
    ] {
        let error = RepositoryManager::new(queue.clone())
            .init(
                &bootstrap,
                RepositoryInitOptions {
                    storage_root: root.join("storage"),
                    policy: RepositoryPolicy {
                        operation_state: state,
                        canonical_repository: GitRepository::LocalBare {
                            object_format: GitObjectFormat::Sha1,
                            path: canonical.clone(),
                            device: metadata.dev(),
                            inode: metadata.ino(),
                        },
                        target_branch: "main".into(),
                        integration_policy: IntegrationPolicy::Direct,
                        replication_policy: ReplicationPolicy::None,
                    },
                },
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("requires enabled operation state"));
        assert!(!root.join("storage").exists());
        assert!(queue.list_repositories().unwrap().is_empty());
        assert_eq!(std::fs::read(&database).unwrap(), before);
    }
    drop(environment);
}

#[test]
fn registration_verifies_every_replica_before_any_mutation() {
    let _environment = environment_lock();
    let temporary = tempfile::Builder::new()
        .prefix(".iq-registration-replica-preflight-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let root = temporary.path();
    let bootstrap = root.join("bootstrap");
    std::fs::create_dir(&bootstrap).unwrap();
    git(&bootstrap, &["init"]);
    let canonical = root.join("canonical.git");
    let first_replica = root.join("first-replica.git");
    let failing_replica = root.join("failing-replica.git");
    for path in [&canonical, &first_replica, &failing_replica] {
        std::fs::create_dir(path).unwrap();
        git(path, &["init", "--bare"]);
    }
    let failing_path = failing_replica.canonicalize().unwrap();
    let failing_metadata = failing_path.metadata().unwrap();
    let storage_root = root.join("storage");
    std::fs::create_dir(&storage_root).unwrap();
    let queue = SqliteQueue::open(&root.join("queues.db")).unwrap();

    let error = RepositoryManager::new(queue.clone())
        .init(
            &bootstrap,
            RepositoryInitOptions {
                storage_root: storage_root.clone(),
                policy: RepositoryPolicy {
                    operation_state: OperationState::Enabled,
                    canonical_repository: local_bare_identity(&canonical),
                    target_branch: "main".into(),
                    integration_policy: IntegrationPolicy::Direct,
                    replication_policy: ReplicationPolicy::Replicate {
                        targets: vec![
                            local_bare_identity(&first_replica),
                            GitRepository::LocalBare {
                                object_format: GitObjectFormat::Sha1,
                                path: failing_path,
                                device: failing_metadata.dev(),
                                inode: failing_metadata.ino().checked_add(1).unwrap(),
                            },
                        ],
                    },
                },
            },
        )
        .unwrap_err();

    assert!(
        format!("{error:#}").contains("device/inode identity changed"),
        "{error:#}"
    );
    assert_registration_has_no_effects(&queue, &storage_root);
}

#[test]
fn registration_rejects_local_device_inode_and_git_identity_mismatches_without_mutation() {
    let _environment = environment_lock();
    let temporary = tempfile::Builder::new()
        .prefix(".iq-registration-local-preflight-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let root = temporary.path();
    let bootstrap = root.join("bootstrap");
    std::fs::create_dir(&bootstrap).unwrap();
    git(&bootstrap, &["init"]);
    let bare = root.join("canonical.git");
    std::fs::create_dir(&bare).unwrap();
    git(&bare, &["init", "--bare"]);
    let ordinary = root.join("ordinary");
    std::fs::create_dir(&ordinary).unwrap();
    git(&ordinary, &["init"]);
    let bare_path = bare.canonicalize().unwrap();
    let bare_metadata = bare_path.metadata().unwrap();
    let storage_root = root.join("storage");
    std::fs::create_dir(&storage_root).unwrap();
    let queue = SqliteQueue::open(&root.join("queues.db")).unwrap();
    let manager = RepositoryManager::new(queue.clone());

    for (repository, expected) in [
        (
            GitRepository::LocalBare {
                object_format: GitObjectFormat::Sha1,
                path: bare_path.clone(),
                device: bare_metadata.dev(),
                inode: bare_metadata.ino().checked_add(1).unwrap(),
            },
            "device/inode identity changed",
        ),
        (
            GitRepository::LocalBare {
                object_format: GitObjectFormat::Sha256,
                path: bare_path,
                device: bare_metadata.dev(),
                inode: bare_metadata.ino(),
            },
            "object format differs from policy",
        ),
        (
            local_bare_identity(&ordinary),
            "is not a bare Git repository",
        ),
    ] {
        let error = manager
            .init(
                &bootstrap,
                RepositoryInitOptions {
                    storage_root: storage_root.clone(),
                    policy: RepositoryPolicy {
                        operation_state: OperationState::Enabled,
                        canonical_repository: repository,
                        target_branch: "main".into(),
                        integration_policy: IntegrationPolicy::Direct,
                        replication_policy: ReplicationPolicy::None,
                    },
                },
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains(expected), "{error:#}");
        assert_registration_has_no_effects(&queue, &storage_root);
    }
}

#[test]
fn registration_rejects_provider_immutable_identity_mismatch_without_mutation() {
    let _environment = environment_lock();
    let temporary = tempfile::Builder::new()
        .prefix(".iq-registration-provider-preflight-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let root = temporary.path();
    let bootstrap = root.join("bootstrap");
    std::fs::create_dir(&bootstrap).unwrap();
    git(&bootstrap, &["init"]);
    let provider_cli = root.join("gh");
    std::fs::write(
        &provider_cli,
        "#!/bin/sh\nprintf '%s' '{\"node_id\":\"R_observed\",\"full_name\":\"acme/canonical\"}'\n",
    )
    .unwrap();
    std::fs::set_permissions(&provider_cli, std::fs::Permissions::from_mode(0o755)).unwrap();
    let provider_executable =
        iq::providers::inject_test_provider_executable(Provider::Github, &provider_cli).unwrap();
    let storage_root = root.join("storage");
    std::fs::create_dir(&storage_root).unwrap();
    let queue = SqliteQueue::open(&root.join("queues.db")).unwrap();

    let policy = RepositoryPolicy {
        operation_state: OperationState::Enabled,
        canonical_repository: GitRepository::Accessible {
            object_format: GitObjectFormat::Sha1,
            fetch_url: "https://github.com/acme/canonical.git".into(),
            push_url: "git@github.com:acme/canonical.git".into(),
            repository_id: "R_expected".into(),
            provider: ProviderRepository {
                provider: Provider::Github,
                host: "github.com".into(),
                repository: "acme/canonical".into(),
                repository_id: "R_expected".into(),
            },
        },
        target_branch: "main".into(),
        integration_policy: IntegrationPolicy::Direct,
        replication_policy: ReplicationPolicy::None,
    };
    let register = |policy| {
        RepositoryManager::new(queue.clone())
            .init(
                &bootstrap,
                RepositoryInitOptions {
                    storage_root: storage_root.clone(),
                    policy,
                },
            )
            .unwrap_err()
    };
    let error = register(policy.clone());

    assert!(
        format!("{error:#}").contains("GitHub repository identity differs from policy"),
        "{error:#}"
    );
    assert_registration_has_no_effects(&queue, &storage_root);

    drop(provider_executable);
    std::fs::write(
        &provider_cli,
        "#!/bin/sh\nprintf '%s' '{\"node_id\":\"R_expected\",\"full_name\":\"acme/canonical\"}'\n",
    )
    .unwrap();
    let _provider_executable =
        iq::providers::inject_test_provider_executable(Provider::Github, &provider_cli).unwrap();
    let unsupported = register(policy);
    assert!(
        format!("{unsupported:#}").contains("Git object format before effect"),
        "{unsupported:#}"
    );
    assert_registration_has_no_effects(&queue, &storage_root);
}
