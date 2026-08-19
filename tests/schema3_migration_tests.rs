use iq::control_domain::{
    BlockedEffort, ExecutableIdentity, InfrastructureBlocker, InfrastructureCause,
    InfrastructureComponent, IntegrationBlocker, IntegrationEffortState, LandingUncertain,
    LegacyRunnerScopeAuthority, ProviderGateKind, ProviderGateStatus, ProviderSignoffBlocker,
    ResumeState, RunnerBounds, RunnerKind, RunnerSnapshot, SandboxIdentity,
};
use iq::git_object::GitObjectFormat;
use iq::repository_policy::{
    ActiveItemDisposition, GitRepository, IncompatibleItemDisposition, IntegrationPolicy,
    InterruptedProvisioningDisposition, MigrationDevelopmentWorkspace, MigrationRepositoryState,
    MigrationWorkspaceIdentity, OperationState, PolicyAssignment, PolicyInventory, Provider,
    ProviderMergeMethod, ProviderRepository, ReplicationPolicy, RepositoryPolicy,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::time::{Duration, Instant};
use tempfile::tempdir;
mod support;
use support::Command;

const REPO_KEY: &str = "00000000-0000-4000-8000-000000000001";
const PROVISIONING_REPO_KEY: &str = "00000000-0000-4000-8000-000000000002";
const ACTIVE_MR_ID: &str = "00000000-0000-4000-8000-000000000007";
const SHA1: &str = "1111111111111111111111111111111111111111";
const SHA2: &str = "2222222222222222222222222222222222222222";

fn legacy_executable(path: &str) -> ExecutableIdentity {
    ExecutableIdentity {
        path: path.into(),
        device: 1,
        inode: 1,
        sha256: "a".repeat(64),
    }
}

struct MigrationTestScope {
    wrapper: Child,
    unit_name: String,
    control_group: String,
    pid: u32,
    process_start_ticks: u64,
}

impl MigrationTestScope {
    #[allow(clippy::zombie_processes)]
    fn start(cycle_id: &str) -> Self {
        let unit_name = format!("iq-agent-{cycle_id}.scope");
        iq::control_domain::validate_legacy_systemd_scope_name(cycle_id, &unit_name).unwrap();
        let mut wrapper = Command::new("/usr/bin/systemd-run")
            .args([
                "--user",
                "--scope",
                "--quiet",
                "--collect",
                &format!("--unit={unit_name}"),
                "--",
                "/bin/sleep",
                "3600",
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
                    "--property=ActiveState,ControlGroup",
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
                let members = fs::read_to_string(
                    Path::new("/sys/fs/cgroup")
                        .join(control_group.trim_start_matches('/'))
                        .join("cgroup.procs"),
                )
                .unwrap();
                if let Some(pid) = members.lines().find_map(|line| line.parse::<u32>().ok()) {
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
                panic!("systemd scope did not become active");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for MigrationTestScope {
    fn drop(&mut self) {
        let _ = Command::new("/usr/bin/systemctl")
            .args(["--user", "stop", &self.unit_name])
            .status();
        let _ = self.wrapper.wait();
    }
}

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/schema3-2a69e24.db")
}

fn copy_fixture(database: &Path) {
    copy_named_fixture(database, &fixture());
}

fn fixture_owned_root(parent: &Path) -> PathBuf {
    parent.join("repositories").join(REPO_KEY).join("root")
}

fn rewrite_fixture_with_disabled_triggers(
    database: &Path,
    trigger_names: &[&str],
    rewrite: impl FnOnce(&Connection),
) {
    let connection = Connection::open(database).unwrap();
    let triggers = trigger_names
        .iter()
        .map(|name| {
            let sql = connection
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?1",
                    [name],
                    |row| row.get::<_, String>(0),
                )
                .unwrap();
            ((*name).to_owned(), sql)
        })
        .collect::<Vec<_>>();
    connection
        .execute_batch("PRAGMA writable_schema=ON")
        .unwrap();
    for (name, _) in &triggers {
        connection
            .execute(
                "UPDATE sqlite_schema SET sql=replace(sql,'RAISE(ABORT,','printf(') WHERE type='trigger' AND name=?1",
                [name],
            )
            .unwrap();
    }
    drop(connection);

    let connection = Connection::open(database).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .unwrap();
    rewrite(&connection);
    connection
        .execute_batch("PRAGMA writable_schema=ON")
        .unwrap();
    for (name, sql) in triggers {
        connection
            .execute(
                "UPDATE sqlite_schema SET sql=?1 WHERE type='trigger' AND name=?2",
                rusqlite::params![sql, name],
            )
            .unwrap();
    }
}

fn copy_named_fixture(database: &Path, source: &Path) {
    fs::copy(source, database).unwrap();
    fs::copy(
        format!("{}.control.lock", source.display()),
        format!("{}.control.lock", database.display()),
    )
    .unwrap();
    let owned_root = fixture_owned_root(database.parent().unwrap());
    let reservation = owned_root.parent().unwrap();
    let development = reservation.join("development");
    let integration = reservation.join("integration");
    fs::create_dir_all(&owned_root).unwrap();
    fs::create_dir_all(&development).unwrap();
    fs::create_dir_all(&integration).unwrap();
    if !owned_root.join(".git").is_dir() {
        assert!(Command::new("/usr/bin/git")
            .args(["init", "--object-format=sha1", owned_root.to_str().unwrap()])
            .status()
            .unwrap()
            .success());
        assert!(Command::new("/usr/bin/git")
            .current_dir(&owned_root)
            .args([
                "-c",
                "user.name=IQ Test",
                "-c",
                "user.email=iq@example.test",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--allow-empty",
                "-m",
                "fixture root",
            ])
            .status()
            .unwrap()
            .success());
    }
    let root_head = String::from_utf8(
        Command::new("/usr/bin/git")
            .current_dir(&owned_root)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let root_head = root_head.trim();
    rewrite_fixture_with_disabled_triggers(
        database,
        &[
            "registered_repository_identity_immutable",
            "workspace_root_exact_identity_update",
        ],
        |connection| {
            connection
                .execute(
                    "UPDATE registered_repositories SET owned_root_path=?1,development_root_path=?2,integration_root_path=?3,source_sha=?4,checkout_json=json_object('state','ready','target_sha',?4)",
                    rusqlite::params![owned_root.as_os_str().as_encoded_bytes(),development.as_os_str().as_encoded_bytes(),integration.as_os_str().as_encoded_bytes(),root_head],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE workspace_roots SET source_path=?1,root_path=CASE kind WHEN 'development' THEN ?2 ELSE ?3 END",
                    rusqlite::params![owned_root.as_os_str().as_encoded_bytes(),development.as_os_str().as_encoded_bytes(),integration.as_os_str().as_encoded_bytes()],
                )
                .unwrap();
        },
    );
}

#[test]
fn schema3_absolute_local_bare_transport_migrates_and_opens_for_first_cli_operation() {
    let temporary = tempdir().unwrap();
    let canonical = temporary.path().join("local-bare.git");
    let initialized = Command::new("/usr/bin/git")
        .args([
            "init",
            "--bare",
            "--object-format=sha1",
            canonical.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(initialized.status.success());
    let metadata = fs::metadata(&canonical).unwrap();
    let database = temporary.path().join("local-queues.db");
    let source = fixture();
    copy_named_fixture(&database, &source);
    rewrite_fixture_with_disabled_triggers(
        &database,
        &[
            "repository_remote_owner_identity_immutable",
            "registered_repository_identity_immutable",
        ],
        |connection| {
            connection
                .execute(
                    "UPDATE repository_remote_owners SET fetch_url=?1,push_url=?1",
                    [canonical.to_str().unwrap()],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE registered_repositories SET fetch_url=?1,push_url=?1",
                    [canonical.to_str().unwrap()],
                )
                .unwrap();
        },
    );
    let mut inventory = inventory(true);
    inventory.repositories[0].policy.canonical_repository = GitRepository::LocalBare {
        object_format: GitObjectFormat::Sha1,
        path: canonical.clone(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    inventory.repositories[0].policy.integration_policy = IntegrationPolicy::Direct;
    inventory.repositories[0].item_dispositions[1].disposition =
        IncompatibleItemDisposition::Cancel;
    inventory.repositories[0].item_dispositions[1].provider_repository = Some(ProviderRepository {
        provider: Provider::Github,
        host: "github.com".into(),
        repository: "acme/legacy".into(),
        repository_id: "legacy-repository-id".into(),
    });
    let inventory_path = temporary.path().join("local-inventory.json");
    write_inventory(&inventory_path, &inventory);

    let migrated = run_migration(&database, &inventory_path);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let first_operation = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args(["--queue-db", database.to_str().unwrap(), "repo", "list"])
        .output()
        .unwrap();
    assert!(
        first_operation.status.success(),
        "{}",
        String::from_utf8_lossy(&first_operation.stderr)
    );
    let repositories: Value = serde_json::from_slice(&first_operation.stdout).unwrap();
    assert_eq!(
        repositories[0]["policy"]["canonical_repository"]["path"],
        canonical.to_str().unwrap()
    );
    assert_eq!(
        text(
            &Connection::open(&database).unwrap(),
            "SELECT status FROM queue_items WHERE id='direct-active'"
        ),
        "cancelled"
    );
}

fn inventory(include_mr_base: bool) -> PolicyInventory {
    PolicyInventory {
        version: 3,
        repositories: vec![PolicyAssignment {
            repo_key: REPO_KEY.into(),
            repository: MigrationRepositoryState::Ready { git_binding: None },
            development_workspaces: Vec::new(),
            policy: RepositoryPolicy {
                operation_state: OperationState::Enabled,
                canonical_repository: GitRepository::Accessible {
                    object_format: GitObjectFormat::Sha1,
                    fetch_url: "https://github.com/acme/legacy.git".into(),
                    push_url: "https://github.com/acme/legacy.git".into(),
                    repository_id: "legacy-repository-id".into(),
                    provider: ProviderRepository {
                        provider: Provider::Github,
                        host: "github.com".into(),
                        repository: "acme/legacy".into(),
                        repository_id: "legacy-repository-id".into(),
                    },
                },
                target_branch: "main".into(),
                integration_policy: IntegrationPolicy::MergeRequestRequired,
                replication_policy: ReplicationPolicy::None,
            },
            item_dispositions: vec![
                ActiveItemDisposition {
                    item_id: "direct-active".into(),
                    disposition: IncompatibleItemDisposition::Cancel,
                    admitted_base_sha: None,
                    provider_repository: None,
                    provider_merge_method: None,
                    workspace_identity: Some(MigrationWorkspaceIdentity {
                        path: "/legacy/iq/integration/direct-active".into(),
                        rift_id: "legacy-effort-rift".into(),
                        source_rift_id: "legacy-owned-root-rift".into(),
                        git_binding: None,
                    }),
                    runner_snapshot: Some(RunnerSnapshot {
                        kind: RunnerKind::Opencode,
                        executable: ExecutableIdentity {
                            path: "/legacy/bin/opencode".into(),
                            device: 1,
                            inode: 1,
                            sha256:
                                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                    .into(),
                        },
                        agent: "legacy-agent".into(),
                        model: "legacy-model".into(),
                        cycle_timeout_seconds: 1,
                        bounds: RunnerBounds {
                            max_log_bytes: 1,
                            max_result_bytes: 1,
                            max_processes: 1,
                            memory_bytes: 1,
                            cpu_seconds: 1,
                            writable_bytes: 1,
                            open_files: 1,
                        },
                        sandbox: SandboxIdentity {
                            implementation: "legacy".into(),
                            bubblewrap: legacy_executable("/legacy/bin/bwrap"),
                            unshare: legacy_executable("/legacy/bin/unshare"),
                            systemd_run: legacy_executable("/legacy/bin/systemd-run"),
                            systemctl: legacy_executable("/legacy/bin/systemctl"),
                        },
                        credential_env: "LEGACY_CREDENTIAL".into(),
                    }),
                    runner_termination_authority: None,
                },
                ActiveItemDisposition {
                    item_id: ACTIVE_MR_ID.into(),
                    disposition: IncompatibleItemDisposition::Continue,
                    admitted_base_sha: include_mr_base.then(|| SHA1.into()),
                    provider_repository: None,
                    provider_merge_method: None,
                    workspace_identity: None,
                    runner_snapshot: None,
                    runner_termination_authority: None,
                },
                ActiveItemDisposition {
                    item_id: "mr-terminal".into(),
                    disposition: IncompatibleItemDisposition::Continue,
                    admitted_base_sha: Some(SHA1.into()),
                    provider_repository: Some(ProviderRepository {
                        provider: Provider::Github,
                        host: "github.com".into(),
                        repository: "acme/legacy".into(),
                        repository_id: "historical-repository-id".into(),
                    }),
                    provider_merge_method: None,
                    workspace_identity: None,
                    runner_snapshot: None,
                    runner_termination_authority: None,
                },
            ],
        }],
    }
}

fn provisioning_policy() -> RepositoryPolicy {
    RepositoryPolicy {
        operation_state: OperationState::Enabled,
        canonical_repository: GitRepository::Accessible {
            object_format: GitObjectFormat::Sha1,
            fetch_url: "https://github.com/acme/provisioning.git".into(),
            push_url: "https://github.com/acme/provisioning.git".into(),
            repository_id: "provisioning-repository-id".into(),
            provider: ProviderRepository {
                provider: Provider::Github,
                host: "github.com".into(),
                repository: "acme/provisioning".into(),
                repository_id: "provisioning-repository-id".into(),
            },
        },
        target_branch: "main".into(),
        integration_policy: IntegrationPolicy::MergeRequestRequired,
        replication_policy: ReplicationPolicy::None,
    }
}

fn provisioning_migration_state(
    lifecycle: &str,
    disposition: InterruptedProvisioningDisposition,
    binding: Option<iq::git_command::RepositoryBinding>,
) -> MigrationRepositoryState {
    match lifecycle {
        "reserved" => MigrationRepositoryState::Reserved { disposition },
        "staging_directory" => MigrationRepositoryState::StagingDirectory { disposition },
        "git_initialized" => MigrationRepositoryState::GitInitialized {
            disposition,
            git_binding: binding,
        },
        "remote_configured" => MigrationRepositoryState::RemoteConfigured {
            disposition,
            git_binding: binding,
        },
        "target_fetched" => MigrationRepositoryState::TargetFetched {
            disposition,
            git_binding: binding,
        },
        "target_checked_out" => MigrationRepositoryState::TargetCheckedOut {
            disposition,
            git_binding: binding,
        },
        "root_published" => MigrationRepositoryState::RootPublished {
            disposition,
            git_binding: binding,
        },
        "policy_published" => MigrationRepositoryState::PolicyPublished {
            disposition,
            git_binding: binding,
        },
        "rift_initialized" => MigrationRepositoryState::RiftInitialized {
            disposition,
            git_binding: binding,
        },
        "rift_verified" => MigrationRepositoryState::RiftVerified {
            disposition,
            git_binding: binding,
        },
        "owner_published" => MigrationRepositoryState::OwnerPublished {
            disposition,
            git_binding: binding,
        },
        "child_roots_published" => MigrationRepositoryState::ChildRootsPublished {
            disposition,
            git_binding: binding,
        },
        _ => panic!("unknown provisioning lifecycle {lifecycle}"),
    }
}

fn prepare_provisioning_assignment(
    database: &Path,
    lifecycle: &str,
    disposition: InterruptedProvisioningDisposition,
) -> PolicyAssignment {
    let parent = database.parent().unwrap();
    let reservation = parent.join("repositories").join(PROVISIONING_REPO_KEY);
    let root = reservation.join("root");
    let staging = reservation.join(".root.tmp");
    let registry = parent.join("provisioning-rift.db");
    fs::create_dir_all(&reservation).unwrap();
    fs::write(&registry, b"rift\n").unwrap();
    let has_git = !matches!(lifecycle, "reserved" | "staging_directory");
    let uses_staging = matches!(
        lifecycle,
        "git_initialized" | "remote_configured" | "target_fetched" | "target_checked_out"
    );
    let repository_path = if uses_staging { &staging } else { &root };
    let (source_sha, binding) = if has_git {
        fs::create_dir_all(repository_path).unwrap();
        assert!(Command::new("/usr/bin/git")
            .args([
                "init",
                "--object-format=sha1",
                repository_path.to_str().unwrap(),
            ])
            .status()
            .unwrap()
            .success());
        assert!(Command::new("/usr/bin/git")
            .current_dir(repository_path)
            .args([
                "-c",
                "user.name=IQ Test",
                "-c",
                "user.email=iq@example.test",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--allow-empty",
                "-m",
                "provisioning fixture",
            ])
            .status()
            .unwrap()
            .success());
        let source_sha = String::from_utf8(
            Command::new("/usr/bin/git")
                .current_dir(repository_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        (
            source_sha,
            Some(iq::git_command::RepositoryBinding::capture(repository_path).unwrap()),
        )
    } else {
        if lifecycle == "staging_directory" {
            fs::create_dir(&staging).unwrap();
        }
        (SHA1.to_string(), None)
    };
    let registry_metadata = fs::metadata(&registry).unwrap();
    let lifecycle_json = match lifecycle {
        "policy_published" | "rift_initialized" => serde_json::json!({
            "state": lifecycle,
            "identity": {"policy_sha256": null}
        }),
        "rift_verified" | "owner_published" | "child_roots_published" => {
            serde_json::json!({
                "state": lifecycle,
                "identity": {
                    "rift": {
                        "rift_id": "PROVISIONINGRIFT0000000001",
                        "registry_identity": registry,
                        "registry_device": registry_metadata.dev(),
                        "registry_inode": registry_metadata.ino(),
                        "generation": 0
                    },
                    "policy_sha256": null
                }
            })
        }
        _ => serde_json::json!({"state": lifecycle}),
    };
    let connection = Connection::open(database).unwrap();
    connection
        .execute(
            "INSERT INTO repository_remote_owners(repo_key,fetch_url,push_url,target_branch,created_at) VALUES(?1,'https://github.com/acme/provisioning.git','https://github.com/acme/provisioning.git','main','2026-01-01T00:00:00Z')",
            [PROVISIONING_REPO_KEY],
        )
        .unwrap();
    let bootstrap = parent.join("provisioning-bootstrap");
    connection
        .execute(
            "INSERT INTO repository_bootstrap_requests(request_path,target_branch,remote_name,storage_root_path,rift_registry_path,repo_key,created_at,updated_at) VALUES(?1,'main','origin',?2,?3,?4,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            rusqlite::params![bootstrap.as_os_str().as_encoded_bytes(),parent.as_os_str().as_encoded_bytes(),registry.as_os_str().as_encoded_bytes(),PROVISIONING_REPO_KEY],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO repository_provisioning_intents(repo_key,bootstrap_path,owned_root_path,staging_root_path,rift_registry_path,target_branch,fetch_url,push_url,source_sha,policy_bytes,lifecycle_json,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,'main','https://github.com/acme/provisioning.git','https://github.com/acme/provisioning.git',?6,NULL,?7,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            rusqlite::params![PROVISIONING_REPO_KEY,bootstrap.as_os_str().as_encoded_bytes(),root.as_os_str().as_encoded_bytes(),staging.as_os_str().as_encoded_bytes(),registry.as_os_str().as_encoded_bytes(),source_sha,serde_json::to_string(&lifecycle_json).unwrap()],
        )
        .unwrap();
    PolicyAssignment {
        repo_key: PROVISIONING_REPO_KEY.into(),
        policy: provisioning_policy(),
        repository: provisioning_migration_state(lifecycle, disposition, binding),
        development_workspaces: Vec::new(),
        item_dispositions: Vec::new(),
    }
}

fn migration_provider_cli(parent: &Path) -> PathBuf {
    let executable = parent.join("migration-gh");
    fs::write(
        &executable,
        r#"#!/bin/sh
if [ "$1 $2" != "api --hostname" ]; then exit 2; fi
case "$4" in
  repos/acme/legacy) printf '%s' '{"node_id":"legacy-repository-id","full_name":"acme/legacy"}' ;;
  repos/acme/legacy/hash-algorithm) printf '%s' '{"hash_algorithm":"sha1"}' ;;
  repos/acme/provisioning) printf '%s' '{"node_id":"provisioning-repository-id","full_name":"acme/provisioning"}' ;;
  repos/acme/provisioning/hash-algorithm) printf '%s' '{"hash_algorithm":"sha1"}' ;;
  *) exit 3 ;;
esac
"#,
    )
    .unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    executable
}

fn run_migration(database: &Path, inventory: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(database)
        .arg("--test-github-executable")
        .arg(migration_provider_cli(inventory.parent().unwrap()))
        .args(["migrate", "schema3", "--policy-inventory"])
        .arg(inventory)
        .output()
        .unwrap()
}

#[cfg(debug_assertions)]
fn run_interrupted_migration(database: &Path, inventory: &Path, trigger: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(database)
        .arg("--test-github-executable")
        .arg(migration_provider_cli(inventory.parent().unwrap()))
        .args(["migrate", "schema3", "--policy-inventory"])
        .arg(inventory)
        .env(trigger, "1")
        .output()
        .unwrap()
}

#[cfg(debug_assertions)]
fn run_failed_publication_migration(database: &Path, inventory: &Path, boundary: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iq"))
        .arg("--queue-db")
        .arg(database)
        .arg("--test-github-executable")
        .arg(migration_provider_cli(inventory.parent().unwrap()))
        .args(["migrate", "schema3", "--policy-inventory"])
        .arg(inventory)
        .env("IQ_TEST_SCHEMA3_FAIL_PUBLICATION_AFTER", boundary)
        .output()
        .unwrap()
}

fn write_inventory(path: &Path, value: &PolicyInventory) {
    let mut value = value.clone();
    value.version = 3;
    let binding =
        iq::git_command::RepositoryBinding::capture(&fixture_owned_root(path.parent().unwrap()))
            .unwrap();
    for assignment in &mut value.repositories {
        if let MigrationRepositoryState::Ready { git_binding } = &mut assignment.repository {
            *git_binding = Some(binding.clone());
        }
        for workspace in &mut assignment.development_workspaces {
            workspace.git_binding = Some(
                iq::git_command::RepositoryBinding::capture(Path::new(&workspace.path)).unwrap(),
            );
        }
        for disposition in &mut assignment.item_dispositions {
            let Some(workspace) = &mut disposition.workspace_identity else {
                continue;
            };
            if !Path::new(&workspace.path).is_absolute() {
                continue;
            }
            let workspace_path = path
                .parent()
                .unwrap()
                .join("migration-workspaces")
                .join(&disposition.item_id);
            if !workspace_path.is_dir() {
                fs::create_dir_all(workspace_path.parent().unwrap()).unwrap();
                assert!(Command::new("/usr/bin/git")
                    .current_dir(binding.top_level.as_path())
                    .args([
                        "worktree",
                        "add",
                        "--detach",
                        workspace_path.to_str().unwrap(),
                    ])
                    .status()
                    .unwrap()
                    .success());
            }
            workspace.path = workspace_path.to_str().unwrap().to_string();
            workspace.git_binding =
                Some(iq::git_command::RepositoryBinding::capture(&workspace_path).unwrap());
        }
    }
    fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

#[test]
fn schema3_migration_rejects_replaced_live_git_binding_before_primary_mutation() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("inventory.json");
    write_inventory(&inventory_path, &inventory(true));
    let before = fs::read(&database).unwrap();
    let root = fixture_owned_root(temporary.path());
    fs::rename(root.join(".git"), root.join("authorized.git")).unwrap();
    assert!(Command::new("/usr/bin/git")
        .args(["init", "--object-format=sha1", root.to_str().unwrap()])
        .status()
        .unwrap()
        .success());

    let rejected = run_migration(&database, &inventory_path);

    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("live Git binding"));
    assert_eq!(fs::read(&database).unwrap(), before);
}

#[test]
fn schema3_migration_requires_and_persists_active_development_workspace_binding() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let root = fixture_owned_root(temporary.path());
    let workspace = root.parent().unwrap().join("development/active-workspace");
    let added = Command::new("/usr/bin/git")
        .current_dir(&root)
        .args([
            "worktree",
            "add",
            "-b",
            "migration-active-workspace",
            workspace.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let base_sha = String::from_utf8(
        Command::new("/usr/bin/git")
            .current_dir(&workspace)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let base_sha = base_sha.trim().to_string();
    Connection::open(&database)
        .unwrap()
        .execute(
            "UPDATE development_workspaces SET path=?1,rift_id='active-rift-id',source_rift_id='owned-root-rift-id',base_sha=?2,status='active' WHERE id='workspace-1'",
            rusqlite::params![workspace.as_os_str().as_encoded_bytes(),base_sha],
        )
        .unwrap();
    let missing_inventory = temporary.path().join("missing-development.json");
    write_inventory(&missing_inventory, &inventory(true));

    let rejected = run_migration(&database, &missing_inventory);

    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr)
        .contains("omits active schema-3 development workspace"));
    let mut supplied = inventory(true);
    supplied.repositories[0]
        .development_workspaces
        .push(MigrationDevelopmentWorkspace {
            workspace_id: "workspace-1".into(),
            path: workspace.to_str().unwrap().into(),
            rift_id: "active-rift-id".into(),
            source_rift_id: "owned-root-rift-id".into(),
            base_sha,
            git_binding: None,
        });
    let supplied_inventory = temporary.path().join("supplied-development.json");
    write_inventory(&supplied_inventory, &supplied);

    let migrated = run_migration(&database, &supplied_inventory);

    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let connection = Connection::open(&database).unwrap();
    assert_eq!(
        text(
            &connection,
            "SELECT owner_kind FROM workspace_git_bindings WHERE owner_id='workspace-1'"
        ),
        "development"
    );
    assert_eq!(
        text(
            &connection,
            "SELECT CAST(top_level AS TEXT) FROM workspace_git_bindings WHERE owner_id='workspace-1'"
        ),
        workspace.to_str().unwrap()
    );
}

#[test]
fn schema3_migration_models_every_interrupted_provisioning_lifecycle() {
    let lifecycles = [
        "reserved",
        "staging_directory",
        "git_initialized",
        "remote_configured",
        "target_fetched",
        "target_checked_out",
        "root_published",
        "policy_published",
        "rift_initialized",
        "rift_verified",
        "owner_published",
        "child_roots_published",
    ];
    for lifecycle in lifecycles {
        for disposition in [
            InterruptedProvisioningDisposition::Preserve,
            InterruptedProvisioningDisposition::Cancel,
        ] {
            let temporary = tempdir().unwrap();
            let database = temporary.path().join("queues.db");
            copy_fixture(&database);
            let assignment = prepare_provisioning_assignment(&database, lifecycle, disposition);
            let mut supplied = inventory(true);
            supplied.repositories.push(assignment);
            let inventory_path = temporary.path().join("inventory.json");
            write_inventory(&inventory_path, &supplied);

            let migrated = run_migration(&database, &inventory_path);

            assert!(
                migrated.status.success(),
                "lifecycle={lifecycle} disposition={disposition:?}: {}",
                String::from_utf8_lossy(&migrated.stderr)
            );
            let connection = Connection::open(&database).unwrap();
            let intent_count = connection
                .query_row(
                    "SELECT COUNT(*) FROM repository_provisioning_intents WHERE repo_key=?1",
                    [PROVISIONING_REPO_KEY],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap();
            let policy_count = connection
                .query_row(
                    "SELECT COUNT(*) FROM repository_policies WHERE repo_key=?1",
                    [PROVISIONING_REPO_KEY],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap();
            let request_count = connection
                .query_row(
                    "SELECT COUNT(*) FROM repository_bootstrap_requests WHERE repo_key=?1",
                    [PROVISIONING_REPO_KEY],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap();
            let registered_count = connection
                .query_row(
                    "SELECT COUNT(*) FROM registered_repositories WHERE repo_key=?1",
                    [PROVISIONING_REPO_KEY],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap();
            assert_eq!(registered_count, 0, "lifecycle={lifecycle}");
            match disposition {
                InterruptedProvisioningDisposition::Preserve => {
                    assert_eq!(intent_count, 1, "lifecycle={lifecycle}");
                    assert_eq!(policy_count, 1, "lifecycle={lifecycle}");
                    assert_eq!(request_count, 1, "lifecycle={lifecycle}");
                    assert_eq!(
                        text(
                            &connection,
                            &format!(
                                "SELECT json_extract(lifecycle_json,'$.state') FROM repository_provisioning_intents WHERE repo_key='{PROVISIONING_REPO_KEY}'"
                            )
                        ),
                        lifecycle
                    );
                }
                InterruptedProvisioningDisposition::Cancel => {
                    assert_eq!(intent_count, 0, "lifecycle={lifecycle}");
                    assert_eq!(policy_count, 0, "lifecycle={lifecycle}");
                    assert_eq!(request_count, 0, "lifecycle={lifecycle}");
                }
            }
        }
    }
}

#[test]
fn schema3_migration_rejects_incomplete_or_contradictory_inventory_without_mutation() {
    let temporary = tempdir().unwrap();
    let cases = [
        ("missing-provider", {
            let mut value = inventory(true);
            value.repositories[0].item_dispositions[2].provider_repository = None;
            value
        }),
        ("wrong-provider-path", {
            let mut value = inventory(true);
            value.repositories[0].item_dispositions[2]
                .provider_repository
                .as_mut()
                .unwrap()
                .repository = "acme/other".into();
            value
        }),
        ("invalid-runner-repair", {
            let mut value = inventory(true);
            value.repositories[0].item_dispositions[0]
                .runner_snapshot
                .as_mut()
                .unwrap()
                .bounds
                .max_processes = 0;
            value
        }),
        ("invalid-workspace-repair", {
            let mut value = inventory(true);
            value.repositories[0].item_dispositions[0]
                .workspace_identity
                .as_mut()
                .unwrap()
                .path = "relative/workspace".into();
            value
        }),
        ("object-format-mismatch", {
            let mut value = inventory(true);
            match &mut value.repositories[0].policy.canonical_repository {
                GitRepository::Accessible { object_format, .. } => {
                    *object_format = GitObjectFormat::Sha256;
                }
                GitRepository::LocalBare { .. } => unreachable!(),
            }
            value
        }),
        ("provider-repository-identity-mismatch", {
            let mut value = inventory(true);
            match &mut value.repositories[0].policy.canonical_repository {
                GitRepository::Accessible {
                    repository_id,
                    provider,
                    ..
                } => {
                    *repository_id = "wrong-repository-id".into();
                    provider.repository_id = "wrong-repository-id".into();
                }
                GitRepository::LocalBare { .. } => unreachable!(),
            }
            value
        }),
    ];
    for (name, value) in cases {
        let database = temporary.path().join(format!("{name}.db"));
        copy_fixture(&database);
        let inventory_path = temporary.path().join(format!("{name}.json"));
        write_inventory(&inventory_path, &value);
        let before = database_family(&database);
        let rejected = run_migration(&database, &inventory_path);
        assert!(
            !rejected.status.success(),
            "case {name} unexpectedly migrated"
        );
        assert_eq!(
            database_family(&database),
            before,
            "case {name} mutated input"
        );
    }

    let database = temporary.path().join("stored-object-format-mismatch.db");
    copy_fixture(&database);
    Connection::open(&database)
        .unwrap()
        .execute(
            "UPDATE queue_items SET current_head_sha=?1 WHERE id=(SELECT id FROM queue_items ORDER BY id LIMIT 1)",
            ["a".repeat(64)],
        )
        .unwrap();
    let inventory_path = temporary.path().join("stored-object-format-mismatch.json");
    write_inventory(&inventory_path, &inventory(true));
    let before = database_family(&database);
    let rejected = run_migration(&database, &inventory_path);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("queue head"));
    assert_eq!(database_family(&database), before);

    let mut duplicate = inventory(true);
    let mut second = duplicate.repositories[0].clone();
    second.repo_key = "00000000-0000-4000-8000-000000000002".into();
    second.item_dispositions = vec![duplicate.repositories[0].item_dispositions[0].clone()];
    duplicate.repositories.push(second);
    let duplicate_path = temporary.path().join("duplicate.json");
    write_inventory(&duplicate_path, &duplicate);
    assert!(PolicyInventory::load(&duplicate_path)
        .unwrap_err()
        .to_string()
        .contains("duplicate item dispositions"));

    let replica = temporary.path().join("replica.git");
    assert!(Command::new("/usr/bin/git")
        .args([
            "init",
            "--bare",
            "--object-format=sha1",
            replica.to_str().unwrap(),
        ])
        .status()
        .unwrap()
        .success());
    let replica = replica.canonicalize().unwrap();
    let metadata = fs::metadata(&replica).unwrap();
    for (name, object_format, inode) in [
        (
            "replica-inode-mismatch",
            GitObjectFormat::Sha1,
            metadata.ino() + 1,
        ),
        (
            "replica-object-format-mismatch",
            GitObjectFormat::Sha256,
            metadata.ino(),
        ),
    ] {
        let database = temporary.path().join(format!("{name}.db"));
        copy_fixture(&database);
        let mut value = inventory(true);
        value.repositories[0].policy.replication_policy = ReplicationPolicy::Replicate {
            targets: vec![GitRepository::LocalBare {
                object_format,
                path: replica.clone(),
                device: metadata.dev(),
                inode,
            }],
        };
        let inventory_path = temporary.path().join(format!("{name}.json"));
        write_inventory(&inventory_path, &value);
        let before = database_family(&database);
        let rejected = run_migration(&database, &inventory_path);
        assert!(
            !rejected.status.success(),
            "case {name} unexpectedly migrated"
        );
        assert_eq!(
            database_family(&database),
            before,
            "case {name} mutated input"
        );
    }
}

#[test]
fn schema3_mr_unrelated_source_ref_requires_cancellation() {
    let temporary = tempdir().unwrap();
    let prepare = |database: &Path| {
        copy_fixture(database);
        Connection::open(database)
            .unwrap()
            .execute(
                "UPDATE queue_items SET source_branch='refs/heads/unrelated',source_ref='refs/heads/unrelated' WHERE id=?1",
                [ACTIVE_MR_ID],
            )
            .unwrap();
    };
    let rejected_database = temporary.path().join("unrelated-rejected.db");
    prepare(&rejected_database);
    let rejected_inventory = temporary.path().join("unrelated-rejected.json");
    write_inventory(&rejected_inventory, &inventory(true));
    let before = database_family(&rejected_database);
    let rejected = run_migration(&rejected_database, &rejected_inventory);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("must be explicitly cancelled"));
    assert_eq!(database_family(&rejected_database), before);

    let cancelled_database = temporary.path().join("unrelated-cancelled.db");
    prepare(&cancelled_database);
    let mut cancelled = inventory(true);
    cancelled.repositories[0].item_dispositions[1].disposition =
        IncompatibleItemDisposition::Cancel;
    let cancelled_inventory = temporary.path().join("unrelated-cancelled.json");
    write_inventory(&cancelled_inventory, &cancelled);
    let migrated = run_migration(&cancelled_database, &cancelled_inventory);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let connection = Connection::open(cancelled_database).unwrap();
    assert_eq!(
        text(
            &connection,
            &format!("SELECT status FROM queue_items WHERE id='{ACTIVE_MR_ID}'")
        ),
        "cancelled"
    );
    assert_eq!(
        text(
            &connection,
            &format!("SELECT source_branch FROM queue_admissions WHERE item_id='{ACTIVE_MR_ID}'")
        ),
        "refs/pull/8/head"
    );
}

#[test]
fn schema3_uncertain_provider_item_requires_explicit_merge_method() {
    let temporary = tempdir().unwrap();
    let prepare = |database: &Path| {
        copy_fixture(database);
        Connection::open(database)
            .unwrap()
            .execute(
                "UPDATE queue_items SET status='blocked',blocked_phase='integrating',blocked_reason='provider',blocked_message='migrated uncertainty',landing_state_json=json_object('state','uncertain','candidate_sha',?1,'expected_target_sha',?2) WHERE id=?3",
                rusqlite::params![SHA2, SHA1, ACTIVE_MR_ID],
            )
            .unwrap();
    };
    let rejected_database = temporary.path().join("uncertain-provider-rejected.db");
    prepare(&rejected_database);
    let rejected_inventory = temporary.path().join("uncertain-provider-rejected.json");
    write_inventory(&rejected_inventory, &inventory(true));
    let before = database_family(&rejected_database);

    let rejected = run_migration(&rejected_database, &rejected_inventory);

    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("requires provider_merge_method"));
    assert_eq!(database_family(&rejected_database), before);

    let migrated_database = temporary.path().join("uncertain-provider-migrated.db");
    prepare(&migrated_database);
    let mut supplied = inventory(true);
    supplied.repositories[0].item_dispositions[1].provider_merge_method =
        Some(ProviderMergeMethod::Squash);
    let supplied_inventory = temporary.path().join("uncertain-provider-migrated.json");
    write_inventory(&supplied_inventory, &supplied);
    let migrated = run_migration(&migrated_database, &supplied_inventory);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_eq!(
        text(
            &Connection::open(migrated_database).unwrap(),
            &format!(
                "SELECT provider_merge_method FROM queue_admissions WHERE item_id='{ACTIVE_MR_ID}'"
            )
        ),
        "squash"
    );
}

#[test]
fn schema3_cancellation_preserves_direct_and_blocked_external_landing_authority() {
    let temporary = tempdir().unwrap();
    let uncertain = LandingUncertain {
        candidate_sha: SHA2.into(),
        expected_target_sha: SHA1.into(),
        command_id: "migration-command".into(),
        evidence: "command_gate_released".into(),
    };
    let cases = [
        (
            "direct",
            IntegrationEffortState::LandingUncertain(uncertain.clone()),
            None,
        ),
        (
            "infrastructure-blocked",
            IntegrationEffortState::InfrastructureBlocked(BlockedEffort {
                blocker: IntegrationBlocker::Infrastructure(InfrastructureBlocker {
                    component: InfrastructureComponent::Filesystem,
                    operation: "landing".into(),
                    cause: InfrastructureCause::Interrupted {
                        detail: "restart required".into(),
                    },
                }),
                resume: ResumeState::LandingUncertain(uncertain.clone()),
            }),
            Some("infrastructure"),
        ),
        (
            "provider-blocked",
            IntegrationEffortState::ProviderBlocked(BlockedEffort {
                blocker: IntegrationBlocker::ProviderSignoff(ProviderSignoffBlocker {
                    gate: ProviderGateKind::Provider,
                    repository: "acme/legacy".into(),
                    context: "landing".into(),
                    candidate_sha: SHA2.into(),
                    status: ProviderGateStatus::Pending,
                    evidence: "provider response is pending".into(),
                }),
                resume: ResumeState::LandingUncertain(uncertain.clone()),
            }),
            Some("provider_signoff"),
        ),
    ];

    for (name, state, blocker_kind) in cases {
        let database = temporary.path().join(format!("{name}.db"));
        copy_fixture(&database);
        let inventory_path = temporary.path().join(format!("{name}.json"));
        write_inventory(&inventory_path, &inventory(true));
        let workspace = iq::sqlite::WorkspaceIdentity {
            path: temporary
                .path()
                .join("migration-workspaces/direct-active")
                .to_str()
                .unwrap()
                .into(),
            rift_id: "legacy-effort-rift".into(),
            source_rift_id: "legacy-owned-root-rift".into(),
        };
        let connection = Connection::open(&database).unwrap();
        let state_triggers = [
            "integration_effort_legal_transition",
            "integration_effort_related_state_update",
        ]
        .map(|trigger| {
            connection
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?1",
                    [trigger],
                    |row| row.get::<_, String>(0),
                )
                .unwrap()
        });
        connection
            .execute_batch(
                "DROP TRIGGER integration_effort_legal_transition;
                 DROP TRIGGER integration_effort_related_state_update;",
            )
            .unwrap();
        connection
            .execute(
                "UPDATE integration_efforts SET state=?1,state_json=?2,blocker_kind=?3,workspace_json=?4 WHERE item_id='direct-active'",
                rusqlite::params![state.name(), serde_json::to_string(&state).unwrap(), blocker_kind, serde_json::to_string(&workspace).unwrap()],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE queue_items SET status=CASE WHEN ?1 IS NULL THEN 'integrating' ELSE 'blocked' END,blocked_phase=CASE WHEN ?1 IS NULL THEN NULL ELSE 'integrating' END,blocked_reason=CASE ?1 WHEN 'provider_signoff' THEN 'provider' WHEN 'infrastructure' THEN 'infra' ELSE NULL END,blocked_message=CASE WHEN ?1 IS NULL THEN NULL ELSE 'landing reconciliation is blocked' END,landing_state_json=json_object('state','uncertain','candidate_sha',?2,'expected_target_sha',?3),integration_workspace_path=?4,integration_workspace_rift_id=?5,integration_workspace_source_rift_id=?6 WHERE id='direct-active'",
                rusqlite::params![blocker_kind, SHA2, SHA1, workspace.path, workspace.rift_id, workspace.source_rift_id],
            )
            .unwrap();
        for trigger in state_triggers {
            connection.execute_batch(&trigger).unwrap();
        }
        drop(connection);
        let before = database_family(&database);

        let migrated = run_migration(&database, &inventory_path);

        assert!(
            !migrated.status.success(),
            "migration erased {name} landing authority"
        );
        assert!(
            String::from_utf8_lossy(&migrated.stderr).contains("landing authority"),
            "{}",
            String::from_utf8_lossy(&migrated.stderr)
        );
        assert_eq!(
            database_family(&database),
            before,
            "case {name} mutated input"
        );
    }
}

#[test]
fn schema3_no_effort_cancellation_releases_local_submission_workspace() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("no-effort-local.db");
    copy_fixture(&database);
    let connection = Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "UPDATE queue_items SET status='ready' WHERE id='local-terminal';
             UPDATE local_submissions SET state='queued' WHERE queue_item_id='local-terminal';
             UPDATE development_workspaces SET status='submitted' WHERE id=(SELECT workspace_id FROM local_submissions WHERE queue_item_id='local-terminal');",
        )
        .unwrap();
    drop(connection);
    let mut inventory = inventory(true);
    inventory.repositories[0]
        .item_dispositions
        .push(ActiveItemDisposition {
            item_id: "local-terminal".into(),
            disposition: IncompatibleItemDisposition::Cancel,
            admitted_base_sha: None,
            provider_repository: None,
            provider_merge_method: None,
            workspace_identity: None,
            runner_snapshot: None,
            runner_termination_authority: None,
        });
    let inventory_path = temporary.path().join("no-effort-local.json");
    write_inventory(&inventory_path, &inventory);

    let migrated = run_migration(&database, &inventory_path);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let connection = Connection::open(database).unwrap();
    let states: (String, String, String) = connection
        .query_row(
            "SELECT item.status,submission.state,workspace.status FROM queue_items item JOIN local_submissions submission ON submission.queue_item_id=item.id JOIN development_workspaces workspace ON workspace.id=submission.workspace_id WHERE item.id='local-terminal'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        states,
        ("cancelled".into(), "cancelled".into(), "active".into())
    );
}

#[test]
fn schema3_semantically_invalid_workspace_and_runner_require_explicit_repair() {
    let temporary = tempdir().unwrap();

    let workspace_database = temporary.path().join("invalid-workspace.db");
    copy_fixture(&workspace_database);
    Connection::open(&workspace_database)
        .unwrap()
        .execute(
            "UPDATE integration_efforts SET workspace_json=?1 WHERE item_id='direct-active'",
            [serde_json::json!({
                "path":"relative/workspace",
                "rift_id":"rift",
                "source_rift_id":"source"
            })
            .to_string()],
        )
        .unwrap();
    let mut no_workspace_repair = inventory(true);
    no_workspace_repair.repositories[0].item_dispositions[0].workspace_identity = None;
    let workspace_inventory = temporary.path().join("invalid-workspace.json");
    write_inventory(&workspace_inventory, &no_workspace_repair);
    let before = database_family(&workspace_database);
    let rejected = run_migration(&workspace_database, &workspace_inventory);
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("requires explicit workspace_identity")
    );
    assert_eq!(database_family(&workspace_database), before);

    let runner_database = temporary.path().join("invalid-runner.db");
    copy_fixture(&runner_database);
    let mut invalid_runner = inventory(true).repositories[0].item_dispositions[0]
        .runner_snapshot
        .clone()
        .unwrap();
    invalid_runner.bounds.max_processes = 0;
    Connection::open(&runner_database)
        .unwrap()
        .execute(
            "UPDATE integration_efforts SET runner_snapshot_json=?1 WHERE item_id='direct-active'",
            [serde_json::to_string(&invalid_runner).unwrap()],
        )
        .unwrap();
    let mut no_runner_repair = inventory(true);
    no_runner_repair.repositories[0].item_dispositions[0].runner_snapshot = None;
    let runner_inventory = temporary.path().join("invalid-runner.json");
    write_inventory(&runner_inventory, &no_runner_repair);
    let before = database_family(&runner_database);
    let rejected = run_migration(&runner_database, &runner_inventory);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("requires explicit runner_snapshot"));
    assert_eq!(database_family(&runner_database), before);

    let repaired_inventory = temporary.path().join("repaired-runner.json");
    write_inventory(&repaired_inventory, &inventory(true));
    let repaired = run_migration(&runner_database, &repaired_inventory);
    assert!(
        repaired.status.success(),
        "{}",
        String::from_utf8_lossy(&repaired.stderr)
    );
}

fn database_family(path: &Path) -> BTreeMap<String, Vec<u8>> {
    ["", "-wal", "-shm", "-journal", ".control.lock"]
        .into_iter()
        .filter_map(|suffix| {
            let member = PathBuf::from(format!("{}{suffix}", path.display()));
            member
                .is_file()
                .then(|| (suffix.to_string(), fs::read(member).unwrap()))
        })
        .collect()
}

fn spawn_shared_lease_holder(database: &Path, root: &Path) -> Child {
    let ready = root.join("schema3-shared-lease-ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "schema3_shared_lease_holder_process",
            "--nocapture",
        ])
        .env("IQ_TEST_SCHEMA3_SHARED_LEASE", database)
        .env("IQ_TEST_SCHEMA3_SHARED_LEASE_READY", &ready)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "schema-3 lease holder exited before readiness"
        );
        assert!(Instant::now() < deadline, "schema-3 lease holder timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
    child
}

fn text(connection: &Connection, sql: &str) -> String {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn integer(connection: &Connection, sql: &str) -> i64 {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn schema3_shared_lease_holder_process() {
    let Some(database) = std::env::var_os("IQ_TEST_SCHEMA3_SHARED_LEASE") else {
        return;
    };
    let ready = std::env::var_os("IQ_TEST_SCHEMA3_SHARED_LEASE_READY").unwrap();
    let _lease = iq::control_store::DatabaseProcessLease::acquire(Path::new(&database)).unwrap();
    fs::write(ready, b"ready\n").unwrap();
    std::io::stdin().read_to_end(&mut Vec::new()).unwrap();
}

#[test]
fn normal_cli_rejects_schema3_without_database_family_mutation() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let before = database_family(&database);

    let rejected = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args(["--queue-db", database.to_str().unwrap(), "list"])
        .output()
        .unwrap();

    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stdout.is_empty());
    assert_eq!(
        String::from_utf8(rejected.stderr).unwrap(),
        "Error: IQ schema 3 requires explicit offline migration with `iq migrate schema3 --policy-inventory <path>`\n"
    );
    assert_eq!(database_family(&database), before);
}

#[test]
fn schema3_migration_uses_default_database_path_without_normal_schema_open() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("iq/integration-queues/queues.db");
    fs::create_dir_all(database.parent().unwrap()).unwrap();
    copy_fixture(&database);
    let inventory_path = database.parent().unwrap().join("inventory.json");
    write_inventory(&inventory_path, &inventory(true));
    let inspected = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("XDG_STATE_HOME", temporary.path())
        .args(["migrate", "inspect-git-binding", "--path"])
        .arg(fixture_owned_root(database.parent().unwrap()))
        .output()
        .unwrap();
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );

    let migrated = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("XDG_STATE_HOME", temporary.path())
        .arg("--test-github-executable")
        .arg(migration_provider_cli(inventory_path.parent().unwrap()))
        .args(["migrate", "schema3", "--policy-inventory"])
        .arg(&inventory_path)
        .output()
        .unwrap();

    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_eq!(
        text(
            &Connection::open(database).unwrap(),
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'"
        ),
        "4"
    );
}

#[test]
fn schema3_migration_requires_exclusive_process_lease_before_backup_or_mutation() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("inventory.json");
    write_inventory(&inventory_path, &inventory(true));
    let before = database_family(&database);
    let mut holder = spawn_shared_lease_holder(&database, temporary.path());

    let blocked = run_migration(&database, &inventory_path);

    assert_eq!(blocked.status.code(), Some(1));
    assert!(blocked.stdout.is_empty());
    let stderr = String::from_utf8(blocked.stderr).unwrap();
    assert!(
        stderr.starts_with("Error: take exclusive offline migration authority\n"),
        "{stderr}"
    );
    assert!(
        stderr.contains("acquire exclusive IQ database process lease"),
        "{stderr}"
    );
    assert_eq!(database_family(&database), before);
    assert_eq!(
        fs::read_dir(temporary.path())
            .unwrap()
            .filter(|entry| entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("schema3-backup"))
            .count(),
        0
    );

    drop(holder.stdin.take());
    assert!(holder.wait().unwrap().success());
    let migrated = run_migration(&database, &inventory_path);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_eq!(
        text(
            &Connection::open(&database).unwrap(),
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'"
        ),
        "4"
    );
}

#[test]
#[cfg(debug_assertions)]
fn schema3_migration_interruption_before_publication_preserves_primary_and_can_retry() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("before-publication.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("before-publication.json");
    write_inventory(&inventory_path, &inventory(true));
    let before = database_family(&database);

    let interrupted = run_interrupted_migration(
        &database,
        &inventory_path,
        "IQ_TEST_SCHEMA3_STOP_BEFORE_PUBLICATION",
    );

    assert_eq!(interrupted.status.code(), Some(92));
    assert_eq!(database_family(&database), before);
    let migrated = run_migration(&database, &inventory_path);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_eq!(
        text(
            &Connection::open(database).unwrap(),
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'"
        ),
        "4"
    );
}

#[test]
#[cfg(debug_assertions)]
fn schema3_migration_interruption_after_publication_recovers_from_primary_and_backup() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("after-publication.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("after-publication.json");
    write_inventory(&inventory_path, &inventory(true));

    let interrupted = run_interrupted_migration(
        &database,
        &inventory_path,
        "IQ_TEST_SCHEMA3_STOP_AFTER_PUBLICATION",
    );

    assert_eq!(interrupted.status.code(), Some(93));
    assert_eq!(
        text(
            &Connection::open(&database).unwrap(),
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'"
        ),
        "4"
    );
    let recovered = run_migration(&database, &inventory_path);
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    let report: Value = serde_json::from_slice(&recovered.stdout).unwrap();
    assert_eq!(report["from_schema"], 3);
    assert_eq!(report["to_schema"], 4);
    assert!(Path::new(report["backup_path"].as_str().unwrap()).is_file());
}

#[test]
#[cfg(debug_assertions)]
fn schema3_publication_faults_preserve_exact_source_bytes_and_recover() {
    for boundary in [
        "exchange",
        "primary_sync",
        "exchanged_state",
        "validation",
        "candidate_cleanup",
    ] {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join(format!("{boundary}.db"));
        copy_fixture(&database);
        let inventory_path = temporary.path().join(format!("{boundary}.json"));
        write_inventory(&inventory_path, &inventory(true));
        let original = fs::read(&database).unwrap();

        let failed = run_failed_publication_migration(&database, &inventory_path, boundary);

        assert!(!failed.status.success(), "{boundary}");
        assert_eq!(
            fs::read(
                temporary
                    .path()
                    .join(format!("{boundary}.db.schema3-backup-authority/database"))
            )
            .unwrap(),
            original,
            "{boundary}"
        );
        let recovered = run_migration(&database, &inventory_path);
        assert!(
            recovered.status.success(),
            "{boundary}: {}",
            String::from_utf8_lossy(&recovered.stderr)
        );
        assert_eq!(
            text(
                &Connection::open(&database).unwrap(),
                "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'"
            ),
            "4",
            "{boundary}"
        );
        let state: Value = serde_json::from_slice(
            &fs::read(
                temporary
                    .path()
                    .join(format!("{boundary}.db.schema3-publication-state.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(state["phase"], "complete", "{boundary}");
    }
}

#[test]
#[cfg(debug_assertions)]
fn schema3_backup_publication_interruption_rejects_unverifiable_backup_on_retry() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("backup-interruption.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("backup-interruption.json");
    write_inventory(&inventory_path, &inventory(true));

    let interrupted = run_interrupted_migration(
        &database,
        &inventory_path,
        "IQ_TEST_SCHEMA3_STOP_AFTER_BACKUP_PUBLICATION",
    );

    assert_eq!(interrupted.status.code(), Some(94));
    let backup_root = temporary
        .path()
        .join("backup-interruption.db.schema3-backup-authority");
    let backup_database = backup_root.join("database");
    let backup_before = fs::read(&backup_database).unwrap();
    fs::remove_file(backup_root.join("manifest.json")).unwrap();
    let migrated = run_migration(&database, &inventory_path);
    assert!(!migrated.status.success());
    assert!(
        String::from_utf8_lossy(&migrated.stderr)
            .contains("pre-existing schema-3 backup authority is not owned by IQ"),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_eq!(fs::read(backup_database).unwrap(), backup_before);
    assert!(!backup_root.join("manifest.json").exists());
}

#[test]
fn schema3_migration_preserves_unowned_legacy_artifact_collisions() {
    for collision_kind in ["candidate", "backup"] {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("queues.db");
        copy_fixture(&database);
        let inventory_path = temporary.path().join("inventory.json");
        write_inventory(&inventory_path, &inventory(true));
        let before = database_family(&database);
        let collision = match collision_kind {
            "candidate" => temporary
                .path()
                .join(".queues.db.schema3-migration-candidate"),
            "backup" => temporary.path().join("queues.db.schema3-backup-authority"),
            _ => unreachable!(),
        };
        fs::create_dir(&collision).unwrap();
        fs::write(collision.join("user-data"), collision_kind).unwrap();

        let migrated = run_migration(&database, &inventory_path);

        assert!(!migrated.status.success(), "collision {collision_kind}");
        let expected_error = match collision_kind {
            "candidate" => "unowned fixed-name schema-3 migration candidate exists",
            "backup" => "pre-existing schema-3 backup authority is not owned by IQ",
            _ => unreachable!(),
        };
        assert!(
            String::from_utf8_lossy(&migrated.stderr).contains(expected_error),
            "collision {collision_kind}: {}",
            String::from_utf8_lossy(&migrated.stderr)
        );
        assert_eq!(database_family(&database), before);
        assert_eq!(
            fs::read_to_string(collision.join("user-data")).unwrap(),
            collision_kind
        );
    }
}

#[test]
fn schema3_migration_rejects_unsafe_systemd_authority_before_mutation() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("inventory.json");
    write_inventory(&inventory_path, &inventory(true));
    let connection = Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER integration_effort_exact_payload_update;
             DROP TRIGGER integration_effort_legal_transition;
             DROP TRIGGER integration_effort_related_state_update;",
        )
        .unwrap();
    let state = serde_json::json!({
        "state": "agent_launching",
        "payload": {
            "launch_operation_id": "launch-1",
            "unit_name": "../unrelated.service",
            "cycle_id": "cycle-1",
            "cycle_number": 1,
            "authority_lease_id": "lease-1",
            "input_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "protocol_directory": "/tmp/iq-protocol",
            "prepared_at": "2026-08-17T00:00:00Z"
        }
    });
    connection
        .execute(
            "UPDATE integration_efforts SET state='agent_launching',state_json=?1 WHERE id='effort-1'",
            [state.to_string()],
        )
        .unwrap();
    drop(connection);
    let before = database_family(&database);

    let migrated = run_migration(&database, &inventory_path);

    assert!(!migrated.status.success());
    assert!(
        String::from_utf8_lossy(&migrated.stderr).contains("systemd unit name"),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_eq!(database_family(&database), before);
    assert!(!temporary
        .path()
        .join(".queues.db.schema3-migration-candidate")
        .exists());
    assert!(!temporary
        .path()
        .join("queues.db.schema3-backup-authority")
        .exists());
}

#[test]
fn schema3_migration_does_not_publish_unverifiable_runner_termination_debt() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let cycle_id = format!("migration-{}", uuid::Uuid::new_v4());
    let scope = MigrationTestScope::start(&cycle_id);
    let invalid_start = scope.process_start_ticks + 1;
    let state = serde_json::json!({
        "state": "agent_running",
        "payload": {
            "launch_operation_id": "launch-migration",
            "unit_name": scope.unit_name.clone(),
            "cycle_id": cycle_id.clone(),
            "cycle_number": 1,
            "pid": scope.pid,
            "process_start_ticks": invalid_start,
            "process_group_id": 1,
            "authority_lease_id": "lease-migration",
            "sandbox_id": "sandbox-migration",
            "input_sha256": "a".repeat(64),
            "result": {"state": "absent"},
            "started_at": "2026-08-17T00:00:00Z"
        }
    });
    let process = state["payload"].clone();
    rewrite_fixture_with_disabled_triggers(
        &database,
        &[
            "integration_effort_legal_transition",
            "integration_effort_related_state_update",
        ],
        |connection| {
            connection
                .execute(
                    "INSERT INTO integration_cycles(id,effort_id,cycle_number,status,process_json,input_digest,result_state_json,created_at) VALUES(?1,'effort-1',1,'running',?2,?3,?4,?5)",
                    rusqlite::params![
                        &cycle_id,
                        process.to_string(),
                        "a".repeat(64),
                        serde_json::json!({"state":"absent"}).to_string(),
                        "2026-08-17T00:00:00Z"
                    ],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE integration_efforts SET state='agent_running',state_json=?1 WHERE id='effort-1'",
                    [state.to_string()],
                )
                .unwrap();
        },
    );

    let mut policy_inventory = inventory(true);
    let disposition = &mut policy_inventory.repositories[0].item_dispositions[0];
    let systemctl = iq::agent_config::executable_identity(Path::new("/usr/bin/systemctl")).unwrap();
    disposition
        .runner_snapshot
        .as_mut()
        .unwrap()
        .sandbox
        .systemctl = systemctl;
    disposition.runner_termination_authority = Some(LegacyRunnerScopeAuthority {
        cycle_id,
        unit_name: scope.unit_name.clone(),
        control_group: scope.control_group.clone(),
        pid: scope.pid,
        process_start_ticks: invalid_start,
    });
    let inventory_path = temporary.path().join("inventory.json");
    write_inventory(&inventory_path, &policy_inventory);
    let before = database_family(&database);

    let migrated = run_migration(&database, &inventory_path);

    assert!(!migrated.status.success());
    assert!(
        String::from_utf8_lossy(&migrated.stderr).contains("process is not alive"),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_eq!(database_family(&database), before);
    assert!(!temporary
        .path()
        .join("queues.db.schema3-backup-authority")
        .exists());
}

#[test]
fn schema3_migration_reports_published_but_incomplete_when_runner_debt_remains() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let cycle_id = format!("migration-incomplete-{}", uuid::Uuid::new_v4());
    let scope = MigrationTestScope::start(&cycle_id);
    let systemctl = temporary.path().join("migration-systemctl");
    fs::write(
        &systemctl,
        format!(
            "#!/bin/sh\nversion=$(/usr/bin/sqlite3 '{}' \"SELECT value FROM queue_metadata WHERE key='workspace_schema_version'\")\n[ \"$version\" = 4 ] && exit 1\nexec /usr/bin/systemctl \"$@\"\n",
            database.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();
    let state = serde_json::json!({
        "state": "agent_running",
        "payload": {
            "launch_operation_id": "launch-migration-incomplete",
            "unit_name": scope.unit_name.clone(),
            "cycle_id": cycle_id.clone(),
            "cycle_number": 1,
            "pid": scope.pid,
            "process_start_ticks": scope.process_start_ticks,
            "process_group_id": 1,
            "authority_lease_id": "lease-migration-incomplete",
            "sandbox_id": "sandbox-migration-incomplete",
            "input_sha256": "a".repeat(64),
            "result": {"state": "absent"},
            "started_at": "2026-08-17T00:00:00Z"
        }
    });
    let process = state["payload"].clone();
    rewrite_fixture_with_disabled_triggers(
        &database,
        &[
            "integration_effort_legal_transition",
            "integration_effort_related_state_update",
        ],
        |connection| {
            connection
                .execute(
                    "INSERT INTO integration_cycles(id,effort_id,cycle_number,status,process_json,input_digest,result_state_json,created_at) VALUES(?1,'effort-1',1,'running',?2,?3,?4,?5)",
                    rusqlite::params![
                        &cycle_id,
                        process.to_string(),
                        "a".repeat(64),
                        serde_json::json!({"state":"absent"}).to_string(),
                        "2026-08-17T00:00:00Z"
                    ],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE integration_efforts SET state='agent_running',state_json=?1 WHERE id='effort-1'",
                    [state.to_string()],
                )
                .unwrap();
        },
    );
    let mut policy_inventory = inventory(true);
    let disposition = &mut policy_inventory.repositories[0].item_dispositions[0];
    disposition
        .runner_snapshot
        .as_mut()
        .unwrap()
        .sandbox
        .systemctl = iq::agent_config::executable_identity(&systemctl).unwrap();
    disposition.runner_termination_authority = Some(LegacyRunnerScopeAuthority {
        cycle_id,
        unit_name: scope.unit_name.clone(),
        control_group: scope.control_group.clone(),
        pid: scope.pid,
        process_start_ticks: scope.process_start_ticks,
    });
    let inventory_path = temporary.path().join("inventory.json");
    write_inventory(&inventory_path, &policy_inventory);

    let migrated = run_migration(&database, &inventory_path);

    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let report: Value = serde_json::from_slice(&migrated.stdout).unwrap();
    assert_eq!(report["completion"]["state"], "published_but_incomplete");
    assert_eq!(
        report["completion"]["remaining_runner_termination_debts"],
        1
    );
    assert_eq!(
        integer(
            &Connection::open(&database).unwrap(),
            "SELECT COUNT(*) FROM runner_termination_debt"
        ),
        1
    );
}

#[test]
#[cfg(debug_assertions)]
fn schema3_backup_retry_replaces_stale_digest_after_source_mutation() {
    use std::os::unix::fs::MetadataExt;

    let temporary = tempdir().unwrap();
    let database = temporary.path().join("backup-stale.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("backup-stale.json");
    write_inventory(&inventory_path, &inventory(true));
    let interrupted = run_interrupted_migration(
        &database,
        &inventory_path,
        "IQ_TEST_SCHEMA3_STOP_AFTER_BACKUP_PUBLICATION",
    );
    assert_eq!(interrupted.status.code(), Some(94));
    let backup_database = temporary
        .path()
        .join("backup-stale.db.schema3-backup-authority/database");
    let old_backup_inode = fs::metadata(&backup_database).unwrap().ino();
    Connection::open(&database)
        .unwrap()
        .execute(
            "UPDATE queue_items SET producer_metadata_json='{\"worker\":\"changed-before-retry\"}' WHERE id='direct-active'",
            [],
        )
        .unwrap();

    let migrated = run_migration(&database, &inventory_path);

    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    assert_ne!(
        fs::metadata(&backup_database).unwrap().ino(),
        old_backup_inode
    );
    let backup = Connection::open_with_flags(
        format!("file:{}?immutable=1", backup_database.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap();
    assert_eq!(
        text(
            &backup,
            "SELECT json_extract(producer_metadata_json,'$.worker') FROM queue_items WHERE id='direct-active'"
        ),
        "changed-before-retry"
    );
}

#[test]
fn schema3_cli_migration_uses_frozen_release_fixture_and_preserves_exact_values() {
    let temporary = tempdir().unwrap();
    let rejected_database = temporary.path().join("rejected.db");
    copy_fixture(&rejected_database);
    let rejected_inventory = temporary.path().join("rejected-inventory.json");
    write_inventory(&rejected_inventory, &inventory(false));
    let before = database_family(&rejected_database);
    let rejected = run_migration(&rejected_database, &rejected_inventory);
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("requires admitted_base_sha"),
        "{}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert_eq!(database_family(&rejected_database), before);

    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("inventory.json");
    write_inventory(&inventory_path, &inventory(true));
    let migrated = run_migration(&database, &inventory_path);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let report: Value = serde_json::from_slice(&migrated.stdout).unwrap();
    assert_eq!(report["from_schema"], 3);
    assert_eq!(report["to_schema"], 4);
    assert_eq!(report["repositories"], 1);
    assert_eq!(report["admissions"], 4);

    let connection = Connection::open(&database).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    assert_eq!(
        text(
            &connection,
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'"
        ),
        "4"
    );
    assert_eq!(
        text(&connection, "SELECT repo_key FROM registered_repositories"),
        REPO_KEY
    );
    assert_eq!(text(&connection, "SELECT json_extract(producer_metadata_json,'$.worker') FROM queue_items WHERE id='direct-active'"), "frozen");
    assert_eq!(text(&connection, "SELECT json_extract(validation_evidence_json,'$.proof') FROM queue_items WHERE id='direct-active'"), "exact");
    assert_eq!(
        text(
            &connection,
            &format!("SELECT status FROM queue_items WHERE id='{ACTIVE_MR_ID}'")
        ),
        "ready"
    );
    assert_eq!(
        text(
            &connection,
            "SELECT status FROM queue_items WHERE id='direct-active'"
        ),
        "cancelled"
    );
    assert_eq!(
        text(
            &connection,
            "SELECT state FROM integration_efforts WHERE id='effort-1'"
        ),
        "cancelled"
    );
    assert_eq!(
        text(
            &connection,
            "SELECT result FROM integration_attempts WHERE id='attempt-1'"
        ),
        "cancelled"
    );
    assert_eq!(
        integer(
            &connection,
            "SELECT COUNT(*) FROM durable_events WHERE effort_id='effort-1' AND event_type='cancelled'"
        ),
        1
    );
    assert_eq!(
        integer(
            &connection,
            "SELECT COUNT(*) FROM terminal_workspace_cleanup_debt WHERE item_id='direct-active'"
        ),
        1
    );
    let effort = iq::control_store::ControlStore::open(&database)
        .unwrap()
        .effort_for_item("direct-active")
        .unwrap()
        .unwrap();
    assert!(matches!(
        effort.state,
        iq::control_domain::IntegrationEffortState::Cancelled(_)
    ));
    assert_eq!(
        effort.workspace.path,
        temporary
            .path()
            .join("migration-workspaces/direct-active")
            .to_str()
            .unwrap()
    );
    assert_eq!(
        text(
            &connection,
            "SELECT source_head_sha FROM integration_attempts WHERE id='attempt-1'"
        ),
        SHA2
    );
    assert_eq!(
        text(
            &connection,
            "SELECT target_base_sha FROM integration_attempts WHERE id='attempt-1'"
        ),
        SHA1
    );
    assert_eq!(
        text(
            &connection,
            "SELECT policy_digest FROM integration_attempts WHERE id='attempt-1'"
        ),
        "fixture-policy-digest"
    );
    assert_eq!(text(&connection, "SELECT command FROM validation_invocations WHERE attempt_id='attempt-1' AND invocation_number=1"), "true");
    assert_eq!(text(&connection, "SELECT validated_commit_sha FROM validation_invocations WHERE attempt_id='attempt-1' AND invocation_number=1"), SHA2);
    assert_eq!(
        text(
            &connection,
            "SELECT message FROM queue_events WHERE id='queue-event-1'"
        ),
        "frozen queue event"
    );
    assert_eq!(text(&connection, "SELECT json_extract(payload_json,'$.evidence') FROM durable_events WHERE id='durable-event-1'"), "frozen");
    assert_eq!(
        text(
            &connection,
            "SELECT question FROM prompts WHERE id='prompt-1'"
        ),
        "Continue frozen work?"
    );
    assert_eq!(
        text(
            &connection,
            "SELECT answer FROM prompts WHERE id='prompt-1'"
        ),
        "yes"
    );
    assert_eq!(
        integer(
            &connection,
            "SELECT attempt_count FROM notification_deliveries WHERE event_id='durable-event-1'"
        ),
        2
    );
    assert_eq!(text(&connection, "SELECT snapshot_json FROM item_state_repository_bindings WHERE item_id='direct-active'"), "{\"kind\":\"local\"}");
    assert_eq!(
        text(
            &connection,
            "SELECT artifact_url FROM state_repository_artifacts WHERE effort_id='effort-1'"
        ),
        "https://github.com/acme/state/issues/42"
    );
    assert_eq!(
        integer(
            &connection,
            "SELECT projection_revision FROM state_repository_artifacts WHERE effort_id='effort-1'"
        ),
        3
    );
    assert_eq!(
        text(
            &connection,
            "SELECT target_kind FROM terminal_workspace_cleanup_debt WHERE item_id='mr-terminal'"
        ),
        "creation_intent"
    );
    assert_eq!(
        text(
            &connection,
            &format!("SELECT base_sha FROM queue_admissions WHERE item_id='{ACTIVE_MR_ID}'")
        ),
        SHA1
    );
    assert_eq!(
        text(
            &connection,
            "SELECT kind FROM queue_admissions WHERE item_id='mr-terminal'"
        ),
        "historical_merge_request"
    );
    assert_eq!(
        text(
            &connection,
            "SELECT provider_repository_id FROM queue_admissions WHERE item_id='mr-terminal'"
        ),
        "historical-repository-id"
    );
    assert_eq!(
        text(
            &connection,
            "SELECT base_sha FROM queue_admissions WHERE item_id='mr-terminal'"
        ),
        SHA1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn schema3_cli_failure_rolls_back_bytes_and_creates_recoverable_backup() {
    let temporary = tempdir().unwrap();
    let database = temporary.path().join("queues.db");
    copy_fixture(&database);
    let inventory_path = temporary.path().join("inventory.json");
    write_inventory(&inventory_path, &inventory(true));
    let before = database_family(&database);
    let failed = Command::new(env!("CARGO_BIN_EXE_iq"))
        .env("IQ_TEST_SCHEMA3_FAIL_BEFORE_COMMIT", "1")
        .arg("--test-github-executable")
        .arg(migration_provider_cli(inventory_path.parent().unwrap()))
        .arg("--queue-db")
        .arg(&database)
        .args(["migrate", "schema3", "--policy-inventory"])
        .arg(&inventory_path)
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert_eq!(database_family(&database), before);

    let backups = fs::read_dir(temporary.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name().unwrap().to_string_lossy() == "queues.db.schema3-backup-authority"
        })
        .collect::<Vec<_>>();
    assert_eq!(backups.len(), 1);
    let backup_database = backups[0].join("database");
    assert!(backups[0].join("manifest.json").is_file());
    let backup = Connection::open_with_flags(
        format!("file:{}?immutable=1", backup_database.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap();
    assert_eq!(
        text(
            &backup,
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'"
        ),
        "3"
    );
    assert_eq!(
        text(
            &backup,
            "SELECT message FROM queue_events WHERE id='queue-event-1'"
        ),
        "frozen queue event"
    );
    assert_eq!(
        backup
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}
