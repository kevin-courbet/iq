use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use uuid::Uuid;

pub const SCHEMA_VERSION: &str = "3";
pub const INTERNAL_REMOTE_NAME: &str = "iq-target";
const POLICY_PATH: &str = ".iq/config.json";
const MAX_POLICY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RepoKey(String);

impl RepoKey {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub(crate) fn from_stored(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let parsed = Uuid::parse_str(&value).context("repository key must be a UUID")?;
        if parsed.to_string() != value {
            anyhow::bail!("repository key must use canonical lowercase UUID form");
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for RepoKey {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RepoKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub fn validate_target_branch(value: &str) -> Result<&str> {
    match value {
        "main" | "master" => Ok(value),
        _ => anyhow::bail!("IQ target branch must be main or master"),
    }
}

fn target_ref(target: &str) -> Result<String> {
    Ok(format!("refs/heads/{}", validate_target_branch(target)?))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteIdentity {
    pub fetch_url: String,
    pub push_url: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiftRootIdentity {
    pub rift_id: String,
    pub registry_identity: PathBuf,
    pub registry_device: u64,
    pub registry_inode: u64,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildWorkspaceRoots {
    pub development: PathBuf,
    pub integration: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RiftVerified {
    rift: RiftRootIdentity,
    policy_sha256: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "identity",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ProvisioningLifecycle {
    Reserved,
    StagingDirectory,
    GitInitialized,
    RemoteConfigured,
    TargetFetched,
    TargetCheckedOut,
    RootPublished,
    PolicyPublished { policy_sha256: Option<String> },
    RiftInitialized { policy_sha256: Option<String> },
    RiftVerified(RiftVerified),
    OwnerPublished(RiftVerified),
    ChildRootsPublished(RiftVerified),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OwnedRepositoryRoot {
    repo_key: RepoKey,
    path: PathBuf,
    rift: RiftRootIdentity,
    remote: RemoteIdentity,
    target: String,
    children: ChildWorkspaceRoots,
    source_sha: String,
}

impl OwnedRepositoryRoot {
    pub fn repo_key(&self) -> &RepoKey {
        &self.repo_key
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn rift(&self) -> &RiftRootIdentity {
        &self.rift
    }

    pub fn remote(&self) -> &RemoteIdentity {
        &self.remote
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn children(&self) -> &ChildWorkspaceRoots {
        &self.children
    }

    pub fn source_sha(&self) -> &str {
        &self.source_sha
    }
}

#[derive(Clone, Debug)]
pub struct ProvisionOptions {
    pub storage_root: PathBuf,
    pub bootstrap_path: PathBuf,
    pub target: String,
    pub remote_name: String,
    pub rift_database: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct ProvisionPlan {
    repo_key: RepoKey,
    storage_root: PathBuf,
    path: PathBuf,
    staging_path: PathBuf,
    target: String,
    remote: RemoteIdentity,
    rift_database: PathBuf,
    source_sha: String,
    policy: Option<Vec<u8>>,
    created_at: String,
}

pub(crate) fn provision(
    connection: &mut Connection,
    options: &ProvisionOptions,
) -> Result<OwnedRepositoryRoot> {
    let mut options = options.clone();
    options.storage_root = absolute_path(&options.storage_root)?;
    let requested_rift_database = planned_rift_database(options.rift_database.as_deref())?;
    options.rift_database = Some(requested_rift_database.clone());
    validate_target_branch(&options.target)?;
    require_absolute_directory(&options.storage_root, "owned repository storage root")?;
    let bootstrap_identity = absolute_path(&options.bootstrap_path)?;
    let request_repo_key = prepare_bootstrap_request(connection, &options, &bootstrap_identity)?;
    let persisted = load_intent_by_bootstrap(connection, &bootstrap_identity)?;
    let (plan, newly_reserved) = if let Some((plan, _)) = persisted {
        require_requested_locations(
            connection,
            &bootstrap_identity,
            &options.storage_root,
            &plan.storage_root,
            &requested_rift_database,
            &plan.rift_database,
        )?;
        if plan.target != options.target {
            anyhow::bail!(
                "remote is already reserved with immutable target {}",
                plan.target
            );
        }
        (plan, false)
    } else if let Some(repo_key) = request_repo_key {
        if let Some(existing) = find_registered_by_key(connection, &repo_key)? {
            require_requested_locations(
                connection,
                &bootstrap_identity,
                &options.storage_root,
                &repository_storage_root(existing.path(), existing.repo_key())?,
                &requested_rift_database,
                &existing.rift().registry_identity,
            )?;
            if existing.target != options.target {
                anyhow::bail!(
                    "remote is already registered with immutable target {}",
                    existing.target
                );
            }
            return Ok(existing);
        }
        let (plan, _) = load_intent_by_repo_key(connection, &repo_key)?.context(
            "bootstrap request is linked to neither active provisioning intent nor ready repository",
        )?;
        require_requested_locations(
            connection,
            &bootstrap_identity,
            &options.storage_root,
            &plan.storage_root,
            &requested_rift_database,
            &plan.rift_database,
        )?;
        if plan.target != options.target {
            anyhow::bail!(
                "remote is already reserved with immutable target {}",
                plan.target
            );
        }
        (plan, false)
    } else {
        let bootstrap_path = canonical_git_root(&options.bootstrap_path)?;
        let remote = remote_identity(&bootstrap_path, &options.remote_name)?;
        if let Some(existing) = find_registered(connection, &remote)? {
            require_requested_locations(
                connection,
                &bootstrap_identity,
                &options.storage_root,
                &repository_storage_root(existing.path(), existing.repo_key())?,
                &requested_rift_database,
                &existing.rift().registry_identity,
            )?;
            if existing.target != options.target {
                anyhow::bail!(
                    "remote is already registered with immutable target {}",
                    existing.target
                );
            }
            link_bootstrap_request(connection, &bootstrap_identity, existing.repo_key.as_str())?;
            return Ok(existing);
        }
        if let Some((repo_key, target)) = find_remote_owner(connection, &remote)? {
            if target != options.target {
                anyhow::bail!("remote is already reserved with immutable target {target}");
            }
            if let Some(existing) = find_registered_by_key(connection, &repo_key)? {
                require_requested_locations(
                    connection,
                    &bootstrap_identity,
                    &options.storage_root,
                    &repository_storage_root(existing.path(), existing.repo_key())?,
                    &requested_rift_database,
                    &existing.rift().registry_identity,
                )?;
                link_bootstrap_request(connection, &bootstrap_identity, repo_key.as_str())?;
                return Ok(existing);
            }
            let (plan, _) = load_intent_by_repo_key(connection, &repo_key)?.context(
                "remote owner has neither active provisioning intent nor ready repository",
            )?;
            if plan.remote != remote {
                anyhow::bail!("remote owner identity differs from durable provisioning intent");
            }
            require_requested_locations(
                connection,
                &bootstrap_identity,
                &options.storage_root,
                &plan.storage_root,
                &requested_rift_database,
                &plan.rift_database,
            )?;
            link_bootstrap_request(connection, &bootstrap_identity, repo_key.as_str())?;
            (plan, false)
        } else {
            let source_sha = remote_target_sha(&remote.fetch_url, &options.target)?;
            let policy = read_bootstrap_policy(&bootstrap_path)?;
            let (plan, _, newly_reserved) = reserve_plan(
                connection,
                &options,
                bootstrap_identity,
                remote,
                source_sha,
                policy,
            )?;
            (plan, newly_reserved)
        }
    };
    if newly_reserved {
        stop_after("reservation");
    }

    let _fence = ProvisioningFence::acquire(&plan.storage_root, &plan.repo_key)?;
    if let Some(existing) = find_registered_by_key(connection, &plan.repo_key)? {
        return Ok(existing);
    }
    let mut lifecycle = connection
        .query_row(
            "SELECT lifecycle_json FROM repository_provisioning_intents WHERE repo_key=?1",
            [plan.repo_key.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .context("repository provisioning intent disappeared before ready publication")
        .and_then(|json| serde_json::from_str(&json).context("decode provisioning lifecycle"))?;
    loop {
        verify_completed_effects(connection, &plan, &lifecycle)?;
        lifecycle = match lifecycle {
            ProvisioningLifecycle::Reserved => {
                ensure_staging_directory(&plan)?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "staging_directory",
                    ProvisioningLifecycle::StagingDirectory,
                )?
            }
            ProvisioningLifecycle::StagingDirectory => {
                initialize_git(&plan)?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "git_init",
                    ProvisioningLifecycle::GitInitialized,
                )?
            }
            ProvisioningLifecycle::GitInitialized => {
                configure_remote(&plan)?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "remote",
                    ProvisioningLifecycle::RemoteConfigured,
                )?
            }
            ProvisioningLifecycle::RemoteConfigured => {
                fetch_target(&plan)?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "fetch",
                    ProvisioningLifecycle::TargetFetched,
                )?
            }
            ProvisioningLifecycle::TargetFetched => {
                checkout_target(&plan)?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "checkout",
                    ProvisioningLifecycle::TargetCheckedOut,
                )?
            }
            ProvisioningLifecycle::TargetCheckedOut => {
                publish_root(&plan)?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "root",
                    ProvisioningLifecycle::RootPublished,
                )?
            }
            ProvisioningLifecycle::RootPublished => {
                let policy_sha256 = copy_policy(&plan)?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "policy",
                    ProvisioningLifecycle::PolicyPublished { policy_sha256 },
                )?
            }
            ProvisioningLifecycle::PolicyPublished { policy_sha256 } => {
                provision_rift(&plan.path, Some(&plan.rift_database))?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "rift_init",
                    ProvisioningLifecycle::RiftInitialized { policy_sha256 },
                )?
            }
            ProvisioningLifecycle::RiftInitialized { policy_sha256 } => {
                verify_independent_rift_root(&plan.path, Some(&plan.rift_database))?;
                let registry_identity = rift_registry_identity(Some(&plan.rift_database))?;
                let registry_metadata = fs::metadata(&registry_identity)?;
                let state = RiftVerified {
                    rift: RiftRootIdentity {
                        rift_id: read_rift_id(&plan.path)?,
                        registry_identity,
                        registry_device: registry_metadata.dev(),
                        registry_inode: registry_metadata.ino(),
                        generation: 0,
                    },
                    policy_sha256,
                };
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "rift_proof",
                    ProvisioningLifecycle::RiftVerified(state),
                )?
            }
            ProvisioningLifecycle::RiftVerified(state) => {
                write_owner_marker(
                    &plan.path,
                    &database_id(connection)?,
                    plan.repo_key.as_str(),
                    &state.rift,
                )?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "owner",
                    ProvisioningLifecycle::OwnerPublished(state),
                )?
            }
            ProvisioningLifecycle::OwnerPublished(state) => {
                provision_child_roots(connection, &plan, &state.rift, Some(&plan.rift_database))?;
                advance(
                    connection,
                    &plan,
                    &plan.repo_key,
                    "child_roots",
                    ProvisioningLifecycle::ChildRootsPublished(state),
                )?
            }
            ProvisioningLifecycle::ChildRootsPublished(state) => {
                let repository = owned_repository(&plan, state.rift);
                verify_owned_root(
                    &repository,
                    &database_id(connection)?,
                    Some(&plan.rift_database),
                )?;
                sync_plan_effects(&plan)?;
                stop_after_effect("ready");
                persist_ready(connection, &repository, &plan.created_at)?;
                stop_after("ready");
                return Ok(repository);
            }
        };
    }
}

fn load_intent_by_bootstrap(
    connection: &Connection,
    bootstrap_path: &Path,
) -> Result<Option<(ProvisionPlan, ProvisioningLifecycle)>> {
    let stored = connection
        .query_row(
            "SELECT repo_key,owned_root_path,staging_root_path,rift_registry_path,target_branch,fetch_url,push_url,source_sha,policy_bytes,lifecycle_json,created_at FROM repository_provisioning_intents WHERE bootstrap_path=?1",
            [bootstrap_path.as_os_str().as_bytes()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Option<Vec<u8>>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                ))
            },
        )
        .optional()?;
    stored
        .map(
            |(
                repo_key,
                path,
                staging_path,
                rift_database,
                target,
                fetch_url,
                push_url,
                source_sha,
                policy,
                lifecycle,
                created_at,
            )| {
                validate_target_branch(&target)?;
                crate::control_domain::require_sha(&source_sha, "provisioning source SHA")?;
                let repo_key = RepoKey::from_stored(repo_key)?;
                let path = path_from_bytes(path);
                let plan = ProvisionPlan {
                    storage_root: repository_storage_root(&path, &repo_key)?,
                    repo_key,
                    path,
                    staging_path: path_from_bytes(staging_path),
                    target,
                    remote: RemoteIdentity {
                        fetch_url,
                        push_url,
                    },
                    rift_database: path_from_bytes(rift_database),
                    source_sha,
                    policy,
                    created_at,
                };
                Ok((plan, serde_json::from_str(&lifecycle)?))
            },
        )
        .transpose()
}

fn load_intent_by_repo_key(
    connection: &Connection,
    repo_key: &RepoKey,
) -> Result<Option<(ProvisionPlan, ProvisioningLifecycle)>> {
    connection
        .query_row(
            "SELECT owned_root_path,staging_root_path,rift_registry_path,target_branch,fetch_url,push_url,source_sha,policy_bytes,lifecycle_json,created_at FROM repository_provisioning_intents WHERE repo_key=?1",
            [repo_key.as_str()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<Vec<u8>>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                ))
            },
        )
        .optional()?
        .map(|stored| {
            crate::control_domain::require_sha(&stored.6, "provisioning source SHA")?;
            let path = path_from_bytes(stored.0);
            let plan = ProvisionPlan {
                    storage_root: repository_storage_root(&path, repo_key)?,
                    repo_key: repo_key.clone(),
                    path,
                    staging_path: path_from_bytes(stored.1),
                    rift_database: path_from_bytes(stored.2),
                    target: stored.3,
                    remote: RemoteIdentity {
                        fetch_url: stored.4,
                        push_url: stored.5,
                    },
                    source_sha: stored.6,
                    policy: stored.7,
                    created_at: stored.9,
            };
            Ok((plan, serde_json::from_str(&stored.8)?))
        })
        .transpose()
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn repository_storage_root(path: &Path, repo_key: &RepoKey) -> Result<PathBuf> {
    let repository_directory = path
        .parent()
        .context("owned repository root has no repository directory")?;
    let repositories_directory = repository_directory
        .parent()
        .context("owned repository root has no repositories directory")?;
    if path.file_name() != Some(OsStr::new("root"))
        || repository_directory.file_name() != Some(OsStr::new(repo_key.as_str()))
        || repositories_directory.file_name() != Some(OsStr::new("repositories"))
    {
        anyhow::bail!("owned repository root does not match its repository identity");
    }
    repositories_directory
        .parent()
        .map(Path::to_path_buf)
        .context("owned repository root has no storage root")
}

fn require_requested_locations(
    connection: &Connection,
    request_path: &Path,
    requested_storage: &Path,
    durable_storage: &Path,
    requested_registry: &Path,
    durable_registry: &Path,
) -> Result<()> {
    if requested_storage == durable_storage && requested_registry == durable_registry {
        return Ok(());
    }
    connection.execute(
        "DELETE FROM repository_bootstrap_requests WHERE request_path=?1 AND repo_key IS NULL",
        [request_path.as_os_str().as_bytes()],
    )?;
    anyhow::bail!(
        "remote repository is already bound to owned storage root {} and Rift registry {}",
        durable_storage.display(),
        durable_registry.display()
    )
}

fn prepare_bootstrap_request(
    connection: &mut Connection,
    options: &ProvisionOptions,
    request_path: &Path,
) -> Result<Option<RepoKey>> {
    let storage_root = absolute_path(&options.storage_root)?;
    let rift_database = planned_rift_database(options.rift_database.as_deref())?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let timestamp = chrono::Utc::now().to_rfc3339();
    transaction.execute(
        "INSERT INTO repository_bootstrap_requests(request_path,target_branch,remote_name,storage_root_path,rift_registry_path,repo_key,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,NULL,?6,?6) ON CONFLICT(request_path) DO NOTHING",
        params![request_path.as_os_str().as_bytes(),options.target,options.remote_name,storage_root.as_os_str().as_bytes(),rift_database.as_os_str().as_bytes(),timestamp],
    )?;
    let stored = transaction.query_row(
        "SELECT target_branch,remote_name,storage_root_path,rift_registry_path,repo_key FROM repository_bootstrap_requests WHERE request_path=?1",
        [request_path.as_os_str().as_bytes()],
        |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, Vec<u8>>(2)?,row.get::<_, Vec<u8>>(3)?,row.get::<_, Option<String>>(4)?)),
    )?;
    if stored.0 != options.target {
        anyhow::bail!(
            "bootstrap request is already reserved with immutable target {}",
            stored.0
        );
    }
    if stored.1 != options.remote_name
        || path_from_bytes(stored.2) != storage_root
        || path_from_bytes(stored.3) != rift_database
    {
        anyhow::bail!(
            "bootstrap request identity is already bound to different repository options"
        );
    }
    let repo_key = stored.4.map(RepoKey::from_stored).transpose()?;
    transaction.commit()?;
    Ok(repo_key)
}

fn link_bootstrap_request(
    connection: &Connection,
    request_path: &Path,
    repo_key: &str,
) -> Result<()> {
    let changed = connection.execute(
        "UPDATE repository_bootstrap_requests SET repo_key=?1,updated_at=?2 WHERE request_path=?3 AND (repo_key IS NULL OR repo_key=?1)",
        params![repo_key,chrono::Utc::now().to_rfc3339(),request_path.as_os_str().as_bytes()],
    )?;
    if changed != 1 {
        anyhow::bail!("bootstrap request repository identity changed concurrently");
    }
    Ok(())
}

fn planned_rift_database(explicit: Option<&Path>) -> Result<PathBuf> {
    let configured = explicit
        .map(Path::to_path_buf)
        .or_else(|| std::env::var_os("IQ_RIFT_DATABASE").map(PathBuf::from))
        .unwrap_or_else(|| {
            std::env::var_os("XDG_DATA_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(std::env::var_os("HOME").expect("HOME must be configured"))
                        .join(".local/share")
                })
                .join("rift/rift.sqlite")
        });
    let path = absolute_path(&configured)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => path
            .canonicalize()
            .with_context(|| format!("resolve Rift registry {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().context("Rift registry path has no parent")?;
            let name = path.file_name().context("Rift registry path has no name")?;
            Ok(parent
                .canonicalize()
                .with_context(|| format!("resolve Rift registry parent {}", parent.display()))?
                .join(name))
        }
        Err(error) => {
            Err(error).with_context(|| format!("inspect Rift registry {}", path.display()))
        }
    }
}

fn reserve_plan(
    connection: &mut Connection,
    options: &ProvisionOptions,
    bootstrap_request: PathBuf,
    remote: RemoteIdentity,
    source_sha: String,
    policy: Option<Vec<u8>>,
) -> Result<(ProvisionPlan, ProvisioningLifecycle, bool)> {
    let rift_database = planned_rift_database(options.rift_database.as_deref())?;
    wait_at_reservation_barrier()?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let owner = transaction
        .query_row(
            "SELECT repo_key,target_branch FROM repository_remote_owners WHERE fetch_url=?1 AND push_url=?2",
            params![remote.fetch_url, remote.push_url],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let (repo_key, newly_reserved) = if let Some((repo_key, target)) = owner {
        if target != options.target {
            anyhow::bail!("remote is already reserved with immutable target {target}");
        }
        (RepoKey::from_stored(repo_key)?, false)
    } else {
        let repo_key = RepoKey::new();
        transaction.execute(
            "INSERT INTO repository_remote_owners(repo_key,fetch_url,push_url,target_branch,created_at) VALUES(?1,?2,?3,?4,?5)",
            params![repo_key.as_str(),remote.fetch_url,remote.push_url,options.target,chrono::Utc::now().to_rfc3339()],
        )?;
        (repo_key, true)
    };
    let stored = transaction
        .query_row(
            "SELECT owned_root_path,staging_root_path,rift_registry_path,target_branch,fetch_url,push_url,source_sha,policy_bytes,lifecycle_json,created_at FROM repository_provisioning_intents WHERE repo_key=?1",
            [repo_key.as_str()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<Vec<u8>>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                ))
            },
        )
        .optional()?;
    let (
        storage_root,
        path,
        staging_path,
        rift_database,
        target,
        remote,
        source_sha,
        policy,
        lifecycle,
        created_at,
    ) = if let Some(stored) = stored {
        let path = path_from_bytes(stored.0);
        let storage_root = repository_storage_root(&path, &repo_key)?;
        let durable_rift_database = path_from_bytes(stored.2);
        if storage_root != options.storage_root || durable_rift_database != rift_database {
            transaction.execute(
                "DELETE FROM repository_bootstrap_requests WHERE request_path=?1 AND repo_key IS NULL",
                [bootstrap_request.as_os_str().as_bytes()],
            )?;
            transaction.commit()?;
            anyhow::bail!(
                "remote repository is already bound to owned storage root {} and Rift registry {}",
                storage_root.display(),
                durable_rift_database.display()
            );
        }
        (
            storage_root,
            path,
            path_from_bytes(stored.1),
            durable_rift_database,
            stored.3,
            RemoteIdentity {
                fetch_url: stored.4,
                push_url: stored.5,
            },
            stored.6,
            stored.7,
            serde_json::from_str(&stored.8)?,
            stored.9,
        )
    } else {
        if !newly_reserved {
            anyhow::bail!(
                "remote reservation has neither provisioning intent nor ready repository"
            );
        }
        let parent = options
            .storage_root
            .join("repositories")
            .join(repo_key.as_str());
        let path = parent.join("root");
        let staging_path = parent.join(".root.tmp");
        let lifecycle = ProvisioningLifecycle::Reserved;
        let created_at = chrono::Utc::now().to_rfc3339();
        transaction.execute(
            "INSERT INTO repository_provisioning_intents(repo_key,bootstrap_path,owned_root_path,staging_root_path,rift_registry_path,target_branch,fetch_url,push_url,source_sha,policy_bytes,lifecycle_json,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?12)",
            params![repo_key.as_str(),bootstrap_request.as_os_str().as_bytes(),path.as_os_str().as_bytes(),staging_path.as_os_str().as_bytes(),rift_database.as_os_str().as_bytes(),options.target,remote.fetch_url,remote.push_url,source_sha,policy,serde_json::to_string(&lifecycle)?,created_at],
        )?;
        (
            options.storage_root.clone(),
            path,
            staging_path,
            rift_database,
            options.target.clone(),
            remote,
            source_sha,
            policy,
            lifecycle,
            created_at,
        )
    };
    let linked = transaction.execute(
        "UPDATE repository_bootstrap_requests SET repo_key=?1,updated_at=?2 WHERE request_path=?3 AND (repo_key IS NULL OR repo_key=?1)",
        params![repo_key.as_str(),chrono::Utc::now().to_rfc3339(),bootstrap_request.as_os_str().as_bytes()],
    )?;
    if linked != 1 {
        anyhow::bail!("bootstrap request repository identity changed during reservation");
    }
    transaction.commit()?;
    Ok((
        ProvisionPlan {
            repo_key,
            storage_root,
            path,
            staging_path,
            target,
            remote,
            rift_database,
            source_sha,
            policy,
            created_at,
        },
        lifecycle,
        newly_reserved,
    ))
}

#[cfg(debug_assertions)]
fn wait_at_reservation_barrier() -> Result<()> {
    let Some(directory) = std::env::var_os("IQ_TEST_RESERVATION_BARRIER") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    let parties = std::env::var("IQ_TEST_RESERVATION_BARRIER_PARTIES")
        .context("reservation barrier party count is required")?
        .parse::<usize>()
        .context("reservation barrier party count must be an integer")?;
    if parties < 2 {
        anyhow::bail!("reservation barrier needs at least two parties");
    }
    fs::write(
        directory.join(format!("{}-{}", std::process::id(), Uuid::new_v4())),
        b"ready\n",
    )?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if fs::read_dir(&directory)?.count() >= parties {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("reservation barrier timed out");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(not(debug_assertions))]
fn wait_at_reservation_barrier() -> Result<()> {
    Ok(())
}

fn advance(
    connection: &Connection,
    plan: &ProvisionPlan,
    repo_key: &RepoKey,
    boundary: &str,
    lifecycle: ProvisioningLifecycle,
) -> Result<ProvisioningLifecycle> {
    sync_plan_effects(plan)?;
    stop_after_effect(boundary);
    update_lifecycle(connection, repo_key, &lifecycle)?;
    stop_after(boundary);
    Ok(lifecycle)
}

#[cfg(debug_assertions)]
fn stop_after_effect(boundary: &str) {
    if std::env::var("IQ_TEST_PROVISION_STOP_AFTER_EFFECT").as_deref() == Ok(boundary) {
        std::process::exit(85);
    }
}

#[cfg(not(debug_assertions))]
fn stop_after_effect(_boundary: &str) {}

#[cfg(debug_assertions)]
fn stop_after(boundary: &str) {
    if std::env::var("IQ_TEST_PROVISION_STOP_AFTER").as_deref() == Ok(boundary) {
        std::process::exit(86);
    }
}

#[cfg(not(debug_assertions))]
fn stop_after(_boundary: &str) {}

fn verify_completed_effects(
    connection: &Connection,
    plan: &ProvisionPlan,
    lifecycle: &ProvisioningLifecycle,
) -> Result<()> {
    match lifecycle {
        ProvisioningLifecycle::Reserved => {}
        ProvisioningLifecycle::StagingDirectory => verify_staging_directory(plan)?,
        ProvisioningLifecycle::GitInitialized => verify_initialized_git(plan)?,
        ProvisioningLifecycle::RemoteConfigured => {
            verify_initialized_git(plan)?;
            verify_remote(&plan.staging_path, &plan.remote)?;
        }
        ProvisioningLifecycle::TargetFetched => verify_fetched_target(plan)?,
        ProvisioningLifecycle::TargetCheckedOut => {
            match (entry_exists(&plan.path)?, entry_exists(&plan.staging_path)?) {
                (false, true) => verify_git_at(plan, &plan.staging_path, false)?,
                (true, false) => verify_git_at(plan, &plan.path, false)?,
                (true, true) => {
                    anyhow::bail!("owned repository root and staging root both exist")
                }
                (false, false) => {
                    anyhow::bail!("owned repository root publication lost both paths")
                }
            }
        }
        ProvisioningLifecycle::RootPublished => verify_published_root(plan)?,
        ProvisioningLifecycle::PolicyPublished { policy_sha256 } => {
            verify_published_root(plan)?;
            verify_published_policy(plan, policy_sha256.as_deref())?;
        }
        ProvisioningLifecycle::RiftInitialized { policy_sha256 } => {
            verify_published_root(plan)?;
            verify_published_policy(plan, policy_sha256.as_deref())?;
            read_rift_id(&plan.path)?;
            verify_independent_rift_root(&plan.path, Some(&plan.rift_database))?;
        }
        ProvisioningLifecycle::RiftVerified(state) => {
            verify_rift_proof(plan, state)?;
        }
        ProvisioningLifecycle::OwnerPublished(state) => {
            verify_rift_proof(plan, state)?;
            verify_owner_marker(
                &plan.path,
                &database_id(connection)?,
                plan.repo_key.as_str(),
                &state.rift,
            )?;
        }
        ProvisioningLifecycle::ChildRootsPublished(state) => {
            verify_rift_proof(plan, state)?;
            verify_owner_marker(
                &plan.path,
                &database_id(connection)?,
                plan.repo_key.as_str(),
                &state.rift,
            )?;
            verify_child_roots(connection, plan, &state.rift)?;
        }
    }
    Ok(())
}

fn verify_staging_directory(plan: &ProvisionPlan) -> Result<()> {
    if entry_exists(&plan.path)? {
        anyhow::bail!("owned repository root appeared before publication");
    }
    require_real_directory(&plan.staging_path, "owned repository staging directory")?;
    Ok(())
}

fn verify_initialized_git(plan: &ProvisionPlan) -> Result<()> {
    if entry_exists(&plan.path)? {
        anyhow::bail!("owned repository root appeared before publication");
    }
    require_real_directory(
        &plan.staging_path.join(".git"),
        "owned repository Git directory",
    )?;
    let git = open_directory(
        &plan.staging_path.join(".git"),
        "owned repository Git directory",
    )?;
    read_regular_file_at(
        &git,
        OsStr::new("iq-operation.lock"),
        0,
        "repository operation lock",
    )?;
    if !git_text(&plan.staging_path, ["remote"])?.is_empty()
        && git_text(&plan.staging_path, ["remote"])? != INTERNAL_REMOTE_NAME
    {
        anyhow::bail!("owned repository staging checkout has unexpected remotes");
    }
    Ok(())
}

fn verify_fetched_target(plan: &ProvisionPlan) -> Result<()> {
    verify_remote(&plan.staging_path, &plan.remote)?;
    if !git_object_exists(&plan.staging_path, &plan.source_sha)?
        || git_text(
            &plan.staging_path,
            ["rev-parse", &format!("{}^{{commit}}", plan.source_sha)],
        )? != plan.source_sha
    {
        anyhow::bail!("fetched target differs from durable provisioning intent");
    }
    Ok(())
}

fn verify_published_root(plan: &ProvisionPlan) -> Result<()> {
    if entry_exists(&plan.staging_path)? {
        anyhow::bail!("published repository retained stale staging residue");
    }
    verify_git_at(plan, &plan.path, false)
}

fn verify_published_policy(plan: &ProvisionPlan, expected_digest: Option<&str>) -> Result<()> {
    let actual = read_owned_policy(&plan.path)?;
    if actual != plan.policy {
        anyhow::bail!("owned repository policy differs from durable provisioning intent");
    }
    let digest = actual
        .as_ref()
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)));
    if digest.as_deref() != expected_digest {
        anyhow::bail!("owned repository policy digest differs from persisted phase");
    }
    if !read_exclude(&plan.path)?
        .lines()
        .any(|line| line == POLICY_PATH)
    {
        anyhow::bail!("owned repository policy exclusion is missing");
    }
    Ok(())
}

fn verify_rift_proof(plan: &ProvisionPlan, state: &RiftVerified) -> Result<()> {
    verify_published_root(plan)?;
    verify_published_policy(plan, state.policy_sha256.as_deref())?;
    verify_independent_rift_root(&plan.path, Some(&plan.rift_database))?;
    if read_rift_id(&plan.path)? != state.rift.rift_id
        || rift_registry_identity(Some(&plan.rift_database))? != state.rift.registry_identity
    {
        anyhow::bail!("owned repository Rift proof differs from persisted phase");
    }
    let registry = fs::symlink_metadata(&state.rift.registry_identity)?;
    if registry.file_type().is_symlink()
        || !registry.is_file()
        || registry.dev() != state.rift.registry_device
        || registry.ino() != state.rift.registry_inode
        || state.rift.generation != 0
    {
        anyhow::bail!("owned repository Rift registry proof changed");
    }
    Ok(())
}

fn sync_plan_effects(plan: &ProvisionPlan) -> Result<()> {
    let active = if entry_exists(&plan.path)? {
        Some(plan.path.as_path())
    } else if entry_exists(&plan.staging_path)? {
        Some(plan.staging_path.as_path())
    } else {
        None
    };
    if let Some(active) = active {
        File::open(active)?.sync_all()?;
        if entry_exists(&active.join(".git"))? {
            File::open(active.join(".git"))?.sync_all()?;
        }
        File::open(
            active
                .parent()
                .context("repository effect path has no parent")?,
        )?
        .sync_all()?;
    }
    Ok(())
}

fn owned_repository(plan: &ProvisionPlan, rift: RiftRootIdentity) -> OwnedRepositoryRoot {
    OwnedRepositoryRoot {
        repo_key: plan.repo_key.clone(),
        path: plan.path.clone(),
        rift,
        remote: plan.remote.clone(),
        target: plan.target.clone(),
        children: child_roots(plan),
        source_sha: plan.source_sha.clone(),
    }
}

fn child_roots(plan: &ProvisionPlan) -> ChildWorkspaceRoots {
    ChildWorkspaceRoots {
        development: plan
            .path
            .parent()
            .expect("planned root has a parent")
            .join("development"),
        integration: plan
            .path
            .parent()
            .expect("planned root has a parent")
            .join("integration"),
    }
}

pub(crate) fn validate_provisioning_rows(connection: &Connection) -> Result<()> {
    let mut requests = connection.prepare(
        "SELECT request_path,target_branch,remote_name,storage_root_path,rift_registry_path,repo_key FROM repository_bootstrap_requests",
    )?;
    let request_rows = requests.query_map([], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
    for row in request_rows {
        let (request, target, remote, storage, registry, repo_key) = row?;
        if !path_from_bytes(request).is_absolute()
            || !path_from_bytes(storage).is_absolute()
            || !path_from_bytes(registry).is_absolute()
            || remote.is_empty()
        {
            anyhow::bail!("repository bootstrap request authority is invalid");
        }
        validate_target_branch(&target)?;
        repo_key.map(RepoKey::from_stored).transpose()?;
    }
    let mut statement = connection.prepare(
        "SELECT repo_key,owned_root_path,staging_root_path,rift_registry_path,target_branch,source_sha,policy_bytes,lifecycle_json FROM repository_provisioning_intents",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<Vec<u8>>>(6)?,
            row.get::<_, String>(7)?,
        ))
    })?;
    for row in rows {
        let (repo_key, root, staging, rift_database, target, source_sha, policy, lifecycle) = row?;
        RepoKey::from_stored(repo_key)?;
        let root = path_from_bytes(root);
        let staging = path_from_bytes(staging);
        let rift_database = path_from_bytes(rift_database);
        if !root.is_absolute()
            || root.file_name() != Some(OsStr::new("root"))
            || staging != root.with_file_name(".root.tmp")
            || !rift_database.is_absolute()
        {
            anyhow::bail!("repository provisioning paths are not canonical planned paths");
        }
        validate_target_branch(&target)?;
        crate::control_domain::require_sha(&source_sha, "provisioning source SHA")?;
        let lifecycle: ProvisioningLifecycle = serde_json::from_str(&lifecycle)?;
        let persisted_digest = match &lifecycle {
            ProvisioningLifecycle::PolicyPublished { policy_sha256 }
            | ProvisioningLifecycle::RiftInitialized { policy_sha256 } => {
                Some(policy_sha256.as_ref())
            }
            ProvisioningLifecycle::RiftVerified(state)
            | ProvisioningLifecycle::OwnerPublished(state)
            | ProvisioningLifecycle::ChildRootsPublished(state) => {
                if !state.rift.registry_identity.is_absolute()
                    || state.rift.registry_inode == 0
                    || state.rift.generation != 0
                {
                    anyhow::bail!("repository provisioning Rift authority is invalid");
                }
                Some(state.policy_sha256.as_ref())
            }
            _ => None,
        };
        if let Some(persisted_digest) = persisted_digest {
            let actual_digest = policy
                .as_ref()
                .map(|bytes| format!("{:x}", Sha256::digest(bytes)));
            if persisted_digest.map(String::as_str) != actual_digest.as_deref() {
                anyhow::bail!("repository provisioning policy digest is invalid");
            }
        }
    }
    Ok(())
}

fn persist_ready(
    connection: &mut Connection,
    repository: &OwnedRepositoryRoot,
    created_at: &str,
) -> Result<()> {
    let transaction = connection.transaction()?;
    let timestamp = chrono::Utc::now().to_rfc3339();
    transaction.execute(
        "DELETE FROM repository_provisioning_intents WHERE repo_key=?1",
        [repository.repo_key.as_str()],
    )?;
    transaction.execute(
        "INSERT INTO registered_repositories(repo_key,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode,generation,remote_name,fetch_url,push_url,target_branch,source_sha,checkout_json,development_root_path,integration_root_path,provisioning_json,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,'iq-target',?8,?9,?10,?11,?12,?13,?14,'{\"state\":\"ready\"}',?15,?16)",
        params![repository.repo_key.as_str(),repository.path.as_os_str().as_bytes(),repository.rift.rift_id,repository.rift.registry_identity.as_os_str().as_bytes(),repository.rift.registry_device,repository.rift.registry_inode,repository.rift.generation,repository.remote.fetch_url,repository.remote.push_url,repository.target,repository.source_sha,serde_json::to_string(&crate::sqlite::CheckoutReconciliationState::ready(&repository.source_sha)?)?,repository.children.development.as_os_str().as_bytes(),repository.children.integration.as_os_str().as_bytes(),created_at,timestamp],
    )?;
    for (kind, path) in [
        ("development", &repository.children.development),
        ("integration", &repository.children.integration),
    ] {
        transaction.execute(
            "INSERT INTO workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode,generation) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![repository.repo_key.as_str(),kind,path.as_os_str().as_bytes(),repository.path.as_os_str().as_bytes(),repository.rift.rift_id,repository.rift.registry_identity.as_os_str().as_bytes(),repository.rift.registry_device,repository.rift.registry_inode,repository.rift.generation],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn find_registered(
    connection: &Connection,
    remote: &RemoteIdentity,
) -> Result<Option<OwnedRepositoryRoot>> {
    connection
        .query_row(
            "SELECT repo_key,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode,generation,fetch_url,push_url,target_branch,source_sha,development_root_path,integration_root_path FROM registered_repositories WHERE fetch_url=?1 AND push_url=?2",
            params![remote.fetch_url,remote.push_url],
            map_owned_root,
        )
        .optional()
        .map_err(Into::into)
}

fn find_remote_owner(
    connection: &Connection,
    remote: &RemoteIdentity,
) -> Result<Option<(RepoKey, String)>> {
    connection
        .query_row(
            "SELECT repo_key,target_branch FROM repository_remote_owners WHERE fetch_url=?1 AND push_url=?2",
            params![remote.fetch_url, remote.push_url],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .map(|(repo_key, target)| Ok((RepoKey::from_stored(repo_key)?, target)))
        .transpose()
}

fn find_registered_by_key(
    connection: &Connection,
    repo_key: &RepoKey,
) -> Result<Option<OwnedRepositoryRoot>> {
    connection
        .query_row(
            "SELECT repo_key,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode,generation,fetch_url,push_url,target_branch,source_sha,development_root_path,integration_root_path FROM registered_repositories WHERE repo_key=?1",
            [repo_key.as_str()],
            map_owned_root,
        )
        .optional()
        .map_err(Into::into)
}

fn map_owned_root(row: &rusqlite::Row<'_>) -> rusqlite::Result<OwnedRepositoryRoot> {
    let target: String = row.get(9)?;
    Ok(OwnedRepositoryRoot {
        repo_key: RepoKey::from_stored(row.get::<_, String>(0)?).map_err(sql_conversion_error)?,
        path: path_from_bytes(row.get(1)?),
        rift: RiftRootIdentity {
            rift_id: row.get(2)?,
            registry_identity: path_from_bytes(row.get(3)?),
            registry_device: row.get(4)?,
            registry_inode: row.get(5)?,
            generation: row.get(6)?,
        },
        remote: RemoteIdentity {
            fetch_url: row.get(7)?,
            push_url: row.get(8)?,
        },
        target: validate_target_branch(&target)
            .map(str::to_string)
            .map_err(sql_conversion_error)?,
        source_sha: row.get(10)?,
        children: ChildWorkspaceRoots {
            development: path_from_bytes(row.get(11)?),
            integration: path_from_bytes(row.get(12)?),
        },
    })
}

fn sql_conversion_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}

fn path_from_bytes(bytes: Vec<u8>) -> PathBuf {
    PathBuf::from(std::ffi::OsString::from_vec(bytes))
}

fn database_id(connection: &Connection) -> Result<String> {
    let id: String = connection.query_row(
        "SELECT value FROM queue_metadata WHERE key='database_id'",
        [],
        |row| row.get(0),
    )?;
    if id.is_empty() {
        anyhow::bail!("database ID must not be empty");
    }
    Ok(id)
}

fn ensure_staging_directory(plan: &ProvisionPlan) -> Result<()> {
    let parent = plan.path.parent().context("owned root has no parent")?;
    let repositories_path = parent
        .parent()
        .context("owned repository reservation has no repositories parent")?;
    let storage_path = repositories_path
        .parent()
        .context("owned repository reservation has no storage parent")?;
    let storage = open_directory(storage_path, "owned repository storage root")?;
    let repositories = ensure_directory_child(
        &storage,
        OsStr::new("repositories"),
        "owned repository collection",
    )?;
    let reservation = ensure_directory_child(
        &repositories,
        OsStr::new(plan.repo_key.as_str()),
        "owned repository reservation",
    )?;
    let reservation_metadata = reservation.metadata()?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.dev() != reservation_metadata.dev()
        || parent_metadata.ino() != reservation_metadata.ino()
    {
        anyhow::bail!("owned repository reservation path identity changed");
    }
    if entry_exists(&plan.path)? {
        anyhow::bail!("owned repository root appeared before publication");
    }
    match fs::symlink_metadata(&plan.staging_path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            anyhow::bail!("owned repository staging path is not a real directory")
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&plan.staging_path)?;
            File::open(parent)?.sync_all()?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn initialize_git(plan: &ProvisionPlan) -> Result<()> {
    ensure_staging_directory(plan)?;
    let git_path = plan.staging_path.join(".git");
    if entry_exists(&git_path)? {
        require_real_directory(&git_path, "owned repository Git directory")?;
        for entry in fs::read_dir(&plan.staging_path)? {
            if entry?.file_name() != OsStr::new(".git") {
                anyhow::bail!("partially initialized staging directory has unknown content");
            }
        }
    } else if fs::read_dir(&plan.staging_path)?
        .next()
        .transpose()?
        .is_some()
    {
        anyhow::bail!("uninitialized owned repository staging directory is not empty");
    }
    git(&plan.staging_path, ["init"], "initialize owned repository")?;
    require_real_directory(&git_path, "owned repository Git directory")?;
    if !git_text(&plan.staging_path, ["remote"])?.is_empty() {
        anyhow::bail!("owned repository gained a remote before remote configuration");
    }
    ensure_operation_lock(&plan.staging_path)?;
    verify_initialized_git(plan)
}

fn configure_remote(plan: &ProvisionPlan) -> Result<()> {
    require_real_directory(
        &plan.staging_path.join(".git"),
        "owned repository Git directory",
    )?;
    let remotes = git_text(&plan.staging_path, ["remote"])?;
    match remotes.as_str() {
        "" => git(
            &plan.staging_path,
            [
                "remote",
                "add",
                INTERNAL_REMOTE_NAME,
                &plan.remote.fetch_url,
            ],
            "record fixed fetch remote",
        )?,
        INTERNAL_REMOTE_NAME => {}
        _ => anyhow::bail!("owned repository staging checkout has unexpected remotes"),
    }
    git(
        &plan.staging_path,
        [
            "remote",
            "set-url",
            INTERNAL_REMOTE_NAME,
            &plan.remote.fetch_url,
        ],
        "record fixed fetch remote",
    )?;
    git(
        &plan.staging_path,
        [
            "remote",
            "set-url",
            "--push",
            INTERNAL_REMOTE_NAME,
            &plan.remote.push_url,
        ],
        "record fixed push remote",
    )?;
    verify_remote(&plan.staging_path, &plan.remote)
}

fn fetch_target(plan: &ProvisionPlan) -> Result<()> {
    verify_remote(&plan.staging_path, &plan.remote)?;
    if !git_object_exists(&plan.staging_path, &plan.source_sha)? {
        git(
            &plan.staging_path,
            ["fetch", "--no-tags", INTERNAL_REMOTE_NAME, &plan.source_sha],
            "fetch exact target commit",
        )?;
    }
    let fetched = git_text(
        &plan.staging_path,
        ["rev-parse", &format!("{}^{{commit}}", plan.source_sha)],
    )?;
    if fetched != plan.source_sha {
        anyhow::bail!("fetched target SHA differs from provisioning intent");
    }
    Ok(())
}

fn checkout_target(plan: &ProvisionPlan) -> Result<()> {
    git(
        &plan.staging_path,
        ["checkout", "-B", &plan.target, &plan.source_sha],
        "checkout exact target",
    )?;
    git(
        &plan.staging_path,
        ["reset", "--hard", &plan.source_sha],
        "reset exact target index and worktree",
    )?;
    git(
        &plan.staging_path,
        ["clean", "-ffdx"],
        "remove staging checkout residue",
    )?;
    git(
        &plan.staging_path,
        [
            "update-ref",
            &format!("refs/remotes/{}/{}", INTERNAL_REMOTE_NAME, plan.target),
            &plan.source_sha,
        ],
        "publish exact fetched target ref",
    )?;
    verify_git_at(plan, &plan.staging_path, false)
}

fn publish_root(plan: &ProvisionPlan) -> Result<()> {
    let root_exists = entry_exists(&plan.path)?;
    let staging_exists = entry_exists(&plan.staging_path)?;
    match (root_exists, staging_exists) {
        (false, true) => {
            verify_git_at(plan, &plan.staging_path, false)?;
            rename_noreplace(
                &plan.staging_path,
                &plan.path,
                "publish owned repository root",
            )?;
            File::open(plan.path.parent().context("owned root has no parent")?)?.sync_all()?;
        }
        (true, false) => {}
        (true, true) => anyhow::bail!("owned repository root and staging root both exist"),
        (false, false) => anyhow::bail!("owned repository root publication lost both paths"),
    }
    verify_git_at(plan, &plan.path, false)
}

fn verify_git(plan: &ProvisionPlan) -> Result<()> {
    verify_git_at(plan, &plan.path, true)
}

fn verify_git_at(plan: &ProvisionPlan, path: &Path, require_exclusion: bool) -> Result<()> {
    require_real_directory(&path.join(".git"), "owned repository Git directory")?;
    if !path.join(".git").is_dir() {
        anyhow::bail!("owned repository is not a full Git checkout");
    }
    if entry_exists(&path.join(".git/objects/info/alternates"))? {
        anyhow::bail!("owned repository must not use Git alternates");
    }
    let git_directory = open_directory(&path.join(".git"), "owned repository Git directory")?;
    if !read_regular_file_at(
        &git_directory,
        OsStr::new("iq-operation.lock"),
        0,
        "repository operation lock",
    )?
    .is_empty()
    {
        anyhow::bail!("repository operation lock contains unexpected data");
    }
    if git_text(path, ["rev-parse", "HEAD"])? != plan.source_sha {
        anyhow::bail!("owned repository HEAD differs from provisioning intent");
    }
    verify_exact_checkout(path, &plan.source_sha)?;
    if git_text(path, ["symbolic-ref", "--short", "HEAD"])? != plan.target {
        anyhow::bail!("owned repository is not on its target branch");
    }
    verify_remote(path, &plan.remote)?;
    if require_exclusion && !read_exclude(path)?.lines().any(|line| line == POLICY_PATH) {
        anyhow::bail!("owned repository does not exclude its local policy");
    }
    reject_tracked_policy(path)
}

fn ensure_operation_lock(path: &Path) -> Result<()> {
    let git = open_directory(&path.join(".git"), "owned repository Git directory")?;
    publish_file_noreplace(
        &git,
        OsStr::new("iq-operation.lock"),
        b"",
        "repository operation lock",
    )
}

fn copy_policy(plan: &ProvisionPlan) -> Result<Option<String>> {
    ensure_policy_exclusion(&plan.path)?;
    reject_tracked_policy(&plan.path)?;
    let root = open_directory(&plan.path, "owned repository root")?;
    let iq = open_optional_directory_child(&root, OsStr::new(".iq"), "owned policy directory")?;
    let Some(policy) = &plan.policy else {
        if let Some(iq) = iq {
            if entry_exists_at(&iq, OsStr::new("config.json"))? {
                anyhow::bail!("owned repository has policy absent from provisioning input");
            }
        }
        return Ok(None);
    };
    let iq = match iq {
        Some(iq) => iq,
        None => ensure_directory_child(&root, OsStr::new(".iq"), "owned policy directory")?,
    };
    publish_file_noreplace(
        &iq,
        OsStr::new("config.json"),
        policy,
        "owned repository policy",
    )?;
    Ok(Some(format!("{:x}", Sha256::digest(policy))))
}

fn ensure_policy_exclusion(root: &Path) -> Result<()> {
    let git = open_directory(&root.join(".git"), "owned repository Git directory")?;
    let info = open_directory_child(&git, OsStr::new("info"), "Git info directory")?;
    let excludes = String::from_utf8(read_regular_file_at(
        &info,
        OsStr::new("exclude"),
        1024 * 1024,
        "Git exclude file",
    )?)?;
    if !excludes.lines().any(|line| line == POLICY_PATH) {
        let mut updated = excludes.into_bytes();
        if !updated.is_empty() && !updated.ends_with(b"\n") {
            updated.push(b'\n');
        }
        updated.extend_from_slice(format!("{POLICY_PATH}\n").as_bytes());
        replace_file_atomically(&info, OsStr::new("exclude"), &updated, "Git exclude file")?;
    }
    Ok(())
}

fn read_exclude(root: &Path) -> Result<String> {
    let git = open_directory(&root.join(".git"), "owned repository Git directory")?;
    let info = open_directory_child(&git, OsStr::new("info"), "Git info directory")?;
    String::from_utf8(read_regular_file_at(
        &info,
        OsStr::new("exclude"),
        1024 * 1024,
        "Git exclude file",
    )?)
    .context("Git exclude file is not UTF-8")
}

fn provision_rift(path: &Path, database: Option<&Path>) -> Result<()> {
    let database_path = database
        .map(Path::to_path_buf)
        .or_else(|| std::env::var_os("IQ_RIFT_DATABASE").map(PathBuf::from))
        .context("provisioning requires an exact Rift registry path")?;
    let path_text = path
        .to_str()
        .context("owned repository root path is not UTF-8")?;
    let mut root = open_directory(path, "owned repository root")?;
    let marker = if entry_exists_at(&root, OsStr::new(".rift"))? {
        let bytes = read_regular_file_at(&root, OsStr::new(".rift"), 128, "Rift identity marker")?;
        String::from_utf8(bytes)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| {
                value.len() == 26 && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
    } else {
        None
    };
    let registry_row = if entry_exists(&database_path)? {
        let registry = Connection::open_with_flags(
            &database_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        let has_rift: bool = registry.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='rift')",
            [],
            |row| row.get(0),
        )?;
        if has_rift {
            registry
                .query_row(
                    "SELECT id,parent_id FROM rift WHERE path=?1",
                    [path_text],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .optional()?
        } else {
            None
        }
    } else {
        None
    };
    match (marker.as_deref(), registry_row.as_ref()) {
        (Some(marker), Some((registered, None))) if marker == registered => {}
        (_, Some((registered, None))) => {
            if entry_exists_at(&root, OsStr::new(".rift"))? {
                fs::remove_file(path.join(".rift"))?;
                root.sync_all()?;
            }
            publish_file_noreplace(
                &root,
                OsStr::new(".rift"),
                format!("{registered}\n").as_bytes(),
                "recovered Rift identity marker",
            )?;
        }
        (_, Some((_, Some(_)))) => {
            anyhow::bail!("owned repository Rift registry row has an unexpected parent")
        }
        (_, None) => {
            if let Some(marker) = marker.as_deref() {
                let marker_elsewhere = if entry_exists(&database_path)? {
                    let registry = Connection::open_with_flags(
                        &database_path,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
                    )?;
                    let has_rift: bool = registry.query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='rift')",
                        [],
                        |row| row.get(0),
                    )?;
                    has_rift
                        && registry.query_row(
                            "SELECT EXISTS(SELECT 1 FROM rift WHERE id=?1 AND path!=?2)",
                            params![marker, path_text],
                            |row| row.get(0),
                        )?
                } else {
                    false
                };
                if marker_elsewhere {
                    anyhow::bail!("Rift marker identity belongs to a different registry path");
                }
            }
            if entry_exists_at(&root, OsStr::new(".rift"))? {
                fs::remove_file(path.join(".rift"))?;
                root.sync_all()?;
            }
            let mut command = Command::new("rift");
            if let Some(database) = database {
                command.args([
                    "--database",
                    database
                        .to_str()
                        .context("Rift database path is not UTF-8")?,
                ]);
            }
            let output = command.args(["init", "--here"]).arg(path).output()?;
            require_success(output, "initialize independent Rift root")?;
            root = open_directory(path, "initialized owned repository root")?;
        }
    }
    read_regular_file_at(&root, OsStr::new(".rift"), 128, "Rift identity marker")?;
    let verified = Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?
    .query_row(
        "SELECT id,parent_id FROM rift WHERE path=?1",
        [path_text],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
    )?;
    if verified.1.is_some() || read_rift_id(path)? != verified.0 {
        anyhow::bail!("reconciled Rift root differs from exact registry authority");
    }
    Ok(())
}

fn verify_independent_rift_root(path: &Path, database: Option<&Path>) -> Result<()> {
    let ancestors = rift_output(
        database,
        [
            "ancestors",
            path.to_str().context("owned root path is not UTF-8")?,
        ],
    )?;
    if !String::from_utf8(ancestors.stdout)?.trim().is_empty() {
        anyhow::bail!("owned repository Rift root has ancestors");
    }
    Ok(())
}

fn verify_owned_root(
    repository: &OwnedRepositoryRoot,
    database_id: &str,
    database: Option<&Path>,
) -> Result<()> {
    let plan = ProvisionPlan {
        storage_root: repository_storage_root(&repository.path, &repository.repo_key)?,
        repo_key: repository.repo_key.clone(),
        path: repository.path.clone(),
        staging_path: repository.path.with_file_name(".root.tmp"),
        target: repository.target.clone(),
        remote: repository.remote.clone(),
        rift_database: database
            .map(Path::to_path_buf)
            .unwrap_or_else(|| repository.rift.registry_identity.clone()),
        source_sha: repository.source_sha.clone(),
        policy: read_owned_policy(&repository.path)?,
        created_at: String::new(),
    };
    verify_git(&plan)?;
    verify_independent_rift_root(&repository.path, database)?;
    if read_rift_id(&repository.path)? != repository.rift.rift_id {
        anyhow::bail!("owned repository Rift identity changed");
    }
    if rift_registry_identity(database)? != repository.rift.registry_identity {
        anyhow::bail!("owned repository Rift registry identity changed");
    }
    let registry_metadata = fs::symlink_metadata(&repository.rift.registry_identity)?;
    if registry_metadata.file_type().is_symlink()
        || !registry_metadata.is_file()
        || registry_metadata.dev() != repository.rift.registry_device
        || registry_metadata.ino() != repository.rift.registry_inode
    {
        anyhow::bail!("owned repository Rift registry file identity changed");
    }
    verify_owner_marker(
        &repository.path,
        database_id,
        repository.repo_key.as_str(),
        &repository.rift,
    )?;
    Ok(())
}

pub(crate) fn verify_registered_repository(
    repository: &crate::sqlite::RegisteredRepository,
    database_id: &str,
) -> Result<()> {
    let target = validate_target_branch(&repository.target_branch)?.to_string();
    let head = git_text(&repository.owned_root_path, ["rev-parse", "HEAD"])?;
    let checkout_target = repository.checkout_reconciliation.target_sha();
    let head_allowed = if repository
        .checkout_reconciliation
        .is_ready_for(&repository.source_sha)
    {
        head == repository.source_sha
    } else {
        head == repository.source_sha || head == checkout_target
    };
    if !head_allowed {
        anyhow::bail!("owned repository HEAD differs from durable checkout authority");
    }
    let owned = OwnedRepositoryRoot {
        repo_key: RepoKey::from_stored(repository.key.clone())?,
        path: repository.owned_root_path.clone(),
        rift: RiftRootIdentity {
            rift_id: repository.root_rift_id.clone(),
            registry_identity: repository.registry_identity.clone(),
            registry_device: repository.registry_device,
            registry_inode: repository.registry_inode,
            generation: u64::try_from(repository.generation)
                .context("owned repository generation is negative")?,
        },
        remote: RemoteIdentity {
            fetch_url: repository.remote.fetch_url.clone(),
            push_url: repository.remote.push_url.clone(),
        },
        target,
        children: ChildWorkspaceRoots {
            development: repository.development_root_path.clone(),
            integration: repository.integration_root_path.clone(),
        },
        source_sha: head,
    };
    verify_owned_root(&owned, database_id, Some(&repository.registry_identity))?;
    let target_ref = format!(
        "refs/remotes/{}/{}",
        INTERNAL_REMOTE_NAME, repository.target_branch
    );
    let remote_sha = git_text(&repository.owned_root_path, ["rev-parse", &target_ref])?;
    let allowed = if repository
        .checkout_reconciliation
        .is_ready_for(&repository.source_sha)
    {
        remote_sha == repository.source_sha
    } else {
        remote_sha == repository.source_sha || remote_sha == checkout_target
    };
    if !allowed {
        anyhow::bail!("owned repository target ref differs from durable checkout authority");
    }
    Ok(())
}

fn provision_child_roots(
    connection: &Connection,
    plan: &ProvisionPlan,
    rift: &RiftRootIdentity,
    rift_database: Option<&Path>,
) -> Result<()> {
    let children = child_roots(plan);
    let queue_id = database_id(connection)?;
    for (kind, path) in [
        ("development", &children.development),
        ("integration", &children.integration),
    ] {
        if entry_exists(path)? {
            require_real_directory(path, &format!("{kind} child root"))?;
        } else {
            publish_directory_noreplace(path, kind)?;
        }
        let manager = crate::integrator::RiftWorkspaceManager::claim(
            plan.path.clone(),
            path.clone(),
            plan.repo_key.as_str().to_string(),
            kind,
            rift_database.map(Path::to_path_buf),
            &queue_id,
            0,
        )?;
        if manager.source_id() != rift.rift_id
            || Path::new(manager.registry_identity()) != rift.registry_identity
            || manager.root() != path
        {
            anyhow::bail!("{kind} child-root authority differs from owned root authority");
        }
    }
    Ok(())
}

fn verify_child_roots(
    connection: &Connection,
    plan: &ProvisionPlan,
    rift: &RiftRootIdentity,
) -> Result<()> {
    let children = child_roots(plan);
    let queue_id = database_id(connection)?;
    for (kind, path) in [
        ("development", children.development),
        ("integration", children.integration),
    ] {
        crate::integrator::RiftWorkspaceManager::inspect(
            plan.path.clone(),
            path,
            plan.repo_key.as_str().to_string(),
            kind,
            Some(rift.registry_identity.clone()),
            &queue_id,
            crate::sqlite::WorkspaceGenerationState::Ready {
                current: i64::try_from(rift.generation).context("Rift generation is too large")?,
            },
        )
        .with_context(|| format!("verify exact {kind} child root"))?;
    }
    Ok(())
}

fn update_lifecycle(
    connection: &Connection,
    repo_key: &RepoKey,
    lifecycle: &ProvisioningLifecycle,
) -> Result<()> {
    let changed = connection.execute(
        "UPDATE repository_provisioning_intents SET lifecycle_json=?1,updated_at=?2 WHERE repo_key=?3",
        params![serde_json::to_string(lifecycle)?,chrono::Utc::now().to_rfc3339(),repo_key.as_str()],
    )?;
    if changed != 1 {
        anyhow::bail!("repository provisioning intent disappeared");
    }
    Ok(())
}

fn read_bootstrap_policy(path: &Path) -> Result<Option<Vec<u8>>> {
    reject_tracked_policy(path)?;
    read_policy(path, "bootstrap")
}

fn read_owned_policy(path: &Path) -> Result<Option<Vec<u8>>> {
    reject_tracked_policy(path)?;
    read_policy(path, "owned repository")
}

pub(crate) fn read_local_policy_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    read_owned_policy(path)
}

fn read_policy(path: &Path, label: &str) -> Result<Option<Vec<u8>>> {
    let root = open_directory(path, label)?;
    let Some(iq) = open_optional_directory_child(&root, OsStr::new(".iq"), "policy directory")?
    else {
        return Ok(None);
    };
    if !entry_exists_at(&iq, OsStr::new("config.json"))? {
        return Ok(None);
    }
    read_regular_file_at(
        &iq,
        OsStr::new("config.json"),
        MAX_POLICY_BYTES,
        &format!("{label} policy"),
    )
    .map(Some)
}

fn reject_tracked_policy(path: &Path) -> Result<()> {
    let status = Command::new("git")
        .args(["ls-files", "--error-unmatch", POLICY_PATH])
        .current_dir(path)
        .output()?;
    if status.status.success() {
        anyhow::bail!(
            ".iq/config.json is local control-plane configuration and must not be tracked"
        );
    }
    if status.status.code() != Some(1) {
        anyhow::bail!(
            "cannot determine whether .iq/config.json is tracked: {}",
            String::from_utf8_lossy(&status.stderr).trim()
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OwnerMarker {
    version: u32,
    database_id: String,
    repo_key: String,
    owned_root_path: PathBuf,
    root_rift_id: String,
    registry_identity: PathBuf,
    registry_device: u64,
    registry_inode: u64,
    generation: u64,
}

fn verify_owner_marker(
    root: &Path,
    database_id: &str,
    repo_key: &str,
    rift: &RiftRootIdentity,
) -> Result<()> {
    let git = open_directory(&root.join(".git"), "owned repository Git directory")?;
    let marker: OwnerMarker = serde_json::from_slice(&read_regular_file_at(
        &git,
        OsStr::new("iq-owner.json"),
        64 * 1024,
        "owned repository marker",
    )?)?;
    if marker.version != 2
        || marker.database_id != database_id
        || marker.repo_key != repo_key
        || marker.owned_root_path != root
        || marker.root_rift_id != rift.rift_id
        || marker.registry_identity != rift.registry_identity
        || marker.registry_device != rift.registry_device
        || marker.registry_inode != rift.registry_inode
        || marker.generation != rift.generation
    {
        anyhow::bail!("owned repository marker differs from database authority");
    }
    Ok(())
}

fn write_owner_marker(
    root: &Path,
    database_id: &str,
    repo_key: &str,
    rift: &RiftRootIdentity,
) -> Result<()> {
    if entry_exists(&root.join(".git/iq-owner.json"))? {
        return verify_owner_marker(root, database_id, repo_key, rift);
    }
    let git = open_directory(&root.join(".git"), "owned repository Git directory")?;
    let bytes = serde_json::to_vec_pretty(&OwnerMarker {
        version: 2,
        database_id: database_id.into(),
        repo_key: repo_key.into(),
        owned_root_path: root.to_path_buf(),
        root_rift_id: rift.rift_id.clone(),
        registry_identity: rift.registry_identity.clone(),
        registry_device: rift.registry_device,
        registry_inode: rift.registry_inode,
        generation: rift.generation,
    })?;
    publish_file_noreplace(
        &git,
        OsStr::new("iq-owner.json"),
        &bytes,
        "owned repository marker",
    )
}

fn read_rift_id(path: &Path) -> Result<String> {
    let root = open_directory(path, "owned repository root")?;
    let id = String::from_utf8(read_regular_file_at(
        &root,
        OsStr::new(".rift"),
        128,
        "Rift identity marker",
    )?)?;
    let id = id.trim();
    if id.len() != 26 || !id.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        anyhow::bail!("invalid owned-root Rift identity");
    }
    Ok(id.into())
}

fn rift_registry_identity(explicit: Option<&Path>) -> Result<PathBuf> {
    let path = explicit
        .map(Path::to_path_buf)
        .or_else(|| std::env::var_os("IQ_RIFT_DATABASE").map(PathBuf::from))
        .unwrap_or_else(|| {
            std::env::var_os("XDG_DATA_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(std::env::var_os("HOME").unwrap()).join(".local/share")
                })
                .join("rift/rift.sqlite")
        });
    path.canonicalize()
        .with_context(|| format!("resolve Rift registry {}", path.display()))
}

fn verify_remote(path: &Path, expected: &RemoteIdentity) -> Result<()> {
    if git_text(path, ["remote"])? != INTERNAL_REMOTE_NAME
        || git_text(path, ["remote", "get-url", "--all", INTERNAL_REMOTE_NAME])?
            != expected.fetch_url
        || git_text(
            path,
            ["remote", "get-url", "--push", "--all", INTERNAL_REMOTE_NAME],
        )? != expected.push_url
    {
        anyhow::bail!("owned repository remote identity changed");
    }
    Ok(())
}

fn verify_exact_checkout(path: &Path, expected_sha: &str) -> Result<()> {
    let expected_tree = git_text(path, ["rev-parse", &format!("{expected_sha}^{{tree}}")])?;
    if git_text(path, ["write-tree"])? != expected_tree {
        anyhow::bail!("owned repository index differs from expected target tree");
    }
    let status = git_text(path, ["status", "--porcelain=v1", "--untracked-files=all"])?;
    if !status.is_empty() {
        anyhow::bail!("owned repository worktree differs from expected target tree");
    }
    Ok(())
}

fn rift_output<const N: usize>(database: Option<&Path>, args: [&str; N]) -> Result<Output> {
    let mut command = Command::new("rift");
    if let Some(database) = database {
        command.args([
            "--database",
            database
                .to_str()
                .context("Rift database path is not UTF-8")?,
        ]);
    }
    require_success(command.args(args).output()?, "run Rift command")
}

fn remote_identity(path: &Path, remote_name: &str) -> Result<RemoteIdentity> {
    if remote_name.is_empty() {
        anyhow::bail!("bootstrap remote name must not be empty");
    }
    let remote = crate::composition::resolve_remote_identity(path, remote_name)?;
    Ok(RemoteIdentity {
        fetch_url: remote.fetch_url,
        push_url: remote.push_url,
    })
}

fn remote_target_sha(fetch_url: &str, target: &str) -> Result<String> {
    let target_ref = target_ref(target)?;
    let output = require_success(
        Command::new("git")
            .args(["ls-remote", "--exit-code", fetch_url, &target_ref])
            .output()?,
        "resolve remote target ref",
    )?;
    let text = String::from_utf8(output.stdout)?;
    let mut fields = text.split_whitespace();
    let sha = fields
        .next()
        .context("remote target ref has no SHA")?
        .to_string();
    if fields.next() != Some(target_ref.as_str()) || fields.next().is_some() {
        anyhow::bail!("remote target ref did not resolve exactly once");
    }
    crate::control_domain::require_sha(&sha, "remote target SHA")?;
    Ok(sha)
}

fn canonical_git_root(path: &Path) -> Result<PathBuf> {
    let path = path
        .canonicalize()
        .with_context(|| format!("resolve bootstrap checkout {}", path.display()))?;
    let metadata = fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("bootstrap checkout must be a real directory");
    }
    let root = PathBuf::from(git_text(&path, ["rev-parse", "--show-toplevel"])?).canonicalize()?;
    if root != path {
        anyhow::bail!("bootstrap checkout must be the Git root");
    }
    Ok(path)
}

fn git<const N: usize>(path: &Path, args: [&str; N], label: &str) -> Result<()> {
    require_success(
        Command::new("git").args(args).current_dir(path).output()?,
        label,
    )
    .map(|_| ())
}

fn git_text<const N: usize>(path: &Path, args: [&str; N]) -> Result<String> {
    Ok(String::from_utf8(
        require_success(
            Command::new("git").args(args).current_dir(path).output()?,
            "inspect Git repository",
        )?
        .stdout,
    )?
    .trim()
    .into())
}

fn git_object_exists(path: &Path, sha: &str) -> Result<bool> {
    let object = format!("{sha}^{{commit}}");
    let output = Command::new("git")
        .args(["cat-file", "-e", &object])
        .current_dir(path)
        .output()?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(128) => Ok(false),
        _ => anyhow::bail!(
            "inspect provisioned Git object failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

fn require_success(output: Output, label: &str) -> Result<Output> {
    if !output.status.success() {
        anyhow::bail!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

fn require_absolute_directory(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute() {
        anyhow::bail!("{label} must be absolute");
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("{label} must be a real directory");
    }
    if path.canonicalize()? != path {
        anyhow::bail!("{label} must be a canonical path without symlinked ancestors");
    }
    Ok(())
}

fn require_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("{label} must be a real directory: {}", path.display());
    }
    Ok(())
}

fn entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn child_name(name: &OsStr, label: &str) -> Result<CString> {
    if name.as_bytes().is_empty() || name.as_bytes().contains(&b'/') {
        anyhow::bail!("invalid {label} child name");
    }
    CString::new(name.as_bytes()).context("child name contains NUL")
}

fn open_directory(path: &Path, label: &str) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .with_context(|| format!("open {label} {}", path.display()))?;
    if !file.metadata()?.is_dir() {
        anyhow::bail!("{label} must be a real directory");
    }
    Ok(file)
}

fn open_directory_child(parent: &File, name: &OsStr, label: &str) -> Result<File> {
    let name = child_name(name, label)?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("open {label}"));
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn open_optional_directory_child(parent: &File, name: &OsStr, label: &str) -> Result<Option<File>> {
    match open_directory_child(parent, name, label) {
        Ok(directory) => Ok(Some(directory)),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(None)
        }
        Err(error)
            if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                matches!(
                    error.raw_os_error(),
                    Some(libc::ELOOP) | Some(libc::ENOTDIR)
                )
            }) =>
        {
            anyhow::bail!("{label} must be a real non-symlink directory")
        }
        Err(error) => Err(error),
    }
}

fn ensure_directory_child(parent: &File, name: &OsStr, label: &str) -> Result<File> {
    let name_c = child_name(name, label)?;
    if unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o700) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error).with_context(|| format!("create {label}"));
        }
    } else {
        parent.sync_all()?;
    }
    open_directory_child(parent, name, label)
}

fn entry_exists_at(parent: &File, name: &OsStr) -> Result<bool> {
    let name = child_name(name, "filesystem entry")?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } == 0
    {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(false)
    } else {
        Err(error).context("inspect filesystem entry")
    }
}

fn read_regular_file_at(parent: &File, name: &OsStr, limit: u64, label: &str) -> Result<Vec<u8>> {
    let name = child_name(name, label)?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("open {label}"));
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    if !file.metadata()?.is_file() {
        anyhow::bail!("{label} must be a regular non-symlink file");
    }
    let mut contents = Vec::new();
    Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut contents)?;
    if contents.len() as u64 > limit {
        anyhow::bail!("{label} exceeds its {limit} byte limit");
    }
    Ok(contents)
}

fn create_temporary_file(parent: &File, name: &OsStr, bytes: &[u8], label: &str) -> Result<File> {
    let name = child_name(name, label)?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("create {label}"));
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(file)
}

fn rename_at_noreplace(parent: &File, from: &OsStr, to: &OsStr, label: &str) -> Result<()> {
    let from = child_name(from, label)?;
    let to = child_name(to, label)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let result = unsafe {
        libc::renameat2(
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe {
        libc::renameatx_np(
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
    let result = -1;
    if result != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("rename {label}"));
    }
    Ok(())
}

fn publish_file_noreplace(parent: &File, name: &OsStr, bytes: &[u8], label: &str) -> Result<()> {
    if entry_exists_at(parent, name)? {
        if read_regular_file_at(parent, name, bytes.len() as u64, label)? != bytes {
            anyhow::bail!("{label} differs from durable provisioning intent");
        }
        sync_regular_file_at(parent, name, label)?;
        stop_file_publication_after(label, "resynced");
        return Ok(());
    }
    let temporary = OsString::from(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        Uuid::new_v4()
    ));
    let file = create_temporary_file(parent, &temporary, bytes, label)?;
    drop(file);
    let result = rename_at_noreplace(parent, &temporary, name, label);
    if result.is_err() && entry_exists_at(parent, name)? {
        let temporary = child_name(&temporary, label)?;
        unsafe { libc::unlinkat(parent.as_raw_fd(), temporary.as_ptr(), 0) };
        if read_regular_file_at(parent, name, bytes.len() as u64, label)? == bytes {
            sync_regular_file_at(parent, name, label)?;
            stop_file_publication_after(label, "resynced");
            return Ok(());
        }
    }
    result?;
    stop_file_publication_after(label, "renamed");
    sync_regular_file_at(parent, name, label)?;
    stop_file_publication_after(label, "resynced");
    Ok(())
}

fn sync_regular_file_at(parent: &File, name: &OsStr, label: &str) -> Result<()> {
    let name = child_name(name, label)?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("open {label}"));
    }
    let file = unsafe { File::from_raw_fd(descriptor) };
    if !file.metadata()?.is_file() {
        anyhow::bail!("{label} is not a regular file");
    }
    file.sync_all()?;
    parent.sync_all()?;
    Ok(())
}

#[cfg(debug_assertions)]
fn stop_file_publication_after(label: &str, boundary: &str) {
    if std::env::var("IQ_TEST_FILE_PUBLICATION_LABEL").as_deref() == Ok(label)
        && std::env::var("IQ_TEST_FILE_PUBLICATION_STOP_AFTER").as_deref() == Ok(boundary)
    {
        std::process::exit(88);
    }
}

#[cfg(not(debug_assertions))]
fn stop_file_publication_after(_label: &str, _boundary: &str) {}

fn replace_file_atomically(parent: &File, name: &OsStr, bytes: &[u8], label: &str) -> Result<()> {
    let temporary = OsString::from(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        Uuid::new_v4()
    ));
    let file = create_temporary_file(parent, &temporary, bytes, label)?;
    drop(file);
    let from = child_name(&temporary, label)?;
    let to = child_name(name, label)?;
    if unsafe {
        libc::renameat(
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("replace {label}"));
    }
    parent.sync_all()?;
    Ok(())
}

fn rename_noreplace(from: &Path, to: &Path, label: &str) -> Result<()> {
    let from_parent = from.parent().context("source path has no parent")?;
    let to_parent = to.parent().context("destination path has no parent")?;
    if from_parent != to_parent {
        anyhow::bail!("{label} requires one parent directory");
    }
    let parent = open_directory(from_parent, label)?;
    rename_at_noreplace(
        &parent,
        from.file_name().context("source path has no file name")?,
        to.file_name()
            .context("destination path has no file name")?,
        label,
    )
}

fn publish_directory_noreplace(path: &Path, label: &str) -> Result<()> {
    if entry_exists(path)? {
        return require_real_directory(path, label);
    }
    let parent_path = path.parent().context("directory path has no parent")?;
    let parent = open_directory(parent_path, label)?;
    let temporary = OsString::from(format!(".{label}.{}.tmp", Uuid::new_v4()));
    let temporary_c = child_name(&temporary, label)?;
    if unsafe { libc::mkdirat(parent.as_raw_fd(), temporary_c.as_ptr(), 0o700) } != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("create {label}"));
    }
    let directory = open_directory_child(&parent, &temporary, label)?;
    directory.sync_all()?;
    rename_at_noreplace(
        &parent,
        &temporary,
        path.file_name().context("directory path has no name")?,
        label,
    )?;
    parent.sync_all()?;
    require_real_directory(path, label)
}

struct ProvisioningFence {
    file: File,
}

impl ProvisioningFence {
    fn acquire(storage_root: &Path, repo_key: &RepoKey) -> Result<Self> {
        let storage = open_directory(storage_root, "owned repository storage root")?;
        let name = CString::new(format!(".repository-{}.lock", repo_key.as_str()))?;
        let descriptor = unsafe {
            libc::openat(
                storage.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error())
                .context("open repository provisioning fence");
        }
        let file = unsafe { File::from_raw_fd(descriptor) };
        if !file.metadata()?.is_file() {
            anyhow::bail!("repository provisioning fence must be a regular file");
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("acquire repository provisioning fence");
        }
        Ok(Self { file })
    }
}

impl Drop for ProvisioningFence {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}
