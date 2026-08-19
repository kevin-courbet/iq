use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationState {
    Enabled,
    Draining { obligations: BTreeSet<Obligation> },
    Disabled,
}

impl OperationState {
    pub fn require_new_work(&self) -> Result<()> {
        match self {
            Self::Enabled => Ok(()),
            Self::Draining { .. } => anyhow::bail!("repository is draining and rejects new work"),
            Self::Disabled => anyhow::bail!("repository is disabled"),
        }
    }

    pub fn require_obligation(&self, obligation: &Obligation) -> Result<()> {
        match self {
            Self::Enabled => Ok(()),
            Self::Draining { obligations } if obligations.contains(obligation) => Ok(()),
            Self::Draining { .. } => {
                anyhow::bail!("operation is not a captured draining obligation")
            }
            Self::Disabled => anyhow::bail!("repository is disabled"),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Obligation {
    Workspace { id: String },
    QueueItem { id: String },
    Replication { id: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationPolicy {
    Direct,
    MergeRequestRequired,
}

impl std::fmt::Display for IntegrationPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Direct => "direct",
            Self::MergeRequestRequired => "merge_request_required",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Github,
    Gitlab,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMergeMethod {
    Merge,
    Squash,
}

impl std::fmt::Display for Provider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Github => "github",
            Self::Gitlab => "gitlab",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRepository {
    pub provider: Provider,
    pub host: String,
    pub repository: String,
    pub repository_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GitRepository {
    LocalBare {
        path: PathBuf,
        device: u64,
        inode: u64,
        object_format: crate::git_object::GitObjectFormat,
    },
    Accessible {
        fetch_url: String,
        push_url: String,
        repository_id: String,
        provider: ProviderRepository,
        object_format: crate::git_object::GitObjectFormat,
    },
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PhysicalRepositoryIdentity<'a> {
    LocalBare {
        device: u64,
        inode: u64,
    },
    Provider {
        provider: Provider,
        host: &'a str,
        repository_id: &'a str,
    },
}

impl GitRepository {
    pub fn validate(self, label: &str) -> Result<Self> {
        match &self {
            Self::LocalBare {
                path,
                device,
                inode,
                ..
            } => {
                if !path.is_absolute() {
                    anyhow::bail!("{label} local bare path must be absolute");
                }
                if path.to_str().is_none() {
                    anyhow::bail!("{label} local bare path must be valid UTF-8");
                }
                if *device == 0 || *inode == 0 {
                    anyhow::bail!("{label} local bare identity must contain device and inode");
                }
            }
            Self::Accessible {
                fetch_url,
                push_url,
                repository_id,
                provider,
                ..
            } => {
                require_nonblank(fetch_url, &format!("{label} fetch URL"))?;
                require_nonblank(push_url, &format!("{label} push URL"))?;
                if is_local_git_transport(fetch_url) || is_local_git_transport(push_url) {
                    anyhow::bail!("{label} local transport must use the local_bare variant");
                }
                validate_accessible_transport(fetch_url, label)?;
                validate_accessible_transport(push_url, label)?;
                require_nonblank(repository_id, &format!("{label} immutable repository ID"))?;
                validate_provider_repository(provider, label)?;
                if repository_id != &provider.repository_id {
                    anyhow::bail!(
                        "{label} repository ID must equal its immutable provider repository ID"
                    );
                }
                validate_provider_transport(fetch_url, provider, label)?;
                validate_provider_transport(push_url, provider, label)?;
            }
        }
        Ok(self)
    }

    pub fn fetch_argument(&self) -> &OsStr {
        match self {
            Self::LocalBare { path, .. } => path.as_os_str(),
            Self::Accessible { fetch_url, .. } => OsStr::new(fetch_url),
        }
    }

    pub fn push_argument(&self) -> &OsStr {
        match self {
            Self::LocalBare { path, .. } => path.as_os_str(),
            Self::Accessible { push_url, .. } => OsStr::new(push_url),
        }
    }

    pub fn provider(&self) -> Option<&ProviderRepository> {
        match self {
            Self::LocalBare { .. } => None,
            Self::Accessible { provider, .. } => Some(provider),
        }
    }

    pub fn object_format(&self) -> crate::git_object::GitObjectFormat {
        match self {
            Self::LocalBare { object_format, .. } | Self::Accessible { object_format, .. } => {
                *object_format
            }
        }
    }

    pub fn fetch_identity_bytes(&self) -> Vec<u8> {
        self.fetch_argument().as_bytes().to_vec()
    }

    pub fn push_identity_bytes(&self) -> Vec<u8> {
        self.push_argument().as_bytes().to_vec()
    }

    pub fn operational_fetch_url(&self) -> String {
        self.fetch_argument()
            .to_str()
            .expect("validated Git transport must be UTF-8")
            .to_string()
    }

    pub fn operational_push_url(&self) -> String {
        self.push_argument()
            .to_str()
            .expect("validated Git transport must be UTF-8")
            .to_string()
    }

    pub fn verify_local_bare(&self) -> Result<()> {
        let Self::LocalBare {
            path,
            device,
            inode,
            object_format,
        } = self
        else {
            return Ok(());
        };
        let lexical = std::fs::symlink_metadata(path)
            .with_context(|| format!("inspect local bare repository {}", path.display()))?;
        if lexical.file_type().is_symlink() || !lexical.is_dir() {
            anyhow::bail!("local bare repository must be a real directory");
        }
        let canonical = std::fs::canonicalize(path)
            .with_context(|| format!("canonicalize local bare repository {}", path.display()))?;
        if canonical != *path {
            anyhow::bail!("local bare repository path must be canonical");
        }
        let metadata = std::fs::metadata(path)?;
        if (metadata.dev(), metadata.ino()) != (*device, *inode) {
            anyhow::bail!("local bare repository device/inode identity changed");
        }
        let binding = crate::git_command::authorize_current(path)?;
        if binding.object_format != *object_format {
            anyhow::bail!("local bare repository object format differs from policy");
        }
        if !binding.is_bare() {
            anyhow::bail!("local repository is not a bare Git repository");
        }
        Ok(())
    }

    pub fn verify_effect_identity(&self) -> Result<()> {
        self.verify_local_bare()?;
        if let Some(provider) = self.provider() {
            crate::providers::verify_repository(provider, self.object_format())?;
        }
        Ok(())
    }

    pub fn transport_identity(&self) -> (Vec<u8>, Vec<u8>) {
        (self.fetch_identity_bytes(), self.push_identity_bytes())
    }

    pub fn canonical_ownership_key(&self) -> Result<String> {
        self.physical_identity_key()
    }

    pub fn physical_identity_key(&self) -> Result<String> {
        let identity = match self {
            Self::LocalBare { device, inode, .. } => PhysicalRepositoryIdentity::LocalBare {
                device: *device,
                inode: *inode,
            },
            Self::Accessible { provider, .. } => PhysicalRepositoryIdentity::Provider {
                provider: provider.provider,
                host: &provider.host,
                repository_id: &provider.repository_id,
            },
        };
        serde_json::to_string(&identity).context("serialize physical repository identity")
    }

    pub fn destination_identity_key(&self) -> Result<String> {
        self.physical_identity_key()
    }

    fn has_same_push_destination(&self, other: &Self) -> bool {
        self.push_identity_bytes() == other.push_identity_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicationPolicy {
    None,
    Replicate { targets: Vec<GitRepository> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryPolicy {
    pub operation_state: OperationState,
    pub canonical_repository: GitRepository,
    pub target_branch: String,
    pub integration_policy: IntegrationPolicy,
    pub replication_policy: ReplicationPolicy,
}

#[derive(Clone, Debug)]
pub(crate) struct VerifiedRepositoryPolicy(RepositoryPolicy);

impl std::ops::Deref for VerifiedRepositoryPolicy {
    type Target = RepositoryPolicy;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl VerifiedRepositoryPolicy {
    pub(crate) fn reverify(&self) -> Result<()> {
        for (repository, role, ordinal) in self.physical_repositories() {
            repository.verify_effect_identity().with_context(|| {
                format!("reverify {role} repository effect identity at ordinal {ordinal}")
            })?;
        }
        Ok(())
    }
}

impl RepositoryPolicy {
    pub fn validate(self) -> Result<Self> {
        crate::repository::validate_target_branch(&self.target_branch)?;
        let canonical_repository = self.canonical_repository.validate("canonical repository")?;
        let replication_policy = match self.replication_policy {
            ReplicationPolicy::None => ReplicationPolicy::None,
            ReplicationPolicy::Replicate { targets } => {
                if targets.is_empty() {
                    anyhow::bail!("replication policy must contain at least one target");
                }
                let mut identities = BTreeSet::new();
                let mut validated = Vec::with_capacity(targets.len());
                for target in targets {
                    let target = target.validate("replica")?;
                    if target.object_format() != canonical_repository.object_format() {
                        anyhow::bail!(
                            "replica object format must equal canonical repository object format"
                        );
                    }
                    let identity = target.destination_identity_key()?;
                    if identity == canonical_repository.destination_identity_key()?
                        || target.has_same_push_destination(&canonical_repository)
                    {
                        anyhow::bail!("canonical repository cannot also be a replica");
                    }
                    if !identities.insert(identity)
                        || validated
                            .iter()
                            .any(|existing| target.has_same_push_destination(existing))
                    {
                        anyhow::bail!("replication policy contains a duplicate target");
                    }
                    validated.push(target);
                }
                ReplicationPolicy::Replicate { targets: validated }
            }
        };
        if self.integration_policy == IntegrationPolicy::MergeRequestRequired
            && canonical_repository.provider().is_none()
        {
            anyhow::bail!(
                "merge-request-required integration needs a provider canonical repository"
            );
        }
        Ok(Self {
            operation_state: self.operation_state,
            canonical_repository,
            target_branch: self.target_branch,
            integration_policy: self.integration_policy,
            replication_policy,
        })
    }

    pub(crate) fn physical_repositories(&self) -> Vec<(&GitRepository, &'static str, usize)> {
        let mut repositories = vec![(&self.canonical_repository, "canonical", 0)];
        if let ReplicationPolicy::Replicate { targets } = &self.replication_policy {
            repositories.extend(
                targets
                    .iter()
                    .enumerate()
                    .map(|(ordinal, repository)| (repository, "replica", ordinal)),
            );
        }
        repositories
    }

    pub(crate) fn verify_effect_identities(self) -> Result<VerifiedRepositoryPolicy> {
        for (repository, role, ordinal) in self.physical_repositories() {
            repository.verify_effect_identity().with_context(|| {
                format!("verify {role} repository effect identity at ordinal {ordinal}")
            })?;
        }
        Ok(VerifiedRepositoryPolicy(self))
    }

    pub fn require_new_work(&self) -> Result<()> {
        self.operation_state.require_new_work()
    }

    pub fn require_queue_mutation(&self, item_id: &str) -> Result<()> {
        self.require_obligation(&Obligation::QueueItem {
            id: item_id.to_string(),
        })
    }

    pub fn require_workspace_mutation(&self, workspace_id: &str) -> Result<()> {
        self.require_obligation(&Obligation::Workspace {
            id: workspace_id.to_string(),
        })
    }

    pub fn require_replication(&self, debt_id: &str) -> Result<()> {
        self.require_obligation(&Obligation::Replication {
            id: debt_id.to_string(),
        })
    }

    fn require_obligation(&self, obligation: &Obligation) -> Result<()> {
        self.operation_state.require_obligation(obligation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyInventory {
    pub version: u32,
    pub repositories: Vec<PolicyAssignment>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyAssignment {
    pub repo_key: String,
    pub policy: RepositoryPolicy,
    pub repository: MigrationRepositoryState,
    #[serde(default)]
    pub development_workspaces: Vec<MigrationDevelopmentWorkspace>,
    #[serde(default)]
    pub item_dispositions: Vec<ActiveItemDisposition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum MigrationRepositoryState {
    Ready {
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    Reserved {
        disposition: InterruptedProvisioningDisposition,
    },
    StagingDirectory {
        disposition: InterruptedProvisioningDisposition,
    },
    GitInitialized {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    RemoteConfigured {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    TargetFetched {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    TargetCheckedOut {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    RootPublished {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    PolicyPublished {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    RiftInitialized {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    RiftVerified {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    OwnerPublished {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
    ChildRootsPublished {
        disposition: InterruptedProvisioningDisposition,
        git_binding: Option<crate::git_command::RepositoryBinding>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptedProvisioningDisposition {
    Preserve,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationDevelopmentWorkspace {
    pub workspace_id: String,
    pub path: String,
    pub rift_id: String,
    pub source_rift_id: String,
    pub base_sha: String,
    pub git_binding: Option<crate::git_command::RepositoryBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveItemDisposition {
    pub item_id: String,
    pub disposition: IncompatibleItemDisposition,
    pub admitted_base_sha: Option<String>,
    pub provider_repository: Option<ProviderRepository>,
    pub provider_merge_method: Option<ProviderMergeMethod>,
    pub workspace_identity: Option<MigrationWorkspaceIdentity>,
    pub runner_snapshot: Option<crate::control_domain::RunnerSnapshot>,
    pub runner_termination_authority: Option<crate::control_domain::LegacyRunnerScopeAuthority>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationWorkspaceIdentity {
    pub path: String,
    pub rift_id: String,
    pub source_rift_id: String,
    #[serde(default)]
    pub git_binding: Option<crate::git_command::RepositoryBinding>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompatibleItemDisposition {
    Continue,
    Cancel,
}

impl PolicyInventory {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("read repository policy inventory {}", path.display()))?;
        let inventory: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse repository policy inventory {}", path.display()))?;
        inventory.validate()
    }

    pub fn validate(self) -> Result<Self> {
        if self.version != 3 {
            anyhow::bail!("repository policy inventory version must be 3");
        }
        let mut keys = BTreeSet::new();
        let mut disposition_ids = BTreeSet::new();
        let mut development_workspace_ids = BTreeSet::new();
        let repositories = self
            .repositories
            .into_iter()
            .map(|assignment| {
                let key = crate::repository::RepoKey::from_stored(assignment.repo_key)?;
                if !keys.insert(key.as_str().to_string()) {
                    anyhow::bail!("repository policy inventory contains a duplicate repo_key");
                }
                let policy = assignment.policy.validate()?;
                let object_format = policy.canonical_repository.object_format();
                let item_dispositions =
                    validate_dispositions(assignment.item_dispositions, object_format)?;
                let repository =
                    validate_migration_repository_state(assignment.repository, object_format)?;
                let development_workspaces = assignment
                    .development_workspaces
                    .into_iter()
                    .map(|workspace| {
                        validate_development_workspace(
                            workspace,
                            &mut development_workspace_ids,
                            object_format,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                for disposition in &item_dispositions {
                    if !disposition_ids.insert(disposition.item_id.clone()) {
                        anyhow::bail!("migration inventory contains duplicate item dispositions");
                    }
                }
                Ok(PolicyAssignment {
                    repo_key: key.as_str().to_string(),
                    policy,
                    repository,
                    development_workspaces,
                    item_dispositions,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            version: self.version,
            repositories,
        })
    }
}

impl MigrationRepositoryState {
    pub(crate) fn lifecycle(&self) -> Option<&'static str> {
        match self {
            Self::Ready { .. } => None,
            Self::Reserved { .. } => Some("reserved"),
            Self::StagingDirectory { .. } => Some("staging_directory"),
            Self::GitInitialized { .. } => Some("git_initialized"),
            Self::RemoteConfigured { .. } => Some("remote_configured"),
            Self::TargetFetched { .. } => Some("target_fetched"),
            Self::TargetCheckedOut { .. } => Some("target_checked_out"),
            Self::RootPublished { .. } => Some("root_published"),
            Self::PolicyPublished { .. } => Some("policy_published"),
            Self::RiftInitialized { .. } => Some("rift_initialized"),
            Self::RiftVerified { .. } => Some("rift_verified"),
            Self::OwnerPublished { .. } => Some("owner_published"),
            Self::ChildRootsPublished { .. } => Some("child_roots_published"),
        }
    }

    pub(crate) fn disposition(&self) -> Option<InterruptedProvisioningDisposition> {
        match self {
            Self::Ready { .. } => None,
            Self::Reserved { disposition }
            | Self::StagingDirectory { disposition }
            | Self::GitInitialized { disposition, .. }
            | Self::RemoteConfigured { disposition, .. }
            | Self::TargetFetched { disposition, .. }
            | Self::TargetCheckedOut { disposition, .. }
            | Self::RootPublished { disposition, .. }
            | Self::PolicyPublished { disposition, .. }
            | Self::RiftInitialized { disposition, .. }
            | Self::RiftVerified { disposition, .. }
            | Self::OwnerPublished { disposition, .. }
            | Self::ChildRootsPublished { disposition, .. } => Some(*disposition),
        }
    }

    pub(crate) fn git_binding(&self) -> Option<&crate::git_command::RepositoryBinding> {
        match self {
            Self::Ready { git_binding }
            | Self::GitInitialized { git_binding, .. }
            | Self::RemoteConfigured { git_binding, .. }
            | Self::TargetFetched { git_binding, .. }
            | Self::TargetCheckedOut { git_binding, .. }
            | Self::RootPublished { git_binding, .. }
            | Self::PolicyPublished { git_binding, .. }
            | Self::RiftInitialized { git_binding, .. }
            | Self::RiftVerified { git_binding, .. }
            | Self::OwnerPublished { git_binding, .. }
            | Self::ChildRootsPublished { git_binding, .. } => git_binding.as_ref(),
            Self::Reserved { .. } | Self::StagingDirectory { .. } => None,
        }
    }

    pub(crate) fn uses_staging_repository(&self) -> bool {
        matches!(
            self,
            Self::GitInitialized { .. }
                | Self::RemoteConfigured { .. }
                | Self::TargetFetched { .. }
        )
    }

    pub(crate) fn requires_source_commit(&self) -> bool {
        matches!(
            self,
            Self::TargetFetched { .. }
                | Self::TargetCheckedOut { .. }
                | Self::RootPublished { .. }
                | Self::PolicyPublished { .. }
                | Self::RiftInitialized { .. }
                | Self::RiftVerified { .. }
                | Self::OwnerPublished { .. }
                | Self::ChildRootsPublished { .. }
        )
    }

    pub(crate) fn requires_checked_out_head(&self) -> bool {
        matches!(
            self,
            Self::TargetCheckedOut { .. }
                | Self::RootPublished { .. }
                | Self::PolicyPublished { .. }
                | Self::RiftInitialized { .. }
                | Self::RiftVerified { .. }
                | Self::OwnerPublished { .. }
                | Self::ChildRootsPublished { .. }
        )
    }
}

fn validate_migration_repository_state(
    state: MigrationRepositoryState,
    object_format: crate::git_object::GitObjectFormat,
) -> Result<MigrationRepositoryState> {
    let requires_binding = !matches!(
        state,
        MigrationRepositoryState::Reserved { .. }
            | MigrationRepositoryState::StagingDirectory { .. }
    );
    if requires_binding && state.git_binding().is_none() {
        anyhow::bail!("migration repository state has no verified Git binding");
    }
    if state
        .git_binding()
        .is_some_and(|binding| binding.object_format != object_format)
    {
        anyhow::bail!("migration repository Git binding object format differs from policy");
    }
    Ok(state)
}

fn validate_development_workspace(
    workspace: MigrationDevelopmentWorkspace,
    identities: &mut BTreeSet<String>,
    object_format: crate::git_object::GitObjectFormat,
) -> Result<MigrationDevelopmentWorkspace> {
    require_nonblank(
        &workspace.workspace_id,
        "migration development workspace ID",
    )?;
    if !identities.insert(workspace.workspace_id.clone()) {
        anyhow::bail!("migration inventory contains duplicate development workspaces");
    }
    require_nonblank(&workspace.path, "migration development workspace path")?;
    if !Path::new(&workspace.path).is_absolute() {
        anyhow::bail!("migration development workspace path must be absolute");
    }
    require_nonblank(
        &workspace.rift_id,
        "migration development workspace Rift ID",
    )?;
    require_nonblank(
        &workspace.source_rift_id,
        "migration development workspace source Rift ID",
    )?;
    let binding = workspace
        .git_binding
        .as_ref()
        .context("migration development workspace has no verified Git binding")?;
    if binding.top_level != Path::new(&workspace.path) {
        anyhow::bail!("migration development workspace binding has a different top-level");
    }
    if binding.object_format != object_format {
        anyhow::bail!("migration development workspace object format differs from policy");
    }
    object_format.require_oid(&workspace.base_sha, "migration development base SHA")?;
    Ok(workspace)
}

fn validate_provider_repository(value: &ProviderRepository, label: &str) -> Result<()> {
    let host = require_nonblank(&value.host, &format!("{label} provider host"))?;
    if host != host.to_ascii_lowercase()
        || host.contains(['/', ':', '@'])
        || host.starts_with('.')
        || host.ends_with('.')
    {
        anyhow::bail!("{label} provider host must be one canonical lowercase DNS name");
    }
    let repository = require_nonblank(&value.repository, &format!("{label} provider repository"))?;
    if !repository.contains('/') || repository.split('/').any(str::is_empty) {
        anyhow::bail!("{label} provider repository must contain non-empty path components");
    }
    require_nonblank(
        &value.repository_id,
        &format!("{label} immutable provider repository ID"),
    )?;
    Ok(())
}

fn validate_provider_transport(
    transport: &str,
    provider: &ProviderRepository,
    label: &str,
) -> Result<()> {
    let (host, repository) = parse_git_transport(transport)
        .with_context(|| format!("parse {label} provider Git transport"))?;
    if host != provider.host || repository != provider.repository {
        anyhow::bail!(
            "{label} Git transport does not identify provider repository {}/{}",
            provider.host,
            provider.repository
        );
    }
    Ok(())
}

fn parse_git_transport(value: &str) -> Result<(String, String)> {
    let (host, path) = if let Some(rest) = value.strip_prefix("https://") {
        let (authority, path) = rest
            .split_once('/')
            .context("URL Git transport has no repository path")?;
        if authority.contains('@') {
            anyhow::bail!("HTTPS Git transport must not contain credentials or userinfo");
        }
        (transport_authority_host(authority, 443)?, path)
    } else if let Some(rest) = value.strip_prefix("ssh://") {
        let (authority, path) = rest
            .split_once('/')
            .context("SSH Git transport has no repository path")?;
        (transport_authority_host(authority, 22)?, path)
    } else {
        let (authority, path) = value
            .split_once(':')
            .context("Git transport must be HTTPS, SSH, or SCP syntax")?;
        (
            authority
                .rsplit_once('@')
                .map_or(authority, |(_, host)| host),
            path,
        )
    };
    let host = host.to_ascii_lowercase();
    let repository = path.strip_suffix(".git").unwrap_or(path).trim_matches('/');
    if host.is_empty() || repository.is_empty() || repository.split('/').any(str::is_empty) {
        anyhow::bail!("Git transport has invalid host or repository path");
    }
    Ok((host, repository.to_string()))
}

fn validate_accessible_transport(value: &str, label: &str) -> Result<()> {
    if value.starts_with("https://") || value.starts_with("ssh://") {
        parse_git_transport(value).with_context(|| format!("parse {label} Git transport"))?;
        return Ok(());
    }
    if value.contains("://") || value.contains("::") || value.starts_with("ext::") {
        anyhow::bail!("{label} Git transport must use HTTPS, SSH, or SCP syntax");
    }
    let (authority, path) = value
        .split_once(':')
        .context("Git transport must use HTTPS, SSH, or SCP syntax")?;
    if authority.contains('/') || authority.is_empty() || path.is_empty() {
        anyhow::bail!("{label} Git transport must use HTTPS, SSH, or SCP syntax");
    }
    parse_git_transport(value).with_context(|| format!("parse {label} Git transport"))?;
    Ok(())
}

fn transport_authority_host(authority: &str, default_port: u16) -> Result<&str> {
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let Some((host, port)) = host.rsplit_once(':') else {
        return Ok(host);
    };
    let port = port
        .parse::<u16>()
        .context("provider Git transport has an invalid endpoint port")?;
    if port != default_port {
        #[cfg(any(debug_assertions, feature = "test-hooks"))]
        if crate::providers::test_provider_executable_is_injected() {
            return Ok(host);
        }
        anyhow::bail!("provider Git transport uses a non-default endpoint port");
    }
    Ok(host)
}

fn is_local_git_transport(value: &str) -> bool {
    if value.starts_with("file://") || Path::new(value).is_absolute() {
        return true;
    }
    if value.contains("://") {
        return false;
    }
    value
        .split_once(':')
        .is_none_or(|(authority, _)| authority.contains('/'))
}

fn validate_dispositions(
    dispositions: Vec<ActiveItemDisposition>,
    object_format: crate::git_object::GitObjectFormat,
) -> Result<Vec<ActiveItemDisposition>> {
    let mut identities = BTreeSet::new();
    for disposition in &dispositions {
        require_nonblank(&disposition.item_id, "migration item disposition ID")?;
        if !identities.insert(disposition.item_id.clone()) {
            anyhow::bail!("migration inventory contains duplicate item dispositions");
        }
        if let Some(base) = &disposition.admitted_base_sha {
            object_format.require_oid(base, "migration admitted base SHA")?;
        }
        if let Some(provider) = &disposition.provider_repository {
            validate_provider_repository(provider, "migration provider repository")?;
        }
        if let Some(workspace) = &disposition.workspace_identity {
            require_nonblank(&workspace.path, "migration workspace path")?;
            if !Path::new(&workspace.path).is_absolute() {
                anyhow::bail!("migration workspace path must be absolute");
            }
            require_nonblank(&workspace.rift_id, "migration workspace Rift ID")?;
            require_nonblank(
                &workspace.source_rift_id,
                "migration workspace source Rift ID",
            )?;
            let binding = workspace
                .git_binding
                .as_ref()
                .context("migration workspace identity has no verified Git binding")?;
            if binding.top_level != Path::new(&workspace.path) {
                anyhow::bail!("migration workspace Git binding has a different top-level");
            }
            if binding.object_format != object_format {
                anyhow::bail!("migration workspace object format differs from policy");
            }
        }
        if let Some(runner) = &disposition.runner_snapshot {
            runner.clone().validate()?;
        }
        if let Some(authority) = &disposition.runner_termination_authority {
            authority.validate()?;
        }
    }
    Ok(dispositions)
}

fn require_nonblank<'a>(value: &'a str, label: &str) -> Result<&'a str> {
    if value.is_empty() || value.trim() != value {
        anyhow::bail!("{label} must be non-empty and have no surrounding whitespace");
    }
    Ok(value)
}
