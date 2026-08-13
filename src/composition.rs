use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::ffi::{CString, OsStr};
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;
use uuid::Uuid;

use crate::control_domain::StateRepositorySnapshot;
use crate::integrator::{
    git_output, RepositoryOperationLease, ResidueDiscardRequest, RiftWorkspaceManager,
};
use crate::sqlite::{
    CheckoutReconciliationState, CleanupState, DevelopmentWorkspace, DevelopmentWorkspaceStatus,
    QueueItem, RegisteredRemote, RegisteredRepository, ReplacementState, ResidueDiscardState,
    SeedRefreshState, SqliteQueue, WorkspaceIdentity, WorkspaceState,
};

const LEASE_SECONDS: i64 = 30;
const MAX_POLICY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RepositoryInitOptions {
    pub target_branch: String,
    pub remote: String,
    pub seed_path: Option<PathBuf>,
    pub workspace_root: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySnapshot {
    pub version: u32,
    pub policy: ValidationPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ValidationPolicy {
    None,
    Command {
        command: String,
        signoff: SignoffPolicy,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SignoffPolicy {
    None,
    Required {
        command: String,
        contexts: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    version: u32,
    integration: RawIntegration,
    #[serde(default)]
    state_repository: StateRepositorySnapshot,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIntegration {
    validation: RawValidation,
    signoff: SignoffPolicy,
    #[serde(default)]
    agent: Option<ProjectAgentConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectAgentConfig {
    pub model: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectControlPolicy {
    pub model: Option<String>,
    pub state_repository: StateRepositorySnapshot,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawValidation {
    command: String,
}

#[derive(Debug, Serialize)]
pub struct RepositoryStatus {
    pub repository: RegisteredRepository,
    pub integration_head: String,
    pub integration_clean: bool,
    pub seed_head: Option<String>,
    pub seed_clean: bool,
}

#[derive(Debug, Serialize)]
pub struct DevelopmentWorkspaceObservation {
    pub workspace: DevelopmentWorkspace,
    pub exists: bool,
    pub branch: Option<String>,
    pub head: Option<String>,
    pub clean: bool,
}

struct RepositoryGuard {
    operation: RepositoryOperationLease,
}

struct DurableDiscardIdentity {
    quarantine_name: String,
    inspected_identity: Option<(u64, u64, [u8; 32])>,
    pending_child_move: Option<crate::sqlite::ResidueChildMove>,
}

fn discard_identity(discard: &ResidueDiscardState) -> DurableDiscardIdentity {
    match discard {
        ResidueDiscardState::Pending { quarantine_name }
        | ResidueDiscardState::FailedPending {
            quarantine_name, ..
        } => DurableDiscardIdentity {
            quarantine_name: quarantine_name.clone(),
            inspected_identity: None,
            pending_child_move: None,
        },
        ResidueDiscardState::Inspected {
            quarantine_name,
            device,
            inode,
            tree_digest,
            child_move,
        }
        | ResidueDiscardState::FailedInspected {
            quarantine_name,
            device,
            inode,
            tree_digest,
            child_move,
            ..
        } => DurableDiscardIdentity {
            quarantine_name: quarantine_name.clone(),
            inspected_identity: Some((*device, *inode, *tree_digest)),
            pending_child_move: child_move.clone(),
        },
    }
}

fn failed_discard_state(
    cleanup: &CleanupState,
    quarantine_name: &str,
    message: String,
) -> ResidueDiscardState {
    match cleanup {
        CleanupState::ResidueDiscard { discard } => match discard_identity(discard)
            .inspected_identity
        {
            Some((device, inode, tree_digest)) => ResidueDiscardState::FailedInspected {
                quarantine_name: quarantine_name.to_string(),
                device,
                inode,
                tree_digest,
                child_move: match discard.as_ref() {
                    ResidueDiscardState::Inspected { child_move, .. }
                    | ResidueDiscardState::FailedInspected { child_move, .. } => child_move.clone(),
                    _ => None,
                },
                message,
            },
            None => ResidueDiscardState::FailedPending {
                quarantine_name: quarantine_name.to_string(),
                message,
            },
        },
        _ => ResidueDiscardState::FailedPending {
            quarantine_name: quarantine_name.to_string(),
            message,
        },
    }
}

fn require_discard_authority(
    workspace: &DevelopmentWorkspace,
    repo_key: &str,
    identity: &WorkspaceIdentity,
    quarantine_name: &str,
) -> Result<()> {
    if workspace.repo_key != repo_key
        || !matches!(
            workspace.status,
            DevelopmentWorkspaceStatus::CleanupPending | DevelopmentWorkspaceStatus::CleanupFailed
        )
        || workspace.identity.as_ref() != Some(identity)
    {
        anyhow::bail!("workspace residue-discard lifecycle authority changed");
    }
    let CleanupState::ResidueDiscard { discard } = &workspace.cleanup else {
        anyhow::bail!("workspace residue-discard authorization disappeared");
    };
    if discard_identity(discard).quarantine_name != quarantine_name {
        anyhow::bail!("workspace residue-discard quarantine identity changed");
    }
    Ok(())
}

impl RepositoryGuard {
    fn acquire(
        queue: SqliteQueue,
        integration_path: &Path,
        repo_key: &str,
        owner_id: &str,
    ) -> Result<Self> {
        Ok(Self {
            operation: RepositoryOperationLease::acquire(
                queue,
                integration_path,
                repo_key,
                owner_id,
                LEASE_SECONDS,
            )?,
        })
    }

    fn ensure(&self) -> Result<()> {
        self.operation.ensure()
    }

    fn run<I, S>(
        &self,
        program: &str,
        args: I,
        cwd: Option<&Path>,
        timeout: Duration,
        label: &str,
    ) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.operation
            .run_command(program, args, cwd, timeout, label)
    }

    fn git<I, S>(&self, cwd: &Path, args: I, label: &str) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.run("git", args, Some(cwd), Duration::from_secs(60), label)?;
        Ok(())
    }
}

pub struct RepositoryManager {
    queue: SqliteQueue,
    owner_id: String,
}

impl RepositoryManager {
    pub fn new(queue: SqliteQueue) -> Self {
        Self {
            queue,
            owner_id: format!("iq-composition-{}-{}", std::process::id(), Uuid::new_v4()),
        }
    }

    pub fn init(
        &self,
        integration_path: &Path,
        options: RepositoryInitOptions,
    ) -> Result<RegisteredRepository> {
        let integration_path = canonical_checkout(integration_path)?;
        validate_ref_component(&options.target_branch, "target branch")?;
        validate_git_branch(&integration_path, &options.target_branch, "target branch")?;
        validate_ref_component(&options.remote, "remote")?;
        let remote = resolve_remote_identity(&integration_path, &options.remote)?;
        validate_integration_checkout(&integration_path, &options.target_branch, &remote.name)?;
        let repo_key = repository_key(&integration_path, &options.target_branch)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &integration_path,
            &repo_key,
            &self.owner_id,
        )?;
        self.queue.save_registered_remote_intent(
            &repo_key,
            &self.owner_id,
            &integration_path,
            &options.target_branch,
            &remote,
        )?;
        verify_remote_identity(&integration_path, &remote)?;
        guard.git(
            &integration_path,
            ["fetch", &remote.name, &options.target_branch],
            "fetch target during repository registration",
        )?;
        let target_ref = format!("refs/remotes/{}/{}", remote.name, options.target_branch);
        let target_sha = git_output(&integration_path, ["rev-parse", &target_ref])?;
        require_exact_integration_head(&integration_path, &target_sha)?;
        reject_tracked_policy(&integration_path)?;
        let state_root = self
            .queue
            .path()
            .parent()
            .context("queue database has no state parent")?
            .join("repositories")
            .join(stable_component(&repo_key));
        let seed_path = absolute_managed_path(
            &options
                .seed_path
                .unwrap_or_else(|| state_root.join("seed-root/seed")),
        )?;
        let workspace_root = absolute_managed_path(
            &options
                .workspace_root
                .unwrap_or_else(|| state_root.join("workspaces")),
        )?;
        validate_managed_layout(&integration_path, &seed_path, &workspace_root)?;
        if let Some(existing) = self.queue.repository_if_exists(&repo_key)? {
            if existing.integration_path != integration_path
                || existing.target_branch != options.target_branch
                || existing.remote != remote
                || existing.seed.path() != Some(path_text(&seed_path)?)
                || existing.workspace_root != workspace_root
            {
                anyhow::bail!("repository {repo_key} is registered with different configuration");
            }
            if existing.seed_refresh.target_sha() != target_sha {
                self.queue
                    .refresh_registered_target(&repo_key, &self.owner_id, &target_sha)?;
            }
            let existing = self.queue.repository(&repo_key)?;
            self.reconcile_seed(&guard, &existing)?;
            let existing = self.queue.repository(&repo_key)?;
            self.reconcile_seed_refresh_locked(&guard, &existing)?;
            return self.queue.repository(&repo_key);
        }
        if self
            .queue
            .repository_for_integration_path(&integration_path)?
            .is_some()
        {
            anyhow::bail!("integration checkout is already registered for another target");
        }
        let seed_name = seed_path
            .file_name()
            .and_then(|value| value.to_str())
            .context("seed path must have a UTF-8 leaf name")?;
        validate_workspace_component(seed_name, "seed name")?;
        let seed_root = seed_path.parent().context("seed path has no parent")?;
        let seed_scope = seed_scope(&repo_key);
        let seed_manager = self.root_manager(&integration_path, seed_root, &seed_scope, false)?;
        if seed_manager.expected_path(seed_name)? != seed_path {
            anyhow::bail!("seed path does not match its exact managed Rift path");
        }
        let timestamp = chrono::Utc::now().to_rfc3339();
        let repository = RegisteredRepository {
            key: repo_key.clone(),
            integration_path: integration_path.clone(),
            target_branch: options.target_branch,
            remote,
            seed: WorkspaceState::CreationIntent {
                path: path_text(&seed_path)?.to_string(),
            },
            workspace_root,
            checkout_reconciliation: CheckoutReconciliationState::Ready {
                target_sha: target_sha.clone(),
            },
            seed_refresh: SeedRefreshState::Pending {
                target_sha: target_sha.clone(),
            },
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        self.queue
            .save_repository_intent(&self.owner_id, &repository)?;
        self.create_or_reconcile_seed(&guard, &seed_manager, &repository, seed_name)?;
        self.sync_seed_locked(&guard, &self.queue.repository(&repo_key)?)?;
        self.queue.repository(&repo_key)
    }

    pub fn list(&self) -> Result<Vec<RegisteredRepository>> {
        self.queue.list_repositories()
    }

    pub fn inspect_local_policy(&self, repo_key: &str) -> Result<PolicySnapshot> {
        let repository = self.queue.repository(repo_key)?;
        verify_remote_identity(&repository.integration_path, &repository.remote)?;
        let _guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.integration_path,
            repo_key,
            &self.owner_id,
        )?;
        let (policy, _, _) = load_local_policy(&repository.integration_path)?;
        Ok(policy)
    }

    pub fn status(&self, repo_key: &str) -> Result<RepositoryStatus> {
        let repository = self.queue.repository(repo_key)?;
        let integration_head = git_output(&repository.integration_path, ["rev-parse", "HEAD"])?;
        let integration_clean = is_clean(&repository.integration_path)?;
        let seed_path = repository.seed.path().map(PathBuf::from);
        let (seed_head, seed_clean) = match seed_path.as_deref() {
            Some(path) if entry_exists(path)? => (
                Some(git_output(path, ["rev-parse", "HEAD"])?),
                is_clean(path)?,
            ),
            _ => (None, false),
        };
        Ok(RepositoryStatus {
            repository,
            integration_head,
            integration_clean,
            seed_head,
            seed_clean,
        })
    }

    pub fn create_workspace(&self, repo_key: &str, name: &str) -> Result<DevelopmentWorkspace> {
        validate_workspace_component(name, "workspace name")?;
        let repository = self.queue.repository(repo_key)?;
        verify_remote_identity(&repository.integration_path, &repository.remote)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.integration_path,
            repo_key,
            &self.owner_id,
        )?;
        self.reconcile_seed(&guard, &repository)?;
        let base_sha = self.sync_seed_locked(&guard, &self.queue.repository(repo_key)?)?;
        let repository = self.queue.repository(repo_key)?;
        let manager = self.development_manager(&repository)?;
        self.reconcile_development_workspaces(&guard, &repository, &manager)?;
        if let Some(existing) = self
            .queue
            .list_development_workspaces(Some(repo_key))?
            .into_iter()
            .find(|workspace| workspace.name == name)
        {
            if existing.status == DevelopmentWorkspaceStatus::Creating {
                return self.resume_workspace_creation(&guard, &repository, &manager, existing);
            }
            anyhow::bail!("development workspace name is already allocated: {name}");
        }
        let id = Uuid::new_v4().to_string();
        let branch = format!("iq-{id}-{name}");
        validate_git_branch(
            &repository.integration_path,
            &branch,
            "derived development branch",
        )?;
        let path = manager.expected_path(&id)?;
        let timestamp = chrono::Utc::now().to_rfc3339();
        let workspace = DevelopmentWorkspace {
            id: id.clone(),
            repo_key: repo_key.to_string(),
            name: name.to_string(),
            identity: None,
            path,
            branch,
            base_sha,
            status: DevelopmentWorkspaceStatus::Creating,
            cleanup: CleanupState::Pending,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        self.queue
            .save_development_workspace(&self.owner_id, &workspace)?;
        self.resume_workspace_creation(&guard, &repository, &manager, workspace)
    }

    pub fn workspaces(&self, repo_key: Option<&str>) -> Result<Vec<DevelopmentWorkspace>> {
        self.queue.list_development_workspaces(repo_key)
    }

    pub fn workspace_status(&self, id: &str) -> Result<DevelopmentWorkspaceObservation> {
        let workspace = self.queue.workspace(id)?;
        let exists = entry_exists(&workspace.path)?;
        let branch = exists
            .then(|| git_output(&workspace.path, ["branch", "--show-current"]))
            .transpose()?;
        let head = exists
            .then(|| git_output(&workspace.path, ["rev-parse", "HEAD"]))
            .transpose()?;
        let clean = exists && is_clean(&workspace.path)?;
        Ok(DevelopmentWorkspaceObservation {
            workspace,
            exists,
            branch,
            head,
            clean,
        })
    }

    pub fn submit(
        &self,
        workspace_id: &str,
        replace: Option<&str>,
    ) -> Result<(crate::sqlite::LocalSubmission, QueueItem)> {
        let workspace = self.queue.workspace(workspace_id)?;
        let replacement = replace.is_some();
        if (!replacement && workspace.status != DevelopmentWorkspaceStatus::Active)
            || (replacement && workspace.status != DevelopmentWorkspaceStatus::Submitted)
        {
            anyhow::bail!("development workspace is not in a valid submission state");
        }
        let repository = self.queue.repository(&workspace.repo_key)?;
        verify_remote_identity(&repository.integration_path, &repository.remote)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.integration_path,
            &repository.key,
            &self.owner_id,
        )?;
        let manager = self.development_manager(&repository)?;
        let identity = workspace
            .identity
            .as_ref()
            .context("development workspace has no durable Rift identity")?;
        if manager.verify_retained(identity)? != workspace.path {
            anyhow::bail!("development workspace path differs from its durable Rift identity");
        }
        reject_tracked_policy(&workspace.path)?;
        let submission = if let Some(submission) =
            self.queue.creating_local_submission(&workspace.id)?
        {
            if submission.repo_key != repository.key
                || submission.replaces_item_id.as_deref() != replace
            {
                anyhow::bail!("workspace has a different incomplete immutable submission intent");
            }
            submission
        } else {
            require_workspace_submission_state(&workspace)?;
            let head = git_output(&workspace.path, ["rev-parse", "HEAD"])?;
            if !is_ancestor(&workspace.path, &workspace.base_sha, &head)? {
                anyhow::bail!("development workspace HEAD is not based on its exact recorded base");
            }
            if head == workspace.base_sha {
                anyhow::bail!("development workspace has no committed change to submit");
            }
            self.queue.begin_local_submission(
                &repository.key,
                &self.owner_id,
                &workspace.id,
                &head,
                replace,
            )?
        };
        let intent_sha = submission.commit_sha.as_str();
        let private_sha =
            resolve_optional_ref(&repository.integration_path, &submission.private_ref)?;
        let staging_sha =
            resolve_optional_ref(&repository.integration_path, &submission.staging_ref)?;
        if private_sha.as_deref().is_some_and(|sha| sha != intent_sha)
            || staging_sha.as_deref().is_some_and(|sha| sha != intent_sha)
        {
            anyhow::bail!("immutable submission ref identity differs from its creation intent");
        }
        if private_sha.is_none() {
            if staging_sha.is_none() {
                guard.git(
                    &repository.integration_path,
                    [
                        "fetch",
                        path_text(&workspace.path)?,
                        &format!("{}:{}", intent_sha, submission.staging_ref),
                    ],
                    "stage immutable local submission",
                )?;
            }
            if resolve_optional_ref(&repository.integration_path, &submission.staging_ref)?
                .as_deref()
                != Some(intent_sha)
            {
                anyhow::bail!("staged local submission does not resolve to exact workspace HEAD");
            }
            guard.git(
                &repository.integration_path,
                [
                    "update-ref",
                    &submission.private_ref,
                    intent_sha,
                    "0000000000000000000000000000000000000000",
                ],
                "publish immutable local submission",
            )?;
        }
        if resolve_optional_ref(&repository.integration_path, &submission.staging_ref)?.is_some() {
            guard.git(
                &repository.integration_path,
                ["update-ref", "-d", &submission.staging_ref],
                "remove local submission staging ref",
            )?;
        }
        if resolve_optional_ref(&repository.integration_path, &submission.private_ref)?.as_deref()
            != Some(intent_sha)
        {
            anyhow::bail!("immutable local submission ref was not published exactly");
        }
        guard.ensure()?;
        let state_repository =
            load_project_control_only(&repository.integration_path)?.state_repository;
        crate::state_repository::repository(&state_repository)?.verify()?;
        self.queue.finalize_local_submission(
            &repository.key,
            &self.owner_id,
            &submission.id,
            &Value::Object(Default::default()),
            &state_repository,
        )
    }

    pub fn remove_workspace(&self, id: &str) -> Result<DevelopmentWorkspace> {
        let workspace = self.queue.workspace(id)?;
        let repository = self.queue.repository(&workspace.repo_key)?;
        verify_remote_identity(&repository.integration_path, &repository.remote)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.integration_path,
            &repository.key,
            &self.owner_id,
        )?;
        let manager = self.development_manager(&repository)?;
        self.remove_workspace_locked(&guard, &repository, &manager, &workspace)
    }

    pub fn discard_workspace_residue(&self, id: &str) -> Result<DevelopmentWorkspace> {
        let workspace = self.queue.workspace(id)?;
        let repository = self.queue.repository(&workspace.repo_key)?;
        verify_remote_identity(&repository.integration_path, &repository.remote)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.integration_path,
            &repository.key,
            &self.owner_id,
        )?;
        let manager = self.development_manager(&repository)?;
        self.discard_workspace_residue_locked(&guard, &repository, &manager, id, true)
    }

    fn discard_workspace_residue_locked(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
        manager: &RiftWorkspaceManager,
        id: &str,
        authorize_new: bool,
    ) -> Result<DevelopmentWorkspace> {
        let mut workspace = self.queue.workspace(id)?;
        if workspace.repo_key != repository.key {
            anyhow::bail!("workspace repository identity changed");
        }
        if !matches!(
            workspace.status,
            DevelopmentWorkspaceStatus::CleanupPending | DevelopmentWorkspaceStatus::CleanupFailed
        ) {
            anyhow::bail!(
                "workspace {} in status {} cannot discard residue",
                workspace.id,
                workspace.status
            );
        }
        let identity = workspace
            .identity
            .clone()
            .context("workspace residue discard requires a durable Rift identity")?;
        let expected_path = manager.expected_path(&workspace.id)?;
        if workspace.path != expected_path || Path::new(&identity.path) != expected_path {
            anyhow::bail!(
                "workspace residue is not at its exact expected IQ-owned path: {}",
                workspace.path.display()
            );
        }
        let durable_discard = match &workspace.cleanup {
            CleanupState::ResidueDiscard { discard } => discard_identity(discard),
            _ if authorize_new => {
                let quarantine_name = manager.new_residue_quarantine_name(&workspace.id)?;
                workspace = self.queue.update_development_workspace_cleanup(
                    &repository.key,
                    &self.owner_id,
                    &workspace.id,
                    DevelopmentWorkspaceStatus::CleanupPending,
                    &CleanupState::ResidueDiscard {
                        discard: Box::new(ResidueDiscardState::Pending {
                            quarantine_name: quarantine_name.clone(),
                        }),
                    },
                )?;
                DurableDiscardIdentity {
                    quarantine_name,
                    inspected_identity: None,
                    pending_child_move: None,
                }
            }
            _ => anyhow::bail!(
                "workspace {} has no durable residue-discard authorization",
                workspace.id
            ),
        };
        let DurableDiscardIdentity {
            quarantine_name,
            inspected_identity,
            pending_child_move,
        } = durable_discard;
        let operation_result = manager.discard_retained_residue(
            ResidueDiscardRequest {
                identity: &identity,
                quarantine_name: &quarantine_name,
                inspected_identity,
                pending_child_move,
            },
            |device, inode, tree_digest, child_move| {
                guard.ensure()?;
                let current = self.queue.workspace(&workspace.id)?;
                require_discard_authority(&current, &repository.key, &identity, &quarantine_name)?;
                self.queue.update_development_workspace_cleanup(
                    &repository.key,
                    &self.owner_id,
                    &workspace.id,
                    DevelopmentWorkspaceStatus::CleanupPending,
                    &CleanupState::ResidueDiscard {
                        discard: Box::new(ResidueDiscardState::Inspected {
                            quarantine_name: quarantine_name.clone(),
                            device,
                            inode,
                            tree_digest,
                            child_move: child_move.cloned(),
                        }),
                    },
                )?;
                Ok(())
            },
            |gate| {
                guard.ensure()?;
                let current = self.queue.workspace(&workspace.id)?;
                require_discard_authority(&current, &repository.key, &identity, &quarantine_name)?;
                self.queue
                    .record_workspace_gc_debt(manager.registry_identity())?;
                gate.write_all(b"run\n")?;
                Ok(true)
            },
            || self.queue.lease_authority(&repository.key, &self.owner_id),
            || {
                guard.ensure()?;
                if entry_exists(&workspace.path)? {
                    anyhow::bail!(
                        "workspace path became occupied during residue discard: {}",
                        workspace.path.display()
                    );
                }
                self.queue.complete_development_workspace_cleanup(
                    &repository.key,
                    &self.owner_id,
                    &workspace.id,
                    manager.registry_identity(),
                )?;
                Ok(())
            },
        );
        match operation_result {
            Ok(()) => self.queue.workspace(&workspace.id),
            Err(error) => {
                let current = self.queue.workspace(&workspace.id)?;
                if current.status == DevelopmentWorkspaceStatus::Removed {
                    return Err(error);
                }
                let discard =
                    failed_discard_state(&current.cleanup, &quarantine_name, format!("{error:#}"));
                self.queue.update_development_workspace_cleanup(
                    &repository.key,
                    &self.owner_id,
                    &workspace.id,
                    DevelopmentWorkspaceStatus::CleanupFailed,
                    &CleanupState::ResidueDiscard {
                        discard: Box::new(discard),
                    },
                )?;
                Err(error)
            }
        }
    }

    pub fn cleanup_repo(&self, repo_key: &str) -> Result<Vec<DevelopmentWorkspace>> {
        let repository = self.queue.repository(repo_key)?;
        verify_remote_identity(&repository.integration_path, &repository.remote)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.integration_path,
            repo_key,
            &self.owner_id,
        )?;
        self.cleanup_terminal_agent_artifacts(repo_key)?;
        self.reconcile_seed(&guard, &repository)?;
        let repository = self.queue.repository(repo_key)?;
        let manager = self.development_manager(&repository)?;
        self.reconcile_development_workspaces(&guard, &repository, &manager)?;
        self.cleanup_replacements(&guard, &repository)?;
        let seed_result = self.reconcile_seed_refresh_locked(&guard, &repository);
        let mut results = Vec::new();
        for workspace in self.queue.list_development_workspaces(Some(repo_key))? {
            if matches!(
                workspace.status,
                DevelopmentWorkspaceStatus::CleanupPending
                    | DevelopmentWorkspaceStatus::CleanupFailed
            ) {
                results.push(self.remove_workspace_locked(
                    &guard,
                    &repository,
                    &manager,
                    &workspace,
                )?);
            }
        }
        seed_result?;
        Ok(results)
    }

    fn cleanup_terminal_agent_artifacts(&self, repo_key: &str) -> Result<()> {
        crate::integrator::cleanup_terminal_agent_artifacts(&self.queue, repo_key, true)
    }

    fn root_manager(
        &self,
        source: &Path,
        root: &Path,
        scope: &str,
        child_source: bool,
    ) -> Result<RiftWorkspaceManager> {
        let queue_id = self.queue.database_id()?;
        let generation = self.queue.workspace_root_generation(scope)?;
        let manager = if child_source {
            RiftWorkspaceManager::new_child_source(
                source.to_path_buf(),
                root.to_path_buf(),
                scope.to_string(),
                None,
                &queue_id,
                generation,
            )?
        } else {
            RiftWorkspaceManager::new(
                source.to_path_buf(),
                root.to_path_buf(),
                scope.to_string(),
                None,
                &queue_id,
                generation,
            )?
        };
        self.queue.register_workspace_root(
            scope,
            source,
            manager.source_id(),
            manager.root(),
            manager.registry_identity(),
        )?;
        Ok(manager)
    }

    fn development_manager(
        &self,
        repository: &RegisteredRepository,
    ) -> Result<RiftWorkspaceManager> {
        let seed = repository
            .seed
            .identity()
            .context("registered repository seed has no Rift identity")?;
        self.root_manager(
            Path::new(&seed.path),
            &repository.workspace_root,
            &development_scope(&repository.key),
            true,
        )
    }

    fn create_or_reconcile_seed(
        &self,
        guard: &RepositoryGuard,
        manager: &RiftWorkspaceManager,
        repository: &RegisteredRepository,
        seed_name: &str,
    ) -> Result<()> {
        let expected = manager.expected_path(seed_name)?;
        let existing = manager
            .list()?
            .into_iter()
            .find(|identity| Path::new(&identity.path) == expected);
        let identity = if let Some(identity) = existing {
            identity
        } else {
            let generation = self
                .queue
                .advance_workspace_generation(&seed_scope(&repository.key))?;
            manager.persist_generation(generation)?;
            let (path, rift_id) = manager.create(
                seed_name,
                |gate| {
                    guard.ensure()?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || {
                    guard.ensure()?;
                    Ok(crate::sqlite::ExecutionAuthority::Active)
                },
            )?;
            WorkspaceIdentity {
                path: path_text(&path)?.to_string(),
                rift_id,
                source_rift_id: manager.source_id().to_string(),
            }
        };
        manager.verify_retained(&identity)?;
        self.queue
            .set_repository_seed_identity(&repository.key, &self.owner_id, &identity)?;
        Ok(())
    }

    fn reconcile_seed(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
    ) -> Result<()> {
        let seed_path = PathBuf::from(
            repository
                .seed
                .path()
                .context("registered repository seed has no path")?,
        );
        let seed_root = seed_path.parent().context("seed path has no parent")?;
        let manager = self.root_manager(
            &repository.integration_path,
            seed_root,
            &seed_scope(&repository.key),
            false,
        )?;
        match &repository.seed {
            WorkspaceState::CreationIntent { .. } => {
                let seed_name = seed_path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .context("seed path has no UTF-8 name")?;
                self.create_or_reconcile_seed(guard, &manager, repository, seed_name)
            }
            WorkspaceState::Retained { identity } => {
                if manager.verify_retained(identity)? != seed_path {
                    anyhow::bail!("registered seed moved from its exact IQ-owned path");
                }
                Ok(())
            }
            _ => anyhow::bail!("registered repository has invalid seed lifecycle state"),
        }
    }

    fn sync_seed_locked(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
    ) -> Result<String> {
        validate_integration_checkout(
            &repository.integration_path,
            &repository.target_branch,
            &repository.remote.name,
        )?;
        verify_remote_identity(&repository.integration_path, &repository.remote)?;
        guard.git(
            &repository.integration_path,
            ["fetch", &repository.remote.name, &repository.target_branch],
            "fetch target during seed refresh",
        )?;
        let target_ref = format!(
            "refs/remotes/{}/{}",
            repository.remote.name, repository.target_branch
        );
        let target_sha = git_output(&repository.integration_path, ["rev-parse", &target_ref])?;
        self.sync_seed_to_target_locked(guard, repository, &target_sha)
    }

    fn reconcile_seed_refresh_locked(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
    ) -> Result<String> {
        let checkout_target = match &repository.checkout_reconciliation {
            CheckoutReconciliationState::Ready { target_sha }
            | CheckoutReconciliationState::Pending { target_sha }
            | CheckoutReconciliationState::Failed { target_sha, .. } => target_sha,
        };
        reconcile_registered_checkout(
            &self.queue,
            repository,
            &self.owner_id,
            checkout_target,
            |path, target_sha| {
                guard.git(
                    path,
                    ["reset", "--hard", target_sha],
                    "resume registered checkout exact reset",
                )
            },
        )?;
        match &repository.seed_refresh {
            SeedRefreshState::Ready { target_sha } => Ok(target_sha.clone()),
            SeedRefreshState::Pending { target_sha }
            | SeedRefreshState::Failed { target_sha, .. } => {
                self.sync_seed_to_target_locked(guard, repository, target_sha)
            }
        }
    }

    fn sync_seed_to_target_locked(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
        target_sha: &str,
    ) -> Result<String> {
        reconcile_registered_checkout(
            &self.queue,
            repository,
            &self.owner_id,
            target_sha,
            |path, target_sha| {
                guard.git(
                    path,
                    ["reset", "--hard", target_sha],
                    "reset registered checkout to exact fetched target",
                )
            },
        )?;
        self.queue.update_seed_refresh(
            &repository.key,
            &self.owner_id,
            &SeedRefreshState::Pending {
                target_sha: target_sha.to_string(),
            },
        )?;
        let result = (|| {
            let seed = repository
                .seed
                .identity()
                .context("registered seed has no Rift identity")?;
            let seed_path = Path::new(&seed.path);
            guard.git(
                seed_path,
                [
                    "fetch",
                    path_text(&repository.integration_path)?,
                    target_sha,
                ],
                "fetch exact target into seed",
            )?;
            guard.git(
                seed_path,
                ["checkout", "--force", "--detach", target_sha],
                "detach seed at exact target",
            )?;
            guard.git(
                seed_path,
                ["reset", "--hard", target_sha],
                "reset seed to exact target",
            )?;
            guard.git(seed_path, ["clean", "-ffd"], "clean refreshed seed")?;
            if git_output(seed_path, ["rev-parse", "HEAD"])? != target_sha
                || !is_clean(seed_path)?
                || !git_output(seed_path, ["branch", "--show-current"])?.is_empty()
            {
                anyhow::bail!("seed did not reach exact clean detached target state");
            }
            guard.ensure()?;
            Ok(target_sha.to_string())
        })();
        match result {
            Ok(target_sha) => {
                self.queue.update_seed_refresh(
                    &repository.key,
                    &self.owner_id,
                    &SeedRefreshState::Ready {
                        target_sha: target_sha.clone(),
                    },
                )?;
                Ok(target_sha)
            }
            Err(error) => {
                let state = SeedRefreshState::Failed {
                    target_sha: target_sha.to_string(),
                    message: format!("{error:#}"),
                };
                self.queue
                    .update_seed_refresh(&repository.key, &self.owner_id, &state)?;
                Err(error)
            }
        }
    }

    fn resume_workspace_creation(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
        manager: &RiftWorkspaceManager,
        workspace: DevelopmentWorkspace,
    ) -> Result<DevelopmentWorkspace> {
        let expected = manager.expected_path(&workspace.id)?;
        if workspace.path != expected {
            anyhow::bail!("development workspace creation path changed");
        }
        let existing = manager
            .list()?
            .into_iter()
            .find(|identity| Path::new(&identity.path) == expected);
        let identity = if let Some(identity) = existing {
            identity
        } else {
            let scope = development_scope(&repository.key);
            let generation = self.queue.advance_workspace_generation(&scope)?;
            manager.persist_generation(generation)?;
            let (path, rift_id) = manager.create(
                &workspace.id,
                |gate| {
                    guard.ensure()?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || {
                    guard.ensure()?;
                    Ok(crate::sqlite::ExecutionAuthority::Active)
                },
            )?;
            WorkspaceIdentity {
                path: path_text(&path)?.to_string(),
                rift_id,
                source_rift_id: manager.source_id().to_string(),
            }
        };
        manager.verify_retained(&identity)?;
        let current_branch = git_output(&workspace.path, ["branch", "--show-current"])?;
        let current_head = git_output(&workspace.path, ["rev-parse", "HEAD"])?;
        if current_head != workspace.base_sha || !is_clean(&workspace.path)? {
            anyhow::bail!("interrupted development workspace has changed; IQ preserves it");
        }
        if current_branch.is_empty() {
            guard.git(
                &workspace.path,
                ["switch", "--create", &workspace.branch],
                "create development branch",
            )?;
        } else if current_branch != workspace.branch {
            anyhow::bail!("interrupted development workspace is on an unknown branch");
        }
        guard.ensure()?;
        self.queue.set_development_workspace_identity(
            &repository.key,
            &self.owner_id,
            &workspace.id,
            &identity,
        )
    }

    fn reconcile_development_workspaces(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
        manager: &RiftWorkspaceManager,
    ) -> Result<()> {
        for workspace in self
            .queue
            .list_development_workspaces(Some(&repository.key))?
        {
            if workspace.status == DevelopmentWorkspaceStatus::Creating {
                self.resume_workspace_creation(guard, repository, manager, workspace)?;
            } else if let Some(identity) = workspace.identity.as_ref() {
                if workspace.status == DevelopmentWorkspaceStatus::Removed {
                    if entry_exists(&workspace.path)? {
                        anyhow::bail!(
                            "removed development workspace path reappeared: {}",
                            workspace.path.display()
                        );
                    }
                } else if entry_exists(&workspace.path)? {
                    if !matches!(
                        workspace.status,
                        DevelopmentWorkspaceStatus::CleanupPending
                            | DevelopmentWorkspaceStatus::CleanupFailed
                    ) {
                        manager.verify_retained(identity)?;
                    }
                } else if !matches!(
                    workspace.status,
                    DevelopmentWorkspaceStatus::CleanupPending
                        | DevelopmentWorkspaceStatus::CleanupFailed
                ) {
                    anyhow::bail!(
                        "active development Rift is missing: {}",
                        workspace.path.display()
                    );
                }
            }
        }
        Ok(())
    }

    fn remove_workspace_locked(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
        manager: &RiftWorkspaceManager,
        workspace: &DevelopmentWorkspace,
    ) -> Result<DevelopmentWorkspace> {
        if matches!(workspace.cleanup, CleanupState::ResidueDiscard { .. }) {
            return self.discard_workspace_residue_locked(
                guard,
                repository,
                manager,
                &workspace.id,
                false,
            );
        }
        if workspace.status == DevelopmentWorkspaceStatus::Removed {
            if entry_exists(&workspace.path)? {
                anyhow::bail!(
                    "removed development workspace path reappeared: {}",
                    workspace.path.display()
                );
            }
            return Ok(workspace.clone());
        }
        if !matches!(
            workspace.status,
            DevelopmentWorkspaceStatus::Creating
                | DevelopmentWorkspaceStatus::CleanupPending
                | DevelopmentWorkspaceStatus::CleanupFailed
        ) {
            anyhow::bail!(
                "workspace {} has no durable cleanup authorization",
                workspace.id
            );
        }
        let Some(identity) = workspace.identity.as_ref() else {
            if workspace.status != DevelopmentWorkspaceStatus::Creating {
                anyhow::bail!("workspace cleanup has no durable Rift identity");
            }
            if entry_exists(&workspace.path)? {
                anyhow::bail!(
                    "incomplete workspace has an unknown path entry: {}",
                    workspace.path.display()
                );
            }
            return self.queue.update_development_workspace_cleanup(
                &repository.key,
                &self.owner_id,
                &workspace.id,
                DevelopmentWorkspaceStatus::Removed,
                &CleanupState::Complete {
                    completed_at: chrono::Utc::now().to_rfc3339(),
                },
            );
        };
        let retained = manager.resolve_retained(identity)?;
        if let Some(actual) = retained.as_ref() {
            let actual_path = Path::new(&actual.path);
            if !is_clean(actual_path)? || has_git_operation(actual_path)? {
                let state = CleanupState::Failed {
                    message: "workspace is dirty or has an active Git operation; IQ preserved it"
                        .into(),
                };
                self.queue.update_development_workspace_cleanup(
                    &repository.key,
                    &self.owner_id,
                    &workspace.id,
                    DevelopmentWorkspaceStatus::CleanupFailed,
                    &state,
                )?;
                anyhow::bail!(
                    "workspace {} is dirty; preserved without removal",
                    workspace.id
                );
            }
            if workspace.status != DevelopmentWorkspaceStatus::Creating {
                let integrated_sha = self
                    .queue
                    .integrated_submission_sha(&workspace.id)?
                    .context("workspace has no integrated immutable submission")?;
                if git_output(actual_path, ["rev-parse", "HEAD"])? != integrated_sha {
                    anyhow::bail!(
                        "workspace HEAD differs from its integrated immutable submission"
                    );
                }
            }
            self.queue.update_development_workspace_cleanup(
                &repository.key,
                &self.owner_id,
                &workspace.id,
                DevelopmentWorkspaceStatus::CleanupPending,
                &CleanupState::Pending,
            )?;
        }
        self.remove_rift(guard, manager, identity)?;
        self.queue.update_development_workspace_cleanup(
            &repository.key,
            &self.owner_id,
            &workspace.id,
            DevelopmentWorkspaceStatus::Removed,
            &CleanupState::Complete {
                completed_at: chrono::Utc::now().to_rfc3339(),
            },
        )
    }

    fn remove_rift(
        &self,
        guard: &RepositoryGuard,
        manager: &RiftWorkspaceManager,
        identity: &WorkspaceIdentity,
    ) -> Result<()> {
        manager.remove_retained(
            identity,
            |gate| {
                guard.ensure()?;
                self.queue
                    .record_workspace_gc_debt(manager.registry_identity())?;
                gate.write_all(b"run\n")?;
                Ok(true)
            },
            || {
                guard.ensure()?;
                Ok(crate::sqlite::ExecutionAuthority::Active)
            },
            || {
                self.queue
                    .clear_workspace_gc_debt(manager.registry_identity())
            },
        )?;
        Ok(())
    }

    fn cleanup_replacements(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
    ) -> Result<()> {
        let items = self
            .queue
            .list_items()?
            .into_iter()
            .filter(|item| {
                item.repo_key == repository.key
                    && matches!(item.replacement, ReplacementState::CleanupPending { .. })
            })
            .collect::<Vec<_>>();
        if items.is_empty() {
            return Ok(());
        }
        let integration_root = self
            .queue
            .workspace_root_path(&repository.key)?
            .context("replacement cleanup has no persisted integration workspace root")?;
        let manager = self.root_manager(
            &repository.integration_path,
            &integration_root,
            &repository.key,
            false,
        )?;
        let mut preserved_dirty = Vec::new();
        for item in items {
            let ReplacementState::CleanupPending {
                old_attempt_id,
                old_workspace,
            } = &item.replacement
            else {
                continue;
            };
            let retained = manager.resolve_retained(old_workspace)?;
            if let Some(actual) = retained.as_ref() {
                let path = Path::new(&actual.path);
                if !is_clean(path)? || has_git_operation(path)? {
                    preserved_dirty.push(item.id);
                    continue;
                }
            }
            self.remove_rift(guard, &manager, old_workspace)?;
            self.queue.finish_replacement_cleanup(
                &repository.key,
                &self.owner_id,
                &item.id,
                old_attempt_id,
            )?;
        }
        if !preserved_dirty.is_empty() {
            anyhow::bail!(
                "replacement cleanup preserved dirty integration work for item(s): {}",
                preserved_dirty.join(", ")
            );
        }
        Ok(())
    }
}

pub(crate) fn reconcile_registered_checkout(
    queue: &SqliteQueue,
    repository: &RegisteredRepository,
    owner_id: &str,
    target_sha: &str,
    mut reset: impl FnMut(&Path, &str) -> Result<()>,
) -> Result<()> {
    require_full_sha(target_sha)?;
    validate_integration_checkout(
        &repository.integration_path,
        &repository.target_branch,
        &repository.remote.name,
    )?;
    verify_remote_identity(&repository.integration_path, &repository.remote)?;
    let remote_ref = format!(
        "refs/remotes/{}/{}",
        repository.remote.name, repository.target_branch
    );
    let fetched_target = git_output(&repository.integration_path, ["rev-parse", &remote_ref])?;
    if fetched_target != target_sha {
        anyhow::bail!(
            "fetched target {fetched_target} differs from checkout reconciliation target {target_sha}"
        );
    }
    let head = git_output(&repository.integration_path, ["rev-parse", "HEAD"])?;
    if matches!(
        &repository.checkout_reconciliation,
        CheckoutReconciliationState::Ready { target_sha: ready } if ready == target_sha
    ) && head == target_sha
    {
        return Ok(());
    }
    queue.update_checkout_reconciliation(
        &repository.key,
        owner_id,
        &CheckoutReconciliationState::Pending {
            target_sha: target_sha.to_string(),
        },
    )?;
    let result = (|| {
        if head != target_sha {
            reset(&repository.integration_path, target_sha)?;
        }
        validate_integration_checkout(
            &repository.integration_path,
            &repository.target_branch,
            &repository.remote.name,
        )?;
        require_exact_integration_head(&repository.integration_path, target_sha)
    })();
    match result {
        Ok(()) => queue.update_checkout_reconciliation(
            &repository.key,
            owner_id,
            &CheckoutReconciliationState::Ready {
                target_sha: target_sha.to_string(),
            },
        ),
        Err(error) => {
            queue.update_checkout_reconciliation(
                &repository.key,
                owner_id,
                &CheckoutReconciliationState::Failed {
                    target_sha: target_sha.to_string(),
                    message: format!("{error:#}"),
                },
            )?;
            Err(error)
        }
    }
}

pub fn load_local_policy(repo: &Path) -> Result<(PolicySnapshot, String, String)> {
    let (policy, json, digest, _) = load_project_control_policy(repo)?;
    Ok((policy, json, digest))
}

pub fn load_project_control_only(repo: &Path) -> Result<ProjectControlPolicy> {
    let (_, _, _, control) = load_project_control_policy(repo)?;
    Ok(control)
}

pub fn load_project_control_policy(
    repo: &Path,
) -> Result<(PolicySnapshot, String, String, ProjectControlPolicy)> {
    reject_tracked_policy(repo)?;
    let config_directory = repo.join(".iq");
    let config_path = config_directory.join("config.json");
    let directory_metadata = match fs::symlink_metadata(&config_directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return default_project_control_policy()
        }
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {}", config_directory.display()))
        }
    };
    if directory_metadata.file_type().is_symlink() || !directory_metadata.is_dir() {
        anyhow::bail!(
            "local IQ policy directory must be a regular non-symlink directory: {}",
            config_directory.display()
        );
    }
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&config_directory)
        .with_context(|| {
            format!(
                "open local IQ policy directory {}",
                config_directory.display()
            )
        })?;
    verify_policy_directory(&config_directory, &directory, &directory_metadata)?;
    let file_name = CString::new("config.json").expect("static policy file name has no NUL");
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        let error = std::io::Error::last_os_error();
        verify_policy_directory(&config_directory, &directory, &directory_metadata)?;
        if error.kind() == std::io::ErrorKind::NotFound {
            return default_project_control_policy();
        }
        return Err(error).context("open local IQ policy config.json");
    }
    let file = unsafe { fs::File::from_raw_fd(descriptor) };
    verify_policy_directory(&config_directory, &directory, &directory_metadata)?;
    let metadata = file
        .metadata()
        .context("inspect open local IQ policy config.json")?;
    if !metadata.is_file() {
        anyhow::bail!("local IQ policy config.json must be a regular non-symlink file");
    }
    let mut contents = Vec::new();
    file.take((MAX_POLICY_BYTES + 1) as u64)
        .read_to_end(&mut contents)
        .context("read local IQ policy config.json")?;
    verify_policy_directory(&config_directory, &directory, &directory_metadata)?;
    if contents.len() > MAX_POLICY_BYTES {
        anyhow::bail!("local IQ policy exceeds the {MAX_POLICY_BYTES} byte limit");
    }
    let raw: RawConfig = serde_json::from_slice(&contents).with_context(|| {
        format!(
            "parse strict versioned local policy {}",
            config_path.display()
        )
    })?;
    if raw.version != 2 {
        anyhow::bail!(
            "unsupported .iq/config.json version {}; expected 2",
            raw.version
        );
    }
    let validation_command =
        exact_nonblank(raw.integration.validation.command, "validation command")?;
    let signoff = validate_signoff(raw.integration.signoff)?;
    let (policy, json, digest) = canonical_policy_snapshot(ValidationPolicy::Command {
        command: validation_command,
        signoff,
    })?;
    let model = raw
        .integration
        .agent
        .map(|agent| exact_nonblank(agent.model, "integration agent model"))
        .transpose()?;
    let state_repository = raw.state_repository.validate()?;
    Ok((
        policy,
        json,
        digest,
        ProjectControlPolicy {
            model,
            state_repository,
        },
    ))
}

fn default_project_control_policy() -> Result<(PolicySnapshot, String, String, ProjectControlPolicy)>
{
    let (policy, json, digest) = canonical_policy_snapshot(ValidationPolicy::None)?;
    Ok((
        policy,
        json,
        digest,
        ProjectControlPolicy {
            model: None,
            state_repository: StateRepositorySnapshot::Local,
        },
    ))
}

fn verify_policy_directory(
    path: &Path,
    directory: &fs::File,
    original: &fs::Metadata,
) -> Result<()> {
    let current = fs::symlink_metadata(path)
        .with_context(|| format!("reinspect local IQ policy directory {}", path.display()))?;
    let open = directory
        .metadata()
        .with_context(|| format!("inspect open local IQ policy directory {}", path.display()))?;
    if current.file_type().is_symlink()
        || !current.is_dir()
        || original.dev() != open.dev()
        || original.ino() != open.ino()
        || current.dev() != open.dev()
        || current.ino() != open.ino()
    {
        anyhow::bail!("local IQ policy directory identity changed while reading policy");
    }
    Ok(())
}

fn canonical_policy_snapshot(policy: ValidationPolicy) -> Result<(PolicySnapshot, String, String)> {
    let policy = PolicySnapshot { version: 1, policy };
    let json = serde_json::to_string(&policy)?;
    let digest = format!("{:x}", Sha256::digest(json.as_bytes()));
    Ok((policy, json, digest))
}

pub(crate) fn no_validation_policy_snapshot() -> Result<(String, String)> {
    let (_, snapshot, digest) = canonical_policy_snapshot(ValidationPolicy::None)?;
    Ok((snapshot, digest))
}

pub fn verify_policy_snapshot(policy_json: &str, digest: &str) -> Result<PolicySnapshot> {
    let actual = format!("{:x}", Sha256::digest(policy_json.as_bytes()));
    if actual != digest {
        anyhow::bail!("persisted policy SHA-256 digest does not match its snapshot");
    }
    let mut policy: PolicySnapshot =
        serde_json::from_str(policy_json).context("parse persisted strict policy snapshot")?;
    if policy.version != 1 {
        anyhow::bail!("persisted policy snapshot has an unsupported version");
    }
    policy.policy = match policy.policy {
        ValidationPolicy::None => ValidationPolicy::None,
        ValidationPolicy::Command { command, signoff } => ValidationPolicy::Command {
            command: exact_nonblank(command, "persisted validation command")?,
            signoff: validate_signoff(signoff)?,
        },
    };
    if serde_json::to_string(&policy)? != policy_json {
        anyhow::bail!("persisted policy snapshot is not canonical JSON");
    }
    Ok(policy)
}

fn validate_signoff(signoff: SignoffPolicy) -> Result<SignoffPolicy> {
    match signoff {
        SignoffPolicy::None => Ok(SignoffPolicy::None),
        SignoffPolicy::Required { command, contexts } => {
            let command = exact_nonblank(command, "signoff command")?;
            if contexts.is_empty() {
                anyhow::bail!("required signoff policy must contain contexts");
            }
            let mut validated = Vec::with_capacity(contexts.len());
            for context in contexts {
                let context = exact_nonblank(context, "signoff context")?;
                if validated.contains(&context) {
                    anyhow::bail!("required signoff contexts must be unique");
                }
                validated.push(context);
            }
            Ok(SignoffPolicy::Required {
                command,
                contexts: validated,
            })
        }
    }
}

pub(crate) fn reject_tracked_policy(repo: &Path) -> Result<()> {
    let output = Command::new("git")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .args(["ls-files", "--error-unmatch", "--", ".iq/config.json"])
        .current_dir(repo)
        .output()
        .with_context(|| format!("check tracked IQ policy in {}", repo.display()))?;
    if output.status.success() {
        anyhow::bail!(
            ".iq/config.json is local control-plane configuration and must not be tracked"
        );
    }
    if output.status.code() != Some(1) {
        anyhow::bail!(
            "cannot determine whether .iq/config.json is tracked: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn exact_nonblank(value: String, label: &str) -> Result<String> {
    if value.is_empty() || value.trim() != value {
        anyhow::bail!("{label} must be non-empty and must not have surrounding whitespace");
    }
    Ok(value)
}

fn canonical_checkout(path: &Path) -> Result<PathBuf> {
    let path = path
        .canonicalize()
        .with_context(|| format!("resolve integration checkout {}", path.display()))?;
    if fs::symlink_metadata(&path)?.file_type().is_symlink() || !path.is_dir() {
        anyhow::bail!("integration checkout must be a real directory");
    }
    path_text(&path)?;
    Ok(path)
}

pub(crate) fn resolve_remote_identity(repo: &Path, name: &str) -> Result<RegisteredRemote> {
    validate_ref_component(name, "remote")?;
    Ok(RegisteredRemote {
        name: name.to_string(),
        fetch_url: resolve_remote_url(repo, name, false)?,
        push_url: resolve_remote_url(repo, name, true)?,
    })
}

pub(crate) fn verify_remote_identity(repo: &Path, expected: &RegisteredRemote) -> Result<()> {
    let actual = resolve_remote_identity(repo, &expected.name)?;
    if actual != *expected {
        anyhow::bail!(
            "remote {} identity changed: fetch {} -> {}, push {} -> {}",
            expected.name,
            expected.fetch_url,
            actual.fetch_url,
            expected.push_url,
            actual.push_url
        );
    }
    Ok(())
}

fn resolve_remote_url(repo: &Path, name: &str, push: bool) -> Result<String> {
    let mut args = vec!["remote", "get-url"];
    if push {
        args.push("--push");
    }
    args.push("--all");
    args.push(name);
    let output = Command::new("git")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .args(args)
        .current_dir(repo)
        .output()
        .with_context(|| format!("resolve remote {name} URL in {}", repo.display()))?;
    if !output.status.success() {
        anyhow::bail!(
            "resolve remote {name} {} URL failed: {}",
            if push { "push" } else { "fetch" },
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let raw = String::from_utf8(output.stdout).context("resolved remote URL is not valid UTF-8")?;
    let raw = raw.trim_end_matches(['\r', '\n']);
    if raw.is_empty() || raw.contains(['\r', '\n']) {
        anyhow::bail!("remote {name} resolved to an invalid URL identity");
    }
    canonical_remote_url(repo, raw)
}

fn canonical_remote_url(repo: &Path, value: &str) -> Result<String> {
    let local = if let Some(path) = value.strip_prefix("file://localhost") {
        Some(PathBuf::from(path))
    } else if let Some(path) = value.strip_prefix("file://") {
        Some(PathBuf::from(path))
    } else if value.contains("://") || is_scp_remote(value) {
        None
    } else {
        Some(PathBuf::from(value))
    };
    let Some(mut path) = local else {
        return Ok(value.to_string());
    };
    if path.is_relative() {
        path = repo.join(path);
    }
    let path = path
        .canonicalize()
        .with_context(|| format!("resolve local remote URL {value}"))?;
    let path = path
        .to_str()
        .context("canonical local remote URL is not valid UTF-8")?;
    Ok(format!("file://{path}"))
}

fn is_scp_remote(value: &str) -> bool {
    value
        .find(':')
        .is_some_and(|colon| value[..colon].bytes().all(|byte| byte != b'/'))
}

fn entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn absolute_managed_path(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    for component in path.components() {
        if matches!(
            component,
            std::path::Component::ParentDir | std::path::Component::CurDir
        ) {
            anyhow::bail!(
                "managed path must not contain dot aliases: {}",
                path.display()
            );
        }
    }
    let mut existing = path.as_path();
    let mut missing = Vec::new();
    while !existing.exists() {
        missing.push(
            existing
                .file_name()
                .context("managed path has no existing ancestor")?,
        );
        existing = existing
            .parent()
            .context("managed path has no existing ancestor")?;
    }
    let mut resolved = existing.canonicalize()?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    path_text(&resolved)?;
    Ok(resolved)
}

fn validate_integration_checkout(path: &Path, target: &str, remote: &str) -> Result<()> {
    if !path.join(".git").is_dir() {
        anyhow::bail!("registered integration checkout must be a primary Git checkout");
    }
    if !path.join(".rift").is_file() {
        anyhow::bail!("registered integration checkout must be an initialized Rift root");
    }
    let branch = git_output(path, ["symbolic-ref", "--quiet", "--short", "HEAD"])?;
    if branch != target {
        anyhow::bail!("integration checkout branch is {branch}, expected {target}");
    }
    git_output(path, ["remote", "get-url", remote])?;
    if !is_clean(path)? {
        anyhow::bail!("registered integration checkout must be clean and integration-only");
    }
    Ok(())
}

fn require_exact_integration_head(path: &Path, target_sha: &str) -> Result<()> {
    let head = git_output(path, ["rev-parse", "HEAD"])?;
    if head != target_sha {
        anyhow::bail!(
            "integration checkout HEAD {head} differs from exact fetched target {target_sha}"
        );
    }
    Ok(())
}

fn validate_managed_layout(integration: &Path, seed: &Path, workspaces: &Path) -> Result<()> {
    for (left, right) in [
        (integration, seed),
        (integration, workspaces),
        (seed, workspaces),
    ] {
        if left == right || left.starts_with(right) || right.starts_with(left) {
            anyhow::bail!("integration, seed, and development workspace paths must not overlap");
        }
    }
    Ok(())
}

fn require_workspace_submission_state(workspace: &DevelopmentWorkspace) -> Result<()> {
    if !is_clean(&workspace.path)? {
        anyhow::bail!("development workspace must be clean before submission");
    }
    if has_git_operation(&workspace.path)? {
        anyhow::bail!("development workspace has an active Git operation");
    }
    let branch = git_output(
        &workspace.path,
        ["symbolic-ref", "--quiet", "--short", "HEAD"],
    )?;
    if branch != workspace.branch {
        anyhow::bail!(
            "development workspace is attached to {branch}, expected {}",
            workspace.branch
        );
    }
    Ok(())
}

pub(crate) fn has_git_operation(path: &Path) -> Result<bool> {
    for marker in [
        "MERGE_HEAD",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "rebase-merge",
        "rebase-apply",
    ] {
        let raw = PathBuf::from(git_output(path, ["rev-parse", "--git-path", marker])?);
        let marker_path = if raw.is_absolute() {
            raw
        } else {
            path.join(raw)
        };
        if entry_exists(&marker_path)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_clean(path: &Path) -> Result<bool> {
    Ok(git_output(path, ["status", "--porcelain=v1", "--untracked-files=all"])?.is_empty())
}

fn is_ancestor(path: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
    let output = Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .current_dir(path)
        .output()?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => anyhow::bail!(
            "git ancestry check failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

fn resolve_optional_ref(repo: &Path, reference: &str) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", reference])
        .current_dir(repo)
        .output()
        .with_context(|| format!("resolve Git ref {reference}"))?;
    match output.status.code() {
        Some(0) => Ok(Some(String::from_utf8(output.stdout)?.trim().to_string())),
        Some(128) => Ok(None),
        _ => anyhow::bail!(
            "resolve Git ref {reference} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

fn validate_git_branch(repo: &Path, branch: &str, label: &str) -> Result<()> {
    let output = Command::new("git")
        .args(["check-ref-format", "--branch", branch])
        .current_dir(repo)
        .output()?;
    if !output.status.success() {
        anyhow::bail!("{label} is invalid: {branch}");
    }
    Ok(())
}

fn validate_workspace_component(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 80
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        anyhow::bail!("{label} must be a safe non-empty ASCII path component");
    }
    Ok(())
}

fn validate_ref_component(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.trim() != value || value.starts_with('-') {
        anyhow::bail!("{label} is invalid");
    }
    Ok(())
}

fn require_full_sha(value: &str) -> Result<()> {
    if !matches!(value.len(), 40 | 64) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("Git object identity must be a full hexadecimal object ID");
    }
    Ok(())
}

fn repository_key(path: &Path, target: &str) -> Result<String> {
    Ok(format!("{}::{target}", path_text(path)?))
}

fn seed_scope(repo_key: &str) -> String {
    format!("composition-seed:{repo_key}")
}

fn development_scope(repo_key: &str) -> String {
    format!("composition-development:{repo_key}")
}

fn stable_component(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str().context("managed path is not valid UTF-8")
}
