use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs;
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
    EnqueueRequest, QueueItem, RegisteredRemote, RegisteredRepository, ReplacementState,
    ResidueDiscardState, SqliteQueue, WorkspaceIdentity,
};

const LEASE_SECONDS: i64 = 30;

#[cfg(debug_assertions)]
fn stop_composition_target_after(boundary: &str) {
    if std::env::var("IQ_TEST_COMPOSITION_TARGET_STOP_AFTER").as_deref() == Ok(boundary) {
        std::process::exit(82);
    }
}

#[cfg(not(debug_assertions))]
fn stop_composition_target_after(_boundary: &str) {}

#[derive(Clone, Debug)]
pub struct RepositoryInitOptions {
    pub storage_root: PathBuf,
    pub target_branch: String,
    pub remote: String,
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
    pub owned_root_head: String,
    pub owned_root_clean: bool,
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

    fn acquire_wait(
        queue: SqliteQueue,
        integration_path: &Path,
        repo_key: &str,
        owner_id: &str,
    ) -> Result<Self> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(operation) = RepositoryOperationLease::try_acquire(
                queue.clone(),
                integration_path,
                repo_key,
                owner_id,
                LEASE_SECONDS,
            )? {
                return Ok(Self { operation });
            }
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("repository queue {repo_key} has an active operation");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
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
        bootstrap_path: &Path,
        options: RepositoryInitOptions,
    ) -> Result<RegisteredRepository> {
        crate::repository::validate_target_branch(&options.target_branch)?;
        let provisioned =
            self.queue
                .provision_repository(&crate::repository::ProvisionOptions {
                    storage_root: options.storage_root,
                    bootstrap_path: bootstrap_path.to_path_buf(),
                    target: options.target_branch,
                    remote_name: options.remote,
                    rift_database: std::env::var_os("IQ_RIFT_DATABASE").map(PathBuf::from),
                })?;
        let repository = self.queue.repository(provisioned.repo_key().as_str())?;
        let _guard = RepositoryGuard::acquire_wait(
            self.queue.clone(),
            &repository.owned_root_path,
            &repository.key,
            &self.owner_id,
        )?;
        Ok(repository)
    }

    pub fn list(&self) -> Result<Vec<RegisteredRepository>> {
        self.queue.list_repositories()
    }

    pub fn inspect_local_policy(&self, repo_key: &str) -> Result<PolicySnapshot> {
        let repository = self.queue.repository(repo_key)?;
        let _guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            repo_key,
            &self.owner_id,
        )?;
        let (policy, _, _) = load_local_policy(&repository.owned_root_path)?;
        Ok(policy)
    }

    pub fn status(&self, repo_key: &str) -> Result<RepositoryStatus> {
        let repository = self.queue.repository(repo_key)?;
        let _guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            repo_key,
            &self.owner_id,
        )?;
        let owned_root_head = git_output(&repository.owned_root_path, ["rev-parse", "HEAD"])?;
        let owned_root_clean = is_clean(&repository.owned_root_path)?;
        Ok(RepositoryStatus {
            repository,
            owned_root_head,
            owned_root_clean,
        })
    }

    pub fn enqueue_remote(&self, request: EnqueueRequest) -> Result<QueueItem> {
        let repository = self.queue.repository(&request.repo_key)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            &repository.key,
            &self.owner_id,
        )?;
        if request.source_branch == repository.target_branch {
            anyhow::bail!(
                "source branch must not be target branch {}",
                repository.target_branch
            );
        }
        require_full_sha(&request.current_head_sha)?;
        guard.run(
            "git",
            ["check-ref-format", "--branch", &request.source_branch],
            None,
            Duration::from_secs(20),
            "validate source branch",
        )?;
        let source_ref = format!("refs/heads/{}", request.source_branch);
        let observed = guard.run(
            "git",
            [
                "ls-remote",
                "--exit-code",
                "--heads",
                &repository.remote.name,
                &source_ref,
            ],
            Some(&repository.owned_root_path),
            Duration::from_secs(60),
            "resolve exact source branch",
        )?;
        let observed_sha = parse_exact_remote_ref(&observed.stdout, &source_ref)?;
        if observed_sha != request.current_head_sha {
            anyhow::bail!(
                "remote branch {}/{} is {observed_sha}, expected {}",
                repository.remote.name,
                request.source_branch,
                request.current_head_sha
            );
        }
        let state_repository =
            load_project_control_only(&repository.owned_root_path)?.state_repository;
        crate::state_repository::repository(&state_repository)?.verify()?;
        self.queue.enqueue(EnqueueRequest {
            state_repository,
            ..request
        })
    }

    pub fn create_workspace(&self, repo_key: &str, name: &str) -> Result<DevelopmentWorkspace> {
        validate_workspace_component(name, "workspace name")?;
        let repository = self.queue.repository(repo_key)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            repo_key,
            &self.owner_id,
        )?;
        let base_sha = self.sync_owned_root_locked(&guard, &self.queue.repository(repo_key)?)?;
        let repository = self.queue.repository(repo_key)?;
        let manager = self.development_manager(&repository)?;
        let requested_creation = self
            .queue
            .list_development_workspaces(Some(repo_key))?
            .into_iter()
            .find(|workspace| {
                workspace.name == name && workspace.status == DevelopmentWorkspaceStatus::Creating
            })
            .map(|workspace| workspace.id);
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
            if requested_creation.as_deref() == Some(existing.id.as_str()) {
                return Ok(existing);
            }
            anyhow::bail!("development workspace name is already allocated: {name}");
        }
        let id = Uuid::new_v4().to_string();
        let branch = format!("iq-{id}-{name}");
        validate_git_branch(
            &repository.owned_root_path,
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
        let repository = self.queue.repository(&workspace.repo_key)?;
        let _guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            &repository.key,
            &self.owner_id,
        )?;
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
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
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
            resolve_optional_ref(&repository.owned_root_path, &submission.private_ref)?;
        let staging_sha =
            resolve_optional_ref(&repository.owned_root_path, &submission.staging_ref)?;
        if private_sha.as_deref().is_some_and(|sha| sha != intent_sha)
            || staging_sha.as_deref().is_some_and(|sha| sha != intent_sha)
        {
            anyhow::bail!("immutable submission ref identity differs from its creation intent");
        }
        if private_sha.is_none() {
            if staging_sha.is_none() {
                guard.git(
                    &repository.owned_root_path,
                    [
                        "fetch",
                        path_text(&workspace.path)?,
                        &format!("{}:{}", intent_sha, submission.staging_ref),
                    ],
                    "stage immutable local submission",
                )?;
            }
            if resolve_optional_ref(&repository.owned_root_path, &submission.staging_ref)?
                .as_deref()
                != Some(intent_sha)
            {
                anyhow::bail!("staged local submission does not resolve to exact workspace HEAD");
            }
            guard.git(
                &repository.owned_root_path,
                [
                    "update-ref",
                    &submission.private_ref,
                    intent_sha,
                    "0000000000000000000000000000000000000000",
                ],
                "publish immutable local submission",
            )?;
        }
        if resolve_optional_ref(&repository.owned_root_path, &submission.staging_ref)?.is_some() {
            guard.git(
                &repository.owned_root_path,
                ["update-ref", "-d", &submission.staging_ref],
                "remove local submission staging ref",
            )?;
        }
        if resolve_optional_ref(&repository.owned_root_path, &submission.private_ref)?.as_deref()
            != Some(intent_sha)
        {
            anyhow::bail!("immutable local submission ref was not published exactly");
        }
        guard.ensure()?;
        let state_repository =
            load_project_control_only(&repository.owned_root_path)?.state_repository;
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
        let mut workspace = self.queue.workspace(id)?;
        let repository = self.queue.repository(&workspace.repo_key)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            &repository.key,
            &self.owner_id,
        )?;
        if workspace.status == DevelopmentWorkspaceStatus::Active {
            workspace = self.queue.update_development_workspace_cleanup(
                &repository.key,
                &self.owner_id,
                &workspace.id,
                DevelopmentWorkspaceStatus::CleanupPending,
                &CleanupState::OperatorRequested,
            )?;
        }
        let manager = self.development_manager(&repository)?;
        self.remove_workspace_locked(&guard, &repository, &manager, &workspace)
    }

    pub fn discard_workspace_residue(&self, id: &str) -> Result<DevelopmentWorkspace> {
        let workspace = self.queue.workspace(id)?;
        let repository = self.queue.repository(&workspace.repo_key)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
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
        self.cleanup_repo_development_only(repo_key)
    }

    pub fn cleanup_repo_with_system(
        &self,
        repo_key: &str,
        system_config: &crate::agent_config::SystemConfig,
    ) -> Result<crate::integrator::TerminalCleanupAggregate> {
        let repository = self.queue.repository(repo_key)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            repo_key,
            &self.owner_id,
        )?;
        let integrator = crate::integrator::Integrator::new_with_operation_owner(
            crate::integrator::IntegratorOptions {
                repo_key: repository.key.clone(),
                repo_path: repository.owned_root_path.clone(),
                queue_db: self.queue.path().to_path_buf(),
                owner_id: self.owner_id.clone(),
                lease_ttl_seconds: 30,
                base_remote: repository.remote.name.clone(),
                workspace_root: self
                    .queue
                    .workspace_root_path(repo_key)?
                    .context("registered repository has no integration child root")?,
                rift_database: None,
                system_config: system_config.clone(),
            },
            self.owner_id.clone(),
            self.queue.clone(),
        )?;
        self.cleanup_terminal_agent_artifacts(repo_key)?;
        let terminal = integrator.reset_workspaces_under_lease(&guard.operation)?;
        let manager = self.development_manager(&repository)?;
        self.reconcile_development_workspaces(&guard, &repository, &manager)?;
        self.cleanup_replacements(&guard, &repository)?;
        let refresh_result = self.reconcile_owned_root_locked(&guard, &repository);
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
        refresh_result?;
        Ok(crate::integrator::TerminalCleanupAggregate {
            terminal,
            development: results,
        })
    }

    fn cleanup_repo_development_only(&self, repo_key: &str) -> Result<Vec<DevelopmentWorkspace>> {
        let repository = self.queue.repository(repo_key)?;
        let guard = RepositoryGuard::acquire(
            self.queue.clone(),
            &repository.owned_root_path,
            repo_key,
            &self.owner_id,
        )?;
        self.cleanup_terminal_agent_artifacts(repo_key)?;
        let manager = self.development_manager(&repository)?;
        self.reconcile_development_workspaces(&guard, &repository, &manager)?;
        self.cleanup_replacements(&guard, &repository)?;
        let refresh_result = self.reconcile_owned_root_locked(&guard, &repository);
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
        refresh_result?;
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
        kind: &str,
        registry: &Path,
    ) -> Result<RiftWorkspaceManager> {
        let queue_id = self.queue.database_id()?;
        let generation = self
            .queue
            .workspace_root_generation_state_for_kind(scope, kind)?;
        let manager = RiftWorkspaceManager::open(
            source.to_path_buf(),
            root.to_path_buf(),
            scope.to_string(),
            kind,
            Some(registry.to_path_buf()),
            &queue_id,
            generation,
        )?;
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
        self.root_manager(
            &repository.owned_root_path,
            &repository.development_root_path,
            &repository.key,
            "development",
            &repository.registry_identity,
        )
    }

    fn sync_owned_root_locked(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
    ) -> Result<String> {
        validate_integration_checkout(
            &repository.owned_root_path,
            &repository.target_branch,
            &repository.remote.name,
        )?;
        verify_remote_identity(&repository.owned_root_path, &repository.remote)?;
        let target_sha = if matches!(
            repository.checkout_reconciliation,
            CheckoutReconciliationState::Ready(_)
        ) {
            let target_full_ref = format!("refs/heads/{}", repository.target_branch);
            let observed = guard.run(
                "git",
                [
                    "ls-remote",
                    "--exit-code",
                    &repository.remote.name,
                    &target_full_ref,
                ],
                Some(&repository.owned_root_path),
                Duration::from_secs(60),
                "resolve exact target before owned-root refresh",
            )?;
            let observed_target = parse_exact_remote_ref(&observed.stdout, &target_full_ref)?;
            self.queue.update_checkout_reconciliation(
                &repository.key,
                &self.owner_id,
                &CheckoutReconciliationState::pending(&observed_target)?,
            )?;
            stop_composition_target_after("observation");
            observed_target
        } else {
            repository.checkout_reconciliation.target_sha().to_string()
        };
        let private_ref = format!(
            "refs/iq/repository-targets/{}/{}",
            repository.key, target_sha
        );
        let exact_refspec = format!("+{target_sha}:{private_ref}");
        guard.git(
            &repository.owned_root_path,
            [
                "fetch",
                "--no-tags",
                &repository.remote.name,
                &exact_refspec,
            ],
            "fetch exact target during owned-root refresh",
        )?;
        let private_sha = git_output(&repository.owned_root_path, ["rev-parse", &private_ref])?;
        if private_sha != target_sha {
            anyhow::bail!("private target ref differs from durable checkout observation");
        }
        guard.git(
            &repository.owned_root_path,
            ["cat-file", "-e", &format!("{target_sha}^{{commit}}")],
            "verify exact owned-root target object",
        )?;
        let target_ref = format!(
            "refs/remotes/{}/{}",
            repository.remote.name, repository.target_branch
        );
        guard.git(
            &repository.owned_root_path,
            ["update-ref", &target_ref, &target_sha],
            "publish exact owned-root target ref",
        )?;
        let published_sha = git_output(&repository.owned_root_path, ["rev-parse", &target_ref])?;
        if published_sha != target_sha {
            anyhow::bail!("published target differs from durable checkout observation");
        }
        reconcile_registered_checkout(
            &self.queue,
            repository,
            &self.owner_id,
            &target_sha,
            |path, target_sha| {
                guard.git(
                    path,
                    ["reset", "--hard", target_sha],
                    "reset owned root to exact fetched target",
                )
            },
        )?;
        Ok(target_sha)
    }

    fn reconcile_owned_root_locked(
        &self,
        guard: &RepositoryGuard,
        repository: &RegisteredRepository,
    ) -> Result<String> {
        let checkout_target = repository.checkout_reconciliation.target_sha();
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
        Ok(checkout_target.to_string())
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
            let generation = self
                .queue
                .begin_development_workspace_generation(&repository.key)?;
            manager.reconcile_pending_generation(generation)?;
            self.queue
                .complete_workspace_generation(&repository.key, "development", generation)?;
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
            if workspace.status != DevelopmentWorkspaceStatus::Creating
                && !matches!(
                    workspace.cleanup,
                    CleanupState::OperatorRequested | CleanupState::OperatorFailed { .. }
                )
            {
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
                let message =
                    "workspace is dirty or has an active Git operation; IQ preserved it".into();
                let state = if workspace.cleanup == CleanupState::OperatorRequested {
                    CleanupState::OperatorFailed { message }
                } else {
                    CleanupState::Failed { message }
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
            if workspace.status != DevelopmentWorkspaceStatus::Creating
                && !matches!(
                    workspace.cleanup,
                    CleanupState::OperatorRequested | CleanupState::OperatorFailed { .. }
                )
            {
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
            &repository.owned_root_path,
            &integration_root,
            &repository.key,
            "integration",
            &repository.registry_identity,
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

pub(crate) fn parse_exact_remote_ref(output: &[u8], expected_ref: &str) -> Result<String> {
    let output = std::str::from_utf8(output).context("remote ref output is not UTF-8")?;
    let mut lines = output.lines();
    let line = lines.next().context("remote ref did not resolve")?;
    if lines.next().is_some() {
        anyhow::bail!("remote ref resolved more than once");
    }
    let mut fields = line.split_whitespace();
    let sha = fields.next().context("remote ref has no object ID")?;
    if fields.next() != Some(expected_ref) || fields.next().is_some() {
        anyhow::bail!("remote ref output differs from requested ref");
    }
    require_full_sha(sha)?;
    Ok(sha.to_string())
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
        &repository.owned_root_path,
        &repository.target_branch,
        &repository.remote.name,
    )?;
    verify_remote_identity(&repository.owned_root_path, &repository.remote)?;
    let remote_ref = format!(
        "refs/remotes/{}/{}",
        repository.remote.name, repository.target_branch
    );
    let fetched_target = git_output(&repository.owned_root_path, ["rev-parse", &remote_ref])?;
    if fetched_target != target_sha {
        anyhow::bail!(
            "fetched target {fetched_target} differs from checkout reconciliation target {target_sha}"
        );
    }
    let head = git_output(&repository.owned_root_path, ["rev-parse", "HEAD"])?;
    if repository.checkout_reconciliation.is_ready_for(target_sha) && head == target_sha {
        return Ok(());
    }
    queue.update_checkout_reconciliation(
        &repository.key,
        owner_id,
        &CheckoutReconciliationState::pending(target_sha)?,
    )?;
    let result = (|| {
        if head != target_sha {
            reset(&repository.owned_root_path, target_sha)?;
        }
        validate_integration_checkout(
            &repository.owned_root_path,
            &repository.target_branch,
            &repository.remote.name,
        )?;
        require_exact_integration_head(&repository.owned_root_path, target_sha)
    })();
    match result {
        Ok(()) => queue.update_checkout_reconciliation(
            &repository.key,
            owner_id,
            &CheckoutReconciliationState::ready(target_sha)?,
        ),
        Err(error) => {
            queue.update_checkout_reconciliation(
                &repository.key,
                owner_id,
                &CheckoutReconciliationState::failed(target_sha, &format!("{error:#}"))?,
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
    let Some(contents) = crate::repository::read_local_policy_bytes(repo)? else {
        return default_project_control_policy();
    };
    let config_path = repo.join(".iq/config.json");
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
        "BISECT_START",
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

fn path_text(path: &Path) -> Result<&str> {
    path.to_str().context("managed path is not valid UTF-8")
}
