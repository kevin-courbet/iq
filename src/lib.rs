pub mod agent_config;
pub mod agent_protocol;
pub mod agent_runner;
pub mod composition;
pub mod control_api;
pub mod control_domain;
pub mod control_store;
pub mod notifications;
pub mod state_repository;

pub mod core {
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use std::str::FromStr;

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum QueueStatus {
        Ready,
        Merging,
        Merged,
        Validating,
        Validated,
        Integrating,
        Integrated,
        Blocked,
        Cancelled,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub enum QueueSource {
        RemoteBranch {
            branch: String,
        },
        LocalSubmission {
            submission_id: String,
            commit_sha: String,
        },
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum LandingPolicy {
        Provider,
        Direct,
        Squash,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum BlockedPhase {
        Merging,
        Validating,
        Integrating,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum BlockedReason {
        NeedsUserInput,
        NeedsAgentFix,
        Infra,
        Dependency,
        Credentials,
        Provider,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BlockedState {
        pub phase: BlockedPhase,
        pub reason: BlockedReason,
        pub prompt_id: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub struct StateMachine;

    impl StateMachine {
        pub fn transition(&self, current: QueueStatus, target: QueueStatus) -> Result<(), String> {
            if current == QueueStatus::Cancelled || current == QueueStatus::Integrated {
                return Err(format!("terminal item cannot transition from {current}"));
            }

            let allowed = matches!(
                (current, target),
                (QueueStatus::Ready, QueueStatus::Merging)
                    | (QueueStatus::Ready, QueueStatus::Blocked)
                    | (QueueStatus::Merging, QueueStatus::Merged)
                    | (QueueStatus::Merging, QueueStatus::Blocked)
                    | (QueueStatus::Merged, QueueStatus::Validating)
                    | (QueueStatus::Merged, QueueStatus::Blocked)
                    | (QueueStatus::Validating, QueueStatus::Validated)
                    | (QueueStatus::Validating, QueueStatus::Blocked)
                    | (QueueStatus::Validated, QueueStatus::Integrating)
                    | (QueueStatus::Validated, QueueStatus::Blocked)
                    | (QueueStatus::Integrating, QueueStatus::Integrated)
                    | (QueueStatus::Integrating, QueueStatus::Blocked)
                    | (QueueStatus::Ready, QueueStatus::Cancelled)
                    | (QueueStatus::Merging, QueueStatus::Cancelled)
                    | (QueueStatus::Merged, QueueStatus::Cancelled)
                    | (QueueStatus::Validating, QueueStatus::Cancelled)
                    | (QueueStatus::Validated, QueueStatus::Cancelled)
                    | (QueueStatus::Integrating, QueueStatus::Cancelled)
                    | (QueueStatus::Blocked, QueueStatus::Cancelled)
            );

            if allowed {
                Ok(())
            } else {
                Err(format!("invalid transition {current} -> {target}"))
            }
        }

        pub fn block_phase_for_status(&self, current: QueueStatus) -> Result<BlockedPhase, String> {
            match current {
                QueueStatus::Merging => Ok(BlockedPhase::Merging),
                QueueStatus::Validating => Ok(BlockedPhase::Validating),
                QueueStatus::Integrating => Ok(BlockedPhase::Integrating),
                _ => Err(format!("status {current} cannot be blocked")),
            }
        }

        pub fn resume_target(&self, blocked: &BlockedState) -> Result<QueueStatus, String> {
            match blocked.reason {
                BlockedReason::NeedsUserInput
                | BlockedReason::Infra
                | BlockedReason::Dependency
                | BlockedReason::Credentials
                | BlockedReason::Provider => Ok(blocked.phase.into()),
                BlockedReason::NeedsAgentFix => {
                    Err("needs_agent_fix requires explicit requeue to ready".into())
                }
            }
        }
    }

    impl From<BlockedPhase> for QueueStatus {
        fn from(value: BlockedPhase) -> Self {
            match value {
                BlockedPhase::Merging => QueueStatus::Merging,
                BlockedPhase::Validating => QueueStatus::Validating,
                BlockedPhase::Integrating => QueueStatus::Integrating,
            }
        }
    }

    macro_rules! enum_text {
        ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
            impl fmt::Display for $name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    let value = match self { $(Self::$variant => $text),+ };
                    f.write_str(value)
                }
            }

            impl FromStr for $name {
                type Err = String;

                fn from_str(input: &str) -> Result<Self, Self::Err> {
                    match input { $($text => Ok(Self::$variant),)+ _ => Err(format!("unknown {}: {input}", stringify!($name))) }
                }
            }
        };
    }

    enum_text!(QueueStatus {
        Ready => "ready",
        Merging => "merging",
        Merged => "merged",
        Validating => "validating",
        Validated => "validated",
        Integrating => "integrating",
        Integrated => "integrated",
        Blocked => "blocked",
        Cancelled => "cancelled",
    });

    enum_text!(BlockedPhase {
        Merging => "merging",
        Validating => "validating",
        Integrating => "integrating",
    });

    enum_text!(BlockedReason {
        NeedsUserInput => "needs_user_input",
        NeedsAgentFix => "needs_agent_fix",
        Infra => "infra",
        Dependency => "dependency",
        Credentials => "credentials",
        Provider => "provider",
    });

    enum_text!(LandingPolicy {
        Provider => "provider",
        Direct => "direct",
        Squash => "squash",
    });
}

pub mod sqlite {
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use rusqlite::{
        params, Connection, Error as SqliteError, OpenFlags, OptionalExtension, Row,
        TransactionBehavior,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::ffi::CString;
    use std::fmt;
    use std::fs::{self, OpenOptions};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::str::FromStr;
    use uuid::Uuid;

    use crate::core::{
        BlockedPhase, BlockedReason, LandingPolicy, QueueSource, QueueStatus, StateMachine,
    };

    #[derive(Clone, Debug)]
    pub struct EnqueueRequest {
        pub repo_key: String,
        pub repo_path: String,
        pub source_branch: String,
        pub target_branch: String,
        pub current_head_sha: String,
        pub pr_url: Option<String>,
        pub producer_metadata: Value,
        pub state_repository: crate::control_domain::StateRepositorySnapshot,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct WorkspaceIdentity {
        pub path: String,
        pub rift_id: String,
        pub source_rift_id: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct RiftWorkspaceRootOwner {
        pub version: u32,
        pub queue_database_id: String,
        pub repo_key: String,
        pub source: PathBuf,
        pub source_rift_id: String,
        pub registry_identity: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LegacyRiftWorkspaceRootOwner {
        version: u32,
        queue_database_id: String,
        queue_database_path: PathBuf,
        repo_key: String,
        source: PathBuf,
        source_rift_id: String,
        registry_identity: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum WorkspaceState {
        NotCreated,
        CreationIntent { path: String },
        Retained { identity: WorkspaceIdentity },
        Cleaned { cleaned_at: String },
    }

    impl WorkspaceState {
        pub fn path(&self) -> Option<&str> {
            match self {
                Self::CreationIntent { path } => Some(path),
                Self::Retained { identity } => Some(&identity.path),
                Self::NotCreated | Self::Cleaned { .. } => None,
            }
        }

        pub fn identity(&self) -> Option<&WorkspaceIdentity> {
            match self {
                Self::Retained { identity } => Some(identity),
                _ => None,
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct QueueItem {
        pub id: String,
        pub repo_key: String,
        pub repo_path: String,
        pub source_branch: String,
        pub target_branch: String,
        pub current_head_sha: String,
        pub pr_url: Option<String>,
        pub status: QueueStatus,
        pub blocked_phase: Option<BlockedPhase>,
        pub blocked_reason: Option<BlockedReason>,
        pub current_attempt_id: Option<String>,
        pub workspace: WorkspaceState,
        pub conflict: Option<serde_json::Value>,
        pub target_sha: Option<String>,
        pub source_sha: Option<String>,
        pub landed_commit_sha: Option<String>,
        pub producer_metadata: Value,
        pub validation_evidence: Value,
        pub landing: LandingState,
        pub source: QueueSource,
        pub landing_policy: LandingPolicy,
        pub replacement: ReplacementState,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    pub enum LandingState {
        Ready,
        Uncertain {
            candidate_sha: String,
            expected_target_sha: String,
        },
        Landed {
            candidate_sha: String,
            commit_sha: String,
        },
    }

    impl LandingState {
        pub fn is_uncertain(&self) -> bool {
            matches!(self, Self::Uncertain { .. })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct RegisteredRemote {
        pub name: String,
        pub fetch_url: String,
        pub push_url: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum ReplacementState {
        None,
        CleanupPending {
            old_attempt_id: String,
            old_workspace: WorkspaceIdentity,
        },
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct RegisteredRepository {
        pub key: String,
        pub integration_path: PathBuf,
        pub target_branch: String,
        pub remote: RegisteredRemote,
        pub seed: WorkspaceState,
        pub workspace_root: PathBuf,
        pub checkout_reconciliation: CheckoutReconciliationState,
        pub seed_refresh: SeedRefreshState,
        pub created_at: String,
        pub updated_at: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum CleanupState {
        Ready,
        Pending,
        Failed { message: String },
        ResidueDiscard { discard: Box<ResidueDiscardState> },
        Complete { completed_at: String },
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum ResidueDiscardState {
        Pending {
            quarantine_name: String,
        },
        Inspected {
            quarantine_name: String,
            device: u64,
            inode: u64,
            tree_digest: [u8; 32],
            child_move: Option<ResidueChildMove>,
        },
        FailedPending {
            quarantine_name: String,
            message: String,
        },
        FailedInspected {
            quarantine_name: String,
            device: u64,
            inode: u64,
            tree_digest: [u8; 32],
            child_move: Option<ResidueChildMove>,
            message: String,
        },
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ResidueChildMove {
        pub parent_components: Vec<Vec<u8>>,
        pub original_name: Vec<u8>,
        pub quarantine_name: String,
        pub identity: ResidueEntryIdentity,
        pub remaining_tree_digest: [u8; 32],
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub enum ResidueEntryIdentity {
        Directory {
            device: u64,
            inode: u64,
        },
        RegularFile {
            device: u64,
            inode: u64,
            length: u64,
            modified_seconds: i64,
            modified_nanoseconds: i64,
            changed_seconds: i64,
            changed_nanoseconds: i64,
            digest: [u8; 32],
        },
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum SeedRefreshState {
        Ready { target_sha: String },
        Pending { target_sha: String },
        Failed { target_sha: String, message: String },
    }

    impl SeedRefreshState {
        pub fn target_sha(&self) -> &str {
            match self {
                Self::Ready { target_sha }
                | Self::Pending { target_sha }
                | Self::Failed { target_sha, .. } => target_sha,
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum CheckoutReconciliationState {
        Ready { target_sha: String },
        Pending { target_sha: String },
        Failed { target_sha: String, message: String },
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum DevelopmentWorkspaceStatus {
        Creating,
        Active,
        Submitted,
        CleanupPending,
        CleanupFailed,
        Removed,
    }

    impl fmt::Display for DevelopmentWorkspaceStatus {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(match self {
                Self::Creating => "creating",
                Self::Active => "active",
                Self::Submitted => "submitted",
                Self::CleanupPending => "cleanup_pending",
                Self::CleanupFailed => "cleanup_failed",
                Self::Removed => "removed",
            })
        }
    }

    impl FromStr for DevelopmentWorkspaceStatus {
        type Err = String;

        fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
            match value {
                "creating" => Ok(Self::Creating),
                "active" => Ok(Self::Active),
                "submitted" => Ok(Self::Submitted),
                "cleanup_pending" => Ok(Self::CleanupPending),
                "cleanup_failed" => Ok(Self::CleanupFailed),
                "removed" => Ok(Self::Removed),
                _ => Err(format!("unknown development workspace status: {value}")),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DevelopmentWorkspace {
        pub id: String,
        pub repo_key: String,
        pub name: String,
        pub identity: Option<WorkspaceIdentity>,
        pub path: PathBuf,
        pub branch: String,
        pub base_sha: String,
        pub status: DevelopmentWorkspaceStatus,
        pub cleanup: CleanupState,
        pub created_at: String,
        pub updated_at: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct LocalSubmission {
        pub id: String,
        pub queue_item_id: String,
        pub repo_key: String,
        pub workspace_id: String,
        pub base_sha: String,
        pub commit_sha: String,
        pub private_ref: String,
        pub staging_ref: String,
        pub replaces_item_id: Option<String>,
        pub state: LocalSubmissionState,
        pub created_at: String,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum LocalSubmissionState {
        Creating,
        Queued,
        Replaced,
        Cancelled,
        Integrated,
    }

    impl fmt::Display for LocalSubmissionState {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(match self {
                Self::Creating => "creating",
                Self::Queued => "queued",
                Self::Replaced => "replaced",
                Self::Cancelled => "cancelled",
                Self::Integrated => "integrated",
            })
        }
    }

    impl FromStr for LocalSubmissionState {
        type Err = String;

        fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
            match value {
                "creating" => Ok(Self::Creating),
                "queued" => Ok(Self::Queued),
                "replaced" => Ok(Self::Replaced),
                "cancelled" => Ok(Self::Cancelled),
                "integrated" => Ok(Self::Integrated),
                _ => Err(format!("unknown local submission state: {value}")),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Prompt {
        pub id: String,
        pub item_id: String,
        pub attempt_id: Option<String>,
        pub blocked_phase: BlockedPhase,
        pub status: String,
        pub question: String,
        pub answer: Option<String>,
        pub options: Vec<String>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct QueueEvent {
        pub id: String,
        pub item_id: String,
        pub event_type: String,
        pub message: String,
        pub created_at: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Attempt {
        pub id: String,
        pub item_id: String,
        pub attempt_number: i64,
        pub source_head_sha: String,
        pub target_base_sha: Option<String>,
        pub merge_commit_sha: Option<String>,
        pub validated_commit_sha: Option<String>,
        pub landed_commit_sha: Option<String>,
        pub validation_command: Option<String>,
        pub validation_exit_code: Option<i64>,
        pub validation_log_path: Option<String>,
        pub policy_snapshot_json: Option<String>,
        pub policy_digest: Option<String>,
        pub signoff_evidence_json: Option<String>,
        pub moved_base: MovedBaseState,
    }

    struct NoValidationAttemptRow {
        item_id: String,
        target_base_sha: Option<String>,
        merge_commit_sha: Option<String>,
        validated_commit_sha: Option<String>,
        validation_command: Option<String>,
        validation_exit_code: Option<i64>,
        validation_log_path: Option<String>,
        policy_snapshot_json: Option<String>,
        policy_digest: Option<String>,
        signoff_evidence_json: Option<String>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum MovedBaseState {
        None,
        Pending {
            target_sha: String,
            source_sha: String,
        },
        Applied {
            target_sha: String,
            source_sha: String,
            candidate_sha: String,
        },
    }

    pub(crate) enum AttemptPolicy<'a> {
        Snapshot {
            snapshot_json: &'a str,
            digest: &'a str,
        },
        HostValidation,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum ExecutionAuthority {
        Active,
        Cancelled,
        Lost(String),
    }

    enum MutationAuthority<'a> {
        External,
        RepositoryLease {
            repo_key: &'a str,
            owner_id: &'a str,
        },
    }

    #[allow(dead_code)]
    struct CanonicalPathUpdate {
        stored: String,
        canonical: String,
    }

    #[allow(dead_code)]
    struct V2RepositoryPathUpdates {
        queue: Vec<CanonicalPathUpdate>,
        workspace_roots: Vec<CanonicalPathUpdate>,
    }

    #[derive(Clone)]
    pub struct SqliteQueue {
        path: PathBuf,
        database_dev: u64,
        database_ino: u64,
    }

    #[derive(Clone)]
    pub struct SqliteQueueReader {
        path: PathBuf,
        database_dev: u64,
        database_ino: u64,
    }

    fn require_current_schema_version(version: Option<&str>) -> Result<()> {
        match version {
            Some("2" | "3" | "4" | "5" | "6" | "7") => {
                anyhow::bail!("IQ schema must first be upgraded to version 8 by the prior release")
            }
            Some("8") => anyhow::bail!(
                "IQ schema version 8 requires explicit migration with a verified system configuration path"
            ),
            Some("9") => Ok(()),
            Some(version) => anyhow::bail!("unsupported IQ schema version {version}"),
            None => anyhow::bail!("existing IQ database has no standalone schema version"),
        }
    }

    impl SqliteQueue {
        const MIGRATION_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        const WRITE_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(200);
        const AUTHORITY_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

        pub fn default_db_path() -> Result<PathBuf> {
            let (state_root, legacy_directory, iq_directory) = if cfg!(target_os = "macos") {
                let home = std::env::var_os("HOME").context("HOME is required for IQ state")?;
                let root = PathBuf::from(home).join("Library/Application Support");
                (
                    root.clone(),
                    root.join("Threadmill/IntegrationQueues"),
                    root.join("IQ/IntegrationQueues"),
                )
            } else {
                let root = if let Some(state_home) = std::env::var_os("XDG_STATE_HOME") {
                    PathBuf::from(state_home)
                } else {
                    let home = std::env::var_os("HOME").context("HOME is required for IQ state")?;
                    PathBuf::from(home).join(".local/state")
                };
                (
                    root.clone(),
                    root.join("threadmill/integration-queues"),
                    root.join("iq/integration-queues"),
                )
            };
            if state_root.as_os_str().is_empty() || !state_root.is_absolute() {
                anyhow::bail!(
                    "IQ state root must be a non-empty absolute path: {}",
                    state_root.display()
                );
            }
            let state_root_exists = path_entry_exists(&state_root)?;
            if !state_root_exists {
                fs::create_dir_all(&state_root)
                    .with_context(|| format!("create IQ state root {}", state_root.display()))?;
            }
            require_real_directory(&state_root, "IQ state root")?;
            let state_directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(&state_root)
                .with_context(|| format!("open IQ state root {}", state_root.display()))?;
            verify_open_directory(&state_root, &state_directory, "IQ state root")?;
            let before_lock = inspect_state_layout(&legacy_directory, &iq_directory);
            let lock = open_lock_at(&state_directory, ".iq-state-migration.lock")?;
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("acquire IQ state migration lock");
            }
            verify_open_directory(&state_root, &state_directory, "IQ state root")?;
            let locked_layout = inspect_state_layout(&legacy_directory, &iq_directory);
            let layout = match (before_lock, locked_layout) {
                (Ok(before), Ok(locked)) if before == locked => locked,
                (Ok(_), Ok(_)) | (Err(_), Ok(_)) => {
                    anyhow::bail!("IQ state roots changed while acquiring the migration lock")
                }
                (_, Err(error)) => return Err(error),
            };
            let legacy_exists = layout.legacy.is_some();
            if legacy_exists {
                let legacy_db = legacy_directory.join("queues.db");
                let legacy_db = legacy_db.canonicalize()?;
                let connection = Connection::open_with_flags(
                    &legacy_db,
                    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
                )?;
                let active_leases: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM repo_leases WHERE expires_at>?1",
                    params![now()],
                    |row| row.get(0),
                )?;
                if active_leases != 0 {
                    anyhow::bail!(
                        "legacy IQ state has {active_leases} active repository operation lease(s); stop legacy IQ processes before migration"
                    );
                }
                Self::reconcile_workspace_owner_markers(&connection, &legacy_db, false)?;
                connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
                drop(connection);
                drop(Self::open(&legacy_db)?);
                let connection = Connection::open_with_flags(
                    &legacy_db,
                    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
                )?;
                Self::reconcile_workspace_owner_markers(&connection, &legacy_db, true)?;
                connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
                drop(connection);
                let iq_parent = iq_directory
                    .parent()
                    .context("IQ state directory has no parent")?;
                fs::create_dir_all(iq_parent)
                    .with_context(|| format!("create IQ state parent {}", iq_parent.display()))?;
                require_real_directory(iq_parent, "IQ state parent")?;
                fs::rename(&legacy_directory, &iq_directory).with_context(|| {
                    format!(
                        "atomically migrate IQ state {} to {}",
                        legacy_directory.display(),
                        iq_directory.display()
                    )
                })?;
                OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                    .open(iq_parent)?
                    .sync_all()?;
                if let Some(legacy_parent) = legacy_directory.parent() {
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                        .open(legacy_parent)?
                        .sync_all()?;
                }
            }
            let database = iq_directory.join("queues.db");
            Self::open(&database)?;
            Ok(database)
        }

        pub fn open(path: &Path) -> Result<Self> {
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            };
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create queue db parent {}", parent.display()))?;
            }
            let preexisting_identity = match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    anyhow::bail!("queue database must be a regular file: {}", path.display())
                }
                Ok(metadata) => Some((metadata.dev(), metadata.ino())),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("inspect queue db {}", path.display()))
                }
            };
            let parent = path.parent().context("queue database path has no parent")?;
            let file_name = path
                .file_name()
                .context("queue database path has no file name")?;
            let path = parent
                .canonicalize()
                .with_context(|| format!("resolve queue db parent {}", parent.display()))?
                .join(file_name);
            let mut conn = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )
            .with_context(|| format!("open queue db {}", path.display()))?;
            conn.busy_timeout(Self::MIGRATION_BUSY_TIMEOUT)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            let existing_tables: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )?;
            if existing_tables > 0 {
                let metadata_exists: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='queue_metadata')",
                    [],
                    |row| row.get(0),
                )?;
                if !metadata_exists {
                    anyhow::bail!("existing IQ database has no standalone schema identity");
                }
                let version: Option<String> = conn
                    .query_row(
                        "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?;
                require_current_schema_version(version.as_deref())?;
            }
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(SCHEMA)?;
            tx.execute(
                "INSERT OR IGNORE INTO queue_metadata (key,value) VALUES ('database_id',?1)",
                params![Uuid::new_v4().to_string()],
            )?;
            let workspace_schema_version: Option<String> = tx
                .query_row(
                    "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            if workspace_schema_version
                .as_deref()
                .is_some_and(|version| version != "9")
            {
                anyhow::bail!(
                    "unsupported workspace schema version {}",
                    workspace_schema_version.as_deref().unwrap_or_default()
                );
            }
            tx.execute_batch(COMPOSITION_SCHEMA)?;
            tx.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
            tx.execute_batch(REGISTERED_CHECKOUT_TRIGGERS)?;
            tx.execute_batch(LANDING_STATE_TRIGGERS)?;
            tx.execute_batch(REGISTERED_REMOTE_TRIGGERS)?;
            crate::control_store::install_fresh_v9(&tx)?;
            tx.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
            tx.commit()?;
            crate::control_store::ControlStore::open(&path)?;
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect queue db {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                anyhow::bail!("queue database must be a regular file: {}", path.display());
            }
            if preexisting_identity
                .is_some_and(|identity| identity != (metadata.dev(), metadata.ino()))
            {
                anyhow::bail!(
                    "queue database identity changed while opening: {}",
                    path.display()
                );
            }
            Ok(Self {
                path,
                database_dev: metadata.dev(),
                database_ino: metadata.ino(),
            })
        }

        pub fn migrate_v8(path: &Path, system_config_path: &Path) -> Result<Self> {
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            };
            let parent = path.parent().context("queue database path has no parent")?;
            let canonical = parent.canonicalize()?.join(
                path.file_name()
                    .context("queue database path has no file name")?,
            );
            let metadata = fs::symlink_metadata(&canonical)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                anyhow::bail!("queue database must be a regular non-symlink file");
            }
            let mut connection = Connection::open_with_flags(
                &canonical,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            connection.busy_timeout(Self::MIGRATION_BUSY_TIMEOUT)?;
            connection.pragma_update(None, "foreign_keys", "ON")?;
            let version: String = connection.query_row(
                "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                [],
                |row| row.get(0),
            )?;
            if version != "8" {
                anyhow::bail!("explicit schema migration requires version 8, found {version}");
            }
            crate::control_store::migrate_v8_to_v9(
                &mut connection,
                &canonical,
                system_config_path,
            )?;
            drop(connection);
            Self::open(&canonical)
        }

        #[allow(dead_code)]
        fn validate_standalone_v7(conn: &Connection) -> Result<()> {
            let foreign_key_errors: i64 =
                conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                    row.get(0)
                })?;
            if foreign_key_errors != 0 {
                anyhow::bail!("IQ schema version 7 has {foreign_key_errors} foreign-key errors");
            }
            let local_source_foreign_key: i64 = conn.query_row(
                "SELECT COUNT(*) FROM pragma_foreign_key_list('queue_items') WHERE \"table\"='local_submissions' AND \"from\"='submission_id' AND \"to\"='id'",
                [],
                |row| row.get(0),
            )?;
            if local_source_foreign_key != 1 {
                anyhow::bail!("IQ schema version 7 lacks the exact local submission foreign key");
            }
            let source_triggers: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name IN ('queue_items_local_source_insert','queue_items_local_source_update','local_submission_identity_immutable')",
                [],
                |row| row.get(0),
            )?;
            if source_triggers != 3 {
                anyhow::bail!("IQ schema version 7 lacks exact queue source constraints");
            }
            let checkout_triggers: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name IN ('registered_checkout_state_insert','registered_checkout_state_update')",
                [],
                |row| row.get(0),
            )?;
            if checkout_triggers != 2 {
                anyhow::bail!("IQ schema version 7 lacks registered checkout state constraints");
            }
            let identity_triggers: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name IN ('registered_repository_remote_insert','registered_repository_remote_update','registered_remote_identity_immutable','registered_remote_identity_delete','queue_items_landing_state_insert','queue_items_landing_state_update')",
                [],
                |row| row.get(0),
            )?;
            if identity_triggers != 6 {
                anyhow::bail!("IQ schema version 7 lacks remote or landing identity constraints");
            }
            let base_column: i64 = conn.query_row(
                "SELECT COUNT(*) FROM pragma_table_info('local_submissions') WHERE name='base_sha' AND \"notnull\"=1",
                [],
                |row| row.get(0),
            )?;
            if base_column != 1 {
                anyhow::bail!("IQ schema version 7 lacks immutable submission base identity");
            }
            let malformed: i64 = conn.query_row(
                "SELECT
                   (SELECT COUNT(*) FROM local_submissions WHERE
                     length(base_sha) NOT IN (40,64) OR base_sha GLOB '*[^0-9A-Fa-f]*' OR
                     length(commit_sha) NOT IN (40,64) OR commit_sha GLOB '*[^0-9A-Fa-f]*') +
                   (SELECT COUNT(*) FROM registered_repositories WHERE
                     json_valid(checkout_reconciliation_json)=0 OR
                     json_extract(checkout_reconciliation_json,'$.state') NOT IN ('ready','pending','failed') OR
                     length(json_extract(checkout_reconciliation_json,'$.target_sha')) NOT IN (40,64) OR
                     json_extract(checkout_reconciliation_json,'$.target_sha') GLOB '*[^0-9A-Fa-f]*' OR
                     (json_extract(checkout_reconciliation_json,'$.state')='failed' AND COALESCE(json_extract(checkout_reconciliation_json,'$.message'),'')='') OR
                     json_valid(seed_refresh_json)=0 OR
                     json_extract(seed_refresh_json,'$.state') NOT IN ('ready','pending','failed') OR
                     length(json_extract(seed_refresh_json,'$.target_sha')) NOT IN (40,64) OR
                     json_extract(seed_refresh_json,'$.target_sha') GLOB '*[^0-9A-Fa-f]*' OR
                     (json_extract(seed_refresh_json,'$.state')='failed' AND COALESCE(json_extract(seed_refresh_json,'$.message'),'')='')) +
                    (SELECT COUNT(*) FROM integration_attempts WHERE json_valid(moved_base_json)=0) +
                    (SELECT COUNT(*) FROM queue_items item WHERE
                      json_valid(item.landing_state_json)=0 OR NOT (
                        (json_extract(item.landing_state_json,'$.state')='ready' AND item.status!='integrated') OR
                        (json_extract(item.landing_state_json,'$.state')='uncertain' AND (item.status='integrating' OR (item.status='blocked' AND item.blocked_phase='integrating')) AND
                         length(json_extract(item.landing_state_json,'$.candidate_sha')) IN (40,64) AND json_extract(item.landing_state_json,'$.candidate_sha') NOT GLOB '*[^0-9A-Fa-f]*' AND
                         length(json_extract(item.landing_state_json,'$.expected_target_sha')) IN (40,64) AND json_extract(item.landing_state_json,'$.expected_target_sha') NOT GLOB '*[^0-9A-Fa-f]*') OR
                        (json_extract(item.landing_state_json,'$.state')='landed' AND item.status='integrated' AND
                         length(json_extract(item.landing_state_json,'$.candidate_sha')) IN (40,64) AND json_extract(item.landing_state_json,'$.candidate_sha') NOT GLOB '*[^0-9A-Fa-f]*' AND
                         item.landed_commit_sha=json_extract(item.landing_state_json,'$.commit_sha'))
                      )) +
                    (SELECT COUNT(*) FROM registered_remote_identities WHERE repo_key='' OR target_branch='' OR remote_name='' OR fetch_url='' OR push_url='') +
                    (SELECT COUNT(*) FROM registered_repositories repository WHERE NOT EXISTS (
                      SELECT 1 FROM registered_remote_identities identity
                      WHERE identity.repo_key=repository.repo_key
                        AND identity.integration_path=repository.integration_path
                        AND identity.target_branch=repository.target_branch
                        AND identity.remote_name=repository.remote
                    ))",
                [],
                |row| row.get(0),
            )?;
            if malformed != 0 {
                anyhow::bail!("IQ schema version 7 contains {malformed} malformed durable row(s)");
            }
            Ok(())
        }

        #[allow(dead_code)]
        fn migrate_standalone_v7(conn: &mut Connection) -> Result<()> {
            conn.pragma_update(None, "foreign_keys", "OFF")?;
            conn.pragma_update(None, "legacy_alter_table", "ON")?;
            let migration = (|| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Self::reject_active_migration_leases(&tx, "7")?;
                Self::reject_nonterminal_migration_items(&tx, "7")?;
                tx.execute_batch(
                    r#"
ALTER TABLE integration_attempts RENAME TO integration_attempts_v7;
ALTER TABLE registered_repositories RENAME TO registered_repositories_v7;
"#,
                )?;
                tx.execute_batch(SCHEMA)?;
                tx.execute_batch(COMPOSITION_SCHEMA)?;
                tx.execute_batch(
                    r#"INSERT INTO registered_repositories (
  repo_key,integration_path,target_branch,remote,seed_path,seed_rift_id,seed_source_rift_id,workspace_root,checkout_reconciliation_json,seed_refresh_json,created_at,updated_at
)
SELECT
  repo_key,integration_path,target_branch,remote,seed_path,seed_rift_id,seed_source_rift_id,workspace_root,checkout_reconciliation_json,seed_refresh_json,created_at,updated_at
FROM registered_repositories_v7;
INSERT INTO integration_attempts (
  id,item_id,attempt_number,source_head_sha,target_base_sha,merge_commit_sha,validated_commit_sha,landed_commit_sha,validation_command,validation_exit_code,validation_log_path,policy_snapshot_json,policy_digest,signoff_evidence_json,moved_base_json,started_at,finished_at,result
)
SELECT
  id,item_id,attempt_number,source_head_sha,target_base_sha,merge_commit_sha,validated_commit_sha,landed_commit_sha,validation_command,validation_exit_code,validation_log_path,policy_snapshot_json,policy_digest,signoff_evidence_json,moved_base_json,started_at,finished_at,result
FROM integration_attempts_v7;
DROP TABLE integration_attempts_v7;
DROP TABLE registered_repositories_v7;
UPDATE queue_metadata SET value='8' WHERE key='workspace_schema_version';"#,
                )?;
                tx.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_CHECKOUT_TRIGGERS)?;
                tx.execute_batch(LANDING_STATE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_REMOTE_TRIGGERS)?;
                tx.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
                Self::validate_standalone_v8(&tx)?;
                tx.commit()?;
                Ok::<(), anyhow::Error>(())
            })();
            conn.pragma_update(None, "legacy_alter_table", "OFF")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            migration
        }

        #[allow(dead_code)]
        fn validate_standalone_v8(conn: &Connection) -> Result<()> {
            Self::validate_standalone_v7(conn).map_err(|error| {
                anyhow::anyhow!("IQ schema version 8 structural validation failed: {error:#}")
            })?;
            let retired_columns: i64 = conn.query_row(
                "SELECT (SELECT COUNT(*) FROM pragma_table_info('registered_repositories') WHERE name IN ('policy_target_sha','policy_snapshot_json','policy_digest')) + (SELECT COUNT(*) FROM pragma_table_info('integration_attempts') WHERE name='policy_target_sha')",
                [],
                |row| row.get(0),
            )?;
            if retired_columns != 0 {
                anyhow::bail!("IQ schema version 8 retains retired repository policy columns");
            }
            let malformed_policy_pairs: i64 = conn.query_row(
                "SELECT COUNT(*) FROM queue_items item JOIN integration_attempts attempt ON attempt.id=item.current_attempt_id WHERE item.status IN ('merging','merged','validating','validated','integrating','blocked') AND ((attempt.policy_snapshot_json IS NULL)!=(attempt.policy_digest IS NULL) OR (attempt.policy_snapshot_json IS NOT NULL AND json_valid(attempt.policy_snapshot_json)=0))",
                [],
                |row| row.get(0),
            )?;
            if malformed_policy_pairs != 0 {
                anyhow::bail!(
                    "IQ schema version 8 has {malformed_policy_pairs} malformed attempt policy row(s)"
                );
            }
            let missing_active_policy: i64 = conn.query_row(
                "SELECT COUNT(*) FROM queue_items item JOIN registered_repositories repository ON repository.repo_key=item.repo_key JOIN integration_attempts attempt ON attempt.id=item.current_attempt_id WHERE item.status IN ('merging','merged','validating','validated','integrating','blocked') AND (attempt.policy_snapshot_json IS NULL OR attempt.policy_digest IS NULL)",
                [],
                |row| row.get(0),
            )?;
            if missing_active_policy != 0 {
                anyhow::bail!(
                    "IQ schema version 8 has {missing_active_policy} active registered attempt(s) without policy snapshots"
                );
            }
            let mut statement = conn.prepare(
                "SELECT attempt.policy_snapshot_json,attempt.policy_digest FROM queue_items item JOIN integration_attempts attempt ON attempt.id=item.current_attempt_id WHERE item.status IN ('merging','merged','validating','validated','integrating','blocked') AND attempt.policy_snapshot_json IS NOT NULL",
            )?;
            let snapshots = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for (snapshot, digest) in snapshots {
                crate::composition::verify_policy_snapshot(&snapshot, &digest)?;
            }
            Ok(())
        }

        #[allow(dead_code)]
        fn migrate_released_v2(conn: &mut Connection) -> Result<()> {
            let invalid_rows: i64 = conn.query_row(
                "SELECT
                   (SELECT COUNT(*) FROM queue_items item WHERE
                     item.id='' OR item.repo_key='' OR item.repo_path='' OR item.source_branch='' OR item.target_branch='' OR
                     length(item.current_head_sha) NOT IN (40,64) OR item.current_head_sha GLOB '*[^0-9A-Fa-f]*' OR
                     json_valid(item.producer_metadata_json)=0 OR json_valid(item.validation_evidence_json)=0 OR
                     (item.conflict_json IS NOT NULL AND json_valid(item.conflict_json)=0) OR
                     item.status NOT IN ('ready','merging','merged','validating','validated','integrating','blocked','integrated','cancelled') OR
                     NOT (
                       (item.integration_workspace_cleaned_at IS NULL AND item.integration_workspace_path IS NULL AND item.integration_workspace_rift_id IS NULL AND item.integration_workspace_source_rift_id IS NULL AND item.status IN ('ready','merging','blocked','cancelled')) OR
                       (item.integration_workspace_cleaned_at IS NULL AND item.integration_workspace_path IS NOT NULL AND item.integration_workspace_rift_id IS NULL AND item.integration_workspace_source_rift_id IS NULL AND item.status IN ('merging','cancelled')) OR
                       (item.integration_workspace_cleaned_at IS NULL AND item.integration_workspace_path IS NOT NULL AND item.integration_workspace_rift_id IS NOT NULL AND item.integration_workspace_source_rift_id IS NOT NULL AND item.status IN ('ready','merging','merged','validating','validated','integrating','blocked','integrated','cancelled')) OR
                       (item.integration_workspace_cleaned_at IS NOT NULL AND item.status IN ('integrated','cancelled') AND item.integration_workspace_path IS NULL AND item.integration_workspace_rift_id IS NULL AND item.integration_workspace_source_rift_id IS NULL)
                     ) OR
                     (item.current_attempt_id IS NOT NULL AND NOT EXISTS (
                       SELECT 1 FROM integration_attempts attempt WHERE attempt.id=item.current_attempt_id AND attempt.item_id=item.id
                     ))) +
                   (SELECT COUNT(*) FROM integration_attempts attempt WHERE
                     attempt.id='' OR attempt.attempt_number<1 OR
                     length(attempt.source_head_sha) NOT IN (40,64) OR attempt.source_head_sha GLOB '*[^0-9A-Fa-f]*' OR
                     (attempt.target_base_sha IS NOT NULL AND (length(attempt.target_base_sha) NOT IN (40,64) OR attempt.target_base_sha GLOB '*[^0-9A-Fa-f]*')) OR
                     (attempt.merge_commit_sha IS NOT NULL AND (length(attempt.merge_commit_sha) NOT IN (40,64) OR attempt.merge_commit_sha GLOB '*[^0-9A-Fa-f]*')) OR
                     (attempt.validated_commit_sha IS NOT NULL AND (length(attempt.validated_commit_sha) NOT IN (40,64) OR attempt.validated_commit_sha GLOB '*[^0-9A-Fa-f]*')) OR
                     (attempt.landed_commit_sha IS NOT NULL AND (length(attempt.landed_commit_sha) NOT IN (40,64) OR attempt.landed_commit_sha GLOB '*[^0-9A-Fa-f]*')) OR
                     (attempt.validation_exit_code IS NULL) != (attempt.validation_log_path IS NULL)) +
                   (SELECT COUNT(*) FROM queue_events event WHERE event.id='' OR event.event_type='' OR event.message='') +
                   (SELECT COUNT(*) FROM prompts prompt WHERE prompt.id='' OR prompt.question='' OR prompt.status='') +
                   (SELECT COUNT(*) FROM communication_bindings binding WHERE
                     binding.id='' OR binding.repo_key='' OR binding.transport_id='' OR binding.transport_kind='' OR binding.endpoint_fingerprint='' OR binding.marker='' OR
                     (binding.external_ref_json IS NOT NULL AND json_valid(binding.external_ref_json)=0)) +
                   (SELECT COUNT(*) FROM communication_response_receipts receipt WHERE receipt.binding_id='' OR receipt.external_response_id='' OR receipt.prompt_id='' OR receipt.actor='' OR receipt.disposition='') +
                   (SELECT COUNT(*) FROM workspace_roots root WHERE root.repo_key='' OR root.source_path='' OR root.source_rift_id='' OR root.workspace_root='' OR root.registry_identity='' OR root.generation<0) +
                   (SELECT COUNT(*) FROM workspace_gc_debt debt WHERE debt.registry_identity='')",
                [],
                |row| row.get(0),
            )?;
            if invalid_rows != 0 {
                anyhow::bail!(
                    "released IQ schema version 2 contains {invalid_rows} malformed durable row(s)"
                );
            }
            conn.pragma_update(None, "foreign_keys", "OFF")?;
            conn.pragma_update(None, "legacy_alter_table", "ON")?;
            let migration = (|| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Self::reject_active_migration_leases(&tx, "2")?;
                Self::reject_nonterminal_migration_items(&tx, "2")?;
                let path_updates = Self::validated_v2_repository_paths(&tx)?;
                for update in path_updates.queue {
                    tx.execute(
                        "UPDATE queue_items SET repo_path=?1 WHERE repo_path=?2",
                        params![update.canonical, update.stored],
                    )?;
                }
                for update in path_updates.workspace_roots {
                    tx.execute(
                        "UPDATE workspace_roots SET source_path=?1 WHERE source_path=?2",
                        params![update.canonical, update.stored],
                    )?;
                }
                tx.execute_batch(
                    r#"
DROP INDEX IF EXISTS queue_items_active_identity;
ALTER TABLE queue_items RENAME TO queue_items_v2;
ALTER TABLE integration_attempts RENAME TO integration_attempts_v2;
"#,
                )?;
                tx.execute_batch(SCHEMA)?;
                tx.execute_batch(COMPOSITION_SCHEMA)?;
                tx.execute_batch(
                    r#"INSERT INTO queue_items (
  id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,landing_state_json,source_kind,source_ref,submission_id,landing_policy,replacement_json,created_at,updated_at
)
SELECT
  id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,
  CASE
    WHEN status='integrated' THEN json_object('state','landed','candidate_sha',(SELECT validated_commit_sha FROM integration_attempts_v2 WHERE id=queue_items_v2.current_attempt_id),'commit_sha',landed_commit_sha)
    WHEN landing_fenced=1 THEN json_object('state','uncertain','candidate_sha',(SELECT validated_commit_sha FROM integration_attempts_v2 WHERE id=queue_items_v2.current_attempt_id),'expected_target_sha',(SELECT target_base_sha FROM integration_attempts_v2 WHERE id=queue_items_v2.current_attempt_id))
    ELSE json_object('state','ready')
  END,
  'remote_branch',source_branch,NULL,CASE WHEN pr_url IS NULL THEN 'direct' ELSE 'provider' END,NULL,created_at,updated_at
FROM queue_items_v2;
INSERT INTO integration_attempts (
  id,item_id,attempt_number,source_head_sha,target_base_sha,merge_commit_sha,validated_commit_sha,landed_commit_sha,validation_command,validation_exit_code,validation_log_path,policy_snapshot_json,policy_digest,signoff_evidence_json,moved_base_json,started_at,finished_at,result
)
SELECT
  id,item_id,attempt_number,source_head_sha,target_base_sha,merge_commit_sha,validated_commit_sha,landed_commit_sha,validation_command,validation_exit_code,validation_log_path,NULL,NULL,NULL,'{"state":"none"}',started_at,finished_at,result
FROM integration_attempts_v2;
DROP TABLE integration_attempts_v2;
DROP TABLE queue_items_v2;
UPDATE queue_metadata SET value='8' WHERE key='workspace_schema_version';"#,
                )?;
                Self::migrate_registered_remote_identities(&tx)?;
                tx.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_CHECKOUT_TRIGGERS)?;
                tx.execute_batch(LANDING_STATE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_REMOTE_TRIGGERS)?;
                tx.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
                Self::validate_standalone_v8(&tx)?;
                tx.commit()?;
                Ok::<(), anyhow::Error>(())
            })();
            conn.pragma_update(None, "legacy_alter_table", "OFF")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            migration?;
            Ok(())
        }

        #[allow(dead_code)]
        fn validated_v2_repository_paths(conn: &Connection) -> Result<V2RepositoryPathUpdates> {
            let mut queue_statement = conn.prepare(
                "SELECT DISTINCT CAST(repo_path AS BLOB),target_branch,repo_key FROM queue_items",
            )?;
            let queue_rows = queue_statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut bindings = HashMap::<String, (String, String)>::new();
            let mut physical_targets = HashMap::<(String, String), String>::new();
            let mut queue_paths = HashMap::<String, String>::new();
            for (stored, target, repo_key) in queue_rows {
                let stored = String::from_utf8(stored)
                    .context("version 2 queue repository path is not valid UTF-8")?;
                let canonical = Self::canonical_git_root(&stored, "version 2 queue repository")?;
                if let Some(existing) =
                    bindings.insert(repo_key.clone(), (canonical.clone(), target.clone()))
                {
                    if existing != (canonical.clone(), target.clone()) {
                        anyhow::bail!(
                            "version 2 repo_key {repo_key} maps to multiple repository targets"
                        );
                    }
                }
                if let Some(existing_key) =
                    physical_targets.insert((canonical.clone(), target.clone()), repo_key.clone())
                {
                    if existing_key != repo_key {
                        anyhow::bail!(
                            "version 2 repository target {canonical}::{target} has multiple repo_key values"
                        );
                    }
                }
                queue_paths.insert(stored, canonical);
            }

            let mut root_statement = conn.prepare(
                "SELECT DISTINCT CAST(source_path AS BLOB),repo_key FROM workspace_roots",
            )?;
            let root_rows = root_statement
                .query_map([], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut root_paths = HashMap::<String, String>::new();
            for (stored, repo_key) in root_rows {
                let stored = String::from_utf8(stored)
                    .context("version 2 Rift source path is not valid UTF-8")?;
                let canonical = Self::canonical_git_root(&stored, "version 2 Rift source")?;
                if let Some((queue_path, _)) = bindings.get(&repo_key) {
                    if queue_path != &canonical {
                        anyhow::bail!(
                            "version 2 workspace root for {repo_key} has source {canonical}, expected {queue_path}"
                        );
                    }
                }
                root_paths.insert(stored, canonical);
            }
            Ok(V2RepositoryPathUpdates {
                queue: queue_paths
                    .into_iter()
                    .map(|(stored, canonical)| CanonicalPathUpdate { stored, canonical })
                    .collect(),
                workspace_roots: root_paths
                    .into_iter()
                    .map(|(stored, canonical)| CanonicalPathUpdate { stored, canonical })
                    .collect(),
            })
        }

        #[allow(dead_code)]
        fn canonical_git_root(stored: &str, label: &str) -> Result<String> {
            if stored.is_empty() {
                anyhow::bail!("{label} path must not be empty");
            }
            let path = Path::new(stored);
            if !path.is_absolute() {
                anyhow::bail!("{label} path must be absolute: {stored}");
            }
            let canonical = path
                .canonicalize()
                .with_context(|| format!("resolve {label} path {stored}"))?;
            require_real_directory(&canonical, label)?;
            let output = Command::new("git")
                .env("GIT_OPTIONAL_LOCKS", "0")
                .args(["rev-parse", "--show-toplevel"])
                .current_dir(&canonical)
                .output()
                .with_context(|| format!("verify {label} Git root {}", canonical.display()))?;
            if !output.status.success() {
                anyhow::bail!(
                    "{label} is not a verifiable Git repository {}: {}",
                    canonical.display(),
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            let top_level = PathBuf::from(String::from_utf8(output.stdout)?);
            let top_level = top_level
                .as_os_str()
                .to_str()
                .context("Git top-level output is not valid UTF-8")?
                .trim();
            if Path::new(top_level).canonicalize()? != canonical {
                anyhow::bail!(
                    "{label} path {} is not the canonical Git root",
                    canonical.display()
                );
            }
            canonical
                .to_str()
                .map(str::to_string)
                .context("canonical repository path is not valid UTF-8")
        }

        #[allow(dead_code)]
        fn migrate_standalone_v3(conn: &mut Connection, database_path: &Path) -> Result<()> {
            conn.pragma_update(None, "foreign_keys", "OFF")?;
            conn.pragma_update(None, "legacy_alter_table", "ON")?;
            let migration = (|| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Self::reject_active_migration_leases(&tx, "3")?;
                Self::reject_nonterminal_migration_items(&tx, "3")?;
                Self::reconcile_workspace_owner_markers(&tx, database_path, true)?;
                let invalid_seed_state: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM registered_repositories WHERE json_extract(seed_refresh_json,'$.state')!='ready'",
                    [],
                    |row| row.get(0),
                )?;
                if invalid_seed_state != 0 {
                    anyhow::bail!(
                        "unpublished IQ schema version 3 has {invalid_seed_state} unresolved seed refresh row(s)"
                    );
                }
                let invalid_sources: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM queue_items item WHERE NOT (
                        (item.source_kind='remote_branch' AND item.source_ref IS NOT NULL AND item.submission_id IS NULL AND item.source_ref=item.source_branch AND ((item.landing_policy='direct' AND item.pr_url IS NULL) OR (item.landing_policy='provider' AND item.pr_url IS NOT NULL))) OR
                        (item.source_kind='local_submission' AND item.source_ref IS NOT NULL AND item.submission_id IS NOT NULL AND item.source_ref=item.source_branch AND item.landing_policy='squash' AND item.pr_url IS NULL AND EXISTS (
                            SELECT 1 FROM local_submissions submission
                            WHERE submission.id=item.submission_id
                              AND submission.repo_key=item.repo_key
                              AND submission.commit_sha=item.current_head_sha
                              AND submission.private_ref=item.source_ref
                        ))
                    )",
                    [],
                    |row| row.get(0),
                )?;
                if invalid_sources != 0 {
                    anyhow::bail!(
                        "standalone version 3 contains {invalid_sources} invalid queue source rows"
                    );
                }
                tx.execute_batch(
                    r#"
DROP INDEX IF EXISTS queue_items_active_identity;
ALTER TABLE queue_items RENAME TO queue_items_v3;
ALTER TABLE local_submissions RENAME TO local_submissions_v3;
ALTER TABLE integration_attempts ADD COLUMN moved_base_json TEXT NOT NULL DEFAULT '{"state":"none"}';
"#,
                )?;
                tx.execute_batch(SCHEMA)?;
                tx.execute_batch(COMPOSITION_SCHEMA)?;
                tx.execute(
                    r#"INSERT INTO local_submissions (
  id,queue_item_id,repo_key,workspace_id,base_sha,commit_sha,private_ref,staging_ref,replaces_item_id,state,created_at
)
SELECT
  submission.id,
  COALESCE(
    (SELECT item.id FROM queue_items_v3 item WHERE item.submission_id=submission.id LIMIT 1),
    (SELECT item.id
       FROM queue_items_v3 item
       JOIN local_submissions_v3 current_submission ON current_submission.id=item.submission_id
      WHERE current_submission.workspace_id=submission.workspace_id
      ORDER BY item.created_at DESC,item.id DESC LIMIT 1)
  ),
  submission.repo_key,
  submission.workspace_id,
  workspace.base_sha,
  submission.commit_sha,
  submission.private_ref,
  'refs/iq/staging/' || submission.id,
  NULL,
  CASE
    WHEN EXISTS(SELECT 1 FROM queue_items_v3 item WHERE item.submission_id=submission.id AND item.status='integrated') THEN 'integrated'
    WHEN EXISTS(SELECT 1 FROM queue_items_v3 item WHERE item.submission_id=submission.id AND item.status='cancelled') THEN 'cancelled'
    WHEN EXISTS(SELECT 1 FROM queue_items_v3 item WHERE item.submission_id=submission.id) THEN 'queued'
    ELSE 'replaced'
  END,
  submission.created_at
FROM local_submissions_v3 submission
JOIN development_workspaces workspace ON workspace.id=submission.workspace_id"#,
                    [],
                )?;
                tx.execute_batch(
                    r#"INSERT INTO queue_items (
  id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,landing_state_json,source_kind,source_ref,submission_id,landing_policy,replacement_json,created_at,updated_at
)
SELECT
  id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,
  CASE
    WHEN status='integrated' THEN json_object('state','landed','candidate_sha',(SELECT validated_commit_sha FROM integration_attempts WHERE id=queue_items_v3.current_attempt_id),'commit_sha',landed_commit_sha)
    WHEN landing_fenced=1 THEN json_object('state','uncertain','candidate_sha',(SELECT validated_commit_sha FROM integration_attempts WHERE id=queue_items_v3.current_attempt_id),'expected_target_sha',(SELECT target_base_sha FROM integration_attempts WHERE id=queue_items_v3.current_attempt_id))
    ELSE json_object('state','ready')
  END,
  source_kind,source_ref,submission_id,landing_policy,replacement_json,created_at,updated_at
FROM queue_items_v3;
DROP TABLE queue_items_v3;
DROP TABLE local_submissions_v3;
ALTER TABLE registered_repositories ADD COLUMN checkout_reconciliation_json TEXT NOT NULL DEFAULT '{"state":"ready","target_sha":""}';
UPDATE registered_repositories
SET checkout_reconciliation_json=json_object('state','ready','target_sha',policy_target_sha);
UPDATE registered_repositories
SET seed_refresh_json=json_object('state','ready','target_sha',policy_target_sha);"#,
                )?;
                tx.execute(
                    "INSERT INTO queue_metadata (key,value) VALUES ('workspace_schema_version','7') ON CONFLICT(key) DO UPDATE SET value='7'",
                    [],
                )?;
                Self::migrate_registered_remote_identities(&tx)?;
                tx.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_CHECKOUT_TRIGGERS)?;
                tx.execute_batch(LANDING_STATE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_REMOTE_TRIGGERS)?;
                tx.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
                Self::validate_standalone_v7(&tx)?;
                tx.commit()?;
                Ok::<(), anyhow::Error>(())
            })();
            conn.pragma_update(None, "legacy_alter_table", "OFF")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            migration?;
            Ok(())
        }

        #[allow(dead_code)]
        fn migrate_standalone_v4(conn: &mut Connection) -> Result<()> {
            conn.pragma_update(None, "foreign_keys", "OFF")?;
            conn.pragma_update(None, "legacy_alter_table", "ON")?;
            let migration = (|| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Self::reject_active_migration_leases(&tx, "4")?;
                Self::reject_nonterminal_migration_items(&tx, "4")?;
                let invalid_seed_state: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM registered_repositories WHERE json_extract(seed_refresh_json,'$.state')!='ready'",
                    [],
                    |row| row.get(0),
                )?;
                if invalid_seed_state != 0 {
                    anyhow::bail!(
                        "unpublished IQ schema version 4 has {invalid_seed_state} unresolved seed refresh row(s)"
                    );
                }
                tx.execute_batch(
                    r#"
ALTER TABLE local_submissions RENAME TO local_submissions_v4;
"#,
                )?;
                tx.execute_batch(COMPOSITION_SCHEMA)?;
                tx.execute_batch(
                    r#"INSERT INTO local_submissions (
  id,queue_item_id,repo_key,workspace_id,base_sha,commit_sha,private_ref,staging_ref,replaces_item_id,state,created_at
)
SELECT
  submission.id,submission.queue_item_id,submission.repo_key,submission.workspace_id,workspace.base_sha,submission.commit_sha,submission.private_ref,submission.staging_ref,submission.replaces_item_id,submission.state,submission.created_at
FROM local_submissions_v4 submission
JOIN development_workspaces workspace ON workspace.id=submission.workspace_id;
DROP TABLE local_submissions_v4;
ALTER TABLE registered_repositories ADD COLUMN checkout_reconciliation_json TEXT NOT NULL DEFAULT '{"state":"ready","target_sha":""}';
UPDATE registered_repositories
SET checkout_reconciliation_json=json_object('state','ready','target_sha',policy_target_sha);
UPDATE registered_repositories
SET seed_refresh_json=json_object('state','ready','target_sha',policy_target_sha);
UPDATE queue_metadata SET value='7' WHERE key='workspace_schema_version';"#,
                )?;
                Self::migrate_queue_landing_state(&tx)?;
                Self::migrate_registered_remote_identities(&tx)?;
                tx.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_CHECKOUT_TRIGGERS)?;
                tx.execute_batch(LANDING_STATE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_REMOTE_TRIGGERS)?;
                tx.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
                Self::validate_standalone_v7(&tx)?;
                tx.commit()?;
                Ok::<(), anyhow::Error>(())
            })();
            conn.pragma_update(None, "legacy_alter_table", "OFF")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            migration?;
            Ok(())
        }

        #[allow(dead_code)]
        fn migrate_standalone_v5(conn: &mut Connection) -> Result<()> {
            conn.pragma_update(None, "foreign_keys", "OFF")?;
            conn.pragma_update(None, "legacy_alter_table", "ON")?;
            let migration = (|| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Self::reject_active_migration_leases(&tx, "5")?;
                Self::reject_nonterminal_migration_items(&tx, "5")?;
                let malformed_seed_targets: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM registered_repositories WHERE
                   json_valid(seed_refresh_json)=0 OR
                   length(json_extract(seed_refresh_json,'$.target_sha')) NOT IN (40,64) OR
                   json_extract(seed_refresh_json,'$.target_sha') GLOB '*[^0-9A-Fa-f]*'",
                    [],
                    |row| row.get(0),
                )?;
                if malformed_seed_targets != 0 {
                    anyhow::bail!(
                        "unpublished IQ schema version 5 has {malformed_seed_targets} invalid seed target row(s)"
                    );
                }
                tx.execute_batch(
                    r#"ALTER TABLE registered_repositories ADD COLUMN checkout_reconciliation_json TEXT NOT NULL DEFAULT '{"state":"ready","target_sha":""}';
UPDATE registered_repositories
SET checkout_reconciliation_json=json_object('state','ready','target_sha',json_extract(seed_refresh_json,'$.target_sha'));
UPDATE queue_metadata SET value='7' WHERE key='workspace_schema_version';"#,
                )?;
                tx.execute_batch(COMPOSITION_SCHEMA)?;
                Self::migrate_queue_landing_state(&tx)?;
                Self::migrate_registered_remote_identities(&tx)?;
                tx.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_CHECKOUT_TRIGGERS)?;
                tx.execute_batch(LANDING_STATE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_REMOTE_TRIGGERS)?;
                tx.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
                Self::validate_standalone_v7(&tx)?;
                tx.commit()?;
                Ok::<(), anyhow::Error>(())
            })();
            conn.pragma_update(None, "legacy_alter_table", "OFF")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            migration
        }

        #[allow(dead_code)]
        fn migrate_standalone_v6(conn: &mut Connection) -> Result<()> {
            conn.pragma_update(None, "foreign_keys", "OFF")?;
            conn.pragma_update(None, "legacy_alter_table", "ON")?;
            let migration = (|| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Self::reject_active_migration_leases(&tx, "6")?;
                Self::reject_nonterminal_migration_items(&tx, "6")?;
                tx.execute_batch(COMPOSITION_SCHEMA)?;
                Self::migrate_queue_landing_state(&tx)?;
                Self::migrate_registered_remote_identities(&tx)?;
                tx.execute(
                    "UPDATE queue_metadata SET value='7' WHERE key='workspace_schema_version'",
                    [],
                )?;
                tx.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_CHECKOUT_TRIGGERS)?;
                tx.execute_batch(LANDING_STATE_TRIGGERS)?;
                tx.execute_batch(REGISTERED_REMOTE_TRIGGERS)?;
                tx.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
                Self::validate_standalone_v7(&tx)?;
                tx.commit()?;
                Ok::<(), anyhow::Error>(())
            })();
            conn.pragma_update(None, "legacy_alter_table", "OFF")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            migration
        }

        #[allow(dead_code)]
        fn migrate_queue_landing_state(conn: &Connection) -> Result<()> {
            conn.execute_batch(
                r#"
DROP INDEX IF EXISTS queue_items_active_identity;
ALTER TABLE queue_items RENAME TO queue_items_v6;
"#,
            )?;
            conn.execute_batch(SCHEMA)?;
            conn.execute_batch(
                r#"INSERT INTO queue_items (
  id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,landing_state_json,source_kind,source_ref,submission_id,landing_policy,replacement_json,created_at,updated_at
)
SELECT
  id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,
  CASE
    WHEN status='integrated' THEN json_object('state','landed','candidate_sha',(SELECT validated_commit_sha FROM integration_attempts WHERE id=queue_items_v6.current_attempt_id),'commit_sha',landed_commit_sha)
    WHEN landing_fenced=1 THEN json_object('state','uncertain','candidate_sha',(SELECT validated_commit_sha FROM integration_attempts WHERE id=queue_items_v6.current_attempt_id),'expected_target_sha',(SELECT target_base_sha FROM integration_attempts WHERE id=queue_items_v6.current_attempt_id))
    ELSE json_object('state','ready')
  END,
  source_kind,source_ref,submission_id,landing_policy,replacement_json,created_at,updated_at
FROM queue_items_v6;
DROP TABLE queue_items_v6;"#,
            )?;
            Ok(())
        }

        #[allow(dead_code)]
        fn migrate_registered_remote_identities(conn: &Connection) -> Result<()> {
            conn.execute_batch(COMPOSITION_SCHEMA)?;
            let mut statement = conn.prepare(
                "SELECT repo_key,integration_path,target_branch,remote,created_at FROM registered_repositories",
            )?;
            let repositories = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(statement);
            for (repo_key, path, target, remote_name, created_at) in repositories {
                let path = PathBuf::from(std::ffi::OsString::from_vec(path));
                let canonical = path
                    .canonicalize()
                    .with_context(|| format!("resolve registered repository {}", path.display()))?;
                if canonical != path {
                    anyhow::bail!(
                        "registered repository path is not canonical: {}",
                        path.display()
                    );
                }
                let remote = crate::composition::resolve_remote_identity(&path, &remote_name)
                    .with_context(|| format!("resolve registered remote for {repo_key}"))?;
                conn.execute(
                    "INSERT INTO registered_remote_identities(repo_key,integration_path,target_branch,remote_name,fetch_url,push_url,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![repo_key,path_bytes(&path),target,remote.name,remote.fetch_url,remote.push_url,created_at],
                )?;
            }
            Ok(())
        }

        #[allow(dead_code)]
        fn reject_active_migration_leases(conn: &Connection, version: &str) -> Result<()> {
            let active_leases: i64 = conn.query_row(
                "SELECT COUNT(*) FROM repo_leases WHERE expires_at>?1",
                params![now()],
                |row| row.get(0),
            )?;
            if active_leases != 0 {
                anyhow::bail!(
                    "standalone IQ schema version {version} has {active_leases} active repository operation lease(s)"
                );
            }
            Ok(())
        }

        #[allow(dead_code)]
        fn reject_nonterminal_migration_items(conn: &Connection, version: &str) -> Result<()> {
            let nonterminal_items: i64 = conn.query_row(
                "SELECT COUNT(*) FROM queue_items WHERE status NOT IN ('integrated','cancelled')",
                [],
                |row| row.get(0),
            )?;
            if nonterminal_items != 0 {
                anyhow::bail!(
                    "standalone IQ schema version {version} has {nonterminal_items} nonterminal queue item(s); finish or cancel them before migration"
                );
            }
            Ok(())
        }

        fn reconcile_workspace_owner_markers(
            conn: &Connection,
            expected_legacy_database: &Path,
            rewrite_legacy: bool,
        ) -> Result<()> {
            let metadata_exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='queue_metadata')",
                [],
                |row| row.get(0),
            )?;
            if !metadata_exists {
                anyhow::bail!("legacy state has no standalone IQ schema identity");
            }
            let version: String = conn.query_row(
                "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                [],
                |row| row.get(0),
            )?;
            if !matches!(version.as_str(), "2" | "3" | "4" | "5" | "6" | "7") {
                anyhow::bail!(
                    "legacy state schema version {version} cannot migrate to IQ-owned state"
                );
            }
            let database_id: String = conn.query_row(
                "SELECT value FROM queue_metadata WHERE key='database_id'",
                [],
                |row| row.get(0),
            )?;
            let roots_exist: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='workspace_roots')",
                [],
                |row| row.get(0),
            )?;
            if !roots_exist {
                return Ok(());
            }
            let mut statement = conn.prepare(
                "SELECT repo_key,source_path,source_rift_id,workspace_root,registry_identity FROM workspace_roots",
            )?;
            let roots = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for (repo_key, source, source_rift_id, root, registry_identity) in roots {
                let marker = PathBuf::from(&root).join(".iq-workspace-owner.json");
                require_regular_file(&marker, "IQ workspace owner marker")?;
                let raw = fs::read(&marker)?;
                let value: Value = serde_json::from_slice(&raw)?;
                let marker_version = value
                    .get("version")
                    .and_then(Value::as_u64)
                    .context("IQ workspace owner marker has no version")?;
                let expected = RiftWorkspaceRootOwner {
                    version: 3,
                    queue_database_id: database_id.clone(),
                    repo_key,
                    source: PathBuf::from(source),
                    source_rift_id,
                    registry_identity,
                };
                match marker_version {
                    3 => {
                        let actual: RiftWorkspaceRootOwner = serde_json::from_value(value)?;
                        if actual != expected {
                            anyhow::bail!(
                                "IQ workspace owner marker differs from standalone database: {}",
                                marker.display()
                            );
                        }
                    }
                    2 => {
                        let actual: LegacyRiftWorkspaceRootOwner = serde_json::from_value(value)?;
                        if actual.version != 2
                            || actual.queue_database_id != expected.queue_database_id
                            || actual.queue_database_path != expected_legacy_database
                            || actual.repo_key != expected.repo_key
                            || actual.source != expected.source
                            || actual.source_rift_id != expected.source_rift_id
                            || actual.registry_identity != expected.registry_identity
                        {
                            anyhow::bail!(
                                "legacy IQ workspace owner marker differs from standalone database: {}",
                                marker.display()
                            );
                        }
                        if !rewrite_legacy {
                            continue;
                        }
                        let temporary = marker
                            .with_file_name(format!(".iq-workspace-owner-{}.tmp", Uuid::new_v4()));
                        fs::write(&temporary, serde_json::to_vec_pretty(&expected)?)?;
                        OpenOptions::new().read(true).open(&temporary)?.sync_all()?;
                        fs::rename(&temporary, &marker)?;
                        OpenOptions::new()
                            .read(true)
                            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                            .open(marker.parent().context("owner marker has no parent")?)?
                            .sync_all()?;
                    }
                    other => anyhow::bail!(
                        "unsupported IQ workspace owner marker version {other}: {}",
                        marker.display()
                    ),
                }
            }
            Ok(())
        }

        fn connect(&self) -> Result<Connection> {
            self.verify_database_file()?;
            let conn = Connection::open_with_flags(
                &self.path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )
            .with_context(|| format!("open queue db {}", self.path.display()))?;
            self.verify_database_file()?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.busy_timeout(Self::WRITE_BUSY_TIMEOUT)?;
            Ok(conn)
        }

        fn connect_read_only(&self) -> Result<Connection> {
            self.reader().connect(SqliteQueueReader::BUSY_TIMEOUT)
        }

        pub(crate) fn reader(&self) -> SqliteQueueReader {
            SqliteQueueReader {
                path: self.path.clone(),
                database_dev: self.database_dev,
                database_ino: self.database_ino,
            }
        }

        fn verify_database_file(&self) -> Result<()> {
            let metadata = fs::symlink_metadata(&self.path)
                .with_context(|| format!("inspect queue db {}", self.path.display()))?;
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || metadata.dev() != self.database_dev
                || metadata.ino() != self.database_ino
            {
                anyhow::bail!(
                    "queue database identity changed while IQ was running: {}",
                    self.path.display()
                );
            }
            Ok(())
        }

        pub fn enqueue(&self, request: EnqueueRequest) -> Result<QueueItem> {
            let state_repository = request.state_repository.clone().validate()?;
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = now();
            let existing: Option<(String, String, Option<String>)> = tx
                .query_row(
                    "SELECT id,current_head_sha,pr_url FROM queue_items WHERE repo_key=?1 AND source_branch=?2 AND target_branch=?3 AND status NOT IN ('integrated','cancelled')",
                    params![request.repo_key, request.source_branch, request.target_branch],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;

            let item_id = if let Some((id, current_head_sha, pr_url)) = existing {
                if current_head_sha != request.current_head_sha || pr_url != request.pr_url {
                    anyhow::bail!(
                        "active queue item {id} already tracks {current_head_sha}; update blocked agent work through requeue instead of enqueue"
                    );
                }
                let stored: String = tx.query_row(
                    "SELECT snapshot_json FROM item_state_repository_bindings WHERE item_id=?1",
                    params![id],
                    |row| row.get(0),
                )?;
                if serde_json::from_str::<crate::control_domain::StateRepositorySnapshot>(&stored)?
                    != state_repository
                {
                    anyhow::bail!("active queue item has a different state-repository binding");
                }
                id
            } else {
                let id = Uuid::new_v4().to_string();
                tx.execute(
                    "INSERT INTO queue_items (id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,source_kind,source_ref,landing_policy,created_at,updated_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,'{}','ready',?8,'remote_branch',?4,?10,?9,?9)",
                    params![
                        id,
                        request.repo_key,
                        request.repo_path,
                        request.source_branch,
                        request.target_branch,
                        request.pr_url,
                        request.producer_metadata.to_string(),
                        request.current_head_sha,
                        now,
                        if request.pr_url.is_some() { "provider" } else { "direct" },
                    ],
                )?;
                Self::record_event_tx(&tx, &id, "item_enqueued", "item enqueued")?;
                insert_state_repository_binding(&tx, &id, &state_repository, &now)?;
                id
            };
            tx.commit()?;
            self.get_item(&item_id)
        }

        pub fn list_items(&self) -> Result<Vec<QueueItem>> {
            self.reader().list_items()
        }

        pub fn get_item(&self, item_id: &str) -> Result<QueueItem> {
            self.reader().get_item(item_id)
        }

        pub(crate) fn execution_authority(
            &self,
            item_id: &str,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<ExecutionAuthority> {
            self.reader().execution_authority(
                item_id,
                repo_key,
                owner_id,
                Self::AUTHORITY_READ_TIMEOUT,
            )
        }

        pub(crate) fn lease_authority(
            &self,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<ExecutionAuthority> {
            self.reader()
                .lease_authority(repo_key, owner_id, Self::AUTHORITY_READ_TIMEOUT)
        }

        pub fn oldest_active_item(&self, repo_key: &str) -> Result<Option<QueueItem>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT * FROM queue_items WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled') ORDER BY created_at ASC, id ASC LIMIT 1",
                params![repo_key],
                map_item,
            )
            .optional()
            .with_context(|| format!("read oldest active item for repo queue {repo_key}"))
        }

        pub fn claim_next_ready(&self, repo_key: &str) -> Result<Option<(QueueItem, Attempt)>> {
            self.claim_next_ready_with_authority(
                repo_key,
                MutationAuthority::External,
                AttemptPolicy::HostValidation,
            )
        }

        pub(crate) fn claim_next_ready_owned(
            &self,
            repo_key: &str,
            owner_id: &str,
            policy: AttemptPolicy<'_>,
        ) -> Result<Option<(QueueItem, Attempt)>> {
            self.claim_next_ready_with_authority(
                repo_key,
                MutationAuthority::RepositoryLease { repo_key, owner_id },
                policy,
            )
        }

        fn claim_next_ready_with_authority(
            &self,
            repo_key: &str,
            authority: MutationAuthority<'_>,
            policy: AttemptPolicy<'_>,
        ) -> Result<Option<(QueueItem, Attempt)>> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item: Option<QueueItem> = tx
                .query_row(
                    "SELECT * FROM queue_items WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled') ORDER BY created_at ASC, id ASC LIMIT 1",
                    params![repo_key],
                    map_item,
                )
                .optional()?;
            let Some(item) = item else {
                tx.commit()?;
                return Ok(None);
            };
            Self::require_mutation_authority(&tx, &item.repo_key, authority)?;
            if item.status != QueueStatus::Ready {
                tx.commit()?;
                return Ok(None);
            }
            let registered: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM registered_repositories WHERE repo_key=?1)",
                params![repo_key],
                |row| row.get(0),
            )?;
            if matches!((&policy, registered), (AttemptPolicy::HostValidation, true)) {
                anyhow::bail!("registered attempt requires a local policy snapshot");
            }
            let attempt_id = Uuid::new_v4().to_string();
            let attempt_number: i64 = tx.query_row(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM integration_attempts WHERE item_id=?1",
                params![item.id],
                |row| row.get(0),
            )?;
            let now = now();
            let (policy_snapshot, policy_digest) = match policy {
                AttemptPolicy::Snapshot {
                    snapshot_json,
                    digest,
                } => {
                    crate::composition::verify_policy_snapshot(snapshot_json, digest)?;
                    (Some(snapshot_json), Some(digest))
                }
                AttemptPolicy::HostValidation => (None, None),
            };
            tx.execute(
                "INSERT INTO integration_attempts (id,item_id,attempt_number,source_head_sha,policy_snapshot_json,policy_digest,started_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![attempt_id, item.id, attempt_number, item.current_head_sha, policy_snapshot, policy_digest, now],
            )?;
            tx.execute(
                r#"UPDATE queue_items SET status='merging',current_attempt_id=?1,landing_state_json='{"state":"ready"}',blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3"#,
                params![attempt_id, now, item.id],
            )?;
            Self::record_event_tx(
                &tx,
                &item.id,
                "attempt_started",
                "claimed ready item for merging",
            )?;
            tx.commit()?;
            let claimed = self.get_item(&item.id)?;
            let attempt = self.get_attempt(&attempt_id)?;
            Ok(Some((claimed, attempt)))
        }

        pub fn next_resumable_active_item(&self, repo_key: &str) -> Result<Option<QueueItem>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT * FROM queue_items WHERE repo_key=?1 AND status IN ('merging','merged','validating','validated','integrating') ORDER BY created_at ASC LIMIT 1",
                params![repo_key],
                map_item,
            )
            .optional()
            .with_context(|| format!("read next resumable active item for repo queue {repo_key}"))
        }

        pub fn transition_item(&self, item_id: &str, target: QueueStatus) -> Result<QueueItem> {
            self.transition_item_with_authority(item_id, target, MutationAuthority::External)
        }

        pub(crate) fn transition_item_owned(
            &self,
            item_id: &str,
            target: QueueStatus,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<QueueItem> {
            self.transition_item_with_authority(
                item_id,
                target,
                MutationAuthority::RepositoryLease { repo_key, owner_id },
            )
        }

        fn transition_item_with_authority(
            &self,
            item_id: &str,
            target: QueueStatus,
            authority: MutationAuthority<'_>,
        ) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = required_row(
                tx.query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                ),
                "queue item",
                item_id,
            )?;
            let effort_owned: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM integration_efforts WHERE item_id=?1)",
                params![item_id],
                |row| row.get(0),
            )?;
            if effort_owned {
                anyhow::bail!("queue lifecycle is read-only after integration effort creation");
            }
            Self::require_mutation_authority(&tx, &item.repo_key, authority)?;
            StateMachine
                .transition(item.status, target)
                .map_err(anyhow::Error::msg)?;
            if target == QueueStatus::Cancelled && item.landing.is_uncertain() {
                anyhow::bail!(
                    "item {item_id} has an uncertain landing outcome and cannot be cancelled"
                );
            }
            if target == QueueStatus::Cancelled {
                let replacement_creating: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM local_submissions WHERE replaces_item_id=?1 AND state='creating')",
                    params![item_id],
                    |row| row.get(0),
                )?;
                if replacement_creating {
                    anyhow::bail!(
                        "item has an incomplete immutable replacement; finish that submission before cancellation"
                    );
                }
                tx.execute(
                    "UPDATE prompts SET status='cancelled' WHERE item_id=?1 AND status='open'",
                    params![item_id],
                )?;
                if let QueueSource::LocalSubmission { submission_id, .. } = &item.source {
                    let workspace_id: String = tx.query_row(
                        "SELECT workspace_id FROM local_submissions WHERE id=?1",
                        params![submission_id],
                        |row| row.get(0),
                    )?;
                    let changed = tx.execute(
                        "UPDATE development_workspaces SET status='active',updated_at=?1 WHERE id=?2 AND status='submitted'",
                        params![now(), workspace_id],
                    )?;
                    if changed != 1 {
                        anyhow::bail!(
                            "cancelled local submission workspace cannot return to reusable state"
                        );
                    }
                    let changed = tx.execute(
                        "UPDATE local_submissions SET state='cancelled' WHERE id=?1 AND state='queued'",
                        params![submission_id],
                    )?;
                    if changed != 1 {
                        anyhow::bail!("cancelled local submission is not queued");
                    }
                }
                if let Some(attempt_id) = item.current_attempt_id.as_deref() {
                    tx.execute(
                        "UPDATE integration_attempts SET result='cancelled',finished_at=?1 WHERE id=?2 AND result IS NULL",
                        params![now(), attempt_id],
                    )?;
                }
            }
            tx.execute(
                "UPDATE queue_items SET status=?1,replacement_json=CASE WHEN ?1='cancelled' THEN NULL ELSE replacement_json END,updated_at=?2 WHERE id=?3",
                params![target.to_string(), now(), item_id],
            )?;
            Self::record_event_tx(
                &tx,
                item_id,
                "item_transitioned",
                &format!("transitioned to {target}"),
            )?;
            tx.commit()?;
            self.get_item(item_id)
        }

        fn require_mutation_authority(
            tx: &rusqlite::Transaction<'_>,
            item_repo_key: &str,
            authority: MutationAuthority<'_>,
        ) -> Result<()> {
            let MutationAuthority::RepositoryLease { repo_key, owner_id } = authority else {
                return Ok(());
            };
            if item_repo_key != repo_key {
                anyhow::bail!("repository mutation authority does not match queue item");
            }
            let authorized: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM repo_leases WHERE repo_key=?1 AND owner_id=?2 AND expires_at>?3)",
                params![repo_key, owner_id, now()],
                |row| row.get(0),
            )?;
            if !authorized {
                anyhow::bail!("repository operation lease is not owned by {owner_id}");
            }
            Ok(())
        }

        pub(crate) fn authorize_execution_start(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            release_gate: impl FnOnce() -> Result<()>,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (status, current_attempt_id): (String, Option<String>) = required_row(
                tx.query_row(
                    "SELECT status,current_attempt_id FROM queue_items WHERE id=?1",
                    params![item_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ),
                "queue item",
                item_id,
            )?;
            let authorized = status == expected_status.to_string()
                && current_attempt_id.as_deref() == Some(attempt_id);
            if authorized {
                release_gate()?;
            }
            tx.commit()?;
            Ok(authorized)
        }

        pub fn block_item(
            &self,
            item_id: &str,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
        ) -> Result<String> {
            self.block_item_from_status(
                item_id,
                phase.into(),
                phase,
                reason,
                message,
                MutationAuthority::External,
            )
        }

        pub(crate) fn block_item_owned(
            &self,
            item_id: &str,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<String> {
            self.block_item_from_status(
                item_id,
                phase.into(),
                phase,
                reason,
                message,
                MutationAuthority::RepositoryLease { repo_key, owner_id },
            )
        }

        pub(crate) fn block_integrating_recovery_owned(
            &self,
            item_id: &str,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<String> {
            if !matches!(phase, BlockedPhase::Merging | BlockedPhase::Validating) {
                anyhow::bail!("integrating recovery may only resume merging or validating");
            }
            self.block_item_from_status(
                item_id,
                QueueStatus::Integrating,
                phase,
                reason,
                message,
                MutationAuthority::RepositoryLease { repo_key, owner_id },
            )
        }

        fn block_item_from_status(
            &self,
            item_id: &str,
            expected_status: QueueStatus,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
            authority: MutationAuthority<'_>,
        ) -> Result<String> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = required_row(
                tx.query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                ),
                "queue item",
                item_id,
            )?;
            let effort_owned: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM integration_efforts WHERE item_id=?1)",
                params![item_id],
                |row| row.get(0),
            )?;
            if effort_owned {
                anyhow::bail!("queue lifecycle is read-only after integration effort creation");
            }
            Self::require_mutation_authority(&tx, &item.repo_key, authority)?;
            if item.status != expected_status {
                anyhow::bail!(
                    "item {item_id} in status {} cannot block in {phase}",
                    item.status
                );
            }
            let timestamp = now();
            let prompt_id = if reason == BlockedReason::NeedsUserInput {
                let prompt_id = Uuid::new_v4().to_string();
                let options: Vec<&str> = Vec::new();
                let options_json = serde_json::to_string(&options)?;
                tx.execute(
                    "INSERT INTO prompts (id,item_id,attempt_id,blocked_phase,status,question,options_json,allow_freeform,created_by,created_at) VALUES (?1,?2,?3,?4,'open',?5,?6,?7,'integrator',?8)",
                    params![prompt_id, item_id, item.current_attempt_id, phase.to_string(), message, options_json, options.is_empty(), timestamp],
                )?;
                Some(prompt_id)
            } else {
                None
            };
            tx.execute(
                "UPDATE queue_items SET status='blocked',blocked_phase=?1,blocked_reason=?2,blocked_message=?3,prompt_id=?4,updated_at=?5 WHERE id=?6",
                params![phase.to_string(), reason.to_string(), message, prompt_id, timestamp, item_id],
            )?;
            Self::record_event_tx(
                &tx,
                item_id,
                "item_blocked",
                &format!("{phase}/{reason}: {message}"),
            )?;
            tx.commit()?;
            Ok(prompt_id.unwrap_or_default())
        }

        pub fn requeue_agent_fix(&self, item_id: &str, new_head: &str) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = required_row(
                tx.query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                ),
                "queue item",
                item_id,
            )?;
            let effort_owned: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM integration_efforts WHERE item_id=?1)",
                params![item_id],
                |row| row.get(0),
            )?;
            if effort_owned {
                anyhow::bail!("queue lifecycle is read-only after integration effort creation");
            }
            if item.status != QueueStatus::Blocked
                || item.blocked_reason != Some(BlockedReason::NeedsAgentFix)
            {
                anyhow::bail!("item {item_id} is not blocked for agent fix")
            }
            if !matches!(item.source, QueueSource::RemoteBranch { .. }) {
                anyhow::bail!(
                    "local submissions are immutable; use submit --replace for an agent fix"
                );
            }
            tx.execute(
                "UPDATE prompts SET status='superseded' WHERE item_id=?1 AND status='open'",
                params![item_id],
            )?;
            tx.execute(
                r#"UPDATE queue_items SET status='ready',current_head_sha=?1,landing_state_json='{"state":"ready"}',blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3"#,
                params![new_head, now(), item_id],
            )?;
            Self::record_event_tx(&tx, item_id, "agent_requeued", "agent fix marked ready")?;
            tx.commit()?;
            self.get_item(item_id)
        }

        pub fn update_current_head(&self, item_id: &str, new_head: &str) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE queue_items SET current_head_sha=?1,updated_at=?2 WHERE id=?3 AND source_kind='remote_branch'",
                params![new_head, now(), item_id],
            )?;
            if changed != 1 {
                anyhow::bail!("only a remote branch queue source can update its current head");
            }
            Self::record_event_tx(
                &tx,
                item_id,
                "source_head_updated",
                &format!("source head updated to {new_head}"),
            )?;
            tx.commit()?;
            self.get_item(item_id)
        }

        pub fn acquire_repo_lease(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current: Option<(String, String)> = tx
                .query_row(
                    "SELECT owner_id,expires_at FROM repo_leases WHERE repo_key=?1",
                    params![repo_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let now = now();
            let expires_at = (Utc::now() + Duration::seconds(ttl_seconds)).to_rfc3339();
            let can_acquire = match current {
                None => true,
                Some((owner, _expires)) if owner == owner_id => true,
                Some((_owner, expires)) => expires <= now,
            };
            if can_acquire {
                tx.execute(
                    "INSERT INTO repo_leases (repo_key,owner_id,heartbeat_at,expires_at) VALUES (?1,?2,?3,?4)
                     ON CONFLICT(repo_key) DO UPDATE SET owner_id=excluded.owner_id,heartbeat_at=excluded.heartbeat_at,expires_at=excluded.expires_at",
                    params![repo_key, owner_id, now, expires_at],
                )?;
            }
            tx.commit()?;
            Ok(can_acquire)
        }

        pub(crate) fn acquire_repo_operation_lease(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
            repository: &Path,
            target: &str,
        ) -> Result<bool> {
            self.validate_repository_binding(repo_key, repository, target)?;
            self.acquire_repo_lease(repo_key, owner_id, ttl_seconds)
        }

        pub(crate) fn validate_repository_binding(
            &self,
            repo_key: &str,
            repository: &Path,
            target: &str,
        ) -> Result<()> {
            let conn = self.connect_read_only()?;
            let repository_text = path_text(repository)?;
            let invalid: i64 = conn.query_row(
                "SELECT
                   (SELECT COUNT(*) FROM queue_items WHERE repo_key=?1 AND (repo_path!=?2 OR target_branch!=?3)) +
                   (SELECT COUNT(*) FROM queue_items WHERE repo_key!=?1 AND repo_path=?2 AND target_branch=?3) +
                   (SELECT COUNT(*) FROM registered_repositories WHERE repo_key=?1 AND (integration_path!=?4 OR target_branch!=?3)) +
                   (SELECT COUNT(*) FROM registered_repositories WHERE repo_key!=?1 AND integration_path=?4 AND target_branch=?3) +
                   (SELECT COUNT(*) FROM registered_remote_identities WHERE repo_key=?1 AND (integration_path!=?4 OR target_branch!=?3)) +
                   (SELECT COUNT(*) FROM registered_remote_identities WHERE repo_key!=?1 AND integration_path=?4 AND target_branch=?3) +
                   (SELECT COUNT(*) FROM workspace_roots WHERE repo_key=?1 AND source_path!=?2)",
                params![repo_key, repository_text, target, path_bytes(repository)],
                |row| row.get(0),
            )?;
            if invalid != 0 {
                anyhow::bail!(
                    "repository queue {repo_key} has {invalid} durable repository binding conflict(s)"
                );
            }
            Ok(())
        }

        pub fn heartbeat_repo_lease(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current_time = now();
            let changed = tx.execute(
                "UPDATE repo_leases SET heartbeat_at=?1,expires_at=?2 WHERE repo_key=?3 AND owner_id=?4 AND expires_at>?1",
                params![current_time, (Utc::now() + Duration::seconds(ttl_seconds)).to_rfc3339(), repo_key, owner_id],
            )?;
            tx.commit()?;
            Ok(changed == 1)
        }

        pub fn ensure_repo_lease_owner(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<bool> {
            self.heartbeat_repo_lease(repo_key, owner_id, ttl_seconds)
        }

        pub fn release_repo_lease(&self, repo_key: &str, owner_id: &str) -> Result<bool> {
            let conn = self.connect()?;
            Ok(conn.execute(
                "DELETE FROM repo_leases WHERE repo_key=?1 AND owner_id=?2",
                params![repo_key, owner_id],
            )? == 1)
        }

        pub fn register_workspace_root(
            &self,
            repo_key: &str,
            source_path: &Path,
            source_rift_id: &str,
            workspace_root: &Path,
            registry_identity: &str,
        ) -> Result<()> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing: Option<(String, String, String, String)> = tx
                .query_row(
                    "SELECT source_path,source_rift_id,workspace_root,registry_identity FROM workspace_roots WHERE repo_key=?1",
                    params![repo_key],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let expected = (
                source_path
                    .to_str()
                    .context("Rift source path is not valid UTF-8")?
                    .to_string(),
                source_rift_id.to_string(),
                workspace_root
                    .to_str()
                    .context("IQ workspace root is not valid UTF-8")?
                    .to_string(),
                registry_identity.to_string(),
            );
            match existing {
                Some(actual) if actual == expected => {}
                Some(_) => anyhow::bail!(
                    "repository queue {repo_key} workspace ownership differs from persisted state"
                ),
                None => {
                    tx.execute(
                        "INSERT INTO workspace_roots (repo_key,source_path,source_rift_id,workspace_root,registry_identity) VALUES (?1,?2,?3,?4,?5)",
                        params![repo_key, expected.0, expected.1, expected.2, expected.3],
                    )
                    .with_context(|| {
                        format!(
                            "register exclusive workspace root {} for {repo_key}",
                            workspace_root.display()
                        )
                    })?;
                }
            }
            tx.commit()?;
            Ok(())
        }

        pub fn verify_workspace_root_path(
            &self,
            repo_key: &str,
            workspace_root: &Path,
        ) -> Result<()> {
            let conn = self.connect_read_only()?;
            let existing: Option<String> = conn
                .query_row(
                    "SELECT workspace_root FROM workspace_roots WHERE repo_key=?1",
                    params![repo_key],
                    |row| row.get(0),
                )
                .optional()?;
            let expected = workspace_root
                .to_str()
                .context("IQ workspace root is not valid UTF-8")?;
            if existing.as_deref().is_some_and(|actual| actual != expected) {
                anyhow::bail!(
                    "repository queue {repo_key} workspace root differs from persisted state"
                );
            }
            Ok(())
        }

        pub fn workspace_root_generation(&self, repo_key: &str) -> Result<i64> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT generation FROM workspace_roots WHERE repo_key=?1",
                params![repo_key],
                |row| row.get(0),
            )
            .optional()
            .map(|generation| generation.unwrap_or(0))
            .context("read workspace root generation")
        }

        pub fn workspace_root_path(&self, repo_key: &str) -> Result<Option<PathBuf>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT workspace_root FROM workspace_roots WHERE repo_key=?1",
                params![repo_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map(|path| path.map(PathBuf::from))
            .map_err(Into::into)
        }

        pub fn advance_workspace_generation(&self, repo_key: &str) -> Result<i64> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE workspace_roots SET generation=generation+1 WHERE repo_key=?1",
                params![repo_key],
            )?;
            if changed != 1 {
                anyhow::bail!("repository queue {repo_key} has no registered workspace root");
            }
            let generation = tx.query_row(
                "SELECT generation FROM workspace_roots WHERE repo_key=?1",
                params![repo_key],
                |row| row.get(0),
            )?;
            tx.commit()?;
            Ok(generation)
        }

        pub fn database_id(&self) -> Result<String> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT value FROM queue_metadata WHERE key='database_id'",
                [],
                |row| row.get(0),
            )
            .context("read queue database identity")
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn record_workspace_gc_debt(&self, registry_identity: &str) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "INSERT OR IGNORE INTO workspace_gc_debt (registry_identity,created_at) VALUES (?1,?2)",
                params![registry_identity, now()],
            )?;
            Ok(())
        }

        pub fn clear_workspace_gc_debt(&self, registry_identity: &str) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "DELETE FROM workspace_gc_debt WHERE registry_identity=?1",
                params![registry_identity],
            )?;
            Ok(())
        }

        pub fn has_workspace_gc_debt(&self, registry_identity: &str) -> Result<bool> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_gc_debt WHERE registry_identity=?1)",
                params![registry_identity],
                |row| row.get(0),
            )
            .context("read workspace garbage-collection debt")
        }

        pub fn record_event(&self, item_id: &str, event_type: &str, message: &str) -> Result<()> {
            let conn = self.connect()?;
            self.record_event_with_conn(&conn, item_id, event_type, message)
        }

        pub(crate) fn record_event_if_status(
            &self,
            item_id: &str,
            expected_status: QueueStatus,
            event_type: &str,
            message: &str,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let status: String = required_row(
                tx.query_row(
                    "SELECT status FROM queue_items WHERE id=?1",
                    params![item_id],
                    |row| row.get(0),
                ),
                "queue item",
                item_id,
            )?;
            if status != expected_status.to_string() {
                tx.commit()?;
                return Ok(false);
            }
            Self::record_event_tx(&tx, item_id, event_type, message)?;
            tx.commit()?;
            Ok(true)
        }

        pub fn events(&self, item_id: &str) -> Result<Vec<QueueEvent>> {
            self.reader().events(item_id)
        }

        pub fn latest_answered_prompt(
            &self,
            item_id: &str,
            attempt_id: Option<&str>,
        ) -> Result<Option<Prompt>> {
            let conn = self.connect_read_only()?;
            let mut sql = String::from(
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE item_id=?1 AND status='answered'",
            );
            if attempt_id.is_some() {
                sql.push_str(" AND attempt_id=?2");
            }
            sql.push_str(" ORDER BY answered_at DESC LIMIT 1");
            if let Some(attempt_id) = attempt_id {
                conn.query_row(&sql, params![item_id, attempt_id], map_prompt)
                    .optional()
            } else {
                conn.query_row(&sql, params![item_id], map_prompt)
                    .optional()
            }
            .with_context(|| format!("read latest answered prompt for item {item_id}"))
        }

        pub fn prompts_for_item(&self, item_id: &str) -> Result<Vec<Prompt>> {
            self.reader().prompts_for_item(item_id)
        }

        pub fn get_attempt(&self, attempt_id: &str) -> Result<Attempt> {
            self.reader().get_attempt(attempt_id)
        }

        pub fn update_attempt_base(&self, attempt_id: &str, target_base_sha: &str) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE integration_attempts SET target_base_sha=?1 WHERE id=?2",
                params![target_base_sha, attempt_id],
            )?;
            Ok(())
        }

        pub fn update_attempt_merge(&self, attempt_id: &str, merge_commit_sha: &str) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE integration_attempts SET merge_commit_sha=?1 WHERE id=?2",
                params![merge_commit_sha, attempt_id],
            )?;
            Ok(())
        }

        pub fn update_attempt_validation(
            &self,
            attempt_id: &str,
            command: &str,
            exit_code: i64,
            log_path: &str,
            validated_commit_sha: Option<&str>,
        ) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE integration_attempts SET validation_command=?1,validation_exit_code=?2,validation_log_path=?3,validated_commit_sha=?4 WHERE id=?5",
                params![command, exit_code, log_path, validated_commit_sha, attempt_id],
            )?;
            Ok(())
        }

        pub fn update_attempt_revalidation(
            &self,
            attempt_id: &str,
            target_base_sha: &str,
            command: &str,
            exit_code: i64,
            log_path: &str,
            validated_commit_sha: Option<&str>,
        ) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE integration_attempts SET target_base_sha=?1,validation_command=?2,validation_exit_code=?3,validation_log_path=?4,validated_commit_sha=?5 WHERE id=?6",
                params![target_base_sha, command, exit_code, log_path, validated_commit_sha, attempt_id],
            )?;
            Ok(())
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) fn accept_candidate_without_validation(
            &self,
            item_id: &str,
            attempt_id: &str,
            target_base_sha: &str,
            candidate_sha: &str,
            expected_status: QueueStatus,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<()> {
            for (value, label) in [
                (target_base_sha, "no-validation target"),
                (candidate_sha, "no-validation candidate"),
            ] {
                if !matches!(value.len(), 40 | 64)
                    || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    anyhow::bail!("{label} must be a full hexadecimal Git object ID");
                }
            }
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (item_status, current_attempt_id, item_repo_key): (String, Option<String>, String) =
                required_row(
                    tx.query_row(
                        "SELECT status,current_attempt_id,repo_key FROM queue_items WHERE id=?1",
                        params![item_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    ),
                    "queue item",
                    item_id,
                )?;
            Self::require_mutation_authority(
                &tx,
                &item_repo_key,
                MutationAuthority::RepositoryLease { repo_key, owner_id },
            )?;
            if item_status != expected_status.to_string()
                || current_attempt_id.as_deref() != Some(attempt_id)
            {
                anyhow::bail!(
                    "no-validation acceptance does not match the current item attempt and status"
                );
            }
            let attempt = required_row(
                tx.query_row(
                    "SELECT item_id,target_base_sha,merge_commit_sha,validated_commit_sha,validation_command,validation_exit_code,validation_log_path,policy_snapshot_json,policy_digest,signoff_evidence_json FROM integration_attempts WHERE id=?1",
                    params![attempt_id],
                    |row| Ok(NoValidationAttemptRow {
                        item_id: row.get(0)?,
                        target_base_sha: row.get(1)?,
                        merge_commit_sha: row.get(2)?,
                        validated_commit_sha: row.get(3)?,
                        validation_command: row.get(4)?,
                        validation_exit_code: row.get(5)?,
                        validation_log_path: row.get(6)?,
                        policy_snapshot_json: row.get(7)?,
                        policy_digest: row.get(8)?,
                        signoff_evidence_json: row.get(9)?,
                    }),
                ),
                "integration attempt",
                attempt_id,
            )?;
            if attempt.item_id != item_id
                || attempt.target_base_sha.as_deref() != Some(target_base_sha)
                || attempt.merge_commit_sha.as_deref() != Some(candidate_sha)
            {
                anyhow::bail!(
                    "no-validation acceptance does not match the persisted target and candidate"
                );
            }
            let snapshot = attempt
                .policy_snapshot_json
                .as_deref()
                .context("no-validation attempt has no persisted policy snapshot")?;
            let digest = attempt
                .policy_digest
                .as_deref()
                .context("no-validation attempt has no persisted policy digest")?;
            let policy = crate::composition::verify_policy_snapshot(snapshot, digest)?;
            if policy.policy != crate::composition::ValidationPolicy::None {
                anyhow::bail!("attempt policy requires validation");
            }
            if attempt.validated_commit_sha.as_deref() == Some(candidate_sha)
                && attempt.validation_command.is_none()
                && attempt.validation_exit_code.is_none()
                && attempt.validation_log_path.is_none()
                && attempt.signoff_evidence_json.is_none()
            {
                tx.commit()?;
                return Ok(());
            }
            if attempt.validated_commit_sha.is_some()
                || attempt.validation_command.is_some()
                || attempt.validation_exit_code.is_some()
                || attempt.validation_log_path.is_some()
                || attempt.signoff_evidence_json.is_some()
            {
                anyhow::bail!("attempt already has different candidate evidence");
            }
            let changed = tx.execute(
                "UPDATE integration_attempts SET validated_commit_sha=?1 WHERE id=?2 AND item_id=?3 AND target_base_sha=?4 AND merge_commit_sha=?1",
                params![candidate_sha,attempt_id,item_id,target_base_sha],
            )?;
            if changed != 1 {
                anyhow::bail!(
                    "integration attempt identity changed during no-validation acceptance"
                );
            }
            Self::record_event_tx(
                &tx,
                item_id,
                "validation_skipped",
                &format!("validation skipped for exact candidate {candidate_sha}"),
            )?;
            Self::record_event_tx(
                &tx,
                item_id,
                "signoff_not_required",
                &format!("signoff not required for exact candidate {candidate_sha}"),
            )?;
            tx.commit()?;
            Ok(())
        }

        pub fn update_attempt_signoff(&self, attempt_id: &str, evidence: &Value) -> Result<()> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE integration_attempts SET signoff_evidence_json=?1 WHERE id=?2",
                params![evidence.to_string(), attempt_id],
            )?;
            if changed != 1 {
                anyhow::bail!("integration attempt disappeared during signoff");
            }
            Ok(())
        }

        pub fn set_workspace_intent(&self, item_id: &str, path: &str) -> Result<()> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE queue_items SET integration_workspace_path=?1,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,integration_workspace_cleaned_at=NULL,updated_at=?2 WHERE id=?3 AND status='merging'",
                params![path, now(), item_id],
            )?;
            if changed != 1 {
                anyhow::bail!("item {item_id} is no longer merging; refusing workspace creation");
            }
            Ok(())
        }

        pub fn begin_workspace_creation(
            &self,
            repo_key: &str,
            item_id: &str,
            path: &str,
        ) -> Result<i64> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let root_changed = tx.execute(
                "UPDATE workspace_roots SET generation=generation+1 WHERE repo_key=?1",
                params![repo_key],
            )?;
            if root_changed != 1 {
                anyhow::bail!("repository queue {repo_key} has no registered workspace root");
            }
            let generation: i64 = tx.query_row(
                "SELECT generation FROM workspace_roots WHERE repo_key=?1",
                params![repo_key],
                |row| row.get(0),
            )?;
            let item_changed = tx.execute(
                "UPDATE queue_items SET integration_workspace_path=?1,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,integration_workspace_cleaned_at=NULL,updated_at=?2 WHERE id=?3 AND repo_key=?4 AND status='merging'",
                params![path, now(), item_id, repo_key],
            )?;
            if item_changed != 1 {
                anyhow::bail!("item {item_id} is no longer merging; refusing workspace creation");
            }
            tx.commit()?;
            Ok(generation)
        }

        pub fn set_workspace_identity(
            &self,
            item_id: &str,
            path: &str,
            rift_id: &str,
            source_rift_id: &str,
        ) -> Result<()> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE queue_items SET integration_workspace_rift_id=?1,integration_workspace_source_rift_id=?2,updated_at=?3 WHERE id=?4 AND status='merging' AND integration_workspace_path=?5",
                params![rift_id, source_rift_id, now(), item_id, path],
            )?;
            if changed != 1 {
                anyhow::bail!(
                    "item {item_id} workspace intent changed before Rift identity was persisted"
                );
            }
            Ok(())
        }

        pub fn mark_workspace_cleaned(&self, item_id: &str) -> Result<()> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE queue_items SET integration_workspace_path=NULL,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,integration_workspace_cleaned_at=?1,updated_at=?1 WHERE id=?2 AND status IN ('integrated','cancelled') AND integration_workspace_cleaned_at IS NULL",
                params![now(), item_id],
            )?;
            if changed == 1 {
                Self::record_event_tx(
                    &tx,
                    item_id,
                    "workspace_cleaned",
                    "removed terminal Rift workspace and reclaimed Rift trash",
                )?;
            }
            tx.commit()?;
            Ok(())
        }

        fn composition_transaction<T>(
            &self,
            repo_key: &str,
            owner_id: &str,
            operation: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T>,
        ) -> Result<T> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current_time = now();
            let changed = tx.execute(
                "UPDATE repo_leases SET heartbeat_at=?1,expires_at=?2 WHERE repo_key=?3 AND owner_id=?4 AND expires_at>?1",
                params![current_time, (Utc::now() + Duration::seconds(30)).to_rfc3339(), repo_key, owner_id],
            )?;
            if changed != 1 {
                anyhow::bail!("repo queue {repo_key} composition lease is not owned by {owner_id}");
            }
            let result = operation(&tx)?;
            tx.commit()?;
            Ok(result)
        }

        pub fn repository(&self, repo_key: &str) -> Result<RegisteredRepository> {
            let conn = self.connect_read_only()?;
            required_row(
                conn.query_row(
                    &format!("{REGISTERED_REPOSITORY_SELECT} WHERE repository.repo_key=?1"),
                    params![repo_key],
                    map_repository,
                ),
                "registered repository",
                repo_key,
            )
        }

        pub fn repository_if_exists(&self, repo_key: &str) -> Result<Option<RegisteredRepository>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                &format!("{REGISTERED_REPOSITORY_SELECT} WHERE repository.repo_key=?1"),
                params![repo_key],
                map_repository,
            )
            .optional()
            .map_err(Into::into)
        }

        pub fn repository_for_integration_path(
            &self,
            path: &Path,
        ) -> Result<Option<RegisteredRepository>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                &format!("{REGISTERED_REPOSITORY_SELECT} WHERE repository.integration_path=?1"),
                params![path_bytes(path)],
                map_repository,
            )
            .optional()
            .map_err(Into::into)
        }

        pub fn list_repositories(&self) -> Result<Vec<RegisteredRepository>> {
            let conn = self.connect_read_only()?;
            let mut statement = conn.prepare(&format!(
                "{REGISTERED_REPOSITORY_SELECT} ORDER BY repository.repo_key"
            ))?;
            let repositories = statement
                .query_map([], map_repository)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(repositories)
        }

        pub(crate) fn registered_remote_identity(
            &self,
            repo_key: &str,
        ) -> Result<Option<(PathBuf, String, RegisteredRemote)>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT integration_path,target_branch,remote_name,fetch_url,push_url FROM registered_remote_identities WHERE repo_key=?1",
                params![repo_key],
                |row| {
                    Ok((
                        row_path(row, "integration_path")?,
                        row.get("target_branch")?,
                        RegisteredRemote {
                            name: row.get("remote_name")?,
                            fetch_url: row.get("fetch_url")?,
                            push_url: row.get("push_url")?,
                        },
                    ))
                },
            )
            .optional()
            .map_err(Into::into)
        }

        pub(crate) fn save_registered_remote_intent(
            &self,
            repo_key: &str,
            owner_id: &str,
            integration_path: &Path,
            target_branch: &str,
            remote: &RegisteredRemote,
        ) -> Result<()> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                tx.execute(
                    "INSERT OR IGNORE INTO registered_remote_identities(repo_key,integration_path,target_branch,remote_name,fetch_url,push_url,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![repo_key,path_bytes(integration_path),target_branch,remote.name,remote.fetch_url,remote.push_url,now()],
                )?;
                let persisted: (Vec<u8>, String, String, String, String) = tx.query_row(
                    "SELECT integration_path,target_branch,remote_name,fetch_url,push_url FROM registered_remote_identities WHERE repo_key=?1",
                    params![repo_key],
                    |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
                )?;
                if persisted
                    != (
                        path_bytes(integration_path),
                        target_branch.to_string(),
                        remote.name.clone(),
                        remote.fetch_url.clone(),
                        remote.push_url.clone(),
                    )
                {
                    anyhow::bail!("registered remote identity intent differs from the current repository remote");
                }
                Ok(())
            })
        }

        pub fn save_repository_intent(
            &self,
            owner_id: &str,
            repository: &RegisteredRepository,
        ) -> Result<()> {
            self.composition_transaction(&repository.key, owner_id, |tx| {
                let existing = tx
                    .query_row(
                        &format!("{REGISTERED_REPOSITORY_SELECT} WHERE repository.repo_key=?1"),
                        params![repository.key],
                        map_repository,
                    )
                    .optional()?;
                if let Some(existing) = existing {
                    if existing != *repository {
                        anyhow::bail!("repository {} is already registered with different durable state", repository.key);
                    }
                    return Ok(());
                }
                tx.execute(
                    "INSERT INTO registered_repositories (repo_key,integration_path,target_branch,remote,seed_path,seed_rift_id,seed_source_rift_id,workspace_root,checkout_reconciliation_json,seed_refresh_json,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,NULL,NULL,?6,?7,?8,?9,?9)",
                    params![repository.key,path_bytes(&repository.integration_path),repository.target_branch,repository.remote.name,path_bytes(Path::new(repository.seed.path().context("repository seed intent has no path")?)),path_bytes(&repository.workspace_root),serde_json::to_string(&repository.checkout_reconciliation)?,serde_json::to_string(&repository.seed_refresh)?,repository.created_at],
                )?;
                Ok(())
            })
        }

        pub fn set_repository_seed_identity(
            &self,
            repo_key: &str,
            owner_id: &str,
            identity: &WorkspaceIdentity,
        ) -> Result<RegisteredRepository> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = tx.execute(
                    "UPDATE registered_repositories SET seed_path=?1,seed_rift_id=?2,seed_source_rift_id=?3,updated_at=?4 WHERE repo_key=?5 AND seed_path=?1",
                    params![path_bytes(Path::new(&identity.path)),identity.rift_id,identity.source_rift_id,now(),repo_key],
                )?;
                if changed != 1 { anyhow::bail!("repository seed creation intent changed"); }
                Ok(())
            })?;
            self.repository(repo_key)
        }

        pub fn update_seed_refresh(
            &self,
            repo_key: &str,
            owner_id: &str,
            state: &SeedRefreshState,
        ) -> Result<()> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = tx.execute(
                    "UPDATE registered_repositories SET seed_refresh_json=?1,updated_at=?2 WHERE repo_key=?3",
                    params![serde_json::to_string(state)?,now(),repo_key],
                )?;
                if changed != 1 { anyhow::bail!("registered repository disappeared"); }
                Ok(())
            })
        }

        pub fn update_checkout_reconciliation(
            &self,
            repo_key: &str,
            owner_id: &str,
            state: &CheckoutReconciliationState,
        ) -> Result<()> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = tx.execute(
                    "UPDATE registered_repositories SET checkout_reconciliation_json=?1,updated_at=?2 WHERE repo_key=?3",
                    params![serde_json::to_string(state)?,now(),repo_key],
                )?;
                if changed != 1 { anyhow::bail!("registered repository disappeared"); }
                Ok(())
            })
        }

        pub fn refresh_registered_target(
            &self,
            repo_key: &str,
            owner_id: &str,
            target_sha: &str,
        ) -> Result<()> {
            let checkout = CheckoutReconciliationState::Ready {
                target_sha: target_sha.to_string(),
            };
            let seed = SeedRefreshState::Pending {
                target_sha: target_sha.to_string(),
            };
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = tx.execute(
                    "UPDATE registered_repositories SET checkout_reconciliation_json=?1,seed_refresh_json=?2,updated_at=?3 WHERE repo_key=?4",
                    params![serde_json::to_string(&checkout)?,serde_json::to_string(&seed)?,now(),repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("registered repository disappeared during target refresh");
                }
                Ok(())
            })
        }

        pub fn workspace(&self, id: &str) -> Result<DevelopmentWorkspace> {
            let conn = self.connect_read_only()?;
            required_row(
                conn.query_row(
                    "SELECT * FROM development_workspaces WHERE id=?1",
                    params![id],
                    map_development_workspace,
                ),
                "development workspace",
                id,
            )
        }

        pub fn list_development_workspaces(
            &self,
            repo_key: Option<&str>,
        ) -> Result<Vec<DevelopmentWorkspace>> {
            let conn = self.connect_read_only()?;
            let mut statement = if repo_key.is_some() {
                conn.prepare(
                    "SELECT * FROM development_workspaces WHERE repo_key=?1 ORDER BY created_at,id",
                )?
            } else {
                conn.prepare("SELECT * FROM development_workspaces ORDER BY created_at,id")?
            };
            if let Some(repo_key) = repo_key {
                statement
                    .query_map(params![repo_key], map_development_workspace)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            } else {
                statement
                    .query_map([], map_development_workspace)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
        }

        pub fn save_development_workspace(
            &self,
            owner_id: &str,
            workspace: &DevelopmentWorkspace,
        ) -> Result<()> {
            self.composition_transaction(&workspace.repo_key, owner_id, |tx| {
                tx.execute(
                    "INSERT INTO development_workspaces (id,repo_key,name,path,rift_id,source_rift_id,branch,base_sha,status,cleanup_json,created_at,updated_at) VALUES (?1,?2,?3,?4,NULL,NULL,?5,?6,?7,?8,?9,?9)",
                    params![workspace.id,workspace.repo_key,workspace.name,path_bytes(&workspace.path),workspace.branch,workspace.base_sha,workspace.status.to_string(),serde_json::to_string(&workspace.cleanup)?,workspace.created_at],
                )?;
                Ok(())
            })
        }

        pub fn set_development_workspace_identity(
            &self,
            repo_key: &str,
            owner_id: &str,
            id: &str,
            identity: &WorkspaceIdentity,
        ) -> Result<DevelopmentWorkspace> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = tx.execute(
                    "UPDATE development_workspaces SET rift_id=?1,source_rift_id=?2,status='active',updated_at=?3 WHERE id=?4 AND repo_key=?5 AND status='creating' AND path=?6",
                    params![identity.rift_id,identity.source_rift_id,now(),id,repo_key,path_bytes(Path::new(&identity.path))],
                )?;
                if changed != 1 { anyhow::bail!("development workspace creation intent changed"); }
                Ok(())
            })?;
            self.workspace(id)
        }

        pub fn update_development_workspace_cleanup(
            &self,
            repo_key: &str,
            owner_id: &str,
            id: &str,
            status: DevelopmentWorkspaceStatus,
            cleanup: &CleanupState,
        ) -> Result<DevelopmentWorkspace> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = tx.execute(
                    "UPDATE development_workspaces SET status=?1,cleanup_json=?2,updated_at=?3 WHERE id=?4 AND repo_key=?5",
                    params![status.to_string(),serde_json::to_string(cleanup)?,now(),id,repo_key],
                )?;
                if changed != 1 { anyhow::bail!("development workspace disappeared"); }
                Ok(())
            })?;
            self.workspace(id)
        }

        pub fn complete_development_workspace_cleanup(
            &self,
            repo_key: &str,
            owner_id: &str,
            id: &str,
            registry_identity: &str,
        ) -> Result<DevelopmentWorkspace> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = tx.execute(
                    "UPDATE development_workspaces SET status='removed',cleanup_json=?1,updated_at=?2 WHERE id=?3 AND repo_key=?4 AND status IN ('cleanup_pending','cleanup_failed')",
                    params![serde_json::to_string(&CleanupState::Complete { completed_at: now() })?,now(),id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("development workspace cleanup authority changed");
                }
                tx.execute(
                    "DELETE FROM workspace_gc_debt WHERE registry_identity=?1",
                    params![registry_identity],
                )?;
                Ok(())
            })?;
            self.workspace(id)
        }

        pub fn begin_local_submission(
            &self,
            repo_key: &str,
            owner_id: &str,
            workspace_id: &str,
            commit_sha: &str,
            replaces_item_id: Option<&str>,
        ) -> Result<LocalSubmission> {
            let mut submission_id = None;
            self.composition_transaction(repo_key, owner_id, |tx| {
                let existing = tx
                    .query_row(
                        "SELECT * FROM local_submissions WHERE workspace_id=?1 AND state='creating'",
                        params![workspace_id],
                        map_local_submission,
                    )
                    .optional()?;
                if let Some(existing) = existing {
                    if existing.repo_key != repo_key
                        || existing.commit_sha != commit_sha
                        || existing.replaces_item_id.as_deref() != replaces_item_id
                    {
                        anyhow::bail!(
                            "workspace has a different incomplete immutable submission intent"
                        );
                    }
                    submission_id = Some(existing.id);
                    return Ok(());
                }
                let workspace = required_row(
                    tx.query_row(
                        "SELECT * FROM development_workspaces WHERE id=?1",
                        params![workspace_id],
                        map_development_workspace,
                    ),
                    "development workspace",
                    workspace_id,
                )?;
                let expected_status = if replaces_item_id.is_some() {
                    DevelopmentWorkspaceStatus::Submitted
                } else {
                    DevelopmentWorkspaceStatus::Active
                };
                if workspace.repo_key != repo_key || workspace.status != expected_status {
                    anyhow::bail!("development workspace is not in the required submission state");
                }
                if let Some(item_id) = replaces_item_id {
                    let item = required_row(
                        tx.query_row(
                            "SELECT * FROM queue_items WHERE id=?1",
                            params![item_id],
                            map_item,
                        ),
                        "queue item",
                        item_id,
                    )?;
                    let old_submission_id = match &item.source {
                        QueueSource::LocalSubmission { submission_id, .. } => submission_id,
                        QueueSource::RemoteBranch { .. } => {
                            anyhow::bail!("replacement item is not a local submission")
                        }
                    };
                    let old_workspace_id: String = tx.query_row(
                        "SELECT workspace_id FROM local_submissions WHERE id=?1",
                        params![old_submission_id],
                        |row| row.get(0),
                    )?;
                    if item.repo_key != repo_key
                        || item.status != QueueStatus::Blocked
                        || item.blocked_reason != Some(BlockedReason::NeedsAgentFix)
                        || old_workspace_id != workspace_id
                    {
                        anyhow::bail!("replacement requires the original needs-agent-fix local submission");
                    }
                }
                let id = Uuid::new_v4().to_string();
                let item_id = replaces_item_id
                    .map(str::to_string)
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let private_ref = format!("refs/iq/submissions/{id}");
                let staging_ref = format!("refs/iq/staging/{id}");
                tx.execute(
                    "INSERT INTO local_submissions (id,queue_item_id,repo_key,workspace_id,base_sha,commit_sha,private_ref,staging_ref,replaces_item_id,state,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'creating',?10)",
                    params![id,item_id,repo_key,workspace_id,workspace.base_sha,commit_sha,private_ref,staging_ref,replaces_item_id,now()],
                )?;
                submission_id = Some(id);
                Ok(())
            })?;
            self.local_submission(
                submission_id
                    .as_deref()
                    .context("local submission intent did not produce an identity")?,
            )
        }

        pub fn finalize_local_submission(
            &self,
            repo_key: &str,
            owner_id: &str,
            submission_id: &str,
            producer_metadata: &Value,
            state_repository: &crate::control_domain::StateRepositorySnapshot,
        ) -> Result<(LocalSubmission, QueueItem)> {
            let state_repository = state_repository.clone().validate()?;
            let mut queue_item_id = None;
            self.composition_transaction(repo_key, owner_id, |tx| {
                let submission = required_row(
                    tx.query_row(
                        "SELECT * FROM local_submissions WHERE id=?1",
                        params![submission_id],
                        map_local_submission,
                    ),
                    "local submission",
                    submission_id,
                )?;
                if submission.repo_key != repo_key
                    || submission.state != LocalSubmissionState::Creating
                {
                    anyhow::bail!("local submission is not an exact creation intent");
                }
                let repository = required_row(
                    tx.query_row(
                        &format!(
                            "{REGISTERED_REPOSITORY_SELECT} WHERE repository.repo_key=?1"
                        ),
                        params![repo_key],
                        map_repository,
                    ),
                    "registered repository",
                    repo_key,
                )?;
                let timestamp = now();
                if let Some(item_id) = submission.replaces_item_id.as_deref() {
                    let item = required_row(
                        tx.query_row(
                            "SELECT * FROM queue_items WHERE id=?1",
                            params![item_id],
                            map_item,
                        ),
                        "queue item",
                        item_id,
                    )?;
                    let old_submission_id = match &item.source {
                        QueueSource::LocalSubmission { submission_id, .. } => submission_id,
                        QueueSource::RemoteBranch { .. } => {
                            anyhow::bail!("replacement item is not a local submission")
                        }
                    };
                    if item.repo_key != repo_key
                        || item.status != QueueStatus::Blocked
                        || item.blocked_reason != Some(BlockedReason::NeedsAgentFix)
                    {
                        anyhow::bail!("local replacement target changed before finalization");
                    }
                    let old_attempt_id = item
                        .current_attempt_id
                        .as_deref()
                        .context("replacement target has no active integration attempt")?;
                    let replacement = match &item.workspace {
                        WorkspaceState::Retained { identity } => ReplacementState::CleanupPending {
                            old_attempt_id: old_attempt_id.to_string(),
                            old_workspace: identity.clone(),
                        },
                        WorkspaceState::CreationIntent { .. } => anyhow::bail!("replacement cannot discard an incomplete integration workspace"),
                        _ => ReplacementState::None,
                    };
                    let (status, current_attempt, workspace_path, workspace_rift, workspace_source, replacement_json) = match &replacement {
                        ReplacementState::None => ("ready", None, None, None, None, None),
                        ReplacementState::CleanupPending { .. } => (
                            "blocked",
                            item.current_attempt_id.as_deref(),
                            item.workspace.path(),
                            item.workspace.identity().map(|identity| identity.rift_id.as_str()),
                            item.workspace.identity().map(|identity| identity.source_rift_id.as_str()),
                            Some(serde_json::to_string(&replacement)?),
                        ),
                    };
                    let changed = tx.execute(
                        r#"UPDATE queue_items SET source_branch=?1,source_ref=?1,submission_id=?2,current_head_sha=?3,repo_path=?4,target_branch=?5,status=?6,current_attempt_id=?7,integration_workspace_path=?8,integration_workspace_rift_id=?9,integration_workspace_source_rift_id=?10,replacement_json=?11,conflict_json=NULL,target_sha=NULL,source_sha=NULL,validation_evidence_json='{}',updated_at=?12 WHERE id=?13 AND repo_key=?14 AND status='blocked'"#,
                        params![submission.private_ref,submission.id,submission.commit_sha,path_text(&repository.integration_path)?,repository.target_branch,status,current_attempt,workspace_path,workspace_rift,workspace_source,replacement_json,timestamp,item_id,repo_key],
                    )?;
                    if changed != 1 { anyhow::bail!("local replacement state changed concurrently"); }
                    let changed = tx.execute(
                        "UPDATE local_submissions SET state='replaced' WHERE id=?1 AND state='queued'",
                        params![old_submission_id],
                    )?;
                    if changed != 1 { anyhow::bail!("old local submission is not queued"); }
                    if matches!(replacement, ReplacementState::None) {
                        let changed = tx.execute(
                            "UPDATE integration_attempts SET result='superseded',finished_at=?1 WHERE id=?2 AND item_id=?3 AND finished_at IS NULL AND result IS NULL",
                            params![timestamp,old_attempt_id,item_id],
                        )?;
                        if changed != 1 { anyhow::bail!("old integration attempt cannot be superseded"); }
                    }
                    tx.execute("UPDATE prompts SET status='superseded' WHERE item_id=?1 AND status='open'",params![item_id])?;
                    Self::record_event_tx(tx,item_id,"local_submission_replaced",if matches!(replacement, ReplacementState::None) { "immutable local submission replaced" } else { "immutable local submission replaced; old integration Rift cleanup is pending" })?;
                } else {
                    tx.execute(
                        "INSERT INTO queue_items (id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,source_kind,source_ref,submission_id,landing_policy,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,NULL,?6,'{}','ready',?7,'local_submission',?4,?8,'squash',?9,?9)",
                        params![submission.queue_item_id,repo_key,path_text(&repository.integration_path)?,submission.private_ref,repository.target_branch,producer_metadata.to_string(),submission.commit_sha,submission.id,timestamp],
                    )?;
                    let changed = tx.execute(
                        "UPDATE development_workspaces SET status='submitted',updated_at=?1 WHERE id=?2 AND repo_key=?3 AND status='active'",
                        params![timestamp,submission.workspace_id,repo_key],
                    )?;
                    if changed != 1 { anyhow::bail!("development workspace is not active"); }
                    Self::record_event_tx(tx,&submission.queue_item_id,"item_enqueued","immutable local submission enqueued")?;
                    insert_state_repository_binding(
                        tx,
                        &submission.queue_item_id,
                        &state_repository,
                        &timestamp,
                    )?;
                }
                let changed = tx.execute(
                    "UPDATE local_submissions SET state='queued' WHERE id=?1 AND state='creating'",
                    params![submission.id],
                )?;
                if changed != 1 { anyhow::bail!("local submission intent changed before finalization"); }
                queue_item_id = Some(submission.queue_item_id);
                Ok(())
            })?;
            let queue_item_id = queue_item_id
                .context("local submission finalization did not produce a queue item identity")?;
            Ok((
                self.local_submission(submission_id)?,
                self.get_item(&queue_item_id)?,
            ))
        }

        pub fn finish_replacement_cleanup(
            &self,
            repo_key: &str,
            owner_id: &str,
            item_id: &str,
            old_attempt_id: &str,
        ) -> Result<QueueItem> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let item = required_row(tx.query_row("SELECT * FROM queue_items WHERE id=?1",params![item_id],map_item),"queue item",item_id)?;
                let ReplacementState::CleanupPending { old_attempt_id: expected, .. } = &item.replacement else {
                    anyhow::bail!("queue item has no replacement cleanup debt");
                };
                if expected != old_attempt_id { anyhow::bail!("replacement cleanup attempt identity changed"); }
                let changed = tx.execute("UPDATE integration_attempts SET result='superseded',finished_at=?1 WHERE id=?2 AND item_id=?3 AND finished_at IS NULL AND result IS NULL",params![now(),old_attempt_id,item_id])?;
                if changed != 1 { anyhow::bail!("old integration attempt cannot be superseded after cleanup"); }
                let changed = tx.execute(
                    "UPDATE queue_items SET status='ready',current_attempt_id=NULL,integration_workspace_path=NULL,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,replacement_json=NULL,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?1 WHERE id=?2 AND repo_key=?3 AND status='blocked'",
                    params![now(),item_id,repo_key],
                )?;
                if changed != 1 { anyhow::bail!("replacement cleanup state changed concurrently"); }
                tx.execute("UPDATE prompts SET status='superseded' WHERE item_id=?1 AND status='open'",params![item_id])?;
                Self::record_event_tx(tx,item_id,"local_submission_replacement_cleanup_complete","old integration Rift cleanup completed")?;
                Ok(())
            })?;
            self.get_item(item_id)
        }

        pub fn local_submission(&self, id: &str) -> Result<LocalSubmission> {
            let conn = self.connect_read_only()?;
            required_row(
                conn.query_row(
                    "SELECT * FROM local_submissions WHERE id=?1",
                    params![id],
                    map_local_submission,
                ),
                "local submission",
                id,
            )
        }

        pub fn creating_local_submission(
            &self,
            workspace_id: &str,
        ) -> Result<Option<LocalSubmission>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT * FROM local_submissions WHERE workspace_id=?1 AND state='creating'",
                params![workspace_id],
                map_local_submission,
            )
            .optional()
            .map_err(Into::into)
        }

        pub fn integrated_submission_sha(&self, workspace_id: &str) -> Result<Option<String>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT ls.commit_sha FROM local_submissions ls JOIN queue_items qi ON qi.submission_id=ls.id WHERE ls.workspace_id=?1 AND qi.status='integrated' ORDER BY ls.created_at DESC LIMIT 1",
                params![workspace_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
        }

        pub fn set_conflict_metadata(
            &self,
            item_id: &str,
            conflict_json: &Value,
            target_sha: &str,
            source_sha: &str,
        ) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE queue_items SET conflict_json=?1,target_sha=?2,source_sha=?3,updated_at=?4 WHERE id=?5",
                params![conflict_json.to_string(), target_sha, source_sha, now(), item_id],
            )?;
            Ok(())
        }

        pub fn clear_conflict_metadata(&self, item_id: &str) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE queue_items SET conflict_json=NULL,target_sha=NULL,source_sha=NULL,updated_at=?1 WHERE id=?2",
                params![now(), item_id],
            )?;
            Ok(())
        }

        pub fn get_prompt(&self, prompt_id: &str) -> Result<Prompt> {
            self.reader().get_prompt(prompt_id)
        }

        fn record_event_with_conn(
            &self,
            conn: &Connection,
            item_id: &str,
            event_type: &str,
            message: &str,
        ) -> Result<()> {
            conn.execute(
                "INSERT INTO queue_events (id,item_id,event_type,message,created_at) VALUES (?1,?2,?3,?4,?5)",
                params![Uuid::new_v4().to_string(), item_id, event_type, message, now()],
            )?;
            Ok(())
        }

        fn record_event_tx(
            tx: &rusqlite::Transaction<'_>,
            item_id: &str,
            event_type: &str,
            message: &str,
        ) -> Result<()> {
            tx.execute(
                "INSERT INTO queue_events (id,item_id,event_type,message,created_at) VALUES (?1,?2,?3,?4,?5)",
                params![Uuid::new_v4().to_string(), item_id, event_type, message, now()],
            )?;
            Ok(())
        }
    }

    impl SqliteQueueReader {
        const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        const COMMAND_AUTHORITY_RESERVE: Duration = Duration::milliseconds(100);

        pub fn open(path: &Path) -> Result<Self> {
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            };
            let original_metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect queue db {}", path.display()))?;
            if original_metadata.file_type().is_symlink() || !original_metadata.is_file() {
                anyhow::bail!("queue database must be a regular file: {}", path.display());
            }
            let path = path
                .canonicalize()
                .with_context(|| format!("resolve existing queue db {}", path.display()))?;
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect queue db {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                anyhow::bail!("queue database must be a regular file: {}", path.display());
            }
            let reader = Self {
                path: path.clone(),
                database_dev: metadata.dev(),
                database_ino: metadata.ino(),
            };
            let conn = reader
                .connect(Self::BUSY_TIMEOUT)
                .with_context(|| format!("open existing queue db {}", path.display()))?;
            let metadata_exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='queue_metadata')",
                [],
                |row| row.get(0),
            )?;
            if !metadata_exists {
                anyhow::bail!("existing IQ database has no standalone schema identity");
            }
            let workspace_schema_version: Option<String> = conn
                .query_row(
                    "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            require_current_schema_version(workspace_schema_version.as_deref())?;
            let columns = {
                let mut statement = conn.prepare("PRAGMA table_info(queue_items)")?;
                let columns = statement
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                columns
            };
            if !columns.iter().any(|column| column == "landing_state_json")
                || !columns
                    .iter()
                    .any(|column| column == "integration_workspace_rift_id")
                || !columns
                    .iter()
                    .any(|column| column == "integration_workspace_source_rift_id")
                || !columns
                    .iter()
                    .any(|column| column == "integration_workspace_cleaned_at")
            {
                anyhow::bail!("IQ schema version 9 is missing required queue columns");
            }
            Ok(reader)
        }

        fn connect(&self, timeout: std::time::Duration) -> Result<Connection> {
            self.verify_database_file()?;
            let conn = Connection::open_with_flags(
                &self.path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )
            .with_context(|| format!("open queue db for reading {}", self.path.display()))?;
            self.verify_database_file()?;
            conn.busy_timeout(timeout)?;
            conn.pragma_update(None, "query_only", "ON")?;
            Ok(conn)
        }

        fn verify_database_file(&self) -> Result<()> {
            let metadata = fs::symlink_metadata(&self.path)
                .with_context(|| format!("inspect queue db {}", self.path.display()))?;
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || metadata.dev() != self.database_dev
                || metadata.ino() != self.database_ino
            {
                anyhow::bail!(
                    "queue database identity changed while IQ was running: {}",
                    self.path.display()
                );
            }
            Ok(())
        }

        pub fn list_items(&self) -> Result<Vec<QueueItem>> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            let mut stmt = conn.prepare("SELECT * FROM queue_items ORDER BY created_at ASC")?;
            let items = stmt
                .query_map([], map_item)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(items)
        }

        pub fn database_id(&self) -> Result<String> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            conn.query_row(
                "SELECT value FROM queue_metadata WHERE key='database_id'",
                [],
                |row| row.get(0),
            )
            .context("read queue database identity")
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn verify_workspace_root_path(
            &self,
            repo_key: &str,
            workspace_root: &Path,
        ) -> Result<()> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            let existing: Option<String> = conn
                .query_row(
                    "SELECT workspace_root FROM workspace_roots WHERE repo_key=?1",
                    params![repo_key],
                    |row| row.get(0),
                )
                .optional()?;
            let expected = workspace_root
                .to_str()
                .context("IQ workspace root is not valid UTF-8")?;
            if existing.as_deref().is_some_and(|actual| actual != expected) {
                anyhow::bail!(
                    "repository queue {repo_key} workspace root differs from persisted state"
                );
            }
            Ok(())
        }

        pub fn workspace_root_generation(&self, repo_key: &str) -> Result<i64> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            conn.query_row(
                "SELECT generation FROM workspace_roots WHERE repo_key=?1",
                params![repo_key],
                |row| row.get(0),
            )
            .optional()
            .map(|generation| generation.unwrap_or(0))
            .context("read workspace root generation")
        }

        pub fn get_item(&self, item_id: &str) -> Result<QueueItem> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            required_row(
                conn.query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                ),
                "queue item",
                item_id,
            )
        }

        fn execution_authority(
            &self,
            item_id: &str,
            repo_key: &str,
            owner_id: &str,
            timeout: std::time::Duration,
        ) -> Result<ExecutionAuthority> {
            let conn = self.connect(timeout)?;
            let authority: Option<(
                String,
                String,
                Option<String>,
                Option<String>,
            )> = conn
                .query_row(
                    "SELECT q.status,q.repo_key,l.owner_id,l.expires_at FROM queue_items q LEFT JOIN repo_leases l ON l.repo_key=q.repo_key WHERE q.id=?1",
                    params![item_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let Some((status, stored_repo_key, lease_owner, lease_expires)) = authority else {
                return Ok(ExecutionAuthority::Lost(format!(
                    "queue item {item_id} no longer exists"
                )));
            };
            if stored_repo_key != repo_key {
                return Ok(ExecutionAuthority::Lost(format!(
                    "item {item_id} belongs to repo queue {stored_repo_key}, not {repo_key}"
                )));
            }
            if lease_owner.as_deref() != Some(owner_id) {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} lease is no longer owned by {owner_id}"
                )));
            }
            let Some(lease_expires) = lease_expires else {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} lease has no expiry"
                )));
            };
            let lease_expires = match DateTime::parse_from_rfc3339(&lease_expires) {
                Ok(expires) => expires.with_timezone(&Utc),
                Err(error) => {
                    return Ok(ExecutionAuthority::Lost(format!(
                        "repo queue {repo_key} lease expiry is invalid: {error}"
                    )));
                }
            };
            if lease_expires <= Utc::now() + Self::COMMAND_AUTHORITY_RESERVE {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} lease cannot cover the next command authority check"
                )));
            }
            match QueueStatus::from_str(&status) {
                Ok(QueueStatus::Cancelled) => Ok(ExecutionAuthority::Cancelled),
                Ok(_) => Ok(ExecutionAuthority::Active),
                Err(error) => Ok(ExecutionAuthority::Lost(format!(
                    "queue item {item_id} has invalid status: {error}"
                ))),
            }
        }

        fn lease_authority(
            &self,
            repo_key: &str,
            owner_id: &str,
            timeout: std::time::Duration,
        ) -> Result<ExecutionAuthority> {
            let conn = self.connect(timeout)?;
            let lease: Option<(String, String)> = conn
                .query_row(
                    "SELECT owner_id,expires_at FROM repo_leases WHERE repo_key=?1",
                    params![repo_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((lease_owner, lease_expires)) = lease else {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} lease no longer exists"
                )));
            };
            if lease_owner != owner_id {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} lease is no longer owned by {owner_id}"
                )));
            }
            let lease_expires = match DateTime::parse_from_rfc3339(&lease_expires) {
                Ok(expires) => expires.with_timezone(&Utc),
                Err(error) => {
                    return Ok(ExecutionAuthority::Lost(format!(
                        "repo queue {repo_key} lease expiry is invalid: {error}"
                    )));
                }
            };
            if lease_expires <= Utc::now() + Self::COMMAND_AUTHORITY_RESERVE {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} lease cannot cover the next command authority check"
                )));
            }
            Ok(ExecutionAuthority::Active)
        }

        pub fn events(&self, item_id: &str) -> Result<Vec<QueueEvent>> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            let mut stmt = conn.prepare("SELECT id,item_id,event_type,message,created_at FROM queue_events WHERE item_id=?1 ORDER BY created_at ASC")?;
            let events = stmt
                .query_map(params![item_id], |row| {
                    Ok(QueueEvent {
                        id: row.get(0)?,
                        item_id: row.get(1)?,
                        event_type: row.get(2)?,
                        message: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(events)
        }

        pub fn prompts_for_item(&self, item_id: &str) -> Result<Vec<Prompt>> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            let mut stmt = conn.prepare(
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE item_id=?1 ORDER BY created_at ASC",
            )?;
            let prompts = stmt
                .query_map(params![item_id], map_prompt)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(prompts)
        }

        pub fn get_attempt(&self, attempt_id: &str) -> Result<Attempt> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            required_row(
                conn.query_row(
                    "SELECT id,item_id,attempt_number,source_head_sha,target_base_sha,merge_commit_sha,validated_commit_sha,landed_commit_sha,validation_command,validation_exit_code,validation_log_path,policy_snapshot_json,policy_digest,signoff_evidence_json,moved_base_json FROM integration_attempts WHERE id=?1",
                    params![attempt_id],
                    |row| {
                        Ok(Attempt {
                            id: row.get(0)?,
                            item_id: row.get(1)?,
                            attempt_number: row.get(2)?,
                            source_head_sha: row.get(3)?,
                            target_base_sha: row.get(4)?,
                            merge_commit_sha: row.get(5)?,
                            validated_commit_sha: row.get(6)?,
                            landed_commit_sha: row.get(7)?,
                            validation_command: row.get(8)?,
                            validation_exit_code: row.get(9)?,
                            validation_log_path: row.get(10)?,
                            policy_snapshot_json: row.get(11)?,
                            policy_digest: row.get(12)?,
                            signoff_evidence_json: row.get(13)?,
                            moved_base: serde_json::from_str(&row.get::<_, String>(14)?)
                                .map_err(|error| map_json_error("moved_base_json", error))?,
                        })
                    },
                ),
                "attempt",
                attempt_id,
            )
        }

        pub fn get_prompt(&self, prompt_id: &str) -> Result<Prompt> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            required_row(
                conn.query_row(
                    "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE id=?1",
                    params![prompt_id],
                    map_prompt,
                ),
                "prompt",
                prompt_id,
            )
        }
    }

    fn path_text(path: &Path) -> Result<&str> {
        path.to_str().context("Git path is not valid UTF-8")
    }

    fn path_entry_exists(path: &Path) -> Result<bool> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct StatePathIdentity {
        dev: u64,
        ino: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct StateDirectoryIdentity {
        directory: StatePathIdentity,
        database: StatePathIdentity,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct StateLayout {
        legacy: Option<StateDirectoryIdentity>,
        iq: Option<StateDirectoryIdentity>,
    }

    fn inspect_state_layout(legacy: &Path, iq: &Path) -> Result<StateLayout> {
        let legacy_identity = inspect_state_directory(legacy, "legacy IQ state directory")?;
        let iq_identity = inspect_state_directory(iq, "IQ state directory")?;
        if legacy_identity.is_some() && iq_identity.is_some() {
            anyhow::bail!(
                "both legacy and IQ state directories exist; refuse ambiguous state migration: {} and {}",
                legacy.display(),
                iq.display()
            );
        }
        Ok(StateLayout {
            legacy: legacy_identity,
            iq: iq_identity,
        })
    }

    fn inspect_state_directory(path: &Path, label: &str) -> Result<Option<StateDirectoryIdentity>> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("inspect {label}")),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("{label} must be a real directory: {}", path.display());
        }
        let database = path.join("queues.db");
        let database_metadata = fs::symlink_metadata(&database)
            .with_context(|| format!("inspect {label} database {}", database.display()))?;
        if database_metadata.file_type().is_symlink() || !database_metadata.is_file() {
            anyhow::bail!(
                "{label} database must be a regular file: {}",
                database.display()
            );
        }
        Ok(Some(StateDirectoryIdentity {
            directory: StatePathIdentity {
                dev: metadata.dev(),
                ino: metadata.ino(),
            },
            database: StatePathIdentity {
                dev: database_metadata.dev(),
                ino: database_metadata.ino(),
            },
        }))
    }

    fn verify_open_directory(path: &Path, directory: &fs::File, label: &str) -> Result<()> {
        let path_metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect {label} path {}", path.display()))?;
        let open_metadata = directory
            .metadata()
            .with_context(|| format!("inspect open {label} {}", path.display()))?;
        if path_metadata.file_type().is_symlink()
            || !path_metadata.is_dir()
            || path_metadata.dev() != open_metadata.dev()
            || path_metadata.ino() != open_metadata.ino()
        {
            anyhow::bail!("{label} identity changed: {}", path.display());
        }
        Ok(())
    }

    fn open_lock_at(directory: &fs::File, name: &str) -> Result<fs::File> {
        let name = CString::new(name).context("state lock name contains a NUL byte")?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("open IQ state migration lock");
        }
        Ok(unsafe { fs::File::from_raw_fd(fd) })
    }

    fn require_real_directory(path: &Path, label: &str) -> Result<()> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect {label} {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("{label} must be a real directory: {}", path.display());
        }
        Ok(())
    }

    fn require_regular_file(path: &Path, label: &str) -> Result<()> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect {label} {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            anyhow::bail!("{label} must be a regular file: {}", path.display());
        }
        Ok(())
    }

    fn path_bytes(path: &Path) -> Vec<u8> {
        path.as_os_str().as_bytes().to_vec()
    }

    fn row_path(row: &Row<'_>, column: &str) -> rusqlite::Result<PathBuf> {
        let bytes: Vec<u8> = row.get(column)?;
        Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
    }

    const REGISTERED_REPOSITORY_SELECT: &str = "SELECT repository.*,identity.remote_name,identity.fetch_url,identity.push_url FROM registered_repositories repository JOIN registered_remote_identities identity ON identity.repo_key=repository.repo_key";

    fn map_repository(row: &Row<'_>) -> rusqlite::Result<RegisteredRepository> {
        let seed_path = row_path(row, "seed_path")?;
        let seed_rift_id: Option<String> = row.get("seed_rift_id")?;
        let seed_source_rift_id: Option<String> = row.get("seed_source_rift_id")?;
        let seed = match (seed_rift_id, seed_source_rift_id) {
            (None, None) => WorkspaceState::CreationIntent {
                path: seed_path
                    .to_str()
                    .ok_or_else(|| map_parse_error("seed path is not valid UTF-8".into()))?
                    .to_string(),
            },
            (Some(rift_id), Some(source_rift_id)) => WorkspaceState::Retained {
                identity: WorkspaceIdentity {
                    path: seed_path
                        .to_str()
                        .ok_or_else(|| map_parse_error("seed path is not valid UTF-8".into()))?
                        .to_string(),
                    rift_id,
                    source_rift_id,
                },
            },
            _ => return Err(map_parse_error("invalid repository seed identity".into())),
        };
        let seed_refresh: SeedRefreshState =
            serde_json::from_str(&row.get::<_, String>("seed_refresh_json")?)
                .map_err(|error| map_json_error("seed_refresh_json", error))?;
        let seed_target = match &seed_refresh {
            SeedRefreshState::Ready { target_sha }
            | SeedRefreshState::Pending { target_sha }
            | SeedRefreshState::Failed { target_sha, .. } => target_sha,
        };
        require_persisted_sha(seed_target, "seed refresh target")?;
        let checkout_reconciliation: CheckoutReconciliationState =
            serde_json::from_str(&row.get::<_, String>("checkout_reconciliation_json")?)
                .map_err(|error| map_json_error("checkout_reconciliation_json", error))?;
        let checkout_target = match &checkout_reconciliation {
            CheckoutReconciliationState::Ready { target_sha }
            | CheckoutReconciliationState::Pending { target_sha }
            | CheckoutReconciliationState::Failed { target_sha, .. } => target_sha,
        };
        require_persisted_sha(checkout_target, "registered checkout target")?;
        Ok(RegisteredRepository {
            key: row.get("repo_key")?,
            integration_path: row_path(row, "integration_path")?,
            target_branch: row.get("target_branch")?,
            remote: RegisteredRemote {
                name: row.get("remote_name")?,
                fetch_url: row.get("fetch_url")?,
                push_url: row.get("push_url")?,
            },
            seed,
            workspace_root: row_path(row, "workspace_root")?,
            checkout_reconciliation,
            seed_refresh,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }

    fn map_development_workspace(row: &Row<'_>) -> rusqlite::Result<DevelopmentWorkspace> {
        let path = row_path(row, "path")?;
        let rift_id: Option<String> = row.get("rift_id")?;
        let source_rift_id: Option<String> = row.get("source_rift_id")?;
        let identity = match (rift_id, source_rift_id) {
            (None, None) => None,
            (Some(rift_id), Some(source_rift_id)) => Some(WorkspaceIdentity {
                path: path
                    .to_str()
                    .ok_or_else(|| map_parse_error("development path is not valid UTF-8".into()))?
                    .to_string(),
                rift_id,
                source_rift_id,
            }),
            _ => return Err(map_parse_error("invalid development Rift identity".into())),
        };
        Ok(DevelopmentWorkspace {
            id: row.get("id")?,
            repo_key: row.get("repo_key")?,
            name: row.get("name")?,
            identity,
            path,
            branch: row.get("branch")?,
            base_sha: row.get("base_sha")?,
            status: DevelopmentWorkspaceStatus::from_str(&row.get::<_, String>("status")?)
                .map_err(map_parse_error)?,
            cleanup: serde_json::from_str(&row.get::<_, String>("cleanup_json")?)
                .map_err(|error| map_json_error("cleanup_json", error))?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }

    fn map_local_submission(row: &Row<'_>) -> rusqlite::Result<LocalSubmission> {
        let base_sha: String = row.get("base_sha")?;
        let commit_sha: String = row.get("commit_sha")?;
        require_persisted_sha(&base_sha, "local submission base")?;
        require_persisted_sha(&commit_sha, "local submission commit")?;
        Ok(LocalSubmission {
            id: row.get("id")?,
            queue_item_id: row.get("queue_item_id")?,
            repo_key: row.get("repo_key")?,
            workspace_id: row.get("workspace_id")?,
            base_sha,
            commit_sha,
            private_ref: row.get("private_ref")?,
            staging_ref: row.get("staging_ref")?,
            replaces_item_id: row.get("replaces_item_id")?,
            state: LocalSubmissionState::from_str(&row.get::<_, String>("state")?)
                .map_err(map_parse_error)?,
            created_at: row.get("created_at")?,
        })
    }

    fn require_persisted_sha(value: &str, label: &str) -> rusqlite::Result<()> {
        if matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            Ok(())
        } else {
            Err(map_parse_error(format!("invalid {label} SHA")))
        }
    }

    fn required_row<T>(result: rusqlite::Result<T>, entity: &str, id: &str) -> Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(SqliteError::QueryReturnedNoRows) => anyhow::bail!("{entity} not found: {id}"),
            Err(error) => Err(error).with_context(|| format!("read {entity} {id}")),
        }
    }

    fn map_prompt(row: &Row<'_>) -> rusqlite::Result<Prompt> {
        let phase: String = row.get(3)?;
        let status: String = row.get(4)?;
        let answer: Option<String> = row.get(6)?;
        if status == "answered" && answer.is_none() {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Null,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "answered prompt has no answer",
                )),
            ));
        }
        Ok(Prompt {
            id: row.get(0)?,
            item_id: row.get(1)?,
            attempt_id: row.get(2)?,
            blocked_phase: BlockedPhase::from_str(&phase).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
                )
            })?,
            status,
            question: row.get(5)?,
            answer,
            options: row
                .get::<_, Option<String>>(7)?
                .map(|value| {
                    serde_json::from_str(&value)
                        .map_err(|error| map_json_error("options_json", error))
                })
                .transpose()?
                .unwrap_or_default(),
        })
    }

    fn map_item(row: &Row<'_>) -> rusqlite::Result<QueueItem> {
        let status: String = row.get("status")?;
        let blocked_phase: Option<String> = row.get("blocked_phase")?;
        let blocked_reason: Option<String> = row.get("blocked_reason")?;
        let prompt_id: Option<String> = row.get("prompt_id")?;
        let mut validation_evidence = parse_json_value(row, "validation_evidence_json")?;
        if let Some(prompt_id) = prompt_id {
            validation_evidence["prompt_id"] = Value::String(prompt_id);
        }
        Ok(QueueItem {
            id: row.get("id")?,
            repo_key: row.get("repo_key")?,
            repo_path: row.get("repo_path")?,
            source_branch: row.get("source_branch")?,
            target_branch: row.get("target_branch")?,
            current_head_sha: row.get("current_head_sha")?,
            pr_url: row.get("pr_url")?,
            status: QueueStatus::from_str(&status).map_err(map_parse_error)?,
            blocked_phase: blocked_phase
                .as_deref()
                .map(BlockedPhase::from_str)
                .transpose()
                .map_err(map_parse_error)?,
            blocked_reason: blocked_reason
                .as_deref()
                .map(BlockedReason::from_str)
                .transpose()
                .map_err(map_parse_error)?,
            current_attempt_id: row.get("current_attempt_id")?,
            workspace: map_workspace_state(row)?,
            conflict: parse_json_option(row, "conflict_json")?,
            target_sha: row.get("target_sha")?,
            source_sha: row.get("source_sha")?,
            landed_commit_sha: row.get("landed_commit_sha")?,
            producer_metadata: parse_json_value(row, "producer_metadata_json")?,
            validation_evidence,
            landing: serde_json::from_str(&row.get::<_, String>("landing_state_json")?)
                .map_err(|error| map_json_error("landing_state_json", error))?,
            source: match row.get::<_, String>("source_kind")?.as_str() {
                "remote_branch" => QueueSource::RemoteBranch {
                    branch: row.get("source_ref")?,
                },
                "local_submission" => QueueSource::LocalSubmission {
                    submission_id: row.get("submission_id")?,
                    commit_sha: row.get("current_head_sha")?,
                },
                value => return Err(map_parse_error(format!("unknown queue source: {value}"))),
            },
            landing_policy: LandingPolicy::from_str(&row.get::<_, String>("landing_policy")?)
                .map_err(map_parse_error)?,
            replacement: row
                .get::<_, Option<String>>("replacement_json")?
                .map(|value| {
                    serde_json::from_str(&value)
                        .map_err(|error| map_json_error("replacement_json", error))
                })
                .transpose()?
                .unwrap_or(ReplacementState::None),
        })
    }

    fn map_workspace_state(row: &Row<'_>) -> rusqlite::Result<WorkspaceState> {
        let path: Option<String> = row.get("integration_workspace_path")?;
        let rift_id: Option<String> = row.get("integration_workspace_rift_id")?;
        let source_rift_id: Option<String> = row.get("integration_workspace_source_rift_id")?;
        let cleaned_at: Option<String> = row.get("integration_workspace_cleaned_at")?;
        match (path, rift_id, source_rift_id, cleaned_at) {
            (None, None, None, None) => Ok(WorkspaceState::NotCreated),
            (Some(path), None, None, None) => Ok(WorkspaceState::CreationIntent { path }),
            (Some(path), Some(rift_id), Some(source_rift_id), None) => {
                Ok(WorkspaceState::Retained {
                    identity: WorkspaceIdentity {
                        path,
                        rift_id,
                        source_rift_id,
                    },
                })
            }
            (None, None, None, Some(cleaned_at)) => Ok(WorkspaceState::Cleaned { cleaned_at }),
            _ => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid queue item workspace state",
                )),
            )),
        }
    }

    fn parse_json_value(row: &Row<'_>, column: &'static str) -> rusqlite::Result<Value> {
        let raw: String = row.get(column)?;
        serde_json::from_str(&raw).map_err(|error| map_json_error(column, error))
    }

    fn parse_json_option(row: &Row<'_>, column: &'static str) -> rusqlite::Result<Option<Value>> {
        let raw: Option<String> = row.get(column)?;
        raw.map(|value| serde_json::from_str(&value).map_err(|error| map_json_error(column, error)))
            .transpose()
    }

    fn map_json_error(column: &'static str, error: serde_json::Error) -> rusqlite::Error {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid JSON in queue_items.{column}: {error}"),
            )),
        )
    }

    fn map_parse_error(error: String) -> rusqlite::Error {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        )
    }

    fn insert_state_repository_binding(
        transaction: &rusqlite::Transaction<'_>,
        item_id: &str,
        snapshot: &crate::control_domain::StateRepositorySnapshot,
        timestamp: &str,
    ) -> Result<()> {
        use crate::control_domain::{IssueVisibility, StateRepositorySnapshot};
        let (provider, repository, visibility, reservation_state) = match snapshot {
            StateRepositorySnapshot::Local => (None, None, None, "none"),
            StateRepositorySnapshot::GithubIssue(issue) => (
                Some("github"),
                Some(issue.repository.as_str()),
                Some(match issue.visibility {
                    IssueVisibility::Minimal => "minimal",
                    IssueVisibility::Full => "full",
                }),
                if issue.visibility == IssueVisibility::Full {
                    "pending"
                } else {
                    "none"
                },
            ),
            StateRepositorySnapshot::GitlabIssue(issue) => (
                Some("gitlab"),
                Some(issue.repository.as_str()),
                Some(match issue.visibility {
                    IssueVisibility::Minimal => "minimal",
                    IssueVisibility::Full => "full",
                }),
                if issue.visibility == IssueVisibility::Full {
                    "pending"
                } else {
                    "none"
                },
            ),
        };
        transaction.execute(
            "INSERT INTO item_state_repository_bindings(item_id,snapshot_json,provider,repository,visibility,reservation_state,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![item_id,serde_json::to_string(snapshot)?,provider,repository,visibility,reservation_state,timestamp],
        )?;
        Ok(())
    }

    fn now() -> String {
        Utc::now().to_rfc3339()
    }

    const WORKSPACE_STATE_TRIGGERS: &str = r#"
DROP TRIGGER IF EXISTS queue_items_workspace_state_insert;
DROP TRIGGER IF EXISTS queue_items_workspace_state_update;

CREATE TRIGGER queue_items_workspace_state_insert
BEFORE INSERT ON queue_items
WHEN NOT (
  (NEW.integration_workspace_cleaned_at IS NULL AND NEW.integration_workspace_path IS NULL AND NEW.integration_workspace_rift_id IS NULL AND NEW.integration_workspace_source_rift_id IS NULL AND NEW.status IN ('ready','merging','blocked','cancelled')) OR
  (NEW.integration_workspace_cleaned_at IS NULL AND NEW.integration_workspace_path IS NOT NULL AND NEW.integration_workspace_rift_id IS NULL AND NEW.integration_workspace_source_rift_id IS NULL AND NEW.status IN ('merging','cancelled')) OR
  (NEW.integration_workspace_cleaned_at IS NULL AND NEW.integration_workspace_path IS NOT NULL AND NEW.integration_workspace_rift_id IS NOT NULL AND NEW.integration_workspace_source_rift_id IS NOT NULL AND NEW.status IN ('ready','merging','merged','validating','validated','integrating','blocked','integrated','cancelled')) OR
  (NEW.integration_workspace_cleaned_at IS NOT NULL AND NEW.status IN ('integrated','cancelled') AND NEW.integration_workspace_path IS NULL AND NEW.integration_workspace_rift_id IS NULL AND NEW.integration_workspace_source_rift_id IS NULL)
)
BEGIN
  SELECT RAISE(ABORT, 'invalid queue item workspace state');
END;

CREATE TRIGGER queue_items_workspace_state_update
BEFORE UPDATE OF status,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at ON queue_items
WHEN NOT (
  (NEW.integration_workspace_cleaned_at IS NULL AND NEW.integration_workspace_path IS NULL AND NEW.integration_workspace_rift_id IS NULL AND NEW.integration_workspace_source_rift_id IS NULL AND NEW.status IN ('ready','merging','blocked','cancelled')) OR
  (NEW.integration_workspace_cleaned_at IS NULL AND NEW.integration_workspace_path IS NOT NULL AND NEW.integration_workspace_rift_id IS NULL AND NEW.integration_workspace_source_rift_id IS NULL AND NEW.status IN ('merging','cancelled')) OR
  (NEW.integration_workspace_cleaned_at IS NULL AND NEW.integration_workspace_path IS NOT NULL AND NEW.integration_workspace_rift_id IS NOT NULL AND NEW.integration_workspace_source_rift_id IS NOT NULL AND NEW.status IN ('ready','merging','merged','validating','validated','integrating','blocked','integrated','cancelled')) OR
  (NEW.integration_workspace_cleaned_at IS NOT NULL AND NEW.status IN ('integrated','cancelled') AND NEW.integration_workspace_path IS NULL AND NEW.integration_workspace_rift_id IS NULL AND NEW.integration_workspace_source_rift_id IS NULL)
)
BEGIN
  SELECT RAISE(ABORT, 'invalid queue item workspace state');
END;
"#;

    const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queue_items (
  id TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL,
  repo_path TEXT NOT NULL,
  source_branch TEXT NOT NULL,
  target_branch TEXT NOT NULL,
  pr_url TEXT,
  producer_metadata_json TEXT NOT NULL,
  validation_evidence_json TEXT NOT NULL,
  status TEXT NOT NULL,
  current_head_sha TEXT NOT NULL,
  current_attempt_id TEXT,
  blocked_phase TEXT,
  blocked_reason TEXT,
  blocked_message TEXT,
  retry_after TEXT,
  prompt_id TEXT,
  conflict_json TEXT,
  integration_workspace_path TEXT,
  integration_workspace_rift_id TEXT,
  integration_workspace_source_rift_id TEXT,
  integration_workspace_cleaned_at TEXT,
  target_sha TEXT,
  source_sha TEXT,
  landed_commit_sha TEXT,
  landing_state_json TEXT NOT NULL DEFAULT '{"state":"ready"}',
  source_kind TEXT NOT NULL DEFAULT 'remote_branch',
  source_ref TEXT,
  submission_id TEXT,
  landing_policy TEXT NOT NULL DEFAULT 'direct',
  replacement_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(submission_id) REFERENCES local_submissions(id),
  CHECK (
    (source_kind='remote_branch' AND source_ref IS NOT NULL AND submission_id IS NULL AND source_ref=source_branch AND ((landing_policy='direct' AND pr_url IS NULL) OR (landing_policy='provider' AND pr_url IS NOT NULL))) OR
    (source_kind='local_submission' AND source_ref IS NOT NULL AND submission_id IS NOT NULL AND source_ref=source_branch AND landing_policy='squash' AND pr_url IS NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS queue_items_active_identity
ON queue_items(repo_key, source_branch, target_branch)
WHERE status NOT IN ('integrated','cancelled');

CREATE TABLE IF NOT EXISTS integration_attempts (
  id TEXT PRIMARY KEY,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL,
  source_head_sha TEXT NOT NULL,
  target_base_sha TEXT,
  merge_commit_sha TEXT,
  validated_commit_sha TEXT,
  landed_commit_sha TEXT,
  validation_command TEXT,
  validation_exit_code INTEGER,
  validation_log_path TEXT,
  policy_snapshot_json TEXT,
  policy_digest TEXT,
  signoff_evidence_json TEXT,
  moved_base_json TEXT NOT NULL DEFAULT '{"state":"none"}',
  started_at TEXT NOT NULL,
  finished_at TEXT,
  result TEXT,
  UNIQUE(item_id, attempt_number)
);
CREATE UNIQUE INDEX IF NOT EXISTS integration_attempt_item_identity ON integration_attempts(id,item_id);

CREATE TABLE IF NOT EXISTS queue_events (
  id TEXT PRIMARY KEY,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prompts (
  id TEXT PRIMARY KEY,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  attempt_id TEXT,
  blocked_phase TEXT NOT NULL,
  status TEXT NOT NULL,
  question TEXT NOT NULL,
  options_json TEXT,
  allow_freeform INTEGER NOT NULL DEFAULT 1,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  answer TEXT,
  answered_by TEXT,
  answered_at TEXT
);

CREATE TABLE IF NOT EXISTS communication_bindings (
  id TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  transport_id TEXT NOT NULL,
  transport_kind TEXT NOT NULL,
  endpoint_fingerprint TEXT NOT NULL,
  marker TEXT NOT NULL UNIQUE,
  external_ref_json TEXT,
  external_url TEXT,
  status TEXT NOT NULL,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(item_id, transport_id)
);

CREATE TABLE IF NOT EXISTS communication_response_receipts (
  binding_id TEXT NOT NULL REFERENCES communication_bindings(id) ON DELETE CASCADE,
  external_response_id TEXT NOT NULL,
  prompt_id TEXT NOT NULL,
  answer TEXT NOT NULL,
  actor TEXT NOT NULL,
  disposition TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(binding_id, external_response_id)
);

-- Schema v9 has no communication transport authority.
DROP TABLE IF EXISTS communication_response_receipts;
DROP TABLE IF EXISTS communication_bindings;

CREATE TABLE IF NOT EXISTS repo_leases (
  repo_key TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_roots (
  repo_key TEXT PRIMARY KEY,
  source_path TEXT NOT NULL,
  source_rift_id TEXT NOT NULL,
  workspace_root TEXT NOT NULL UNIQUE,
  registry_identity TEXT NOT NULL,
  generation INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS workspace_gc_debt (
  registry_identity TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
);
"#;

    const COMPOSITION_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS registered_remote_identities (
  repo_key TEXT PRIMARY KEY,
  integration_path BLOB NOT NULL UNIQUE,
  target_branch TEXT NOT NULL,
  remote_name TEXT NOT NULL,
  fetch_url TEXT NOT NULL,
  push_url TEXT NOT NULL,
  created_at TEXT NOT NULL,
  CHECK(repo_key!='' AND target_branch!='' AND remote_name!='' AND fetch_url!='' AND push_url!='')
);

CREATE TABLE IF NOT EXISTS registered_repositories (
  repo_key TEXT PRIMARY KEY,
  integration_path BLOB NOT NULL UNIQUE,
  target_branch TEXT NOT NULL,
  remote TEXT NOT NULL,
  seed_path BLOB NOT NULL UNIQUE,
  seed_rift_id TEXT,
  seed_source_rift_id TEXT,
  workspace_root BLOB NOT NULL UNIQUE,
  checkout_reconciliation_json TEXT NOT NULL,
  seed_refresh_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS development_workspaces (
  id TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  name TEXT NOT NULL,
  path BLOB NOT NULL UNIQUE,
  rift_id TEXT,
  source_rift_id TEXT,
  branch TEXT NOT NULL UNIQUE,
  base_sha TEXT NOT NULL,
  status TEXT NOT NULL,
  cleanup_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(repo_key,name)
);

CREATE TABLE IF NOT EXISTS local_submissions (
  id TEXT PRIMARY KEY,
  queue_item_id TEXT NOT NULL,
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  workspace_id TEXT NOT NULL REFERENCES development_workspaces(id),
  base_sha TEXT NOT NULL CHECK(length(base_sha) IN (40,64) AND base_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  commit_sha TEXT NOT NULL CHECK(length(commit_sha) IN (40,64) AND commit_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  private_ref TEXT NOT NULL UNIQUE,
  staging_ref TEXT NOT NULL UNIQUE,
  replaces_item_id TEXT,
  state TEXT NOT NULL CHECK(state IN ('creating','queued','replaced','cancelled','integrated')),
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS local_submissions_creating_workspace
ON local_submissions(workspace_id)
WHERE state='creating';
"#;

    const LANDING_STATE_TRIGGERS: &str = r#"
DROP TRIGGER IF EXISTS queue_items_landing_state_insert;
DROP TRIGGER IF EXISTS queue_items_landing_state_update;

CREATE TRIGGER queue_items_landing_state_insert
BEFORE INSERT ON queue_items
WHEN json_valid(NEW.landing_state_json)=0 OR NOT (
  (json_extract(NEW.landing_state_json,'$.state')='ready' AND NEW.status!='integrated') OR
  (json_extract(NEW.landing_state_json,'$.state')='uncertain' AND (NEW.status='integrating' OR (NEW.status='blocked' AND NEW.blocked_phase='integrating')) AND
   length(json_extract(NEW.landing_state_json,'$.candidate_sha')) IN (40,64) AND json_extract(NEW.landing_state_json,'$.candidate_sha') NOT GLOB '*[^0-9A-Fa-f]*' AND
   length(json_extract(NEW.landing_state_json,'$.expected_target_sha')) IN (40,64) AND json_extract(NEW.landing_state_json,'$.expected_target_sha') NOT GLOB '*[^0-9A-Fa-f]*') OR
  (json_extract(NEW.landing_state_json,'$.state')='landed' AND NEW.status='integrated' AND
   length(json_extract(NEW.landing_state_json,'$.candidate_sha')) IN (40,64) AND json_extract(NEW.landing_state_json,'$.candidate_sha') NOT GLOB '*[^0-9A-Fa-f]*' AND
   NEW.landed_commit_sha=json_extract(NEW.landing_state_json,'$.commit_sha'))
)
BEGIN
  SELECT RAISE(ABORT, 'invalid queue landing state');
END;

CREATE TRIGGER queue_items_landing_state_update
BEFORE UPDATE OF status,blocked_phase,landed_commit_sha,landing_state_json ON queue_items
WHEN json_valid(NEW.landing_state_json)=0 OR NOT (
  (json_extract(NEW.landing_state_json,'$.state')='ready' AND NEW.status!='integrated') OR
  (json_extract(NEW.landing_state_json,'$.state')='uncertain' AND (NEW.status='integrating' OR (NEW.status='blocked' AND NEW.blocked_phase='integrating')) AND
   length(json_extract(NEW.landing_state_json,'$.candidate_sha')) IN (40,64) AND json_extract(NEW.landing_state_json,'$.candidate_sha') NOT GLOB '*[^0-9A-Fa-f]*' AND
   length(json_extract(NEW.landing_state_json,'$.expected_target_sha')) IN (40,64) AND json_extract(NEW.landing_state_json,'$.expected_target_sha') NOT GLOB '*[^0-9A-Fa-f]*') OR
  (json_extract(NEW.landing_state_json,'$.state')='landed' AND NEW.status='integrated' AND
   length(json_extract(NEW.landing_state_json,'$.candidate_sha')) IN (40,64) AND json_extract(NEW.landing_state_json,'$.candidate_sha') NOT GLOB '*[^0-9A-Fa-f]*' AND
   NEW.landed_commit_sha=json_extract(NEW.landing_state_json,'$.commit_sha'))
)
BEGIN
  SELECT RAISE(ABORT, 'invalid queue landing state');
END;
"#;

    const REGISTERED_REMOTE_TRIGGERS: &str = r#"
DROP TRIGGER IF EXISTS registered_repository_remote_insert;
DROP TRIGGER IF EXISTS registered_repository_remote_update;
DROP TRIGGER IF EXISTS registered_remote_identity_immutable;
DROP TRIGGER IF EXISTS registered_remote_identity_delete;

CREATE TRIGGER registered_repository_remote_insert
BEFORE INSERT ON registered_repositories
WHEN NOT EXISTS (
  SELECT 1 FROM registered_remote_identities identity
  WHERE identity.repo_key=NEW.repo_key
    AND identity.integration_path=NEW.integration_path
    AND identity.target_branch=NEW.target_branch
    AND identity.remote_name=NEW.remote
)
BEGIN
  SELECT RAISE(ABORT, 'registered repository has no exact remote identity intent');
END;

CREATE TRIGGER registered_repository_remote_update
BEFORE UPDATE OF repo_key,integration_path,target_branch,remote ON registered_repositories
WHEN NOT EXISTS (
  SELECT 1 FROM registered_remote_identities identity
  WHERE identity.repo_key=NEW.repo_key
    AND identity.integration_path=NEW.integration_path
    AND identity.target_branch=NEW.target_branch
    AND identity.remote_name=NEW.remote
)
BEGIN
  SELECT RAISE(ABORT, 'registered repository differs from its remote identity');
END;

CREATE TRIGGER registered_remote_identity_immutable
BEFORE UPDATE ON registered_remote_identities
BEGIN
  SELECT RAISE(ABORT, 'registered remote identity is immutable');
END;

CREATE TRIGGER registered_remote_identity_delete
BEFORE DELETE ON registered_remote_identities
WHEN EXISTS (SELECT 1 FROM registered_repositories WHERE repo_key=OLD.repo_key)
BEGIN
  SELECT RAISE(ABORT, 'registered remote identity is still in use');
END;
"#;

    const REGISTERED_CHECKOUT_TRIGGERS: &str = r#"
DROP TRIGGER IF EXISTS registered_checkout_state_insert;
DROP TRIGGER IF EXISTS registered_checkout_state_update;

CREATE TRIGGER registered_checkout_state_insert
BEFORE INSERT ON registered_repositories
WHEN json_valid(NEW.checkout_reconciliation_json)=0 OR
     json_extract(NEW.checkout_reconciliation_json,'$.state') NOT IN ('ready','pending','failed') OR
     length(json_extract(NEW.checkout_reconciliation_json,'$.target_sha')) NOT IN (40,64) OR
     json_extract(NEW.checkout_reconciliation_json,'$.target_sha') GLOB '*[^0-9A-Fa-f]*' OR
     (json_extract(NEW.checkout_reconciliation_json,'$.state')='failed' AND COALESCE(json_extract(NEW.checkout_reconciliation_json,'$.message'),'')='')
BEGIN
  SELECT RAISE(ABORT, 'invalid registered checkout reconciliation state');
END;

CREATE TRIGGER registered_checkout_state_update
BEFORE UPDATE OF checkout_reconciliation_json ON registered_repositories
WHEN json_valid(NEW.checkout_reconciliation_json)=0 OR
     json_extract(NEW.checkout_reconciliation_json,'$.state') NOT IN ('ready','pending','failed') OR
     length(json_extract(NEW.checkout_reconciliation_json,'$.target_sha')) NOT IN (40,64) OR
     json_extract(NEW.checkout_reconciliation_json,'$.target_sha') GLOB '*[^0-9A-Fa-f]*' OR
     (json_extract(NEW.checkout_reconciliation_json,'$.state')='failed' AND COALESCE(json_extract(NEW.checkout_reconciliation_json,'$.message'),'')='')
BEGIN
  SELECT RAISE(ABORT, 'invalid registered checkout reconciliation state');
END;
"#;

    const QUEUE_SOURCE_TRIGGERS: &str = r#"
DROP TRIGGER IF EXISTS queue_items_local_source_insert;
DROP TRIGGER IF EXISTS queue_items_local_source_update;
DROP TRIGGER IF EXISTS local_submission_identity_immutable;

CREATE TRIGGER queue_items_local_source_insert
BEFORE INSERT ON queue_items
WHEN NEW.source_kind='local_submission' AND NOT EXISTS (
  SELECT 1 FROM local_submissions submission
  WHERE submission.id=NEW.submission_id
    AND submission.repo_key=NEW.repo_key
    AND submission.commit_sha=NEW.current_head_sha
    AND submission.private_ref=NEW.source_ref
    AND submission.queue_item_id=NEW.id
    AND submission.state='creating'
)
BEGIN
  SELECT RAISE(ABORT, 'local queue source does not match exact submission intent');
END;

CREATE TRIGGER queue_items_local_source_update
BEFORE UPDATE OF source_kind,source_ref,submission_id,current_head_sha,repo_key,id ON queue_items
WHEN NEW.source_kind='local_submission' AND NOT EXISTS (
  SELECT 1 FROM local_submissions submission
  WHERE submission.id=NEW.submission_id
    AND submission.repo_key=NEW.repo_key
    AND submission.commit_sha=NEW.current_head_sha
    AND submission.private_ref=NEW.source_ref
    AND submission.queue_item_id=NEW.id
    AND submission.state='creating'
)
BEGIN
  SELECT RAISE(ABORT, 'local queue source does not match exact submission intent');
END;

CREATE TRIGGER local_submission_identity_immutable
BEFORE UPDATE OF id,queue_item_id,repo_key,workspace_id,base_sha,commit_sha,private_ref,staging_ref,replaces_item_id,created_at ON local_submissions
BEGIN
  SELECT RAISE(ABORT, 'local submission identity is immutable');
END;
"#;
}

pub mod integrator {
    use anyhow::{Context, Result};
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value as JsonValue};
    use sha2::{Digest, Sha256};
    use std::collections::{HashSet, VecDeque};
    use std::ffi::{OsStr, OsString};
    use std::fs::{self, OpenOptions};
    use std::io::{Read, Seek, Write};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitStatus, Output, Stdio};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::mpsc;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration as StdDuration, Instant};
    use uuid::Uuid;
    use wait_timeout::ChildExt;

    use crate::core::{BlockedPhase, BlockedReason, QueueStatus};
    use crate::sqlite::{
        Attempt, ExecutionAuthority, QueueItem, ResidueChildMove, ResidueEntryIdentity,
        RiftWorkspaceRootOwner, SqliteQueue, SqliteQueueReader, WorkspaceIdentity, WorkspaceState,
    };

    #[derive(Clone, Debug)]
    pub struct IntegratorOptions {
        pub repo_key: String,
        pub repo_path: PathBuf,
        pub queue_db: PathBuf,
        pub owner_id: String,
        pub lease_ttl_seconds: i64,
        pub base_remote: String,
        pub workspace_root: PathBuf,
        pub rift_database: Option<PathBuf>,
        pub system_config: crate::agent_config::SystemConfig,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct SignoffPolicy {
        pub command: String,
        pub repository: String,
        pub required_contexts: Vec<String>,
        pub trusted_creator: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum IntegrationPolicy {
        NoValidation,
        Validation {
            command: String,
            signoff: HostSignoffPolicy,
        },
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum HostSignoffPolicy {
        None,
        Required(SignoffPolicy),
    }

    pub struct Integrator {
        queue: SqliteQueue,
        options: IntegratorOptions,
        policy: IntegrationPolicy,
        registered: bool,
        lease_owner_id: String,
        workspaces: RiftWorkspaceManager,
        control_store: crate::control_store::ControlStore,
    }

    pub(crate) struct RiftWorkspaceManager {
        source: PathBuf,
        source_id: String,
        source_ancestors: Vec<PathBuf>,
        root: PathBuf,
        repo_key: String,
        queue_database_id: String,
        registry_identity: String,
        registry_dev: u64,
        registry_ino: u64,
        generation: AtomicI64,
        program: String,
        database: Option<OsString>,
        root_directory: fs::File,
    }

    pub(crate) struct ResidueDiscardRequest<'a> {
        pub identity: &'a WorkspaceIdentity,
        pub quarantine_name: &'a str,
        pub inspected_identity: Option<(u64, u64, [u8; 32])>,
        pub pending_child_move: Option<ResidueChildMove>,
    }

    struct EvidenceDirectory {
        path: PathBuf,
        directory: fs::File,
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    struct DirectoryIdentity {
        device: u64,
        inode: u64,
    }

    struct DirectoryNode {
        components: Vec<OsString>,
        identity: DirectoryIdentity,
        child_names: Vec<OsString>,
    }

    struct RegularFileNode {
        components: Vec<OsString>,
        identity: ResidueEntryIdentity,
    }

    struct ResidueTree {
        directories: Vec<DirectoryNode>,
        files: Vec<RegularFileNode>,
    }

    const RIFT_TRASH_DIRECTORY: &str = ".trash";

    fn validate_host_policy(policy: IntegrationPolicy) -> Result<IntegrationPolicy> {
        let IntegrationPolicy::Validation { command, signoff } = policy else {
            return Ok(IntegrationPolicy::NoValidation);
        };
        if command.is_empty() || command.trim() != command {
            anyhow::bail!(
                "host validation command must be non-empty and have no surrounding whitespace"
            );
        }
        let signoff = match signoff {
            HostSignoffPolicy::None => HostSignoffPolicy::None,
            HostSignoffPolicy::Required(signoff) => {
                if signoff.command.is_empty()
                    || signoff.command.trim() != signoff.command
                    || signoff.repository.is_empty()
                    || signoff.repository.trim() != signoff.repository
                    || signoff.trusted_creator.is_empty()
                    || signoff.trusted_creator.trim() != signoff.trusted_creator
                    || signoff.required_contexts.is_empty()
                    || signoff
                        .required_contexts
                        .iter()
                        .any(|context| context.is_empty() || context.trim() != context)
                    || signoff
                        .required_contexts
                        .iter()
                        .enumerate()
                        .any(|(index, context)| {
                            signoff.required_contexts[..index].contains(context)
                        })
                {
                    anyhow::bail!(
                        "host signoff policy requires exact command, repository, trusted_creator, and unique required_contexts"
                    );
                }
                HostSignoffPolicy::Required(signoff)
            }
        };
        Ok(IntegrationPolicy::Validation { command, signoff })
    }

    pub fn workspace_scope(repo_key: &str) -> String {
        let hash = repo_key
            .as_bytes()
            .iter()
            .fold(0xcbf29ce484222325_u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
            });
        format!("{hash:016x}")
    }

    fn default_rift_database_path() -> Result<PathBuf> {
        if cfg!(target_os = "macos") {
            let home = std::env::var_os("HOME").context("HOME is required for Rift registry")?;
            Ok(PathBuf::from(home).join("Library/Application Support/rift/rift.sqlite"))
        } else if let Some(data_home) = std::env::var_os("XDG_DATA_HOME") {
            Ok(PathBuf::from(data_home).join("rift/rift.sqlite"))
        } else {
            let home = std::env::var_os("HOME").context("HOME is required for Rift registry")?;
            Ok(PathBuf::from(home).join(".local/share/rift/rift.sqlite"))
        }
    }

    fn resolve_rift_database(
        database: Option<PathBuf>,
    ) -> Result<(Option<OsString>, String, u64, u64)> {
        let database = database
            .or_else(|| std::env::var_os("IQ_RIFT_DATABASE").map(PathBuf::from))
            .unwrap_or(default_rift_database_path()?);
        let database = if database.is_absolute() {
            database
        } else {
            std::env::current_dir()?.join(database)
        };
        let database_display = database.display().to_string();
        let unresolved_metadata = fs::symlink_metadata(&database)
            .with_context(|| format!("inspect Rift registry database {database_display}"))?;
        if unresolved_metadata.file_type().is_symlink() || !unresolved_metadata.is_file() {
            anyhow::bail!("Rift registry database must be a regular non-symlink file");
        }
        let database = database
            .canonicalize()
            .with_context(|| format!("resolve Rift registry database {database_display}"))?;
        let registry_identity = database
            .to_str()
            .context("Rift registry database path is not valid UTF-8")?
            .to_string();
        let metadata = fs::symlink_metadata(&database)?;
        Ok((
            Some(database.into_os_string()),
            registry_identity,
            metadata.dev(),
            metadata.ino(),
        ))
    }

    fn entry_exists(path: &Path) -> Result<bool> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
        }
    }

    fn is_rift_workspace_root_entry(path: &Path) -> Result<bool> {
        if path.file_name() != Some(OsStr::new(RIFT_TRASH_DIRECTORY)) {
            return Ok(false);
        }
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect Rift trash directory {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!(
                "Rift trash path must be a real directory: {}",
                path.display()
            );
        }
        Ok(true)
    }

    #[cfg(target_os = "linux")]
    fn mount_id(path: &Path) -> Result<u64> {
        let path_bytes = std::ffi::CString::new(path.as_os_str().as_bytes())
            .context("filesystem path contains NUL")?;
        let mut stat = std::mem::MaybeUninit::<libc::statx>::zeroed();
        if unsafe {
            libc::statx(
                libc::AT_FDCWD,
                path_bytes.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
                libc::STATX_MNT_ID,
                stat.as_mut_ptr(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("inspect mount for {}", path.display()));
        }
        let stat = unsafe { stat.assume_init() };
        if stat.stx_mask & libc::STATX_MNT_ID == 0 {
            anyhow::bail!(
                "kernel did not report mount identity for {}",
                path.display()
            );
        }
        Ok(stat.stx_mnt_id)
    }

    fn require_same_filesystem(source: &Path, workspace_root: &Path) -> Result<()> {
        let same_device = fs::metadata(source)?.dev() == fs::metadata(workspace_root)?.dev();
        #[cfg(target_os = "linux")]
        let same_mount = !same_device && mount_id(source)? == mount_id(workspace_root)?;
        #[cfg(not(target_os = "linux"))]
        let same_mount = false;
        if !same_device && !same_mount {
            anyhow::bail!(
                "IQ workspace root {} must use the same filesystem as Rift source {}",
                workspace_root.display(),
                source.display()
            );
        }
        Ok(())
    }

    fn read_regular_file(path: &Path, label: &str) -> Result<Vec<u8>> {
        let before = fs::symlink_metadata(path)
            .with_context(|| format!("inspect {label} {}", path.display()))?;
        if before.file_type().is_symlink() || !before.is_file() {
            anyhow::bail!(
                "{label} must be a regular non-symlink file: {}",
                path.display()
            );
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("open {label} {}", path.display()))?;
        let after = file
            .metadata()
            .with_context(|| format!("inspect open {label} {}", path.display()))?;
        if before.dev() != after.dev() || before.ino() != after.ino() {
            anyhow::bail!("{label} changed while opening: {}", path.display());
        }
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .with_context(|| format!("read {label} {}", path.display()))?;
        Ok(contents)
    }

    fn child_name(name: &OsStr, label: &str) -> Result<std::ffi::CString> {
        if name.as_bytes().is_empty() || name.as_bytes().contains(&b'/') {
            anyhow::bail!("invalid {label} child name");
        }
        std::ffi::CString::new(name.as_bytes()).context("child name contains NUL")
    }

    fn open_directory_child(parent: &fs::File, name: &OsStr) -> Result<fs::File> {
        let name = child_name(name, "directory")?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("open directory child");
        }
        Ok(unsafe { fs::File::from_raw_fd(fd) })
    }

    enum ResidueEntryType {
        Directory,
        RegularFile,
    }

    fn residue_entry_type(parent: &fs::File, name: &OsStr) -> Result<ResidueEntryType> {
        let name = child_name(name, "cleanup residue")?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
        if unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error()).context("inspect cleanup residue entry");
        }
        let mode = unsafe { stat.assume_init() }.st_mode & libc::S_IFMT;
        match mode {
            libc::S_IFDIR => Ok(ResidueEntryType::Directory),
            libc::S_IFREG => Ok(ResidueEntryType::RegularFile),
            _ => anyhow::bail!("cleanup residue entry is a symlink or special entry"),
        }
    }

    fn open_directory_path(root: &fs::File, components: &[OsString]) -> Result<fs::File> {
        let duplicate = unsafe { libc::fcntl(root.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if duplicate < 0 {
            return Err(std::io::Error::last_os_error()).context("duplicate directory descriptor");
        }
        let mut directory = unsafe { fs::File::from_raw_fd(duplicate) };
        for component in components {
            directory = open_directory_child(&directory, component)?;
        }
        Ok(directory)
    }

    fn directory_identity(directory: &fs::File) -> Result<DirectoryIdentity> {
        let metadata = directory.metadata()?;
        if !metadata.is_dir() {
            anyhow::bail!("cleanup residue entry changed from a directory");
        }
        Ok(DirectoryIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn clear_errno() {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        unsafe {
            *libc::__errno_location() = 0;
        }
        #[cfg(any(
            target_os = "macos",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd"
        ))]
        unsafe {
            *libc::__error() = 0;
        }
    }

    fn directory_names(directory: &fs::File) -> Result<Vec<OsString>> {
        let duplicate = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if duplicate < 0 {
            return Err(std::io::Error::last_os_error()).context("duplicate residue directory");
        }
        if unsafe { libc::lseek(duplicate, 0, libc::SEEK_SET) } < 0 {
            let error = std::io::Error::last_os_error();
            unsafe {
                libc::close(duplicate);
            }
            return Err(error).context("rewind residue directory");
        }
        let stream = unsafe { libc::fdopendir(duplicate) };
        if stream.is_null() {
            let error = std::io::Error::last_os_error();
            unsafe {
                libc::close(duplicate);
            }
            return Err(error).context("open residue directory stream");
        }
        let names = (|| {
            let mut names = Vec::new();
            loop {
                clear_errno();
                let entry = unsafe { libc::readdir(stream) };
                if entry.is_null() {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() == Some(0) {
                        break;
                    }
                    return Err(error).context("read residue directory");
                }
                let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
                if name != b"." && name != b".." {
                    names.push(OsString::from_vec(name.to_vec()));
                }
            }
            names.sort();
            Ok(names)
        })();
        let close_result = unsafe { libc::closedir(stream) };
        let names = names?;
        if close_result != 0 {
            return Err(std::io::Error::last_os_error()).context("close residue directory stream");
        }
        Ok(names)
    }

    fn residue_path(root_path: &Path, components: &[OsString]) -> PathBuf {
        components
            .iter()
            .fold(root_path.to_path_buf(), |path, component| {
                path.join(component)
            })
    }

    fn regular_file_identity(file: &mut fs::File) -> Result<ResidueEntryIdentity> {
        let before = file.metadata()?;
        if !before.is_file() {
            anyhow::bail!("cleanup residue entry changed from a regular file");
        }
        file.rewind()?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        let after = file.metadata()?;
        let metadata_matches = before.dev() == after.dev()
            && before.ino() == after.ino()
            && before.len() == after.len()
            && before.mtime() == after.mtime()
            && before.mtime_nsec() == after.mtime_nsec()
            && before.ctime() == after.ctime()
            && before.ctime_nsec() == after.ctime_nsec();
        if !metadata_matches {
            anyhow::bail!("cleanup residue regular file changed while reading");
        }
        Ok(ResidueEntryIdentity::RegularFile {
            device: after.dev(),
            inode: after.ino(),
            length: after.len(),
            modified_seconds: after.mtime(),
            modified_nanoseconds: after.mtime_nsec(),
            changed_seconds: after.ctime(),
            changed_nanoseconds: after.ctime_nsec(),
            digest: digest.finalize().into(),
        })
    }

    fn inspect_residue_tree(
        root: &fs::File,
        root_path: &Path,
        name: &OsStr,
        allow_regular_files: bool,
    ) -> Result<ResidueTree> {
        let mut pending = vec![vec![name.to_os_string()]];
        let mut directories = Vec::new();
        let mut files = Vec::new();
        while let Some(components) = pending.pop() {
            let path = residue_path(root_path, &components);
            let directory = open_directory_path(root, &components).with_context(|| {
                format!(
                    "cleanup residue contains a non-directory or symlink entry: {}",
                    path.display()
                )
            })?;
            let identity = directory_identity(&directory)?;
            let child_names = directory_names(&directory)?;
            for child_name in &child_names {
                if child_name == OsStr::new(".git") || child_name == OsStr::new(".rift") {
                    anyhow::bail!(
                        "cleanup residue contains a Git or Rift marker: {}",
                        path.join(child_name).display()
                    );
                }
                let mut child_components = components.clone();
                child_components.push(child_name.clone());
                match residue_entry_type(&directory, child_name).with_context(|| {
                    format!(
                        "cleanup residue contains a symlink or special entry: {}",
                        path.join(child_name).display()
                    )
                })? {
                    ResidueEntryType::Directory => pending.push(child_components),
                    ResidueEntryType::RegularFile => {
                        if !allow_regular_files {
                            anyhow::bail!(
                                "cleanup residue contains a non-directory entry: {}",
                                path.join(child_name).display()
                            );
                        }
                        let mut file =
                            open_file_at(&directory, child_name, "cleanup residue regular file")
                                .with_context(|| {
                                    format!(
                                        "cleanup residue contains a symlink or special entry: {}",
                                        path.join(child_name).display()
                                    )
                                })?;
                        files.push(RegularFileNode {
                            components: child_components,
                            identity: regular_file_identity(&mut file).with_context(|| {
                                format!(
                                    "inspect cleanup residue regular file {}",
                                    path.join(child_name).display()
                                )
                            })?,
                        });
                    }
                }
            }
            directories.push(DirectoryNode {
                components,
                identity,
                child_names,
            });
        }
        let tree = ResidueTree { directories, files };
        verify_residue_tree(root, root_path, &tree)?;
        Ok(tree)
    }

    fn verify_residue_tree(root: &fs::File, root_path: &Path, tree: &ResidueTree) -> Result<()> {
        for node in &tree.directories {
            let path = residue_path(root_path, &node.components);
            let directory = open_directory_path(root, &node.components)
                .with_context(|| format!("cleanup residue changed: {}", path.display()))?;
            if directory_identity(&directory)? != node.identity
                || directory_names(&directory)? != node.child_names
            {
                anyhow::bail!(
                    "cleanup residue identity or contents changed: {}",
                    path.display()
                );
            }
        }
        for node in &tree.files {
            let path = residue_path(root_path, &node.components);
            let (name, parent_components) = node
                .components
                .split_last()
                .context("cleanup residue file has no name")?;
            let parent = open_directory_path(root, parent_components)
                .with_context(|| format!("cleanup residue parent changed: {}", path.display()))?;
            let mut file = open_file_at(&parent, name, "cleanup residue regular file")
                .with_context(|| format!("cleanup residue changed: {}", path.display()))?;
            if regular_file_identity(&mut file)? != node.identity {
                anyhow::bail!(
                    "cleanup residue identity or contents changed: {}",
                    path.display()
                );
            }
        }
        Ok(())
    }

    fn residue_tree_digest(tree: &ResidueTree) -> [u8; 32] {
        let mut digest = Sha256::new();
        for node in &tree.directories {
            digest.update(b"directory\0");
            for component in node.components.iter().skip(1) {
                digest.update((component.as_bytes().len() as u64).to_le_bytes());
                digest.update(component.as_bytes());
            }
            digest.update(node.identity.device.to_le_bytes());
            digest.update(node.identity.inode.to_le_bytes());
            for child in &node.child_names {
                digest.update((child.as_bytes().len() as u64).to_le_bytes());
                digest.update(child.as_bytes());
            }
        }
        for node in &tree.files {
            digest.update(b"file\0");
            for component in node.components.iter().skip(1) {
                digest.update((component.as_bytes().len() as u64).to_le_bytes());
                digest.update(component.as_bytes());
            }
            digest.update(serde_json::to_vec(&node.identity).expect("serialize file identity"));
        }
        digest.finalize().into()
    }

    fn next_residue_child(tree: &ResidueTree) -> Result<ResidueChildMove> {
        if let Some(node) = tree.files.iter().max_by_key(|node| node.components.len()) {
            let (name, parent_components) = node
                .components
                .split_last()
                .context("cleanup residue file has no name")?;
            return Ok(child_move(parent_components, name, node.identity.clone()));
        }
        let node = tree
            .directories
            .iter()
            .max_by_key(|node| node.components.len())
            .context("cleanup residue tree has no root directory")?;
        let (name, parent_components) = node
            .components
            .split_last()
            .context("cleanup residue directory has no name")?;
        Ok(child_move(
            parent_components,
            name,
            ResidueEntryIdentity::Directory {
                device: node.identity.device,
                inode: node.identity.inode,
            },
        ))
    }

    fn remove_residue_tree<U>(
        root: &fs::File,
        root_path: &Path,
        quarantine_name: &OsStr,
        mut expected_digest: [u8; 32],
        pending_move: Option<ResidueChildMove>,
        mut update_move: U,
    ) -> Result<()>
    where
        U: FnMut([u8; 32], Option<&ResidueChildMove>) -> Result<()>,
    {
        if let Some(movement) = pending_move {
            resolve_child_move(root, root_path, &movement)?;
            update_move(movement.remaining_tree_digest, None)?;
        }
        loop {
            if !entry_exists_at(root, quarantine_name)? {
                return Ok(());
            }
            let tree = inspect_residue_tree(root, root_path, quarantine_name, true)?;
            if residue_tree_digest(&tree) != expected_digest {
                anyhow::bail!(
                    "cleanup residue quarantine differs from its durable authorized contents"
                );
            }
            let mut movement = next_residue_child(&tree)?;
            let next_digest = if movement.parent_components.is_empty() {
                Sha256::digest([]).into()
            } else {
                let mut remaining = inspect_residue_tree(root, root_path, quarantine_name, true)?;
                let target_components = os_components(&movement.parent_components)
                    .into_iter()
                    .chain(std::iter::once(OsString::from_vec(
                        movement.original_name.clone(),
                    )))
                    .collect::<Vec<_>>();
                remaining
                    .files
                    .retain(|node| node.components != target_components);
                remaining
                    .directories
                    .retain(|node| node.components != target_components);
                if let Some(parent) = remaining
                    .directories
                    .iter_mut()
                    .find(|node| node.components == os_components(&movement.parent_components))
                {
                    parent
                        .child_names
                        .retain(|name| name.as_bytes() != movement.original_name.as_slice());
                }
                residue_tree_digest(&remaining)
            };
            movement.remaining_tree_digest = next_digest;
            update_move(expected_digest, Some(&movement))?;
            resolve_child_move(root, root_path, &movement)?;
            expected_digest = next_digest;
            update_move(expected_digest, None)?;
        }
    }

    fn remove_empty_directory_tree(
        root: &fs::File,
        root_path: &Path,
        tree: &ResidueTree,
    ) -> Result<()> {
        let mut nodes = tree.directories.iter().collect::<Vec<_>>();
        nodes.sort_by_key(|node| std::cmp::Reverse(node.components.len()));
        for node in nodes {
            let path = residue_path(root_path, &node.components);
            let (name, parent_components) = node
                .components
                .split_last()
                .context("cleanup residue node has no name")?;
            let parent = open_directory_path(root, parent_components)
                .with_context(|| format!("cleanup residue parent changed: {}", path.display()))?;
            let directory = open_directory_child(&parent, name)
                .with_context(|| format!("cleanup residue changed: {}", path.display()))?;
            if directory_identity(&directory)? != node.identity
                || !directory_names(&directory)?.is_empty()
            {
                anyhow::bail!(
                    "cleanup residue identity or contents changed during removal: {}",
                    path.display()
                );
            }
            let name = child_name(name, "cleanup residue")?;
            if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0
            {
                return Err(std::io::Error::last_os_error())
                    .with_context(|| format!("remove empty cleanup residue {}", path.display()));
            }
        }
        Ok(())
    }

    fn ensure_directory_child(parent: &fs::File, name: &OsStr) -> Result<fs::File> {
        let name = child_name(name, "directory")?;
        let created = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        if created != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(error).context("create evidence directory");
            }
        }
        open_directory_child(parent, OsStr::from_bytes(name.as_bytes()))
    }

    fn create_file_at(directory: &fs::File, name: &OsStr, label: &str) -> Result<fs::File> {
        let name = child_name(name, label)?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| format!("create {label}"));
        }
        let file = unsafe { fs::File::from_raw_fd(fd) };
        if !file.metadata()?.is_file() {
            anyhow::bail!("{label} must be a regular file");
        }
        Ok(file)
    }

    fn open_file_at(directory: &fs::File, name: &OsStr, label: &str) -> Result<fs::File> {
        let name = child_name(name, label)?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| format!("open {label}"));
        }
        let file = unsafe { fs::File::from_raw_fd(fd) };
        if !file.metadata()?.is_file() {
            anyhow::bail!("{label} must be a regular file");
        }
        Ok(file)
    }

    fn remove_file_at(directory: &fs::File, name: &OsStr, label: &str) -> Result<()> {
        let name = child_name(name, label)?;
        if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } != 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| format!("remove {label}"));
        }
        Ok(())
    }

    fn rename_child_noreplace(
        parent: &fs::File,
        from: &OsStr,
        to: &OsStr,
        label: &str,
    ) -> Result<()> {
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
        let result = {
            anyhow::bail!("atomic no-replace quarantine rename is unsupported on this platform")
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| format!("rename {label}"));
        }
        Ok(())
    }

    fn entry_identity(parent: &fs::File, name: &OsStr) -> Result<ResidueEntryIdentity> {
        match residue_entry_type(parent, name)? {
            ResidueEntryType::Directory => {
                let directory = open_directory_child(parent, name)?;
                let identity = directory_identity(&directory)?;
                Ok(ResidueEntryIdentity::Directory {
                    device: identity.device,
                    inode: identity.inode,
                })
            }
            ResidueEntryType::RegularFile => {
                let mut file = open_file_at(parent, name, "cleanup residue regular file")?;
                regular_file_identity(&mut file)
            }
        }
    }

    fn moved_entry_identity_matches(
        expected: &ResidueEntryIdentity,
        actual: &ResidueEntryIdentity,
    ) -> bool {
        match (expected, actual) {
            (
                ResidueEntryIdentity::Directory {
                    device: expected_device,
                    inode: expected_inode,
                },
                ResidueEntryIdentity::Directory {
                    device: actual_device,
                    inode: actual_inode,
                },
            ) => expected_device == actual_device && expected_inode == actual_inode,
            (
                ResidueEntryIdentity::RegularFile {
                    device: expected_device,
                    inode: expected_inode,
                    length: expected_length,
                    modified_seconds: expected_modified_seconds,
                    modified_nanoseconds: expected_modified_nanoseconds,
                    digest: expected_digest,
                    ..
                },
                ResidueEntryIdentity::RegularFile {
                    device: actual_device,
                    inode: actual_inode,
                    length: actual_length,
                    modified_seconds: actual_modified_seconds,
                    modified_nanoseconds: actual_modified_nanoseconds,
                    digest: actual_digest,
                    ..
                },
            ) => {
                expected_device == actual_device
                    && expected_inode == actual_inode
                    && expected_length == actual_length
                    && expected_modified_seconds == actual_modified_seconds
                    && expected_modified_nanoseconds == actual_modified_nanoseconds
                    && expected_digest == actual_digest
            }
            _ => false,
        }
    }

    fn os_components(components: &[Vec<u8>]) -> Vec<OsString> {
        components
            .iter()
            .map(|component| OsString::from_vec(component.clone()))
            .collect()
    }

    fn child_move(
        parent_components: &[OsString],
        original_name: &OsStr,
        identity: ResidueEntryIdentity,
    ) -> ResidueChildMove {
        ResidueChildMove {
            parent_components: parent_components
                .iter()
                .map(|component| component.as_bytes().to_vec())
                .collect(),
            original_name: original_name.as_bytes().to_vec(),
            quarantine_name: format!(".iq-residue-entry-{}", Uuid::new_v4()),
            identity,
            remaining_tree_digest: [0; 32],
        }
    }

    fn resolve_child_move(
        root: &fs::File,
        root_path: &Path,
        movement: &ResidueChildMove,
    ) -> Result<bool> {
        let parent_components = os_components(&movement.parent_components);
        let parent = open_directory_path(root, &parent_components).with_context(|| {
            format!(
                "open cleanup quarantine parent {}",
                residue_path(root_path, &parent_components).display()
            )
        })?;
        let original_name = OsStr::from_bytes(&movement.original_name);
        let quarantine_name = OsStr::new(&movement.quarantine_name);
        let original_exists = entry_exists_at(&parent, original_name)?;
        let quarantine_exists = entry_exists_at(&parent, quarantine_name)?;
        if !original_exists && !quarantine_exists {
            return Ok(false);
        }
        if !quarantine_exists {
            if entry_identity(&parent, original_name)? != movement.identity {
                anyhow::bail!("cleanup residue child identity changed before quarantine rename");
            }
            rename_child_noreplace(
                &parent,
                original_name,
                quarantine_name,
                "cleanup residue child into quarantine",
            )?;
        }
        if entry_exists_at(&parent, original_name)? {
            anyhow::bail!(
                "cleanup residue child name became occupied after quarantine rename; replacement was preserved"
            );
        }
        let moved_identity = entry_identity(&parent, quarantine_name)?;
        if !moved_entry_identity_matches(&movement.identity, &moved_identity) {
            if !entry_exists_at(&parent, original_name)? {
                let _ = rename_child_noreplace(
                    &parent,
                    quarantine_name,
                    original_name,
                    "unverified cleanup residue child back to its original name",
                );
            }
            anyhow::bail!(
                "quarantined cleanup residue child identity mismatch; unverified content was preserved"
            );
        }
        match &movement.identity {
            ResidueEntryIdentity::RegularFile { .. } => {
                remove_file_at(
                    &parent,
                    quarantine_name,
                    "verified quarantined residue file",
                )?;
            }
            ResidueEntryIdentity::Directory { .. } => {
                let directory = open_directory_child(&parent, quarantine_name)?;
                if !directory_names(&directory)?.is_empty() {
                    anyhow::bail!("verified quarantined residue directory is not empty");
                }
                let name = child_name(quarantine_name, "verified quarantined residue directory")?;
                if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) }
                    != 0
                {
                    return Err(std::io::Error::last_os_error())
                        .context("remove verified quarantined residue directory");
                }
            }
        }
        Ok(original_exists)
    }

    fn entry_exists_at(parent: &fs::File, name: &OsStr) -> Result<bool> {
        let name = child_name(name, "cleanup residue")?;
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
            Err(error).context("inspect cleanup residue entry")
        }
    }

    fn acquire_exclusive_lock(path: &Path, label: &str) -> Result<fs::File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("open {label} {}", path.display()))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("inspect {label} {}", path.display()))?;
        if !metadata.is_file() {
            anyhow::bail!("{label} must be a regular file: {}", path.display());
        }
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("acquire exclusive {label} {}", path.display()));
        }
        Ok(file)
    }

    fn acquire_root_lock(path: &Path) -> Result<fs::File> {
        const ROOT_LOCK_TIMEOUT: StdDuration = StdDuration::from_secs(5);
        const ROOT_LOCK_RETRY: StdDuration = StdDuration::from_millis(10);

        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("open IQ workspace root lock {}", path.display()))?;
        if !file.metadata()?.is_dir() {
            anyhow::bail!(
                "IQ workspace root lock is not a directory: {}",
                path.display()
            );
        }
        let deadline = Instant::now() + ROOT_LOCK_TIMEOUT;
        loop {
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(file);
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EWOULDBLOCK) || Instant::now() >= deadline {
                return Err(error).with_context(|| {
                    format!(
                        "acquire exclusive IQ workspace root lock {} within {} seconds",
                        path.display(),
                        ROOT_LOCK_TIMEOUT.as_secs()
                    )
                });
            }
            thread::sleep(ROOT_LOCK_RETRY);
        }
    }

    fn resolve_path_without_creating(path: &Path) -> Result<PathBuf> {
        let mut existing = path;
        let mut missing = Vec::new();
        while !entry_exists(existing)? {
            missing.push(
                existing
                    .file_name()
                    .context("configured path has no existing ancestor")?
                    .to_os_string(),
            );
            existing = existing
                .parent()
                .context("configured path has no existing ancestor")?;
        }
        let mut resolved = existing
            .canonicalize()
            .with_context(|| format!("resolve existing path ancestor {}", existing.display()))?;
        for component in missing.into_iter().rev() {
            resolved.push(component);
        }
        Ok(resolved)
    }

    impl RiftWorkspaceManager {
        fn inspect(
            source: PathBuf,
            root: PathBuf,
            repo_key: String,
            database: Option<PathBuf>,
            queue_database_id: &str,
            workspace_generation: i64,
        ) -> Result<()> {
            if root.starts_with(&source) {
                anyhow::bail!(
                    "IQ workspace root {} must be outside Rift source {}",
                    root.display(),
                    source.display()
                );
            }
            let root_exists = entry_exists(&root)?;
            let root = if root_exists {
                let metadata = fs::symlink_metadata(&root)
                    .with_context(|| format!("inspect IQ workspace root {}", root.display()))?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    anyhow::bail!(
                        "IQ workspace root must be a real directory: {}",
                        root.display()
                    );
                }
                root.canonicalize()
                    .with_context(|| format!("resolve IQ workspace root {}", root.display()))?
            } else {
                resolve_path_without_creating(&root)?
            };
            if root.starts_with(&source) {
                anyhow::bail!(
                    "IQ workspace root {} resolves inside Rift source {}",
                    root.display(),
                    source.display()
                );
            }
            let mut filesystem_probe = root.as_path();
            while !entry_exists(filesystem_probe)? {
                filesystem_probe = filesystem_probe
                    .parent()
                    .context("IQ workspace root has no existing ancestor")?;
            }
            require_same_filesystem(&source, filesystem_probe)?;
            let source_id = Self::read_marker_id(&source)?;
            let (database, registry_identity, _, _) = resolve_rift_database(database)?;
            if !source.join(".git").is_dir() {
                anyhow::bail!(
                    "repository {} must be a primary Git checkout, not a linked worktree",
                    source.display()
                );
            }
            let program = std::env::var("IQ_RIFT_CLI").unwrap_or_else(|_| "rift".into());
            let mut args = Vec::new();
            if let Some(database) = database.as_ref() {
                args.push(OsString::from("--database"));
                args.push(database.clone());
            }
            args.extend([OsString::from("ancestors"), source.as_os_str().into()]);
            let ancestors = command_output_timeout(
                &program,
                args,
                None,
                StdDuration::from_secs(60),
                |gate| {
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || Ok(ExecutionAuthority::Active),
            )?;
            let ancestors = match ancestors {
                CommandOutputOutcome::Exited(output) if output.status.success() => output,
                CommandOutputOutcome::Exited(output) => anyhow::bail!(
                    "verify Rift source root failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                CommandOutputOutcome::Cancelled => {
                    anyhow::bail!("verify Rift source root lost command authority")
                }
            };
            if !String::from_utf8_lossy(&ancestors.stdout).trim().is_empty() {
                anyhow::bail!(
                    "repository {} is a child Rift; IQ requires an independently managed Rift root",
                    source.display()
                );
            }
            if root_exists {
                let marker = root.join(".iq-workspace-owner.json");
                if entry_exists(&marker)? {
                    let owner: RiftWorkspaceRootOwner = serde_json::from_slice(&read_regular_file(
                        &marker,
                        "IQ workspace owner marker",
                    )?)
                    .with_context(|| format!("parse {}", marker.display()))?;
                    if owner.version != 3
                        || owner.queue_database_id != queue_database_id
                        || owner.repo_key != repo_key
                        || owner.source != source
                        || owner.source_rift_id != source_id
                        || owner.registry_identity != registry_identity
                    {
                        anyhow::bail!(
                            "IQ workspace root {} is owned by incompatible configuration",
                            root.display()
                        );
                    }
                    let generation_path = root.join(".iq-workspace-generation");
                    let generation = String::from_utf8(read_regular_file(
                        &generation_path,
                        "IQ workspace generation",
                    )?)?
                    .trim()
                    .parse::<i64>()
                    .context("parse IQ workspace generation")?;
                    if generation != workspace_generation {
                        anyhow::bail!(
                            "queue database generation {workspace_generation} differs from IQ workspace root generation {generation}"
                        );
                    }
                } else if fs::read_dir(&root)?.next().transpose()?.is_some() {
                    anyhow::bail!(
                        "refusing non-empty unowned IQ workspace root {}",
                        root.display()
                    );
                }
                if entry_exists(&marker)? {
                    let mut list_args = Vec::new();
                    if let Some(database) = database.as_ref() {
                        list_args.push(OsString::from("--database"));
                        list_args.push(database.clone());
                    }
                    list_args.extend([OsString::from("list"), source.as_os_str().into()]);
                    let listed = command_output_timeout(
                        &program,
                        list_args,
                        None,
                        StdDuration::from_secs(60),
                        |gate| {
                            gate.write_all(b"run\n")?;
                            Ok(true)
                        },
                        || Ok(ExecutionAuthority::Active),
                    )?;
                    let listed = match listed {
                        CommandOutputOutcome::Exited(output) if output.status.success() => output,
                        CommandOutputOutcome::Exited(output) => anyhow::bail!(
                            "list source Rifts failed: {}",
                            String::from_utf8_lossy(&output.stderr).trim()
                        ),
                        CommandOutputOutcome::Cancelled => {
                            anyhow::bail!("list source Rifts lost command authority")
                        }
                    };
                    let listed = String::from_utf8(listed.stdout)?
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .map(|line| {
                            let path = PathBuf::from(line);
                            let metadata = fs::symlink_metadata(&path).with_context(|| {
                                format!("inspect listed Rift workspace {}", path.display())
                            })?;
                            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                                anyhow::bail!(
                                    "listed Rift workspace must be a real directory: {}",
                                    path.display()
                                );
                            }
                            path.canonicalize().with_context(|| {
                                format!("resolve listed Rift workspace {}", path.display())
                            })
                        })
                        .collect::<Result<HashSet<_>>>()?;
                    for entry in fs::read_dir(&root)? {
                        let path = entry?.path();
                        if matches!(
                            path.file_name(),
                            Some(name)
                                if name == OsStr::new(".iq-workspace-owner.json")
                                    || name == OsStr::new(".iq-workspace-generation")
                        ) {
                            continue;
                        }
                        if is_rift_workspace_root_entry(&path)? {
                            continue;
                        }
                        if !listed.contains(&path) {
                            anyhow::bail!(
                                "IQ workspace root contains unknown entry {}",
                                path.display()
                            );
                        }
                    }
                }
            }
            Ok(())
        }

        pub(crate) fn new(
            source: PathBuf,
            root: PathBuf,
            repo_key: String,
            database: Option<PathBuf>,
            queue_database_id: &str,
            workspace_generation: i64,
        ) -> Result<Self> {
            Self::new_with_source_requirement(
                source,
                root,
                repo_key,
                database,
                queue_database_id,
                workspace_generation,
                true,
            )
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) fn new_child_source(
            source: PathBuf,
            root: PathBuf,
            repo_key: String,
            database: Option<PathBuf>,
            queue_database_id: &str,
            workspace_generation: i64,
        ) -> Result<Self> {
            Self::new_with_source_requirement(
                source,
                root,
                repo_key,
                database,
                queue_database_id,
                workspace_generation,
                false,
            )
        }

        #[allow(clippy::too_many_arguments)]
        fn new_with_source_requirement(
            source: PathBuf,
            root: PathBuf,
            repo_key: String,
            database: Option<PathBuf>,
            queue_database_id: &str,
            workspace_generation: i64,
            require_root_source: bool,
        ) -> Result<Self> {
            if root.starts_with(&source) {
                anyhow::bail!(
                    "IQ workspace root {} must be outside Rift source {}",
                    root.display(),
                    source.display()
                );
            }
            if entry_exists(&root)? {
                let metadata = fs::symlink_metadata(&root)
                    .with_context(|| format!("inspect IQ workspace root {}", root.display()))?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    anyhow::bail!(
                        "IQ workspace root must be a real directory: {}",
                        root.display()
                    );
                }
            }
            fs::create_dir_all(&root)
                .with_context(|| format!("create IQ workspace root {}", root.display()))?;
            let root = root
                .canonicalize()
                .with_context(|| format!("resolve IQ workspace root {}", root.display()))?;
            if root.starts_with(&source) {
                anyhow::bail!(
                    "IQ workspace root {} resolves inside Rift source {}",
                    root.display(),
                    source.display()
                );
            }
            require_same_filesystem(&source, &root)?;
            let source_id = Self::read_marker_id(&source)?;
            let (database, registry_identity, registry_dev, registry_ino) =
                resolve_rift_database(database)?;
            let root_directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
                .open(&root)
                .with_context(|| format!("open IQ workspace root {}", root.display()))?;
            let mut manager = Self {
                source,
                source_id,
                source_ancestors: Vec::new(),
                root,
                repo_key,
                queue_database_id: queue_database_id.to_string(),
                registry_identity,
                registry_dev,
                registry_ino,
                generation: AtomicI64::new(0),
                program: std::env::var("IQ_RIFT_CLI").unwrap_or_else(|_| "rift".into()),
                database,
                root_directory,
            };
            manager.source_ancestors = manager.verify_source(require_root_source)?;
            {
                let _root_lock = acquire_root_lock(&manager.root)?;
                manager.ensure_root_owner()?;
                manager.synchronize_generation_unlocked(workspace_generation)?;
            }
            Ok(manager)
        }

        pub(crate) fn expected_path(&self, item_id: &str) -> Result<PathBuf> {
            self.verify_root_identity()?;
            if item_id.is_empty()
                || !item_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            {
                anyhow::bail!("invalid queue item ID for Rift workspace: {item_id}");
            }
            Ok(self.root.join(item_id))
        }

        pub(crate) fn new_residue_quarantine_name(&self, item_id: &str) -> Result<String> {
            self.expected_path(item_id)?;
            loop {
                let name = format!(".iq-residue-quarantine-{item_id}-{}", Uuid::new_v4());
                if !entry_exists_at(&self.root_directory, OsStr::new(&name))? {
                    return Ok(name);
                }
            }
        }

        pub(crate) fn source_id(&self) -> &str {
            &self.source_id
        }

        pub(crate) fn registry_identity(&self) -> &str {
            &self.registry_identity
        }

        pub(crate) fn root(&self) -> &Path {
            &self.root
        }

        fn verify_source(&self, require_root: bool) -> Result<Vec<PathBuf>> {
            if !self.source.join(".git").is_dir() {
                anyhow::bail!(
                    "repository {} must be a primary Git checkout, not a linked worktree",
                    self.source.display()
                );
            }
            let marker = self.source.join(".rift");
            if !entry_exists(&marker)? {
                anyhow::bail!(
                    "repository {} is not a Rift root; provision it with `rift init --here {}` before starting IQ",
                    self.source.display(),
                    self.source.display()
                );
            }
            read_regular_file(&marker, "Rift identity marker")?;
            let ancestors = self.run(
                [OsString::from("ancestors"), self.source.as_os_str().into()],
                "verify Rift source root",
            )?;
            let ancestors = String::from_utf8(ancestors.stdout)?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>();
            if require_root && !ancestors.is_empty() {
                anyhow::bail!(
                    "repository {} is a child Rift; IQ requires an independently managed Rift root",
                    self.source.display()
                );
            }
            Ok(ancestors)
        }

        fn verify_root_identity(&self) -> Result<()> {
            let path_metadata = fs::symlink_metadata(&self.root)
                .with_context(|| format!("inspect IQ workspace root {}", self.root.display()))?;
            let locked_metadata = self.root_directory.metadata().with_context(|| {
                format!("inspect locked IQ workspace root {}", self.root.display())
            })?;
            if path_metadata.file_type().is_symlink()
                || !path_metadata.is_dir()
                || path_metadata.dev() != locked_metadata.dev()
                || path_metadata.ino() != locked_metadata.ino()
            {
                anyhow::bail!(
                    "IQ workspace root identity changed while running: {}",
                    self.root.display()
                );
            }
            let owner = RiftWorkspaceRootOwner {
                version: 3,
                queue_database_id: self.queue_database_id.clone(),
                repo_key: self.repo_key.clone(),
                source: self.source.clone(),
                source_rift_id: self.source_id.clone(),
                registry_identity: self.registry_identity.clone(),
            };
            let marker = self.root.join(".iq-workspace-owner.json");
            let actual: RiftWorkspaceRootOwner =
                serde_json::from_slice(&read_regular_file(&marker, "IQ workspace owner marker")?)?;
            if actual != owner {
                anyhow::bail!(
                    "IQ workspace root ownership changed while running: {}",
                    self.root.display()
                );
            }
            let persisted_generation = self
                .read_generation()?
                .context("IQ workspace root generation is missing")?;
            let expected_generation = self.generation.load(Ordering::Acquire);
            if persisted_generation != expected_generation {
                anyhow::bail!(
                    "IQ workspace root generation changed from {expected_generation} to {persisted_generation}"
                );
            }
            Ok(())
        }

        fn generation_path(&self) -> PathBuf {
            self.root.join(".iq-workspace-generation")
        }

        fn read_generation(&self) -> Result<Option<i64>> {
            let path = self.generation_path();
            if !entry_exists(&path)? {
                return Ok(None);
            }
            let raw = String::from_utf8(read_regular_file(&path, "IQ workspace generation")?)?;
            let generation = raw
                .trim()
                .parse::<i64>()
                .context("parse IQ workspace generation")?;
            if generation < 0 {
                anyhow::bail!("IQ workspace generation must not be negative");
            }
            Ok(Some(generation))
        }

        fn write_generation(&self, generation: i64) -> Result<()> {
            if generation < 0 {
                anyhow::bail!("IQ workspace generation must not be negative");
            }
            let path = self.generation_path();
            if entry_exists(&path)? {
                read_regular_file(&path, "IQ workspace generation")?;
            }
            let temporary = self
                .root
                .join(format!(".iq-workspace-generation-{}.tmp", Uuid::new_v4()));
            fs::write(&temporary, format!("{generation}\n"))?;
            fs::rename(&temporary, &path)?;
            Ok(())
        }

        fn synchronize_generation_unlocked(&self, database_generation: i64) -> Result<()> {
            for entry in fs::read_dir(&self.root)? {
                let entry = entry?;
                if !entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with(".iq-workspace-generation-") && name.ends_with(".tmp")
                }) {
                    continue;
                }
                let raw = String::from_utf8(read_regular_file(
                    &entry.path(),
                    "temporary IQ workspace generation",
                )?)?;
                let temporary_generation = raw
                    .trim()
                    .parse::<i64>()
                    .context("parse temporary IQ workspace generation")?;
                if temporary_generation > database_generation {
                    anyhow::bail!(
                        "queue database generation {database_generation} trails temporary IQ workspace generation {temporary_generation}"
                    );
                }
                fs::remove_file(entry.path())?;
            }
            match self.read_generation()? {
                Some(root_generation) if root_generation > database_generation => {
                    anyhow::bail!(
                        "queue database generation {database_generation} trails IQ workspace root generation {root_generation}; refuse stale-database cleanup"
                    )
                }
                Some(root_generation) if root_generation < database_generation => {
                    self.write_generation(database_generation)?;
                }
                None => self.write_generation(database_generation)?,
                Some(_) => {}
            }
            self.generation
                .store(database_generation, Ordering::Release);
            Ok(())
        }

        fn synchronize_generation(&self, database_generation: i64) -> Result<()> {
            let _root_lock = acquire_root_lock(&self.root)?;
            self.synchronize_generation_unlocked(database_generation)
        }

        pub(crate) fn persist_generation(&self, generation: i64) -> Result<()> {
            let _root_lock = acquire_root_lock(&self.root)?;
            self.verify_root_identity()?;
            let current = self.generation.load(Ordering::Acquire);
            if generation != current + 1 {
                anyhow::bail!(
                    "workspace generation advanced from {current} to unexpected {generation}"
                );
            }
            self.write_generation(generation)?;
            self.generation.store(generation, Ordering::Release);
            Ok(())
        }

        fn ensure_root_owner(&self) -> Result<()> {
            let path = self.root.join(".iq-workspace-owner.json");
            let expected = RiftWorkspaceRootOwner {
                version: 3,
                queue_database_id: self.queue_database_id.clone(),
                repo_key: self.repo_key.clone(),
                source: self.source.clone(),
                source_rift_id: self.source_id.clone(),
                registry_identity: self.registry_identity.clone(),
            };
            if entry_exists(&path)? {
                let actual: RiftWorkspaceRootOwner =
                    serde_json::from_slice(&read_regular_file(&path, "IQ workspace owner marker")?)
                        .with_context(|| format!("parse {}", path.display()))?;
                if actual != expected {
                    anyhow::bail!(
                        "IQ workspace root {} is owned by a different repository queue or Rift registry",
                        self.root.display()
                    );
                }
                for entry in fs::read_dir(&self.root)? {
                    let entry = entry?;
                    let file_name = entry.file_name();
                    if !file_name.to_str().is_some_and(|name| {
                        name.starts_with(".iq-workspace-owner-") && name.ends_with(".tmp")
                    }) {
                        continue;
                    }
                    let temporary_owner: RiftWorkspaceRootOwner = serde_json::from_slice(
                        &read_regular_file(&entry.path(), "temporary IQ workspace owner marker")?,
                    )?;
                    if temporary_owner != expected {
                        anyhow::bail!(
                            "IQ workspace root contains mismatched temporary owner marker {}",
                            entry.path().display()
                        );
                    }
                    fs::remove_file(entry.path())?;
                }
                return Ok(());
            }
            let entries = fs::read_dir(&self.root)
                .with_context(|| {
                    format!("inspect unowned IQ workspace root {}", self.root.display())
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if let [entry] = entries.as_slice() {
                let file_name = entry.file_name();
                if file_name.to_str().is_some_and(|name| {
                    name.starts_with(".iq-workspace-owner-") && name.ends_with(".tmp")
                }) {
                    let temporary_owner: RiftWorkspaceRootOwner = serde_json::from_slice(
                        &read_regular_file(&entry.path(), "temporary IQ workspace owner marker")?,
                    )?;
                    if temporary_owner == expected {
                        fs::rename(entry.path(), &path).with_context(|| {
                            format!("recover IQ workspace owner marker {}", path.display())
                        })?;
                        return Ok(());
                    }
                }
            }
            if !entries.is_empty() {
                anyhow::bail!(
                    "refusing to claim non-empty unowned IQ workspace root {}",
                    self.root.display()
                );
            }
            let temporary = self
                .root
                .join(format!(".iq-workspace-owner-{}.tmp", Uuid::new_v4()));
            fs::write(&temporary, serde_json::to_vec_pretty(&expected)?)
                .with_context(|| format!("write {}", temporary.display()))?;
            let publish = fs::hard_link(&temporary, &path);
            fs::remove_file(&temporary)
                .with_context(|| format!("remove {}", temporary.display()))?;
            match publish {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    self.ensure_root_owner()
                }
                Err(error) => Err(error).with_context(|| format!("publish {}", path.display())),
            }
        }

        pub(crate) fn create(
            &self,
            item_id: &str,
            authorize_start: impl FnOnce(&mut dyn Write) -> Result<bool>,
            check_authority: impl FnMut() -> Result<ExecutionAuthority>,
        ) -> Result<(PathBuf, String)> {
            let _root_lock = acquire_root_lock(&self.root)?;
            let _registry_lock = self.acquire_registry_lock()?;
            self.verify_root_identity()?;
            let expected = self.expected_path(item_id)?;
            if entry_exists(&expected)? {
                anyhow::bail!(
                    "Rift workspace already exists before creation: {}",
                    expected.display()
                );
            }
            let output = self.run_supervised(
                [
                    OsString::from("create"),
                    OsString::from("--copy-all"),
                    OsString::from("--no-hooks"),
                    OsString::from("--name"),
                    OsString::from(item_id),
                    OsString::from("--into"),
                    self.root.as_os_str().into(),
                    self.source.as_os_str().into(),
                ],
                "create Rift integration workspace",
                authorize_start,
                check_authority,
            )?;
            let created = PathBuf::from(String::from_utf8(output.stdout)?.trim());
            let created = created
                .canonicalize()
                .with_context(|| format!("resolve created Rift {}", created.display()))?;
            if created != expected {
                anyhow::bail!(
                    "Rift created workspace {}, expected {}",
                    created.display(),
                    expected.display()
                );
            }
            Ok((created.clone(), self.read_id(&created)?))
        }

        pub(crate) fn list(&self) -> Result<Vec<WorkspaceIdentity>> {
            let output = self.run(
                [OsString::from("list"), self.source.as_os_str().into()],
                "list source Rifts",
            )?;
            String::from_utf8(output.stdout)?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| {
                    let listed = PathBuf::from(line);
                    let metadata = fs::symlink_metadata(&listed).with_context(|| {
                        format!("inspect listed Rift workspace {}", listed.display())
                    })?;
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        anyhow::bail!(
                            "listed Rift workspace must be a real directory: {}",
                            listed.display()
                        );
                    }
                    let path = listed
                        .canonicalize()
                        .with_context(|| format!("resolve listed Rift workspace {line}"))?;
                    let ancestors = self.run(
                        [OsString::from("ancestors"), path.as_os_str().into()],
                        "verify integration Rift parent",
                    )?;
                    let ancestors = String::from_utf8(ancestors.stdout)?
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .map(PathBuf::from)
                        .collect::<Vec<_>>();
                    let mut expected_ancestors = vec![self.source.clone()];
                    expected_ancestors.extend(self.source_ancestors.clone());
                    if ancestors != expected_ancestors {
                        anyhow::bail!(
                            "Rift {} is not a direct child of source {}",
                            path.display(),
                            self.source.display()
                        );
                    }
                    Ok(WorkspaceIdentity {
                        path: path
                            .to_str()
                            .context("Rift workspace path is not valid UTF-8")?
                            .to_string(),
                        rift_id: self.read_id(&path)?,
                        source_rift_id: self.source_id.clone(),
                    })
                })
                .collect()
        }

        pub(crate) fn remove_retained<A, C, F>(
            &self,
            identity: &WorkspaceIdentity,
            authorize_mutation: A,
            check_authority: C,
            complete_mutation: F,
        ) -> Result<bool>
        where
            A: FnMut(&mut dyn Write) -> Result<bool>,
            C: FnMut() -> Result<ExecutionAuthority>,
            F: FnOnce() -> Result<()>,
        {
            self.remove(
                Path::new(&identity.path),
                Some(&identity.rift_id),
                Some(&identity.source_rift_id),
                authorize_mutation,
                check_authority,
                complete_mutation,
            )
        }

        pub(crate) fn discard_retained_residue<P, A, C, F>(
            &self,
            request: ResidueDiscardRequest<'_>,
            mut persist_progress: P,
            mut authorize_mutation: A,
            mut check_authority: C,
            complete_mutation: F,
        ) -> Result<()>
        where
            P: FnMut(u64, u64, [u8; 32], Option<&ResidueChildMove>) -> Result<()>,
            A: FnMut(&mut dyn Write) -> Result<bool>,
            C: FnMut() -> Result<ExecutionAuthority>,
            F: FnOnce() -> Result<()>,
        {
            let ResidueDiscardRequest {
                identity,
                quarantine_name,
                inspected_identity,
                pending_child_move,
            } = request;
            let _root_lock = acquire_root_lock(&self.root)?;
            let _registry_lock = self.acquire_registry_lock()?;
            self.verify_root_identity()?;
            if identity.source_rift_id != self.source_id {
                anyhow::bail!(
                    "Rift source identity changed from {} to {}",
                    identity.source_rift_id,
                    self.source_id
                );
            }
            let path = Path::new(&identity.path);
            self.verify_owned_path(path)?;
            if path.parent() != Some(self.root.as_path()) {
                anyhow::bail!(
                    "cleanup residue is not at its exact IQ-owned path: {}",
                    path.display()
                );
            }
            let original_name = path
                .file_name()
                .context("cleanup residue path has no leaf name")?;
            let quarantine_name = OsStr::new(quarantine_name);
            let prefix = format!(
                ".iq-residue-quarantine-{}-",
                original_name
                    .to_str()
                    .context("workspace ID is not valid UTF-8")?
            );
            if !quarantine_name.to_str().is_some_and(|name| {
                name.strip_prefix(&prefix)
                    .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok())
            }) {
                anyhow::bail!("invalid IQ residue quarantine name");
            }
            if let Some(actual) = self
                .list()?
                .into_iter()
                .find(|candidate| candidate.rift_id == identity.rift_id)
            {
                anyhow::bail!(
                    "Rift {} still exists at {}; use normal verified Rift removal",
                    identity.rift_id,
                    actual.path
                );
            }
            let mut pending_child_move = pending_child_move;
            if let Some(movement) = pending_child_move
                .as_ref()
                .filter(|movement| movement.parent_components.is_empty())
            {
                let Some((device, inode, _)) = inspected_identity else {
                    anyhow::bail!("root residue child move exists without durable root identity");
                };
                match check_authority()? {
                    ExecutionAuthority::Active => {}
                    ExecutionAuthority::Cancelled => {
                        anyhow::bail!("cleanup residue discard lost mutation authority")
                    }
                    ExecutionAuthority::Lost(message) => anyhow::bail!(message),
                }
                resolve_child_move(&self.root_directory, &self.root, movement)?;
                persist_progress(device, inode, movement.remaining_tree_digest, None)?;
                pending_child_move = None;
            }
            let original_exists = entry_exists_at(&self.root_directory, original_name)?;
            let quarantine_exists = entry_exists_at(&self.root_directory, quarantine_name)?;
            if original_exists && quarantine_exists {
                anyhow::bail!(
                    "workspace path is occupied while its durable residue quarantine exists: {}",
                    path.display()
                );
            }
            if quarantine_exists && inspected_identity.is_none() {
                anyhow::bail!("unverified residue quarantine exists without durable root identity");
            }
            if !original_exists && !quarantine_exists {
                match check_authority()? {
                    ExecutionAuthority::Active => {}
                    ExecutionAuthority::Cancelled => {
                        anyhow::bail!("cleanup residue discard lost mutation authority")
                    }
                    ExecutionAuthority::Lost(message) => anyhow::bail!(message),
                }
                let mut sink = std::io::sink();
                if !authorize_mutation(&mut sink)? {
                    anyhow::bail!("cleanup residue discard was not authorized");
                }
                if self
                    .list()?
                    .iter()
                    .any(|candidate| candidate.rift_id == identity.rift_id)
                {
                    anyhow::bail!(
                        "Rift {} reappeared before residue-discard garbage collection",
                        identity.rift_id
                    );
                }
                self.gc_unlocked(&mut authorize_mutation, &mut check_authority)?;
                complete_mutation()?;
                return Ok(());
            }
            let (inspected_root, mut expected_tree_digest) = if let Some((device, inode, digest)) =
                inspected_identity
            {
                (DirectoryIdentity { device, inode }, digest)
            } else if original_exists {
                let tree =
                    inspect_residue_tree(&self.root_directory, &self.root, original_name, true)?;
                let root = tree
                    .directories
                    .iter()
                    .find(|node| node.components.as_slice() == [original_name.to_os_string()])
                    .context("cleanup residue inspection has no root")?
                    .identity;
                let digest = residue_tree_digest(&tree);
                persist_progress(root.device, root.inode, digest, None)?;
                (root, digest)
            } else {
                match check_authority()? {
                    ExecutionAuthority::Active => {}
                    ExecutionAuthority::Cancelled => {
                        anyhow::bail!("cleanup residue discard lost mutation authority")
                    }
                    ExecutionAuthority::Lost(message) => anyhow::bail!(message),
                }
                let mut sink = std::io::sink();
                if !authorize_mutation(&mut sink)? {
                    anyhow::bail!("cleanup residue discard was not authorized");
                }
                if self
                    .list()?
                    .iter()
                    .any(|candidate| candidate.rift_id == identity.rift_id)
                {
                    anyhow::bail!(
                        "Rift {} reappeared before residue-discard garbage collection",
                        identity.rift_id
                    );
                }
                self.gc_unlocked(&mut authorize_mutation, &mut check_authority)?;
                complete_mutation()?;
                return Ok(());
            };
            match check_authority()? {
                ExecutionAuthority::Active => {}
                ExecutionAuthority::Cancelled => {
                    anyhow::bail!("cleanup residue discard lost mutation authority")
                }
                ExecutionAuthority::Lost(message) => anyhow::bail!(message),
            }
            let mut sink = std::io::sink();
            if !authorize_mutation(&mut sink)? {
                anyhow::bail!("cleanup residue discard was not authorized");
            }
            if let Some(actual) = self
                .list()?
                .into_iter()
                .find(|candidate| candidate.rift_id == identity.rift_id)
            {
                anyhow::bail!(
                    "Rift {} reappeared at {} before residue discard",
                    identity.rift_id,
                    actual.path
                );
            }
            match check_authority()? {
                ExecutionAuthority::Active => {}
                ExecutionAuthority::Cancelled => {
                    anyhow::bail!("cleanup residue discard lost mutation authority")
                }
                ExecutionAuthority::Lost(message) => anyhow::bail!(message),
            }
            self.verify_root_identity()?;
            if !quarantine_exists {
                let tree =
                    inspect_residue_tree(&self.root_directory, &self.root, original_name, true)?;
                let current_root = tree
                    .directories
                    .iter()
                    .find(|node| node.components.as_slice() == [original_name.to_os_string()])
                    .context("cleanup residue reinspection has no root")?
                    .identity;
                if current_root != inspected_root {
                    anyhow::bail!("cleanup residue root identity changed before quarantine rename");
                }
                if residue_tree_digest(&tree) != expected_tree_digest {
                    anyhow::bail!("cleanup residue contents changed before quarantine rename");
                }
                match check_authority()? {
                    ExecutionAuthority::Active => {}
                    ExecutionAuthority::Cancelled => {
                        anyhow::bail!("cleanup residue discard lost mutation authority")
                    }
                    ExecutionAuthority::Lost(message) => anyhow::bail!(message),
                }
                let mut sink = std::io::sink();
                if !authorize_mutation(&mut sink)? {
                    anyhow::bail!("cleanup residue discard was not authorized");
                }
                if let Some(actual) = self
                    .list()?
                    .into_iter()
                    .find(|candidate| candidate.rift_id == identity.rift_id)
                {
                    anyhow::bail!(
                        "Rift {} reappeared at {} before residue quarantine rename",
                        identity.rift_id,
                        actual.path
                    );
                }
                rename_child_noreplace(
                    &self.root_directory,
                    original_name,
                    quarantine_name,
                    "cleanup residue root into quarantine",
                )?;
            }
            let quarantine = open_directory_child(&self.root_directory, quarantine_name)
                .context("open quarantined cleanup residue root")?;
            if directory_identity(&quarantine)? != inspected_root {
                if !entry_exists_at(&self.root_directory, original_name)? {
                    let _ = rename_child_noreplace(
                        &self.root_directory,
                        quarantine_name,
                        original_name,
                        "unverified cleanup residue root back to its durable path",
                    );
                }
                anyhow::bail!(
                    "quarantined cleanup residue root identity mismatch; unverified content was preserved"
                );
            }
            if entry_exists_at(&self.root_directory, original_name)? {
                anyhow::bail!(
                    "workspace path became occupied after residue quarantine: {}",
                    path.display()
                );
            }
            if let Some(movement) = pending_child_move {
                resolve_child_move(&self.root_directory, &self.root, &movement)?;
                expected_tree_digest = movement.remaining_tree_digest;
                persist_progress(
                    inspected_root.device,
                    inspected_root.inode,
                    expected_tree_digest,
                    None,
                )?;
            }
            remove_residue_tree(
                &self.root_directory,
                &self.root,
                quarantine_name,
                expected_tree_digest,
                None,
                |digest, movement| {
                    match check_authority()? {
                        ExecutionAuthority::Active => {}
                        ExecutionAuthority::Cancelled => {
                            anyhow::bail!("cleanup residue discard lost mutation authority")
                        }
                        ExecutionAuthority::Lost(message) => anyhow::bail!(message),
                    }
                    persist_progress(
                        inspected_root.device,
                        inspected_root.inode,
                        digest,
                        movement,
                    )
                },
            )?;
            self.verify_root_identity()?;
            if entry_exists(path)? {
                anyhow::bail!("cleanup residue remained after discard: {}", path.display());
            }
            self.gc_unlocked(&mut authorize_mutation, &mut check_authority)?;
            complete_mutation()
        }

        pub(crate) fn verify_retained(&self, identity: &WorkspaceIdentity) -> Result<PathBuf> {
            if identity.source_rift_id != self.source_id {
                anyhow::bail!(
                    "Rift source identity changed from {} to {}",
                    identity.source_rift_id,
                    self.source_id
                );
            }
            let path = PathBuf::from(&identity.path);
            self.verify_workspace_path(&path)?;
            let actual_id = self.read_id(&path)?;
            if actual_id != identity.rift_id {
                anyhow::bail!(
                    "Rift identity mismatch at {}: found {actual_id}, expected {}",
                    path.display(),
                    identity.rift_id
                );
            }
            let ancestors = self.run(
                [OsString::from("ancestors"), path.as_os_str().into()],
                "verify retained Rift parent",
            )?;
            let ancestors = String::from_utf8(ancestors.stdout)?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>();
            let mut expected_ancestors = vec![self.source.clone()];
            expected_ancestors.extend(self.source_ancestors.clone());
            if ancestors != expected_ancestors {
                anyhow::bail!(
                    "retained Rift {} is not a direct child of source {}",
                    path.display(),
                    self.source.display()
                );
            }
            Ok(path)
        }

        pub(crate) fn resolve_retained(
            &self,
            identity: &WorkspaceIdentity,
        ) -> Result<Option<WorkspaceIdentity>> {
            if identity.source_rift_id != self.source_id {
                anyhow::bail!(
                    "Rift source identity changed from {} to {}",
                    identity.source_rift_id,
                    self.source_id
                );
            }
            self.verify_owned_path(Path::new(&identity.path))?;
            Ok(self
                .list()?
                .into_iter()
                .find(|candidate| candidate.rift_id == identity.rift_id))
        }

        fn remove<A, C, F>(
            &self,
            path: &Path,
            expected_id: Option<&str>,
            expected_source_id: Option<&str>,
            mut authorize_mutation: A,
            mut check_authority: C,
            complete_mutation: F,
        ) -> Result<bool>
        where
            A: FnMut(&mut dyn Write) -> Result<bool>,
            C: FnMut() -> Result<ExecutionAuthority>,
            F: FnOnce() -> Result<()>,
        {
            let _root_lock = acquire_root_lock(&self.root)?;
            let _registry_lock = self.acquire_registry_lock()?;
            self.verify_root_identity()?;
            if let Some(expected_source_id) = expected_source_id {
                if expected_source_id != self.source_id {
                    anyhow::bail!(
                        "Rift source identity changed from {expected_source_id} to {}",
                        self.source_id
                    );
                }
            }
            let inventory = self.list()?;
            let removal_path = if let Some(expected_id) = expected_id {
                let Some(actual) = inventory
                    .iter()
                    .find(|candidate| candidate.rift_id == expected_id)
                else {
                    if entry_exists(path)? {
                        self.remove_empty_residue(
                            path,
                            expected_id,
                            &mut authorize_mutation,
                            &mut check_authority,
                        )?;
                    }
                    self.gc_unlocked(&mut authorize_mutation, &mut check_authority)?;
                    complete_mutation()?;
                    return Ok(false);
                };
                if Path::new(&actual.path) != path && entry_exists(path)? {
                    anyhow::bail!(
                        "Rift {expected_id} moved to {} but its old path remains occupied: {}",
                        actual.path,
                        path.display(),
                    );
                }
                PathBuf::from(&actual.path)
            } else {
                self.verify_owned_path(path)?;
                if !entry_exists(path)? {
                    return Ok(false);
                }
                path.to_path_buf()
            };
            self.verify_identity_workspace_path(&removal_path)?;
            let id = self.read_id(&removal_path)?;
            if let Some(expected_id) = expected_id {
                if id != expected_id {
                    anyhow::bail!(
                        "Rift identity mismatch at {}: found {id}, expected {expected_id}",
                        removal_path.display()
                    );
                }
            }
            let canonical = removal_path
                .canonicalize()
                .with_context(|| format!("resolve Rift workspace {}", removal_path.display()))?;
            if !inventory
                .iter()
                .any(|candidate| Path::new(&candidate.path) == canonical && candidate.rift_id == id)
            {
                anyhow::bail!(
                    "workspace {} is not a direct Rift child of {}",
                    removal_path.display(),
                    self.source.display()
                );
            }
            self.require_clean_childless_rift(&canonical)?;
            self.run_supervised(
                [OsString::from("remove"), canonical.as_os_str().into()],
                "remove integration Rift",
                |gate| {
                    self.verify_identity_workspace_path(&canonical)?;
                    let current_id = self.read_id(&canonical)?;
                    if current_id != id {
                        anyhow::bail!(
                            "Rift identity changed from {id} to {current_id} before removal"
                        );
                    }
                    let current_inventory = self.list()?;
                    if !current_inventory.iter().any(|candidate| {
                        Path::new(&candidate.path) == canonical && candidate.rift_id == id
                    }) {
                        anyhow::bail!(
                            "Rift {} left source inventory before removal",
                            canonical.display()
                        );
                    }
                    if canonical != path && entry_exists(path)? {
                        anyhow::bail!(
                            "Rift {id} moved to {} but its old path became occupied before removal: {}",
                            canonical.display(),
                            path.display()
                        );
                    }
                    self.require_clean_childless_rift(&canonical)?;
                    authorize_mutation(gate)
                },
                &mut check_authority,
            )?;
            self.gc_unlocked(&mut authorize_mutation, &mut check_authority)?;
            complete_mutation()?;
            if entry_exists(&canonical)? {
                anyhow::bail!(
                    "Rift workspace remained after cleanup: {}",
                    canonical.display()
                );
            }
            Ok(true)
        }

        fn remove_empty_residue<A, C>(
            &self,
            path: &Path,
            expected_id: &str,
            authorize_mutation: &mut A,
            check_authority: &mut C,
        ) -> Result<()>
        where
            A: FnMut(&mut dyn Write) -> Result<bool>,
            C: FnMut() -> Result<ExecutionAuthority>,
        {
            self.verify_owned_path(path)?;
            if path.parent() != Some(self.root.as_path()) {
                anyhow::bail!(
                    "cleanup residue is not at its exact IQ-owned path: {}",
                    path.display()
                );
            }
            let name = path
                .file_name()
                .context("cleanup residue path has no leaf name")?;
            let tree = inspect_residue_tree(&self.root_directory, &self.root, name, false)?;
            verify_residue_tree(&self.root_directory, &self.root, &tree)?;
            match check_authority()? {
                ExecutionAuthority::Active => {}
                ExecutionAuthority::Cancelled => {
                    anyhow::bail!("cleanup residue removal lost mutation authority")
                }
                ExecutionAuthority::Lost(message) => anyhow::bail!(message),
            }
            let mut sink = std::io::sink();
            if !authorize_mutation(&mut sink)? {
                anyhow::bail!("cleanup residue removal was not authorized");
            }
            if self
                .list()?
                .iter()
                .any(|candidate| candidate.rift_id == expected_id)
            {
                anyhow::bail!(
                    "Rift {expected_id} reappeared in source inventory before residue removal"
                );
            }
            match check_authority()? {
                ExecutionAuthority::Active => {}
                ExecutionAuthority::Cancelled => {
                    anyhow::bail!("cleanup residue removal lost mutation authority")
                }
                ExecutionAuthority::Lost(message) => anyhow::bail!(message),
            }
            verify_residue_tree(&self.root_directory, &self.root, &tree)?;
            remove_empty_directory_tree(&self.root_directory, &self.root, &tree)?;
            self.verify_root_identity()?;
            Ok(())
        }

        fn require_clean_childless_rift(&self, path: &Path) -> Result<()> {
            if let Some(dirty) = workspace_dirty(path)? {
                anyhow::bail!(
                    "Rift workspace is dirty; preserved {}: {dirty}",
                    path.display()
                );
            }
            if crate::composition::has_git_operation(path)? {
                anyhow::bail!(
                    "Rift workspace has an active Git operation; preserved: {}",
                    path.display()
                );
            }
            let descendants = self.run(
                [OsString::from("list"), path.as_os_str().into()],
                "list integration Rift descendants",
            )?;
            if !String::from_utf8_lossy(&descendants.stdout)
                .trim()
                .is_empty()
            {
                anyhow::bail!(
                    "refusing to remove IQ Rift {} with child Rifts",
                    path.display()
                );
            }
            Ok(())
        }

        pub(crate) fn gc<A, C, F>(
            &self,
            authorize_mutation: A,
            check_authority: C,
            complete_mutation: F,
        ) -> Result<()>
        where
            A: FnOnce(&mut dyn Write) -> Result<bool>,
            C: FnMut() -> Result<ExecutionAuthority>,
            F: FnOnce() -> Result<()>,
        {
            let _root_lock = acquire_root_lock(&self.root)?;
            let _registry_lock = self.acquire_registry_lock()?;
            self.verify_root_identity()?;
            self.gc_unlocked(authorize_mutation, check_authority)?;
            complete_mutation()
        }

        fn gc_unlocked<A, C>(&self, authorize_mutation: A, check_authority: C) -> Result<()>
        where
            A: FnOnce(&mut dyn Write) -> Result<bool>,
            C: FnMut() -> Result<ExecutionAuthority>,
        {
            self.run_supervised(
                [OsString::from("gc")],
                "garbage-collect removed Rifts",
                authorize_mutation,
                check_authority,
            )?;
            Ok(())
        }

        fn acquire_registry_lock(&self) -> Result<fs::File> {
            let mut lock_path = OsString::from(&self.registry_identity);
            lock_path.push(".iq-mutation.lock");
            let lock =
                acquire_exclusive_lock(Path::new(&lock_path), "Rift registry mutation lock")?;
            self.verify_registry_identity()?;
            Ok(lock)
        }

        fn verify_registry_identity(&self) -> Result<()> {
            let path = Path::new(&self.registry_identity);
            let metadata = fs::symlink_metadata(path)
                .with_context(|| format!("inspect Rift registry database {}", path.display()))?;
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || metadata.dev() != self.registry_dev
                || metadata.ino() != self.registry_ino
            {
                anyhow::bail!(
                    "Rift registry database identity changed while IQ was running: {}",
                    path.display()
                );
            }
            Ok(())
        }

        fn verify_owned_path(&self, path: &Path) -> Result<()> {
            let normalized = self.normalize_owned_path(path)?;
            let parent = normalized
                .parent()
                .context("normalized IQ workspace path has no parent")?;
            if parent != self.root {
                anyhow::bail!(
                    "workspace {} is outside IQ-owned root {}",
                    path.display(),
                    self.root.display()
                );
            }
            Ok(())
        }

        fn verify_workspace_path(&self, path: &Path) -> Result<()> {
            self.verify_owned_path(path)?;
            self.verify_identity_workspace_path(path)
        }

        fn verify_identity_workspace_path(&self, path: &Path) -> Result<()> {
            let metadata = fs::symlink_metadata(path)
                .with_context(|| format!("inspect Rift workspace {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                anyhow::bail!(
                    "Rift workspace must be a real directory: {}",
                    path.display()
                );
            }
            let canonical = path
                .canonicalize()
                .with_context(|| format!("resolve Rift workspace {}", path.display()))?;
            if canonical != path {
                anyhow::bail!(
                    "Rift workspace {} resolves to unexpected path {}",
                    path.display(),
                    canonical.display()
                );
            }
            Ok(())
        }

        fn normalize_owned_path(&self, path: &Path) -> Result<PathBuf> {
            let parent = path.parent().context("IQ workspace path has no parent")?;
            let name = path
                .file_name()
                .context("IQ workspace path has no leaf name")?;
            let parent = parent
                .canonicalize()
                .with_context(|| format!("resolve IQ workspace parent {}", parent.display()))?;
            Ok(parent.join(name))
        }

        fn read_id(&self, path: &Path) -> Result<String> {
            Self::read_marker_id(path)
        }

        fn read_marker_id(path: &Path) -> Result<String> {
            let marker = path.join(".rift");
            let id = String::from_utf8(read_regular_file(&marker, "Rift identity marker")?)
                .with_context(|| format!("decode Rift identity from {}", path.display()))?;
            let id = id.trim();
            if id.len() != 26 || !id.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
                anyhow::bail!("invalid Rift identity at {}", path.display());
            }
            Ok(id.to_string())
        }

        fn run<I>(&self, args: I, label: &str) -> Result<Output>
        where
            I: IntoIterator<Item = OsString>,
        {
            self.verify_registry_identity()?;
            let mut command_args = Vec::new();
            if let Some(database) = self.database.as_ref() {
                command_args.push(OsString::from("--database"));
                command_args.push(database.clone());
            }
            command_args.extend(args);
            let outcome = command_output_timeout(
                &self.program,
                command_args,
                None,
                StdDuration::from_secs(60),
                |gate| {
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || Ok(ExecutionAuthority::Active),
            )
            .with_context(|| format!("run {label}"))?;
            match outcome {
                CommandOutputOutcome::Exited(output) if output.status.success() => Ok(output),
                CommandOutputOutcome::Exited(output) => anyhow::bail!(
                    "{label} failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                CommandOutputOutcome::Cancelled => {
                    anyhow::bail!("{label} lost command authority")
                }
            }
        }

        fn run_supervised<I>(
            &self,
            args: I,
            label: &str,
            authorize_start: impl FnOnce(&mut dyn Write) -> Result<bool>,
            check_authority: impl FnMut() -> Result<ExecutionAuthority>,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = OsString>,
        {
            self.verify_registry_identity()?;
            let mut command_args = Vec::new();
            if let Some(database) = self.database.as_ref() {
                command_args.push(OsString::from("--database"));
                command_args.push(database.clone());
            }
            command_args.extend(args);
            let outcome = command_output_timeout(
                &self.program,
                command_args,
                None,
                StdDuration::from_secs(60),
                authorize_start,
                check_authority,
            )
            .with_context(|| format!("run {label}"))?;
            match outcome {
                CommandOutputOutcome::Exited(output) if output.status.success() => Ok(output),
                CommandOutputOutcome::Exited(output) => anyhow::bail!(
                    "{label} failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                CommandOutputOutcome::Cancelled => {
                    anyhow::bail!("{label} lost command authority")
                }
            }
        }
    }

    pub fn verify_rift_workspace_config(
        source: &Path,
        root: &Path,
        repo_key: &str,
        rift_database: Option<&Path>,
        queue_database: &Path,
    ) -> Result<()> {
        let source = source
            .canonicalize()
            .with_context(|| format!("resolve configured repository {}", source.display()))?;
        let root = if root.is_absolute() {
            root.to_path_buf()
        } else {
            std::env::current_dir()?.join(root)
        };
        let queue = SqliteQueueReader::open(queue_database)?;
        let queue_database_id = queue.database_id()?;
        let inspected_root = resolve_path_without_creating(&root)?;
        queue.verify_workspace_root_path(repo_key, &inspected_root)?;
        let workspace_generation = queue.workspace_root_generation(repo_key)?;
        RiftWorkspaceManager::inspect(
            source,
            inspected_root,
            repo_key.to_string(),
            rift_database.map(Path::to_path_buf),
            &queue_database_id,
            workspace_generation,
        )
    }

    #[derive(Debug, Deserialize)]
    struct GitHubCommitStatus {
        context: String,
        state: String,
        creator: Option<GitHubStatusCreator>,
    }

    #[derive(Debug, Deserialize)]
    struct GitHubStatusCreator {
        login: String,
    }

    enum SignoffGate {
        Pass,
        Pending(String),
        Fail(String),
        Untrusted(String),
    }

    enum SignoffQueryError {
        Credentials(String),
        Provider(String),
        Cancelled,
    }

    fn signoff_evidence_satisfies(
        evidence: &JsonValue,
        candidate_sha: &str,
        required_contexts: &[String],
    ) -> bool {
        evidence.get("sha").and_then(JsonValue::as_str) == Some(candidate_sha)
            && evidence
                .get("contexts")
                .and_then(JsonValue::as_object)
                .is_some_and(|contexts| {
                    required_contexts.iter().all(|required| {
                        contexts.get(required).and_then(JsonValue::as_str) == Some("success")
                    })
                })
    }

    enum EvidenceCommandOutcome {
        Exited(ExitStatus),
        Cancelled(Option<ExitStatus>),
        TimedOut(ExitStatus),
    }

    enum CommandOutputOutcome {
        Exited(Output),
        Cancelled,
    }

    struct LeaseHeartbeat {
        stop: Option<mpsc::Sender<()>>,
        handle: Option<JoinHandle<Result<()>>>,
    }

    pub(crate) struct RepositoryOperationLease {
        queue: SqliteQueue,
        _database_lease: crate::control_store::DatabaseProcessLease,
        repo_key: String,
        owner_id: String,
        ttl_seconds: i64,
        heartbeat: Option<LeaseHeartbeat>,
        _process_lock: fs::File,
    }

    impl RepositoryOperationLease {
        pub(crate) fn try_acquire(
            queue: SqliteQueue,
            repository: &Path,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<Option<Self>> {
            let repository = repository.canonicalize().with_context(|| {
                format!("resolve repository operation path {}", repository.display())
            })?;
            let (_, target) = repo_key
                .rsplit_once("::")
                .context("repo_key must use <canonical-repository>::<target> scope")?;
            if target.is_empty() {
                anyhow::bail!("repository target must not be empty");
            }
            repository
                .to_str()
                .context("canonical repository path is not valid UTF-8")?;
            let top_level =
                PathBuf::from(git_output(&repository, ["rev-parse", "--show-toplevel"])?);
            if top_level.canonicalize()? != repository {
                anyhow::bail!("repository operation path is not the canonical checkout root");
            }
            queue.validate_repository_binding(repo_key, &repository, target)?;
            let git_dir =
                PathBuf::from(git_output(&repository, ["rev-parse", "--git-common-dir"])?);
            let git_dir = if git_dir.is_absolute() {
                git_dir
            } else {
                repository.join(git_dir)
            };
            let process_lock = acquire_exclusive_lock(
                &git_dir.join("iq-operation.lock"),
                "repository operation lock",
            );
            let process_lock = match process_lock {
                Ok(lock) => lock,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.raw_os_error() == Some(libc::EWOULDBLOCK)) =>
                {
                    return Ok(None)
                }
                Err(error) => return Err(error),
            };
            let database_lease = crate::control_store::DatabaseProcessLease::acquire(queue.path())?;
            if !queue.acquire_repo_operation_lease(
                repo_key,
                owner_id,
                ttl_seconds,
                &repository,
                target,
            )? {
                return Ok(None);
            }
            let heartbeat = LeaseHeartbeat::start(
                queue.clone(),
                repo_key.to_string(),
                owner_id.to_string(),
                ttl_seconds,
            );
            Ok(Some(Self {
                queue,
                _database_lease: database_lease,
                repo_key: repo_key.to_string(),
                owner_id: owner_id.to_string(),
                ttl_seconds,
                heartbeat: Some(heartbeat),
                _process_lock: process_lock,
            }))
        }

        pub(crate) fn acquire(
            queue: SqliteQueue,
            repository: &Path,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<Self> {
            Self::try_acquire(queue, repository, repo_key, owner_id, ttl_seconds)?
                .with_context(|| format!("repository queue {repo_key} has an active operation"))
        }

        pub(crate) fn ensure(&self) -> Result<()> {
            if self.queue.ensure_repo_lease_owner(
                &self.repo_key,
                &self.owner_id,
                self.ttl_seconds,
            )? {
                Ok(())
            } else {
                anyhow::bail!("repository operation lease was lost for {}", self.repo_key)
            }
        }

        pub(crate) fn authority(&self) -> Result<ExecutionAuthority> {
            self.queue.lease_authority(&self.repo_key, &self.owner_id)
        }

        pub(crate) fn run_command<I, S>(
            &self,
            program: &str,
            args: I,
            cwd: Option<&Path>,
            timeout: StdDuration,
            label: &str,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            let outcome = command_output_timeout(
                program,
                args,
                cwd,
                timeout,
                |gate| {
                    self.ensure()?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.authority(),
            )?;
            match outcome {
                CommandOutputOutcome::Exited(output) if output.status.success() => Ok(output),
                CommandOutputOutcome::Exited(output) => anyhow::bail!(
                    "{label} failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                CommandOutputOutcome::Cancelled => {
                    anyhow::bail!("{label} lost repository operation authority")
                }
            }
        }
    }

    impl Drop for RepositoryOperationLease {
        fn drop(&mut self) {
            if let Some(heartbeat) = self.heartbeat.take() {
                let _ = heartbeat.finish("repository operation");
            }
            let _ = self
                .queue
                .release_repo_lease(&self.repo_key, &self.owner_id);
        }
    }

    impl LeaseHeartbeat {
        fn start(queue: SqliteQueue, repo_key: String, owner_id: String, ttl_seconds: i64) -> Self {
            let (stop_tx, stop_rx) = mpsc::channel();
            let (ready_tx, ready_rx) = mpsc::channel();
            let interval = if ttl_seconds <= 1 {
                StdDuration::from_millis(50)
            } else {
                StdDuration::from_secs((ttl_seconds as u64 / 3).max(1))
            };
            let handle = thread::spawn(move || {
                let _ = ready_tx.send(());
                let lease_ttl = StdDuration::from_secs(ttl_seconds.max(1) as u64);
                let mut lease_deadline = Instant::now() + lease_ttl;
                let mut next_interval = interval;
                loop {
                    match stop_rx.recv_timeout(next_interval) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            match queue.heartbeat_repo_lease(&repo_key, &owner_id, ttl_seconds) {
                                Ok(true) => {
                                    lease_deadline = Instant::now() + lease_ttl;
                                    next_interval = interval;
                                }
                                Ok(false) => {
                                    anyhow::bail!(
                                        "repo queue {repo_key} lease lost during heartbeat"
                                    );
                                }
                                Err(error) => {
                                    if Instant::now() >= lease_deadline {
                                        return Err(error).context(format!(
                                            "repo queue {repo_key} heartbeat unavailable until lease expiry"
                                        ));
                                    }
                                    eprintln!(
                                        "repo queue {repo_key} heartbeat unavailable; retrying: {error:#}"
                                    );
                                    next_interval = StdDuration::from_secs(1).min(
                                        lease_deadline.saturating_duration_since(Instant::now()),
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(())
            });
            let _ = ready_rx.recv();
            Self {
                stop: Some(stop_tx),
                handle: Some(handle),
            }
        }

        fn finish(mut self, phase: &str) -> Result<()> {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(handle) = self.handle.take() {
                handle.join().map_err(|_| {
                    anyhow::anyhow!("lease heartbeat thread panicked during {phase}")
                })??;
            }
            Ok(())
        }
    }

    impl Integrator {
        pub fn new(options: IntegratorOptions) -> Result<Self> {
            Self::new_with_policy(options, IntegrationPolicy::NoValidation)
        }

        pub fn new_with_policy(
            mut options: IntegratorOptions,
            policy: IntegrationPolicy,
        ) -> Result<Self> {
            options.repo_path = options.repo_path.canonicalize().with_context(|| {
                format!(
                    "resolve configured repository {}",
                    options.repo_path.display()
                )
            })?;
            let (_, target) = options
                .repo_key
                .rsplit_once("::")
                .context("repo_key must use <canonical-repository>::<target> scope")?;
            options
                .repo_path
                .to_str()
                .context("canonical repository path is not valid UTF-8")?;
            if target.is_empty() {
                anyhow::bail!("repository target must not be empty");
            }
            if options.workspace_root.is_relative() {
                options.workspace_root = std::env::current_dir()?.join(&options.workspace_root);
            }
            let queue = SqliteQueue::open(&options.queue_db)?;
            options.queue_db = queue.path().to_path_buf();
            queue.validate_repository_binding(&options.repo_key, &options.repo_path, target)?;
            let registered = Self::verify_registered_remote_identity_for(
                &queue,
                &options.repo_key,
                &options.repo_path,
                target,
                &options.base_remote,
            )?;
            if registered && !matches!(&policy, IntegrationPolicy::NoValidation) {
                anyhow::bail!(
                    "registered repositories reject daemon validation and signoff; local integration-checkout policy is authoritative"
                );
            }
            options.workspace_root = resolve_path_without_creating(&options.workspace_root)?;
            queue.verify_workspace_root_path(&options.repo_key, &options.workspace_root)?;
            let queue_database_id = queue.database_id()?;
            let workspace_generation = queue.workspace_root_generation(&options.repo_key)?;
            let workspaces = RiftWorkspaceManager::new(
                options.repo_path.clone(),
                options.workspace_root.clone(),
                options.repo_key.clone(),
                options.rift_database.clone(),
                &queue_database_id,
                workspace_generation,
            )?;
            options.workspace_root = workspaces.root.clone();
            let policy = validate_host_policy(policy)?;
            let control_store = crate::control_store::ControlStore::open(queue.path())?;
            let lease_owner_id = format!("{}:{}", options.owner_id, Uuid::new_v4());
            queue.register_workspace_root(
                &options.repo_key,
                &workspaces.source,
                &workspaces.source_id,
                &workspaces.root,
                &workspaces.registry_identity,
            )?;
            Ok(Self {
                queue,
                options,
                policy,
                registered,
                lease_owner_id,
                workspaces,
                control_store,
            })
        }

        fn ensure_effort_after_composition(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
        ) -> Result<crate::control_store::IntegrationEffort> {
            let workspace = item
                .workspace
                .identity()
                .context("composed item has no retained Rift identity")?;
            let target_sha = item
                .target_sha
                .as_deref()
                .context("composed item has no target SHA")?;
            let source_sha = item
                .source_sha
                .as_deref()
                .context("composed item has no source SHA")?;
            let control = crate::composition::load_project_control_only(&self.options.repo_path)?;
            let runner = self
                .options
                .system_config
                .runner_snapshot(control.model.as_deref())?;
            let source_variant = match item.source {
                crate::core::QueueSource::RemoteBranch { .. } => "remote_branch",
                crate::core::QueueSource::LocalSubmission { .. } => "local_submission",
            };
            let landing_variant = item.landing_policy.to_string();
            let state_repository = self.control_store.item_state_repository_binding(&item.id)?;
            self.control_store
                .create_effort(crate::control_store::NewEffort {
                    item_id: &item.id,
                    attempt_id: &attempt.id,
                    target_sha,
                    source_sha,
                    source_variant,
                    landing_variant: &landing_variant,
                    workspace,
                    runner: &runner,
                    state_repository: &state_repository,
                })
        }

        fn verify_registered_remote_identity_for(
            queue: &SqliteQueue,
            repo_key: &str,
            repo_path: &Path,
            target: &str,
            remote_name: &str,
        ) -> Result<bool> {
            let Some((registered_path, registered_target, registered_remote)) =
                queue.registered_remote_identity(repo_key)?
            else {
                return Ok(false);
            };
            if registered_path != repo_path
                || registered_target != target
                || registered_remote.name != remote_name
            {
                anyhow::bail!(
                    "configured remote {remote_name} does not match registered repository remote {}",
                    registered_remote.name
                );
            }
            crate::composition::verify_remote_identity(repo_path, &registered_remote)?;
            Ok(true)
        }

        fn ensure_registered_remote_identity(&self) -> Result<()> {
            let (_, target) = self
                .options
                .repo_key
                .rsplit_once("::")
                .context("repo_key must use <canonical-repository>::<target> scope")?;
            Self::verify_registered_remote_identity_for(
                &self.queue,
                &self.options.repo_key,
                &self.options.repo_path,
                target,
                &self.options.base_remote,
            )?;
            Ok(())
        }

        pub fn run_once(&self) -> Result<Option<QueueItem>> {
            self.ensure_registered_remote_identity()?;
            let Some(_operation) = RepositoryOperationLease::try_acquire(
                self.queue.clone(),
                &self.options.repo_path,
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )?
            else {
                return Ok(None);
            };
            self.synchronize_workspace_generation()?;
            self.with_lease_heartbeat("workspace cleanup", || self.reconcile_workspaces())?;
            let Some(active) = self.queue.oldest_active_item(&self.options.repo_key)? else {
                return Ok(None);
            };
            if let Some(blocked) = self.enforce_item_boundary(&active)? {
                return Ok(Some(blocked));
            }
            if active.status == QueueStatus::Blocked
                && self.control_store.effort_for_item(&active.id)?.is_none()
            {
                return Ok(Some(active));
            }
            if active.status != QueueStatus::Ready {
                return self.resume_item_owned(&active.id).map(Some);
            }
            let policy_snapshot = if self.registered {
                let (_, snapshot, digest) =
                    crate::composition::load_local_policy(&self.options.repo_path)?;
                Some((snapshot, digest))
            } else if matches!(&self.policy, IntegrationPolicy::NoValidation) {
                Some(crate::composition::no_validation_policy_snapshot()?)
            } else {
                None
            };
            let attempt_policy = match policy_snapshot.as_ref() {
                Some((snapshot, digest)) => crate::sqlite::AttemptPolicy::Snapshot {
                    snapshot_json: snapshot,
                    digest,
                },
                None => crate::sqlite::AttemptPolicy::HostValidation,
            };
            let Some((item, attempt)) = self.queue.claim_next_ready_owned(
                &self.options.repo_key,
                &self.lease_owner_id,
                attempt_policy,
            )?
            else {
                return Ok(None);
            };
            let item = self.with_lease_heartbeat("merging", || self.merge_item(item, &attempt))?;
            if item.status != QueueStatus::Merged {
                return Ok(Some(item));
            }
            let item =
                self.with_lease_heartbeat("validating", || self.validate_item(item, &attempt))?;
            if !matches!(
                item.status,
                QueueStatus::Validated | QueueStatus::Integrating
            ) {
                return Ok(Some(item));
            }
            let item =
                self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))?;
            Ok(Some(item))
        }

        pub fn with_repo_lease<T>(
            &self,
            operation: impl FnOnce() -> Result<T>,
        ) -> Result<Option<T>> {
            self.ensure_registered_remote_identity()?;
            let Some(_operation) = RepositoryOperationLease::try_acquire(
                self.queue.clone(),
                &self.options.repo_path,
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )?
            else {
                return Ok(None);
            };
            self.with_lease_heartbeat("communication", operation)
                .map(Some)
        }

        pub fn resume_item(&self, item_id: &str) -> Result<QueueItem> {
            self.ensure_registered_remote_identity()?;
            let _operation = RepositoryOperationLease::acquire(
                self.queue.clone(),
                &self.options.repo_path,
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )?;
            let oldest = self
                .queue
                .oldest_active_item(&self.options.repo_key)?
                .context("repository queue has no active item to resume")?;
            if oldest.id != item_id {
                anyhow::bail!(
                    "item {item_id} is not the oldest active item; {} must complete first",
                    oldest.id
                );
            }
            self.resume_item_owned(item_id)
        }

        fn resume_item_owned(&self, item_id: &str) -> Result<QueueItem> {
            let item = self.queue.get_item(item_id)?;
            if item.repo_key != self.options.repo_key {
                anyhow::bail!(
                    "item {item_id} belongs to repo queue {}, not {}",
                    item.repo_key,
                    self.options.repo_key
                );
            }
            if let Some(blocked) = self.enforce_item_boundary(&item)? {
                return Ok(blocked);
            }
            let attempt_id = item
                .current_attempt_id
                .as_deref()
                .context("item has no active integration attempt")?;
            let attempt = self.queue.get_attempt(attempt_id)?;
            if let Some(effort) = self.control_store.effort_for_item(item_id)? {
                match &effort.state {
                    crate::control_domain::IntegrationEffortState::AgentReady(_) => {
                        return self.run_agent_cycle(item, &attempt)
                    }
                    crate::control_domain::IntegrationEffortState::AgentLaunching(launch) => {
                        if crate::agent_runner::systemd_unit_main_pid(
                            &effort.runner.sandbox.systemctl,
                            &launch.unit_name,
                        )?
                        .is_some()
                        {
                            crate::agent_runner::stop_systemd_unit(
                                &effort.runner.sandbox.systemctl,
                                &launch.unit_name,
                            )?;
                        }
                        self.control_store
                            .reset_prepared_launch(&effort.id, &launch.cycle_id)?;
                        return self.run_agent_cycle(item, &attempt);
                    }
                    crate::control_domain::IntegrationEffortState::CandidateBuilding(building) => {
                        return self.reconcile_candidate_build(item, &attempt, &effort, building)
                    }
                    crate::control_domain::IntegrationEffortState::GuidanceRequired(_)
                    | crate::control_domain::IntegrationEffortState::InfrastructureBlocked(_)
                    | crate::control_domain::IntegrationEffortState::CycleLimitBlocked(_) => {
                        return Ok(item)
                    }
                    crate::control_domain::IntegrationEffortState::ProviderBlocked(_) => {
                        self.control_store
                            .resume_provider_reconciliation(&effort.id)?;
                        let item = self.queue.get_item(item_id)?;
                        return self.with_lease_heartbeat("provider reconciliation", || {
                            self.integrate_item(item, &attempt)
                        });
                    }
                    crate::control_domain::IntegrationEffortState::CandidateReady(_)
                    | crate::control_domain::IntegrationEffortState::Validating(_)
                    | crate::control_domain::IntegrationEffortState::Landing(_)
                    | crate::control_domain::IntegrationEffortState::LandingUncertain(_)
                    | crate::control_domain::IntegrationEffortState::Integrated(_)
                    | crate::control_domain::IntegrationEffortState::Cancelled(_) => {}
                    crate::control_domain::IntegrationEffortState::AgentRunning(running) => {
                        crate::agent_runner::terminate_exact_process(
                            running.pid,
                            running.process_start_ticks,
                            running.process_group_id,
                        )?;
                        let workspace = self.load_owned_workspace(&item)?;
                        match crate::agent_runner::read_restart_result(
                            &workspace,
                            &running.cycle_id,
                            effort.runner.bounds.max_result_bytes,
                        ) {
                            Ok(Some(restart)) => {
                                let input = restart.input;
                                if input.identity.effort_id != effort.id
                                    || input.identity.item_id != item.id
                                    || input.identity.attempt_id != attempt.id
                                    || input.identity.cycle_id != running.cycle_id
                                    || input.identity.target_sha != effort.target_sha
                                    || input.identity.source_sha != effort.source_sha
                                    || input.identity.candidate_sha.is_some()
                                    || restart.input_sha256 != running.input_sha256
                                {
                                    anyhow::bail!(
                                        "restart protocol input differs from effort authority"
                                    );
                                }
                                if running.result != restart.result_state {
                                    self.control_store.record_result_state(
                                        &effort.id,
                                        &running.cycle_id,
                                        &restart.result_state,
                                    )?;
                                }
                                return self.classify_agent_result(
                                    &item,
                                    &attempt,
                                    &effort,
                                    &input.identity,
                                    restart.result,
                                    &restart.export_directory,
                                );
                            }
                            Ok(None) => {
                                crate::agent_runner::quarantine_restart_artifacts(
                                    &workspace,
                                    &running.cycle_id,
                                )?;
                                self.control_store.consume_cycle_failure(
                                    &effort.id,
                                    &running.cycle_id,
                                    crate::control_domain::CycleFailure::Interrupted,
                                )?;
                            }
                            Err(error) => {
                                crate::agent_runner::quarantine_restart_artifacts(
                                    &workspace,
                                    &running.cycle_id,
                                )?;
                                self.control_store.consume_cycle_failure(
                                    &effort.id,
                                    &running.cycle_id,
                                    crate::control_domain::CycleFailure::InvalidResult {
                                        reason: format!("restart result rejected: {error:#}"),
                                    },
                                )?;
                            }
                        }
                        let refreshed = self.queue.get_item(item_id)?;
                        let refreshed_effort = self
                            .control_store
                            .effort_for_item(item_id)?
                            .context("effort disappeared after restart classification")?;
                        if matches!(
                            refreshed_effort.state,
                            crate::control_domain::IntegrationEffortState::AgentReady(_)
                        ) {
                            return self.run_agent_cycle(refreshed, &attempt);
                        }
                        return Ok(refreshed);
                    }
                }
            }
            match item.status {
                QueueStatus::Merging => {
                    let item =
                        self.with_lease_heartbeat("merging", || self.merge_item(item, &attempt))?;
                    if matches!(item.status, QueueStatus::Blocked | QueueStatus::Cancelled) {
                        return Ok(item);
                    }
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if !matches!(
                        item.status,
                        QueueStatus::Validated | QueueStatus::Integrating
                    ) {
                        return Ok(item);
                    }
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Merged => {
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if !matches!(
                        item.status,
                        QueueStatus::Validated | QueueStatus::Integrating
                    ) {
                        return Ok(item);
                    }
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Validating => {
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if !matches!(
                        item.status,
                        QueueStatus::Validated | QueueStatus::Integrating
                    ) {
                        return Ok(item);
                    }
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Validated => {
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Integrating => {
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Blocked => anyhow::bail!(
                    "item {item_id} is still blocked; answer prompt or requeue before resume"
                ),
                other => anyhow::bail!("item {item_id} in status {other} cannot be resumed"),
            }
        }

        fn ensure_repo_lease(&self) -> Result<()> {
            if self.queue.ensure_repo_lease_owner(
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )? {
                Ok(())
            } else {
                anyhow::bail!(
                    "repo queue {} lease is no longer owned by {}",
                    self.options.repo_key,
                    self.lease_owner_id
                )
            }
        }

        fn with_lease_heartbeat<T>(
            &self,
            _phase: &str,
            operation: impl FnOnce() -> Result<T>,
        ) -> Result<T> {
            self.ensure_repo_lease()?;
            let result = operation();
            let lease_result = self.ensure_repo_lease();
            match (result, lease_result) {
                (Ok(value), Ok(())) => Ok(value),
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
            }
        }

        fn transition_item_owned(&self, item_id: &str, target: QueueStatus) -> Result<QueueItem> {
            self.ensure_repo_lease()?;
            match self.queue.transition_item_owned(
                item_id,
                target,
                &self.options.repo_key,
                &self.lease_owner_id,
            ) {
                Ok(item) => Ok(item),
                Err(error) => {
                    let current = self.queue.get_item(item_id)?;
                    if current.status == QueueStatus::Cancelled {
                        Ok(current)
                    } else {
                        Err(error)
                    }
                }
            }
        }

        fn execution_authority(&self, item_id: &str) -> Result<ExecutionAuthority> {
            self.queue
                .execution_authority(item_id, &self.options.repo_key, &self.lease_owner_id)
        }

        fn lease_authority(&self) -> Result<ExecutionAuthority> {
            self.queue
                .lease_authority(&self.options.repo_key, &self.lease_owner_id)
        }

        fn remove_workspace(
            &self,
            path: &Path,
            expected_id: Option<&str>,
            expected_source_id: Option<&str>,
        ) -> Result<bool> {
            self.workspaces.remove(
                path,
                expected_id,
                expected_source_id,
                |gate| {
                    self.ensure_repo_lease()?;
                    self.queue
                        .record_workspace_gc_debt(&self.workspaces.registry_identity)?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.lease_authority(),
                || {
                    self.queue
                        .clear_workspace_gc_debt(&self.workspaces.registry_identity)
                },
            )
        }

        fn remove_retained_workspace(&self, identity: &WorkspaceIdentity) -> Result<bool> {
            self.workspaces.remove_retained(
                identity,
                |gate| {
                    self.ensure_repo_lease()?;
                    self.queue
                        .record_workspace_gc_debt(&self.workspaces.registry_identity)?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.lease_authority(),
                || {
                    self.queue
                        .clear_workspace_gc_debt(&self.workspaces.registry_identity)
                },
            )
        }

        fn gc_workspaces(&self) -> Result<()> {
            self.workspaces.gc(
                |gate| {
                    self.ensure_repo_lease()?;
                    self.queue
                        .record_workspace_gc_debt(&self.workspaces.registry_identity)?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.lease_authority(),
                || {
                    self.queue
                        .clear_workspace_gc_debt(&self.workspaces.registry_identity)
                },
            )
        }

        fn load_owned_workspace(&self, item: &QueueItem) -> Result<PathBuf> {
            if item.repo_key != self.options.repo_key {
                anyhow::bail!(
                    "item {} belongs to repo queue {}, not {}",
                    item.id,
                    item.repo_key,
                    self.options.repo_key
                );
            }
            let identity = item
                .workspace
                .identity()
                .context("item has no retained Rift workspace")?;
            let expected = self.workspaces.expected_path(&item.id)?;
            let path = self.workspaces.verify_retained(identity)?;
            if path != expected {
                anyhow::bail!(
                    "item {} Rift path {} does not match expected {}",
                    item.id,
                    path.display(),
                    expected.display()
                );
            }
            Ok(path)
        }

        fn item_cancelled(&self, item_id: &str) -> Result<bool> {
            match self.execution_authority(item_id)? {
                ExecutionAuthority::Active => Ok(false),
                ExecutionAuthority::Cancelled => Ok(true),
                ExecutionAuthority::Lost(message) => anyhow::bail!(message),
            }
        }

        fn run_supervised_landing_command<I, S>(
            &self,
            item_id: &str,
            attempt_id: &str,
            program: &str,
            args: I,
            cwd: Option<&Path>,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            self.run_supervised_item_command(
                item_id,
                attempt_id,
                QueueStatus::Integrating,
                program,
                args,
                cwd,
                StdDuration::from_secs(20),
                "landing",
            )
        }

        #[allow(clippy::too_many_arguments)]
        fn run_supervised_item_command<I, S>(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            program: &str,
            args: I,
            cwd: Option<&Path>,
            timeout: StdDuration,
            label: &str,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            let output = self.run_supervised_item_command_output(
                item_id,
                attempt_id,
                expected_status,
                program,
                args,
                cwd,
                timeout,
                label,
            )?;
            if output.status.success() {
                Ok(output)
            } else {
                anyhow::bail!(
                    "{program} {label} command failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                )
            }
        }

        #[allow(clippy::too_many_arguments)]
        fn run_supervised_item_command_output<I, S>(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            program: &str,
            args: I,
            cwd: Option<&Path>,
            timeout: StdDuration,
            label: &str,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            let outcome = command_output_timeout(
                program,
                args,
                cwd,
                timeout,
                |gate| {
                    self.authorize_execution_start(item_id, attempt_id, expected_status, || {
                        gate.write_all(b"run\n")
                            .with_context(|| format!("release {label} command admission gate"))
                    })
                },
                || self.execution_authority(item_id),
            )?;
            match outcome {
                CommandOutputOutcome::Exited(output) => Ok(output),
                CommandOutputOutcome::Cancelled => {
                    anyhow::bail!("{program} {label} command lost execution authority")
                }
            }
        }

        fn cancelled_item(&self, item_id: &str) -> Result<Option<QueueItem>> {
            let item = self.queue.get_item(item_id)?;
            Ok((item.status == QueueStatus::Cancelled).then_some(item))
        }

        fn authorize_execution_start(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            release_gate: impl FnOnce() -> Result<()>,
        ) -> Result<bool> {
            self.ensure_repo_lease()?;
            self.queue
                .authorize_execution_start(item_id, attempt_id, expected_status, release_gate)
        }

        fn begin_landing_owned(
            &self,
            item_id: &str,
            attempt_id: &str,
            candidate_sha: &str,
            expected_target_sha: &str,
            command_id: &str,
        ) -> Result<Option<QueueItem>> {
            self.ensure_repo_lease()?;
            let effort = self
                .control_store
                .effort_for_item(item_id)?
                .context("landing item has no integration effort")?;
            if effort.attempt_id != attempt_id {
                anyhow::bail!("landing attempt differs from effort authority");
            }
            let signoff = self
                .queue
                .get_attempt(attempt_id)?
                .signoff_evidence_json
                .map(|_| crate::control_domain::SignoffDisposition::Evidence {
                    evidence_id: format!("attempt:{attempt_id}"),
                    candidate_sha: candidate_sha.to_string(),
                })
                .unwrap_or(crate::control_domain::SignoffDisposition::NotRequired);
            match self.control_store.begin_landing(
                &effort.id,
                expected_target_sha,
                &self.lease_owner_id,
                command_id,
                signoff,
            ) {
                Ok(()) => Ok(None),
                Err(error) => {
                    if let Some(cancelled) = self.cancelled_item(item_id)? {
                        Ok(Some(cancelled))
                    } else {
                        Err(error)
                    }
                }
            }
        }

        fn block_item_owned(
            &self,
            item_id: &str,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
        ) -> Result<String> {
            self.ensure_repo_lease()?;
            self.queue.block_item_owned(
                item_id,
                phase,
                reason,
                message,
                &self.options.repo_key,
                &self.lease_owner_id,
            )
        }

        fn mark_integrated_owned(
            &self,
            item_id: &str,
            attempt_id: &str,
            landed_commit_sha: &str,
            remote_target_sha: &str,
        ) -> Result<QueueItem> {
            self.ensure_repo_lease()?;
            let effort = self
                .control_store
                .effort_for_item(item_id)?
                .context("integrated item has no integration effort")?;
            if effort.attempt_id != attempt_id {
                anyhow::bail!("integrated attempt differs from effort authority");
            }
            self.control_store
                .mark_integrated(&effort.id, landed_commit_sha, remote_target_sha)?;
            let item = self.queue.get_item(item_id)?;
            self.cleanup_terminal_item(&item)?;
            self.queue.get_item(item_id)
        }

        fn cleanup_terminal_item(&self, item: &QueueItem) -> Result<()> {
            if !matches!(
                item.status,
                QueueStatus::Integrated | QueueStatus::Cancelled
            ) {
                anyhow::bail!("item {} is not terminal", item.id);
            }
            let expected = self.workspaces.expected_path(&item.id)?;
            match &item.workspace {
                WorkspaceState::Cleaned { .. } => return Ok(()),
                WorkspaceState::NotCreated => {}
                WorkspaceState::CreationIntent { path } => {
                    if self.workspaces.normalize_owned_path(Path::new(path))? != expected {
                        anyhow::bail!(
                            "item {} workspace {} does not match IQ-owned path {}",
                            item.id,
                            path,
                            expected.display()
                        );
                    }
                    if entry_exists(&expected)? {
                        let actual = self
                            .workspaces
                            .list()?
                            .into_iter()
                            .find(|identity| Path::new(&identity.path) == expected)
                            .context("terminal workspace creation path has unknown occupancy")?;
                        self.remove_clean_terminal_workspace(&actual)?;
                    }
                }
                WorkspaceState::Retained { identity } => {
                    let actual = self.workspaces.resolve_retained(identity)?;
                    if let Some(actual) = actual.as_ref() {
                        self.require_clean_terminal_workspace(actual)?;
                        self.remove_retained_workspace(identity)?;
                    } else {
                        self.remove_retained_workspace(identity)?;
                    }
                }
            }
            self.queue.mark_workspace_cleaned(&item.id)
        }

        fn remove_clean_terminal_workspace(&self, identity: &WorkspaceIdentity) -> Result<()> {
            self.require_clean_terminal_workspace(identity)?;
            self.remove_retained_workspace(identity)?;
            Ok(())
        }

        fn require_clean_terminal_workspace(&self, identity: &WorkspaceIdentity) -> Result<()> {
            let path = Path::new(&identity.path);
            if workspace_dirty(path)?.is_some() || crate::composition::has_git_operation(path)? {
                anyhow::bail!(
                    "terminal integration workspace is dirty or has an active Git operation; preserved: {}",
                    path.display()
                );
            }
            Ok(())
        }

        fn merge_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            if let Err(error) = self.fetch_for_merge(
                &item,
                attempt,
                ["fetch", &self.options.base_remote, &item.target_branch],
            ) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Merging,
                    BlockedReason::Infra,
                    &format!("failed to fetch target before merge: {error}"),
                );
            }
            let base_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            let base_sha = git_output(&self.options.repo_path, ["rev-parse", &base_ref])?;
            self.ensure_repo_lease()?;
            self.queue.update_attempt_base(&attempt.id, &base_sha)?;
            let source_sha = match &item.source {
                crate::core::QueueSource::RemoteBranch { .. } => {
                    let source_refspec = format!(
                        "+refs/heads/{}:refs/remotes/{}/{}",
                        item.source_branch, self.options.base_remote, item.source_branch
                    );
                    match self.fetch_for_merge(
                        &item,
                        attempt,
                        ["fetch", &self.options.base_remote, &source_refspec],
                    ) {
                        Ok(()) => self.source_remote_sha(&item)?,
                        Err(error) => {
                            return self.block_and_get(
                                &item.id,
                                BlockedPhase::Merging,
                                BlockedReason::Infra,
                                &format!(
                                    "failed to fetch source branch {}: {error}",
                                    item.source_branch
                                ),
                            );
                        }
                    }
                }
                crate::core::QueueSource::LocalSubmission {
                    submission_id,
                    commit_sha,
                } => {
                    let submission = self.queue.local_submission(submission_id)?;
                    let resolved = git_output(
                        &self.options.repo_path,
                        ["rev-parse", &submission.private_ref],
                    )?;
                    if submission.commit_sha != *commit_sha || resolved != *commit_sha {
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Merging,
                            BlockedReason::NeedsAgentFix,
                            "immutable local submission ref does not match its recorded exact commit",
                        );
                    }
                    resolved
                }
            };
            if matches!(item.source, crate::core::QueueSource::RemoteBranch { .. })
                && source_sha != item.current_head_sha
            {
                self.ensure_repo_lease()?;
                self.queue.record_event(
                    &item.id,
                    "source_head_mismatch",
                    &format!(
                        "source branch {} resolved to {}, queued head is {}",
                        item.source_branch, source_sha, item.current_head_sha
                    ),
                )?;
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Merging,
                    BlockedReason::NeedsAgentFix,
                    "source branch head does not match queued source head",
                );
            }

            let workspace = self.workspaces.expected_path(&item.id)?;
            if entry_exists(&workspace)? {
                let removal = match item.workspace.identity() {
                    Some(identity) => self.remove_retained_workspace(identity),
                    None => self.remove_workspace(&workspace, None, None),
                };
                if let Err(error) = removal {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Merging,
                        BlockedReason::NeedsAgentFix,
                        &format!(
                            "existing integration Rift cannot be safely replaced and was preserved: {error:#}"
                        ),
                    );
                }
            }
            self.ensure_repo_lease()?;
            let workspace_text = workspace
                .to_str()
                .context("IQ workspace path is not valid UTF-8")?;
            let generation = self.queue.begin_workspace_creation(
                &self.options.repo_key,
                &item.id,
                workspace_text,
            )?;
            self.workspaces.persist_generation(generation)?;
            let (created, rift_id) = self.workspaces.create(
                &item.id,
                |gate| {
                    self.authorize_execution_start(
                        &item.id,
                        &attempt.id,
                        QueueStatus::Merging,
                        || {
                            gate.write_all(b"run\n")
                                .context("release Rift creation admission gate")
                        },
                    )
                },
                || self.execution_authority(&item.id),
            )?;
            if created != workspace {
                anyhow::bail!(
                    "created Rift {} does not match persisted intent {}",
                    created.display(),
                    workspace.display()
                );
            }
            self.ensure_repo_lease()?;
            self.queue.set_workspace_identity(
                &item.id,
                workspace_text,
                &rift_id,
                &self.workspaces.source_id,
            )?;
            let identity = WorkspaceIdentity {
                path: workspace_text.to_string(),
                rift_id,
                source_rift_id: self.workspaces.source_id.clone(),
            };
            let prepare_result = (|| {
                self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    ["checkout", "--force", "--detach", &base_sha],
                    Some(&workspace),
                    StdDuration::from_secs(60),
                    "Rift preparation",
                )?;
                self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    ["reset", "--hard", &base_sha],
                    Some(&workspace),
                    StdDuration::from_secs(60),
                    "Rift preparation",
                )?;
                self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    ["clean", "-ffd"],
                    Some(&workspace),
                    StdDuration::from_secs(60),
                    "Rift preparation",
                )?;
                self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    ["config", "commit.gpgSign", "false"],
                    Some(&workspace),
                    StdDuration::from_secs(20),
                    "Rift Git configuration",
                )?;
                let actual = git_output(&workspace, ["rev-parse", "HEAD"])?;
                if actual != base_sha {
                    anyhow::bail!(
                        "prepared Rift {} at {actual}, expected {base_sha}",
                        workspace.display()
                    );
                }
                if let Some(dirty) = workspace_dirty(&workspace)? {
                    anyhow::bail!("prepared Rift is dirty: {dirty}");
                }
                Ok(())
            })();
            if let Err(error) = prepare_result {
                let cleanup = self.remove_retained_workspace(&identity);
                if self.queue.get_item(&item.id)?.status == QueueStatus::Merging {
                    self.queue.set_workspace_intent(&item.id, workspace_text)?;
                }
                cleanup?;
                return Err(error);
            }

            let local_squash = matches!(
                item.source,
                crate::core::QueueSource::LocalSubmission { .. }
            ) && item.landing_policy == crate::core::LandingPolicy::Squash;
            let merge = if local_squash {
                self.apply_local_submission_patch(
                    &item,
                    attempt,
                    &workspace,
                    &base_sha,
                    QueueStatus::Merging,
                    "local submission patch",
                )?
            } else {
                self.run_supervised_item_command_output(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    ["merge", "--no-ff", "--no-commit", source_sha.as_str()],
                    Some(&workspace),
                    StdDuration::from_secs(60),
                    "merge",
                )?
            };
            let conflict_files =
                match git_output(&workspace, ["diff", "--name-only", "--diff-filter=U"]) {
                    Ok(files) => files.lines().map(str::to_string).collect::<Vec<_>>(),
                    Err(error) => {
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Merging,
                            BlockedReason::Infra,
                            &format!("failed to inspect merge conflicts: {error:#}"),
                        )
                    }
                };
            if !merge.status.success() || !conflict_files.is_empty() {
                let conflict_json = json!({
                    "files": conflict_files,
                    "summary": String::from_utf8_lossy(&merge.stderr).trim(),
                    "target_sha": base_sha,
                    "source_sha": source_sha,
                    "workspace_path": workspace,
                });
                self.ensure_repo_lease()?;
                self.queue.set_conflict_metadata(
                    &item.id,
                    &conflict_json,
                    &base_sha,
                    &source_sha,
                )?;
                let composed = self.queue.get_item(&item.id)?;
                let effort = self.ensure_effort_after_composition(&composed, attempt)?;
                if let Err(error) = crate::composition::reject_tracked_policy(&workspace) {
                    self.control_store.block_infrastructure(
                        &effort.id,
                        crate::control_domain::InfrastructureBlocker {
                            component:
                                crate::control_domain::InfrastructureComponent::Configuration,
                            operation: "admit_cycle".into(),
                            cause: crate::control_domain::InfrastructureCause::Unavailable {
                                detail: format!("{error:#}"),
                            },
                        },
                    )?;
                    return self.queue.get_item(&item.id);
                }
                let composed_attempt = self.queue.get_attempt(&attempt.id)?;
                return self.run_agent_cycle(composed, &composed_attempt);
            }

            self.queue.set_conflict_metadata(
                &item.id,
                &json!({"files": [], "target_sha": base_sha, "source_sha": source_sha}),
                &base_sha,
                &source_sha,
            )?;
            let composed = self.queue.get_item(&item.id)?;
            let effort = self.ensure_effort_after_composition(&composed, attempt)?;
            if let Err(error) = crate::composition::reject_tracked_policy(&workspace) {
                self.control_store.block_infrastructure(
                    &effort.id,
                    crate::control_domain::InfrastructureBlocker {
                        component: crate::control_domain::InfrastructureComponent::Configuration,
                        operation: "admit_cycle".into(),
                        cause: crate::control_domain::InfrastructureCause::Unavailable {
                            detail: format!("{error:#}"),
                        },
                    },
                )?;
                return self.queue.get_item(&item.id);
            }
            let composed_attempt = self.queue.get_attempt(&attempt.id)?;
            self.run_agent_cycle(composed, &composed_attempt)
        }

        fn run_agent_cycle(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            use crate::agent_protocol::{
                AgentInput, ConflictEntry, LandingVariant, ProtocolLimits, RepositoryIdentity,
                RiftIdentity, SourceVariant,
            };
            use crate::control_domain::{
                AgentRunning, AtomicResultState, EncodedPath, ExactEffortIdentity,
                InfrastructureBlocker, InfrastructureCause, InfrastructureComponent,
            };
            use std::os::unix::ffi::OsStrExt;

            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("composed item has no integration effort")?;
            let crate::control_domain::IntegrationEffortState::AgentReady(ready) = &effort.state
            else {
                anyhow::bail!("integration effort is not ready for an agent cycle");
            };
            let workspace = self.load_owned_workspace(&item)?;
            let cycle_id = Uuid::new_v4().to_string();
            let identity = ExactEffortIdentity {
                effort_id: effort.id.clone(),
                item_id: item.id.clone(),
                attempt_id: attempt.id.clone(),
                cycle_id: cycle_id.clone(),
                target_sha: effort.target_sha.clone(),
                source_sha: effort.source_sha.clone(),
                candidate_sha: None,
            };
            let conflict_paths = conflict_files(&workspace)?;
            let conflicts = conflict_paths
                .iter()
                .map(|path| {
                    Ok(ConflictEntry {
                        path: EncodedPath::from_bytes(path.as_bytes())?,
                        base_blob: conflict_blob(&workspace, 1, path)?,
                        target_blob: conflict_blob(&workspace, 2, path)?,
                        source_blob: conflict_blob(&workspace, 3, path)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let (prior_outcomes, validation_evidence) =
                self.control_store.agent_evidence(&effort.id, 100)?;
            let input = AgentInput {
                version: crate::control_domain::PROTOCOL_VERSION,
                identity: identity.clone(),
                repository: RepositoryIdentity {
                    repo_key: item.repo_key.clone(),
                    target_branch: item.target_branch.clone(),
                },
                source: match &item.source {
                    crate::core::QueueSource::RemoteBranch { branch } => {
                        SourceVariant::RemoteBranch {
                            branch: branch.clone(),
                            sha: effort.source_sha.clone(),
                        }
                    }
                    crate::core::QueueSource::LocalSubmission {
                        submission_id,
                        commit_sha,
                    } => SourceVariant::LocalSubmission {
                        submission_id: submission_id.clone(),
                        sha: commit_sha.clone(),
                    },
                },
                landing: match item.landing_policy {
                    crate::core::LandingPolicy::Direct => LandingVariant::Direct,
                    crate::core::LandingPolicy::Provider => LandingVariant::Provider {
                        url: item.pr_url.clone().context("provider item has no URL")?,
                    },
                    crate::core::LandingPolicy::Squash => LandingVariant::Squash,
                },
                base_sha: attempt
                    .target_base_sha
                    .clone()
                    .context("attempt has no exact base SHA")?,
                rift: RiftIdentity {
                    rift_id: effort.workspace.rift_id.clone(),
                    source_rift_id: effort.workspace.source_rift_id.clone(),
                    relative_path: EncodedPath::from_bytes(
                        workspace
                            .file_name()
                            .context("retained Rift has no name")?
                            .as_bytes(),
                    )?,
                },
                conflicts,
                prior_outcomes,
                validation_evidence,
                instructions: instruction_identities(&workspace)?,
                limits: ProtocolLimits {
                    max_result_bytes: effort.runner.bounds.max_result_bytes,
                    max_text_bytes: 16 * 1024,
                    max_paths: 10_000,
                    max_evidence_entries: 100,
                },
            };
            let input_bytes = serde_json::to_vec(&input)?;
            let runner = crate::agent_runner::OpenCodeRunner::new(
                self.options.system_config.integration_agent.clone(),
                effort.runner.clone(),
            )?;
            if let Err(error) = runner.verify_capability(self.queue.path()) {
                self.control_store.block_infrastructure(
                    &effort.id,
                    InfrastructureBlocker {
                        component: InfrastructureComponent::Sandbox,
                        operation: "admit_cycle".into(),
                        cause: InfrastructureCause::Unavailable {
                            detail: format!("{error:#}"),
                        },
                    },
                )?;
                return self.queue.get_item(&item.id);
            }
            let protected = vec![self.options.repo_path.clone()];
            let launch_operation_id = Uuid::new_v4().to_string();
            let outcome = runner.run(
                &workspace,
                &input,
                &protected,
                crate::agent_runner::RunnerLifecycle {
                    on_prepared: |unit_name: &str, protocol: &Path| {
                        self.control_store.prepare_cycle_launch(
                            &effort.id,
                            &crate::control_domain::AgentLaunching {
                                launch_operation_id: launch_operation_id.clone(),
                                unit_name: unit_name.to_string(),
                                cycle_id: cycle_id.clone(),
                                cycle_number: ready.next_cycle,
                                authority_lease_id: self.lease_owner_id.clone(),
                                input_sha256: format!("{:x}", Sha256::digest(&input_bytes)),
                                protocol_directory: protocol.to_path_buf(),
                                prepared_at: chrono::Utc::now().to_rfc3339(),
                            },
                        )
                    },
                    on_started: |pid: u32,
                                 start: u64,
                                 group: i32,
                                 sandbox: &str,
                                 protocol: &Path| {
                        self.control_store.record_cycle_started(
                            &effort.id,
                            &AgentRunning {
                                launch_operation_id: launch_operation_id.clone(),
                                unit_name: format!("iq-agent-{cycle_id}"),
                                cycle_id: cycle_id.clone(),
                                cycle_number: ready.next_cycle,
                                pid,
                                process_start_ticks: start,
                                process_group_id: group,
                                authority_lease_id: self.lease_owner_id.clone(),
                                sandbox_id: sandbox.to_string(),
                                input_sha256: format!("{:x}", Sha256::digest(&input_bytes)),
                                result: AtomicResultState::Absent,
                                started_at: chrono::Utc::now().to_rfc3339(),
                            },
                        )?;
                        let _ = protocol;
                        Ok(())
                    },
                    on_writing: |state: &crate::control_domain::AtomicResultState| {
                        self.control_store
                            .record_result_state(&effort.id, &cycle_id, state)
                    },
                    authority_active: || {
                        Ok(matches!(
                            self.execution_authority(&item.id)?,
                            ExecutionAuthority::Active
                        ))
                    },
                },
            )?;
            match outcome {
                crate::agent_runner::RunnerOutcome::Complete {
                    result,
                    result_state,
                    log,
                    export_directory,
                    ..
                } => {
                    self.control_store
                        .record_cycle_log(&effort.id, &cycle_id, &log)?;
                    self.control_store
                        .record_result_state(&effort.id, &cycle_id, &result_state)?;
                    self.classify_agent_result(
                        &item,
                        attempt,
                        &effort,
                        &identity,
                        result,
                        &export_directory,
                    )
                }
                crate::agent_runner::RunnerOutcome::MissingResult { log, exit_status } => {
                    self.control_store
                        .record_cycle_log(&effort.id, &cycle_id, &log)?;
                    let result = self.consume_agent_failure(
                        &effort,
                        &cycle_id,
                        crate::control_domain::CycleFailure::RunnerCrash {
                            exit_code: exit_status.code(),
                        },
                    );
                    remove_cycle_protocol(&workspace, &cycle_id)?;
                    result
                }
                crate::agent_runner::RunnerOutcome::TimedOut { log } => {
                    self.control_store
                        .record_cycle_log(&effort.id, &cycle_id, &log)?;
                    let result = self.consume_agent_failure(
                        &effort,
                        &cycle_id,
                        crate::control_domain::CycleFailure::Timeout,
                    );
                    remove_cycle_protocol(&workspace, &cycle_id)?;
                    result
                }
                crate::agent_runner::RunnerOutcome::AuthorityLost { log } => {
                    let current = self.queue.get_item(&item.id)?;
                    if current.status == QueueStatus::Cancelled {
                        remove_cycle_protocol(&workspace, &cycle_id)?;
                        Ok(current)
                    } else {
                        self.control_store
                            .record_cycle_log(&effort.id, &cycle_id, &log)?;
                        anyhow::bail!("runner lost repository or queue authority")
                    }
                }
                crate::agent_runner::RunnerOutcome::InvalidResult {
                    log,
                    reason,
                    export_directory,
                } => {
                    self.control_store
                        .record_cycle_log(&effort.id, &cycle_id, &log)?;
                    crate::agent_runner::remove_sandbox_export(&export_directory)?;
                    remove_cycle_protocol(&workspace, &cycle_id)?;
                    self.consume_agent_failure(
                        &effort,
                        &cycle_id,
                        crate::control_domain::CycleFailure::InvalidResult { reason },
                    )
                }
            }
        }

        fn classify_agent_result(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            effort: &crate::control_store::IntegrationEffort,
            identity: &crate::control_domain::ExactEffortIdentity,
            result: Box<crate::agent_protocol::AgentResult>,
            export_directory: &Path,
        ) -> Result<QueueItem> {
            match *result {
                crate::agent_protocol::AgentResult::Resolved(resolved) => {
                    let workspace = self.load_owned_workspace(item)?;
                    if let Err(error) = crate::agent_runner::import_staged_result(
                        export_directory,
                        &workspace,
                        &resolved.staged_tree_sha256,
                        &resolved.changed_paths,
                    ) {
                        remove_cycle_protocol(&workspace, &identity.cycle_id)?;
                        return self.consume_agent_failure(
                            effort,
                            &identity.cycle_id,
                            crate::control_domain::CycleFailure::InvalidResult {
                                reason: format!("staged result rejected: {error:#}"),
                            },
                        );
                    }
                    remove_cycle_protocol(&workspace, &identity.cycle_id)?;
                    if !conflict_files(&workspace)?.is_empty() {
                        return self.consume_agent_failure(
                            effort,
                            &identity.cycle_id,
                            crate::control_domain::CycleFailure::InvalidResult {
                                reason: "resolved result left unresolved index entries".into(),
                            },
                        );
                    }
                    self.build_candidate(
                        item,
                        attempt,
                        effort,
                        &identity.cycle_id,
                        &resolved.staged_tree_sha256,
                    )
                }
                crate::agent_protocol::AgentResult::GuidanceRequired(guidance) => {
                    self.control_store.require_guidance(
                        &effort.id,
                        crate::control_domain::SemanticGuidanceBlocker {
                            request_id: Uuid::new_v4().to_string(),
                            question: guidance.question,
                            affected_contracts: guidance.affected_contracts,
                            affected_paths: guidance.affected_paths,
                            alternatives: guidance.alternatives,
                            evidence: guidance.evidence,
                            identity: identity.clone(),
                        },
                    )?;
                    remove_cycle_protocol(&self.load_owned_workspace(item)?, &identity.cycle_id)?;
                    self.queue.get_item(&item.id)
                }
                crate::agent_protocol::AgentResult::MechanicalFailure(failure) => {
                    let result = self.consume_agent_failure(
                        effort,
                        &identity.cycle_id,
                        crate::control_domain::CycleFailure::Mechanical {
                            operation: failure.operation,
                            reason: failure.reason,
                            evidence: failure.evidence,
                        },
                    );
                    remove_cycle_protocol(&self.load_owned_workspace(item)?, &identity.cycle_id)?;
                    result
                }
            }
        }

        fn consume_agent_failure(
            &self,
            effort: &crate::control_store::IntegrationEffort,
            cycle_id: &str,
            failure: crate::control_domain::CycleFailure,
        ) -> Result<QueueItem> {
            self.control_store
                .consume_cycle_failure(&effort.id, cycle_id, failure)?;
            let refreshed = self
                .control_store
                .effort_for_item(&effort.item_id)?
                .context("effort disappeared")?;
            if matches!(
                refreshed.state,
                crate::control_domain::IntegrationEffortState::AgentReady(_)
            ) {
                let item = self.queue.get_item(&effort.item_id)?;
                let attempt = self.queue.get_attempt(&effort.attempt_id)?;
                self.run_agent_cycle(item, &attempt)
            } else {
                self.queue.get_item(&effort.item_id)
            }
        }

        fn build_candidate(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            effort: &crate::control_store::IntegrationEffort,
            cycle_id: &str,
            staged_tree_sha256: &str,
        ) -> Result<QueueItem> {
            let workspace = self.load_owned_workspace(item)?;
            if workspace_has_unstaged_changes(&workspace)? {
                anyhow::bail!("candidate builder requires an entirely staged worktree");
            }
            if !conflict_files(&workspace)?.is_empty() {
                anyhow::bail!("candidate builder rejects unresolved index entries");
            }
            crate::composition::reject_tracked_policy(&workspace)?;
            let base = attempt
                .target_base_sha
                .as_deref()
                .context("candidate has no target base")?;
            let tree = git_output(&workspace, ["write-tree"])?;
            let parents = match item.landing_policy {
                crate::core::LandingPolicy::Squash => vec![base.to_string()],
                crate::core::LandingPolicy::Direct | crate::core::LandingPolicy::Provider => {
                    vec![base.to_string(), effort.source_sha.clone()]
                }
            };
            if item.landing_policy == crate::core::LandingPolicy::Squash {
                let target_tree =
                    git_output(&workspace, ["rev-parse", &format!("{base}^{{tree}}")])?;
                if tree.trim() == target_tree.trim() {
                    anyhow::bail!("local squash staged tree is empty relative to the exact target");
                }
            }
            let operation_id = Uuid::new_v4().to_string();
            let operation_ref = format!("refs/iq/candidate-operations/{operation_id}");
            let timestamp = chrono::Utc::now().timestamp().to_string();
            let message = match item.landing_policy {
                crate::core::LandingPolicy::Squash => format!(
                    "Squash integrate queue item {}\n\nIQ-Builder-Operation: {operation_id}",
                    item.id
                ),
                _ => format!(
                    "Integrate queue item {}\n\nIQ-Builder-Operation: {operation_id}",
                    item.id
                ),
            };
            let intent = crate::control_store::CandidateIntent {
                operation_id: operation_id.clone(),
                cycle_id: cycle_id.to_string(),
                staged_tree_sha256: staged_tree_sha256.to_string(),
                tree_sha: tree.trim().to_string(),
                parents: parents.clone(),
                author_name: "IQ Integration Builder".into(),
                author_email: "iq@localhost".into(),
                author_timestamp: timestamp.clone(),
                committer_name: "IQ Integration Builder".into(),
                committer_email: "iq@localhost".into(),
                committer_timestamp: timestamp,
                message: message.clone(),
                operation_ref: operation_ref.clone(),
            };
            self.control_store
                .accept_resolved_cycle(&effort.id, &intent)?;
            let mut args = vec!["commit-tree".to_string(), tree.trim().to_string()];
            for parent in &parents {
                args.push("-p".into());
                args.push(parent.clone());
            }
            args.push("-m".into());
            args.push(message);
            let output = Command::new("git")
                .args(&args)
                .env("GIT_AUTHOR_NAME", "IQ Integration Builder")
                .env("GIT_AUTHOR_EMAIL", "iq@localhost")
                .env(
                    "GIT_AUTHOR_DATE",
                    format!("{} +0000", intent.author_timestamp),
                )
                .env("GIT_COMMITTER_NAME", "IQ Integration Builder")
                .env("GIT_COMMITTER_EMAIL", "iq@localhost")
                .env(
                    "GIT_COMMITTER_DATE",
                    format!("{} +0000", intent.committer_timestamp),
                )
                .current_dir(&workspace)
                .output()?;
            if !output.status.success() {
                anyhow::bail!(
                    "candidate builder commit-tree failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            let candidate = String::from_utf8(output.stdout)?.trim().to_string();
            git(
                &workspace,
                [
                    "update-ref",
                    operation_ref.as_str(),
                    candidate.as_str(),
                    "0000000000000000000000000000000000000000",
                ],
            )?;
            git(&workspace, ["reset", "--hard", candidate.as_str()])?;
            if item.landing_policy == crate::core::LandingPolicy::Squash {
                verify_one_parent_candidate(&workspace, &candidate, base)?;
            }
            self.control_store.record_candidate(
                &effort.id,
                &crate::control_store::CandidateObservation::read(
                    &workspace,
                    &candidate,
                    &operation_ref,
                )?,
            )?;
            self.queue.get_item(&item.id)
        }

        fn reconcile_candidate_build(
            &self,
            item: QueueItem,
            _attempt: &Attempt,
            effort: &crate::control_store::IntegrationEffort,
            building: &crate::control_domain::CandidateBuilding,
        ) -> Result<QueueItem> {
            let workspace = self.load_owned_workspace(&item)?;
            let output = git_status(
                &workspace,
                ["rev-parse", "--verify", building.operation_ref.as_str()],
            )?;
            let candidate = if output.status.success() {
                String::from_utf8(output.stdout)?.trim().to_string()
            } else {
                let mut args = vec!["commit-tree".to_string(), building.tree_sha.clone()];
                for parent in &building.parent_shas {
                    args.push("-p".into());
                    args.push(parent.clone());
                }
                args.push("-m".into());
                args.push(building.message.clone());
                let output = Command::new("git")
                    .args(args)
                    .env("GIT_AUTHOR_NAME", &building.author_name)
                    .env("GIT_AUTHOR_EMAIL", &building.author_email)
                    .env(
                        "GIT_AUTHOR_DATE",
                        format!("{} +0000", building.author_timestamp),
                    )
                    .env("GIT_COMMITTER_NAME", &building.committer_name)
                    .env("GIT_COMMITTER_EMAIL", &building.committer_email)
                    .env(
                        "GIT_COMMITTER_DATE",
                        format!("{} +0000", building.committer_timestamp),
                    )
                    .current_dir(&workspace)
                    .output()?;
                if !output.status.success() {
                    anyhow::bail!(
                        "candidate reconciliation commit-tree failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }
                let candidate = String::from_utf8(output.stdout)?.trim().to_string();
                git(
                    &workspace,
                    [
                        "update-ref",
                        building.operation_ref.as_str(),
                        candidate.as_str(),
                        "0000000000000000000000000000000000000000",
                    ],
                )?;
                candidate
            };
            let tree = git_output(&workspace, ["show", "-s", "--format=%T", &candidate])?;
            let digest = format!("{:x}", Sha256::digest(tree.trim().as_bytes()));
            let parents = git_output(&workspace, ["show", "-s", "--format=%P", &candidate])?
                .split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>();
            let metadata = git_output(
                &workspace,
                [
                    "show",
                    "-s",
                    "--format=%an%x00%ae%x00%at%x00%cn%x00%ce%x00%ct%x00%B",
                    &candidate,
                ],
            )?;
            let fields = metadata.splitn(7, '\0').collect::<Vec<_>>();
            if tree.trim() != building.tree_sha
                || digest != building.staged_tree_sha256
                || parents != building.parent_shas
                || fields.len() != 7
                || fields[0] != building.author_name
                || fields[1] != building.author_email
                || fields[2] != building.author_timestamp
                || fields[3] != building.committer_name
                || fields[4] != building.committer_email
                || fields[5] != building.committer_timestamp
                || fields[6].trim_end() != building.message
            {
                anyhow::bail!("candidate operation ref differs from durable builder intent");
            }
            git(&workspace, ["reset", "--hard", candidate.as_str()])?;
            self.control_store.record_candidate(
                &effort.id,
                &crate::control_store::CandidateObservation::read(
                    &workspace,
                    &candidate,
                    &building.operation_ref,
                )?,
            )?;
            self.queue.get_item(&item.id)
        }

        fn policy_for_attempt(
            &self,
            attempt: &Attempt,
        ) -> Result<crate::composition::PolicySnapshot> {
            let snapshot = attempt
                .policy_snapshot_json
                .as_deref()
                .context("registered attempt has no local policy snapshot")?;
            let digest = attempt
                .policy_digest
                .as_deref()
                .context("registered attempt has no local policy digest")?;
            crate::composition::verify_policy_snapshot(snapshot, digest)
        }

        fn requires_trusted_policy(&self, item: &QueueItem) -> Result<bool> {
            let registered = self.queue.repository_if_exists(&item.repo_key)?.is_some();
            if matches!(
                item.source,
                crate::core::QueueSource::LocalSubmission { .. }
            ) && !registered
            {
                anyhow::bail!("local submission belongs to an unregistered repository");
            }
            Ok(registered)
        }

        fn require_exact_policy_signoff(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            candidate_sha: &str,
        ) -> Result<Option<QueueItem>> {
            let current = self.queue.get_attempt(&attempt.id)?;
            let policy = self.policy_for_attempt(&current)?;
            let crate::composition::ValidationPolicy::Command {
                signoff: crate::composition::SignoffPolicy::Required { command, contexts },
                ..
            } = policy.policy
            else {
                return Ok(None);
            };
            if let Some(raw) = current.signoff_evidence_json.as_deref() {
                let evidence: JsonValue = serde_json::from_str(raw)
                    .context("parse persisted exact-SHA signoff evidence")?;
                if signoff_evidence_satisfies(&evidence, candidate_sha, &contexts) {
                    return Ok(None);
                }
            }
            if let Some(dirty) = workspace_dirty(workspace)? {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate is dirty before signoff: {dirty}"),
                )?));
            }
            let args = vec![
                OsString::from(format!("IQ_SIGNOFF_SHA={candidate_sha}")),
                OsString::from("sh"),
                OsString::from("-lc"),
                OsString::from(command),
            ];
            let output = match self.run_supervised_item_command_output(
                &item.id,
                &attempt.id,
                QueueStatus::Integrating,
                "env",
                args,
                Some(workspace),
                StdDuration::from_secs(3 * 60 * 60),
                "exact-SHA signoff",
            ) {
                Ok(output) => output,
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("signoff command could not complete: {error:#}"),
                    )?));
                }
            };
            let log_dir = self.evidence_dir(item, attempt)?;
            let mut log = create_file_at(
                &log_dir.directory,
                OsStr::new("signoff.log"),
                "signoff evidence log",
            )?;
            writeln!(
                log,
                "--- stdout ---\n{}\n--- stderr ---\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )?;
            if !output.status.success() {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    "required exact-SHA signoff command failed",
                )?));
            }
            let evidence: JsonValue = match serde_json::from_slice(&output.stdout) {
                Ok(evidence) => evidence,
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("signoff command returned invalid JSON: {error}"),
                    )?));
                }
            };
            if !signoff_evidence_satisfies(&evidence, candidate_sha, &contexts) {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    "signoff evidence does not prove all required contexts on the exact candidate SHA",
                )?));
            }
            if git_output(workspace, ["rev-parse", "HEAD"])? != candidate_sha
                || workspace_dirty(workspace)?.is_some()
            {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    "signoff command changed the exact candidate workspace",
                )?));
            }
            self.ensure_repo_lease()?;
            self.queue.update_attempt_signoff(&attempt.id, &evidence)?;
            Ok(None)
        }

        fn verify_fenced_policy_evidence(&self, item: &QueueItem, attempt: &Attempt) -> Result<()> {
            if !self.requires_trusted_policy(item)? {
                return Ok(());
            }
            let current = self.queue.get_attempt(&attempt.id)?;
            let policy = self.policy_for_attempt(&current)?;
            let crate::composition::ValidationPolicy::Command {
                signoff: crate::composition::SignoffPolicy::Required { contexts, .. },
                ..
            } = policy.policy
            else {
                return Ok(());
            };
            let candidate_sha = current
                .validated_commit_sha
                .as_deref()
                .context("fenced attempt has no validated candidate")?;
            let evidence: JsonValue = serde_json::from_str(
                current
                    .signoff_evidence_json
                    .as_deref()
                    .context("fenced attempt has no exact-SHA signoff evidence")?,
            )?;
            if !signoff_evidence_satisfies(&evidence, candidate_sha, &contexts) {
                anyhow::bail!("fenced landing has invalid exact-SHA signoff evidence");
            }
            Ok(())
        }

        fn validate_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            let workspace = self.load_owned_workspace(&item)?;
            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("candidate has no integration effort")?;
            if matches!(
                effort.state,
                crate::control_domain::IntegrationEffortState::CandidateReady(_)
            ) {
                self.control_store.start_validation(
                    &effort.id,
                    attempt.policy_digest.as_deref().unwrap_or("host_policy"),
                )?;
            } else if !matches!(
                effort.state,
                crate::control_domain::IntegrationEffortState::Validating(
                    crate::control_domain::Validating {
                        stage: crate::control_domain::ValidationStage::Running,
                        ..
                    }
                )
            ) {
                anyhow::bail!("integration effort is not ready to validate");
            }
            let item = self.queue.get_item(&item.id)?;
            if item.status == QueueStatus::Cancelled {
                return Ok(item);
            }
            let command = if self.requires_trusted_policy(&item)? {
                match self.policy_for_attempt(attempt) {
                    Ok(policy) => match policy.policy {
                        crate::composition::ValidationPolicy::None => None,
                        crate::composition::ValidationPolicy::Command { command, .. } => {
                            Some(command)
                        }
                    },
                    Err(error) => {
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Validating,
                            BlockedReason::NeedsAgentFix,
                            &format!("attempt policy snapshot is missing or invalid: {error:#}"),
                        );
                    }
                }
            } else {
                match &self.policy {
                    IntegrationPolicy::NoValidation => None,
                    IntegrationPolicy::Validation { command, .. } => Some(command.clone()),
                }
            };
            if let Some(dirty) = workspace_dirty(&workspace)? {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate is dirty before validation: {dirty}"),
                );
            }
            let Some(command) = command else {
                let candidate_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
                let target_base_sha = self
                    .queue
                    .get_attempt(&attempt.id)?
                    .target_base_sha
                    .context("no-validation attempt has no exact target base")?;
                self.ensure_repo_lease()?;
                self.queue.accept_candidate_without_validation(
                    &item.id,
                    &attempt.id,
                    &target_base_sha,
                    &candidate_sha,
                    QueueStatus::Validating,
                    &self.options.repo_key,
                    &self.lease_owner_id,
                )?;
                self.control_store
                    .complete_validation(&effort.id, &candidate_sha)?;
                return self.queue.get_item(&item.id);
            };
            let log_dir = self.evidence_dir(&item, attempt)?;
            let log_path = log_dir.path.join("validation.log");
            let outcome = match run_evidence_command(
                &command,
                &workspace,
                &log_path,
                &log_dir.directory,
                &[],
                StdDuration::from_secs(2 * 60 * 60),
                |gate| {
                    self.authorize_execution_start(
                        &item.id,
                        &attempt.id,
                        QueueStatus::Validating,
                        || {
                            gate.write_all(b"run\n")
                                .context("release command admission gate")
                        },
                    )
                },
                || self.execution_authority(&item.id),
            ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!("validation command could not complete: {error}"),
                    );
                }
            };
            let status = match outcome {
                EvidenceCommandOutcome::Exited(status) => status,
                EvidenceCommandOutcome::Cancelled(status) => {
                    self.ensure_repo_lease()?;
                    self.queue.update_attempt_validation(
                        &attempt.id,
                        &command,
                        status.and_then(|status| status.code()).unwrap_or(-1) as i64,
                        &log_path.to_string_lossy(),
                        None,
                    )?;
                    return self.queue.get_item(&item.id);
                }
                EvidenceCommandOutcome::TimedOut(status) => {
                    self.ensure_repo_lease()?;
                    self.queue.update_attempt_validation(
                        &attempt.id,
                        &command,
                        status.code().unwrap_or(-1) as i64,
                        &log_path.to_string_lossy(),
                        None,
                    )?;
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!(
                            "validation command timed out; inspect {}",
                            log_path.display()
                        ),
                    );
                }
            };
            let exit_code = status.code().unwrap_or(-1) as i64;
            if self.item_cancelled(&item.id)? {
                self.ensure_repo_lease()?;
                self.queue.update_attempt_validation(
                    &attempt.id,
                    &command,
                    exit_code,
                    &log_path.to_string_lossy(),
                    None,
                )?;
                return self.queue.get_item(&item.id);
            }
            if !status.success() {
                self.ensure_repo_lease()?;
                self.queue.update_attempt_validation(
                    &attempt.id,
                    &command,
                    exit_code,
                    &log_path.to_string_lossy(),
                    None,
                )?;
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("validation command failed; inspect {}", log_path.display()),
                );
            }
            if let Some(dirty) = workspace_dirty(&workspace)? {
                self.ensure_repo_lease()?;
                self.queue.update_attempt_validation(
                    &attempt.id,
                    &command,
                    exit_code,
                    &log_path.to_string_lossy(),
                    None,
                )?;
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("validation modified candidate worktree: {dirty}"),
                );
            }
            let validated_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
            self.ensure_repo_lease()?;
            self.queue.update_attempt_validation(
                &attempt.id,
                &command,
                exit_code,
                &log_path.to_string_lossy(),
                Some(&validated_sha),
            )?;
            self.control_store
                .complete_validation(&effort.id, &validated_sha)?;
            self.queue.get_item(&item.id)
        }

        fn integrate_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            if let Some(blocked) = self.enforce_item_boundary(&item)? {
                return Ok(blocked);
            }
            if self.requires_trusted_policy(&item)? {
                let persisted_attempt = self.queue.get_attempt(&attempt.id)?;
                if let Err(error) = self.policy_for_attempt(&persisted_attempt) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::NeedsAgentFix,
                        &format!(
                            "attempt policy snapshot is missing or invalid during integration: {error:#}"
                        ),
                    );
                }
            }
            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("candidate has no integration effort")?;
            if !matches!(
                effort.state,
                crate::control_domain::IntegrationEffortState::Validating(
                    crate::control_domain::Validating {
                        stage: crate::control_domain::ValidationStage::Gates,
                        ..
                    }
                ) | crate::control_domain::IntegrationEffortState::Landing(_)
                    | crate::control_domain::IntegrationEffortState::LandingUncertain(_)
            ) {
                anyhow::bail!("integration effort is not ready for landing gates");
            }
            let item = self.queue.get_item(&item.id)?;
            if item.status == QueueStatus::Cancelled {
                return Ok(item);
            }
            if item.landing.is_uncertain() {
                if let Err(error) = self.verify_fenced_policy_evidence(&item, attempt) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::NeedsAgentFix,
                        &format!("fenced landing policy evidence is invalid: {error:#}"),
                    );
                }
            }
            match item.landing_policy {
                crate::core::LandingPolicy::Provider => {
                    let pr_url = item
                        .pr_url
                        .clone()
                        .context("provider landing policy has no PR/MR URL")?;
                    return self.integrate_provider_item(item, attempt, &pr_url);
                }
                crate::core::LandingPolicy::Direct | crate::core::LandingPolicy::Squash => {
                    if item.pr_url.is_some() {
                        anyhow::bail!("non-provider landing policy cannot carry a PR/MR URL");
                    }
                }
            }
            let workspace = self.load_owned_workspace(&item)?;
            let fetch_result = self.fetch_target_supervised(&item, attempt);
            if let Err(error) = fetch_result {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("failed to fetch target before direct landing: {error}"),
                );
            }
            let remote_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            let remote_sha = match git_output(&self.options.repo_path, ["rev-parse", &remote_ref]) {
                Ok(sha) => sha,
                Err(error) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("failed to resolve target ref {remote_ref}: {error}"),
                    );
                }
            };
            if item.landing.is_uncertain() {
                return self.reconcile_fenced_exact_landing(
                    &item,
                    attempt,
                    &workspace,
                    &remote_ref,
                    &remote_sha,
                );
            }
            let current_attempt = self.queue.get_attempt(&attempt.id)?;
            let needs_moved_base_reconciliation = current_attempt.target_base_sha.as_deref()
                != Some(remote_sha.as_str())
                || (!matches!(
                    current_attempt.moved_base,
                    crate::sqlite::MovedBaseState::None
                ) && current_attempt.validated_commit_sha.is_none());
            if needs_moved_base_reconciliation {
                if let Some(blocked) = self.merge_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &remote_sha,
                    "target branch moved before direct landing",
                )? {
                    return Ok(blocked);
                }
                if let Some(blocked) = self.revalidate_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &remote_sha,
                    "target moved",
                )? {
                    return Ok(blocked);
                }
                if let Some(cancelled) = self.cancelled_item(&item.id)? {
                    return Ok(cancelled);
                }
            }
            let landed_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
            if let Err(error) = self.verify_candidate_graph(&item, attempt, &workspace) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate graph is invalid before signoff: {error}"),
                );
            }
            if let Some(cancelled) = self.cancelled_item(&item.id)? {
                return Ok(cancelled);
            }
            if self.requires_trusted_policy(&item)? {
                if let Some(blocked) =
                    self.require_exact_policy_signoff(&item, attempt, &workspace, &landed_sha)?
                {
                    return Ok(blocked);
                }
            } else if let IntegrationPolicy::Validation {
                signoff: HostSignoffPolicy::Required(signoff),
                ..
            } = &self.policy
            {
                if let Some(blocked) =
                    self.sign_candidate(&item, attempt, &workspace, &landed_sha, signoff)?
                {
                    return Ok(blocked);
                }
            }
            if let Err(error) = self.fetch_target_supervised(&item, attempt) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("failed to fetch target after signoff: {error}"),
                );
            }
            let target_after_signoff =
                git_output(&self.options.repo_path, ["rev-parse", &remote_ref])?;
            if target_after_signoff != remote_sha {
                self.queue.record_event(
                    &item.id,
                    "target_moved_after_signoff",
                    &format!(
                        "target moved from {remote_sha} to {target_after_signoff}; rebuilding and invalidating evidence"
                    ),
                )?;
                if let Some(blocked) = self.merge_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &target_after_signoff,
                    "target branch moved after signoff",
                )? {
                    return Ok(blocked);
                }
                if let Some(blocked) = self.revalidate_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &target_after_signoff,
                    "target moved after signoff",
                )? {
                    return Ok(blocked);
                }
                if let Some(cancelled) = self.cancelled_item(&item.id)? {
                    return Ok(cancelled);
                }
                return self.queue.get_item(&item.id);
            }
            self.land_exact_candidate(&item, attempt, &workspace, &landed_sha, &remote_sha)
        }

        fn land_exact_candidate(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            candidate_sha: &str,
            expected_target_sha: &str,
        ) -> Result<QueueItem> {
            if let Some(cancelled) = self.cancelled_item(&item.id)? {
                return Ok(cancelled);
            }
            if let Err(error) = self.verify_candidate_graph(item, attempt, workspace) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate graph is invalid before target push: {error}"),
                );
            }
            if let Some(dirty) = workspace_dirty(workspace)? {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate is dirty before target push: {dirty}"),
                );
            }
            self.ensure_registered_remote_identity()?;
            if let Some(cancelled) = self.begin_landing_owned(
                &item.id,
                &attempt.id,
                candidate_sha,
                expected_target_sha,
                &format!("git-push:{}", item.id),
            )? {
                return Ok(cancelled);
            }
            let target_ref = format!("refs/heads/{}", item.target_branch);
            let remote_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            let push_ref = format!("{candidate_sha}:{target_ref}");
            let lease = format!("--force-with-lease={target_ref}:{expected_target_sha}");
            let landing_result = self.run_supervised_item_command_output(
                &item.id,
                &attempt.id,
                QueueStatus::Integrating,
                "git",
                [
                    "push",
                    "--porcelain",
                    lease.as_str(),
                    &self.options.base_remote,
                    &push_ref,
                ],
                Some(workspace),
                StdDuration::from_secs(20),
                "landing",
            );
            if landing_result
                .as_ref()
                .is_ok_and(definite_force_with_lease_rejection)
            {
                return self.recover_definite_cas_rejection(
                    item,
                    attempt,
                    workspace,
                    expected_target_sha,
                );
            }
            let landing_error = match landing_result {
                Ok(output) if output.status.success() => None,
                Ok(output) => Some(anyhow::anyhow!(
                    "git push failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )),
                Err(error) => Some(error),
            };
            self.ensure_repo_lease()?;
            if let Err(error) = self.fetch_target_supervised(item, attempt) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &match landing_error {
                        Some(landing_error) => format!(
                            "exact landing outcome is unknown after {landing_error}; failed to fetch target for reconciliation: {error}"
                        ),
                        None => format!("failed to fetch target after exact landing push: {error}"),
                    },
                );
            }
            if let Err(error) = git(
                &self.options.repo_path,
                ["merge-base", "--is-ancestor", candidate_sha, &remote_ref],
            ) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &match landing_error {
                        Some(landing_error) => format!(
                            "exact landing failed or remained unconfirmed for {candidate_sha}: {landing_error}; remote reconciliation: {error}"
                        ),
                        None => format!(
                            "remote target does not contain exact landed commit {candidate_sha}: {error}"
                        ),
                    },
                );
            }
            if matches!(item.source, crate::core::QueueSource::RemoteBranch { .. }) {
                if let Err(error) = git(
                    &self.options.repo_path,
                    [
                        "merge-base",
                        "--is-ancestor",
                        item.current_head_sha.as_str(),
                        &remote_ref,
                    ],
                ) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!(
                            "remote target does not contain queued source commit {}: {error}",
                            item.current_head_sha
                        ),
                    );
                }
            }
            let remote_target_sha =
                git_output(&self.options.repo_path, ["rev-parse", &remote_ref])?;
            self.reconcile_registered_checkout(item, &attempt.id, &remote_target_sha)?;
            self.mark_integrated_owned(&item.id, &attempt.id, candidate_sha, &remote_target_sha)
        }

        fn recover_definite_cas_rejection(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            expected_target_sha: &str,
        ) -> Result<QueueItem> {
            self.ensure_repo_lease()?;
            if let Some(cancelled) = self.cancelled_item(&item.id)? {
                return Ok(cancelled);
            }
            if let Err(error) = self.fetch_target_supervised(item, attempt) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("fetch after definite compare-and-set rejection failed: {error}"),
                );
            }
            let remote_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            let moved_target = git_output(&self.options.repo_path, ["rev-parse", &remote_ref])?;
            self.queue.record_event(
                &item.id,
                "target_moved_during_landing",
                &format!(
                    "compare-and-set rejected base {expected_target_sha}; rebuilding on {moved_target}"
                ),
            )?;
            if let Some(blocked) = self.merge_moved_base(
                item,
                attempt,
                workspace,
                &moved_target,
                "definite compare-and-set rejection",
            )? {
                return Ok(blocked);
            }
            if let Some(blocked) = self.revalidate_moved_base(
                item,
                attempt,
                workspace,
                &moved_target,
                "definite compare-and-set rejection",
            )? {
                return Ok(blocked);
            }
            if let Some(cancelled) = self.cancelled_item(&item.id)? {
                return Ok(cancelled);
            }
            self.queue.get_item(&item.id)
        }

        fn reconcile_fenced_exact_landing(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            remote_ref: &str,
            remote_sha: &str,
        ) -> Result<QueueItem> {
            let crate::sqlite::LandingState::Uncertain {
                candidate_sha: fenced_candidate,
                expected_target_sha,
            } = &item.landing
            else {
                anyhow::bail!("item has no uncertain exact landing to reconcile");
            };
            if attempt.validated_commit_sha.as_deref() != Some(fenced_candidate)
                || attempt.target_base_sha.as_deref() != Some(expected_target_sha)
            {
                anyhow::bail!("uncertain landing identity differs from exact attempt evidence");
            }
            let candidate_landed =
                git_is_ancestor(&self.options.repo_path, fenced_candidate, remote_ref)?;
            let source_landed = git_is_ancestor(
                &self.options.repo_path,
                item.current_head_sha.as_str(),
                remote_ref,
            )?;
            let source_identity_satisfied = source_landed
                || matches!(
                    item.source,
                    crate::core::QueueSource::LocalSubmission { .. }
                );
            if candidate_landed && source_identity_satisfied {
                self.reconcile_registered_checkout(item, &attempt.id, remote_sha)?;
                return self.mark_integrated_owned(
                    &item.id,
                    &attempt.id,
                    fenced_candidate,
                    remote_sha,
                );
            }
            if remote_sha != expected_target_sha {
                return self
                    .merge_moved_base(
                        item,
                        attempt,
                        workspace,
                        remote_sha,
                        "reconciled rejected compare-and-set landing",
                    )?
                    .context("target recomposition did not return an authoritative item state");
            }
            self.block_and_get(
                &item.id,
                BlockedPhase::Integrating,
                BlockedReason::Infra,
                "fenced exact landing remains unresolved; retry to reconcile remote target state",
            )
        }

        fn reconcile_registered_checkout(
            &self,
            item: &QueueItem,
            attempt_id: &str,
            target_sha: &str,
        ) -> Result<()> {
            let Some(repository) = self.queue.repository_if_exists(&item.repo_key)? else {
                return Ok(());
            };
            crate::composition::reconcile_registered_checkout(
                &self.queue,
                &repository,
                &self.lease_owner_id,
                target_sha,
                |path, target_sha| {
                    self.run_supervised_item_command(
                        &item.id,
                        attempt_id,
                        QueueStatus::Integrating,
                        "git",
                        ["reset", "--hard", target_sha],
                        Some(path),
                        StdDuration::from_secs(60),
                        "registered checkout exact reset",
                    )?;
                    Ok(())
                },
            )
        }

        #[allow(dead_code)]
        fn sign_candidate(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            candidate_sha: &str,
            policy: &SignoffPolicy,
        ) -> Result<Option<QueueItem>> {
            self.ensure_registered_remote_identity()?;
            if let Some(cancelled) = self.cancelled_item(&item.id)? {
                return Ok(Some(cancelled));
            }
            if let Some(dirty) = workspace_dirty(workspace)? {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate is dirty before signoff: {dirty}"),
                )?));
            }
            let candidate_branch = format!("iq/candidates/{}", item.id);
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["switch", "-C", &candidate_branch],
                Some(workspace),
            )?;
            let candidate_push_ref = format!("HEAD:refs/heads/{candidate_branch}");
            if let Err(error) = self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                [
                    "push",
                    "--force",
                    "--set-upstream",
                    &self.options.base_remote,
                    &candidate_push_ref,
                ],
                Some(workspace),
            ) {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("failed to push signoff candidate {candidate_sha}: {error}"),
                )?));
            }
            let remote_candidate = git_output(
                workspace,
                [
                    "ls-remote",
                    "--heads",
                    &self.options.base_remote,
                    &format!("refs/heads/{candidate_branch}"),
                ],
            )?
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
            if remote_candidate != candidate_sha {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!(
                        "remote candidate branch resolved to {remote_candidate}, expected {candidate_sha}"
                    ),
                )?));
            }
            self.queue.record_event(
                &item.id,
                "candidate_pushed",
                &format!("pushed {candidate_sha} to {candidate_branch}"),
            )?;

            let log_dir = self.evidence_dir(item, attempt)?;
            let log_path = log_dir.path.join("signoff.log");
            let outcome = run_evidence_command(
                &policy.command,
                workspace,
                &log_path,
                &log_dir.directory,
                &[
                    ("IQ_CANDIDATE_SHA", candidate_sha),
                    ("IQ_ITEM_ID", &item.id),
                ],
                StdDuration::from_secs(3 * 60 * 60),
                |gate| {
                    self.authorize_execution_start(
                        &item.id,
                        &attempt.id,
                        QueueStatus::Integrating,
                        || {
                            gate.write_all(b"run\n")
                                .context("release command admission gate")
                        },
                    )
                },
                || self.execution_authority(&item.id),
            );
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("signoff command could not complete: {error}"),
                    )?));
                }
            };
            let status = match outcome {
                EvidenceCommandOutcome::Exited(status) => status,
                EvidenceCommandOutcome::Cancelled(_) => {
                    return Ok(Some(self.queue.get_item(&item.id)?));
                }
                EvidenceCommandOutcome::TimedOut(_) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("signoff command timed out; inspect {}", log_path.display()),
                    )?));
                }
            };
            if self.item_cancelled(&item.id)? {
                return Ok(Some(self.queue.get_item(&item.id)?));
            }
            if !status.success() {
                let reason = match status.code() {
                    None | Some(70) => BlockedReason::Infra,
                    Some(75) => BlockedReason::Provider,
                    Some(77) => BlockedReason::Credentials,
                    Some(_) => BlockedReason::NeedsAgentFix,
                };
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    reason,
                    &format!(
                        "signoff command failed for {candidate_sha}; inspect {}",
                        log_path.display()
                    ),
                )?));
            }
            if let Some(dirty) = workspace_dirty(workspace)? {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("signoff modified candidate worktree: {dirty}"),
                )?));
            }
            let head_after = git_output(workspace, ["rev-parse", "HEAD"])?;
            if head_after != candidate_sha {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("candidate changed during signoff: {candidate_sha} -> {head_after}"),
                )?));
            }

            match self.github_signoff_gate(&item.id, &attempt.id, candidate_sha, policy) {
                Ok(SignoffGate::Pass) => {
                    if !self.queue.record_event_if_status(
                        &item.id,
                        QueueStatus::Integrating,
                        "signoff_verified",
                        &format!(
                            "verified {} on {candidate_sha}",
                            policy.required_contexts.join(", ")
                        ),
                    )? {
                        if let Some(cancelled) = self.cancelled_item(&item.id)? {
                            return Ok(Some(cancelled));
                        }
                        anyhow::bail!(
                            "item {} left integrating before signoff verification",
                            item.id
                        );
                    }
                    Ok(None)
                }
                Ok(SignoffGate::Pending(message)) => Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Dependency,
                    &message,
                )?)),
                Ok(SignoffGate::Fail(message)) => Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &message,
                )?)),
                Ok(SignoffGate::Untrusted(message)) => Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Credentials,
                    &message,
                )?)),
                Err(SignoffQueryError::Credentials(error)) => Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Credentials,
                    &format!("failed to verify signoff statuses for {candidate_sha}: {error}"),
                )?)),
                Err(SignoffQueryError::Provider(error)) => Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Provider,
                    &format!("failed to verify signoff statuses for {candidate_sha}: {error}"),
                )?)),
                Err(SignoffQueryError::Cancelled) => Ok(Some(self.queue.get_item(&item.id)?)),
            }
        }

        fn github_signoff_gate(
            &self,
            item_id: &str,
            attempt_id: &str,
            candidate_sha: &str,
            policy: &SignoffPolicy,
        ) -> std::result::Result<SignoffGate, SignoffQueryError> {
            let gh = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
            let endpoint = format!(
                "repos/{}/commits/{candidate_sha}/statuses",
                policy.repository
            );
            let output = command_output_timeout(
                &gh,
                ["api", endpoint.as_str()],
                None,
                StdDuration::from_secs(60),
                |gate| {
                    self.authorize_execution_start(
                        item_id,
                        attempt_id,
                        QueueStatus::Integrating,
                        || {
                            gate.write_all(b"run\n")
                                .context("release command admission gate")
                        },
                    )
                },
                || self.execution_authority(item_id),
            )
            .map_err(|error| {
                SignoffQueryError::Provider(format!(
                    "failed to run {gh} api for {candidate_sha}: {error}"
                ))
            })?;
            let output = match output {
                CommandOutputOutcome::Exited(output) => output,
                CommandOutputOutcome::Cancelled => return Err(SignoffQueryError::Cancelled),
            };
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let normalized = stderr.to_ascii_lowercase();
                let message = format!("gh status query failed: {stderr}");
                if normalized.contains("http 401")
                    || normalized.contains("http 403")
                    || normalized.contains("authentication")
                    || normalized.contains("not logged")
                {
                    return Err(SignoffQueryError::Credentials(message));
                }
                return Err(SignoffQueryError::Provider(message));
            }
            let statuses: Vec<GitHubCommitStatus> = serde_json::from_slice(&output.stdout)
                .map_err(|error| {
                    SignoffQueryError::Provider(format!(
                        "parse GitHub commit statuses JSON: {error}"
                    ))
                })?;
            for required in &policy.required_contexts {
                // GitHub's commit-statuses endpoint returns newest statuses first.
                let latest = statuses.iter().find(|status| status.context == *required);
                let Some(status) = latest else {
                    return Ok(SignoffGate::Pending(format!(
                        "required status {required} is missing on {candidate_sha}"
                    )));
                };
                let creator = status
                    .creator
                    .as_ref()
                    .map(|creator| creator.login.as_str());
                if creator != Some(policy.trusted_creator.as_str()) {
                    return Ok(SignoffGate::Untrusted(format!(
                        "required status {required} on {candidate_sha} was created by {}, expected {}",
                        creator.unwrap_or("unknown"),
                        policy.trusted_creator
                    )));
                }
                match status.state.as_str() {
                    "success" => {}
                    "pending" => {
                        return Ok(SignoffGate::Pending(format!(
                            "required status {required} is pending on {candidate_sha}"
                        )))
                    }
                    state => {
                        return Ok(SignoffGate::Fail(format!(
                            "required status {required} is {state} on {candidate_sha}"
                        )))
                    }
                }
            }
            Ok(SignoffGate::Pass)
        }

        fn verify_candidate_graph(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
        ) -> Result<()> {
            crate::composition::reject_tracked_policy(workspace)?;
            let current_attempt = self.queue.get_attempt(&attempt.id)?;
            let head = git_output(workspace, ["rev-parse", "HEAD"])?;
            let validated = current_attempt
                .validated_commit_sha
                .context("attempt has no validated candidate SHA")?;
            if head != validated {
                anyhow::bail!("workspace HEAD {head} differs from validated SHA {validated}");
            }
            if matches!(item.source, crate::core::QueueSource::RemoteBranch { .. }) {
                git(
                    workspace,
                    [
                        "merge-base",
                        "--is-ancestor",
                        item.current_head_sha.as_str(),
                        head.as_str(),
                    ],
                )
                .with_context(|| {
                    format!(
                        "validated candidate {head} does not contain queued source {}",
                        item.current_head_sha
                    )
                })?;
            }
            let target_base = current_attempt
                .target_base_sha
                .context("attempt has no target base SHA")?;
            if matches!(
                item.source,
                crate::core::QueueSource::LocalSubmission { .. }
            ) {
                if item.landing_policy != crate::core::LandingPolicy::Squash {
                    anyhow::bail!("local submission does not use squash landing");
                }
                verify_one_parent_candidate(workspace, &head, &target_base)?;
            } else {
                git(
                    workspace,
                    [
                        "merge-base",
                        "--is-ancestor",
                        target_base.as_str(),
                        head.as_str(),
                    ],
                )
                .with_context(|| {
                    format!("validated candidate {head} does not contain target base {target_base}")
                })?;
            }
            Ok(())
        }

        fn merge_moved_base(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            moved_base_sha: &str,
            summary_prefix: &str,
        ) -> Result<Option<QueueItem>> {
            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("target movement item has no integration effort")?;
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                [
                    "fetch",
                    self.options
                        .repo_path
                        .to_str()
                        .context("registered repository path is not valid UTF-8")?,
                    moved_base_sha,
                ],
                Some(workspace),
            )?;
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["reset", "--hard", moved_base_sha],
                Some(workspace),
            )?;
            let local_squash = matches!(
                item.source,
                crate::core::QueueSource::LocalSubmission { .. }
            ) && item.landing_policy == crate::core::LandingPolicy::Squash;
            let merge = if local_squash {
                self.apply_local_submission_patch(
                    item,
                    attempt,
                    workspace,
                    moved_base_sha,
                    QueueStatus::Integrating,
                    "moved-base local submission patch",
                )?
            } else {
                self.run_supervised_item_command_output(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Integrating,
                    "git",
                    [
                        "merge",
                        "--no-ff",
                        "--no-commit",
                        effort.source_sha.as_str(),
                    ],
                    Some(workspace),
                    StdDuration::from_secs(60),
                    "moved-base merge",
                )?
            };
            let conflict_files =
                match git_output(workspace, ["diff", "--name-only", "--diff-filter=U"]) {
                    Ok(files) => files
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .map(str::to_string)
                        .collect::<Vec<_>>(),
                    Err(error) => {
                        return Ok(Some(self.block_and_get(
                            &item.id,
                            BlockedPhase::Merging,
                            BlockedReason::Infra,
                            &format!("failed to inspect moved-base conflicts: {error:#}"),
                        )?))
                    }
                };
            let conflict_json = json!({
                "files": conflict_files,
                "summary": format!(
                    "{summary_prefix}: {}",
                    String::from_utf8_lossy(&merge.stderr).trim()
                ),
                "target_sha": moved_base_sha,
                "source_sha": effort.source_sha,
                "workspace_path": workspace,
            });
            if local_squash
                && merge.status.success()
                && git_status(workspace, ["diff", "--cached", "--quiet"])?
                    .status
                    .success()
            {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Merging,
                    BlockedReason::NeedsAgentFix,
                    "local submission is empty after target movement",
                )?));
            }
            self.control_store.recompose_after_target_move(
                &effort.id,
                moved_base_sha,
                &conflict_json,
            )?;
            let recomposed = self.queue.get_item(&item.id)?;
            let recomposed_attempt = self.queue.get_attempt(&attempt.id)?;
            let candidate = self.run_agent_cycle(recomposed, &recomposed_attempt)?;
            if candidate.status != QueueStatus::Merged {
                return Ok(Some(candidate));
            }
            let validated = self.validate_item(candidate, &recomposed_attempt)?;
            if validated.status != QueueStatus::Integrating {
                return Ok(Some(validated));
            }
            Ok(Some(self.integrate_item(validated, &recomposed_attempt)?))
        }

        fn apply_local_submission_patch(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            target_sha: &str,
            expected_status: QueueStatus,
            label: &str,
        ) -> Result<Output> {
            let crate::core::QueueSource::LocalSubmission { submission_id, .. } = &item.source
            else {
                anyhow::bail!("exact local patch requested for a remote queue source");
            };
            let submission = self.queue.local_submission(submission_id)?;
            self.run_supervised_item_command_output(
                &item.id,
                &attempt.id,
                expected_status,
                "git",
                [
                    "read-tree",
                    "-m",
                    "-u",
                    submission.base_sha.as_str(),
                    target_sha,
                    submission.commit_sha.as_str(),
                ],
                Some(workspace),
                StdDuration::from_secs(60),
                label,
            )
        }

        fn revalidate_moved_base(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            moved_base_sha: &str,
            label: &str,
        ) -> Result<Option<QueueItem>> {
            let command = if self.requires_trusted_policy(item)? {
                match self.policy_for_attempt(attempt) {
                    Ok(policy) => match policy.policy {
                        crate::composition::ValidationPolicy::None => None,
                        crate::composition::ValidationPolicy::Command { command, .. } => {
                            Some(command)
                        }
                    },
                    Err(error) => {
                        return Ok(Some(self.block_and_get(
                            &item.id,
                            BlockedPhase::Validating,
                            BlockedReason::NeedsAgentFix,
                            &format!("attempt policy snapshot is missing or invalid after {label}: {error:#}"),
                        )?));
                    }
                }
            } else {
                match &self.policy {
                    IntegrationPolicy::NoValidation => None,
                    IntegrationPolicy::Validation { command, .. } => Some(command.clone()),
                }
            };
            if let Some(dirty) = workspace_dirty(workspace)? {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate is dirty before revalidation after {label}: {dirty}"),
                )?));
            }
            let Some(command) = command else {
                let candidate_sha = git_output(workspace, ["rev-parse", "HEAD"])?;
                self.ensure_repo_lease()?;
                self.queue.accept_candidate_without_validation(
                    &item.id,
                    &attempt.id,
                    moved_base_sha,
                    &candidate_sha,
                    QueueStatus::Integrating,
                    &self.options.repo_key,
                    &self.lease_owner_id,
                )?;
                return Ok(None);
            };
            let log_dir = self.evidence_dir(item, attempt)?;
            let safe_label = label.replace([' ', '/'], "-");
            let log_path = log_dir
                .path
                .join(format!("revalidation-after-{safe_label}.log"));
            let outcome = match run_evidence_command(
                &command,
                workspace,
                &log_path,
                &log_dir.directory,
                &[],
                StdDuration::from_secs(2 * 60 * 60),
                |gate| {
                    self.authorize_execution_start(
                        &item.id,
                        &attempt.id,
                        QueueStatus::Integrating,
                        || {
                            gate.write_all(b"run\n")
                                .context("release command admission gate")
                        },
                    )
                },
                || self.execution_authority(&item.id),
            ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!("failed to run validation command after {label}: {error}"),
                    )?));
                }
            };
            let status = match outcome {
                EvidenceCommandOutcome::Exited(status) => status,
                EvidenceCommandOutcome::Cancelled(status) => {
                    self.ensure_repo_lease()?;
                    self.queue.update_attempt_revalidation(
                        &attempt.id,
                        moved_base_sha,
                        &command,
                        status.and_then(|status| status.code()).unwrap_or(-1) as i64,
                        &log_path.to_string_lossy(),
                        None,
                    )?;
                    return Ok(Some(self.queue.get_item(&item.id)?));
                }
                EvidenceCommandOutcome::TimedOut(status) => {
                    self.ensure_repo_lease()?;
                    self.queue.update_attempt_revalidation(
                        &attempt.id,
                        moved_base_sha,
                        &command,
                        status.code().unwrap_or(-1) as i64,
                        &log_path.to_string_lossy(),
                        None,
                    )?;
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!(
                            "validation timed out after {label}; inspect {}",
                            log_path.display()
                        ),
                    )?));
                }
            };
            let exit_code = status.code().unwrap_or(-1) as i64;
            if self.item_cancelled(&item.id)? {
                self.ensure_repo_lease()?;
                self.queue.update_attempt_revalidation(
                    &attempt.id,
                    moved_base_sha,
                    &command,
                    exit_code,
                    &log_path.to_string_lossy(),
                    None,
                )?;
                return Ok(Some(self.queue.get_item(&item.id)?));
            }
            let dirty = workspace_dirty(workspace)?;
            let validated_sha = if status.success() && dirty.is_none() {
                match git_output(workspace, ["rev-parse", "HEAD"]) {
                    Ok(sha) => Some(sha),
                    Err(error) => {
                        return Ok(Some(self.block_and_get(
                            &item.id,
                            BlockedPhase::Validating,
                            BlockedReason::Infra,
                            &format!("cannot resolve candidate after revalidation: {error}"),
                        )?));
                    }
                }
            } else {
                None
            };
            self.ensure_repo_lease()?;
            self.queue.update_attempt_revalidation(
                &attempt.id,
                moved_base_sha,
                &command,
                exit_code,
                &log_path.to_string_lossy(),
                validated_sha.as_deref(),
            )?;
            if !status.success() {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!(
                        "validation failed after {label}; inspect {}",
                        log_path.display()
                    ),
                )?));
            }
            if let Some(dirty) = dirty {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("revalidation after {label} modified candidate worktree: {dirty}"),
                )?));
            }
            Ok(None)
        }

        fn block_and_get(
            &self,
            item_id: &str,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
        ) -> Result<QueueItem> {
            let item = self.queue.get_item(item_id)?;
            if item.status == QueueStatus::Cancelled {
                return Ok(item);
            }
            if let Some(effort) = self.control_store.effort_for_item(item_id)? {
                match reason {
                    BlockedReason::NeedsAgentFix => {
                        self.control_store.reject_candidate(&effort.id, message)?;
                    }
                    BlockedReason::Infra
                    | BlockedReason::Dependency
                    | BlockedReason::Credentials => {
                        self.control_store.block_infrastructure(
                            &effort.id,
                            crate::control_domain::InfrastructureBlocker {
                                component: if phase == BlockedPhase::Validating {
                                    crate::control_domain::InfrastructureComponent::Validation
                                } else {
                                    crate::control_domain::InfrastructureComponent::Filesystem
                                },
                                operation: phase.to_string(),
                                cause: crate::control_domain::InfrastructureCause::Unavailable {
                                    detail: message.to_string(),
                                },
                            },
                        )?;
                    }
                    BlockedReason::Provider => {
                        self.block_provider_effort(
                            &item,
                            &effort,
                            phase,
                            crate::control_domain::ProviderGateStatus::Pending,
                            message,
                        )?;
                    }
                    BlockedReason::NeedsUserInput => anyhow::bail!(
                        "post-composition semantic guidance must come from a running agent cycle"
                    ),
                }
                return self.queue.get_item(item_id);
            }
            let result = if item.status == QueueStatus::Integrating
                && matches!(phase, BlockedPhase::Merging | BlockedPhase::Validating)
            {
                self.ensure_repo_lease()?;
                self.queue.block_integrating_recovery_owned(
                    item_id,
                    phase,
                    reason,
                    message,
                    &self.options.repo_key,
                    &self.lease_owner_id,
                )
            } else {
                self.block_item_owned(item_id, phase, reason, message)
            };
            if let Err(error) = result {
                let current = self.queue.get_item(item_id)?;
                if current.status == QueueStatus::Cancelled {
                    return Ok(current);
                }
                return Err(error);
            }
            self.queue.get_item(item_id)
        }

        fn block_provider_and_get(
            &self,
            item: &QueueItem,
            phase: BlockedPhase,
            status: crate::control_domain::ProviderGateStatus,
            message: &str,
        ) -> Result<QueueItem> {
            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("provider outcome has no integration effort")?;
            self.block_provider_effort(item, &effort, phase, status, message)?;
            self.queue.get_item(&item.id)
        }

        fn block_provider_effort(
            &self,
            item: &QueueItem,
            effort: &crate::control_store::IntegrationEffort,
            phase: BlockedPhase,
            status: crate::control_domain::ProviderGateStatus,
            message: &str,
        ) -> Result<()> {
            let candidate_sha = match &effort.state {
                crate::control_domain::IntegrationEffortState::Validating(value) => {
                    value.candidate_sha.clone()
                }
                crate::control_domain::IntegrationEffortState::Landing(value) => {
                    value.candidate_sha.clone()
                }
                crate::control_domain::IntegrationEffortState::LandingUncertain(value) => {
                    value.candidate_sha.clone()
                }
                _ => anyhow::bail!("provider blocker requires candidate or landing authority"),
            };
            self.control_store.block_provider(
                &effort.id,
                crate::control_domain::ProviderSignoffBlocker {
                    gate: crate::control_domain::ProviderGateKind::Provider,
                    repository: item
                        .pr_url
                        .clone()
                        .context("provider item has no exact provider URL")?,
                    context: phase.to_string(),
                    candidate_sha,
                    status,
                    evidence: message.to_string(),
                },
            )
        }

        fn integrate_provider_item(
            &self,
            mut item: QueueItem,
            attempt: &Attempt,
            pr_url: &str,
        ) -> Result<QueueItem> {
            let provider = match crate::providers::provider_for_url(pr_url) {
                Ok(provider) => provider,
                Err(error) => {
                    let effort = self
                        .control_store
                        .effort_for_item(&item.id)?
                        .context("provider item has no integration effort")?;
                    let candidate_sha = effort
                        .state
                        .candidate_sha()
                        .context("provider URL failure has no exact candidate authority")?;
                    self.control_store.block_infrastructure(
                        &effort.id,
                        crate::control_domain::InfrastructureBlocker {
                            component:
                                crate::control_domain::InfrastructureComponent::Configuration,
                            operation: "select_provider_adapter".into(),
                            cause: crate::control_domain::InfrastructureCause::Unavailable {
                                detail: format!(
                                    "PR/MR URL {pr_url:?} is invalid or unsupported for exact candidate {candidate_sha}: {error:#}"
                                ),
                            },
                        },
                    )?;
                    return self.queue.get_item(&item.id);
                }
            };
            let registered = self.queue.repository_if_exists(&item.repo_key)?.is_some();
            if item.landing.is_uncertain() && registered {
                let workspace = self.load_owned_workspace(&item)?;
                if let Err(error) = self.fetch_target_supervised(&item, attempt) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!(
                            "failed to fetch target while reconciling fenced exact landing: {error}"
                        ),
                    );
                }
                let remote_ref = format!(
                    "refs/remotes/{}/{}",
                    self.options.base_remote, item.target_branch
                );
                let remote_sha = git_output(&self.options.repo_path, ["rev-parse", &remote_ref])?;
                return self.reconcile_fenced_exact_landing(
                    &item,
                    attempt,
                    &workspace,
                    &remote_ref,
                    &remote_sha,
                );
            }
            if item.landing.is_uncertain() {
                match self.reconcile_provider_landing(&item, attempt, provider.as_ref(), pr_url) {
                    Ok(Some(integrated)) => return Ok(integrated),
                    Ok(None) => {
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Integrating,
                            BlockedReason::Provider,
                            "fenced provider landing remains unresolved; retry after the provider reaches a terminal state",
                        );
                    }
                    Err(error) => {
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Integrating,
                            BlockedReason::Provider,
                            &format!("failed to reconcile fenced provider landing: {error}"),
                        );
                    }
                }
            }
            match self.push_provider_resolution_branch_if_needed(&item, attempt) {
                Ok(Some(updated)) => item = updated,
                Ok(None) => {}
                Err(error) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("failed to push PR/MR conflict resolution: {error}"),
                    );
                }
            }
            if let Err(error) = self.fetch_target_supervised(&item, attempt) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("failed to fetch target before provider policy check: {error}"),
                );
            }
            let snapshot = match provider.snapshot(pr_url) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("provider snapshot failed: {error}"),
                    );
                }
            };
            if snapshot.head_sha != item.current_head_sha {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    "PR/MR head does not match queued source head",
                );
            }
            let remote_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            if git_output(&self.options.repo_path, ["rev-parse", &remote_ref])? != snapshot.base_sha
            {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Provider,
                    "provider base SHA differs from the exact fetched target",
                );
            }
            match snapshot.gate {
                crate::providers::ProviderGate::Pass => {}
                crate::providers::ProviderGate::Pending(message) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        &message,
                    );
                }
                crate::providers::ProviderGate::Fail(message) => {
                    return self.block_provider_and_get(
                        &item,
                        BlockedPhase::Integrating,
                        crate::control_domain::ProviderGateStatus::Failed,
                        &message,
                    );
                }
            }

            let current_attempt = self.queue.get_attempt(&attempt.id)?;
            let needs_moved_base_reconciliation = current_attempt.target_base_sha.as_deref()
                != Some(snapshot.base_sha.as_str())
                || (!matches!(
                    current_attempt.moved_base,
                    crate::sqlite::MovedBaseState::None
                ) && current_attempt.validated_commit_sha.is_none());
            if needs_moved_base_reconciliation {
                let workspace = self.load_owned_workspace(&item)?;
                if let Some(blocked) = self.merge_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &snapshot.base_sha,
                    "PR/MR base moved before provider landing",
                )? {
                    return Ok(blocked);
                }
                if let Some(blocked) = self.revalidate_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &snapshot.base_sha,
                    "PR/MR base moved",
                )? {
                    return Ok(blocked);
                }
                if let Some(cancelled) = self.cancelled_item(&item.id)? {
                    return Ok(cancelled);
                }
            }

            let workspace = self.load_owned_workspace(&item)?;
            let candidate_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
            if let Err(error) = self.verify_candidate_graph(&item, attempt, &workspace) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("provider candidate graph is invalid before signoff: {error}"),
                );
            }
            if self.requires_trusted_policy(&item)? {
                if let Some(blocked) =
                    self.require_exact_policy_signoff(&item, attempt, &workspace, &candidate_sha)?
                {
                    return Ok(blocked);
                }
            } else if let IntegrationPolicy::Validation {
                signoff: HostSignoffPolicy::Required(signoff),
                ..
            } = &self.policy
            {
                if let Some(blocked) =
                    self.sign_candidate(&item, attempt, &workspace, &candidate_sha, signoff)?
                {
                    return Ok(blocked);
                }
            }
            if registered {
                if let Err(error) = self.fetch_target_supervised(&item, attempt) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("failed to fetch target before final provider gate: {error}"),
                    );
                }
                let final_target = git_output(&self.options.repo_path, ["rev-parse", &remote_ref])?;
                let final_snapshot = match provider.snapshot(pr_url) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Integrating,
                            BlockedReason::Infra,
                            &format!("final provider snapshot failed: {error}"),
                        );
                    }
                };
                if final_snapshot.head_sha != item.current_head_sha {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::NeedsAgentFix,
                        "PR/MR head moved before exact target push",
                    );
                }
                if final_snapshot.base_sha != final_target {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        "final provider base differs from the exact fetched target",
                    );
                }
                match final_snapshot.gate {
                    crate::providers::ProviderGate::Pass => {}
                    crate::providers::ProviderGate::Pending(message) => {
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Integrating,
                            BlockedReason::Provider,
                            &message,
                        );
                    }
                    crate::providers::ProviderGate::Fail(message) => {
                        return self.block_provider_and_get(
                            &item,
                            BlockedPhase::Integrating,
                            crate::control_domain::ProviderGateStatus::Failed,
                            &message,
                        );
                    }
                }
                let current_attempt = self.queue.get_attempt(&attempt.id)?;
                if current_attempt.target_base_sha.as_deref() != Some(final_target.as_str()) {
                    self.queue.record_event(
                        &item.id,
                        "target_moved_after_signoff",
                        &format!(
                            "target moved to {final_target}; rebuilding and invalidating evidence"
                        ),
                    )?;
                    if let Some(blocked) = self.merge_moved_base(
                        &item,
                        attempt,
                        &workspace,
                        &final_target,
                        "target branch moved before final provider gate",
                    )? {
                        return Ok(blocked);
                    }
                    if let Some(blocked) = self.revalidate_moved_base(
                        &item,
                        attempt,
                        &workspace,
                        &final_target,
                        "target moved before final provider gate",
                    )? {
                        return Ok(blocked);
                    }
                    if let Some(cancelled) = self.cancelled_item(&item.id)? {
                        return Ok(cancelled);
                    }
                    return self.queue.get_item(&item.id);
                }
                return self.land_exact_candidate(
                    &item,
                    attempt,
                    &workspace,
                    &candidate_sha,
                    &final_target,
                );
            }
            let signed_snapshot = match provider.snapshot(pr_url) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("provider snapshot after signoff failed: {error}"),
                    );
                }
            };
            if signed_snapshot.head_sha != item.current_head_sha {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    "PR/MR head moved after exact candidate signoff",
                );
            }
            if signed_snapshot.base_sha != snapshot.base_sha {
                if let Err(error) = self.fetch_target_supervised(&item, attempt) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("failed to fetch provider base after signoff: {error}"),
                    );
                }
                if git_output(&self.options.repo_path, ["rev-parse", &remote_ref])?
                    != signed_snapshot.base_sha
                {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        "provider base after signoff differs from the exact fetched target",
                    );
                }
                self.queue.record_event(
                    &item.id,
                    "target_moved_after_signoff",
                    &format!(
                        "provider base moved from {} to {}; exact evidence was invalidated",
                        snapshot.base_sha, signed_snapshot.base_sha
                    ),
                )?;
                if let Some(blocked) = self.merge_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &signed_snapshot.base_sha,
                    "PR/MR base moved after signoff",
                )? {
                    return Ok(blocked);
                }
                if let Some(blocked) = self.revalidate_moved_base(
                    &item,
                    attempt,
                    &workspace,
                    &signed_snapshot.base_sha,
                    "PR/MR base moved after signoff",
                )? {
                    return Ok(blocked);
                }
                return self.queue.get_item(&item.id);
            }
            match signed_snapshot.gate {
                crate::providers::ProviderGate::Pass => {}
                crate::providers::ProviderGate::Pending(message) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        &message,
                    );
                }
                crate::providers::ProviderGate::Fail(message) => {
                    return self.block_provider_and_get(
                        &item,
                        BlockedPhase::Integrating,
                        crate::control_domain::ProviderGateStatus::Failed,
                        &message,
                    );
                }
            }

            if let Some(cancelled) = self.begin_landing_owned(
                &item.id,
                &attempt.id,
                &candidate_sha,
                &signed_snapshot.base_sha,
                &format!("provider-merge:{}", item.id),
            )? {
                return Ok(cancelled);
            }
            let merge_command = provider.merge_command(pr_url, &item.current_head_sha);
            let merge_error = self
                .run_supervised_landing_command(
                    &item.id,
                    &attempt.id,
                    &merge_command.program,
                    &merge_command.args,
                    None,
                )
                .err();
            match self.reconcile_provider_landing(&item, attempt, provider.as_ref(), pr_url) {
                Ok(Some(integrated)) => Ok(integrated),
                Ok(None) => self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Provider,
                    &match merge_error {
                        Some(error) => {
                            format!("provider merge failed or remained unconfirmed: {error}")
                        }
                        None => "provider merge did not report the exact landed commit SHA".into(),
                    },
                ),
                Err(error) => self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Provider,
                    &match merge_error {
                        Some(merge_error) => format!(
                            "provider merge outcome is unknown after {merge_error}; reconciliation failed: {error}"
                        ),
                        None => format!("failed to reconcile provider merge: {error}"),
                    },
                ),
            }
        }

        fn reconcile_provider_landing(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            provider: &dyn crate::providers::ProviderAdapter,
            pr_url: &str,
        ) -> Result<Option<QueueItem>> {
            self.ensure_repo_lease()?;
            let Some(landing) = provider
                .landing(pr_url)
                .context("query exact provider landing revision")?
            else {
                return Ok(None);
            };
            if landing.head_sha != item.current_head_sha {
                anyhow::bail!(
                    "provider landed head {}, expected queued head {}",
                    landing.head_sha,
                    item.current_head_sha
                );
            }
            self.fetch_target_supervised(item, attempt)
                .context("fetch target while reconciling provider landing")?;
            let remote_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            let persisted_attempt = self.queue.get_attempt(&attempt.id)?;
            let expected_base = persisted_attempt
                .target_base_sha
                .as_deref()
                .context("provider landing attempt has no validated target base")?;
            let validated_commit = persisted_attempt
                .validated_commit_sha
                .as_deref()
                .context("provider landing attempt has no validated commit")?;
            let landed_tree = git_output(
                &self.options.repo_path,
                ["rev-parse", &format!("{}^{{tree}}", landing.commit_sha)],
            )?;
            let validated_tree = git_output(
                &self.options.repo_path,
                ["rev-parse", &format!("{validated_commit}^{{tree}}")],
            )?;
            if landed_tree != validated_tree {
                anyhow::bail!(
                    "provider-landed tree {landed_tree} differs from validated tree {validated_tree}"
                );
            }
            let first_parent = git_output(
                &self.options.repo_path,
                ["rev-parse", &format!("{}^1", landing.commit_sha)],
            )?;
            let validated_fast_forward = landing.commit_sha == item.current_head_sha
                && git_is_ancestor(&self.options.repo_path, expected_base, &landing.commit_sha)?;
            if first_parent != expected_base && !validated_fast_forward {
                anyhow::bail!(
                    "provider landed on base {first_parent}, expected validated base {expected_base}"
                );
            }
            git(
                &self.options.repo_path,
                [
                    "merge-base",
                    "--is-ancestor",
                    &landing.commit_sha,
                    &remote_ref,
                ],
            )
            .with_context(|| {
                format!(
                    "remote target does not contain provider-landed commit {}",
                    landing.commit_sha
                )
            })?;
            let remote_target_sha =
                git_output(&self.options.repo_path, ["rev-parse", &remote_ref])?;
            self.reconcile_registered_checkout(item, &attempt.id, &remote_target_sha)?;
            self.mark_integrated_owned(
                &item.id,
                &attempt.id,
                &landing.commit_sha,
                &remote_target_sha,
            )
            .map(Some)
        }

        fn push_provider_resolution_branch_if_needed(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
        ) -> Result<Option<QueueItem>> {
            let events = self.queue.events(&item.id)?;
            if !events
                .iter()
                .any(|event| event.event_type == "merge_resumed")
            {
                return Ok(None);
            }
            let workspace = self.load_owned_workspace(item)?;
            let workspace_head = git_output(&workspace, ["rev-parse", "HEAD"])?;
            if workspace_head == item.current_head_sha {
                return Ok(None);
            }
            self.ensure_registered_remote_identity()?;
            let push_ref = format!("HEAD:refs/heads/{}", item.source_branch);
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["push", &self.options.base_remote, &push_ref],
                Some(&workspace),
            )?;
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["fetch", &self.options.base_remote, &item.source_branch],
                Some(&self.options.repo_path),
            )?;
            self.ensure_repo_lease()?;
            self.queue.record_event(
                &item.id,
                "source_branch_pushed",
                &format!(
                    "pushed conflict resolution {} to {}",
                    workspace_head, item.source_branch
                ),
            )?;
            self.queue
                .update_current_head(&item.id, &workspace_head)
                .map(Some)
        }

        pub fn workspace_status(&self) -> Result<Vec<WorkspaceStatus>> {
            workspace_status(&self.queue.reader(), &self.options.repo_key)
        }

        pub fn reset_workspaces(&self) -> Result<Vec<PathBuf>> {
            self.ensure_registered_remote_identity()?;
            let _operation = RepositoryOperationLease::acquire(
                self.queue.clone(),
                &self.options.repo_path,
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )?;
            self.synchronize_workspace_generation()?;
            self.with_lease_heartbeat("workspace cleanup", || self.reconcile_workspaces())
        }

        fn synchronize_workspace_generation(&self) -> Result<()> {
            let generation = self
                .queue
                .workspace_root_generation(&self.options.repo_key)?;
            self.workspaces.synchronize_generation(generation)
        }

        fn reconcile_workspaces(&self) -> Result<Vec<PathBuf>> {
            if self
                .queue
                .has_workspace_gc_debt(&self.workspaces.registry_identity)?
            {
                self.gc_workspaces()?;
            }
            let items = self
                .queue
                .list_items()?
                .into_iter()
                .filter(|item| item.repo_key == self.options.repo_key)
                .collect::<Vec<_>>();
            let inventory = self.workspaces.list()?;
            let inventory_paths = inventory
                .iter()
                .map(|identity| PathBuf::from(&identity.path))
                .collect::<HashSet<_>>();
            for entry in fs::read_dir(&self.workspaces.root).with_context(|| {
                format!(
                    "inspect IQ workspace root {}",
                    self.workspaces.root.display()
                )
            })? {
                let path = entry?.path();
                if matches!(
                    path.file_name(),
                    Some(name)
                        if name == OsStr::new(".iq-workspace-owner.json")
                            || name == OsStr::new(".iq-workspace-generation")
                ) {
                    continue;
                }
                if is_rift_workspace_root_entry(&path)? {
                    continue;
                }
                if !inventory_paths.contains(&path) {
                    anyhow::bail!(
                        "IQ workspace root contains unknown entry {}",
                        path.display()
                    );
                }
            }
            let mut retained_ids = HashSet::new();
            let mut removed = Vec::new();
            for item in &items {
                let expected = self.workspaces.expected_path(&item.id)?;
                let terminal = matches!(
                    item.status,
                    QueueStatus::Integrated | QueueStatus::Cancelled
                );
                match &item.workspace {
                    WorkspaceState::Cleaned { .. } => {
                        if entry_exists(&expected)?
                            && !inventory
                                .iter()
                                .any(|candidate| Path::new(&candidate.path) == expected)
                        {
                            anyhow::bail!(
                                "cleaned item {} has unknown workspace entry {}",
                                item.id,
                                expected.display()
                            );
                        }
                    }
                    WorkspaceState::NotCreated => {
                        if entry_exists(&expected)? {
                            anyhow::bail!(
                                "item {} has workspace entry {} without creation intent",
                                item.id,
                                expected.display()
                            );
                        }
                        if terminal {
                            self.queue.mark_workspace_cleaned(&item.id)?;
                        }
                    }
                    WorkspaceState::CreationIntent { path } => {
                        let stored = self.workspaces.normalize_owned_path(Path::new(path))?;
                        if stored != expected {
                            anyhow::bail!(
                                "item {} workspace {} does not match IQ-owned path {}",
                                item.id,
                                stored.display(),
                                expected.display()
                            );
                        }
                        let existing = inventory
                            .iter()
                            .find(|candidate| Path::new(&candidate.path) == expected);
                        if terminal {
                            if let Some(identity) = existing {
                                if self.remove_retained_workspace(identity)? {
                                    removed.push(expected);
                                }
                            } else if entry_exists(&expected)? {
                                anyhow::bail!(
                                    "terminal item {} has unknown partial Rift entry {}",
                                    item.id,
                                    expected.display()
                                );
                            }
                            self.queue.mark_workspace_cleaned(&item.id)?;
                        } else if let Some(identity) = existing {
                            if item.status != QueueStatus::Merging {
                                anyhow::bail!(
                                    "item {} in {} has only a Rift creation intent",
                                    item.id,
                                    item.status
                                );
                            }
                            self.queue.set_workspace_identity(
                                &item.id,
                                &identity.path,
                                &identity.rift_id,
                                &identity.source_rift_id,
                            )?;
                            retained_ids.insert(identity.rift_id.clone());
                        } else if entry_exists(&expected)? {
                            anyhow::bail!(
                                "item {} has unknown partial Rift entry {}",
                                item.id,
                                expected.display()
                            );
                        } else if !matches!(item.status, QueueStatus::Ready | QueueStatus::Merging)
                        {
                            anyhow::bail!(
                                "active item {} in {} is missing its Rift workspace {}",
                                item.id,
                                item.status,
                                expected.display()
                            );
                        }
                    }
                    WorkspaceState::Retained { identity } => {
                        let stored = self
                            .workspaces
                            .normalize_owned_path(Path::new(&identity.path))?;
                        if stored != expected {
                            anyhow::bail!(
                                "item {} workspace {} does not match IQ-owned path {}",
                                item.id,
                                stored.display(),
                                expected.display()
                            );
                        }
                        if identity.source_rift_id != self.workspaces.source_id {
                            anyhow::bail!(
                                "item {} Rift source changed from {} to {}",
                                item.id,
                                identity.source_rift_id,
                                self.workspaces.source_id
                            );
                        }
                        let existing = inventory
                            .iter()
                            .find(|candidate| candidate.rift_id == identity.rift_id);
                        match existing {
                            Some(actual) if terminal => {
                                self.require_clean_terminal_workspace(actual)?;
                                self.remove_retained_workspace(identity)?;
                                removed.push(PathBuf::from(&actual.path));
                                self.queue.mark_workspace_cleaned(&item.id)?;
                            }
                            Some(actual) if actual.path != identity.path => anyhow::bail!(
                                "item {} Rift {} moved from {} to {}",
                                item.id,
                                identity.rift_id,
                                identity.path,
                                actual.path
                            ),
                            Some(actual) => {
                                retained_ids.insert(actual.rift_id.clone());
                            }
                            None if terminal => {
                                self.remove_retained_workspace(identity)?;
                                self.queue.mark_workspace_cleaned(&item.id)?;
                            }
                            None => anyhow::bail!(
                                "active item {} is missing retained Rift {}",
                                item.id,
                                identity.rift_id
                            ),
                        }
                    }
                }
            }

            for identity in inventory {
                let workspace = PathBuf::from(&identity.path);
                if workspace.parent() != Some(self.workspaces.root.as_path()) {
                    continue;
                }
                if retained_ids.contains(&identity.rift_id) {
                    continue;
                }
                if self.remove_retained_workspace(&identity)? {
                    removed.push(workspace);
                }
            }
            Ok(removed)
        }

        fn fetch_for_merge<I, S>(&self, item: &QueueItem, attempt: &Attempt, args: I) -> Result<()>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            self.ensure_registered_remote_identity()?;
            self.run_supervised_item_command(
                &item.id,
                &attempt.id,
                QueueStatus::Merging,
                "git",
                args,
                Some(&self.options.repo_path),
                StdDuration::from_secs(60),
                "merge fetch",
            )?;
            Ok(())
        }

        fn fetch_target_supervised(&self, item: &QueueItem, attempt: &Attempt) -> Result<()> {
            self.ensure_repo_lease()?;
            self.ensure_registered_remote_identity()?;
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["fetch", &self.options.base_remote, &item.target_branch],
                Some(&self.options.repo_path),
            )?;
            Ok(())
        }

        fn source_remote_sha(&self, item: &QueueItem) -> Result<String> {
            let source_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.source_branch
            );
            git_output(&self.options.repo_path, ["rev-parse", &source_ref])
        }

        fn enforce_item_boundary(&self, item: &QueueItem) -> Result<Option<QueueItem>> {
            let queued_repo = Path::new(&item.repo_path)
                .canonicalize()
                .with_context(|| format!("resolve queued repository path {}", item.repo_path))?;
            let expected_target = self
                .options
                .repo_key
                .rsplit_once("::")
                .map(|(_, target)| target)
                .context("configured repo_key has no target scope")?;
            if queued_repo == self.options.repo_path && item.target_branch == expected_target {
                return Ok(None);
            }
            let phase = match item.status {
                QueueStatus::Ready => {
                    self.transition_item_owned(&item.id, QueueStatus::Merging)?;
                    BlockedPhase::Merging
                }
                QueueStatus::Merging => BlockedPhase::Merging,
                QueueStatus::Merged => {
                    self.transition_item_owned(&item.id, QueueStatus::Validating)?;
                    BlockedPhase::Validating
                }
                QueueStatus::Validating => BlockedPhase::Validating,
                QueueStatus::Validated => {
                    self.transition_item_owned(&item.id, QueueStatus::Integrating)?;
                    BlockedPhase::Integrating
                }
                QueueStatus::Integrating => BlockedPhase::Integrating,
                _ => anyhow::bail!(
                    "item {} in status {} cannot be checked against host policy",
                    item.id,
                    item.status
                ),
            };
            self.block_and_get(
                &item.id,
                phase,
                BlockedReason::Infra,
                &format!(
                    "queued repository/target {}::{} does not match host policy {}::{}; cancel and enqueue on the correct queue",
                    queued_repo.display(),
                    item.target_branch,
                    self.options.repo_path.display(),
                    expected_target
                ),
            )
            .map(Some)
        }

        fn evidence_dir(&self, item: &QueueItem, attempt: &Attempt) -> Result<EvidenceDirectory> {
            self.queue.database_id()?;
            let safe_item = item.id.replace('/', "-");
            let safe_attempt = attempt.id.replace('/', "-");
            let queue_parent = self
                .options
                .queue_db
                .parent()
                .context("queue database has no parent for evidence storage")?;
            let queue_parent_directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(queue_parent)
                .with_context(|| {
                    format!("open queue evidence parent {}", queue_parent.display())
                })?;
            let evidence_root = queue_parent.join("evidence");
            let item_dir = evidence_root.join(&safe_item);
            let attempt_dir = item_dir.join(&safe_attempt);
            let evidence_directory =
                ensure_directory_child(&queue_parent_directory, OsStr::new("evidence"))?;
            let item_directory =
                ensure_directory_child(&evidence_directory, OsStr::new(&safe_item))?;
            let attempt_directory =
                ensure_directory_child(&item_directory, OsStr::new(&safe_attempt))?;
            Ok(EvidenceDirectory {
                path: attempt_dir,
                directory: attempt_directory,
            })
        }
    }

    #[derive(Clone, Debug, serde::Serialize)]
    pub struct WorkspaceStatus {
        pub item_id: String,
        pub status: QueueStatus,
        pub path: PathBuf,
        pub exists: bool,
        pub dirty: bool,
        pub conflict_files: Vec<String>,
    }

    pub fn workspace_status(
        queue: &SqliteQueueReader,
        repo_key: &str,
    ) -> Result<Vec<WorkspaceStatus>> {
        let items = queue.list_items()?;
        let mut statuses = Vec::new();
        for item in items.into_iter().filter(|item| item.repo_key == repo_key) {
            let Some(path) = item.workspace.path().map(PathBuf::from) else {
                continue;
            };
            let observation = observe_workspace(&path)?;
            let exists = observation.is_some();
            let (dirty, conflict_files) = observation.unwrap_or_default();
            statuses.push(WorkspaceStatus {
                item_id: item.id,
                status: item.status,
                path,
                exists,
                dirty,
                conflict_files,
            });
        }
        Ok(statuses)
    }

    pub fn git<I, S>(cwd: &Path, args: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<OsString>>();
        let output = git_status(cwd, &args)?;
        if output.status.success() {
            Ok(())
        } else {
            anyhow::bail!(
                "git {:?} failed in {}: {}",
                args,
                cwd.display(),
                String::from_utf8_lossy(&output.stderr)
            )
        }
    }

    pub fn git_output<I, S>(cwd: &Path, args: I) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<OsString>>();
        let output = git_status(cwd, &args)?;
        if !output.status.success() {
            anyhow::bail!(
                "git {:?} failed in {}: {}",
                args,
                cwd.display(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn definite_force_with_lease_rejection(output: &Output) -> bool {
        if output.status.success() {
            return false;
        }
        let report = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let porcelain_rejection = report.lines().any(|line| {
            line.starts_with("!\t")
                && ((line.contains("[rejected]") && line.contains("(stale info)"))
                    || (line.contains("[remote rejected]")
                        && line.contains("(failed to update ref)")))
        });
        porcelain_rejection
            && (report.contains("(stale info)")
                || (report.contains("cannot lock ref") && report.contains("but expected")))
    }

    fn verify_one_parent_candidate(cwd: &Path, candidate: &str, base: &str) -> Result<()> {
        let graph = git_output(cwd, ["rev-list", "--parents", "-n", "1", candidate])?;
        let fields = graph.split_whitespace().collect::<Vec<_>>();
        if fields.as_slice() != [candidate, base] {
            anyhow::bail!("squash candidate {candidate} must have exactly one parent {base}");
        }
        let candidate_tree = git_output(cwd, ["rev-parse", &format!("{candidate}^{{tree}}")])?;
        let base_tree = git_output(cwd, ["rev-parse", &format!("{base}^{{tree}}")])?;
        if candidate_tree == base_tree {
            anyhow::bail!("squash candidate is empty relative to exact target base");
        }
        Ok(())
    }

    fn git_is_ancestor(cwd: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
        let output = git_status(cwd, ["merge-base", "--is-ancestor", ancestor, descendant])?;
        if output.status.success() {
            Ok(true)
        } else if output.status.code() == Some(1) {
            Ok(false)
        } else {
            anyhow::bail!(
                "git merge-base --is-ancestor {ancestor} {descendant} failed in {}: {}",
                cwd.display(),
                String::from_utf8_lossy(&output.stderr)
            )
        }
    }

    fn command_output_timeout<I, S>(
        program: &str,
        args: I,
        cwd: Option<&Path>,
        timeout: StdDuration,
        authorize_start: impl FnOnce(&mut dyn Write) -> Result<bool>,
        mut check_authority: impl FnMut() -> Result<ExecutionAuthority>,
    ) -> Result<CommandOutputOutcome>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        const POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);
        const CANCELLATION_GRACE: StdDuration = StdDuration::from_millis(50);
        const TIMEOUT_GRACE: StdDuration = StdDuration::from_secs(5);

        let mut process = gated_process(program, args);
        if let Some(cwd) = cwd {
            process.current_dir(cwd);
        }
        process.stdout(Stdio::piped()).stderr(Stdio::piped());
        let Some((mut child, process_group, authority_pipe)) =
            spawn_authorized(process, authorize_start, &format!("run {program}"))?
        else {
            return Ok(CommandOutputOutcome::Cancelled);
        };
        let stdout = child.stdout.take().context("capture command stdout")?;
        let stderr = child.stderr.take().context("capture command stderr")?;
        let stdout_thread = thread::spawn(move || capture_memory_bounded(stdout));
        let stderr_thread = thread::spawn(move || capture_memory_bounded(stderr));
        let deadline = Instant::now() + timeout;
        let mut timed_out = false;
        let mut cancelled = false;
        let mut cancellation_failure_started = None;
        let mut cancellation_error = None;
        let mut next_cancellation_check = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            let current_time = Instant::now();
            if current_time >= next_cancellation_check {
                match authority_state(&mut check_authority, &mut cancellation_failure_started) {
                    Ok(Some(ExecutionAuthority::Cancelled)) => {
                        cancelled = true;
                        break terminate_process_group(
                            &mut child,
                            process_group,
                            CANCELLATION_GRACE,
                        )?;
                    }
                    Ok(Some(ExecutionAuthority::Lost(message))) => {
                        cancellation_error = Some(anyhow::anyhow!(message));
                        break terminate_process_group(
                            &mut child,
                            process_group,
                            CANCELLATION_GRACE,
                        )?;
                    }
                    Ok(Some(ExecutionAuthority::Active)) | Ok(None) => {}
                    Err(error) => {
                        cancellation_error = Some(error);
                        break terminate_process_group(
                            &mut child,
                            process_group,
                            CANCELLATION_GRACE,
                        )?;
                    }
                }
                next_cancellation_check = current_time + CANCELLATION_POLL_INTERVAL;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                timed_out = true;
                break terminate_process_group(&mut child, process_group, TIMEOUT_GRACE)?;
            }
            if let Some(status) = child.wait_timeout(POLL_INTERVAL.min(remaining))? {
                break status;
            }
        };
        drop(authority_pipe);
        signal_process_group(process_group, libc::SIGKILL)?;
        let stdout = stdout_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stdout capture thread panicked"))??;
        let stderr = stderr_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stderr capture thread panicked"))??;
        if cancelled {
            return Ok(CommandOutputOutcome::Cancelled);
        }
        if let Some(error) = cancellation_error {
            return Err(error).context("monitor command cancellation");
        }
        match wait_for_authority_state(&mut check_authority, &mut cancellation_failure_started)
            .context("check command authority after command exit")?
        {
            ExecutionAuthority::Active => {}
            ExecutionAuthority::Cancelled => return Ok(CommandOutputOutcome::Cancelled),
            ExecutionAuthority::Lost(message) => anyhow::bail!(message),
        }
        if timed_out {
            anyhow::bail!("{program} timed out after {} seconds", timeout.as_secs());
        }
        Ok(CommandOutputOutcome::Exited(Output {
            status,
            stdout,
            stderr,
        }))
    }

    fn capture_memory_bounded(mut input: impl Read) -> Result<Vec<u8>> {
        const MAX_BYTES: usize = 1024 * 1024;
        let mut output = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            let remaining = MAX_BYTES.saturating_sub(output.len());
            output.extend_from_slice(&buffer[..remaining.min(count)]);
        }
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn run_evidence_command(
        command: &str,
        cwd: &Path,
        log_path: &Path,
        log_directory: &fs::File,
        environment: &[(&str, &str)],
        timeout: StdDuration,
        authorize_start: impl FnOnce(&mut dyn Write) -> Result<bool>,
        mut check_authority: impl FnMut() -> Result<ExecutionAuthority>,
    ) -> Result<EvidenceCommandOutcome> {
        const POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);
        const CANCELLATION_GRACE: StdDuration = StdDuration::from_millis(50);
        const TIMEOUT_GRACE: StdDuration = StdDuration::from_secs(5);

        let stdout_path = log_path.with_extension("stdout.tmp");
        let stderr_path = log_path.with_extension("stderr.tmp");
        let log_name = log_path
            .file_name()
            .context("evidence log has no file name")?;
        let stdout_name = stdout_path
            .file_name()
            .context("evidence stdout has no file name")?;
        let stderr_name = stderr_path
            .file_name()
            .context("evidence stderr has no file name")?;
        let stdout_capture =
            create_file_at(log_directory, stdout_name, "temporary evidence stdout")?;
        let stderr_capture =
            create_file_at(log_directory, stderr_name, "temporary evidence stderr")?;
        let mut process = gated_process("sh", ["-lc", command]);
        process
            .current_dir(cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in environment {
            process.env(key, value);
        }
        let Some((mut child, process_group, authority_pipe)) = spawn_authorized(
            process,
            authorize_start,
            &format!("run evidence command: {command}"),
        )?
        else {
            let mut log = create_file_at(log_directory, log_name, "evidence log")?;
            writeln!(log, "$ {command}\n\n[IQ cancelled before command start]")?;
            remove_file_at(log_directory, stdout_name, "evidence stdout")?;
            remove_file_at(log_directory, stderr_name, "evidence stderr")?;
            return Ok(EvidenceCommandOutcome::Cancelled(None));
        };
        let stdout = child.stdout.take().context("capture command stdout")?;
        let stderr = child.stderr.take().context("capture command stderr")?;
        let stdout_thread = thread::spawn(move || capture_bounded(stdout, stdout_capture));
        let stderr_thread = thread::spawn(move || capture_bounded(stderr, stderr_capture));
        let deadline = Instant::now() + timeout;
        let mut timed_out = false;
        let mut cancelled = false;
        let mut cancellation_failure_started = None;
        let mut cancellation_error = None;
        let mut next_cancellation_check = Instant::now();
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("poll evidence command: {command}"))?
            {
                break status;
            }
            let current_time = Instant::now();
            if current_time >= next_cancellation_check {
                match authority_state(&mut check_authority, &mut cancellation_failure_started) {
                    Ok(Some(ExecutionAuthority::Cancelled)) => {
                        cancelled = true;
                        break terminate_process_group(
                            &mut child,
                            process_group,
                            CANCELLATION_GRACE,
                        )?;
                    }
                    Ok(Some(ExecutionAuthority::Lost(message))) => {
                        cancellation_error = Some(anyhow::anyhow!(message));
                        break terminate_process_group(
                            &mut child,
                            process_group,
                            CANCELLATION_GRACE,
                        )?;
                    }
                    Ok(Some(ExecutionAuthority::Active)) | Ok(None) => {}
                    Err(error) => {
                        cancellation_error = Some(error);
                        break terminate_process_group(
                            &mut child,
                            process_group,
                            CANCELLATION_GRACE,
                        )?;
                    }
                }
                next_cancellation_check = current_time + CANCELLATION_POLL_INTERVAL;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                timed_out = true;
                break terminate_process_group(&mut child, process_group, TIMEOUT_GRACE)?;
            }
            if let Some(status) = child
                .wait_timeout(POLL_INTERVAL.min(remaining))
                .with_context(|| format!("wait for evidence command: {command}"))?
            {
                break status;
            }
        };
        drop(authority_pipe);
        signal_process_group(process_group, libc::SIGKILL)?;
        stdout_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stdout capture thread panicked"))??;
        stderr_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stderr capture thread panicked"))??;

        let mut log = create_file_at(log_directory, log_name, "evidence log")?;
        writeln!(log, "$ {command}\n\n--- stdout ---")?;
        let mut stdout_file = open_file_at(log_directory, stdout_name, "evidence stdout")?;
        std::io::copy(&mut stdout_file, &mut log)?;
        writeln!(log, "\n--- stderr ---")?;
        let mut stderr_file = open_file_at(log_directory, stderr_name, "evidence stderr")?;
        std::io::copy(&mut stderr_file, &mut log)?;
        let final_cancellation_error = if let Some(error) = cancellation_error {
            Some(error)
        } else if cancelled {
            None
        } else {
            match wait_for_authority_state(&mut check_authority, &mut cancellation_failure_started)
            {
                Ok(ExecutionAuthority::Active) => None,
                Ok(ExecutionAuthority::Cancelled) => {
                    cancelled = true;
                    None
                }
                Ok(ExecutionAuthority::Lost(message)) => Some(anyhow::anyhow!(message)),
                Err(error) => Some(error),
            }
        };
        if cancelled {
            writeln!(log, "\n[IQ cancelled command]")?;
        } else if let Some(error) = final_cancellation_error.as_ref() {
            writeln!(
                log,
                "\n[IQ could not verify cancellation after command exit: {error:#}]"
            )?;
        }
        remove_file_at(log_directory, stdout_name, "evidence stdout")?;
        remove_file_at(log_directory, stderr_name, "evidence stderr")?;
        if let Some(error) = final_cancellation_error {
            return Err(error).context("check evidence command cancellation after command exit");
        }
        if cancelled {
            return Ok(EvidenceCommandOutcome::Cancelled(Some(status)));
        }
        if timed_out {
            return Ok(EvidenceCommandOutcome::TimedOut(status));
        }
        Ok(EvidenceCommandOutcome::Exited(status))
    }

    const CANCELLATION_POLL_INTERVAL: StdDuration = StdDuration::from_millis(100);
    const CANCELLATION_FAILURE_GRACE: StdDuration = StdDuration::from_millis(100);

    fn authority_state(
        check_authority: &mut impl FnMut() -> Result<ExecutionAuthority>,
        failure_started: &mut Option<Instant>,
    ) -> Result<Option<ExecutionAuthority>> {
        match check_authority() {
            Ok(authority) => {
                if failure_started.take().is_some() {
                    eprintln!("IQ command authority probe recovered");
                }
                Ok(Some(authority))
            }
            Err(error) => {
                let started = failure_started.get_or_insert_with(|| {
                    eprintln!("IQ command authority probe unavailable; command continues briefly");
                    Instant::now()
                });
                if started.elapsed() >= CANCELLATION_FAILURE_GRACE {
                    return Err(error).context("command authority probe remained unavailable");
                }
                Ok(None)
            }
        }
    }

    fn wait_for_authority_state(
        check_authority: &mut impl FnMut() -> Result<ExecutionAuthority>,
        failure_started: &mut Option<Instant>,
    ) -> Result<ExecutionAuthority> {
        loop {
            if let Some(authority) = authority_state(check_authority, failure_started)? {
                return Ok(authority);
            }
            thread::sleep(CANCELLATION_POLL_INTERVAL);
        }
    }

    fn gated_process<I, S>(program: &str, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut process = Command::new("sh");
        process
            .arg("-c")
            .arg(
                r#"IFS= read -r gate && [ "$gate" = run ] || exit 125
exec 3<&0
"$@" &
command_pid=$!
(
  while IFS= read -r _; do :; done
  kill -TERM -$$ 2>/dev/null || true
) <&3 &
watcher_pid=$!
wait "$command_pid"
status=$?
kill "$watcher_pid" 2>/dev/null || true
wait "$watcher_pid" 2>/dev/null || true
exit "$status""#,
            )
            .arg("iq-command-gate")
            .arg(program)
            .args(args)
            .stdin(Stdio::piped())
            .process_group(0);
        process
    }

    fn spawn_authorized(
        mut process: Command,
        authorize_start: impl FnOnce(&mut dyn Write) -> Result<bool>,
        description: &str,
    ) -> Result<Option<(std::process::Child, i32, std::process::ChildStdin)>> {
        const CANCELLATION_GRACE: StdDuration = StdDuration::from_millis(50);

        let mut child = process.spawn().with_context(|| description.to_string())?;
        let process_group = -(child.id() as i32);
        let mut gate = match child.stdin.take() {
            Some(gate) => gate,
            None => {
                terminate_process_group(&mut child, process_group, CANCELLATION_GRACE)?;
                anyhow::bail!("capture command admission gate");
            }
        };
        let authorized = match authorize_start(&mut gate) {
            Ok(authorized) => authorized,
            Err(error) => {
                drop(gate);
                terminate_process_group(&mut child, process_group, CANCELLATION_GRACE)?;
                return Err(error).context("authorize command start");
            }
        };
        if !authorized {
            drop(gate);
            terminate_process_group(&mut child, process_group, CANCELLATION_GRACE)?;
            return Ok(None);
        }
        Ok(Some((child, process_group, gate)))
    }

    fn terminate_process_group(
        child: &mut std::process::Child,
        process_group: i32,
        grace: StdDuration,
    ) -> Result<ExitStatus> {
        signal_process_group(process_group, libc::SIGTERM)?;
        if let Some(status) = child.wait_timeout(grace)? {
            return Ok(status);
        }
        signal_process_group(process_group, libc::SIGKILL)?;
        child.wait().context("reap terminated command")
    }

    fn signal_process_group(process_group: i32, signal: i32) -> Result<()> {
        if unsafe { libc::kill(process_group, signal) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(error).context("signal command process group")
        }
    }

    fn capture_bounded(mut input: impl Read, mut output: fs::File) -> Result<()> {
        const HEAD_BYTES: usize = 2 * 1024 * 1024;
        const TAIL_BYTES: usize = 2 * 1024 * 1024;
        let mut buffer = [0_u8; 8192];
        let mut head_written = 0_usize;
        let mut tail = VecDeque::with_capacity(TAIL_BYTES);
        let mut total = 0_usize;
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            total = total.saturating_add(count);
            let head_remaining = HEAD_BYTES.saturating_sub(head_written);
            let head_count = head_remaining.min(count);
            if head_count > 0 {
                output.write_all(&buffer[..head_count])?;
                head_written += head_count;
            }
            for byte in &buffer[head_count..count] {
                if tail.len() == TAIL_BYTES {
                    tail.pop_front();
                }
                tail.push_back(*byte);
            }
        }
        let retained = head_written + tail.len();
        if total > retained {
            writeln!(
                output,
                "\n[IQ omitted {} middle output bytes]\n",
                total - retained
            )?;
        }
        let (first, second) = tail.as_slices();
        output.write_all(first)?;
        output.write_all(second)?;
        Ok(())
    }

    fn workspace_dirty(workspace: &Path) -> Result<Option<String>> {
        let status = git_observe_output(
            workspace,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )?;
        let dirty = status
            .split('\0')
            .filter(|record| !record.is_empty())
            .filter(|record| {
                let Some(path) = record.strip_prefix("?? ") else {
                    return true;
                };
                path != ".iq-agent-protocol" && !path.starts_with(".iq-agent-protocol/")
            })
            .take(20)
            .collect::<Vec<_>>();
        if dirty.is_empty() {
            Ok(None)
        } else {
            Ok(Some(dirty.join("; ")))
        }
    }

    pub fn git_status<I, S>(cwd: &Path, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Command::new("git")
            .args(["-c", "commit.gpgSign=false"])
            .args(args)
            .current_dir(cwd)
            .output()
            .with_context(|| format!("run git in {}", cwd.display()))
    }

    fn git_observe_output<I, S>(cwd: &Path, args: I) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<OsString>>();
        let output = Command::new("git")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .args(&args)
            .current_dir(cwd)
            .output()
            .with_context(|| format!("run observational git in {}", cwd.display()))?;
        if !output.status.success() {
            anyhow::bail!(
                "observational git {:?} failed in {}: {}",
                args,
                cwd.display(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn observe_workspace(workspace: &Path) -> Result<Option<(bool, Vec<String>)>> {
        if !entry_exists(workspace)? {
            return Ok(None);
        }
        let status = match git_observe_output(
            workspace,
            ["status", "--porcelain=v1", "--untracked-files=all"],
        ) {
            Ok(status) => status,
            Err(_) if !entry_exists(workspace)? => return Ok(None),
            Err(error) => return Err(error),
        };
        let conflicts =
            match git_observe_output(workspace, ["diff", "--name-only", "--diff-filter=U"]) {
                Ok(conflicts) => conflicts,
                Err(_) if !entry_exists(workspace)? => return Ok(None),
                Err(error) => return Err(error),
            };
        Ok(Some((
            !status.is_empty(),
            conflicts
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(str::to_string)
                .collect(),
        )))
    }

    fn conflict_files(workspace: &Path) -> Result<Vec<String>> {
        Ok(
            git_output(workspace, ["diff", "--name-only", "--diff-filter=U"])?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(str::to_string)
                .collect(),
        )
    }

    fn conflict_blob(workspace: &Path, stage: u8, path: &str) -> Result<Option<String>> {
        let object = format!(":{stage}:{path}");
        let output = git_status(workspace, ["rev-parse", "--verify", object.as_str()])?;
        if output.status.success() {
            Ok(Some(String::from_utf8(output.stdout)?.trim().to_string()))
        } else {
            Ok(None)
        }
    }

    fn instruction_identities(
        workspace: &Path,
    ) -> Result<Vec<crate::agent_protocol::InstructionIdentity>> {
        use std::os::unix::ffi::OsStrExt;
        let mut identities = Vec::new();
        for name in ["AGENTS.md", "DOMAIN_LANGUAGE.md"] {
            let path = workspace.join(name);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    let bytes = fs::read(&path)?;
                    identities.push(crate::agent_protocol::InstructionIdentity {
                        path: crate::control_domain::EncodedPath::from_bytes(
                            OsStr::new(name).as_bytes(),
                        )?,
                        sha256: format!("{:x}", Sha256::digest(bytes)),
                    });
                }
                Ok(_) => anyhow::bail!("repository instruction path is not a regular file: {name}"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("inspect repository instruction file"),
            }
        }
        Ok(identities)
    }

    fn workspace_has_unstaged_changes(workspace: &Path) -> Result<bool> {
        if !git_status(workspace, ["diff", "--quiet"])?.status.success() {
            return Ok(true);
        }
        let untracked = git_output(
            workspace,
            ["ls-files", "--others", "--exclude-standard", "-z"],
        )?;
        Ok(untracked
            .split('\0')
            .filter(|path| !path.is_empty())
            .any(|path| !path.starts_with(".iq-agent-protocol/")))
    }

    fn remove_cycle_protocol(workspace: &Path, cycle_id: &str) -> Result<()> {
        crate::agent_protocol::remove_protocol_cycle(workspace, cycle_id)
    }
}

pub mod providers {
    use anyhow::{Context, Result};
    use serde::Deserialize;
    use std::process::Command;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum ProviderKind {
        GitHub,
        GitLab,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum ProviderGate {
        Pass,
        Pending(String),
        Fail(String),
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ProviderSnapshot {
        pub head_sha: String,
        pub base_sha: String,
        pub gate: ProviderGate,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ProviderMergeCommand {
        pub program: String,
        pub args: Vec<String>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ProviderLanding {
        pub head_sha: String,
        pub commit_sha: String,
    }

    pub trait ProviderAdapter {
        fn kind(&self) -> ProviderKind;
        fn snapshot(&self, url: &str) -> Result<ProviderSnapshot>;
        fn merge_command(&self, url: &str, expected_head_sha: &str) -> ProviderMergeCommand;
        fn landing(&self, url: &str) -> Result<Option<ProviderLanding>>;
    }

    pub fn provider_for_url(url: &str) -> Result<Box<dyn ProviderAdapter>> {
        let (scheme, location) = url.split_once("://").context("PR/MR URL has no scheme")?;
        if !matches!(scheme, "http" | "https") {
            anyhow::bail!("unsupported PR/MR URL scheme: {scheme}");
        }
        let (host, path) = location
            .split_once('/')
            .context("PR/MR URL has no repository path")?;
        let path = path
            .split(['?', '#'])
            .next()
            .context("PR/MR URL has no path")?;
        let segments = path.split('/').collect::<Vec<_>>();
        let github_pull = host.eq_ignore_ascii_case("github.com")
            && matches!(segments.as_slice(), [owner, repository, "pull", number] if !owner.is_empty() && !repository.is_empty() && number.parse::<u64>().is_ok());
        let gitlab_merge_request = host.to_ascii_lowercase().contains("gitlab")
            && segments.len() >= 5
            && segments[segments.len() - 3] == "-"
            && segments[segments.len() - 2] == "merge_requests"
            && segments[segments.len() - 1].parse::<u64>().is_ok()
            && segments[..segments.len() - 3]
                .iter()
                .all(|segment| !segment.is_empty());
        if github_pull {
            Ok(Box::new(GitHubProvider))
        } else if gitlab_merge_request {
            Ok(Box::new(GitLabProvider))
        } else {
            anyhow::bail!("unsupported PR/MR provider URL: {url}")
        }
    }

    #[derive(Default)]
    pub struct GitHubProvider;

    impl ProviderAdapter for GitHubProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::GitHub
        }

        fn snapshot(&self, url: &str) -> Result<ProviderSnapshot> {
            let value = provider_json(
                std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into()),
                [
                    "pr",
                    "view",
                    url,
                    "--json",
                    "headRefOid,baseRefOid,reviewDecision,statusCheckRollup,mergeStateStatus",
                ],
            )?;
            let parsed: GitHubPrView =
                serde_json::from_value(value).context("parse gh pr view JSON")?;
            let gate = github_gate(&parsed);
            Ok(ProviderSnapshot {
                head_sha: parsed.head_ref_oid,
                base_sha: parsed.base_ref_oid,
                gate,
            })
        }

        fn merge_command(&self, url: &str, expected_head_sha: &str) -> ProviderMergeCommand {
            ProviderMergeCommand {
                program: std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into()),
                args: [
                    "pr",
                    "merge",
                    url,
                    "--merge",
                    "--match-head-commit",
                    expected_head_sha,
                ]
                .into_iter()
                .map(str::to_string)
                .collect(),
            }
        }

        fn landing(&self, url: &str) -> Result<Option<ProviderLanding>> {
            github_landing(url)
        }
    }

    #[derive(Default)]
    pub struct GitLabProvider;

    impl ProviderAdapter for GitLabProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::GitLab
        }

        fn snapshot(&self, url: &str) -> Result<ProviderSnapshot> {
            let value = provider_json(
                std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into()),
                ["mr", "view", url, "--output", "json"],
            )?;
            let parsed: GitLabMrView =
                serde_json::from_value(value).context("parse glab mr view JSON")?;
            Ok(ProviderSnapshot {
                head_sha: parsed
                    .head_sha
                    .or(parsed.sha)
                    .context("glab MR JSON missing head_sha/sha")?,
                base_sha: parsed
                    .base_sha
                    .or(parsed.diff_refs.and_then(|refs| refs.base_sha))
                    .context("glab MR JSON missing base_sha/diff_refs.base_sha")?,
                gate: gitlab_gate(&parsed.state, &parsed.pipeline_status, &parsed.approved),
            })
        }

        fn merge_command(&self, url: &str, expected_head_sha: &str) -> ProviderMergeCommand {
            ProviderMergeCommand {
                program: std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into()),
                args: ["mr", "merge", url, "--yes", "--sha", expected_head_sha]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            }
        }

        fn landing(&self, url: &str) -> Result<Option<ProviderLanding>> {
            gitlab_landing(url)
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GitHubPrView {
        head_ref_oid: String,
        base_ref_oid: String,
        review_decision: Option<String>,
        merge_state_status: Option<String>,
        #[serde(default)]
        status_check_rollup: Vec<serde_json::Value>,
    }

    #[derive(Debug, Deserialize)]
    struct GitLabMrView {
        sha: Option<String>,
        head_sha: Option<String>,
        base_sha: Option<String>,
        merge_commit_sha: Option<String>,
        squash_commit_sha: Option<String>,
        state: Option<String>,
        pipeline_status: Option<String>,
        approved: Option<bool>,
        diff_refs: Option<GitLabDiffRefs>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GitHubMergeView {
        head_ref_oid: String,
        merge_commit: Option<GitHubMergeCommit>,
    }

    #[derive(Debug, Deserialize)]
    struct GitHubMergeCommit {
        oid: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct GitLabDiffRefs {
        base_sha: Option<String>,
    }

    fn github_gate(view: &GitHubPrView) -> ProviderGate {
        if matches!(view.review_decision.as_deref(), Some("CHANGES_REQUESTED")) {
            return ProviderGate::Fail("GitHub review requested changes".into());
        }
        if matches!(view.review_decision.as_deref(), Some("REVIEW_REQUIRED")) {
            return ProviderGate::Pending("GitHub review approval is still required".into());
        }
        if matches!(
            view.merge_state_status.as_deref(),
            Some("BLOCKED") | Some("DIRTY") | Some("UNKNOWN")
        ) {
            return ProviderGate::Pending(format!(
                "GitHub merge state is {}",
                view.merge_state_status.as_deref().unwrap_or("unknown")
            ));
        }
        for check in &view.status_check_rollup {
            let conclusion = check.get("conclusion").and_then(|value| value.as_str());
            let status = check.get("status").and_then(|value| value.as_str());
            if matches!(
                conclusion,
                Some("FAILURE" | "CANCELLED" | "TIMED_OUT" | "ACTION_REQUIRED")
            ) {
                return ProviderGate::Fail("GitHub required check failed".into());
            }
            if !matches!(status, None | Some("COMPLETED")) || matches!(conclusion, None | Some(""))
            {
                return ProviderGate::Pending("GitHub required checks are pending".into());
            }
        }
        ProviderGate::Pass
    }

    fn gitlab_gate(
        state: &Option<String>,
        pipeline_status: &Option<String>,
        approved: &Option<bool>,
    ) -> ProviderGate {
        if !matches!(state.as_deref(), None | Some("opened" | "open")) {
            return ProviderGate::Pending(format!(
                "GitLab MR state is {}",
                state.as_deref().unwrap_or("unknown")
            ));
        }
        match pipeline_status.as_deref() {
            Some("failed" | "canceled" | "skipped") => {
                return ProviderGate::Fail("GitLab pipeline failed".into())
            }
            Some("pending" | "running" | "created" | "manual") => {
                return ProviderGate::Pending("GitLab pipeline is pending".into())
            }
            _ => {}
        }
        if approved == &Some(false) {
            return ProviderGate::Pending("GitLab MR approvals are still required".into());
        }
        ProviderGate::Pass
    }

    fn github_landing(url: &str) -> Result<Option<ProviderLanding>> {
        let value = provider_json(
            std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into()),
            ["pr", "view", url, "--json", "headRefOid,mergeCommit"],
        )?;
        let parsed: GitHubMergeView =
            serde_json::from_value(value).context("parse gh merge JSON")?;
        Ok(parsed
            .merge_commit
            .and_then(|commit| commit.oid)
            .map(|commit_sha| ProviderLanding {
                head_sha: parsed.head_ref_oid,
                commit_sha,
            }))
    }

    fn gitlab_landing(url: &str) -> Result<Option<ProviderLanding>> {
        let value = provider_json(
            std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into()),
            ["mr", "view", url, "--output", "json"],
        )?;
        let parsed: GitLabMrView =
            serde_json::from_value(value).context("parse glab merged MR JSON")?;
        let commit_sha = parsed.merge_commit_sha.or(parsed.squash_commit_sha);
        let head_sha = parsed.head_sha.or(parsed.sha);
        match (head_sha, commit_sha) {
            (_, None) => Ok(None),
            (Some(head_sha), Some(commit_sha)) => Ok(Some(ProviderLanding {
                head_sha,
                commit_sha,
            })),
            (None, Some(_)) => anyhow::bail!("glab merged MR JSON missing head_sha/sha"),
        }
    }

    fn provider_json<I, S>(program: String, args: I) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let output = Command::new(&program)
            .args(args)
            .output()
            .with_context(|| {
                format!("run provider CLI {program}; install CLI or set provider credentials")
            })?;
        if !output.status.success() {
            anyhow::bail!(
                "provider CLI {program} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        serde_json::from_slice(&output.stdout).context("parse provider CLI JSON")
    }
}

pub mod issue_backends {
    use crate::core::{BlockedPhase, BlockedReason, QueueStatus};
    use crate::sqlite::{Prompt, QueueEvent, QueueItem};
    use anyhow::{Context, Result};
    use serde::Deserialize;
    use std::collections::HashSet;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::Duration;
    use wait_timeout::ChildExt;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IssueProjection {
        pub title: String,
        pub labels: Vec<String>,
        pub body: String,
        pub comments: Vec<String>,
    }

    pub trait IssueBackendAdapter {
        fn project_item(
            &self,
            item: &QueueItem,
            events: &[QueueEvent],
            prompts: &[Prompt],
        ) -> IssueProjection;
    }

    pub trait IssueRemoteAdapter {
        fn sync_projection(
            &self,
            target: &IssueSyncTarget,
            projection: &IssueProjection,
        ) -> Result<IssueSyncResult>;
        fn close(&self, target: &IssueSyncTarget) -> Result<()>;
        fn verify_destination(&self, repo: &str) -> Result<()>;
        fn answer_comments(&self, target: &IssueSyncTarget) -> Result<Vec<IssueAnswerComment>>;
    }

    #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
    pub struct IssueSyncTarget {
        pub repo: String,
        pub issue: Option<String>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
    pub struct IssueSyncResult {
        pub issue: String,
        pub url: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IssueAnswerComment {
        pub id: String,
        pub actor: Option<String>,
        pub body: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum IssueProvider {
        GitHub,
        GitLab,
    }

    pub fn issue_adapter_for_provider(
        provider: IssueProvider,
    ) -> Result<Box<dyn IssueRemoteAdapter>> {
        match provider {
            IssueProvider::GitHub => Ok(Box::new(GitHubIssueBackend)),
            IssueProvider::GitLab => Ok(Box::new(GitLabIssueBackend)),
        }
    }

    #[derive(Clone, Debug)]
    pub struct MarkdownIssueBackend {
        pub provider: IssueProvider,
    }

    impl IssueBackendAdapter for MarkdownIssueBackend {
        fn project_item(
            &self,
            item: &QueueItem,
            events: &[QueueEvent],
            prompts: &[Prompt],
        ) -> IssueProjection {
            let provider_prefix = match self.provider {
                IssueProvider::GitHub => "iq",
                IssueProvider::GitLab => "iq",
            };
            let mut labels = vec![
                format!("{provider_prefix}:queue"),
                format!("{provider_prefix}:status:{}", item.status),
            ];
            if let Some(reason) = item.blocked_reason {
                labels.push(format!("{provider_prefix}:blocked:{reason}"));
            }
            let mut body = format!(
                "<!-- iq:item:{} -->\nrepo: `{}`\nsource: `{}`\ntarget: `{}`\nhead: `{}`\nstatus: `{}`\n",
                item.id, item.repo_key, item.source_branch, item.target_branch, item.current_head_sha, item.status
            );
            if let Some(pr_url) = &item.pr_url {
                body.push_str(&format!("pr: {pr_url}\n"));
            }
            if let (Some(phase), Some(reason)) = (item.blocked_phase, item.blocked_reason) {
                body.push_str(&format!("blocked: `{phase}` / `{reason}`\n"));
            }
            let mut comments = Vec::new();
            for event in events {
                comments.push(format!(
                    "<!-- iq:event:{} -->\n**{}**: {}",
                    event.id, event.event_type, event.message
                ));
            }
            for prompt in prompts {
                if prompt.status == "open" && !prompt.options.is_empty() {
                    let options = prompt
                        .options
                        .iter()
                        .filter(|option| option.as_str() != "accept-current")
                        .map(|option| format!("- `iq answer {} {option}`", prompt.id))
                        .collect::<Vec<_>>();
                    if !options.is_empty() {
                        comments.push(format!(
                            "<!-- iq:prompt:{} -->\n**Decision required ({})**: {}\n\nReply with exactly one allowed answer:\n{}",
                            prompt.id,
                            prompt.blocked_phase,
                            prompt.question,
                            options.join("\n")
                        ));
                    }
                } else if prompt.status == "answered" {
                    if let Some(answer) = prompt.answer.as_deref() {
                        comments.push(format!(
                            "<!-- iq:prompt-status:{}:answered -->\n**Decision accepted**: `{answer}`. IQ resumed `{}` automatically.",
                            prompt.id, prompt.blocked_phase
                        ));
                    } else {
                        comments.push(format!(
                            "<!-- iq:prompt-status:{}:invalid -->\n**IQ state error**: answered prompt has no recorded answer.",
                            prompt.id
                        ));
                    }
                }
            }
            IssueProjection {
                title: format!(
                    "Integration queue: {} → {}",
                    item.source_branch, item.target_branch
                ),
                labels,
                body,
                comments,
            }
        }
    }

    #[allow(dead_code)]
    fn _keep_enums_linked(_: QueueStatus, _: BlockedPhase, _: BlockedReason) {}

    struct GitHubIssueBackend;

    impl IssueRemoteAdapter for GitHubIssueBackend {
        fn sync_projection(
            &self,
            target: &IssueSyncTarget,
            projection: &IssueProjection,
        ) -> Result<IssueSyncResult> {
            let program = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
            let issue = if let Some(issue) = target.issue.as_ref() {
                let existing = github_issue_view(&program, target, issue)?;
                let label_update = ManagedLabelUpdate::new(&existing.labels, &projection.labels);
                github_issue_edit(&program, target, issue, projection, &label_update)?;
                sync_missing_github_comments(
                    &program,
                    target,
                    issue,
                    projection,
                    &existing.comments,
                )?;
                issue.clone()
            } else if let Some(issue) = find_github_issue(&program, target, projection)? {
                let existing = github_issue_view(&program, target, &issue)?;
                let label_update = ManagedLabelUpdate::new(&existing.labels, &projection.labels);
                github_issue_edit(&program, target, &issue, projection, &label_update)?;
                sync_missing_github_comments(
                    &program,
                    target,
                    &issue,
                    projection,
                    &existing.comments,
                )?;
                issue
            } else {
                let mut args = vec![
                    "issue".to_string(),
                    "create".to_string(),
                    "--repo".to_string(),
                    target.repo.clone(),
                    "--title".to_string(),
                    projection.title.clone(),
                    "--body".to_string(),
                    projection.body.clone(),
                ];
                if !projection.labels.is_empty() {
                    args.push("--label".to_string());
                    args.push(projection.labels.join(","));
                }
                let output = command_output(&program, args)?;
                let issue =
                    parse_issue_number(&output).context("parse GitHub created issue number")?;
                sync_missing_github_comments(&program, target, &issue, projection, &[])?;
                issue
            };
            Ok(IssueSyncResult {
                url: format!("https://github.com/{}/issues/{issue}", target.repo),
                issue,
            })
        }

        fn close(&self, target: &IssueSyncTarget) -> Result<()> {
            let issue = target
                .issue
                .as_deref()
                .context("GitHub issue number required")?;
            let program = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
            command_ok(
                &program,
                [
                    "issue",
                    "close",
                    issue,
                    "--repo",
                    &target.repo,
                    "--reason",
                    "completed",
                ],
            )
        }

        fn verify_destination(&self, repo: &str) -> Result<()> {
            let program = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
            command_ok(&program, ["repo", "view", repo, "--json", "nameWithOwner"])
        }

        fn answer_comments(&self, target: &IssueSyncTarget) -> Result<Vec<IssueAnswerComment>> {
            let issue = target
                .issue
                .as_deref()
                .context("GitHub issue number required")?;
            let program = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
            let view = github_issue_view(&program, target, issue)?;
            issue_answer_comments(view.comments, "GitHub")
        }
    }

    struct GitLabIssueBackend;

    impl IssueRemoteAdapter for GitLabIssueBackend {
        fn sync_projection(
            &self,
            target: &IssueSyncTarget,
            projection: &IssueProjection,
        ) -> Result<IssueSyncResult> {
            let program = std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into());
            let issue = if let Some(issue) = target.issue.as_ref() {
                let existing = gitlab_issue_view(&program, target, issue)?;
                let label_update = ManagedLabelUpdate::new(&existing.labels, &projection.labels);
                gitlab_issue_update(&program, target, issue, projection, &label_update)?;
                sync_missing_gitlab_comments(
                    &program,
                    target,
                    issue,
                    projection,
                    &existing.comments,
                )?;
                issue.clone()
            } else if let Some(issue) = find_gitlab_issue(&program, target, projection)? {
                let existing = gitlab_issue_view(&program, target, &issue)?;
                let label_update = ManagedLabelUpdate::new(&existing.labels, &projection.labels);
                gitlab_issue_update(&program, target, &issue, projection, &label_update)?;
                sync_missing_gitlab_comments(
                    &program,
                    target,
                    &issue,
                    projection,
                    &existing.comments,
                )?;
                issue
            } else {
                let mut args = vec![
                    "issue".to_string(),
                    "create".to_string(),
                    "--repo".to_string(),
                    target.repo.clone(),
                    "--title".to_string(),
                    projection.title.clone(),
                    "--description".to_string(),
                    projection.body.clone(),
                ];
                if !projection.labels.is_empty() {
                    args.push("--label".to_string());
                    args.push(projection.labels.join(","));
                }
                let output = command_output(&program, args)?;
                let issue =
                    parse_issue_number(&output).context("parse GitLab created issue number")?;
                sync_missing_gitlab_comments(&program, target, &issue, projection, &[])?;
                issue
            };
            Ok(IssueSyncResult {
                url: format!("https://gitlab.com/{}/-/issues/{issue}", target.repo),
                issue,
            })
        }

        fn close(&self, target: &IssueSyncTarget) -> Result<()> {
            let issue = target
                .issue
                .as_deref()
                .context("GitLab issue number required")?;
            let program = std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into());
            command_ok(&program, ["issue", "close", issue, "--repo", &target.repo])
        }

        fn verify_destination(&self, repo: &str) -> Result<()> {
            let program = std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into());
            command_ok(&program, ["repo", "view", repo, "--output", "json"])
        }

        fn answer_comments(&self, target: &IssueSyncTarget) -> Result<Vec<IssueAnswerComment>> {
            let issue = target
                .issue
                .as_deref()
                .context("GitLab issue number required")?;
            let program = std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into());
            issue_answer_comments(gitlab_issue_notes(&program, target, issue)?, "GitLab")
        }
    }

    fn find_github_issue(
        program: &str,
        target: &IssueSyncTarget,
        projection: &IssueProjection,
    ) -> Result<Option<String>> {
        let marker = projection_identity_marker(&projection.body)
            .context("issue projection is missing a stable IQ marker")?;
        if marker.starts_with("iq:binding:") {
            let output = command_output(
                program,
                [
                    "api",
                    &format!("repos/{}/issues", target.repo),
                    "--method",
                    "GET",
                    "--raw-field",
                    "state=all",
                    "--raw-field",
                    "per_page=100",
                    "--paginate",
                    "--slurp",
                ],
            )?;
            let pages: Vec<Vec<ListedIssue>> =
                serde_json::from_str(&output).context("parse paginated GitHub issue inventory")?;
            return find_exact_issue_candidates(
                pages.into_iter().flatten().collect(),
                &marker,
                "GitHub",
            );
        }
        let output = command_output(
            program,
            [
                "issue",
                "list",
                "--repo",
                &target.repo,
                "--state",
                "all",
                "--search",
                &format!("\"{marker}\" in:body"),
                "--json",
                "number,url,body",
                "--limit",
                "10",
            ],
        )?;
        find_exact_issue(&output, &marker, "GitHub")
    }

    fn find_gitlab_issue(
        program: &str,
        target: &IssueSyncTarget,
        projection: &IssueProjection,
    ) -> Result<Option<String>> {
        let marker = projection_identity_marker(&projection.body)
            .context("issue projection is missing a stable IQ marker")?;
        if marker.starts_with("iq:binding:") {
            let output = command_output(
                program,
                [
                    "api",
                    &format!("projects/{}/issues", percent_encode_path(&target.repo)),
                    "--paginate",
                ],
            )?;
            return find_exact_issue(&output, &marker, "GitLab");
        }
        let output = command_output(
            program,
            [
                "issue",
                "list",
                "--repo",
                &target.repo,
                "--all",
                "--search",
                &marker,
                "--in",
                "description",
                "--output",
                "json",
            ],
        )?;
        find_exact_issue(&output, &marker, "GitLab")
    }

    fn find_exact_issue(output: &str, marker: &str, provider: &str) -> Result<Option<String>> {
        if output.trim().is_empty() {
            return Ok(None);
        }
        let candidates: Vec<ListedIssue> = serde_json::from_str(output)
            .with_context(|| format!("parse {provider} issue search"))?;
        find_exact_issue_candidates(candidates, marker, provider)
    }

    fn find_exact_issue_candidates(
        candidates: Vec<ListedIssue>,
        marker: &str,
        provider: &str,
    ) -> Result<Option<String>> {
        let exact = candidates
            .into_iter()
            .filter(|candidate| {
                candidate
                    .body
                    .as_deref()
                    .is_some_and(|body| body.contains(marker))
            })
            .filter_map(|candidate| {
                candidate
                    .number
                    .or(candidate.iid)
                    .map(|number| number.to_string())
            })
            .collect::<Vec<_>>();
        match exact.as_slice() {
            [] => Ok(None),
            [issue] => Ok(Some(issue.clone())),
            _ => anyhow::bail!("multiple {provider} issues contain IQ marker {marker}"),
        }
    }

    fn percent_encode_path(value: &str) -> String {
        value
            .bytes()
            .flat_map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    vec![byte as char]
                }
                other => format!("%{other:02X}").chars().collect(),
            })
            .collect()
    }

    fn projection_identity_marker(body: &str) -> Option<String> {
        body.lines()
            .find_map(|line| {
                line.trim()
                    .strip_prefix("<!-- iq:binding:")
                    .and_then(|tail| tail.strip_suffix(" -->"))
                    .map(|id| format!("iq:binding:{id}"))
            })
            .or_else(|| issue_marker(body))
    }

    fn github_issue_edit(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
        projection: &IssueProjection,
        label_update: &ManagedLabelUpdate,
    ) -> Result<()> {
        let mut args = vec![
            "issue".to_string(),
            "edit".to_string(),
            issue.to_string(),
            "--repo".to_string(),
            target.repo.clone(),
            "--title".to_string(),
            projection.title.clone(),
            "--body".to_string(),
            projection.body.clone(),
        ];
        if !label_update.add.is_empty() {
            args.push("--add-label".to_string());
            args.push(label_update.add.join(","));
        }
        if !label_update.remove.is_empty() {
            args.push("--remove-label".to_string());
            args.push(label_update.remove.join(","));
        }
        command_ok(program, args)
    }

    fn github_issue_view(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
    ) -> Result<IssueView> {
        let value = command_json(
            program,
            [
                "issue",
                "view",
                issue,
                "--repo",
                &target.repo,
                "--json",
                "labels,comments",
            ],
        )?;
        serde_json::from_value(value).context("parse gh issue view")
    }

    fn gitlab_issue_view(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
    ) -> Result<IssueView> {
        let value = command_json(
            program,
            [
                "issue",
                "view",
                issue,
                "--repo",
                &target.repo,
                "--output",
                "json",
            ],
        )?;
        let mut view: IssueView = serde_json::from_value(value).context("parse glab issue view")?;
        let mut notes = gitlab_issue_notes(program, target, issue)?;
        view.comments.append(&mut notes);
        Ok(view)
    }

    fn gitlab_issue_notes(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
    ) -> Result<Vec<IssueComment>> {
        let output = command_output(
            program,
            [
                "api",
                &format!(
                    "projects/{}/issues/{issue}/notes",
                    percent_encode_path(&target.repo)
                ),
                "--paginate",
            ],
        )?;
        if output.trim().is_empty() {
            return Ok(Vec::new());
        }
        serde_json::from_str(&output).context("parse paginated GitLab issue notes")
    }

    fn gitlab_issue_update(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
        projection: &IssueProjection,
        label_update: &ManagedLabelUpdate,
    ) -> Result<()> {
        let mut args = vec![
            "issue".to_string(),
            "update".to_string(),
            issue.to_string(),
            "--repo".to_string(),
            target.repo.clone(),
            "--title".to_string(),
            projection.title.clone(),
            "--description".to_string(),
            projection.body.clone(),
        ];
        if !label_update.add.is_empty() {
            args.push("--label".to_string());
            args.push(label_update.add.join(","));
        }
        if !label_update.remove.is_empty() {
            args.push("--unlabel".to_string());
            args.push(label_update.remove.join(","));
        }
        command_ok(program, args)
    }

    fn sync_missing_github_comments(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
        projection: &IssueProjection,
        existing_comments: &[IssueComment],
    ) -> Result<()> {
        for comment in comments_missing_from_issue(&projection.comments, existing_comments) {
            command_ok(
                program,
                [
                    "issue",
                    "comment",
                    issue,
                    "--repo",
                    &target.repo,
                    "--body",
                    comment,
                ],
            )?;
        }
        Ok(())
    }

    fn sync_missing_gitlab_comments(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
        projection: &IssueProjection,
        existing_comments: &[IssueComment],
    ) -> Result<()> {
        for comment in comments_missing_from_issue(&projection.comments, existing_comments) {
            command_ok(
                program,
                [
                    "issue",
                    "note",
                    issue,
                    "--repo",
                    &target.repo,
                    "--message",
                    comment,
                ],
            )?;
        }
        Ok(())
    }

    fn comments_missing_from_issue<'a>(
        projection_comments: &'a [String],
        existing_comments: &[IssueComment],
    ) -> Vec<&'a str> {
        let existing_markers: HashSet<String> = existing_comments
            .iter()
            .filter_map(|comment| issue_marker(&comment.body))
            .collect();
        projection_comments
            .iter()
            .filter(|comment| {
                issue_marker(comment)
                    .map(|marker| !existing_markers.contains(&marker))
                    .unwrap_or(true)
            })
            .map(String::as_str)
            .collect()
    }

    fn issue_marker(body: &str) -> Option<String> {
        let start = body.find("<!-- iq:")?;
        let marker_tail = &body[start + "<!-- ".len()..];
        let end = marker_tail.find(" -->")?;
        Some(marker_tail[..end].to_string())
    }

    struct ManagedLabelUpdate {
        add: Vec<String>,
        remove: Vec<String>,
    }

    impl ManagedLabelUpdate {
        fn new(existing: &[IssueLabel], desired: &[String]) -> Self {
            let existing: HashSet<String> = existing.iter().filter_map(IssueLabel::name).collect();
            let desired: HashSet<String> = desired.iter().cloned().collect();
            let mut add: Vec<String> = desired.difference(&existing).cloned().collect();
            let mut remove: Vec<String> = existing
                .difference(&desired)
                .filter(|label| label.starts_with("iq:"))
                .cloned()
                .collect();
            add.sort();
            remove.sort();
            Self { add, remove }
        }
    }

    #[derive(Debug, Deserialize)]
    struct IssueView {
        #[serde(default)]
        labels: Vec<IssueLabel>,
        #[serde(default)]
        comments: Vec<IssueComment>,
    }

    #[derive(Debug, Deserialize)]
    struct ListedIssue {
        #[serde(default)]
        number: Option<u64>,
        #[serde(default)]
        iid: Option<u64>,
        #[serde(default, alias = "description")]
        body: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum IssueLabel {
        Named { name: String },
        Text(String),
    }

    impl IssueLabel {
        fn name(&self) -> Option<String> {
            match self {
                IssueLabel::Named { name } => Some(name.clone()),
                IssueLabel::Text(name) => Some(name.clone()),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct IssueComment {
        #[serde(default)]
        id: Option<serde_json::Value>,
        #[serde(default, alias = "body")]
        body: String,
        #[serde(default)]
        author: Option<IssueCommentAuthor>,
    }

    #[derive(Debug, Deserialize)]
    struct IssueCommentAuthor {
        #[serde(default, alias = "username")]
        login: Option<String>,
    }

    fn issue_answer_comments(
        comments: Vec<IssueComment>,
        provider: &str,
    ) -> Result<Vec<IssueAnswerComment>> {
        comments
            .into_iter()
            .filter(|comment| !comment.body.contains("<!-- iq:"))
            .map(|comment| {
                let id = match comment.id.context("provider answer comment has no ID")? {
                    serde_json::Value::String(value) if !value.is_empty() => value,
                    serde_json::Value::Number(value) => value.to_string(),
                    _ => anyhow::bail!("provider answer comment has an invalid ID"),
                };
                let actor = comment
                    .author
                    .and_then(|author| author.login)
                    .filter(|actor| !actor.is_empty());
                Ok(IssueAnswerComment {
                    id: format!("{provider}:{id}"),
                    actor,
                    body: comment.body,
                })
            })
            .collect()
    }

    fn parse_issue_number(output: &str) -> Option<String> {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(output) {
            if let Some(number) = value.get("number").and_then(|number| number.as_i64()) {
                return Some(number.to_string());
            }
            if let Some(url) = value.get("url").and_then(|url| url.as_str()) {
                return parse_issue_number(url);
            }
        }
        let trimmed = output.trim();
        let marker = if trimmed.contains("/-/issues/") {
            "/-/issues/"
        } else {
            "/issues/"
        };
        let tail = trimmed.split(marker).last()?;
        let number: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
        (!number.is_empty()).then_some(number)
    }

    fn command_json<I, S>(program: &str, args: I) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let output = command_output(program, args)?;
        serde_json::from_str(&output).context("parse issue CLI JSON")
    }

    fn command_output<I, S>(program: &str, args: I) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut child = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("run issue CLI {program}"))?;
        let stdout = child.stdout.take().context("capture issue CLI stdout")?;
        let stderr = child.stderr.take().context("capture issue CLI stderr")?;
        let stdout_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let result = std::io::BufReader::new(stdout).read_to_end(&mut bytes);
            (result, bytes)
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let result = std::io::BufReader::new(stderr).read_to_end(&mut bytes);
            (result, bytes)
        });
        let status = match child.wait_timeout(Duration::from_secs(30))? {
            Some(status) => status,
            None => {
                child.kill()?;
                child.wait()?;
                anyhow::bail!("issue CLI {program} timed out after 30 seconds");
            }
        };
        let (stdout_result, stdout) = stdout_reader
            .join()
            .map_err(|_| anyhow::anyhow!("issue CLI stdout reader panicked"))?;
        stdout_result?;
        let (stderr_result, stderr) = stderr_reader
            .join()
            .map_err(|_| anyhow::anyhow!("issue CLI stderr reader panicked"))?;
        stderr_result?;
        if !status.success() {
            anyhow::bail!(
                "issue CLI {program} failed: {}",
                String::from_utf8_lossy(&stderr)
            );
        }
        Ok(String::from_utf8_lossy(&stdout).trim().to_string())
    }

    fn command_ok<I, S>(program: &str, args: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        command_output(program, args).map(|_| ())
    }
}
