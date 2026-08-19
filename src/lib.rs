pub mod agent_config;
pub mod agent_protocol;
pub mod agent_runner;
pub mod composition;
pub mod control_api;
pub mod control_domain;
pub mod control_store;
pub mod git_command;
pub mod git_object;
pub mod notifications;
pub mod repository;
pub mod repository_policy;
#[doc(hidden)]
pub mod secure_fs;
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
    use sha2::{Digest, Sha256};
    use std::ffi::{OsStr, OsString};
    use std::fmt;
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::str::FromStr;
    use uuid::Uuid;

    use crate::core::{
        BlockedPhase, BlockedReason, LandingPolicy, QueueSource, QueueStatus, StateMachine,
    };

    pub(crate) fn configure_connection(connection: &Connection) -> Result<()> {
        connection.pragma_update(None, "recursive_triggers", "ON")?;
        let enabled: i64 =
            connection.query_row("PRAGMA recursive_triggers", [], |row| row.get(0))?;
        if enabled != 1 {
            anyhow::bail!("SQLite recursive trigger enforcement is unavailable");
        }
        Ok(())
    }

    #[derive(Clone, Debug)]
    pub struct DirectAdmissionRequest {
        pub repo_key: String,
        pub source_branch: String,
        pub current_head_sha: String,
        pub producer_metadata: Value,
        pub state_repository: crate::control_domain::StateRepositorySnapshot,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct MergeRequestAdmission {
        pub provider: crate::repository_policy::Provider,
        pub provider_host: String,
        pub repository: String,
        pub repository_id: String,
        pub target_branch: String,
        pub identity: String,
        pub url: String,
        pub source_branch: String,
        pub head_sha: String,
        pub base_sha: Option<String>,
        pub provider_merge_method: Option<crate::repository_policy::ProviderMergeMethod>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub enum QueueAdmission {
        LocalSubmission {
            source_branch: String,
            head_sha: String,
            source_ref: String,
            submission_id: String,
        },
        Direct {
            source_branch: String,
            head_sha: String,
        },
        MergeRequest(MergeRequestAdmission),
        HistoricalMergeRequest(MergeRequestAdmission),
    }

    pub(crate) struct ProviderLandingEvidence<'a> {
        pub item_id: &'a str,
        pub provider: crate::repository_policy::Provider,
        pub provider_host: &'a str,
        pub provider_repository: &'a str,
        pub provider_repository_id: &'a str,
        pub merge_request_identity: &'a str,
        pub admitted_base_sha: &'a str,
        pub admitted_head_sha: &'a str,
        pub validated_target_sha: &'a str,
        pub validated_candidate_sha: &'a str,
        pub validated_tree_sha: &'a str,
        pub landed_commit_sha: &'a str,
        pub landed_tree_sha: &'a str,
        pub first_parent_sha: &'a str,
        pub history_contract: &'a str,
        pub contains_admitted_head: bool,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ReplicationDebt {
        pub id: String,
        pub item_id: String,
        pub repo_key: String,
        pub canonical_source_sha: String,
        pub destination_key: String,
        pub target_branch: String,
        pub sequence: i64,
        pub replica: crate::repository_policy::GitRepository,
        pub expected_destination_sha: Option<String>,
        pub operation: String,
        pub outcome: String,
        pub application_id: Option<String>,
        pub failure: Option<String>,
        pub superseded_by_id: Option<String>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum PrivateRefKind {
        RepositoryTarget,
        Landing,
    }

    impl PrivateRefKind {
        fn as_str(self) -> &'static str {
            match self {
                Self::RepositoryTarget => "repository_target",
                Self::Landing => "landing",
            }
        }

        fn parse(value: &str) -> Result<Self> {
            match value {
                "repository_target" => Ok(Self::RepositoryTarget),
                "landing" => Ok(Self::Landing),
                _ => anyhow::bail!("unknown private-ref cleanup kind {value}"),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct PrivateRefCleanupDebt {
        pub repo_key: String,
        pub kind: PrivateRefKind,
        pub owner_id: String,
        pub ref_name: String,
        pub expected_sha: String,
    }

    impl QueueAdmission {
        pub fn merge_request(&self) -> Option<&MergeRequestAdmission> {
            match self {
                Self::MergeRequest(admission) | Self::HistoricalMergeRequest(admission) => {
                    Some(admission)
                }
                Self::LocalSubmission { .. } | Self::Direct { .. } => None,
            }
        }

        pub fn source_branch(&self) -> &str {
            match self {
                Self::LocalSubmission { source_branch, .. }
                | Self::Direct { source_branch, .. } => source_branch,
                Self::MergeRequest(admission) | Self::HistoricalMergeRequest(admission) => {
                    &admission.source_branch
                }
            }
        }

        pub fn head_sha(&self) -> &str {
            match self {
                Self::LocalSubmission { head_sha, .. } | Self::Direct { head_sha, .. } => head_sha,
                Self::MergeRequest(admission) | Self::HistoricalMergeRequest(admission) => {
                    &admission.head_sha
                }
            }
        }
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
        pub role: String,
        pub root: PathBuf,
        pub source: PathBuf,
        pub source_rift_id: String,
        pub registry_identity: String,
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
        pub owned_root_path: String,
        pub source_branch: String,
        pub target_branch: String,
        pub current_head_sha: String,
        pub admission: QueueAdmission,
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

        pub fn contains_external_landing_authority(&self) -> bool {
            match self {
                Self::Uncertain { .. } | Self::Landed { .. } => true,
                Self::Ready => false,
            }
        }
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
    pub struct RegisteredRemote {
        pub name: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct RegisteredRepository {
        pub key: String,
        pub owned_root_path: PathBuf,
        pub root_rift_id: String,
        pub registry_identity: PathBuf,
        pub registry_device: u64,
        pub registry_inode: u64,
        pub generation: i64,
        pub target_branch: String,
        pub remote: RegisteredRemote,
        pub development_root_path: PathBuf,
        pub integration_root_path: PathBuf,
        pub source_sha: String,
        pub checkout_reconciliation: CheckoutReconciliationState,
        pub policy: crate::repository_policy::RepositoryPolicy,
        pub policy_revision: i64,
        pub created_at: String,
        pub updated_at: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WorkspaceRootIdentity {
        pub path: PathBuf,
        pub source: PathBuf,
        pub source_rift_id: String,
        pub scope: String,
        pub registry_identity: String,
        pub generation: i64,
        pub pending_generation: Option<i64>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum WorkspaceGenerationState {
        Ready { current: i64 },
        Pending { current: i64, pending: i64 },
    }

    impl WorkspaceGenerationState {
        pub(crate) fn from_stored(current: i64, pending: Option<i64>) -> Result<Self> {
            match (current, pending) {
                (current, None) if current >= 0 => Ok(Self::Ready { current }),
                (current, Some(pending)) if current >= 0 && pending == current + 1 => {
                    Ok(Self::Pending { current, pending })
                }
                _ => anyhow::bail!("repository workspace generation authority is invalid"),
            }
        }

        pub(crate) fn current(self) -> i64 {
            match self {
                Self::Ready { current } | Self::Pending { current, .. } => current,
            }
        }

        pub(crate) fn pending(self) -> Option<i64> {
            match self {
                Self::Ready { .. } => None,
                Self::Pending { pending, .. } => Some(pending),
            }
        }
    }

    #[cfg(debug_assertions)]
    pub(crate) fn stop_workspace_generation_after(boundary: &str) {
        if std::env::var("IQ_TEST_WORKSPACE_GENERATION_STOP_AFTER").as_deref() == Ok(boundary) {
            std::process::exit(84);
        }
    }

    #[cfg(not(debug_assertions))]
    pub(crate) fn stop_workspace_generation_after(_boundary: &str) {}

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum DeletedCycleSandboxRepair {
        Authorized,
        PreservedDurableAuthority,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum CleanupState {
        Ready,
        Pending,
        OperatorRequested,
        OperatorFailed { message: String },
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

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum CheckoutReconciliationState {
        Ready(CheckoutTarget),
        Pending(CheckoutTarget),
        Failed(CheckoutFailure),
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub struct CheckoutTarget {
        target_sha: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub struct CheckoutFailure {
        target_sha: String,
        message: String,
    }

    #[derive(Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum RawCheckoutReconciliationState {
        Ready { target_sha: String },
        Pending { target_sha: String },
        Failed { target_sha: String, message: String },
    }

    impl CheckoutReconciliationState {
        pub fn ready(
            target_sha: &str,
            object_format: crate::git_object::GitObjectFormat,
        ) -> Result<Self> {
            Ok(Self::Ready(CheckoutTarget::new(target_sha, object_format)?))
        }

        pub fn pending(
            target_sha: &str,
            object_format: crate::git_object::GitObjectFormat,
        ) -> Result<Self> {
            Ok(Self::Pending(CheckoutTarget::new(
                target_sha,
                object_format,
            )?))
        }

        pub fn failed(
            target_sha: &str,
            object_format: crate::git_object::GitObjectFormat,
            message: &str,
        ) -> Result<Self> {
            let message = message.trim();
            if message.is_empty() {
                anyhow::bail!("checkout reconciliation failure message must not be empty");
            }
            Ok(Self::Failed(CheckoutFailure {
                target_sha: CheckoutTarget::new(target_sha, object_format)?.target_sha,
                message: message.to_string(),
            }))
        }

        pub fn target_sha(&self) -> &str {
            match self {
                Self::Ready(target) | Self::Pending(target) => &target.target_sha,
                Self::Failed(failure) => &failure.target_sha,
            }
        }

        pub fn is_ready_for(&self, source_sha: &str) -> bool {
            matches!(self, Self::Ready(target) if target.target_sha == source_sha)
        }
    }

    impl CheckoutTarget {
        fn new(
            target_sha: &str,
            object_format: crate::git_object::GitObjectFormat,
        ) -> Result<Self> {
            object_format.require_oid(target_sha, "checkout reconciliation target")?;
            Ok(Self {
                target_sha: target_sha.to_string(),
            })
        }

        fn from_serialized(target_sha: &str) -> Result<Self> {
            let object_format = match target_sha.len() {
                40 => crate::git_object::GitObjectFormat::Sha1,
                64 => crate::git_object::GitObjectFormat::Sha256,
                _ => anyhow::bail!("checkout reconciliation target must be a full Git object ID"),
            };
            Self::new(target_sha, object_format)
        }
    }

    impl<'de> Deserialize<'de> for CheckoutReconciliationState {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let raw = RawCheckoutReconciliationState::deserialize(deserializer)?;
            let checked = match raw {
                RawCheckoutReconciliationState::Ready { target_sha } => {
                    CheckoutTarget::from_serialized(&target_sha).map(Self::Ready)
                }
                RawCheckoutReconciliationState::Pending { target_sha } => {
                    CheckoutTarget::from_serialized(&target_sha).map(Self::Pending)
                }
                RawCheckoutReconciliationState::Failed {
                    target_sha,
                    message,
                } => CheckoutTarget::from_serialized(&target_sha).and_then(|target| {
                    let message = message.trim();
                    if message.is_empty() {
                        anyhow::bail!("checkout reconciliation failure message must not be empty")
                    }
                    Ok(Self::Failed(CheckoutFailure {
                        target_sha: target.target_sha,
                        message: message.to_string(),
                    }))
                }),
            };
            checked.map_err(serde::de::Error::custom)
        }
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

    pub(crate) struct AttemptValidationInvocation<'a> {
        pub candidate_sha: &'a str,
        pub command: &'a str,
        pub exit_code: i64,
        pub log_path: &'a str,
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
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum ExecutionAuthority {
        Active,
        Cancelled,
        Lost(String),
    }

    pub(crate) enum ExecutionStartAuthority<'a> {
        RepositoryLease {
            repo_key: &'a str,
            owner_id: &'a str,
        },
        ProviderVerified {
            repo_key: &'a str,
            owner_id: &'a str,
            policy_revision: i64,
            canonical: &'a crate::repository_policy::GitRepository,
        },
    }

    enum MutationAuthority<'a> {
        RepositoryLease {
            repo_key: &'a str,
            owner_id: &'a str,
        },
        Cancellation,
    }

    #[derive(Clone)]
    pub struct SqliteQueue {
        authority: crate::control_store::ValidatedDatabaseAuthority,
    }

    #[derive(Clone)]
    pub struct SqliteQueueReader {
        authority: crate::control_store::ValidatedDatabaseAuthority,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub struct MigrationReport {
        pub completion: MigrationCompletion,
        pub database_id: String,
        pub from_schema: u32,
        pub to_schema: u32,
        pub repositories: usize,
        pub admissions: usize,
        pub backup_path: PathBuf,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    #[serde(tag = "state", rename_all = "snake_case")]
    pub enum MigrationCompletion {
        Complete,
        PublishedButIncomplete {
            remaining_runner_termination_debts: Option<usize>,
            error: String,
        },
    }

    fn reconcile_migrated_runner_termination_debt(path: &Path) -> MigrationCompletion {
        let reconciliation = crate::control_store::ControlStore::open(path)
            .and_then(|store| store.reconcile_cancelled_runner_terminations());
        let remaining = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .and_then(|connection| {
            connection.query_row("SELECT COUNT(*) FROM runner_termination_debt", [], |row| {
                row.get::<_, usize>(0)
            })
        });
        match (reconciliation, remaining) {
            (Ok(_), Ok(0)) => MigrationCompletion::Complete,
            (result, count) => MigrationCompletion::PublishedButIncomplete {
                remaining_runner_termination_debts: count.ok(),
                error: result
                    .err()
                    .map(|error| format!("{error:#}"))
                    .unwrap_or_else(|| {
                        "runner termination debt remains after reconciliation".into()
                    }),
            },
        }
    }

    pub(crate) fn resolve_queue_database_path_without_creating(path: &Path) -> Result<PathBuf> {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let parent = path.parent().context("queue database path has no parent")?;
        let file_name = path
            .file_name()
            .context("queue database path has no file name")?;
        Ok(parent
            .canonicalize()
            .with_context(|| format!("resolve queue db parent {}", parent.display()))?
            .join(file_name))
    }

    fn require_current_schema_version(version: Option<&str>) -> Result<()> {
        match version {
            Some(crate::repository::SCHEMA_VERSION) => Ok(()),
            Some("3") => anyhow::bail!(
                "IQ schema 3 requires explicit offline migration with `iq migrate schema3 --policy-inventory <path>`"
            ),
            _ => incompatible_local_state(),
        }
    }

    #[cfg(debug_assertions)]
    fn stop_fresh_database_after(boundary: &str) {
        if std::env::var("IQ_TEST_DATABASE_STOP_AFTER").as_deref() == Ok(boundary) {
            std::process::exit(87);
        }
    }

    #[cfg(not(debug_assertions))]
    fn stop_fresh_database_after(_boundary: &str) {}

    #[cfg(debug_assertions)]
    fn fail_schema3_publication_after(boundary: &str) -> Result<()> {
        if std::env::var("IQ_TEST_SCHEMA3_FAIL_PUBLICATION_AFTER").as_deref() == Ok(boundary) {
            anyhow::bail!("test failure after schema-3 publication boundary {boundary}");
        }
        Ok(())
    }

    #[cfg(not(debug_assertions))]
    fn fail_schema3_publication_after(_boundary: &str) -> Result<()> {
        Ok(())
    }

    #[cfg(debug_assertions)]
    fn wait_at_database_publish_barrier() -> Result<()> {
        let Some(directory) = std::env::var_os("IQ_TEST_DATABASE_PUBLISH_BARRIER") else {
            return Ok(());
        };
        let directory = PathBuf::from(directory);
        let parties = std::env::var("IQ_TEST_DATABASE_PUBLISH_BARRIER_PARTIES")
            .context("database publication barrier party count is required")?
            .parse::<usize>()
            .context("database publication barrier party count must be an integer")?;
        if parties < 2 {
            anyhow::bail!("database publication barrier needs at least two parties");
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
                anyhow::bail!("database publication barrier timed out");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[cfg(not(debug_assertions))]
    fn wait_at_database_publish_barrier() -> Result<()> {
        Ok(())
    }

    fn private_database_temp(path: &Path) -> Result<PathBuf> {
        let name = path
            .file_name()
            .context("queue database path has no file name")?;
        let mut temporary = OsString::from(".");
        temporary.push(name);
        temporary.push(format!(".iq-new-{}.tmp", Uuid::new_v4()));
        Ok(path.with_file_name(temporary))
    }

    fn remove_database_temp(path: &Path) {
        let _ = fs::remove_file(path);
        for suffix in ["-journal", "-wal", "-shm"] {
            let mut name = path.as_os_str().to_os_string();
            name.push(suffix);
            let _ = fs::remove_file(PathBuf::from(name));
        }
    }

    fn publish_database_noreplace(from: &Path, to: &Path) -> Result<()> {
        let from = std::ffi::CString::new(from.as_os_str().as_bytes())?;
        let to = std::ffi::CString::new(to.as_os_str().as_bytes())?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(target_os = "macos")]
        let result = unsafe {
            libc::renameatx_np(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_EXCL,
            )
        };
        #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
        anyhow::bail!("atomic no-replace database publication is unsupported on this platform");
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("publish queue database {}", to.to_string_lossy()));
        }
        Ok(())
    }

    fn exchange_database_files(first: &Path, second: &Path) -> Result<()> {
        let first = std::ffi::CString::new(first.as_os_str().as_bytes())?;
        let second = std::ffi::CString::new(second.as_os_str().as_bytes())?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                first.as_ptr(),
                libc::AT_FDCWD,
                second.as_ptr(),
                libc::RENAME_EXCHANGE,
            )
        };
        #[cfg(target_os = "macos")]
        let result = unsafe {
            libc::renameatx_np(
                libc::AT_FDCWD,
                first.as_ptr(),
                libc::AT_FDCWD,
                second.as_ptr(),
                libc::RENAME_SWAP,
            )
        };
        #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
        anyhow::bail!("atomic database exchange is unsupported on this platform");
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .context("atomically exchange database files");
        }
        Ok(())
    }

    struct PrivateDatabaseCandidate {
        root: PathBuf,
        path: PathBuf,
        ownership: MigrationOwnershipManifest,
        device: u64,
        inode: u64,
        cleanup_on_drop: bool,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct MigrationOwnershipManifest {
        version: u32,
        database_id: String,
        source_digest: String,
        operation_id: String,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum MigrationPublicationPhase {
        Prepared,
        Exchanged,
        Validated,
        Complete,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct MigrationPublicationState {
        version: u32,
        database_id: String,
        source_digest: String,
        operation_id: String,
        source_members: Vec<Schema3BackupMember>,
        candidate_root: PathBuf,
        candidate_device: u64,
        candidate_inode: u64,
        phase: MigrationPublicationPhase,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Schema3BackupMember {
        suffix: String,
        length: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        device: u64,
        inode: u64,
        modified_seconds: i64,
        modified_nanoseconds: i64,
        changed_seconds: i64,
        changed_nanoseconds: i64,
        sha256: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Schema3BackupManifest {
        version: u32,
        database_id: String,
        source_digest: String,
        operation_id: String,
        members: Vec<Schema3BackupMember>,
    }

    struct PrivateBackupDirectory {
        path: PathBuf,
        ownership: MigrationOwnershipManifest,
        device: u64,
        inode: u64,
    }

    impl PrivateBackupDirectory {
        fn new(database: &Path, manifest: &Schema3BackupManifest) -> Result<Self> {
            use std::os::unix::fs::DirBuilderExt;

            let file_name = database
                .file_name()
                .context("schema-3 database path has no file name")?;
            let mut name = OsString::from(".");
            name.push(file_name);
            name.push(format!(".schema3-backup-{}.tmp", Uuid::new_v4()));
            let path = database.with_file_name(name);
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .with_context(|| format!("create private schema-3 backup {}", path.display()))?;
            let metadata = fs::symlink_metadata(&path)?;
            let ownership = MigrationOwnershipManifest {
                version: 1,
                database_id: manifest.database_id.clone(),
                source_digest: manifest.source_digest.clone(),
                operation_id: manifest.operation_id.clone(),
            };
            write_migration_ownership_manifest(&path, &ownership)?;
            Ok(Self {
                path,
                ownership,
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
    }

    impl Drop for PrivateBackupDirectory {
        fn drop(&mut self) {
            remove_owned_migration_directory(&self.path, &self.ownership, self.device, self.inode);
        }
    }

    impl PrivateDatabaseCandidate {
        fn new(
            path: &Path,
            database_id: &str,
            source_digest: &str,
            operation_id: &str,
        ) -> Result<Self> {
            use std::os::unix::fs::DirBuilderExt;

            let name = path
                .file_name()
                .context("queue database path has no file name")?;
            let mut legacy_name = OsString::from(".");
            legacy_name.push(name);
            legacy_name.push(".schema3-migration-candidate");
            let legacy = path.with_file_name(legacy_name);
            if legacy.exists() {
                anyhow::bail!(
                    "unowned fixed-name schema-3 migration candidate exists: {}",
                    legacy.display()
                );
            }
            let mut candidate_name = OsString::from(".");
            candidate_name.push(name);
            candidate_name.push(format!(".schema3-migration-{operation_id}.tmp"));
            let root = path.with_file_name(candidate_name);
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&root)
                .with_context(|| {
                    format!("create private schema-3 migration root {}", root.display())
                })?;
            let metadata = fs::symlink_metadata(&root)?;
            let ownership = MigrationOwnershipManifest {
                version: 1,
                database_id: database_id.to_string(),
                source_digest: source_digest.to_string(),
                operation_id: operation_id.to_string(),
            };
            write_migration_ownership_manifest(&root, &ownership)?;
            Ok(Self {
                path: root.join("database"),
                root,
                ownership,
                device: metadata.dev(),
                inode: metadata.ino(),
                cleanup_on_drop: true,
            })
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn preserve_for_recovery(&mut self) {
            self.cleanup_on_drop = false;
        }

        fn remove(mut self) {
            remove_owned_migration_directory(&self.root, &self.ownership, self.device, self.inode);
            self.cleanup_on_drop = false;
        }
    }

    impl Drop for PrivateDatabaseCandidate {
        fn drop(&mut self) {
            if self.cleanup_on_drop {
                remove_owned_migration_directory(
                    &self.root,
                    &self.ownership,
                    self.device,
                    self.inode,
                );
            }
        }
    }

    fn migration_publication_state_path(database: &Path) -> Result<PathBuf> {
        let mut name = database
            .file_name()
            .context("queue database path has no file name")?
            .to_os_string();
        name.push(".schema3-publication-state.json");
        Ok(database.with_file_name(name))
    }

    fn write_migration_publication_state(
        database: &Path,
        state: &MigrationPublicationState,
    ) -> Result<()> {
        let path = migration_publication_state_path(database)?;
        if path.exists() {
            let existing = read_migration_publication_state(database)?
                .context("migration publication state disappeared")?;
            if existing.operation_id != state.operation_id
                || existing.database_id != state.database_id
                || existing.source_digest != state.source_digest
            {
                anyhow::bail!("migration publication state belongs to another operation");
            }
        }
        let mut temporary_name = path
            .file_name()
            .context("migration publication state has no file name")?
            .to_os_string();
        temporary_name.push(format!(".{}.tmp", Uuid::new_v4()));
        let temporary = path.with_file_name(temporary_name);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary)?;
        serde_json::to_writer(&mut file, state)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        File::open(path.parent().context("migration state has no parent")?)?.sync_all()?;
        Ok(())
    }

    fn read_migration_publication_state(
        database: &Path,
    ) -> Result<Option<MigrationPublicationState>> {
        let path = migration_publication_state_path(database)?;
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("inspect migration publication state"),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 1024 * 1024
        {
            anyhow::bail!("migration publication state is not a bounded regular file");
        }
        let state: MigrationPublicationState = serde_json::from_slice(&fs::read(&path)?)?;
        if state.version != 1
            || Uuid::parse_str(&state.operation_id)
                .map_or(true, |id| id.to_string() != state.operation_id)
        {
            anyhow::bail!("migration publication state identity is invalid");
        }
        let name = database
            .file_name()
            .context("queue database path has no file name")?;
        let mut expected_name = OsString::from(".");
        expected_name.push(name);
        expected_name.push(format!(".schema3-migration-{}.tmp", state.operation_id));
        if state.candidate_root != database.with_file_name(expected_name) {
            anyhow::bail!("migration publication state has an invalid candidate root");
        }
        Ok(Some(state))
    }

    fn same_schema3_bytes(
        actual: &[Schema3BackupMember],
        expected: &[Schema3BackupMember],
    ) -> bool {
        actual
            .iter()
            .map(|member| (&member.suffix, member.length, &member.sha256))
            .eq(expected
                .iter()
                .map(|member| (&member.suffix, member.length, &member.sha256)))
    }

    fn remove_migration_publication_state(database: &Path) -> Result<()> {
        let path = migration_publication_state_path(database)?;
        fs::remove_file(&path)?;
        File::open(path.parent().context("migration state has no parent")?)?.sync_all()?;
        Ok(())
    }

    fn recover_schema3_publication(database: &Path) -> Result<()> {
        let Some(mut state) = read_migration_publication_state(database)? else {
            return Ok(());
        };
        validate_schema3_backup(database, &state.database_id, Some(&state.source_digest))
            .context("migration publication recovery has no exact schema-3 backup")?;
        let candidate_path = state.candidate_root.join("database");
        let candidate_source = if candidate_path.exists() {
            let ownership = read_migration_ownership_manifest(&state.candidate_root)?;
            let metadata = fs::symlink_metadata(&state.candidate_root)?;
            if ownership.operation_id != state.operation_id
                || ownership.database_id != state.database_id
                || ownership.source_digest != state.source_digest
                || (metadata.dev(), metadata.ino())
                    != (state.candidate_device, state.candidate_inode)
            {
                anyhow::bail!("migration publication candidate lost recovery authority");
            }
            let members = schema3_source_family(&candidate_path)?;
            Some((
                ownership,
                same_schema3_bytes(&members, &state.source_members),
            ))
        } else {
            None
        };
        let published = open_immutable_database(database)
            .and_then(|connection| validate_existing_schema_identity(&connection));
        if matches!(published, Ok(ref id) if id == &state.database_id) {
            if let Some((ownership, exact_source)) = &candidate_source {
                if !*exact_source {
                    anyhow::bail!("exchanged schema-3 source bytes differ from publication state");
                }
                let source = open_immutable_database(&candidate_path)?;
                if validate_schema3_identity(&source)? != state.database_id {
                    anyhow::bail!("exchanged schema-3 source has a different database identity");
                }
                remove_owned_migration_directory(
                    &state.candidate_root,
                    ownership,
                    state.candidate_device,
                    state.candidate_inode,
                );
            }
            state.phase = MigrationPublicationPhase::Complete;
            write_migration_publication_state(database, &state)?;
            sync_database_file_and_parent(database)?;
            return Ok(());
        }
        let source = open_immutable_database(database)
            .and_then(|connection| validate_schema3_identity(&connection));
        if matches!(source, Ok(ref id) if id == &state.database_id) {
            if let Some((ownership, _)) = &candidate_source {
                remove_owned_migration_directory(
                    &state.candidate_root,
                    ownership,
                    state.candidate_device,
                    state.candidate_inode,
                );
            }
            remove_migration_publication_state(database)?;
            return Ok(());
        }
        let Some((ownership, true)) = candidate_source else {
            anyhow::bail!("migration publication cannot restore exact schema-3 source bytes");
        };
        exchange_database_files(&candidate_path, database)?;
        sync_database_file_and_parent(database)?;
        let restored = open_immutable_database(database)?;
        if validate_schema3_identity(&restored)? != state.database_id
            || !same_schema3_bytes(&schema3_source_family(database)?, &state.source_members)
        {
            anyhow::bail!("migration publication rollback did not restore exact schema-3 source");
        }
        drop(restored);
        remove_owned_migration_directory(
            &state.candidate_root,
            &ownership,
            state.candidate_device,
            state.candidate_inode,
        );
        remove_migration_publication_state(database)
    }

    fn write_migration_ownership_manifest(
        root: &Path,
        ownership: &MigrationOwnershipManifest,
    ) -> Result<()> {
        let path = root.join("ownership.json");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        serde_json::to_writer(&mut file, ownership)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        File::open(root)?.sync_all()?;
        Ok(())
    }

    fn read_migration_ownership_manifest(root: &Path) -> Result<MigrationOwnershipManifest> {
        let path = root.join("ownership.json");
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 4096 {
            anyhow::bail!("migration ownership manifest is not a bounded regular file");
        }
        let manifest: MigrationOwnershipManifest = serde_json::from_slice(&fs::read(path)?)?;
        if manifest.version != 1
            || Uuid::parse_str(&manifest.operation_id).map_or(true, |operation| {
                operation.to_string() != manifest.operation_id
            })
        {
            anyhow::bail!("migration ownership manifest identity is invalid");
        }
        Ok(manifest)
    }

    fn remove_owned_migration_directory(
        root: &Path,
        ownership: &MigrationOwnershipManifest,
        device: u64,
        inode: u64,
    ) {
        let Ok(directory) = crate::secure_fs::DirectoryHandle::open(root, "migration artifact")
        else {
            return;
        };
        let metadata = match directory.directory().metadata() {
            Ok(metadata) => metadata,
            Err(_) => return,
        };
        let manifest = directory
            .open_file(OsStr::new("ownership.json"), "migration ownership manifest")
            .and_then(|file| serde_json::from_reader(file).context("parse migration ownership"));
        if (metadata.dev(), metadata.ino()) == (device, inode)
            && manifest.is_ok_and(|actual: MigrationOwnershipManifest| actual == *ownership)
        {
            let _ = directory.remove("migration artifact");
        }
    }

    fn sync_database_file_and_parent(path: &Path) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("open published queue database {}", path.display()))?;
        if !file.metadata()?.is_file() {
            anyhow::bail!(
                "published queue database is not a regular file: {}",
                path.display()
            );
        }
        file.sync_all()?;
        File::open(path.parent().context("queue database path has no parent")?)?.sync_all()?;
        Ok(())
    }

    fn validate_and_sync_published_database(path: &Path) -> Result<()> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        configure_connection(&connection)?;
        let version: Option<String> = connection
            .query_row(
                "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        require_current_schema_version(version.as_deref())?;
        validate_existing_schema_identity(&connection)?;
        drop(connection);
        sync_database_file_and_parent(path)
    }

    fn publish_fresh_database(path: &Path) -> Result<()> {
        if fs::symlink_metadata(path).is_ok() {
            validate_and_sync_published_database(path)?;
            stop_fresh_database_after("resynced");
            return Ok(());
        }
        let temporary = private_database_temp(path)?;
        let created = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary)
            .with_context(|| format!("create private queue database {}", temporary.display()))?;
        if created.metadata()?.permissions().mode() & 0o777 != 0o600 {
            remove_database_temp(&temporary);
            anyhow::bail!("fresh queue database temporary file is not private");
        }
        drop(created);
        stop_fresh_database_after("temp_created");

        let prepared: Result<()> = (|| {
            let mut connection = Connection::open_with_flags(
                &temporary,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            configure_connection(&connection)?;
            connection.pragma_update(None, "foreign_keys", "ON")?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            install_schema(&transaction)?;
            transaction.execute(
                "INSERT INTO queue_metadata (key,value) VALUES ('database_id',?1)",
                params![Uuid::new_v4().to_string()],
            )?;
            transaction.execute(
                "INSERT INTO queue_metadata (key,value) VALUES ('workspace_schema_version',?1)",
                [crate::repository::SCHEMA_VERSION],
            )?;
            transaction.commit()?;
            validate_existing_schema_identity(&connection)?;
            connection.execute_batch("PRAGMA integrity_check; PRAGMA foreign_key_check;")?;
            drop(connection);
            File::open(&temporary)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = prepared {
            remove_database_temp(&temporary);
            return Err(error).context("prepare fresh queue database for atomic publication");
        }
        stop_fresh_database_after("temp_validated");
        wait_at_database_publish_barrier()?;

        match publish_database_noreplace(&temporary, path) {
            Ok(()) => {
                stop_fresh_database_after("published");
                validate_and_sync_published_database(path)?;
                stop_fresh_database_after("resynced");
            }
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) =>
            {
                remove_database_temp(&temporary);
                validate_and_sync_published_database(path)?;
                stop_fresh_database_after("resynced");
            }
            Err(error) => {
                remove_database_temp(&temporary);
                return Err(error).with_context(|| {
                    format!(
                        "publish fresh queue database without replacement {}",
                        path.display()
                    )
                });
            }
        }
        Ok(())
    }

    impl SqliteQueue {
        const OPEN_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        const WRITE_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(200);
        const AUTHORITY_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

        pub fn default_db_path() -> Result<PathBuf> {
            let database = Self::default_db_path_without_open()?;
            Self::open(&database)?;
            Ok(database)
        }

        pub fn default_db_path_without_open() -> Result<PathBuf> {
            let iq_directory = if cfg!(target_os = "macos") {
                let home = std::env::var_os("HOME").context("HOME is required for IQ state")?;
                PathBuf::from(home).join("Library/Application Support/IQ/IntegrationQueues")
            } else {
                let root = if let Some(state_home) = std::env::var_os("XDG_STATE_HOME") {
                    PathBuf::from(state_home)
                } else {
                    let home = std::env::var_os("HOME").context("HOME is required for IQ state")?;
                    PathBuf::from(home).join(".local/state")
                };
                root.join("iq/integration-queues")
            };
            if iq_directory.as_os_str().is_empty() || !iq_directory.is_absolute() {
                anyhow::bail!(
                    "IQ state directory must be a non-empty absolute path: {}",
                    iq_directory.display()
                );
            }
            fs::create_dir_all(&iq_directory)
                .with_context(|| format!("create IQ state directory {}", iq_directory.display()))?;
            require_real_directory(&iq_directory, "IQ state directory")?;
            Ok(iq_directory.join("queues.db"))
        }

        pub fn migrate_schema3(
            path: &Path,
            inventory: crate::repository_policy::PolicyInventory,
        ) -> Result<MigrationReport> {
            #[derive(Clone)]
            enum AdmissionPlan {
                Local {
                    item_id: String,
                    kind: &'static str,
                    source_branch: String,
                    head_sha: String,
                    source_ref: Option<String>,
                    submission_id: Option<String>,
                    admitted_at: String,
                },
                MergeRequest {
                    item_id: String,
                    kind: &'static str,
                    source_branch: String,
                    head_sha: String,
                    provider: crate::repository_policy::Provider,
                    provider_host: String,
                    provider_repository: String,
                    provider_repository_id: String,
                    target_branch: String,
                    base_sha: Option<String>,
                    provider_merge_method: Option<crate::repository_policy::ProviderMergeMethod>,
                    identity: String,
                    url: String,
                    admitted_at: String,
                },
            }

            let inventory = inventory.validate()?;
            for assignment in &inventory.repositories {
                assignment
                    .policy
                    .clone()
                    .verify_effect_identities()
                    .with_context(|| {
                        format!(
                            "verify schema-3 migration repository identities for {}",
                            assignment.repo_key
                        )
                    })?;
            }
            let path = resolve_queue_database_path_without_creating(path)?;
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect schema-3 queue database {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                anyhow::bail!("migration source must be a regular queue database");
            }
            let exclusive = crate::control_store::DatabaseProcessLease::acquire_exclusive(&path)
                .context("take exclusive offline migration authority")?;
            recover_schema3_publication(&path)?;
            for suffix in ["-journal", "-wal", "-shm"] {
                let mut sidecar = path.as_os_str().to_os_string();
                sidecar.push(suffix);
                if PathBuf::from(sidecar).exists() {
                    anyhow::bail!(
                        "schema-3 migration requires a closed database with no sidecar files"
                    );
                }
            }
            let migration_source_members = schema3_source_family(&path)?;
            let migration_source_digest = schema3_source_digest(&migration_source_members)?;
            let source = open_immutable_database(&path)?;
            source.pragma_update(None, "foreign_keys", "ON")?;
            let stored_version: String = source.query_row(
                "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                [],
                |row| row.get(0),
            )?;
            if stored_version == crate::repository::SCHEMA_VERSION {
                let database_id = validate_existing_schema_identity(&source)?;
                let backup_path = validate_schema3_backup(&path, &database_id, None)
                    .context("published migration has no valid durable schema-3 backup")?;
                let repositories: usize =
                    source.query_row("SELECT COUNT(*) FROM repository_policies", [], |row| {
                        row.get(0)
                    })?;
                let admissions: usize =
                    source.query_row("SELECT COUNT(*) FROM queue_admissions", [], |row| {
                        row.get(0)
                    })?;
                drop(source);
                sync_database_file_and_parent(&path)?;
                drop(exclusive);
                return Ok(MigrationReport {
                    completion: reconcile_migrated_runner_termination_debt(&path),
                    database_id,
                    from_schema: 3,
                    to_schema: 4,
                    repositories,
                    admissions,
                    backup_path,
                });
            }
            crate::control_store::validate_schema3_systemd_authority(&source)?;
            let database_id = validate_schema3_identity(&source)?;
            drop(source);
            let operation_id = Uuid::new_v4().to_string();
            let mut candidate = PrivateDatabaseCandidate::new(
                &path,
                &database_id,
                &migration_source_digest,
                &operation_id,
            )?;
            copy_database_file(&path, candidate.path())?;
            let source_after_copy = schema3_source_family(&path)?;
            if source_after_copy != migration_source_members {
                anyhow::bail!(
                    "schema-3 source family changed after candidate copy: expected {migration_source_members:?}, observed {source_after_copy:?}"
                );
            }
            let connection = Connection::open_with_flags(
                candidate.path(),
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            configure_connection(&connection)?;
            connection.pragma_update(None, "foreign_keys", "ON")?;
            let candidate_database_id = validate_schema3_identity(&connection)?;
            if candidate_database_id != database_id {
                anyhow::bail!("schema-3 migration candidate changed database identity");
            }

            let stored_keys = {
                let mut statement = connection
                    .prepare("SELECT repo_key FROM repository_remote_owners ORDER BY repo_key")?;
                let keys = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                keys
            };
            let mut inventory_keys = inventory
                .repositories
                .iter()
                .map(|assignment| assignment.repo_key.clone())
                .collect::<Vec<_>>();
            inventory_keys.sort();
            if stored_keys != inventory_keys {
                anyhow::bail!(
                    "policy inventory must assign every schema-3 repository UUID exactly once"
                );
            }
            let inventory_formats = inventory
                .repositories
                .iter()
                .map(|assignment| {
                    (
                        assignment.repo_key.clone(),
                        assignment.policy.canonical_repository.object_format(),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            validate_stored_object_ids(&connection, &inventory_formats, true)
                .context("validate schema-3 Git object IDs against policy inventory")?;

            let mut policies = std::collections::BTreeMap::new();
            let mut ready_repository_keys = std::collections::BTreeSet::new();
            let mut preserved_provisioning_keys = std::collections::BTreeSet::new();
            let mut cancelled_provisioning_keys = std::collections::BTreeSet::new();
            let mut schema4_repository_bindings = std::collections::BTreeMap::new();
            let mut schema4_provisioning_lifecycles = std::collections::BTreeMap::new();
            let mut schema4_workspace_bindings = std::collections::BTreeMap::new();
            let mut schema4_development_bindings = std::collections::BTreeMap::new();
            let mut dispositions = std::collections::BTreeMap::new();
            for assignment in inventory.repositories {
                let registered = connection
                    .query_row(
                        "SELECT owned_root_path,source_sha FROM registered_repositories WHERE repo_key=?1",
                        [&assignment.repo_key],
                        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?;
                let intent = connection
                    .query_row(
                        "SELECT owned_root_path,staging_root_path,source_sha,json_extract(lifecycle_json,'$.state'),lifecycle_json FROM repository_provisioning_intents WHERE repo_key=?1",
                        [&assignment.repo_key],
                        |row| {
                            Ok((
                                row.get::<_, Vec<u8>>(0)?,
                                row.get::<_, Vec<u8>>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                            ))
                        },
                    )
                    .optional()?;
                match (&assignment.repository, registered, intent) {
                    (
                        crate::repository_policy::MigrationRepositoryState::Ready { .. },
                        Some((owned_root, source_sha)),
                        None,
                    ) => {
                        let binding = assignment
                            .repository
                            .git_binding()
                            .context("validated ready repository has no Git binding")?;
                        let owned_root = PathBuf::from(OsString::from_vec(owned_root));
                        if binding.top_level != owned_root {
                            anyhow::bail!(
                                "schema-3 repository {} Git binding top-level differs from stored owned root",
                                assignment.repo_key
                            );
                        }
                        binding.verify_head(&source_sha).with_context(|| {
                            format!(
                                "verify schema-3 repository {} live Git binding and HEAD",
                                assignment.repo_key
                            )
                        })?;
                        schema4_repository_bindings.insert(
                            assignment.repo_key.clone(),
                            serde_json::to_string(binding)?,
                        );
                        ready_repository_keys.insert(assignment.repo_key.clone());
                    }
                    (state, None, Some((owned_root, staging_root, source_sha, lifecycle, lifecycle_json)))
                        if state.lifecycle() == Some(lifecycle.as_str()) =>
                    {
                        if let Some(binding) = state.git_binding() {
                            let owned_root = PathBuf::from(OsString::from_vec(owned_root));
                            let staging_root = PathBuf::from(OsString::from_vec(staging_root));
                            let expected_path = if state.uses_staging_repository() {
                                &staging_root
                            } else if matches!(
                                state,
                                crate::repository_policy::MigrationRepositoryState::TargetCheckedOut { .. }
                            ) {
                                if binding.top_level == staging_root {
                                    &staging_root
                                } else {
                                    &owned_root
                                }
                            } else {
                                &owned_root
                            };
                            if binding.top_level != *expected_path {
                                anyhow::bail!(
                                    "schema-3 repository {} provisioning Git binding has the wrong lifecycle path",
                                    assignment.repo_key
                                );
                            }
                            if state.requires_checked_out_head() {
                                binding.verify_head(&source_sha)?;
                            } else if state.requires_source_commit() {
                                binding.verify_commit(&source_sha)?;
                            } else {
                                binding.verify()?;
                            }
                        }
                        let mut lifecycle_value: serde_json::Value =
                            serde_json::from_str(&lifecycle_json)?;
                        if let Some(binding) = state.git_binding() {
                            lifecycle_value
                                .as_object_mut()
                                .context("schema-3 provisioning lifecycle is not an object")?
                                .entry("identity")
                                .or_insert_with(|| serde_json::json!({}))
                                .as_object_mut()
                                .context("schema-3 provisioning lifecycle identity is not an object")?
                                .insert(
                                    "git_binding".into(),
                                    serde_json::to_value(binding)?,
                                );
                        }
                        schema4_provisioning_lifecycles.insert(
                            assignment.repo_key.clone(),
                            serde_json::to_string(&lifecycle_value)?,
                        );
                        match state.disposition().context(
                            "validated interrupted provisioning state has no disposition",
                        )? {
                            crate::repository_policy::InterruptedProvisioningDisposition::Preserve => {
                                preserved_provisioning_keys.insert(assignment.repo_key.clone());
                            }
                            crate::repository_policy::InterruptedProvisioningDisposition::Cancel => {
                                cancelled_provisioning_keys.insert(assignment.repo_key.clone());
                            }
                        }
                    }
                    (crate::repository_policy::MigrationRepositoryState::Ready { .. }, _, _) => {
                        anyhow::bail!(
                            "schema-3 repository {} inventory says ready but durable state is not ready",
                            assignment.repo_key
                        )
                    }
                    (state, _, _) => anyhow::bail!(
                        "schema-3 repository {} provisioning inventory state {} differs from durable lifecycle",
                        assignment.repo_key,
                        state.lifecycle().unwrap_or("ready")
                    ),
                }
                let mut stored_development = std::collections::BTreeMap::new();
                {
                    let mut statement = connection.prepare(
                        "SELECT id,path,rift_id,source_rift_id,base_sha FROM development_workspaces WHERE repo_key=?1 AND rift_id IS NOT NULL AND status!='removed' ORDER BY id",
                    )?;
                    let rows = statement.query_map([&assignment.repo_key], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    })?;
                    for row in rows {
                        let (id, path, rift_id, source_rift_id, base_sha) = row?;
                        stored_development.insert(
                            id,
                            (
                                PathBuf::from(OsString::from_vec(path)),
                                rift_id,
                                source_rift_id,
                                base_sha,
                            ),
                        );
                    }
                }
                for workspace in assignment.development_workspaces {
                    let stored = stored_development
                        .remove(&workspace.workspace_id)
                        .with_context(|| {
                            format!(
                                "migration development workspace {} is not active in schema 3",
                                workspace.workspace_id
                            )
                        })?;
                    if Path::new(&workspace.path) != stored.0
                        || workspace.rift_id != stored.1
                        || workspace.source_rift_id != stored.2
                        || workspace.base_sha != stored.3
                    {
                        anyhow::bail!(
                            "migration development workspace {} differs from durable identity",
                            workspace.workspace_id
                        );
                    }
                    let binding = workspace
                        .git_binding
                        .as_ref()
                        .context("validated development workspace has no Git binding")?;
                    binding.verify_base(&workspace.base_sha).with_context(|| {
                        format!(
                            "verify schema-3 development workspace {} live Git binding and base",
                            workspace.workspace_id
                        )
                    })?;
                    schema4_development_bindings.insert(
                        workspace.workspace_id,
                        (workspace.path, serde_json::to_string(binding)?),
                    );
                }
                if !stored_development.is_empty() {
                    anyhow::bail!(
                        "migration inventory omits active schema-3 development workspace(s) for repository {}",
                        assignment.repo_key
                    );
                }
                assignment.policy.canonical_repository.verify_local_bare()?;
                let owner: (String, String, String) = connection.query_row(
                    "SELECT fetch_url,push_url,target_branch FROM repository_remote_owners WHERE repo_key=?1",
                    [&assignment.repo_key],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?;
                if !legacy_transport_matches(
                    &owner.0,
                    &assignment.policy.canonical_repository,
                    true,
                ) || !legacy_transport_matches(
                    &owner.1,
                    &assignment.policy.canonical_repository,
                    false,
                ) || owner.2 != assignment.policy.target_branch
                {
                    anyhow::bail!(
                        "schema-3 repository {} transport differs from its explicit canonical policy",
                        assignment.repo_key
                    );
                }
                for disposition in assignment.item_dispositions {
                    if let Some(workspace) = &disposition.workspace_identity {
                        let binding = workspace
                            .git_binding
                            .as_ref()
                            .context("validated migration workspace has no Git binding")?;
                        binding.verify().with_context(|| {
                            format!(
                                "verify schema-3 item {} live workspace Git binding",
                                disposition.item_id
                            )
                        })?;
                        schema4_workspace_bindings.insert(
                            disposition.item_id.clone(),
                            (workspace.path.clone(), serde_json::to_string(binding)?),
                        );
                    }
                    let stored_repo_key = connection
                        .query_row(
                            "SELECT repo_key FROM queue_items WHERE id=?1",
                            [&disposition.item_id],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?;
                    match stored_repo_key.as_deref() {
                        Some(stored) if stored == assignment.repo_key => {}
                        Some(_) => anyhow::bail!(
                            "schema-3 item {} disposition is assigned to the wrong repository",
                            disposition.item_id
                        ),
                        None => anyhow::bail!(
                            "migration inventory contains a disposition for no stored item"
                        ),
                    }
                    dispositions.insert(disposition.item_id.clone(), disposition);
                }
                policies.insert(assignment.repo_key, assignment.policy);
            }

            let mut admissions = Vec::new();
            let mut cancelled_incompatible_items = std::collections::BTreeSet::new();
            let mut consumed_dispositions = std::collections::BTreeSet::new();
            let mut effort_workspace_repairs = std::collections::BTreeMap::new();
            let mut effort_runner_repairs = std::collections::BTreeMap::new();
            let mut effort_runners = std::collections::BTreeMap::new();
            {
                let mut statement = connection.prepare(
                    "SELECT item_id,workspace_json,runner_snapshot_json FROM integration_efforts ORDER BY item_id",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?;
                for row in rows {
                    let (item_id, stored_workspace, stored_runner) = row?;
                    let stored = serde_json::from_str::<WorkspaceIdentity>(&stored_workspace)
                        .and_then(|workspace| {
                            validate_migration_workspace_identity(&workspace)
                                .map(|()| workspace)
                                .map_err(|error| {
                                    serde_json::Error::io(std::io::Error::other(error))
                                })
                        });
                    let disposition = dispositions.get(&item_id);
                    let supplied =
                        disposition.and_then(|disposition| disposition.workspace_identity.as_ref());
                    match (stored, supplied) {
                        (Ok(stored), Some(supplied))
                            if stored.path != supplied.path
                                || stored.rift_id != supplied.rift_id
                                || stored.source_rift_id != supplied.source_rift_id =>
                        {
                            anyhow::bail!(
                                "schema-3 item {item_id} migration workspace identity contradicts durable identity"
                            );
                        }
                        (Ok(_), _) => {}
                        (Err(_), Some(supplied)) => {
                            effort_workspace_repairs.insert(
                                item_id.clone(),
                                WorkspaceIdentity {
                                    path: supplied.path.clone(),
                                    rift_id: supplied.rift_id.clone(),
                                    source_rift_id: supplied.source_rift_id.clone(),
                                },
                            );
                        }
                        (Err(_), None) => anyhow::bail!(
                            "schema-3 item {item_id} requires explicit workspace_identity in migration inventory"
                        ),
                    }
                    let stored = serde_json::from_str::<crate::control_domain::RunnerSnapshot>(
                        &stored_runner,
                    )
                    .and_then(|runner| {
                        runner
                            .validate()
                            .map_err(|error| serde_json::Error::io(std::io::Error::other(error)))
                    });
                    let supplied =
                        disposition.and_then(|disposition| disposition.runner_snapshot.as_ref());
                    let runner = match (stored, supplied) {
                        (Ok(stored), Some(supplied)) if &stored != supplied => anyhow::bail!(
                            "schema-3 item {item_id} migration runner snapshot contradicts durable snapshot"
                        ),
                        (Ok(stored), _) => stored,
                        (Err(_), Some(supplied)) => {
                            effort_runner_repairs.insert(item_id.clone(), supplied.clone());
                            supplied.clone()
                        }
                        (Err(_), None) => anyhow::bail!(
                            "schema-3 item {item_id} requires explicit runner_snapshot in migration inventory"
                        ),
                    };
                    effort_runners.insert(item_id, runner);
                }
            }
            let mut migration_termination_authorities = std::collections::BTreeMap::new();
            {
                let mut statement = connection.prepare(
                    "SELECT effort.item_id,effort.state,effort.state_json,debt.authority_json
                     FROM integration_efforts effort
                     LEFT JOIN runner_termination_debt debt ON debt.effort_id=effort.id
                     WHERE effort.state IN ('agent_launching','agent_running') OR debt.effort_id IS NOT NULL
                     ORDER BY effort.item_id",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?;
                for row in rows {
                    let (item_id, state, state_json, debt_json) = row?;
                    let disposition = dispositions.get(&item_id).with_context(|| {
                        format!(
                            "schema-3 runner authority for item {item_id} requires an explicit migration disposition"
                        )
                    })?;
                    if matches!(state.as_str(), "agent_launching" | "agent_running")
                        && disposition.disposition
                            != crate::repository_policy::IncompatibleItemDisposition::Cancel
                    {
                        anyhow::bail!(
                            "schema-3 active runner item {item_id} must be explicitly cancelled"
                        );
                    }
                    let authority = disposition
                        .runner_termination_authority
                        .as_ref()
                        .with_context(|| {
                            format!(
                                "schema-3 runner item {item_id} requires explicit termination authority"
                            )
                        })?;
                    let stored: serde_json::Value =
                        serde_json::from_str(debt_json.as_deref().unwrap_or(&state_json))?;
                    let payload = stored
                        .get("payload")
                        .context("schema-3 runner authority has no payload")?;
                    let stored_authority_state =
                        stored.get("state").and_then(serde_json::Value::as_str);
                    if payload.get("cycle_id").and_then(serde_json::Value::as_str)
                        != Some(authority.cycle_id.as_str())
                        || payload.get("unit_name").and_then(serde_json::Value::as_str)
                            != Some(authority.unit_name.as_str())
                    {
                        anyhow::bail!(
                            "schema-3 runner item {item_id} termination authority contradicts durable unit identity"
                        );
                    }
                    if (state == "agent_running" || stored_authority_state == Some("running"))
                        && (payload.get("pid").and_then(serde_json::Value::as_u64)
                            != Some(u64::from(authority.pid))
                            || payload
                                .get("process_start_ticks")
                                .and_then(serde_json::Value::as_u64)
                                != Some(authority.process_start_ticks))
                    {
                        anyhow::bail!(
                            "schema-3 runner item {item_id} termination authority contradicts durable process identity"
                        );
                    }
                    let runner = effort_runners
                        .get(&item_id)
                        .context("schema-3 runner item has no validated runner snapshot")?;
                    crate::agent_runner::verify_live_legacy_runner_scope_authority(
                        &runner.sandbox.systemctl,
                        authority,
                    )
                    .with_context(|| {
                        format!("verify schema-3 runner termination authority for item {item_id}")
                    })?;
                    migration_termination_authorities.insert(item_id, authority.clone());
                }
            }
            for (item_id, disposition) in &dispositions {
                if disposition.runner_termination_authority.is_some()
                    && !migration_termination_authorities.contains_key(item_id)
                {
                    anyhow::bail!(
                        "migration inventory contains runner termination authority for item without runner debt"
                    );
                }
            }
            {
                let mut statement = connection.prepare(
                    "SELECT item.id,item.repo_key,item.status,item.source_kind,item.source_branch,item.current_head_sha,item.pr_url,item.landing_policy,item.source_ref,item.submission_id,item.created_at,repository.target_branch,item.landing_state_json
                     FROM queue_items item
                     JOIN registered_repositories repository ON repository.repo_key=item.repo_key
                     ORDER BY item.created_at,item.id",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, String>(11)?,
                        row.get::<_, String>(12)?,
                    ))
                })?;
                for row in rows {
                    let (
                        item_id,
                        repo_key,
                        status,
                        source_kind,
                        source_branch,
                        head_sha,
                        pr_url,
                        landing_policy,
                        source_ref,
                        submission_id,
                        admitted_at,
                        legacy_target,
                        landing_state,
                    ) = row?;
                    let terminal = matches!(status.as_str(), "integrated" | "cancelled");
                    let policy = policies
                        .get(&repo_key)
                        .context("migration item repository has no policy")?;
                    policy
                        .canonical_repository
                        .object_format()
                        .require_oid(&head_sha, "schema-3 queue head SHA")?;
                    let disposition = dispositions.get(&item_id);
                    let compatible = match (
                        source_kind.as_str(),
                        landing_policy.as_str(),
                        pr_url.as_deref(),
                    ) {
                        ("local_submission", "squash", None) => {
                            let source_ref = source_ref
                                .context("legacy local submission has no exact source ref")?;
                            let submission_id = submission_id
                                .context("legacy local submission has no submission identity")?;
                            admissions.push(AdmissionPlan::Local {
                                item_id: item_id.clone(),
                                kind: "local_submission",
                                source_branch,
                                head_sha,
                                source_ref: Some(source_ref),
                                submission_id: Some(submission_id),
                                admitted_at,
                            });
                            policy.integration_policy
                                == crate::repository_policy::IntegrationPolicy::Direct
                        }
                        ("remote_branch", "direct", None) => {
                            admissions.push(AdmissionPlan::Local {
                                item_id: item_id.clone(),
                                kind: "direct",
                                source_branch,
                                head_sha,
                                source_ref: None,
                                submission_id: None,
                                admitted_at,
                            });
                            policy.integration_policy
                                == crate::repository_policy::IntegrationPolicy::Direct
                        }
                        ("remote_branch", "provider", Some(url)) => {
                            let locator = crate::providers::merge_request_locator(url)?;
                            let provider_policy = policy.canonical_repository.provider();
                            let (provider_repository, admitted_base) = if terminal {
                                let historical = disposition.with_context(|| {
                                    format!(
                                        "historical schema-3 MR item {item_id} requires explicit provider identity and admitted base inventory"
                                    )
                                })?;
                                let provider_repository = historical
                                    .provider_repository
                                    .clone()
                                    .with_context(|| {
                                        format!(
                                            "historical schema-3 MR item {item_id} requires complete provider_repository in migration inventory"
                                        )
                                    })?;
                                let admitted_base = historical
                                    .admitted_base_sha
                                    .clone()
                                    .with_context(|| {
                                        format!(
                                            "historical schema-3 MR item {item_id} requires admitted_base_sha in migration inventory"
                                        )
                                    })?;
                                (provider_repository, Some(admitted_base))
                            } else {
                                let provider_repository = match (
                                    provider_policy,
                                    disposition.and_then(|value| value.provider_repository.as_ref()),
                                ) {
                                    (Some(configured), Some(supplied)) if configured != supplied => {
                                        anyhow::bail!(
                                            "active schema-3 MR item {item_id} provider inventory contradicts canonical provider identity"
                                        )
                                    }
                                    (Some(configured), _) => configured.clone(),
                                    (None, Some(supplied)) => supplied.clone(),
                                    (None, None) => anyhow::bail!(
                                        "incompatible active schema-3 MR item {item_id} requires complete provider_repository inventory for cancellation"
                                    ),
                                };
                                let admitted_base = disposition
                                    .and_then(|value| value.admitted_base_sha.clone());
                                (provider_repository, admitted_base)
                            };
                            if locator.provider != provider_repository.provider
                                || locator.host != provider_repository.host
                                || locator.repository != provider_repository.repository
                            {
                                anyhow::bail!(
                                    "schema-3 MR item {item_id} URL differs from explicit provider repository inventory"
                                );
                            }
                            if !terminal && admitted_base.is_none() {
                                anyhow::bail!(
                                    "active schema-3 MR item {item_id} requires admitted_base_sha in migration inventory"
                                );
                            }
                            let provider_merge_method =
                                disposition.and_then(|value| value.provider_merge_method);
                            if !terminal
                                && serde_json::from_str::<LandingState>(&landing_state)?
                                    .is_uncertain()
                                && provider_merge_method.is_none()
                            {
                                anyhow::bail!(
                                    "migrated uncertain provider item {item_id} requires provider_merge_method in migration inventory"
                                );
                            }
                            let exact_source_ref = match provider_repository.provider {
                                crate::repository_policy::Provider::Github => {
                                    format!("refs/pull/{}/head", locator.identity)
                                }
                                crate::repository_policy::Provider::Gitlab => {
                                    format!("refs/merge-requests/{}/head", locator.identity)
                                }
                            };
                            let source_ref_matches = source_branch == exact_source_ref;
                            let belongs_to_canonical = provider_policy
                                .is_some_and(|configured| configured == &provider_repository);
                            admissions.push(AdmissionPlan::MergeRequest {
                                item_id: item_id.clone(),
                                kind: if terminal {
                                    "historical_merge_request"
                                } else {
                                    "merge_request"
                                },
                                source_branch: exact_source_ref,
                                head_sha,
                                provider: provider_repository.provider,
                                provider_host: provider_repository.host,
                                provider_repository: provider_repository.repository,
                                provider_repository_id: provider_repository.repository_id,
                                target_branch: legacy_target,
                                base_sha: admitted_base,
                                provider_merge_method,
                                identity: locator.identity,
                                url: url.to_string(),
                                admitted_at,
                            });
                            policy.integration_policy
                                == crate::repository_policy::IntegrationPolicy::MergeRequestRequired
                                && belongs_to_canonical
                                && source_ref_matches
                        }
                        _ => anyhow::bail!(
                            "schema-3 item {item_id} has an invalid explicit source/landing/URL combination"
                        ),
                    };
                    if !terminal {
                        match disposition.map(|value| value.disposition) {
                            Some(
                                crate::repository_policy::IncompatibleItemDisposition::Cancel,
                            ) => {
                                cancelled_incompatible_items.insert(item_id.clone());
                            }
                            Some(
                                crate::repository_policy::IncompatibleItemDisposition::Continue,
                            ) if !compatible => anyhow::bail!(
                                "incompatible active schema-3 item {item_id} must be explicitly cancelled"
                            ),
                            None if !compatible => anyhow::bail!(
                                "active schema-3 item {item_id} is incompatible with new policy and requires explicit disposition"
                            ),
                            Some(
                                crate::repository_policy::IncompatibleItemDisposition::Continue,
                            )
                            | None => {}
                        }
                    }
                    if disposition.is_some() {
                        consumed_dispositions.insert(item_id.clone());
                    }
                    if !terminal
                        && matches!(
                            policy.operation_state,
                            crate::repository_policy::OperationState::Disabled
                        )
                    {
                        anyhow::bail!(
                            "disabled migration policy cannot contain active item {item_id}"
                        );
                    }
                }
            }
            if consumed_dispositions.len() != dispositions.len() {
                anyhow::bail!("migration inventory contains a disposition for no stored item");
            }

            for (repo_key, policy) in &mut policies {
                if matches!(
                    policy.operation_state,
                    crate::repository_policy::OperationState::Draining { .. }
                ) {
                    let mut obligations = std::collections::BTreeSet::new();
                    {
                        let mut statement = connection.prepare(
                            "SELECT id FROM development_workspaces WHERE repo_key=?1 AND status!='removed'",
                        )?;
                        for id in statement.query_map([repo_key], |row| row.get::<_, String>(0))? {
                            obligations.insert(crate::repository_policy::Obligation::Workspace {
                                id: id?,
                            });
                        }
                    }
                    {
                        let mut statement = connection.prepare(
                            "SELECT id FROM queue_items WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled')",
                        )?;
                        for id in statement.query_map([repo_key], |row| row.get::<_, String>(0))? {
                            obligations.insert(crate::repository_policy::Obligation::QueueItem {
                                id: id?,
                            });
                        }
                    }
                    obligations.retain(|obligation| {
                        !matches!(
                            obligation,
                            crate::repository_policy::Obligation::QueueItem { id }
                                if cancelled_incompatible_items.contains(id)
                        )
                    });
                    policy.operation_state =
                        crate::repository_policy::OperationState::Draining { obligations };
                }
            }

            let backup_path = create_schema3_backup(
                &path,
                &database_id,
                &migration_source_members,
                &migration_source_digest,
                &operation_id,
            )?;
            #[cfg(debug_assertions)]
            if std::env::var_os("IQ_TEST_SCHEMA3_STOP_AFTER_BACKUP_PUBLICATION").is_some() {
                std::process::exit(94);
            }
            drop(connection);
            let mut connection = Connection::open_with_flags(
                candidate.path(),
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            configure_connection(&connection)?;
            let journal_mode: String =
                connection.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
            if journal_mode != "delete" {
                anyhow::bail!("migration candidate could not enter single-file journal mode");
            }
            connection.pragma_update(None, "foreign_keys", "ON")?;
            connection.pragma_update(None, "foreign_keys", "OFF")?;
            connection.pragma_update(None, "legacy_alter_table", "ON")?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Exclusive)?;
            transaction.execute_batch(REPOSITORY_POLICY_TABLE_SCHEMA)?;
            let timestamp = now();
            for (repo_key, policy) in &policies {
                if cancelled_provisioning_keys.contains(repo_key) {
                    continue;
                }
                transaction.execute(
                    "INSERT INTO repository_policies(repo_key,revision,operation_state_json,canonical_repository_json,canonical_ownership_key,target_branch,integration_policy,replication_policy_json,created_at,updated_at) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?8)",
                    params![repo_key,serde_json::to_string(&policy.operation_state)?,serde_json::to_string(&policy.canonical_repository)?,policy.canonical_repository.canonical_ownership_key()?,policy.target_branch,policy.integration_policy.to_string(),serde_json::to_string(&policy.replication_policy)?,timestamp],
                )?;
            }
            crate::control_store::upgrade_schema3_control_identity(&transaction)?;
            transaction.execute_batch(
                "DROP VIEW queue_items_runtime;
                 DROP TRIGGER IF EXISTS queue_items_local_source_insert;
                 DROP TRIGGER IF EXISTS queue_items_local_source_update;
                 DROP TRIGGER IF EXISTS queue_items_landing_state_insert;
                 DROP TRIGGER IF EXISTS queue_items_landing_state_update;
                 DROP TRIGGER IF EXISTS queue_items_workspace_state_insert;
                 DROP TRIGGER IF EXISTS queue_items_workspace_state_update;
                 DROP TRIGGER IF EXISTS registered_repository_path_identity_insert;
                 DROP TRIGGER IF EXISTS registered_repository_excludes_provisioning_intent;
                 DROP TRIGGER IF EXISTS repository_provisioning_intent_excludes_ready;
                 DROP TRIGGER IF EXISTS repository_remote_owner_identity_immutable;
                 DROP TRIGGER IF EXISTS registered_repository_identity_immutable;
                 DROP TRIGGER IF EXISTS registered_repository_exact_provisioning_insert;
                 DROP TRIGGER IF EXISTS registered_repository_checkout_insert;
                 DROP TRIGGER IF EXISTS registered_repository_checkout_update;
                 DROP TRIGGER IF EXISTS registered_repository_delete_guard;
                 DROP TRIGGER IF EXISTS workspace_root_exact_identity_insert;
                 DROP TRIGGER IF EXISTS workspace_root_exact_identity_update;
                 DROP TRIGGER IF EXISTS workspace_root_delete_guard;
                 DROP TRIGGER IF EXISTS local_submission_identity_immutable;
                 ALTER TABLE queue_items RENAME TO queue_items_schema3;
                 ALTER TABLE registered_repositories RENAME TO registered_repositories_schema3;
                 ALTER TABLE repository_provisioning_intents RENAME TO repository_provisioning_intents_schema3;
                 ALTER TABLE repository_bootstrap_requests RENAME TO repository_bootstrap_requests_schema3;
                 ALTER TABLE repository_remote_owners RENAME TO repository_remote_owners_schema3;",
            )?;
            transaction.execute_batch(SCHEMA4)?;
            transaction.execute_batch(COMPOSITION_SCHEMA4)?;
            transaction.execute_batch(
                "INSERT INTO queue_items(id,repo_key,producer_metadata_json,validation_evidence_json,status,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,landing_state_json,replacement_json,created_at,updated_at)
                 SELECT id,repo_key,producer_metadata_json,validation_evidence_json,status,current_attempt_id,blocked_phase,blocked_reason,blocked_message,retry_after,prompt_id,conflict_json,integration_workspace_path,integration_workspace_rift_id,integration_workspace_source_rift_id,integration_workspace_cleaned_at,target_sha,source_sha,landed_commit_sha,landing_state_json,replacement_json,created_at,updated_at FROM queue_items_schema3;
                 INSERT INTO repository_bootstrap_requests(request_path,storage_root_path,rift_registry_path,repo_key,created_at,updated_at)
                 SELECT request_path,storage_root_path,rift_registry_path,NULL,created_at,updated_at FROM repository_bootstrap_requests_schema3 WHERE repo_key IS NULL;",
            )?;
            for repo_key in &ready_repository_keys {
                transaction.execute(
                    "INSERT INTO registered_repositories(repo_key,owned_root_path,git_binding_json,root_rift_id,registry_identity,registry_device,registry_inode,generation,source_sha,checkout_json,development_root_path,development_kind,integration_root_path,integration_kind,provisioning_json,created_at,updated_at) SELECT repo_key,owned_root_path,'{}',root_rift_id,registry_identity,registry_device,registry_inode,generation,source_sha,checkout_json,development_root_path,development_kind,integration_root_path,integration_kind,provisioning_json,created_at,updated_at FROM registered_repositories_schema3 WHERE repo_key=?1",
                    [repo_key],
                )?;
                transaction.execute(
                    "INSERT INTO repository_bootstrap_requests(request_path,storage_root_path,rift_registry_path,repo_key,created_at,updated_at) SELECT request_path,storage_root_path,rift_registry_path,repo_key,created_at,updated_at FROM repository_bootstrap_requests_schema3 WHERE repo_key=?1",
                    [repo_key],
                )?;
            }
            for repo_key in &preserved_provisioning_keys {
                transaction.execute(
                    "INSERT INTO repository_provisioning_intents(repo_key,bootstrap_path,owned_root_path,staging_root_path,rift_registry_path,source_sha,policy_bytes,lifecycle_json,created_at,updated_at) SELECT repo_key,bootstrap_path,owned_root_path,staging_root_path,rift_registry_path,source_sha,policy_bytes,lifecycle_json,created_at,updated_at FROM repository_provisioning_intents_schema3 WHERE repo_key=?1",
                    [repo_key],
                )?;
                transaction.execute(
                    "INSERT INTO repository_bootstrap_requests(request_path,storage_root_path,rift_registry_path,repo_key,created_at,updated_at) SELECT request_path,storage_root_path,rift_registry_path,repo_key,created_at,updated_at FROM repository_bootstrap_requests_schema3 WHERE repo_key=?1",
                    [repo_key],
                )?;
                let lifecycle = schema4_provisioning_lifecycles
                    .get(repo_key)
                    .context("preserved provisioning lifecycle has no verified schema-4 state")?;
                transaction.execute(
                    "UPDATE repository_provisioning_intents SET lifecycle_json=?1 WHERE repo_key=?2",
                    params![lifecycle, repo_key],
                )?;
            }
            transaction.execute_batch(
                "DROP TABLE queue_items_schema3;
                 DROP TABLE registered_repositories_schema3;
                 DROP TABLE repository_provisioning_intents_schema3;
                 DROP TABLE repository_bootstrap_requests_schema3;
                 DROP TABLE repository_remote_owners_schema3;",
            )?;
            for (repo_key, binding) in &schema4_repository_bindings {
                let changed = transaction.execute(
                    "UPDATE registered_repositories SET git_binding_json=?1 WHERE repo_key=?2",
                    params![binding, repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("migration repository Git binding authority changed");
                }
            }
            transaction.execute_batch(REPOSITORY_POLICY_SCHEMA)?;
            reserve_all_policy_physical_ownership(&transaction)?;
            for admission in &admissions {
                match admission {
                    AdmissionPlan::Local {
                        item_id,
                        kind,
                        source_branch,
                        head_sha,
                        source_ref,
                        submission_id,
                        admitted_at,
                    } => {
                        transaction.execute(
                            "INSERT INTO queue_admissions(item_id,kind,source_branch,head_sha,source_ref,submission_id,admitted_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                            params![item_id,kind,source_branch,head_sha,source_ref,submission_id,admitted_at],
                        )?;
                    }
                    AdmissionPlan::MergeRequest {
                        item_id,
                        kind,
                        source_branch,
                        head_sha,
                        provider,
                        provider_host,
                        provider_repository,
                        provider_repository_id,
                        target_branch,
                        base_sha,
                        provider_merge_method,
                        identity,
                        url,
                        admitted_at,
                    } => {
                        transaction.execute(
                            "INSERT INTO queue_admissions(item_id,kind,source_branch,head_sha,provider,provider_host,provider_repository,provider_repository_id,target_branch,base_sha,provider_merge_method,merge_request_identity,merge_request_url,admitted_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                            params![item_id,kind,source_branch,head_sha,provider.to_string(),provider_host,provider_repository,provider_repository_id,target_branch,base_sha,provider_merge_method.map(|method| match method { crate::repository_policy::ProviderMergeMethod::Merge => "merge", crate::repository_policy::ProviderMergeMethod::Squash => "squash" }),identity,url,admitted_at],
                        )?;
                    }
                }
            }
            for (item_id, workspace) in &effort_workspace_repairs {
                let changed = transaction.execute(
                    "UPDATE integration_efforts SET workspace_json=?1,updated_at=?2 WHERE item_id=?3",
                    params![serde_json::to_string(workspace)?,timestamp,item_id],
                )?;
                if changed != 1 {
                    anyhow::bail!("migration effort workspace authority changed");
                }
                let queue_changed = transaction.execute(
                    "UPDATE queue_items SET integration_workspace_path=?1,integration_workspace_rift_id=?2,integration_workspace_source_rift_id=?3,updated_at=?4 WHERE id=?5 AND integration_workspace_path IS NULL AND integration_workspace_rift_id IS NULL AND integration_workspace_source_rift_id IS NULL AND integration_workspace_cleaned_at IS NULL",
                    params![workspace.path,workspace.rift_id,workspace.source_rift_id,timestamp,item_id],
                )?;
                if queue_changed != 1 {
                    anyhow::bail!(
                        "migration queue workspace authority contradicts explicit inventory"
                    );
                }
            }
            for (item_id, (path, binding)) in &schema4_workspace_bindings {
                let changed = transaction.execute(
                    "INSERT INTO workspace_git_bindings(owner_kind,owner_id,top_level,binding_json,created_at) SELECT 'integration',?1,CAST(?2 AS BLOB),?3,?4 WHERE EXISTS(SELECT 1 FROM queue_items WHERE id=?1 AND integration_workspace_path=?2 AND integration_workspace_rift_id IS NOT NULL)",
                    params![item_id,path,binding,timestamp],
                )?;
                if changed != 1 {
                    anyhow::bail!("migration workspace Git binding differs from queue authority");
                }
            }
            for (workspace_id, (path, binding)) in &schema4_development_bindings {
                let changed = transaction.execute(
                    "INSERT INTO workspace_git_bindings(owner_kind,owner_id,top_level,binding_json,created_at) SELECT 'development',?1,CAST(?2 AS BLOB),?3,?4 WHERE EXISTS(SELECT 1 FROM development_workspaces WHERE id=?1 AND CAST(path AS TEXT)=?2 AND rift_id IS NOT NULL AND status!='removed')",
                    params![workspace_id,path,binding,timestamp],
                )?;
                if changed != 1 {
                    anyhow::bail!(
                        "migration development workspace Git binding differs from workspace authority"
                    );
                }
            }
            for (item_id, runner) in &effort_runner_repairs {
                let changed = transaction.execute(
                    "UPDATE integration_efforts SET runner_snapshot_json=?1,updated_at=?2 WHERE item_id=?3",
                    params![serde_json::to_string(runner)?,timestamp,item_id],
                )?;
                if changed != 1 {
                    anyhow::bail!("migration effort runner authority changed");
                }
            }
            transaction.execute_batch(LANDING_STATE_TRIGGERS)?;
            transaction.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
            crate::control_store::install_control_schema(&transaction)?;
            for (item_id, authority) in &migration_termination_authorities {
                transaction.execute(
                    "UPDATE runner_termination_debt
                     SET authority_json=?1
                     WHERE effort_id=(SELECT id FROM integration_efforts WHERE item_id=?2)",
                    params![
                        serde_json::to_string(&serde_json::json!({
                            "state": "legacy_scope",
                            "payload": authority,
                        }))?,
                        item_id,
                    ],
                )?;
            }
            for item_id in &cancelled_incompatible_items {
                crate::control_store::cancel_item_for_migration(
                    &transaction,
                    item_id,
                    migration_termination_authorities.get(item_id),
                )?;
            }
            transaction.execute_batch(REGISTERED_REPOSITORY_TRIGGERS4)?;
            transaction.execute(
                "UPDATE queue_metadata SET value=?1 WHERE key='workspace_schema_version' AND value='3'",
                [crate::repository::SCHEMA_VERSION],
            )?;
            validate_schema_objects(&transaction)?;
            validate_schema4_contents(&transaction)?;
            validate_registered_repository_rows(&transaction)?;
            crate::repository::validate_provisioning_rows(&transaction)?;
            crate::control_store::validate_control_contents(&transaction)?;
            #[cfg(debug_assertions)]
            if std::env::var_os("IQ_TEST_SCHEMA3_FAIL_BEFORE_COMMIT").is_some() {
                anyhow::bail!("test interruption before schema-3 migration commit");
            }
            let integrity: String =
                transaction.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
            let foreign_key_errors: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM pragma_foreign_key_check",
                [],
                |row| row.get(0),
            )?;
            if integrity != "ok" || foreign_key_errors != 0 {
                anyhow::bail!("migrated schema/content invariant validation failed");
            }
            transaction.commit()?;
            connection.pragma_update(None, "legacy_alter_table", "OFF")?;
            connection.pragma_update(None, "foreign_keys", "ON")?;
            let validated_database_id = validate_existing_schema_identity(&connection)?;
            if validated_database_id != database_id {
                anyhow::bail!(
                    "migration candidate changed database identity; preserve backup {backup_path:?}"
                );
            }
            drop(connection);
            sync_database_file_and_parent(candidate.path())?;
            let mut publication = MigrationPublicationState {
                version: 1,
                database_id: database_id.clone(),
                source_digest: migration_source_digest.clone(),
                operation_id: operation_id.clone(),
                source_members: migration_source_members.clone(),
                candidate_root: candidate.root.clone(),
                candidate_device: candidate.device,
                candidate_inode: candidate.inode,
                phase: MigrationPublicationPhase::Prepared,
            };
            write_migration_publication_state(&path, &publication)?;
            #[cfg(debug_assertions)]
            if std::env::var_os("IQ_TEST_SCHEMA3_STOP_BEFORE_PUBLICATION").is_some() {
                std::process::exit(92);
            }
            exchange_database_files(candidate.path(), &path)?;
            candidate.preserve_for_recovery();
            fail_schema3_publication_after("exchange")?;
            #[cfg(debug_assertions)]
            if std::env::var_os("IQ_TEST_SCHEMA3_STOP_AFTER_PUBLICATION").is_some() {
                std::process::exit(93);
            }
            sync_database_file_and_parent(&path)?;
            fail_schema3_publication_after("primary_sync")?;
            sync_database_file_and_parent(candidate.path())?;
            publication.phase = MigrationPublicationPhase::Exchanged;
            write_migration_publication_state(&path, &publication)?;
            fail_schema3_publication_after("exchanged_state")?;
            sync_database_file_and_parent(&backup_path)?;
            let published = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            configure_connection(&published)?;
            if validate_existing_schema_identity(&published)? != database_id {
                anyhow::bail!("published schema-4 database identity is invalid");
            }
            drop(published);
            fail_schema3_publication_after("validation")?;
            publication.phase = MigrationPublicationPhase::Validated;
            write_migration_publication_state(&path, &publication)?;
            candidate.remove();
            fail_schema3_publication_after("candidate_cleanup")?;
            publication.phase = MigrationPublicationPhase::Complete;
            write_migration_publication_state(&path, &publication)?;
            File::open(path.parent().context("queue database path has no parent")?)?.sync_all()?;
            drop(exclusive);
            Ok(MigrationReport {
                completion: reconcile_migrated_runner_termination_debt(&path),
                database_id,
                from_schema: 3,
                to_schema: 4,
                repositories: stored_keys.len(),
                admissions: admissions.len(),
                backup_path,
            })
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
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    anyhow::bail!("queue database must be a regular file: {}", path.display())
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let unresolved = resolve_queue_database_path_without_creating(&path)?;
                    publish_fresh_database(&unresolved)?;
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("inspect queue db {}", path.display()))
                }
            }
            let path = resolve_queue_database_path_without_creating(&path)?;
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                anyhow::bail!("queue database must be a regular file: {}", path.display());
            }
            let expected_identity = (metadata.dev(), metadata.ino());
            let lease = crate::control_store::DatabaseProcessLease::acquire_existing(&path)?;
            let source = crate::control_store::PrimaryDatabaseIdentity::open(&path)?;
            let validated_database_id =
                crate::control_store::validate_database_snapshot_under_lease(
                    &path,
                    &lease,
                    |validation| {
                        let existing_tables: i64 = validation.query_row(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                            [],
                            |row| row.get(0),
                        )?;
                        if existing_tables == 0 {
                            return incompatible_local_state();
                        }
                        let metadata_exists: bool = validation.query_row(
                            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='queue_metadata')",
                            [],
                            |row| row.get(0),
                        )?;
                        if !metadata_exists {
                            return incompatible_local_state();
                        }
                        let version: Option<String> = validation
                            .query_row(
                                "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                                [],
                                |row| row.get(0),
                            )
                            .optional()?;
                        require_current_schema_version(version.as_deref())?;
                        crate::sqlite::validate_existing_schema_identity(validation)
                    },
                )?;
            let lease = lease.stabilize(&path)?;
            sync_database_file_and_parent(&path)?;
            stop_fresh_database_after("open_resynced");
            crate::control_store::run_runtime_open_handoff_test_hook(&path);
            lease.verify_authority(&path)?;
            source.verify_authoritative(&path)?;
            let conn = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )
            .with_context(|| format!("open queue db {}", path.display()))?;
            configure_connection(&conn)?;
            conn.busy_timeout(Self::OPEN_BUSY_TIMEOUT)?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            source.verify_authoritative(&path)?;
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect queue db {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                anyhow::bail!("queue database must be a regular file: {}", path.display());
            }
            if expected_identity != (metadata.dev(), metadata.ino()) {
                anyhow::bail!(
                    "queue database identity changed while opening: {}",
                    path.display()
                );
            }
            conn.pragma_update(None, "journal_mode", "WAL")?;
            let authoritative_database_id = validated_database_id;
            lease.verify_authority(&path)?;
            source.verify_authoritative(&path)?;
            let final_metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect queue db {}", path.display()))?;
            if final_metadata.file_type().is_symlink() || !final_metadata.is_file() {
                anyhow::bail!("queue database must be a regular file: {}", path.display());
            }
            if (final_metadata.dev(), final_metadata.ino()) != (metadata.dev(), metadata.ino()) {
                anyhow::bail!(
                    "queue database identity changed while opening: {}",
                    path.display()
                );
            }
            let authority =
                crate::control_store::ValidatedDatabaseAuthority::from_validated_connection(
                    path,
                    final_metadata.dev(),
                    final_metadata.ino(),
                    authoritative_database_id,
                    &conn,
                )?;
            drop(conn);
            drop(source);
            drop(lease);
            Ok(Self { authority })
        }

        fn connect(&self) -> Result<Connection> {
            let conn = self.authority.open_connection(
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.busy_timeout(Self::WRITE_BUSY_TIMEOUT)?;
            self.authority.verify_configured_connection(&conn)?;
            Ok(conn)
        }

        fn connect_read_only(&self) -> Result<Connection> {
            self.reader().connect(SqliteQueueReader::BUSY_TIMEOUT)
        }

        pub fn purge_terminal_item(&self, item_id: &str) -> Result<()> {
            crate::control_domain::require_exact_text(item_id, "queue item identity")?;
            let mut connection = self.connect()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute(
                "INSERT INTO queue_item_purge_authority(item_id,authorized_at) VALUES(?1,?2)",
                params![item_id, now()],
            )?;
            let deleted = transaction.execute("DELETE FROM queue_items WHERE id=?1", [item_id])?;
            if deleted != 1 {
                anyhow::bail!("terminal queue-item purge lost exact item authority");
            }
            transaction.commit()?;
            Ok(())
        }

        pub(crate) fn reader(&self) -> SqliteQueueReader {
            SqliteQueueReader {
                authority: self.authority.clone(),
            }
        }

        pub(crate) fn admit_direct(&self, request: DirectAdmissionRequest) -> Result<QueueItem> {
            let state_repository = request.state_repository.clone().validate()?;
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let policy: Option<(String, String, String)> = tx.query_row(
                "SELECT policy.operation_state_json,policy.integration_policy,policy.canonical_repository_json FROM registered_repositories repository JOIN repository_policies policy ON policy.repo_key=repository.repo_key WHERE repository.repo_key=?1",
                [&request.repo_key],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
            ).optional()?;
            let Some((operation_state, integration_policy, canonical_repository)) = policy else {
                anyhow::bail!("cannot enqueue for unknown repository {}", request.repo_key);
            };
            serde_json::from_str::<crate::repository_policy::GitRepository>(&canonical_repository)?
                .object_format()
                .require_oid(&request.current_head_sha, "direct admission head")?;
            serde_json::from_str::<crate::repository_policy::OperationState>(&operation_state)?
                .require_new_work()?;
            if integration_policy != "direct" {
                anyhow::bail!("repository policy rejects direct admission");
            }
            let now = now();
            let existing: Option<(String, String)> = tx
                .query_row(
                    "SELECT item.id,admission.head_sha FROM queue_items item JOIN queue_admissions admission ON admission.item_id=item.id WHERE item.repo_key=?1 AND admission.source_branch=?2 AND item.status NOT IN ('integrated','cancelled')",
                    params![request.repo_key, request.source_branch],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            let item_id = if let Some((id, current_head_sha)) = existing {
                if current_head_sha != request.current_head_sha {
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
                    "INSERT INTO queue_items (id,repo_key,producer_metadata_json,validation_evidence_json,status,created_at,updated_at)
                     VALUES (?1,?2,?3,'{}','ready',?4,?4)",
                    params![
                        id,
                        request.repo_key,
                        request.producer_metadata.to_string(),
                        now,
                    ],
                )?;
                tx.execute(
                    "INSERT INTO queue_admissions(item_id,kind,source_branch,head_sha,admitted_at) VALUES(?1,'direct',?2,?3,?4)",
                    params![id,request.source_branch,request.current_head_sha,now],
                )?;
                Self::record_event_tx(&tx, &id, "item_enqueued", "item enqueued")?;
                insert_state_repository_binding(&tx, &id, &state_repository, &now)?;
                id
            };
            tx.commit()?;
            self.get_item(&item_id)
        }

        pub(crate) fn admit_merge_request(
            &self,
            repo_key: &str,
            admission: &MergeRequestAdmission,
            producer_metadata: &Value,
            state_repository: &crate::control_domain::StateRepositorySnapshot,
        ) -> Result<QueueItem> {
            let state_repository = state_repository.clone().validate()?;
            let source_ref = match admission.provider {
                crate::repository_policy::Provider::Github => {
                    format!("refs/pull/{}/head", admission.identity)
                }
                crate::repository_policy::Provider::Gitlab => {
                    format!("refs/merge-requests/{}/head", admission.identity)
                }
            };
            if source_ref != admission.source_branch {
                anyhow::bail!("merge-request source ref differs from exact provider identity");
            }
            let mut connection = self.connect()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let stored_policy: (String, String, String) = transaction.query_row(
                "SELECT operation_state_json,integration_policy,canonical_repository_json FROM repository_policies WHERE repo_key=?1 AND EXISTS(SELECT 1 FROM registered_repositories WHERE repo_key=?1)",
                [repo_key],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
            )?;
            serde_json::from_str::<crate::repository_policy::OperationState>(&stored_policy.0)?
                .require_new_work()?;
            if stored_policy.1 != "merge_request_required" {
                anyhow::bail!("repository policy rejects merge-request admission");
            }
            let canonical: crate::repository_policy::GitRepository =
                serde_json::from_str(&stored_policy.2)?;
            canonical
                .object_format()
                .require_oid(&admission.head_sha, "admitted MR head SHA")?;
            canonical.object_format().require_oid(
                admission
                    .base_sha
                    .as_deref()
                    .context("merge-request admission requires exact admitted base SHA")?,
                "admitted MR base SHA",
            )?;
            let configured = canonical
                .provider()
                .context("merge-request policy has no canonical provider identity")?;
            if configured.provider != admission.provider
                || configured.host != admission.provider_host
                || configured.repository != admission.repository
                || configured.repository_id != admission.repository_id
            {
                anyhow::bail!("merge request does not belong to canonical provider repository");
            }
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT item.id FROM queue_items item JOIN queue_admissions admission ON admission.item_id=item.id WHERE item.repo_key=?1 AND admission.kind='merge_request' AND admission.provider=?2 AND admission.provider_repository=?3 AND admission.merge_request_identity=?4 AND item.status NOT IN ('integrated','cancelled')",
                    params![repo_key,admission.provider.to_string(),admission.repository,admission.identity],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(item_id) = existing {
                let stored: (String, Option<String>, String) = transaction.query_row(
                    "SELECT head_sha,base_sha,merge_request_url FROM queue_admissions WHERE item_id=?1",
                    [&item_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?;
                if stored
                    != (
                        admission.head_sha.clone(),
                        admission.base_sha.clone(),
                        admission.url.clone(),
                    )
                {
                    anyhow::bail!(
                        "active merge-request admission has different immutable identity"
                    );
                }
                transaction.commit()?;
                return self.get_item(&item_id);
            }
            let item_id = Uuid::new_v4().to_string();
            let timestamp = now();
            transaction.execute(
                "INSERT INTO queue_items(id,repo_key,producer_metadata_json,validation_evidence_json,status,created_at,updated_at) VALUES(?1,?2,?3,'{}','ready',?4,?4)",
                params![item_id,repo_key,producer_metadata.to_string(),timestamp],
            )?;
            transaction.execute(
                "INSERT INTO queue_admissions(item_id,kind,source_branch,head_sha,provider,provider_host,provider_repository,provider_repository_id,target_branch,base_sha,merge_request_identity,merge_request_url,admitted_at) VALUES(?1,'merge_request',?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
                params![item_id,source_ref,admission.head_sha,admission.provider.to_string(),admission.provider_host,admission.repository,admission.repository_id,admission.target_branch,admission.base_sha,admission.identity,admission.url,timestamp],
            )?;
            Self::record_event_tx(
                &transaction,
                &item_id,
                "merge_request_admitted",
                "coding-agent merge request admitted with exact head",
            )?;
            insert_state_repository_binding(&transaction, &item_id, &state_repository, &timestamp)?;
            transaction.commit()?;
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
                "SELECT * FROM queue_items_runtime WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled') ORDER BY created_at ASC, id ASC LIMIT 1",
                params![repo_key],
                map_item,
            )
            .optional()
            .with_context(|| format!("read oldest active item for repo queue {repo_key}"))
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
                    "SELECT * FROM queue_items_runtime WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled') ORDER BY created_at ASC, id ASC LIMIT 1",
                    params![repo_key],
                    map_item,
                )
                .optional()?;
            let Some(item) = item else {
                tx.commit()?;
                return Ok(None);
            };
            Self::require_mutation_authority(&tx, &item.repo_key, authority)?;
            Self::require_obligation_tx(
                &tx,
                &item.repo_key,
                &crate::repository_policy::Obligation::QueueItem {
                    id: item.id.clone(),
                },
            )?;
            if item.status != QueueStatus::Ready {
                tx.commit()?;
                return Ok(None);
            }
            let registered: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM registered_repositories WHERE repo_key=?1)",
                params![repo_key],
                |row| row.get(0),
            )?;
            let _ = registered;
            let attempt_id = Uuid::new_v4().to_string();
            let attempt_number: i64 = tx.query_row(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM integration_attempts WHERE item_id=?1",
                params![item.id],
                |row| row.get(0),
            )?;
            let now = now();
            let AttemptPolicy::Snapshot {
                snapshot_json,
                digest,
            } = policy;
            crate::composition::verify_policy_snapshot(snapshot_json, digest)?;
            tx.execute(
                "INSERT INTO integration_attempts (id,item_id,attempt_number,source_head_sha,policy_snapshot_json,policy_digest,started_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![attempt_id, item.id, attempt_number, item.current_head_sha, snapshot_json, digest, now],
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
                "SELECT * FROM queue_items_runtime WHERE repo_key=?1 AND status IN ('merging','merged','validating','validated','integrating') ORDER BY created_at ASC LIMIT 1",
                params![repo_key],
                map_item,
            )
            .optional()
            .with_context(|| format!("read next resumable active item for repo queue {repo_key}"))
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

        pub(crate) fn cancel_item_without_effort(&self, item_id: &str) -> Result<QueueItem> {
            self.transition_item_with_authority(
                item_id,
                QueueStatus::Cancelled,
                MutationAuthority::Cancellation,
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
                    "SELECT * FROM queue_items_runtime WHERE id=?1",
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
            if matches!(&authority, MutationAuthority::Cancellation)
                && target != QueueStatus::Cancelled
            {
                anyhow::bail!("cancellation authority can only cancel a queue item");
            }
            Self::require_mutation_authority(&tx, &item.repo_key, authority)?;
            if target != QueueStatus::Cancelled {
                Self::require_obligation_tx(
                    &tx,
                    &item.repo_key,
                    &crate::repository_policy::Obligation::QueueItem {
                        id: item.id.clone(),
                    },
                )?;
            }
            StateMachine
                .transition(item.status, target)
                .map_err(anyhow::Error::msg)?;
            if target == QueueStatus::Cancelled
                && item.landing.contains_external_landing_authority()
            {
                anyhow::bail!(
                    "item {item_id} has external landing authority and cannot be cancelled"
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
            if target == QueueStatus::Cancelled {
                match &item.workspace {
                    WorkspaceState::CreationIntent { path } => {
                        tx.execute(
                            "INSERT OR IGNORE INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,json_object('path',?2),'creation_intent','pending',NULL,0,?3,NULL,?3,?3)",
                            params![item_id, path, now()],
                        )?;
                    }
                    WorkspaceState::Retained { identity } => {
                        tx.execute(
                            "INSERT OR IGNORE INTO terminal_workspace_cleanup_debt(item_id,workspace_json,target_kind,state,reason,observation_count,next_retry_at,alert_event_id,created_at,updated_at) VALUES(?1,?2,'retained','pending',NULL,0,?3,NULL,?3,?3)",
                            params![item_id, serde_json::to_string(identity)?, now()],
                        )?;
                    }
                    WorkspaceState::NotCreated | WorkspaceState::Cleaned { .. } => {}
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
                "SELECT EXISTS(SELECT 1 FROM repo_leases WHERE repo_key=?1 AND owner_id=?2 AND expires_at>?3)
                   AND NOT EXISTS(
                     SELECT 1 FROM physical_repository_ownership ownership
                     LEFT JOIN physical_repository_leases lease ON lease.identity_key=ownership.identity_key
                     WHERE ownership.repo_key=?1 AND (lease.owner_id IS NOT ?2 OR lease.expires_at<=?3)
                   )",
                params![repo_key, owner_id, now()],
                |row| row.get(0),
            )?;
            if !authorized {
                anyhow::bail!("repository operation lease is not owned by {owner_id}");
            }
            Ok(())
        }

        fn require_new_work_tx(tx: &rusqlite::Transaction<'_>, repo_key: &str) -> Result<()> {
            let state: String = tx.query_row(
                "SELECT operation_state_json FROM repository_policies WHERE repo_key=?1",
                [repo_key],
                |row| row.get(0),
            )?;
            serde_json::from_str::<crate::repository_policy::OperationState>(&state)?
                .require_new_work()
        }

        fn require_obligation_tx(
            tx: &rusqlite::Transaction<'_>,
            repo_key: &str,
            obligation: &crate::repository_policy::Obligation,
        ) -> Result<()> {
            let state: String = tx.query_row(
                "SELECT operation_state_json FROM repository_policies WHERE repo_key=?1",
                [repo_key],
                |row| row.get(0),
            )?;
            serde_json::from_str::<crate::repository_policy::OperationState>(&state)?
                .require_obligation(obligation)
        }

        pub(crate) fn authorize_execution_start(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            authority: ExecutionStartAuthority<'_>,
            release_gate: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<()>,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (status, current_attempt_id, item_repo_key): (String, Option<String>, String) =
                required_row(
                    tx.query_row(
                        "SELECT status,current_attempt_id,repo_key FROM queue_items WHERE id=?1",
                        params![item_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    ),
                    "queue item",
                    item_id,
                )?;
            let authorized = status == expected_status.to_string()
                && current_attempt_id.as_deref() == Some(attempt_id);
            if authorized {
                let (repo_key, owner_id, expected_policy) = match authority {
                    ExecutionStartAuthority::RepositoryLease { repo_key, owner_id } => {
                        (repo_key, owner_id, None)
                    }
                    ExecutionStartAuthority::ProviderVerified {
                        repo_key,
                        owner_id,
                        policy_revision,
                        canonical,
                    } => (repo_key, owner_id, Some((policy_revision, canonical))),
                };
                Self::require_mutation_authority(
                    &tx,
                    &item_repo_key,
                    MutationAuthority::RepositoryLease { repo_key, owner_id },
                )?;
                let (revision, operation_state, canonical): (i64, String, String) = tx.query_row(
                    "SELECT policy.revision,policy.operation_state_json,policy.canonical_repository_json FROM queue_items item JOIN repository_policies policy ON policy.repo_key=item.repo_key WHERE item.id=?1",
                    [item_id],
                    |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
                )?;
                serde_json::from_str::<crate::repository_policy::OperationState>(&operation_state)?
                    .require_obligation(&crate::repository_policy::Obligation::QueueItem {
                        id: item_id.to_string(),
                    })?;
                let canonical =
                    serde_json::from_str::<crate::repository_policy::GitRepository>(&canonical)?;
                canonical.verify_local_bare()?;
                if let Some((expected_revision, expected_canonical)) = expected_policy {
                    if revision != expected_revision || &canonical != expected_canonical {
                        anyhow::bail!(
                            "repository policy changed after provider identity verification"
                        );
                    }
                }
                release_gate(&tx)?;
            }
            tx.commit()?;
            Ok(authorized)
        }

        pub(crate) fn authorize_new_work(&self, repo_key: &str) -> Result<()> {
            let connection = self.connect_read_only()?;
            let (operation, canonical): (String, String) = connection.query_row(
                "SELECT operation_state_json,canonical_repository_json FROM repository_policies WHERE repo_key=?1",
                [repo_key],
                |row| Ok((row.get(0)?,row.get(1)?)),
            )?;
            serde_json::from_str::<crate::repository_policy::OperationState>(&operation)?
                .require_new_work()?;
            serde_json::from_str::<crate::repository_policy::GitRepository>(&canonical)?
                .verify_local_bare()
        }

        pub(crate) fn authorize_obligation(
            &self,
            repo_key: &str,
            obligation: &crate::repository_policy::Obligation,
        ) -> Result<()> {
            let connection = self.connect_read_only()?;
            let (operation, canonical): (String, String) = connection.query_row(
                "SELECT operation_state_json,canonical_repository_json FROM repository_policies WHERE repo_key=?1",
                [repo_key],
                |row| Ok((row.get(0)?,row.get(1)?)),
            )?;
            serde_json::from_str::<crate::repository_policy::OperationState>(&operation)?
                .require_obligation(obligation)?;
            serde_json::from_str::<crate::repository_policy::GitRepository>(&canonical)?
                .verify_local_bare()
        }

        pub(crate) fn authorize_replication(
            &self,
            repo_key: &str,
            debt_id: &str,
            owner_id: &str,
        ) -> Result<()> {
            let connection = self.connect_read_only()?;
            Self::validate_replication_binding(&connection, repo_key, debt_id, Some(owner_id))?;
            let (state, item_id, canonical): (String, String, String) = connection.query_row(
                "SELECT policy.operation_state_json,debt.item_id,policy.canonical_repository_json FROM replication_debt debt JOIN repository_policies policy ON policy.repo_key=debt.repo_key WHERE debt.id=?1 AND debt.repo_key=?2",
                params![debt_id,repo_key],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
            )?;
            let state: crate::repository_policy::OperationState = serde_json::from_str(&state)?;
            if let crate::repository_policy::OperationState::Draining { obligations } = &state {
                if obligations
                    .contains(&crate::repository_policy::Obligation::QueueItem { id: item_id })
                {
                    return serde_json::from_str::<crate::repository_policy::GitRepository>(
                        &canonical,
                    )?
                    .verify_local_bare();
                }
            }
            state.require_obligation(&crate::repository_policy::Obligation::Replication {
                id: debt_id.to_string(),
            })?;
            serde_json::from_str::<crate::repository_policy::GitRepository>(&canonical)?
                .verify_local_bare()
        }

        pub(crate) fn authorize_replication_command(
            &self,
            repo_key: &str,
            debt_id: &str,
            owner_id: &str,
            args: &[OsString],
        ) -> Result<ReplicationDebt> {
            self.authorize_replication(repo_key, debt_id, owner_id)?;
            let debt = self.replication_debt(debt_id)?;
            if debt.operation == "pin_source" {
                let object = OsString::from(format!("{}^{{commit}}", debt.canonical_source_sha));
                let preserved_ref = OsString::from(format!("refs/iq/replication/{debt_id}"));
                let zero = OsString::from(debt.replica.object_format().zero_oid());
                let verify = [OsString::from("cat-file"), OsString::from("-e"), object];
                let publish = [
                    OsString::from("update-ref"),
                    preserved_ref.clone(),
                    OsString::from(&debt.canonical_source_sha),
                    zero,
                ];
                let confirm = [
                    OsString::from("update-ref"),
                    preserved_ref,
                    OsString::from(&debt.canonical_source_sha),
                    OsString::from(&debt.canonical_source_sha),
                ];
                if args != verify && args != publish && args != confirm {
                    anyhow::bail!(
                        "pin_source permits only exact source verification and pin publication"
                    );
                }
            }
            Ok(debt)
        }

        fn validate_replication_binding(
            connection: &Connection,
            repo_key: &str,
            debt_id: &str,
            lease_owner: Option<&str>,
        ) -> Result<ReplicationDebt> {
            let debt = required_row(
                connection.query_row(
                    "SELECT id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,expected_destination_sha,operation,outcome,application_id,failure,superseded_by_id FROM replication_debt WHERE id=?1 AND repo_key=?2",
                    params![debt_id, repo_key],
                    map_replication_debt,
                ),
                "replication debt",
                debt_id,
            )?;
            let (target_branch, replication_policy, landed_sha): (String, String, String) =
                connection.query_row(
                    "SELECT policy.target_branch,policy.replication_policy_json,item.landed_commit_sha FROM repository_policies policy JOIN queue_items item ON item.repo_key=policy.repo_key JOIN replication_debt debt ON debt.item_id=item.id AND debt.repo_key=item.repo_key WHERE policy.repo_key=?1 AND debt.id=?2 AND item.status='integrated'",
                    params![repo_key, debt_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?;
            let policy: crate::repository_policy::ReplicationPolicy =
                serde_json::from_str(&replication_policy)?;
            let configured = match policy {
                crate::repository_policy::ReplicationPolicy::Replicate { targets } => targets,
                crate::repository_policy::ReplicationPolicy::None => {
                    anyhow::bail!("replication debt has no immutable policy replica")
                }
            };
            if debt.target_branch != target_branch
                || debt.canonical_source_sha != landed_sha
                || debt.destination_key != debt.replica.destination_identity_key()?
                || !configured.iter().any(|replica| replica == &debt.replica)
            {
                anyhow::bail!("replication debt differs from immutable policy or landed item");
            }
            let repository_json = serde_json::to_string(&debt.replica)?;
            let ownership_matches: bool = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM physical_repository_ownership WHERE identity_key=?1 AND repo_key=?2 AND role='replica' AND repository_json=?3)",
                params![debt.destination_key, repo_key, repository_json],
                |row| row.get(0),
            )?;
            if !ownership_matches {
                anyhow::bail!("replication debt has no exact global destination ownership");
            }
            if let Some(owner_id) = lease_owner {
                let lease_matches: bool = connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM physical_repository_leases WHERE identity_key=?1 AND repo_key=?2 AND owner_id=?3 AND expires_at>?4)",
                    params![debt.destination_key, repo_key, owner_id, now()],
                    |row| row.get(0),
                )?;
                if !lease_matches {
                    anyhow::bail!("replication destination physical lease is not active");
                }
            }
            Ok(debt)
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
                    "SELECT * FROM queue_items_runtime WHERE id=?1",
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

        pub(crate) fn acquire_repo_operation_lease(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
            repository: &Path,
            target: &str,
        ) -> Result<()> {
            self.validate_repository_binding(repo_key, repository, target)?;
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let heartbeat_at = now();
            let expires_at = (Utc::now() + Duration::seconds(ttl_seconds)).to_rfc3339();
            let identities = {
                let mut statement = tx.prepare(
                    "SELECT identity_key FROM physical_repository_ownership WHERE repo_key=?1 ORDER BY identity_key",
                )?;
                let identities = statement
                    .query_map([repo_key], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                identities
            };
            if identities.is_empty() {
                anyhow::bail!("repository has no physical ownership authority");
            }
            for identity in identities {
                let changed = tx.execute(
                    "INSERT INTO physical_repository_leases(identity_key,repo_key,owner_id,heartbeat_at,expires_at) VALUES(?1,?2,?3,?4,?5)
                     ON CONFLICT(identity_key) DO UPDATE SET repo_key=excluded.repo_key,owner_id=excluded.owner_id,heartbeat_at=excluded.heartbeat_at,expires_at=excluded.expires_at
                     WHERE physical_repository_leases.expires_at<=excluded.heartbeat_at OR physical_repository_leases.repo_key=excluded.repo_key",
                    params![identity, repo_key, owner_id, heartbeat_at, expires_at],
                )?;
                if changed != 1 {
                    anyhow::bail!("physical repository has an active operation");
                }
            }
            tx.execute(
                "INSERT INTO repo_leases (repo_key,owner_id,heartbeat_at,expires_at) VALUES (?1,?2,?3,?4)
                 ON CONFLICT(repo_key) DO UPDATE SET owner_id=excluded.owner_id,heartbeat_at=excluded.heartbeat_at,expires_at=excluded.expires_at",
                params![repo_key, owner_id, heartbeat_at, expires_at],
            )?;
            tx.commit()?;
            Ok(())
        }

        pub(crate) fn validate_repository_binding(
            &self,
            repo_key: &str,
            repository: &Path,
            target: &str,
        ) -> Result<()> {
            let conn = self.connect_read_only()?;
            let invalid: i64 = conn.query_row(
                "SELECT
                   (SELECT CASE WHEN COUNT(*)=1 THEN 0 ELSE 1 END FROM registered_repositories WHERE repo_key=?1) +
                   (SELECT COUNT(*) FROM registered_repositories repository JOIN repository_policies policy ON policy.repo_key=repository.repo_key WHERE repository.repo_key=?1 AND (repository.owned_root_path!=?2 OR policy.target_branch!=?3)) +
                   (SELECT COUNT(*) FROM registered_repositories WHERE repo_key!=?1 AND owned_root_path=?2)",
                params![repo_key,path_bytes(repository),target],
                |row| row.get(0),
            )?;
            if invalid != 0 {
                anyhow::bail!(
                    "repository queue {repo_key} has {invalid} durable repository binding conflict(s)"
                );
            }
            Ok(())
        }

        pub(crate) fn heartbeat_repo_lease(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current_time = now();
            let expires_at = (Utc::now() + Duration::seconds(ttl_seconds)).to_rfc3339();
            let physical_changed = tx.execute(
                "UPDATE physical_repository_leases SET heartbeat_at=?1,expires_at=?2 WHERE repo_key=?3 AND owner_id=?4 AND expires_at>?1",
                params![current_time, expires_at, repo_key, owner_id],
            )?;
            let physical_expected: usize = tx.query_row(
                "SELECT COUNT(*) FROM physical_repository_ownership WHERE repo_key=?1",
                [repo_key],
                |row| row.get(0),
            )?;
            if physical_changed != physical_expected || physical_expected == 0 {
                return Ok(false);
            }
            let changed = tx.execute(
                "UPDATE repo_leases SET heartbeat_at=?1,expires_at=?2 WHERE repo_key=?3 AND owner_id=?4 AND expires_at>?1",
                params![current_time, expires_at, repo_key, owner_id],
            )?;
            tx.commit()?;
            Ok(changed == 1)
        }

        pub(crate) fn ensure_repo_lease_owner(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<bool> {
            self.heartbeat_repo_lease(repo_key, owner_id, ttl_seconds)
        }

        pub(crate) fn release_repo_lease(&self, repo_key: &str, owner_id: &str) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "DELETE FROM physical_repository_leases WHERE repo_key=?1 AND owner_id=?2",
                params![repo_key, owner_id],
            )?;
            let released = tx.execute(
                "DELETE FROM repo_leases WHERE repo_key=?1 AND owner_id=?2",
                params![repo_key, owner_id],
            )? == 1;
            tx.commit()?;
            Ok(released)
        }

        pub(crate) fn register_workspace_root(
            &self,
            repo_key: &str,
            source_path: &Path,
            source_rift_id: &str,
            workspace_root: &Path,
            registry_identity: &str,
        ) -> Result<()> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let registered: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM registered_repositories WHERE repo_key=?1)",
                params![repo_key],
                |row| row.get(0),
            )?;
            if !registered {
                anyhow::bail!("owned repository is not registered");
            }
            let exact: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_roots WHERE repo_key=?1 AND root_path=?2 AND source_path=?3 AND source_rift_id=?4 AND registry_identity=?5)",
                params![repo_key,path_bytes(workspace_root),path_bytes(source_path),source_rift_id,path_bytes(Path::new(registry_identity))],
                |row| row.get(0),
            )?;
            if !exact {
                anyhow::bail!("owned repository child-root authority differs from persisted state");
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
                    "SELECT CAST(root_path AS TEXT) FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
                    params![repo_key],
                    |row| row.get(0),
                )
                .optional()?;
            let expected = workspace_root
                .to_str()
                .context("IQ workspace root is not valid UTF-8")?;
            if existing.as_deref() != Some(expected) {
                anyhow::bail!(
                    "repository queue {repo_key} workspace root differs from persisted state"
                );
            }
            Ok(())
        }

        pub fn workspace_root_generation(&self, repo_key: &str) -> Result<i64> {
            self.workspace_root_generation_for_kind(repo_key, "integration")
        }

        pub(crate) fn workspace_root_generation_for_kind(
            &self,
            repo_key: &str,
            kind: &str,
        ) -> Result<i64> {
            match self.workspace_root_generation_state_for_kind(repo_key, kind)? {
                WorkspaceGenerationState::Ready { current } => Ok(current),
                WorkspaceGenerationState::Pending { .. } => anyhow::bail!(
                    "repository queue {repo_key} {kind} workspace generation is pending reconciliation"
                ),
            }
        }

        pub(crate) fn workspace_root_generation_state_for_kind(
            &self,
            repo_key: &str,
            kind: &str,
        ) -> Result<WorkspaceGenerationState> {
            let conn = self.connect_read_only()?;
            let state = conn.query_row(
                "SELECT generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind=?2",
                params![repo_key, kind],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .optional()?
            .with_context(|| format!("repository queue {repo_key} has no {kind} workspace root"))?;
            WorkspaceGenerationState::from_stored(state.0, state.1)
        }

        pub fn workspace_root_path(&self, repo_key: &str) -> Result<Option<PathBuf>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT CAST(root_path AS TEXT) FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
                params![repo_key],
                |row| row.get::<_, String>(0),
            )
                .optional()
                .map(|path| path.map(PathBuf::from))
                .map_err(Into::into)
        }

        pub fn workspace_root_identity(
            &self,
            repo_key: &str,
            workspace_root: &Path,
        ) -> Result<Option<WorkspaceRootIdentity>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT root_path,source_path,source_rift_id,repo_key,registry_identity,generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND root_path=?2 AND kind='integration'",
                params![repo_key,path_bytes(workspace_root)],
                |row| {
                    Ok(WorkspaceRootIdentity {
                        path: row_path(row, "root_path")?,
                        source: row_path(row, "source_path")?,
                        source_rift_id: row.get(2)?,
                        scope: row.get(3)?,
                        registry_identity: row_path(row, "registry_identity")?
                            .into_os_string()
                            .into_string()
                            .map_err(|_| {
                                map_parse_error("Rift registry identity is not valid UTF-8".into())
                            })?,
                        generation: row.get(5)?,
                        pending_generation: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        }

        pub fn registered_workspace_root_identity(
            &self,
            repo_key: &str,
        ) -> Result<Option<WorkspaceRootIdentity>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT root_path,source_path,source_rift_id,repo_key,registry_identity,generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
                params![repo_key],
                |row| {
                    Ok(WorkspaceRootIdentity {
                        path: row_path(row, "root_path")?,
                        source: row_path(row, "source_path")?,
                        source_rift_id: row.get(2)?,
                        scope: row.get(3)?,
                        registry_identity: row_path(row, "registry_identity")?
                            .into_os_string()
                            .into_string()
                            .map_err(|_| {
                                map_parse_error("Rift registry identity is not valid UTF-8".into())
                            })?,
                        generation: row.get(5)?,
                        pending_generation: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        }

        pub(crate) fn begin_development_workspace_generation(
            &self,
            repo_key: &str,
            workspace_id: &str,
        ) -> Result<WorkspaceGenerationState> {
            let state = self.begin_workspace_generation_for_kind(
                repo_key,
                "development",
                Some(&crate::repository_policy::Obligation::Workspace {
                    id: workspace_id.to_string(),
                }),
            )?;
            stop_workspace_generation_after("development_recorded");
            Ok(state)
        }

        fn begin_workspace_generation_for_kind(
            &self,
            repo_key: &str,
            kind: &str,
            obligation: Option<&crate::repository_policy::Obligation>,
        ) -> Result<WorkspaceGenerationState> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if let Some(obligation) = obligation {
                Self::require_obligation_tx(&tx, repo_key, obligation)?;
            }
            let changed = tx.execute(
                "UPDATE workspace_roots SET pending_generation=generation+1 WHERE repo_key=?1 AND kind=?2 AND pending_generation IS NULL",
                params![repo_key, kind],
            )?;
            let state: (i64, Option<i64>) = tx
                .query_row(
                    "SELECT generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind=?2",
                    params![repo_key,kind],
                    |row| Ok((row.get(0)?,row.get(1)?)),
                )
                .optional()?
                .with_context(|| format!("repository queue {repo_key} has no registered {kind} workspace root"))?;
            if changed == 0 && state.1.is_none() {
                anyhow::bail!("repository workspace generation did not enter pending state");
            }
            tx.commit()?;
            let pending = state
                .1
                .context("pending workspace generation disappeared")?;
            Ok(WorkspaceGenerationState::Pending {
                current: state.0,
                pending,
            })
        }

        pub(crate) fn complete_workspace_generation(
            &self,
            repo_key: &str,
            kind: &str,
            expected: WorkspaceGenerationState,
        ) -> Result<i64> {
            let WorkspaceGenerationState::Pending { current, pending } = expected else {
                anyhow::bail!("workspace generation completion requires pending authority");
            };
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE workspace_roots SET generation=?1,pending_generation=NULL WHERE repo_key=?2 AND kind=?3 AND generation=?4 AND pending_generation=?1",
                params![pending,repo_key,kind,current],
            )?;
            if changed != 1 {
                anyhow::bail!("pending workspace generation authority changed before completion");
            }
            tx.commit()?;
            Ok(pending)
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

        pub(crate) fn provision_repository(
            &self,
            options: &crate::repository::ProvisionOptions,
        ) -> Result<crate::repository::OwnedRepositoryRoot> {
            let mut connection = self.connect()?;
            crate::repository::provision(&mut connection, options)
        }

        pub(crate) fn verify_owned_repository(
            &self,
            repo_key: &str,
        ) -> Result<RegisteredRepository> {
            let repository = self.repository(repo_key)?;
            let database_id = self.database_id()?;
            crate::repository::verify_registered_repository(&repository, &database_id)?;
            let conn = self.connect_read_only()?;
            let mut statement = conn.prepare(
                "SELECT kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode,generation,pending_generation FROM workspace_roots WHERE repo_key=?1 ORDER BY kind",
            )?;
            let roots = statement
                .query_map([repo_key], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row_path(row, "root_path")?,
                        row_path(row, "source_path")?,
                        row.get::<_, String>(3)?,
                        row_path(row, "registry_identity")?,
                        row.get::<_, u64>(5)?,
                        row.get::<_, u64>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(statement);
            drop(conn);
            if roots.len() != 2 {
                anyhow::bail!("owned repository must have exactly two child-root authorities");
            }
            for (
                kind,
                root,
                source,
                source_rift_id,
                registry,
                registry_device,
                registry_inode,
                generation,
                pending_generation,
            ) in roots
            {
                let expected = match kind.as_str() {
                    "development" => &repository.development_root_path,
                    "integration" => &repository.integration_root_path,
                    _ => anyhow::bail!("owned repository has unknown child-root scope {kind}"),
                };
                if &root != expected
                    || source != repository.owned_root_path
                    || source_rift_id != repository.root_rift_id
                    || registry != repository.registry_identity
                    || registry_device != repository.registry_device
                    || registry_inode != repository.registry_inode
                {
                    anyhow::bail!("owned repository {kind} child-root authority changed");
                }
                crate::integrator::RiftWorkspaceManager::inspect(
                    repository.owned_root_path.clone(),
                    root,
                    repository.key.clone(),
                    &kind,
                    Some(repository.registry_identity.clone()),
                    &database_id,
                    WorkspaceGenerationState::from_stored(generation, pending_generation)?,
                )?;
            }
            Ok(repository)
        }

        pub fn path(&self) -> &Path {
            self.authority.path()
        }

        pub fn validated_control_store(&self) -> Result<crate::control_store::ControlStore> {
            crate::control_store::ControlStore::open_validated(self.authority.clone())
        }

        pub fn notification_dispatcher(
            &self,
            config: crate::agent_config::NotificationConfig,
        ) -> Result<crate::notifications::NotificationDispatcher> {
            self.authority.verify_path()?;
            Ok(
                crate::notifications::NotificationDispatcher::from_validated_authority(
                    self.authority.clone(),
                    config,
                ),
            )
        }

        pub(crate) fn record_workspace_gc_debt(&self, registry_identity: &str) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "INSERT OR IGNORE INTO workspace_gc_debt (registry_identity,created_at) VALUES (?1,?2)",
                params![registry_identity, now()],
            )?;
            Ok(())
        }

        pub fn authorize_deleted_cycle_sandbox_repair(
            &self,
            repo_key: &str,
            workspace_root: &Path,
            cycle_id: &str,
        ) -> Result<DeletedCycleSandboxRepair> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let live: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM integration_cycles cycle JOIN integration_efforts effort ON effort.id=cycle.effort_id JOIN queue_items item ON item.id=effort.item_id WHERE item.repo_key=?1 AND cycle.id=?2 AND cycle.status IN ('starting','running')) OR EXISTS(SELECT 1 FROM integration_efforts effort JOIN queue_items item ON item.id=effort.item_id WHERE item.repo_key=?1 AND effort.state IN ('agent_launching','agent_running') AND json_extract(effort.state_json,'$.payload.cycle_id')=?2) OR EXISTS(SELECT 1 FROM runner_termination_debt debt JOIN integration_efforts effort ON effort.id=debt.effort_id JOIN queue_items item ON item.id=effort.item_id WHERE item.repo_key=?1 AND json_extract(debt.authority_json,'$.payload.cycle_id')=?2)",
                params![repo_key,cycle_id],
                |row| row.get(0),
            )?;
            if live {
                tx.commit()?;
                return Ok(DeletedCycleSandboxRepair::PreservedDurableAuthority);
            }
            let cycle_exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM integration_cycles cycle JOIN integration_efforts effort ON effort.id=cycle.effort_id JOIN queue_items item ON item.id=effort.item_id WHERE item.repo_key=?1 AND cycle.id=?2)",
                params![repo_key,cycle_id],
                |row| row.get(0),
            )?;
            if cycle_exists {
                tx.commit()?;
                return Ok(DeletedCycleSandboxRepair::PreservedDurableAuthority);
            }
            let key = format!("deleted_cycle_sandbox_repair:{repo_key}:{cycle_id}");
            let value = serde_json::json!({
                "version": 1,
                "repo_key": repo_key,
                "workspace_root": workspace_root,
                "cycle_id": cycle_id,
                "state": "authorized"
            });
            let value = serde_json::to_string(&value)?;
            tx.execute(
                "INSERT INTO queue_metadata(key,value) VALUES(?1,?2) ON CONFLICT(key) DO NOTHING",
                params![key, value],
            )?;
            let persisted: String = tx.query_row(
                "SELECT value FROM queue_metadata WHERE key=?1",
                params![key],
                |row| row.get(0),
            )?;
            if persisted != value {
                anyhow::bail!(
                    "deleted-cycle sandbox repair authority differs from durable metadata"
                );
            }
            tx.commit()?;
            Ok(DeletedCycleSandboxRepair::Authorized)
        }

        pub(crate) fn clear_workspace_gc_debt(&self, registry_identity: &str) -> Result<()> {
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

        pub(crate) fn record_event(
            &self,
            item_id: &str,
            event_type: &str,
            message: &str,
        ) -> Result<()> {
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

        pub(crate) fn record_attempt_validation(
            &self,
            attempt_id: &str,
            invocation: &AttemptValidationInvocation<'_>,
        ) -> Result<i64> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE integration_attempts SET validation_command=?1,validation_exit_code=?2,validation_log_path=?3,validated_commit_sha=NULL WHERE id=?4",
                params![invocation.command, invocation.exit_code, invocation.log_path, attempt_id],
            )?;
            if changed != 1 {
                anyhow::bail!("validation attempt disappeared before invocation recording");
            }
            let invocation_number: i64 = tx.query_row(
                "SELECT coalesce(max(invocation_number)+1,1) FROM validation_invocations WHERE attempt_id=?1",
                [attempt_id],
                |row| row.get(0),
            )?;
            let inserted = tx.execute(
                "INSERT INTO validation_invocations(attempt_id,invocation_number,target_base_sha,candidate_sha,command,exit_code,log_path,validated_commit_sha,invalidated_at,created_at)
                 SELECT id,?2,target_base_sha,?3,?4,?5,?6,NULL,NULL,?7 FROM integration_attempts WHERE id=?1 AND target_base_sha IS NOT NULL",
                params![attempt_id,invocation_number,invocation.candidate_sha,invocation.command,invocation.exit_code,invocation.log_path,now()],
            )?;
            if inserted != 1 {
                anyhow::bail!("validation invocation has no exact target base");
            }
            tx.commit()?;
            Ok(invocation_number)
        }

        pub(crate) fn record_attempt_revalidation(
            &self,
            attempt_id: &str,
            target_base_sha: &str,
            invocation: &AttemptValidationInvocation<'_>,
        ) -> Result<i64> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE integration_attempts SET target_base_sha=?1,validation_command=?2,validation_exit_code=?3,validation_log_path=?4,validated_commit_sha=NULL WHERE id=?5",
                params![target_base_sha, invocation.command, invocation.exit_code, invocation.log_path, attempt_id],
            )?;
            if changed != 1 {
                anyhow::bail!("revalidation attempt disappeared before invocation recording");
            }
            let invocation_number: i64 = tx.query_row(
                "SELECT coalesce(max(invocation_number)+1,1) FROM validation_invocations WHERE attempt_id=?1",
                [attempt_id],
                |row| row.get(0),
            )?;
            let inserted = tx.execute(
                "INSERT INTO validation_invocations(attempt_id,invocation_number,target_base_sha,candidate_sha,command,exit_code,log_path,validated_commit_sha,invalidated_at,created_at)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,NULL,?8)",
                params![attempt_id,invocation_number,target_base_sha,invocation.candidate_sha,invocation.command,invocation.exit_code,invocation.log_path,now()],
            )?;
            if inserted != 1 {
                anyhow::bail!("revalidation invocation was not recorded");
            }
            tx.commit()?;
            Ok(invocation_number)
        }

        pub(crate) fn complete_attempt_validation(
            &self,
            attempt_id: &str,
            invocation_number: i64,
            validated_commit_sha: &str,
        ) -> Result<()> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            attempt_object_format(&tx, attempt_id)?
                .require_oid(validated_commit_sha, "validated invocation candidate SHA")?;
            let invocation_changed = tx.execute(
                "UPDATE validation_invocations SET validated_commit_sha=?1
                 WHERE attempt_id=?2 AND invocation_number=?3 AND candidate_sha=?1
                   AND validated_commit_sha IS NULL AND invalidated_at IS NULL
                   AND invocation_number=(SELECT max(invocation_number) FROM validation_invocations WHERE attempt_id=?2)
                   AND EXISTS(
                     SELECT 1 FROM integration_efforts effort
                     WHERE effort.attempt_id=?2
                       AND effort.state='validating'
                       AND json_extract(effort.state_json,'$.payload.stage')='running'
                       AND json_extract(effort.state_json,'$.payload.candidate_sha')=?1
                   )",
                params![validated_commit_sha, attempt_id, invocation_number],
            )?;
            if invocation_changed != 1 {
                anyhow::bail!("validation completion differs from invocation candidate authority");
            }
            let attempt_changed = tx.execute(
                "UPDATE integration_attempts SET validated_commit_sha=?1 WHERE id=?2",
                params![validated_commit_sha, attempt_id],
            )?;
            if attempt_changed != 1 {
                anyhow::bail!("validation attempt disappeared before success recording");
            }
            tx.commit()?;
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
                "UPDATE integration_attempts SET validated_commit_sha=?1
                 WHERE id=?2 AND item_id=?3 AND target_base_sha=?4 AND merge_commit_sha=?1
                   AND EXISTS(
                     SELECT 1 FROM integration_efforts effort
                     WHERE effort.attempt_id=integration_attempts.id
                       AND effort.item_id=integration_attempts.item_id
                       AND effort.state='validating'
                       AND json_extract(effort.state_json,'$.payload.stage')='running'
                       AND json_extract(effort.state_json,'$.payload.candidate_sha')=?1
                   )",
                params![candidate_sha, attempt_id, item_id, target_base_sha],
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

        pub(crate) fn update_attempt_signoff(
            &self,
            attempt_id: &str,
            evidence: &Value,
        ) -> Result<()> {
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

        pub(crate) fn set_workspace_intent(&self, item_id: &str, path: &str) -> Result<()> {
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

        pub(crate) fn begin_workspace_creation(
            &self,
            repo_key: &str,
            item_id: &str,
            path: &str,
        ) -> Result<WorkspaceGenerationState> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let root_changed = tx.execute(
                "UPDATE workspace_roots SET pending_generation=generation+1 WHERE repo_key=?1 AND kind='integration' AND pending_generation IS NULL",
                params![repo_key],
            )?;
            let generation: (i64, Option<i64>) = tx.query_row(
                "SELECT generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
                params![repo_key],
                |row| Ok((row.get(0)?,row.get(1)?)),
            ).optional()?.with_context(|| format!("repository queue {repo_key} has no registered integration workspace root"))?;
            if root_changed == 0 && generation.1.is_none() {
                anyhow::bail!("integration workspace generation did not enter pending state");
            }
            let item_changed = tx.execute(
                "UPDATE queue_items SET integration_workspace_path=?1,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,integration_workspace_cleaned_at=NULL,updated_at=?2 WHERE id=?3 AND repo_key=?4 AND status='merging'",
                params![path, now(), item_id, repo_key],
            )?;
            if item_changed != 1 {
                anyhow::bail!("item {item_id} is no longer merging; refusing workspace creation");
            }
            tx.commit()?;
            stop_workspace_generation_after("integration_recorded");
            Ok(WorkspaceGenerationState::Pending {
                current: generation.0,
                pending: generation
                    .1
                    .context("pending integration generation disappeared")?,
            })
        }

        pub(crate) fn set_workspace_identity(
            &self,
            item_id: &str,
            path: &str,
            rift_id: &str,
            source_rift_id: &str,
        ) -> Result<()> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE queue_items SET integration_workspace_rift_id=?1,integration_workspace_source_rift_id=?2,updated_at=?3 WHERE id=?4 AND status='merging' AND integration_workspace_path=?5",
                params![rift_id, source_rift_id, now(), item_id, path],
            )?;
            if changed != 1 {
                anyhow::bail!(
                    "item {item_id} workspace intent changed before Rift identity was persisted"
                );
            }
            let repo_key: String = tx.query_row(
                "SELECT repo_key FROM queue_items WHERE id=?1",
                [item_id],
                |row| row.get(0),
            )?;
            insert_workspace_git_binding(&tx, &repo_key, "integration", item_id, Path::new(path))?;
            tx.commit()?;
            Ok(())
        }

        pub(crate) fn mark_workspace_cleaned(&self, item_id: &str) -> Result<()> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE queue_items SET integration_workspace_path=NULL,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,integration_workspace_cleaned_at=?1,updated_at=?1 WHERE id=?2 AND status IN ('integrated','cancelled') AND integration_workspace_cleaned_at IS NULL",
                params![now(), item_id],
            )?;
            if changed == 1 {
                tx.execute(
                    "DELETE FROM workspace_git_bindings WHERE owner_kind='integration' AND owner_id=?1",
                    [item_id],
                )?;
                Self::record_event_tx(
                    &tx,
                    item_id,
                    "workspace_cleaned",
                    "removed terminal Rift workspace and reclaimed Rift trash",
                )?;
            } else {
                let still_cleaned: bool = tx.query_row(
                    "SELECT integration_workspace_cleaned_at IS NOT NULL FROM queue_items WHERE id=?1",
                    params![item_id],
                    |row| row.get(0),
                )?;
                if !still_cleaned {
                    anyhow::bail!("terminal workspace cleanup state changed concurrently");
                }
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
            let physical_changed = tx.execute(
                "UPDATE physical_repository_leases SET heartbeat_at=?1,expires_at=?2 WHERE repo_key=?3 AND owner_id=?4 AND expires_at>?1",
                params![current_time, (Utc::now() + Duration::seconds(30)).to_rfc3339(), repo_key, owner_id],
            )?;
            let physical_expected: usize = tx.query_row(
                "SELECT COUNT(*) FROM physical_repository_ownership WHERE repo_key=?1",
                [repo_key],
                |row| row.get(0),
            )?;
            if physical_changed != physical_expected || physical_expected == 0 {
                anyhow::bail!(
                    "repo queue {repo_key} physical composition lease is not owned by {owner_id}"
                );
            }
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
            crate::repository::RepoKey::from_stored(repo_key)?;
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

        pub(crate) fn begin_repository_draining(
            &self,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<RegisteredRepository> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                let (revision, state): (i64, String) = transaction.query_row(
                    "SELECT revision,operation_state_json FROM repository_policies WHERE repo_key=?1",
                    [repo_key],
                    |row| Ok((row.get(0)?,row.get(1)?)),
                )?;
                if serde_json::from_str::<crate::repository_policy::OperationState>(&state)?
                    != crate::repository_policy::OperationState::Enabled
                {
                    anyhow::bail!("only an enabled repository can enter draining");
                }
                let creating_submissions: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM local_submissions WHERE repo_key=?1 AND state='creating')",
                    [repo_key],
                    |row| row.get(0),
                )?;
                if creating_submissions {
                    anyhow::bail!("repository has an incomplete local submission intent");
                }
                let mut obligations = std::collections::BTreeSet::new();
                let mut workspaces = transaction.prepare(
                    "SELECT id FROM development_workspaces WHERE repo_key=?1 AND status!='removed'",
                )?;
                for id in workspaces.query_map([repo_key], |row| row.get::<_, String>(0))? {
                    obligations.insert(crate::repository_policy::Obligation::Workspace { id: id? });
                }
                let mut items = transaction.prepare(
                    "SELECT id FROM queue_items WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled')",
                )?;
                for id in items.query_map([repo_key], |row| row.get::<_, String>(0))? {
                    obligations.insert(crate::repository_policy::Obligation::QueueItem { id: id? });
                }
                let mut debts = transaction.prepare(
                    "SELECT id FROM replication_debt WHERE repo_key=?1 AND outcome NOT IN ('succeeded','superseded')",
                )?;
                for id in debts.query_map([repo_key], |row| row.get::<_, String>(0))? {
                    obligations.insert(crate::repository_policy::Obligation::Replication { id: id? });
                }
                let state = crate::repository_policy::OperationState::Draining { obligations };
                let changed = transaction.execute(
                    "UPDATE repository_policies SET revision=revision+1,operation_state_json=?1,updated_at=?2 WHERE repo_key=?3 AND revision=?4",
                    params![serde_json::to_string(&state)?,now(),repo_key,revision],
                )?;
                if changed != 1 {
                    anyhow::bail!("repository policy changed during draining transition");
                }
                Ok(())
            })?;
            self.repository(repo_key)
        }

        pub(crate) fn disable_drained_repository(
            &self,
            repo_key: &str,
            owner_id: &str,
        ) -> Result<RegisteredRepository> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                let (revision, state): (i64, String) = transaction.query_row(
                    "SELECT revision,operation_state_json FROM repository_policies WHERE repo_key=?1",
                    [repo_key],
                    |row| Ok((row.get(0)?,row.get(1)?)),
                )?;
                let crate::repository_policy::OperationState::Draining { obligations } =
                    serde_json::from_str(&state)?
                else {
                    anyhow::bail!("only a draining repository can be disabled");
                };
                for obligation in obligations {
                    let complete = match obligation {
                        crate::repository_policy::Obligation::Workspace { id } => transaction
                            .query_row(
                                "SELECT status='removed' FROM development_workspaces WHERE id=?1 AND repo_key=?2",
                                params![id,repo_key],
                                |row| row.get::<_, bool>(0),
                            )
                            .optional()?
                            .unwrap_or(false),
                        crate::repository_policy::Obligation::QueueItem { id } => transaction
                            .query_row(
                                "SELECT status IN ('integrated','cancelled') AND (integration_workspace_path IS NULL OR integration_workspace_cleaned_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM terminal_workspace_cleanup_debt debt WHERE debt.item_id=queue_items.id AND debt.state!='complete') AND NOT EXISTS(SELECT 1 FROM replication_debt debt WHERE debt.item_id=queue_items.id AND debt.outcome NOT IN ('succeeded','superseded')) FROM queue_items WHERE id=?1 AND repo_key=?2",
                                params![id,repo_key],
                                |row| row.get::<_, bool>(0),
                            )
                            .optional()?
                            .unwrap_or(false),
                        crate::repository_policy::Obligation::Replication { id } => transaction
                            .query_row(
                                "SELECT outcome IN ('succeeded','superseded') FROM replication_debt WHERE id=?1 AND repo_key=?2",
                                params![id,repo_key],
                                |row| row.get::<_, bool>(0),
                            )
                            .optional()?
                            .unwrap_or(false),
                    };
                    if !complete {
                        anyhow::bail!("captured draining obligation is not terminal and clean");
                    }
                }
                let changed = transaction.execute(
                    "UPDATE repository_policies SET revision=revision+1,operation_state_json='{\"state\":\"disabled\"}',updated_at=?1 WHERE repo_key=?2 AND revision=?3",
                    params![now(),repo_key,revision],
                )?;
                if changed != 1 {
                    anyhow::bail!("repository policy changed during disable transition");
                }
                Ok(())
            })?;
            self.repository(repo_key)
        }

        pub(crate) fn registered_remote_identity(
            &self,
            repo_key: &str,
        ) -> Result<Option<(PathBuf, String, RegisteredRemote)>> {
            let conn = self.connect_read_only()?;
            conn.query_row("SELECT repository.owned_root_path,policy.target_branch FROM registered_repositories repository JOIN repository_policies policy ON policy.repo_key=repository.repo_key WHERE repository.repo_key=?1", params![repo_key], |row| {
                Ok((
                    row_path(row, "owned_root_path")?,
                    row.get("target_branch")?,
                    RegisteredRemote {
                        name: crate::repository::INTERNAL_REMOTE_NAME.into(),
                    },
                ))
            })
            .optional()
            .map_err(Into::into)
        }

        pub(crate) fn update_checkout_reconciliation(
            &self,
            repo_key: &str,
            owner_id: &str,
            state: &CheckoutReconciliationState,
        ) -> Result<()> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let changed = match state {
                    CheckoutReconciliationState::Ready(_) => tx.execute(
                        "UPDATE registered_repositories SET source_sha=?1,checkout_json=?2,updated_at=?3 WHERE repo_key=?4",
                        params![state.target_sha(),serde_json::to_string(state)?,now(),repo_key],
                    )?,
                    CheckoutReconciliationState::Pending(_)
                    | CheckoutReconciliationState::Failed(_) => tx.execute(
                        "UPDATE registered_repositories SET checkout_json=?1,updated_at=?2 WHERE repo_key=?3",
                        params![serde_json::to_string(state)?,now(),repo_key],
                    )?,
                };
                if changed != 1 { anyhow::bail!("registered repository disappeared"); }
                if matches!(state, CheckoutReconciliationState::Ready(_)) {
                    let ref_name = format!(
                        "refs/iq/repository-targets/{repo_key}/{}",
                        state.target_sha()
                    );
                    let inserted = tx.execute(
                        "INSERT INTO private_ref_cleanup_debt(repo_key,kind,owner_id,ref_name,expected_sha,created_at,updated_at) VALUES(?1,'repository_target',?2,?3,?2,?4,?4) ON CONFLICT(repo_key,ref_name) DO NOTHING",
                        params![repo_key,state.target_sha(),ref_name,now()],
                    )?;
                    if inserted == 0 {
                        let stored: (String,String,String) = tx.query_row(
                            "SELECT kind,owner_id,expected_sha FROM private_ref_cleanup_debt WHERE repo_key=?1 AND ref_name=?2",
                            params![repo_key,ref_name],
                            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
                        )?;
                        if stored != ("repository_target".into(),state.target_sha().into(),state.target_sha().into()) {
                            anyhow::bail!("repository-target cleanup debt differs from checkout authority");
                        }
                    }
                }
                Ok(())
            })
        }

        pub(crate) fn record_provider_landing_guarantee(
            &self,
            repo_key: &str,
            owner_id: &str,
            evidence: &ProviderLandingEvidence<'_>,
        ) -> Result<()> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                let changed = transaction.execute(
                    "INSERT INTO provider_landing_guarantees(item_id,provider,provider_host,provider_repository,provider_repository_id,merge_request_identity,admitted_base_sha,admitted_head_sha,validated_target_sha,validated_candidate_sha,validated_tree_sha,landed_commit_sha,landed_tree_sha,first_parent_sha,history_contract,contains_admitted_head,verified_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17) ON CONFLICT(item_id) DO NOTHING",
                    params![evidence.item_id,evidence.provider.to_string(),evidence.provider_host,evidence.provider_repository,evidence.provider_repository_id,evidence.merge_request_identity,evidence.admitted_base_sha,evidence.admitted_head_sha,evidence.validated_target_sha,evidence.validated_candidate_sha,evidence.validated_tree_sha,evidence.landed_commit_sha,evidence.landed_tree_sha,evidence.first_parent_sha,evidence.history_contract,evidence.contains_admitted_head,now()],
                )?;
                if changed == 0 {
                    let stored: Vec<String> = transaction.query_row(
                        "SELECT provider,provider_host,provider_repository,provider_repository_id,merge_request_identity,admitted_base_sha,admitted_head_sha,validated_target_sha,validated_candidate_sha,validated_tree_sha,landed_commit_sha,landed_tree_sha,first_parent_sha,history_contract,CAST(contains_admitted_head AS TEXT) FROM provider_landing_guarantees WHERE item_id=?1",
                        [evidence.item_id],
                        |row| (0..15).map(|column| row.get(column)).collect(),
                    )?;
                    let expected = vec![
                        evidence.provider.to_string(),evidence.provider_host.to_string(),evidence.provider_repository.to_string(),evidence.provider_repository_id.to_string(),evidence.merge_request_identity.to_string(),evidence.admitted_base_sha.to_string(),evidence.admitted_head_sha.to_string(),evidence.validated_target_sha.to_string(),evidence.validated_candidate_sha.to_string(),evidence.validated_tree_sha.to_string(),evidence.landed_commit_sha.to_string(),evidence.landed_tree_sha.to_string(),evidence.first_parent_sha.to_string(),evidence.history_contract.to_string(),i64::from(evidence.contains_admitted_head).to_string(),
                    ];
                    if stored != expected {
                        anyhow::bail!("provider landing guarantee differs from durable evidence");
                    }
                }
                Ok(())
            })
        }

        pub(crate) fn private_ref_cleanup_debts(
            &self,
            repo_key: &str,
        ) -> Result<Vec<PrivateRefCleanupDebt>> {
            let connection = self.connect_read_only()?;
            let mut statement = connection.prepare(
                "SELECT repo_key,kind,owner_id,ref_name,expected_sha FROM private_ref_cleanup_debt WHERE repo_key=?1 ORDER BY created_at,ref_name",
            )?;
            let debts = statement
                .query_map([repo_key], |row| {
                    let kind: String = row.get(1)?;
                    Ok(PrivateRefCleanupDebt {
                        repo_key: row.get(0)?,
                        kind: PrivateRefKind::parse(&kind)
                            .map_err(|error| map_parse_error(error.to_string()))?,
                        owner_id: row.get(2)?,
                        ref_name: row.get(3)?,
                        expected_sha: row.get(4)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(anyhow::Error::from)?;
            Ok(debts)
        }

        pub(crate) fn private_ref_authority(
            &self,
            repo_key: &str,
            kind: PrivateRefKind,
            ref_owner_id: &str,
            observed_sha: &str,
        ) -> Result<(bool, String)> {
            let connection = self.connect_read_only()?;
            let object_format = repository_object_format(&connection, repo_key)?;
            object_format.require_oid(observed_sha, "private ref object ID")?;
            match kind {
                PrivateRefKind::RepositoryTarget => {
                    object_format.require_oid(ref_owner_id, "repository-target owner")?;
                    if ref_owner_id != observed_sha {
                        anyhow::bail!("repository-target ref drifted from its encoded object ID");
                    }
                    let checkout: CheckoutReconciliationState =
                        serde_json::from_str(&connection.query_row(
                            "SELECT checkout_json FROM registered_repositories WHERE repo_key=?1",
                            [repo_key],
                            |row| row.get::<_, String>(0),
                        )?)?;
                    let required = !matches!(checkout, CheckoutReconciliationState::Ready(_))
                        && checkout.target_sha() == ref_owner_id;
                    Ok((required, ref_owner_id.to_string()))
                }
                PrivateRefKind::Landing => {
                    let cleanup_authority = connection
                        .query_row(
                            "SELECT expected_sha FROM private_ref_cleanup_debt WHERE repo_key=?1 AND kind='landing' AND owner_id=?2 AND ref_name=?3",
                            params![repo_key,ref_owner_id,format!("refs/iq/landings/{ref_owner_id}")],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?;
                    if let Some(expected) = cleanup_authority.as_deref() {
                        object_format.require_oid(expected, "landing cleanup candidate")?;
                        if expected != observed_sha {
                            anyhow::bail!("landing ref drifted from cleanup debt authority");
                        }
                    }
                    let authority = connection
                        .query_row(
                            "SELECT item.status,item.landing_state_json,attempt.merge_commit_sha,effort.state_json,EXISTS(SELECT 1 FROM replication_debt debt WHERE debt.item_id=item.id AND debt.operation='pin_source' AND debt.outcome NOT IN ('succeeded','superseded')) FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id LEFT JOIN integration_efforts effort ON effort.item_id=item.id WHERE attempt.id=?1 AND item.repo_key=?2",
                            params![ref_owner_id,repo_key],
                            |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, Option<String>>(2)?,row.get::<_, Option<String>>(3)?,row.get::<_, bool>(4)?)),
                        )
                        .optional()?;
                    let Some((
                        status,
                        landing_state,
                        attempt_candidate,
                        effort_state,
                        replication_pin_pending,
                    )) = authority
                    else {
                        return Ok((
                            false,
                            cleanup_authority.unwrap_or_else(|| observed_sha.to_string()),
                        ));
                    };
                    let landing_state: LandingState = serde_json::from_str(&landing_state)?;
                    let state = effort_state
                        .as_deref()
                        .map(serde_json::from_str::<crate::control_domain::IntegrationEffortState>)
                        .transpose()?;
                    let expected = state
                        .as_ref()
                        .and_then(crate::control_domain::IntegrationEffortState::candidate_sha)
                        .map(str::to_string)
                        .or(attempt_candidate)
                        .context("landing ref has no durable candidate authority")?;
                    object_format.require_oid(&expected, "landing ref candidate")?;
                    if cleanup_authority
                        .as_deref()
                        .is_some_and(|cleanup_expected| cleanup_expected != expected)
                    {
                        anyhow::bail!("landing cleanup debt differs from current authority");
                    }
                    let required = if cleanup_authority.is_some() {
                        landing_state.is_uncertain() || replication_pin_pending
                    } else {
                        !matches!(status.as_str(), "integrated" | "cancelled")
                            || landing_state.is_uncertain()
                            || replication_pin_pending
                    };
                    Ok((required, expected))
                }
            }
        }

        pub(crate) fn schedule_private_ref_cleanup(
            &self,
            repo_key: &str,
            lease_owner_id: &str,
            kind: PrivateRefKind,
            ref_owner_id: &str,
            ref_name: &str,
            expected_sha: &str,
        ) -> Result<()> {
            self.composition_transaction(repo_key, lease_owner_id, |transaction| {
                repository_object_format(transaction, repo_key)?
                    .require_oid(expected_sha, "private-ref cleanup object ID")?;
                let expected_ref = match kind {
                    PrivateRefKind::RepositoryTarget => {
                        format!("refs/iq/repository-targets/{repo_key}/{expected_sha}")
                    }
                    PrivateRefKind::Landing => format!("refs/iq/landings/{ref_owner_id}"),
                };
                if ref_name != expected_ref {
                    anyhow::bail!("private-ref cleanup identity is not canonical");
                }
                let inserted = transaction.execute(
                    "INSERT INTO private_ref_cleanup_debt(repo_key,kind,owner_id,ref_name,expected_sha,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?6) ON CONFLICT(repo_key,ref_name) DO NOTHING",
                    params![repo_key,kind.as_str(),ref_owner_id,ref_name,expected_sha,now()],
                )?;
                if inserted == 0 {
                    let stored: (String,String,String) = transaction.query_row(
                        "SELECT kind,owner_id,expected_sha FROM private_ref_cleanup_debt WHERE repo_key=?1 AND ref_name=?2",
                        params![repo_key,ref_name],
                        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
                    )?;
                    if stored != (kind.as_str().to_string(),ref_owner_id.to_string(),expected_sha.to_string()) {
                        anyhow::bail!("private-ref cleanup debt differs from exact authority");
                    }
                }
                Ok(())
            })
        }

        pub(crate) fn complete_private_ref_cleanup(
            &self,
            repo_key: &str,
            lease_owner_id: &str,
            debt: &PrivateRefCleanupDebt,
        ) -> Result<()> {
            self.composition_transaction(repo_key, lease_owner_id, |transaction| {
                let changed = transaction.execute(
                    "DELETE FROM private_ref_cleanup_debt WHERE repo_key=?1 AND kind=?2 AND owner_id=?3 AND ref_name=?4 AND expected_sha=?5",
                    params![repo_key,debt.kind.as_str(),debt.owner_id,debt.ref_name,debt.expected_sha],
                )?;
                if changed != 1 {
                    anyhow::bail!("private-ref cleanup debt authority changed during finalization");
                }
                Ok(())
            })
        }

        pub(crate) fn finish_replication_debt(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
            result: std::result::Result<(), &str>,
        ) -> Result<ReplicationDebt> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let (outcome, failure) = match result {
                    Ok(()) => ("applied", None),
                    Err(message) if !message.trim().is_empty() => ("failed", Some(message)),
                    Err(_) => anyhow::bail!("replication failure must not be empty"),
                };
                let changed = transaction.execute(
                    "UPDATE replication_debt SET outcome=?1,application_id=NULL,failure=?2,updated_at=?3 WHERE id=?4 AND repo_key=?5 AND outcome IN ('pinning','pending','applying','uncertain','failed')",
                    params![outcome,failure,now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("replication debt authority changed");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        pub(crate) fn complete_replication_source_pin(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
        ) -> Result<ReplicationDebt> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let changed = transaction.execute(
                    "UPDATE replication_debt SET operation='resolve_destination',outcome='pending',failure=NULL,updated_at=?1 WHERE id=?2 AND repo_key=?3 AND operation='pin_source' AND outcome IN ('pinning','failed')",
                    params![now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("replication source pin authority changed");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        pub(crate) fn complete_replication_source_cleanup(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
        ) -> Result<ReplicationDebt> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let changed = transaction.execute(
                    "UPDATE replication_debt SET outcome='succeeded',updated_at=?1 WHERE id=?2 AND repo_key=?3 AND outcome='applied'",
                    params![now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("replication source cleanup authority changed");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        pub(crate) fn begin_replication_application(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
            expected_destination_sha: &str,
        ) -> Result<ReplicationDebt> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                repository_object_format(transaction, repo_key)?.require_oid(
                    expected_destination_sha,
                    "replication expected destination SHA",
                )?;
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let application_id = Uuid::new_v4().to_string();
                let changed = transaction.execute(
                    "UPDATE replication_debt SET expected_destination_sha=?1,operation='advance_exact_target',outcome='applying',application_id=?2,failure=NULL,updated_at=?3 WHERE id=?4 AND repo_key=?5 AND outcome IN ('pending','failed')",
                    params![expected_destination_sha,application_id,now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("replication debt cannot begin an application");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        pub(crate) fn resume_replication_application(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
        ) -> Result<ReplicationDebt> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let changed = transaction.execute(
                    "UPDATE replication_debt SET outcome='applying',failure=NULL,updated_at=?1 WHERE id=?2 AND repo_key=?3 AND outcome='uncertain' AND application_id IS NOT NULL AND expected_destination_sha IS NOT NULL",
                    params![now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("uncertain replication application cannot resume");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        pub(crate) fn mark_replication_uncertain(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
            failure: &str,
        ) -> Result<ReplicationDebt> {
            if failure.trim().is_empty() {
                anyhow::bail!("replication uncertainty evidence must not be empty");
            }
            self.composition_transaction(repo_key, owner_id, |transaction| {
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let changed = transaction.execute(
                    "UPDATE replication_debt SET outcome='uncertain',failure=?1,updated_at=?2 WHERE id=?3 AND repo_key=?4 AND outcome IN ('applying','uncertain') AND application_id IS NOT NULL",
                    params![failure,now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("replication application is not in flight");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        fn require_replication_tx(
            transaction: &rusqlite::Transaction<'_>,
            repo_key: &str,
            debt_id: &str,
        ) -> Result<()> {
            Self::validate_replication_binding(transaction, repo_key, debt_id, None)?;
            let (state, item_id): (String, String) = transaction.query_row(
                "SELECT policy.operation_state_json,debt.item_id FROM replication_debt debt JOIN repository_policies policy ON policy.repo_key=debt.repo_key WHERE debt.id=?1 AND debt.repo_key=?2",
                params![debt_id,repo_key],
                |row| Ok((row.get(0)?,row.get(1)?)),
            )?;
            let state: crate::repository_policy::OperationState = serde_json::from_str(&state)?;
            match &state {
                crate::repository_policy::OperationState::Draining { obligations }
                    if obligations.contains(&crate::repository_policy::Obligation::QueueItem {
                        id: item_id,
                    }) =>
                {
                    Ok(())
                }
                _ => state.require_obligation(&crate::repository_policy::Obligation::Replication {
                    id: debt_id.to_string(),
                }),
            }
        }

        pub fn replication_debts(&self, repo_key: Option<&str>) -> Result<Vec<ReplicationDebt>> {
            let connection = self.connect_read_only()?;
            let mut statement = if repo_key.is_some() {
                connection.prepare("SELECT id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,expected_destination_sha,operation,outcome,application_id,failure,superseded_by_id FROM replication_debt WHERE repo_key=?1 ORDER BY destination_key,target_branch,sequence")?
            } else {
                connection.prepare("SELECT id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,expected_destination_sha,operation,outcome,application_id,failure,superseded_by_id FROM replication_debt ORDER BY destination_key,target_branch,sequence")?
            };
            let map = |row: &Row<'_>| map_replication_debt(row);
            if let Some(repo_key) = repo_key {
                statement
                    .query_map([repo_key], map)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            } else {
                statement
                    .query_map([], map)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
        }

        pub fn replication_debt(&self, debt_id: &str) -> Result<ReplicationDebt> {
            let connection = self.connect_read_only()?;
            required_row(
                connection.query_row(
                    "SELECT id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,expected_destination_sha,operation,outcome,application_id,failure,superseded_by_id FROM replication_debt WHERE id=?1",
                    [debt_id],
                    map_replication_debt,
                ),
                "replication debt",
                debt_id,
            )
        }

        pub(crate) fn older_unfinished_replication(
            &self,
            debt: &ReplicationDebt,
        ) -> Result<Option<ReplicationDebt>> {
            let connection = self.connect_read_only()?;
            connection
                .query_row(
                    "SELECT id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,expected_destination_sha,operation,outcome,application_id,failure,superseded_by_id FROM replication_debt WHERE destination_key=?1 AND target_branch=?2 AND sequence<?3 AND outcome NOT IN ('succeeded','superseded') ORDER BY sequence LIMIT 1",
                    params![debt.destination_key,debt.target_branch,debt.sequence],
                    map_replication_debt,
                )
                .optional()
                .map_err(Into::into)
        }

        pub(crate) fn newer_completed_replication(
            &self,
            debt: &ReplicationDebt,
        ) -> Result<Option<ReplicationDebt>> {
            let connection = self.connect_read_only()?;
            connection
                .query_row(
                    "SELECT id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,expected_destination_sha,operation,outcome,application_id,failure,superseded_by_id FROM replication_debt WHERE destination_key=?1 AND target_branch=?2 AND sequence>?3 AND outcome IN ('succeeded','superseded') ORDER BY sequence DESC LIMIT 1",
                    params![debt.destination_key,debt.target_branch,debt.sequence],
                    map_replication_debt,
                )
                .optional()
                .map_err(Into::into)
        }

        pub(crate) fn begin_replication_supersession(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
            newer_id: &str,
        ) -> Result<ReplicationDebt> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let changed = transaction.execute(
                    "UPDATE replication_debt AS older SET outcome='superseded_cleanup_pending',application_id=NULL,failure=NULL,superseded_by_id=?1,updated_at=?2 WHERE older.id=?3 AND older.repo_key=?4 AND older.outcome NOT IN ('succeeded','superseded','superseded_cleanup_pending') AND EXISTS(SELECT 1 FROM replication_debt newer WHERE newer.id=?1 AND newer.destination_key=older.destination_key AND newer.target_branch=older.target_branch AND newer.sequence>older.sequence AND newer.outcome IN ('succeeded','superseded'))",
                    params![newer_id,now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("replication debt cannot begin supersession cleanup");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        pub(crate) fn complete_replication_supersession(
            &self,
            repo_key: &str,
            owner_id: &str,
            debt_id: &str,
        ) -> Result<ReplicationDebt> {
            self.composition_transaction(repo_key, owner_id, |transaction| {
                Self::require_replication_tx(transaction, repo_key, debt_id)?;
                let changed = transaction.execute(
                    "UPDATE replication_debt SET outcome='superseded',updated_at=?1 WHERE id=?2 AND repo_key=?3 AND outcome='superseded_cleanup_pending' AND superseded_by_id IS NOT NULL",
                    params![now(),debt_id,repo_key],
                )?;
                if changed != 1 {
                    anyhow::bail!("replication supersession cleanup authority changed");
                }
                Ok(())
            })?;
            self.replication_debt(debt_id)
        }

        pub(crate) fn begin_initial_target_fetch(
            &self,
            repo_key: &str,
            owner_id: &str,
            item_id: &str,
            attempt_id: &str,
            target_sha: &str,
        ) -> Result<()> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                repository_object_format(tx, repo_key)?
                    .require_oid(target_sha, "observed target SHA")?;
                let attempt_changed = tx.execute(
                    "UPDATE integration_attempts SET target_base_sha=?1 WHERE id=?2 AND item_id=?3 AND target_base_sha IS NULL",
                    params![target_sha,attempt_id,item_id],
                )?;
                if attempt_changed == 0 {
                    let existing: Option<String> = tx
                        .query_row(
                            "SELECT target_base_sha FROM integration_attempts WHERE id=?1 AND item_id=?2",
                            params![attempt_id,item_id],
                            |row| row.get(0),
                        )
                        .optional()?
                        .flatten();
                    if existing.as_deref() != Some(target_sha) {
                        anyhow::bail!("integration attempt target authority changed before fetch");
                    }
                }
                let checkout = CheckoutReconciliationState::pending(
                    target_sha,
                    repository_object_format(tx, repo_key)?,
                )?;
                let repository_changed = tx.execute(
                    "UPDATE registered_repositories SET checkout_json=?1,updated_at=?2 WHERE repo_key=?3",
                    params![serde_json::to_string(&checkout)?,now(),repo_key],
                )?;
                if repository_changed != 1 {
                    anyhow::bail!("registered repository disappeared before target fetch");
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

        pub(crate) fn save_development_workspace(
            &self,
            owner_id: &str,
            workspace: &DevelopmentWorkspace,
        ) -> Result<()> {
            self.composition_transaction(&workspace.repo_key, owner_id, |tx| {
                Self::require_new_work_tx(tx, &workspace.repo_key)?;
                repository_object_format(tx, &workspace.repo_key)?
                    .require_oid(&workspace.base_sha, "development workspace base")?;
                tx.execute(
                    "INSERT INTO development_workspaces (id,repo_key,name,path,rift_id,source_rift_id,branch,base_sha,status,cleanup_json,created_at,updated_at) VALUES (?1,?2,?3,?4,NULL,NULL,?5,?6,?7,?8,?9,?9)",
                    params![workspace.id,workspace.repo_key,workspace.name,path_bytes(&workspace.path),workspace.branch,workspace.base_sha,workspace.status.to_string(),serde_json::to_string(&workspace.cleanup)?,workspace.created_at],
                )?;
                Ok(())
            })
        }

        pub(crate) fn set_development_workspace_identity(
            &self,
            repo_key: &str,
            owner_id: &str,
            id: &str,
            identity: &WorkspaceIdentity,
        ) -> Result<DevelopmentWorkspace> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                Self::require_obligation_tx(
                    tx,
                    repo_key,
                    &crate::repository_policy::Obligation::Workspace { id: id.to_string() },
                )?;
                let changed = tx.execute(
                    "UPDATE development_workspaces SET rift_id=?1,source_rift_id=?2,status='active',updated_at=?3 WHERE id=?4 AND repo_key=?5 AND status='creating' AND path=?6",
                    params![identity.rift_id,identity.source_rift_id,now(),id,repo_key,path_bytes(Path::new(&identity.path))],
                )?;
                if changed != 1 { anyhow::bail!("development workspace creation intent changed"); }
                insert_workspace_git_binding(
                    tx,
                    repo_key,
                    "development",
                    id,
                    Path::new(&identity.path),
                )?;
                Ok(())
            })?;
            self.workspace(id)
        }

        pub(crate) fn update_development_workspace_cleanup(
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
                if status == DevelopmentWorkspaceStatus::Removed {
                    tx.execute(
                        "DELETE FROM workspace_git_bindings WHERE owner_kind='development' AND owner_id=?1",
                        [id],
                    )?;
                }
                Ok(())
            })?;
            self.workspace(id)
        }

        pub(crate) fn complete_development_workspace_cleanup(
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
                    "DELETE FROM workspace_git_bindings WHERE owner_kind='development' AND owner_id=?1",
                    [id],
                )?;
                tx.execute(
                    "DELETE FROM workspace_gc_debt WHERE registry_identity=?1",
                    params![registry_identity],
                )?;
                Ok(())
            })?;
            self.workspace(id)
        }

        pub(crate) fn begin_local_submission(
            &self,
            repo_key: &str,
            owner_id: &str,
            workspace_id: &str,
            commit_sha: &str,
            replaces_item_id: Option<&str>,
        ) -> Result<LocalSubmission> {
            let mut submission_id = None;
            self.composition_transaction(repo_key, owner_id, |tx| {
                let object_format = repository_object_format(tx, repo_key)?;
                object_format.require_oid(commit_sha, "local submission commit")?;
                Self::require_obligation_tx(
                    tx,
                    repo_key,
                    &crate::repository_policy::Obligation::Workspace {
                        id: workspace_id.to_string(),
                    },
                )?;
                let existing = tx
                    .query_row(
                        &format!(
                            "{LOCAL_SUBMISSION_SELECT} WHERE submission.workspace_id=?1 AND submission.state='creating'"
                        ),
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
                Self::require_new_work_tx(tx, repo_key)?;
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
                object_format.require_oid(&workspace.base_sha, "local submission base")?;
                if let Some(item_id) = replaces_item_id {
                    let item = required_row(
                        tx.query_row(
                            "SELECT * FROM queue_items_runtime WHERE id=?1",
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
                let item_id = Uuid::new_v4().to_string();
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

        pub(crate) fn finalize_local_submission(
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
                        &format!("{LOCAL_SUBMISSION_SELECT} WHERE submission.id=?1"),
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
                Self::require_obligation_tx(
                    tx,
                    repo_key,
                    &crate::repository_policy::Obligation::Workspace {
                        id: submission.workspace_id.clone(),
                    },
                )?;
                let _ = required_row(
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
                if let Some(replaced_item_id) = submission.replaces_item_id.as_deref() {
                    let replaced_status: String = tx.query_row(
                        "SELECT status FROM queue_items WHERE id=?1 AND repo_key=?2",
                        params![replaced_item_id, repo_key],
                        |row| row.get(0),
                    )?;
                    if replaced_status != "cancelled" {
                        anyhow::bail!("superseded queue item is not cancelled");
                    }
                    tx.execute(
                        "UPDATE local_submissions SET state='replaced' WHERE queue_item_id=?1 AND state='cancelled'",
                        [replaced_item_id],
                    )?;
                }
                tx.execute(
                    "INSERT INTO queue_items (id,repo_key,producer_metadata_json,validation_evidence_json,status,created_at,updated_at) VALUES (?1,?2,?3,'{}','ready',?4,?4)",
                    params![submission.queue_item_id,repo_key,producer_metadata.to_string(),timestamp],
                )?;
                tx.execute(
                    "INSERT INTO queue_admissions(item_id,kind,source_branch,head_sha,source_ref,submission_id,admitted_at) VALUES(?1,'local_submission',?2,?3,?2,?4,?5)",
                    params![submission.queue_item_id,submission.private_ref,submission.commit_sha,submission.id,timestamp],
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

        pub(crate) fn finish_replacement_cleanup(
            &self,
            repo_key: &str,
            owner_id: &str,
            item_id: &str,
            old_attempt_id: &str,
        ) -> Result<QueueItem> {
            self.composition_transaction(repo_key, owner_id, |tx| {
                let item = required_row(tx.query_row("SELECT * FROM queue_items_runtime WHERE id=?1",params![item_id],map_item),"queue item",item_id)?;
                let ReplacementState::CleanupPending { old_attempt_id: expected, .. } = &item.replacement else {
                    anyhow::bail!("queue item has no replacement cleanup debt");
                };
                if expected != old_attempt_id { anyhow::bail!("replacement cleanup attempt identity changed"); }
                if item.status == QueueStatus::Blocked {
                    let changed = tx.execute("UPDATE integration_attempts SET result='superseded',finished_at=?1 WHERE id=?2 AND item_id=?3 AND finished_at IS NULL AND result IS NULL",params![now(),old_attempt_id,item_id])?;
                    if changed == 0 {
                        let (finished_at, result) = tx
                            .query_row(
                                "SELECT finished_at,result FROM integration_attempts WHERE id=?1 AND item_id=?2",
                                params![old_attempt_id,item_id],
                                |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<String>>(1)?)),
                            )
                            .optional()?
                            .context("old integration attempt disappeared after cleanup")?;
                        if finished_at.is_none()
                            || !matches!(result.as_deref(), Some("cancelled" | "superseded"))
                        {
                            anyhow::bail!("old integration attempt terminal state is incompatible with replacement cleanup");
                        }
                    }
                    let effort_pending = crate::control_store::ControlStore::mark_effort_replacement_pending(
                        tx,
                        item_id,
                        old_attempt_id,
                    )?;
                    let expected_status = if effort_pending { "ready" } else { "blocked" };
                    let changed = tx.execute(
                        "UPDATE queue_items SET status='ready',current_attempt_id=NULL,integration_workspace_path=NULL,integration_workspace_rift_id=NULL,integration_workspace_source_rift_id=NULL,replacement_json=NULL,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?1 WHERE id=?2 AND repo_key=?3 AND status=?4",
                        params![now(),item_id,repo_key,expected_status],
                    )?;
                    if changed != 1 { anyhow::bail!("replacement cleanup state changed concurrently"); }
                } else {
                    let (finished_at, result) = tx
                        .query_row(
                            "SELECT finished_at,result FROM integration_attempts WHERE id=?1 AND item_id=?2",
                            params![old_attempt_id,item_id],
                            |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<String>>(1)?)),
                        )
                        .optional()?
                        .context("old integration attempt disappeared after cleanup")?;
                    let valid_terminal = finished_at.is_some()
                        && matches!(
                            (item.status, result.as_deref()),
                            (QueueStatus::Integrated, Some("integrated"))
                                | (QueueStatus::Cancelled, Some("cancelled"))
                        );
                    if !valid_terminal {
                        anyhow::bail!("old integration attempt terminal state is incompatible with replacement cleanup");
                    }
                }
                if item.status != QueueStatus::Blocked {
                    let changed = tx.execute(
                        "UPDATE queue_items SET replacement_json=NULL,updated_at=?1 WHERE id=?2 AND repo_key=?3 AND status=?4 AND current_attempt_id=?5",
                        params![now(),item_id,repo_key,item.status.to_string(),old_attempt_id],
                    )?;
                    if changed != 1 { anyhow::bail!("replacement cleanup state changed concurrently"); }
                }
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
                    &format!("{LOCAL_SUBMISSION_SELECT} WHERE submission.id=?1"),
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
                &format!(
                    "{LOCAL_SUBMISSION_SELECT} WHERE submission.workspace_id=?1 AND submission.state='creating'"
                ),
                params![workspace_id],
                map_local_submission,
            )
            .optional()
            .map_err(Into::into)
        }

        pub fn integrated_submission_sha(&self, workspace_id: &str) -> Result<Option<String>> {
            let conn = self.connect_read_only()?;
            conn.query_row(
                "SELECT submission.commit_sha,policy.canonical_repository_json FROM local_submissions submission JOIN queue_admissions admission ON admission.submission_id=submission.id JOIN queue_items item ON item.id=admission.item_id JOIN repository_policies policy ON policy.repo_key=submission.repo_key WHERE submission.workspace_id=?1 AND item.status='integrated' ORDER BY submission.created_at DESC LIMIT 1",
                params![workspace_id],
                |row| {
                    let commit_sha: String = row.get(0)?;
                    let repository: String = row.get(1)?;
                    let object_format = serde_json::from_str::<
                        crate::repository_policy::GitRepository,
                    >(&repository)
                    .map_err(|error| map_json_error("canonical_repository_json", error))?
                    .object_format();
                    object_format
                        .require_oid(&commit_sha, "integrated local submission commit")
                        .map_err(|error| map_parse_error(format!("{error:#}")))?;
                    Ok(commit_sha)
                },
            )
            .optional()
            .map_err(Into::into)
        }

        pub(crate) fn set_conflict_metadata(
            &self,
            item_id: &str,
            attempt_id: &str,
            conflict_json: &Value,
            target_sha: &str,
            source_sha: &str,
        ) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE queue_items SET conflict_json=?1,target_sha=?2,source_sha=?3,updated_at=?4 WHERE id=?5 AND status='merging' AND current_attempt_id=?6",
                params![conflict_json.to_string(), target_sha, source_sha, now(), item_id, attempt_id],
            )?;
            if changed != 1 {
                let item = required_row(
                    tx.query_row(
                        "SELECT * FROM queue_items_runtime WHERE id=?1",
                        [item_id],
                        map_item,
                    ),
                    "queue item",
                    item_id,
                )?;
                if item.status == QueueStatus::Cancelled {
                    tx.commit()?;
                    return Ok(item);
                }
                anyhow::bail!("conflict metadata lost exact merging attempt authority");
            }
            tx.commit()?;
            self.get_item(item_id)
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
            if (original_metadata.dev(), original_metadata.ino())
                != (metadata.dev(), metadata.ino())
            {
                anyhow::bail!(
                    "queue database identity changed while resolving: {}",
                    path.display()
                );
            }
            let lease = crate::control_store::DatabaseProcessLease::acquire_existing(&path)?;
            let source = crate::control_store::PrimaryDatabaseIdentity::open(&path)?;
            let current_metadata = fs::symlink_metadata(&path)?;
            if (metadata.dev(), metadata.ino()) != (current_metadata.dev(), current_metadata.ino())
            {
                anyhow::bail!(
                    "queue database identity changed before validation: {}",
                    path.display()
                );
            }
            let validated_database_id =
                crate::control_store::validate_database_snapshot_under_lease(
                    &path,
                    &lease,
                    |conn| {
                        let metadata_exists: bool = conn.query_row(
                            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='queue_metadata')",
                            [],
                            |row| row.get(0),
                        )?;
                        if !metadata_exists {
                            return incompatible_local_state();
                        }
                        let workspace_schema_version: Option<String> = conn
                            .query_row(
                                "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
                                [],
                                |row| row.get(0),
                            )
                            .optional()?;
                        require_current_schema_version(workspace_schema_version.as_deref())?;
                        validate_existing_schema_identity(conn)
                    },
                )?;
            let lease = lease.stabilize(&path)?;
            crate::control_store::run_runtime_open_handoff_test_hook(&path);
            lease.verify_authority(&path)?;
            source.verify_authoritative(&path)?;
            let conn = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )
            .with_context(|| format!("open existing queue db {}", path.display()))?;
            configure_connection(&conn)?;
            conn.busy_timeout(Self::BUSY_TIMEOUT)?;
            conn.pragma_update(None, "query_only", "ON")?;
            source.verify_authoritative(&path)?;
            let authoritative_database_id = validate_existing_schema_identity(&conn)?;
            if authoritative_database_id != validated_database_id {
                anyhow::bail!("queue database identity changed after snapshot validation");
            }
            lease.verify_authority(&path)?;
            source.verify_authoritative(&path)?;
            let final_metadata = fs::symlink_metadata(&path)?;
            if final_metadata.file_type().is_symlink()
                || !final_metadata.is_file()
                || (final_metadata.dev(), final_metadata.ino()) != (metadata.dev(), metadata.ino())
            {
                anyhow::bail!(
                    "queue database identity changed while opening: {}",
                    path.display()
                );
            }
            let authority =
                crate::control_store::ValidatedDatabaseAuthority::from_validated_connection(
                    path,
                    final_metadata.dev(),
                    final_metadata.ino(),
                    authoritative_database_id,
                    &conn,
                )?;
            drop(conn);
            drop(source);
            drop(lease);
            Ok(Self { authority })
        }

        fn connect(&self, timeout: std::time::Duration) -> Result<Connection> {
            let conn = self.authority.open_connection(
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            conn.busy_timeout(timeout)?;
            conn.pragma_update(None, "query_only", "ON")?;
            self.authority.verify_configured_connection(&conn)?;
            Ok(conn)
        }

        pub fn list_items(&self) -> Result<Vec<QueueItem>> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            let mut stmt =
                conn.prepare("SELECT * FROM queue_items_runtime ORDER BY created_at ASC")?;
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
            self.authority.path()
        }

        pub fn validated_control_store(&self) -> Result<crate::control_store::ControlStore> {
            crate::control_store::ControlStore::open_validated(self.authority.clone())
        }

        pub fn verify_workspace_root_path(
            &self,
            repo_key: &str,
            workspace_root: &Path,
        ) -> Result<()> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            let existing: Option<String> = conn
                .query_row(
                    "SELECT CAST(root_path AS TEXT) FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
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
            match self.workspace_root_generation_state(repo_key)? {
                WorkspaceGenerationState::Ready { current } => Ok(current),
                WorkspaceGenerationState::Pending { .. } => anyhow::bail!(
                    "repository queue {repo_key} integration workspace generation is pending reconciliation"
                ),
            }
        }

        pub(crate) fn workspace_root_generation_state(
            &self,
            repo_key: &str,
        ) -> Result<WorkspaceGenerationState> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            let state = conn.query_row(
                "SELECT generation,pending_generation FROM workspace_roots WHERE repo_key=?1 AND kind='integration'",
                params![repo_key],
                |row| Ok((row.get::<_, i64>(0)?,row.get::<_, Option<i64>>(1)?)),
            )
            .optional()?
            .unwrap_or((0,None));
            match state {
                (current, None) if current >= 0 => Ok(WorkspaceGenerationState::Ready { current }),
                (current, Some(pending)) if current >= 0 && pending == current + 1 => {
                    Ok(WorkspaceGenerationState::Pending { current, pending })
                }
                _ => anyhow::bail!("integration workspace generation authority is invalid"),
            }
        }

        pub fn get_item(&self, item_id: &str) -> Result<QueueItem> {
            let conn = self.connect(Self::BUSY_TIMEOUT)?;
            required_row(
                conn.query_row(
                    "SELECT * FROM queue_items_runtime WHERE id=?1",
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
            let physical_invalid: bool = conn.query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM physical_repository_ownership ownership
                   LEFT JOIN physical_repository_leases lease ON lease.identity_key=ownership.identity_key
                   WHERE ownership.repo_key=?1 AND (lease.owner_id IS NOT ?2 OR lease.expires_at<=?3)
                 )",
                params![repo_key, owner_id, (Utc::now() + Self::COMMAND_AUTHORITY_RESERVE).to_rfc3339()],
                |row| row.get(0),
            )?;
            if physical_invalid {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} physical lease is not owned by {owner_id}"
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
            let physical_invalid: bool = conn.query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM physical_repository_ownership ownership
                   LEFT JOIN physical_repository_leases lease ON lease.identity_key=ownership.identity_key
                   WHERE ownership.repo_key=?1 AND (lease.owner_id IS NOT ?2 OR lease.expires_at<=?3)
                 )",
                params![repo_key, owner_id, (Utc::now() + Self::COMMAND_AUTHORITY_RESERVE).to_rfc3339()],
                |row| row.get(0),
            )?;
            if physical_invalid {
                return Ok(ExecutionAuthority::Lost(format!(
                    "repo queue {repo_key} physical lease is not owned by {owner_id}"
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

    fn require_real_directory(path: &Path, label: &str) -> Result<()> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect {label} {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("{label} must be a real directory: {}", path.display());
        }
        Ok(())
    }

    fn path_bytes(path: &Path) -> Vec<u8> {
        path.as_os_str().as_bytes().to_vec()
    }

    fn insert_workspace_git_binding(
        transaction: &rusqlite::Transaction<'_>,
        repo_key: &str,
        owner_kind: &str,
        owner_id: &str,
        path: &Path,
    ) -> Result<()> {
        let binding = crate::git_command::expected_binding(path)?;
        binding.verify()?;
        if binding.object_format != repository_object_format(transaction, repo_key)? {
            anyhow::bail!("workspace Git object format differs from repository policy");
        }
        transaction.execute(
            "INSERT INTO workspace_git_bindings(owner_kind,owner_id,top_level,binding_json,created_at) VALUES(?1,?2,?3,?4,?5)",
            params![owner_kind,owner_id,path_bytes(path),serde_json::to_string(&binding)?,now()],
        )?;
        Ok(())
    }

    fn row_path(row: &Row<'_>, column: &str) -> rusqlite::Result<PathBuf> {
        let bytes: Vec<u8> = row.get(column)?;
        Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
    }

    fn repository_object_format(
        connection: &Connection,
        repo_key: &str,
    ) -> Result<crate::git_object::GitObjectFormat> {
        let repository: String = connection.query_row(
            "SELECT canonical_repository_json FROM repository_policies WHERE repo_key=?1",
            [repo_key],
            |row| row.get(0),
        )?;
        Ok(
            serde_json::from_str::<crate::repository_policy::GitRepository>(&repository)?
                .object_format(),
        )
    }

    fn attempt_object_format(
        connection: &Connection,
        attempt_id: &str,
    ) -> Result<crate::git_object::GitObjectFormat> {
        let repository: String = connection.query_row(
            "SELECT policy.canonical_repository_json FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id JOIN repository_policies policy ON policy.repo_key=item.repo_key WHERE attempt.id=?1",
            [attempt_id],
            |row| row.get(0),
        )?;
        Ok(
            serde_json::from_str::<crate::repository_policy::GitRepository>(&repository)?
                .object_format(),
        )
    }

    const REGISTERED_REPOSITORY_SELECT: &str = "SELECT repository.* FROM (
        SELECT registered.repo_key,registered.owned_root_path,registered.root_rift_id,
                registered.git_binding_json,registered.registry_identity,registered.registry_device,registered.registry_inode,
               registered.generation,policy.target_branch,registered.development_root_path,
               registered.integration_root_path,registered.source_sha,
               registered.checkout_json AS checkout_reconciliation_json,
               registered.created_at,registered.updated_at,policy.revision AS policy_revision,
               policy.operation_state_json,policy.canonical_repository_json,
               policy.integration_policy,policy.replication_policy_json
        FROM registered_repositories registered
        JOIN repository_policies policy ON policy.repo_key=registered.repo_key
        JOIN workspace_roots development
          ON development.repo_key=registered.repo_key
         AND development.kind='development'
         AND development.root_path=registered.development_root_path
         AND development.source_path=registered.owned_root_path
         AND development.source_rift_id=registered.root_rift_id
         AND development.registry_identity=registered.registry_identity
         AND development.registry_device=registered.registry_device
         AND development.registry_inode=registered.registry_inode
        JOIN workspace_roots integration
          ON integration.repo_key=registered.repo_key
         AND integration.kind='integration'
         AND integration.root_path=registered.integration_root_path
         AND integration.source_path=registered.owned_root_path
         AND integration.source_rift_id=registered.root_rift_id
         AND integration.registry_identity=registered.registry_identity
         AND integration.registry_device=registered.registry_device
         AND integration.registry_inode=registered.registry_inode
    ) repository";

    fn map_repository(row: &Row<'_>) -> rusqlite::Result<RegisteredRepository> {
        let binding: crate::git_command::RepositoryBinding =
            serde_json::from_str(&row.get::<_, String>("git_binding_json")?)
                .map_err(|error| map_json_error("git_binding_json", error))?;
        crate::git_command::register_binding(&binding).map_err(|error| {
            map_parse_error(format!("invalid Git repository binding: {error:#}"))
        })?;
        let checkout_reconciliation: CheckoutReconciliationState =
            serde_json::from_str(&row.get::<_, String>("checkout_reconciliation_json")?)
                .map_err(|error| map_json_error("checkout_reconciliation_json", error))?;
        let checkout_target = checkout_reconciliation.target_sha();
        let source_sha: String = row.get("source_sha")?;
        let policy = crate::repository_policy::RepositoryPolicy {
            operation_state: serde_json::from_str(&row.get::<_, String>("operation_state_json")?)
                .map_err(|error| map_json_error("operation_state_json", error))?,
            canonical_repository: serde_json::from_str(
                &row.get::<_, String>("canonical_repository_json")?,
            )
            .map_err(|error| map_json_error("canonical_repository_json", error))?,
            target_branch: row.get("target_branch")?,
            integration_policy: match row.get::<_, String>("integration_policy")?.as_str() {
                "direct" => crate::repository_policy::IntegrationPolicy::Direct,
                "merge_request_required" => {
                    crate::repository_policy::IntegrationPolicy::MergeRequestRequired
                }
                _ => {
                    return Err(map_parse_error(
                        "invalid repository integration policy".into(),
                    ))
                }
            },
            replication_policy: serde_json::from_str(
                &row.get::<_, String>("replication_policy_json")?,
            )
            .map_err(|error| map_json_error("replication_policy_json", error))?,
        }
        .validate()
        .map_err(|error| map_parse_error(format!("invalid repository policy: {error:#}")))?;
        let object_format = policy.canonical_repository.object_format();
        object_format
            .require_oid(checkout_target, "registered checkout target")
            .map_err(|error| map_parse_error(format!("{error:#}")))?;
        object_format
            .require_oid(&source_sha, "registered checkout source")
            .map_err(|error| map_parse_error(format!("{error:#}")))?;
        if binding.object_format != object_format {
            return Err(map_parse_error(
                "registered Git binding object format differs from policy".into(),
            ));
        }
        Ok(RegisteredRepository {
            key: row.get("repo_key")?,
            owned_root_path: row_path(row, "owned_root_path")?,
            root_rift_id: row.get("root_rift_id")?,
            registry_identity: row_path(row, "registry_identity")?,
            registry_device: row.get("registry_device")?,
            registry_inode: row.get("registry_inode")?,
            generation: row.get("generation")?,
            target_branch: row.get("target_branch")?,
            remote: RegisteredRemote {
                name: crate::repository::INTERNAL_REMOTE_NAME.into(),
            },
            development_root_path: row_path(row, "development_root_path")?,
            integration_root_path: row_path(row, "integration_root_path")?,
            source_sha,
            checkout_reconciliation,
            policy,
            policy_revision: row.get("policy_revision")?,
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

    const LOCAL_SUBMISSION_SELECT: &str = "SELECT submission.*,policy.canonical_repository_json
        AS canonical_repository_json FROM local_submissions submission
        JOIN repository_policies policy ON policy.repo_key=submission.repo_key";

    fn map_local_submission(row: &Row<'_>) -> rusqlite::Result<LocalSubmission> {
        let base_sha: String = row.get("base_sha")?;
        let commit_sha: String = row.get("commit_sha")?;
        let repository: crate::repository_policy::GitRepository =
            serde_json::from_str(&row.get::<_, String>("canonical_repository_json")?)
                .map_err(|error| map_json_error("canonical_repository_json", error))?;
        repository
            .object_format()
            .require_oid(&base_sha, "local submission base")
            .and_then(|()| {
                repository
                    .object_format()
                    .require_oid(&commit_sha, "local submission commit")
            })
            .map_err(|error| map_parse_error(format!("{error:#}")))?;
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

    fn map_replication_debt(row: &Row<'_>) -> rusqlite::Result<ReplicationDebt> {
        Ok(ReplicationDebt {
            id: row.get(0)?,
            item_id: row.get(1)?,
            repo_key: row.get(2)?,
            canonical_source_sha: row.get(3)?,
            destination_key: row.get(4)?,
            target_branch: row.get(5)?,
            sequence: row.get(6)?,
            replica: serde_json::from_str(&row.get::<_, String>(7)?)
                .map_err(|error| map_json_error("replica_json", error))?,
            expected_destination_sha: row.get(8)?,
            operation: row.get(9)?,
            outcome: row.get(10)?,
            application_id: row.get(11)?,
            failure: row.get(12)?,
            superseded_by_id: row.get(13)?,
        })
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
        let admission = match row.get::<_, String>("admission_kind")?.as_str() {
            "local_submission" => QueueAdmission::LocalSubmission {
                source_branch: row.get("admission_source_branch")?,
                head_sha: row.get("admission_head_sha")?,
                source_ref: row.get("admission_source_ref")?,
                submission_id: row.get("admission_submission_id")?,
            },
            "direct" => QueueAdmission::Direct {
                source_branch: row.get("admission_source_branch")?,
                head_sha: row.get("admission_head_sha")?,
            },
            "merge_request" | "historical_merge_request" => {
                let admission = MergeRequestAdmission {
                    provider: match row.get::<_, String>("admission_provider")?.as_str() {
                        "github" => crate::repository_policy::Provider::Github,
                        "gitlab" => crate::repository_policy::Provider::Gitlab,
                        _ => return Err(map_parse_error("invalid admitted provider".into())),
                    },
                    provider_host: row.get("admission_provider_host")?,
                    repository: row.get("admission_provider_repository")?,
                    repository_id: row.get("admission_provider_repository_id")?,
                    target_branch: row.get("admission_target_branch")?,
                    identity: row.get("admission_merge_request_identity")?,
                    url: row.get("admission_merge_request_url")?,
                    source_branch: row.get("admission_source_branch")?,
                    head_sha: row.get("admission_head_sha")?,
                    base_sha: row.get("admission_base_sha")?,
                    provider_merge_method: row
                        .get::<_, Option<String>>("admission_provider_merge_method")?
                        .map(|method| match method.as_str() {
                            "merge" => Ok(crate::repository_policy::ProviderMergeMethod::Merge),
                            "squash" => Ok(crate::repository_policy::ProviderMergeMethod::Squash),
                            _ => Err(map_parse_error(
                                "invalid admitted provider merge method".into(),
                            )),
                        })
                        .transpose()?,
                };
                if row.get::<_, String>("admission_kind")? == "merge_request" {
                    QueueAdmission::MergeRequest(admission)
                } else {
                    QueueAdmission::HistoricalMergeRequest(admission)
                }
            }
            _ => return Err(map_parse_error("invalid queue admission kind".into())),
        };
        let source_branch = admission.source_branch().to_string();
        let current_head_sha = admission.head_sha().to_string();
        let (source, landing_policy) = match &admission {
            QueueAdmission::LocalSubmission {
                submission_id,
                head_sha,
                ..
            } => (
                QueueSource::LocalSubmission {
                    submission_id: submission_id.clone(),
                    commit_sha: head_sha.clone(),
                },
                LandingPolicy::Squash,
            ),
            QueueAdmission::Direct { source_branch, .. } => (
                QueueSource::RemoteBranch {
                    branch: source_branch.clone(),
                },
                LandingPolicy::Direct,
            ),
            QueueAdmission::MergeRequest(value) | QueueAdmission::HistoricalMergeRequest(value) => {
                (
                    QueueSource::RemoteBranch {
                        branch: value.source_branch.clone(),
                    },
                    LandingPolicy::Provider,
                )
            }
        };
        Ok(QueueItem {
            id: row.get("id")?,
            repo_key: row.get("repo_key")?,
            owned_root_path: row.get("owned_root_path")?,
            source_branch,
            target_branch: row.get("target_branch")?,
            current_head_sha,
            admission,
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
            source,
            landing_policy,
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

    pub(crate) fn install_schema(connection: &Connection) -> Result<()> {
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS iq_sqlite_sequence_init(id INTEGER PRIMARY KEY AUTOINCREMENT);
             DROP TABLE iq_sqlite_sequence_init;",
        )?;
        connection.execute_batch(SCHEMA4)?;
        connection.execute_batch(REPOSITORY_POLICY_SCHEMA)?;
        reserve_all_policy_physical_ownership(connection)?;
        connection.execute_batch(COMPOSITION_SCHEMA4)?;
        connection.execute_batch(LANDING_STATE_TRIGGERS)?;
        connection.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
        crate::control_store::install_control_schema(connection)?;
        connection.execute_batch(REGISTERED_REPOSITORY_TRIGGERS4)?;
        #[cfg(debug_assertions)]
        if std::env::var_os("IQ_TEST_SCHEMA_STOP_AFTER_OBJECTS").is_some() {
            std::process::exit(86);
        }
        Ok(())
    }

    fn install_schema3_objects(connection: &Connection) -> Result<()> {
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS iq_sqlite_sequence_init(id INTEGER PRIMARY KEY AUTOINCREMENT);
             DROP TABLE iq_sqlite_sequence_init;",
        )?;
        connection.execute_batch(SCHEMA)?;
        connection.execute_batch(COMPOSITION_SCHEMA)?;
        connection.execute_batch(QUEUE_SOURCE_TRIGGERS)?;
        connection.execute_batch(LANDING_STATE_TRIGGERS)?;
        connection.execute_batch(WORKSPACE_STATE_TRIGGERS)?;
        crate::control_store::install_schema3_control_identity(connection)?;
        install_schema3_landing_projection(connection)?;
        connection.execute_batch(REGISTERED_REPOSITORY_TRIGGERS)?;
        #[cfg(debug_assertions)]
        if std::env::var_os("IQ_TEST_SCHEMA_STOP_AFTER_OBJECTS").is_some() {
            std::process::exit(86);
        }
        Ok(())
    }

    fn install_schema3_landing_projection(connection: &Connection) -> Result<()> {
        let current: String = connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='queue_effort_projection_guard'",
            [],
            |row| row.get(0),
        )?;
        let legacy = current
            .replacen(
                "WHEN effort.state='landing_uncertain' THEN",
                "WHEN effort.state IN ('landing','landing_uncertain') THEN",
                1,
            )
            .replacen(
                "AND json_extract(effort.state_json,'$.payload.resume.state')='landing_uncertain' THEN",
                "AND json_extract(effort.state_json,'$.payload.resume.state') IN ('landing','landing_uncertain') THEN",
                1,
            );
        if legacy == current {
            anyhow::bail!("schema-3 landing projection template did not change");
        }
        connection.execute_batch("DROP TRIGGER queue_effort_projection_guard")?;
        connection.execute_batch(&legacy)?;
        Ok(())
    }

    pub(crate) fn reserve_policy_physical_ownership(
        connection: &Connection,
        repo_key: &str,
        policy: &crate::repository_policy::RepositoryPolicy,
    ) -> Result<()> {
        for (repository, role, ordinal) in policy.physical_repositories() {
            let identity_key = repository.physical_identity_key()?;
            let repository_json = serde_json::to_string(repository)?;
            let existing = connection
                .query_row(
                    "SELECT repo_key,role,ordinal,repository_json FROM physical_repository_ownership WHERE identity_key=?1",
                    [&identity_key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, usize>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .optional()?;
            match existing {
                Some(existing)
                    if existing
                        == (
                            repo_key.to_string(),
                            role.to_string(),
                            ordinal,
                            repository_json.clone(),
                        ) => {}
                Some((owner, owner_role, _, _)) => anyhow::bail!(
                    "physical repository is already reserved as {owner_role} by repository {owner}"
                ),
                None => {
                    connection.execute(
                        "INSERT INTO physical_repository_ownership(identity_key,repo_key,role,ordinal,repository_json,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
                        params![identity_key, repo_key, role, ordinal, repository_json, now()],
                    )?;
                }
            }
        }
        let expected = policy.physical_repositories().len();
        let actual: usize = connection.query_row(
            "SELECT COUNT(*) FROM physical_repository_ownership WHERE repo_key=?1",
            [repo_key],
            |row| row.get(0),
        )?;
        if actual != expected {
            anyhow::bail!("repository physical ownership inventory differs from policy");
        }
        Ok(())
    }

    fn reserve_all_policy_physical_ownership(connection: &Connection) -> Result<()> {
        let mut statement = connection.prepare(
            "SELECT repo_key,operation_state_json,canonical_repository_json,target_branch,integration_policy,replication_policy_json FROM repository_policies ORDER BY repo_key",
        )?;
        let policies = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);
        for (repo_key, operation, canonical, target, integration, replication) in policies {
            let policy = crate::repository_policy::RepositoryPolicy {
                operation_state: serde_json::from_str(&operation)?,
                canonical_repository: serde_json::from_str(&canonical)?,
                target_branch: target,
                integration_policy: match integration.as_str() {
                    "direct" => crate::repository_policy::IntegrationPolicy::Direct,
                    "merge_request_required" => {
                        crate::repository_policy::IntegrationPolicy::MergeRequestRequired
                    }
                    _ => anyhow::bail!("stored repository integration policy is invalid"),
                },
                replication_policy: serde_json::from_str(&replication)?,
            }
            .validate()?;
            reserve_policy_physical_ownership(connection, &repo_key, &policy)?;
        }
        Ok(())
    }

    fn legacy_transport_matches(
        legacy: &str,
        repository: &crate::repository_policy::GitRepository,
        fetch: bool,
    ) -> bool {
        match repository {
            crate::repository_policy::GitRepository::Accessible {
                fetch_url,
                push_url,
                ..
            } => legacy == if fetch { fetch_url } else { push_url },
            crate::repository_policy::GitRepository::LocalBare { path, .. } => {
                let mut expected = b"file://".to_vec();
                expected.extend_from_slice(path.as_os_str().as_bytes());
                legacy.as_bytes() == path.as_os_str().as_bytes() || legacy.as_bytes() == expected
            }
        }
    }

    fn validate_migration_workspace_identity(workspace: &WorkspaceIdentity) -> Result<()> {
        if !Path::new(&workspace.path).is_absolute() {
            anyhow::bail!("migration workspace path must be absolute");
        }
        crate::control_domain::require_exact_text(
            &workspace.rift_id,
            "migration workspace Rift ID",
        )?;
        crate::control_domain::require_exact_text(
            &workspace.source_rift_id,
            "migration workspace source Rift ID",
        )?;
        Ok(())
    }

    fn create_schema3_backup(
        path: &Path,
        database_id: &str,
        members: &[Schema3BackupMember],
        source_digest: &str,
        operation_id: &str,
    ) -> Result<PathBuf> {
        if schema3_source_family(path)? != members
            || schema3_source_digest(members)? != source_digest
        {
            anyhow::bail!("schema-3 source family changed before backup publication");
        }
        let root = schema3_backup_root(path)?;
        let manifest = Schema3BackupManifest {
            version: 2,
            database_id: database_id.to_string(),
            source_digest: source_digest.to_string(),
            operation_id: operation_id.to_string(),
            members: members.to_vec(),
        };
        if root.exists() {
            let existing = validate_schema3_backup_root(&root, database_id, None)
                .context("pre-existing schema-3 backup authority is not owned by IQ")?;
            let existing_manifest: Schema3BackupManifest =
                serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
            if existing_manifest.source_digest == source_digest {
                return Ok(existing);
            }
        }

        let temporary = PrivateBackupDirectory::new(path, &manifest)?;
        for member in members {
            copy_database_file(
                &database_family_member(path, &member.suffix),
                &backup_family_member(&temporary.path, &member.suffix)?,
            )?;
        }
        let copied = backup_family_members(&temporary.path, members)?;
        if copied
            .iter()
            .map(|member| (&member.suffix, member.length, &member.sha256))
            .ne(members
                .iter()
                .map(|member| (&member.suffix, member.length, &member.sha256)))
        {
            anyhow::bail!("private schema-3 backup differs from exact source bytes");
        }
        let backup_database = backup_family_member(&temporary.path, "")?;
        let backup = open_immutable_database(&backup_database)?;
        if validate_schema3_identity(&backup)? != database_id {
            anyhow::bail!("private schema-3 backup changed database identity");
        }
        drop(backup);
        let manifest_path = temporary.path.join("manifest.json");
        let mut manifest_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&manifest_path)?;
        serde_json::to_writer(&mut manifest_file, &manifest)?;
        manifest_file.write_all(b"\n")?;
        manifest_file.sync_all()?;
        File::open(&temporary.path)?.sync_all()?;

        let stale = if root.exists() {
            let mut name = root
                .file_name()
                .context("schema-3 backup root has no file name")?
                .to_os_string();
            name.push(format!(".stale-{}", Uuid::new_v4()));
            let stale = root.with_file_name(name);
            fs::rename(&root, &stale).context("quarantine stale schema-3 backup")?;
            File::open(root.parent().context("schema-3 backup has no parent")?)?.sync_all()?;
            Some(stale)
        } else {
            None
        };
        if let Err(error) = publish_database_noreplace(&temporary.path, &root) {
            if let Some(stale) = &stale {
                if !root.exists() {
                    let _ = fs::rename(stale, &root);
                }
            }
            return Err(error).context("publish exact schema-3 backup authority");
        }
        File::open(root.parent().context("schema-3 backup has no parent")?)?.sync_all()?;
        let published =
            validate_schema3_backup_root(&root, database_id, Some(&manifest.source_digest))?;
        if let Some(stale) = stale {
            let stale_directory =
                crate::secure_fs::DirectoryHandle::open(&stale, "stale schema-3 backup authority")?;
            let stale_identity = stale_directory.directory().metadata()?;
            validate_schema3_backup_root(&stale, database_id, None)
                .context("quarantined schema-3 backup lost ownership proof")?;
            let live_identity = fs::symlink_metadata(&stale)?;
            if (stale_identity.dev(), stale_identity.ino())
                != (live_identity.dev(), live_identity.ino())
            {
                anyhow::bail!("quarantined schema-3 backup changed during validation");
            }
            stale_directory.remove("stale schema-3 backup authority")?;
        }
        Ok(published)
    }

    fn schema3_backup_root(path: &Path) -> Result<PathBuf> {
        let file_name = path
            .file_name()
            .context("schema-3 database path has no file name")?;
        let mut backup_name = file_name.to_os_string();
        backup_name.push(".schema3-backup-authority");
        Ok(path.with_file_name(backup_name))
    }

    fn validate_schema3_backup(
        source_path: &Path,
        database_id: &str,
        expected_source_digest: Option<&str>,
    ) -> Result<PathBuf> {
        let root = schema3_backup_root(source_path)?;
        validate_schema3_backup_root(&root, database_id, expected_source_digest)
    }

    fn validate_schema3_backup_root(
        root: &Path,
        database_id: &str,
        expected_source_digest: Option<&str>,
    ) -> Result<PathBuf> {
        let metadata = fs::symlink_metadata(root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("schema-3 backup authority is not a real directory");
        }
        let manifest_path = root.join("manifest.json");
        let manifest_metadata = fs::symlink_metadata(&manifest_path)?;
        if manifest_metadata.file_type().is_symlink()
            || !manifest_metadata.is_file()
            || manifest_metadata.len() > 64 * 1024
        {
            anyhow::bail!("schema-3 backup manifest is invalid");
        }
        let manifest: Schema3BackupManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
        if manifest.version != 2
            || manifest.database_id != database_id
            || Uuid::parse_str(&manifest.operation_id).map_or(true, |operation| {
                operation.to_string() != manifest.operation_id
            })
        {
            anyhow::bail!("schema-3 backup manifest identity is invalid");
        }
        let ownership = read_migration_ownership_manifest(root)?;
        if ownership
            != (MigrationOwnershipManifest {
                version: 1,
                database_id: manifest.database_id.clone(),
                source_digest: manifest.source_digest.clone(),
                operation_id: manifest.operation_id.clone(),
            })
        {
            anyhow::bail!("schema-3 backup ownership manifest is inconsistent");
        }
        if expected_source_digest.is_some_and(|expected| expected != manifest.source_digest) {
            anyhow::bail!("schema-3 backup source digest is stale");
        }
        if schema3_source_digest(&manifest.members)? != manifest.source_digest {
            anyhow::bail!("schema-3 backup manifest digest is inconsistent");
        }
        let copied = backup_family_members(root, &manifest.members)?;
        if copied
            .iter()
            .map(|member| (&member.suffix, member.length, &member.sha256))
            .ne(manifest
                .members
                .iter()
                .map(|member| (&member.suffix, member.length, &member.sha256)))
        {
            anyhow::bail!("schema-3 backup bytes differ from its source digest");
        }
        let expected_names = manifest
            .members
            .iter()
            .map(|member| {
                backup_family_member(root, &member.suffix).map(|path| {
                    path.file_name()
                        .expect("backup family member has a file name")
                        .to_os_string()
                })
            })
            .chain([
                Ok(OsString::from("manifest.json")),
                Ok(OsString::from("ownership.json")),
            ])
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        let actual_names = fs::read_dir(root)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()?;
        if actual_names != expected_names {
            anyhow::bail!("schema-3 backup family contains unexpected members");
        }
        let database = backup_family_member(root, "")?;
        let connection = open_immutable_database(&database)?;
        if validate_schema3_identity(&connection)? != database_id {
            anyhow::bail!("schema-3 backup database identity is inconsistent");
        }
        drop(connection);
        sync_database_file_and_parent(&database)?;
        Ok(database)
    }

    fn schema3_source_family(path: &Path) -> Result<Vec<Schema3BackupMember>> {
        let mut members = ["", "-journal", "-wal", "-shm"]
            .into_iter()
            .filter_map(|suffix| {
                let member = database_family_member(path, suffix);
                match fs::symlink_metadata(&member) {
                    Ok(_) => Some(backup_member_identity(&member, suffix)),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => Some(Err(error.into())),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        members.sort_by(|left, right| left.suffix.cmp(&right.suffix));
        if members
            .first()
            .is_none_or(|member| !member.suffix.is_empty())
        {
            anyhow::bail!("schema-3 source family has no primary database");
        }
        Ok(members)
    }

    fn backup_family_members(
        root: &Path,
        expected: &[Schema3BackupMember],
    ) -> Result<Vec<Schema3BackupMember>> {
        expected
            .iter()
            .map(|member| {
                backup_member_identity(&backup_family_member(root, &member.suffix)?, &member.suffix)
            })
            .collect()
    }

    fn backup_member_identity(path: &Path, suffix: &str) -> Result<Schema3BackupMember> {
        if !matches!(suffix, "" | "-journal" | "-wal" | "-shm") {
            anyhow::bail!("schema-3 backup manifest has an invalid family suffix");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let before = file.metadata()?;
        if !before.is_file() {
            anyhow::bail!("schema-3 database family member is not a regular file");
        }
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
        if (
            before.dev(),
            before.ino(),
            before.len(),
            before.mtime(),
            before.mtime_nsec(),
        ) != (
            after.dev(),
            after.ino(),
            after.len(),
            after.mtime(),
            after.mtime_nsec(),
        ) || (before.ctime(), before.ctime_nsec()) != (after.ctime(), after.ctime_nsec())
        {
            anyhow::bail!("schema-3 database family changed while computing its digest");
        }
        Ok(Schema3BackupMember {
            suffix: suffix.to_string(),
            length: before.len(),
            mode: before.mode(),
            uid: before.uid(),
            gid: before.gid(),
            device: before.dev(),
            inode: before.ino(),
            modified_seconds: before.mtime(),
            modified_nanoseconds: before.mtime_nsec(),
            changed_seconds: before.ctime(),
            changed_nanoseconds: before.ctime_nsec(),
            sha256: format!("{:x}", digest.finalize()),
        })
    }

    fn schema3_source_digest(members: &[Schema3BackupMember]) -> Result<String> {
        if members.is_empty()
            || members
                .windows(2)
                .any(|pair| pair[0].suffix >= pair[1].suffix)
        {
            anyhow::bail!("schema-3 source family manifest is not canonical");
        }
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(members)?)
        ))
    }

    fn database_family_member(path: &Path, suffix: &str) -> PathBuf {
        let mut member = path.as_os_str().to_os_string();
        member.push(suffix);
        PathBuf::from(member)
    }

    fn backup_family_member(root: &Path, suffix: &str) -> Result<PathBuf> {
        if !matches!(suffix, "" | "-journal" | "-wal" | "-shm") {
            anyhow::bail!("schema-3 backup manifest has an invalid family suffix");
        }
        Ok(root.join(format!("database{suffix}")))
    }

    fn open_immutable_database(path: &Path) -> Result<Connection> {
        let mut uri = String::from("file:");
        for byte in path.as_os_str().as_bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                    uri.push(*byte as char)
                }
                _ => uri.push_str(&format!("%{byte:02X}")),
            }
        }
        uri.push_str("?immutable=1");
        let connection = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NOFOLLOW
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .context("open immutable schema-3 backup")?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    fn copy_database_file(source_path: &Path, destination_path: &Path) -> Result<()> {
        let mut source = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NOATIME)
            .open(source_path)?;
        let source_metadata = source.metadata()?;
        let mut destination = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(destination_path)
            .with_context(|| format!("create database copy {}", destination_path.display()))?;
        std::io::copy(&mut source, &mut destination)?;
        destination.sync_all()?;
        let after = source.metadata()?;
        if !source_metadata.is_file()
            || source_metadata.len() != destination.metadata()?.len()
            || (source_metadata.dev(), source_metadata.ino()) != (after.dev(), after.ino())
            || source_metadata.len() != after.len()
            || source_metadata.mtime() != after.mtime()
            || source_metadata.mtime_nsec() != after.mtime_nsec()
        {
            anyhow::bail!("database changed while creating private copy");
        }
        Ok(())
    }

    fn validate_stored_object_ids(
        connection: &Connection,
        object_formats: &std::collections::BTreeMap<String, crate::git_object::GitObjectFormat>,
        has_legacy_queue_head: bool,
    ) -> Result<()> {
        let queue_head = if has_legacy_queue_head {
            "UNION ALL SELECT repo_key,'queue head',current_head_sha FROM queue_items"
        } else {
            ""
        };
        let oid_query = format!(
            "SELECT repo_key,label,oid FROM (
             SELECT repo_key,'registered repository source' AS label,source_sha AS oid FROM registered_repositories
             UNION ALL SELECT repo_key,'provisioning source',source_sha FROM repository_provisioning_intents
             UNION ALL SELECT repo_key,'development workspace base',base_sha FROM development_workspaces
             UNION ALL SELECT repo_key,'local submission base',base_sha FROM local_submissions
             UNION ALL SELECT repo_key,'local submission commit',commit_sha FROM local_submissions
             {queue_head}
             UNION ALL SELECT repo_key,'queue target',target_sha FROM queue_items
             UNION ALL SELECT repo_key,'queue source',source_sha FROM queue_items
             UNION ALL SELECT repo_key,'queue landed commit',landed_commit_sha FROM queue_items
             UNION ALL SELECT item.repo_key,'attempt source head',attempt.source_head_sha FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'attempt target base',attempt.target_base_sha FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'attempt merge commit',attempt.merge_commit_sha FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'attempt validated commit',attempt.validated_commit_sha FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'attempt landed commit',attempt.landed_commit_sha FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'validation target base',invocation.target_base_sha FROM validation_invocations invocation JOIN integration_attempts attempt ON attempt.id=invocation.attempt_id JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'validation candidate',invocation.candidate_sha FROM validation_invocations invocation JOIN integration_attempts attempt ON attempt.id=invocation.attempt_id JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'validation result',invocation.validated_commit_sha FROM validation_invocations invocation JOIN integration_attempts attempt ON attempt.id=invocation.attempt_id JOIN queue_items item ON item.id=attempt.item_id
             UNION ALL SELECT item.repo_key,'effort target',effort.target_sha FROM integration_efforts effort JOIN queue_items item ON item.id=effort.item_id
             UNION ALL SELECT item.repo_key,'effort source',effort.source_sha FROM integration_efforts effort JOIN queue_items item ON item.id=effort.item_id
             UNION ALL SELECT item.repo_key,'candidate evidence',evidence.candidate_sha FROM candidate_evidence evidence JOIN integration_efforts effort ON effort.id=evidence.effort_id JOIN queue_items item ON item.id=effort.item_id
             ) WHERE oid IS NOT NULL"
        );
        let mut statement = connection.prepare(&oid_query)?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (repo_key, label, oid) = row?;
            object_formats
                .get(&repo_key)
                .with_context(|| format!("stored {label} has no repository object-format policy"))?
                .require_oid(&oid, &label)?;
        }

        let mut checkouts = connection.prepare(
            "SELECT repo_key,checkout_json FROM registered_repositories ORDER BY repo_key",
        )?;
        for row in checkouts.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })? {
            let (repo_key, checkout) = row?;
            let checkout: CheckoutReconciliationState = serde_json::from_str(&checkout)?;
            object_formats
                .get(&repo_key)
                .context("registered checkout has no repository object-format policy")?
                .require_oid(checkout.target_sha(), "registered checkout target")?;
        }

        let mut item_states = connection
            .prepare("SELECT repo_key,landing_state_json FROM queue_items ORDER BY id")?;
        for row in item_states.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })? {
            let (repo_key, state) = row?;
            let object_format = object_formats
                .get(&repo_key)
                .context("queue landing state has no repository object-format policy")?;
            match serde_json::from_str::<LandingState>(&state)? {
                LandingState::Ready => {}
                LandingState::Uncertain {
                    candidate_sha,
                    expected_target_sha,
                } => {
                    object_format.require_oid(&candidate_sha, "uncertain queue candidate")?;
                    object_format.require_oid(&expected_target_sha, "uncertain queue target")?;
                }
                LandingState::Landed {
                    candidate_sha,
                    commit_sha,
                } => {
                    object_format.require_oid(&candidate_sha, "landed queue candidate")?;
                    object_format.require_oid(&commit_sha, "landed queue commit")?;
                }
            }
        }

        let mut attempts = connection.prepare(
            "SELECT item.repo_key,attempt.moved_base_json FROM integration_attempts attempt JOIN queue_items item ON item.id=attempt.item_id ORDER BY attempt.id",
        )?;
        for row in attempts.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })? {
            let (repo_key, moved_base) = row?;
            let object_format = object_formats
                .get(&repo_key)
                .context("moved-base state has no repository object-format policy")?;
            match serde_json::from_str::<MovedBaseState>(&moved_base)? {
                MovedBaseState::None => {}
                MovedBaseState::Pending {
                    target_sha,
                    source_sha,
                } => {
                    object_format.require_oid(&target_sha, "moved-base target")?;
                    object_format.require_oid(&source_sha, "moved-base source")?;
                }
                MovedBaseState::Applied {
                    target_sha,
                    source_sha,
                    candidate_sha,
                } => {
                    object_format.require_oid(&target_sha, "applied moved-base target")?;
                    object_format.require_oid(&source_sha, "applied moved-base source")?;
                    object_format.require_oid(&candidate_sha, "applied moved-base candidate")?;
                }
            }
        }

        let mut efforts = connection.prepare(
            "SELECT item.repo_key,effort.state,effort.state_json FROM integration_efforts effort JOIN queue_items item ON item.id=effort.item_id ORDER BY effort.id",
        )?;
        for row in efforts.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })? {
            let (repo_key, state_name, state) = row?;
            if has_legacy_queue_head
                && matches!(state_name.as_str(), "agent_launching" | "agent_running")
            {
                continue;
            }
            serde_json::from_str::<crate::control_domain::IntegrationEffortState>(&state)?
                .validate_object_ids(
                    *object_formats
                        .get(&repo_key)
                        .context("effort state has no repository object-format policy")?,
                )?;
        }
        Ok(())
    }

    fn validate_schema4_contents(connection: &Connection) -> Result<()> {
        let invalid: i64 = connection.query_row(
            "SELECT
             (SELECT COUNT(*) FROM registered_repositories repository LEFT JOIN repository_policies policy ON policy.repo_key=repository.repo_key WHERE policy.repo_key IS NULL)+
             (SELECT COUNT(*) FROM repository_provisioning_intents intent LEFT JOIN repository_policies policy ON policy.repo_key=intent.repo_key WHERE policy.repo_key IS NULL)+
             (SELECT COUNT(*) FROM repository_policies policy LEFT JOIN registered_repositories repository ON repository.repo_key=policy.repo_key LEFT JOIN repository_provisioning_intents intent ON intent.repo_key=policy.repo_key WHERE (repository.repo_key IS NULL AND intent.repo_key IS NULL) OR (repository.repo_key IS NOT NULL AND intent.repo_key IS NOT NULL))+
             (SELECT COUNT(*) FROM queue_items item LEFT JOIN queue_admissions admission ON admission.item_id=item.id WHERE admission.item_id IS NULL)+
             (SELECT COUNT(*) FROM queue_admissions admission LEFT JOIN queue_items item ON item.id=admission.item_id WHERE item.id IS NULL)+
             (SELECT COUNT(*) FROM queue_admissions admission JOIN queue_items item ON item.id=admission.item_id WHERE admission.kind='historical_merge_request' AND item.status NOT IN ('integrated','cancelled'))",
            [],
            |row| row.get(0),
        )?;
        if invalid != 0 {
            anyhow::bail!("schema-4 authority content is inconsistent");
        }
        let invalid_supersession: i64 = connection.query_row(
            "SELECT COUNT(*) FROM replication_debt older LEFT JOIN replication_debt newer ON newer.id=older.superseded_by_id WHERE older.outcome IN ('superseded_cleanup_pending','superseded') AND (newer.id IS NULL OR newer.destination_key!=older.destination_key OR newer.target_branch!=older.target_branch OR newer.sequence<=older.sequence OR newer.outcome NOT IN ('succeeded','superseded'))",
            [],
            |row| row.get(0),
        )?;
        if invalid_supersession != 0 {
            anyhow::bail!("replication supersession authority is inconsistent");
        }
        let object_formats = {
            let mut statement = connection.prepare(
                "SELECT repo_key,canonical_repository_json FROM repository_policies ORDER BY repo_key",
            )?;
            let formats = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .map(|row| {
                    let (repo_key, repository) = row?;
                    Ok((
                        repo_key,
                        serde_json::from_str::<crate::repository_policy::GitRepository>(
                            &repository,
                        )?
                        .object_format(),
                    ))
                })
                .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
            formats
        };
        validate_stored_object_ids(connection, &object_formats, false)?;
        let mut schema4_oids = connection.prepare(
            "SELECT repo_key,label,oid FROM (
             SELECT item.repo_key,'admission head' AS label,admission.head_sha AS oid FROM queue_admissions admission JOIN queue_items item ON item.id=admission.item_id
             UNION ALL SELECT item.repo_key,'admission base',admission.base_sha FROM queue_admissions admission JOIN queue_items item ON item.id=admission.item_id
             UNION ALL SELECT item.repo_key,'provider admitted base',guarantee.admitted_base_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
             UNION ALL SELECT item.repo_key,'provider admitted head',guarantee.admitted_head_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
             UNION ALL SELECT item.repo_key,'provider validated target',guarantee.validated_target_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
             UNION ALL SELECT item.repo_key,'provider validated candidate',guarantee.validated_candidate_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
             UNION ALL SELECT item.repo_key,'provider validated tree',guarantee.validated_tree_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
             UNION ALL SELECT item.repo_key,'provider landed commit',guarantee.landed_commit_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
             UNION ALL SELECT item.repo_key,'provider landed tree',guarantee.landed_tree_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
             UNION ALL SELECT item.repo_key,'provider first parent',guarantee.first_parent_sha FROM provider_landing_guarantees guarantee JOIN queue_items item ON item.id=guarantee.item_id
              UNION ALL SELECT debt.repo_key,'replication canonical source',debt.canonical_source_sha FROM replication_debt debt
              UNION ALL SELECT debt.repo_key,'replication expected destination',debt.expected_destination_sha FROM replication_debt debt
              UNION ALL SELECT debt.repo_key,'private-ref cleanup object',debt.expected_sha FROM private_ref_cleanup_debt debt
              ) WHERE oid IS NOT NULL",
        )?;
        for row in schema4_oids.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })? {
            let (repo_key, label, oid) = row?;
            object_formats
                .get(&repo_key)
                .with_context(|| format!("stored {label} has no repository object-format policy"))?
                .require_oid(&oid, &label)?;
        }
        let mut private_refs = connection.prepare(
            "SELECT repo_key,kind,owner_id,ref_name,expected_sha FROM private_ref_cleanup_debt ORDER BY repo_key,ref_name",
        )?;
        for row in private_refs.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })? {
            let (repo_key, kind, owner_id, ref_name, expected_sha) = row?;
            let expected_ref = match kind.as_str() {
                "repository_target" if owner_id == expected_sha => {
                    format!("refs/iq/repository-targets/{repo_key}/{expected_sha}")
                }
                "landing" if !owner_id.is_empty() && !owner_id.contains('/') => {
                    format!("refs/iq/landings/{owner_id}")
                }
                _ => anyhow::bail!("private-ref cleanup identity is inconsistent"),
            };
            if ref_name != expected_ref {
                anyhow::bail!("private-ref cleanup name differs from durable identity");
            }
        }
        let mut repository_bindings = connection.prepare(
            "SELECT repository.owned_root_path,repository.git_binding_json,policy.canonical_repository_json FROM registered_repositories repository JOIN repository_policies policy ON policy.repo_key=repository.repo_key ORDER BY repository.repo_key",
        )?;
        for binding in repository_bindings.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })? {
            let (path, binding, repository) = binding?;
            let path = PathBuf::from(OsString::from_vec(path));
            let binding: crate::git_command::RepositoryBinding = serde_json::from_str(&binding)?;
            let object_format =
                serde_json::from_str::<crate::repository_policy::GitRepository>(&repository)?
                    .object_format();
            if binding.top_level != path || binding.object_format != object_format {
                anyhow::bail!(
                    "registered repository Git binding differs from path or object format authority"
                );
            }
            crate::git_command::register_binding(&binding)?;
        }
        let invalid_workspace_bindings: i64 = connection.query_row(
            "SELECT
             (SELECT COUNT(*) FROM development_workspaces workspace LEFT JOIN workspace_git_bindings binding ON binding.owner_kind='development' AND binding.owner_id=workspace.id WHERE workspace.rift_id IS NOT NULL AND workspace.status!='removed' AND (binding.owner_id IS NULL OR binding.top_level!=workspace.path))+
             (SELECT COUNT(*) FROM queue_items item LEFT JOIN workspace_git_bindings binding ON binding.owner_kind='integration' AND binding.owner_id=item.id WHERE item.integration_workspace_rift_id IS NOT NULL AND (binding.owner_id IS NULL OR binding.top_level!=CAST(item.integration_workspace_path AS BLOB)))+
             (SELECT COUNT(*) FROM workspace_git_bindings binding LEFT JOIN development_workspaces workspace ON binding.owner_kind='development' AND workspace.id=binding.owner_id LEFT JOIN queue_items item ON binding.owner_kind='integration' AND item.id=binding.owner_id WHERE (binding.owner_kind='development' AND (workspace.id IS NULL OR workspace.rift_id IS NULL OR workspace.status='removed')) OR (binding.owner_kind='integration' AND (item.id IS NULL OR item.integration_workspace_rift_id IS NULL)))",
            [],
            |row| row.get(0),
        )?;
        if invalid_workspace_bindings != 0 {
            anyhow::bail!("workspace Git binding ownership is inconsistent");
        }
        let mut workspace_bindings = connection.prepare(
            "SELECT binding.top_level,binding.binding_json,policy.canonical_repository_json,workspace.base_sha,item.target_sha,item.source_sha,item.landed_commit_sha FROM workspace_git_bindings binding LEFT JOIN development_workspaces workspace ON binding.owner_kind='development' AND workspace.id=binding.owner_id LEFT JOIN queue_items item ON binding.owner_kind='integration' AND item.id=binding.owner_id JOIN repository_policies policy ON policy.repo_key=COALESCE(workspace.repo_key,item.repo_key) ORDER BY binding.owner_kind,binding.owner_id",
        )?;
        for binding in workspace_bindings.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })? {
            let (path, binding, repository, base, target, source, landed) = binding?;
            let path = PathBuf::from(OsString::from_vec(path));
            let binding: crate::git_command::RepositoryBinding = serde_json::from_str(&binding)?;
            let object_format =
                serde_json::from_str::<crate::repository_policy::GitRepository>(&repository)?
                    .object_format();
            if binding.top_level != path || binding.object_format != object_format {
                anyhow::bail!("workspace Git binding differs from path or object format authority");
            }
            for (value, label) in [
                (base, "development workspace base"),
                (target, "integration workspace target"),
                (source, "integration workspace source"),
                (landed, "integration workspace landed commit"),
            ] {
                if let Some(value) = value {
                    object_format.require_oid(&value, label)?;
                }
            }
            crate::git_command::register_binding(&binding)?;
        }
        let mut statement = connection.prepare(
            "SELECT repo_key,operation_state_json,canonical_repository_json,canonical_ownership_key,target_branch,integration_policy,replication_policy_json FROM repository_policies",
        )?;
        let policies = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;
        for policy in policies {
            let (repo_key, operation, canonical, ownership_key, target, integration, replication) =
                policy?;
            let policy = crate::repository_policy::RepositoryPolicy {
                operation_state: serde_json::from_str(&operation)?,
                canonical_repository: serde_json::from_str(&canonical)?,
                target_branch: target,
                integration_policy: match integration.as_str() {
                    "direct" => crate::repository_policy::IntegrationPolicy::Direct,
                    "merge_request_required" => {
                        crate::repository_policy::IntegrationPolicy::MergeRequestRequired
                    }
                    _ => anyhow::bail!("stored repository integration policy is invalid"),
                },
                replication_policy: serde_json::from_str(&replication)?,
            }
            .validate()?;
            if policy.canonical_repository.canonical_ownership_key()? != ownership_key {
                anyhow::bail!("stored canonical repository ownership key is inconsistent");
            }
            if matches!(
                policy.operation_state,
                crate::repository_policy::OperationState::Disabled
            ) {
                let active: i64 = connection.query_row(
                    "SELECT
                     (SELECT COUNT(*) FROM queue_items WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled'))+
                     (SELECT COUNT(*) FROM development_workspaces WHERE repo_key=?1 AND status!='removed')+
                     (SELECT COUNT(*) FROM replication_debt WHERE repo_key=?1 AND outcome NOT IN ('succeeded','superseded'))",
                    [&repo_key],
                    |row| row.get(0),
                )?;
                if active != 0 {
                    anyhow::bail!("disabled repository retains nonterminal obligations");
                }
            }
        }
        let mut debts = connection.prepare(
             "SELECT id,repo_key,destination_key,replica_json,operation,outcome FROM replication_debt ORDER BY id",
        )?;
        for debt in debts.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })? {
            let (id, repo_key, destination_key, replica, operation, outcome) = debt?;
            let replica: crate::repository_policy::GitRepository = serde_json::from_str(&replica)?;
            if replica.clone().validate("stored replica")? != replica
                || replica.destination_identity_key()? != destination_key
            {
                anyhow::bail!("replication debt {id} destination identity is inconsistent");
            }
            let validated =
                SqliteQueue::validate_replication_binding(connection, &repo_key, &id, None)?;
            if operation != "pin_source"
                && !matches!(
                    outcome.as_str(),
                    "succeeded" | "superseded" | "superseded_cleanup_pending" | "applied"
                )
            {
                let owned_root: Vec<u8> = connection.query_row(
                    "SELECT owned_root_path FROM registered_repositories WHERE repo_key=?1",
                    [&repo_key],
                    |row| row.get(0),
                )?;
                let owned_root = PathBuf::from(OsString::from_vec(owned_root));
                let preserved_ref = format!("refs/iq/replication/{id}");
                let preserved = crate::git_command::output(
                    &owned_root,
                    ["rev-parse", "--verify", preserved_ref.as_str()],
                )?;
                if !preserved.status.success()
                    || String::from_utf8(preserved.stdout)?.trim() != validated.canonical_source_sha
                {
                    anyhow::bail!("replication debt {id} source pin is missing or inconsistent");
                }
            }
        }
        Ok(())
    }

    fn validate_schema3_identity(connection: &Connection) -> Result<String> {
        let expected = Connection::open_in_memory()?;
        configure_connection(&expected)?;
        expected.pragma_update(None, "foreign_keys", "ON")?;
        install_schema3_objects(&expected)?;
        let expected_objects = schema_objects(&expected)?;
        let actual_objects = schema_objects(connection)?;
        if expected_objects != actual_objects {
            let differences = expected_objects
                .keys()
                .chain(actual_objects.keys())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .filter(|key| expected_objects.get(*key) != actual_objects.get(*key))
                .map(|(object_type, name)| format!("{object_type}:{name}"))
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!(
                "migration source is not the exact IQ schema 3; differing objects: {differences}"
            );
        }
        let version: String = connection.query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )?;
        if version != "3" {
            anyhow::bail!("migration source schema must be 3");
        }
        let database_id: String = connection.query_row(
            "SELECT value FROM queue_metadata WHERE key='database_id'",
            [],
            |row| row.get(0),
        )?;
        if database_id.is_empty() {
            anyhow::bail!("migration source database ID must not be empty");
        }
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        let foreign_keys: i64 =
            connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })?;
        if integrity != "ok" || foreign_keys != 0 {
            anyhow::bail!("migration source integrity validation failed");
        }
        validate_registered_repository_rows(connection)?;
        crate::repository::validate_provisioning_rows(connection)?;
        crate::control_store::validate_control_contents(connection)?;
        Ok(database_id)
    }

    pub fn validate_existing_schema_identity(connection: &Connection) -> Result<String> {
        validate_schema_objects(connection)?;
        let version: String = connection.query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )?;
        if version != crate::repository::SCHEMA_VERSION {
            return incompatible_local_state();
        }
        let database_id: String = connection.query_row(
            "SELECT value FROM queue_metadata WHERE key='database_id'",
            [],
            |row| row.get(0),
        )?;
        if database_id.is_empty() {
            anyhow::bail!("database ID must not be empty");
        }
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return incompatible_local_state();
        }
        let foreign_keys: i64 =
            connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })?;
        let foreign_keys_enabled: i64 =
            connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
        if foreign_keys != 0 || foreign_keys_enabled != 1 {
            return incompatible_local_state();
        }
        validate_schema4_contents(connection).map_err(|error| {
            anyhow::anyhow!(
                "IQ local state is incompatible; schema-4 content is invalid: {error:#}"
            )
        })?;
        let mut repository_keys = connection.prepare("SELECT repo_key FROM repository_policies")?;
        let repository_keys = repository_keys
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if repository_keys
            .into_iter()
            .any(|key| Uuid::parse_str(&key).map_or(true, |identity| identity.to_string() != key))
        {
            return incompatible_local_state();
        }
        validate_registered_repository_rows(connection).map_err(|error| {
            anyhow::anyhow!(
                "IQ local state is incompatible; registered repository authority is invalid: {error:#}"
            )
        })?;
        crate::repository::validate_provisioning_rows(connection).map_err(|error| {
            anyhow::anyhow!(
                "IQ local state is incompatible; provisioning authority is invalid: {error:#}"
            )
        })?;
        crate::control_store::validate_control_contents(connection).map_err(|error| {
            anyhow::anyhow!(
                "IQ local state is incompatible; control authority is invalid: {error:#}"
            )
        })?;
        Ok(database_id)
    }

    fn validate_registered_repository_rows(connection: &Connection) -> Result<()> {
        let has_policy: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='repository_policies')",
            [],
            |row| row.get(0),
        )?;
        if !has_policy {
            let mut statement = connection.prepare(
                "SELECT repo_key,target_branch,remote_name,fetch_url,push_url,source_sha,checkout_json FROM registered_repositories",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })?;
            for row in rows {
                let (key, target, remote, fetch, push, source, checkout) = row?;
                crate::repository::RepoKey::from_stored(key)?;
                crate::repository::validate_target_branch(&target)?;
                crate::git_object::GitObjectFormat::Sha1
                    .require_oid(&source, "legacy registered source SHA")?;
                if remote != crate::repository::INTERNAL_REMOTE_NAME
                    || fetch.is_empty()
                    || push.is_empty()
                    || !serde_json::from_str::<CheckoutReconciliationState>(&checkout)?
                        .is_ready_for(&source)
                {
                    anyhow::bail!("legacy registered repository authority is invalid");
                }
            }
            return Ok(());
        }
        let mut statement = connection.prepare(
            "SELECT repository.repo_key,repository.owned_root_path,repository.root_rift_id,repository.registry_identity,repository.generation,policy.target_branch,repository.source_sha,repository.checkout_json,repository.development_root_path,repository.integration_root_path,policy.canonical_repository_json FROM registered_repositories repository JOIN repository_policies policy ON policy.repo_key=repository.repo_key",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Vec<u8>>(8)?,
                row.get::<_, Vec<u8>>(9)?,
                row.get::<_, String>(10)?,
            ))
        })?;
        for row in rows {
            let (
                repo_key,
                owned_root,
                root_rift_id,
                registry,
                generation,
                target,
                source_sha,
                checkout,
                development,
                integration,
                canonical_repository,
            ) = row?;
            let owned_root = PathBuf::from(OsString::from_vec(owned_root));
            let development = PathBuf::from(OsString::from_vec(development));
            let integration = PathBuf::from(OsString::from_vec(integration));
            let registry = PathBuf::from(OsString::from_vec(registry));
            let reservation = owned_root
                .parent()
                .context("owned repository root has no reservation parent")?;
            if !owned_root.is_absolute()
                || owned_root.file_name() != Some(OsStr::new("root"))
                || reservation.file_name() != Some(OsStr::new(&repo_key))
                || reservation.parent().and_then(Path::file_name)
                    != Some(OsStr::new("repositories"))
                || development != reservation.join("development")
                || integration != reservation.join("integration")
                || !registry.is_absolute()
                || root_rift_id.len() != 26
                || !root_rift_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric())
                || generation < 0
            {
                anyhow::bail!("registered repository structural authority is invalid");
            }
            crate::repository::validate_target_branch(&target)?;
            let object_format = serde_json::from_str::<crate::repository_policy::GitRepository>(
                &canonical_repository,
            )?
            .object_format();
            object_format.require_oid(&source_sha, "registered source SHA")?;
            let checkout: CheckoutReconciliationState = serde_json::from_str(&checkout)?;
            object_format.require_oid(checkout.target_sha(), "registered checkout target")?;
            if matches!(checkout, CheckoutReconciliationState::Ready(_))
                && !checkout.is_ready_for(&source_sha)
            {
                anyhow::bail!("registered ready checkout differs from source SHA");
            }
        }
        Ok(())
    }

    pub(crate) fn validate_schema_objects(connection: &Connection) -> Result<()> {
        let expected = Connection::open_in_memory()?;
        configure_connection(&expected)?;
        expected.pragma_update(None, "foreign_keys", "ON")?;
        install_schema(&expected)?;
        let expected_objects = schema_objects(&expected)?;
        let actual_objects = schema_objects(connection)?;
        if actual_objects != expected_objects {
            if let Some((identity, _)) = expected_objects
                .iter()
                .find(|(identity, _)| !actual_objects.contains_key(*identity))
            {
                anyhow::bail!(
                    "IQ local state is incompatible; missing schema object {} {}",
                    identity.0,
                    identity.1
                );
            }
            if let Some((identity, _)) = actual_objects
                .iter()
                .find(|(identity, _)| !expected_objects.contains_key(*identity))
            {
                anyhow::bail!(
                    "IQ local state is incompatible; unexpected schema object {} {}",
                    identity.0,
                    identity.1
                );
            }
            let identity = expected_objects
                .keys()
                .find(|identity| actual_objects.get(*identity) != expected_objects.get(*identity))
                .context("schema object maps differ without an identifiable object")?;
            anyhow::bail!(
                "IQ local state is incompatible; schema object {} {} differs",
                identity.0,
                identity.1
            );
        }
        Ok(())
    }

    fn incompatible_local_state<T>() -> Result<T> {
        anyhow::bail!("IQ local state is incompatible; remove it and reinitialize IQ")
    }

    fn schema_objects(
        connection: &Connection,
    ) -> Result<std::collections::BTreeMap<(String, String), String>> {
        let mut statement =
            connection.prepare("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name")?;
        let objects = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?
            .filter_map(|row| match row {
                Ok((object_type, name, None))
                    if object_type == "index"
                        && name.starts_with("sqlite_autoindex_")
                        && name["sqlite_autoindex_".len()..]
                            .rsplit_once('_')
                            .is_some_and(|(table, ordinal)| {
                                !table.is_empty()
                                    && ordinal.parse::<u32>().is_ok_and(|value| value > 0)
                            }) =>
                {
                    None
                }
                Ok((object_type, name, sql)) => Some(Ok((
                    (object_type, name),
                    canonical_schema_sql(sql.as_deref().unwrap_or_default()),
                ))),
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<_, _>>()?;
        Ok(objects)
    }

    fn canonical_schema_sql(sql: &str) -> String {
        #[derive(Clone, Copy)]
        enum Quote {
            Single,
            Double,
            Backtick,
            Bracket,
        }

        let mut canonical = String::with_capacity(sql.len());
        let mut quote = None;
        let mut whitespace = false;
        let mut chars = sql.chars().peekable();
        while let Some(character) = chars.next() {
            if let Some(active) = quote {
                canonical.push(character);
                let closing = match active {
                    Quote::Single => '\'',
                    Quote::Double => '"',
                    Quote::Backtick => '`',
                    Quote::Bracket => ']',
                };
                if character == closing {
                    if !matches!(active, Quote::Bracket) && chars.peek() == Some(&closing) {
                        canonical.push(chars.next().unwrap());
                    } else {
                        quote = None;
                    }
                }
                continue;
            }
            if character.is_ascii_whitespace() {
                whitespace = !canonical.is_empty();
            } else {
                if whitespace {
                    canonical.push(' ');
                    whitespace = false;
                }
                canonical.push(character);
                quote = match character {
                    '\'' => Some(Quote::Single),
                    '"' => Some(Quote::Double),
                    '`' => Some(Quote::Backtick),
                    '[' => Some(Quote::Bracket),
                    _ => None,
                };
            }
        }
        canonical
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
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  source_branch TEXT NOT NULL,
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
ON queue_items(repo_key, source_branch)
WHERE status NOT IN ('integrated','cancelled');

CREATE VIEW queue_items_runtime AS
SELECT item.*,CAST(repository.owned_root_path AS TEXT) AS owned_root_path,repository.target_branch
FROM queue_items item JOIN registered_repositories repository ON repository.repo_key=item.repo_key;

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

CREATE TABLE IF NOT EXISTS validation_invocations (
  attempt_id TEXT NOT NULL REFERENCES integration_attempts(id) ON DELETE CASCADE,
  invocation_number INTEGER NOT NULL CHECK(invocation_number>0),
  target_base_sha TEXT NOT NULL CHECK(length(target_base_sha) IN (40,64) AND target_base_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  candidate_sha TEXT NOT NULL CHECK(length(candidate_sha) IN (40,64) AND candidate_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  command TEXT NOT NULL CHECK(command!=''),
  exit_code INTEGER NOT NULL,
  log_path TEXT NOT NULL CHECK(log_path!=''),
  validated_commit_sha TEXT CHECK(validated_commit_sha IS NULL OR validated_commit_sha=candidate_sha),
  invalidated_at TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(attempt_id,invocation_number)
);

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
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key) DEFERRABLE INITIALLY DEFERRED,
  kind TEXT NOT NULL CHECK(kind IN ('development','integration')),
  root_path BLOB NOT NULL UNIQUE,
  source_path BLOB NOT NULL,
  source_rift_id TEXT NOT NULL CHECK(source_rift_id!=''),
  registry_identity BLOB NOT NULL,
  registry_device INTEGER NOT NULL CHECK(registry_device>=0),
  registry_inode INTEGER NOT NULL CHECK(registry_inode>0),
  generation INTEGER NOT NULL CHECK(generation>=0),
  pending_generation INTEGER CHECK(pending_generation IS NULL OR pending_generation=generation+1),
  PRIMARY KEY(repo_key,kind),
  UNIQUE(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode)
);

CREATE TABLE IF NOT EXISTS workspace_gc_debt (
  registry_identity TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
);
"#;

    const SCHEMA4: &str = r#"
CREATE TABLE IF NOT EXISTS queue_items (
  id TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  producer_metadata_json TEXT NOT NULL,
  validation_evidence_json TEXT NOT NULL,
  status TEXT NOT NULL,
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
  replacement_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

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

CREATE TABLE IF NOT EXISTS validation_invocations (
  attempt_id TEXT NOT NULL REFERENCES integration_attempts(id) ON DELETE CASCADE,
  invocation_number INTEGER NOT NULL CHECK(invocation_number>0),
  target_base_sha TEXT NOT NULL CHECK(length(target_base_sha) IN (40,64) AND target_base_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  candidate_sha TEXT NOT NULL CHECK(length(candidate_sha) IN (40,64) AND candidate_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  command TEXT NOT NULL CHECK(command!=''),
  exit_code INTEGER NOT NULL,
  log_path TEXT NOT NULL CHECK(log_path!=''),
  validated_commit_sha TEXT CHECK(validated_commit_sha IS NULL OR validated_commit_sha=candidate_sha),
  invalidated_at TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(attempt_id,invocation_number)
);

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

CREATE TABLE IF NOT EXISTS repo_leases (
  repo_key TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS physical_repository_leases (
  identity_key TEXT PRIMARY KEY REFERENCES physical_repository_ownership(identity_key),
  repo_key TEXT NOT NULL REFERENCES repository_policies(repo_key),
  owner_id TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_roots (
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key) DEFERRABLE INITIALLY DEFERRED,
  kind TEXT NOT NULL CHECK(kind IN ('development','integration')),
  root_path BLOB NOT NULL UNIQUE,
  source_path BLOB NOT NULL,
  source_rift_id TEXT NOT NULL CHECK(source_rift_id!=''),
  registry_identity BLOB NOT NULL,
  registry_device INTEGER NOT NULL CHECK(registry_device>=0),
  registry_inode INTEGER NOT NULL CHECK(registry_inode>0),
  generation INTEGER NOT NULL CHECK(generation>=0),
  pending_generation INTEGER CHECK(pending_generation IS NULL OR pending_generation=generation+1),
  PRIMARY KEY(repo_key,kind),
  UNIQUE(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode)
);

CREATE TABLE IF NOT EXISTS workspace_gc_debt (
  registry_identity TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
);
"#;

    const COMPOSITION_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS repository_remote_owners (
  repo_key TEXT PRIMARY KEY CHECK(length(repo_key)=36 AND substr(repo_key,9,1)='-' AND substr(repo_key,14,1)='-' AND substr(repo_key,19,1)='-' AND substr(repo_key,24,1)='-' AND lower(repo_key)=repo_key AND repo_key NOT GLOB '*[^0-9a-f-]*'),
  fetch_url TEXT NOT NULL CHECK(fetch_url!=''),
  push_url TEXT NOT NULL CHECK(push_url!=''),
  target_branch TEXT NOT NULL CHECK(target_branch IN ('main','master')),
  created_at TEXT NOT NULL,
  UNIQUE(fetch_url,push_url),
  UNIQUE(repo_key,fetch_url,push_url,target_branch)
);

CREATE TABLE IF NOT EXISTS repository_bootstrap_requests (
  request_path BLOB PRIMARY KEY,
  target_branch TEXT NOT NULL CHECK(target_branch IN ('main','master')),
  remote_name TEXT NOT NULL CHECK(remote_name!=''),
  storage_root_path BLOB NOT NULL,
  rift_registry_path BLOB NOT NULL,
  repo_key TEXT REFERENCES repository_remote_owners(repo_key),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS registered_repositories (
  repo_key TEXT PRIMARY KEY REFERENCES repository_remote_owners(repo_key),
  owned_root_path BLOB NOT NULL UNIQUE,
  root_rift_id TEXT NOT NULL CHECK(root_rift_id!=''),
  registry_identity BLOB NOT NULL,
  registry_device INTEGER NOT NULL CHECK(registry_device>=0),
  registry_inode INTEGER NOT NULL CHECK(registry_inode>0),
  generation INTEGER NOT NULL CHECK(generation>=0),
  remote_name TEXT NOT NULL CHECK(remote_name='iq-target'),
  fetch_url TEXT NOT NULL CHECK(fetch_url!=''),
  push_url TEXT NOT NULL CHECK(push_url!=''),
  target_branch TEXT NOT NULL CHECK(target_branch IN ('main','master')),
  source_sha TEXT NOT NULL CHECK(length(source_sha) IN (40,64) AND source_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  checkout_json TEXT NOT NULL CHECK(json_valid(checkout_json)),
  development_root_path BLOB NOT NULL UNIQUE,
  development_kind TEXT NOT NULL DEFAULT 'development' CHECK(development_kind='development'),
  integration_root_path BLOB NOT NULL UNIQUE,
  integration_kind TEXT NOT NULL DEFAULT 'integration' CHECK(integration_kind='integration'),
  provisioning_json TEXT NOT NULL CHECK(json_valid(provisioning_json) AND json_extract(provisioning_json,'$.state')='ready'),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(repo_key,fetch_url,push_url,target_branch) REFERENCES repository_remote_owners(repo_key,fetch_url,push_url,target_branch),
  FOREIGN KEY(repo_key,development_kind,development_root_path,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode)
    REFERENCES workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(repo_key,integration_kind,integration_root_path,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode)
    REFERENCES workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS repository_provisioning_intents (
  repo_key TEXT PRIMARY KEY REFERENCES repository_remote_owners(repo_key),
  bootstrap_path BLOB NOT NULL UNIQUE,
  owned_root_path BLOB NOT NULL UNIQUE,
  staging_root_path BLOB NOT NULL UNIQUE,
  rift_registry_path BLOB NOT NULL,
  target_branch TEXT NOT NULL CHECK(target_branch IN ('main','master')),
  fetch_url TEXT NOT NULL CHECK(fetch_url!=''),
  push_url TEXT NOT NULL CHECK(push_url!=''),
  source_sha TEXT NOT NULL CHECK(length(source_sha) IN (40,64) AND source_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  policy_bytes BLOB,
  lifecycle_json TEXT NOT NULL CHECK(json_valid(lifecycle_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(repo_key,fetch_url,push_url,target_branch) REFERENCES repository_remote_owners(repo_key,fetch_url,push_url,target_branch)
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

    const COMPOSITION_SCHEMA4: &str = r#"
CREATE TABLE IF NOT EXISTS repository_bootstrap_requests (
  request_path BLOB PRIMARY KEY,
  storage_root_path BLOB NOT NULL,
  rift_registry_path BLOB NOT NULL,
  repo_key TEXT REFERENCES repository_policies(repo_key),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS registered_repositories (
  repo_key TEXT PRIMARY KEY REFERENCES repository_policies(repo_key),
  owned_root_path BLOB NOT NULL UNIQUE,
  git_binding_json TEXT NOT NULL CHECK(json_valid(git_binding_json)),
  root_rift_id TEXT NOT NULL CHECK(root_rift_id!=''),
  registry_identity BLOB NOT NULL,
  registry_device INTEGER NOT NULL CHECK(registry_device>=0),
  registry_inode INTEGER NOT NULL CHECK(registry_inode>0),
  generation INTEGER NOT NULL CHECK(generation>=0),
  source_sha TEXT NOT NULL CHECK(length(source_sha) IN (40,64) AND source_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  checkout_json TEXT NOT NULL CHECK(json_valid(checkout_json)),
  development_root_path BLOB NOT NULL UNIQUE,
  development_kind TEXT NOT NULL DEFAULT 'development' CHECK(development_kind='development'),
  integration_root_path BLOB NOT NULL UNIQUE,
  integration_kind TEXT NOT NULL DEFAULT 'integration' CHECK(integration_kind='integration'),
  provisioning_json TEXT NOT NULL CHECK(json_valid(provisioning_json) AND json_extract(provisioning_json,'$.state')='ready'),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(repo_key,development_kind,development_root_path,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode)
    REFERENCES workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(repo_key,integration_kind,integration_root_path,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode)
    REFERENCES workspace_roots(repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS repository_provisioning_intents (
  repo_key TEXT PRIMARY KEY REFERENCES repository_policies(repo_key),
  bootstrap_path BLOB NOT NULL UNIQUE,
  owned_root_path BLOB NOT NULL UNIQUE,
  staging_root_path BLOB NOT NULL UNIQUE,
  rift_registry_path BLOB NOT NULL,
  source_sha TEXT NOT NULL CHECK(length(source_sha) IN (40,64) AND source_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  policy_bytes BLOB,
  lifecycle_json TEXT NOT NULL CHECK(json_valid(lifecycle_json)),
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

    const REPOSITORY_POLICY_TABLE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS repository_policies (
  repo_key TEXT PRIMARY KEY CHECK(length(repo_key)=36 AND substr(repo_key,9,1)='-' AND substr(repo_key,14,1)='-' AND substr(repo_key,19,1)='-' AND substr(repo_key,24,1)='-' AND lower(repo_key)=repo_key AND repo_key NOT GLOB '*[^0-9a-f-]*'),
  revision INTEGER NOT NULL CHECK(revision>0),
  operation_state_json TEXT NOT NULL CHECK(json_valid(operation_state_json)),
  canonical_repository_json TEXT NOT NULL CHECK(json_valid(canonical_repository_json)),
  canonical_ownership_key TEXT NOT NULL UNIQUE,
  target_branch TEXT NOT NULL CHECK(target_branch IN ('main','master')),
  integration_policy TEXT NOT NULL CHECK(integration_policy IN ('direct','merge_request_required')),
  replication_policy_json TEXT NOT NULL CHECK(json_valid(replication_policy_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(repo_key,target_branch)
);

CREATE TABLE IF NOT EXISTS physical_repository_ownership (
  identity_key TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL REFERENCES repository_policies(repo_key),
  role TEXT NOT NULL CHECK(role IN ('canonical','replica')),
  ordinal INTEGER NOT NULL CHECK(ordinal>=0),
  repository_json TEXT NOT NULL CHECK(json_valid(repository_json)),
  created_at TEXT NOT NULL,
  UNIQUE(repo_key,role,ordinal)
);
"#;

    const REPOSITORY_POLICY_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS repository_policies (
  repo_key TEXT PRIMARY KEY CHECK(length(repo_key)=36 AND substr(repo_key,9,1)='-' AND substr(repo_key,14,1)='-' AND substr(repo_key,19,1)='-' AND substr(repo_key,24,1)='-' AND lower(repo_key)=repo_key AND repo_key NOT GLOB '*[^0-9a-f-]*'),
  revision INTEGER NOT NULL CHECK(revision>0),
  operation_state_json TEXT NOT NULL CHECK(json_valid(operation_state_json)),
  canonical_repository_json TEXT NOT NULL CHECK(json_valid(canonical_repository_json)),
  canonical_ownership_key TEXT NOT NULL UNIQUE,
  target_branch TEXT NOT NULL CHECK(target_branch IN ('main','master')),
  integration_policy TEXT NOT NULL CHECK(integration_policy IN ('direct','merge_request_required')),
  replication_policy_json TEXT NOT NULL CHECK(json_valid(replication_policy_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(repo_key,target_branch)
);

CREATE TABLE IF NOT EXISTS physical_repository_ownership (
  identity_key TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL REFERENCES repository_policies(repo_key),
  role TEXT NOT NULL CHECK(role IN ('canonical','replica')),
  ordinal INTEGER NOT NULL CHECK(ordinal>=0),
  repository_json TEXT NOT NULL CHECK(json_valid(repository_json)),
  created_at TEXT NOT NULL,
  UNIQUE(repo_key,role,ordinal)
);

CREATE TRIGGER IF NOT EXISTS repository_policy_insert_conflict_guard
BEFORE INSERT ON repository_policies
WHEN EXISTS(
  SELECT 1 FROM repository_policies existing
  WHERE existing.repo_key=NEW.repo_key
     OR existing.canonical_ownership_key=NEW.canonical_ownership_key
     OR (existing.repo_key=NEW.repo_key AND existing.target_branch=NEW.target_branch)
)
BEGIN SELECT RAISE(ABORT,'repository policy authority already exists'); END;

CREATE TRIGGER IF NOT EXISTS physical_repository_ownership_insert_conflict_guard
BEFORE INSERT ON physical_repository_ownership
WHEN EXISTS(
  SELECT 1 FROM physical_repository_ownership existing
  WHERE existing.identity_key=NEW.identity_key
     OR (existing.repo_key=NEW.repo_key AND existing.role=NEW.role AND existing.ordinal=NEW.ordinal)
)
BEGIN SELECT RAISE(ABORT,'physical repository ownership authority already exists'); END;

CREATE TRIGGER IF NOT EXISTS physical_repository_ownership_immutable
BEFORE UPDATE ON physical_repository_ownership
BEGIN SELECT RAISE(ABORT,'physical repository ownership is immutable'); END;

CREATE TRIGGER IF NOT EXISTS physical_repository_ownership_delete_guard
BEFORE DELETE ON physical_repository_ownership
BEGIN SELECT RAISE(ABORT,'physical repository ownership is immutable'); END;

CREATE TRIGGER IF NOT EXISTS repository_policy_authority_immutable
BEFORE UPDATE ON repository_policies
WHEN NEW.repo_key!=OLD.repo_key
  OR NEW.canonical_repository_json!=OLD.canonical_repository_json
  OR NEW.canonical_ownership_key!=OLD.canonical_ownership_key
  OR NEW.target_branch!=OLD.target_branch
  OR NEW.integration_policy!=OLD.integration_policy
  OR NEW.replication_policy_json!=OLD.replication_policy_json
  OR NEW.created_at!=OLD.created_at
BEGIN SELECT RAISE(ABORT,'repository policy authority is immutable'); END;

CREATE TRIGGER IF NOT EXISTS repository_policy_operation_transition
BEFORE UPDATE OF operation_state_json,revision ON repository_policies
WHEN NOT (
  NEW.revision=OLD.revision+1
  AND (
    (json_extract(OLD.operation_state_json,'$.state')='enabled'
      AND json_extract(NEW.operation_state_json,'$.state')='draining'
      AND json_type(NEW.operation_state_json,'$.obligations')='array')
    OR
    (json_extract(OLD.operation_state_json,'$.state')='draining'
      AND NEW.operation_state_json='{"state":"disabled"}')
  )
)
BEGIN SELECT RAISE(ABORT,'invalid repository operation-state transition'); END;

CREATE TRIGGER IF NOT EXISTS repository_policy_delete_guard
BEFORE DELETE ON repository_policies
BEGIN SELECT RAISE(ABORT,'repository policy authority is immutable'); END;

CREATE TABLE IF NOT EXISTS queue_admissions (
  item_id TEXT PRIMARY KEY REFERENCES queue_items(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK(kind IN ('local_submission','direct','merge_request','historical_merge_request')),
  source_branch TEXT NOT NULL,
  head_sha TEXT NOT NULL CHECK(length(head_sha) IN (40,64) AND head_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  source_ref TEXT,
  submission_id TEXT REFERENCES local_submissions(id),
  provider TEXT CHECK(provider IS NULL OR provider IN ('github','gitlab')),
  provider_host TEXT,
  provider_repository TEXT,
  provider_repository_id TEXT,
  target_branch TEXT,
  base_sha TEXT CHECK(base_sha IS NULL OR (length(base_sha) IN (40,64) AND base_sha NOT GLOB '*[^0-9A-Fa-f]*')),
  provider_merge_method TEXT CHECK(provider_merge_method IS NULL OR provider_merge_method IN ('merge','squash')),
  merge_request_identity TEXT,
  merge_request_url TEXT,
  admitted_at TEXT NOT NULL,
  CHECK(
    (kind='local_submission' AND source_ref IS NOT NULL AND submission_id IS NOT NULL AND provider IS NULL AND provider_host IS NULL AND provider_repository IS NULL AND provider_repository_id IS NULL AND target_branch IS NULL AND base_sha IS NULL AND provider_merge_method IS NULL AND merge_request_identity IS NULL AND merge_request_url IS NULL) OR
    (kind='direct' AND source_ref IS NULL AND submission_id IS NULL AND provider IS NULL AND provider_host IS NULL AND provider_repository IS NULL AND provider_repository_id IS NULL AND target_branch IS NULL AND base_sha IS NULL AND provider_merge_method IS NULL AND merge_request_identity IS NULL AND merge_request_url IS NULL) OR
    (kind='merge_request' AND source_ref IS NULL AND submission_id IS NULL AND provider IS NOT NULL AND provider_host IS NOT NULL AND provider_repository IS NOT NULL AND provider_repository_id IS NOT NULL AND target_branch IN ('main','master') AND base_sha IS NOT NULL AND merge_request_identity IS NOT NULL AND merge_request_identity!='' AND merge_request_url IS NOT NULL AND merge_request_url!='') OR
    (kind='historical_merge_request' AND source_ref IS NULL AND submission_id IS NULL AND provider IS NOT NULL AND provider_host IS NOT NULL AND provider_repository IS NOT NULL AND provider_repository_id IS NOT NULL AND target_branch IN ('main','master') AND merge_request_identity IS NOT NULL AND merge_request_identity!='' AND merge_request_url IS NOT NULL AND merge_request_url!='')
  )
);

CREATE TRIGGER queue_admission_identity_immutable
BEFORE UPDATE ON queue_admissions
BEGIN SELECT RAISE(ABORT,'queue admission identity is immutable'); END;

CREATE TRIGGER queue_admission_insert_conflict_guard
BEFORE INSERT ON queue_admissions
WHEN EXISTS(
  SELECT 1 FROM queue_admissions admission
  WHERE admission.item_id=NEW.item_id
    AND (admission.kind IS NOT NEW.kind
      OR admission.source_branch IS NOT NEW.source_branch
      OR admission.head_sha IS NOT NEW.head_sha
      OR admission.source_ref IS NOT NEW.source_ref
      OR admission.submission_id IS NOT NEW.submission_id
      OR admission.provider IS NOT NEW.provider
      OR admission.provider_host IS NOT NEW.provider_host
      OR admission.provider_repository IS NOT NEW.provider_repository
      OR admission.provider_repository_id IS NOT NEW.provider_repository_id
      OR admission.target_branch IS NOT NEW.target_branch
      OR admission.base_sha IS NOT NEW.base_sha
      OR admission.provider_merge_method IS NOT NEW.provider_merge_method
      OR admission.merge_request_identity IS NOT NEW.merge_request_identity
      OR admission.merge_request_url IS NOT NEW.merge_request_url
      OR admission.admitted_at IS NOT NEW.admitted_at)
)
BEGIN SELECT RAISE(ABORT,'queue admission insert conflicts with immutable identity'); END;

CREATE TRIGGER queue_admission_delete_guard
BEFORE DELETE ON queue_admissions
WHEN EXISTS(SELECT 1 FROM queue_items WHERE id=OLD.item_id)
  AND NOT EXISTS(SELECT 1 FROM queue_item_purge_authority WHERE item_id=OLD.item_id)
BEGIN SELECT RAISE(ABORT,'queue admission authority cannot be removed before queue-item purge'); END;

CREATE TABLE IF NOT EXISTS queue_item_purge_authority (
  item_id TEXT PRIMARY KEY,
  authorized_at TEXT NOT NULL
);

CREATE TRIGGER queue_item_purge_authority_guard
BEFORE INSERT ON queue_item_purge_authority
WHEN NOT EXISTS(
    SELECT 1 FROM queue_items item
    WHERE item.id=NEW.item_id
      AND item.status IN ('integrated','cancelled')
      AND item.integration_workspace_path IS NULL
      AND item.integration_workspace_rift_id IS NULL
      AND item.integration_workspace_source_rift_id IS NULL
  )
  OR EXISTS(SELECT 1 FROM integration_attempts WHERE item_id=NEW.item_id AND (finished_at IS NULL OR result IS NULL))
  OR EXISTS(SELECT 1 FROM integration_efforts WHERE item_id=NEW.item_id AND state NOT IN ('integrated','cancelled'))
  OR EXISTS(SELECT 1 FROM integration_efforts effort JOIN integration_cycles cycle ON cycle.effort_id=effort.id WHERE effort.item_id=NEW.item_id AND cycle.status IN ('starting','running'))
  OR EXISTS(SELECT 1 FROM integration_efforts effort JOIN runner_termination_debt debt ON debt.effort_id=effort.id WHERE effort.item_id=NEW.item_id)
  OR EXISTS(SELECT 1 FROM integration_efforts effort JOIN guidance_requests request ON request.effort_id=effort.id WHERE effort.item_id=NEW.item_id AND request.status='open')
  OR EXISTS(SELECT 1 FROM prompts WHERE item_id=NEW.item_id AND status='open')
  OR EXISTS(SELECT 1 FROM integration_efforts effort JOIN projection_debt debt ON debt.effort_id=effort.id WHERE effort.item_id=NEW.item_id)
  OR EXISTS(SELECT 1 FROM integration_efforts effort JOIN state_repository_artifacts artifact ON artifact.effort_id=effort.id WHERE effort.item_id=NEW.item_id AND artifact.state!='closed')
  OR EXISTS(SELECT 1 FROM item_state_repository_bindings WHERE item_id=NEW.item_id AND reservation_state='pending')
  OR EXISTS(SELECT 1 FROM item_state_repository_reservations WHERE item_id=NEW.item_id)
  OR EXISTS(SELECT 1 FROM terminal_workspace_cleanup_debt WHERE item_id=NEW.item_id)
  OR EXISTS(SELECT 1 FROM durable_events event JOIN notification_deliveries delivery ON delivery.event_id=event.id WHERE event.item_id=NEW.item_id AND delivery.state NOT IN ('delivered','failed','expired'))
  OR EXISTS(SELECT 1 FROM replication_debt WHERE item_id=NEW.item_id AND outcome NOT IN ('succeeded','superseded'))
  OR EXISTS(SELECT 1 FROM integration_attempts attempt JOIN private_ref_cleanup_debt debt ON debt.owner_id=attempt.id WHERE attempt.item_id=NEW.item_id AND debt.kind='landing')
BEGIN SELECT RAISE(ABORT,'queue-item purge requires terminal state with no unfinished obligations'); END;

CREATE TRIGGER queue_item_delete_guard
BEFORE DELETE ON queue_items
WHEN NOT EXISTS(SELECT 1 FROM queue_item_purge_authority WHERE item_id=OLD.id)
BEGIN SELECT RAISE(ABORT,'queue-item deletion requires explicit purge authority'); END;

CREATE TRIGGER queue_item_purge_authority_cleanup
AFTER DELETE ON queue_items
BEGIN DELETE FROM queue_item_purge_authority WHERE item_id=OLD.id; END;

CREATE TRIGGER historical_merge_request_terminal_insert
BEFORE INSERT ON queue_admissions
WHEN NEW.kind='historical_merge_request' AND NOT EXISTS(
  SELECT 1 FROM queue_items WHERE id=NEW.item_id AND status IN ('integrated','cancelled')
)
BEGIN SELECT RAISE(ABORT,'historical MR admission requires a terminal queue item'); END;

CREATE TABLE IF NOT EXISTS workspace_git_bindings (
  owner_kind TEXT NOT NULL CHECK(owner_kind IN ('development','integration')),
  owner_id TEXT NOT NULL,
  top_level BLOB NOT NULL UNIQUE,
  binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
  created_at TEXT NOT NULL,
  PRIMARY KEY(owner_kind,owner_id)
);

CREATE TRIGGER IF NOT EXISTS workspace_git_binding_immutable
BEFORE UPDATE ON workspace_git_bindings
BEGIN SELECT RAISE(ABORT,'workspace Git binding is immutable'); END;

CREATE TABLE IF NOT EXISTS private_ref_cleanup_debt (
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  kind TEXT NOT NULL CHECK(kind IN ('repository_target','landing')),
  owner_id TEXT NOT NULL CHECK(owner_id!=''),
  ref_name TEXT NOT NULL CHECK(ref_name!=''),
  expected_sha TEXT NOT NULL CHECK(length(expected_sha) IN (40,64) AND expected_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(repo_key,ref_name),
  CHECK(
    (kind='repository_target' AND owner_id=expected_sha AND ref_name='refs/iq/repository-targets/'||repo_key||'/'||expected_sha) OR
    (kind='landing' AND instr(owner_id,'/')=0 AND ref_name='refs/iq/landings/'||owner_id)
  )
);

CREATE TRIGGER IF NOT EXISTS private_ref_cleanup_identity_immutable
BEFORE UPDATE OF repo_key,kind,owner_id,ref_name,expected_sha,created_at ON private_ref_cleanup_debt
BEGIN SELECT RAISE(ABORT,'private-ref cleanup identity is immutable'); END;

CREATE TABLE IF NOT EXISTS provider_landing_guarantees (
  item_id TEXT PRIMARY KEY REFERENCES queue_items(id) ON DELETE CASCADE,
  provider TEXT NOT NULL CHECK(provider IN ('github','gitlab')),
  provider_host TEXT NOT NULL,
  provider_repository TEXT NOT NULL,
  provider_repository_id TEXT NOT NULL,
  merge_request_identity TEXT NOT NULL,
  admitted_base_sha TEXT NOT NULL,
  admitted_head_sha TEXT NOT NULL,
  validated_target_sha TEXT NOT NULL,
  validated_candidate_sha TEXT NOT NULL,
  validated_tree_sha TEXT NOT NULL,
  landed_commit_sha TEXT NOT NULL,
  landed_tree_sha TEXT NOT NULL,
  first_parent_sha TEXT NOT NULL,
  history_contract TEXT NOT NULL CHECK(history_contract IN ('preserve_head','squash')),
  contains_admitted_head INTEGER NOT NULL CHECK(contains_admitted_head IN (0,1)),
  verified_at TEXT NOT NULL,
  CHECK(history_contract='squash' OR contains_admitted_head=1)
);

CREATE TABLE IF NOT EXISTS replication_debt (
  id TEXT PRIMARY KEY,
  item_id TEXT NOT NULL REFERENCES queue_items(id),
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  canonical_source_sha TEXT NOT NULL CHECK(length(canonical_source_sha) IN (40,64) AND canonical_source_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  destination_key TEXT NOT NULL,
  target_branch TEXT NOT NULL CHECK(target_branch IN ('main','master')),
  sequence INTEGER NOT NULL CHECK(sequence>0),
  replica_json TEXT NOT NULL CHECK(json_valid(replica_json)),
  expected_destination_sha TEXT,
  operation TEXT NOT NULL CHECK(operation IN ('pin_source','resolve_destination','advance_exact_target')),
  outcome TEXT NOT NULL CHECK(outcome IN ('pinning','pending','applying','uncertain','applied','succeeded','failed','superseded_cleanup_pending','superseded')),
  application_id TEXT,
  failure TEXT,
  superseded_by_id TEXT REFERENCES replication_debt(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK((operation IN ('pin_source','resolve_destination') AND expected_destination_sha IS NULL) OR (operation='advance_exact_target' AND expected_destination_sha IS NOT NULL)),
  CHECK((outcome IN ('failed','uncertain') AND failure IS NOT NULL AND failure!='') OR (outcome NOT IN ('failed','uncertain') AND failure IS NULL)),
  CHECK((outcome IN ('applying','uncertain') AND application_id IS NOT NULL) OR (outcome NOT IN ('applying','uncertain') AND application_id IS NULL)),
  CHECK((outcome='pinning' AND operation='pin_source') OR outcome!='pinning'),
  CHECK((outcome IN ('superseded_cleanup_pending','superseded') AND superseded_by_id IS NOT NULL) OR (outcome NOT IN ('superseded_cleanup_pending','superseded') AND superseded_by_id IS NULL)),
  UNIQUE(item_id,destination_key),
  UNIQUE(destination_key,target_branch,sequence)
);

CREATE TRIGGER IF NOT EXISTS replication_debt_identity_immutable
BEFORE UPDATE OF id,item_id,repo_key,canonical_source_sha,destination_key,target_branch,sequence,replica_json,created_at
ON replication_debt
BEGIN SELECT RAISE(ABORT,'replication debt identity is immutable'); END;

CREATE VIEW queue_items_runtime AS
SELECT item.*,CAST(repository.owned_root_path AS TEXT) AS owned_root_path,policy.target_branch,
       admission.kind AS admission_kind,admission.head_sha AS admission_head_sha,
       admission.source_branch AS admission_source_branch,
       admission.source_ref AS admission_source_ref,
       admission.submission_id AS admission_submission_id,
       admission.provider AS admission_provider,
       admission.provider_host AS admission_provider_host,
       admission.provider_repository AS admission_provider_repository,
       admission.provider_repository_id AS admission_provider_repository_id,
       admission.target_branch AS admission_target_branch,
       admission.base_sha AS admission_base_sha,
       admission.provider_merge_method AS admission_provider_merge_method,
       admission.merge_request_identity AS admission_merge_request_identity,
       admission.merge_request_url AS admission_merge_request_url
FROM queue_items item
JOIN registered_repositories repository ON repository.repo_key=item.repo_key
JOIN repository_policies policy ON policy.repo_key=item.repo_key
JOIN queue_admissions admission ON admission.item_id=item.id;
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

    const REGISTERED_REPOSITORY_TRIGGERS: &str = r#"
CREATE TRIGGER registered_repository_path_identity_insert
BEFORE INSERT ON registered_repositories
WHEN NEW.owned_root_path=NEW.development_root_path
  OR NEW.owned_root_path=NEW.integration_root_path
  OR NEW.development_root_path=NEW.integration_root_path
  OR EXISTS(
    SELECT 1 FROM registered_repositories existing
    WHERE NEW.owned_root_path IN (existing.owned_root_path,existing.development_root_path,existing.integration_root_path)
       OR NEW.development_root_path IN (existing.owned_root_path,existing.development_root_path,existing.integration_root_path)
       OR NEW.integration_root_path IN (existing.owned_root_path,existing.development_root_path,existing.integration_root_path)
  )
  OR EXISTS(
    SELECT 1 FROM workspace_roots root
    WHERE root.root_path IN (NEW.owned_root_path,NEW.development_root_path,NEW.integration_root_path)
  )
BEGIN SELECT RAISE(ABORT,'owned repository paths overlap existing repository authority'); END;

CREATE TRIGGER registered_repository_excludes_provisioning_intent
BEFORE INSERT ON registered_repositories
WHEN EXISTS(SELECT 1 FROM repository_provisioning_intents intent WHERE intent.repo_key=NEW.repo_key)
BEGIN SELECT RAISE(ABORT,'ready repository cannot coexist with provisioning intent'); END;

CREATE TRIGGER repository_provisioning_intent_excludes_ready
BEFORE INSERT ON repository_provisioning_intents
WHEN EXISTS(SELECT 1 FROM registered_repositories repository WHERE repository.repo_key=NEW.repo_key)
BEGIN SELECT RAISE(ABORT,'provisioning intent cannot coexist with ready repository'); END;

CREATE TRIGGER repository_remote_owner_identity_immutable
BEFORE UPDATE OF repo_key,fetch_url,push_url,target_branch,created_at ON repository_remote_owners
BEGIN SELECT RAISE(ABORT,'repository remote ownership is immutable'); END;

CREATE TRIGGER registered_repository_identity_immutable
BEFORE UPDATE OF repo_key,owned_root_path,root_rift_id,registry_identity,registry_device,registry_inode,remote_name,fetch_url,push_url,target_branch,development_root_path,development_kind,integration_root_path,integration_kind,created_at ON registered_repositories
BEGIN SELECT RAISE(ABORT,'owned repository identity is immutable'); END;

CREATE TRIGGER registered_repository_exact_provisioning_insert
BEFORE INSERT ON registered_repositories
WHEN (SELECT COUNT(*) FROM json_each(NEW.provisioning_json))!=1
  OR EXISTS(SELECT 1 FROM json_each(NEW.provisioning_json) WHERE key!='state')
BEGIN SELECT RAISE(ABORT,'owned repository ready state has invalid keys'); END;

CREATE TRIGGER registered_repository_checkout_insert
BEFORE INSERT ON registered_repositories
WHEN json_extract(NEW.checkout_json,'$.state')!='ready'
  OR (SELECT COUNT(*) FROM json_each(NEW.checkout_json))!=2
  OR EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha'))
  OR length(json_extract(NEW.checkout_json,'$.target_sha')) NOT IN (40,64)
  OR json_extract(NEW.checkout_json,'$.target_sha') GLOB '*[^0-9A-Fa-f]*'
  OR json_extract(NEW.checkout_json,'$.target_sha')!=NEW.source_sha
BEGIN SELECT RAISE(ABORT,'owned repository initial checkout state is invalid'); END;

CREATE TRIGGER registered_repository_checkout_update
BEFORE UPDATE OF source_sha,checkout_json ON registered_repositories
WHEN NOT (
  (json_extract(NEW.checkout_json,'$.state')='ready'
    AND (SELECT COUNT(*) FROM json_each(NEW.checkout_json))=2
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha'))
    AND length(json_extract(NEW.checkout_json,'$.target_sha')) IN (40,64)
    AND json_extract(NEW.checkout_json,'$.target_sha') NOT GLOB '*[^0-9A-Fa-f]*'
    AND json_extract(NEW.checkout_json,'$.target_sha')=NEW.source_sha) OR
  (json_extract(NEW.checkout_json,'$.state')='pending'
    AND (SELECT COUNT(*) FROM json_each(NEW.checkout_json))=2
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha'))
    AND length(json_extract(NEW.checkout_json,'$.target_sha')) IN (40,64)
    AND json_extract(NEW.checkout_json,'$.target_sha') NOT GLOB '*[^0-9A-Fa-f]*') OR
  (json_extract(NEW.checkout_json,'$.state')='failed'
    AND (SELECT COUNT(*) FROM json_each(NEW.checkout_json))=3
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha','message'))
    AND length(json_extract(NEW.checkout_json,'$.target_sha')) IN (40,64)
    AND json_extract(NEW.checkout_json,'$.target_sha') NOT GLOB '*[^0-9A-Fa-f]*'
    AND trim(json_extract(NEW.checkout_json,'$.message'))!='')
)
BEGIN SELECT RAISE(ABORT,'owned repository checkout state is invalid'); END;

CREATE TRIGGER registered_repository_delete_guard
BEFORE DELETE ON registered_repositories
WHEN EXISTS(SELECT 1 FROM queue_items WHERE repo_key=OLD.repo_key)
BEGIN SELECT RAISE(ABORT,'owned repository has queue history'); END;

CREATE TRIGGER workspace_root_exact_identity_insert
BEFORE INSERT ON workspace_roots
WHEN NOT EXISTS(
  SELECT 1 FROM registered_repositories repository
  WHERE repository.repo_key=NEW.repo_key
    AND NEW.source_path=repository.owned_root_path
    AND NEW.source_rift_id=repository.root_rift_id
    AND NEW.registry_identity=repository.registry_identity
    AND NEW.registry_device=repository.registry_device
    AND NEW.registry_inode=repository.registry_inode
    AND ((NEW.kind='development' AND NEW.root_path=repository.development_root_path)
      OR (NEW.kind='integration' AND NEW.root_path=repository.integration_root_path))
)
BEGIN SELECT RAISE(ABORT,'workspace root differs from exact registered repository authority'); END;

CREATE TRIGGER workspace_root_exact_identity_update
BEFORE UPDATE OF repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode ON workspace_roots
WHEN NOT EXISTS(
  SELECT 1 FROM registered_repositories repository
  WHERE repository.repo_key=NEW.repo_key
    AND NEW.source_path=repository.owned_root_path
    AND NEW.source_rift_id=repository.root_rift_id
    AND NEW.registry_identity=repository.registry_identity
    AND NEW.registry_device=repository.registry_device
    AND NEW.registry_inode=repository.registry_inode
    AND ((NEW.kind='development' AND NEW.root_path=repository.development_root_path)
      OR (NEW.kind='integration' AND NEW.root_path=repository.integration_root_path))
)
BEGIN SELECT RAISE(ABORT,'workspace root update differs from exact registered repository authority'); END;

CREATE TRIGGER workspace_root_delete_guard
BEFORE DELETE ON workspace_roots
WHEN EXISTS(SELECT 1 FROM registered_repositories repository WHERE repository.repo_key=OLD.repo_key)
BEGIN SELECT RAISE(ABORT,'registered repository child-root authority cannot be removed'); END;
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

    const REGISTERED_REPOSITORY_TRIGGERS4: &str = r#"
CREATE TRIGGER registered_repository_path_identity_insert
BEFORE INSERT ON registered_repositories
WHEN NEW.owned_root_path=NEW.development_root_path
  OR NEW.owned_root_path=NEW.integration_root_path
  OR NEW.development_root_path=NEW.integration_root_path
  OR EXISTS(
    SELECT 1 FROM registered_repositories existing
    WHERE NEW.owned_root_path IN (existing.owned_root_path,existing.development_root_path,existing.integration_root_path)
       OR NEW.development_root_path IN (existing.owned_root_path,existing.development_root_path,existing.integration_root_path)
       OR NEW.integration_root_path IN (existing.owned_root_path,existing.development_root_path,existing.integration_root_path)
  )
  OR EXISTS(
    SELECT 1 FROM workspace_roots root
    WHERE root.root_path IN (NEW.owned_root_path,NEW.development_root_path,NEW.integration_root_path)
  )
BEGIN SELECT RAISE(ABORT,'owned repository paths overlap existing repository authority'); END;

CREATE TRIGGER registered_repository_excludes_provisioning_intent
BEFORE INSERT ON registered_repositories
WHEN EXISTS(SELECT 1 FROM repository_provisioning_intents intent WHERE intent.repo_key=NEW.repo_key)
BEGIN SELECT RAISE(ABORT,'ready repository cannot coexist with provisioning intent'); END;

CREATE TRIGGER repository_provisioning_intent_excludes_ready
BEFORE INSERT ON repository_provisioning_intents
WHEN EXISTS(SELECT 1 FROM registered_repositories repository WHERE repository.repo_key=NEW.repo_key)
BEGIN SELECT RAISE(ABORT,'provisioning intent cannot coexist with ready repository'); END;

CREATE TRIGGER registered_repository_identity_immutable
BEFORE UPDATE OF repo_key,owned_root_path,git_binding_json,root_rift_id,registry_identity,registry_device,registry_inode,development_root_path,development_kind,integration_root_path,integration_kind,created_at ON registered_repositories
BEGIN SELECT RAISE(ABORT,'owned repository identity is immutable'); END;

CREATE TRIGGER registered_repository_exact_provisioning_insert
BEFORE INSERT ON registered_repositories
WHEN (SELECT COUNT(*) FROM json_each(NEW.provisioning_json))!=1
  OR EXISTS(SELECT 1 FROM json_each(NEW.provisioning_json) WHERE key!='state')
BEGIN SELECT RAISE(ABORT,'owned repository ready state has invalid keys'); END;

CREATE TRIGGER registered_repository_checkout_insert
BEFORE INSERT ON registered_repositories
WHEN json_extract(NEW.checkout_json,'$.state')!='ready'
  OR (SELECT COUNT(*) FROM json_each(NEW.checkout_json))!=2
  OR EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha'))
  OR length(json_extract(NEW.checkout_json,'$.target_sha')) NOT IN (40,64)
  OR json_extract(NEW.checkout_json,'$.target_sha') GLOB '*[^0-9A-Fa-f]*'
  OR json_extract(NEW.checkout_json,'$.target_sha')!=NEW.source_sha
BEGIN SELECT RAISE(ABORT,'owned repository initial checkout state is invalid'); END;

CREATE TRIGGER registered_repository_checkout_update
BEFORE UPDATE OF source_sha,checkout_json ON registered_repositories
WHEN NOT (
  (json_extract(NEW.checkout_json,'$.state')='ready'
    AND (SELECT COUNT(*) FROM json_each(NEW.checkout_json))=2
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha'))
    AND length(json_extract(NEW.checkout_json,'$.target_sha')) IN (40,64)
    AND json_extract(NEW.checkout_json,'$.target_sha') NOT GLOB '*[^0-9A-Fa-f]*'
    AND json_extract(NEW.checkout_json,'$.target_sha')=NEW.source_sha) OR
  (json_extract(NEW.checkout_json,'$.state')='pending'
    AND (SELECT COUNT(*) FROM json_each(NEW.checkout_json))=2
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha'))
    AND length(json_extract(NEW.checkout_json,'$.target_sha')) IN (40,64)
    AND json_extract(NEW.checkout_json,'$.target_sha') NOT GLOB '*[^0-9A-Fa-f]*') OR
  (json_extract(NEW.checkout_json,'$.state')='failed'
    AND (SELECT COUNT(*) FROM json_each(NEW.checkout_json))=3
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.checkout_json) WHERE key NOT IN ('state','target_sha','message'))
    AND length(json_extract(NEW.checkout_json,'$.target_sha')) IN (40,64)
    AND json_extract(NEW.checkout_json,'$.target_sha') NOT GLOB '*[^0-9A-Fa-f]*'
    AND trim(json_extract(NEW.checkout_json,'$.message'))!='')
)
BEGIN SELECT RAISE(ABORT,'owned repository checkout state is invalid'); END;

CREATE TRIGGER registered_repository_delete_guard
BEFORE DELETE ON registered_repositories
WHEN EXISTS(SELECT 1 FROM queue_items WHERE repo_key=OLD.repo_key)
BEGIN SELECT RAISE(ABORT,'owned repository has queue history'); END;

CREATE TRIGGER workspace_root_exact_identity_insert
BEFORE INSERT ON workspace_roots
WHEN NOT EXISTS(
  SELECT 1 FROM registered_repositories repository
  WHERE repository.repo_key=NEW.repo_key
    AND NEW.source_path=repository.owned_root_path
    AND NEW.source_rift_id=repository.root_rift_id
    AND NEW.registry_identity=repository.registry_identity
    AND NEW.registry_device=repository.registry_device
    AND NEW.registry_inode=repository.registry_inode
    AND ((NEW.kind='development' AND NEW.root_path=repository.development_root_path)
      OR (NEW.kind='integration' AND NEW.root_path=repository.integration_root_path))
)
BEGIN SELECT RAISE(ABORT,'workspace root differs from exact registered repository authority'); END;

CREATE TRIGGER workspace_root_exact_identity_update
BEFORE UPDATE OF repo_key,kind,root_path,source_path,source_rift_id,registry_identity,registry_device,registry_inode ON workspace_roots
WHEN NOT EXISTS(
  SELECT 1 FROM registered_repositories repository
  WHERE repository.repo_key=NEW.repo_key
    AND NEW.source_path=repository.owned_root_path
    AND NEW.source_rift_id=repository.root_rift_id
    AND NEW.registry_identity=repository.registry_identity
    AND NEW.registry_device=repository.registry_device
    AND NEW.registry_inode=repository.registry_inode
    AND ((NEW.kind='development' AND NEW.root_path=repository.development_root_path)
      OR (NEW.kind='integration' AND NEW.root_path=repository.integration_root_path))
)
BEGIN SELECT RAISE(ABORT,'workspace root update differs from exact registered repository authority'); END;

CREATE TRIGGER workspace_root_delete_guard
BEFORE DELETE ON workspace_roots
WHEN EXISTS(SELECT 1 FROM registered_repositories repository WHERE repository.repo_key=OLD.repo_key)
BEGIN SELECT RAISE(ABORT,'registered repository child-root authority cannot be removed'); END;

CREATE TRIGGER queue_admission_local_source_insert
BEFORE INSERT ON queue_admissions
WHEN NEW.kind='local_submission' AND NOT EXISTS (
  SELECT 1 FROM local_submissions submission
  WHERE submission.id=NEW.submission_id
    AND submission.repo_key=(SELECT repo_key FROM queue_items WHERE id=NEW.item_id)
    AND submission.commit_sha=NEW.head_sha
    AND submission.private_ref=NEW.source_ref
    AND submission.queue_item_id=NEW.item_id
    AND submission.state='creating'
)
BEGIN SELECT RAISE(ABORT,'local queue admission does not match exact submission intent'); END;

CREATE TRIGGER local_submission_identity_immutable
BEFORE UPDATE OF id,queue_item_id,repo_key,workspace_id,base_sha,commit_sha,private_ref,staging_ref,replaces_item_id,created_at ON local_submissions
BEGIN SELECT RAISE(ABORT,'local submission identity is immutable'); END;
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
    use std::path::{Path, PathBuf};
    use std::process::{ExitStatus, Output, Stdio};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::mpsc;
    use std::sync::OnceLock;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration as StdDuration, Instant};
    use uuid::Uuid;
    use wait_timeout::ChildExt;

    use crate::core::{BlockedPhase, BlockedReason, QueueStatus};
    use crate::sqlite::{
        Attempt, AttemptValidationInvocation, ExecutionAuthority, QueueItem, ResidueChildMove,
        ResidueEntryIdentity, RiftWorkspaceRootOwner, SqliteQueue, SqliteQueueReader,
        WorkspaceGenerationState, WorkspaceIdentity, WorkspaceState,
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

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum TerminalCleanupMode {
        Automatic,
        OperatorRequested,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub enum TerminalCleanupOutcome {
        Removed { path: PathBuf },
        Preserved { path: PathBuf, reason: String },
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub struct TerminalCleanupReport {
        pub mode: String,
        pub outcomes: Vec<TerminalCleanupOutcome>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub struct TerminalCleanupAggregate {
        pub terminal: TerminalCleanupReport,
        pub development: Vec<crate::sqlite::DevelopmentWorkspace>,
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
        lease_owner_id: String,
        workspaces: OnceLock<RiftWorkspaceManager>,
        control_store: crate::control_store::ControlStore,
    }

    enum MovedBaseCause<'a> {
        TargetMoved(&'a str),
        DefiniteLandingRejection(&'a crate::control_store::DefiniteLandingRejection),
    }

    pub(crate) struct RiftWorkspaceManager {
        source: PathBuf,
        source_id: String,
        source_ancestors: Vec<PathBuf>,
        root: PathBuf,
        repo_key: String,
        role: String,
        queue_database_id: String,
        registry_identity: String,
        registry_dev: u64,
        registry_ino: u64,
        generation: AtomicI64,
        program: crate::agent_config::ExecutableAuthority,
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

    #[cfg(debug_assertions)]
    fn stop_initial_target_after(boundary: &str) {
        if std::env::var("IQ_TEST_TARGET_FETCH_STOP_AFTER").as_deref() == Ok(boundary) {
            std::process::exit(83);
        }
    }

    #[cfg(not(debug_assertions))]
    fn stop_initial_target_after(_boundary: &str) {}

    #[cfg(debug_assertions)]
    fn stop_supervised_target_after(boundary: &str) {
        if std::env::var("IQ_TEST_SUPERVISED_TARGET_STOP_AFTER").as_deref() == Ok(boundary) {
            std::process::exit(84);
        }
    }

    #[cfg(not(debug_assertions))]
    fn stop_supervised_target_after(_boundary: &str) {}

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

    fn rift_executable_authority() -> Result<crate::agent_config::ExecutableAuthority> {
        crate::agent_config::rift_executable_authority()
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

    fn sandbox_cycle_id(path: &Path) -> Result<Option<String>> {
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            return Ok(None);
        };
        let Some(cycle_id) = name.strip_prefix(".iq-agent-sandbox-") else {
            return Ok(None);
        };
        let parsed = Uuid::parse_str(cycle_id)
            .with_context(|| format!("IQ agent sandbox has malformed cycle identity: {name}"))?;
        if parsed.to_string() != cycle_id {
            anyhow::bail!("IQ agent sandbox has non-canonical cycle identity: {name}");
        }
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect IQ agent sandbox {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!(
                "IQ agent sandbox must be a real directory: {}",
                path.display()
            );
        }
        Ok(Some(cycle_id.to_string()))
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

    fn acquire_existing_exclusive_lock(path: &Path, label: &str) -> Result<fs::File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
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
        pub(crate) fn inspect(
            source: PathBuf,
            root: PathBuf,
            repo_key: String,
            role: &str,
            database: Option<PathBuf>,
            queue_database_id: &str,
            generation_authority: WorkspaceGenerationState,
        ) -> Result<()> {
            if !matches!(role, "development" | "integration") {
                anyhow::bail!("unknown IQ workspace root role {role}");
            }
            if root.starts_with(&source) {
                anyhow::bail!(
                    "IQ workspace root {} must be outside Rift source {}",
                    root.display(),
                    source.display()
                );
            }
            if !entry_exists(&root)? {
                anyhow::bail!("IQ workspace root is missing: {}", root.display());
            }
            let metadata = fs::symlink_metadata(&root)
                .with_context(|| format!("inspect IQ workspace root {}", root.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                anyhow::bail!(
                    "IQ workspace root must be a real directory: {}",
                    root.display()
                );
            }
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
            let program = rift_executable_authority()?;
            let mut ancestors_args = Vec::new();
            if let Some(database) = database.as_ref() {
                ancestors_args.push(OsString::from("--database"));
                ancestors_args.push(database.clone());
            }
            ancestors_args.extend([OsString::from("ancestors"), source.as_os_str().into()]);
            let mut list_args = Vec::new();
            if let Some(database) = database.as_ref() {
                list_args.push(OsString::from("--database"));
                list_args.push(database.clone());
            }
            list_args.extend([OsString::from("list"), source.as_os_str().into()]);
            let mut commands = [ancestors_args, list_args]
                .into_iter()
                .map(|args| {
                    let mut command = gated_process(
                        &CommandProgram::Descriptor {
                            label: "rift",
                            authority: &program,
                        },
                        args,
                    )?;
                    crate::agent_config::harden_rift_environment(&mut command);
                    Ok(command)
                })
                .collect::<Result<Vec<_>>>()?;
            let mut outputs = crate::agent_runner::service_read_operation(
                &mut commands,
                StdDuration::from_secs(60),
                |_| Ok(()),
            )?
            .into_iter();
            let ancestors = outputs
                .next()
                .context("Rift ancestors operation output is absent")?;
            let ancestors = if ancestors.status.success() {
                ancestors
            } else {
                anyhow::bail!(
                    "verify Rift source root failed: {}",
                    String::from_utf8_lossy(&ancestors.stderr).trim()
                )
            };
            let listed = outputs
                .next()
                .context("Rift list operation output is absent")?;
            if outputs.next().is_some() {
                anyhow::bail!("Rift verification operation returned extra output");
            }
            if !String::from_utf8_lossy(&ancestors.stdout).trim().is_empty() {
                anyhow::bail!(
                    "repository {} is a child Rift; IQ requires an independently managed Rift root",
                    source.display()
                );
            }
            {
                let marker = root.join(".iq-workspace-owner.json");
                if !entry_exists(&marker)? {
                    anyhow::bail!(
                        "IQ workspace root owner marker is missing: {}",
                        marker.display()
                    );
                }
                if entry_exists(&marker)? {
                    let owner: RiftWorkspaceRootOwner = serde_json::from_slice(&read_regular_file(
                        &marker,
                        "IQ workspace owner marker",
                    )?)
                    .with_context(|| format!("parse {}", marker.display()))?;
                    if owner.version != 4
                        || owner.queue_database_id != queue_database_id
                        || owner.repo_key != repo_key
                        || owner.role != role
                        || owner.root != root
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
                    if generation != generation_authority.current()
                        && generation_authority.pending() != Some(generation)
                    {
                        anyhow::bail!(
                            "queue database current/pending generation authority differs from IQ workspace root generation {generation}"
                        );
                    }
                }
                if entry_exists(&marker)? {
                    let listed = if listed.status.success() {
                        listed
                    } else {
                        anyhow::bail!(
                            "list source Rifts failed: {}",
                            String::from_utf8_lossy(&listed.stderr).trim()
                        )
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
                        if is_rift_workspace_root_entry(&path)?
                            || sandbox_cycle_id(&path)?.is_some()
                        {
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

        pub(crate) fn claim(
            source: PathBuf,
            root: PathBuf,
            repo_key: String,
            role: &str,
            database: Option<PathBuf>,
            queue_database_id: &str,
            workspace_generation: i64,
        ) -> Result<Self> {
            if !matches!(role, "development" | "integration") {
                anyhow::bail!("unknown IQ workspace root role {role}");
            }
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
                role: role.to_string(),
                queue_database_id: queue_database_id.to_string(),
                registry_identity,
                registry_dev,
                registry_ino,
                generation: AtomicI64::new(0),
                program: rift_executable_authority()?,
                database,
                root_directory,
            };
            manager.source_ancestors = manager.verify_source()?;
            {
                let _root_lock = acquire_root_lock(&manager.root)?;
                manager.ensure_root_owner()?;
                manager.synchronize_generation_unlocked(workspace_generation)?;
            }
            Ok(manager)
        }

        pub(crate) fn open(
            source: PathBuf,
            root: PathBuf,
            repo_key: String,
            role: &str,
            database: Option<PathBuf>,
            queue_database_id: &str,
            generation: WorkspaceGenerationState,
        ) -> Result<Self> {
            Self::inspect(
                source.clone(),
                root.clone(),
                repo_key.clone(),
                role,
                database.clone(),
                queue_database_id,
                generation,
            )?;
            let source = source
                .canonicalize()
                .with_context(|| format!("resolve Rift source {}", source.display()))?;
            let root = root
                .canonicalize()
                .with_context(|| format!("resolve IQ workspace root {}", root.display()))?;
            let source_id = Self::read_marker_id(&source)?;
            let (database, registry_identity, registry_dev, registry_ino) =
                resolve_rift_database(database)?;
            let root_directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
                .open(&root)
                .with_context(|| format!("open IQ workspace root {}", root.display()))?;
            let persisted_generation = Self::read_generation_at(&root)?
                .context("IQ workspace root generation is missing")?;
            let mut manager = Self {
                source,
                source_id,
                source_ancestors: Vec::new(),
                root,
                repo_key,
                role: role.to_string(),
                queue_database_id: queue_database_id.to_string(),
                registry_identity,
                registry_dev,
                registry_ino,
                generation: AtomicI64::new(persisted_generation),
                program: rift_executable_authority()?,
                database,
                root_directory,
            };
            manager.source_ancestors = manager.verify_source()?;
            manager.verify_root_identity()?;
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

        fn verify_source(&self) -> Result<Vec<PathBuf>> {
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
            if !ancestors.is_empty() {
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
                version: 4,
                queue_database_id: self.queue_database_id.clone(),
                repo_key: self.repo_key.clone(),
                role: self.role.clone(),
                root: self.root.clone(),
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
            Self::read_generation_at(&self.root)
        }

        fn read_generation_at(root: &Path) -> Result<Option<i64>> {
            let path = root.join(".iq-workspace-generation");
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
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&temporary)?;
            file.write_all(format!("{generation}\n").as_bytes())?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &path)?;
            self.root_directory.sync_all()?;
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

        pub(crate) fn reconcile_pending_generation(
            &self,
            authority: WorkspaceGenerationState,
        ) -> Result<i64> {
            let WorkspaceGenerationState::Pending { current, pending } = authority else {
                anyhow::bail!("workspace generation reconciliation requires pending authority");
            };
            let _root_lock = acquire_root_lock(&self.root)?;
            self.verify_root_identity()?;
            match self.generation.load(Ordering::Acquire) {
                actual if actual == current => self.write_generation(pending)?,
                actual if actual == pending => {}
                actual => anyhow::bail!(
                    "IQ workspace root generation {actual} differs from current {current} and pending {pending} authority"
                ),
            }
            self.generation.store(pending, Ordering::Release);
            crate::sqlite::stop_workspace_generation_after(&format!("{}_marker", self.role));
            Ok(pending)
        }

        fn ensure_root_owner(&self) -> Result<()> {
            let path = self.root.join(".iq-workspace-owner.json");
            let expected = RiftWorkspaceRootOwner {
                version: 4,
                queue_database_id: self.queue_database_id.clone(),
                repo_key: self.repo_key.clone(),
                role: self.role.clone(),
                root: self.root.clone(),
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
                        self.root_directory.sync_all()?;
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
            let mut temporary_file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&temporary)
                .with_context(|| format!("create {}", temporary.display()))?;
            temporary_file.write_all(&serde_json::to_vec_pretty(&expected)?)?;
            temporary_file.sync_all()?;
            drop(temporary_file);
            let publish = fs::hard_link(&temporary, &path);
            fs::remove_file(&temporary)
                .with_context(|| format!("remove {}", temporary.display()))?;
            self.root_directory.sync_all()?;
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
            authorize_start: impl FnOnce(&mut CommandRelease) -> Result<bool>,
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
                    crate::git_command::authorize_current(&path)?;
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
            A: FnMut(&mut CommandRelease) -> Result<bool>,
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
            A: FnMut(&mut CommandRelease) -> Result<bool>,
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
                let mut release = CommandRelease::new();
                if !authorize_mutation(&mut release)? {
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
                let mut release = CommandRelease::new();
                if !authorize_mutation(&mut release)? {
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
            let mut release = CommandRelease::new();
            if !authorize_mutation(&mut release)? {
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
                let mut release = CommandRelease::new();
                if !authorize_mutation(&mut release)? {
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
            A: FnMut(&mut CommandRelease) -> Result<bool>,
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
            A: FnMut(&mut CommandRelease) -> Result<bool>,
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
            let mut release = CommandRelease::new();
            if !authorize_mutation(&mut release)? {
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
            A: FnOnce(&mut CommandRelease) -> Result<bool>,
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
            A: FnOnce(&mut CommandRelease) -> Result<bool>,
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
            let outcome = command_output_timeout_with_prepare(
                CommandProgram::Descriptor {
                    label: "rift",
                    authority: &self.program,
                },
                command_args,
                None,
                StdDuration::from_secs(60),
                |gate| {
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || Ok(ExecutionAuthority::Active),
                |command| {
                    crate::agent_config::harden_rift_environment(command);
                    Ok(())
                },
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
            authorize_start: impl FnOnce(&mut CommandRelease) -> Result<bool>,
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
            let outcome = command_output_timeout_with_prepare(
                CommandProgram::Descriptor {
                    label: "rift",
                    authority: &self.program,
                },
                command_args,
                None,
                StdDuration::from_secs(60),
                authorize_start,
                check_authority,
                |command| {
                    crate::agent_config::harden_rift_environment(command);
                    Ok(())
                },
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
        let workspace_generation = queue.workspace_root_generation_state(repo_key)?;
        RiftWorkspaceManager::inspect(
            source,
            inspected_root,
            repo_key.to_string(),
            "integration",
            rift_database.map(Path::to_path_buf),
            &queue_database_id,
            workspace_generation,
        )
    }

    #[doc(hidden)]
    pub fn verify_rift_workspace_config_with_queue(
        queue: &SqliteQueue,
        source: &Path,
        root: &Path,
        repo_key: &str,
        rift_database: Option<&Path>,
    ) -> Result<()> {
        let source = source
            .canonicalize()
            .with_context(|| format!("resolve configured repository {}", source.display()))?;
        let root = if root.is_absolute() {
            root.to_path_buf()
        } else {
            std::env::current_dir()?.join(root)
        };
        let queue_database_id = queue.database_id()?;
        let inspected_root = resolve_path_without_creating(&root)?;
        queue.verify_workspace_root_path(repo_key, &inspected_root)?;
        let workspace_generation =
            queue.workspace_root_generation_state_for_kind(repo_key, "integration")?;
        RiftWorkspaceManager::inspect(
            source,
            inspected_root,
            repo_key.to_string(),
            "integration",
            rift_database.map(Path::to_path_buf),
            &queue_database_id,
            workspace_generation,
        )
    }

    pub(crate) fn verify_terminal_workspace_root(
        queue: &SqliteQueue,
        repo_key: &str,
        root: &crate::sqlite::WorkspaceRootIdentity,
    ) -> Result<()> {
        let marker = root.path.join(".iq-workspace-owner.json");
        let queue_database_id = queue.database_id()?;
        let owner: crate::sqlite::RiftWorkspaceRootOwner =
            serde_json::from_slice(&read_regular_file(&marker, "IQ workspace owner marker")?)?;
        if owner.version != 4
            || owner.queue_database_id != queue_database_id
            || owner.repo_key != repo_key
            || owner.role != "integration"
            || owner.root != root.path
            || owner.source_rift_id != root.source_rift_id
        {
            anyhow::bail!(
                "terminal cycle workspace root owner differs from durable authority: {}",
                root.path.display()
            );
        }
        let generation_path = root.path.join(".iq-workspace-generation");
        let generation = String::from_utf8(read_regular_file(
            &generation_path,
            "IQ workspace generation",
        )?)?
        .trim()
        .parse::<i64>()
        .context("parse IQ workspace generation")?;
        if generation < 0 {
            anyhow::bail!("terminal cycle workspace root generation must not be negative");
        }
        if root.scope != repo_key
            || owner.source != root.source
            || owner.registry_identity != root.registry_identity
        {
            anyhow::bail!(
                "terminal cycle workspace root {} differs from persisted root authority",
                root.path.display()
            );
        }
        if generation != root.generation && root.pending_generation != Some(generation) {
            anyhow::bail!(
                "terminal cycle workspace root generation {generation} differs from persisted generation {}",
                root.generation
            );
        }
        Ok(())
    }

    pub(crate) fn cleanup_terminal_agent_artifacts(
        queue: &SqliteQueue,
        repo_key: &str,
        repair_deleted_cycles: bool,
    ) -> Result<()> {
        let store = queue.validated_control_store()?;
        let cycles = store.terminal_cycle_artifacts(repo_key)?;
        let mut roots = HashSet::new();
        for cycle in cycles {
            let durable_root = Path::new(&cycle.workspace.path)
                .parent()
                .context("terminal cycle workspace has no parent root")?;
            let canonical_root = durable_root.canonicalize().with_context(|| {
                format!(
                    "resolve terminal cycle workspace root {}",
                    durable_root.display()
                )
            })?;
            if canonical_root != durable_root {
                anyhow::bail!(
                    "terminal cycle workspace root {} resolves to unexpected path {}",
                    durable_root.display(),
                    canonical_root.display()
                );
            }
            let workspace_root = queue
                .workspace_root_identity(repo_key, &canonical_root)?
                .with_context(|| {
                    format!(
                        "terminal cycle cleanup has no persisted workspace root authority for {}",
                        canonical_root.display()
                    )
                })?;
            verify_terminal_workspace_root(queue, repo_key, &workspace_root)?;
            crate::agent_runner::cleanup_terminal_cycle_artifacts(&workspace_root, &cycle)?;
            roots.insert(workspace_root.path);
        }
        if repair_deleted_cycles {
            let repair_root = queue
                .registered_workspace_root_identity(repo_key)?
                .context("deleted-cycle repair has no persisted workspace root authority")?;
            verify_terminal_workspace_root(queue, repo_key, &repair_root)?;
            let canonical_root = repair_root.path.canonicalize().with_context(|| {
                format!(
                    "resolve deleted-cycle repair root {}",
                    repair_root.path.display()
                )
            })?;
            if canonical_root != repair_root.path {
                anyhow::bail!(
                    "deleted-cycle repair root {} resolves to unexpected path {}",
                    repair_root.path.display(),
                    canonical_root.display()
                );
            }
            roots.insert(repair_root.path);
            for root in roots {
                let workspace_root = queue
                    .workspace_root_identity(repo_key, &root)?
                    .context("deleted-cycle repair lost persisted workspace root authority")?;
                verify_terminal_workspace_root(queue, repo_key, &workspace_root)?;
                let mut sandboxes = Vec::new();
                for entry in fs::read_dir(&root)? {
                    let path = entry?.path();
                    let Some(cycle_id) = sandbox_cycle_id(&path)? else {
                        continue;
                    };
                    sandboxes.push((path, cycle_id));
                }
                sandboxes.sort_by(|left, right| left.1.cmp(&right.1));
                for (path, cycle_id) in sandboxes {
                    match queue
                        .authorize_deleted_cycle_sandbox_repair(repo_key, &root, &cycle_id)?
                    {
                        crate::sqlite::DeletedCycleSandboxRepair::Authorized => {
                            crate::agent_runner::remove_sandbox_export(&path.join("export"))?
                        }
                        crate::sqlite::DeletedCycleSandboxRepair::PreservedDurableAuthority => {}
                    }
                }
            }
        }
        Ok(())
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

    pub(crate) enum CommandOutputOutcome {
        Exited(Output),
        Cancelled,
    }

    pub(crate) enum CommandProgram<'a> {
        SearchPath(&'a str),
        Descriptor {
            label: &'a str,
            authority: &'a crate::agent_config::ExecutableAuthority,
        },
    }

    impl CommandProgram<'_> {
        fn label(&self) -> &str {
            match self {
                Self::SearchPath(program) | Self::Descriptor { label: program, .. } => program,
            }
        }
    }

    pub(crate) struct CommandRelease {
        token: Vec<u8>,
        https_credential: Option<crate::git_command::HttpsCredential>,
    }

    impl CommandRelease {
        fn new() -> Self {
            Self {
                token: Vec::new(),
                https_credential: None,
            }
        }

        pub(crate) fn set_https_credential(
            &mut self,
            credential: Option<crate::git_command::HttpsCredential>,
        ) {
            self.https_credential = credential;
        }
    }

    impl Write for CommandRelease {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.token.write(buffer)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub(crate) enum CommandInfrastructureError {
        #[error("{program} timed out after {timeout_seconds} seconds")]
        TimedOut {
            program: String,
            timeout_seconds: u64,
        },
        #[error("{program} {stream} exceeded the {maximum_bytes}-byte capture limit")]
        OutputLimit {
            program: String,
            stream: &'static str,
            maximum_bytes: usize,
        },
    }

    struct LeaseHeartbeat {
        stop: Option<mpsc::Sender<()>>,
        handle: Option<JoinHandle<Result<()>>>,
    }

    #[cfg(debug_assertions)]
    fn pause_repository_operation_after_acquire() {
        let Some(ready) = std::env::var_os("IQ_TEST_REPOSITORY_OPERATION_READY") else {
            return;
        };
        fs::write(ready, b"ready\n").expect("write repository operation test readiness");
        loop {
            std::thread::park();
        }
    }

    #[cfg(not(debug_assertions))]
    fn pause_repository_operation_after_acquire() {}

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
            let (registered_repository, target, _) = queue
                .registered_remote_identity(repo_key)?
                .context("queue repository is not registered")?;
            if repository != registered_repository {
                anyhow::bail!("repository operation path differs from database authority");
            }
            queue.validate_repository_binding(repo_key, &registered_repository, &target)?;
            let database_lease = crate::control_store::DatabaseProcessLease::acquire(queue.path())?;
            let process_lock = acquire_existing_exclusive_lock(
                &registered_repository.join(".git/iq-operation.lock"),
                "repository operation lock",
            );
            let process_lock = match process_lock {
                Ok(lock) => lock,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.raw_os_error() == Some(libc::EWOULDBLOCK)) =>
                {
                    return Ok(None);
                }
                Err(error) => {
                    return Err(error);
                }
            };
            queue.acquire_repo_operation_lease(
                repo_key,
                owner_id,
                ttl_seconds,
                &registered_repository,
                &target,
            )?;
            if let Err(error) = queue.verify_owned_repository(repo_key) {
                let _ = queue.release_repo_lease(repo_key, owner_id);
                return Err(error).context("verify owned repository operation authority");
            }
            let heartbeat = LeaseHeartbeat::start(
                queue.clone(),
                repo_key.to_string(),
                owner_id.to_string(),
                ttl_seconds,
            );
            pause_repository_operation_after_acquire();
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
            self.ensure_lease()?;
            self.queue
                .verify_owned_repository(&self.repo_key)
                .context("reverify owned repository operation authority")?;
            Ok(())
        }

        fn ensure_lease(&self) -> Result<()> {
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

        fn ensure_command_authority(&self, program: &str, args: &[OsString]) -> Result<()> {
            if program == "git" && crate::git_command::is_read_only_operation(args) {
                self.ensure_lease()
            } else {
                self.ensure()
            }
        }

        pub(crate) fn authority(&self) -> Result<ExecutionAuthority> {
            self.queue.lease_authority(&self.repo_key, &self.owner_id)
        }

        #[doc(hidden)]
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
            let args = args
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect::<Vec<_>>();
            let outcome = command_output_timeout(
                program,
                &args,
                cwd,
                timeout,
                |gate| {
                    self.ensure_command_authority(program, &args)?;
                    if program == "git" && crate::git_command::is_external_operation(&args) {
                        let cwd =
                            cwd.context("Git command requires an explicit working directory")?;
                        let repository = self.queue.repository(&self.repo_key)?;
                        let canonical = repository.policy.canonical_repository;
                        let credential =
                            crate::git_command::authorize_external_effect(cwd, &args, &canonical)?;
                        self.ensure_command_authority(program, &args)?;
                        if self
                            .queue
                            .repository(&self.repo_key)?
                            .policy
                            .canonical_repository
                            != canonical
                        {
                            anyhow::bail!("repository policy changed during provider verification");
                        }
                        crate::git_command::authorize_external_effect_with_verified_provider(
                            cwd, &args, &canonical,
                        )?;
                        gate.set_https_credential(credential);
                    }
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

        pub(crate) fn run_internal_command<I, S>(
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

        pub(crate) fn run_new_work_command<I, S>(
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
            let args = args
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect::<Vec<_>>();
            self.run_policy_command(
                program,
                &args,
                cwd,
                timeout,
                label,
                || self.queue.authorize_new_work(&self.repo_key),
                || {
                    Ok(self
                        .queue
                        .repository(&self.repo_key)?
                        .policy
                        .canonical_repository)
                },
            )
        }

        pub(crate) fn run_obligation_command<I, S>(
            &self,
            obligation: &crate::repository_policy::Obligation,
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
            let args = args
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect::<Vec<_>>();
            self.run_policy_command(
                program,
                &args,
                cwd,
                timeout,
                label,
                || self.queue.authorize_obligation(&self.repo_key, obligation),
                || {
                    Ok(self
                        .queue
                        .repository(&self.repo_key)?
                        .policy
                        .canonical_repository)
                },
            )
        }

        pub(crate) fn run_replication_command<I, S>(
            &self,
            debt_id: &str,
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
            let args = args
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect::<Vec<_>>();
            self.run_policy_command(
                program,
                &args,
                cwd,
                timeout,
                label,
                || {
                    self.queue.authorize_replication_command(
                        &self.repo_key,
                        debt_id,
                        &self.owner_id,
                        &args,
                    )?;
                    Ok(())
                },
                || {
                    let debt = self.queue.replication_debt(debt_id)?;
                    Ok(debt.replica)
                },
            )
        }

        #[allow(clippy::too_many_arguments)]
        fn run_policy_command<I, S>(
            &self,
            program: &str,
            args: I,
            cwd: Option<&Path>,
            timeout: StdDuration,
            label: &str,
            authorize: impl Fn() -> Result<()>,
            external_repository: impl Fn() -> Result<crate::repository_policy::GitRepository>,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            let args = args
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect::<Vec<_>>();
            let outcome = command_output_timeout(
                program,
                &args,
                cwd,
                timeout,
                |gate| {
                    self.ensure_command_authority(program, &args)?;
                    authorize()?;
                    if program == "git" && crate::git_command::is_external_operation(&args) {
                        let cwd =
                            cwd.context("Git command requires an explicit working directory")?;
                        let repository = external_repository()?;
                        let credential =
                            crate::git_command::authorize_external_effect(cwd, &args, &repository)?;
                        self.ensure_command_authority(program, &args)?;
                        authorize()?;
                        if external_repository()? != repository {
                            anyhow::bail!(
                                "external repository policy changed during provider verification"
                            );
                        }
                        crate::git_command::authorize_external_effect_with_verified_provider(
                            cwd,
                            &args,
                            &repository,
                        )?;
                        gate.set_https_credential(credential);
                    }
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

        pub(crate) fn new_with_operation_owner(
            options: IntegratorOptions,
            lease_owner_id: String,
            validated_queue: SqliteQueue,
        ) -> Result<Self> {
            Self::new_with_policy_and_owner(
                options,
                IntegrationPolicy::NoValidation,
                Some(lease_owner_id),
                Some(validated_queue),
            )
        }

        pub fn new_with_policy(
            options: IntegratorOptions,
            policy: IntegrationPolicy,
        ) -> Result<Self> {
            Self::new_with_policy_and_owner(options, policy, None, None)
        }

        pub fn new_with_policy_and_validated_queue(
            options: IntegratorOptions,
            policy: IntegrationPolicy,
            queue: SqliteQueue,
        ) -> Result<Self> {
            Self::new_with_policy_and_owner(options, policy, None, Some(queue))
        }

        fn new_with_policy_and_owner(
            mut options: IntegratorOptions,
            policy: IntegrationPolicy,
            lease_owner_id: Option<String>,
            validated_queue: Option<SqliteQueue>,
        ) -> Result<Self> {
            if let Some(queue) = validated_queue.as_ref() {
                let configured_queue = if options.queue_db.is_absolute() {
                    options.queue_db.clone()
                } else {
                    std::env::current_dir()?.join(&options.queue_db)
                };
                if configured_queue != queue.path() {
                    anyhow::bail!(
                        "validated queue authority path {} does not match configured integrator queue database {}",
                        queue.path().display(),
                        configured_queue.display()
                    );
                }
            }
            let queue = match validated_queue {
                Some(queue) => queue,
                None => SqliteQueue::open(&options.queue_db)?,
            };
            options.queue_db = queue.path().to_path_buf();
            let registered_repository = queue.repository(&options.repo_key)?;
            options.repo_path = registered_repository.owned_root_path.clone();
            options.base_remote = registered_repository.remote.name.clone();
            options.workspace_root = registered_repository.integration_root_path.clone();
            options.rift_database = Some(registered_repository.registry_identity.clone());
            queue.validate_repository_binding(
                &options.repo_key,
                &options.repo_path,
                &registered_repository.target_branch,
            )?;
            if !matches!(&policy, IntegrationPolicy::NoValidation) {
                anyhow::bail!(
                    "registered repositories reject daemon validation and signoff; local integration-checkout policy is authoritative"
                );
            }
            queue.verify_workspace_root_path(&options.repo_key, &options.workspace_root)?;
            let policy = validate_host_policy(policy)?;
            let control_store = queue.validated_control_store()?;
            let lease_owner_id = lease_owner_id
                .unwrap_or_else(|| format!("{}:{}", options.owner_id, Uuid::new_v4()));
            Ok(Self {
                queue,
                options,
                policy,
                lease_owner_id,
                workspaces: OnceLock::new(),
                control_store,
            })
        }

        fn initialize_workspaces(&self) -> Result<()> {
            if self.workspaces.get().is_some() {
                return Ok(());
            }
            let generation = self
                .queue
                .workspace_root_generation_state_for_kind(&self.options.repo_key, "integration")?;
            let manager = RiftWorkspaceManager::open(
                self.options.repo_path.clone(),
                self.options.workspace_root.clone(),
                self.options.repo_key.clone(),
                "integration",
                self.options.rift_database.clone(),
                &self.queue.database_id()?,
                generation,
            )?;
            if self.workspaces.set(manager).is_err() {
                anyhow::bail!("integration workspace manager initialized more than once");
            }
            Ok(())
        }

        fn workspaces(&self) -> &RiftWorkspaceManager {
            self.workspaces
                .get()
                .expect("repository lease must initialize integration workspace manager")
        }

        fn ensure_effort_after_composition(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
        ) -> Result<Option<crate::control_store::IntegrationEffort>> {
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
            match self
                .control_store
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
                }) {
                Ok(effort) => Ok(Some(effort)),
                Err(error)
                    if matches!(
                        error.downcast_ref::<crate::control_store::EffortCreationError>(),
                        Some(crate::control_store::EffortCreationError::Cancelled { .. })
                    ) =>
                {
                    let cancelled = self.queue.get_item(&item.id)?;
                    if cancelled.status != QueueStatus::Cancelled {
                        anyhow::bail!("effort creation reported cancellation for an active item");
                    }
                    Ok(None)
                }
                Err(error) => Err(error),
            }
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
            let canonical = queue.repository(repo_key)?.policy.canonical_repository;
            crate::composition::verify_remote_identity(repo_path, &registered_remote, &canonical)?;
            Ok(true)
        }

        fn verify_registered_remote_transport_identity_for(
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
                anyhow::bail!("configured remote differs from registered repository identity");
            }
            let canonical = queue.repository(repo_key)?.policy.canonical_repository;
            crate::composition::verify_remote_transport_identity(
                repo_path,
                &registered_remote,
                &canonical,
            )?;
            Ok(true)
        }

        fn ensure_registered_remote_identity(&self) -> Result<()> {
            let (_, target, _) = self
                .queue
                .registered_remote_identity(&self.options.repo_key)?
                .context("queue repository is not registered")?;
            Self::verify_registered_remote_identity_for(
                &self.queue,
                &self.options.repo_key,
                &self.options.repo_path,
                &target,
                &self.options.base_remote,
            )?;
            Ok(())
        }

        fn ensure_registered_remote_transport_identity(&self) -> Result<()> {
            let (_, target, _) = self
                .queue
                .registered_remote_identity(&self.options.repo_key)?
                .context("queue repository is not registered")?;
            Self::verify_registered_remote_transport_identity_for(
                &self.queue,
                &self.options.repo_key,
                &self.options.repo_path,
                &target,
                &self.options.base_remote,
            )?;
            Ok(())
        }

        fn ensure_registered_remote_identity_for_item(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            expected_status: QueueStatus,
        ) -> Result<()> {
            self.ensure_registered_remote_transport_identity()?;
            let canonical = self
                .queue
                .repository(&self.options.repo_key)?
                .policy
                .canonical_repository;
            if let Some(provider) = canonical.provider() {
                let mut executor = |provider_kind, program: &str, args: &[OsString]| {
                    self.run_supervised_provider_command(
                        &item.id,
                        &attempt.id,
                        expected_status,
                        provider_kind,
                        program,
                        args,
                    )
                };
                crate::providers::verify_repository_with(
                    provider,
                    canonical.object_format(),
                    &mut executor,
                )
                .context("verify immutable provider repository identity")?;
            }
            Ok(())
        }

        fn canonical_fetch_transport(&self) -> Result<String> {
            self.ensure_registered_remote_transport_identity()?;
            let canonical = self
                .queue
                .repository(&self.options.repo_key)?
                .policy
                .canonical_repository;
            canonical.verify_local_bare()?;
            Ok(canonical.operational_fetch_url())
        }

        fn canonical_push_transport(&self) -> Result<String> {
            self.ensure_registered_remote_transport_identity()?;
            let canonical = self
                .queue
                .repository(&self.options.repo_key)?
                .policy
                .canonical_repository;
            canonical.verify_local_bare()?;
            Ok(canonical.operational_push_url())
        }

        fn require_item_integration_policy(&self, item: &QueueItem) -> Result<()> {
            let repository = self.queue.repository(&item.repo_key)?;
            let authorized = matches!(
                (&repository.policy.integration_policy, &item.admission),
                (
                    crate::repository_policy::IntegrationPolicy::Direct,
                    crate::sqlite::QueueAdmission::Direct { .. }
                        | crate::sqlite::QueueAdmission::LocalSubmission { .. }
                ) | (
                    crate::repository_policy::IntegrationPolicy::MergeRequestRequired,
                    crate::sqlite::QueueAdmission::MergeRequest(_)
                )
            );
            if !authorized {
                anyhow::bail!("queue admission is not authorized by repository policy");
            }
            Ok(())
        }

        pub fn run_once(&self) -> Result<Option<QueueItem>> {
            let operation = RepositoryOperationLease::acquire(
                self.queue.clone(),
                &self.options.repo_path,
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )?;
            let repository_policy = &self.queue.repository(&self.options.repo_key)?.policy;
            match &repository_policy.operation_state {
                crate::repository_policy::OperationState::Enabled => {}
                crate::repository_policy::OperationState::Draining { .. } => {}
                crate::repository_policy::OperationState::Disabled => {
                    anyhow::bail!("repository is disabled")
                }
            }
            self.initialize_workspaces()?;
            cleanup_terminal_agent_artifacts(&self.queue, &self.options.repo_key, false)?;
            self.synchronize_workspace_generation()?;
            self.with_lease_heartbeat("workspace cleanup", || {
                self.reconcile_workspaces(TerminalCleanupMode::Automatic)
            })?;
            self.reconcile_pending_replication()?;
            self.reconcile_private_refs(&operation)?;
            let Some(active) = self.queue.oldest_active_item(&self.options.repo_key)? else {
                return Ok(None);
            };
            repository_policy.require_queue_mutation(&active.id)?;
            self.require_item_integration_policy(&active)?;
            if let Some(blocked) = self.enforce_item_boundary(&active)? {
                return Ok(Some(blocked));
            }
            if active.status == QueueStatus::Blocked
                && self.control_store.effort_for_item(&active.id)?.is_none()
            {
                return Ok(Some(active));
            }
            if active.status != QueueStatus::Ready {
                let item = self.resume_item_owned(&active.id, &operation)?;
                self.reconcile_private_refs(&operation)?;
                return Ok(Some(item));
            }
            let (_, snapshot, digest) =
                crate::composition::load_local_policy(&self.options.repo_path)?;
            let attempt_policy = crate::sqlite::AttemptPolicy::Snapshot {
                snapshot_json: &snapshot,
                digest: &digest,
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
            let item = self.with_lease_heartbeat("integrating", || {
                self.integrate_item(item, &attempt, &operation)
            })?;
            self.reconcile_private_refs(&operation)?;
            Ok(Some(item))
        }

        fn reconcile_private_refs(&self, operation: &RepositoryOperationLease) -> Result<()> {
            let repository = self.queue.repository(&self.options.repo_key)?;
            crate::composition::reconcile_private_refs(
                &self.queue,
                &repository,
                &self.lease_owner_id,
                |args, cwd, label| {
                    operation.run_internal_command(
                        "git",
                        args,
                        Some(cwd),
                        StdDuration::from_secs(20),
                        label,
                    )
                },
            )?;
            Ok(())
        }

        fn reconcile_pending_replication(&self) -> Result<()> {
            let mut targets = std::collections::BTreeSet::new();
            for debt in self.queue.replication_debts(Some(&self.options.repo_key))? {
                if matches!(debt.outcome.as_str(), "succeeded" | "superseded")
                    || !targets.insert(debt.destination_key.clone())
                {
                    continue;
                }
                crate::composition::reconcile_replication_debt(
                    &self.queue,
                    &self.lease_owner_id,
                    &debt.id,
                    true,
                    |debt_id, args, cwd, _label| self.run_replication_git(debt_id, args, cwd),
                )?;
            }
            Ok(())
        }

        pub fn resume_item(&self, item_id: &str) -> Result<QueueItem> {
            let operation = RepositoryOperationLease::acquire(
                self.queue.clone(),
                &self.options.repo_path,
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )?;
            self.initialize_workspaces()?;
            self.reconcile_private_refs(&operation)?;
            self.queue
                .repository(&self.options.repo_key)?
                .policy
                .require_queue_mutation(item_id)?;
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
            let item = self.resume_item_owned(item_id, &operation)?;
            self.reconcile_private_refs(&operation)?;
            Ok(item)
        }

        fn resume_item_owned(
            &self,
            item_id: &str,
            operation: &RepositoryOperationLease,
        ) -> Result<QueueItem> {
            let item = self.queue.get_item(item_id)?;
            if item.repo_key != self.options.repo_key {
                anyhow::bail!(
                    "item {item_id} belongs to repo queue {}, not {}",
                    item.repo_key,
                    self.options.repo_key
                );
            }
            self.require_item_integration_policy(&item)?;
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
                        if crate::agent_runner::exact_process_is_alive(
                            launch.launcher.pid,
                            launch.launcher.process_start_ticks,
                        )? {
                            return Ok(item);
                        }
                        crate::agent_runner::stop_and_verify_systemd_unit(
                            &effort.runner.sandbox.systemctl,
                            &launch.unit_name,
                        )?;
                        let workspace = self.load_owned_workspace(&item)?;
                        crate::agent_runner::quarantine_restart_artifacts(
                            &workspace,
                            &launch.cycle_id,
                        )?;
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
                            self.integrate_item(item, &attempt, operation)
                        });
                    }
                    crate::control_domain::IntegrationEffortState::TargetMovePending(_) => {
                        return self.with_lease_heartbeat("target-move reconciliation", || {
                            self.integrate_item(item, &attempt, operation)
                        });
                    }
                    crate::control_domain::IntegrationEffortState::ReplacementPending(_)
                    | crate::control_domain::IntegrationEffortState::CandidateReady(_)
                    | crate::control_domain::IntegrationEffortState::Validating(_)
                    | crate::control_domain::IntegrationEffortState::Landing(_)
                    | crate::control_domain::IntegrationEffortState::LandingUncertain(_)
                    | crate::control_domain::IntegrationEffortState::Integrated(_)
                    | crate::control_domain::IntegrationEffortState::Cancelled(_) => {}
                    crate::control_domain::IntegrationEffortState::AgentRunning(running) => {
                        crate::agent_runner::stop_exact_runner_service(
                            &effort.runner.sandbox.systemctl,
                            &running.unit_name,
                            &running.control_group,
                            running.pid,
                            running.process_start_ticks,
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
                    if item.status != QueueStatus::Merged {
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
                    self.with_lease_heartbeat("integrating", || {
                        self.integrate_item(item, &attempt, operation)
                    })
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
                    self.with_lease_heartbeat("integrating", || {
                        self.integrate_item(item, &attempt, operation)
                    })
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
                    self.with_lease_heartbeat("integrating", || {
                        self.integrate_item(item, &attempt, operation)
                    })
                }
                QueueStatus::Validated => self.with_lease_heartbeat("integrating", || {
                    self.integrate_item(item, &attempt, operation)
                }),
                QueueStatus::Integrating => self.with_lease_heartbeat("integrating", || {
                    self.integrate_item(item, &attempt, operation)
                }),
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
            self.workspaces().remove(
                path,
                expected_id,
                expected_source_id,
                |gate| {
                    self.ensure_repo_lease()?;
                    self.queue
                        .record_workspace_gc_debt(&self.workspaces().registry_identity)?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.lease_authority(),
                || {
                    self.queue
                        .clear_workspace_gc_debt(&self.workspaces().registry_identity)
                },
            )
        }

        fn remove_retained_workspace(&self, identity: &WorkspaceIdentity) -> Result<bool> {
            self.workspaces().remove_retained(
                identity,
                |gate| {
                    self.ensure_repo_lease()?;
                    self.queue
                        .record_workspace_gc_debt(&self.workspaces().registry_identity)?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.lease_authority(),
                || {
                    self.queue
                        .clear_workspace_gc_debt(&self.workspaces().registry_identity)
                },
            )
        }

        fn gc_workspaces(&self) -> Result<()> {
            self.workspaces().gc(
                |gate| {
                    self.ensure_repo_lease()?;
                    self.queue
                        .record_workspace_gc_debt(&self.workspaces().registry_identity)?;
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.lease_authority(),
                || {
                    self.queue
                        .clear_workspace_gc_debt(&self.workspaces().registry_identity)
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
            let expected = self.workspaces().expected_path(&item.id)?;
            let path = self.workspaces().verify_retained(identity)?;
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

        fn run_supervised_internal_git_command<I, S>(
            &self,
            item_id: &str,
            attempt_id: &str,
            args: I,
            cwd: &Path,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            let outcome = command_output_timeout(
                "git",
                args,
                Some(cwd),
                StdDuration::from_secs(20),
                |gate| {
                    self.authorize_execution_start(
                        item_id,
                        attempt_id,
                        QueueStatus::Integrating,
                        |_| {
                            gate.write_all(b"run\n")
                                .context("release internal Git command gate")
                        },
                    )
                },
                || self.execution_authority(item_id),
            )?;
            match outcome {
                CommandOutputOutcome::Exited(output) if output.status.success() => Ok(output),
                CommandOutputOutcome::Exited(output) => anyhow::bail!(
                    "internal Git command failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                CommandOutputOutcome::Cancelled => {
                    anyhow::bail!("internal Git command lost execution authority")
                }
            }
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
            self.run_supervised_item_command_output_with_landing_release(
                item_id,
                attempt_id,
                expected_status,
                program,
                args,
                cwd,
                timeout,
                label,
                None,
            )
        }

        fn run_supervised_provider_command(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            provider: crate::repository_policy::Provider,
            program: &str,
            args: &[OsString],
        ) -> Result<Output> {
            let outcome = crate::providers::output_with_authority(
                provider,
                program,
                args,
                None,
                StdDuration::from_secs(20),
                |gate| {
                    self.authorize_execution_start(item_id, attempt_id, expected_status, |_| {
                        gate.write_all(b"run\n")
                            .context("release provider CLI command admission gate")
                    })
                },
                || self.execution_authority(item_id),
            )
            .map_err(|error| {
                crate::providers::ProviderInfrastructureError::from_execution(program, error)
            })?;
            match outcome {
                CommandOutputOutcome::Exited(output) => Ok(output),
                CommandOutputOutcome::Cancelled => {
                    Err(crate::providers::ProviderInfrastructureError::Cancelled {
                        program: program.to_string(),
                    }
                    .into())
                }
            }
        }

        fn run_provider_command_at_effect_release(
            &self,
            item_id: &str,
            provider: crate::repository_policy::Provider,
            program: &str,
            args: &[OsString],
        ) -> Result<Output> {
            let outcome = crate::providers::output_with_authority(
                provider,
                program,
                args,
                None,
                StdDuration::from_secs(20),
                |gate| match self.execution_authority(item_id)? {
                    ExecutionAuthority::Active => {
                        gate.write_all(b"run\n")?;
                        Ok(true)
                    }
                    ExecutionAuthority::Cancelled => Ok(false),
                    ExecutionAuthority::Lost(message) => anyhow::bail!(message),
                },
                || self.execution_authority(item_id),
            )
            .map_err(|error| {
                crate::providers::ProviderInfrastructureError::from_execution(program, error)
            })?;
            match outcome {
                CommandOutputOutcome::Exited(output) => Ok(output),
                CommandOutputOutcome::Cancelled => {
                    Err(crate::providers::ProviderInfrastructureError::Cancelled {
                        program: program.to_string(),
                    }
                    .into())
                }
            }
        }

        #[allow(clippy::too_many_arguments)]
        fn run_supervised_item_command_output_with_landing_release<I, S>(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            program: &str,
            args: I,
            cwd: Option<&Path>,
            timeout: StdDuration,
            label: &str,
            landing_release: Option<(&str, &str)>,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            let args = args
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect::<Vec<_>>();
            let outcome = command_output_timeout(
                program,
                &args,
                cwd,
                timeout,
                |gate| {
                    let mut external_policy = if program == "git"
                        && crate::git_command::is_external_operation(&args)
                    {
                        let cwd =
                            cwd.context("Git command requires an explicit working directory")?;
                        let repository = self.queue.repository(&self.options.repo_key)?;
                        let canonical = repository.policy.canonical_repository;
                        crate::git_command::authorize_external_effect_with_verified_provider(
                            cwd, &args, &canonical,
                        )?;
                        let credential = if let Some(provider) = canonical.provider() {
                            let mut executor = |provider, program: &str, args: &[OsString]| {
                                self.run_provider_command_at_effect_release(
                                    item_id, provider, program, args,
                                )
                            };
                            crate::providers::verify_repository_with(
                                provider,
                                canonical.object_format(),
                                &mut executor,
                            )
                            .context("reverify provider identity at Git effect release boundary")?;
                            if crate::git_command::external_effect_uses_https(&args, &canonical)? {
                                crate::providers::https_credential_with(provider, &mut executor)?
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        Some((repository.policy_revision, canonical, credential))
                    } else {
                        None
                    };
                    let record_release = |tx: &rusqlite::Transaction<'_>| {
                        if let Some((effort_id, command_id)) = landing_release {
                            crate::control_store::ControlStore::release_landing(
                                tx, effort_id, command_id,
                            )?;
                        }
                        Ok(())
                    };
                    let authorized = match &external_policy {
                        Some((revision, canonical, _)) => self
                            .authorize_execution_start_after_provider_check(
                                item_id,
                                attempt_id,
                                expected_status,
                                *revision,
                                canonical,
                                record_release,
                            ),
                        None => self.authorize_execution_start(
                            item_id,
                            attempt_id,
                            expected_status,
                            record_release,
                        ),
                    }?;
                    if !authorized {
                        return Ok(false);
                    }
                    if let Some((_, _, credential)) = &mut external_policy {
                        gate.set_https_credential(credential.take());
                    }
                    gate.write_all(if landing_release.is_some() {
                        b"landing\n"
                    } else {
                        b"run\n"
                    })
                    .with_context(|| format!("release {label} command admission gate"))?;
                    Ok(true)
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
            release_gate: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<()>,
        ) -> Result<bool> {
            self.ensure_repo_lease()?;
            self.queue.authorize_execution_start(
                item_id,
                attempt_id,
                expected_status,
                crate::sqlite::ExecutionStartAuthority::RepositoryLease {
                    repo_key: &self.options.repo_key,
                    owner_id: &self.lease_owner_id,
                },
                release_gate,
            )
        }

        fn authorize_execution_start_after_provider_check(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            policy_revision: i64,
            canonical: &crate::repository_policy::GitRepository,
            release_gate: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<()>,
        ) -> Result<bool> {
            self.ensure_repo_lease()?;
            self.queue.authorize_execution_start(
                item_id,
                attempt_id,
                expected_status,
                crate::sqlite::ExecutionStartAuthority::ProviderVerified {
                    repo_key: &self.options.repo_key,
                    owner_id: &self.lease_owner_id,
                    policy_revision,
                    canonical,
                },
                release_gate,
            )
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
            if let crate::control_domain::IntegrationEffortState::Landing(landing) = &effort.state {
                if landing.candidate_sha != candidate_sha
                    || landing.expected_target_sha != expected_target_sha
                {
                    anyhow::bail!("prepared landing authority differs from requested command");
                }
                return Ok(None);
            }
            let attempt = self.queue.get_attempt(attempt_id)?;
            let policy = self.policy_for_attempt(&attempt)?;
            let policy_digest = attempt
                .policy_digest
                .clone()
                .context("landing attempt has no exact policy digest")?;
            let signoff = match (policy.policy, attempt.signoff_evidence_json.as_ref()) {
                (crate::composition::ValidationPolicy::None, None) => {
                    crate::control_domain::SignoffDisposition::NoValidation { policy_digest }
                }
                (
                    crate::composition::ValidationPolicy::Command {
                        signoff: crate::composition::SignoffPolicy::None,
                        ..
                    },
                    None,
                ) => crate::control_domain::SignoffDisposition::ValidationWithoutSignoff {
                    policy_digest,
                },
                (
                    crate::composition::ValidationPolicy::Command {
                        signoff: crate::composition::SignoffPolicy::Required { .. },
                        ..
                    },
                    Some(_),
                ) => crate::control_domain::SignoffDisposition::Evidence {
                    evidence_id: format!("attempt:{attempt_id}"),
                    candidate_sha: candidate_sha.to_string(),
                    policy_digest,
                },
                _ => anyhow::bail!(
                    "persisted policy and signoff evidence variant are inconsistent for landing"
                ),
            };
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
            #[cfg(debug_assertions)]
            if std::env::var_os("IQ_TEST_INTEGRATION_STOP_AFTER_MARK_INTEGRATED").is_some() {
                std::process::exit(95);
            }
            let item = self.queue.get_item(item_id)?;
            let repository = self.queue.repository(&item.repo_key)?;
            crate::composition::reconcile_registered_checkout(
                &self.queue,
                &repository,
                &self.lease_owner_id,
                remote_target_sha,
                |_path, _target_sha| {
                    anyhow::bail!("registered checkout changed after exact landing verification")
                },
            )?;
            self.replicate_canonical_landing(&item, landed_commit_sha)?;
            self.cleanup_terminal_item(&item)?;
            self.queue.get_item(item_id)
        }

        fn replicate_canonical_landing(&self, item: &QueueItem, landed_sha: &str) -> Result<()> {
            let debts = self
                .queue
                .replication_debts(Some(&item.repo_key))?
                .into_iter()
                .filter(|debt| debt.item_id == item.id)
                .collect::<Vec<_>>();
            for debt in debts {
                if debt.canonical_source_sha != landed_sha {
                    anyhow::bail!("replication debt differs from exact canonical landing");
                }
                if debt.operation == "pin_source" {
                    let repository = self.queue.repository(&item.repo_key)?;
                    crate::composition::publish_replication_source_pin(
                        &self.queue,
                        &repository,
                        &self.lease_owner_id,
                        &debt,
                        &mut |debt_id, args, cwd, _label| {
                            self.run_replication_git(debt_id, args, cwd)
                        },
                    )?;
                }
            }
            self.reconcile_pending_replication()
        }

        fn run_replication_git<I, S>(
            &self,
            debt_id: &str,
            args: I,
            cwd: Option<&Path>,
        ) -> Result<Output>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            let args = args
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect::<Vec<_>>();
            let outcome = command_output_timeout(
                "git",
                &args,
                cwd,
                StdDuration::from_secs(60),
                |gate| {
                    self.ensure_repo_lease()?;
                    let debt = self.queue.authorize_replication_command(
                        &self.options.repo_key,
                        debt_id,
                        &self.lease_owner_id,
                        &args,
                    )?;
                    if debt.operation != "pin_source" {
                        let repository = self.queue.repository(&self.options.repo_key)?;
                        let preserved_ref = format!("refs/iq/replication/{debt_id}");
                        let preserved = git_output(
                            &repository.owned_root_path,
                            ["rev-parse", "--verify", preserved_ref.as_str()],
                        )?;
                        if preserved != debt.canonical_source_sha {
                            anyhow::bail!(
                                "replication source pin differs from landed item authority"
                            );
                        }
                    }
                    if crate::git_command::is_external_operation(&args) {
                        let cwd =
                            cwd.context("Git command requires an explicit working directory")?;
                        let credential = crate::git_command::authorize_external_effect(
                            cwd,
                            &args,
                            &debt.replica,
                        )?;
                        self.ensure_repo_lease()?;
                        let current = self.queue.authorize_replication_command(
                            &self.options.repo_key,
                            debt_id,
                            &self.lease_owner_id,
                            &args,
                        )?;
                        if current != debt {
                            anyhow::bail!("replication debt changed during provider verification");
                        }
                        crate::git_command::authorize_external_effect_with_verified_provider(
                            cwd,
                            &args,
                            &debt.replica,
                        )?;
                        gate.set_https_credential(credential);
                    }
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || self.lease_authority(),
            )?;
            match outcome {
                CommandOutputOutcome::Exited(output) if output.status.success() => Ok(output),
                CommandOutputOutcome::Exited(output) => anyhow::bail!(
                    "replication Git command failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                CommandOutputOutcome::Cancelled => {
                    anyhow::bail!("replication lost repository operation authority")
                }
            }
        }

        fn cleanup_terminal_item(&self, item: &QueueItem) -> Result<()> {
            if !matches!(
                item.status,
                QueueStatus::Integrated | QueueStatus::Cancelled
            ) {
                anyhow::bail!("item {} is not terminal", item.id);
            }
            let expected = self.workspaces().expected_path(&item.id)?;
            match &item.workspace {
                WorkspaceState::Cleaned { .. } => return Ok(()),
                WorkspaceState::NotCreated => {}
                WorkspaceState::CreationIntent { path } => {
                    if self.workspaces().normalize_owned_path(Path::new(path))? != expected {
                        anyhow::bail!(
                            "item {} workspace {} does not match IQ-owned path {}",
                            item.id,
                            path,
                            expected.display()
                        );
                    }
                    if entry_exists(&expected)? {
                        let actual = self
                            .workspaces()
                            .list()?
                            .into_iter()
                            .find(|identity| Path::new(&identity.path) == expected)
                            .context("terminal workspace creation path has unknown occupancy")?;
                        let actual_path = Path::new(&actual.path);
                        let dirty = workspace_dirty(actual_path)?.is_some();
                        let active_git_operation =
                            crate::composition::has_git_operation(actual_path)?;
                        if dirty || active_git_operation {
                            self.control_store.record_terminal_workspace_preserved(
                                &item.id,
                                &crate::control_store::TerminalWorkspaceTarget::Retained {
                                    identity: actual.clone(),
                                },
                                &actual,
                                dirty,
                                active_git_operation,
                            )?;
                            return Ok(());
                        }
                        self.remove_clean_terminal_workspace(&actual)?;
                    }
                }
                WorkspaceState::Retained { identity } => {
                    let actual = self.workspaces().resolve_retained(identity)?;
                    if let Some(actual) = actual.as_ref() {
                        let path = Path::new(&actual.path);
                        let dirty = workspace_dirty(path)?.is_some();
                        let active_git_operation = crate::composition::has_git_operation(path)?;
                        if dirty || active_git_operation {
                            self.control_store.record_terminal_workspace_preserved(
                                &item.id,
                                &crate::control_store::TerminalWorkspaceTarget::Retained {
                                    identity: identity.clone(),
                                },
                                actual,
                                dirty,
                                active_git_operation,
                            )?;
                            return Ok(());
                        }
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

        fn reconcile_pending_target_before_merge(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            repository: &crate::sqlite::RegisteredRepository,
        ) -> Result<()> {
            let canonical_fetch = self.canonical_fetch_transport()?;
            let target_sha = repository.checkout_reconciliation.target_sha();
            let attempt_target = self.queue.get_attempt(&attempt.id)?.target_base_sha;
            let private_target_ref = match attempt_target.as_deref() {
                Some(attempt_target) if attempt_target == target_sha => {
                    format!("refs/iq/targets/{}", attempt.id)
                }
                Some(_) => {
                    anyhow::bail!(
                        "attempt target authority differs from pending checkout authority"
                    )
                }
                None => format!(
                    "refs/iq/repository-targets/{}/{}",
                    repository.key, target_sha
                ),
            };
            let exact_refspec = format!("+{target_sha}:{private_target_ref}");
            self.fetch_for_merge(
                item,
                attempt,
                ["fetch", "--no-tags", &canonical_fetch, &exact_refspec],
            )?;
            let fetched = git_output(&self.options.repo_path, ["rev-parse", &private_target_ref])?;
            if fetched != target_sha {
                anyhow::bail!("private fetched target differs from pending checkout authority");
            }
            self.run_supervised_item_command(
                &item.id,
                &attempt.id,
                QueueStatus::Merging,
                "git",
                ["cat-file", "-e", &format!("{target_sha}^{{commit}}")],
                Some(&self.options.repo_path),
                StdDuration::from_secs(60),
                "verify pending target object",
            )?;
            let tracking_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            self.run_supervised_item_command(
                &item.id,
                &attempt.id,
                QueueStatus::Merging,
                "git",
                ["update-ref", &tracking_ref, target_sha],
                Some(&self.options.repo_path),
                StdDuration::from_secs(60),
                "publish pending target ref",
            )?;
            if git_output(&self.options.repo_path, ["rev-parse", &tracking_ref])? != target_sha {
                anyhow::bail!("published target differs from pending checkout authority");
            }
            crate::composition::reconcile_registered_checkout(
                &self.queue,
                repository,
                &self.lease_owner_id,
                target_sha,
                |path, target_sha| {
                    self.run_supervised_item_command(
                        &item.id,
                        &attempt.id,
                        QueueStatus::Merging,
                        "git",
                        ["reset", "--hard", target_sha],
                        Some(path),
                        StdDuration::from_secs(60),
                        "reconcile pending target checkout",
                    )?;
                    Ok(())
                },
            )
        }

        fn merge_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            let repository = self.queue.repository(&item.repo_key)?;
            let canonical_fetch = self.canonical_fetch_transport()?;
            if !matches!(
                &repository.checkout_reconciliation,
                crate::sqlite::CheckoutReconciliationState::Ready(_)
            ) {
                if let Err(error) =
                    self.reconcile_pending_target_before_merge(&item, attempt, &repository)
                {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Merging,
                        BlockedReason::Infra,
                        &format!("failed to reconcile pending target before merge: {error:#}"),
                    );
                }
                return self.queue.get_item(&item.id);
            }
            let base_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            let current_attempt = self.queue.get_attempt(&attempt.id)?;
            let durable_target = current_attempt.target_base_sha.as_deref();
            let checkout_target = repository.checkout_reconciliation.target_sha();
            let target_is_recorded = durable_target == Some(checkout_target);
            let base_sha = if target_is_recorded {
                checkout_target.to_string()
            } else if durable_target.is_some() {
                anyhow::bail!("attempt target authority differs from checkout reconciliation")
            } else {
                let target_full_ref = format!("refs/heads/{}", item.target_branch);
                let observed = self.run_supervised_item_command_output(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    [
                        "ls-remote",
                        "--exit-code",
                        &canonical_fetch,
                        &target_full_ref,
                    ],
                    Some(&self.options.repo_path),
                    StdDuration::from_secs(60),
                    "observe exact initial target",
                )?;
                if !observed.status.success() {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Merging,
                        BlockedReason::Infra,
                        &format!(
                            "failed to observe target before merge: {}",
                            String::from_utf8_lossy(&observed.stderr).trim()
                        ),
                    );
                }
                let observed = crate::composition::parse_exact_remote_ref(
                    &observed.stdout,
                    &target_full_ref,
                    repository.policy.canonical_repository.object_format(),
                )?;
                self.ensure_repo_lease()?;
                self.queue.begin_initial_target_fetch(
                    &item.repo_key,
                    &self.lease_owner_id,
                    &item.id,
                    &attempt.id,
                    &observed,
                )?;
                stop_initial_target_after("observation");
                observed
            };
            let current_repository = self.queue.repository(&item.repo_key)?;
            if !current_repository
                .checkout_reconciliation
                .is_ready_for(&base_sha)
            {
                let private_target_ref = format!("refs/iq/targets/{}", attempt.id);
                let exact_refspec = format!("+{base_sha}:{private_target_ref}");
                if let Err(error) = self.fetch_for_merge(
                    &item,
                    attempt,
                    ["fetch", "--no-tags", &canonical_fetch, &exact_refspec],
                ) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Merging,
                        BlockedReason::Infra,
                        &format!("failed to fetch exact observed target before merge: {error}"),
                    );
                }
                let fetched =
                    git_output(&self.options.repo_path, ["rev-parse", &private_target_ref])?;
                if fetched != base_sha {
                    anyhow::bail!("private fetched target differs from durable exact observation");
                }
                self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    ["cat-file", "-e", &format!("{base_sha}^{{commit}}")],
                    Some(&self.options.repo_path),
                    StdDuration::from_secs(60),
                    "verify exact observed target object",
                )?;
                stop_initial_target_after("fetch");
                self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Merging,
                    "git",
                    ["update-ref", &base_ref, &base_sha],
                    Some(&self.options.repo_path),
                    StdDuration::from_secs(60),
                    "publish exact observed target ref",
                )?;
                let published = git_output(&self.options.repo_path, ["rev-parse", &base_ref])?;
                if published != base_sha {
                    anyhow::bail!("published target differs from durable exact observation");
                }
                crate::composition::reconcile_registered_checkout(
                    &self.queue,
                    &current_repository,
                    &self.lease_owner_id,
                    &base_sha,
                    |path, target_sha| {
                        self.run_supervised_item_command(
                            &item.id,
                            &attempt.id,
                            QueueStatus::Merging,
                            "git",
                            ["reset", "--hard", target_sha],
                            Some(path),
                            StdDuration::from_secs(60),
                            "reset owned root to observed initial target",
                        )?;
                        Ok(())
                    },
                )?;
            }
            let source_sha = match &item.source {
                crate::core::QueueSource::RemoteBranch { .. } => {
                    let (source_refspec, fetched_ref) = match &item.admission {
                        crate::sqlite::QueueAdmission::Direct { .. } => (
                            format!(
                                "+refs/heads/{}:refs/remotes/{}/{}",
                                item.source_branch, self.options.base_remote, item.source_branch
                            ),
                            format!(
                                "refs/remotes/{}/{}",
                                self.options.base_remote, item.source_branch
                            ),
                        ),
                        crate::sqlite::QueueAdmission::MergeRequest(_) => {
                            let fetched_ref = format!("refs/iq/mr-sources/{}", item.id);
                            (
                                format!("+{}:{fetched_ref}", item.source_branch),
                                fetched_ref,
                            )
                        }
                        crate::sqlite::QueueAdmission::LocalSubmission { .. } => {
                            anyhow::bail!("remote queue source has local admission")
                        }
                        crate::sqlite::QueueAdmission::HistoricalMergeRequest(_) => {
                            anyhow::bail!("terminal historical MR admission cannot be integrated")
                        }
                    };
                    match self.fetch_for_merge(
                        &item,
                        attempt,
                        ["fetch", &canonical_fetch, &source_refspec],
                    ) {
                        Ok(()) => git_output(
                            &self.options.repo_path,
                            ["rev-parse", fetched_ref.as_str()],
                        )?,
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

            let workspace = self.workspaces().expected_path(&item.id)?;
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
            self.workspaces().reconcile_pending_generation(generation)?;
            self.queue.complete_workspace_generation(
                &self.options.repo_key,
                "integration",
                generation,
            )?;
            let (created, rift_id) = self.workspaces().create(
                &item.id,
                |gate| {
                    self.authorize_execution_start(
                        &item.id,
                        &attempt.id,
                        QueueStatus::Merging,
                        |_| {
                            gate.write_all(b"run\n")
                                .context("release Rift creation admission gate")
                        },
                    )
                },
                || self.execution_authority(&item.id),
            )?;
            crate::git_command::authorize_current(&created)?;
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
                &self.workspaces().source_id,
            )?;
            let identity = WorkspaceIdentity {
                path: workspace_text.to_string(),
                rift_id,
                source_rift_id: self.workspaces().source_id.clone(),
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
                let composed = self.queue.set_conflict_metadata(
                    &item.id,
                    &attempt.id,
                    &conflict_json,
                    &base_sha,
                    &source_sha,
                )?;
                if composed.status == QueueStatus::Cancelled {
                    return Ok(composed);
                }
                let Some(effort) = self.ensure_effort_after_composition(&composed, attempt)? else {
                    return self.queue.get_item(&item.id);
                };
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

            let composed = self.queue.set_conflict_metadata(
                &item.id,
                &attempt.id,
                &json!({"files": [], "target_sha": base_sha, "source_sha": source_sha}),
                &base_sha,
                &source_sha,
            )?;
            if composed.status == QueueStatus::Cancelled {
                return Ok(composed);
            }
            let Some(effort) = self.ensure_effort_after_composition(&composed, attempt)? else {
                return self.queue.get_item(&item.id);
            };
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
            if self
                .control_store
                .candidate_publication_waits_for_cleanup(&effort.id)?
            {
                return Ok(item);
            }
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
                    object_format: self
                        .queue
                        .repository(&item.repo_key)?
                        .policy
                        .canonical_repository
                        .object_format(),
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
                        url: item
                            .admission
                            .merge_request()
                            .context("provider item has no MR admission")?
                            .url
                            .clone(),
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
            let launcher = crate::control_domain::LauncherAuthority {
                pid: std::process::id(),
                process_start_ticks: crate::agent_runner::process_start_ticks(std::process::id())?,
                token: Uuid::new_v4().to_string(),
            };
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
                                launcher: launcher.clone(),
                                input_sha256: format!("{:x}", Sha256::digest(&input_bytes)),
                                protocol_directory: protocol.to_path_buf(),
                                prepared_at: chrono::Utc::now().to_rfc3339(),
                                spawn_authority: crate::control_domain::SpawnAuthority::Open,
                            },
                        )
                    },
                    on_spawn_surrender: || {
                        self.control_store.surrender_cycle_spawn_authority(
                            &effort.id,
                            &launch_operation_id,
                            &self.lease_owner_id,
                            &launcher,
                        )
                    },
                    recheck_spawn_authority: || {
                        self.control_store.authorize_cycle_spawn(
                            &effort.id,
                            &launch_operation_id,
                            &self.lease_owner_id,
                            &launcher,
                        )
                    },
                    on_spawn_failed: || {
                        self.control_store.acknowledge_cycle_spawn_failed(
                            &effort.id,
                            &launch_operation_id,
                            &self.lease_owner_id,
                            &launcher,
                        )
                    },
                    on_started: |pid: u32,
                                 start: u64,
                                 control_group: &str,
                                 sandbox: &str,
                                 _protocol: &Path| {
                        self.control_store.record_cycle_started(
                            &effort.id,
                            &AgentRunning {
                                launch_operation_id: launch_operation_id.clone(),
                                unit_name: crate::control_domain::systemd_unit_name(&cycle_id)?,
                                cycle_id: cycle_id.clone(),
                                cycle_number: ready.next_cycle,
                                pid,
                                process_start_ticks: start,
                                control_group: control_group.to_string(),
                                authority_lease_id: self.lease_owner_id.clone(),
                                launcher: launcher.clone(),
                                sandbox_id: sandbox.to_string(),
                                input_sha256: format!("{:x}", Sha256::digest(&input_bytes)),
                                result: AtomicResultState::Absent,
                                started_at: chrono::Utc::now().to_rfc3339(),
                            },
                            &self.lease_owner_id,
                            &launcher,
                        )
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
            let mut command = crate::git_command::command_in(&workspace)?;
            command
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
                );
            let output = crate::git_command::service_output(&mut command)?;
            if !output.status.success() {
                anyhow::bail!(
                    "candidate builder commit-tree failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            let candidate = String::from_utf8(output.stdout)?.trim().to_string();
            let zero_oid = crate::git_command::expected_binding(&workspace)?
                .object_format
                .zero_oid();
            git(
                &workspace,
                [
                    "update-ref",
                    operation_ref.as_str(),
                    candidate.as_str(),
                    zero_oid.as_str(),
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
                let mut command = crate::git_command::command_in(&workspace)?;
                command
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
                    );
                let output = crate::git_command::service_output(&mut command)?;
                if !output.status.success() {
                    anyhow::bail!(
                        "candidate reconciliation commit-tree failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }
                let candidate = String::from_utf8(output.stdout)?.trim().to_string();
                let zero_oid = crate::git_command::expected_binding(&workspace)?
                    .object_format
                    .zero_oid();
                git(
                    &workspace,
                    [
                        "update-ref",
                        building.operation_ref.as_str(),
                        candidate.as_str(),
                        zero_oid.as_str(),
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
            Ok(true)
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
                    attempt
                        .policy_digest
                        .as_deref()
                        .context("attempt has no exact persisted policy digest")?,
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
            let validation_candidate_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
            let log_dir = self.evidence_dir(&item, attempt)?;
            let log_path = log_dir.path.join("validation.log");
            let record_validation = |exit_code| {
                self.queue.record_attempt_validation(
                    &attempt.id,
                    &AttemptValidationInvocation {
                        candidate_sha: &validation_candidate_sha,
                        command: &command,
                        exit_code,
                        log_path: &log_path.to_string_lossy(),
                    },
                )
            };
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
                        |_| {
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
                    record_validation(status.and_then(|status| status.code()).unwrap_or(-1) as i64)?;
                    return self.queue.get_item(&item.id);
                }
                EvidenceCommandOutcome::TimedOut(status) => {
                    self.ensure_repo_lease()?;
                    record_validation(status.code().unwrap_or(-1) as i64)?;
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
                record_validation(exit_code)?;
                return self.queue.get_item(&item.id);
            }
            if !status.success() {
                self.ensure_repo_lease()?;
                record_validation(exit_code)?;
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("validation command failed; inspect {}", log_path.display()),
                );
            }
            self.ensure_repo_lease()?;
            let invocation_number = record_validation(exit_code)?;
            let validated_sha = match git_output(&workspace, ["rev-parse", "HEAD"]) {
                Ok(sha) => sha,
                Err(error) => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!("cannot resolve candidate after validation: {error}"),
                    );
                }
            };
            if validated_sha != validation_candidate_sha {
                let repair = self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Validating,
                    "git",
                    ["reset", "--hard", validation_candidate_sha.as_str()],
                    Some(&workspace),
                    StdDuration::from_secs(60),
                    "restore candidate after validation changed HEAD",
                );
                let repair_detail = match repair {
                    Ok(_) => String::new(),
                    Err(error) => format!("; candidate repair failed: {error}"),
                };
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::Infra,
                    &format!(
                        "validation command changed candidate HEAD from {validation_candidate_sha} to {validated_sha}{repair_detail}"
                    ),
                );
            }
            if let Some(dirty) = workspace_dirty(&workspace)? {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("validation modified candidate worktree: {dirty}"),
                );
            }
            self.queue.complete_attempt_validation(
                &attempt.id,
                invocation_number,
                &validated_sha,
            )?;
            self.control_store
                .complete_validation(&effort.id, &validated_sha)?;
            self.queue.get_item(&item.id)
        }

        fn integrate_item(
            &self,
            item: QueueItem,
            attempt: &Attempt,
            operation: &RepositoryOperationLease,
        ) -> Result<QueueItem> {
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
                    | crate::control_domain::IntegrationEffortState::TargetMovePending(_)
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
            match &item.admission {
                crate::sqlite::QueueAdmission::MergeRequest(_) => {
                    let pr_url = item
                        .admission
                        .merge_request()
                        .context("provider landing has no exact MR admission")?
                        .url
                        .clone();
                    return self.integrate_provider_item(item, attempt, &pr_url, operation);
                }
                crate::sqlite::QueueAdmission::Direct { .. }
                | crate::sqlite::QueueAdmission::LocalSubmission { .. } => {}
                crate::sqlite::QueueAdmission::HistoricalMergeRequest(_) => {
                    anyhow::bail!("terminal historical MR admission cannot enter landing")
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
                    MovedBaseCause::TargetMoved("target branch moved before direct landing"),
                    operation,
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
                    MovedBaseCause::TargetMoved("target branch moved after signoff"),
                    operation,
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
            self.land_exact_candidate(
                &item,
                attempt,
                &workspace,
                &landed_sha,
                &remote_sha,
                operation,
            )
        }

        fn land_exact_candidate(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            candidate_sha: &str,
            expected_target_sha: &str,
            operation: &RepositoryOperationLease,
        ) -> Result<QueueItem> {
            let canonical_push = self.canonical_push_transport()?;
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
            let landing_ref = format!("refs/iq/landings/{}", attempt.id);
            let landing_refspec = format!("+{candidate_sha}:{landing_ref}");
            self.run_supervised_internal_git_command(
                &item.id,
                &attempt.id,
                [
                    OsString::from("fetch"),
                    OsString::from("--no-tags"),
                    workspace.as_os_str().to_os_string(),
                    OsString::from(&landing_refspec),
                ],
                &self.options.repo_path,
            )?;
            let pinned_candidate =
                git_output(&self.options.repo_path, ["rev-parse", &landing_ref])?;
            if pinned_candidate != candidate_sha {
                anyhow::bail!("canonical landing pin differs from validated candidate");
            }
            self.ensure_registered_remote_identity_for_item(
                item,
                attempt,
                QueueStatus::Integrating,
            )?;
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
            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("prepared landing effort disappeared")?;
            let command_id = format!("git-push:{}", item.id);
            let landing_result = self.run_supervised_item_command_output_with_landing_release(
                &item.id,
                &attempt.id,
                QueueStatus::Integrating,
                "git",
                [
                    "push",
                    "--porcelain",
                    lease.as_str(),
                    &canonical_push,
                    &push_ref,
                ],
                Some(workspace),
                StdDuration::from_secs(20),
                "landing",
                Some((&effort.id, &command_id)),
            );
            if landing_result
                .as_ref()
                .is_ok_and(|output| definite_force_with_lease_rejection(output, &target_ref))
            {
                return self.recover_definite_cas_rejection(
                    item,
                    attempt,
                    workspace,
                    expected_target_sha,
                    &command_id,
                    operation,
                );
            }
            if let Err(error) = &landing_result {
                let effort = self
                    .control_store
                    .effort_for_item(&item.id)?
                    .context("landing effort disappeared after command admission")?;
                if matches!(
                    effort.state,
                    crate::control_domain::IntegrationEffortState::Landing(_)
                ) {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("landing command was not released: {error:#}"),
                    );
                }
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
            command_id: &str,
            operation: &RepositoryOperationLease,
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
            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("definite rejection has no integration effort")?;
            let rejection = self.control_store.authorize_definite_landing_rejection(
                &effort.id,
                command_id,
                expected_target_sha,
            )?;
            if let Some(blocked) = self.merge_moved_base(
                item,
                attempt,
                workspace,
                &moved_target,
                MovedBaseCause::DefiniteLandingRejection(&rejection),
                operation,
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
            self.block_and_get(
                &item.id,
                BlockedPhase::Integrating,
                BlockedReason::Infra,
                &format!(
                    "fenced exact landing remains unresolved at target {remote_sha}; target observation alone is not proof of compare-and-set rejection"
                ),
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
            self.ensure_registered_remote_identity_for_item(
                item,
                attempt,
                QueueStatus::Integrating,
            )?;
            let canonical_push = self.canonical_push_transport()?;
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
                    &canonical_push,
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
                    &canonical_push,
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
                        |_| {
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
            let gh = crate::providers::provider_program(crate::repository_policy::Provider::Github)
                .map_err(|error| SignoffQueryError::Provider(format!("{error:#}")))?;
            let endpoint = format!(
                "repos/{}/commits/{candidate_sha}/statuses",
                policy.repository
            );
            let output = crate::providers::output_with_authority(
                crate::repository_policy::Provider::Github,
                &gh,
                ["api", endpoint.as_str()],
                None,
                StdDuration::from_secs(60),
                |gate| {
                    self.authorize_execution_start(
                        item_id,
                        attempt_id,
                        QueueStatus::Integrating,
                        |_| {
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
            cause: MovedBaseCause<'_>,
            operation: &RepositoryOperationLease,
        ) -> Result<Option<QueueItem>> {
            let (summary_prefix, landing_rejection) = match cause {
                MovedBaseCause::TargetMoved(summary) => (summary, None),
                MovedBaseCause::DefiniteLandingRejection(rejection) => {
                    ("definite compare-and-set rejection", Some(rejection))
                }
            };
            let effort = self
                .control_store
                .effort_for_item(&item.id)?
                .context("target movement item has no integration effort")?;
            match landing_rejection {
                Some(rejection) => self
                    .control_store
                    .begin_target_move_after_definite_landing_rejection(
                        &effort.id,
                        moved_base_sha,
                        rejection,
                    )?,
                None => self
                    .control_store
                    .begin_target_move(&effort.id, moved_base_sha)?,
            }
            self.run_supervised_internal_git_command(
                &item.id,
                &attempt.id,
                [
                    "fetch",
                    self.options
                        .repo_path
                        .to_str()
                        .context("registered repository path is not valid UTF-8")?,
                    moved_base_sha,
                ],
                workspace,
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
            match landing_rejection {
                Some(rejection) => self
                    .control_store
                    .prepare_target_recomposition_after_definite_landing_rejection(
                        &effort.id,
                        moved_base_sha,
                        rejection,
                    )?,
                None => self
                    .control_store
                    .prepare_target_recomposition(&effort.id, moved_base_sha)?,
            };
            self.reconcile_private_refs(operation)?;
            self.control_store.complete_target_recomposition(
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
            Ok(Some(self.integrate_item(
                validated,
                &recomposed_attempt,
                operation,
            )?))
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
            let validation_candidate_sha = git_output(workspace, ["rev-parse", "HEAD"])?;
            let log_dir = self.evidence_dir(item, attempt)?;
            let safe_label = label.replace([' ', '/'], "-");
            let log_path = log_dir
                .path
                .join(format!("revalidation-after-{safe_label}.log"));
            let record_validation = |exit_code| {
                self.queue.record_attempt_revalidation(
                    &attempt.id,
                    moved_base_sha,
                    &AttemptValidationInvocation {
                        candidate_sha: &validation_candidate_sha,
                        command: &command,
                        exit_code,
                        log_path: &log_path.to_string_lossy(),
                    },
                )
            };
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
                        |_| {
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
                    record_validation(status.and_then(|status| status.code()).unwrap_or(-1) as i64)?;
                    return Ok(Some(self.queue.get_item(&item.id)?));
                }
                EvidenceCommandOutcome::TimedOut(status) => {
                    self.ensure_repo_lease()?;
                    record_validation(status.code().unwrap_or(-1) as i64)?;
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
                record_validation(exit_code)?;
                return Ok(Some(self.queue.get_item(&item.id)?));
            }
            if !status.success() {
                self.ensure_repo_lease()?;
                record_validation(exit_code)?;
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
            self.ensure_repo_lease()?;
            let invocation_number = record_validation(exit_code)?;
            let validated_sha = match git_output(workspace, ["rev-parse", "HEAD"]) {
                Ok(sha) => sha,
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!("cannot resolve candidate after revalidation: {error}"),
                    )?));
                }
            };
            if validated_sha != validation_candidate_sha {
                let repair = self.run_supervised_item_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Integrating,
                    "git",
                    ["reset", "--hard", validation_candidate_sha.as_str()],
                    Some(workspace),
                    StdDuration::from_secs(60),
                    "restore candidate after revalidation changed HEAD",
                );
                let repair_detail = match repair {
                    Ok(_) => String::new(),
                    Err(error) => format!("; candidate repair failed: {error}"),
                };
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::Infra,
                    &format!(
                        "revalidation after {label} changed candidate HEAD from {validation_candidate_sha} to {validated_sha}{repair_detail}"
                    ),
                )?));
            }
            if let Some(dirty) = workspace_dirty(workspace)? {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("revalidation after {label} modified candidate worktree: {dirty}"),
                )?));
            }
            self.queue.complete_attempt_validation(
                &attempt.id,
                invocation_number,
                &validated_sha,
            )?;
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
                        .admission
                        .merge_request()
                        .context("provider item has no exact MR admission")?
                        .url
                        .clone(),
                    context: phase.to_string(),
                    candidate_sha,
                    status,
                    evidence: message.to_string(),
                },
            )
        }

        fn integrate_provider_item(
            &self,
            item: QueueItem,
            attempt: &Attempt,
            pr_url: &str,
            operation: &RepositoryOperationLease,
        ) -> Result<QueueItem> {
            let admission = item
                .admission
                .merge_request()
                .context("provider integration has no MR admission")?;
            if admission.url != pr_url || admission.head_sha != item.current_head_sha {
                anyhow::bail!("provider integration differs from exact MR admission");
            }
            let locator = crate::providers::merge_request_locator(pr_url)?;
            if locator.provider != admission.provider
                || locator.host != admission.provider_host
                || locator.repository != admission.repository
                || locator.identity != admission.identity
                || admission.target_branch != item.target_branch
            {
                anyhow::bail!("provider URL identity differs from exact MR admission");
            }
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
                        let reason = if error
                            .downcast_ref::<crate::providers::ProviderInfrastructureError>()
                            .is_some()
                        {
                            BlockedReason::Infra
                        } else {
                            BlockedReason::Provider
                        };
                        return self.block_and_get(
                            &item.id,
                            BlockedPhase::Integrating,
                            reason,
                            &format!("failed to reconcile fenced provider landing: {error}"),
                        );
                    }
                }
            }
            if self
                .queue
                .events(&item.id)?
                .iter()
                .any(|event| event.event_type == "merge_resumed")
            {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    "coding agent must update the admitted MR; IQ cannot push source changes",
                );
            }
            if let Err(error) = self.fetch_target_supervised(&item, attempt) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("failed to fetch target before provider policy check: {error}"),
                );
            }
            let expected_repository = crate::repository_policy::ProviderRepository {
                provider: admission.provider,
                host: admission.provider_host.clone(),
                repository: admission.repository.clone(),
                repository_id: admission.repository_id.clone(),
            };
            let mut provider_executor = |provider_kind, program: &str, args: &[OsString]| {
                self.run_supervised_provider_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Integrating,
                    provider_kind,
                    program,
                    args,
                )
            };
            if let Err(error) = provider.verify_repository(
                &expected_repository,
                self.queue
                    .repository(&item.repo_key)?
                    .policy
                    .canonical_repository
                    .object_format(),
                &mut provider_executor,
            ) {
                let reason = if error
                    .downcast_ref::<crate::providers::ProviderInfrastructureError>()
                    .is_some()
                {
                    BlockedReason::Infra
                } else {
                    BlockedReason::Provider
                };
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    reason,
                    &format!("provider repository identity cannot be revalidated: {error:#}"),
                );
            }
            let snapshot = match provider.snapshot(pr_url, &mut provider_executor) {
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
            if snapshot.repository != expected_repository {
                return self.block_provider_and_get(
                    &item,
                    BlockedPhase::Integrating,
                    crate::control_domain::ProviderGateStatus::Failed,
                    "provider repository identity differs from exact admission",
                );
            }
            if snapshot.target_branch != admission.target_branch {
                return self.block_provider_and_get(
                    &item,
                    BlockedPhase::Integrating,
                    crate::control_domain::ProviderGateStatus::Failed,
                    "provider target branch moved from exact MR admission",
                );
            }
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
                    MovedBaseCause::TargetMoved("PR/MR base moved before provider landing"),
                    operation,
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
            let signed_snapshot = match provider.snapshot(pr_url, &mut provider_executor) {
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
            if signed_snapshot.repository != snapshot.repository {
                return self.block_provider_and_get(
                    &item,
                    BlockedPhase::Integrating,
                    crate::control_domain::ProviderGateStatus::Failed,
                    "provider repository identity moved after signoff",
                );
            }
            if signed_snapshot.target_branch != admission.target_branch {
                return self.block_provider_and_get(
                    &item,
                    BlockedPhase::Integrating,
                    crate::control_domain::ProviderGateStatus::Failed,
                    "provider target branch moved after signoff",
                );
            }
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
                    MovedBaseCause::TargetMoved("PR/MR base moved after signoff"),
                    operation,
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

            let error = provider.atomic_landing_unsupported();
            self.block_provider_and_get(
                &item,
                BlockedPhase::Integrating,
                crate::control_domain::ProviderGateStatus::Failed,
                &format!("provider landing is unsupported before mutation: {error:#}"),
            )
        }

        fn reconcile_provider_landing(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            provider: &dyn crate::providers::ProviderAdapter,
            pr_url: &str,
        ) -> Result<Option<QueueItem>> {
            self.ensure_repo_lease()?;
            let admission = item
                .admission
                .merge_request()
                .context("provider landing has no MR admission")?;
            let expected_repository = crate::repository_policy::ProviderRepository {
                provider: admission.provider,
                host: admission.provider_host.clone(),
                repository: admission.repository.clone(),
                repository_id: admission.repository_id.clone(),
            };
            let mut provider_executor = |provider_kind, program: &str, args: &[OsString]| {
                self.run_supervised_provider_command(
                    &item.id,
                    &attempt.id,
                    QueueStatus::Integrating,
                    provider_kind,
                    program,
                    args,
                )
            };
            provider
                .verify_repository(
                    &expected_repository,
                    self.queue
                        .repository(&item.repo_key)?
                        .policy
                        .canonical_repository
                        .object_format(),
                    &mut provider_executor,
                )
                .context("revalidate provider repository before landing observation")?;
            let Some(landing) = provider
                .landing(pr_url, &mut provider_executor)
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
            let admitted_base = admission
                .base_sha
                .as_deref()
                .context("active provider admission has no exact admitted base")?;
            provider
                .verify_repository(
                    &expected_repository,
                    self.queue
                        .repository(&item.repo_key)?
                        .policy
                        .canonical_repository
                        .object_format(),
                    &mut provider_executor,
                )
                .context("revalidate provider repository before final snapshot")?;
            let final_snapshot = provider
                .snapshot(pr_url, &mut provider_executor)
                .context("query provider identity after landing")?;
            if final_snapshot.repository != expected_repository
                || final_snapshot.head_sha != admission.head_sha
                || final_snapshot.target_branch != admission.target_branch
            {
                anyhow::bail!(
                    "provider source, repository, or target branch moved after exact validation"
                );
            }
            let landed_tree = git_output(
                &self.options.repo_path,
                ["rev-parse", &format!("{}^{{tree}}", landing.commit_sha)],
            )?;
            let workspace = self.load_owned_workspace(item)?;
            let validated_tree = git_output(
                &workspace,
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
            if first_parent != expected_base {
                anyhow::bail!(
                    "provider landed on base {first_parent}, expected validated base {expected_base}"
                );
            }
            let declared_history = admission.provider_merge_method.map(|method| match method {
                crate::repository_policy::ProviderMergeMethod::Merge => {
                    crate::providers::ProviderLandingHistory::PreserveHead
                }
                crate::repository_policy::ProviderMergeMethod::Squash => {
                    crate::providers::ProviderLandingHistory::Squash
                }
            });
            let history = match (landing.history, declared_history) {
                (Some(observed), Some(declared)) if observed != declared => {
                    anyhow::bail!("provider landing history contradicts migrated merge method")
                }
                (Some(observed), _) => observed,
                (None, Some(declared)) => declared,
                (None, None) => anyhow::bail!(
                    "provider landing has no durable or observable merge method authority"
                ),
            };
            let contains_admitted_head = history
                == crate::providers::ProviderLandingHistory::PreserveHead
                && git_is_ancestor(
                    &self.options.repo_path,
                    &item.current_head_sha,
                    &landing.commit_sha,
                )?;
            if history == crate::providers::ProviderLandingHistory::PreserveHead
                && !contains_admitted_head
            {
                anyhow::bail!("provider landing violates its admitted-head history contract");
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
            self.queue.record_provider_landing_guarantee(
                &item.repo_key,
                &self.lease_owner_id,
                &crate::sqlite::ProviderLandingEvidence {
                    item_id: &item.id,
                    provider: admission.provider,
                    provider_host: &admission.provider_host,
                    provider_repository: &admission.repository,
                    provider_repository_id: &admission.repository_id,
                    merge_request_identity: &admission.identity,
                    admitted_base_sha: admitted_base,
                    admitted_head_sha: &admission.head_sha,
                    validated_target_sha: expected_base,
                    validated_candidate_sha: validated_commit,
                    validated_tree_sha: &validated_tree,
                    landed_commit_sha: &landing.commit_sha,
                    landed_tree_sha: &landed_tree,
                    first_parent_sha: &first_parent,
                    history_contract: match history {
                        crate::providers::ProviderLandingHistory::PreserveHead => "preserve_head",
                        crate::providers::ProviderLandingHistory::Squash => "squash",
                    },
                    contains_admitted_head,
                },
            )?;
            self.reconcile_registered_checkout(item, &attempt.id, &remote_target_sha)?;
            self.mark_integrated_owned(
                &item.id,
                &attempt.id,
                &landing.commit_sha,
                &remote_target_sha,
            )
            .map(Some)
        }

        pub fn workspace_status(&self) -> Result<Vec<WorkspaceStatus>> {
            workspace_status(&self.queue.reader(), &self.options.repo_key)
        }

        pub fn reset_workspaces(&self) -> Result<TerminalCleanupReport> {
            let _operation = RepositoryOperationLease::acquire(
                self.queue.clone(),
                &self.options.repo_path,
                &self.options.repo_key,
                &self.lease_owner_id,
                self.options.lease_ttl_seconds,
            )?;
            self.initialize_workspaces()?;
            cleanup_terminal_agent_artifacts(&self.queue, &self.options.repo_key, false)?;
            self.synchronize_workspace_generation()?;
            self.with_lease_heartbeat("workspace cleanup", || {
                self.reconcile_workspaces(TerminalCleanupMode::OperatorRequested)
            })
        }

        pub(crate) fn reset_workspaces_under_lease(
            &self,
            operation: &RepositoryOperationLease,
        ) -> Result<TerminalCleanupReport> {
            if operation.repo_key != self.options.repo_key
                || operation.owner_id != self.lease_owner_id
            {
                anyhow::bail!("terminal cleanup operation authority differs from integrator");
            }
            operation.ensure()?;
            self.initialize_workspaces()?;
            self.ensure_registered_remote_identity()?;
            self.synchronize_workspace_generation()?;
            self.with_lease_heartbeat("workspace cleanup", || {
                self.reconcile_workspaces(TerminalCleanupMode::OperatorRequested)
            })
        }

        fn synchronize_workspace_generation(&self) -> Result<()> {
            let generation = self
                .queue
                .workspace_root_generation_state_for_kind(&self.options.repo_key, "integration")?;
            match generation {
                WorkspaceGenerationState::Ready { current } => {
                    self.workspaces().synchronize_generation(current)
                }
                WorkspaceGenerationState::Pending { .. } => {
                    self.workspaces().reconcile_pending_generation(generation)?;
                    Ok(())
                }
            }
        }

        fn reconcile_workspaces(&self, mode: TerminalCleanupMode) -> Result<TerminalCleanupReport> {
            self.ensure_repo_lease()?;
            self.workspaces().verify_root_identity()?;
            if self
                .queue
                .has_workspace_gc_debt(&self.workspaces().registry_identity)?
            {
                self.gc_workspaces()?;
            }
            let items = self
                .queue
                .list_items()?
                .into_iter()
                .filter(|item| item.repo_key == self.options.repo_key)
                .collect::<Vec<_>>();
            let inventory = self.workspaces().list()?;
            let inventory_paths = inventory
                .iter()
                .map(|identity| PathBuf::from(&identity.path))
                .collect::<HashSet<_>>();
            for entry in fs::read_dir(&self.workspaces().root).with_context(|| {
                format!(
                    "inspect IQ workspace root {}",
                    self.workspaces().root.display()
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
                if is_rift_workspace_root_entry(&path)? || sandbox_cycle_id(&path)?.is_some() {
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
            let mut outcomes = Vec::new();
            for item in &items {
                let expected = self.workspaces().expected_path(&item.id)?;
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
                        self.control_store
                            .clear_terminal_workspace_cleanup_debt(&item.id)?;
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
                        let stored = self.workspaces().normalize_owned_path(Path::new(path))?;
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
                                if identity.path != path.as_str()
                                    || identity.source_rift_id != self.workspaces().source_id
                                {
                                    anyhow::bail!(
                                        "item {} actual Rift identity differs from creation intent authority",
                                        item.id
                                    );
                                }
                                self.control_store.create_terminal_creation_intent_debt(
                                    &item.id,
                                    &identity.path,
                                )?;
                                let debt = self
                                    .control_store
                                    .terminal_workspace_cleanup_debt(&item.id)?
                                    .context("terminal creation intent has no cleanup debt")?;
                                if debt.target
                                    != (crate::control_store::TerminalWorkspaceTarget::CreationIntent {
                                        path: path.clone(),
                                    })
                                {
                                    anyhow::bail!(
                                        "terminal workspace cleanup debt target differs from creation intent authority"
                                    );
                                }
                                if mode == TerminalCleanupMode::Automatic
                                    && matches!(
                                        debt.state,
                                        crate::control_store::TerminalWorkspaceCleanupState::Preserved {
                                            next_retry_at,
                                            ..
                                        } if next_retry_at > chrono::Utc::now()
                                    )
                                {
                                    retained_ids.insert(identity.rift_id.clone());
                                    continue;
                                }
                                let dirty = workspace_dirty(Path::new(&identity.path))?.is_some();
                                let active_git_operation = crate::composition::has_git_operation(
                                    Path::new(&identity.path),
                                )?;
                                if dirty || active_git_operation {
                                    self.control_store.record_terminal_workspace_preserved(
                                        &item.id,
                                        &crate::control_store::TerminalWorkspaceTarget::CreationIntent { path: path.clone() },
                                        identity,
                                        dirty,
                                        active_git_operation,
                                    )?;
                                    retained_ids.insert(identity.rift_id.clone());
                                    outcomes.push(TerminalCleanupOutcome::Preserved {
                                        path: identity.path.clone().into(),
                                        reason: "dirty_or_active_git_operation".into(),
                                    });
                                    continue;
                                }
                                if self.remove_retained_workspace(identity)? {
                                    outcomes
                                        .push(TerminalCleanupOutcome::Removed { path: expected });
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
                            .workspaces()
                            .normalize_owned_path(Path::new(&identity.path))?;
                        if stored != expected {
                            anyhow::bail!(
                                "item {} workspace {} does not match IQ-owned path {}",
                                item.id,
                                stored.display(),
                                expected.display()
                            );
                        }
                        if identity.source_rift_id != self.workspaces().source_id {
                            anyhow::bail!(
                                "item {} Rift source changed from {} to {}",
                                item.id,
                                identity.source_rift_id,
                                self.workspaces().source_id
                            );
                        }
                        let existing = inventory
                            .iter()
                            .find(|candidate| candidate.rift_id == identity.rift_id);
                        match existing {
                            Some(actual) if terminal => {
                                if actual.path != identity.path {
                                    anyhow::bail!(
                                        "item {} Rift {} moved from {} to {}",
                                        item.id,
                                        identity.rift_id,
                                        identity.path,
                                        actual.path
                                    );
                                }
                                if actual.source_rift_id != identity.source_rift_id {
                                    anyhow::bail!(
                                        "item {} Rift {} source changed from {} to {}",
                                        item.id,
                                        identity.rift_id,
                                        identity.source_rift_id,
                                        actual.source_rift_id
                                    );
                                }
                                if let Some(debt) = self
                                    .control_store
                                    .terminal_workspace_cleanup_debt(&item.id)?
                                {
                                    if !matches!(
                                        debt.target,
                                        crate::control_store::TerminalWorkspaceTarget::Retained { .. }
                                    ) {
                                        anyhow::bail!(
                                            "terminal workspace cleanup debt target kind differs from retained queue authority"
                                        );
                                    }
                                    if let crate::control_store::TerminalWorkspaceTarget::Retained { identity: debt_identity } = &debt.target {
                                        if debt_identity != identity {
                                            anyhow::bail!(
                                                "terminal workspace cleanup debt identity differs from queue authority"
                                            );
                                        }
                                    }
                                    if mode == TerminalCleanupMode::Automatic
                                        && matches!(
                                            debt.state,
                                            crate::control_store::TerminalWorkspaceCleanupState::Preserved {
                                                next_retry_at,
                                                ..
                                            } if next_retry_at > chrono::Utc::now()
                                        )
                                    {
                                        retained_ids.insert(actual.rift_id.clone());
                                        continue;
                                    }
                                }
                                let path = Path::new(&actual.path);
                                let dirty = workspace_dirty(path)?.is_some();
                                let active_git_operation =
                                    crate::composition::has_git_operation(path)?;
                                if dirty || active_git_operation {
                                    self.control_store.record_terminal_workspace_preserved(
                                        &item.id,
                                        &crate::control_store::TerminalWorkspaceTarget::Retained {
                                            identity: identity.clone(),
                                        },
                                        actual,
                                        dirty,
                                        active_git_operation,
                                    )?;
                                    retained_ids.insert(actual.rift_id.clone());
                                    outcomes.push(TerminalCleanupOutcome::Preserved {
                                        path: actual.path.clone().into(),
                                        reason: "dirty_or_active_git_operation".into(),
                                    });
                                    continue;
                                }
                                self.remove_retained_workspace(identity)?;
                                outcomes.push(TerminalCleanupOutcome::Removed {
                                    path: PathBuf::from(&actual.path),
                                });
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
                if workspace.parent() != Some(self.workspaces().root.as_path()) {
                    continue;
                }
                if retained_ids.contains(&identity.rift_id) {
                    continue;
                }
                if self.remove_retained_workspace(&identity)? {
                    outcomes.push(TerminalCleanupOutcome::Removed { path: workspace });
                }
            }
            Ok(TerminalCleanupReport {
                mode: match mode {
                    TerminalCleanupMode::Automatic => "automatic",
                    TerminalCleanupMode::OperatorRequested => "operator_requested",
                }
                .into(),
                outcomes,
            })
        }

        fn fetch_for_merge<I, S>(&self, item: &QueueItem, attempt: &Attempt, args: I) -> Result<()>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            self.ensure_registered_remote_identity_for_item(item, attempt, QueueStatus::Merging)?;
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
            self.ensure_registered_remote_identity_for_item(
                item,
                attempt,
                QueueStatus::Integrating,
            )?;
            let canonical_fetch = self.canonical_fetch_transport()?;
            let repository = self.queue.repository(&self.options.repo_key)?;
            let target_sha = if matches!(
                repository.checkout_reconciliation,
                crate::sqlite::CheckoutReconciliationState::Ready(_)
            ) {
                let target_full_ref = format!("refs/heads/{}", item.target_branch);
                let observed = self.run_supervised_landing_command(
                    &item.id,
                    &attempt.id,
                    "git",
                    [
                        "ls-remote",
                        "--exit-code",
                        &canonical_fetch,
                        &target_full_ref,
                    ],
                    Some(&self.options.repo_path),
                )?;
                let observed_target = crate::composition::parse_exact_remote_ref(
                    &observed.stdout,
                    &target_full_ref,
                    repository.policy.canonical_repository.object_format(),
                )?;
                self.queue.update_checkout_reconciliation(
                    &self.options.repo_key,
                    &self.lease_owner_id,
                    &crate::sqlite::CheckoutReconciliationState::pending(
                        &observed_target,
                        repository.policy.canonical_repository.object_format(),
                    )?,
                )?;
                stop_supervised_target_after("observation");
                observed_target
            } else {
                repository.checkout_reconciliation.target_sha().to_string()
            };
            let private_ref = format!("refs/iq/supervised-targets/{}/{}", attempt.id, target_sha);
            let exact_refspec = format!("+{target_sha}:{private_ref}");
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["fetch", "--no-tags", &canonical_fetch, &exact_refspec],
                Some(&self.options.repo_path),
            )?;
            let private_sha = git_output(&self.options.repo_path, ["rev-parse", &private_ref])?;
            if private_sha != target_sha {
                anyhow::bail!("private target ref differs from durable checkout observation");
            }
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["cat-file", "-e", &format!("{target_sha}^{{commit}}")],
                Some(&self.options.repo_path),
            )?;
            let tracking_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            self.run_supervised_landing_command(
                &item.id,
                &attempt.id,
                "git",
                ["update-ref", &tracking_ref, &target_sha],
                Some(&self.options.repo_path),
            )?;
            if git_output(&self.options.repo_path, ["rev-parse", &tracking_ref])? != target_sha {
                anyhow::bail!("published target differs from durable checkout observation");
            }
            let current_repository = self.queue.repository(&self.options.repo_key)?;
            crate::composition::reconcile_registered_checkout(
                &self.queue,
                &current_repository,
                &self.lease_owner_id,
                &target_sha,
                |path, target_sha| {
                    self.run_supervised_landing_command(
                        &item.id,
                        &attempt.id,
                        "git",
                        ["reset", "--hard", target_sha],
                        Some(path),
                    )?;
                    Ok(())
                },
            )?;
            stop_supervised_target_after("reconciled");
            Ok(())
        }

        fn enforce_item_boundary(&self, item: &QueueItem) -> Result<Option<QueueItem>> {
            let queued_repo = Path::new(&item.owned_root_path)
                .canonicalize()
                .with_context(|| {
                    format!("resolve owned repository path {}", item.owned_root_path)
                })?;
            let (registered_path, expected_target, _) = self
                .queue
                .registered_remote_identity(&self.options.repo_key)?
                .context("queue repository is not registered")?;
            if queued_repo == registered_path && item.target_branch == expected_target {
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

    pub(crate) fn workspace_status(
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

    pub(crate) fn git<I, S>(cwd: &Path, args: I) -> Result<()>
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

    pub(crate) fn git_output<I, S>(cwd: &Path, args: I) -> Result<String>
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

    fn definite_force_with_lease_rejection(output: &Output, expected_target_ref: &str) -> bool {
        if output.status.success() {
            return false;
        }
        let Ok(report) = std::str::from_utf8(&output.stdout) else {
            return false;
        };
        let mut statuses = report.lines().filter(|line| {
            line.as_bytes()
                .first()
                .is_some_and(|status| matches!(status, b' ' | b'+' | b'-' | b'*' | b'!' | b'='))
        });
        let Some(status) = statuses.next() else {
            return false;
        };
        if statuses.next().is_some() {
            return false;
        }
        let mut fields = status.split('\t');
        if fields.next() != Some("!") {
            return false;
        }
        let Some(mapping) = fields.next() else {
            return false;
        };
        let Some((_, target_ref)) = mapping.rsplit_once(':') else {
            return false;
        };
        target_ref == expected_target_ref
            && fields.next() == Some("[rejected] (stale info)")
            && fields.next().is_none()
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

    pub(crate) fn command_output_timeout<I, S>(
        program: &str,
        args: I,
        cwd: Option<&Path>,
        timeout: StdDuration,
        authorize_start: impl FnOnce(&mut CommandRelease) -> Result<bool>,
        check_authority: impl FnMut() -> Result<ExecutionAuthority>,
    ) -> Result<CommandOutputOutcome>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        command_output_timeout_with_prepare(
            CommandProgram::SearchPath(program),
            args,
            cwd,
            timeout,
            authorize_start,
            check_authority,
            |_| Ok(()),
        )
    }

    pub(crate) fn command_output_timeout_with_prepare<I, S>(
        program: CommandProgram<'_>,
        args: I,
        cwd: Option<&Path>,
        timeout: StdDuration,
        authorize_start: impl FnOnce(&mut CommandRelease) -> Result<bool>,
        mut check_authority: impl FnMut() -> Result<ExecutionAuthority>,
        prepare_spawn: impl FnOnce(&mut crate::agent_config::AuthorizedCommand) -> Result<()>,
    ) -> Result<CommandOutputOutcome>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        const POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);

        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<_>>();
        let label = program.label();
        let git_binding = if label == "git" {
            let cwd = cwd.context("Git command requires an explicit working directory")?;
            crate::git_command::require_verified_cwd(cwd)?;
            crate::git_command::require_safe_local_config(cwd)?;
            if crate::git_command::is_external_operation(&args) {
                crate::git_command::require_no_url_rewrites(cwd)?;
            }
            Some(crate::git_command::expected_binding(cwd)?)
        } else {
            None
        };
        let mut process = gated_process(&program, &args)?;
        let git_authority = git_binding
            .as_ref()
            .map(|binding| crate::git_command::bind_verified(&mut process, binding))
            .transpose()?;
        if git_authority.is_none() {
            if let Some(cwd) = cwd {
                process.current_dir(cwd);
            }
        }
        process.stdout(Stdio::piped()).stderr(Stdio::piped());
        if let Some(authority) = &git_authority {
            authority.verify_control_state()?;
            crate::git_command::initialize_executable_authority()?;
        }
        prepare_spawn(&mut process)?;
        let mut release = CommandRelease::new();
        if !authorize_start(&mut release)? {
            return Ok(CommandOutputOutcome::Cancelled);
        }
        if release.token != b"run\n" && release.token != b"landing\n" {
            anyhow::bail!("command authorization produced an invalid release token");
        }
        #[cfg(debug_assertions)]
        if release.token == b"landing\n"
            && std::env::var_os("IQ_TEST_LANDING_STOP_AFTER_RELEASE_COMMIT").is_some()
        {
            std::process::exit(91);
        }
        #[cfg(debug_assertions)]
        if release.token == b"landing\n"
            && std::env::var_os("IQ_TEST_LANDING_FAIL_AFTER_RELEASE_COMMIT").is_some()
        {
            anyhow::bail!("test failure after landing release commit");
        }
        let _askpass = release
            .https_credential
            .as_ref()
            .map(|credential| crate::git_command::apply_https_credential(&mut process, credential))
            .transpose()?;
        if let Some(authority) = &git_authority {
            authority.verify_control_state()?;
        }
        unsafe {
            process.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = process.spawn().with_context(|| format!("run {label}"))?;
        let stdout = child.stdout.take().context("capture command stdout")?;
        let stderr = child.stderr.take().context("capture command stderr")?;
        let stop_capture = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stdout_stop = stop_capture.clone();
        let stderr_stop = stop_capture.clone();
        let stdout_thread = thread::spawn(move || capture_memory_bounded(stdout, &stdout_stop));
        let stderr_thread = thread::spawn(move || capture_memory_bounded(stderr, &stderr_stop));
        let deadline = Instant::now() + timeout;
        let mut timed_out = false;
        let mut cancelled = false;
        let mut cancellation_failure_started = None;
        let mut cancellation_error = None;
        let mut termination_error = None;
        let mut next_cancellation_check = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break Some(status);
            }
            let current_time = Instant::now();
            if current_time >= next_cancellation_check {
                match authority_state(&mut check_authority, &mut cancellation_failure_started) {
                    Ok(Some(ExecutionAuthority::Cancelled)) => {
                        cancelled = true;
                        match stop_direct_command(&mut child) {
                            Ok(status) => break Some(status),
                            Err(error) => {
                                termination_error = Some(error);
                                break None;
                            }
                        }
                    }
                    Ok(Some(ExecutionAuthority::Lost(message))) => {
                        cancellation_error = Some(anyhow::anyhow!(message));
                        match stop_direct_command(&mut child) {
                            Ok(status) => break Some(status),
                            Err(error) => {
                                termination_error = Some(error);
                                break None;
                            }
                        }
                    }
                    Ok(Some(ExecutionAuthority::Active)) | Ok(None) => {}
                    Err(error) => {
                        cancellation_error = Some(error);
                        match stop_direct_command(&mut child) {
                            Ok(status) => break Some(status),
                            Err(error) => {
                                termination_error = Some(error);
                                break None;
                            }
                        }
                    }
                }
                next_cancellation_check = current_time + CANCELLATION_POLL_INTERVAL;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                timed_out = true;
                match stop_direct_command(&mut child) {
                    Ok(status) => break Some(status),
                    Err(error) => {
                        termination_error = Some(error);
                        break None;
                    }
                }
            }
            if let Some(status) = child.wait_timeout(POLL_INTERVAL.min(remaining))? {
                break Some(status);
            }
        };
        stop_capture.store(true, std::sync::atomic::Ordering::Release);
        let stdout = stdout_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stdout capture thread panicked"))??;
        let stderr = stderr_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stderr capture thread panicked"))??;
        if let Some(error) = termination_error {
            return Err(error);
        }
        if cancelled {
            return Ok(CommandOutputOutcome::Cancelled);
        }
        if let Some(error) = cancellation_error {
            return Err(error).context("monitor command cancellation");
        }
        if stdout.exceeded {
            return Err(CommandInfrastructureError::OutputLimit {
                program: label.to_string(),
                stream: "stdout",
                maximum_bytes: stdout.bytes.len(),
            }
            .into());
        }
        if stderr.exceeded {
            return Err(CommandInfrastructureError::OutputLimit {
                program: label.to_string(),
                stream: "stderr",
                maximum_bytes: stderr.bytes.len(),
            }
            .into());
        }
        match wait_for_authority_state(&mut check_authority, &mut cancellation_failure_started)
            .context("check command authority after command exit")?
        {
            ExecutionAuthority::Active => {}
            ExecutionAuthority::Cancelled => return Ok(CommandOutputOutcome::Cancelled),
            ExecutionAuthority::Lost(message) => anyhow::bail!(message),
        }
        if timed_out {
            return Err(CommandInfrastructureError::TimedOut {
                program: label.to_string(),
                timeout_seconds: timeout.as_secs(),
            }
            .into());
        }
        Ok(CommandOutputOutcome::Exited(Output {
            status: status.context("command ended without an exit status")?,
            stdout: stdout.bytes,
            stderr: stderr.bytes,
        }))
    }

    fn stop_direct_command(child: &mut std::process::Child) -> Result<ExitStatus> {
        let process_group = child.id() as libc::pid_t;
        if unsafe { libc::kill(-process_group, libc::SIGTERM) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error).context("stop command process group");
            }
        }
        if let Some(status) = child.wait_timeout(StdDuration::from_secs(2))? {
            return Ok(status);
        }
        if unsafe { libc::kill(-process_group, libc::SIGKILL) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error).context("kill command process group");
            }
        }
        child.wait().context("reap command process")
    }

    struct MemoryCapture {
        bytes: Vec<u8>,
        exceeded: bool,
    }

    fn capture_memory_bounded(
        mut input: impl Read + AsRawFd,
        stop: &std::sync::atomic::AtomicBool,
    ) -> Result<MemoryCapture> {
        const MAX_BYTES: usize = 1024 * 1024;
        let descriptor = input.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } != 0
        {
            return Err(std::io::Error::last_os_error())
                .context("make command capture stream nonblocking");
        }
        let mut output = Vec::new();
        let mut exceeded = false;
        let mut buffer = [0_u8; 8192];
        loop {
            match input.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    let remaining = MAX_BYTES.saturating_sub(output.len());
                    output.extend_from_slice(&buffer[..remaining.min(count)]);
                    exceeded |= count > remaining;
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if stop.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                    thread::sleep(StdDuration::from_millis(5));
                }
                Err(error) => return Err(error).context("capture command output"),
            }
        }
        Ok(MemoryCapture {
            bytes: output,
            exceeded,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn run_evidence_command(
        command: &str,
        cwd: &Path,
        log_path: &Path,
        log_directory: &fs::File,
        environment: &[(&str, &str)],
        timeout: StdDuration,
        authorize_start: impl FnOnce(&mut CommandRelease) -> Result<bool>,
        mut check_authority: impl FnMut() -> Result<ExecutionAuthority>,
    ) -> Result<EvidenceCommandOutcome> {
        const POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);

        let binding = crate::git_command::expected_binding(cwd)?;
        binding.verify()?;
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
        let mut process = gated_process(&CommandProgram::SearchPath("/bin/sh"), ["-lc", command])?;
        process
            .current_dir(cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in environment {
            process.env(key, value);
        }
        crate::git_command::harden_authorized(&mut process);
        binding.verify()?;
        let mut release = CommandRelease::new();
        if !authorize_start(&mut release)? {
            let mut log = create_file_at(log_directory, log_name, "evidence log")?;
            writeln!(log, "$ {command}\n\n[IQ cancelled before command start]")?;
            remove_file_at(log_directory, stdout_name, "evidence stdout")?;
            remove_file_at(log_directory, stderr_name, "evidence stderr")?;
            return Ok(EvidenceCommandOutcome::Cancelled(None));
        }
        if release.token != b"run\n" {
            anyhow::bail!("evidence command authorization produced an invalid release token");
        }
        unsafe {
            process.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = process
            .spawn()
            .with_context(|| format!("run evidence command: {command}"))?;
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
                        break stop_direct_command(&mut child)?;
                    }
                    Ok(Some(ExecutionAuthority::Lost(message))) => {
                        cancellation_error = Some(anyhow::anyhow!(message));
                        break stop_direct_command(&mut child)?;
                    }
                    Ok(Some(ExecutionAuthority::Active)) | Ok(None) => {}
                    Err(error) => {
                        cancellation_error = Some(error);
                        break stop_direct_command(&mut child)?;
                    }
                }
                next_cancellation_check = current_time + CANCELLATION_POLL_INTERVAL;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                timed_out = true;
                break stop_direct_command(&mut child)?;
            }
            if let Some(status) = child
                .wait_timeout(POLL_INTERVAL.min(remaining))
                .with_context(|| format!("wait for evidence command: {command}"))?
            {
                break status;
            }
        };
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

    fn gated_process<I, S>(
        program: &CommandProgram<'_>,
        args: I,
    ) -> Result<crate::agent_config::AuthorizedCommand>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut process = match program {
            CommandProgram::SearchPath("git") => crate::git_command::command()?,
            CommandProgram::SearchPath(program) => {
                let identity = crate::agent_config::search_path_executable_identity(program)?;
                crate::agent_config::open_executable_authority(&identity)?.command()
            }
            CommandProgram::Descriptor { authority, .. } => authority.command(),
        };
        crate::git_command::harden_authorized(&mut process);
        process.args(args).stdin(Stdio::null());
        Ok(process)
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

    pub(crate) fn git_status<I, S>(cwd: &Path, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<_>>();
        if crate::git_command::is_external_operation(&args) {
            crate::git_command::require_no_url_rewrites(cwd)?;
        }
        let mut command = crate::git_command::command_in(cwd)?;
        command.args(["-c", "commit.gpgSign=false"]).args(args);
        crate::git_command::service_output(&mut command)
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
        let mut command = crate::git_command::command_in(cwd)?;
        command.env("GIT_OPTIONAL_LOCKS", "0").args(&args);
        let output = crate::git_command::service_output(&mut command)
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
    use std::ffi::OsString;
    use std::io::Write;
    use std::path::PathBuf;
    use std::process::Output;
    use std::sync::OnceLock;
    use std::time::Duration;

    #[cfg(not(test))]
    static GITHUB_EXECUTABLE: OnceLock<
        std::result::Result<crate::control_domain::ExecutableIdentity, String>,
    > = OnceLock::new();
    #[cfg(not(test))]
    static GITLAB_EXECUTABLE: OnceLock<
        std::result::Result<crate::control_domain::ExecutableIdentity, String>,
    > = OnceLock::new();

    #[cfg(any(debug_assertions, test, feature = "test-hooks"))]
    static TEST_EXECUTABLES: OnceLock<
        std::sync::Mutex<(
            Option<crate::control_domain::ExecutableIdentity>,
            Option<crate::control_domain::ExecutableIdentity>,
        )>,
    > = OnceLock::new();

    #[cfg(test)]
    static TEST_PROVIDER_EXECUTION_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

    #[cfg(test)]
    pub(crate) fn lock_test_provider_execution() -> std::sync::MutexGuard<'static, ()> {
        TEST_PROVIDER_EXECUTION_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(any(debug_assertions, test, feature = "test-hooks"))]
    pub struct TestProviderExecutableGuard {
        provider: crate::repository_policy::Provider,
        previous: Option<crate::control_domain::ExecutableIdentity>,
    }

    #[cfg(any(debug_assertions, test, feature = "test-hooks"))]
    impl Drop for TestProviderExecutableGuard {
        fn drop(&mut self) {
            let mut authorities = TEST_EXECUTABLES
                .get_or_init(|| std::sync::Mutex::new((None, None)))
                .lock()
                .expect("test provider executable authority is poisoned");
            *match self.provider {
                crate::repository_policy::Provider::Github => &mut authorities.0,
                crate::repository_policy::Provider::Gitlab => &mut authorities.1,
            } = self.previous.take();
        }
    }

    #[cfg(any(debug_assertions, test, feature = "test-hooks"))]
    pub fn inject_test_provider_executable(
        provider: crate::repository_policy::Provider,
        path: &std::path::Path,
    ) -> Result<TestProviderExecutableGuard> {
        let identity = crate::agent_config::executable_identity(path)?;
        let mut authorities = TEST_EXECUTABLES
            .get_or_init(|| std::sync::Mutex::new((None, None)))
            .lock()
            .map_err(|_| anyhow::anyhow!("test provider executable authority is poisoned"))?;
        let authority = match provider {
            crate::repository_policy::Provider::Github => &mut authorities.0,
            crate::repository_policy::Provider::Gitlab => &mut authorities.1,
        };
        let previous = authority.replace(identity);
        Ok(TestProviderExecutableGuard { provider, previous })
    }

    #[cfg(any(debug_assertions, feature = "test-hooks"))]
    pub(crate) fn test_provider_executable_is_injected() -> bool {
        TEST_EXECUTABLES
            .get_or_init(|| std::sync::Mutex::new((None, None)))
            .lock()
            .is_ok_and(|authorities| authorities.0.is_some() || authorities.1.is_some())
    }

    #[cfg(test)]
    pub(crate) fn provider_program(provider: crate::repository_policy::Provider) -> Result<String> {
        let injected = {
            let authorities = TEST_EXECUTABLES
                .get_or_init(|| std::sync::Mutex::new((None, None)))
                .lock()
                .map_err(|_| anyhow::anyhow!("test provider executable authority is poisoned"))?;
            match provider {
                crate::repository_policy::Provider::Github => authorities.0.clone(),
                crate::repository_policy::Provider::Gitlab => authorities.1.clone(),
            }
        };
        if let Some(identity) = injected {
            crate::agent_config::verify_executable(&identity)?;
            return identity
                .path
                .into_os_string()
                .into_string()
                .map_err(|_| anyhow::anyhow!("provider executable path is not UTF-8"));
        }
        Ok(match provider {
            crate::repository_policy::Provider::Github => "/test/gh".into(),
            crate::repository_policy::Provider::Gitlab => "/test/glab".into(),
        })
    }

    #[cfg(not(test))]
    pub(crate) fn provider_program(provider: crate::repository_policy::Provider) -> Result<String> {
        validate_executable_environment()?;
        #[cfg(any(debug_assertions, feature = "test-hooks"))]
        let injected = {
            let authorities = TEST_EXECUTABLES
                .get_or_init(|| std::sync::Mutex::new((None, None)))
                .lock()
                .map_err(|_| anyhow::anyhow!("test provider executable authority is poisoned"))?;
            match provider {
                crate::repository_policy::Provider::Github => authorities.0.clone(),
                crate::repository_policy::Provider::Gitlab => authorities.1.clone(),
            }
        };
        #[cfg(any(debug_assertions, feature = "test-hooks"))]
        if let Some(identity) = injected {
            crate::agent_config::verify_executable(&identity)?;
            return identity
                .path
                .into_os_string()
                .into_string()
                .map_err(|_| anyhow::anyhow!("provider executable path is not UTF-8"));
        }
        let (program, authority) = match provider {
            crate::repository_policy::Provider::Github => ("gh", &GITHUB_EXECUTABLE),
            crate::repository_policy::Provider::Gitlab => ("glab", &GITLAB_EXECUTABLE),
        };
        let identity = authority
            .get_or_init(|| {
                crate::agent_config::trusted_executable_identity(program)
                    .map_err(|error| format!("{error:#}"))
            })
            .as_ref()
            .map_err(|error| anyhow::anyhow!(error.clone()))?;
        crate::agent_config::verify_executable(identity)?;
        identity
            .path
            .to_str()
            .map(str::to_string)
            .context("provider executable path is not UTF-8")
    }

    pub fn validate_executable_environment() -> Result<()> {
        if std::env::var_os("IQ_GITHUB_CLI").is_some()
            || std::env::var_os("IQ_GITLAB_CLI").is_some()
        {
            anyhow::bail!("provider executable environment overrides are forbidden");
        }
        Ok(())
    }

    pub(crate) fn harden_authorized_provider_environment(
        provider: crate::repository_policy::Provider,
        command: &mut crate::agent_config::AuthorizedCommand,
    ) {
        if let Some((name, directory)) = provider_config_directory(provider) {
            command.env(name, directory);
        }
        #[cfg(debug_assertions)]
        for (key, value) in std::env::vars_os() {
            if key.as_encoded_bytes().starts_with(b"IQ_TEST_PROVIDER_") {
                command.env(key, value);
            }
        }
    }

    #[cfg(test)]
    fn harden_provider_environment(
        provider: crate::repository_policy::Provider,
        command: &mut std::process::Command,
    ) {
        command.env_clear();
        if let Some((name, directory)) = provider_config_directory(provider) {
            command.env(name, directory);
        }
        #[cfg(debug_assertions)]
        for (key, value) in std::env::vars_os() {
            if key.as_encoded_bytes().starts_with(b"IQ_TEST_PROVIDER_") {
                command.env(key, value);
            }
        }
    }

    fn provider_config_directory(
        provider: crate::repository_policy::Provider,
    ) -> Option<(&'static str, PathBuf)> {
        let (variable, configured, directory_name) = match provider {
            crate::repository_policy::Provider::Github => {
                ("GH_CONFIG_DIR", std::env::var_os("GH_CONFIG_DIR"), "gh")
            }
            crate::repository_policy::Provider::Gitlab => (
                "GLAB_CONFIG_DIR",
                std::env::var_os("GLAB_CONFIG_DIR"),
                "glab-cli",
            ),
        };
        let directory = configured
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .map(|root| root.join(directory_name))
            })
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .map(|root| root.join(".config").join(directory_name))
            })?;
        directory.is_absolute().then_some((variable, directory))
    }

    pub(crate) fn reverify_provider_program(
        provider: crate::repository_policy::Provider,
        program: &str,
    ) -> Result<Option<crate::control_domain::ExecutableIdentity>> {
        #[cfg(debug_assertions)]
        {
            let path = std::path::Path::new(program);
            let authorities = TEST_EXECUTABLES
                .get_or_init(|| std::sync::Mutex::new((None, None)))
                .lock()
                .map_err(|_| anyhow::anyhow!("test provider executable authority is poisoned"))?;
            let identity = match provider {
                crate::repository_policy::Provider::Github => &authorities.0,
                crate::repository_policy::Provider::Gitlab => &authorities.1,
            };
            if let Some(identity) = identity {
                if identity.path != path {
                    anyhow::bail!("provider executable path differs from requested authority");
                }
                crate::agent_config::verify_executable(identity)?;
                return Ok(Some(identity.clone()));
            }
        }
        #[cfg(test)]
        if matches!(
            (provider, program),
            (crate::repository_policy::Provider::Github, "/test/gh")
                | (crate::repository_policy::Provider::Gitlab, "/test/glab")
        ) {
            return Ok(None);
        }
        #[cfg(not(test))]
        {
            let candidate = provider_program(provider)?;
            if candidate != program {
                anyhow::bail!("provider executable path differs from requested authority");
            }
            let authority = match provider {
                crate::repository_policy::Provider::Github => &GITHUB_EXECUTABLE,
                crate::repository_policy::Provider::Gitlab => &GITLAB_EXECUTABLE,
            };
            let identity = authority
                .get()
                .context("provider executable authority was not initialized")?
                .as_ref()
                .map_err(|error| anyhow::anyhow!(error.clone()))?;
            crate::agent_config::verify_executable(identity)?;
            Ok(Some(identity.clone()))
        }
        #[cfg(test)]
        anyhow::bail!("provider executable does not match the requested pinned authority")
    }

    #[cfg(not(test))]
    const PROVIDER_TIMEOUT: Duration = Duration::from_secs(20);
    #[cfg(test)]
    const PROVIDER_TIMEOUT: Duration = Duration::from_millis(200);

    #[derive(Debug, thiserror::Error)]
    pub enum ProviderInfrastructureError {
        #[error("provider CLI {program} execution failed: {detail}")]
        Execution { program: String, detail: String },
        #[error("provider CLI {program} timed out after {timeout_seconds} seconds")]
        TimedOut {
            program: String,
            timeout_seconds: u64,
        },
        #[error("provider CLI {program} {stream} exceeded the {maximum_bytes}-byte capture limit")]
        OutputLimit {
            program: String,
            stream: &'static str,
            maximum_bytes: usize,
        },
        #[error("provider CLI {program} lost item execution authority")]
        Cancelled { program: String },
        #[error("provider CLI {program} failed with status {status:?}: {stderr}")]
        Exit {
            program: String,
            status: Option<i32>,
            stderr: String,
        },
    }

    impl ProviderInfrastructureError {
        pub(crate) fn from_execution(program: &str, error: anyhow::Error) -> Self {
            match error.downcast_ref::<crate::integrator::CommandInfrastructureError>() {
                Some(crate::integrator::CommandInfrastructureError::TimedOut {
                    timeout_seconds,
                    ..
                }) => Self::TimedOut {
                    program: program.to_string(),
                    timeout_seconds: *timeout_seconds,
                },
                Some(crate::integrator::CommandInfrastructureError::OutputLimit {
                    stream,
                    maximum_bytes,
                    ..
                }) => Self::OutputLimit {
                    program: program.to_string(),
                    stream,
                    maximum_bytes: *maximum_bytes,
                },
                None => Self::Execution {
                    program: program.to_string(),
                    detail: format!("{error:#}"),
                },
            }
        }
    }

    pub trait ProviderCommandExecutor {
        fn output(
            &mut self,
            provider: crate::repository_policy::Provider,
            program: &str,
            args: &[OsString],
        ) -> Result<Output>;
    }

    impl<F> ProviderCommandExecutor for F
    where
        F: FnMut(crate::repository_policy::Provider, &str, &[OsString]) -> Result<Output>,
    {
        fn output(
            &mut self,
            provider: crate::repository_policy::Provider,
            program: &str,
            args: &[OsString],
        ) -> Result<Output> {
            self(provider, program, args)
        }
    }

    pub(crate) struct DirectProviderExecutor;

    impl ProviderCommandExecutor for DirectProviderExecutor {
        fn output(
            &mut self,
            provider: crate::repository_policy::Provider,
            program: &str,
            args: &[OsString],
        ) -> Result<Output> {
            let outcome = output_with_authority(
                provider,
                program,
                args,
                None,
                PROVIDER_TIMEOUT,
                |gate| {
                    gate.write_all(b"run\n")?;
                    Ok(true)
                },
                || Ok(crate::sqlite::ExecutionAuthority::Active),
            )
            .map_err(|error| ProviderInfrastructureError::from_execution(program, error))?;
            match outcome {
                crate::integrator::CommandOutputOutcome::Exited(output) => Ok(output),
                crate::integrator::CommandOutputOutcome::Cancelled => {
                    Err(ProviderInfrastructureError::Cancelled {
                        program: program.to_string(),
                    }
                    .into())
                }
            }
        }
    }

    pub(crate) fn output_with_authority<I, S>(
        provider: crate::repository_policy::Provider,
        program: &str,
        args: I,
        cwd: Option<&std::path::Path>,
        timeout: Duration,
        authorize_start: impl FnOnce(&mut crate::integrator::CommandRelease) -> Result<bool>,
        check_authority: impl FnMut() -> Result<crate::sqlite::ExecutionAuthority>,
    ) -> Result<crate::integrator::CommandOutputOutcome>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let identity = reverify_provider_program(provider, program)?
            .context("provider executable has no descriptor authority")?;
        let executable = crate::agent_config::open_executable_authority(&identity)?;
        crate::integrator::command_output_timeout_with_prepare(
            crate::integrator::CommandProgram::Descriptor {
                label: program,
                authority: &executable,
            },
            args,
            cwd,
            timeout,
            authorize_start,
            check_authority,
            |command| {
                harden_authorized_provider_environment(provider, command);
                Ok(())
            },
        )
    }

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
        pub repository: crate::repository_policy::ProviderRepository,
        pub head_sha: String,
        pub base_sha: String,
        pub target_branch: String,
        pub gate: ProviderGate,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeRequestLocator {
        pub provider: crate::repository_policy::Provider,
        pub host: String,
        pub repository: String,
        pub identity: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ProviderLanding {
        pub head_sha: String,
        pub commit_sha: String,
        pub history: Option<ProviderLandingHistory>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum ProviderLandingHistory {
        PreserveHead,
        Squash,
    }

    pub trait ProviderAdapter {
        fn kind(&self) -> ProviderKind;
        fn verify_repository(
            &self,
            expected: &crate::repository_policy::ProviderRepository,
            object_format: crate::git_object::GitObjectFormat,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<()>;
        fn snapshot(
            &self,
            url: &str,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<ProviderSnapshot>;
        fn atomic_landing_unsupported(&self) -> anyhow::Error;
        fn landing(
            &self,
            url: &str,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<Option<ProviderLanding>>;
    }

    pub fn provider_for_url(url: &str) -> Result<Box<dyn ProviderAdapter>> {
        let locator = merge_request_locator(url)?;
        match locator.provider {
            crate::repository_policy::Provider::Github => Ok(Box::new(GitHubProvider)),
            crate::repository_policy::Provider::Gitlab => Ok(Box::new(GitLabProvider)),
        }
    }

    pub fn verify_repository(
        expected: &crate::repository_policy::ProviderRepository,
        object_format: crate::git_object::GitObjectFormat,
    ) -> Result<()> {
        verify_repository_with(expected, object_format, &mut DirectProviderExecutor)
    }

    pub fn verify_repository_with(
        expected: &crate::repository_policy::ProviderRepository,
        object_format: crate::git_object::GitObjectFormat,
        executor: &mut dyn ProviderCommandExecutor,
    ) -> Result<()> {
        let adapter: Box<dyn ProviderAdapter> = match expected.provider {
            crate::repository_policy::Provider::Github => Box::new(GitHubProvider),
            crate::repository_policy::Provider::Gitlab => Box::new(GitLabProvider),
        };
        adapter.verify_repository(expected, object_format, executor)
    }

    pub(crate) fn https_credential(
        expected: &crate::repository_policy::ProviderRepository,
    ) -> Result<Option<crate::git_command::HttpsCredential>> {
        https_credential_with(expected, &mut DirectProviderExecutor)
    }

    pub(crate) fn https_credential_with(
        expected: &crate::repository_policy::ProviderRepository,
        executor: &mut dyn ProviderCommandExecutor,
    ) -> Result<Option<crate::git_command::HttpsCredential>> {
        let program = provider_program(expected.provider)?;
        let args = [
            OsString::from("auth"),
            OsString::from("token"),
            OsString::from("--hostname"),
            OsString::from(&expected.host),
        ];
        let output = executor.output(expected.provider, &program, &args)?;
        if !output.status.success() {
            return Ok(None);
        }
        let token = std::str::from_utf8(&output.stdout)
            .context("provider HTTPS credential is not UTF-8")?
            .trim_end_matches(['\r', '\n']);
        let username = match expected.provider {
            crate::repository_policy::Provider::Github => "x-access-token",
            crate::repository_policy::Provider::Gitlab => "oauth2",
        };
        Ok(Some(crate::git_command::HttpsCredential::new(
            username, token,
        )?))
    }

    pub fn snapshot(adapter: &dyn ProviderAdapter, url: &str) -> Result<ProviderSnapshot> {
        adapter.snapshot(url, &mut DirectProviderExecutor)
    }

    pub fn merge_request_locator(url: &str) -> Result<MergeRequestLocator> {
        let (scheme, location) = url.split_once("://").context("PR/MR URL has no scheme")?;
        if scheme != "https" {
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
        let host = host.to_ascii_lowercase();
        if host.is_empty() || host.contains([':', '@']) {
            anyhow::bail!("PR/MR URL has an invalid host");
        }
        let github_pull = matches!(segments.as_slice(), [owner, repository, "pull", number] if !owner.is_empty() && !repository.is_empty() && number.parse::<u64>().is_ok_and(|number| number > 0));
        let gitlab_merge_request = segments.len() >= 5
            && segments[segments.len() - 3] == "-"
            && segments[segments.len() - 2] == "merge_requests"
            && segments[segments.len() - 1]
                .parse::<u64>()
                .is_ok_and(|number| number > 0)
            && segments[..segments.len() - 3]
                .iter()
                .all(|segment| !segment.is_empty());
        if github_pull {
            Ok(MergeRequestLocator {
                provider: crate::repository_policy::Provider::Github,
                host,
                repository: format!("{}/{}", segments[0], segments[1]),
                identity: segments[3].to_string(),
            })
        } else if gitlab_merge_request {
            Ok(MergeRequestLocator {
                provider: crate::repository_policy::Provider::Gitlab,
                host,
                repository: segments[..segments.len() - 3].join("/"),
                identity: segments[segments.len() - 1].to_string(),
            })
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

        fn verify_repository(
            &self,
            expected: &crate::repository_policy::ProviderRepository,
            object_format: crate::git_object::GitObjectFormat,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<()> {
            if expected.provider != crate::repository_policy::Provider::Github {
                anyhow::bail!("GitHub adapter cannot verify a non-GitHub repository");
            }
            let (owner, name) = expected
                .repository
                .split_once('/')
                .context("GitHub repository identity has no owner/name boundary")?;
            let endpoint = format!("repos/{owner}/{name}");
            let value = provider_json(
                crate::repository_policy::Provider::Github,
                provider_program(crate::repository_policy::Provider::Github)?,
                [
                    "api",
                    "--hostname",
                    expected.host.as_str(),
                    endpoint.as_str(),
                ],
                executor,
            )?;
            let observed: GitHubVerifiedRepository =
                serde_json::from_value(value).context("parse gh repository JSON")?;
            if observed.node_id != expected.repository_id
                || observed.full_name != expected.repository
            {
                anyhow::bail!("GitHub repository identity differs from policy");
            }
            let format_endpoint = format!("repos/{owner}/{name}/hash-algorithm");
            let format_value = provider_json(
                crate::repository_policy::Provider::Github,
                provider_program(crate::repository_policy::Provider::Github)?,
                [
                    "api",
                    "--hostname",
                    expected.host.as_str(),
                    format_endpoint.as_str(),
                ],
                executor,
            )
            .context("GitHub provider cannot report Git object format before effect")?;
            let observed_format: GitHubHashAlgorithm =
                serde_json::from_value(format_value).context("parse GitHub hash-algorithm JSON")?;
            let observed_format = observed_format
                .hash_algorithm
                .context("GitHub hash-algorithm API omitted Git object format before effect")?;
            if crate::git_object::GitObjectFormat::parse(&observed_format, "GitHub repository API")?
                != object_format
            {
                anyhow::bail!("GitHub repository object format differs from policy");
            }
            Ok(())
        }

        fn snapshot(
            &self,
            url: &str,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<ProviderSnapshot> {
            let value = provider_json(
                crate::repository_policy::Provider::Github,
                provider_program(crate::repository_policy::Provider::Github)?,
                [
                    "pr",
                    "view",
                    url,
                    "--json",
                    "headRefOid,baseRefOid,baseRefName,baseRepository,reviewDecision,statusCheckRollup,mergeStateStatus",
                ],
                executor,
            )?;
            let parsed: GitHubPrView =
                serde_json::from_value(value).context("parse gh pr view JSON")?;
            let gate = github_gate(&parsed);
            let locator = merge_request_locator(url)?;
            let base_repository = parsed
                .base_repository
                .context("gh PR JSON missing baseRepository")?;
            Ok(ProviderSnapshot {
                repository: crate::repository_policy::ProviderRepository {
                    provider: crate::repository_policy::Provider::Github,
                    host: locator.host,
                    repository: base_repository.name_with_owner,
                    repository_id: base_repository.id,
                },
                head_sha: parsed.head_ref_oid,
                base_sha: parsed.base_ref_oid,
                target_branch: parsed.base_ref_name,
                gate,
            })
        }

        fn atomic_landing_unsupported(&self) -> anyhow::Error {
            anyhow::anyhow!(
                "GitHub CLI can pin the admitted head but cannot atomically pin the validated base"
            )
        }

        fn landing(
            &self,
            url: &str,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<Option<ProviderLanding>> {
            github_landing(url, executor)
        }
    }

    #[derive(Default)]
    pub struct GitLabProvider;

    impl ProviderAdapter for GitLabProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::GitLab
        }

        fn verify_repository(
            &self,
            expected: &crate::repository_policy::ProviderRepository,
            object_format: crate::git_object::GitObjectFormat,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<()> {
            if expected.provider != crate::repository_policy::Provider::Gitlab {
                anyhow::bail!("GitLab adapter cannot verify a non-GitLab repository");
            }
            let project = percent_encode_path(&expected.repository);
            let endpoint = format!("projects/{project}");
            let value = provider_json(
                crate::repository_policy::Provider::Gitlab,
                provider_program(crate::repository_policy::Provider::Gitlab)?,
                [
                    "api",
                    "--hostname",
                    expected.host.as_str(),
                    endpoint.as_str(),
                ],
                executor,
            )?;
            let observed: GitLabRepository =
                serde_json::from_value(value).context("parse glab repository JSON")?;
            if json_identity(&observed.id).as_deref() != Some(expected.repository_id.as_str())
                || observed.path_with_namespace != expected.repository
            {
                anyhow::bail!("GitLab repository identity differs from policy");
            }
            let observed_format = observed
                .repository_object_format
                .context("GitLab repository API does not report Git object format before effect")?;
            if crate::git_object::GitObjectFormat::parse(&observed_format, "GitLab repository API")?
                != object_format
            {
                anyhow::bail!("GitLab repository object format differs from policy");
            }
            Ok(())
        }

        fn snapshot(
            &self,
            url: &str,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<ProviderSnapshot> {
            let locator = merge_request_locator(url)?;
            let repository_url = format!("https://{}/{}", locator.host, locator.repository);
            let value = provider_json(
                crate::repository_policy::Provider::Gitlab,
                provider_program(crate::repository_policy::Provider::Gitlab)?,
                [
                    "mr",
                    "view",
                    url,
                    "--repo",
                    repository_url.as_str(),
                    "--output",
                    "json",
                ],
                executor,
            )?;
            let parsed: GitLabMrView =
                serde_json::from_value(value).context("parse glab mr view JSON")?;
            let repository_id = parsed
                .target_project_id
                .as_ref()
                .and_then(json_identity)
                .context("glab MR JSON missing target_project_id")?;
            Ok(ProviderSnapshot {
                repository: crate::repository_policy::ProviderRepository {
                    provider: crate::repository_policy::Provider::Gitlab,
                    host: locator.host,
                    repository: locator.repository,
                    repository_id,
                },
                head_sha: parsed
                    .head_sha
                    .or(parsed.sha)
                    .context("glab MR JSON missing head_sha/sha")?,
                base_sha: parsed
                    .base_sha
                    .or(parsed.diff_refs.and_then(|refs| refs.base_sha))
                    .context("glab MR JSON missing base_sha/diff_refs.base_sha")?,
                target_branch: parsed
                    .target_branch
                    .context("glab MR JSON missing target_branch")?,
                gate: gitlab_gate(&parsed.state, &parsed.pipeline_status, &parsed.approved),
            })
        }

        fn atomic_landing_unsupported(&self) -> anyhow::Error {
            anyhow::anyhow!(
                "GitLab CLI can pin the admitted head but cannot atomically pin the validated base"
            )
        }

        fn landing(
            &self,
            url: &str,
            executor: &mut dyn ProviderCommandExecutor,
        ) -> Result<Option<ProviderLanding>> {
            gitlab_landing(url, executor)
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GitHubPrView {
        head_ref_oid: String,
        base_ref_oid: String,
        base_ref_name: String,
        base_repository: Option<GitHubRepository>,
        review_decision: Option<String>,
        merge_state_status: Option<String>,
        #[serde(default)]
        status_check_rollup: Vec<serde_json::Value>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GitHubRepository {
        id: String,
        name_with_owner: String,
    }

    #[derive(Debug, Deserialize)]
    struct GitHubVerifiedRepository {
        node_id: String,
        full_name: String,
    }

    #[derive(Debug, Deserialize)]
    struct GitHubHashAlgorithm {
        hash_algorithm: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct GitLabRepository {
        id: serde_json::Value,
        path_with_namespace: String,
        repository_object_format: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct GitLabMrView {
        sha: Option<String>,
        head_sha: Option<String>,
        base_sha: Option<String>,
        target_branch: Option<String>,
        target_project_id: Option<serde_json::Value>,
        merge_commit_sha: Option<String>,
        squash_commit_sha: Option<String>,
        state: Option<String>,
        pipeline_status: Option<String>,
        approved: Option<bool>,
        diff_refs: Option<GitLabDiffRefs>,
    }

    fn json_identity(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::String(value) if !value.is_empty() => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            _ => None,
        }
    }

    fn percent_encode_path(value: &str) -> String {
        value
            .bytes()
            .flat_map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    vec![byte as char]
                }
                _ => format!("%{byte:02X}").chars().collect(),
            })
            .collect()
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

    fn github_landing(
        url: &str,
        executor: &mut dyn ProviderCommandExecutor,
    ) -> Result<Option<ProviderLanding>> {
        let value = provider_json(
            crate::repository_policy::Provider::Github,
            provider_program(crate::repository_policy::Provider::Github)?,
            ["pr", "view", url, "--json", "headRefOid,mergeCommit"],
            executor,
        )?;
        let parsed: GitHubMergeView =
            serde_json::from_value(value).context("parse gh merge JSON")?;
        Ok(parsed
            .merge_commit
            .and_then(|commit| commit.oid)
            .map(|commit_sha| ProviderLanding {
                head_sha: parsed.head_ref_oid,
                commit_sha,
                history: None,
            }))
    }

    fn gitlab_landing(
        url: &str,
        executor: &mut dyn ProviderCommandExecutor,
    ) -> Result<Option<ProviderLanding>> {
        let locator = merge_request_locator(url)?;
        let repository_url = format!("https://{}/{}", locator.host, locator.repository);
        let value = provider_json(
            crate::repository_policy::Provider::Gitlab,
            provider_program(crate::repository_policy::Provider::Gitlab)?,
            [
                "mr",
                "view",
                url,
                "--repo",
                repository_url.as_str(),
                "--output",
                "json",
            ],
            executor,
        )?;
        let parsed: GitLabMrView =
            serde_json::from_value(value).context("parse glab merged MR JSON")?;
        let landing = match (parsed.merge_commit_sha, parsed.squash_commit_sha) {
            (Some(commit_sha), _) => Some((commit_sha, ProviderLandingHistory::PreserveHead)),
            (None, Some(commit_sha)) => Some((commit_sha, ProviderLandingHistory::Squash)),
            (None, None) => None,
        };
        let head_sha = parsed.head_sha.or(parsed.sha);
        match (head_sha, landing) {
            (_, None) => Ok(None),
            (Some(head_sha), Some((commit_sha, history))) => Ok(Some(ProviderLanding {
                head_sha,
                commit_sha,
                history: Some(history),
            })),
            (None, Some(_)) => anyhow::bail!("glab merged MR JSON missing head_sha/sha"),
        }
    }

    fn provider_json<I, S>(
        provider: crate::repository_policy::Provider,
        program: String,
        args: I,
        executor: &mut dyn ProviderCommandExecutor,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<_>>();
        let output = executor
            .output(provider, &program, &args)
            .map_err(|error| {
                if error
                    .downcast_ref::<ProviderInfrastructureError>()
                    .is_some()
                {
                    error
                } else {
                    ProviderInfrastructureError::Execution {
                        program: program.clone(),
                        detail: format!("{error:#}"),
                    }
                    .into()
                }
            })?;
        if !output.status.success() {
            return Err(ProviderInfrastructureError::Exit {
                program,
                status: output.status.code(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            }
            .into());
        }
        serde_json::from_slice(&output.stdout).context("parse provider CLI JSON")
    }

    #[cfg(test)]
    mod tests {
        use super::{
            DirectProviderExecutor, GitHubProvider, ProviderAdapter, ProviderCommandExecutor,
            ProviderInfrastructureError,
        };
        use std::ffi::OsString;
        use std::os::unix::process::ExitStatusExt;
        use std::process::{ExitStatus, Output};

        #[test]
        fn github_registration_and_pr_fixtures_use_the_same_node_id() {
            let repository_fixture =
                include_bytes!("../tests/fixtures/github-repository-rest.json");
            let hash_algorithm_fixture =
                include_bytes!("../tests/fixtures/github-hash-algorithm.json");
            let pr_fixture = include_bytes!("../tests/fixtures/github-pr-view.json");
            let mut executor = |_: crate::repository_policy::Provider,
                                _: &str,
                                args: &[OsString]|
             -> anyhow::Result<Output> {
                let args = args
                    .iter()
                    .map(|argument| argument.to_str().unwrap())
                    .collect::<Vec<_>>();
                let stdout = if args.last() == Some(&"repos/octo-org/octo-repo") {
                    assert_eq!(
                        args,
                        [
                            "api",
                            "--hostname",
                            "github.com",
                            "repos/octo-org/octo-repo",
                        ]
                    );
                    repository_fixture.to_vec()
                } else if args.last() == Some(&"repos/octo-org/octo-repo/hash-algorithm") {
                    assert_eq!(
                        args,
                        [
                            "api",
                            "--hostname",
                            "github.com",
                            "repos/octo-org/octo-repo/hash-algorithm",
                        ]
                    );
                    hash_algorithm_fixture.to_vec()
                } else {
                    assert_eq!(
                        args,
                        [
                            "pr",
                            "view",
                            "https://github.com/octo-org/octo-repo/pull/7",
                            "--json",
                            "headRefOid,baseRefOid,baseRefName,baseRepository,reviewDecision,statusCheckRollup,mergeStateStatus",
                        ]
                    );
                    pr_fixture.to_vec()
                };
                Ok(Output {
                    status: ExitStatus::from_raw(0),
                    stdout,
                    stderr: Vec::new(),
                })
            };
            let expected = crate::repository_policy::ProviderRepository {
                provider: crate::repository_policy::Provider::Github,
                host: "github.com".into(),
                repository: "octo-org/octo-repo".into(),
                repository_id: "R_kgDOKV0Z9Q".into(),
            };
            let adapter = GitHubProvider;

            adapter
                .verify_repository(
                    &expected,
                    crate::git_object::GitObjectFormat::Sha1,
                    &mut executor,
                )
                .unwrap();
            let snapshot = adapter
                .snapshot(
                    "https://github.com/octo-org/octo-repo/pull/7",
                    &mut executor,
                )
                .unwrap();

            assert_eq!(snapshot.repository, expected);
        }

        #[test]
        fn direct_provider_executor_returns_typed_timeout_and_output_limit_errors() {
            use std::os::unix::fs::PermissionsExt;
            let _execution = super::lock_test_provider_execution();
            let root = tempfile::tempdir().unwrap();
            let program = root.path().join("gh");
            std::fs::write(
                &program,
                "#!/bin/sh\ncase \"$1\" in sleep) /bin/sleep 30 ;; output) dd if=/dev/zero bs=1048577 count=1 2>/dev/null ;; *) exit 2 ;; esac\n",
            )
            .unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            let _authority = super::inject_test_provider_executable(
                crate::repository_policy::Provider::Github,
                &program,
            )
            .unwrap();
            let program = program.to_str().unwrap();
            let mut executor = DirectProviderExecutor;
            let timeout = executor
                .output(
                    crate::repository_policy::Provider::Github,
                    program,
                    &[OsString::from("sleep")],
                )
                .unwrap_err();
            assert!(matches!(
                timeout.downcast_ref::<ProviderInfrastructureError>(),
                Some(ProviderInfrastructureError::TimedOut { .. })
            ));

            let output_limit = executor
                .output(
                    crate::repository_policy::Provider::Github,
                    program,
                    &[OsString::from("output")],
                )
                .unwrap_err();
            assert!(matches!(
                output_limit.downcast_ref::<ProviderInfrastructureError>(),
                Some(ProviderInfrastructureError::OutputLimit {
                    stream: "stdout",
                    maximum_bytes: 1_048_576,
                    ..
                })
            ));
        }

        #[test]
        fn direct_provider_executor_rejects_unknown_and_replaced_executables() {
            use std::os::unix::fs::PermissionsExt;

            let _execution = super::lock_test_provider_execution();
            let mut executor = DirectProviderExecutor;
            let unknown = executor
                .output(crate::repository_policy::Provider::Github, "/bin/true", &[])
                .unwrap_err();
            assert!(format!("{unknown:#}").contains("requested pinned authority"));

            let root = tempfile::tempdir().unwrap();
            let program = root.path().join("gh");
            let marker = root.path().join("executed");
            std::fs::write(&program, "#!/bin/sh\nexit 0\n").unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            let _authority = super::inject_test_provider_executable(
                crate::repository_policy::Provider::Github,
                &program,
            )
            .unwrap();
            std::fs::write(&program, format!("#!/bin/sh\n: > '{}'\n", marker.display())).unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();

            let replaced = executor
                .output(
                    crate::repository_policy::Provider::Github,
                    program.to_str().unwrap(),
                    &[],
                )
                .unwrap_err();
            assert!(format!("{replaced:#}").contains("executable"));
            assert!(!marker.exists());
        }

        #[test]
        fn provider_environment_removes_ambient_credentials_and_command_controls() {
            let mut command = std::process::Command::new("/bin/true");
            for key in [
                "GH_TOKEN",
                "GITHUB_TOKEN",
                "GITLAB_TOKEN",
                "GLAB_TOKEN",
                "LD_PRELOAD",
                "DYLD_INSERT_LIBRARIES",
                "DYLD_LIBRARY_PATH",
                "GIT_CONFIG_GLOBAL",
                "GIT_EXTERNAL_DIFF",
                "GIT_SSH_COMMAND",
                "GIT_ASKPASS",
                "SSH_ASKPASS",
                "SSH_AUTH_SOCK",
            ] {
                command.env(key, "hostile");
            }

            super::harden_provider_environment(
                crate::repository_policy::Provider::Github,
                &mut command,
            );

            let environment = command
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            for key in [
                "GH_TOKEN",
                "GITHUB_TOKEN",
                "GITLAB_TOKEN",
                "GLAB_TOKEN",
                "LD_PRELOAD",
                "DYLD_INSERT_LIBRARIES",
                "DYLD_LIBRARY_PATH",
                "GIT_CONFIG_GLOBAL",
                "GIT_EXTERNAL_DIFF",
                "GIT_SSH_COMMAND",
                "GIT_ASKPASS",
                "SSH_ASKPASS",
                "SSH_AUTH_SOCK",
                "HOME",
                "PATH",
                "XDG_CONFIG_HOME",
            ] {
                assert!(!environment.contains_key(std::ffi::OsStr::new(key)));
            }
        }

        #[test]
        fn github_execution_does_not_require_gitlab_authority() {
            execute_with_only_requested_provider(crate::repository_policy::Provider::Github, "gh");
        }

        #[test]
        fn gitlab_execution_does_not_require_github_authority() {
            execute_with_only_requested_provider(
                crate::repository_policy::Provider::Gitlab,
                "glab",
            );
        }

        fn execute_with_only_requested_provider(
            provider: crate::repository_policy::Provider,
            name: &str,
        ) {
            use std::os::unix::fs::PermissionsExt;

            let _execution = super::lock_test_provider_execution();
            let root = tempfile::tempdir().unwrap();
            let program = root.path().join(name);
            std::fs::write(&program, "#!/bin/sh\nprintf requested-provider\n").unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            let _authority = super::inject_test_provider_executable(provider, &program).unwrap();
            let mut executor = DirectProviderExecutor;

            let output = executor
                .output(provider, program.to_str().unwrap(), &[])
                .unwrap();

            assert!(output.status.success());
            assert_eq!(output.stdout, b"requested-provider");
        }
    }
}

pub mod issue_backends {
    use crate::core::{BlockedPhase, BlockedReason, QueueStatus};
    use crate::sqlite::{Prompt, QueueEvent, QueueItem};
    use anyhow::{Context, Result};
    use serde::Deserialize;
    use std::collections::HashSet;

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
            if let Some(admission) = item.admission.merge_request() {
                body.push_str(&format!("mr: {}\n", admission.url));
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
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Github)?;
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
                let output =
                    command_output(crate::repository_policy::Provider::Github, &program, args)?;
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
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Github)?;
            command_ok(
                crate::repository_policy::Provider::Github,
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
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Github)?;
            command_ok(
                crate::repository_policy::Provider::Github,
                &program,
                ["repo", "view", repo, "--json", "nameWithOwner"],
            )
        }

        fn answer_comments(&self, target: &IssueSyncTarget) -> Result<Vec<IssueAnswerComment>> {
            let issue = target
                .issue
                .as_deref()
                .context("GitHub issue number required")?;
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Github)?;
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
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Gitlab)?;
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
                let output =
                    command_output(crate::repository_policy::Provider::Gitlab, &program, args)?;
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
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Gitlab)?;
            command_ok(
                crate::repository_policy::Provider::Gitlab,
                &program,
                ["issue", "close", issue, "--repo", &target.repo],
            )
        }

        fn verify_destination(&self, repo: &str) -> Result<()> {
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Gitlab)?;
            command_ok(
                crate::repository_policy::Provider::Gitlab,
                &program,
                ["repo", "view", repo, "--output", "json"],
            )
        }

        fn answer_comments(&self, target: &IssueSyncTarget) -> Result<Vec<IssueAnswerComment>> {
            let issue = target
                .issue
                .as_deref()
                .context("GitLab issue number required")?;
            let program =
                crate::providers::provider_program(crate::repository_policy::Provider::Gitlab)?;
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
                crate::repository_policy::Provider::Github,
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
            crate::repository_policy::Provider::Github,
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
                crate::repository_policy::Provider::Gitlab,
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
            crate::repository_policy::Provider::Gitlab,
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
        command_ok(crate::repository_policy::Provider::Github, program, args)
    }

    fn github_issue_view(
        program: &str,
        target: &IssueSyncTarget,
        issue: &str,
    ) -> Result<IssueView> {
        let value = command_json(
            crate::repository_policy::Provider::Github,
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
            crate::repository_policy::Provider::Gitlab,
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
            crate::repository_policy::Provider::Gitlab,
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
        command_ok(crate::repository_policy::Provider::Gitlab, program, args)
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
                crate::repository_policy::Provider::Github,
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
                crate::repository_policy::Provider::Gitlab,
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

    fn command_json<I, S>(
        provider: crate::repository_policy::Provider,
        program: &str,
        args: I,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let output = command_output(provider, program, args)?;
        serde_json::from_str(&output).context("parse issue CLI JSON")
    }

    fn command_output<I, S>(
        provider: crate::repository_policy::Provider,
        program: &str,
        args: I,
    ) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<_>>();
        let mut executor = crate::providers::DirectProviderExecutor;
        let output = crate::providers::ProviderCommandExecutor::output(
            &mut executor,
            provider,
            program,
            &args,
        )?;
        if !output.status.success() {
            anyhow::bail!(
                "issue CLI {program} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8(output.stdout)
            .context("issue CLI stdout is not UTF-8")?
            .trim()
            .to_string())
    }

    fn command_ok<I, S>(
        provider: crate::repository_policy::Provider,
        program: &str,
        args: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        command_output(provider, program, args).map(|_| ())
    }

    #[cfg(test)]
    mod tests {
        use super::{GitHubIssueBackend, IssueRemoteAdapter};
        use std::os::unix::fs::PermissionsExt;

        #[test]
        fn issue_backend_uses_bounded_provider_execution() {
            let _execution = crate::providers::lock_test_provider_execution();
            let root = tempfile::tempdir().unwrap();
            let program = root.path().join("gh");
            std::fs::write(&program, "#!/bin/sh\n/bin/sleep 30\n").unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            let _authority = crate::providers::inject_test_provider_executable(
                crate::repository_policy::Provider::Github,
                &program,
            )
            .unwrap();

            let error = GitHubIssueBackend
                .verify_destination("owner/repository")
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<crate::providers::ProviderInfrastructureError>(),
                Some(crate::providers::ProviderInfrastructureError::TimedOut { .. })
            ));
        }
    }
}
