use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

pub const AUTOMATIC_CYCLE_LIMIT: u8 = 10;

pub fn validate_cycle_id(cycle_id: &str) -> Result<()> {
    if cycle_id.is_empty()
        || cycle_id.len() > 128
        || !cycle_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !cycle_id
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !cycle_id
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
    {
        anyhow::bail!("cycle ID has invalid systemd identity grammar");
    }
    Ok(())
}

pub fn systemd_unit_name(cycle_id: &str) -> Result<String> {
    validate_cycle_id(cycle_id)?;
    Ok(format!("iq-agent-{cycle_id}.service"))
}

pub fn validate_systemd_unit_name(cycle_id: &str, unit_name: &str) -> Result<()> {
    if unit_name != systemd_unit_name(cycle_id)? {
        anyhow::bail!("systemd unit name differs from exact cycle authority");
    }
    Ok(())
}

pub fn validate_legacy_systemd_scope_name(cycle_id: &str, unit_name: &str) -> Result<()> {
    validate_cycle_id(cycle_id)?;
    if unit_name != format!("iq-agent-{cycle_id}.scope") {
        anyhow::bail!("legacy systemd unit name differs from exact cycle authority");
    }
    Ok(())
}
pub const PROTOCOL_VERSION: u32 = 2;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateRepositorySnapshot {
    #[default]
    Local,
    GithubIssue(IssueRepositorySnapshot),
    GitlabIssue(IssueRepositorySnapshot),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueRepositorySnapshot {
    pub repository: String,
    pub visibility: IssueVisibility,
    pub allowed_responders: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueVisibility {
    Minimal,
    Full,
}

impl StateRepositorySnapshot {
    pub fn validate(self) -> Result<Self> {
        let issue = match &self {
            Self::Local => return Ok(self),
            Self::GithubIssue(issue) | Self::GitlabIssue(issue) => issue,
        };
        require_exact_text(&issue.repository, "state repository identity")?;
        if !issue.repository.contains('/')
            || issue.repository.starts_with('/')
            || issue.repository.ends_with('/')
        {
            anyhow::bail!("state repository identity must be an exact owner/repository path");
        }
        if issue.allowed_responders.is_empty() {
            anyhow::bail!("issue state repository requires allowed responders");
        }
        let mut responders = HashSet::new();
        for responder in &issue.allowed_responders {
            require_exact_text(responder, "allowed responder")?;
            if !responders.insert(responder.to_ascii_lowercase()) {
                anyhow::bail!("issue state repository has a duplicate allowed responder");
            }
        }
        Ok(self)
    }

    pub fn visibility(&self) -> Option<IssueVisibility> {
        match self {
            Self::Local => None,
            Self::GithubIssue(issue) | Self::GitlabIssue(issue) => Some(issue.visibility),
        }
    }

    pub fn permits_actor(&self, actor: &str) -> bool {
        match self {
            Self::Local => false,
            Self::GithubIssue(issue) | Self::GitlabIssue(issue) => issue
                .allowed_responders
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(actor)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerSnapshot {
    pub kind: RunnerKind,
    pub executable: ExecutableIdentity,
    pub agent: String,
    pub model: String,
    pub cycle_timeout_seconds: u64,
    pub bounds: RunnerBounds,
    pub sandbox: SandboxIdentity,
    pub credential_env: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerKind {
    Opencode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableIdentity {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerBounds {
    pub max_log_bytes: u64,
    pub max_result_bytes: u64,
    pub max_processes: u32,
    pub memory_bytes: u64,
    pub cpu_seconds: u64,
    pub writable_bytes: u64,
    pub open_files: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxIdentity {
    pub implementation: String,
    pub bubblewrap: ExecutableIdentity,
    pub unshare: ExecutableIdentity,
    pub systemd_run: ExecutableIdentity,
    pub systemctl: ExecutableIdentity,
}

impl RunnerSnapshot {
    pub fn validate(self) -> Result<Self> {
        require_exact_text(&self.agent, "runner agent")?;
        require_exact_text(&self.model, "runner model")?;
        require_exact_text(
            &self.credential_env,
            "runner credential environment variable",
        )?;
        if !self
            .credential_env
            .bytes()
            .enumerate()
            .all(|(index, byte)| {
                byte == b'_' || byte.is_ascii_uppercase() || (index > 0 && byte.is_ascii_digit())
            })
        {
            anyhow::bail!(
                "runner credential environment variable must be a valid uppercase environment name"
            );
        }
        if !self.executable.path.is_absolute()
            || self.executable.device == 0
            || self.executable.inode == 0
            || self.executable.sha256.len() != 64
            || !self
                .executable
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            anyhow::bail!("runner executable identity is invalid");
        }
        if self.cycle_timeout_seconds == 0
            || self.bounds.max_log_bytes == 0
            || self.bounds.max_result_bytes == 0
            || self.bounds.max_processes == 0
            || self.bounds.memory_bytes == 0
            || self.bounds.cpu_seconds == 0
            || self.bounds.writable_bytes == 0
            || self.bounds.open_files == 0
        {
            anyhow::bail!("runner limits must all be positive");
        }
        require_exact_text(
            &self.sandbox.implementation,
            "runner sandbox implementation",
        )?;
        for (executable, label) in [
            (&self.sandbox.bubblewrap, "runner bubblewrap executable"),
            (&self.sandbox.unshare, "runner unshare executable"),
            (&self.sandbox.systemd_run, "runner systemd-run executable"),
            (&self.sandbox.systemctl, "runner systemctl executable"),
        ] {
            if !executable.path.is_absolute()
                || executable.device == 0
                || executable.inode == 0
                || executable.sha256.len() != 64
                || !executable
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                anyhow::bail!("{label} identity is invalid");
            }
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IntegrationEffortState {
    ReplacementPending(ReplacementPending),
    AgentReady(AgentReady),
    AgentLaunching(AgentLaunching),
    AgentRunning(AgentRunning),
    CandidateBuilding(CandidateBuilding),
    CandidateReady(CandidateReady),
    Validating(Validating),
    GuidanceRequired(BlockedEffort),
    InfrastructureBlocked(BlockedEffort),
    CycleLimitBlocked(BlockedEffort),
    ProviderBlocked(BlockedEffort),
    Landing(Landing),
    LandingUncertain(LandingUncertain),
    TargetMovePending(TargetMovePending),
    Integrated(Integrated),
    Cancelled(Cancelled),
}

impl IntegrationEffortState {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ReplacementPending(_) => "replacement_pending",
            Self::AgentReady(_) => "agent_ready",
            Self::AgentLaunching(_) => "agent_launching",
            Self::AgentRunning(_) => "agent_running",
            Self::CandidateBuilding(_) => "candidate_building",
            Self::CandidateReady(_) => "candidate_ready",
            Self::Validating(_) => "validating",
            Self::GuidanceRequired(_) => "guidance_required",
            Self::InfrastructureBlocked(_) => "infrastructure_blocked",
            Self::CycleLimitBlocked(_) => "cycle_limit_blocked",
            Self::ProviderBlocked(_) => "provider_blocked",
            Self::Landing(_) => "landing",
            Self::LandingUncertain(_) => "landing_uncertain",
            Self::TargetMovePending(_) => "target_move_pending",
            Self::Integrated(_) => "integrated",
            Self::Cancelled(_) => "cancelled",
        }
    }

    pub fn blocker(&self) -> Option<&IntegrationBlocker> {
        match self {
            Self::GuidanceRequired(value)
            | Self::InfrastructureBlocked(value)
            | Self::CycleLimitBlocked(value)
            | Self::ProviderBlocked(value) => Some(&value.blocker),
            _ => None,
        }
    }

    pub fn candidate_sha(&self) -> Option<&str> {
        match self {
            Self::CandidateReady(value) => Some(&value.candidate_sha),
            Self::Validating(value) => Some(&value.candidate_sha),
            Self::Landing(value) => Some(&value.candidate_sha),
            Self::LandingUncertain(value) => Some(&value.candidate_sha),
            Self::TargetMovePending(value) => value.previous.candidate_sha(),
            Self::InfrastructureBlocked(value) | Self::ProviderBlocked(value) => {
                value.resume.candidate_sha()
            }
            Self::Integrated(value) => Some(&value.candidate_sha),
            _ => None,
        }
    }

    pub fn contains_external_landing_authority(&self) -> bool {
        match self {
            Self::GuidanceRequired(blocked)
            | Self::InfrastructureBlocked(blocked)
            | Self::CycleLimitBlocked(blocked)
            | Self::ProviderBlocked(blocked) => {
                blocked.resume.contains_external_landing_authority()
            }
            Self::LandingUncertain(_) | Self::Integrated(_) => true,
            Self::ReplacementPending(_)
            | Self::AgentReady(_)
            | Self::AgentLaunching(_)
            | Self::AgentRunning(_)
            | Self::CandidateBuilding(_)
            | Self::CandidateReady(_)
            | Self::Validating(_)
            | Self::Landing(_)
            | Self::TargetMovePending(_)
            | Self::Cancelled(_) => false,
        }
    }

    pub fn validate_for_count(&self, failed_cycles: u8) -> Result<()> {
        match self {
            Self::ReplacementPending(value)
                if failed_cycles != 0
                    || value.old_attempt_id.is_empty()
                    || value.replaced_at.is_empty() =>
            {
                anyhow::bail!(
                    "replacement_pending requires its old attempt, replacement time, and zero failed cycles"
                )
            }
            Self::GuidanceRequired(value)
                if !matches!(value.blocker, IntegrationBlocker::SemanticGuidance(_)) =>
            {
                anyhow::bail!("guidance_required requires semantic_guidance")
            }
            Self::InfrastructureBlocked(value)
                if !matches!(value.blocker, IntegrationBlocker::Infrastructure(_)) =>
            {
                anyhow::bail!("infrastructure_blocked requires infrastructure")
            }
            Self::InfrastructureBlocked(value) => value.resume.validate_infrastructure()?,
            Self::CycleLimitBlocked(value) => match &value.blocker {
                IntegrationBlocker::CycleLimit(blocker)
                    if failed_cycles == AUTOMATIC_CYCLE_LIMIT
                        && blocker.count == AUTOMATIC_CYCLE_LIMIT => {}
                _ => anyhow::bail!("cycle_limit_blocked requires exactly ten failed cycles"),
            },
            Self::ProviderBlocked(value)
                if !matches!(value.blocker, IntegrationBlocker::ProviderSignoff(_)) =>
            {
                anyhow::bail!("provider_blocked requires provider_signoff")
            }
            Self::ProviderBlocked(value) => {
                let IntegrationBlocker::ProviderSignoff(blocker) = &value.blocker else {
                    unreachable!("provider blocker variant was validated")
                };
                value.resume.validate_provider(blocker)?;
            }
            _ if failed_cycles > AUTOMATIC_CYCLE_LIMIT => {
                anyhow::bail!("failed consumed-cycle count exceeds ten")
            }
            Self::AgentLaunching(value) => {
                if value.launch_operation_id.is_empty()
                    || value.unit_name.is_empty()
                    || value.cycle_id.is_empty()
                    || value.cycle_number == 0
                    || value.authority_lease_id.is_empty()
                    || value.launcher.pid == 0
                    || value.launcher.process_start_ticks == 0
                    || value.launcher.token.len() < 32
                    || !value
                        .launcher
                        .token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    || value.input_sha256.len() != 64
                    || !value
                        .input_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                    || !matches!(
                        value.spawn_authority,
                        SpawnAuthority::Open | SpawnAuthority::Surrendered
                    )
                {
                    anyhow::bail!("agent_launching has invalid launch authority")
                }
                validate_systemd_unit_name(&value.cycle_id, &value.unit_name)?;
            }
            Self::AgentRunning(value) => {
                if value.launch_operation_id.is_empty()
                    || value.unit_name.is_empty()
                    || value.cycle_id.is_empty()
                    || value.cycle_number == 0
                    || value.pid == 0
                    || !value.control_group.starts_with("/user.slice/")
                    || value.authority_lease_id.is_empty()
                    || value.launcher.pid == 0
                    || value.launcher.process_start_ticks == 0
                    || value.launcher.token.len() < 32
                    || !value
                        .launcher
                        .token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    || value.input_sha256.len() != 64
                    || !value
                        .input_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                {
                    anyhow::bail!("agent_running has invalid launch authority")
                }
                validate_systemd_unit_name(&value.cycle_id, &value.unit_name)?;
                value.termination_authority().validate()?;
            }
            Self::TargetMovePending(value) => {
                require_exact_text(&value.target_sha, "pending target move target")?;
                require_exact_text(&value.source_sha, "pending target move source")?;
                match &value.cause {
                    TargetMoveCause::Observed {
                        previous_target_sha,
                    } => {
                        require_exact_text(previous_target_sha, "previous target")?;
                    }
                    TargetMoveCause::StaleLandingLease {
                        command_id,
                        expected_target_sha,
                    } => {
                        require_exact_text(command_id, "stale landing command")?;
                        require_exact_text(expected_target_sha, "stale landing expected target")?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn validate_object_ids(
        &self,
        object_format: crate::git_object::GitObjectFormat,
    ) -> Result<()> {
        match self {
            Self::CandidateBuilding(value) => {
                object_format.require_oid(&value.tree_sha, "candidate-building tree")?;
                for parent in &value.parent_shas {
                    object_format.require_oid(parent, "candidate-building parent")?;
                }
            }
            Self::CandidateReady(value) => {
                object_format.require_oid(&value.candidate_sha, "ready candidate")?;
            }
            Self::Validating(value) => {
                object_format.require_oid(&value.candidate_sha, "validating candidate")?;
            }
            Self::GuidanceRequired(value)
            | Self::InfrastructureBlocked(value)
            | Self::CycleLimitBlocked(value)
            | Self::ProviderBlocked(value) => {
                value.resume.validate_object_ids(object_format)?;
                match &value.blocker {
                    IntegrationBlocker::SemanticGuidance(blocker) => {
                        validate_exact_effort_object_ids(&blocker.identity, object_format)?;
                    }
                    IntegrationBlocker::ProviderSignoff(blocker) => {
                        object_format
                            .require_oid(&blocker.candidate_sha, "provider-blocker candidate")?;
                    }
                    IntegrationBlocker::Infrastructure(_) | IntegrationBlocker::CycleLimit(_) => {}
                }
            }
            Self::Landing(value) => validate_landing_object_ids(value, object_format)?,
            Self::LandingUncertain(value) => {
                validate_landing_uncertain_object_ids(value, object_format)?;
            }
            Self::TargetMovePending(value) => {
                object_format.require_oid(&value.target_sha, "pending target move target")?;
                object_format.require_oid(&value.source_sha, "pending target move source")?;
                value.previous.validate_object_ids(object_format)?;
                match &value.cause {
                    TargetMoveCause::Observed {
                        previous_target_sha,
                    } => {
                        object_format.require_oid(previous_target_sha, "previous target")?;
                    }
                    TargetMoveCause::StaleLandingLease {
                        expected_target_sha,
                        ..
                    } => {
                        object_format
                            .require_oid(expected_target_sha, "stale landing expected target")?;
                    }
                }
            }
            Self::Integrated(value) => {
                object_format.require_oid(&value.candidate_sha, "integrated candidate")?;
                object_format.require_oid(&value.landed_sha, "integrated commit")?;
            }
            Self::ReplacementPending(_)
            | Self::AgentReady(_)
            | Self::AgentLaunching(_)
            | Self::AgentRunning(_)
            | Self::Cancelled(_) => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementPending {
    pub old_attempt_id: String,
    pub replaced_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentReady {
    pub next_cycle: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLaunching {
    pub launch_operation_id: String,
    pub unit_name: String,
    pub cycle_id: String,
    pub cycle_number: u8,
    pub authority_lease_id: String,
    pub launcher: LauncherAuthority,
    pub input_sha256: String,
    pub protocol_directory: PathBuf,
    pub prepared_at: String,
    pub spawn_authority: SpawnAuthority,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LauncherAuthority {
    pub pid: u32,
    pub process_start_ticks: u64,
    pub token: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnAuthority {
    Open,
    CloseRequested,
    Surrendered,
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRunning {
    pub launch_operation_id: String,
    pub unit_name: String,
    pub cycle_id: String,
    pub cycle_number: u8,
    pub pid: u32,
    pub process_start_ticks: u64,
    pub control_group: String,
    pub authority_lease_id: String,
    pub launcher: LauncherAuthority,
    pub sandbox_id: String,
    pub input_sha256: String,
    pub result: AtomicResultState,
    pub started_at: String,
}

impl AgentRunning {
    pub fn termination_authority(&self) -> RunnerServiceAuthority {
        RunnerServiceAuthority {
            cycle_id: self.cycle_id.clone(),
            unit_name: self.unit_name.clone(),
            control_group: self.control_group.clone(),
            pid: self.pid,
            process_start_ticks: self.process_start_ticks,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerServiceAuthority {
    pub cycle_id: String,
    pub unit_name: String,
    pub control_group: String,
    pub pid: u32,
    pub process_start_ticks: u64,
}

impl RunnerServiceAuthority {
    pub fn validate(&self) -> Result<()> {
        validate_systemd_unit_name(&self.cycle_id, &self.unit_name)?;
        if self.pid == 0 || self.process_start_ticks == 0 {
            anyhow::bail!("runner service process identity is invalid");
        }
        let expected_suffix = format!("/{}", self.unit_name);
        if !self.control_group.starts_with("/user.slice/")
            || !self.control_group.ends_with(&expected_suffix)
        {
            anyhow::bail!("runner control group differs from its exact systemd unit");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyRunnerScopeAuthority {
    pub cycle_id: String,
    pub unit_name: String,
    pub control_group: String,
    pub pid: u32,
    pub process_start_ticks: u64,
}

impl LegacyRunnerScopeAuthority {
    pub fn validate(&self) -> Result<()> {
        validate_legacy_systemd_scope_name(&self.cycle_id, &self.unit_name)?;
        if self.pid == 0 || self.process_start_ticks == 0 {
            anyhow::bail!("legacy runner scope process identity is invalid");
        }
        let expected_suffix = format!("/{}", self.unit_name);
        if !self.control_group.starts_with("/user.slice/")
            || !self.control_group.ends_with(&expected_suffix)
        {
            anyhow::bail!("legacy runner cgroup differs from its exact systemd scope");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum AtomicResultState {
    Absent,
    Writing {
        device: u64,
        inode: u64,
    },
    Complete {
        device: u64,
        inode: u64,
        sha256: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateBuilding {
    pub operation_id: String,
    pub cycle_id: String,
    pub staged_tree_sha256: String,
    pub tree_sha: String,
    pub parent_shas: Vec<String>,
    pub author_name: String,
    pub author_email: String,
    pub author_timestamp: String,
    pub committer_name: String,
    pub committer_email: String,
    pub committer_timestamp: String,
    pub message: String,
    pub operation_ref: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateReady {
    pub operation_id: String,
    pub cycle_id: String,
    pub candidate_sha: String,
    pub staged_tree_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Validating {
    pub candidate_sha: String,
    pub policy_digest: String,
    pub stage: ValidationStage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetMovePending {
    pub target_sha: String,
    pub source_sha: String,
    pub previous: ResumeState,
    pub cause: TargetMoveCause,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TargetMoveCause {
    Observed {
        previous_target_sha: String,
    },
    StaleLandingLease {
        command_id: String,
        expected_target_sha: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStage {
    Running,
    Gates,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockedEffort {
    pub blocker: IntegrationBlocker,
    pub resume: ResumeState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ResumeState {
    AgentReady(AgentReady),
    CandidateBuilding(CandidateBuilding),
    CandidateReady(CandidateReady),
    Validating(Validating),
    Landing(Landing),
    LandingUncertain(LandingUncertain),
}

impl ResumeState {
    pub(crate) fn candidate_sha(&self) -> Option<&str> {
        match self {
            Self::CandidateReady(value) => Some(&value.candidate_sha),
            Self::Validating(value) => Some(&value.candidate_sha),
            Self::Landing(value) => Some(&value.candidate_sha),
            Self::LandingUncertain(value) => Some(&value.candidate_sha),
            Self::AgentReady(_) | Self::CandidateBuilding(_) => None,
        }
    }

    fn contains_external_landing_authority(&self) -> bool {
        match self {
            Self::LandingUncertain(_) => true,
            Self::AgentReady(_)
            | Self::CandidateBuilding(_)
            | Self::CandidateReady(_)
            | Self::Validating(_)
            | Self::Landing(_) => false,
        }
    }

    pub(crate) fn capture(state: &IntegrationEffortState) -> Result<Self> {
        match state {
            IntegrationEffortState::AgentReady(value) => Ok(Self::AgentReady(value.clone())),
            IntegrationEffortState::CandidateBuilding(value) => {
                Ok(Self::CandidateBuilding(value.clone()))
            }
            IntegrationEffortState::CandidateReady(value) => {
                Ok(Self::CandidateReady(value.clone()))
            }
            IntegrationEffortState::Validating(value) => Ok(Self::Validating(value.clone())),
            IntegrationEffortState::Landing(value) => Ok(Self::Landing(value.clone())),
            IntegrationEffortState::LandingUncertain(value) => {
                Ok(Self::LandingUncertain(value.clone()))
            }
            _ => anyhow::bail!("effort state cannot be suspended for external repair"),
        }
    }

    pub(crate) fn restore(&self) -> IntegrationEffortState {
        match self {
            Self::AgentReady(value) => IntegrationEffortState::AgentReady(value.clone()),
            Self::CandidateBuilding(value) => {
                IntegrationEffortState::CandidateBuilding(value.clone())
            }
            Self::CandidateReady(value) => IntegrationEffortState::CandidateReady(value.clone()),
            Self::Validating(value) => IntegrationEffortState::Validating(value.clone()),
            Self::Landing(value) => IntegrationEffortState::Landing(value.clone()),
            Self::LandingUncertain(value) => {
                IntegrationEffortState::LandingUncertain(value.clone())
            }
        }
    }

    fn validate_infrastructure(&self) -> Result<()> {
        if matches!(
            self,
            Self::AgentReady(_)
                | Self::CandidateBuilding(_)
                | Self::CandidateReady(_)
                | Self::Validating(_)
                | Self::Landing(_)
                | Self::LandingUncertain(_)
        ) {
            Ok(())
        } else {
            anyhow::bail!("infrastructure blocker has an invalid resume state")
        }
    }

    fn validate_provider(&self, blocker: &ProviderSignoffBlocker) -> Result<()> {
        let candidate_sha = match self {
            Self::Validating(Validating {
                candidate_sha,
                stage: ValidationStage::Gates,
                ..
            })
            | Self::Landing(Landing { candidate_sha, .. })
            | Self::LandingUncertain(LandingUncertain { candidate_sha, .. }) => candidate_sha,
            _ => anyhow::bail!("provider blocker requires a landing-gate resume state"),
        };
        if candidate_sha != &blocker.candidate_sha {
            anyhow::bail!("provider blocker candidate differs from its resume state");
        }
        Ok(())
    }

    fn validate_object_ids(&self, object_format: crate::git_object::GitObjectFormat) -> Result<()> {
        match self {
            Self::CandidateBuilding(value) => {
                object_format.require_oid(&value.tree_sha, "blocked candidate-building tree")?;
                for parent in &value.parent_shas {
                    object_format.require_oid(parent, "blocked candidate-building parent")?;
                }
            }
            Self::CandidateReady(value) => {
                object_format.require_oid(&value.candidate_sha, "blocked ready candidate")?;
            }
            Self::Validating(value) => {
                object_format.require_oid(&value.candidate_sha, "blocked validating candidate")?;
            }
            Self::Landing(value) => validate_landing_object_ids(value, object_format)?,
            Self::LandingUncertain(value) => {
                validate_landing_uncertain_object_ids(value, object_format)?;
            }
            Self::AgentReady(_) => {}
        }
        Ok(())
    }
}

fn validate_exact_effort_object_ids(
    identity: &ExactEffortIdentity,
    object_format: crate::git_object::GitObjectFormat,
) -> Result<()> {
    object_format.require_oid(&identity.target_sha, "effort identity target")?;
    object_format.require_oid(&identity.source_sha, "effort identity source")?;
    if let Some(candidate) = &identity.candidate_sha {
        object_format.require_oid(candidate, "effort identity candidate")?;
    }
    Ok(())
}

fn validate_landing_object_ids(
    landing: &Landing,
    object_format: crate::git_object::GitObjectFormat,
) -> Result<()> {
    object_format.require_oid(&landing.candidate_sha, "landing candidate")?;
    object_format.require_oid(&landing.expected_target_sha, "landing expected target")?;
    if let SignoffDisposition::Evidence { candidate_sha, .. } = &landing.signoff {
        object_format.require_oid(candidate_sha, "landing signoff candidate")?;
    }
    Ok(())
}

fn validate_landing_uncertain_object_ids(
    landing: &LandingUncertain,
    object_format: crate::git_object::GitObjectFormat,
) -> Result<()> {
    object_format.require_oid(&landing.candidate_sha, "uncertain landing candidate")?;
    object_format.require_oid(
        &landing.expected_target_sha,
        "uncertain landing expected target",
    )?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Landing {
    pub candidate_sha: String,
    pub expected_target_sha: String,
    pub lease_id: String,
    pub signoff: SignoffDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SignoffDisposition {
    NoValidation {
        policy_digest: String,
    },
    ValidationWithoutSignoff {
        policy_digest: String,
    },
    Evidence {
        evidence_id: String,
        candidate_sha: String,
        policy_digest: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingUncertain {
    pub candidate_sha: String,
    pub expected_target_sha: String,
    pub command_id: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Integrated {
    pub candidate_sha: String,
    pub landed_sha: String,
    pub attempt_id: String,
    pub event_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cancelled {
    pub actor: String,
    pub reason: String,
    pub cancelled_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IntegrationBlocker {
    SemanticGuidance(Box<SemanticGuidanceBlocker>),
    Infrastructure(InfrastructureBlocker),
    CycleLimit(CycleLimitBlocker),
    ProviderSignoff(ProviderSignoffBlocker),
}

impl IntegrationBlocker {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SemanticGuidance(_) => "semantic_guidance",
            Self::Infrastructure(_) => "infrastructure",
            Self::CycleLimit(_) => "cycle_limit",
            Self::ProviderSignoff(_) => "provider_signoff",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactEffortIdentity {
    pub effort_id: String,
    pub item_id: String,
    pub attempt_id: String,
    pub cycle_id: String,
    pub target_sha: String,
    pub source_sha: String,
    pub candidate_sha: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGuidanceBlocker {
    pub request_id: String,
    pub question: String,
    pub affected_contracts: Vec<String>,
    pub affected_paths: Vec<EncodedPath>,
    pub alternatives: GuidanceAlternatives,
    pub evidence: String,
    pub identity: ExactEffortIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GuidanceAlternatives {
    Explicit { values: Vec<String> },
    FreeText,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfrastructureBlocker {
    pub component: InfrastructureComponent,
    pub operation: String,
    pub cause: InfrastructureCause,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InfrastructureComponent {
    Configuration,
    Sandbox,
    Runner,
    Filesystem,
    Database,
    Validation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InfrastructureCause {
    Unavailable { detail: String },
    IdentityChanged { expected: String, actual: String },
    Interrupted { detail: String },
    LimitUnavailable { limit: String, detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CycleLimitBlocker {
    pub count: u8,
    pub cycle_ids: Vec<String>,
    pub last_failure: CycleFailure,
    pub alert_event_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CycleFailure {
    Mechanical {
        operation: String,
        reason: String,
        evidence: String,
    },
    InvalidResult {
        reason: String,
    },
    RunnerCrash {
        exit_code: Option<i32>,
    },
    Timeout,
    Interrupted,
    CandidateDefect {
        evidence: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSignoffBlocker {
    pub gate: ProviderGateKind,
    pub repository: String,
    pub context: String,
    pub candidate_sha: String,
    pub status: ProviderGateStatus,
    pub evidence: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderGateKind {
    Provider,
    Signoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderGateStatus {
    Pending,
    Failed,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EncodedPath(pub Vec<EncodedPathComponent>);

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncodedPathComponent {
    pub hex: String,
}

impl EncodedPath {
    pub fn from_bytes(path: &[u8]) -> Result<Self> {
        if path.is_empty() || path.starts_with(b"/") || path.ends_with(b"/") {
            anyhow::bail!("protocol path must be a non-empty relative path");
        }
        let mut components = Vec::new();
        for component in path.split(|byte| *byte == b'/') {
            if component.is_empty()
                || component == b"."
                || component == b".."
                || component.contains(&0)
            {
                anyhow::bail!("protocol path has an invalid component");
            }
            components.push(EncodedPathComponent {
                hex: component.iter().map(|byte| format!("{byte:02x}")).collect(),
            });
        }
        Ok(Self(components))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        if self.0.is_empty() {
            anyhow::bail!("encoded path has no components");
        }
        let mut path = Vec::new();
        for (index, component) in self.0.iter().enumerate() {
            if component.hex.is_empty()
                || component.hex.len() % 2 != 0
                || !component.hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                anyhow::bail!("encoded path component is not canonical hexadecimal");
            }
            let bytes = (0..component.hex.len())
                .step_by(2)
                .map(|offset| u8::from_str_radix(&component.hex[offset..offset + 2], 16))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if bytes.is_empty()
                || bytes == b"."
                || bytes == b".."
                || bytes.contains(&0)
                || bytes.contains(&b'/')
            {
                anyhow::bail!("encoded path component is invalid");
            }
            if index != 0 {
                path.push(b'/');
            }
            path.extend(bytes);
        }
        Ok(path)
    }
}

pub fn require_exact_text<'a>(value: &'a str, label: &str) -> Result<&'a str> {
    if value.is_empty() || value.trim() != value {
        anyhow::bail!("{label} must be non-empty with no surrounding whitespace");
    }
    Ok(value)
}

pub fn parse_strict_json<T>(bytes: &[u8], label: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(bytes).with_context(|| format!("parse strict {label}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    type BlockedStateConstructor = fn(BlockedEffort) -> IntegrationEffortState;

    fn landing() -> Landing {
        Landing {
            candidate_sha: "3".repeat(40),
            expected_target_sha: "1".repeat(40),
            lease_id: "lease-1".into(),
            signoff: SignoffDisposition::NoValidation {
                policy_digest: "a".repeat(64),
            },
        }
    }

    fn landing_uncertain() -> LandingUncertain {
        LandingUncertain {
            candidate_sha: "3".repeat(40),
            expected_target_sha: "1".repeat(40),
            command_id: "command-1".into(),
            evidence: "command_gate_released".into(),
        }
    }

    fn blockers() -> Vec<(BlockedStateConstructor, IntegrationBlocker)> {
        vec![
            (
                IntegrationEffortState::GuidanceRequired,
                IntegrationBlocker::SemanticGuidance(Box::new(SemanticGuidanceBlocker {
                    request_id: "request-1".into(),
                    question: "Choose an integration contract".into(),
                    affected_contracts: vec!["integration".into()],
                    affected_paths: vec![EncodedPath::from_bytes(b"src/lib.rs").unwrap()],
                    alternatives: GuidanceAlternatives::FreeText,
                    evidence: "contract is ambiguous".into(),
                    identity: ExactEffortIdentity {
                        effort_id: "effort-1".into(),
                        item_id: "item-1".into(),
                        attempt_id: "attempt-1".into(),
                        cycle_id: "cycle-1".into(),
                        target_sha: "1".repeat(40),
                        source_sha: "2".repeat(40),
                        candidate_sha: Some("3".repeat(40)),
                    },
                })),
            ),
            (
                IntegrationEffortState::InfrastructureBlocked,
                IntegrationBlocker::Infrastructure(InfrastructureBlocker {
                    component: InfrastructureComponent::Filesystem,
                    operation: "landing".into(),
                    cause: InfrastructureCause::Interrupted {
                        detail: "restart required".into(),
                    },
                }),
            ),
            (
                IntegrationEffortState::CycleLimitBlocked,
                IntegrationBlocker::CycleLimit(CycleLimitBlocker {
                    count: AUTOMATIC_CYCLE_LIMIT,
                    cycle_ids: vec!["cycle-1".into()],
                    last_failure: CycleFailure::Interrupted,
                    alert_event_id: "event-1".into(),
                }),
            ),
            (
                IntegrationEffortState::ProviderBlocked,
                IntegrationBlocker::ProviderSignoff(ProviderSignoffBlocker {
                    gate: ProviderGateKind::Provider,
                    repository: "org/repository".into(),
                    context: "landing".into(),
                    candidate_sha: "3".repeat(40),
                    status: ProviderGateStatus::Pending,
                    evidence: "provider response is pending".into(),
                }),
            ),
        ]
    }

    #[test]
    fn external_landing_authority_predicate_is_exhaustive_for_direct_states() {
        let ordinary = [
            IntegrationEffortState::ReplacementPending(ReplacementPending {
                old_attempt_id: "attempt-1".into(),
                replaced_at: "2026-01-01T00:00:00Z".into(),
            }),
            IntegrationEffortState::AgentReady(AgentReady { next_cycle: 1 }),
            IntegrationEffortState::AgentLaunching(AgentLaunching {
                launch_operation_id: "launch-1".into(),
                unit_name: "iq-agent-cycle-1.service".into(),
                cycle_id: "cycle-1".into(),
                cycle_number: 1,
                authority_lease_id: "lease-1".into(),
                launcher: LauncherAuthority {
                    pid: 1,
                    process_start_ticks: 1,
                    token: "00000000-0000-4000-8000-000000000001".into(),
                },
                input_sha256: "a".repeat(64),
                protocol_directory: PathBuf::from("/tmp/protocol"),
                prepared_at: "2026-01-01T00:00:00Z".into(),
                spawn_authority: SpawnAuthority::Open,
            }),
            IntegrationEffortState::AgentRunning(AgentRunning {
                launch_operation_id: "launch-1".into(),
                unit_name: "iq-agent-cycle-1.service".into(),
                cycle_id: "cycle-1".into(),
                cycle_number: 1,
                pid: 1,
                process_start_ticks: 1,
                control_group:
                    "/user.slice/user-1000.slice/user@1000.service/app.slice/iq-agent-cycle-1.service"
                        .into(),
                authority_lease_id: "lease-1".into(),
                launcher: LauncherAuthority {
                    pid: 1,
                    process_start_ticks: 1,
                    token: "00000000-0000-4000-8000-000000000001".into(),
                },
                sandbox_id: "sandbox-1".into(),
                input_sha256: "a".repeat(64),
                result: AtomicResultState::Absent,
                started_at: "2026-01-01T00:00:00Z".into(),
            }),
            IntegrationEffortState::CandidateBuilding(CandidateBuilding {
                operation_id: "candidate-1".into(),
                cycle_id: "cycle-1".into(),
                staged_tree_sha256: "a".repeat(64),
                tree_sha: "4".repeat(40),
                parent_shas: vec!["1".repeat(40)],
                author_name: "IQ Test".into(),
                author_email: "iq@example.test".into(),
                author_timestamp: "2026-01-01T00:00:00Z".into(),
                committer_name: "IQ Test".into(),
                committer_email: "iq@example.test".into(),
                committer_timestamp: "2026-01-01T00:00:00Z".into(),
                message: "candidate".into(),
                operation_ref: "refs/iq/candidate-operations/candidate-1".into(),
            }),
            IntegrationEffortState::CandidateReady(CandidateReady {
                operation_id: "candidate-1".into(),
                cycle_id: "cycle-1".into(),
                candidate_sha: "3".repeat(40),
                staged_tree_sha256: "a".repeat(64),
            }),
            IntegrationEffortState::Validating(Validating {
                candidate_sha: "3".repeat(40),
                policy_digest: "a".repeat(64),
                stage: ValidationStage::Gates,
            }),
            IntegrationEffortState::Landing(landing()),
            IntegrationEffortState::Cancelled(Cancelled {
                actor: "operator".into(),
                reason: "cancelled".into(),
                cancelled_at: "2026-01-01T00:00:00Z".into(),
            }),
        ];
        for state in ordinary {
            assert!(
                !state.contains_external_landing_authority(),
                "ordinary state reported external landing authority: {state:?}"
            );
        }
        for (wrap, blocker) in blockers() {
            let state = wrap(BlockedEffort {
                blocker,
                resume: ResumeState::Landing(landing()),
            });
            assert!(
                !state.contains_external_landing_authority(),
                "prepared landing reported released authority: {state:?}"
            );
        }
        assert!(
            IntegrationEffortState::LandingUncertain(landing_uncertain())
                .contains_external_landing_authority()
        );
        assert!(IntegrationEffortState::Integrated(Integrated {
            candidate_sha: "3".repeat(40),
            landed_sha: "4".repeat(40),
            attempt_id: "attempt-1".into(),
            event_id: "event-1".into(),
        })
        .contains_external_landing_authority());
    }

    #[test]
    fn every_blocked_wrapper_preserves_released_landing_authority() {
        for (wrap, blocker) in blockers() {
            let state = wrap(BlockedEffort {
                blocker,
                resume: ResumeState::LandingUncertain(landing_uncertain()),
            });
            assert!(
                state.contains_external_landing_authority(),
                "blocked state hid released landing authority: {state:?}"
            );
        }
    }
}
