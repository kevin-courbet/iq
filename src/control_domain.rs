use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

pub const AUTOMATIC_CYCLE_LIMIT: u8 = 10;
pub const PROTOCOL_VERSION: u32 = 1;

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
    pub bubblewrap: PathBuf,
    pub unshare: PathBuf,
    pub systemd_run: PathBuf,
    pub systemctl: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IntegrationEffortState {
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
    Integrated(Integrated),
    Cancelled(Cancelled),
}

impl IntegrationEffortState {
    pub fn name(&self) -> &'static str {
        match self {
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
            Self::InfrastructureBlocked(value) | Self::ProviderBlocked(value) => {
                value.resume.candidate_sha()
            }
            Self::Integrated(value) => Some(&value.candidate_sha),
            _ => None,
        }
    }

    pub fn validate_for_count(&self, failed_cycles: u8) -> Result<()> {
        match self {
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
            Self::AgentLaunching(value)
                if value.launch_operation_id.is_empty()
                    || value.unit_name.is_empty()
                    || value.cycle_id.is_empty()
                    || value.cycle_number == 0
                    || value.authority_lease_id.is_empty()
                    || value.input_sha256.len() != 64
                    || !value
                        .input_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit()) =>
            {
                anyhow::bail!("agent_launching has invalid launch authority")
            }
            _ => {}
        }
        Ok(())
    }
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
    pub input_sha256: String,
    pub protocol_directory: PathBuf,
    pub prepared_at: String,
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
    pub process_group_id: i32,
    pub authority_lease_id: String,
    pub sandbox_id: String,
    pub input_sha256: String,
    pub result: AtomicResultState,
    pub started_at: String,
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
    fn candidate_sha(&self) -> Option<&str> {
        match self {
            Self::CandidateReady(value) => Some(&value.candidate_sha),
            Self::Validating(value) => Some(&value.candidate_sha),
            Self::Landing(value) => Some(&value.candidate_sha),
            Self::LandingUncertain(value) => Some(&value.candidate_sha),
            Self::AgentReady(_) | Self::CandidateBuilding(_) => None,
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
    NotRequired,
    Evidence {
        evidence_id: String,
        candidate_sha: String,
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

pub fn require_sha(value: &str, label: &str) -> Result<()> {
    if !matches!(value.len(), 40 | 64) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{label} must be a full hexadecimal Git object ID");
    }
    Ok(())
}

pub fn parse_strict_json<T>(bytes: &[u8], label: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(bytes).with_context(|| format!("parse strict {label}"))
}
