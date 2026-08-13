use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{
    backup::Backup, params, Connection, OpenFlags, OptionalExtension, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use uuid::Uuid;

use crate::agent_config::SystemConfig;
use crate::control_domain::{
    AgentLaunching, AgentReady, IntegrationBlocker, IntegrationEffortState, ResumeState,
    RunnerSnapshot, StateRepositorySnapshot, AUTOMATIC_CYCLE_LIMIT,
};
use crate::sqlite::WorkspaceIdentity;

pub const SCHEMA_VERSION: &str = "9";

pub struct DatabaseProcessLease {
    file: File,
}

impl DatabaseProcessLease {
    pub fn acquire(database_path: &Path) -> Result<Self> {
        Self::acquire_with_mode(database_path, libc::LOCK_SH)
    }

    pub fn acquire_exclusive(database_path: &Path) -> Result<Self> {
        Self::acquire_with_mode(database_path, libc::LOCK_EX)
    }

    fn acquire_with_mode(database_path: &Path, mode: libc::c_int) -> Result<Self> {
        let parent = database_path
            .parent()
            .context("database path has no parent")?;
        let name = database_path
            .file_name()
            .context("database path has no file name")?;
        let path = parent.join(format!("{}.control.lock", name.to_string_lossy()));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        if unsafe { libc::flock(file.as_raw_fd(), mode | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("acquire exclusive IQ database process lease");
        }
        Ok(Self { file })
    }
}

impl Drop for DatabaseProcessLease {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationEffort {
    pub id: String,
    pub item_id: String,
    pub attempt_id: String,
    pub target_sha: String,
    pub source_sha: String,
    pub source_variant: String,
    pub landing_variant: String,
    pub workspace: WorkspaceIdentity,
    pub runner: RunnerSnapshot,
    pub state_repository: StateRepositorySnapshot,
    pub failed_cycles: u8,
    pub state: IntegrationEffortState,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableEvent {
    pub sequence: u64,
    pub id: String,
    pub item_id: String,
    pub effort_id: Option<String>,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub alert: bool,
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnswerDisposition {
    Applied,
    Duplicate,
    Stale,
    Malformed,
    Unauthorized,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerCommand {
    pub external_id: String,
    pub request_id: String,
    pub effort_id: String,
    pub attempt_id: String,
    pub cycle_id: String,
    pub target_sha: String,
    pub source_sha: String,
    pub candidate_sha: Option<String>,
    pub answer: String,
}

pub struct ProviderCommentReceipt<'a> {
    pub provider: &'a str,
    pub repository: &'a str,
    pub artifact_id: &'a str,
    pub comment_id: &'a str,
    pub effort_id: &'a str,
    pub actor: Option<&'a str>,
    pub body: &'a str,
    pub disposition: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponderIdentity {
    LocalPeer { uid: u32 },
    Provider { actor: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateIntent {
    pub operation_id: String,
    pub cycle_id: String,
    pub staged_tree_sha256: String,
    pub tree_sha: String,
    pub parents: Vec<String>,
    pub author_name: String,
    pub author_email: String,
    pub author_timestamp: String,
    pub committer_name: String,
    pub committer_email: String,
    pub committer_timestamp: String,
    pub message: String,
    pub operation_ref: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateObservation {
    pub operation_id: String,
    pub candidate_sha: String,
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

impl CandidateObservation {
    pub fn read(repository: &Path, candidate_sha: &str, operation_ref: &str) -> Result<Self> {
        crate::control_domain::require_sha(candidate_sha, "candidate SHA")?;
        crate::control_domain::require_exact_text(operation_ref, "candidate operation ref")?;
        let reference = git_text(repository, ["rev-parse", "--verify", operation_ref])?;
        if reference != candidate_sha {
            anyhow::bail!("candidate operation ref does not name the observed candidate");
        }
        let tree_sha = git_text(repository, ["show", "-s", "--format=%T", candidate_sha])?;
        let parent_shas = git_text(repository, ["show", "-s", "--format=%P", candidate_sha])?
            .split_whitespace()
            .map(str::to_string)
            .collect();
        let metadata = git_text(
            repository,
            [
                "show",
                "-s",
                "--format=%an%x00%ae%x00%at%x00%cn%x00%ce%x00%ct%x00%B",
                candidate_sha,
            ],
        )?;
        let fields = metadata.splitn(7, '\0').collect::<Vec<_>>();
        if fields.len() != 7 {
            anyhow::bail!("candidate metadata observation is incomplete");
        }
        let marker = "IQ-Builder-Operation: ";
        let operation_id = fields[6]
            .lines()
            .find_map(|line| line.strip_prefix(marker))
            .context("candidate message has no builder operation marker")?
            .to_string();
        Ok(Self {
            operation_id,
            candidate_sha: candidate_sha.to_string(),
            tree_sha,
            parent_shas,
            author_name: fields[0].to_string(),
            author_email: fields[1].to_string(),
            author_timestamp: fields[2].to_string(),
            committer_name: fields[3].to_string(),
            committer_email: fields[4].to_string(),
            committer_timestamp: fields[5].to_string(),
            message: fields[6].trim_end().to_string(),
            operation_ref: operation_ref.to_string(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRepositoryArtifact {
    pub provider: String,
    pub repository: String,
    pub artifact_id: String,
    pub artifact_url: String,
    pub projection_revision: u64,
    pub last_event_sequence: u64,
    pub state: String,
}

pub struct RepositoryProjectionReceipt<'a> {
    pub effort_id: &'a str,
    pub provider: &'a str,
    pub repository: &'a str,
    pub artifact_id: &'a str,
    pub artifact_url: &'a str,
    pub last_event_sequence: u64,
    pub closed: bool,
}

pub struct NewEffort<'a> {
    pub item_id: &'a str,
    pub attempt_id: &'a str,
    pub target_sha: &'a str,
    pub source_sha: &'a str,
    pub source_variant: &'a str,
    pub landing_variant: &'a str,
    pub workspace: &'a WorkspaceIdentity,
    pub runner: &'a RunnerSnapshot,
    pub state_repository: &'a StateRepositorySnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", content = "payload", rename_all = "snake_case")]
enum RunnerTerminationAuthority {
    Launching(AgentLaunching),
    Running(crate::control_domain::AgentRunning),
}

#[derive(Clone, Debug)]
struct RunnerTerminationDebt {
    effort_id: String,
    authority: RunnerTerminationAuthority,
    runner: RunnerSnapshot,
}

#[derive(Clone)]
pub struct ControlStore {
    path: PathBuf,
}

impl ControlStore {
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.canonicalize()?;
        let store = Self { path };
        store.validate_schema()?;
        Ok(store)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub fn effort_for_item(&self, item_id: &str) -> Result<Option<IntegrationEffort>> {
        let connection = self.connect(false)?;
        connection
            .query_row(
                "SELECT id,item_id,attempt_id,target_sha,source_sha,source_variant,landing_variant,workspace_json,runner_snapshot_json,state_repository_json,failed_cycles,state_json,created_at,updated_at FROM integration_efforts WHERE item_id=?1",
                params![item_id],
                map_effort,
            )
            .optional()
            .context("read integration effort")
    }

    pub fn agent_evidence(
        &self,
        effort_id: &str,
        max_entries: u32,
    ) -> Result<(
        Vec<crate::agent_protocol::PriorOutcome>,
        Vec<crate::agent_protocol::BoundedEvidence>,
    )> {
        let connection = self.connect(false)?;
        required_effort(&connection, effort_id)?;
        let mut prior = Vec::new();
        let mut evidence = Vec::new();
        let mut cycles = connection.prepare(
            "SELECT id,status,failure_json FROM integration_cycles WHERE effort_id=?1 AND status IN ('failed','guidance_required','superseded') ORDER BY cycle_number",
        )?;
        let rows = cycles.query_map(params![effort_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            let (cycle_id, status, failure) = row?;
            let outcome_evidence = failure.unwrap_or_else(|| status.clone());
            if let Ok(crate::control_domain::CycleFailure::CandidateDefect {
                evidence: validation,
            }) = serde_json::from_str(&outcome_evidence)
            {
                evidence.push(crate::agent_protocol::BoundedEvidence {
                    kind: format!("candidate_defect:{cycle_id}"),
                    text: validation,
                });
            }
            prior.push(crate::agent_protocol::PriorOutcome {
                cycle_id,
                kind: status,
                evidence: outcome_evidence,
            });
        }
        let mut answers = connection.prepare(
            "SELECT request_id,answer FROM answer_receipts WHERE effort_id=?1 AND disposition='applied' ORDER BY created_at,external_id",
        )?;
        for row in answers.query_map(params![effort_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })? {
            let (request_id, answer) = row?;
            evidence.push(crate::agent_protocol::BoundedEvidence {
                kind: format!("guidance_answer:{request_id}"),
                text: answer,
            });
        }
        let maximum = usize::try_from(max_entries)?;
        if prior.len().saturating_add(evidence.len()) > maximum {
            anyhow::bail!("durable agent evidence exceeds protocol entry limit");
        }
        Ok((prior, evidence))
    }

    pub fn create_effort(&self, new: NewEffort<'_>) -> Result<IntegrationEffort> {
        crate::control_domain::require_sha(new.target_sha, "effort target SHA")?;
        crate::control_domain::require_sha(new.source_sha, "effort source SHA")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT id,item_id,attempt_id,target_sha,source_sha,source_variant,landing_variant,workspace_json,runner_snapshot_json,state_repository_json,failed_cycles,state_json,created_at,updated_at FROM integration_efforts WHERE item_id=?1",
                params![new.item_id],
                map_effort,
            )
            .optional()?
        {
            if existing.attempt_id != new.attempt_id
                || existing.target_sha != new.target_sha
                || existing.source_sha != new.source_sha
                || existing.workspace != *new.workspace
                || existing.runner != *new.runner
                || existing.state_repository != *new.state_repository
            {
                anyhow::bail!("existing integration effort identity differs from composition");
            }
            transaction.commit()?;
            return Ok(existing);
        }
        let id = Uuid::new_v4().to_string();
        let timestamp = now();
        let state = IntegrationEffortState::AgentReady(AgentReady { next_cycle: 1 });
        transaction.execute(
            "INSERT INTO integration_efforts(id,item_id,attempt_id,target_sha,source_sha,source_variant,landing_variant,workspace_json,runner_snapshot_json,state_repository_json,failed_cycles,state,state_json,blocker_kind,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,'agent_ready',?11,NULL,?12,?12)",
            params![id,new.item_id,new.attempt_id,new.target_sha,new.source_sha,new.source_variant,new.landing_variant,serde_json::to_string(new.workspace)?,serde_json::to_string(new.runner)?,serde_json::to_string(new.state_repository)?,serde_json::to_string(&state)?,timestamp],
        )?;
        let effort = required_effort(&transaction, &id)?;
        transaction.execute(
            "INSERT INTO state_repository_artifacts(effort_id,provider,repository,artifact_id,artifact_url,projection_revision,last_event_sequence,state,created_at,updated_at) SELECT ?1,provider,repository,artifact_id,artifact_url,0,0,'reserved',created_at,?2 FROM item_state_repository_reservations WHERE item_id=?3",
            params![id,timestamp,new.item_id],
        )?;
        transaction.execute(
            "DELETE FROM item_state_repository_reservations WHERE item_id=?1",
            params![new.item_id],
        )?;
        append_event(
            &transaction,
            &effort,
            "agent_ready",
            serde_json::json!({"cycle":1}),
            false,
        )?;
        transaction.commit()?;
        self.effort_by_id(&id)
    }

    pub fn prepare_cycle_launch(&self, effort_id: &str, launch: &AgentLaunching) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::AgentReady(ready) = &effort.state else {
            anyhow::bail!("cycle start requires agent_ready effort");
        };
        if ready.next_cycle != launch.cycle_number || effort.failed_cycles >= AUTOMATIC_CYCLE_LIMIT
        {
            anyhow::bail!("cycle number does not match effort authority");
        }
        transaction.execute(
            "INSERT INTO integration_cycles(id,effort_id,cycle_number,status,process_json,input_digest,result_state_json,created_at) VALUES(?1,?2,?3,'starting',?4,?5,?6,?7)",
            params![launch.cycle_id,effort_id,launch.cycle_number,serde_json::to_string(launch)?,launch.input_sha256,serde_json::to_string(&crate::control_domain::AtomicResultState::Absent)?,launch.prepared_at],
        )?;
        let state = IntegrationEffortState::AgentLaunching(launch.clone());
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "agent_launching",
            serde_json::json!({"cycle_id":launch.cycle_id,"operation_id":launch.launch_operation_id,"unit_name":launch.unit_name}),
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_cycle_started(
        &self,
        effort_id: &str,
        running: &crate::control_domain::AgentRunning,
    ) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::AgentLaunching(launch) = &effort.state else {
            anyhow::bail!("cycle start requires agent_launching effort");
        };
        if launch.launch_operation_id != running.launch_operation_id
            || launch.unit_name != running.unit_name
            || launch.cycle_id != running.cycle_id
            || launch.cycle_number != running.cycle_number
            || launch.authority_lease_id != running.authority_lease_id
            || launch.input_sha256 != running.input_sha256
        {
            anyhow::bail!("started process differs from prepared launch authority");
        }
        let changed = transaction.execute(
            "UPDATE integration_cycles SET status='running',process_json=?1 WHERE id=?2 AND effort_id=?3 AND status='starting'",
            params![serde_json::to_string(running)?,running.cycle_id,effort_id],
        )?;
        if changed != 1 {
            anyhow::bail!("prepared cycle authority changed before process start");
        }
        let state = IntegrationEffortState::AgentRunning(running.clone());
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "agent_running",
            serde_json::json!({"cycle_id":running.cycle_id,"operation_id":running.launch_operation_id,"unit_name":running.unit_name}),
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn reset_prepared_launch(&self, effort_id: &str, cycle_id: &str) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::AgentLaunching(launch) = &effort.state else {
            anyhow::bail!("launch reset requires agent_launching effort");
        };
        if launch.cycle_id != cycle_id {
            anyhow::bail!("launch reset differs from prepared cycle");
        }
        let changed = transaction.execute(
            "DELETE FROM integration_cycles WHERE id=?1 AND effort_id=?2 AND status='starting'",
            params![cycle_id, effort_id],
        )?;
        if changed != 1 {
            anyhow::bail!("prepared cycle authority changed before reset");
        }
        let state = IntegrationEffortState::AgentReady(AgentReady {
            next_cycle: launch.cycle_number,
        });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "agent_launch_reset",
            serde_json::json!({"cycle_id":cycle_id,"operation_id":launch.launch_operation_id}),
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_result_state(
        &self,
        effort_id: &str,
        cycle_id: &str,
        result: &crate::control_domain::AtomicResultState,
    ) -> Result<()> {
        if matches!(result, crate::control_domain::AtomicResultState::Absent) {
            anyhow::bail!("result state recording requires writing or complete identity");
        }
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::AgentRunning(running) = &effort.state else {
            anyhow::bail!("result recording requires agent_running effort");
        };
        if running.cycle_id != cycle_id {
            anyhow::bail!("result state differs from running cycle authority");
        }
        match (&running.result, result) {
            (
                crate::control_domain::AtomicResultState::Absent,
                crate::control_domain::AtomicResultState::Writing { .. },
            ) => {}
            (
                crate::control_domain::AtomicResultState::Writing {
                    device: writing_device,
                    inode: writing_inode,
                },
                crate::control_domain::AtomicResultState::Complete {
                    device: complete_device,
                    inode: complete_inode,
                    ..
                },
            ) if writing_device == complete_device && writing_inode == complete_inode => {}
            _ => anyhow::bail!("result state transition is invalid"),
        }
        let mut completed = running.clone();
        completed.result = result.clone();
        let changed = transaction.execute(
            "UPDATE integration_cycles SET process_json=?1,result_state_json=?2 WHERE id=?3 AND effort_id=?4 AND status='running'",
            params![serde_json::to_string(&completed)?,serde_json::to_string(result)?,cycle_id,effort_id],
        )?;
        if changed != 1 {
            anyhow::bail!("running cycle authority changed before result recording");
        }
        update_state(
            &transaction,
            &effort,
            &IntegrationEffortState::AgentRunning(completed),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_cycle_log(&self, effort_id: &str, cycle_id: &str, log: &[u8]) -> Result<()> {
        let connection = self.connect(true)?;
        let changed = connection.execute(
            "UPDATE integration_cycles SET log_blob=?1 WHERE id=?2 AND effort_id=?3 AND status='running'",
            params![log,cycle_id,effort_id],
        )?;
        if changed != 1 {
            anyhow::bail!("running cycle authority changed before log recording");
        }
        Ok(())
    }

    pub fn require_guidance(
        &self,
        effort_id: &str,
        guidance: crate::control_domain::SemanticGuidanceBlocker,
    ) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::AgentRunning(running) = &effort.state else {
            anyhow::bail!("guidance requires agent_running effort");
        };
        if guidance.identity.cycle_id != running.cycle_id {
            anyhow::bail!("guidance identity differs from running cycle");
        }
        let count = effort.failed_cycles + 1;
        transaction.execute(
            "UPDATE integration_cycles SET status='guidance_required',finished_at=?1 WHERE id=?2 AND status='running'",
            params![now(),running.cycle_id],
        )?;
        transaction.execute(
            "INSERT INTO guidance_requests(id,effort_id,cycle_id,request_json,status,created_at) VALUES(?1,?2,?3,?4,'open',?5)",
            params![guidance.request_id,effort_id,running.cycle_id,serde_json::to_string(&guidance)?,now()],
        )?;
        let state =
            IntegrationEffortState::GuidanceRequired(crate::control_domain::BlockedEffort {
                blocker: IntegrationBlocker::SemanticGuidance(Box::new(guidance)),
                resume: crate::control_domain::ResumeState::AgentReady(AgentReady {
                    next_cycle: next_cycle_number(&transaction, effort_id)?,
                }),
            });
        update_state_and_count(&transaction, &effort, &state, count)?;
        append_event(
            &transaction,
            &effort,
            "guidance_required",
            serde_json::to_value(&state)?,
            true,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn block_infrastructure(
        &self,
        effort_id: &str,
        blocker: crate::control_domain::InfrastructureBlocker,
    ) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let resume = ResumeState::capture(&effort.state)?;
        let state =
            IntegrationEffortState::InfrastructureBlocked(crate::control_domain::BlockedEffort {
                blocker: IntegrationBlocker::Infrastructure(blocker),
                resume,
            });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "infrastructure_blocked",
            serde_json::to_value(&state)?,
            true,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn inbox(&self, limit: u32) -> Result<Vec<IntegrationEffort>> {
        if !(1..=1000).contains(&limit) {
            anyhow::bail!("inbox limit must be from 1 through 1000");
        }
        let connection = self.connect(false)?;
        let mut statement = connection.prepare(
            "SELECT id,item_id,attempt_id,target_sha,source_sha,source_variant,landing_variant,workspace_json,runner_snapshot_json,state_repository_json,failed_cycles,state_json,created_at,updated_at FROM integration_efforts WHERE state NOT IN ('integrated','cancelled') ORDER BY created_at,id LIMIT ?1",
        )?;
        let rows = statement
            .query_map(params![limit], map_effort)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn projection_items(&self, limit: u32) -> Result<Vec<String>> {
        if !(1..=1000).contains(&limit) {
            anyhow::bail!("projection item limit must be from 1 through 1000");
        }
        let connection = self.connect(false)?;
        let mut statement = connection.prepare(
            "SELECT effort.item_id
             FROM integration_efforts effort
             LEFT JOIN state_repository_artifacts artifact ON artifact.effort_id=effort.id
             LEFT JOIN projection_debt debt ON debt.effort_id=effort.id
             WHERE json_extract(effort.state_repository_json,'$.kind')!='local'
               AND (
                 (debt.effort_id IS NULL AND (
                   (artifact.effort_id IS NULL AND (
                     json_extract(effort.state_repository_json,'$.visibility')='full'
                     OR effort.blocker_kind IS NOT NULL
                   ))
                   OR (
                     artifact.effort_id IS NOT NULL
                     AND EXISTS(
                       SELECT 1 FROM durable_events event
                       WHERE event.effort_id=effort.id
                         AND event.sequence>artifact.last_event_sequence
                         AND (
                           json_extract(effort.state_repository_json,'$.visibility')='full'
                           OR event.alert=1
                           OR event.event_type IN (
                             'answer_applied',
                             'cycle_limit_retry_authorized',
                             'infrastructure_retry_authorized',
                             'provider_retry_authorized',
                             'provider_reconciliation_resumed'
                           )
                           OR effort.state IN ('integrated','cancelled')
                         )
                     )
                   )
                   OR (effort.state IN ('integrated','cancelled') AND artifact.state!='closed')
                 ))
                 OR (debt.effort_id IS NOT NULL AND debt.attempt_count<10 AND debt.next_attempt_at<=?1)
               )
             ORDER BY effort.created_at,effort.id
             LIMIT ?2",
        )?;
        let items = statement
            .query_map(params![now(), limit], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(anyhow::Error::from)?;
        Ok(items)
    }

    pub fn events_after(&self, cursor: u64, limit: u32) -> Result<Vec<DurableEvent>> {
        if !(1..=10_000).contains(&limit) {
            anyhow::bail!("event limit must be from 1 through 10000");
        }
        let connection = self.connect(false)?;
        let oldest: Option<u64> =
            connection.query_row("SELECT MIN(sequence) FROM durable_events", [], |row| {
                row.get(0)
            })?;
        if let Some(oldest) = oldest {
            if cursor != 0 && cursor + 1 < oldest {
                anyhow::bail!("cursor_expired:{oldest}");
            }
        }
        let mut statement = connection.prepare(
            "SELECT sequence,id,item_id,effort_id,event_type,payload_json,alert,created_at FROM durable_events WHERE sequence>?1 ORDER BY sequence LIMIT ?2",
        )?;
        let rows = statement
            .query_map(params![cursor, limit], |row| {
                let payload: String = row.get(5)?;
                Ok(DurableEvent {
                    sequence: row.get(0)?,
                    id: row.get(1)?,
                    item_id: row.get(2)?,
                    effort_id: row.get(3)?,
                    event_type: row.get(4)?,
                    payload: serde_json::from_str(&payload).map_err(json_error)?,
                    alert: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn effort_events_after(
        &self,
        effort_id: &str,
        cursor: u64,
        limit: u32,
    ) -> Result<Vec<DurableEvent>> {
        if !(1..=10_000).contains(&limit) {
            anyhow::bail!("effort event limit must be from 1 through 10000");
        }
        let connection = self.connect(false)?;
        let mut statement = connection.prepare(
            "SELECT sequence,id,item_id,effort_id,event_type,payload_json,alert,created_at FROM durable_events WHERE effort_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3",
        )?;
        let rows = statement
            .query_map(params![effort_id, cursor, limit], |row| {
                let payload: String = row.get(5)?;
                Ok(DurableEvent {
                    sequence: row.get(0)?,
                    id: row.get(1)?,
                    item_id: row.get(2)?,
                    effort_id: row.get(3)?,
                    event_type: row.get(4)?,
                    payload: serde_json::from_str(&payload).map_err(json_error)?,
                    alert: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn oldest_event_sequence(&self) -> Result<Option<u64>> {
        let connection = self.connect(false)?;
        connection
            .query_row("SELECT MIN(sequence) FROM durable_events", [], |row| {
                row.get(0)
            })
            .map_err(Into::into)
    }

    pub fn repository_artifact(&self, effort_id: &str) -> Result<Option<StoredRepositoryArtifact>> {
        let connection = self.connect(false)?;
        connection
            .query_row(
                "SELECT provider,repository,artifact_id,artifact_url,projection_revision,last_event_sequence,state FROM state_repository_artifacts WHERE effort_id=?1",
                params![effort_id],
                |row| Ok(StoredRepositoryArtifact {
                    provider: row.get(0)?,
                    repository: row.get(1)?,
                    artifact_id: row.get(2)?,
                    artifact_url: row.get(3)?,
                    projection_revision: row.get(4)?,
                    last_event_sequence: row.get(5)?,
                    state: row.get(6)?,
                }),
            )
            .optional()
            .context("read state-repository artifact")
    }

    pub fn record_item_repository_reservation(
        &self,
        item_id: &str,
        provider: &str,
        repository: &str,
        artifact_id: &str,
        artifact_url: &str,
    ) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let expected: (String, String, String) = transaction.query_row(
            "SELECT provider,repository,reservation_state FROM item_state_repository_bindings WHERE item_id=?1",
            params![item_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        )?;
        if expected.0 != provider || expected.1 != repository || expected.2 != "pending" {
            anyhow::bail!("issue reservation result differs from immutable enqueue intent");
        }
        transaction.execute(
            "INSERT INTO item_state_repository_reservations(item_id,provider,repository,artifact_id,artifact_url,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![item_id,provider,repository,artifact_id,artifact_url,now()],
        )?;
        transaction.execute(
            "UPDATE item_state_repository_bindings SET reservation_state='reserved' WHERE item_id=?1 AND reservation_state='pending'",
            params![item_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn item_repository_reservation(
        &self,
        item_id: &str,
    ) -> Result<Option<StoredRepositoryArtifact>> {
        let connection = self.connect(false)?;
        connection
            .query_row(
                "SELECT provider,repository,artifact_id,artifact_url,0,0,'reserved' FROM item_state_repository_reservations WHERE item_id=?1",
                params![item_id],
                |row| Ok(StoredRepositoryArtifact {
                    provider: row.get(0)?,
                    repository: row.get(1)?,
                    artifact_id: row.get(2)?,
                    artifact_url: row.get(3)?,
                    projection_revision: row.get(4)?,
                    last_event_sequence: row.get(5)?,
                    state: row.get(6)?,
                }),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn item_state_repository_binding(&self, item_id: &str) -> Result<StateRepositorySnapshot> {
        let connection = self.connect(false)?;
        let raw: String = connection.query_row(
            "SELECT snapshot_json FROM item_state_repository_bindings WHERE item_id=?1",
            params![item_id],
            |row| row.get(0),
        )?;
        serde_json::from_str::<StateRepositorySnapshot>(&raw)?.validate()
    }

    pub fn pending_issue_reservations(&self, limit: u32) -> Result<Vec<String>> {
        if !(1..=1000).contains(&limit) {
            anyhow::bail!("reservation work limit must be from 1 through 1000");
        }
        let connection = self.connect(false)?;
        let mut statement = connection.prepare(
            "SELECT item_id FROM item_state_repository_bindings WHERE visibility='full' AND reservation_state='pending' ORDER BY created_at,item_id LIMIT ?1",
        )?;
        let items = statement
            .query_map([limit], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(items)
    }

    pub fn record_repository_projection(
        &self,
        receipt: RepositoryProjectionReceipt<'_>,
    ) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "INSERT INTO state_repository_artifacts(effort_id,provider,repository,artifact_id,artifact_url,projection_revision,last_event_sequence,state,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,1,?6,?7,?8,?8) ON CONFLICT(effort_id) DO UPDATE SET artifact_id=excluded.artifact_id,artifact_url=excluded.artifact_url,projection_revision=state_repository_artifacts.projection_revision+1,last_event_sequence=excluded.last_event_sequence,state=excluded.state,updated_at=excluded.updated_at WHERE state_repository_artifacts.provider=excluded.provider AND state_repository_artifacts.repository=excluded.repository AND state_repository_artifacts.artifact_id=excluded.artifact_id",
            params![receipt.effort_id,receipt.provider,receipt.repository,receipt.artifact_id,receipt.artifact_url,receipt.last_event_sequence,if receipt.closed {"closed"} else {"active"},now()],
        )?;
        if changed != 1 {
            anyhow::bail!("projection receipt differs from immutable repository artifact identity");
        }
        transaction.execute(
            "DELETE FROM projection_debt WHERE effort_id=?1",
            params![receipt.effort_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_projection_debt(&self, effort_id: &str, error: &anyhow::Error) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let timestamp = now();
        let prior_attempts: u32 = transaction
            .query_row(
                "SELECT attempt_count FROM projection_debt WHERE effort_id=?1",
                params![effort_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let attempt_count = prior_attempts.saturating_add(1).min(10);
        let backoff_seconds = 30_i64
            .saturating_mul(1_i64 << attempt_count.saturating_sub(1).min(7))
            .min(3600);
        transaction.execute(
            "INSERT INTO projection_debt(effort_id,attempt_count,next_attempt_at,last_error_json,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?5) ON CONFLICT(effort_id) DO UPDATE SET attempt_count=excluded.attempt_count,next_attempt_at=excluded.next_attempt_at,last_error_json=excluded.last_error_json,updated_at=excluded.updated_at",
            params![effort_id,attempt_count,(Utc::now()+chrono::Duration::seconds(backoff_seconds)).to_rfc3339(),serde_json::json!({"kind":"projection","detail":format!("{error:#}")}).to_string(),timestamp],
        )?;
        let effort = required_effort(&transaction, effort_id)?;
        append_event_raw(
            &transaction,
            &effort.item_id,
            Some(effort_id),
            "projection_debt",
            serde_json::json!({"attempt_count":attempt_count,"next_attempt_seconds":backoff_seconds,"detail":format!("{error:#}")}),
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn alert_exhausted_projection_debt(&self, age_seconds: u64) -> Result<usize> {
        if age_seconds == 0 {
            anyhow::bail!("projection debt alert age must be non-zero");
        }
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cutoff =
            (Utc::now() - chrono::Duration::seconds(i64::try_from(age_seconds)?)).to_rfc3339();
        let rows = {
            let mut statement = transaction.prepare(
                "SELECT debt.effort_id,effort.item_id,debt.last_error_json FROM projection_debt debt JOIN integration_efforts effort ON effort.id=debt.effort_id LEFT JOIN projection_debt_alerts alert ON alert.effort_id=debt.effort_id WHERE debt.attempt_count>=10 AND debt.created_at<=?1 AND alert.effort_id IS NULL ORDER BY debt.created_at,debt.effort_id LIMIT 1000",
            )?;
            let rows = statement
                .query_map([cutoff], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };
        for (effort_id, item_id, error_json) in &rows {
            let event_id = Uuid::new_v4().to_string();
            transaction.execute(
                "INSERT INTO durable_events(id,item_id,effort_id,event_type,payload_json,alert,created_at) VALUES(?1,?2,?3,'projection_debt_exhausted',json_object('error',json(?4)),1,?5)",
                params![event_id,item_id,effort_id,error_json,now()],
            )?;
            transaction.execute(
                "INSERT INTO projection_debt_alerts(effort_id,event_id,created_at) VALUES(?1,?2,?3)",
                params![effort_id,event_id,now()],
            )?;
            transaction.execute(
                "INSERT OR IGNORE INTO notification_deliveries(event_id,backend,state,attempt_count,next_attempt_at,created_at,updated_at) SELECT ?1,backend,'pending',0,?2,?2,?2 FROM notification_backends WHERE enabled=1",
                params![event_id,now()],
            )?;
        }
        transaction.commit()?;
        Ok(rows.len())
    }

    pub fn provider_comment_seen(
        &self,
        provider: &str,
        repository: &str,
        artifact_id: &str,
        comment_id: &str,
    ) -> Result<bool> {
        let connection = self.connect(false)?;
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM answer_receipts WHERE external_id=?1 UNION SELECT 1 FROM provider_comment_receipts WHERE provider=?2 AND repository=?3 AND artifact_id=?4 AND comment_id=?5)",
                params![provider_comment_key(provider,repository,artifact_id,comment_id)?,provider,repository,artifact_id,comment_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn record_provider_comment_receipt(
        &self,
        receipt: &ProviderCommentReceipt<'_>,
    ) -> Result<()> {
        validate_provider_comment_identity(
            receipt.provider,
            receipt.repository,
            receipt.artifact_id,
            receipt.comment_id,
        )?;
        let connection = self.connect(true)?;
        connection.execute(
            "INSERT OR IGNORE INTO provider_comment_receipts(provider,repository,artifact_id,comment_id,effort_id,actor,body,disposition,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![receipt.provider,receipt.repository,receipt.artifact_id,receipt.comment_id,receipt.effort_id,receipt.actor,receipt.body,receipt.disposition,now()],
        )?;
        Ok(())
    }

    pub fn start_candidate_build(&self, effort_id: &str, intent: &CandidateIntent) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::AgentRunning(running) = &effort.state else {
            anyhow::bail!("candidate build requires agent_running effort");
        };
        if running.cycle_id != intent.cycle_id || intent.parents.is_empty() {
            anyhow::bail!("candidate intent does not match accepted cycle");
        }
        let state =
            IntegrationEffortState::CandidateBuilding(crate::control_domain::CandidateBuilding {
                operation_id: intent.operation_id.clone(),
                cycle_id: intent.cycle_id.clone(),
                staged_tree_sha256: intent.staged_tree_sha256.clone(),
                tree_sha: intent.tree_sha.clone(),
                parent_shas: intent.parents.clone(),
                author_name: intent.author_name.clone(),
                author_email: intent.author_email.clone(),
                author_timestamp: intent.author_timestamp.clone(),
                committer_name: intent.committer_name.clone(),
                committer_email: intent.committer_email.clone(),
                committer_timestamp: intent.committer_timestamp.clone(),
                message: intent.message.clone(),
                operation_ref: intent.operation_ref.clone(),
            });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "candidate_building",
            serde_json::to_value(intent)?,
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn accept_resolved_cycle(&self, effort_id: &str, intent: &CandidateIntent) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::AgentRunning(running) = &effort.state else {
            anyhow::bail!("resolved result requires agent_running effort");
        };
        if running.cycle_id != intent.cycle_id || intent.parents.is_empty() {
            anyhow::bail!("resolved result does not match running cycle");
        }
        let changed = transaction.execute(
            "UPDATE integration_cycles SET status='resolved',finished_at=?1 WHERE id=?2 AND status='running'",
            params![now(),running.cycle_id],
        )?;
        if changed != 1 {
            anyhow::bail!("running cycle authority changed before resolved result acceptance");
        }
        let state =
            IntegrationEffortState::CandidateBuilding(crate::control_domain::CandidateBuilding {
                operation_id: intent.operation_id.clone(),
                cycle_id: intent.cycle_id.clone(),
                staged_tree_sha256: intent.staged_tree_sha256.clone(),
                tree_sha: intent.tree_sha.clone(),
                parent_shas: intent.parents.clone(),
                author_name: intent.author_name.clone(),
                author_email: intent.author_email.clone(),
                author_timestamp: intent.author_timestamp.clone(),
                committer_name: intent.committer_name.clone(),
                committer_email: intent.committer_email.clone(),
                committer_timestamp: intent.committer_timestamp.clone(),
                message: intent.message.clone(),
                operation_ref: intent.operation_ref.clone(),
            });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "candidate_building",
            serde_json::to_value(intent)?,
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_candidate(
        &self,
        effort_id: &str,
        observation: &CandidateObservation,
    ) -> Result<()> {
        crate::control_domain::require_sha(&observation.candidate_sha, "candidate SHA")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::CandidateBuilding(building) = &effort.state else {
            anyhow::bail!("candidate record requires candidate_building effort");
        };
        if building.operation_id != observation.operation_id
            || building.tree_sha != observation.tree_sha
            || building.parent_shas != observation.parent_shas
            || building.author_name != observation.author_name
            || building.author_email != observation.author_email
            || building.author_timestamp != observation.author_timestamp
            || building.committer_name != observation.committer_name
            || building.committer_email != observation.committer_email
            || building.committer_timestamp != observation.committer_timestamp
            || building.message != observation.message
            || building.operation_ref != observation.operation_ref
        {
            anyhow::bail!("complete candidate Git observation differs from durable builder intent");
        }
        let state = IntegrationEffortState::CandidateReady(crate::control_domain::CandidateReady {
            operation_id: observation.operation_id.clone(),
            cycle_id: building.cycle_id.clone(),
            candidate_sha: observation.candidate_sha.clone(),
            staged_tree_sha256: building.staged_tree_sha256.clone(),
        });
        transaction.execute(
            "INSERT INTO candidate_evidence(effort_id,cycle_id,candidate_sha,builder_operation_id,created_at) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(effort_id) DO UPDATE SET cycle_id=excluded.cycle_id,candidate_sha=excluded.candidate_sha,builder_operation_id=excluded.builder_operation_id,created_at=excluded.created_at",
            params![effort_id,building.cycle_id,observation.candidate_sha,observation.operation_id,now()],
        )?;
        let projected = transaction.execute(
            "UPDATE integration_attempts SET merge_commit_sha=?1,validated_commit_sha=NULL,validation_command=NULL,validation_exit_code=NULL,validation_log_path=NULL,signoff_evidence_json=NULL WHERE id=?2 AND item_id=?3",
            params![observation.candidate_sha,effort.attempt_id,effort.item_id],
        )?;
        if projected != 1 {
            anyhow::bail!("candidate attempt projection lost exact effort authority");
        }
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "candidate_ready",
            serde_json::json!({"candidate_sha":observation.candidate_sha}),
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn reset_interrupted_candidate_build(&self, effort_id: &str) -> Result<IntegrationEffort> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        if !matches!(effort.state, IntegrationEffortState::CandidateBuilding(_)) {
            anyhow::bail!("candidate reset requires candidate_building effort");
        }
        let state = IntegrationEffortState::AgentReady(AgentReady {
            next_cycle: next_cycle_number(&transaction, effort_id)?,
        });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "candidate_build_interrupted",
            serde_json::json!({"resume":"agent_ready"}),
            false,
        )?;
        transaction.commit()?;
        self.effort_by_id(effort_id)
    }

    pub fn start_validation(&self, effort_id: &str, policy_digest: &str) -> Result<()> {
        crate::control_domain::require_exact_text(policy_digest, "validation policy digest")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::CandidateReady(candidate) = &effort.state else {
            anyhow::bail!("validation requires candidate_ready effort");
        };
        let state = IntegrationEffortState::Validating(crate::control_domain::Validating {
            candidate_sha: candidate.candidate_sha.clone(),
            policy_digest: policy_digest.to_string(),
            stage: crate::control_domain::ValidationStage::Running,
        });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "validating",
            serde_json::json!({"candidate_sha":candidate.candidate_sha,"policy_digest":policy_digest}),
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_validation(&self, effort_id: &str, candidate_sha: &str) -> Result<()> {
        crate::control_domain::require_sha(candidate_sha, "validated candidate SHA")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::Validating(validating) = &effort.state else {
            anyhow::bail!("validation completion requires validating effort");
        };
        if validating.stage != crate::control_domain::ValidationStage::Running
            || validating.candidate_sha != candidate_sha
        {
            anyhow::bail!("validation completion differs from current candidate authority");
        }
        let state = IntegrationEffortState::Validating(crate::control_domain::Validating {
            candidate_sha: candidate_sha.to_string(),
            policy_digest: validating.policy_digest.clone(),
            stage: crate::control_domain::ValidationStage::Gates,
        });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "validation_succeeded",
            serde_json::json!({"candidate_sha":candidate_sha}),
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn block_provider(
        &self,
        effort_id: &str,
        blocker: crate::control_domain::ProviderSignoffBlocker,
    ) -> Result<()> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let resume = ResumeState::capture(&effort.state)?;
        let state = IntegrationEffortState::ProviderBlocked(crate::control_domain::BlockedEffort {
            blocker: IntegrationBlocker::ProviderSignoff(blocker),
            resume,
        });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "provider_blocked",
            serde_json::to_value(&state)?,
            true,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn begin_landing(
        &self,
        effort_id: &str,
        expected_target_sha: &str,
        lease_id: &str,
        command_id: &str,
        signoff: crate::control_domain::SignoffDisposition,
    ) -> Result<()> {
        crate::control_domain::require_sha(expected_target_sha, "landing target SHA")?;
        crate::control_domain::require_exact_text(lease_id, "landing lease identity")?;
        crate::control_domain::require_exact_text(command_id, "landing command identity")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::Validating(validating) = &effort.state else {
            anyhow::bail!("landing requires validating effort");
        };
        if let crate::control_domain::SignoffDisposition::Evidence { candidate_sha, .. } = &signoff
        {
            if candidate_sha != &validating.candidate_sha {
                anyhow::bail!("signoff evidence names a different candidate");
            }
        }
        let landing = crate::control_domain::Landing {
            candidate_sha: validating.candidate_sha.clone(),
            expected_target_sha: expected_target_sha.to_string(),
            lease_id: lease_id.to_string(),
            signoff,
        };
        let landing_state = IntegrationEffortState::Landing(landing.clone());
        update_state(&transaction, &effort, &landing_state)?;
        append_event(
            &transaction,
            &effort,
            "landing",
            serde_json::to_value(&landing_state)?,
            false,
        )?;
        let uncertain =
            IntegrationEffortState::LandingUncertain(crate::control_domain::LandingUncertain {
                candidate_sha: landing.candidate_sha,
                expected_target_sha: landing.expected_target_sha,
                command_id: command_id.to_string(),
                evidence: "command_authorized_before_external_mutation".into(),
            });
        let landing_effort = IntegrationEffort {
            state: landing_state,
            ..effort.clone()
        };
        update_state(&transaction, &landing_effort, &uncertain)?;
        append_event(
            &transaction,
            &landing_effort,
            "landing_uncertain",
            serde_json::to_value(&uncertain)?,
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_landing_uncertain(
        &self,
        effort_id: &str,
        command_id: &str,
        evidence: &str,
    ) -> Result<()> {
        crate::control_domain::require_exact_text(command_id, "landing command identity")?;
        crate::control_domain::require_exact_text(evidence, "landing reconciliation evidence")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::Landing(landing) = &effort.state else {
            anyhow::bail!("landing uncertainty requires landing effort");
        };
        let state =
            IntegrationEffortState::LandingUncertain(crate::control_domain::LandingUncertain {
                candidate_sha: landing.candidate_sha.clone(),
                expected_target_sha: landing.expected_target_sha.clone(),
                command_id: command_id.to_string(),
                evidence: evidence.to_string(),
            });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "landing_uncertain",
            serde_json::to_value(&state)?,
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_integrated(
        &self,
        effort_id: &str,
        landed_sha: &str,
        remote_target_sha: &str,
    ) -> Result<()> {
        crate::control_domain::require_sha(landed_sha, "landed SHA")?;
        crate::control_domain::require_sha(remote_target_sha, "remote target SHA")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let candidate_sha = match &effort.state {
            IntegrationEffortState::Landing(landing) => &landing.candidate_sha,
            IntegrationEffortState::LandingUncertain(landing) => &landing.candidate_sha,
            _ => anyhow::bail!("integration completion requires landing authority"),
        };
        let event_id = Uuid::new_v4().to_string();
        let state = IntegrationEffortState::Integrated(crate::control_domain::Integrated {
            candidate_sha: candidate_sha.clone(),
            landed_sha: landed_sha.to_string(),
            attempt_id: effort.attempt_id.clone(),
            event_id: event_id.clone(),
        });
        transaction.execute(
            "UPDATE integration_attempts SET landed_commit_sha=?1,result='integrated',finished_at=?2 WHERE id=?3 AND item_id=?4",
            params![landed_sha,now(),effort.attempt_id,effort.item_id],
        )?;
        update_state(&transaction, &effort, &state)?;
        let submission: Option<(String, String)> = transaction
            .query_row(
                "SELECT submission.id,submission.workspace_id FROM queue_items item JOIN local_submissions submission ON submission.id=item.submission_id WHERE item.id=?1 AND item.source_kind='local_submission'",
                params![effort.item_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((submission_id, workspace_id)) = submission {
            let submission_changed = transaction.execute(
                "UPDATE local_submissions SET state='integrated' WHERE id=?1 AND state='queued'",
                params![submission_id],
            )?;
            let workspace_changed = transaction.execute(
                "UPDATE development_workspaces SET status='cleanup_pending',cleanup_json=json_object('state','pending'),updated_at=?1 WHERE id=?2 AND status IN ('submitted','cleanup_pending','cleanup_failed')",
                params![now(),workspace_id],
            )?;
            if submission_changed != 1 || workspace_changed != 1 {
                anyhow::bail!("local submission cannot enter integrated cleanup debt");
            }
        }
        transaction.execute(
            "UPDATE registered_repositories SET seed_refresh_json=json_object('state','pending','target_sha',?1),updated_at=?2 WHERE repo_key=(SELECT repo_key FROM queue_items WHERE id=?3)",
            params![remote_target_sha,now(),effort.item_id],
        )?;
        append_event_with_id(
            &transaction,
            &event_id,
            &effort,
            "integrated",
            serde_json::to_value(&state)?,
            false,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn consume_cycle_failure(
        &self,
        effort_id: &str,
        cycle_id: &str,
        failure: crate::control_domain::CycleFailure,
    ) -> Result<IntegrationEffort> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let count = effort
            .failed_cycles
            .checked_add(1)
            .context("cycle count overflow")?;
        let state = if count == AUTOMATIC_CYCLE_LIMIT {
            let event_id = Uuid::new_v4().to_string();
            let blocker =
                IntegrationBlocker::CycleLimit(crate::control_domain::CycleLimitBlocker {
                    count,
                    cycle_ids: cycle_ids(&transaction, effort_id, cycle_id)?,
                    last_failure: failure.clone(),
                    alert_event_id: event_id.clone(),
                });
            let state =
                IntegrationEffortState::CycleLimitBlocked(crate::control_domain::BlockedEffort {
                    blocker,
                    resume: crate::control_domain::ResumeState::AgentReady(AgentReady {
                        next_cycle: next_cycle_number(&transaction, effort_id)?,
                    }),
                });
            append_event_with_id(
                &transaction,
                &event_id,
                &effort,
                "cycle_limit",
                serde_json::to_value(&state)?,
                true,
            )?;
            state
        } else if count < AUTOMATIC_CYCLE_LIMIT {
            IntegrationEffortState::AgentReady(AgentReady {
                next_cycle: next_cycle_number(&transaction, effort_id)?,
            })
        } else {
            anyhow::bail!("automatic cycle limit is already reached");
        };
        let changed = transaction.execute(
            "UPDATE integration_cycles SET status='failed',failure_json=?1,finished_at=?2 WHERE id=?3 AND effort_id=?4 AND status='running'",
            params![serde_json::to_string(&failure)?,now(),cycle_id,effort_id],
        )?;
        if changed != 1 {
            anyhow::bail!("failed cycle does not match one running cycle authority");
        }
        update_state_and_count(&transaction, &effort, &state, count)?;
        if count < AUTOMATIC_CYCLE_LIMIT {
            append_event(
                &transaction,
                &effort,
                "cycle_failed",
                serde_json::to_value(failure)?,
                false,
            )?;
        }
        transaction.commit()?;
        self.effort_by_id(effort_id)
    }

    pub fn recompose_after_target_move(
        &self,
        effort_id: &str,
        target_sha: &str,
        conflict: &serde_json::Value,
    ) -> Result<IntegrationEffort> {
        crate::control_domain::require_sha(target_sha, "replacement target SHA")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        if !matches!(
            effort.state,
            IntegrationEffortState::CandidateReady(_)
                | IntegrationEffortState::Validating(_)
                | IntegrationEffortState::Landing(_)
                | IntegrationEffortState::LandingUncertain(_)
        ) {
            anyhow::bail!("target movement requires candidate or landing authority");
        }
        transaction.execute(
            "UPDATE integration_cycles SET status='superseded',finished_at=?1 WHERE effort_id=?2 AND status='resolved'",
            params![now(),effort_id],
        )?;
        transaction.execute(
            "DELETE FROM candidate_evidence WHERE effort_id=?1",
            params![effort_id],
        )?;
        transaction.execute(
            "UPDATE integration_attempts SET target_base_sha=?1,merge_commit_sha=NULL,validated_commit_sha=NULL,validation_command=NULL,validation_exit_code=NULL,validation_log_path=NULL,signoff_evidence_json=NULL,moved_base_json=json_object('state','none') WHERE id=?2",
            params![target_sha,effort.attempt_id],
        )?;
        transaction.execute(
            "UPDATE integration_efforts SET target_sha=?1 WHERE id=?2",
            params![target_sha, effort_id],
        )?;
        let state = IntegrationEffortState::AgentReady(AgentReady {
            next_cycle: next_cycle_number(&transaction, effort_id)?,
        });
        update_state(&transaction, &effort, &state)?;
        let projected = transaction.execute(
            "UPDATE queue_items SET target_sha=?1,source_sha=?2,conflict_json=?3,updated_at=?4 WHERE id=?5 AND current_attempt_id=?6",
            params![target_sha,effort.source_sha,conflict.to_string(),now(),effort.item_id,effort.attempt_id],
        )?;
        if projected != 1 {
            anyhow::bail!("target recomposition projection lost queue item or attempt identity");
        }
        append_event(
            &transaction,
            &effort,
            "target_moved",
            serde_json::json!({"old_target_sha":effort.target_sha,"target_sha":target_sha}),
            false,
        )?;
        transaction.commit()?;
        self.effort_by_id(effort_id)
    }

    pub fn reject_candidate(&self, effort_id: &str, evidence: &str) -> Result<IntegrationEffort> {
        crate::control_domain::require_exact_text(evidence, "candidate defect evidence")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let cycle_id = match &effort.state {
            IntegrationEffortState::CandidateReady(candidate) => candidate.cycle_id.clone(),
            IntegrationEffortState::Validating(validating) => transaction.query_row(
                "SELECT cycle_id FROM candidate_evidence WHERE effort_id=?1 AND candidate_sha=?2",
                params![effort_id, validating.candidate_sha],
                |row| row.get(0),
            )?,
            IntegrationEffortState::Landing(landing) => transaction.query_row(
                "SELECT cycle_id FROM candidate_evidence WHERE effort_id=?1 AND candidate_sha=?2",
                params![effort_id, landing.candidate_sha],
                |row| row.get(0),
            )?,
            IntegrationEffortState::LandingUncertain(landing) => transaction.query_row(
                "SELECT cycle_id FROM candidate_evidence WHERE effort_id=?1 AND candidate_sha=?2",
                params![effort_id, landing.candidate_sha],
                |row| row.get(0),
            )?,
            _ => anyhow::bail!("candidate rejection requires current candidate authority"),
        };
        let count = effort
            .failed_cycles
            .checked_add(1)
            .context("cycle count overflow")?;
        let failure = crate::control_domain::CycleFailure::CandidateDefect {
            evidence: evidence.to_string(),
        };
        let state = if count == AUTOMATIC_CYCLE_LIMIT {
            let event_id = Uuid::new_v4().to_string();
            let state =
                IntegrationEffortState::CycleLimitBlocked(crate::control_domain::BlockedEffort {
                    blocker: IntegrationBlocker::CycleLimit(
                        crate::control_domain::CycleLimitBlocker {
                            count,
                            cycle_ids: cycle_ids(&transaction, effort_id, &cycle_id)?,
                            last_failure: failure.clone(),
                            alert_event_id: event_id.clone(),
                        },
                    ),
                    resume: crate::control_domain::ResumeState::AgentReady(AgentReady {
                        next_cycle: next_cycle_number(&transaction, effort_id)?,
                    }),
                });
            append_event_with_id(
                &transaction,
                &event_id,
                &effort,
                "cycle_limit",
                serde_json::to_value(&state)?,
                true,
            )?;
            state
        } else if count < AUTOMATIC_CYCLE_LIMIT {
            IntegrationEffortState::AgentReady(AgentReady {
                next_cycle: count + 1,
            })
        } else {
            anyhow::bail!("automatic cycle limit is already reached");
        };
        let changed = transaction.execute(
            "UPDATE integration_cycles SET status='failed',failure_json=?1,finished_at=?2 WHERE id=?3 AND effort_id=?4 AND status='resolved'",
            params![serde_json::to_string(&failure)?,now(),cycle_id,effort_id],
        )?;
        if changed != 1 {
            anyhow::bail!("candidate cycle is not available for one defect classification");
        }
        transaction.execute(
            "DELETE FROM candidate_evidence WHERE effort_id=?1",
            params![effort_id],
        )?;
        transaction.execute(
            "UPDATE integration_attempts SET merge_commit_sha=NULL,validated_commit_sha=NULL,validation_command=NULL,validation_exit_code=NULL,validation_log_path=NULL,signoff_evidence_json=NULL WHERE id=?1",
            params![effort.attempt_id],
        )?;
        update_state_and_count(&transaction, &effort, &state, count)?;
        if count < AUTOMATIC_CYCLE_LIMIT {
            append_event(
                &transaction,
                &effort,
                "candidate_rejected",
                serde_json::to_value(&failure)?,
                false,
            )?;
        }
        transaction.commit()?;
        self.effort_by_id(effort_id)
    }

    pub fn answer(
        &self,
        command: &AnswerCommand,
        responder: &ResponderIdentity,
        daemon_uid: u32,
    ) -> Result<AnswerDisposition> {
        self.answer_for_effort(command, &command.effort_id, responder, daemon_uid)
    }

    pub(crate) fn answer_for_effort(
        &self,
        command: &AnswerCommand,
        authoritative_effort_id: &str,
        responder: &ResponderIdentity,
        daemon_uid: u32,
    ) -> Result<AnswerDisposition> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT disposition FROM answer_receipts WHERE external_id=?1",
                params![command.external_id],
                |row| row.get(0),
            )
            .optional()?;
        if existing.is_some() {
            transaction.commit()?;
            return Ok(AnswerDisposition::Duplicate);
        }
        let effort = required_effort(&transaction, authoritative_effort_id)?;
        let authorized = match responder {
            ResponderIdentity::LocalPeer { uid } => {
                *uid == daemon_uid
                    && matches!(effort.state_repository, StateRepositorySnapshot::Local)
            }
            ResponderIdentity::Provider { actor } => effort.state_repository.permits_actor(actor),
        };
        let disposition = if !authorized {
            AnswerDisposition::Unauthorized
        } else if command.answer.trim().is_empty() || command.answer != command.answer.trim() {
            AnswerDisposition::Malformed
        } else {
            match &effort.state {
                IntegrationEffortState::GuidanceRequired(blocked) => {
                    let IntegrationBlocker::SemanticGuidance(guidance) = &blocked.blocker else {
                        unreachable!("state validation requires semantic guidance")
                    };
                    if effort.id != command.effort_id
                        || guidance.request_id != command.request_id
                        || guidance.identity.attempt_id != command.attempt_id
                        || guidance.identity.cycle_id != command.cycle_id
                        || guidance.identity.target_sha != command.target_sha
                        || guidance.identity.source_sha != command.source_sha
                        || guidance.identity.candidate_sha != command.candidate_sha
                    {
                        AnswerDisposition::Stale
                    } else {
                        let state = IntegrationEffortState::AgentReady(AgentReady {
                            next_cycle: next_cycle_number(&transaction, &effort.id)?,
                        });
                        let changed = transaction.execute(
                            "UPDATE guidance_requests SET status='answered',answered_at=?1 WHERE id=?2 AND effort_id=?3 AND status='open'",
                            params![now(),command.request_id,effort.id],
                        )?;
                        if changed != 1 {
                            anyhow::bail!("open guidance request authority changed before answer");
                        }
                        update_state(&transaction, &effort, &state)?;
                        append_event(
                            &transaction,
                            &effort,
                            "answer_applied",
                            serde_json::json!({"request_id":command.request_id}),
                            false,
                        )?;
                        AnswerDisposition::Applied
                    }
                }
                _ => AnswerDisposition::Stale,
            }
        };
        transaction.execute(
            "INSERT INTO answer_receipts(external_id,effort_id,request_id,responder_json,answer,disposition,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![command.external_id, effort.id, command.request_id, serde_json::to_string(responder)?, command.answer, answer_disposition(disposition), now()],
        )?;
        transaction.commit()?;
        Ok(disposition)
    }

    pub fn retry_blocked(
        &self,
        effort_id: &str,
        responder: &ResponderIdentity,
        daemon_uid: u32,
    ) -> Result<IntegrationEffort> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let authorized =
            matches!(responder, ResponderIdentity::LocalPeer { uid } if *uid == daemon_uid);
        if !authorized {
            anyhow::bail!("explicit blocker retry is not authorized");
        }
        let (state, failed_cycles, event_type) = match &effort.state {
            IntegrationEffortState::CycleLimitBlocked(_) => (
                IntegrationEffortState::AgentReady(AgentReady {
                    next_cycle: next_cycle_number(&transaction, effort_id)?,
                }),
                0,
                "cycle_limit_retry_authorized",
            ),
            IntegrationEffortState::InfrastructureBlocked(blocked) => (
                blocked.resume.restore(),
                effort.failed_cycles,
                "infrastructure_retry_authorized",
            ),
            IntegrationEffortState::ProviderBlocked(blocked) => (
                blocked.resume.restore(),
                effort.failed_cycles,
                "provider_retry_authorized",
            ),
            _ => anyhow::bail!("effort is not explicitly retryable"),
        };
        update_state_and_count(&transaction, &effort, &state, failed_cycles)?;
        append_event(
            &transaction,
            &effort,
            event_type,
            serde_json::to_value(responder)?,
            false,
        )?;
        transaction.commit()?;
        self.effort_by_id(effort_id)
    }

    pub fn resume_provider_reconciliation(&self, effort_id: &str) -> Result<IntegrationEffort> {
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        let IntegrationEffortState::ProviderBlocked(blocked) = &effort.state else {
            anyhow::bail!("provider reconciliation requires provider_blocked effort");
        };
        let state = blocked.resume.restore();
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "provider_reconciliation_resumed",
            serde_json::to_value(&state)?,
            false,
        )?;
        transaction.commit()?;
        self.effort_by_id(effort_id)
    }

    pub fn cancel(&self, effort_id: &str, actor: &str, reason: &str) -> Result<IntegrationEffort> {
        crate::control_domain::require_exact_text(actor, "cancellation actor")?;
        crate::control_domain::require_exact_text(reason, "cancellation reason")?;
        let mut connection = self.connect(true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let effort = required_effort(&transaction, effort_id)?;
        if matches!(
            effort.state,
            IntegrationEffortState::LandingUncertain(_) | IntegrationEffortState::Integrated(_)
        ) {
            anyhow::bail!("uncertain or integrated effort cannot be cancelled");
        }
        if matches!(effort.state, IntegrationEffortState::Cancelled(_)) {
            transaction.commit()?;
            return self.effort_by_id(effort_id);
        }
        let replacement_creating: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM local_submissions WHERE replaces_item_id=?1 AND state='creating')",
            params![effort.item_id],
            |row| row.get(0),
        )?;
        if replacement_creating {
            anyhow::bail!(
                "item has an incomplete immutable replacement; finish that submission before cancellation"
            );
        }
        let termination = match &effort.state {
            IntegrationEffortState::AgentLaunching(launching) => {
                Some(RunnerTerminationAuthority::Launching(launching.clone()))
            }
            IntegrationEffortState::AgentRunning(running) => {
                Some(RunnerTerminationAuthority::Running(running.clone()))
            }
            _ => None,
        };
        if let Some(termination) = termination {
            transaction.execute(
                "INSERT INTO runner_termination_debt(effort_id,authority_json,created_at) VALUES(?1,?2,?3)",
                params![effort.id, serde_json::to_string(&termination)?, now()],
            )?;
        }
        transaction.execute(
            "UPDATE integration_cycles SET status='cancelled',finished_at=?1 WHERE effort_id=?2 AND status IN ('starting','running')",
            params![now(),effort_id],
        )?;
        transaction.execute(
            "UPDATE guidance_requests SET status='cancelled' WHERE effort_id=?1 AND status='open'",
            params![effort_id],
        )?;
        transaction.execute(
            "UPDATE prompts SET status='cancelled' WHERE item_id=?1 AND status='open'",
            params![effort.item_id],
        )?;
        let attempt_changed = transaction.execute(
            "UPDATE integration_attempts SET result='cancelled',finished_at=?1 WHERE id=?2 AND result IS NULL",
            params![now(),effort.attempt_id],
        )?;
        if attempt_changed != 1 {
            anyhow::bail!("cancelled effort attempt is not active");
        }
        let submission: Option<(String, String)> = transaction
            .query_row(
                "SELECT submission.id,submission.workspace_id FROM queue_items item JOIN local_submissions submission ON submission.id=item.submission_id WHERE item.id=?1 AND item.source_kind='local_submission'",
                params![effort.item_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((submission_id, workspace_id)) = submission {
            let submission_changed = transaction.execute(
                "UPDATE local_submissions SET state='cancelled' WHERE id=?1 AND state='queued'",
                params![submission_id],
            )?;
            let workspace_changed = transaction.execute(
                "UPDATE development_workspaces SET status='active',cleanup_json=json_object('state','pending'),updated_at=?1 WHERE id=?2 AND status='submitted'",
                params![now(), workspace_id],
            )?;
            if submission_changed != 1 || workspace_changed != 1 {
                anyhow::bail!("cancelled local submission cannot return its workspace to active");
            }
        }
        let state = IntegrationEffortState::Cancelled(crate::control_domain::Cancelled {
            actor: actor.to_string(),
            reason: reason.to_string(),
            cancelled_at: now(),
        });
        update_state(&transaction, &effort, &state)?;
        append_event(
            &transaction,
            &effort,
            "cancelled",
            serde_json::to_value(&state)?,
            false,
        )?;
        transaction.commit()?;
        self.effort_by_id(effort_id)
    }

    pub fn reconcile_cancelled_runner_terminations(&self, startup: bool) -> Result<usize> {
        let debts = {
            let connection = self.connect(false)?;
            let mut statement = connection.prepare(
                "SELECT debt.effort_id,debt.authority_json,effort.runner_snapshot_json FROM runner_termination_debt debt JOIN integration_efforts effort ON effort.id=debt.effort_id WHERE effort.state='cancelled' ORDER BY debt.created_at,debt.effort_id",
            )?;
            let rows = statement
                .query_map([], |row| {
                    let authority: String = row.get(1)?;
                    let runner: String = row.get(2)?;
                    Ok(RunnerTerminationDebt {
                        effort_id: row.get(0)?,
                        authority: serde_json::from_str(&authority).map_err(json_error)?,
                        runner: serde_json::from_str(&runner).map_err(json_error)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };
        let mut reconciled = 0;
        for debt in &debts {
            let resolved = match &debt.authority {
                RunnerTerminationAuthority::Launching(launching) => {
                    let main_pid = crate::agent_runner::systemd_unit_main_pid(
                        &debt.runner.sandbox.systemctl,
                        &launching.unit_name,
                    )?;
                    if main_pid.is_some() {
                        crate::agent_runner::stop_systemd_unit(
                            &debt.runner.sandbox.systemctl,
                            &launching.unit_name,
                        )?;
                    }
                    main_pid.is_some() || startup
                }
                RunnerTerminationAuthority::Running(running) => {
                    crate::agent_runner::terminate_exact_process(
                        running.pid,
                        running.process_start_ticks,
                        running.process_group_id,
                    )?;
                    true
                }
            };
            if !resolved {
                continue;
            }
            let connection = self.connect(true)?;
            let changed = connection.execute(
                "DELETE FROM runner_termination_debt WHERE effort_id=?1 AND authority_json=?2 AND EXISTS(SELECT 1 FROM integration_efforts WHERE id=?1 AND state='cancelled')",
                params![debt.effort_id, serde_json::to_string(&debt.authority)?],
            )?;
            if changed != 1 {
                anyhow::bail!("runner termination debt authority changed during reconciliation");
            }
            reconciled += 1;
        }
        Ok(reconciled)
    }

    pub fn record_alert(
        &self,
        effort_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<String> {
        crate::control_domain::require_exact_text(event_type, "alert event type")?;
        let connection = self.connect(true)?;
        let effort = required_effort(&connection, effort_id)?;
        append_event(&connection, &effort, event_type, payload, true)
    }

    fn effort_by_id(&self, effort_id: &str) -> Result<IntegrationEffort> {
        let connection = self.connect(false)?;
        required_effort(&connection, effort_id)
    }

    fn validate_schema(&self) -> Result<()> {
        let connection = self.connect(false)?;
        let version: String = connection.query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )?;
        if version != SCHEMA_VERSION {
            anyhow::bail!("control store requires IQ schema version 9");
        }
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            anyhow::bail!("IQ schema version 9 integrity check failed: {integrity}");
        }
        let foreign_keys: i64 =
            connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })?;
        if foreign_keys != 0 {
            anyhow::bail!("IQ schema version 9 has foreign-key errors");
        }
        Ok(())
    }

    fn connect(&self, write: bool) -> Result<Connection> {
        let flags = if write {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        } else {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(&self.path, flags)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(connection)
    }

    #[doc(hidden)]
    pub fn open_test_database(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(
            "CREATE TABLE queue_items(id TEXT PRIMARY KEY,status TEXT NOT NULL DEFAULT 'merging',current_attempt_id TEXT,blocked_phase TEXT,blocked_reason TEXT,blocked_message TEXT,prompt_id TEXT,conflict_json TEXT,target_sha TEXT,source_sha TEXT,landed_commit_sha TEXT,landing_state_json TEXT NOT NULL DEFAULT '{\"state\":\"ready\"}',source_kind TEXT NOT NULL DEFAULT 'remote_branch',submission_id TEXT,replacement_json TEXT,updated_at TEXT NOT NULL DEFAULT 'test'); CREATE TABLE integration_attempts(id TEXT PRIMARY KEY,item_id TEXT,target_base_sha TEXT,merge_commit_sha TEXT,validated_commit_sha TEXT,landed_commit_sha TEXT,validation_command TEXT,validation_exit_code INTEGER,validation_log_path TEXT,signoff_evidence_json TEXT,moved_base_json TEXT,finished_at TEXT,result TEXT); CREATE TABLE development_workspaces(id TEXT PRIMARY KEY,status TEXT NOT NULL,cleanup_json TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE TABLE local_submissions(id TEXT PRIMARY KEY,workspace_id TEXT NOT NULL,state TEXT NOT NULL,replaces_item_id TEXT); CREATE TABLE prompts(id TEXT PRIMARY KEY,item_id TEXT NOT NULL,status TEXT NOT NULL); CREATE TABLE queue_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO queue_metadata VALUES('workspace_schema_version','9');",
        )?;
        install_fresh_v9(&connection)?;
        drop(connection);
        Self::open(path)
    }
}

pub fn install_fresh_v9(connection: &Connection) -> Result<()> {
    connection.execute_batch(V9_SCHEMA)?;
    connection.execute(
        "INSERT INTO queue_metadata(key,value) VALUES('workspace_schema_version','9') ON CONFLICT(key) DO UPDATE SET value='9'",
        [],
    )?;
    Ok(())
}

pub fn migrate_v8_to_v9(
    connection: &mut Connection,
    database_path: &Path,
    system_config_path: &Path,
) -> Result<()> {
    let _lease = DatabaseProcessLease::acquire_exclusive(database_path)?;
    let system_config = SystemConfig::load(system_config_path)?;
    reject_migration_authority(connection)?;
    let active = active_v8_items(connection)?;
    let mut conversions = Vec::new();
    for item in active {
        let project = crate::composition::load_project_control_only(Path::new(&item.repo_path))?;
        crate::state_repository::repository(&project.state_repository)?.verify()?;
        let runner = system_config.runner_snapshot(project.model.as_deref())?;
        conversions.push((item, runner, project.state_repository));
    }
    let backup = verified_backup(connection, database_path)?;
    let conversion = (|| -> Result<()> {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(V9_SCHEMA)?;
        for (item, runner, state_repository) in &conversions {
            convert_item(&transaction, item, runner, state_repository)?;
        }
        transaction.execute(
            "UPDATE prompts SET status='superseded',answer='schema_v9_agent_first' WHERE status='open'",
            [],
        )?;
        transaction.execute("DELETE FROM communication_response_receipts", [])?;
        transaction.execute("DELETE FROM communication_bindings", [])?;
        transaction.execute_batch(
            "DROP TABLE communication_response_receipts; DROP TABLE communication_bindings;",
        )?;
        transaction.execute(
            "UPDATE queue_metadata SET value='9' WHERE key='workspace_schema_version'",
            [],
        )?;
        let active_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM queue_items WHERE status NOT IN ('integrated','cancelled')",
            [],
            |row| row.get(0),
        )?;
        let effort_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM integration_efforts", [], |row| {
                row.get(0)
            })?;
        if active_count != effort_count {
            anyhow::bail!("schema v9 conversion did not create exactly one effort per active item");
        }
        let foreign_keys: i64 =
            transaction.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })?;
        if foreign_keys != 0 {
            anyhow::bail!("schema v9 conversion has foreign-key errors");
        }
        transaction.commit()?;
        Ok(())
    })();
    if let Err(error) = conversion {
        let version: String = connection.query_row(
            "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
            [],
            |row| row.get(0),
        )?;
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if version != "8" || integrity != "ok" {
            anyhow::bail!(
                "schema v9 migration failed and source v8 proof failed; backup is {}: {error:#}",
                backup.path.display()
            );
        }
        return Err(error).context(format!(
            "schema v9 migration rolled back; verified backup is {}",
            backup.path.display()
        ));
    }
    validate_backup_file(&backup.path, &backup.sha256, backup.size)?;
    Ok(())
}

#[derive(Clone, Debug)]
struct BackupIdentity {
    path: PathBuf,
    sha256: String,
    size: u64,
}

fn verified_backup(source: &Connection, database_path: &Path) -> Result<BackupIdentity> {
    let parent = database_path
        .parent()
        .context("database path has no parent")?;
    let backup_path = parent.join(format!(
        "{}.schema-v8.backup",
        database_path
            .file_name()
            .and_then(|name| name.to_str())
            .context("database name is not UTF-8")?
    ));
    let reserve = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&backup_path)
        .with_context(|| format!("reserve schema-v8 backup {}", backup_path.display()))?;
    drop(reserve);
    let mut destination = Connection::open_with_flags(
        &backup_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    {
        let backup = Backup::new(source, &mut destination)?;
        backup.run_to_completion(128, Duration::from_millis(10), None)?;
    }
    destination.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    let version: String = destination.query_row(
        "SELECT value FROM queue_metadata WHERE key='workspace_schema_version'",
        [],
        |row| row.get(0),
    )?;
    let database_id: String = destination.query_row(
        "SELECT value FROM queue_metadata WHERE key='database_id'",
        [],
        |row| row.get(0),
    )?;
    let source_id: String = source.query_row(
        "SELECT value FROM queue_metadata WHERE key='database_id'",
        [],
        |row| row.get(0),
    )?;
    let integrity: String =
        destination.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if version != "8" || database_id != source_id || integrity != "ok" {
        anyhow::bail!("schema-v8 SQLite backup verification failed");
    }
    drop(destination);
    File::open(&backup_path)?.sync_all()?;
    File::open(parent)?.sync_all()?;
    let (sha256, size) = digest_file(&backup_path)?;
    Ok(BackupIdentity {
        path: backup_path,
        sha256,
        size,
    })
}

fn validate_backup_file(path: &Path, expected_digest: &str, expected_size: u64) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        anyhow::bail!("schema-v8 backup lost its protected regular-file identity");
    }
    let (digest, size) = digest_file(path)?;
    if digest != expected_digest || size != expected_size {
        anyhow::bail!("schema-v8 backup digest or size changed");
    }
    Ok(())
}

fn digest_file(path: &Path) -> Result<(String, u64)> {
    let before = fs::symlink_metadata(path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let opened = file.metadata()?;
    if (before.dev(), before.ino()) != (opened.dev(), opened.ino()) {
        anyhow::bail!("backup changed while opening");
    }
    let mut digest = Sha256::new();
    let size = std::io::copy(&mut file, &mut digest)?;
    Ok((format!("{:x}", digest.finalize()), size))
}

fn reject_migration_authority(connection: &Connection) -> Result<()> {
    let leases: i64 = connection.query_row(
        "SELECT COUNT(*) FROM repo_leases WHERE expires_at>?1",
        params![now()],
        |row| row.get(0),
    )?;
    let has_daemon_leases: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='daemon_leases')",
        [],
        |row| row.get(0),
    )?;
    let daemon_leases: i64 = if has_daemon_leases {
        connection.query_row(
            "SELECT COUNT(*) FROM daemon_leases WHERE expires_at>?1",
            params![now()],
            |row| row.get(0),
        )?
    } else {
        0
    };
    if leases != 0 || daemon_leases != 0 {
        anyhow::bail!(
            "schema v9 migration requires no active repository-operation or daemon lease"
        );
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct V8ActiveItem {
    id: String,
    repo_path: String,
    attempt_id: String,
    target_sha: String,
    source_sha: String,
    source_kind: String,
    landing_policy: String,
    workspace: WorkspaceIdentity,
    status: String,
    blocked_phase: Option<String>,
    blocked_reason: Option<String>,
    blocked_message: Option<String>,
    created_at: String,
    updated_at: String,
}

fn active_v8_items(connection: &Connection) -> Result<Vec<V8ActiveItem>> {
    let mut statement = connection.prepare(
        "SELECT item.id,item.repo_path,item.current_attempt_id,item.target_sha,item.source_sha,attempt.target_base_sha,item.source_kind,item.landing_policy,item.integration_workspace_path,item.integration_workspace_rift_id,item.integration_workspace_source_rift_id,item.status,item.blocked_phase,item.blocked_reason,item.blocked_message,item.created_at,item.updated_at FROM queue_items item LEFT JOIN integration_attempts attempt ON attempt.id=item.current_attempt_id WHERE item.status NOT IN ('integrated','cancelled') ORDER BY item.created_at,item.id",
    )?;
    let items = statement
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let required = |value: Option<String>, field: &'static str| {
                value.ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("active v8 item {id} lacks {field}"),
                        )),
                    )
                })
            };
            let attempt_id = required(row.get(2)?, "attempt identity")?;
            let target_sha = required(row.get(3)?, "target SHA")?;
            let source_sha = required(row.get(4)?, "source SHA")?;
            required(row.get(5)?, "base SHA")?;
            Ok(V8ActiveItem {
                id: id.clone(),
                repo_path: row.get(1)?,
                attempt_id,
                target_sha,
                source_sha,
                source_kind: row.get(6)?,
                landing_policy: row.get(7)?,
                workspace: WorkspaceIdentity {
                    path: required(row.get(8)?, "retained Rift path")?,
                    rift_id: required(row.get(9)?, "retained Rift ID")?,
                    source_rift_id: required(row.get(10)?, "source Rift ID")?,
                },
                status: row.get(11)?,
                blocked_phase: row.get(12)?,
                blocked_reason: row.get(13)?,
                blocked_message: row.get(14)?,
                created_at: row.get(15)?,
                updated_at: row.get(16)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(items)
}

fn convert_item(
    transaction: &rusqlite::Transaction<'_>,
    item: &V8ActiveItem,
    runner: &RunnerSnapshot,
    state_repository: &StateRepositorySnapshot,
) -> Result<()> {
    crate::control_domain::require_sha(&item.target_sha, "converted target SHA")?;
    crate::control_domain::require_sha(&item.source_sha, "converted source SHA")?;
    let conflict_prompt: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM prompts WHERE item_id=?1 AND status='open' AND blocked_phase='merging')",
        params![item.id],
        |row| row.get(0),
    )?;
    let state = match item.status.as_str() {
        "ready" | "merging" => {
            IntegrationEffortState::AgentReady(AgentReady { next_cycle: 1 })
        }
        "merged" | "validating" | "validated" | "integrating" => anyhow::bail!(
            "candidate-bearing v8 item has no durable builder operation identity and staged-tree digest"
        ),
        "blocked" if conflict_prompt => {
            IntegrationEffortState::AgentReady(AgentReady { next_cycle: 1 })
        }
        "blocked" => {
            match item.blocked_reason.as_deref() {
                Some("provider") => anyhow::bail!(
                    "provider-blocked v8 item has no durable v9 candidate operation authority"
                ),
                Some("infra" | "dependency" | "credentials") => {
                    if item.blocked_phase.as_deref() != Some("merging") {
                        anyhow::bail!("non-merging v8 infrastructure blocker requires unavailable candidate authority");
                    }
                    IntegrationEffortState::InfrastructureBlocked(
                        crate::control_domain::BlockedEffort {
                            blocker: IntegrationBlocker::Infrastructure(
                                crate::control_domain::InfrastructureBlocker {
                                    component:
                                        crate::control_domain::InfrastructureComponent::Filesystem,
                                    operation: item.blocked_phase.clone().context("infrastructure-blocked v8 item has no exact phase")?,
                                    cause:
                                        crate::control_domain::InfrastructureCause::Unavailable {
                                            detail: item.blocked_message.clone().context("infrastructure-blocked v8 item has no exact evidence")?,
                                        },
                                },
                            ),
                            resume: ResumeState::AgentReady(AgentReady { next_cycle: 1 }),
                        },
                    )
                }
                _ => anyhow::bail!("active v8 blocked item has no exact schema-v9 mapping"),
            }
        }
        status => anyhow::bail!(
            "active v8 item {} has no exact schema-v9 mapping from {status}",
            item.id
        ),
    };
    let effort_id = Uuid::new_v4().to_string();
    state.validate_for_count(0)?;
    transaction.execute(
        "INSERT INTO integration_efforts(id,item_id,attempt_id,target_sha,source_sha,source_variant,landing_variant,workspace_json,runner_snapshot_json,state_repository_json,failed_cycles,state,state_json,blocker_kind,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,?11,?12,?13,?14,?15)",
        params![effort_id,item.id,item.attempt_id,item.target_sha,item.source_sha,item.source_kind,item.landing_policy,serde_json::to_string(&item.workspace)?,serde_json::to_string(runner)?,serde_json::to_string(state_repository)?,state.name(),serde_json::to_string(&state)?,state.blocker().map(IntegrationBlocker::kind),item.created_at,item.updated_at],
    )?;
    let effort = required_effort(transaction, &effort_id)?;
    project_queue_state(transaction, &effort, &state)?;
    let (provider, repository, visibility, reservation_state) = match state_repository {
        StateRepositorySnapshot::Local => (None, None, None, "none"),
        StateRepositorySnapshot::GithubIssue(issue) => (
            Some("github"),
            Some(issue.repository.as_str()),
            Some(match issue.visibility {
                crate::control_domain::IssueVisibility::Minimal => "minimal",
                crate::control_domain::IssueVisibility::Full => "full",
            }),
            if issue.visibility == crate::control_domain::IssueVisibility::Full {
                "pending"
            } else {
                "none"
            },
        ),
        StateRepositorySnapshot::GitlabIssue(issue) => (
            Some("gitlab"),
            Some(issue.repository.as_str()),
            Some(match issue.visibility {
                crate::control_domain::IssueVisibility::Minimal => "minimal",
                crate::control_domain::IssueVisibility::Full => "full",
            }),
            if issue.visibility == crate::control_domain::IssueVisibility::Full {
                "pending"
            } else {
                "none"
            },
        ),
    };
    transaction.execute(
        "INSERT INTO item_state_repository_bindings(item_id,snapshot_json,provider,repository,visibility,reservation_state,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![item.id,serde_json::to_string(state_repository)?,provider,repository,visibility,reservation_state,item.created_at],
    )?;
    append_event_raw(
        transaction,
        &item.id,
        Some(&effort_id),
        "schema_v9_converted",
        serde_json::json!({"old_status":item.status}),
        false,
    )?;
    Ok(())
}

fn map_effort(row: &rusqlite::Row<'_>) -> rusqlite::Result<IntegrationEffort> {
    let workspace: String = row.get(7)?;
    let runner: String = row.get(8)?;
    let state_repository: String = row.get(9)?;
    let state: String = row.get(11)?;
    let failed_cycles: u8 = row.get(10)?;
    let effort = IntegrationEffort {
        id: row.get(0)?,
        item_id: row.get(1)?,
        attempt_id: row.get(2)?,
        target_sha: row.get(3)?,
        source_sha: row.get(4)?,
        source_variant: row.get(5)?,
        landing_variant: row.get(6)?,
        workspace: serde_json::from_str(&workspace).map_err(json_error)?,
        runner: serde_json::from_str(&runner).map_err(json_error)?,
        state_repository: serde_json::from_str(&state_repository).map_err(json_error)?,
        failed_cycles,
        state: serde_json::from_str(&state).map_err(json_error)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    };
    effort
        .state
        .validate_for_count(failed_cycles)
        .map_err(anyhow_sql_error)?;
    Ok(effort)
}

fn required_effort(connection: &Connection, effort_id: &str) -> Result<IntegrationEffort> {
    connection
        .query_row(
            "SELECT id,item_id,attempt_id,target_sha,source_sha,source_variant,landing_variant,workspace_json,runner_snapshot_json,state_repository_json,failed_cycles,state_json,created_at,updated_at FROM integration_efforts WHERE id=?1",
            params![effort_id],
            map_effort,
        )
        .with_context(|| format!("integration effort not found: {effort_id}"))
}

fn update_state(
    connection: &Connection,
    effort: &IntegrationEffort,
    state: &IntegrationEffortState,
) -> Result<()> {
    update_state_and_count(connection, effort, state, effort.failed_cycles)
}

fn update_state_and_count(
    connection: &Connection,
    effort: &IntegrationEffort,
    state: &IntegrationEffortState,
    failed_cycles: u8,
) -> Result<()> {
    state.validate_for_count(failed_cycles)?;
    let changed = connection.execute(
        "UPDATE integration_efforts SET failed_cycles=?1,state=?2,state_json=?3,blocker_kind=?4,updated_at=?5 WHERE id=?6 AND state=?7",
        params![failed_cycles,state.name(),serde_json::to_string(state)?,state.blocker().map(IntegrationBlocker::kind),now(),effort.id,effort.state.name()],
    )?;
    if changed != 1 {
        anyhow::bail!("integration effort state changed concurrently");
    }
    project_queue_state(connection, effort, state)?;
    Ok(())
}

fn project_queue_state(
    connection: &Connection,
    effort: &IntegrationEffort,
    state: &IntegrationEffortState,
) -> Result<()> {
    if let IntegrationEffortState::Integrated(integrated) = state {
        let changed = connection.execute(
            "UPDATE queue_items SET status='integrated',blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,landed_commit_sha=?1,landing_state_json=json_object('state','landed','candidate_sha',?2,'commit_sha',?1),updated_at=?3 WHERE id=?4",
            params![integrated.landed_sha,integrated.candidate_sha,now(),effort.item_id],
        )?;
        if changed != 1 {
            anyhow::bail!("queue terminal projection lost item identity");
        }
        return Ok(());
    }
    let (status, phase, reason, message) = match state {
        IntegrationEffortState::AgentReady(_)
        | IntegrationEffortState::AgentLaunching(_)
        | IntegrationEffortState::AgentRunning(_)
        | IntegrationEffortState::CandidateBuilding(_) => ("merging", None, None, None),
        IntegrationEffortState::CandidateReady(_) => ("merged", None, None, None),
        IntegrationEffortState::Validating(validating) => match validating.stage {
            crate::control_domain::ValidationStage::Running => ("validating", None, None, None),
            crate::control_domain::ValidationStage::Gates => ("integrating", None, None, None),
        },
        IntegrationEffortState::GuidanceRequired(blocked) => (
            "blocked",
            Some("merging"),
            Some("needs_user_input"),
            Some(serde_json::to_string(&blocked.blocker)?),
        ),
        IntegrationEffortState::InfrastructureBlocked(blocked) => {
            let phase = match blocked.resume {
                crate::control_domain::ResumeState::AgentReady(_)
                | crate::control_domain::ResumeState::CandidateBuilding(_) => "merging",
                crate::control_domain::ResumeState::CandidateReady(_)
                | crate::control_domain::ResumeState::Validating(
                    crate::control_domain::Validating {
                        stage: crate::control_domain::ValidationStage::Running,
                        ..
                    },
                ) => "validating",
                crate::control_domain::ResumeState::Validating(
                    crate::control_domain::Validating {
                        stage: crate::control_domain::ValidationStage::Gates,
                        ..
                    },
                )
                | crate::control_domain::ResumeState::Landing(_)
                | crate::control_domain::ResumeState::LandingUncertain(_) => "integrating",
            };
            (
                "blocked",
                Some(phase),
                Some("infra"),
                Some(serde_json::to_string(&blocked.blocker)?),
            )
        }
        IntegrationEffortState::CycleLimitBlocked(blocked) => (
            "blocked",
            Some("merging"),
            Some("needs_agent_fix"),
            Some(serde_json::to_string(&blocked.blocker)?),
        ),
        IntegrationEffortState::ProviderBlocked(blocked) => (
            "blocked",
            Some("integrating"),
            Some("provider"),
            Some(serde_json::to_string(&blocked.blocker)?),
        ),
        IntegrationEffortState::Landing(_) | IntegrationEffortState::LandingUncertain(_) => {
            ("integrating", None, None, None)
        }
        IntegrationEffortState::Integrated(_) => unreachable!(),
        IntegrationEffortState::Cancelled(_) => ("cancelled", None, None, None),
    };
    let landing = legacy_landing_state(state)?;
    let changed = connection.execute(
        "UPDATE queue_items SET status=?1,blocked_phase=?2,blocked_reason=?3,blocked_message=?4,prompt_id=NULL,landing_state_json=?5,replacement_json=CASE WHEN ?1='cancelled' THEN NULL ELSE replacement_json END,updated_at=?6 WHERE id=?7 AND current_attempt_id=?8",
        params![status,phase,reason,message,landing,now(),effort.item_id,effort.attempt_id],
    )?;
    if changed != 1 {
        anyhow::bail!("queue compatibility projection lost item or attempt identity");
    }
    Ok(())
}

fn legacy_landing_state(state: &IntegrationEffortState) -> Result<String> {
    let landing = match state {
        IntegrationEffortState::Landing(landing) => serde_json::json!({
            "state": "uncertain",
            "candidate_sha": landing.candidate_sha,
            "expected_target_sha": landing.expected_target_sha,
        }),
        IntegrationEffortState::LandingUncertain(landing) => serde_json::json!({
            "state": "uncertain",
            "candidate_sha": landing.candidate_sha,
            "expected_target_sha": landing.expected_target_sha,
        }),
        IntegrationEffortState::InfrastructureBlocked(blocked)
        | IntegrationEffortState::ProviderBlocked(blocked) => match &blocked.resume {
            ResumeState::Landing(landing) => serde_json::json!({
                "state": "uncertain",
                "candidate_sha": landing.candidate_sha,
                "expected_target_sha": landing.expected_target_sha,
            }),
            ResumeState::LandingUncertain(landing) => serde_json::json!({
                "state": "uncertain",
                "candidate_sha": landing.candidate_sha,
                "expected_target_sha": landing.expected_target_sha,
            }),
            _ => serde_json::json!({"state": "ready"}),
        },
        _ => serde_json::json!({"state": "ready"}),
    };
    Ok(serde_json::to_string(&landing)?)
}

fn append_event(
    connection: &Connection,
    effort: &IntegrationEffort,
    event_type: &str,
    payload: serde_json::Value,
    alert: bool,
) -> Result<String> {
    let id = Uuid::new_v4().to_string();
    append_event_with_id(connection, &id, effort, event_type, payload, alert)?;
    Ok(id)
}

fn append_event_with_id(
    connection: &Connection,
    id: &str,
    effort: &IntegrationEffort,
    event_type: &str,
    payload: serde_json::Value,
    alert: bool,
) -> Result<()> {
    connection.execute(
        "INSERT INTO durable_events(id,item_id,effort_id,event_type,payload_json,alert,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![id,effort.item_id,effort.id,event_type,payload.to_string(),alert,now()],
    )?;
    if alert {
        connection.execute(
            "INSERT OR IGNORE INTO notification_deliveries(event_id,backend,state,attempt_count,next_attempt_at,created_at,updated_at) SELECT ?1,backend,'pending',0,?2,?2,?2 FROM notification_backends WHERE enabled=1",
            params![id,now()],
        )?;
    }
    Ok(())
}

fn append_event_raw(
    connection: &Connection,
    item_id: &str,
    effort_id: Option<&str>,
    event_type: &str,
    payload: serde_json::Value,
    alert: bool,
) -> Result<String> {
    let id = Uuid::new_v4().to_string();
    connection.execute(
        "INSERT INTO durable_events(id,item_id,effort_id,event_type,payload_json,alert,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![id,item_id,effort_id,event_type,payload.to_string(),alert,now()],
    )?;
    if alert {
        connection.execute(
            "INSERT OR IGNORE INTO notification_deliveries(event_id,backend,state,attempt_count,next_attempt_at,created_at,updated_at) SELECT ?1,backend,'pending',0,?2,?2,?2 FROM notification_backends WHERE enabled=1",
            params![id,now()],
        )?;
    }
    Ok(id)
}

fn cycle_ids(connection: &Connection, effort_id: &str, current: &str) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT id FROM integration_cycles WHERE effort_id=?1 AND status='failed' ORDER BY cycle_number",
    )?;
    let mut ids = statement
        .query_map(params![effort_id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<String>, _>>()?;
    if !ids.iter().any(|id| id == current) {
        ids.push(current.to_string());
    }
    Ok(ids)
}

fn next_cycle_number(connection: &Connection, effort_id: &str) -> Result<u8> {
    let next: u16 = connection.query_row(
        "SELECT COALESCE(MAX(cycle_number),0)+1 FROM integration_cycles WHERE effort_id=?1",
        params![effort_id],
        |row| row.get(0),
    )?;
    u8::try_from(next).context("integration effort exceeds cycle identity range")
}

fn answer_disposition(value: AnswerDisposition) -> &'static str {
    match value {
        AnswerDisposition::Applied => "applied",
        AnswerDisposition::Duplicate => "duplicate",
        AnswerDisposition::Stale => "stale",
        AnswerDisposition::Malformed => "malformed",
        AnswerDisposition::Unauthorized => "unauthorized",
    }
}

fn validate_provider_comment_identity(
    provider: &str,
    repository: &str,
    artifact_id: &str,
    comment_id: &str,
) -> Result<()> {
    if !matches!(provider, "github" | "gitlab") {
        anyhow::bail!("provider comment has an unknown provider");
    }
    for (value, maximum, label) in [
        (repository, 512, "repository"),
        (artifact_id, 128, "artifact ID"),
        (comment_id, 256, "comment ID"),
    ] {
        crate::control_domain::require_exact_text(value, label)?;
        if value.len() > maximum {
            anyhow::bail!("provider comment {label} exceeds its bound");
        }
    }
    Ok(())
}

fn provider_comment_key(
    provider: &str,
    repository: &str,
    artifact_id: &str,
    comment_id: &str,
) -> Result<String> {
    validate_provider_comment_identity(provider, repository, artifact_id, comment_id)?;
    Ok(format!(
        "provider:{provider}:{repository}:{artifact_id}:{comment_id}"
    ))
}

pub(crate) fn provider_comment_external_id(
    provider: &str,
    repository: &str,
    artifact_id: &str,
    comment_id: &str,
) -> Result<String> {
    provider_comment_key(provider, repository, artifact_id, comment_id)
}

fn json_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn anyhow_sql_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{error:#}"),
        )),
    )
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn git_text<const N: usize>(repository: &Path, args: [&str; N]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "candidate Git observation failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

const V9_SCHEMA: &str = r#"
CREATE UNIQUE INDEX IF NOT EXISTS integration_attempt_item_identity
ON integration_attempts(id,item_id);

CREATE TABLE IF NOT EXISTS integration_efforts (
  id TEXT PRIMARY KEY CHECK(id!=''),
  item_id TEXT NOT NULL UNIQUE REFERENCES queue_items(id) ON DELETE CASCADE,
  attempt_id TEXT NOT NULL,
  target_sha TEXT NOT NULL CHECK(length(target_sha) IN (40,64) AND target_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  source_sha TEXT NOT NULL CHECK(length(source_sha) IN (40,64) AND source_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  source_variant TEXT NOT NULL CHECK(source_variant IN ('remote_branch','local_submission')),
  landing_variant TEXT NOT NULL CHECK(landing_variant IN ('direct','provider','squash')),
  workspace_json TEXT NOT NULL CHECK(json_valid(workspace_json)),
  runner_snapshot_json TEXT NOT NULL CHECK(json_valid(runner_snapshot_json)),
  state_repository_json TEXT NOT NULL CHECK(json_valid(state_repository_json)),
  failed_cycles INTEGER NOT NULL DEFAULT 0 CHECK(failed_cycles BETWEEN 0 AND 10),
  state TEXT NOT NULL CHECK(state IN ('agent_ready','agent_launching','agent_running','candidate_building','candidate_ready','validating','guidance_required','infrastructure_blocked','cycle_limit_blocked','provider_blocked','landing','landing_uncertain','integrated','cancelled')),
  state_json TEXT NOT NULL CHECK(json_valid(state_json) AND json_extract(state_json,'$.state') IS state),
  blocker_kind TEXT CHECK(blocker_kind IN ('semantic_guidance','infrastructure','cycle_limit','provider_signoff')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(attempt_id,item_id) REFERENCES integration_attempts(id,item_id),
  CHECK(COALESCE(CASE state
    WHEN 'guidance_required' THEN blocker_kind IS 'semantic_guidance'
    WHEN 'infrastructure_blocked' THEN blocker_kind IS 'infrastructure'
    WHEN 'cycle_limit_blocked' THEN blocker_kind IS 'cycle_limit'
    WHEN 'provider_blocked' THEN blocker_kind IS 'provider_signoff'
    ELSE blocker_kind IS NULL END,0)),
  CHECK((state='cycle_limit_blocked')=(failed_cycles=10)),
  CHECK(COALESCE(CASE state
    WHEN 'agent_ready' THEN json_type(state_json,'$.payload.next_cycle')='integer' AND json_extract(state_json,'$.payload.next_cycle')>=1
    WHEN 'agent_launching' THEN json_type(state_json,'$.payload.launch_operation_id')='text' AND json_type(state_json,'$.payload.unit_name')='text' AND json_type(state_json,'$.payload.cycle_id')='text' AND json_type(state_json,'$.payload.cycle_number')='integer' AND json_type(state_json,'$.payload.protocol_directory')='text'
    WHEN 'agent_running' THEN json_type(state_json,'$.payload.cycle_id')='text' AND json_type(state_json,'$.payload.pid')='integer' AND json_type(state_json,'$.payload.process_start_ticks')='integer' AND json_type(state_json,'$.payload.process_group_id')='integer'
    WHEN 'candidate_building' THEN json_type(state_json,'$.payload.operation_id')='text' AND json_type(state_json,'$.payload.cycle_id')='text' AND json_type(state_json,'$.payload.tree_sha')='text' AND json_type(state_json,'$.payload.parent_shas')='array' AND json_array_length(json_extract(state_json,'$.payload.parent_shas'))>=1 AND json_type(state_json,'$.payload.operation_ref')='text'
    WHEN 'candidate_ready' THEN json_type(state_json,'$.payload.cycle_id')='text' AND json_type(state_json,'$.payload.candidate_sha')='text'
    WHEN 'validating' THEN json_type(state_json,'$.payload.candidate_sha')='text' AND json_extract(state_json,'$.payload.stage') IN ('running','gates')
    WHEN 'guidance_required' THEN json_extract(state_json,'$.payload.blocker.kind')='semantic_guidance' AND json_extract(state_json,'$.payload.resume.state')='agent_ready'
    WHEN 'infrastructure_blocked' THEN json_extract(state_json,'$.payload.blocker.kind')='infrastructure' AND json_type(state_json,'$.payload.resume.state')='text'
    WHEN 'cycle_limit_blocked' THEN json_extract(state_json,'$.payload.blocker.kind')='cycle_limit' AND json_extract(state_json,'$.payload.blocker.count')=10
    WHEN 'provider_blocked' THEN json_extract(state_json,'$.payload.blocker.kind')='provider_signoff' AND json_type(state_json,'$.payload.blocker.candidate_sha')='text'
    WHEN 'landing' THEN json_type(state_json,'$.payload.candidate_sha')='text' AND json_type(state_json,'$.payload.expected_target_sha')='text' AND json_type(state_json,'$.payload.lease_id')='text'
    WHEN 'landing_uncertain' THEN json_type(state_json,'$.payload.candidate_sha')='text' AND json_type(state_json,'$.payload.expected_target_sha')='text' AND json_type(state_json,'$.payload.command_id')='text'
    WHEN 'integrated' THEN json_type(state_json,'$.payload.candidate_sha')='text' AND json_type(state_json,'$.payload.landed_sha')='text' AND json_type(state_json,'$.payload.event_id')='text'
    WHEN 'cancelled' THEN json_type(state_json,'$.payload.actor')='text' AND json_type(state_json,'$.payload.reason')='text' AND json_type(state_json,'$.payload.cancelled_at')='text'
    ELSE 0 END,0))
);

CREATE TABLE IF NOT EXISTS integration_cycles (
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES integration_efforts(id) ON DELETE CASCADE,
  cycle_number INTEGER NOT NULL CHECK(cycle_number>=1),
  status TEXT NOT NULL CHECK(status IN ('starting','running','resolved','guidance_required','failed','superseded','cancelled')),
  process_json TEXT CHECK(process_json IS NULL OR json_valid(process_json)),
  input_digest TEXT,
  result_state_json TEXT CHECK(result_state_json IS NULL OR json_valid(result_state_json)),
  log_blob BLOB,
  failure_json TEXT CHECK(failure_json IS NULL OR json_valid(failure_json)),
  created_at TEXT NOT NULL,
  finished_at TEXT,
  UNIQUE(effort_id,cycle_number)
);
CREATE UNIQUE INDEX IF NOT EXISTS integration_cycle_effort_identity ON integration_cycles(id,effort_id);
CREATE UNIQUE INDEX IF NOT EXISTS one_running_cycle_per_effort ON integration_cycles(effort_id) WHERE status IN ('starting','running');

CREATE TABLE IF NOT EXISTS runner_termination_debt (
  effort_id TEXT PRIMARY KEY REFERENCES integration_efforts(id) ON DELETE CASCADE,
  authority_json TEXT NOT NULL CHECK(json_valid(authority_json)),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS guidance_requests (
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES integration_efforts(id) ON DELETE CASCADE,
  cycle_id TEXT NOT NULL,
  request_json TEXT NOT NULL CHECK(json_valid(request_json)),
  status TEXT NOT NULL CHECK(status IN ('open','answered','cancelled')),
  created_at TEXT NOT NULL,
  answered_at TEXT,
  FOREIGN KEY(cycle_id,effort_id) REFERENCES integration_cycles(id,effort_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_guidance_per_effort ON guidance_requests(effort_id) WHERE status='open';

CREATE TABLE IF NOT EXISTS candidate_evidence (
  effort_id TEXT PRIMARY KEY REFERENCES integration_efforts(id) ON DELETE CASCADE,
  cycle_id TEXT NOT NULL,
  candidate_sha TEXT NOT NULL CHECK(length(candidate_sha) IN (40,64) AND candidate_sha NOT GLOB '*[^0-9A-Fa-f]*'),
  builder_operation_id TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  FOREIGN KEY(cycle_id,effort_id) REFERENCES integration_cycles(id,effort_id)
);

CREATE TABLE IF NOT EXISTS durable_events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  effort_id TEXT REFERENCES integration_efforts(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  alert INTEGER NOT NULL CHECK(alert IN (0,1)),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS state_repository_artifacts (
  effort_id TEXT PRIMARY KEY REFERENCES integration_efforts(id) ON DELETE CASCADE,
  provider TEXT NOT NULL CHECK(provider IN ('github','gitlab')),
  repository TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  artifact_url TEXT NOT NULL,
  projection_revision INTEGER NOT NULL DEFAULT 0,
  last_event_sequence INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL CHECK(state IN ('reserved','active','pending_close','closed')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS item_state_repository_reservations (
  item_id TEXT PRIMARY KEY REFERENCES queue_items(id) ON DELETE CASCADE,
  provider TEXT NOT NULL CHECK(provider IN ('github','gitlab')),
  repository TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  artifact_url TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_debt (
  effort_id TEXT PRIMARY KEY REFERENCES integration_efforts(id) ON DELETE CASCADE,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,
  last_error_json TEXT NOT NULL CHECK(json_valid(last_error_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_debt_alerts (
  effort_id TEXT PRIMARY KEY REFERENCES projection_debt(effort_id) ON DELETE CASCADE,
  event_id TEXT NOT NULL UNIQUE REFERENCES durable_events(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS answer_receipts (
  external_id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES integration_efforts(id) ON DELETE CASCADE,
  request_id TEXT NOT NULL,
  responder_json TEXT NOT NULL CHECK(json_valid(responder_json)),
  answer TEXT NOT NULL,
  disposition TEXT NOT NULL CHECK(disposition IN ('applied','duplicate','stale','malformed','unauthorized')),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_comment_receipts (
  provider TEXT NOT NULL CHECK(provider IN ('github','gitlab')),
  repository TEXT NOT NULL CHECK(length(repository) BETWEEN 1 AND 512),
  artifact_id TEXT NOT NULL CHECK(length(artifact_id) BETWEEN 1 AND 128),
  comment_id TEXT NOT NULL CHECK(length(comment_id) BETWEEN 1 AND 256),
  effort_id TEXT NOT NULL REFERENCES integration_efforts(id) ON DELETE CASCADE,
  actor TEXT,
  body TEXT NOT NULL,
  disposition TEXT NOT NULL CHECK(disposition IN ('malformed','unknown')),
  created_at TEXT NOT NULL,
  PRIMARY KEY(provider,repository,artifact_id,comment_id)
);

CREATE TABLE IF NOT EXISTS notification_backends (
  backend TEXT PRIMARY KEY CHECK(backend IN ('wslg','windows')),
  enabled INTEGER NOT NULL CHECK(enabled IN (0,1))
);
INSERT OR IGNORE INTO notification_backends(backend,enabled) VALUES('wslg',0),('windows',0);

CREATE TABLE IF NOT EXISTS notification_deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL REFERENCES durable_events(id) ON DELETE CASCADE,
  backend TEXT NOT NULL REFERENCES notification_backends(backend),
  state TEXT NOT NULL CHECK(state IN ('pending','claimed','running','delivered','delivery_unknown','failed','expired')),
  claim_id TEXT,
  claimed_at TEXT,
  attempt_count INTEGER NOT NULL CHECK(attempt_count>=0),
  next_attempt_at TEXT,
  last_error_json TEXT CHECK(last_error_json IS NULL OR json_valid(last_error_json)),
  redelivery_of INTEGER REFERENCES notification_deliveries(id),
  redelivery_actor TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(event_id,backend,redelivery_of),
  CHECK((state IN ('claimed','running'))=(claim_id IS NOT NULL AND claimed_at IS NOT NULL)),
  CHECK((redelivery_of IS NULL)=(redelivery_actor IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS one_original_notification_delivery
ON notification_deliveries(event_id,backend) WHERE redelivery_of IS NULL;

CREATE TABLE IF NOT EXISTS item_state_repository_bindings (
  item_id TEXT PRIMARY KEY REFERENCES queue_items(id) ON DELETE CASCADE,
  snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)),
  provider TEXT CHECK(provider IN ('github','gitlab')),
  repository TEXT,
  visibility TEXT CHECK(visibility IN ('minimal','full')),
  reservation_state TEXT NOT NULL CHECK(reservation_state IN ('none','pending','reserved')),
  created_at TEXT NOT NULL,
  CHECK(
    (provider IS NULL AND repository IS NULL AND visibility IS NULL AND reservation_state='none') OR
    (provider IS NOT NULL AND repository IS NOT NULL AND visibility='minimal' AND reservation_state='none') OR
    (provider IS NOT NULL AND repository IS NOT NULL AND visibility='full' AND reservation_state IN ('pending','reserved'))
  )
);

CREATE TABLE IF NOT EXISTS daemon_leases (
  name TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  pid INTEGER NOT NULL,
  process_start_ticks INTEGER NOT NULL,
  heartbeat_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS integration_effort_exact_payload_insert
BEFORE INSERT ON integration_efforts
WHEN json_type(NEW.state_json,'$')!='object'
  OR json_type(NEW.state_json,'$.payload')!='object'
  OR (SELECT COUNT(*) FROM json_each(NEW.state_json))!=2
  OR EXISTS(SELECT 1 FROM json_each(NEW.state_json) WHERE key NOT IN ('state','payload'))
  OR NOT CASE NEW.state
    WHEN 'agent_ready' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=1 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('next_cycle'))
    WHEN 'agent_launching' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=8 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('launch_operation_id','unit_name','cycle_id','cycle_number','authority_lease_id','input_sha256','protocol_directory','prepared_at'))
    WHEN 'agent_running' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=12 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('launch_operation_id','unit_name','cycle_id','cycle_number','pid','process_start_ticks','process_group_id','authority_lease_id','sandbox_id','input_sha256','result','started_at'))
    WHEN 'candidate_building' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=13 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('operation_id','cycle_id','staged_tree_sha256','tree_sha','parent_shas','author_name','author_email','author_timestamp','committer_name','committer_email','committer_timestamp','message','operation_ref'))
    WHEN 'candidate_ready' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('operation_id','cycle_id','candidate_sha','staged_tree_sha256'))
    WHEN 'validating' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=3 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','policy_digest','stage'))
    WHEN 'guidance_required' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'infrastructure_blocked' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'cycle_limit_blocked' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'provider_blocked' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'landing' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','expected_target_sha','lease_id','signoff'))
    WHEN 'landing_uncertain' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','expected_target_sha','command_id','evidence'))
    WHEN 'integrated' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','landed_sha','attempt_id','event_id'))
    WHEN 'cancelled' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=3 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('actor','reason','cancelled_at'))
    ELSE 0 END
BEGIN
  SELECT RAISE(ABORT,'integration effort payload keys are invalid');
END;

CREATE TRIGGER IF NOT EXISTS integration_effort_exact_payload_update
BEFORE UPDATE OF state,state_json ON integration_efforts
WHEN json_type(NEW.state_json,'$')!='object'
  OR json_type(NEW.state_json,'$.payload')!='object'
  OR (SELECT COUNT(*) FROM json_each(NEW.state_json))!=2
  OR EXISTS(SELECT 1 FROM json_each(NEW.state_json) WHERE key NOT IN ('state','payload'))
  OR NOT CASE NEW.state
    WHEN 'agent_ready' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=1 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('next_cycle'))
    WHEN 'agent_launching' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=8 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('launch_operation_id','unit_name','cycle_id','cycle_number','authority_lease_id','input_sha256','protocol_directory','prepared_at'))
    WHEN 'agent_running' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=12 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('launch_operation_id','unit_name','cycle_id','cycle_number','pid','process_start_ticks','process_group_id','authority_lease_id','sandbox_id','input_sha256','result','started_at'))
    WHEN 'candidate_building' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=13 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('operation_id','cycle_id','staged_tree_sha256','tree_sha','parent_shas','author_name','author_email','author_timestamp','committer_name','committer_email','committer_timestamp','message','operation_ref'))
    WHEN 'candidate_ready' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('operation_id','cycle_id','candidate_sha','staged_tree_sha256'))
    WHEN 'validating' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=3 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','policy_digest','stage'))
    WHEN 'guidance_required' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'infrastructure_blocked' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'cycle_limit_blocked' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'provider_blocked' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=2 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('blocker','resume'))
    WHEN 'landing' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','expected_target_sha','lease_id','signoff'))
    WHEN 'landing_uncertain' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','expected_target_sha','command_id','evidence'))
    WHEN 'integrated' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=4 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('candidate_sha','landed_sha','attempt_id','event_id'))
    WHEN 'cancelled' THEN (SELECT COUNT(*) FROM json_each(NEW.state_json,'$.payload'))=3 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.state_json,'$.payload') WHERE key NOT IN ('actor','reason','cancelled_at'))
    ELSE 0 END
BEGIN
  SELECT RAISE(ABORT,'integration effort payload keys are invalid');
END;

CREATE TRIGGER IF NOT EXISTS integration_effort_legal_transition
BEFORE UPDATE OF state ON integration_efforts
WHEN NEW.state!=OLD.state AND NOT (
  (OLD.state='agent_ready' AND NEW.state IN ('agent_launching','infrastructure_blocked','cancelled')) OR
  (OLD.state='agent_launching' AND NEW.state IN ('agent_ready','agent_running','infrastructure_blocked','cancelled')) OR
  (OLD.state='agent_running' AND NEW.state IN ('candidate_building','agent_ready','guidance_required','infrastructure_blocked','cycle_limit_blocked','cancelled')) OR
  (OLD.state='candidate_building' AND NEW.state IN ('candidate_ready','agent_ready','infrastructure_blocked','cancelled')) OR
  (OLD.state='candidate_ready' AND NEW.state IN ('validating','agent_ready','infrastructure_blocked','cancelled')) OR
  (OLD.state='validating' AND NEW.state IN ('validating','landing','agent_ready','guidance_required','infrastructure_blocked','provider_blocked','cycle_limit_blocked','cancelled')) OR
  (OLD.state='guidance_required' AND NEW.state IN ('agent_ready','cancelled')) OR
  (OLD.state='infrastructure_blocked' AND NEW.state IN ('agent_ready','candidate_building','candidate_ready','validating','landing','landing_uncertain','cancelled')) OR
  (OLD.state='cycle_limit_blocked' AND NEW.state IN ('agent_ready','cancelled')) OR
  (OLD.state='provider_blocked' AND NEW.state IN ('validating','landing','landing_uncertain','agent_ready','cancelled')) OR
  (OLD.state='landing' AND NEW.state IN ('landing_uncertain','integrated','agent_ready','provider_blocked','infrastructure_blocked','cancelled')) OR
  (OLD.state='landing_uncertain' AND NEW.state IN ('integrated','landing','agent_ready','provider_blocked','infrastructure_blocked'))
)
BEGIN
  SELECT RAISE(ABORT,'illegal integration_effort transition');
END;

CREATE TRIGGER IF NOT EXISTS integration_effort_related_state_insert
AFTER INSERT ON integration_efforts
WHEN NOT (
  (NEW.state='agent_ready' AND NOT EXISTS(SELECT 1 FROM integration_cycles WHERE effort_id=NEW.id AND status IN ('starting','running'))) OR
  NEW.state NOT IN ('agent_ready')
)
BEGIN
  SELECT RAISE(ABORT,'integration effort related state is invalid');
END;

CREATE TRIGGER IF NOT EXISTS integration_effort_related_state_update
AFTER UPDATE OF state,state_json ON integration_efforts
WHEN NOT (
  (NEW.state='agent_launching' AND EXISTS(SELECT 1 FROM integration_cycles cycle WHERE cycle.effort_id=NEW.id AND cycle.id=json_extract(NEW.state_json,'$.payload.cycle_id') AND cycle.status='starting')) OR
  (NEW.state='agent_running' AND EXISTS(SELECT 1 FROM integration_cycles cycle WHERE cycle.effort_id=NEW.id AND cycle.id=json_extract(NEW.state_json,'$.payload.cycle_id') AND cycle.status='running')) OR
  (NEW.state='candidate_building' AND EXISTS(SELECT 1 FROM integration_cycles cycle WHERE cycle.effort_id=NEW.id AND cycle.id=json_extract(NEW.state_json,'$.payload.cycle_id') AND cycle.status='resolved')) OR
  (NEW.state IN ('candidate_ready','validating','landing','landing_uncertain','provider_blocked','integrated') AND EXISTS(SELECT 1 FROM candidate_evidence candidate WHERE candidate.effort_id=NEW.id AND candidate.candidate_sha=COALESCE(json_extract(NEW.state_json,'$.payload.candidate_sha'),json_extract(NEW.state_json,'$.payload.blocker.candidate_sha')))) OR
  (NEW.state='guidance_required' AND EXISTS(SELECT 1 FROM guidance_requests request WHERE request.effort_id=NEW.id AND request.status='open')) OR
  NEW.state IN ('agent_ready','infrastructure_blocked','cycle_limit_blocked','cancelled')
)
BEGIN
  SELECT RAISE(ABORT,'integration effort related state is invalid');
END;

DROP TRIGGER IF EXISTS queue_effort_projection_guard;
CREATE TRIGGER queue_effort_projection_guard
BEFORE UPDATE OF status,blocked_phase,blocked_reason,prompt_id,landed_commit_sha,landing_state_json ON queue_items
WHEN EXISTS(SELECT 1 FROM integration_efforts WHERE item_id=OLD.id)
  AND NOT EXISTS(
    SELECT 1 FROM integration_efforts effort
    WHERE effort.item_id=OLD.id
      AND NEW.prompt_id IS NULL
      AND NEW.status=CASE
        WHEN effort.state IN ('agent_ready','agent_launching','agent_running','candidate_building') THEN 'merging'
        WHEN effort.state='candidate_ready' THEN 'merged'
        WHEN effort.state='validating' AND json_extract(effort.state_json,'$.payload.stage')='running' THEN 'validating'
        WHEN effort.state='validating' AND json_extract(effort.state_json,'$.payload.stage')='gates' THEN 'integrating'
        WHEN effort.state IN ('guidance_required','infrastructure_blocked','cycle_limit_blocked','provider_blocked') THEN 'blocked'
        WHEN effort.state IN ('landing','landing_uncertain') THEN 'integrating'
        WHEN effort.state='integrated' THEN 'integrated'
        WHEN effort.state='cancelled' THEN 'cancelled'
      END
      AND COALESCE(NEW.blocked_phase,'')=CASE
        WHEN effort.state='guidance_required' THEN 'merging'
        WHEN effort.state='cycle_limit_blocked' THEN 'merging'
        WHEN effort.state='provider_blocked' THEN 'integrating'
        WHEN effort.state='infrastructure_blocked' THEN CASE json_extract(effort.state_json,'$.payload.resume.state')
          WHEN 'agent_ready' THEN 'merging'
          WHEN 'candidate_building' THEN 'merging'
          WHEN 'candidate_ready' THEN 'validating'
          WHEN 'validating' THEN CASE json_extract(effort.state_json,'$.payload.resume.payload.stage')
            WHEN 'running' THEN 'validating'
            WHEN 'gates' THEN 'integrating'
          END
          WHEN 'landing' THEN 'integrating'
          WHEN 'landing_uncertain' THEN 'integrating'
        END
        ELSE ''
      END
      AND COALESCE(NEW.blocked_reason,'')=CASE
        WHEN effort.state='guidance_required' THEN 'needs_user_input'
        WHEN effort.state='infrastructure_blocked' THEN 'infra'
        WHEN effort.state='cycle_limit_blocked' THEN 'needs_agent_fix'
        WHEN effort.state='provider_blocked' THEN 'provider'
        ELSE ''
      END
      AND NEW.landed_commit_sha IS CASE
        WHEN effort.state='integrated' THEN json_extract(effort.state_json,'$.payload.landed_sha')
        ELSE NULL
      END
      AND CASE
        WHEN effort.state IN ('landing','landing_uncertain') THEN
          json_extract(NEW.landing_state_json,'$.state')='uncertain'
          AND (SELECT COUNT(*) FROM json_each(NEW.landing_state_json))=3
          AND json_extract(NEW.landing_state_json,'$.candidate_sha')=json_extract(effort.state_json,'$.payload.candidate_sha')
          AND json_extract(NEW.landing_state_json,'$.expected_target_sha')=json_extract(effort.state_json,'$.payload.expected_target_sha')
        WHEN effort.state IN ('infrastructure_blocked','provider_blocked')
          AND json_extract(effort.state_json,'$.payload.resume.state') IN ('landing','landing_uncertain') THEN
          json_extract(NEW.landing_state_json,'$.state')='uncertain'
          AND (SELECT COUNT(*) FROM json_each(NEW.landing_state_json))=3
          AND json_extract(NEW.landing_state_json,'$.candidate_sha')=json_extract(effort.state_json,'$.payload.resume.payload.candidate_sha')
          AND json_extract(NEW.landing_state_json,'$.expected_target_sha')=json_extract(effort.state_json,'$.payload.resume.payload.expected_target_sha')
        WHEN effort.state='integrated' THEN
          json_extract(NEW.landing_state_json,'$.state')='landed'
          AND (SELECT COUNT(*) FROM json_each(NEW.landing_state_json))=3
          AND json_extract(NEW.landing_state_json,'$.candidate_sha')=json_extract(effort.state_json,'$.payload.candidate_sha')
          AND json_extract(NEW.landing_state_json,'$.commit_sha')=json_extract(effort.state_json,'$.payload.landed_sha')
        ELSE
          json_extract(NEW.landing_state_json,'$.state')='ready'
          AND (SELECT COUNT(*) FROM json_each(NEW.landing_state_json))=1
      END
  )
BEGIN
  SELECT RAISE(ABORT,'queue lifecycle is a projection of integration_effort');
END;
"#;
