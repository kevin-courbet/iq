pub mod communication;

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
}

pub mod sqlite {
    use anyhow::{Context, Result};
    use chrono::{Duration, Utc};
    use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;
    use uuid::Uuid;

    use crate::core::{BlockedPhase, BlockedReason, BlockedState, QueueStatus, StateMachine};

    #[derive(Clone, Debug)]
    pub struct EnqueueRequest {
        pub repo_key: String,
        pub repo_path: String,
        pub source_branch: String,
        pub target_branch: String,
        pub current_head_sha: String,
        pub pr_url: Option<String>,
        pub producer_metadata: Value,
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
        pub integration_workspace_path: Option<String>,
        pub conflict: Option<serde_json::Value>,
        pub target_sha: Option<String>,
        pub source_sha: Option<String>,
        pub landed_commit_sha: Option<String>,
        pub producer_metadata: Value,
        pub validation_evidence: Value,
        pub landing_fenced: bool,
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
    pub struct CommunicationBinding {
        pub id: String,
        pub repo_key: String,
        pub item_id: String,
        pub transport_id: String,
        pub transport_kind: String,
        pub endpoint_fingerprint: String,
        pub marker: String,
        pub external_ref: Option<Value>,
        pub external_url: Option<String>,
        pub status: String,
        pub last_error: Option<String>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum CommunicationResponseDisposition {
        Applied,
        Duplicate,
        Stale,
        Invalid,
        Unauthorized,
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
    }

    #[derive(Clone)]
    pub struct SqliteQueue {
        path: PathBuf,
    }

    impl SqliteQueue {
        pub fn default_db_path() -> PathBuf {
            if cfg!(target_os = "macos") {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home)
                    .join("Library/Application Support/Threadmill/IntegrationQueues/queues.db")
            } else if let Ok(state_home) = std::env::var("XDG_STATE_HOME") {
                PathBuf::from(state_home).join("threadmill/integration-queues/queues.db")
            } else {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home).join(".local/state/threadmill/integration-queues/queues.db")
            }
        }

        pub fn open(path: &Path) -> Result<Self> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create queue db parent {}", parent.display()))?;
            }
            let queue = Self {
                path: path.to_path_buf(),
            };
            let conn = queue.connect()?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.execute_batch(SCHEMA)?;
            let has_landing_fence = {
                let mut statement = conn.prepare("PRAGMA table_info(queue_items)")?;
                let columns = statement
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                columns.iter().any(|column| column == "landing_fenced")
            };
            if !has_landing_fence {
                conn.execute(
                    "ALTER TABLE queue_items ADD COLUMN landing_fenced INTEGER NOT NULL DEFAULT 0",
                    [],
                )?;
            }
            Ok(queue)
        }

        fn connect(&self) -> Result<Connection> {
            let conn = Connection::open(&self.path)
                .with_context(|| format!("open queue db {}", self.path.display()))?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.busy_timeout(std::time::Duration::from_millis(200))?;
            Ok(conn)
        }

        pub fn enqueue(&self, request: EnqueueRequest) -> Result<QueueItem> {
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
                id
            } else {
                let id = Uuid::new_v4().to_string();
                tx.execute(
                    "INSERT INTO queue_items (id,repo_key,repo_path,source_branch,target_branch,pr_url,producer_metadata_json,validation_evidence_json,status,current_head_sha,created_at,updated_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,'{}','ready',?8,?9,?9)",
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
                    ],
                )?;
                Self::record_event_tx(&tx, &id, "item_enqueued", "item enqueued")?;
                id
            };
            tx.commit()?;
            self.get_item(&item_id)
        }

        pub fn list_items(&self) -> Result<Vec<QueueItem>> {
            let conn = self.connect()?;
            let mut stmt = conn.prepare("SELECT * FROM queue_items ORDER BY created_at ASC")?;
            let items = stmt
                .query_map([], map_item)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(items)
        }

        pub fn get_item(&self, item_id: &str) -> Result<QueueItem> {
            let conn = self.connect()?;
            conn.query_row(
                "SELECT * FROM queue_items WHERE id=?1",
                params![item_id],
                map_item,
            )
            .with_context(|| format!("queue item not found: {item_id}"))
        }

        pub fn oldest_active_item(&self, repo_key: &str) -> Result<Option<QueueItem>> {
            let conn = self.connect()?;
            conn.query_row(
                "SELECT * FROM queue_items WHERE repo_key=?1 AND status NOT IN ('integrated','cancelled') ORDER BY created_at ASC, id ASC LIMIT 1",
                params![repo_key],
                map_item,
            )
            .optional()
            .with_context(|| format!("read oldest active item for repo queue {repo_key}"))
        }

        pub fn claim_next_ready(&self, repo_key: &str) -> Result<Option<(QueueItem, Attempt)>> {
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
            if item.status != QueueStatus::Ready {
                tx.commit()?;
                return Ok(None);
            }
            let attempt_id = Uuid::new_v4().to_string();
            let attempt_number: i64 = tx.query_row(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM integration_attempts WHERE item_id=?1",
                params![item.id],
                |row| row.get(0),
            )?;
            let now = now();
            tx.execute(
                "INSERT INTO integration_attempts (id,item_id,attempt_number,source_head_sha,started_at) VALUES (?1,?2,?3,?4,?5)",
                params![attempt_id, item.id, attempt_number, item.current_head_sha, now],
            )?;
            tx.execute(
                "UPDATE queue_items SET status='merging',current_attempt_id=?1,landing_fenced=0,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
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
            let conn = self.connect()?;
            conn.query_row(
                "SELECT * FROM queue_items WHERE repo_key=?1 AND status IN ('merging','merged','validating','validated','integrating') ORDER BY created_at ASC LIMIT 1",
                params![repo_key],
                map_item,
            )
            .optional()
            .with_context(|| format!("read next resumable active item for repo queue {repo_key}"))
        }

        pub fn transition_item(&self, item_id: &str, target: QueueStatus) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = tx
                .query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            StateMachine
                .transition(item.status, target)
                .map_err(anyhow::Error::msg)?;
            if target == QueueStatus::Cancelled && item.landing_fenced {
                anyhow::bail!(
                    "item {item_id} has crossed the landing fence and cannot be cancelled"
                );
            }
            if target == QueueStatus::Cancelled {
                tx.execute(
                    "UPDATE prompts SET status='cancelled' WHERE item_id=?1 AND status='open'",
                    params![item_id],
                )?;
            }
            tx.execute(
                "UPDATE queue_items SET status=?1,updated_at=?2 WHERE id=?3",
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

        pub(crate) fn authorize_execution_start(
            &self,
            item_id: &str,
            attempt_id: &str,
            expected_status: QueueStatus,
            release_gate: impl FnOnce() -> Result<()>,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (status, current_attempt_id): (String, Option<String>) = tx
                .query_row(
                    "SELECT status,current_attempt_id FROM queue_items WHERE id=?1",
                    params![item_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
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
            self.block_item_from_status(item_id, phase.into(), phase, reason, message)
        }

        pub(crate) fn block_integrating_recovery(
            &self,
            item_id: &str,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
        ) -> Result<String> {
            if !matches!(phase, BlockedPhase::Merging | BlockedPhase::Validating) {
                anyhow::bail!("integrating recovery may only resume merging or validating");
            }
            self.block_item_from_status(item_id, QueueStatus::Integrating, phase, reason, message)
        }

        fn block_item_from_status(
            &self,
            item_id: &str,
            expected_status: QueueStatus,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
        ) -> Result<String> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = tx
                .query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            if item.status != expected_status {
                anyhow::bail!(
                    "item {item_id} in status {} cannot block in {phase}",
                    item.status
                );
            }
            let timestamp = now();
            let prompt_id = if reason == BlockedReason::NeedsUserInput {
                let prompt_id = Uuid::new_v4().to_string();
                let options = if phase == BlockedPhase::Merging {
                    vec!["use source", "use target"]
                } else {
                    Vec::new()
                };
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

        pub fn answer_prompt(
            &self,
            prompt_id: &str,
            answer: &str,
            answered_by: &str,
        ) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let prompt = tx
                .query_row(
                    "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE id=?1",
                    params![prompt_id],
                    map_prompt,
                )
                .with_context(|| format!("prompt not found: {prompt_id}"))?;
            if prompt.status != "open" {
                anyhow::bail!("prompt {prompt_id} is not open")
            }
            let answer = answer.trim();
            let answered_by = answered_by.trim();
            if answer.is_empty() || answered_by.is_empty() {
                anyhow::bail!("prompt answer and answered_by must not be blank");
            }
            if !prompt.options.is_empty()
                && !prompt
                    .options
                    .iter()
                    .any(|option| option.eq_ignore_ascii_case(answer))
            {
                anyhow::bail!(
                    "unsupported answer for prompt {prompt_id}; choose {}",
                    prompt.options.join(", ")
                );
            }
            let item = tx
                .query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![prompt.item_id],
                    map_item,
                )
                .with_context(|| format!("queue item not found: {}", prompt.item_id))?;
            if item.status != QueueStatus::Blocked
                || item.blocked_reason != Some(BlockedReason::NeedsUserInput)
            {
                anyhow::bail!("item {} is not blocked for user input", item.id)
            }
            if item.prompt_id().as_deref() != Some(prompt_id) {
                anyhow::bail!("prompt {prompt_id} is not current for item {}", item.id)
            }
            let resume = StateMachine
                .resume_target(&BlockedState {
                    phase: prompt.blocked_phase,
                    reason: BlockedReason::NeedsUserInput,
                    prompt_id: Some(prompt_id.to_string()),
                })
                .map_err(anyhow::Error::msg)?;
            let timestamp = now();
            tx.execute(
                "UPDATE prompts SET status='answered',answer=?1,answered_by=?2,answered_at=?3 WHERE id=?4",
                params![answer, answered_by, timestamp, prompt_id],
            )?;
            tx.execute(
                "UPDATE queue_items SET status=?1,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
                params![resume.to_string(), timestamp, item.id],
            )?;
            Self::record_event_tx(&tx, &item.id, "user_answered", answer)?;
            tx.commit()?;
            self.get_item(&item.id)
        }

        pub(crate) fn accept_current_merge_resolution(
            &self,
            item_id: &str,
            answered_by: &str,
        ) -> Result<QueueItem> {
            let answered_by = answered_by.trim();
            if answered_by.is_empty() {
                anyhow::bail!("merge recovery actor must not be blank");
            }
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = tx
                .query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            if item.status != QueueStatus::Blocked
                || item.blocked_phase != Some(BlockedPhase::Merging)
                || item.blocked_reason != Some(BlockedReason::NeedsUserInput)
            {
                anyhow::bail!("item {item_id} is not blocked for merge resolution");
            }
            let prompt_id = item
                .prompt_id()
                .context("blocked merge item has no current prompt")?;
            let prompt = tx.query_row(
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE id=?1",
                params![prompt_id],
                map_prompt,
            )?;
            if prompt.status != "open"
                || prompt.item_id != item.id
                || prompt.blocked_phase != BlockedPhase::Merging
            {
                anyhow::bail!(
                    "merge recovery prompt {} is not current and open",
                    prompt.id
                );
            }
            let timestamp = now();
            tx.execute(
                "UPDATE prompts SET status='answered',answer='accept-current',answered_by=?1,answered_at=?2 WHERE id=?3",
                params![answered_by, timestamp, prompt.id],
            )?;
            tx.execute(
                "UPDATE queue_items SET status='merging',blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?1 WHERE id=?2",
                params![timestamp, item.id],
            )?;
            Self::record_event_tx(&tx, &item.id, "user_answered", "accept-current")?;
            tx.commit()?;
            self.get_item(&item.id)
        }

        pub fn requeue_agent_fix(&self, item_id: &str, new_head: &str) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = tx
                .query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            if item.status != QueueStatus::Blocked
                || item.blocked_reason != Some(BlockedReason::NeedsAgentFix)
            {
                anyhow::bail!("item {item_id} is not blocked for agent fix")
            }
            tx.execute(
                "UPDATE prompts SET status='superseded' WHERE item_id=?1 AND status='open'",
                params![item_id],
            )?;
            tx.execute(
                "UPDATE queue_items SET status='ready',current_head_sha=?1,landing_fenced=0,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
                params![new_head, now(), item_id],
            )?;
            Self::record_event_tx(&tx, item_id, "agent_requeued", "agent fix marked ready")?;
            tx.commit()?;
            self.get_item(item_id)
        }

        pub fn retry_blocked(&self, item_id: &str) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item = tx
                .query_row(
                    "SELECT * FROM queue_items WHERE id=?1",
                    params![item_id],
                    map_item,
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            if item.status != QueueStatus::Blocked {
                anyhow::bail!("item {item_id} is not blocked")
            }
            let phase = item
                .blocked_phase
                .ok_or_else(|| anyhow::anyhow!("blocked item {item_id} has no blocked phase"))?;
            let reason = item
                .blocked_reason
                .ok_or_else(|| anyhow::anyhow!("blocked item {item_id} has no blocked reason"))?;
            match reason {
                BlockedReason::NeedsUserInput => {
                    anyhow::bail!("item {item_id} requires an answered prompt before retry")
                }
                BlockedReason::NeedsAgentFix => {
                    anyhow::bail!("item {item_id} requires agent requeue before retry")
                }
                BlockedReason::Infra
                | BlockedReason::Dependency
                | BlockedReason::Credentials
                | BlockedReason::Provider => {}
            }
            let resume: QueueStatus = phase.into();
            tx.execute(
                "UPDATE queue_items SET status=?1,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
                params![resume.to_string(), now(), item_id],
            )?;
            Self::record_event_tx(
                &tx,
                item_id,
                "item_retried",
                &format!("retrying {phase} after {reason} block"),
            )?;
            tx.commit()?;
            self.get_item(item_id)
        }

        pub fn update_current_head(&self, item_id: &str, new_head: &str) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "UPDATE queue_items SET current_head_sha=?1,updated_at=?2 WHERE id=?3",
                params![new_head, now(), item_id],
            )?;
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

        pub fn heartbeat_repo_lease(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<bool> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE repo_leases SET heartbeat_at=?1,expires_at=?2 WHERE repo_key=?3 AND owner_id=?4",
                params![now(), (Utc::now() + Duration::seconds(ttl_seconds)).to_rfc3339(), repo_key, owner_id],
            )?;
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
            let status: String = tx
                .query_row(
                    "SELECT status FROM queue_items WHERE id=?1",
                    params![item_id],
                    |row| row.get(0),
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            if status != expected_status.to_string() {
                tx.commit()?;
                return Ok(false);
            }
            Self::record_event_tx(&tx, item_id, event_type, message)?;
            tx.commit()?;
            Ok(true)
        }

        pub fn events(&self, item_id: &str) -> Result<Vec<QueueEvent>> {
            let conn = self.connect()?;
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

        pub fn latest_answered_prompt(
            &self,
            item_id: &str,
            attempt_id: Option<&str>,
        ) -> Result<Option<Prompt>> {
            let conn = self.connect()?;
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
            let conn = self.connect()?;
            let mut stmt = conn.prepare(
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE item_id=?1 ORDER BY created_at ASC",
            )?;
            let prompts = stmt
                .query_map(params![item_id], map_prompt)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(prompts)
        }

        pub fn communication_bindings(&self, repo_key: &str) -> Result<Vec<CommunicationBinding>> {
            let conn = self.connect()?;
            let mut stmt = conn.prepare(
                "SELECT * FROM communication_bindings WHERE repo_key=?1 ORDER BY created_at ASC",
            )?;
            let bindings = stmt
                .query_map(params![repo_key], map_communication_binding)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(bindings)
        }

        pub fn communication_binding(
            &self,
            item_id: &str,
            transport_id: &str,
        ) -> Result<Option<CommunicationBinding>> {
            let conn = self.connect()?;
            conn.query_row(
                "SELECT * FROM communication_bindings WHERE item_id=?1 AND transport_id=?2",
                params![item_id, transport_id],
                map_communication_binding,
            )
            .optional()
            .context("read communication binding")
        }

        pub fn reserve_communication_binding(
            &self,
            repo_key: &str,
            item_id: &str,
            transport_id: &str,
            transport_kind: &str,
            endpoint_fingerprint: &str,
        ) -> Result<CommunicationBinding> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing = tx
                .query_row(
                    "SELECT * FROM communication_bindings WHERE item_id=?1 AND transport_id=?2",
                    params![item_id, transport_id],
                    map_communication_binding,
                )
                .optional()?;
            let id = if let Some(binding) = existing {
                if binding.repo_key != repo_key
                    || binding.transport_kind != transport_kind
                    || binding.endpoint_fingerprint != endpoint_fingerprint
                {
                    anyhow::bail!(
                        "communication transport {transport_id} changed identity while item {item_id} has a live binding"
                    );
                }
                binding.id
            } else {
                let id = Uuid::new_v4().to_string();
                let marker = format!("iq:binding:{id}");
                let timestamp = now();
                tx.execute(
                    "INSERT INTO communication_bindings (id,repo_key,item_id,transport_id,transport_kind,endpoint_fingerprint,marker,status,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,'pending_create',?8,?8)",
                    params![id, repo_key, item_id, transport_id, transport_kind, endpoint_fingerprint, marker, timestamp],
                )?;
                id
            };
            tx.commit()?;
            self.communication_binding(item_id, transport_id)?
                .with_context(|| {
                    format!("communication binding disappeared after reservation: {id}")
                })
        }

        pub fn activate_communication_binding(
            &self,
            binding_id: &str,
            external_ref: &Value,
            external_url: &str,
        ) -> Result<()> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE communication_bindings SET external_ref_json=?1,external_url=?2,status='active',last_error=NULL,updated_at=?3 WHERE id=?4",
                params![external_ref.to_string(), external_url, now(), binding_id],
            )?;
            if changed != 1 {
                anyhow::bail!("communication binding not found: {binding_id}");
            }
            Ok(())
        }

        pub fn set_communication_binding_status(
            &self,
            binding_id: &str,
            status: &str,
        ) -> Result<()> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE communication_bindings SET status=?1,last_error=NULL,updated_at=?2 WHERE id=?3",
                params![status, now(), binding_id],
            )?;
            if changed != 1 {
                anyhow::bail!("communication binding not found: {binding_id}");
            }
            Ok(())
        }

        pub fn record_communication_error(&self, binding_id: &str, error: &str) -> Result<()> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE communication_bindings SET last_error=?1,updated_at=?2 WHERE id=?3",
                params![error, now(), binding_id],
            )?;
            if changed != 1 {
                anyhow::bail!("communication binding not found: {binding_id}");
            }
            Ok(())
        }

        pub fn clear_communication_error(&self, binding_id: &str) -> Result<()> {
            let conn = self.connect()?;
            let changed = conn.execute(
                "UPDATE communication_bindings SET last_error=NULL,updated_at=?1 WHERE id=?2",
                params![now(), binding_id],
            )?;
            if changed != 1 {
                anyhow::bail!("communication binding not found: {binding_id}");
            }
            Ok(())
        }

        pub fn apply_communication_response(
            &self,
            binding_id: &str,
            external_response_id: &str,
            prompt_id: &str,
            answer: &str,
            actor: &str,
            authorized: bool,
        ) -> Result<CommunicationResponseDisposition> {
            let external_response_id = external_response_id.trim();
            if external_response_id.is_empty() {
                anyhow::bail!("communication response requires a stable external identity");
            }
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let seen: Option<String> = tx
                .query_row(
                    "SELECT disposition FROM communication_response_receipts WHERE binding_id=?1 AND external_response_id=?2",
                    params![binding_id, external_response_id],
                    |row| row.get(0),
                )
                .optional()?;
            if seen.is_some() {
                tx.commit()?;
                return Ok(CommunicationResponseDisposition::Duplicate);
            }

            let binding_item_id: String = tx
                .query_row(
                    "SELECT item_id FROM communication_bindings WHERE id=?1",
                    params![binding_id],
                    |row| row.get(0),
                )
                .with_context(|| format!("communication binding not found: {binding_id}"))?;

            let answer = answer.trim();
            let actor = actor.trim();
            let prompt = tx
                .query_row(
                    "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE id=?1",
                    params![prompt_id],
                    map_prompt,
                )
                .optional()?;
            let disposition = if !authorized || actor.is_empty() {
                CommunicationResponseDisposition::Unauthorized
            } else if answer.is_empty() {
                CommunicationResponseDisposition::Invalid
            } else if let Some(prompt) = prompt {
                let item = tx
                    .query_row(
                        "SELECT * FROM queue_items WHERE id=?1",
                        params![prompt.item_id],
                        map_item,
                    )
                    .optional()?;
                let answer_supported = !prompt.options.is_empty()
                    && prompt
                        .options
                        .iter()
                        .any(|option| option.eq_ignore_ascii_case(answer));
                if prompt.item_id != binding_item_id
                    || prompt.status != "open"
                    || item.as_ref().is_none_or(|item| {
                        item.status != QueueStatus::Blocked
                            || item.blocked_reason != Some(BlockedReason::NeedsUserInput)
                            || item.prompt_id().as_deref() != Some(prompt_id)
                    })
                {
                    CommunicationResponseDisposition::Stale
                } else if !answer_supported {
                    CommunicationResponseDisposition::Invalid
                } else {
                    let item = item.expect("checked above");
                    let resume = StateMachine
                        .resume_target(&BlockedState {
                            phase: prompt.blocked_phase,
                            reason: BlockedReason::NeedsUserInput,
                            prompt_id: Some(prompt_id.to_string()),
                        })
                        .map_err(anyhow::Error::msg)?;
                    let timestamp = now();
                    tx.execute(
                        "UPDATE prompts SET status='answered',answer=?1,answered_by=?2,answered_at=?3 WHERE id=?4",
                        params![answer, actor, timestamp, prompt_id],
                    )?;
                    tx.execute(
                        "UPDATE queue_items SET status=?1,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
                        params![resume.to_string(), timestamp, item.id],
                    )?;
                    Self::record_event_tx(&tx, &item.id, "user_answered", answer)?;
                    CommunicationResponseDisposition::Applied
                }
            } else {
                CommunicationResponseDisposition::Stale
            };
            tx.execute(
                "INSERT INTO communication_response_receipts (binding_id,external_response_id,prompt_id,answer,actor,disposition,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![binding_id, external_response_id, prompt_id, answer, actor, communication_disposition_text(disposition), now()],
            )?;
            tx.commit()?;
            Ok(disposition)
        }

        pub fn get_attempt(&self, attempt_id: &str) -> Result<Attempt> {
            let conn = self.connect()?;
            conn.query_row(
                "SELECT id,item_id,attempt_number,source_head_sha,target_base_sha,merge_commit_sha,validated_commit_sha,landed_commit_sha,validation_command,validation_exit_code,validation_log_path FROM integration_attempts WHERE id=?1",
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
                    })
                },
            ).with_context(|| format!("attempt not found: {attempt_id}"))
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

        pub fn mark_integrated(
            &self,
            item_id: &str,
            attempt_id: &str,
            landed_commit_sha: &str,
        ) -> Result<QueueItem> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item: (String, Option<String>, bool) = tx
                .query_row(
                    "SELECT status,current_attempt_id,landing_fenced FROM queue_items WHERE id=?1",
                    params![item_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            if item.0 != QueueStatus::Integrating.to_string()
                || item.1.as_deref() != Some(attempt_id)
                || !item.2
            {
                anyhow::bail!(
                    "item {item_id} is no longer integrating attempt {attempt_id}; refusing to mark landed"
                );
            }
            tx.execute(
                "UPDATE integration_attempts SET landed_commit_sha=?1,result='integrated',finished_at=?2 WHERE id=?3",
                params![landed_commit_sha, now(), attempt_id],
            )?;
            tx.execute(
                "UPDATE queue_items SET status='integrated',landed_commit_sha=?1,updated_at=?2 WHERE id=?3",
                params![landed_commit_sha, now(), item_id],
            )?;
            Self::record_event_tx(
                &tx,
                item_id,
                "integrated",
                &format!("landed {landed_commit_sha}"),
            )?;
            tx.commit()?;
            self.get_item(item_id)
        }

        pub fn begin_landing(&self, item_id: &str, attempt_id: &str) -> Result<()> {
            let mut conn = self.connect()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let item: (String, Option<String>, bool) = tx
                .query_row(
                    "SELECT status,current_attempt_id,landing_fenced FROM queue_items WHERE id=?1",
                    params![item_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .with_context(|| format!("queue item not found: {item_id}"))?;
            if item.0 != QueueStatus::Integrating.to_string()
                || item.1.as_deref() != Some(attempt_id)
            {
                anyhow::bail!(
                    "item {item_id} is no longer integrating attempt {attempt_id}; refusing target mutation"
                );
            }
            if !item.2 {
                tx.execute(
                    "UPDATE queue_items SET landing_fenced=1,updated_at=?1 WHERE id=?2",
                    params![now(), item_id],
                )?;
                Self::record_event_tx(
                    &tx,
                    item_id,
                    "landing_fenced",
                    "target mutation authorized; cancellation is now closed",
                )?;
            }
            tx.commit()?;
            Ok(())
        }

        pub fn set_workspace_path(&self, item_id: &str, path: &str) -> Result<()> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE queue_items SET integration_workspace_path=?1,updated_at=?2 WHERE id=?3",
                params![path, now(), item_id],
            )?;
            Ok(())
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
            let conn = self.connect()?;
            conn.query_row(
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer,options_json FROM prompts WHERE id=?1",
                params![prompt_id],
                map_prompt,
            ).with_context(|| format!("prompt not found: {prompt_id}"))
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

    impl QueueItem {
        fn prompt_id(&self) -> Option<String> {
            self.validation_evidence
                .get("prompt_id")
                .and_then(|value| value.as_str())
                .map(str::to_string)
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

    fn map_communication_binding(row: &Row<'_>) -> rusqlite::Result<CommunicationBinding> {
        Ok(CommunicationBinding {
            id: row.get("id")?,
            repo_key: row.get("repo_key")?,
            item_id: row.get("item_id")?,
            transport_id: row.get("transport_id")?,
            transport_kind: row.get("transport_kind")?,
            endpoint_fingerprint: row.get("endpoint_fingerprint")?,
            marker: row.get("marker")?,
            external_ref: parse_json_option(row, "external_ref_json")?,
            external_url: row.get("external_url")?,
            status: row.get("status")?,
            last_error: row.get("last_error")?,
        })
    }

    fn communication_disposition_text(
        disposition: CommunicationResponseDisposition,
    ) -> &'static str {
        match disposition {
            CommunicationResponseDisposition::Applied => "applied",
            CommunicationResponseDisposition::Duplicate => "duplicate",
            CommunicationResponseDisposition::Stale => "stale",
            CommunicationResponseDisposition::Invalid => "invalid",
            CommunicationResponseDisposition::Unauthorized => "unauthorized",
        }
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
            integration_workspace_path: row.get("integration_workspace_path")?,
            conflict: parse_json_option(row, "conflict_json")?,
            target_sha: row.get("target_sha")?,
            source_sha: row.get("source_sha")?,
            landed_commit_sha: row.get("landed_commit_sha")?,
            producer_metadata: parse_json_value(row, "producer_metadata_json")?,
            validation_evidence,
            landing_fenced: row.get("landing_fenced")?,
        })
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

    fn now() -> String {
        Utc::now().to_rfc3339()
    }

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
  target_sha TEXT,
  source_sha TEXT,
  landed_commit_sha TEXT,
  landing_fenced INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
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
  started_at TEXT NOT NULL,
  finished_at TEXT,
  result TEXT,
  UNIQUE(item_id, attempt_number)
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

CREATE TABLE IF NOT EXISTS repo_leases (
  repo_key TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);
"#;
}

pub mod integrator {
    use anyhow::{Context, Result};
    use serde::Deserialize;
    use serde_json::{json, Value as JsonValue};
    use std::collections::VecDeque;
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::io::{Read, Write};
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitStatus, Output, Stdio};
    use std::sync::mpsc;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration as StdDuration, Instant};
    use wait_timeout::ChildExt;

    use crate::core::{BlockedPhase, BlockedReason, QueueStatus};
    use crate::sqlite::{Attempt, QueueItem, SqliteQueue};

    #[derive(Clone, Debug)]
    pub struct IntegratorOptions {
        pub repo_key: String,
        pub repo_path: PathBuf,
        pub queue_db: PathBuf,
        pub owner_id: String,
        pub lease_ttl_seconds: i64,
        pub base_remote: String,
        pub workspace_root: PathBuf,
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct SignoffPolicy {
        pub command: String,
        pub repository: String,
        pub required_contexts: Vec<String>,
        pub trusted_creator: String,
    }

    #[derive(Clone, Debug)]
    pub struct IntegrationPolicy {
        pub validation_command: Option<String>,
        pub signoff: Option<SignoffPolicy>,
    }

    pub struct Integrator {
        queue: SqliteQueue,
        options: IntegratorOptions,
        policy: IntegrationPolicy,
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
                loop {
                    match stop_rx.recv_timeout(interval) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            if !queue.heartbeat_repo_lease(&repo_key, &owner_id, ttl_seconds)? {
                                anyhow::bail!("repo queue {repo_key} lease lost during heartbeat");
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
            let policy = IntegrationPolicy {
                validation_command: validation_command(&options.repo_path)?,
                signoff: None,
            };
            Self::new_with_policy(options, policy)
        }

        pub fn new_with_policy(
            mut options: IntegratorOptions,
            mut policy: IntegrationPolicy,
        ) -> Result<Self> {
            options.repo_path = options.repo_path.canonicalize().with_context(|| {
                format!(
                    "resolve configured repository {}",
                    options.repo_path.display()
                )
            })?;
            if options.repo_key.rsplit_once("::").is_none() {
                anyhow::bail!("repo_key must use <repository>::<target> scope");
            }
            policy.validation_command = policy
                .validation_command
                .map(|command| command.trim().to_string())
                .filter(|command| !command.is_empty());
            if let Some(signoff) = policy.signoff.as_mut() {
                signoff.command = signoff.command.trim().to_string();
                signoff.repository = signoff.repository.trim().to_string();
                signoff.trusted_creator = signoff.trusted_creator.trim().to_string();
                signoff.required_contexts = signoff
                    .required_contexts
                    .iter()
                    .map(|context| context.trim().to_string())
                    .filter(|context| !context.is_empty())
                    .fold(Vec::new(), |mut contexts, context| {
                        if !contexts.contains(&context) {
                            contexts.push(context);
                        }
                        contexts
                    });
                if signoff.command.is_empty()
                    || signoff.repository.is_empty()
                    || signoff.trusted_creator.is_empty()
                    || signoff.required_contexts.is_empty()
                {
                    anyhow::bail!(
                        "signoff policy requires command, repository, trusted_creator, and required_contexts"
                    );
                }
            }
            Ok(Self {
                queue: SqliteQueue::open(&options.queue_db)?,
                options,
                policy,
            })
        }

        pub fn run_once(&self) -> Result<Option<QueueItem>> {
            if !self.queue.acquire_repo_lease(
                &self.options.repo_key,
                &self.options.owner_id,
                self.options.lease_ttl_seconds,
            )? {
                return Ok(None);
            }
            let Some(active) = self.queue.oldest_active_item(&self.options.repo_key)? else {
                return Ok(None);
            };
            if let Some(blocked) = self.enforce_item_boundary(&active)? {
                return Ok(Some(blocked));
            }
            if active.status == QueueStatus::Blocked {
                return Ok(Some(active));
            }
            if active.status != QueueStatus::Ready {
                return self.resume_item(&active.id).map(Some);
            }
            let Some((item, attempt)) = self.queue.claim_next_ready(&self.options.repo_key)? else {
                return Ok(None);
            };
            let item = self.with_lease_heartbeat("merging", || self.merge_item(item, &attempt))?;
            if matches!(item.status, QueueStatus::Blocked | QueueStatus::Cancelled) {
                return Ok(Some(item));
            }
            let item =
                self.with_lease_heartbeat("validating", || self.validate_item(item, &attempt))?;
            if matches!(item.status, QueueStatus::Blocked | QueueStatus::Cancelled) {
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
            if !self.queue.acquire_repo_lease(
                &self.options.repo_key,
                &self.options.owner_id,
                self.options.lease_ttl_seconds,
            )? {
                return Ok(None);
            }
            self.with_lease_heartbeat("communication", operation)
                .map(Some)
        }

        pub fn resume_item(&self, item_id: &str) -> Result<QueueItem> {
            if !self.queue.acquire_repo_lease(
                &self.options.repo_key,
                &self.options.owner_id,
                self.options.lease_ttl_seconds,
            )? {
                anyhow::bail!(
                    "repo queue {} is leased by another integrator",
                    self.options.repo_key
                );
            }
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
            match item.status {
                QueueStatus::Merging => {
                    let has_conflict_prompt = self
                        .queue
                        .prompts_for_item(&item.id)?
                        .into_iter()
                        .any(|prompt| {
                            prompt.attempt_id.as_deref() == Some(attempt.id.as_str())
                                && prompt.blocked_phase == BlockedPhase::Merging
                        });
                    let item = self.with_lease_heartbeat("merging", || {
                        if has_conflict_prompt {
                            self.resume_merge(item, &attempt)
                        } else {
                            self.merge_item(item, &attempt)
                        }
                    })?;
                    if matches!(item.status, QueueStatus::Blocked | QueueStatus::Cancelled) {
                        return Ok(item);
                    }
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if matches!(item.status, QueueStatus::Blocked | QueueStatus::Cancelled) {
                        return Ok(item);
                    }
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Merged => {
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if matches!(item.status, QueueStatus::Blocked | QueueStatus::Cancelled) {
                        return Ok(item);
                    }
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Validating => {
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if matches!(item.status, QueueStatus::Blocked | QueueStatus::Cancelled) {
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
                &self.options.owner_id,
                self.options.lease_ttl_seconds,
            )? {
                Ok(())
            } else {
                anyhow::bail!(
                    "repo queue {} lease is no longer owned by {}",
                    self.options.repo_key,
                    self.options.owner_id
                )
            }
        }

        fn with_lease_heartbeat<T>(
            &self,
            phase: &str,
            operation: impl FnOnce() -> Result<T>,
        ) -> Result<T> {
            self.ensure_repo_lease()?;
            let guard = LeaseHeartbeat::start(
                self.queue.clone(),
                self.options.repo_key.clone(),
                self.options.owner_id.clone(),
                self.options.lease_ttl_seconds,
            );
            let result = operation();
            let lease_result = guard.finish(phase).and_then(|_| self.ensure_repo_lease());
            match (result, lease_result) {
                (Ok(value), Ok(())) => Ok(value),
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
            }
        }

        fn transition_item_owned(&self, item_id: &str, target: QueueStatus) -> Result<QueueItem> {
            self.ensure_repo_lease()?;
            match self.queue.transition_item(item_id, target) {
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

        fn item_cancelled(&self, item_id: &str) -> Result<bool> {
            Ok(self.queue.get_item(item_id)?.status == QueueStatus::Cancelled)
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
        ) -> Result<Option<QueueItem>> {
            self.ensure_repo_lease()?;
            match self.queue.begin_landing(item_id, attempt_id) {
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
            self.queue.block_item(item_id, phase, reason, message)
        }

        fn mark_integrated_owned(
            &self,
            item_id: &str,
            attempt_id: &str,
            landed_commit_sha: &str,
        ) -> Result<QueueItem> {
            self.ensure_repo_lease()?;
            self.queue
                .mark_integrated(item_id, attempt_id, landed_commit_sha)
        }

        fn merge_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            if let Err(error) = self.fetch_target(&item) {
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
            let source_sha = match self.fetch_source(&item) {
                Ok(()) => self.source_remote_sha(&item)?,
                Err(error) => {
                    self.block_item_owned(
                        &item.id,
                        BlockedPhase::Merging,
                        BlockedReason::Infra,
                        &format!(
                            "failed to fetch source branch {}: {error}",
                            item.source_branch
                        ),
                    )?;
                    return self.queue.get_item(&item.id);
                }
            };
            if source_sha != item.current_head_sha {
                self.ensure_repo_lease()?;
                self.queue.record_event(
                    &item.id,
                    "source_head_mismatch",
                    &format!(
                        "source branch {} resolved to {}, queued head is {}",
                        item.source_branch, source_sha, item.current_head_sha
                    ),
                )?;
                self.block_item_owned(
                    &item.id,
                    BlockedPhase::Merging,
                    BlockedReason::NeedsAgentFix,
                    "source branch head does not match queued source head",
                )?;
                return self.queue.get_item(&item.id);
            }

            let workspace = self.workspace_path(&item);
            self.remove_stale_workspace(&workspace)?;
            fs::create_dir_all(&self.options.workspace_root)?;
            git(
                &self.options.repo_path,
                [
                    "worktree",
                    "add",
                    "--detach",
                    path_arg(&workspace),
                    &base_sha,
                ],
            )?;
            self.ensure_repo_lease()?;
            self.queue
                .set_workspace_path(&item.id, &workspace.to_string_lossy())?;

            let merge = git_status(&workspace, ["merge", "--no-ff", "--no-commit", &source_sha])?;
            if !merge.status.success() {
                let conflict_files =
                    git_output(&workspace, ["diff", "--name-only", "--diff-filter=U"])
                        .unwrap_or_default()
                        .lines()
                        .map(str::to_string)
                        .collect::<Vec<_>>();
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
                self.block_item_owned(
                    &item.id,
                    BlockedPhase::Merging,
                    BlockedReason::NeedsUserInput,
                    "merge conflict requires user resolution",
                )?;
                return self.queue.get_item(&item.id);
            }

            let merge_in_progress =
                git_status(&workspace, ["rev-parse", "--verify", "MERGE_HEAD"])?
                    .status
                    .success();
            let diff = git_status(&workspace, ["diff", "--cached", "--quiet"])?;
            if merge_in_progress || !diff.status.success() {
                git(
                    &workspace,
                    ["commit", "-m", &format!("Integrate queue item {}", item.id)],
                )?;
            }
            let merge_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
            self.ensure_repo_lease()?;
            self.queue.update_attempt_merge(&attempt.id, &merge_sha)?;
            self.transition_item_owned(&item.id, QueueStatus::Merged)
        }

        fn resume_merge(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            let prompt = self
                .queue
                .prompts_for_item(&item.id)?
                .into_iter()
                .rfind(|prompt| prompt.attempt_id.as_deref() == Some(attempt.id.as_str()))
                .with_context(|| {
                    format!(
                        "item {} cannot resume merge without a prompt for attempt {}",
                        item.id, attempt.id
                    )
                })?;
            if prompt.status != "answered" {
                anyhow::bail!(
                    "item {} cannot resume merge because current prompt {} is {}",
                    item.id,
                    prompt.id,
                    prompt.status
                );
            }
            if prompt.blocked_phase != BlockedPhase::Merging {
                anyhow::bail!(
                    "item {} cannot resume merge from {} prompt {}",
                    item.id,
                    prompt.blocked_phase,
                    prompt.id
                );
            }
            let prompt_id = prompt.id.clone();
            let answer = prompt.answer.with_context(|| {
                format!(
                    "item {} cannot resume merge because answered prompt {} has no answer",
                    item.id, prompt_id
                )
            })?;

            self.resume_merge_with_answer(item, attempt, &answer)
        }

        fn resume_merge_with_answer(
            &self,
            item: QueueItem,
            attempt: &Attempt,
            answer: &str,
        ) -> Result<QueueItem> {
            let workspace = item
                .integration_workspace_path
                .as_ref()
                .map(PathBuf::from)
                .context("item missing integration workspace path")?;
            if !workspace.exists() {
                self.block_item_owned(
                    &item.id,
                    BlockedPhase::Merging,
                    BlockedReason::Infra,
                    "integration workspace is missing during merge resume",
                )?;
                return self.queue.get_item(&item.id);
            }
            self.apply_merge_answer(&workspace, &item, answer)?;
            let unresolved = conflict_files(&workspace)?;
            if !unresolved.is_empty() {
                self.ensure_repo_lease()?;
                self.queue.set_conflict_metadata(
                    &item.id,
                    &json!({
                        "files": unresolved,
                        "summary": "merge still has unresolved conflict files",
                        "workspace_path": workspace,
                    }),
                    item.target_sha.as_deref().unwrap_or_default(),
                    item.source_sha.as_deref().unwrap_or_default(),
                )?;
                self.block_item_owned(
                    &item.id,
                    BlockedPhase::Merging,
                    BlockedReason::NeedsUserInput,
                    "merge conflict remains unresolved",
                )?;
                return self.queue.get_item(&item.id);
            }
            let staged = git_status(&workspace, ["diff", "--cached", "--quiet"])?;
            let merge_in_progress =
                git_status(&workspace, ["rev-parse", "--verify", "MERGE_HEAD"])?
                    .status
                    .success();
            if merge_in_progress || !staged.status.success() {
                git(
                    &workspace,
                    ["commit", "-m", &format!("Resolve queue item {}", item.id)],
                )?;
            }
            let merge_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
            self.ensure_repo_lease()?;
            self.queue.update_attempt_merge(&attempt.id, &merge_sha)?;
            self.queue.clear_conflict_metadata(&item.id)?;
            self.queue.record_event(
                &item.id,
                "merge_resumed",
                "merge resumed from answered prompt",
            )?;
            self.transition_item_owned(&item.id, QueueStatus::Merged)
        }

        fn apply_merge_answer(
            &self,
            workspace: &Path,
            item: &QueueItem,
            answer: &str,
        ) -> Result<()> {
            let normalized = answer.trim().to_ascii_lowercase();
            let conflicts = conflict_files(workspace)?;
            if normalized == "accept-current" || normalized == "resolved" || normalized == "done" {
                return Ok(());
            }
            if conflicts.is_empty() {
                return Ok(());
            }
            let (checkout_arg, stage) = match normalized.as_str() {
                "use source" | "source" | "theirs" | "accept-theirs" => ("--theirs", 3),
                "use target" | "target" | "ours" | "accept-ours" => ("--ours", 2),
                _ => anyhow::bail!(
                    "unsupported merge answer for item {}: {answer}; use accept-current, use source, or use target",
                    item.id
                ),
            };
            for file in &conflicts {
                let selected_stage = format!(":{stage}:{file}");
                if git_status(workspace, ["cat-file", "-e", selected_stage.as_str()])?
                    .status
                    .success()
                {
                    git(workspace, ["checkout", checkout_arg, "--", file.as_str()])?;
                    git(workspace, ["add", "--", file.as_str()])?;
                } else {
                    let relative = Path::new(file);
                    if !relative
                        .components()
                        .all(|component| matches!(component, std::path::Component::Normal(_)))
                    {
                        anyhow::bail!("merge conflict path is not checkout-relative: {file}");
                    }
                    let selected_path = workspace.join(relative);
                    if let Ok(metadata) = fs::symlink_metadata(&selected_path) {
                        if metadata.is_dir() {
                            fs::remove_dir_all(&selected_path)?;
                        } else {
                            fs::remove_file(&selected_path)?;
                        }
                    }
                    git(
                        workspace,
                        ["update-index", "--force-remove", "--", file.as_str()],
                    )?;
                }
            }
            let unresolved = conflict_files(workspace)?;
            if !unresolved.is_empty() {
                anyhow::bail!(
                    "merge answer for item {} left unresolved paths: {}",
                    item.id,
                    unresolved.join(", ")
                );
            }
            Ok(())
        }

        fn validate_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            let workspace = item
                .integration_workspace_path
                .as_ref()
                .map(PathBuf::from)
                .context("item missing integration workspace path")?;
            let item = if item.status == QueueStatus::Merged {
                self.transition_item_owned(&item.id, QueueStatus::Validating)?
            } else if item.status != QueueStatus::Validating {
                anyhow::bail!("item {} in status {} cannot validate", item.id, item.status);
            } else {
                item
            };
            if item.status == QueueStatus::Cancelled {
                return Ok(item);
            }
            let command = match self.policy.validation_command.clone() {
                Some(command) => command,
                None => {
                    return self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::NeedsUserInput,
                        "missing integration validation command",
                    );
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
            let log_dir = self.evidence_dir(&item, attempt);
            fs::create_dir_all(&log_dir)?;
            let log_path = log_dir.join("validation.log");
            let outcome = match run_evidence_command(
                &command,
                &workspace,
                &log_path,
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
                || self.item_cancelled(&item.id),
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
            self.transition_item_owned(&item.id, QueueStatus::Validated)
        }

        fn integrate_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            if let Some(blocked) = self.enforce_item_boundary(&item)? {
                return Ok(blocked);
            }
            let item = if item.status == QueueStatus::Validated {
                self.transition_item_owned(&item.id, QueueStatus::Integrating)?
            } else if item.status != QueueStatus::Integrating {
                anyhow::bail!(
                    "item {} in status {} cannot integrate",
                    item.id,
                    item.status
                );
            } else {
                item
            };
            if item.status == QueueStatus::Cancelled {
                return Ok(item);
            }
            if let Some(pr_url) = item.pr_url.clone() {
                if self.policy.signoff.is_none() {
                    return self.integrate_provider_item(item, attempt, &pr_url);
                }
            }
            let workspace = item
                .integration_workspace_path
                .as_ref()
                .map(PathBuf::from)
                .context("item missing integration workspace path")?;
            if let Err(error) = self.fetch_target(&item) {
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
            let attempt_base = self.queue.get_attempt(&attempt.id)?.target_base_sha;
            if attempt_base.as_deref() != Some(remote_sha.as_str()) {
                if let Some(blocked) = self.merge_moved_base(
                    &item,
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
            if let Some(signoff) = &self.policy.signoff {
                if let Some(cancelled) = self.cancelled_item(&item.id)? {
                    return Ok(cancelled);
                }
                if let Some(blocked) =
                    self.sign_candidate(&item, attempt, &workspace, &landed_sha, signoff)?
                {
                    return Ok(blocked);
                }
                if let Some(cancelled) = self.cancelled_item(&item.id)? {
                    return Ok(cancelled);
                }
                if let Err(error) = self.fetch_target(&item) {
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
                            "target moved from {remote_sha} to {target_after_signoff}; rebuilding and resigning candidate"
                        ),
                    )?;
                    if let Some(blocked) = self.merge_moved_base(
                        &item,
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
            }
            if let Some(cancelled) = self.cancelled_item(&item.id)? {
                return Ok(cancelled);
            }
            if let Err(error) = self.verify_candidate_graph(&item, attempt, &workspace) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate graph is invalid before target push: {error}"),
                );
            }
            if let Some(dirty) = workspace_dirty(&workspace)? {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate is dirty before target push: {dirty}"),
                );
            }
            if let Some(cancelled) = self.begin_landing_owned(&item.id, &attempt.id)? {
                return Ok(cancelled);
            }
            let target_ref = format!("refs/heads/{}", item.target_branch);
            let push_ref = format!("HEAD:{target_ref}");
            let lease = format!("--force-with-lease={target_ref}:{remote_sha}");
            if let Err(error) = git(
                &workspace,
                ["push", lease.as_str(), &self.options.base_remote, &push_ref],
            ) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("failed to push direct landing commit {landed_sha}: {error}"),
                );
            }
            if let Err(error) = self.fetch_target(&item) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!("failed to fetch target after direct landing push: {error}"),
                );
            }
            if let Err(error) = git(
                &self.options.repo_path,
                ["merge-base", "--is-ancestor", &landed_sha, &remote_ref],
            ) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Infra,
                    &format!(
                        "remote target does not contain direct-landed commit {landed_sha}: {error}"
                    ),
                );
            }
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
            self.mark_integrated_owned(&item.id, &attempt.id, &landed_sha)
        }

        fn sign_candidate(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            candidate_sha: &str,
            policy: &SignoffPolicy,
        ) -> Result<Option<QueueItem>> {
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
            git(workspace, ["switch", "-C", &candidate_branch])?;
            let candidate_push_ref = format!("HEAD:refs/heads/{candidate_branch}");
            if let Err(error) = git(
                workspace,
                [
                    "push",
                    "--force",
                    "--set-upstream",
                    &self.options.base_remote,
                    &candidate_push_ref,
                ],
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

            let log_dir = self.evidence_dir(item, attempt);
            fs::create_dir_all(&log_dir)?;
            let log_path = log_dir.join("signoff.log");
            let outcome = run_evidence_command(
                &policy.command,
                workspace,
                &log_path,
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
                || self.item_cancelled(&item.id),
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
                || self.item_cancelled(item_id),
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
            let current_attempt = self.queue.get_attempt(&attempt.id)?;
            let head = git_output(workspace, ["rev-parse", "HEAD"])?;
            let validated = current_attempt
                .validated_commit_sha
                .context("attempt has no validated candidate SHA")?;
            if head != validated {
                anyhow::bail!("workspace HEAD {head} differs from validated SHA {validated}");
            }
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
            let target_base = current_attempt
                .target_base_sha
                .context("attempt has no target base SHA")?;
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
            Ok(())
        }

        fn merge_moved_base(
            &self,
            item: &QueueItem,
            workspace: &Path,
            moved_base_sha: &str,
            summary_prefix: &str,
        ) -> Result<Option<QueueItem>> {
            let source_sha = match git_output(workspace, ["rev-parse", "HEAD"]) {
                Ok(sha) => sha,
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Infra,
                        &format!("cannot resolve candidate before moved-base merge: {error}"),
                    )?));
                }
            };
            git(workspace, ["reset", "--hard", moved_base_sha])?;
            let merge = git_status(workspace, ["merge", "--no-edit", &source_sha])?;
            if merge.status.success() {
                self.ensure_repo_lease()?;
                return Ok(None);
            }
            let conflict_files = git_output(workspace, ["diff", "--name-only", "--diff-filter=U"])
                .unwrap_or_default()
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let conflict_json = json!({
                "files": conflict_files,
                "summary": format!(
                    "{summary_prefix}: {}",
                    String::from_utf8_lossy(&merge.stderr).trim()
                ),
                "target_sha": moved_base_sha,
                "source_sha": source_sha,
                "workspace_path": workspace,
            });
            self.queue.set_conflict_metadata(
                &item.id,
                &conflict_json,
                moved_base_sha,
                &source_sha,
            )?;
            Ok(Some(self.block_and_get(
                &item.id,
                BlockedPhase::Merging,
                BlockedReason::NeedsUserInput,
                "target branch moved and merge requires user resolution",
            )?))
        }

        fn revalidate_moved_base(
            &self,
            item: &QueueItem,
            attempt: &Attempt,
            workspace: &Path,
            moved_base_sha: &str,
            label: &str,
        ) -> Result<Option<QueueItem>> {
            let command = match self.policy.validation_command.clone() {
                Some(command) => command,
                None => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::NeedsUserInput,
                        &format!("missing integration.validation.command after {label}"),
                    )?));
                }
            };
            let log_dir = self.evidence_dir(item, attempt);
            fs::create_dir_all(&log_dir)?;
            let safe_label = label.replace([' ', '/'], "-");
            let log_path = log_dir.join(format!("revalidation-after-{safe_label}.log"));
            if let Some(dirty) = workspace_dirty(workspace)? {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("candidate is dirty before revalidation after {label}: {dirty}"),
                )?));
            }
            let outcome = match run_evidence_command(
                &command,
                workspace,
                &log_path,
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
                || self.item_cancelled(&item.id),
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
            let result = if item.status == QueueStatus::Integrating
                && matches!(phase, BlockedPhase::Merging | BlockedPhase::Validating)
            {
                self.ensure_repo_lease()?;
                self.queue
                    .block_integrating_recovery(item_id, phase, reason, message)
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

        fn integrate_provider_item(
            &self,
            mut item: QueueItem,
            attempt: &Attempt,
            pr_url: &str,
        ) -> Result<QueueItem> {
            let provider = crate::providers::provider_for_url(pr_url)?;
            match self.push_provider_resolution_branch_if_needed(&item) {
                Ok(Some(updated)) => item = updated,
                Ok(None) => {}
                Err(error) => {
                    self.block_item_owned(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        &format!("failed to push PR/MR conflict resolution: {error}"),
                    )?;
                    return self.queue.get_item(&item.id);
                }
            }
            let snapshot = match provider.snapshot(pr_url) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.block_item_owned(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        &format!("provider snapshot failed: {error}"),
                    )?;
                    return self.queue.get_item(&item.id);
                }
            };
            if snapshot.head_sha != item.current_head_sha {
                self.block_item_owned(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::NeedsAgentFix,
                    "PR/MR head does not match queued source head",
                )?;
                return self.queue.get_item(&item.id);
            }
            match snapshot.gate {
                crate::providers::ProviderGate::Pass => {}
                crate::providers::ProviderGate::Pending(message) => {
                    self.block_item_owned(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        &message,
                    )?;
                    return self.queue.get_item(&item.id);
                }
                crate::providers::ProviderGate::Fail(message) => {
                    self.block_item_owned(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::NeedsAgentFix,
                        &message,
                    )?;
                    return self.queue.get_item(&item.id);
                }
            }

            let attempt_base = self.queue.get_attempt(&attempt.id)?.target_base_sha;
            if attempt_base.as_deref() != Some(snapshot.base_sha.as_str()) {
                let workspace = item
                    .integration_workspace_path
                    .as_ref()
                    .map(PathBuf::from)
                    .context("item missing integration workspace path")?;
                if let Some(blocked) = self.merge_moved_base(
                    &item,
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

            if let Some(cancelled) = self.begin_landing_owned(&item.id, &attempt.id)? {
                return Ok(cancelled);
            }
            let merge_result = match provider.merge(pr_url, &item.current_head_sha) {
                Ok(result) => result,
                Err(error) => {
                    self.block_item_owned(
                        &item.id,
                        BlockedPhase::Integrating,
                        BlockedReason::Provider,
                        &format!("provider merge failed: {error}"),
                    )?;
                    return self.queue.get_item(&item.id);
                }
            };
            if let Err(error) = self.fetch_target(&item) {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Provider,
                    &format!("failed to fetch target after provider merge: {error}"),
                );
            }
            let remote_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.target_branch
            );
            let Some(landed_sha) = merge_result.landed_sha else {
                return self.block_and_get(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Provider,
                    "provider merge did not report the exact landed commit SHA",
                );
            };
            if let Err(error) = git(
                &self.options.repo_path,
                ["merge-base", "--is-ancestor", &landed_sha, &remote_ref],
            ) {
                self.block_item_owned(
                    &item.id,
                    BlockedPhase::Integrating,
                    BlockedReason::Provider,
                    &format!(
                        "remote target does not contain provider-landed commit/source head {landed_sha}: {error}"
                    ),
                )?;
                return self.queue.get_item(&item.id);
            }
            self.mark_integrated_owned(&item.id, &attempt.id, &landed_sha)
        }

        fn push_provider_resolution_branch_if_needed(
            &self,
            item: &QueueItem,
        ) -> Result<Option<QueueItem>> {
            let events = self.queue.events(&item.id)?;
            if !events
                .iter()
                .any(|event| event.event_type == "merge_resumed")
            {
                return Ok(None);
            }
            let workspace = item
                .integration_workspace_path
                .as_ref()
                .map(PathBuf::from)
                .context("item missing integration workspace path")?;
            let workspace_head = git_output(&workspace, ["rev-parse", "HEAD"])?;
            if workspace_head == item.current_head_sha {
                return Ok(None);
            }
            let push_ref = format!("HEAD:refs/heads/{}", item.source_branch);
            git(&workspace, ["push", &self.options.base_remote, &push_ref])?;
            git(
                &self.options.repo_path,
                ["fetch", &self.options.base_remote, &item.source_branch],
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
            let items = self.queue.list_items()?;
            let mut statuses = Vec::new();
            for item in items
                .into_iter()
                .filter(|item| item.repo_key == self.options.repo_key)
            {
                let Some(path) = item.integration_workspace_path.as_ref().map(PathBuf::from) else {
                    continue;
                };
                let exists = path.exists();
                let dirty = exists && !git_output(&path, ["status", "--porcelain"])?.is_empty();
                let conflict_files = if exists {
                    conflict_files(&path)?
                } else {
                    Vec::new()
                };
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

        pub fn accept_current_workspace(&self, item_id: &str) -> Result<QueueItem> {
            if !self.queue.acquire_repo_lease(
                &self.options.repo_key,
                &self.options.owner_id,
                self.options.lease_ttl_seconds,
            )? {
                anyhow::bail!(
                    "repo queue {} is leased by another integrator",
                    self.options.repo_key
                );
            }
            let item = self.queue.get_item(item_id)?;
            let workspace = item
                .integration_workspace_path
                .as_ref()
                .map(PathBuf::from)
                .context("item missing integration workspace path")?;
            let unresolved = conflict_files(&workspace)?;
            if !unresolved.is_empty() {
                anyhow::bail!(
                    "cannot accept merge workspace with unresolved paths: {}",
                    unresolved.join(", ")
                );
            }
            let attempt_id = item
                .current_attempt_id
                .as_deref()
                .context("item has no current attempt")?;
            let attempt = self.queue.get_attempt(attempt_id)?;
            let item = self
                .queue
                .accept_current_merge_resolution(item_id, &self.options.owner_id)?;
            self.with_lease_heartbeat("merging", || {
                self.resume_merge_with_answer(item, &attempt, "accept-current")
            })
        }

        pub fn reset_workspaces(&self) -> Result<Vec<PathBuf>> {
            let statuses = self.workspace_status()?;
            let mut removed = Vec::new();
            for status in statuses {
                if status.exists
                    && status.status != QueueStatus::Blocked
                    && status.status != QueueStatus::Merging
                {
                    fs::remove_dir_all(&status.path)
                        .with_context(|| format!("remove workspace {}", status.path.display()))?;
                    removed.push(status.path);
                }
            }
            Ok(removed)
        }

        fn fetch_target(&self, item: &QueueItem) -> Result<()> {
            git(
                &self.options.repo_path,
                ["fetch", &self.options.base_remote, &item.target_branch],
            )
        }

        fn fetch_source(&self, item: &QueueItem) -> Result<()> {
            let refspec = format!(
                "+refs/heads/{}:refs/remotes/{}/{}",
                item.source_branch, self.options.base_remote, item.source_branch
            );
            git(
                &self.options.repo_path,
                ["fetch", &self.options.base_remote, &refspec],
            )
        }

        fn source_remote_sha(&self, item: &QueueItem) -> Result<String> {
            let source_ref = format!(
                "refs/remotes/{}/{}",
                self.options.base_remote, item.source_branch
            );
            git_output(&self.options.repo_path, ["rev-parse", &source_ref])
        }

        fn workspace_path(&self, item: &QueueItem) -> PathBuf {
            let safe_id = item.id.replace('/', "-");
            self.options.workspace_root.join(safe_id)
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
            self.block_item_owned(
                &item.id,
                phase,
                BlockedReason::NeedsAgentFix,
                &format!(
                    "queued repository/target {}::{} does not match host policy {}::{}; cancel and enqueue on the correct queue",
                    queued_repo.display(),
                    item.target_branch,
                    self.options.repo_path.display(),
                    expected_target
                ),
            )?;
            self.queue.get_item(&item.id).map(Some)
        }

        fn remove_stale_workspace(&self, workspace: &Path) -> Result<()> {
            let registered =
                git_output(&self.options.repo_path, ["worktree", "list", "--porcelain"])?
                    .lines()
                    .filter_map(|line| line.strip_prefix("worktree "))
                    .map(Path::new)
                    .any(|path| path == workspace);
            if registered {
                git(
                    &self.options.repo_path,
                    ["worktree", "remove", "--force", path_arg(workspace)],
                )?;
            } else if workspace.exists() {
                fs::remove_dir_all(workspace)
                    .with_context(|| format!("remove stale workspace {}", workspace.display()))?;
            }
            git(&self.options.repo_path, ["worktree", "prune"])
        }

        fn evidence_dir(&self, item: &QueueItem, attempt: &Attempt) -> PathBuf {
            let safe_item = item.id.replace('/', "-");
            let safe_attempt = attempt.id.replace('/', "-");
            self.options
                .workspace_root
                .join(".evidence")
                .join(safe_item)
                .join(safe_attempt)
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

    #[derive(Debug, Deserialize)]
    struct IqConfig {
        integration: Option<IntegrationConfig>,
    }

    #[derive(Debug, Deserialize)]
    struct IntegrationConfig {
        validation: Option<ValidationConfig>,
    }

    #[derive(Debug, Deserialize)]
    struct ValidationConfig {
        command: Option<String>,
    }

    pub fn validation_command(repo_path: &Path) -> Result<Option<String>> {
        let config_path = repo_path.join(".iq/config.json");
        if config_path.exists() {
            let contents = fs::read_to_string(&config_path)
                .with_context(|| format!("read {}", config_path.display()))?;
            let parsed: IqConfig = serde_json::from_str(&contents)
                .with_context(|| format!("parse {}", config_path.display()))?;
            if let Some(command) = parsed
                .integration
                .and_then(|integration| integration.validation)
                .and_then(|validation| validation.command)
                .filter(|command| !command.trim().is_empty())
            {
                return Ok(Some(command));
            }
        }

        default_validation_command(repo_path)
    }

    fn default_validation_command(repo_path: &Path) -> Result<Option<String>> {
        if taskfile_has_validate(repo_path)? {
            return Ok(Some("task validate".into()));
        }
        if makefile_has_validate(repo_path)? {
            return Ok(Some("make validate".into()));
        }
        if repo_path.join("Cargo.toml").exists() {
            return Ok(Some("cargo test".into()));
        }
        if let Some(command) = package_json_validation_command(repo_path)? {
            return Ok(Some(command));
        }
        Ok(None)
    }

    fn taskfile_has_validate(repo_path: &Path) -> Result<bool> {
        for name in [
            "Taskfile.yml",
            "Taskfile.yaml",
            "Taskfile.dist.yml",
            "Taskfile.dist.yaml",
        ] {
            let path = repo_path.join(name);
            if path.exists() && yaml_has_top_level_task(&path, "validate")? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn makefile_has_validate(repo_path: &Path) -> Result<bool> {
        for name in ["Makefile", "makefile", "GNUmakefile"] {
            let path = repo_path.join(name);
            if !path.exists() {
                continue;
            }
            let contents =
                fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
            if contents
                .lines()
                .any(|line| line.starts_with("validate:") || line.starts_with("validate::"))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn yaml_has_top_level_task(path: &Path, task_name: &str) -> Result<bool> {
        let contents =
            fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(&contents).with_context(|| format!("parse {}", path.display()))?;
        Ok(parsed
            .get("tasks")
            .and_then(|tasks| tasks.as_mapping())
            .map(|tasks| tasks.contains_key(serde_yaml::Value::String(task_name.into())))
            .unwrap_or(false))
    }

    fn package_json_validation_command(repo_path: &Path) -> Result<Option<String>> {
        let path = repo_path.join("package.json");
        if !path.exists() {
            return Ok(None);
        }
        let contents =
            fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: JsonValue =
            serde_json::from_str(&contents).with_context(|| format!("parse {}", path.display()))?;
        let scripts = parsed.get("scripts").and_then(JsonValue::as_object);
        let Some(scripts) = scripts else {
            return Ok(None);
        };
        if scripts.contains_key("validate") {
            return Ok(Some(format!("{} run validate", package_manager(repo_path))));
        }
        if scripts.contains_key("test") {
            return Ok(Some(format!("{} test", package_manager(repo_path))));
        }
        Ok(None)
    }

    fn package_manager(repo_path: &Path) -> &'static str {
        if repo_path.join("bun.lock").exists() || repo_path.join("bun.lockb").exists() {
            "bun"
        } else if repo_path.join("pnpm-lock.yaml").exists() {
            "pnpm"
        } else if repo_path.join("yarn.lock").exists() {
            "yarn"
        } else {
            "npm"
        }
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

    fn command_output_timeout<I, S>(
        program: &str,
        args: I,
        timeout: StdDuration,
        authorize_start: impl FnOnce(&mut dyn Write) -> Result<bool>,
        mut is_cancelled: impl FnMut() -> Result<bool>,
    ) -> Result<CommandOutputOutcome>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        const POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);
        const CANCELLATION_GRACE: StdDuration = StdDuration::from_millis(50);
        const TIMEOUT_GRACE: StdDuration = StdDuration::from_secs(5);

        let mut process = gated_process(program, args);
        process.stdout(Stdio::piped()).stderr(Stdio::piped());
        let Some((mut child, process_group)) =
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
        let mut cancellation_error = None;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            match is_cancelled() {
                Ok(true) => {
                    cancelled = true;
                    break terminate_process_group(&mut child, process_group, CANCELLATION_GRACE)?;
                }
                Ok(false) => {}
                Err(error) => {
                    cancellation_error = Some(error);
                    break terminate_process_group(&mut child, process_group, CANCELLATION_GRACE)?;
                }
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
        signal_process_group(process_group, libc::SIGKILL)?;
        let stdout = stdout_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stdout capture thread panicked"))??;
        let stderr = stderr_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stderr capture thread panicked"))??;
        if let Some(error) = cancellation_error {
            return Err(error).context("check command cancellation");
        }
        if cancelled || is_cancelled()? {
            return Ok(CommandOutputOutcome::Cancelled);
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

    fn run_evidence_command(
        command: &str,
        cwd: &Path,
        log_path: &Path,
        environment: &[(&str, &str)],
        timeout: StdDuration,
        authorize_start: impl FnOnce(&mut dyn Write) -> Result<bool>,
        mut is_cancelled: impl FnMut() -> Result<bool>,
    ) -> Result<EvidenceCommandOutcome> {
        const POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);
        const CANCELLATION_GRACE: StdDuration = StdDuration::from_millis(50);
        const TIMEOUT_GRACE: StdDuration = StdDuration::from_secs(5);

        let stdout_path = log_path.with_extension("stdout.tmp");
        let stderr_path = log_path.with_extension("stderr.tmp");
        let mut process = gated_process("sh", ["-lc", command]);
        process
            .current_dir(cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in environment {
            process.env(key, value);
        }
        let Some((mut child, process_group)) = spawn_authorized(
            process,
            authorize_start,
            &format!("run evidence command: {command}"),
        )?
        else {
            let mut log = fs::File::create(log_path)
                .with_context(|| format!("create evidence log {}", log_path.display()))?;
            writeln!(log, "$ {command}\n\n[IQ cancelled before command start]")?;
            return Ok(EvidenceCommandOutcome::Cancelled(None));
        };
        let stdout = child.stdout.take().context("capture command stdout")?;
        let stderr = child.stderr.take().context("capture command stderr")?;
        let stdout_capture = stdout_path.clone();
        let stderr_capture = stderr_path.clone();
        let stdout_thread = thread::spawn(move || capture_bounded(stdout, &stdout_capture));
        let stderr_thread = thread::spawn(move || capture_bounded(stderr, &stderr_capture));
        let deadline = Instant::now() + timeout;
        let mut timed_out = false;
        let mut cancelled = false;
        let mut cancellation_error = None;
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("poll evidence command: {command}"))?
            {
                break status;
            }
            match is_cancelled() {
                Ok(true) => {
                    cancelled = true;
                    break terminate_process_group(&mut child, process_group, CANCELLATION_GRACE)?;
                }
                Ok(false) => {}
                Err(error) => {
                    cancellation_error = Some(error);
                    break terminate_process_group(&mut child, process_group, CANCELLATION_GRACE)?;
                }
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
        signal_process_group(process_group, libc::SIGKILL)?;
        stdout_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stdout capture thread panicked"))??;
        stderr_thread
            .join()
            .map_err(|_| anyhow::anyhow!("stderr capture thread panicked"))??;

        let mut log = fs::File::create(log_path)
            .with_context(|| format!("create evidence log {}", log_path.display()))?;
        writeln!(log, "$ {command}\n\n--- stdout ---")?;
        let mut stdout_file = fs::File::open(&stdout_path)?;
        std::io::copy(&mut stdout_file, &mut log)?;
        writeln!(log, "\n--- stderr ---")?;
        let mut stderr_file = fs::File::open(&stderr_path)?;
        std::io::copy(&mut stderr_file, &mut log)?;
        cancelled = cancelled || is_cancelled()?;
        if cancelled {
            writeln!(log, "\n[IQ cancelled command]")?;
        }
        fs::remove_file(stdout_path)?;
        fs::remove_file(stderr_path)?;
        if let Some(error) = cancellation_error {
            return Err(error).context("check evidence command cancellation");
        }
        if cancelled {
            return Ok(EvidenceCommandOutcome::Cancelled(Some(status)));
        }
        if timed_out {
            return Ok(EvidenceCommandOutcome::TimedOut(status));
        }
        Ok(EvidenceCommandOutcome::Exited(status))
    }

    fn gated_process<I, S>(program: &str, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut process = Command::new("sh");
        process
            .arg("-c")
            .arg("IFS= read -r gate && [ \"$gate\" = run ] || exit 125\nexec \"$@\"")
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
    ) -> Result<Option<(std::process::Child, i32)>> {
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
        drop(gate);
        Ok(Some((child, process_group)))
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

    fn capture_bounded(mut input: impl Read, path: &Path) -> Result<()> {
        const HEAD_BYTES: usize = 2 * 1024 * 1024;
        const TAIL_BYTES: usize = 2 * 1024 * 1024;
        let mut output = fs::File::create(path)?;
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
        let status = git_output(workspace, ["status", "--porcelain"])?;
        if status.is_empty() {
            Ok(None)
        } else {
            Ok(Some(status.lines().take(20).collect::<Vec<_>>().join("; ")))
        }
    }

    pub fn git_status<I, S>(cwd: &Path, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .with_context(|| format!("run git in {}", cwd.display()))
    }

    fn path_arg(path: &Path) -> &str {
        path.to_str().expect("workspace path must be utf-8")
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
    pub struct ProviderMergeResult {
        pub landed_sha: Option<String>,
    }

    pub trait ProviderAdapter {
        fn kind(&self) -> ProviderKind;
        fn snapshot(&self, url: &str) -> Result<ProviderSnapshot>;
        fn merge(&self, url: &str, expected_head_sha: &str) -> Result<ProviderMergeResult>;
    }

    pub fn provider_for_url(url: &str) -> Result<Box<dyn ProviderAdapter>> {
        if url.contains("/pull/") || url.contains("github.com") {
            Ok(Box::new(GitHubProvider))
        } else if url.contains("/merge_requests/") || url.contains("gitlab") {
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

        fn merge(&self, url: &str, expected_head_sha: &str) -> Result<ProviderMergeResult> {
            provider_command(
                std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into()),
                [
                    "pr",
                    "merge",
                    url,
                    "--merge",
                    "--match-head-commit",
                    expected_head_sha,
                ],
            )?;
            let landed_sha = github_landed_sha(url).unwrap_or(None);
            Ok(ProviderMergeResult { landed_sha })
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

        fn merge(&self, url: &str, expected_head_sha: &str) -> Result<ProviderMergeResult> {
            provider_command(
                std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into()),
                ["mr", "merge", url, "--yes", "--sha", expected_head_sha],
            )?;
            let landed_sha = gitlab_landed_sha(url).unwrap_or(None);
            Ok(ProviderMergeResult { landed_sha })
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

    fn github_landed_sha(url: &str) -> Result<Option<String>> {
        let value = provider_json(
            std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into()),
            ["pr", "view", url, "--json", "mergeCommit"],
        )?;
        let parsed: GitHubMergeView =
            serde_json::from_value(value).context("parse gh merge JSON")?;
        Ok(parsed.merge_commit.and_then(|commit| commit.oid))
    }

    fn gitlab_landed_sha(url: &str) -> Result<Option<String>> {
        let value = provider_json(
            std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into()),
            ["mr", "view", url, "--output", "json"],
        )?;
        let parsed: GitLabMrView =
            serde_json::from_value(value).context("parse glab merged MR JSON")?;
        Ok(parsed.merge_commit_sha.or(parsed.squash_commit_sha))
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

    fn provider_command<I, S>(program: String, args: I) -> Result<()>
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
        if output.status.success() {
            Ok(())
        } else {
            anyhow::bail!(
                "provider CLI {program} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        }
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
        fn ingest_prompt_answers(&self, target: &IssueSyncTarget) -> Result<Vec<PromptAnswer>>;
        fn close(&self, target: &IssueSyncTarget) -> Result<()>;
        fn verify_destination(&self, repo: &str) -> Result<()>;
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

    #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
    pub struct PromptAnswer {
        pub external_response_id: Option<String>,
        pub prompt_id: String,
        pub answer: String,
        pub answered_by: Option<String>,
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

        fn ingest_prompt_answers(&self, target: &IssueSyncTarget) -> Result<Vec<PromptAnswer>> {
            let issue = target
                .issue
                .as_deref()
                .context("GitHub issue number required")?;
            let program = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
            let value = command_json(
                &program,
                [
                    "issue",
                    "view",
                    issue,
                    "--repo",
                    &target.repo,
                    "--json",
                    "comments",
                ],
            )?;
            let view: CommentView =
                serde_json::from_value(value).context("parse gh issue comments")?;
            Ok(extract_prompt_answers(view.comments))
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

        fn ingest_prompt_answers(&self, target: &IssueSyncTarget) -> Result<Vec<PromptAnswer>> {
            let issue = target
                .issue
                .as_deref()
                .context("GitLab issue number required")?;
            let program = std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into());
            Ok(extract_prompt_answers(gitlab_issue_notes(
                &program, target, issue,
            )?))
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
    struct CommentView {
        #[serde(default)]
        comments: Vec<IssueComment>,
    }

    #[derive(Debug, Deserialize)]
    struct IssueComment {
        #[serde(default)]
        id: Option<serde_json::Value>,
        #[serde(default, alias = "body")]
        body: String,
        #[serde(default)]
        author: Option<CommentAuthor>,
    }

    #[derive(Debug, Deserialize)]
    struct CommentAuthor {
        #[serde(default, alias = "username")]
        login: Option<String>,
    }

    fn extract_prompt_answers(comments: Vec<IssueComment>) -> Vec<PromptAnswer> {
        comments
            .into_iter()
            .filter_map(|comment| {
                let (prompt_id, answer) = parse_prompt_answer(&comment.body)?;
                Some(PromptAnswer {
                    external_response_id: comment.id.as_ref().and_then(json_identity),
                    prompt_id,
                    answer,
                    answered_by: comment.author.and_then(|author| author.login),
                })
            })
            .collect()
    }

    fn json_identity(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            _ => None,
        }
    }

    fn parse_prompt_answer(body: &str) -> Option<(String, String)> {
        for line in body.lines() {
            let trimmed = line.trim().trim_matches('`').trim();
            let Some(rest) = trimmed.strip_prefix("iq answer ") else {
                continue;
            };
            let mut parts = rest.splitn(2, char::is_whitespace);
            let prompt_id = parts.next()?.trim().to_string();
            let answer = parts.next().unwrap_or("").trim().to_string();
            if !prompt_id.is_empty() && !answer.is_empty() {
                return Some((prompt_id, answer));
            }
        }
        None
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
