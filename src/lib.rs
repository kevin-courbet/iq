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
                    | (QueueStatus::Merging, QueueStatus::Merged)
                    | (QueueStatus::Merging, QueueStatus::Blocked)
                    | (QueueStatus::Merged, QueueStatus::Validating)
                    | (QueueStatus::Validating, QueueStatus::Validated)
                    | (QueueStatus::Validating, QueueStatus::Blocked)
                    | (QueueStatus::Validated, QueueStatus::Integrating)
                    | (QueueStatus::Integrating, QueueStatus::Integrated)
                    | (QueueStatus::Integrating, QueueStatus::Blocked)
                    | (_, QueueStatus::Cancelled)
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
    use rusqlite::{params, Connection, OptionalExtension, Row};
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
            Ok(queue)
        }

        fn connect(&self) -> Result<Connection> {
            let conn = Connection::open(&self.path)
                .with_context(|| format!("open queue db {}", self.path.display()))?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            Ok(conn)
        }

        pub fn enqueue(&self, request: EnqueueRequest) -> Result<QueueItem> {
            let conn = self.connect()?;
            let now = now();
            let existing: Option<String> = conn
                .query_row(
                    "SELECT id FROM queue_items WHERE repo_key=?1 AND source_branch=?2 AND target_branch=?3 AND status NOT IN ('integrated','cancelled')",
                    params![request.repo_key, request.source_branch, request.target_branch],
                    |row| row.get(0),
                )
                .optional()?;

            let item_id = if let Some(id) = existing {
                conn.execute(
                    "UPDATE queue_items SET repo_path=?1,current_head_sha=?2,pr_url=?3,producer_metadata_json=?4,updated_at=?5 WHERE id=?6",
                    params![
                        request.repo_path,
                        request.current_head_sha,
                        request.pr_url,
                        request.producer_metadata.to_string(),
                        now,
                        id,
                    ],
                )?;
                self.record_event_with_conn(
                    &conn,
                    &id,
                    "item_reworked",
                    "source branch/head updated",
                )?;
                id
            } else {
                let id = Uuid::new_v4().to_string();
                conn.execute(
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
                self.record_event_with_conn(&conn, &id, "item_enqueued", "item enqueued")?;
                id
            };

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
            let tx = conn.transaction()?;
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
                "UPDATE queue_items SET status='merging',current_attempt_id=?1,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
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
            let item = self.get_item(item_id)?;
            StateMachine
                .transition(item.status, target)
                .map_err(anyhow::Error::msg)?;
            let conn = self.connect()?;
            conn.execute(
                "UPDATE queue_items SET status=?1,updated_at=?2 WHERE id=?3",
                params![target.to_string(), now(), item_id],
            )?;
            self.record_event_with_conn(
                &conn,
                item_id,
                "item_transitioned",
                &format!("transitioned to {target}"),
            )?;
            self.get_item(item_id)
        }

        pub fn block_item(
            &self,
            item_id: &str,
            phase: BlockedPhase,
            reason: BlockedReason,
            message: &str,
        ) -> Result<String> {
            let item = self.get_item(item_id)?;
            let expected_status: QueueStatus = phase.into();
            if item.status != expected_status {
                StateMachine
                    .transition(item.status, QueueStatus::Blocked)
                    .map_err(anyhow::Error::msg)?;
            }
            let conn = self.connect()?;
            let prompt_id = if reason == BlockedReason::NeedsUserInput {
                let prompt_id = Uuid::new_v4().to_string();
                conn.execute(
                    "INSERT INTO prompts (id,item_id,attempt_id,blocked_phase,status,question,created_by,created_at) VALUES (?1,?2,?3,?4,'open',?5,'integrator',?6)",
                    params![prompt_id, item_id, item.current_attempt_id, phase.to_string(), message, now()],
                )?;
                Some(prompt_id)
            } else {
                None
            };
            conn.execute(
                "UPDATE queue_items SET status='blocked',blocked_phase=?1,blocked_reason=?2,blocked_message=?3,prompt_id=?4,updated_at=?5 WHERE id=?6",
                params![phase.to_string(), reason.to_string(), message, prompt_id, now(), item_id],
            )?;
            self.record_event_with_conn(
                &conn,
                item_id,
                "item_blocked",
                &format!("{phase}/{reason}: {message}"),
            )?;
            Ok(prompt_id.unwrap_or_default())
        }

        pub fn answer_prompt(
            &self,
            prompt_id: &str,
            answer: &str,
            answered_by: &str,
        ) -> Result<QueueItem> {
            let conn = self.connect()?;
            let prompt = self.get_prompt(prompt_id)?;
            if prompt.status != "open" {
                anyhow::bail!("prompt {prompt_id} is not open")
            }
            let item = self.get_item(&prompt.item_id)?;
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
            conn.execute(
                "UPDATE prompts SET status='answered',answer=?1,answered_by=?2,answered_at=?3 WHERE id=?4",
                params![answer, answered_by, now(), prompt_id],
            )?;
            conn.execute(
                "UPDATE queue_items SET status=?1,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
                params![resume.to_string(), now(), item.id],
            )?;
            self.record_event_with_conn(&conn, &item.id, "user_answered", answer)?;
            self.get_item(&item.id)
        }

        pub fn requeue_agent_fix(&self, item_id: &str, new_head: &str) -> Result<QueueItem> {
            let item = self.get_item(item_id)?;
            if item.status != QueueStatus::Blocked
                || item.blocked_reason != Some(BlockedReason::NeedsAgentFix)
            {
                anyhow::bail!("item {item_id} is not blocked for agent fix")
            }
            let conn = self.connect()?;
            conn.execute(
                "UPDATE prompts SET status='superseded' WHERE item_id=?1 AND status='open'",
                params![item_id],
            )?;
            conn.execute(
                "UPDATE queue_items SET status='ready',current_head_sha=?1,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
                params![new_head, now(), item_id],
            )?;
            self.record_event_with_conn(
                &conn,
                item_id,
                "agent_requeued",
                "agent fix marked ready",
            )?;
            self.get_item(item_id)
        }

        pub fn retry_blocked(&self, item_id: &str) -> Result<QueueItem> {
            let item = self.get_item(item_id)?;
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
            let conn = self.connect()?;
            conn.execute(
                "UPDATE queue_items SET status=?1,blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL,updated_at=?2 WHERE id=?3",
                params![resume.to_string(), now(), item_id],
            )?;
            self.record_event_with_conn(
                &conn,
                item_id,
                "item_retried",
                &format!("retrying {phase} after {reason} block"),
            )?;
            self.get_item(item_id)
        }

        pub fn update_current_head(&self, item_id: &str, new_head: &str) -> Result<QueueItem> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE queue_items SET current_head_sha=?1,updated_at=?2 WHERE id=?3",
                params![new_head, now(), item_id],
            )?;
            self.record_event_with_conn(
                &conn,
                item_id,
                "source_head_updated",
                &format!("source head updated to {new_head}"),
            )?;
            self.get_item(item_id)
        }

        pub fn acquire_repo_lease(
            &self,
            repo_key: &str,
            owner_id: &str,
            ttl_seconds: i64,
        ) -> Result<bool> {
            let mut conn = self.connect()?;
            let tx = conn.transaction()?;
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
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer FROM prompts WHERE item_id=?1 AND status='answered'",
            );
            if attempt_id.is_some() {
                sql.push_str(" AND attempt_id=?2");
            }
            sql.push_str(" ORDER BY answered_at DESC LIMIT 1");
            let map = |row: &Row<'_>| {
                let phase: String = row.get(3)?;
                Ok(Prompt {
                    id: row.get(0)?,
                    item_id: row.get(1)?,
                    attempt_id: row.get(2)?,
                    blocked_phase: BlockedPhase::from_str(&phase).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                        )
                    })?,
                    status: row.get(4)?,
                    question: row.get(5)?,
                    answer: row.get(6)?,
                })
            };
            if let Some(attempt_id) = attempt_id {
                conn.query_row(&sql, params![item_id, attempt_id], map)
                    .optional()
            } else {
                conn.query_row(&sql, params![item_id], map).optional()
            }
            .with_context(|| format!("read latest answered prompt for item {item_id}"))
        }

        pub fn prompts_for_item(&self, item_id: &str) -> Result<Vec<Prompt>> {
            let conn = self.connect()?;
            let mut stmt = conn.prepare(
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer FROM prompts WHERE item_id=?1 ORDER BY created_at ASC",
            )?;
            let prompts = stmt
                .query_map(params![item_id], |row| {
                    let phase: String = row.get(3)?;
                    Ok(Prompt {
                        id: row.get(0)?,
                        item_id: row.get(1)?,
                        attempt_id: row.get(2)?,
                        blocked_phase: BlockedPhase::from_str(&phase).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                            )
                        })?,
                        status: row.get(4)?,
                        question: row.get(5)?,
                        answer: row.get(6)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(prompts)
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

        pub fn mark_integrated(
            &self,
            item_id: &str,
            attempt_id: &str,
            landed_commit_sha: &str,
        ) -> Result<QueueItem> {
            let conn = self.connect()?;
            conn.execute(
                "UPDATE integration_attempts SET landed_commit_sha=?1,result='integrated',finished_at=?2 WHERE id=?3",
                params![landed_commit_sha, now(), attempt_id],
            )?;
            conn.execute(
                "UPDATE queue_items SET status='integrated',landed_commit_sha=?1,updated_at=?2 WHERE id=?3",
                params![landed_commit_sha, now(), item_id],
            )?;
            self.record_event_with_conn(
                &conn,
                item_id,
                "integrated",
                &format!("landed {landed_commit_sha}"),
            )?;
            self.get_item(item_id)
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
                "SELECT id,item_id,attempt_id,blocked_phase,status,question,answer FROM prompts WHERE id=?1",
                params![prompt_id],
                |row| {
                    let phase: String = row.get(3)?;
                    Ok(Prompt {
                        id: row.get(0)?,
                        item_id: row.get(1)?,
                        attempt_id: row.get(2)?,
                        blocked_phase: BlockedPhase::from_str(&phase).map_err(|e| rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e))))?,
                        status: row.get(4)?,
                        question: row.get(5)?,
                        answer: row.get(6)?,
                    })
                },
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
    use serde_json::json;
    use std::ffi::OsStr;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};
    use std::sync::mpsc;
    use std::thread::{self, JoinHandle};
    use std::time::Duration as StdDuration;

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

    pub struct Integrator {
        queue: SqliteQueue,
        options: IntegratorOptions,
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
            Ok(Self {
                queue: SqliteQueue::open(&options.queue_db)?,
                options,
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
            if item.status == QueueStatus::Blocked {
                return Ok(Some(item));
            }
            let item =
                self.with_lease_heartbeat("validating", || self.validate_item(item, &attempt))?;
            if item.status == QueueStatus::Blocked {
                return Ok(Some(item));
            }
            let item =
                self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))?;
            Ok(Some(item))
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
            let attempt_id = item
                .current_attempt_id
                .as_deref()
                .context("item has no active integration attempt")?;
            let attempt = self.queue.get_attempt(attempt_id)?;
            match item.status {
                QueueStatus::Merging => {
                    let item =
                        self.with_lease_heartbeat("merging", || self.resume_merge(item, &attempt))?;
                    if item.status == QueueStatus::Blocked {
                        return Ok(item);
                    }
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if item.status == QueueStatus::Blocked {
                        return Ok(item);
                    }
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Merged => {
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if item.status == QueueStatus::Blocked {
                        return Ok(item);
                    }
                    self.with_lease_heartbeat("integrating", || self.integrate_item(item, &attempt))
                }
                QueueStatus::Validating => {
                    let item = self.with_lease_heartbeat("validating", || {
                        self.validate_item(item, &attempt)
                    })?;
                    if item.status == QueueStatus::Blocked {
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
            self.queue.transition_item(item_id, target)
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
            if workspace.exists() {
                fs::remove_dir_all(&workspace)
                    .with_context(|| format!("remove stale workspace {}", workspace.display()))?;
            }
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

            let diff = git_status(&workspace, ["diff", "--cached", "--quiet"])?;
            if !diff.status.success() {
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
                .filter(|prompt| prompt.attempt_id.as_deref() == Some(attempt.id.as_str()))
                .last()
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
            git(&workspace, ["add", "-A"])?;
            let staged = git_status(&workspace, ["diff", "--cached", "--quiet"])?;
            if !staged.status.success() {
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
            let checkout_arg = match normalized.as_str() {
                "use source" | "source" | "theirs" | "accept-theirs" => "--theirs",
                "use target" | "target" | "ours" | "accept-ours" => "--ours",
                _ => anyhow::bail!(
                    "unsupported merge answer for item {}: {answer}; use accept-current, use source, or use target",
                    item.id
                ),
            };
            for file in &conflicts {
                git(workspace, ["checkout", checkout_arg, "--", file.as_str()])?;
                git(workspace, ["add", "--", file.as_str()])?;
            }
            Ok(())
        }

        fn validate_item(&self, item: QueueItem, attempt: &Attempt) -> Result<QueueItem> {
            let workspace = item
                .integration_workspace_path
                .as_ref()
                .map(PathBuf::from)
                .context("item missing integration workspace path")?;
            let command = match validation_command(&workspace)? {
                Some(command) => command,
                None => {
                    self.transition_item_owned(&item.id, QueueStatus::Validating)?;
                    self.block_item_owned(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::NeedsUserInput,
                        "missing integration.validation.command in .threadmill.yml",
                    )?;
                    return self.queue.get_item(&item.id);
                }
            };
            self.transition_item_owned(&item.id, QueueStatus::Validating)?;
            let log_dir = workspace.join(".iq");
            fs::create_dir_all(&log_dir)?;
            let log_path = log_dir.join("validation.log");
            let output = Command::new("sh")
                .arg("-lc")
                .arg(&command)
                .current_dir(&workspace)
                .output()
                .with_context(|| format!("run validation command: {command}"))?;
            let log = format!(
                "$ {command}\n\n--- stdout ---\n{}\n--- stderr ---\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            fs::write(&log_path, log)?;
            let exit_code = output.status.code().unwrap_or(-1) as i64;
            if !output.status.success() {
                self.ensure_repo_lease()?;
                self.queue.update_attempt_validation(
                    &attempt.id,
                    &command,
                    exit_code,
                    &log_path.to_string_lossy(),
                    None,
                )?;
                self.block_item_owned(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    "validation command failed",
                )?;
                return self.queue.get_item(&item.id);
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
            self.transition_item_owned(&item.id, QueueStatus::Integrating)?;
            if let Some(pr_url) = item.pr_url.clone() {
                return self.integrate_provider_item(item, attempt, &pr_url);
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
            }
            let landed_sha = git_output(&workspace, ["rev-parse", "HEAD"])?;
            let push_ref = format!("HEAD:refs/heads/{}", item.target_branch);
            if let Err(error) = git(&workspace, ["push", &self.options.base_remote, &push_ref]) {
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
            self.mark_integrated_owned(&item.id, &attempt.id, &landed_sha)
        }

        fn merge_moved_base(
            &self,
            item: &QueueItem,
            workspace: &Path,
            moved_base_sha: &str,
            summary_prefix: &str,
        ) -> Result<Option<QueueItem>> {
            let source_sha = git_output(workspace, ["rev-parse", "HEAD"])
                .unwrap_or_else(|_| item.current_head_sha.clone());
            let merge = git_status(workspace, ["merge", "--no-edit", moved_base_sha])?;
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
            let command = match validation_command(workspace) {
                Ok(Some(command)) => command,
                Ok(None) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::NeedsUserInput,
                        &format!("missing integration.validation.command after {label}"),
                    )?));
                }
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!("failed to read validation command after {label}: {error}"),
                    )?));
                }
            };
            let log_dir = workspace.join(".iq");
            fs::create_dir_all(&log_dir)?;
            let safe_label = label.replace(' ', "-").replace('/', "-");
            let log_path = log_dir.join(format!("revalidation-after-{safe_label}.log"));
            let output = match Command::new("sh")
                .arg("-lc")
                .arg(&command)
                .current_dir(workspace)
                .output()
            {
                Ok(output) => output,
                Err(error) => {
                    return Ok(Some(self.block_and_get(
                        &item.id,
                        BlockedPhase::Validating,
                        BlockedReason::Infra,
                        &format!("failed to run validation command after {label}: {error}"),
                    )?));
                }
            };
            let log = format!(
                "$ {command}\n\n--- stdout ---\n{}\n--- stderr ---\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            fs::write(&log_path, log)?;
            let exit_code = output.status.code().unwrap_or(-1) as i64;
            let validated_sha = if output.status.success() {
                git_output(workspace, ["rev-parse", "HEAD"]).ok()
            } else {
                None
            };
            self.ensure_repo_lease()?;
            self.queue
                .update_attempt_base(&attempt.id, moved_base_sha)?;
            self.queue.update_attempt_validation(
                &attempt.id,
                &command,
                exit_code,
                &log_path.to_string_lossy(),
                validated_sha.as_deref(),
            )?;
            if !output.status.success() {
                return Ok(Some(self.block_and_get(
                    &item.id,
                    BlockedPhase::Validating,
                    BlockedReason::NeedsAgentFix,
                    &format!("validation failed after {label}"),
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
            self.block_item_owned(item_id, phase, reason, message)?;
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
            }

            let merge_result = match provider.merge(pr_url) {
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
            let landed_sha = merge_result
                .landed_sha
                .unwrap_or_else(|| item.current_head_sha.clone());
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
            let attempt_id = item
                .current_attempt_id
                .as_deref()
                .context("item has no current attempt")?;
            let attempt = self.queue.get_attempt(attempt_id)?;
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
    struct ThreadmillConfig {
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
        let config_path = repo_path.join(".threadmill.yml");
        if !config_path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&config_path)
            .with_context(|| format!("read {}", config_path.display()))?;
        let parsed: ThreadmillConfig = serde_yaml::from_str(&contents)
            .with_context(|| format!("parse {}", config_path.display()))?;
        Ok(parsed
            .integration
            .and_then(|integration| integration.validation)
            .and_then(|validation| validation.command))
    }

    pub fn git<I, S>(cwd: &Path, args: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = git_status(cwd, args)?;
        if output.status.success() {
            Ok(())
        } else {
            anyhow::bail!(
                "git failed in {}: {}",
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
        let output = git_status(cwd, args)?;
        if !output.status.success() {
            anyhow::bail!(
                "git failed in {}: {}",
                cwd.display(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
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
        fn merge(&self, url: &str) -> Result<ProviderMergeResult>;
    }

    pub fn provider_for_url(url: &str) -> Result<Box<dyn ProviderAdapter>> {
        if url.contains("/pull/") || url.contains("github.com") {
            Ok(Box::new(GitHubProvider::default()))
        } else if url.contains("/merge_requests/") || url.contains("gitlab") {
            Ok(Box::new(GitLabProvider::default()))
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

        fn merge(&self, url: &str) -> Result<ProviderMergeResult> {
            provider_command(
                std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into()),
                ["pr", "merge", url, "--merge"],
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

        fn merge(&self, url: &str) -> Result<ProviderMergeResult> {
            provider_command(
                std::env::var("IQ_GITLAB_CLI").unwrap_or_else(|_| "glab".into()),
                ["mr", "merge", url, "--yes"],
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
    use std::process::Command;

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
                comments.push(format!(
                    "<!-- iq:prompt:{} -->\n**Prompt ({})**: {}\n\nAnswer with `iq answer {}` or reply referencing prompt `{}`.",
                    prompt.id, prompt.blocked_phase, prompt.question, prompt.id, prompt.id
                ));
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
            } else {
                let output = command_output(
                    &program,
                    [
                        "issue",
                        "create",
                        "--repo",
                        &target.repo,
                        "--title",
                        &projection.title,
                        "--body",
                        &projection.body,
                        "--label",
                        &projection.labels.join(","),
                    ],
                )?;
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
            } else {
                let output = command_output(
                    &program,
                    [
                        "issue",
                        "create",
                        "--repo",
                        &target.repo,
                        "--title",
                        &projection.title,
                        "--description",
                        &projection.body,
                        "--label",
                        &projection.labels.join(","),
                    ],
                )?;
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
            let value = command_json(
                &program,
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
            let view: CommentView =
                serde_json::from_value(value).context("parse glab issue comments")?;
            Ok(extract_prompt_answers(view.comments))
        }
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
        serde_json::from_value(value).context("parse glab issue view")
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
                    prompt_id,
                    answer,
                    answered_by: comment.author.and_then(|author| author.login),
                })
            })
            .collect()
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
        let output = Command::new(program)
            .args(args)
            .output()
            .with_context(|| format!("run issue CLI {program}"))?;
        if !output.status.success() {
            anyhow::bail!(
                "issue CLI {program} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn command_ok<I, S>(program: &str, args: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        command_output(program, args).map(|_| ())
    }
}
