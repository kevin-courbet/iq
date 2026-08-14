use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::process::Command;

use crate::agent_config::{NotificationBackendConfig, NotificationConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Pending,
    Claimed,
    Running,
    Delivered,
    DeliveryUnknown,
    Failed,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendHealth {
    pub backend: &'static str,
    pub available: bool,
    pub detail: String,
}

#[derive(Clone, Debug)]
struct PendingDelivery {
    id: i64,
    event_id: String,
    backend: String,
    attempt_count: u8,
    event_created_at: String,
    payload: NotificationPayload,
    claim_id: String,
}

struct DeliveryTransition {
    expected: DeliveryState,
    next: DeliveryState,
    attempts: u8,
    next_attempt_at: Option<String>,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationPayload {
    pub repository: String,
    pub item_id: String,
    pub blocker_kind: String,
    pub reason: String,
}

pub struct NotificationDispatcher {
    authority: crate::control_store::ValidatedDatabaseAuthority,
    config: NotificationConfig,
}

impl NotificationDispatcher {
    pub(crate) fn from_validated_authority(
        authority: crate::control_store::ValidatedDatabaseAuthority,
        config: NotificationConfig,
    ) -> Self {
        Self { authority, config }
    }

    pub fn health(&self) -> Vec<BackendHealth> {
        self.config.backends.iter().map(health).collect()
    }

    pub fn configure(&self) -> Result<()> {
        let connection = self.connect()?;
        register_backends(&connection, &self.config)
    }

    pub fn dispatch_once(&self) -> Result<usize> {
        let mut connection = self.connect()?;
        register_backends(&connection, &self.config)?;
        let pending = claim_next(&mut connection)?;
        let Some(delivery) = pending else {
            return Ok(0);
        };
        let created: DateTime<Utc> = delivery.event_created_at.parse()?;
        if Utc::now() - created > Duration::seconds(self.config.max_event_age_seconds as i64) {
            set_delivery_state(
                &connection,
                delivery.id,
                &delivery.claim_id,
                DeliveryTransition {
                    expected: DeliveryState::Claimed,
                    next: DeliveryState::Expired,
                    attempts: delivery.attempt_count,
                    next_attempt_at: None,
                    error: None,
                },
            )?;
            return Ok(1);
        }
        let backend = self
            .config
            .backends
            .iter()
            .find(|backend| backend_name(backend) == delivery.backend)
            .context("pending delivery backend is not configured")?;
        let payload = bounded_payload(&delivery.payload)?;
        mark_command_started(&connection, delivery.id, &delivery.claim_id)?;
        let outcome = run_backend(backend, &payload);
        let connection = self.connect()?;
        match outcome {
            Ok(()) => set_delivery_state(
                &connection,
                delivery.id,
                &delivery.claim_id,
                DeliveryTransition {
                    expected: DeliveryState::Running,
                    next: DeliveryState::Delivered,
                    attempts: delivery.attempt_count + 1,
                    next_attempt_at: None,
                    error: None,
                },
            )?,
            Err(error) if delivery.attempt_count + 1 < self.config.max_attempts => {
                let backoff = 1_i64 << u32::from(delivery.attempt_count.min(10));
                set_delivery_state(
                    &connection,
                    delivery.id,
                    &delivery.claim_id,
                    DeliveryTransition {
                        expected: DeliveryState::Running,
                        next: DeliveryState::Pending,
                        attempts: delivery.attempt_count + 1,
                        next_attempt_at: Some(
                            (Utc::now() + Duration::seconds(backoff)).to_rfc3339(),
                        ),
                        error: Some(format!("{error:#}")),
                    },
                )?
            }
            Err(error) => set_delivery_state(
                &connection,
                delivery.id,
                &delivery.claim_id,
                DeliveryTransition {
                    expected: DeliveryState::Running,
                    next: DeliveryState::Failed,
                    attempts: delivery.attempt_count + 1,
                    next_attempt_at: None,
                    error: Some(format!("{error:#}")),
                },
            )?,
        }
        Ok(1)
    }

    pub fn mark_started_unknown_after_restart(&self) -> Result<usize> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE notification_deliveries SET state='pending',claim_id=NULL,claimed_at=NULL,last_error_json=json_object('kind','claim_recovered_before_start'),updated_at=?1 WHERE state='claimed'",
            params![Utc::now().to_rfc3339()],
        )?;
        let changed = transaction.execute(
            "UPDATE notification_deliveries SET state='delivery_unknown',claim_id=NULL,claimed_at=NULL,next_attempt_at=NULL,last_error_json=json_object('kind','authority_lost_after_start'),updated_at=?1 WHERE state='running'",
            params![Utc::now().to_rfc3339()],
        )?;
        transaction.commit()?;
        Ok(changed)
    }

    pub fn redeliver(&self, delivery_id: i64, actor: &str) -> Result<i64> {
        crate::control_domain::require_exact_text(actor, "notification redelivery actor")?;
        let connection = self.connect()?;
        let (event_id, backend, state): (String, String, String) = connection.query_row(
            "SELECT event_id,backend,state FROM notification_deliveries WHERE id=?1",
            params![delivery_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if state != "delivery_unknown" && state != "failed" && state != "expired" {
            anyhow::bail!(
                "only unknown, failed, or expired delivery can be explicitly redelivered"
            );
        }
        connection.execute(
            "INSERT INTO notification_deliveries(event_id,backend,state,attempt_count,next_attempt_at,redelivery_of,redelivery_actor,created_at,updated_at) VALUES(?1,?2,'pending',0,?3,?4,?5,?3,?3)",
            params![event_id,backend,Utc::now().to_rfc3339(),delivery_id,actor],
        )?;
        Ok(connection.last_insert_rowid())
    }

    fn connect(&self) -> Result<Connection> {
        let connection = self
            .authority
            .open_connection(OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        self.authority.verify_configured_connection(&connection)?;
        Ok(connection)
    }
}

fn register_backends(connection: &Connection, config: &NotificationConfig) -> Result<()> {
    for name in ["wslg", "windows"] {
        connection.execute(
            "UPDATE notification_backends SET enabled=?1 WHERE backend=?2",
            params![
                config
                    .backends
                    .iter()
                    .any(|backend| backend_name(backend) == name),
                name
            ],
        )?;
    }
    Ok(())
}

fn claim_next(connection: &mut Connection) -> Result<Option<PendingDelivery>> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let selected = transaction
        .query_row(
            "SELECT delivery.id,delivery.event_id,delivery.backend,delivery.attempt_count,event.created_at,json_extract(event.payload_json,'$.repository'),event.item_id,COALESCE(json_extract(event.payload_json,'$.blocker_kind'),'unknown'),COALESCE(json_extract(event.payload_json,'$.reason'),event.event_type) FROM notification_deliveries delivery JOIN durable_events event ON event.id=delivery.event_id WHERE delivery.state='pending' AND delivery.next_attempt_at<=?1 ORDER BY delivery.next_attempt_at,delivery.id LIMIT 1",
            params![Utc::now().to_rfc3339()],
            |row| Ok(PendingDelivery {
                id: row.get(0)?,
                event_id: row.get(1)?,
                backend: row.get(2)?,
                attempt_count: row.get(3)?,
                event_created_at: row.get(4)?,
                payload: NotificationPayload {
                    repository: row.get::<_, Option<String>>(5)?.unwrap_or_else(|| "local".into()),
                    item_id: row.get(6)?,
                    blocker_kind: row.get(7)?,
                    reason: row.get(8)?,
                },
                claim_id: uuid::Uuid::new_v4().to_string(),
            }),
        )
        .optional()?;
    let Some(delivery) = selected else {
        transaction.commit()?;
        return Ok(None);
    };
    let changed = transaction.execute(
        "UPDATE notification_deliveries SET state='claimed',claim_id=?1,claimed_at=?2,last_error_json=json_object('kind','claimed','event_id',?3),updated_at=?2 WHERE id=?4 AND state='pending'",
        params![delivery.claim_id,Utc::now().to_rfc3339(),delivery.event_id,delivery.id],
    )?;
    if changed != 1 {
        anyhow::bail!("notification delivery authority changed before command start");
    }
    transaction.commit()?;
    Ok(Some(delivery))
}

fn mark_command_started(connection: &Connection, id: i64, claim_id: &str) -> Result<()> {
    let changed = connection.execute(
        "UPDATE notification_deliveries SET state='running',last_error_json=json_object('kind','command_started'),updated_at=?1 WHERE id=?2 AND state='claimed' AND claim_id=?3",
        params![Utc::now().to_rfc3339(),id,claim_id],
    )?;
    if changed != 1 {
        anyhow::bail!("notification claim changed before command start");
    }
    Ok(())
}

fn set_delivery_state(
    connection: &Connection,
    id: i64,
    claim_id: &str,
    transition: DeliveryTransition,
) -> Result<()> {
    let name = |state: DeliveryState| match state {
        DeliveryState::Pending => "pending",
        DeliveryState::Claimed => "claimed",
        DeliveryState::Running => "running",
        DeliveryState::Delivered => "delivered",
        DeliveryState::DeliveryUnknown => "delivery_unknown",
        DeliveryState::Failed => "failed",
        DeliveryState::Expired => "expired",
    };
    let changed = connection.execute(
        "UPDATE notification_deliveries SET state=?1,claim_id=NULL,claimed_at=NULL,attempt_count=?2,next_attempt_at=?3,last_error_json=?4,updated_at=?5 WHERE id=?6 AND state=?7 AND claim_id=?8",
        params![name(transition.next),transition.attempts,transition.next_attempt_at,transition.error.map(|error| serde_json::json!({"kind":"backend","detail":error}).to_string()),Utc::now().to_rfc3339(),id,name(transition.expected),claim_id],
    )?;
    if changed != 1 {
        anyhow::bail!("notification delivery claim changed before result persistence");
    }
    Ok(())
}

fn bounded_payload(payload: &NotificationPayload) -> Result<String> {
    let reason = payload.reason.chars().take(512).collect::<String>();
    let message = format!(
        "{}\nItem: {}\nBlocker: {}\n{}\nRun: iq show {}",
        payload.repository, payload.item_id, payload.blocker_kind, reason, payload.item_id
    );
    if message.len() > 2048 {
        anyhow::bail!("notification payload exceeds fixed bound");
    }
    Ok(message)
}

fn run_backend(backend: &NotificationBackendConfig, payload: &str) -> Result<()> {
    let output = match backend {
        NotificationBackendConfig::Wslg { executable } => Command::new(executable)
            .args(["--app-name", "IQ", "IQ integration blocked", payload])
            .output(),
        NotificationBackendConfig::Windows { executable } => Command::new(executable)
            .env("IQ_NOTIFICATION_PAYLOAD", payload)
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "$m=$env:IQ_NOTIFICATION_PAYLOAD; [Windows.UI.Notifications.ToastNotificationManager,Windows.UI.Notifications,ContentType=WindowsRuntime] > $null; [Windows.Data.Xml.Dom.XmlDocument,Windows.Data.Xml.Dom.XmlDocument,ContentType=WindowsRuntime] > $null; $safe=[Security.SecurityElement]::Escape($m); $xml=New-Object Windows.Data.Xml.Dom.XmlDocument; $xml.LoadXml(\"<toast><visual><binding template=`\"ToastGeneric`\"><text>IQ integration blocked</text><text>$safe</text></binding></visual></toast>\"); $toast=New-Object Windows.UI.Notifications.ToastNotification $xml; [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('Microsoft.WindowsTerminal_8wekyb3d8bbwe').Show($toast)",
            ])
            .output(),
    }
    .context("start notification backend")?;
    if !output.status.success() {
        anyhow::bail!(
            "notification backend failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn health(backend: &NotificationBackendConfig) -> BackendHealth {
    let (name, path) = match backend {
        NotificationBackendConfig::Wslg { executable } => ("wslg", executable),
        NotificationBackendConfig::Windows { executable } => ("windows", executable),
    };
    let available = path.is_absolute() && path.is_file();
    BackendHealth {
        backend: name,
        available,
        detail: if available {
            "available".into()
        } else {
            format!("unavailable: {}", path.display())
        },
    }
}

fn backend_name(backend: &NotificationBackendConfig) -> &'static str {
    match backend {
        NotificationBackendConfig::Wslg { .. } => "wslg",
        NotificationBackendConfig::Windows { .. } => "windows",
    }
}
