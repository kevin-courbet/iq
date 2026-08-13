use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crate::control_domain::{
    require_exact_text, ExecutableIdentity, RunnerBounds, RunnerKind, RunnerSnapshot,
    SandboxIdentity,
};

const MAX_SYSTEM_CONFIG_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemConfig {
    pub integration_agent: IntegrationAgentConfig,
    pub control_plane: ControlPlaneConfig,
    #[serde(default)]
    pub notifications: NotificationConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationAgentConfig {
    pub runner: RunnerKind,
    pub executable: PathBuf,
    pub agent: String,
    pub model: String,
    pub cycle_timeout_seconds: u64,
    pub max_log_bytes: u64,
    pub max_result_bytes: u64,
    pub max_processes: u32,
    pub memory_bytes: u64,
    pub cpu_seconds: u64,
    pub writable_bytes: u64,
    pub open_files: u32,
    pub credential_env: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneConfig {
    pub unix_socket: PathBuf,
    pub max_request_bytes: u64,
    pub max_free_text_bytes: u64,
    pub max_response_bytes: u64,
    pub max_concurrent_clients: u32,
    pub max_client_queue_bytes: u64,
    pub max_stream_backlog_events: u64,
    pub client_idle_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationConfig {
    #[serde(default)]
    pub backends: Vec<NotificationBackendConfig>,
    #[serde(default = "default_notification_attempts")]
    pub max_attempts: u8,
    #[serde(default = "default_notification_age")]
    pub max_event_age_seconds: u64,
    #[serde(default = "default_projection_debt_alert_age")]
    pub projection_debt_alert_seconds: u64,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            backends: Vec::new(),
            max_attempts: default_notification_attempts(),
            max_event_age_seconds: default_notification_age(),
            projection_debt_alert_seconds: default_projection_debt_alert_age(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NotificationBackendConfig {
    Wslg { executable: PathBuf },
    Windows { executable: PathBuf },
}

fn default_notification_attempts() -> u8 {
    5
}

fn default_notification_age() -> u64 {
    24 * 60 * 60
}

fn default_projection_debt_alert_age() -> u64 {
    15 * 60
}

impl SystemConfig {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            anyhow::bail!("system configuration path must be absolute");
        }
        let before = fs::symlink_metadata(path)
            .with_context(|| format!("inspect system configuration {}", path.display()))?;
        if before.file_type().is_symlink()
            || !before.is_file()
            || before.len() > MAX_SYSTEM_CONFIG_BYTES
        {
            anyhow::bail!("system configuration must be a bounded regular non-symlink file");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("open system configuration {}", path.display()))?;
        let opened = file.metadata()?;
        if (before.dev(), before.ino()) != (opened.dev(), opened.ino()) {
            anyhow::bail!("system configuration changed while opening");
        }
        let mut bytes = Vec::with_capacity(opened.len() as usize);
        file.read_to_end(&mut bytes)?;
        let config: Self = serde_yaml::from_slice(&bytes)
            .with_context(|| format!("parse strict system configuration {}", path.display()))?;
        config.validate()
    }

    pub fn validate(self) -> Result<Self> {
        let agent = &self.integration_agent;
        require_exact_text(&agent.agent, "integration agent")?;
        require_exact_text(&agent.model, "integration model")?;
        require_exact_text(&agent.credential_env, "model credential environment name")?;
        if !agent
            .credential_env
            .bytes()
            .enumerate()
            .all(|(index, byte)| {
                byte == b'_'
                    || byte.is_ascii_alphanumeric() && (index != 0 || !byte.is_ascii_digit())
            })
        {
            anyhow::bail!("model credential environment name is invalid");
        }
        if agent.cycle_timeout_seconds == 0
            || agent.max_log_bytes == 0
            || agent.max_result_bytes == 0
            || agent.max_processes == 0
            || agent.memory_bytes == 0
            || agent.cpu_seconds == 0
            || agent.writable_bytes == 0
            || agent.open_files == 0
        {
            anyhow::bail!("integration agent bounds must be non-zero");
        }
        if agent.max_result_bytes > agent.writable_bytes
            || agent.max_log_bytes > agent.writable_bytes
        {
            anyhow::bail!("protocol and log bounds must fit inside the writable sandbox bound");
        }
        require_absolute(&agent.executable, "runner executable")?;
        let control = &self.control_plane;
        require_absolute(&control.unix_socket, "control-plane socket")?;
        if control.max_request_bytes == 0
            || control.max_free_text_bytes == 0
            || control.max_response_bytes == 0
            || control.max_concurrent_clients == 0
            || control.max_client_queue_bytes == 0
            || control.max_stream_backlog_events == 0
            || control.client_idle_seconds == 0
            || control.max_free_text_bytes > control.max_request_bytes
        {
            anyhow::bail!("control-plane bounds are invalid");
        }
        if self.notifications.max_attempts == 0
            || self.notifications.max_event_age_seconds == 0
            || self.notifications.projection_debt_alert_seconds == 0
        {
            anyhow::bail!("notification bounds must be non-zero");
        }
        for backend in &self.notifications.backends {
            let path = match backend {
                NotificationBackendConfig::Wslg { executable }
                | NotificationBackendConfig::Windows { executable } => executable,
            };
            require_absolute(path, "notification executable")?;
        }
        Ok(self)
    }

    pub fn runner_snapshot(&self, model_override: Option<&str>) -> Result<RunnerSnapshot> {
        let executable = executable_identity(&self.integration_agent.executable)?;
        let sandbox = sandbox_identity()?;
        let model = model_override.unwrap_or(&self.integration_agent.model);
        require_exact_text(model, "effective integration model")?;
        Ok(RunnerSnapshot {
            kind: self.integration_agent.runner,
            executable,
            agent: self.integration_agent.agent.clone(),
            model: model.to_string(),
            cycle_timeout_seconds: self.integration_agent.cycle_timeout_seconds,
            bounds: RunnerBounds {
                max_log_bytes: self.integration_agent.max_log_bytes,
                max_result_bytes: self.integration_agent.max_result_bytes,
                max_processes: self.integration_agent.max_processes,
                memory_bytes: self.integration_agent.memory_bytes,
                cpu_seconds: self.integration_agent.cpu_seconds,
                writable_bytes: self.integration_agent.writable_bytes,
                open_files: self.integration_agent.open_files,
            },
            sandbox,
            credential_env: self.integration_agent.credential_env.clone(),
        })
    }
}

pub fn executable_identity(path: &Path) -> Result<ExecutableIdentity> {
    require_absolute(path, "executable")?;
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve executable {}", path.display()))?;
    let before = fs::symlink_metadata(&canonical)?;
    if !before.is_file() || before.len() == 0 {
        anyhow::bail!("executable must resolve to a non-empty regular file");
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&canonical)?;
    let opened = file.metadata()?;
    if (before.dev(), before.ino()) != (opened.dev(), opened.ino()) {
        anyhow::bail!("executable changed while hashing");
    }
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)?;
    Ok(ExecutableIdentity {
        path: canonical,
        device: opened.dev(),
        inode: opened.ino(),
        sha256: format!("{:x}", digest.finalize()),
    })
}

pub fn verify_executable(identity: &ExecutableIdentity) -> Result<()> {
    if &executable_identity(&identity.path)? != identity {
        anyhow::bail!("approved executable identity changed");
    }
    Ok(())
}

fn sandbox_identity() -> Result<SandboxIdentity> {
    Ok(SandboxIdentity {
        implementation: "linux_userns_tmpfs_overlay_v1".to_string(),
        bubblewrap: resolve_program("bwrap")?,
        unshare: resolve_program("unshare")?,
        systemd_run: resolve_program("systemd-run")?,
        systemctl: resolve_program("systemctl")?,
    })
}

fn resolve_program(program: &str) -> Result<PathBuf> {
    let output = std::process::Command::new("/bin/sh")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .args(["-c", "command -v -- \"$1\"", "sh", program])
        .output()
        .with_context(|| format!("resolve required sandbox program {program}"))?;
    if !output.status.success() {
        anyhow::bail!("required sandbox program is unavailable: {program}");
    }
    let path = String::from_utf8(output.stdout)?.trim().to_string();
    let path = PathBuf::from(path).canonicalize()?;
    require_absolute(&path, "sandbox executable")?;
    Ok(path)
}

fn require_absolute(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        anyhow::bail!(
            "{label} must be an absolute normalized path: {}",
            path.display()
        );
    }
    Ok(())
}
