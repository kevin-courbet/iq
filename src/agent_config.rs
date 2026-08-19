use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

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
    if !before.is_file() || before.len() == 0 || before.permissions().mode() & 0o111 == 0 {
        anyhow::bail!("executable must resolve to a non-empty executable regular file");
    }
    let fingerprint = file_fingerprint(&before);
    if let Some(identity) = executable_cache()
        .lock()
        .map_err(|_| anyhow::anyhow!("executable identity cache is poisoned"))?
        .get(&canonical)
        .filter(|(cached, _)| cached == &fingerprint)
        .map(|(_, identity)| identity.clone())
    {
        return Ok(identity);
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
    let after = file.metadata()?;
    if (
        opened.dev(),
        opened.ino(),
        opened.len(),
        opened.mtime(),
        opened.mtime_nsec(),
    ) != (
        after.dev(),
        after.ino(),
        after.len(),
        after.mtime(),
        after.mtime_nsec(),
    ) || (opened.ctime(), opened.ctime_nsec()) != (after.ctime(), after.ctime_nsec())
    {
        anyhow::bail!("executable changed while hashing");
    }
    let identity = ExecutableIdentity {
        path: canonical,
        device: opened.dev(),
        inode: opened.ino(),
        sha256: format!("{:x}", digest.finalize()),
    };
    executable_cache()
        .lock()
        .map_err(|_| anyhow::anyhow!("executable identity cache is poisoned"))?
        .insert(
            identity.path.clone(),
            (file_fingerprint(&after), identity.clone()),
        );
    Ok(identity)
}

pub fn trusted_executable_identity(program: &str) -> Result<ExecutableIdentity> {
    if program.is_empty() || program.contains('/') {
        anyhow::bail!("trusted executable name is invalid");
    }
    for directory in ["/usr/local/bin", "/usr/bin", "/bin"] {
        let candidate = Path::new(directory).join(program);
        match fs::symlink_metadata(&candidate) {
            Ok(_) => return executable_identity(&candidate),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("inspect trusted executable {program}"))
            }
        }
    }
    anyhow::bail!("trusted executable is unavailable: {program}")
}

pub fn search_path_executable_identity(program: &str) -> Result<ExecutableIdentity> {
    if program.is_empty() {
        anyhow::bail!("executable name is empty");
    }
    if program.contains('/') {
        return executable_identity(Path::new(program));
    }
    let path = std::env::var_os("PATH").context("PATH is unavailable for executable resolution")?;
    for directory in std::env::split_paths(&path) {
        if !directory.is_absolute() {
            anyhow::bail!("PATH contains a relative executable search directory");
        }
        let candidate = directory.join(program);
        match fs::symlink_metadata(&candidate) {
            Ok(_) => return executable_identity(&candidate),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("inspect executable {program}"))
            }
        }
    }
    anyhow::bail!("executable is unavailable on PATH: {program}")
}

pub fn verify_executable(identity: &ExecutableIdentity) -> Result<()> {
    let metadata = fs::symlink_metadata(&identity.path)?;
    let fingerprint = file_fingerprint(&metadata);
    if executable_cache()
        .lock()
        .map_err(|_| anyhow::anyhow!("executable identity cache is poisoned"))?
        .get(&identity.path)
        .is_some_and(|(cached_fingerprint, cached_identity)| {
            cached_fingerprint == &fingerprint && cached_identity == identity
        })
    {
        return Ok(());
    }
    if &executable_identity(&identity.path)? != identity {
        anyhow::bail!("approved executable identity changed");
    }
    Ok(())
}

#[derive(Clone)]
pub struct ExecutableAuthority {
    identity: ExecutableIdentity,
}

static RIFT_EXECUTABLE_AUTHORITY: OnceLock<ExecutableAuthority> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static TEST_RIFT_EXECUTABLE_AUTHORITY: OnceLock<Mutex<Option<ExecutableAuthority>>> =
    OnceLock::new();

pub fn validate_rift_executable_environment() -> Result<()> {
    if std::env::var_os("IQ_RIFT_CLI").is_some() {
        anyhow::bail!("Rift executable environment overrides are forbidden");
    }
    Ok(())
}

pub fn initialize_rift_executable_authority(path: &Path) -> Result<()> {
    validate_rift_executable_environment()?;
    require_absolute(path, "Rift executable")?;
    let authority = open_executable_authority(&executable_identity(path)?)?;
    if let Some(existing) = RIFT_EXECUTABLE_AUTHORITY.get() {
        if existing.identity != authority.identity {
            anyhow::bail!("Rift executable authority was already initialized differently");
        }
        return Ok(());
    }
    RIFT_EXECUTABLE_AUTHORITY
        .set(authority)
        .map_err(|_| anyhow::anyhow!("Rift executable authority changed during initialization"))
}

pub(crate) fn rift_executable_authority() -> Result<ExecutableAuthority> {
    validate_rift_executable_environment()?;
    #[cfg(any(test, feature = "test-hooks"))]
    if let Some(authority) = TEST_RIFT_EXECUTABLE_AUTHORITY
        .get_or_init(|| Mutex::new(None))
        .lock()
        .map_err(|_| anyhow::anyhow!("test Rift executable authority is poisoned"))?
        .clone()
    {
        return Ok(authority);
    }
    RIFT_EXECUTABLE_AUTHORITY.get().cloned().context(
        "Rift executable authority is not initialized; pass --rift-executable <absolute-path>",
    )
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct TestRiftExecutableGuard;

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for TestRiftExecutableGuard {
    fn drop(&mut self) {
        if let Ok(mut authority) = TEST_RIFT_EXECUTABLE_AUTHORITY
            .get_or_init(|| Mutex::new(None))
            .lock()
        {
            *authority = None;
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub fn inject_test_rift_executable(path: &Path) -> Result<TestRiftExecutableGuard> {
    require_absolute(path, "test Rift executable")?;
    let authority = open_executable_authority(&executable_identity(path)?)?;
    let mut slot = TEST_RIFT_EXECUTABLE_AUTHORITY
        .get_or_init(|| Mutex::new(None))
        .lock()
        .map_err(|_| anyhow::anyhow!("test Rift executable authority is poisoned"))?;
    if slot.is_some() {
        anyhow::bail!("test Rift executable authority is already injected");
    }
    *slot = Some(authority);
    Ok(TestRiftExecutableGuard)
}

pub struct AuthorizedCommand {
    command: std::process::Command,
    authority: ExecutableAuthority,
    current_directory_descriptor: Option<std::sync::Arc<File>>,
    retained_files: Vec<RetainedDescriptor>,
    execution_prepared: bool,
}

#[derive(Clone)]
struct RetainedDescriptor {
    file: std::sync::Arc<File>,
}

impl AuthorizedCommand {
    fn new(authority: ExecutableAuthority) -> Self {
        let mut command = std::process::Command::new(authority.invocation_path());
        command.env_clear();
        Self {
            command,
            authority,
            current_directory_descriptor: None,
            retained_files: Vec::new(),
            execution_prepared: false,
        }
    }

    pub(crate) fn executable_authority(&self) -> &ExecutableAuthority {
        &self.authority
    }

    pub fn arg<S: AsRef<OsStr>>(&mut self, argument: S) -> &mut Self {
        self.command.arg(argument);
        self
    }

    pub fn args<I, S>(&mut self, arguments: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        for argument in arguments {
            self.arg(argument);
        }
        self
    }

    pub fn env<K: AsRef<OsStr>, V: AsRef<OsStr>>(&mut self, key: K, value: V) -> &mut Self {
        self.command.env(key, value);
        self
    }

    pub fn envs<I, K, V>(&mut self, variables: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        for (key, value) in variables {
            self.env(key, value);
        }
        self
    }

    pub fn env_remove<K: AsRef<OsStr>>(&mut self, key: K) -> &mut Self {
        self.command.env_remove(key);
        self
    }

    pub fn env_clear(&mut self) -> &mut Self {
        self.command.env_clear();
        self
    }

    pub fn current_dir<P: AsRef<Path>>(&mut self, directory: P) -> &mut Self {
        self.current_directory_descriptor = None;
        self.command.current_dir(directory);
        self
    }

    pub(crate) fn current_dir_descriptor(
        &mut self,
        directory: std::sync::Arc<File>,
        _mount_id: u64,
    ) -> &mut Self {
        self.current_directory_descriptor = Some(directory.clone());
        self.retained_files
            .push(RetainedDescriptor { file: directory });
        self
    }

    pub fn stdin<T: Into<std::process::Stdio>>(&mut self, configuration: T) -> &mut Self {
        self.command.stdin(configuration);
        self
    }

    pub fn stdout<T: Into<std::process::Stdio>>(&mut self, configuration: T) -> &mut Self {
        self.command.stdout(configuration);
        self
    }

    pub fn stderr<T: Into<std::process::Stdio>>(&mut self, configuration: T) -> &mut Self {
        self.command.stderr(configuration);
        self
    }

    pub(crate) unsafe fn pre_exec<F>(&mut self, function: F) -> &mut Self
    where
        F: FnMut() -> std::io::Result<()> + Send + Sync + 'static,
    {
        self.command.pre_exec(function);
        self
    }

    fn prepare_execution(&mut self) -> Result<()> {
        if self.execution_prepared {
            return Ok(());
        }
        let retained_files = self.retained_files.clone();
        let current_directory_descriptor = self.current_directory_descriptor.clone();
        unsafe {
            self.command.pre_exec(move || {
                for retained in &retained_files {
                    if libc::fcntl(retained.file.as_raw_fd(), libc::F_SETFD, 0) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                if let Some(directory) = current_directory_descriptor.as_ref() {
                    if libc::fchdir(directory.as_raw_fd()) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        self.execution_prepared = true;
        Ok(())
    }

    pub fn spawn(&mut self) -> Result<std::process::Child> {
        self.prepare_execution()?;
        self.command
            .spawn()
            .context("execute sealed executable image")
    }

    pub fn output(&mut self) -> Result<std::process::Output> {
        self.prepare_execution()?;
        self.command
            .output()
            .context("execute sealed executable image")
    }

    pub fn status(&mut self) -> Result<std::process::ExitStatus> {
        self.prepare_execution()?;
        self.command
            .status()
            .context("execute sealed executable image")
    }

    pub(crate) fn retain_file(&mut self, file: std::sync::Arc<File>) {
        self.retained_files.push(RetainedDescriptor { file });
    }

    pub(crate) fn retain_directory(&mut self, file: std::sync::Arc<File>, _mount_id: u64) {
        self.retained_files.push(RetainedDescriptor { file });
    }
}

pub(crate) fn harden_rift_environment(command: &mut AuthorizedCommand) {
    let home = std::env::var_os("HOME");
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin");
    if let Some(home) = home {
        command.env("HOME", home);
    }
}

pub(crate) fn harden_user_systemd_environment(command: &mut AuthorizedCommand) -> Result<()> {
    let runtime = PathBuf::from(format!("/run/user/{}", unsafe { libc::geteuid() }));
    let metadata = fs::symlink_metadata(&runtime)
        .with_context(|| format!("inspect user runtime directory {}", runtime.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        anyhow::bail!("user runtime directory has unsafe identity");
    }
    let bus = runtime.join("bus");
    command
        .env_clear()
        .env("LC_ALL", "C")
        .env("XDG_RUNTIME_DIR", &runtime)
        .env(
            "DBUS_SESSION_BUS_ADDRESS",
            format!("unix:path={}", bus.display()),
        );
    Ok(())
}

impl ExecutableAuthority {
    pub fn identity(&self) -> &ExecutableIdentity {
        &self.identity
    }

    pub fn invocation_path(&self) -> PathBuf {
        self.identity.path.clone()
    }

    pub fn command(&self) -> AuthorizedCommand {
        AuthorizedCommand::new(self.clone())
    }
}

pub fn open_executable_authority(identity: &ExecutableIdentity) -> Result<ExecutableAuthority> {
    verify_executable(identity)?;
    Ok(ExecutableAuthority {
        identity: identity.clone(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileFingerprint {
    device: u64,
    inode: u64,
    length: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

fn file_fingerprint(metadata: &fs::Metadata) -> FileFingerprint {
    FileFingerprint {
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        mode: metadata.mode(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
}

fn executable_cache(
) -> &'static Mutex<std::collections::BTreeMap<PathBuf, (FileFingerprint, ExecutableIdentity)>> {
    static CACHE: OnceLock<
        Mutex<std::collections::BTreeMap<PathBuf, (FileFingerprint, ExecutableIdentity)>>,
    > = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

fn sandbox_identity() -> Result<SandboxIdentity> {
    Ok(SandboxIdentity {
        implementation: "linux_userns_tmpfs_overlay_v1".to_string(),
        bubblewrap: executable_identity(&resolve_program("bwrap")?)?,
        unshare: executable_identity(&resolve_program("unshare")?)?,
        systemd_run: executable_identity(&resolve_program("systemd-run")?)?,
        systemctl: executable_identity(&resolve_program("systemctl")?)?,
    })
}

fn resolve_program(program: &str) -> Result<PathBuf> {
    if program.is_empty() || program.contains('/') {
        anyhow::bail!("required sandbox executable name is invalid");
    }
    for directory in ["/usr/local/bin", "/usr/bin", "/bin"] {
        let candidate = Path::new(directory).join(program);
        match fs::symlink_metadata(&candidate) {
            Ok(_) => {
                let path = candidate.canonicalize()?;
                require_absolute(&path, "sandbox executable")?;
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect required sandbox program {program}"))
            }
        }
    }
    anyhow::bail!("required sandbox program is unavailable: {program}")
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
