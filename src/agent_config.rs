use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
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
        let bytes = if path.as_os_str().as_bytes().starts_with(b"/proc/") {
            read_inherited_system_config(path)?
        } else {
            read_system_config_file(path)?
        };
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

fn read_system_config_file(path: &Path) -> Result<Vec<u8>> {
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
    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn read_inherited_system_config(path: &Path) -> Result<Vec<u8>> {
    let descriptor = parse_inherited_descriptor_path(path)?;
    let descriptor_flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if descriptor_flags < 0 {
        return Err(std::io::Error::last_os_error())
            .context("inspect inherited system configuration descriptor flags");
    }
    if unsafe {
        libc::fcntl(
            descriptor,
            libc::F_SETFD,
            descriptor_flags | libc::FD_CLOEXEC,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error())
            .context("protect inherited system configuration descriptor");
    }
    let duplicate = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error())
            .context("duplicate inherited system configuration descriptor");
    }
    let file = unsafe { File::from_raw_fd(duplicate) };
    let before = file
        .metadata()
        .context("inspect inherited system configuration descriptor")?;
    if !before.is_file()
        || before.len() == 0
        || before.len() > MAX_SYSTEM_CONFIG_BYTES
        || before.uid() != unsafe { libc::geteuid() }
        || before.permissions().mode() & 0o222 != 0
    {
        anyhow::bail!(
            "inherited system configuration must be a bounded owner-only read-only regular file"
        );
    }
    let status_flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error())
            .context("inspect inherited system configuration access mode");
    }
    if status_flags & libc::O_ACCMODE != libc::O_RDONLY {
        anyhow::bail!("inherited system configuration descriptor must be read-only");
    }
    let target = fs::read_link(format!("/proc/self/fd/{duplicate}"))
        .context("inspect inherited system configuration target")?;
    let target = target.as_os_str().as_bytes();
    if !target.starts_with(b"/memfd:") && !target.starts_with(b"memfd:") {
        anyhow::bail!("inherited system configuration must use a sealed anonymous memfd");
    }
    let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if seals < 0 {
        return Err(std::io::Error::last_os_error())
            .context("inspect inherited system configuration seals");
    }
    let required = libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE;
    if seals & required != required {
        anyhow::bail!("inherited system configuration memfd is missing required seals");
    }
    let mut bytes = vec![0_u8; before.len() as usize];
    file.read_exact_at(&mut bytes, 0)
        .context("read inherited system configuration descriptor")?;
    let after = file
        .metadata()
        .context("reinspect inherited system configuration descriptor")?;
    let after_seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if descriptor_metadata_identity(&before) != descriptor_metadata_identity(&after)
        || after_seals != seals
    {
        anyhow::bail!("inherited system configuration changed while reading");
    }
    Ok(bytes)
}

#[cfg(not(target_os = "linux"))]
fn read_inherited_system_config(_path: &Path) -> Result<Vec<u8>> {
    anyhow::bail!("inherited system configuration descriptors require Linux")
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
pub struct PathExecutable {
    identity: ExecutableIdentity,
}

#[derive(Clone)]
pub struct InheritedDescriptorExecutable {
    identity: InheritedDescriptorIdentity,
    file: std::sync::Arc<File>,
}

#[derive(Clone)]
pub enum ExecutableAuthority {
    PathExecutable(PathExecutable),
    InheritedDescriptorExecutable(InheritedDescriptorExecutable),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InheritedDescriptorIdentity {
    executable: ExecutableIdentity,
    owner: u32,
    mode: u32,
    size: u64,
    backing: DescriptorBacking,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DescriptorBacking {
    RegularFile,
    SealedMemfd { seals: i32 },
}

static RIFT_EXECUTABLE_AUTHORITY: OnceLock<ExecutableAuthority> = OnceLock::new();
static RIFT_EXECUTABLE_INITIALIZATION: Mutex<()> = Mutex::new(());

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
    let _initialization = RIFT_EXECUTABLE_INITIALIZATION
        .lock()
        .map_err(|_| anyhow::anyhow!("Rift executable initialization lock is poisoned"))?;
    if let Some(existing @ ExecutableAuthority::InheritedDescriptorExecutable(executable)) =
        RIFT_EXECUTABLE_AUTHORITY.get()
    {
        parse_inherited_descriptor_path(path)?;
        if executable.identity.executable.path != path {
            anyhow::bail!("Rift executable authority was already initialized differently");
        }
        return existing.verify_operation_authority();
    }
    let authority = rift_executable_authority_from_argument(path)?;
    if let Some(existing) = RIFT_EXECUTABLE_AUTHORITY.get() {
        if !existing.same_identity(&authority) {
            anyhow::bail!("Rift executable authority was already initialized differently");
        }
        return Ok(());
    }
    RIFT_EXECUTABLE_AUTHORITY
        .set(authority)
        .map_err(|_| anyhow::anyhow!("Rift executable authority changed during initialization"))
}

fn rift_executable_authority_from_argument(path: &Path) -> Result<ExecutableAuthority> {
    if path.as_os_str().as_bytes().starts_with(b"/proc/") {
        return open_inherited_descriptor_executable(path);
    }
    require_absolute(path, "Rift executable")?;
    open_executable_authority(&executable_identity(path)?)
}

#[cfg(target_os = "linux")]
fn open_inherited_descriptor_executable(path: &Path) -> Result<ExecutableAuthority> {
    let descriptor = parse_inherited_descriptor_path(path)?;
    let descriptor_flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if descriptor_flags < 0 {
        return Err(std::io::Error::last_os_error())
            .context("inspect inherited Rift executable descriptor");
    }
    if unsafe {
        libc::fcntl(
            descriptor,
            libc::F_SETFD,
            descriptor_flags | libc::FD_CLOEXEC,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error())
            .context("protect inherited Rift executable descriptor");
    }
    let file = std::sync::Arc::new(unsafe { File::from_raw_fd(descriptor) });
    let identity = inherited_descriptor_identity(path, &file)?;
    Ok(ExecutableAuthority::InheritedDescriptorExecutable(
        InheritedDescriptorExecutable { identity, file },
    ))
}

#[cfg(not(target_os = "linux"))]
fn open_inherited_descriptor_executable(_path: &Path) -> Result<ExecutableAuthority> {
    anyhow::bail!("inherited Rift executable descriptors require Linux")
}

fn parse_inherited_descriptor_path(path: &Path) -> Result<RawFd> {
    const PREFIX: &[u8] = b"/proc/self/fd/";
    let bytes = path.as_os_str().as_bytes();
    let descriptor = bytes.strip_prefix(PREFIX).filter(|value| {
        !value.is_empty()
            && value.iter().all(u8::is_ascii_digit)
            && (value.len() == 1 || value[0] != b'0')
    });
    let Some(descriptor) = descriptor else {
        anyhow::bail!("Rift executable proc path must be exactly /proc/self/fd/<n>");
    };
    let descriptor = std::str::from_utf8(descriptor)?
        .parse::<RawFd>()
        .context("parse inherited Rift executable descriptor")?;
    if descriptor < 0 {
        anyhow::bail!("inherited Rift executable descriptor must not be negative");
    }
    Ok(descriptor)
}

#[cfg(target_os = "linux")]
fn inherited_descriptor_identity(path: &Path, file: &File) -> Result<InheritedDescriptorIdentity> {
    let descriptor = file.as_raw_fd();
    let descriptor_flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if descriptor_flags < 0 {
        return Err(std::io::Error::last_os_error())
            .context("inspect inherited Rift executable descriptor");
    }
    if descriptor_flags & libc::FD_CLOEXEC == 0 {
        anyhow::bail!("inherited Rift executable descriptor lost close-on-exec protection");
    }
    let before = file
        .metadata()
        .context("inspect inherited Rift executable identity")?;
    if !before.is_file() || before.len() == 0 || before.permissions().mode() & 0o111 == 0 {
        anyhow::bail!(
            "inherited Rift executable descriptor must reference a non-empty executable regular file"
        );
    }
    let status_flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error())
            .context("inspect inherited Rift executable access mode");
    }
    if status_flags & libc::O_ACCMODE != libc::O_RDONLY {
        anyhow::bail!("inherited Rift executable descriptor must be read-only");
    }
    let owner = unsafe { libc::geteuid() };
    if before.uid() != owner {
        anyhow::bail!("inherited Rift executable descriptor must be owned by the current user");
    }
    let target = fs::read_link(path).context("inspect inherited Rift executable target")?;
    let target = target.as_os_str().as_bytes();
    let backing = if target.starts_with(b"/memfd:") || target.starts_with(b"memfd:") {
        let seals = unsafe { libc::fcntl(descriptor, libc::F_GET_SEALS) };
        if seals < 0 {
            return Err(std::io::Error::last_os_error())
                .context("inspect inherited Rift executable seals");
        }
        let required =
            libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE;
        if seals & required != required {
            anyhow::bail!("anonymous Rift executable memfd is missing required seals");
        }
        DescriptorBacking::SealedMemfd { seals }
    } else {
        DescriptorBacking::RegularFile
    };
    let sha256 = descriptor_sha256(file, before.len())?;
    let after = file
        .metadata()
        .context("reinspect inherited Rift executable identity")?;
    if descriptor_metadata_identity(&before) != descriptor_metadata_identity(&after) {
        anyhow::bail!("inherited Rift executable changed while hashing");
    }
    Ok(InheritedDescriptorIdentity {
        executable: ExecutableIdentity {
            path: path.to_path_buf(),
            device: before.dev(),
            inode: before.ino(),
            sha256,
        },
        owner: before.uid(),
        mode: before.mode(),
        size: before.len(),
        backing,
    })
}

#[cfg(not(target_os = "linux"))]
fn inherited_descriptor_identity(
    _path: &Path,
    _file: &File,
) -> Result<InheritedDescriptorIdentity> {
    anyhow::bail!("inherited Rift executable descriptors require Linux")
}

fn descriptor_sha256(file: &File, size: u64) -> Result<String> {
    let mut digest = Sha256::new();
    let mut offset = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    while offset < size {
        let count = file
            .read_at(&mut buffer, offset)
            .context("hash inherited Rift executable descriptor")?;
        if count == 0 {
            anyhow::bail!("inherited Rift executable changed while hashing");
        }
        digest.update(&buffer[..count]);
        offset = offset
            .checked_add(count as u64)
            .context("inherited Rift executable size overflow")?;
    }
    if offset != size {
        anyhow::bail!("inherited Rift executable changed while hashing");
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn descriptor_metadata_identity(metadata: &fs::Metadata) -> (u64, u64, u32, u32, u64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.uid(),
        metadata.mode(),
        metadata.len(),
    )
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

    pub(crate) fn current_dir_descriptor(&mut self, directory: std::sync::Arc<File>) -> &mut Self {
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
        self.authority.verify_operation_authority()?;
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
        let output = self.command.output();
        self.authority
            .verify_operation_authority()
            .context("verify executable authority after command exit")?;
        output.context("execute sealed executable image")
    }

    pub fn status(&mut self) -> Result<std::process::ExitStatus> {
        self.prepare_execution()?;
        let status = self.command.status();
        self.authority
            .verify_operation_authority()
            .context("verify executable authority after command exit")?;
        status.context("execute sealed executable image")
    }

    pub(crate) fn retain_file(&mut self, file: std::sync::Arc<File>) {
        self.retained_files.push(RetainedDescriptor { file });
    }

    pub(crate) fn retain_directory(&mut self, file: std::sync::Arc<File>) {
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
        match self {
            Self::PathExecutable(executable) => &executable.identity,
            Self::InheritedDescriptorExecutable(executable) => &executable.identity.executable,
        }
    }

    pub fn invocation_path(&self) -> PathBuf {
        self.identity().path.clone()
    }

    pub fn command(&self) -> AuthorizedCommand {
        AuthorizedCommand::new(self.clone())
    }

    pub(crate) fn verify_operation_authority(&self) -> Result<()> {
        match self {
            Self::PathExecutable(_) => Ok(()),
            Self::InheritedDescriptorExecutable(executable) => {
                let actual = inherited_descriptor_identity(
                    &executable.identity.executable.path,
                    &executable.file,
                )?;
                if actual != executable.identity {
                    anyhow::bail!("inherited Rift executable identity changed");
                }
                Ok(())
            }
        }
    }

    fn same_identity(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PathExecutable(left), Self::PathExecutable(right)) => {
                left.identity == right.identity
            }
            (
                Self::InheritedDescriptorExecutable(left),
                Self::InheritedDescriptorExecutable(right),
            ) => left.identity == right.identity,
            _ => false,
        }
    }
}

pub fn open_executable_authority(identity: &ExecutableIdentity) -> Result<ExecutableAuthority> {
    verify_executable(identity)?;
    Ok(ExecutableAuthority::PathExecutable(PathExecutable {
        identity: identity.clone(),
    }))
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::io::Write as _;
    use std::os::fd::IntoRawFd;

    fn executable_file(path: &Path, content: &[u8]) {
        fs::write(path, content).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn descriptor_authority(path: &Path) -> (ExecutableAuthority, RawFd) {
        let file = OpenOptions::new().read(true).open(path).unwrap();
        let descriptor = file.into_raw_fd();
        let proc_path = PathBuf::from(format!("/proc/self/fd/{descriptor}"));
        (
            open_inherited_descriptor_executable(&proc_path).unwrap(),
            descriptor,
        )
    }

    fn sealed_config_descriptor(content: &[u8]) -> (File, PathBuf) {
        let name = CString::new("iq-system-config-test").unwrap();
        let descriptor = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        assert!(descriptor >= 0, "{}", std::io::Error::last_os_error());
        let mut writer = unsafe { File::from_raw_fd(descriptor) };
        writer.write_all(content).unwrap();
        writer
            .set_permissions(fs::Permissions::from_mode(0o400))
            .unwrap();
        let seals =
            libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE;
        assert_eq!(
            unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_ADD_SEALS, seals) },
            0
        );
        let writer_path = PathBuf::from(format!("/proc/self/fd/{}", writer.as_raw_fd()));
        let reader = OpenOptions::new().read(true).open(writer_path).unwrap();
        let descriptor = reader.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        let path = PathBuf::from(format!("/proc/self/fd/{descriptor}"));
        (reader, path)
    }

    #[test]
    fn system_config_loads_from_a_sealed_inherited_descriptor() {
        let expected = SystemConfig {
            integration_agent: IntegrationAgentConfig {
                runner: RunnerKind::Opencode,
                executable: PathBuf::from("/bin/true"),
                agent: "iq-integration".into(),
                model: "test/model".into(),
                cycle_timeout_seconds: 60,
                max_log_bytes: 4096,
                max_result_bytes: 4096,
                max_processes: 4,
                memory_bytes: 64 * 1024 * 1024,
                cpu_seconds: 60,
                writable_bytes: 1024 * 1024,
                open_files: 64,
                credential_env: "IQ_TEST_MODEL_KEY".into(),
            },
            control_plane: ControlPlaneConfig {
                unix_socket: PathBuf::from("/tmp/iq-test-control.sock"),
                max_request_bytes: 4096,
                max_free_text_bytes: 1024,
                max_response_bytes: 4096,
                max_concurrent_clients: 2,
                max_client_queue_bytes: 4096,
                max_stream_backlog_events: 100,
                client_idle_seconds: 5,
            },
            notifications: NotificationConfig::default(),
        };
        let yaml = serde_yaml::to_string(&expected).unwrap();
        let (descriptor, path) = sealed_config_descriptor(yaml.as_bytes());

        assert_eq!(SystemConfig::load(&path).unwrap(), expected);
        let flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn inherited_descriptor_command_revalidates_content_before_spawn() {
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("rift");
        executable_file(&executable, b"first executable image\n");
        let (authority, _) = descriptor_authority(&executable);

        fs::write(&executable, b"other executable image\n").unwrap();

        let error = authority.command().output().unwrap_err();
        assert!(format!("{error:#}").contains("identity changed"));
    }

    #[test]
    fn inherited_descriptor_command_revalidates_mode_after_exit() {
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("rift");
        fs::copy("/bin/chmod", &executable).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let (authority, _) = descriptor_authority(&executable);

        let error = authority
            .command()
            .args([OsStr::new("500"), executable.as_os_str()])
            .output()
            .unwrap_err();
        assert!(format!("{error:#}").contains("identity changed"));
    }

    #[test]
    fn inherited_descriptor_authority_rejects_a_reused_descriptor() {
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("rift");
        let replacement = root.path().join("replacement");
        executable_file(&executable, b"first executable image\n");
        executable_file(&replacement, b"replacement executable\n");
        let (authority, descriptor) = descriptor_authority(&executable);
        assert_eq!(unsafe { libc::close(descriptor) }, 0);
        let replacement = OpenOptions::new().read(true).open(replacement).unwrap();
        let replacement_descriptor = replacement.into_raw_fd();
        if replacement_descriptor != descriptor {
            assert_eq!(
                unsafe { libc::dup2(replacement_descriptor, descriptor) },
                descriptor
            );
            assert_eq!(unsafe { libc::close(replacement_descriptor) }, 0);
        }

        let error = authority.verify_operation_authority().unwrap_err();
        assert!(error.to_string().contains("identity changed"));
    }
}
