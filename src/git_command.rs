use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs::File;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::{Mutex, OnceLock};

static GIT_EXECUTABLE: OnceLock<
    std::result::Result<crate::control_domain::ExecutableIdentity, String>,
> = OnceLock::new();

#[cfg(any(debug_assertions, feature = "test-hooks"))]
thread_local! {
    static TEST_GIT_EXECUTABLE: std::cell::RefCell<Option<crate::control_domain::ExecutableIdentity>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(any(debug_assertions, feature = "test-hooks"))]
pub struct TestGitExecutableGuard {
    previous: Option<crate::control_domain::ExecutableIdentity>,
}

#[cfg(any(debug_assertions, feature = "test-hooks"))]
impl Drop for TestGitExecutableGuard {
    fn drop(&mut self) {
        TEST_GIT_EXECUTABLE.with(|authority| *authority.borrow_mut() = self.previous.take());
    }
}

#[cfg(any(debug_assertions, feature = "test-hooks"))]
pub fn inject_test_git_executable(path: &Path) -> Result<TestGitExecutableGuard> {
    let identity = crate::agent_config::executable_identity(path)?;
    let previous = TEST_GIT_EXECUTABLE.with(|authority| authority.borrow_mut().replace(identity));
    Ok(TestGitExecutableGuard { previous })
}

pub(crate) struct HttpsCredential {
    username: String,
    secret: String,
}

impl HttpsCredential {
    pub(crate) fn new(username: &str, secret: &str) -> Result<Self> {
        if username.is_empty()
            || secret.is_empty()
            || username.len() > 256
            || secret.len() > 16 * 1024
            || username.bytes().any(|byte| byte.is_ascii_control())
            || secret.bytes().any(|byte| byte.is_ascii_control())
        {
            anyhow::bail!("provider HTTPS credential is invalid");
        }
        Ok(Self {
            username: username.to_string(),
            secret: secret.to_string(),
        })
    }
}

pub(crate) struct AskPassAuthority {
    _helper: tempfile::TempPath,
}

pub(crate) fn apply_https_credential(
    command: &mut crate::agent_config::AuthorizedCommand,
    credential: &HttpsCredential,
) -> Result<AskPassAuthority> {
    use std::os::unix::fs::PermissionsExt;

    let mut helper = tempfile::NamedTempFile::new().context("create Git askpass helper")?;
    helper.write_all(
        b"#!/bin/sh\ncase \"$1\" in\n  *sername*) printf '%s\\n' \"$IQ_GIT_HTTPS_USERNAME\" ;;\n  *) printf '%s\\n' \"$IQ_GIT_HTTPS_TOKEN\" ;;\nesac\n",
    )?;
    helper.flush()?;
    std::fs::set_permissions(helper.path(), std::fs::Permissions::from_mode(0o700))?;
    let helper = helper.into_temp_path();
    command
        .env("GIT_ASKPASS", &helper)
        .env("GIT_ASKPASS_REQUIRE", "force")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("IQ_GIT_HTTPS_USERNAME", &credential.username)
        .env("IQ_GIT_HTTPS_TOKEN", &credential.secret);
    Ok(AskPassAuthority { _helper: helper })
}

pub fn initialize_executable_authority() -> Result<()> {
    if std::env::var_os("IQ_GIT_CLI").is_some() || std::env::var_os("IQ_GIT_EXECUTABLE").is_some() {
        anyhow::bail!("Git executable environment overrides are forbidden");
    }
    let identity = GIT_EXECUTABLE
        .get_or_init(|| {
            crate::agent_config::trusted_executable_identity("git")
                .map_err(|error| format!("{error:#}"))
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))?;
    crate::agent_config::verify_executable(identity)
}

fn git_executable() -> Result<crate::control_domain::ExecutableIdentity> {
    #[cfg(any(debug_assertions, feature = "test-hooks"))]
    if let Some(identity) = TEST_GIT_EXECUTABLE.with(|authority| authority.borrow().clone()) {
        crate::agent_config::verify_executable(&identity)?;
        return Ok(identity);
    }
    initialize_executable_authority()?;
    GIT_EXECUTABLE
        .get()
        .context("Git executable authority was not initialized")?
        .as_ref()
        .cloned()
        .map_err(|error| anyhow::anyhow!(error.clone()))
}

pub(crate) fn executable_identity() -> Result<crate::control_domain::ExecutableIdentity> {
    git_executable()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryBinding {
    pub top_level: PathBuf,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub object_format: crate::git_object::GitObjectFormat,
    bare: bool,
    top_level_device: u64,
    top_level_inode: u64,
    top_level_mount_id: u64,
    git_dir_device: u64,
    git_dir_inode: u64,
    git_dir_mount_id: u64,
    common_dir_device: u64,
    common_dir_inode: u64,
    common_dir_mount_id: u64,
}

pub(crate) struct RepositoryAuthority {
    binding: RepositoryBinding,
    top_level: std::sync::Arc<File>,
    git_dir: std::sync::Arc<File>,
    common_dir: std::sync::Arc<File>,
}

impl RepositoryBinding {
    pub fn is_bare(&self) -> bool {
        self.bare
    }

    pub fn capture(top_level: &Path) -> Result<Self> {
        let mut binding =
            Self::capture_filesystem(top_level, crate::git_object::GitObjectFormat::Sha1)?;
        binding.object_format = detect_object_format_bound(&binding)?;
        validate_live_repository(&binding)?;
        Ok(binding)
    }

    fn capture_filesystem(
        top_level: &Path,
        object_format: crate::git_object::GitObjectFormat,
    ) -> Result<Self> {
        require_verified_cwd(top_level)?;
        let top_level_metadata = real_directory_metadata(top_level, "Git top-level")?;
        let dot_git = top_level.join(".git");
        let (git_dir, bare) = match std::fs::symlink_metadata(&dot_git) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                (dot_git.canonicalize()?, false)
            }
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                (resolve_git_path_file(&dot_git, "gitdir")?, false)
            }
            Ok(_) => anyhow::bail!(
                "Git administrative entry must be a real directory or regular gitdir file"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                require_bare_layout(top_level)?;
                (top_level.to_path_buf(), true)
            }
            Err(error) => return Err(error).context("inspect Git administrative entry"),
        };
        let git_dir_metadata = real_directory_metadata(&git_dir, "Git directory")?;
        let common_file = git_dir.join("commondir");
        let common_dir = match std::fs::symlink_metadata(&common_file) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                resolve_git_path_file(&common_file, "commondir")?
            }
            Ok(_) => anyhow::bail!("Git commondir entry must be a regular file"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => git_dir.clone(),
            Err(error) => return Err(error).context("inspect Git commondir entry"),
        };
        let common_dir_metadata = real_directory_metadata(&common_dir, "Git common directory")?;
        let top_level_mount_id = path_mount_id(top_level, "Git top-level")?;
        let git_dir_mount_id = path_mount_id(&git_dir, "Git directory")?;
        let common_dir_mount_id = path_mount_id(&common_dir, "Git common directory")?;
        let binding = Self {
            top_level: top_level.to_path_buf(),
            git_dir,
            common_dir,
            object_format,
            bare,
            top_level_device: top_level_metadata.dev(),
            top_level_inode: top_level_metadata.ino(),
            top_level_mount_id,
            git_dir_device: git_dir_metadata.dev(),
            git_dir_inode: git_dir_metadata.ino(),
            git_dir_mount_id,
            common_dir_device: common_dir_metadata.dev(),
            common_dir_inode: common_dir_metadata.ino(),
            common_dir_mount_id,
        };
        validate_administrative_layout(&binding, &dot_git)?;
        require_canonical_object_resolution(&binding)?;
        Ok(binding)
    }

    pub fn verify(&self) -> Result<()> {
        let actual = Self::capture_filesystem(&self.top_level, self.object_format)?;
        if actual != *self {
            anyhow::bail!("Git repository binding changed after authorization");
        }
        Ok(())
    }

    pub(crate) fn verify_relocated(&self, top_level: &Path) -> Result<Self> {
        let actual = Self::capture(top_level)?;
        if self.object_format != actual.object_format
            || self.bare != actual.bare
            || self.top_level_device != actual.top_level_device
            || self.top_level_inode != actual.top_level_inode
            || self.top_level_mount_id != actual.top_level_mount_id
            || self.git_dir_device != actual.git_dir_device
            || self.git_dir_inode != actual.git_dir_inode
            || self.git_dir_mount_id != actual.git_dir_mount_id
            || self.common_dir_device != actual.common_dir_device
            || self.common_dir_inode != actual.common_dir_inode
            || self.common_dir_mount_id != actual.common_dir_mount_id
        {
            anyhow::bail!("relocated Git repository differs from durable binding");
        }
        Ok(actual)
    }

    pub fn verify_head(&self, expected: &str) -> Result<()> {
        self.object_format
            .require_oid(expected, "expected Git HEAD")?;
        self.verify()?;
        if resolve_commit(self, "HEAD")? != expected {
            anyhow::bail!("live Git HEAD differs from expected repository authority");
        }
        Ok(())
    }

    pub fn verify_base(&self, expected: &str) -> Result<()> {
        self.object_format
            .require_oid(expected, "expected Git base")?;
        self.verify()?;
        if resolve_commit(self, expected)? != expected {
            anyhow::bail!("expected Git base does not resolve to its exact commit");
        }
        let output =
            hardened_bound_output(self, ["merge-base", "--is-ancestor", expected, "HEAD"])?;
        if !output.status.success() {
            anyhow::bail!("expected Git base is not an ancestor of live HEAD");
        }
        Ok(())
    }

    pub fn verify_commit(&self, expected: &str) -> Result<()> {
        self.object_format
            .require_oid(expected, "expected Git commit")?;
        self.verify()?;
        if resolve_commit(self, expected)? != expected {
            anyhow::bail!("expected Git commit object is unavailable");
        }
        Ok(())
    }

    fn bind(&self, command: &mut crate::agent_config::AuthorizedCommand) -> Result<()> {
        RepositoryAuthority::open(self)?.bind(command)
    }
}

impl RepositoryAuthority {
    fn open(binding: &RepositoryBinding) -> Result<Self> {
        let git_dir = open_directory_authority(
            &binding.git_dir,
            binding.git_dir_device,
            binding.git_dir_inode,
            binding.git_dir_mount_id,
            "Git directory",
        )?;
        let common_dir = if binding.common_dir == binding.git_dir {
            git_dir.clone()
        } else {
            open_directory_authority(
                &binding.common_dir,
                binding.common_dir_device,
                binding.common_dir_inode,
                binding.common_dir_mount_id,
                "Git common directory",
            )?
        };
        let authority = Self {
            binding: binding.clone(),
            top_level: open_directory_authority(
                &binding.top_level,
                binding.top_level_device,
                binding.top_level_inode,
                binding.top_level_mount_id,
                "Git top-level",
            )?,
            git_dir,
            common_dir,
        };
        authority.verify_control_state()?;
        Ok(authority)
    }

    pub(crate) fn verify_control_state(&self) -> Result<()> {
        for directory in [&self.common_dir, &self.git_dir] {
            let path = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
            reject_existing_path(&path.join("info/grafts"), "legacy grafts")?;
            reject_existing_path(&path.join("shallow"), "shallow history")?;
            reject_existing_path(
                &path.join("objects/info/alternates"),
                "alternate object database",
            )?;
            reject_existing_path(
                &path.join("objects/info/http-alternates"),
                "HTTP alternate object database",
            )?;
            reject_nonempty_directory(&path.join("refs/replace"), "replacement refs")?;
            reject_packed_replacement_refs(&path.join("packed-refs"))?;
        }
        if !self.binding.bare && self.binding.git_dir != self.binding.top_level.join(".git") {
            let dot_git = read_bounded_regular_file_at(
                &self.top_level,
                ".git",
                "linked worktree .git entry",
                false,
            )?
            .context("linked worktree .git entry is unavailable")?;
            let target = parse_git_path_bytes(&dot_git, true, "linked worktree .git entry")?;
            if resolve_recorded_path(&self.binding.top_level, target) != self.binding.git_dir {
                anyhow::bail!("linked worktree gitdir backlink differs from its .git file");
            }
            let backlink = read_bounded_regular_file_at(
                &self.git_dir,
                "gitdir",
                "linked worktree gitdir backlink",
                false,
            )?
            .context("linked worktree gitdir backlink is unavailable")?;
            let backlink =
                parse_git_path_bytes(&backlink, false, "linked worktree gitdir backlink")?;
            if resolve_recorded_path(&self.binding.git_dir, backlink)
                != self.binding.top_level.join(".git")
            {
                anyhow::bail!("linked worktree gitdir backlink differs from its .git file");
            }
        }
        Ok(())
    }

    fn git_entry_exists(&self, name: &str) -> Result<bool> {
        if name.is_empty() || name.as_bytes().contains(&b'/') {
            anyhow::bail!("Git administrative entry name is invalid");
        }
        let name = CString::new(name)?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
        if unsafe {
            libc::fstatat(
                self.git_dir.as_raw_fd(),
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
            Err(error).with_context(|| format!("inspect Git administrative entry {name:?}"))
        }
    }

    fn bind(&self, command: &mut crate::agent_config::AuthorizedCommand) -> Result<()> {
        harden_object_resolution_environment(command);
        let (config_path, config) = controlled_config(self)?;
        command.env("GIT_CONFIG", config_path);
        retain_file_for_spawn(command, config);
        command.env(
            "GIT_DIR",
            format!("/proc/self/fd/{}", self.git_dir.as_raw_fd()),
        );
        if self.common_dir.as_raw_fd() == self.git_dir.as_raw_fd() {
            command.env_remove("GIT_COMMON_DIR");
        } else {
            command.env(
                "GIT_COMMON_DIR",
                format!("/proc/self/fd/{}", self.common_dir.as_raw_fd()),
            );
        }
        if self.binding.bare {
            command.env_remove("GIT_WORK_TREE");
        } else {
            command.env(
                "GIT_WORK_TREE",
                format!("/proc/self/fd/{}", self.top_level.as_raw_fd()),
            );
        }
        command.current_dir_descriptor(self.top_level.clone(), self.binding.top_level_mount_id);
        command.retain_directory(self.git_dir.clone(), self.binding.git_dir_mount_id);
        if self.common_dir.as_raw_fd() != self.git_dir.as_raw_fd() {
            command.retain_directory(self.common_dir.clone(), self.binding.common_dir_mount_id);
        }
        Ok(())
    }
}

fn parse_git_path_bytes<'a>(bytes: &'a [u8], prefixed: bool, label: &str) -> Result<&'a OsStr> {
    let value = if prefixed {
        bytes
            .strip_prefix(b"gitdir: ")
            .with_context(|| format!("{label} has an invalid prefix"))?
    } else {
        bytes
    };
    let value = value.strip_suffix(b"\n").unwrap_or(value);
    let value = value.strip_suffix(b"\r").unwrap_or(value);
    if value.is_empty() || value.contains(&b'\n') || value.contains(&b'\r') {
        anyhow::bail!("{label} must contain one non-empty path");
    }
    Ok(OsStr::from_bytes(value))
}

fn resolve_recorded_path(base_directory: &Path, value: &OsStr) -> PathBuf {
    let value = Path::new(value);
    if value.is_absolute() {
        value.to_path_buf()
    } else {
        base_directory.join(value)
    }
}

fn require_canonical_object_resolution(binding: &RepositoryBinding) -> Result<()> {
    let mut administrative_directories = vec![binding.common_dir.as_path()];
    if binding.git_dir != binding.common_dir {
        administrative_directories.push(binding.git_dir.as_path());
    }
    for directory in administrative_directories {
        reject_existing_path(&directory.join("info/grafts"), "legacy grafts")?;
        reject_existing_path(&directory.join("shallow"), "shallow history")?;
        reject_existing_path(
            &directory.join("objects/info/alternates"),
            "alternate object database",
        )?;
        reject_existing_path(
            &directory.join("objects/info/http-alternates"),
            "HTTP alternate object database",
        )?;
        reject_nonempty_directory(&directory.join("refs/replace"), "replacement refs")?;
        reject_packed_replacement_refs(&directory.join("packed-refs"))?;
    }
    Ok(())
}

fn reject_existing_path(path: &Path, label: &str) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => anyhow::bail!("Git repository contains forbidden {label}"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect Git {label}")),
    }
}

fn reject_nonempty_directory(path: &Path, label: &str) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            anyhow::bail!("Git {label} path is not a real directory")
        }
        Ok(_) => {
            if std::fs::read_dir(path)?.next().transpose()?.is_some() {
                anyhow::bail!("Git repository contains forbidden {label}");
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect Git {label}")),
    }
}

fn reject_packed_replacement_refs(path: &Path) -> Result<()> {
    let packed = match std::fs::symlink_metadata(path) {
        Ok(_) => read_bounded_regular_file(path, "Git packed refs")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("inspect Git packed refs"),
    };
    for line in packed.split(|byte| *byte == b'\n') {
        if line.starts_with(b"#") || line.starts_with(b"^") || line.is_empty() {
            continue;
        }
        let Some(separator) = line.iter().position(|byte| *byte == b' ') else {
            anyhow::bail!("Git packed refs contains an invalid record");
        };
        if line[separator + 1..].starts_with(b"refs/replace/") {
            anyhow::bail!("Git repository contains forbidden replacement refs");
        }
    }
    Ok(())
}

fn validate_administrative_layout(binding: &RepositoryBinding, dot_git: &Path) -> Result<()> {
    require_regular_file(&binding.git_dir.join("HEAD"), "Git HEAD")?;
    require_regular_file(&binding.common_dir.join("config"), "Git common config")?;
    real_directory_metadata(&binding.common_dir.join("objects"), "Git objects directory")?;
    real_directory_metadata(&binding.common_dir.join("refs"), "Git refs directory")?;
    let packed_refs = binding.common_dir.join("packed-refs");
    match std::fs::symlink_metadata(&packed_refs) {
        Ok(_) => require_regular_file(&packed_refs, "Git packed refs")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect Git packed refs"),
    }
    if binding.git_dir != binding.common_dir {
        let dot_git_metadata = std::fs::symlink_metadata(dot_git)?;
        if dot_git_metadata.file_type().is_symlink() || !dot_git_metadata.is_file() {
            anyhow::bail!("linked worktree .git entry must be a regular gitdir file");
        }
        let backlink_file = binding.git_dir.join("gitdir");
        require_regular_file(&backlink_file, "linked worktree gitdir backlink")?;
        let backlink = resolve_git_path_file(&backlink_file, "worktree backlink")?;
        if backlink != dot_git.canonicalize()? {
            anyhow::bail!("linked worktree gitdir backlink differs from its .git file");
        }
        let worktrees = binding.common_dir.join("worktrees").canonicalize()?;
        if binding.git_dir.parent() != Some(worktrees.as_path()) {
            anyhow::bail!(
                "linked worktree Git directory is outside the common worktrees directory"
            );
        }
    }
    Ok(())
}

fn validate_live_repository(binding: &RepositoryBinding) -> Result<()> {
    let head = read_head(binding)?;
    let mut args = vec![
        "rev-parse",
        "--absolute-git-dir",
        "--path-format=absolute",
        "--git-common-dir",
        "--is-bare-repository",
    ];
    if !binding.bare {
        args.push("--show-toplevel");
    }
    let identity_argument_count = args.len();
    args.extend(["--verify", "HEAD^{commit}"]);
    let with_head = hardened_bound_output(binding, &args)?;
    if with_head.status.success() {
        let lines = output_lines(with_head, "inspect live Git repository")?;
        validate_live_identity(binding, &lines, true)?;
        return Ok(());
    }

    args.truncate(identity_argument_count);
    let identity = hardened_bound_output(binding, &args)?;
    let lines = output_lines(identity, "inspect live Git repository")?;
    validate_live_identity(binding, &lines, false)?;
    match head {
        HeadState::Symbolic(reference) if !reference_is_declared(binding, &reference)? => Ok(()),
        HeadState::Symbolic(_) => anyhow::bail!("Git HEAD reference does not resolve to a commit"),
        HeadState::Detached => anyhow::bail!("detached Git HEAD does not resolve to a commit"),
    }
}

fn detect_object_format_bound(
    binding: &RepositoryBinding,
) -> Result<crate::git_object::GitObjectFormat> {
    let (config_path, config) = bootstrap_controlled_config(binding)?;
    let mut command = command()?;
    command.env("GIT_CONFIG", config_path);
    retain_file_for_spawn(&mut command, config);
    command.env("GIT_DIR", &binding.git_dir);
    command.env_remove("GIT_COMMON_DIR");
    if binding.bare {
        command.env_remove("GIT_WORK_TREE");
    } else {
        command.env("GIT_WORK_TREE", &binding.top_level);
    }
    command
        .args(["rev-parse", "--show-object-format"])
        .current_dir(&binding.top_level);
    let output = service_output(&mut command)?;
    if !output.status.success() {
        anyhow::bail!(
            "inspect Git object format failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let value = String::from_utf8(output.stdout).context("Git object format is not UTF-8")?;
    crate::git_object::GitObjectFormat::parse(value.trim(), "Git repository")
}

enum HeadState {
    Symbolic(String),
    Detached,
}

fn read_head(binding: &RepositoryBinding) -> Result<HeadState> {
    let bytes = read_bounded_regular_file(&binding.git_dir.join("HEAD"), "Git HEAD")?;
    let value = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
    let value = value.strip_suffix(b"\r").unwrap_or(value);
    if let Some(reference) = value.strip_prefix(b"ref: ") {
        let reference = std::str::from_utf8(reference)?.to_string();
        if !valid_head_reference(&reference) {
            anyhow::bail!("Git HEAD contains an invalid reference");
        }
        Ok(HeadState::Symbolic(reference))
    } else {
        let detached = std::str::from_utf8(value)?;
        binding
            .object_format
            .require_oid(detached, "detached Git HEAD")?;
        Ok(HeadState::Detached)
    }
}

fn valid_head_reference(reference: &str) -> bool {
    reference.starts_with("refs/heads/")
        && reference.len() > "refs/heads/".len()
        && !reference.contains("..")
        && !reference.contains("@{")
        && !reference.contains("//")
        && !reference.ends_with(['.', '/'])
        && reference.split('/').all(|component| {
            !component.is_empty()
                && !component.starts_with('.')
                && !component.ends_with(".lock")
                && !component.bytes().any(|byte| {
                    byte <= b' '
                        || byte == 0x7f
                        || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
                })
        })
}

fn reference_is_declared(binding: &RepositoryBinding, reference: &str) -> Result<bool> {
    let loose = binding.common_dir.join(reference);
    match std::fs::symlink_metadata(&loose) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                anyhow::bail!("Git HEAD loose reference is not a regular file");
            }
            return Ok(true);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect Git HEAD loose reference"),
    }
    let packed = binding.common_dir.join("packed-refs");
    match std::fs::symlink_metadata(&packed) {
        Ok(_) => {
            let packed = read_bounded_regular_file(&packed, "Git packed refs")?;
            Ok(packed.split(|byte| *byte == b'\n').any(|line| {
                line.iter()
                    .position(|byte| *byte == b' ')
                    .is_some_and(|separator| &line[separator + 1..] == reference.as_bytes())
            }))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).context("inspect Git packed refs"),
    }
}

fn validate_live_identity(
    binding: &RepositoryBinding,
    lines: &[String],
    includes_head: bool,
) -> Result<()> {
    let expected = if binding.bare { 3 } else { 4 } + usize::from(includes_head);
    if lines.len() != expected {
        anyhow::bail!("Git repository introspection returned an unexpected identity shape");
    }
    if Path::new(&lines[0]) != binding.git_dir {
        anyhow::bail!("Git reports a different live administrative directory");
    }
    if Path::new(&lines[1]) != binding.common_dir {
        anyhow::bail!("Git reports a different live common directory");
    }
    if (lines[2] == "true") != binding.bare || !matches!(lines[2].as_str(), "true" | "false") {
        anyhow::bail!(
            "Git reports a different repository worktree mode: expected bare={}, observed {:?}",
            binding.bare,
            lines
        );
    }
    if !binding.bare && Path::new(&lines[3]) != binding.top_level {
        anyhow::bail!("Git reports a different live top-level directory");
    }
    if includes_head {
        binding.object_format.require_oid(
            lines.last().expect("validated line count"),
            "live Git HEAD commit",
        )?;
    }
    Ok(())
}

fn resolve_commit(binding: &RepositoryBinding, revision: &str) -> Result<String> {
    let object = format!("{revision}^{{commit}}");
    let resolved = hardened_bound_text(binding, ["rev-parse", "--verify", object.as_str()])
        .with_context(|| format!("resolve exact Git commit {revision}"))?;
    binding
        .object_format
        .require_oid(&resolved, "resolved Git commit")?;
    Ok(resolved)
}

fn hardened_bound_text<const N: usize>(
    binding: &RepositoryBinding,
    args: [&str; N],
) -> Result<String> {
    output_text(
        hardened_bound_output(binding, args)?,
        "inspect live Git repository",
    )
}

fn hardened_bound_output<I, S>(binding: &RepositoryBinding, args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = command()?;
    binding.bind(&mut command)?;
    command.args(args);
    service_output(&mut command).context("run hardened Git repository introspection")
}

fn output_text(output: Output, label: &str) -> Result<String> {
    if !output.status.success() {
        anyhow::bail!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let output = String::from_utf8(output.stdout)?;
    let output = output.trim_end_matches(['\n', '\r']);
    if output.is_empty() || output.contains(['\n', '\r']) {
        anyhow::bail!("{label} returned invalid output");
    }
    Ok(output.to_string())
}

fn output_lines(output: Output, label: &str) -> Result<Vec<String>> {
    if !output.status.success() {
        anyhow::bail!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let output = String::from_utf8(output.stdout)?;
    let lines = output.lines().map(str::to_string).collect::<Vec<_>>();
    if lines.iter().any(String::is_empty) {
        anyhow::bail!("{label} returned invalid output");
    }
    Ok(lines)
}

fn read_bounded_regular_file(path: &Path, label: &str) -> Result<Vec<u8>> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .with_context(|| format!("open {label} {}", path.display()))?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > 16 * 1024 * 1024 {
        anyhow::bail!("{label} is not a bounded regular file");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    std::io::Read::read_to_end(&mut file, &mut bytes)?;
    Ok(bytes)
}

fn open_directory_authority(
    path: &Path,
    expected_device: u64,
    expected_inode: u64,
    expected_mount_id: u64,
    label: &str,
) -> Result<std::sync::Arc<File>> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .with_context(|| format!("open {label} authority {}", path.display()))?;
    let metadata = file.metadata()?;
    if !metadata.is_dir()
        || metadata.dev() != expected_device
        || metadata.ino() != expected_inode
        || descriptor_mount_id(&file, label)? != expected_mount_id
    {
        anyhow::bail!(
            "Git repository binding changed after authorization: {label} descriptor differs"
        );
    }
    Ok(std::sync::Arc::new(file))
}

fn read_bounded_regular_file_at(
    directory: &File,
    name: &str,
    label: &str,
    optional: bool,
) -> Result<Option<Vec<u8>>> {
    let name = CString::new(name)?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        let error = std::io::Error::last_os_error();
        if optional && error.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(error).with_context(|| format!("open {label}"));
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > 16 * 1024 * 1024 {
        anyhow::bail!("{label} is not a bounded regular file");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    std::io::Read::read_to_end(&mut file, &mut bytes)?;
    Ok(Some(bytes))
}

fn require_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        anyhow::bail!("{label} must be a regular file");
    }
    Ok(())
}

fn require_bare_layout(path: &Path) -> Result<()> {
    for (name, directory) in [
        ("HEAD", false),
        ("config", false),
        ("objects", true),
        ("refs", true),
    ] {
        let entry = path.join(name);
        let metadata = std::fs::symlink_metadata(&entry)
            .with_context(|| format!("inspect bare Git entry {}", entry.display()))?;
        if metadata.file_type().is_symlink()
            || (directory && !metadata.is_dir())
            || (!directory && !metadata.is_file())
        {
            anyhow::bail!("bare Git repository has an invalid {name} entry");
        }
    }
    Ok(())
}

fn real_directory_metadata(path: &Path, label: &str) -> Result<std::fs::Metadata> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("{label} must be a real directory");
    }
    Ok(metadata)
}

fn path_mount_id(path: &Path, label: &str) -> Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statx>::zeroed();
    if unsafe {
        libc::statx(
            libc::AT_FDCWD,
            path.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
            libc::STATX_MNT_ID,
            stat.as_mut_ptr(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("inspect {label} mount identity"));
    }
    let stat = unsafe { stat.assume_init() };
    if stat.stx_mask & libc::STATX_MNT_ID == 0 {
        anyhow::bail!("{label} has no mount identity");
    }
    Ok(stat.stx_mnt_id)
}

fn descriptor_mount_id(file: &File, label: &str) -> Result<u64> {
    let mut stat = std::mem::MaybeUninit::<libc::statx>::zeroed();
    if unsafe {
        libc::statx(
            file.as_raw_fd(),
            c"".as_ptr(),
            libc::AT_EMPTY_PATH,
            libc::STATX_MNT_ID,
            stat.as_mut_ptr(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("inspect {label} descriptor mount identity"));
    }
    let stat = unsafe { stat.assume_init() };
    if stat.stx_mask & libc::STATX_MNT_ID == 0 {
        anyhow::bail!("{label} descriptor has no mount identity");
    }
    Ok(stat.stx_mnt_id)
}

fn resolve_git_path_file(path: &Path, label: &str) -> Result<PathBuf> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .with_context(|| format!("open Git {label} file {}", path.display()))?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > 4096 {
        anyhow::bail!("Git {label} file is not a bounded regular file");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    std::io::Read::read_to_end(&mut file, &mut bytes)?;
    let value = if label == "gitdir" {
        bytes
            .strip_prefix(b"gitdir: ")
            .context("Git gitdir file has an invalid prefix")?
    } else {
        bytes.as_slice()
    };
    let value = value.strip_suffix(b"\n").unwrap_or(value);
    let value = value.strip_suffix(b"\r").unwrap_or(value);
    if value.is_empty() || value.contains(&b'\n') || value.contains(&b'\r') {
        anyhow::bail!("Git {label} file must contain one non-empty path");
    }
    let mut resolved = PathBuf::from(OsStr::from_bytes(value));
    if resolved.is_relative() {
        resolved = path
            .parent()
            .context("Git path file has no parent")?
            .join(resolved);
    }
    resolved
        .canonicalize()
        .with_context(|| format!("resolve Git {label} path"))
}

fn bindings() -> &'static Mutex<BTreeMap<PathBuf, RepositoryBinding>> {
    static BINDINGS: OnceLock<Mutex<BTreeMap<PathBuf, RepositoryBinding>>> = OnceLock::new();
    BINDINGS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[doc(hidden)]
pub fn authorize_current(cwd: &Path) -> Result<RepositoryBinding> {
    if let Some(binding) = bindings()
        .lock()
        .map_err(|_| anyhow::anyhow!("Git repository binding registry is poisoned"))?
        .get(cwd)
        .cloned()
    {
        binding.verify()?;
        require_safe_local_config_bound(&binding)?;
        return Ok(binding);
    }
    let binding = RepositoryBinding::capture(cwd)?;
    authorize_binding(&binding)?;
    Ok(binding)
}

pub(crate) fn replace_authorized_binding(cwd: &Path) -> Result<RepositoryBinding> {
    let binding = RepositoryBinding::capture(cwd)?;
    binding.verify()?;
    bindings()
        .lock()
        .map_err(|_| anyhow::anyhow!("Git repository binding registry is poisoned"))?
        .insert(cwd.to_path_buf(), binding.clone());
    Ok(binding)
}

pub(crate) fn authorize_binding(binding: &RepositoryBinding) -> Result<()> {
    binding.verify()?;
    register_binding(binding)
}

pub(crate) fn register_binding(binding: &RepositoryBinding) -> Result<()> {
    let mut registered = bindings()
        .lock()
        .map_err(|_| anyhow::anyhow!("Git repository binding registry is poisoned"))?;
    if let Some(expected) = registered.get(&binding.top_level) {
        if expected != binding {
            anyhow::bail!("Git repository path has a different authorized binding");
        }
    } else {
        registered.insert(binding.top_level.clone(), binding.clone());
    }
    Ok(())
}

pub(crate) fn expected_binding(cwd: &Path) -> Result<RepositoryBinding> {
    let registered = bindings()
        .lock()
        .map_err(|_| anyhow::anyhow!("Git repository binding registry is poisoned"))?;
    registered
        .get(cwd)
        .cloned()
        .with_context(|| format!("Git repository {} has no authorized binding", cwd.display()))
}

pub(crate) fn bind_verified(
    command: &mut crate::agent_config::AuthorizedCommand,
    binding: &RepositoryBinding,
) -> Result<RepositoryAuthority> {
    let authority = RepositoryAuthority::open(binding)?;
    authority.bind(command)?;
    Ok(authority)
}

pub(crate) fn command() -> Result<crate::agent_config::AuthorizedCommand> {
    let executable = git_executable()?;
    let executable = crate::agent_config::open_executable_authority(&executable)?;
    let mut command = executable.command();
    harden_authorized(&mut command);
    Ok(command)
}

pub(crate) fn command_in(cwd: &Path) -> Result<crate::agent_config::AuthorizedCommand> {
    let binding = expected_binding(cwd)?;
    let authority = RepositoryAuthority::open(&binding)?;
    let mut command = command()?;
    authority.bind(&mut command)?;
    Ok(command)
}

pub(crate) fn administrative_entry_exists(cwd: &Path, name: &str) -> Result<bool> {
    let binding = expected_binding(cwd)?;
    RepositoryAuthority::open(&binding)?.git_entry_exists(name)
}

pub(crate) fn init_repository(
    cwd: &Path,
    object_format: crate::git_object::GitObjectFormat,
) -> Result<()> {
    require_verified_cwd(cwd)?;
    let git_directory = cwd.join(".git");
    if git_directory.try_exists()? {
        let metadata = std::fs::symlink_metadata(&git_directory)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("partial Git initialization has an invalid .git directory");
        }
        for (name, label) in [
            ("config", "partial repository-local"),
            ("config.worktree", "partial repository-worktree"),
        ] {
            let path = git_directory.join(name);
            if path.try_exists()? {
                inspect_safe_config_file(cwd, &path, label)?;
            }
        }
    }
    let mut initialize = command()?;
    initialize
        .arg("init")
        .arg(format!("--object-format={object_format}"))
        .current_dir(cwd);
    let initialized =
        service_output(&mut initialize).context("initialize hardened Git repository")?;
    if !initialized.status.success() {
        anyhow::bail!(
            "hardened Git initialization failed: {}",
            String::from_utf8_lossy(&initialized.stderr).trim()
        );
    }
    let binding = authorize_current(cwd)?;
    if binding.object_format != object_format {
        anyhow::bail!("initialized Git repository has a different object format");
    }
    require_safe_local_config(cwd)?;
    let verified = output(cwd, ["rev-parse", "--show-toplevel"])?;
    let root = verified
        .stdout
        .strip_suffix(b"\n")
        .unwrap_or(&verified.stdout);
    if !verified.status.success() || Path::new(OsStr::from_bytes(root)) != cwd {
        anyhow::bail!("hardened Git initialization created an unexpected repository identity");
    }
    Ok(())
}

trait CommandEnvironment {
    fn set_environment(&mut self, key: OsString, value: OsString);
    fn remove_environment(&mut self, key: OsString);
}

impl CommandEnvironment for crate::agent_config::AuthorizedCommand {
    fn set_environment(&mut self, key: OsString, value: OsString) {
        self.env(key, value);
    }

    fn remove_environment(&mut self, key: OsString) {
        self.env_remove(key);
    }
}

fn harden_environment(command: &mut impl CommandEnvironment) {
    for (key, _) in std::env::vars_os() {
        let key_bytes = key.as_encoded_bytes();
        if key_bytes.starts_with(b"GIT_")
            || key_bytes.starts_with(b"SSH_")
            || key_bytes.starts_with(b"ASKPASS")
            || key_bytes.starts_with(b"GCM_")
        {
            command.remove_environment(key);
        }
    }
    for (key, value) in [
        ("GIT_CONFIG_NOSYSTEM", "1"),
        ("GIT_CONFIG_SYSTEM", "/dev/null"),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG", "/dev/null"),
        ("GIT_CONFIG_COUNT", "1"),
        ("GIT_CONFIG_KEY_0", "core.hooksPath"),
        ("GIT_CONFIG_VALUE_0", "/dev/null"),
        ("GIT_TERMINAL_PROMPT", "0"),
        ("GIT_ALLOW_PROTOCOL", "https:ssh:file"),
        ("GIT_PROTOCOL_FROM_USER", "0"),
        (
            "GIT_SSH_COMMAND",
            "/usr/bin/ssh -F /dev/null -oBatchMode=yes -oPermitLocalCommand=no -oProxyCommand=none",
        ),
        ("GIT_AUTHOR_NAME", "IQ Integration"),
        ("GIT_AUTHOR_EMAIL", "iq@localhost"),
        ("GIT_COMMITTER_NAME", "IQ Integration"),
        ("GIT_COMMITTER_EMAIL", "iq@localhost"),
        ("GIT_NO_REPLACE_OBJECTS", "1"),
    ] {
        command.set_environment(key.into(), value.into());
    }
    #[cfg(debug_assertions)]
    if let (Some(identity), Some(known_hosts)) = (
        std::env::var_os("IQ_TEST_SSH_IDENTITY_FILE"),
        std::env::var_os("IQ_TEST_SSH_KNOWN_HOSTS"),
    ) {
        let identity = Path::new(&identity);
        let known_hosts = Path::new(&known_hosts);
        if identity.is_absolute()
            && known_hosts.is_absolute()
            && !identity.as_os_str().as_encoded_bytes().contains(&b' ')
            && !known_hosts.as_os_str().as_encoded_bytes().contains(&b' ')
        {
            command.set_environment(
                "GIT_SSH_COMMAND".into(),
                format!(
                    "/usr/bin/ssh -F /dev/null -oBatchMode=yes -oPermitLocalCommand=no -oProxyCommand=none -oUserKnownHostsFile={} -i {}",
                    known_hosts.display(),
                    identity.display()
                )
                .into(),
            );
        }
    }
    #[cfg(debug_assertions)]
    if let Some(certificate) = std::env::var_os("IQ_TEST_GIT_SSL_CAINFO") {
        let certificate = Path::new(&certificate);
        if certificate.is_absolute()
            && certificate.is_file()
            && !certificate.as_os_str().as_encoded_bytes().contains(&b' ')
        {
            command.set_environment(
                "GIT_SSL_CAINFO".into(),
                certificate.as_os_str().to_os_string(),
            );
        }
    }
}

pub(crate) fn harden_authorized(command: &mut crate::agent_config::AuthorizedCommand) {
    harden_environment(command);
}

fn repository_config_source(binding: &RepositoryBinding) -> Result<Vec<u8>> {
    let mut source = read_bounded_regular_file(
        &binding.common_dir.join("config"),
        "Git common configuration",
    )?;
    let worktree_config = binding.git_dir.join("config.worktree");
    match std::fs::symlink_metadata(&worktree_config) {
        Ok(_) => {
            source.push(b'\n');
            source.extend(read_bounded_regular_file(
                &worktree_config,
                "Git worktree configuration",
            )?);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect Git worktree configuration"),
    }
    if source.len() > 2 * 1024 * 1024 {
        anyhow::bail!("combined Git configuration exceeds the size limit");
    }
    Ok(source)
}

fn repository_config_source_from_authority(authority: &RepositoryAuthority) -> Result<Vec<u8>> {
    let mut source = read_bounded_regular_file_at(
        &authority.common_dir,
        "config",
        "Git common configuration",
        false,
    )?
    .context("Git common configuration is unavailable")?;
    if let Some(worktree) = read_bounded_regular_file_at(
        &authority.git_dir,
        "config.worktree",
        "Git worktree configuration",
        true,
    )? {
        source.push(b'\n');
        source.extend(worktree);
    }
    if source.len() > 2 * 1024 * 1024 {
        anyhow::bail!("combined Git configuration exceeds the size limit");
    }
    Ok(source)
}

fn open_validated_config_snapshot(
    binding: &RepositoryBinding,
    top_level_descriptor: Option<std::sync::Arc<File>>,
    source: &[u8],
    name: &str,
) -> Result<File> {
    use std::io::Write;

    let name = std::ffi::CString::new(name)?;
    let descriptor = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_ALLOW_SEALING) };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).context("create controlled Git config");
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    file.write_all(source)?;
    file.flush()?;
    let file = seal_config(file)?;
    let validation_file = std::sync::Arc::new(file.try_clone()?);
    let path = PathBuf::from(format!("/proc/self/fd/{}", validation_file.as_raw_fd()));
    let mut validation_command = command()?;
    retain_file_for_spawn(&mut validation_command, validation_file);
    validation_command.env_remove("GIT_CONFIG");
    if let Some(directory) = top_level_descriptor {
        validation_command.current_dir_descriptor(directory, binding.top_level_mount_id);
    } else {
        validation_command.current_dir(&binding.top_level);
    }
    validation_command
        .arg("config")
        .arg("--file")
        .arg(&path)
        .args(["--no-includes", "--null", "--list"]);
    let validation = service_output(&mut validation_command)
        .context("validate controlled Git configuration snapshot")?;
    inspect_safe_config_output(validation, "repository-local")?;
    Ok(file)
}

fn seal_config(mut file: File) -> Result<File> {
    std::io::Write::flush(&mut file)?;
    let seals = libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE;
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) } != 0 {
        return Err(std::io::Error::last_os_error()).context("seal controlled Git config");
    }
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("protect cached Git config from unrelated execs");
    }
    Ok(file)
}

fn bootstrap_controlled_config(
    binding: &RepositoryBinding,
) -> Result<(PathBuf, std::sync::Arc<File>)> {
    let source = repository_config_source(binding)?;
    let file = open_validated_config_snapshot(binding, None, &source, "iq-git-bootstrap-config")?;
    let file = duplicate_inheritable_config(&file)?;
    Ok((
        PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd())),
        file,
    ))
}

fn controlled_config(authority: &RepositoryAuthority) -> Result<(PathBuf, std::sync::Arc<File>)> {
    use std::io::Write;

    let binding = &authority.binding;
    let object_format = binding.object_format.to_string();
    let mut source = repository_config_source_from_authority(authority)?;
    let mut snapshot_digest = Sha256::new();
    snapshot_digest.update(binding.common_dir.as_os_str().as_bytes());
    snapshot_digest.update(binding.git_dir.as_os_str().as_bytes());
    snapshot_digest.update(&source);
    let key = format!(
        "{}:{}:{:x}",
        binding.bare,
        object_format,
        snapshot_digest.finalize()
    );
    if let Some(file) = controlled_configs()
        .lock()
        .map_err(|_| anyhow::anyhow!("controlled Git config registry is poisoned"))?
        .get(&key)
    {
        let file = duplicate_inheritable_config(&file)?;
        return Ok((
            PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd())),
            file,
        ));
    }
    let repository_format_version =
        u8::from(binding.object_format != crate::git_object::GitObjectFormat::Sha1);
    write!(
        source,
        "\n[core]\n\trepositoryFormatVersion = {repository_format_version}\n\tbare = {}\n\thooksPath = /dev/null\n[commit]\n\tgpgSign = false\n[extensions]\n\tobjectFormat = {object_format}\n\tworktreeConfig = false\n",
        binding.bare
    )?;
    let file = std::sync::Arc::new(open_validated_config_snapshot(
        binding,
        Some(authority.top_level.clone()),
        &source,
        "iq-git-config",
    )?);
    let file = controlled_configs()
        .lock()
        .map_err(|_| anyhow::anyhow!("controlled Git config registry is poisoned"))?
        .insert(key, file);
    let file = duplicate_inheritable_config(&file)?;
    Ok((
        PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd())),
        file,
    ))
}

fn duplicate_inheritable_config(file: &File) -> Result<std::sync::Arc<File>> {
    let descriptor = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error())
            .context("duplicate controlled Git config for one command");
    }
    Ok(std::sync::Arc::new(unsafe {
        File::from_raw_fd(descriptor)
    }))
}

struct ControlledConfigRegistry {
    entries: BTreeMap<String, std::sync::Arc<File>>,
    order: std::collections::VecDeque<String>,
}

impl ControlledConfigRegistry {
    const MAX_ENTRIES: usize = 64;

    fn get(&mut self, key: &str) -> Option<std::sync::Arc<File>> {
        let file = self.entries.get(key)?.clone();
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.to_string());
        Some(file)
    }

    fn insert(&mut self, key: String, file: std::sync::Arc<File>) -> std::sync::Arc<File> {
        if let Some(existing) = self.get(&key) {
            return existing;
        }
        while self.entries.len() >= Self::MAX_ENTRIES {
            if let Some(expired) = self.order.pop_front() {
                self.entries.remove(&expired);
            }
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, file.clone());
        file
    }
}

fn controlled_configs() -> &'static Mutex<ControlledConfigRegistry> {
    static CONFIGS: OnceLock<Mutex<ControlledConfigRegistry>> = OnceLock::new();
    CONFIGS.get_or_init(|| {
        Mutex::new(ControlledConfigRegistry {
            entries: BTreeMap::new(),
            order: std::collections::VecDeque::new(),
        })
    })
}

fn retain_file_for_spawn(
    command: &mut crate::agent_config::AuthorizedCommand,
    file: std::sync::Arc<File>,
) {
    command.retain_file(file);
}

pub(crate) fn harden_object_resolution_environment(
    command: &mut crate::agent_config::AuthorizedCommand,
) {
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_REPLACE_REF_BASE",
        "GIT_SHALLOW_FILE",
        "GIT_NAMESPACE",
        "GIT_QUARANTINE_PATH",
        "GIT_CEILING_DIRECTORIES",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    ] {
        command.env_remove(key);
    }
    command.env("GIT_NO_REPLACE_OBJECTS", "1");
}

pub(crate) fn authorize_external_effect(
    cwd: &Path,
    args: &[OsString],
    repository: &crate::repository_policy::GitRepository,
) -> Result<Option<HttpsCredential>> {
    authorize_external_effect_inner(cwd, args, repository, true)
}

pub(crate) fn authorize_external_effect_with_verified_provider(
    cwd: &Path,
    args: &[OsString],
    repository: &crate::repository_policy::GitRepository,
) -> Result<Option<HttpsCredential>> {
    authorize_external_effect_inner(cwd, args, repository, false)
}

fn authorize_external_effect_inner(
    cwd: &Path,
    args: &[OsString],
    repository: &crate::repository_policy::GitRepository,
    verify_provider: bool,
) -> Result<Option<HttpsCredential>> {
    require_verified_cwd(cwd)?;
    require_safe_local_config(cwd)?;
    let Some((command, transport)) = external_transport(args)? else {
        return Ok(None);
    };
    require_no_url_rewrites(cwd)?;
    require_no_external_overrides(cwd)?;
    require_controlled_hooks(cwd)?;
    repository.verify_local_bare()?;
    if verify_provider {
        if let Some(provider) = repository.provider() {
            crate::providers::verify_repository(provider, repository.object_format())?;
        }
    }
    let expected = if command == "push" {
        repository.push_argument()
    } else {
        repository.fetch_argument()
    };
    if transport != expected {
        anyhow::bail!("external Git destination differs from repository policy");
    }
    if verify_provider && expected.as_bytes().starts_with(b"https://") {
        let provider = repository
            .provider()
            .context("HTTPS Git effect has no provider credential authority")?;
        crate::providers::https_credential(provider)
    } else {
        Ok(None)
    }
}

pub(crate) fn require_safe_local_config(cwd: &Path) -> Result<()> {
    let binding = expected_binding(cwd)?;
    require_safe_local_config_bound(&binding)
}

fn require_safe_local_config_bound(binding: &RepositoryBinding) -> Result<()> {
    binding.verify()?;
    inspect_safe_local_config_bound(binding)
}

fn inspect_safe_local_config_bound(binding: &RepositoryBinding) -> Result<()> {
    inspect_safe_config(binding, "--local", "repository-local")?;
    let worktree_config = binding.git_dir.join("config.worktree");
    match std::fs::symlink_metadata(&worktree_config) {
        Ok(_) => {
            inspect_safe_config_file(&binding.top_level, &worktree_config, "repository-worktree")
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("inspect repository-worktree Git configuration"),
    }
}

fn inspect_safe_config(binding: &RepositoryBinding, _scope: &str, label: &str) -> Result<()> {
    let path = binding.common_dir.join("config");
    inspect_safe_config_file(&binding.top_level, &path, label)
}

pub(crate) fn local_config_digest(repository: &Path) -> Result<String> {
    let binding = RepositoryBinding::capture(repository)?;
    let mut digest = Sha256::new();
    for path in [
        binding.common_dir.join("config"),
        binding.git_dir.join("config.worktree"),
    ] {
        digest.update(path.as_os_str().as_bytes());
        match std::fs::symlink_metadata(&path) {
            Ok(_) => digest.update(read_bounded_regular_file(&path, "Git configuration")?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => digest.update(b"missing"),
            Err(error) => return Err(error).context("inspect Git configuration"),
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn inspect_safe_config_file(cwd: &Path, path: &Path, label: &str) -> Result<()> {
    let before = config_fingerprint(path, label)?;
    if validated_config_cache()
        .lock()
        .map_err(|_| anyhow::anyhow!("validated Git config cache is poisoned"))?
        .get(path)
        == Some(&before)
    {
        return Ok(());
    }
    let mut inspect = command()?;
    inspect
        .env_remove("GIT_CONFIG")
        .arg("config")
        .arg("--file")
        .arg(path)
        .args(["--no-includes", "--null", "--list"])
        .current_dir(cwd);
    let output = service_output(&mut inspect)
        .with_context(|| format!("inspect {label} Git configuration"))?;
    inspect_safe_config_output(output, label)?;
    cache_validated_config(path.to_path_buf(), before, label)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ConfigFingerprint {
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

fn config_fingerprint(path: &Path, label: &str) -> Result<ConfigFingerprint> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 1024 * 1024 {
        anyhow::bail!("{label} Git configuration is not a bounded regular file");
    }
    Ok(ConfigFingerprint {
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn cache_validated_config(path: PathBuf, before: ConfigFingerprint, label: &str) -> Result<()> {
    if config_fingerprint(&path, label)? != before {
        anyhow::bail!("{label} Git configuration changed while validating");
    }
    validated_config_cache()
        .lock()
        .map_err(|_| anyhow::anyhow!("validated Git config cache is poisoned"))?
        .insert(path, before);
    Ok(())
}

fn validated_config_cache() -> &'static Mutex<BTreeMap<PathBuf, ConfigFingerprint>> {
    static CACHE: OnceLock<Mutex<BTreeMap<PathBuf, ConfigFingerprint>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn inspect_safe_config_output(output: Output, label: &str) -> Result<()> {
    if !output.status.success() {
        anyhow::bail!(
            "cannot inspect {label} Git configuration: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    for record in output.stdout.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let separator = record
            .iter()
            .position(|byte| *byte == b'\n')
            .with_context(|| format!("{label} Git configuration record has no value"))?;
        let (key, value) = record.split_at(separator);
        let value = &value[1..];
        let key = std::str::from_utf8(key)?.to_ascii_lowercase();
        let allowed = matches!(
            key.as_str(),
            "core.repositoryformatversion"
                | "core.filemode"
                | "core.bare"
                | "core.logallrefupdates"
                | "core.ignorecase"
                | "core.precomposeunicode"
                | "extensions.objectformat"
                | "extensions.worktreeconfig"
                | "user.name"
                | "user.email"
        ) || (key.starts_with("branch.")
            && (key.ends_with(".remote") || key.ends_with(".merge")))
            || (key.starts_with("remote.")
                && (key.ends_with(".url") || key.ends_with(".pushurl") || key.ends_with(".fetch")))
            || (key == "commit.gpgsign" && value.eq_ignore_ascii_case(b"false"))
            || (key == "core.hookspath" && value == b"/dev/null");
        if !allowed {
            anyhow::bail!("{label} Git configuration is not allowed: {key}");
        }
    }
    Ok(())
}

fn require_controlled_hooks(cwd: &Path) -> Result<()> {
    let output = output(cwd, ["config", "--show-origin", "--get", "core.hooksPath"])?;
    if !output.status.success() {
        anyhow::bail!(
            "cannot verify controlled Git hooks path: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let values = String::from_utf8(output.stdout)?;
    let Some(line) = values.lines().next() else {
        anyhow::bail!("Git hooks path is not controlled");
    };
    if line.split_whitespace().last() != Some("/dev/null") {
        anyhow::bail!("Git hooks path has an effective override");
    }
    Ok(())
}

fn require_no_external_overrides(cwd: &Path) -> Result<()> {
    let output = output(
        cwd,
        [
            "config",
            "--name-only",
            "--get-regexp",
            r"^(credential($|\.)|core\.(sshcommand|askpass)$|http\..*\.extraheader$|remote\..*\.(uploadpack|receivepack|proxy)$|protocol\..*\.allow$|ssh\.variant$)",
        ],
    )?;
    match output.status.code() {
        Some(1) => Ok(()),
        Some(0) => anyhow::bail!(
            "repository Git configuration contains forbidden credential or transport overrides: {}",
            String::from_utf8_lossy(&output.stdout).trim()
        ),
        _ => anyhow::bail!(
            "cannot inspect repository Git credential and transport overrides: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

pub(crate) fn external_output<I, S>(
    cwd: &Path,
    args: I,
    repository: &crate::repository_policy::GitRepository,
    authorize_release: impl FnOnce() -> Result<()>,
) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args = args
        .into_iter()
        .map(|argument| argument.as_ref().to_os_string())
        .collect::<Vec<_>>();
    let mut command = command_in(cwd)?;
    command.args(&args);
    let outcome = crate::integrator::command_output_timeout_with_prepare(
        crate::integrator::CommandProgram::Descriptor {
            label: "git",
            authority: command.executable_authority(),
        },
        &args,
        Some(cwd),
        std::time::Duration::from_secs(60),
        |gate| {
            authorize_release()?;
            let credential = authorize_external_effect(cwd, &args, repository)?;
            gate.set_https_credential(credential);
            gate.write_all(b"run\n")?;
            Ok(true)
        },
        || Ok(crate::sqlite::ExecutionAuthority::Active),
        |_| Ok(()),
    )?;
    match outcome {
        crate::integrator::CommandOutputOutcome::Exited(output) => Ok(output),
        crate::integrator::CommandOutputOutcome::Cancelled => {
            anyhow::bail!("external Git command lost release authority")
        }
    }
}

fn external_transport(args: &[OsString]) -> Result<Option<(&str, &OsStr)>> {
    let Some(command_index) = args.iter().position(|argument| {
        argument
            .to_str()
            .is_some_and(|value| matches!(value, "clone" | "fetch" | "ls-remote" | "push"))
    }) else {
        return Ok(None);
    };
    let command = args[command_index]
        .to_str()
        .context("external Git command is not valid UTF-8")?;
    let transport = args[command_index + 1..]
        .iter()
        .find(|argument| !argument.as_encoded_bytes().starts_with(b"-"))
        .context("external Git command has no explicit destination")?;
    Ok(Some((command, transport)))
}

pub(crate) fn external_effect_uses_https(
    args: &[OsString],
    repository: &crate::repository_policy::GitRepository,
) -> Result<bool> {
    let Some((command, transport)) = external_transport(args)? else {
        return Ok(false);
    };
    let expected = if command == "push" {
        repository.push_argument()
    } else {
        repository.fetch_argument()
    };
    Ok(transport == expected && expected.as_bytes().starts_with(b"https://"))
}

pub(crate) fn output<I, S>(cwd: &Path, args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = command_in(cwd)?;
    command.args(args);
    service_output(&mut command).context("run hardened Git command")
}

pub(crate) fn read_outputs(cwd: &Path, commands: &[Vec<OsString>]) -> Result<Vec<Output>> {
    if commands.is_empty() {
        anyhow::bail!("Git read operation is empty");
    }
    let binding = expected_binding(cwd)?;
    let authority = RepositoryAuthority::open(&binding)?;
    let mut prepared = commands
        .iter()
        .map(|arguments| {
            let mut command = command_in(cwd)?;
            command.args(arguments);
            Ok(command)
        })
        .collect::<Result<Vec<_>>>()?;
    crate::agent_runner::service_read_operation(
        &mut prepared,
        std::time::Duration::from_secs(60),
        |_| authority.verify_control_state(),
    )
    .context("run bounded Git read operation")
}

pub(crate) fn service_output(
    command: &mut crate::agent_config::AuthorizedCommand,
) -> Result<Output> {
    command.output()
}

pub(crate) fn require_verified_cwd(cwd: &Path) -> Result<()> {
    if !cwd.is_absolute() {
        anyhow::bail!("Git working directory must be absolute");
    }
    let metadata = std::fs::symlink_metadata(cwd)
        .with_context(|| format!("inspect Git working directory {}", cwd.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("Git working directory must be a real directory");
    }
    let canonical = cwd
        .canonicalize()
        .with_context(|| format!("resolve Git working directory {}", cwd.display()))?;
    if canonical != cwd {
        anyhow::bail!("Git working directory must be canonical");
    }
    Ok(())
}

pub(crate) fn require_no_url_rewrites(cwd: &Path) -> Result<()> {
    let output = output(
        cwd,
        [
            "config",
            "--name-only",
            "--get-regexp",
            r"^url\..*\.(insteadof|pushinsteadof)$",
        ],
    )?;
    match output.status.code() {
        Some(1) => Ok(()),
        Some(0) => anyhow::bail!(
            "repository Git configuration contains forbidden URL rewrites: {}",
            String::from_utf8_lossy(&output.stdout).trim()
        ),
        _ => anyhow::bail!(
            "cannot inspect repository Git URL rewrites: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

pub(crate) fn is_external_operation(args: &[std::ffi::OsString]) -> bool {
    external_transport(args).is_ok_and(|value| value.is_some())
}

pub(crate) fn is_read_only_operation(args: &[std::ffi::OsString]) -> bool {
    args.first()
        .and_then(|argument| argument.to_str())
        .is_some_and(|command| {
            matches!(
                command,
                "cat-file"
                    | "config"
                    | "diff"
                    | "diff-index"
                    | "for-each-ref"
                    | "log"
                    | "ls-files"
                    | "ls-tree"
                    | "merge-base"
                    | "name-rev"
                    | "remote"
                    | "rev-list"
                    | "rev-parse"
                    | "show"
                    | "status"
                    | "symbolic-ref"
            )
        })
}

#[cfg(test)]
mod tests {
    #[cfg(debug_assertions)]
    use super::external_output;
    #[cfg(debug_assertions)]
    use super::inject_test_git_executable;
    use super::{
        apply_https_credential, authorize_current, command, command_in, init_repository, output,
        require_safe_local_config, HttpsCredential, RepositoryBinding,
    };
    use std::fs;
    use std::io::Write;
    #[cfg(debug_assertions)]
    use std::net::TcpStream;
    use std::os::unix::fs::symlink;
    #[cfg(debug_assertions)]
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;
    #[cfg(debug_assertions)]
    use std::process::Child;
    use std::process::{Command, Stdio};
    #[cfg(debug_assertions)]
    use std::sync::{Mutex, OnceLock};
    #[cfg(debug_assertions)]
    use std::time::{Duration, Instant};

    #[test]
    fn command_uses_immutable_config_when_local_config_changes_before_release() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().canonicalize().unwrap();
        init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();

        let mut prepared = command_in(&repository).unwrap();
        prepared.args(["config", "--null", "--list"]);
        let mut local_config = fs::OpenOptions::new()
            .append(true)
            .open(repository.join(".git/config"))
            .unwrap();
        local_config
            .write_all(
                b"\n[url \"file:///attacker\"]\n\tinsteadOf = https://trusted/\n[credential]\n\thelper = !false\n",
            )
            .unwrap();
        local_config.sync_all().unwrap();

        let result = prepared.output().unwrap();
        assert!(result.status.success());
        let config = String::from_utf8(result.stdout).unwrap();
        assert!(!config.contains("attacker"));
        assert!(!config.contains("credential.helper"));
    }

    #[test]
    fn memory_only_askpass_supplies_https_credentials_without_a_file() {
        let credential = HttpsCredential::new("x-access-token", "private-test-token").unwrap();
        let mut command = command().unwrap();
        command
            .args(["credential", "fill"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let askpass = apply_https_credential(&mut command, &credential).unwrap();
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"protocol=https\nhost=github.com\n\n")
            .unwrap();
        let output = child.wait_with_output().unwrap();
        drop(askpass);

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let values = String::from_utf8(output.stdout).unwrap();
        assert!(values.contains("username=x-access-token\n"));
        assert!(values.contains("password=private-test-token\n"));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn rejected_external_release_does_not_send_the_command_token() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let remote = temporary.path().join("remote.git");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&remote).unwrap();
        init_repository(&source, crate::git_object::GitObjectFormat::Sha1).unwrap();
        assert!(Command::new("/usr/bin/git")
            .current_dir(&remote)
            .args(["init", "--bare", "--object-format=sha1"])
            .status()
            .unwrap()
            .success());
        output(&source, ["config", "user.name", "IQ Test"]).unwrap();
        output(&source, ["config", "user.email", "iq@example.test"]).unwrap();
        fs::write(source.join("content.txt"), "content\n").unwrap();
        output(&source, ["add", "content.txt"]).unwrap();
        output(&source, ["commit", "-m", "content"]).unwrap();
        let remote = remote.canonicalize().unwrap();
        let metadata = fs::metadata(&remote).unwrap();
        let repository = crate::repository_policy::GitRepository::LocalBare {
            path: remote.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
            object_format: crate::git_object::GitObjectFormat::Sha1,
        };

        let error = external_output(
            &source,
            ["push", remote.to_str().unwrap(), "HEAD:refs/heads/main"],
            &repository,
            || anyhow::bail!("release authority changed"),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("release authority changed"));
        assert!(!Command::new("/usr/bin/git")
            .current_dir(&remote)
            .args(["show-ref", "--verify", "refs/heads/main"])
            .status()
            .unwrap()
            .success());
    }

    #[cfg(debug_assertions)]
    struct HttpsGitServer(Child);

    #[cfg(debug_assertions)]
    impl Drop for HttpsGitServer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[cfg(debug_assertions)]
    fn test_environment_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[cfg(debug_assertions)]
    fn tree_contains(root: &std::path::Path, needle: &[u8]) -> bool {
        fs::read_dir(root).unwrap().any(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                tree_contains(&path, needle)
            } else {
                fs::read(path)
                    .is_ok_and(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
            }
        })
    }

    #[test]
    #[cfg(debug_assertions)]
    fn private_https_fetch_and_push_use_only_injected_memory_credentials() {
        let _provider_execution = crate::providers::lock_test_provider_execution();
        let _lock = test_environment_lock().lock().unwrap();
        let root = tempfile::tempdir().unwrap();
        let projects = root.path().join("projects");
        let remote = projects.join("acme/repo.git");
        fs::create_dir_all(remote.parent().unwrap()).unwrap();
        assert!(Command::new("/usr/bin/git")
            .args(["init", "--bare", remote.to_str().unwrap()])
            .status()
            .unwrap()
            .success());
        assert!(Command::new("/usr/bin/git")
            .args([
                "-C",
                remote.to_str().unwrap(),
                "config",
                "http.receivepack",
                "true"
            ])
            .status()
            .unwrap()
            .success());

        let certificate = root.path().join("certificate.pem");
        let key = root.path().join("certificate.key");
        assert!(Command::new("/usr/bin/openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=127.0.0.1",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-keyout",
                key.to_str().unwrap(),
                "-out",
                certificate.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let server_source = root.path().join("https-git-server.py");
        fs::write(
            &server_source,
            r#"import base64, http.server, os, ssl, subprocess, urllib.parse
class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, format, *args):
        pass
    def run_backend(self):
        expected = "Basic " + base64.b64encode(("x-access-token:" + os.environ["PRIVATE_TOKEN"]).encode()).decode()
        if self.headers.get("Authorization") != expected:
            self.send_response(401)
            self.send_header("WWW-Authenticate", "Basic realm=iq-test")
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return
        parsed = urllib.parse.urlsplit(self.path)
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        env = os.environ.copy()
        env.update({
            "GIT_PROJECT_ROOT": os.environ["PROJECT_ROOT"],
            "GIT_HTTP_EXPORT_ALL": "1",
            "PATH_INFO": parsed.path,
            "QUERY_STRING": parsed.query,
            "REQUEST_METHOD": self.command,
            "CONTENT_TYPE": self.headers.get("Content-Type", ""),
            "CONTENT_LENGTH": str(len(body)),
            "REMOTE_USER": "x-access-token",
        })
        result = subprocess.run(["/usr/lib/git-core/git-http-backend"], input=body, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, check=False)
        headers, payload = result.stdout.split(b"\r\n\r\n", 1)
        status = 200
        parsed_headers = []
        for line in headers.decode().split("\r\n"):
            name, value = line.split(":", 1)
            if name.lower() == "status":
                status = int(value.strip().split()[0])
            else:
                parsed_headers.append((name, value.strip()))
        self.send_response(status)
        for name, value in parsed_headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)
    do_GET = run_backend
    do_POST = run_backend
server = http.server.HTTPServer(("127.0.0.1", int(os.environ["PORT"])), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(os.environ["CERTIFICATE"], os.environ["KEY"])
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
"#,
        )
        .unwrap();
        let server_log = fs::File::create(root.path().join("server.log")).unwrap();
        let server = Command::new("/usr/bin/python3")
            .arg(&server_source)
            .env("PORT", port.to_string())
            .env("PROJECT_ROOT", &projects)
            .env("CERTIFICATE", &certificate)
            .env("KEY", &key)
            .env("PRIVATE_TOKEN", "private-test-token")
            .stdout(Stdio::null())
            .stderr(Stdio::from(server_log))
            .spawn()
            .unwrap();
        let _server = HttpsGitServer(server);
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "HTTPS Git server did not start");
            std::thread::sleep(Duration::from_millis(10));
        }

        let provider = root.path().join("gh");
        let provider_log = root.path().join("provider.log");
        fs::write(
            &provider,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1 $2\" = 'auth token' ]; then printf '%s' \"$IQ_TEST_PROVIDER_TOKEN\"; exit 0; fi\ncase \"$*\" in\n*hash-algorithm*) printf '%s' '{{\"hash_algorithm\":\"sha1\"}}' ;;\n*) printf '%s' '{{\"node_id\":\"R_private\",\"full_name\":\"acme/repo\"}}' ;;\nesac\n",
                provider_log.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&provider, fs::Permissions::from_mode(0o755)).unwrap();
        let _provider = crate::providers::inject_test_provider_executable(
            crate::repository_policy::Provider::Github,
            &provider,
        )
        .unwrap();
        std::env::set_var("IQ_TEST_PROVIDER_TOKEN", "private-test-token");
        std::env::set_var("IQ_TEST_GIT_SSL_CAINFO", &certificate);

        let url = format!("https://127.0.0.1:{port}/acme/repo.git");
        let repository = crate::repository_policy::GitRepository::Accessible {
            fetch_url: url.clone(),
            push_url: url.clone(),
            repository_id: "R_private".into(),
            provider: crate::repository_policy::ProviderRepository {
                provider: crate::repository_policy::Provider::Github,
                host: "127.0.0.1".into(),
                repository: "acme/repo".into(),
                repository_id: "R_private".into(),
            },
            object_format: crate::git_object::GitObjectFormat::Sha1,
        }
        .validate("private test repository")
        .unwrap();
        let source = root.path().join("source");
        fs::create_dir(&source).unwrap();
        init_repository(&source, crate::git_object::GitObjectFormat::Sha1).unwrap();
        output(&source, ["config", "user.name", "IQ Test"]).unwrap();
        output(&source, ["config", "user.email", "iq@example.test"]).unwrap();
        fs::write(source.join("content.txt"), "private content\n").unwrap();
        output(&source, ["add", "content.txt"]).unwrap();
        output(&source, ["commit", "-m", "private content"]).unwrap();
        let source_head = String::from_utf8(output(&source, ["rev-parse", "HEAD"]).unwrap().stdout)
            .unwrap()
            .trim()
            .to_string();
        let pushed = external_output(
            &source,
            ["push", url.as_str(), "HEAD:refs/heads/main"],
            &repository,
            || Ok(()),
        )
        .unwrap();
        assert!(
            pushed.status.success(),
            "{}",
            String::from_utf8_lossy(&pushed.stderr)
        );

        let destination = root.path().join("destination");
        fs::create_dir(&destination).unwrap();
        init_repository(&destination, crate::git_object::GitObjectFormat::Sha1).unwrap();
        let fetched = external_output(
            &destination,
            ["fetch", url.as_str(), "main"],
            &repository,
            || Ok(()),
        )
        .unwrap();
        assert!(
            fetched.status.success(),
            "{}",
            String::from_utf8_lossy(&fetched.stderr)
        );
        let fetched_head = String::from_utf8(
            output(&destination, ["rev-parse", "FETCH_HEAD"])
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        assert_eq!(fetched_head, source_head);

        std::env::remove_var("IQ_TEST_PROVIDER_TOKEN");
        std::env::remove_var("IQ_TEST_GIT_SSL_CAINFO");
        assert!(!fs::read_to_string(provider_log)
            .unwrap()
            .contains("private-test-token"));
        assert!(!tree_contains(root.path(), b"private-test-token"));
    }

    #[test]
    fn repository_local_executable_and_network_overrides_are_rejected() {
        for (key, value) in [
            ("http.proxy", "http://127.0.0.1:9"),
            ("http.sslVerify", "false"),
            ("credential.helper", "!false"),
            ("core.sshCommand", "false"),
            ("core.hooksPath", "/tmp/hostile-hooks"),
            ("core.fsmonitor", "false"),
            ("filter.hostile.process", "false"),
            ("filter.hostile.clean", "false"),
            ("filter.hostile.smudge", "false"),
            ("merge.hostile.driver", "false"),
            ("diff.hostile.command", "false"),
            ("diff.hostile.textconv", "false"),
            ("protocol.file.allow", "always"),
            ("remote.origin.proxy", "false"),
            ("remote.origin.uploadpack", "false"),
            ("remote.origin.receivepack", "false"),
            ("include.path", "/tmp/hostile-config"),
            ("includeIf.gitdir:/tmp/.path", "/tmp/hostile-config"),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let repository = temporary.path().canonicalize().unwrap();
            let initialized = Command::new("/usr/bin/git")
                .args(["init", "--object-format=sha1", repository.to_str().unwrap()])
                .output()
                .unwrap();
            assert!(initialized.status.success());
            authorize_current(&repository).unwrap();
            let configured = Command::new("/usr/bin/git")
                .current_dir(&repository)
                .args(["config", "--local", key, value])
                .output()
                .unwrap();
            assert!(configured.status.success(), "{key}");

            let error = require_safe_local_config(&repository).unwrap_err();

            assert!(
                error.to_string().contains(&key.to_ascii_lowercase()),
                "key={key} error={error:#}"
            );
        }
    }

    #[test]
    fn repository_worktree_executable_overrides_are_rejected_before_git_runs() {
        for (key, value) in [
            ("core.fsmonitor", "./hostile-command"),
            ("filter.hostile.process", "./hostile-command"),
            ("merge.hostile.driver", "./hostile-command"),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let repository = temporary.path().canonicalize().unwrap();
            let marker = repository.join("hostile-command-ran");
            let hostile = repository.join("hostile-command");
            std::fs::write(
                &hostile,
                format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
            )
            .unwrap();
            std::fs::set_permissions(&hostile, std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(Command::new("/usr/bin/git")
                .args(["init", "--object-format=sha1", repository.to_str().unwrap(),])
                .status()
                .unwrap()
                .success());
            authorize_current(&repository).unwrap();
            assert!(Command::new("/usr/bin/git")
                .current_dir(&repository)
                .args(["config", "extensions.worktreeConfig", "true"])
                .status()
                .unwrap()
                .success());
            assert!(Command::new("/usr/bin/git")
                .current_dir(&repository)
                .args(["config", "--worktree", key, value])
                .status()
                .unwrap()
                .success());

            let error = output(&repository, ["status", "--short"]).unwrap_err();

            assert!(
                format!("{error:#}").contains(&key.to_ascii_lowercase()),
                "key={key} error={error:#}"
            );
            assert!(!marker.exists(), "worktree command ran for {key}");
        }
    }

    #[test]
    fn capture_detects_sha1_and_sha256_before_building_controlled_config() {
        for object_format in [
            crate::git_object::GitObjectFormat::Sha1,
            crate::git_object::GitObjectFormat::Sha256,
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let repository = temporary.path().join(object_format.to_string());
            let initialized = Command::new("/usr/bin/git")
                .args([
                    "init",
                    &format!("--object-format={object_format}"),
                    repository.to_str().unwrap(),
                ])
                .output()
                .unwrap();
            assert!(
                initialized.status.success(),
                "{}",
                String::from_utf8_lossy(&initialized.stderr)
            );
            let repository = repository.canonicalize().unwrap();

            assert_eq!(
                RepositoryBinding::capture(&repository)
                    .unwrap()
                    .object_format,
                object_format
            );
        }
    }

    #[test]
    fn hardened_initialization_creates_and_verifies_repository_under_non_git_tmp_parent() {
        let temporary = tempfile::tempdir().unwrap();
        let parent = temporary.path().canonicalize().unwrap();
        assert!(!Command::new("/usr/bin/git")
            .current_dir(&parent)
            .args(["rev-parse", "--is-inside-work-tree"])
            .status()
            .unwrap()
            .success());
        let repository = parent.join("storage");
        std::fs::create_dir(&repository).unwrap();

        init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();

        let root = output(&repository, ["rev-parse", "--show-toplevel"]).unwrap();
        assert!(root.status.success());
        assert_eq!(
            root.stdout,
            format!("{}\n", repository.display()).as_bytes()
        );
        require_safe_local_config(&repository).unwrap();
    }

    #[test]
    fn binding_capture_rejects_empty_admin_directory_and_invalid_head() {
        let temporary = tempfile::tempdir().unwrap();
        let empty = temporary.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        std::fs::create_dir(empty.join(".git")).unwrap();
        assert!(RepositoryBinding::capture(&empty).is_err());

        let malformed = temporary.path().join("malformed");
        std::fs::create_dir(&malformed).unwrap();
        assert!(Command::new("/usr/bin/git")
            .args(["init", "--object-format=sha1", malformed.to_str().unwrap(),])
            .status()
            .unwrap()
            .success());
        std::fs::write(malformed.join(".git/HEAD"), b"ref: invalid reference\n").unwrap();
        assert!(RepositoryBinding::capture(&malformed).is_err());
    }

    #[test]
    fn semantic_object_resolution_overrides_block_commands_before_execution() {
        for (name, install, expected) in [
            (
                "replace",
                "git update-ref refs/replace/$(git rev-parse HEAD^) HEAD",
                "replacement refs",
            ),
            (
                "packed-replace",
                "git update-ref refs/replace/$(git rev-parse HEAD^) HEAD && git pack-refs --all --prune",
                "replacement refs",
            ),
            (
                "grafts",
                "mkdir -p .git/info && git rev-parse HEAD > .git/info/grafts",
                "legacy grafts",
            ),
            (
                "alternates",
                "mkdir -p .git/objects/info && printf '/tmp/hostile-objects\\n' > .git/objects/info/alternates",
                "alternate object database",
            ),
            (
                "shallow",
                "git rev-parse HEAD > .git/shallow",
                "shallow history",
            ),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let repository = temporary.path().join(name);
            std::fs::create_dir(&repository).unwrap();
            init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();
            assert!(output(&repository, ["commit", "--allow-empty", "-m", "base"])
                .unwrap()
                .status
                .success());
            assert!(output(
                &repository,
                ["commit", "--allow-empty", "-m", "candidate"]
            )
            .unwrap()
            .status
            .success());
            let installed = Command::new("/bin/sh")
                .current_dir(&repository)
                .args(["-c", install])
                .status()
                .unwrap();
            assert!(installed.success(), "failed to install {name}");

            let error = output(&repository, ["rev-parse", "HEAD^{tree}"]).unwrap_err();

            assert!(format!("{error:#}").contains(expected), "{name}: {error:#}");
        }
    }

    #[test]
    fn replaced_git_directory_blocks_all_mutating_command_classes() {
        for args in [
            vec!["reset", "--hard", "HEAD"],
            vec!["clean", "-ffd"],
            vec!["update-ref", "refs/heads/hostile", "HEAD"],
            vec!["fetch", "/tmp/hostile"],
            vec!["push", "/tmp/hostile", "HEAD:refs/heads/main"],
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let repository = temporary.path().join("repository");
            std::fs::create_dir(&repository).unwrap();
            init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();
            std::fs::rename(repository.join(".git"), repository.join("authorized.git")).unwrap();
            assert!(Command::new("/usr/bin/git")
                .args(["init", "--object-format=sha1", repository.to_str().unwrap(),])
                .status()
                .unwrap()
                .success());

            let error = output(&repository, args).unwrap_err();

            assert!(
                format!("{error:#}").contains("binding changed after authorization"),
                "{error:#}"
            );
        }
    }

    #[test]
    fn prepared_git_command_uses_bound_repository_after_path_exchange() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let retained = temporary.path().join("retained");
        std::fs::create_dir(&repository).unwrap();
        init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();
        assert!(
            output(&repository, ["commit", "--allow-empty", "-m", "base"])
                .unwrap()
                .status
                .success()
        );
        let mut prepared = command_in(&repository).unwrap();
        prepared.args(["update-ref", "refs/heads/descriptor-bound", "HEAD"]);
        std::fs::rename(&repository, &retained).unwrap();
        std::fs::create_dir(&repository).unwrap();
        assert!(Command::new("/usr/bin/git")
            .args(["init", "--object-format=sha1", repository.to_str().unwrap()])
            .status()
            .unwrap()
            .success());

        let result = prepared.output().unwrap();

        assert!(result.status.success(), "{:?}", result.stderr);
        assert!(Command::new("/usr/bin/git")
            .current_dir(&retained)
            .args(["show-ref", "--verify", "refs/heads/descriptor-bound"])
            .status()
            .unwrap()
            .success());
        assert!(!Command::new("/usr/bin/git")
            .current_dir(&repository)
            .args(["show-ref", "--verify", "refs/heads/descriptor-bound"])
            .status()
            .unwrap()
            .success());
    }

    #[test]
    fn test_script_adapter_preserves_repository_descriptors_for_native_git() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let wrapper = temporary.path().join("git");
        std::fs::create_dir(&repository).unwrap();
        init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();
        std::fs::write(&wrapper, "#!/bin/sh\nexec /usr/bin/git \"$@\"\n").unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        let _authority = inject_test_git_executable(&wrapper).unwrap();

        let result = output(&repository, ["status", "--short"]).unwrap();

        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[test]
    fn linked_worktree_binding_uses_exact_git_and_common_directories() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let worktree = temporary.path().join("worktree");
        std::fs::create_dir(&repository).unwrap();
        init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();
        assert!(
            output(&repository, ["commit", "--allow-empty", "-m", "base"])
                .unwrap()
                .status
                .success()
        );
        assert!(Command::new("/usr/bin/git")
            .current_dir(&repository)
            .args([
                "worktree",
                "add",
                "-b",
                "linked",
                worktree.to_str().unwrap()
            ])
            .status()
            .unwrap()
            .success());
        let binding = authorize_current(&worktree).unwrap();

        let head = output(&worktree, ["rev-parse", "HEAD"]).unwrap();
        let merge = output(&worktree, ["merge", "--no-ff", "--no-commit", "HEAD"]).unwrap();

        assert!(head.status.success());
        assert!(merge.status.success(), "{:?}", merge.stderr);
        assert_ne!(binding.git_dir, binding.common_dir);
    }

    #[test]
    fn alternate_gitdir_file_and_symlink_swaps_are_rejected() {
        for use_symlink in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let repository = temporary.path().join("repository");
            std::fs::create_dir(&repository).unwrap();
            init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();
            let authorized = repository.join("authorized.git");
            std::fs::rename(repository.join(".git"), &authorized).unwrap();
            if use_symlink {
                symlink(&authorized, repository.join(".git")).unwrap();
            } else {
                std::fs::write(
                    repository.join(".git"),
                    format!("gitdir: {}\n", authorized.display()),
                )
                .unwrap();
            }

            let error = output(&repository, ["reset", "--hard", "HEAD"]).unwrap_err();

            assert!(format!("{error:#}").contains("Git"), "{error:#}");
        }
    }

    #[test]
    fn linked_worktree_reassignment_is_rejected() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        std::fs::create_dir(&repository).unwrap();
        init_repository(&repository, crate::git_object::GitObjectFormat::Sha1).unwrap();
        assert!(
            output(&repository, ["commit", "--allow-empty", "-m", "base"])
                .unwrap()
                .status
                .success()
        );
        for (branch, path) in [("first", &first), ("second", &second)] {
            assert!(Command::new("/usr/bin/git")
                .current_dir(&repository)
                .args(["worktree", "add", "-b", branch, path.to_str().unwrap()])
                .status()
                .unwrap()
                .success());
        }
        authorize_current(&first).unwrap();
        std::fs::write(
            first.join(".git"),
            std::fs::read(second.join(".git")).unwrap(),
        )
        .unwrap();

        let error = output(&first, ["clean", "-ffd"]).unwrap_err();

        assert!(
            format!("{error:#}").contains("gitdir backlink differs"),
            "{error:#}"
        );
    }
}
