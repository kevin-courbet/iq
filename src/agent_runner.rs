use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::agent_config::{verify_executable, IntegrationAgentConfig};
use crate::agent_protocol::{
    atomic_write_json, parse_result, prepare_publication, protocol_directory,
    publish_result_bundle, read_complete_result, read_published_result, AgentInput, AgentResult,
};
use crate::control_domain::{InfrastructureCause, RunnerSnapshot};

const SANDBOX_HELPER: &str = r#"#!/bin/sh
set -eu
root=$1
lower=$2
bytes=$3
cycle=$4
cpu=$5
open_files=$6
protocol=$7
log_blocks=$8
shift 8
IFS= read -r admission
test "$admission" = run
ulimit -t "$cpu"
ulimit -n "$open_files"
ulimit -f "$log_blocks"
mount -t tmpfs -o "size=$bytes,nosuid,nodev" tmpfs "$root/tmpfs"
mkdir "$root/tmpfs/upper" "$root/tmpfs/work" "$root/tmpfs/home" "$root/tmpfs/tmp" "$root/tmpfs/protocol" "$root/tmpfs/export"
cp "$protocol/input.json" "$root/tmpfs/protocol/input.json"
chmod 0400 "$root/tmpfs/protocol/input.json"
mount -t overlay overlay -o "lowerdir=$lower,upperdir=$root/tmpfs/upper,workdir=$root/tmpfs/work" "$root/repo"
set +e
"$@"
status=$?
set -e
git -C "$root/repo" diff --cached --binary --full-index > "$root/tmpfs/export/staged.patch"
git -C "$root/repo" diff --cached --name-only -z > "$root/tmpfs/export/staged.paths"
git -C "$root/repo" diff --name-only -z > "$root/tmpfs/export/unstaged.paths"
git -C "$root/repo" write-tree > "$root/tmpfs/export/staged.tree" || : > "$root/tmpfs/export/staged.tree"
git -C "$root/repo" rev-parse HEAD > "$root/tmpfs/export/head"
git -C "$root/repo" for-each-ref --format='%(refname) %(objectname)' > "$root/tmpfs/export/refs"
git -C "$root/repo" config --local --list --show-origin > "$root/tmpfs/export/config"
git -C "$root/repo" remote -v > "$root/tmpfs/export/remotes"
if test -f "$root/tmpfs/protocol/result.json" && ! test -L "$root/tmpfs/protocol/result.json"; then
  cp "$root/tmpfs/protocol/result.json" "$root/tmpfs/export/result.json"
fi
for file in result.json staged.patch staged.paths unstaged.paths staged.tree head refs config remotes; do
  test -f "$root/tmpfs/export/$file" && ! test -L "$root/tmpfs/export/$file"
  cp "$root/tmpfs/export/$file" "$root/export/$file"
  chmod 0600 "$root/export/$file"
done
exit "$status"
"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerOutcome {
    Complete {
        result: Box<AgentResult>,
        result_state: crate::control_domain::AtomicResultState,
        log: Vec<u8>,
        exit_status: ExitStatus,
        export_directory: PathBuf,
    },
    MissingResult {
        log: Vec<u8>,
        exit_status: ExitStatus,
    },
    TimedOut {
        log: Vec<u8>,
    },
    AuthorityLost {
        log: Vec<u8>,
    },
    InvalidResult {
        log: Vec<u8>,
        reason: String,
        export_directory: PathBuf,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProtectedIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    digest: Option<String>,
}

pub struct OpenCodeRunner {
    config: IntegrationAgentConfig,
    snapshot: RunnerSnapshot,
}

pub struct RunnerLifecycle<P, S, W, A> {
    pub on_prepared: P,
    pub on_started: S,
    pub on_writing: W,
    pub authority_active: A,
}

pub struct RestartResult {
    pub input: AgentInput,
    pub input_sha256: String,
    pub result: Box<AgentResult>,
    pub export_directory: PathBuf,
    pub result_state: crate::control_domain::AtomicResultState,
}

impl OpenCodeRunner {
    pub fn new(config: IntegrationAgentConfig, snapshot: RunnerSnapshot) -> Result<Self> {
        verify_executable(&snapshot.executable)?;
        if snapshot.sandbox.implementation != "linux_userns_tmpfs_overlay_v1" {
            anyhow::bail!("unsupported sandbox implementation identity");
        }
        Ok(Self { config, snapshot })
    }

    pub fn verify_capability(&self, state_database: &Path) -> Result<()> {
        verify_executable(&self.snapshot.executable)?;
        verify_sandbox_helpers(&self.snapshot.sandbox)?;
        if !state_database.is_absolute() {
            anyhow::bail!("state database path must be absolute");
        }
        let output = Command::new(&self.snapshot.sandbox.unshare)
            .args([
                "--user",
                "--map-root-user",
                "--mount",
                "sh",
                "-c",
                "mount -t tmpfs -o size=1048576,nosuid,nodev tmpfs /mnt && umount /mnt",
            ])
            .output()
            .context("probe bounded tmpfs in an unprivileged mount namespace")?;
        if !output.status.success() {
            anyhow::bail!(
                "bounded tmpfs sandbox is unavailable: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let output = Command::new(&self.snapshot.sandbox.systemd_run)
            .args([
                "--user",
                "--scope",
                "--quiet",
                "-p",
                "MemoryMax=16777216",
                "-p",
                "TasksMax=4",
                "true",
            ])
            .output()
            .context("probe user-systemd resource scope")?;
        if !output.status.success() {
            anyhow::bail!(
                "user-systemd resource scopes are unavailable: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    pub fn run<P, S, W, A>(
        &self,
        retained_rift: &Path,
        input: &AgentInput,
        protected_paths: &[PathBuf],
        lifecycle: RunnerLifecycle<P, S, W, A>,
    ) -> Result<RunnerOutcome>
    where
        P: FnOnce(&str, &Path) -> Result<()>,
        S: FnOnce(u32, u64, i32, &str, &Path) -> Result<()>,
        W: FnOnce(&crate::control_domain::AtomicResultState) -> Result<()>,
        A: Fn() -> Result<bool>,
    {
        input.validate()?;
        verify_executable(&self.snapshot.executable)?;
        verify_sandbox_helpers(&self.snapshot.sandbox)?;
        let retained = retained_rift.canonicalize()?;
        let before_git = git_identity(&retained)?;

        let cycle_root = retained
            .parent()
            .context("retained Rift has no parent")?
            .join(format!(".iq-agent-sandbox-{}", input.identity.cycle_id));
        if cycle_root.exists() {
            anyhow::bail!(
                "agent sandbox path already exists: {}",
                cycle_root.display()
            );
        }
        fs::create_dir(&cycle_root)?;
        fs::set_permissions(&cycle_root, fs::Permissions::from_mode(0o700))?;
        fs::create_dir(cycle_root.join("tmpfs"))?;
        fs::create_dir(cycle_root.join("repo"))?;
        fs::create_dir(cycle_root.join("export"))?;
        for directory in ["tmpfs", "repo", "export"] {
            fs::set_permissions(
                cycle_root.join(directory),
                fs::Permissions::from_mode(0o700),
            )?;
        }
        let helper = cycle_root.join("sandbox-entry");
        write_helper(&helper)?;

        let protocol = protocol_directory(&retained, &input.identity.cycle_id)?;
        atomic_write_json(&protocol, "input.json", input)?;
        let unit_name = format!("iq-agent-{}", input.identity.cycle_id);
        (lifecycle.on_prepared)(&unit_name, &protocol)
            .context("persist runner launch authority")?;
        let prompt = "Read /iq-protocol/input.json. Integrate target and source behavior in /repo, stage the complete result, and atomically write protocol version 1 JSON to /iq-protocol/result.json. Do not commit, create refs, change Git config/remotes, or access providers. Return exactly resolved, guidance_required, or mechanical_failure.";

        let credential = std::env::var_os(&self.config.credential_env).with_context(|| {
            format!(
                "required model credential {} is unavailable",
                self.config.credential_env
            )
        })?;
        let log_path = cycle_root.join("runner.log");
        let log_out = bounded_log_file(&log_path)?;
        let log_err = log_out.try_clone()?;
        let export = cycle_root.join("export");
        let sandbox_args = sandbox_command(
            &self.config,
            &self.snapshot,
            &cycle_root,
            &retained,
            &input.identity.cycle_id,
            prompt,
            credential,
        )?;
        let mut command = Command::new(&self.snapshot.sandbox.systemd_run);
        command
            .process_group(0)
            .args([
                "--user",
                "--scope",
                "--quiet",
                "--collect",
                "--unit",
                &unit_name,
                "-p",
                &format!("MemoryMax={}", self.snapshot.bounds.memory_bytes),
                "-p",
                &format!("TasksMax={}", self.snapshot.bounds.max_processes),
                "-p",
                "CPUQuota=100%",
                "--",
            ])
            .args(sandbox_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log_out))
            .stderr(Stdio::from(log_err));
        let mut child = command
            .spawn()
            .context("launch OpenCode sandbox in user-systemd scope")?;
        let pid = child.id();
        let process_start_ticks = process_start_ticks(pid)?;
        let process_group_id = unsafe { libc::getpgid(pid as i32) };
        if process_group_id < 0 {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::last_os_error())
                .context("read runner process-group identity");
        }
        if let Err(error) = (lifecycle.on_started)(
            pid,
            process_start_ticks,
            process_group_id,
            &format!("linux-userns-overlay:{}", input.identity.cycle_id),
            &protocol,
        ) {
            let _ = terminate_child_group(&mut child, process_group_id);
            return Err(error).context("persist runner start authority");
        }
        let mut admission = child
            .stdin
            .take()
            .context("runner admission gate is absent")?;
        if let Err(error) = admission.write_all(b"run\n") {
            let _ = terminate_child_group(&mut child, process_group_id);
            return Err(error).context("release runner admission gate");
        }
        drop(admission);
        let protected_baseline = protected_paths
            .iter()
            .map(|path| protected_identity(path))
            .collect::<Result<Vec<_>>>()?;
        let started = Instant::now();
        let timeout = Duration::from_secs(self.snapshot.cycle_timeout_seconds);
        let mut authority_lost = false;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break Some(status);
            }
            if !(lifecycle.authority_active)()? {
                authority_lost = true;
                terminate_child_group(&mut child, process_group_id)?;
                break None;
            }
            let log_length = fs::symlink_metadata(&log_path)?.len();
            let result_length = fs::symlink_metadata(protocol.join("result.json"))
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            if log_length > self.snapshot.bounds.max_log_bytes
                || result_length > self.snapshot.bounds.max_result_bytes
            {
                terminate_child_group(&mut child, process_group_id)?;
                let log = read_prefix(&log_path, self.snapshot.bounds.max_log_bytes)?;
                return Ok(RunnerOutcome::InvalidResult {
                    log,
                    reason: "runner output exceeded a configured bound".into(),
                    export_directory: export,
                });
            }
            if started.elapsed() >= timeout {
                terminate_child_group(&mut child, process_group_id)?;
                break None;
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let log = read_bounded(&log_path, self.snapshot.bounds.max_log_bytes)?;
        if !(lifecycle.authority_active)()? {
            return Ok(RunnerOutcome::AuthorityLost { log });
        }
        verify_protected(&protected_baseline)?;
        if git_identity(&retained)? != before_git {
            anyhow::bail!("retained Rift changed while the sandbox ran");
        }
        let outcome = match status {
            None if authority_lost => RunnerOutcome::AuthorityLost { log },
            None => RunnerOutcome::TimedOut { log },
            Some(exit_status) => {
                let result = export.join("result.json");
                match fs::symlink_metadata(&result) {
                    Ok(_) => {
                        let parsed = (|| -> Result<(
                            Box<AgentResult>,
                            crate::control_domain::AtomicResultState,
                            PathBuf,
                        )> {
                            let bytes = read_complete_result(
                                &export,
                                "result.json",
                                self.snapshot.bounds.max_result_bytes,
                            )
                            .context("read bounded sandbox result for publication")?;
                            let result = Box::new(parse_result(&bytes, input)?);
                            verify_exported_git_identity(&export, &before_git)
                                .context("verify sandbox Git identity publication")?;
                            let prepared = prepare_publication(&protocol)?;
                            (lifecycle.on_writing)(prepared.state())
                                .context("persist result publication intent")?;
                            let state = publish_result_bundle(
                                &protocol,
                                prepared,
                                &export,
                                self.snapshot.bounds.max_result_bytes,
                                self.snapshot.bounds.writable_bytes,
                            )?;
                            let published = protocol.join("publication");
                            remove_sandbox_export(&export)?;
                            Ok((result, state, published))
                        })();
                        match parsed {
                            Ok((result, result_state, published)) => RunnerOutcome::Complete {
                                result,
                                result_state,
                                log,
                                exit_status,
                                export_directory: published,
                            },
                            Err(error) => RunnerOutcome::InvalidResult {
                                log,
                                reason: format!("{error:#}"),
                                export_directory: export,
                            },
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        RunnerOutcome::MissingResult { log, exit_status }
                    }
                    Err(error) => return Err(error).context("inspect sandbox result"),
                }
            }
        };
        Ok(outcome)
    }
}

pub fn systemd_unit_main_pid(systemctl: &Path, unit_name: &str) -> Result<Option<u32>> {
    verify_sandbox_helper(systemctl, "systemctl")?;
    crate::control_domain::require_exact_text(unit_name, "systemd unit name")?;
    let output = Command::new(systemctl)
        .args([
            "--user",
            "show",
            unit_name,
            "--property=LoadState",
            "--property=MainPID",
        ])
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "inspect prepared systemd unit failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let properties = String::from_utf8(output.stdout)?;
    let mut load_state = None;
    let mut pid = None;
    for line in properties.lines() {
        if let Some(value) = line.strip_prefix("LoadState=") {
            load_state = Some(value);
        } else if let Some(value) = line.strip_prefix("MainPID=") {
            pid = Some(value.parse::<u32>()?);
        }
    }
    if load_state == Some("not-found") {
        return Ok(None);
    }
    if load_state != Some("loaded") {
        anyhow::bail!("prepared systemd unit has an unexpected load state");
    }
    let pid = pid.context("prepared systemd unit has no MainPID property")?;
    Ok((pid != 0).then_some(pid))
}

pub fn stop_systemd_unit(systemctl: &Path, unit_name: &str) -> Result<()> {
    verify_sandbox_helper(systemctl, "systemctl")?;
    crate::control_domain::require_exact_text(unit_name, "systemd unit name")?;
    let output = Command::new(systemctl)
        .args(["--user", "stop", unit_name])
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "stop prepared systemd unit failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn terminate_child_group(child: &mut std::process::Child, process_group_id: i32) -> Result<()> {
    if process_group_id <= 0 {
        anyhow::bail!("runner process group identity is invalid");
    }
    if unsafe { libc::kill(-process_group_id, libc::SIGTERM) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error).context("terminate runner process group");
        }
    }
    for _ in 0..20 {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if unsafe { libc::kill(-process_group_id, libc::SIGKILL) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error).context("kill runner process group");
        }
    }
    let _ = child.wait();
    Ok(())
}

pub fn process_start_ticks(pid: u32) -> Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat
        .rfind(')')
        .context("process stat has no command terminator")?;
    stat[end + 2..]
        .split_whitespace()
        .nth(19)
        .context("process stat has no start time")?
        .parse()
        .context("parse process start ticks")
}

pub fn terminate_exact_process(pid: u32, start_ticks: u64, process_group_id: i32) -> Result<()> {
    match process_start_ticks(pid) {
        Ok(actual) if actual == start_ticks => {
            if process_group_id <= 0 {
                anyhow::bail!("persisted runner process-group identity is invalid");
            }
            let actual_group = unsafe { libc::getpgid(pid as i32) };
            if actual_group < 0 {
                return Err(std::io::Error::last_os_error())
                    .context("inspect persisted runner process-group identity");
            }
            if actual_group != process_group_id {
                anyhow::bail!("persisted runner process now belongs to a different process group");
            }
            if unsafe { libc::kill(-process_group_id, libc::SIGTERM) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error).context("terminate exact surviving runner process group");
                }
            }
            for _ in 0..20 {
                if process_start_ticks(pid).is_err() {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if unsafe { libc::kill(-process_group_id, libc::SIGKILL) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error).context("kill exact surviving runner process group");
                }
            }
            Ok(())
        }
        Ok(_) => Ok(()),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(())
        }
        Err(error) => Err(error).context("inspect persisted runner process identity"),
    }
}

pub fn restart_export_directory(retained_rift: &Path, cycle_id: &str) -> Result<PathBuf> {
    let retained = retained_rift.canonicalize()?;
    let parent = retained.parent().context("retained Rift has no parent")?;
    Ok(parent
        .join(format!(".iq-agent-sandbox-{cycle_id}"))
        .join("export"))
}

pub fn read_restart_result(
    retained_rift: &Path,
    cycle_id: &str,
    max_result_bytes: u64,
) -> Result<Option<RestartResult>> {
    let protocol = retained_rift.join(".iq-agent-protocol").join(cycle_id);
    let input_bytes = read_complete_result(&protocol, "input.json", max_result_bytes)?;
    let input: AgentInput =
        crate::control_domain::parse_strict_json(&input_bytes, "integration-agent restart input")?;
    input.validate()?;
    let input_sha256 = format!("{:x}", Sha256::digest(&input_bytes));
    match read_published_result(&protocol, max_result_bytes, 64 * 1024 * 1024)? {
        Some((bytes, export, state)) => {
            let result = Box::new(parse_result(&bytes, &input)?);
            Ok(Some(RestartResult {
                input,
                input_sha256,
                result,
                export_directory: export,
                result_state: state,
            }))
        }
        None => Ok(None),
    }
}

pub fn quarantine_restart_artifacts(retained_rift: &Path, cycle_id: &str) -> Result<()> {
    let export = restart_export_directory(retained_rift, cycle_id)?;
    if export.exists() {
        remove_sandbox_export(&export)?;
    }
    crate::agent_protocol::remove_protocol_cycle(retained_rift, cycle_id)?;
    Ok(())
}

pub fn complete_result_identity(path: &Path) -> Result<crate::control_domain::AtomicResultState> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.nlink() != 1 {
        anyhow::bail!("complete result has invalid file identity");
    }
    let bytes = read_complete_result(
        path.parent().context("complete result has no parent")?,
        path.file_name()
            .and_then(OsStr::to_str)
            .context("complete result name is not UTF-8")?,
        metadata.len(),
    )?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    Ok(crate::control_domain::AtomicResultState::Complete {
        device: metadata.dev(),
        inode: metadata.ino(),
        sha256: digest,
    })
}

pub fn import_staged_result(
    export_directory: &Path,
    retained_rift: &Path,
    expected_staged_digest: &str,
    changed_paths: &[crate::control_domain::EncodedPath],
) -> Result<()> {
    let staged_tree = read_complete_result(export_directory, "staged.tree", 129)?;
    let staged_tree = String::from_utf8(staged_tree)?.trim().to_string();
    let staged_digest = format!("{:x}", Sha256::digest(staged_tree.as_bytes()));
    if staged_digest != expected_staged_digest {
        anyhow::bail!("reported staged-tree digest differs from sandbox index");
    }
    let actual_paths = nul_paths(&read_complete_result(
        export_directory,
        "staged.paths",
        1024 * 1024,
    )?)?;
    let reported = changed_paths
        .iter()
        .map(|path| path.to_bytes())
        .collect::<Result<Vec<_>>>()?;
    if actual_paths != reported {
        anyhow::bail!("reported changed paths differ from sandbox staged paths");
    }
    if !nul_paths(&read_complete_result(
        export_directory,
        "unstaged.paths",
        1024 * 1024,
    )?)?
    .is_empty()
    {
        anyhow::bail!("sandbox result has unstaged worktree changes");
    }
    for path in &actual_paths {
        let path_os = OsStr::from_bytes(path);
        if crate::agent_protocol::is_protocol_path(path_os) || path.starts_with(b".git/") {
            anyhow::bail!("sandbox result changes a forbidden path");
        }
    }
    let patch = read_complete_result(export_directory, "staged.patch", 64 * 1024 * 1024)?;
    run_git(retained_rift, ["reset", "--hard", "HEAD"])?;
    run_git(
        retained_rift,
        ["clean", "-ffd", "-e", ".iq-agent-protocol/"],
    )?;
    let mut child = Command::new("git")
        .args(["apply", "--index", "--binary", "--whitespace=nowarn", "--"])
        .current_dir(retained_rift)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .context("Git patch import stdin is absent")?
        .write_all(&patch)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        anyhow::bail!(
            "verified staged patch import failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    if staged_tree_digest(retained_rift)? != expected_staged_digest {
        anyhow::bail!("imported retained-Rift staged tree differs from verified sandbox result");
    }
    Ok(())
}

pub fn remove_sandbox_export(export_directory: &Path) -> Result<()> {
    let sandbox_root = export_directory
        .parent()
        .context("sandbox export has no parent")?;
    if export_directory.file_name() != Some(OsStr::new("export"))
        || !sandbox_root
            .file_name()
            .is_some_and(|name| name.as_bytes().starts_with(b".iq-agent-sandbox-"))
    {
        anyhow::bail!("sandbox export path has an unexpected identity");
    }
    let metadata = fs::symlink_metadata(sandbox_root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("sandbox export root is not a regular directory");
    }
    fs::remove_dir_all(sandbox_root)?;
    Ok(())
}

pub fn staged_tree_digest(repository: &Path) -> Result<String> {
    let output = git_output(repository, ["write-tree"])?;
    let tree = output.trim();
    if tree.is_empty() {
        anyhow::bail!("Git did not return a staged tree identity");
    }
    Ok(format!("{:x}", Sha256::digest(tree.as_bytes())))
}

fn sandbox_command(
    config: &IntegrationAgentConfig,
    snapshot: &RunnerSnapshot,
    root: &Path,
    retained: &Path,
    cycle_id: &str,
    prompt: &str,
    credential: OsString,
) -> Result<Vec<OsString>> {
    if snapshot.credential_env != config.credential_env {
        anyhow::bail!("credential source identity changed after effort snapshot");
    }
    let mut command = vec![
        snapshot.sandbox.unshare.as_os_str().to_os_string(),
        OsString::from("--user"),
        OsString::from("--map-root-user"),
        OsString::from("--mount"),
        OsString::from("--pid"),
        OsString::from("--fork"),
        OsString::from("--mount-proc"),
        root.join("sandbox-entry").into_os_string(),
        root.as_os_str().to_os_string(),
        retained.as_os_str().to_os_string(),
        snapshot.bounds.writable_bytes.to_string().into(),
        cycle_id.into(),
        snapshot.bounds.cpu_seconds.to_string().into(),
        snapshot.bounds.open_files.to_string().into(),
        retained
            .join(".iq-agent-protocol")
            .join(cycle_id)
            .into_os_string(),
        snapshot
            .bounds
            .max_log_bytes
            .div_ceil(512)
            .to_string()
            .into(),
        snapshot.sandbox.bubblewrap.as_os_str().to_os_string(),
        OsString::from("--die-with-parent"),
        OsString::from("--new-session"),
        OsString::from("--clearenv"),
        OsString::from("--unshare-user"),
        OsString::from("--unshare-pid"),
        OsString::from("--unshare-ipc"),
        OsString::from("--unshare-uts"),
        OsString::from("--proc"),
        OsString::from("/proc"),
        OsString::from("--dev"),
        OsString::from("/dev"),
        OsString::from("--bind"),
        root.join("repo").into_os_string(),
        OsString::from("/repo"),
        OsString::from("--bind"),
        root.join("tmpfs/home").into_os_string(),
        OsString::from("/home/iq"),
        OsString::from("--bind"),
        root.join("tmpfs/tmp").into_os_string(),
        OsString::from("/tmp"),
        OsString::from("--bind"),
        root.join("tmpfs/protocol").into_os_string(),
        OsString::from("/iq-protocol"),
        OsString::from("--dir"),
        OsString::from("/etc"),
    ];
    for path in ["/usr", "/bin", "/lib", "/lib64"] {
        if Path::new(path).exists() {
            command.extend([
                OsString::from("--ro-bind"),
                OsString::from(path),
                OsString::from(path),
            ]);
        }
    }
    for path in [
        "/etc/hosts",
        "/etc/nsswitch.conf",
        "/etc/resolv.conf",
        "/etc/ssl",
    ] {
        if Path::new(path).exists() {
            command.extend([
                OsString::from("--ro-bind"),
                OsString::from(path),
                OsString::from(path),
            ]);
        }
    }
    if let Some(config_directory) = opencode_config_directory()? {
        command.extend([
            OsString::from("--dir"),
            OsString::from("/home/iq/.config"),
            OsString::from("--ro-bind"),
            config_directory.into_os_string(),
            OsString::from("/home/iq/.config/opencode"),
        ]);
    }
    command.extend([
        OsString::from("--ro-bind"),
        snapshot.executable.path.as_os_str().to_os_string(),
        OsString::from("/iq-runner"),
        OsString::from("--setenv"),
        OsString::from("HOME"),
        OsString::from("/home/iq"),
        OsString::from("--setenv"),
        OsString::from("PATH"),
        OsString::from("/usr/local/bin:/usr/bin:/bin"),
        OsString::from("--setenv"),
        OsString::from("GIT_CONFIG_NOSYSTEM"),
        OsString::from("1"),
        OsString::from("--setenv"),
        OsString::from("GIT_CONFIG_GLOBAL"),
        OsString::from("/dev/null"),
        OsString::from("--setenv"),
        OsString::from("GIT_TERMINAL_PROMPT"),
        OsString::from("0"),
        OsString::from("--setenv"),
        OsString::from("SSH_AUTH_SOCK"),
        OsString::from(""),
        OsString::from("--setenv"),
        config.credential_env.clone().into(),
        credential,
        OsString::from("--chdir"),
        OsString::from("/repo"),
        OsString::from("--"),
        OsString::from("/iq-runner"),
        OsString::from("run"),
        OsString::from("--pure"),
        OsString::from("--auto"),
        OsString::from("--agent"),
        snapshot.agent.clone().into(),
        OsString::from("--model"),
        snapshot.model.clone().into(),
        OsString::from("--dir"),
        OsString::from("/repo"),
        prompt.into(),
    ]);
    Ok(command)
}

fn write_helper(path: &Path) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o700)
        .open(path)?;
    file.write_all(SANDBOX_HELPER.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn bounded_log_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .context("create runner log")
}

fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > max {
        anyhow::bail!("runner output exceeds configured log bound");
    }
    Ok(fs::read(path)?)
}

fn read_prefix(path: &Path, max: u64) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(max)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn protected_identity(path: &Path) -> Result<ProtectedIdentity> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect protected path {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("protected path must not be a symlink");
    }
    let digest = if metadata.is_file() && metadata.len() <= 16 * 1024 * 1024 {
        Some(format!("{:x}", Sha256::digest(fs::read(path)?)))
    } else {
        None
    };
    Ok(ProtectedIdentity {
        path: path.to_path_buf(),
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        digest,
    })
}

fn verify_protected(before: &[ProtectedIdentity]) -> Result<()> {
    for expected in before {
        if &protected_identity(&expected.path)? != expected {
            anyhow::bail!(
                "protected path changed during sandbox execution: {}",
                expected.path.display()
            );
        }
    }
    Ok(())
}

fn git_identity(repository: &Path) -> Result<BTreeMap<String, String>> {
    let mut identity = BTreeMap::new();
    for key in ["HEAD", "packed-refs"] {
        let path = repository.join(".git").join(key);
        if let Ok(bytes) = fs::read(&path) {
            identity.insert(key.to_string(), format!("{:x}", Sha256::digest(bytes)));
        }
    }
    let refs = git_output(
        repository,
        ["for-each-ref", "--format=%(refname) %(objectname)"],
    )?;
    identity.insert("refs".to_string(), refs);
    identity.insert(
        "head".to_string(),
        git_output(repository, ["rev-parse", "HEAD"])?,
    );
    identity.insert(
        "config-list".to_string(),
        git_output(repository, ["config", "--local", "--list", "--show-origin"])?,
    );
    identity.insert(
        "remotes".to_string(),
        git_output(repository, ["remote", "-v"])?,
    );
    Ok(identity)
}

fn verify_exported_git_identity(export: &Path, before: &BTreeMap<String, String>) -> Result<()> {
    let head = String::from_utf8(read_bounded(&export.join("head"), 129)?)?;
    if head.trim()
        != before
            .get("head")
            .context("protected Git identity has no HEAD")?
            .trim()
    {
        anyhow::bail!("agent changed Git HEAD");
    }
    for (file, key) in [
        ("refs", "refs"),
        ("config", "config-list"),
        ("remotes", "remotes"),
    ] {
        let actual = String::from_utf8(read_bounded(&export.join(file), 1024 * 1024)?)?;
        if before.get(key).is_some_and(|expected| expected != &actual) {
            anyhow::bail!("agent changed Git {key}");
        }
    }
    Ok(())
}

fn nul_paths(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut paths = bytes
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| path.to_vec())
        .collect::<Vec<_>>();
    paths.sort();
    if paths.windows(2).any(|pair| pair[0] == pair[1]) {
        anyhow::bail!("Git path list contains a duplicate");
    }
    Ok(paths)
}

fn run_git<I, S>(repository: &Path, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "Git command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn git_output<I, S>(repository: &Path, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Ok(String::from_utf8(git_output_bytes(repository, args)?)?)
}

fn git_output_bytes<I, S>(repository: &Path, args: I) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "Git command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

pub fn sandbox_blocker(error: &anyhow::Error) -> InfrastructureCause {
    InfrastructureCause::Unavailable {
        detail: format!("{error:#}"),
    }
}

fn verify_sandbox_helpers(sandbox: &crate::control_domain::SandboxIdentity) -> Result<()> {
    for (path, name) in [
        (&sandbox.bubblewrap, "bwrap"),
        (&sandbox.unshare, "unshare"),
        (&sandbox.systemd_run, "systemd-run"),
        (&sandbox.systemctl, "systemctl"),
    ] {
        verify_sandbox_helper(path, name)?;
    }
    Ok(())
}

fn verify_sandbox_helper(path: &Path, name: &str) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| {
        format!(
            "inspect required sandbox helper {name} at {}",
            path.display()
        )
    })?;
    if !path.is_absolute() || !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        anyhow::bail!(
            "required sandbox helper is not executable: {}",
            path.display()
        );
    }
    Ok(())
}

fn opencode_config_directory() -> Result<Option<PathBuf>> {
    let root = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(path) => PathBuf::from(path),
        None => match std::env::var_os("HOME") {
            Some(home) => PathBuf::from(home).join(".config"),
            None => return Ok(None),
        },
    };
    let path = root.join("opencode");
    match fs::metadata(&path) {
        Ok(metadata) if metadata.is_dir() => Ok(Some(path.canonicalize()?)),
        Ok(_) => anyhow::bail!(
            "OpenCode configuration path is not a directory: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| {
            format!(
                "inspect OpenCode configuration directory {}",
                path.display()
            )
        }),
    }
}
