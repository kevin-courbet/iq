use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::agent_config::IntegrationAgentConfig;
use crate::agent_protocol::{
    atomic_write_json, parse_result, prepare_publication, protocol_directory,
    publish_result_bundle, read_complete_result, read_published_result, AgentInput, AgentResult,
};
use crate::control_domain::{InfrastructureCause, RunnerSnapshot};

const SANDBOX_OWNERSHIP_FILE: &str = ".iq-sandbox-owner.json";
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SandboxOwnership {
    version: u32,
    cycle_id: String,
    parent_device: u64,
    parent_inode: u64,
    root_device: u64,
    root_inode: u64,
}

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

pub struct RunnerLifecycle<P, C, R, F, S, W, A> {
    pub on_prepared: P,
    pub on_spawn_surrender: C,
    pub recheck_spawn_authority: R,
    pub on_spawn_failed: F,
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
        crate::agent_config::open_executable_authority(&snapshot.executable)?;
        if snapshot.sandbox.implementation != "linux_userns_tmpfs_overlay_v1" {
            anyhow::bail!("unsupported sandbox implementation identity");
        }
        Ok(Self { config, snapshot })
    }

    pub fn verify_capability(&self, state_database: &Path) -> Result<()> {
        let systemd_run =
            crate::agent_config::open_executable_authority(&self.snapshot.sandbox.systemd_run)?;
        let unshare =
            crate::agent_config::open_executable_authority(&self.snapshot.sandbox.unshare)?;
        crate::agent_config::open_executable_authority(&self.snapshot.executable)?;
        verify_sandbox_helpers(&self.snapshot.sandbox)?;
        if !state_database.is_absolute() {
            anyhow::bail!("state database path must be absolute");
        }
        let output = unshare
            .command()
            .args([
                "--user",
                "--map-root-user",
                "--mount",
                "/bin/sh",
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
        let mut systemd_command = systemd_run.command();
        crate::agent_config::harden_user_systemd_environment(&mut systemd_command)?;
        let output = systemd_command
            .args([
                "--user",
                "--quiet",
                "--collect",
                "--wait",
                "--pipe",
                "--property=Type=exec",
                "--property",
                "MemoryMax=16777216",
                "--property",
                "TasksMax=4",
                "--",
                "/bin/true",
            ])
            .output()
            .context("probe user-systemd transient service")?;
        if !output.status.success() {
            anyhow::bail!(
                "user-systemd transient services are unavailable: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    pub fn run<P, C, R, F, S, W, A>(
        &self,
        retained_rift: &Path,
        input: &AgentInput,
        protected_paths: &[PathBuf],
        lifecycle: RunnerLifecycle<P, C, R, F, S, W, A>,
    ) -> Result<RunnerOutcome>
    where
        P: FnOnce(&str, &Path) -> Result<()>,
        C: FnOnce() -> Result<bool>,
        R: FnOnce() -> Result<bool>,
        F: FnOnce() -> Result<()>,
        S: FnOnce(u32, u64, &str, &str, &Path) -> Result<bool>,
        W: FnOnce(&crate::control_domain::AtomicResultState) -> Result<()>,
        A: Fn() -> Result<bool>,
    {
        input.validate()?;
        let systemd_run =
            crate::agent_config::open_executable_authority(&self.snapshot.sandbox.systemd_run)?;
        crate::agent_config::open_executable_authority(&self.snapshot.executable)?;
        verify_sandbox_helpers(&self.snapshot.sandbox)?;
        let retained = retained_rift.canonicalize()?;
        let before_git = git_identity(&retained)?;
        let credential = std::env::var_os(&self.config.credential_env).with_context(|| {
            format!(
                "required model credential {} is unavailable",
                self.config.credential_env
            )
        })?;

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
        write_sandbox_ownership(&cycle_root, &input.identity.cycle_id)?;
        write_helper(&cycle_root.join("sandbox-entry"))?;
        let protocol = protocol_directory(&retained, &input.identity.cycle_id)?;
        atomic_write_json(&protocol, "input.json", input)?;
        let unit_name = crate::control_domain::systemd_unit_name(&input.identity.cycle_id)?;
        (lifecycle.on_prepared)(&unit_name, &protocol)
            .context("persist runner launch authority")?;
        let prompt = format!(
            "Read /iq-protocol/input.json. Integrate target and source behavior in /repo, use only the pinned Git executable /iq-git, stage the complete result, and atomically write protocol version {} JSON to /iq-protocol/result.json. Do not commit, create refs, change Git config/remotes, or access providers. Return exactly resolved, guidance_required, or mechanical_failure.",
            crate::control_domain::PROTOCOL_VERSION
        );

        let log_path = cycle_root.join("runner.log");
        let log_out = bounded_log_file(&log_path)?;
        let log_err = log_out.try_clone()?;
        let export = cycle_root.join("export");
        let args = sandbox_command(
            &self.config,
            &self.snapshot,
            &cycle_root,
            &retained,
            &input.identity.cycle_id,
            &prompt,
            credential,
        )?;
        let mut command = systemd_run.command();
        crate::agent_config::harden_user_systemd_environment(&mut command)?;
        command
            .env("PATH", "/usr/bin:/bin")
            .args([
                "--user",
                "--quiet",
                "--collect",
                "--wait",
                "--pipe",
                "--property=Type=exec",
                &format!("--unit={unit_name}"),
                "--property",
                &format!("MemoryMax={}", self.snapshot.bounds.memory_bytes),
                "--property",
                &format!("TasksMax={}", self.snapshot.bounds.max_processes),
                "--property",
                "CPUQuota=100%",
                "--",
            ])
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log_out))
            .stderr(Stdio::from(log_err));
        if !(lifecycle.on_spawn_surrender)()? {
            return Ok(RunnerOutcome::AuthorityLost { log: Vec::new() });
        }
        if !(lifecycle.recheck_spawn_authority)()? {
            (lifecycle.on_spawn_failed)()?;
            return Ok(RunnerOutcome::AuthorityLost { log: Vec::new() });
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                (lifecycle.on_spawn_failed)()?;
                return Err(error).context("launch OpenCode sandbox in user-systemd service");
            }
        };
        let service = match wait_for_service(&self.snapshot.sandbox.systemctl, &unit_name) {
            Ok(service) => service,
            Err(error) => {
                return Err(abort_unrecorded_spawn(
                    &mut child,
                    &self.snapshot.sandbox.systemctl,
                    &unit_name,
                    lifecycle.on_spawn_failed,
                    error.context("verify runner service identity"),
                ));
            }
        };
        let pid = service.main_pid;
        let process_start_ticks = match process_start_ticks(pid) {
            Ok(value) => value,
            Err(error) => {
                return Err(abort_unrecorded_spawn(
                    &mut child,
                    &self.snapshot.sandbox.systemctl,
                    &unit_name,
                    lifecycle.on_spawn_failed,
                    error.context("read runner process start identity"),
                ));
            }
        };
        let started_authority = (lifecycle.on_started)(
            pid,
            process_start_ticks,
            &service.control_group,
            &format!("linux-userns-overlay:{}", input.identity.cycle_id),
            &protocol,
        );
        let started_authority = match started_authority {
            Ok(active) => active,
            Err(error) => {
                let diagnostic =
                    read_prefix(&log_path, self.snapshot.bounds.max_log_bytes).unwrap_or_default();
                return Err(abort_unrecorded_spawn(
                    &mut child,
                    &self.snapshot.sandbox.systemctl,
                    &unit_name,
                    lifecycle.on_spawn_failed,
                    error.context(format!(
                        "persist runner start authority: {}",
                        String::from_utf8_lossy(&diagnostic)
                    )),
                ));
            }
        };
        if !started_authority {
            terminate_runner_service(
                &mut child,
                &self.snapshot.sandbox.systemctl,
                &unit_name,
                &service.control_group,
                pid,
                process_start_ticks,
            )?;
            (lifecycle.on_spawn_failed)()?;
            return Ok(RunnerOutcome::AuthorityLost { log: Vec::new() });
        }
        let mut admission = child
            .stdin
            .take()
            .context("runner admission gate is absent")?;
        if let Err(error) = admission.write_all(b"run\n") {
            let diagnostic =
                read_prefix(&log_path, self.snapshot.bounds.max_log_bytes).unwrap_or_default();
            return Err(abort_unrecorded_spawn(
                &mut child,
                &self.snapshot.sandbox.systemctl,
                &unit_name,
                lifecycle.on_spawn_failed,
                anyhow::Error::new(error).context(format!(
                    "release runner admission gate: {}",
                    String::from_utf8_lossy(&diagnostic)
                )),
            ));
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
                terminate_runner_service(
                    &mut child,
                    &self.snapshot.sandbox.systemctl,
                    &unit_name,
                    &service.control_group,
                    pid,
                    process_start_ticks,
                )?;
                break None;
            }
            let log_length = fs::symlink_metadata(&log_path)?.len();
            let result_length = fs::symlink_metadata(protocol.join("result.json"))
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            if log_length > self.snapshot.bounds.max_log_bytes
                || result_length > self.snapshot.bounds.max_result_bytes
            {
                terminate_runner_service(
                    &mut child,
                    &self.snapshot.sandbox.systemctl,
                    &unit_name,
                    &service.control_group,
                    pid,
                    process_start_ticks,
                )?;
                let log = read_prefix(&log_path, self.snapshot.bounds.max_log_bytes)?;
                return Ok(RunnerOutcome::InvalidResult {
                    log,
                    reason: "runner output exceeded a configured bound".into(),
                    export_directory: export,
                });
            }
            if started.elapsed() >= timeout {
                terminate_runner_service(
                    &mut child,
                    &self.snapshot.sandbox.systemctl,
                    &unit_name,
                    &service.control_group,
                    pid,
                    process_start_ticks,
                )?;
                break None;
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        stop_exact_runner_service(
            &self.snapshot.sandbox.systemctl,
            &unit_name,
            &service.control_group,
            pid,
            process_start_ticks,
        )?;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SystemdUnitState {
    Missing,
    Inactive { main_pid: Option<u32> },
    Active { main_pid: Option<u32> },
}

pub fn systemd_unit_state(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
) -> Result<SystemdUnitState> {
    Ok(inspect_systemd_unit(systemctl, unit_name)?.state)
}

#[derive(Debug)]
struct SystemdUnitInspection {
    state: SystemdUnitState,
    control_group: Option<String>,
}

struct ServiceBrokerAuthority {
    main_pid: u32,
    control_group: String,
}

fn wait_for_service(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
) -> Result<ServiceBrokerAuthority> {
    let mut last = None;
    for _ in 0..100 {
        let inspection = inspect_systemd_unit(systemctl, unit_name)?;
        if let SystemdUnitState::Active { main_pid } = inspection.state {
            let main_pid = main_pid.context("runner service has no MainPID")?;
            let control_group = inspection
                .control_group
                .filter(|value| !value.is_empty())
                .context("runner service has no control group")?;
            return Ok(ServiceBrokerAuthority {
                main_pid,
                control_group,
            });
        }
        last = Some(format!("{inspection:?}"));
        std::thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!(
        "runner systemd service did not become active: {}",
        last.as_deref().unwrap_or("no inspection")
    )
}

fn inspect_systemd_unit(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
) -> Result<SystemdUnitInspection> {
    let systemctl = crate::agent_config::open_executable_authority(systemctl)?;
    validate_iq_systemd_unit(unit_name)?;
    let mut command = systemctl.command();
    crate::agent_config::harden_user_systemd_environment(&mut command)?;
    let output = command
        .args([
            "--user",
            "show",
            "--property=LoadState",
            "--property=ActiveState",
            "--property=MainPID",
            "--property=ControlGroup",
            "--",
            unit_name,
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
    let mut active_state = None;
    let mut pid = None;
    let mut control_group = None;
    for line in properties.lines() {
        if let Some(value) = line.strip_prefix("LoadState=") {
            load_state = Some(value);
        } else if let Some(value) = line.strip_prefix("ActiveState=") {
            active_state = Some(value);
        } else if let Some(value) = line.strip_prefix("MainPID=") {
            pid = Some(value.parse::<u32>()?);
        } else if let Some(value) = line.strip_prefix("ControlGroup=") {
            control_group = Some(value.to_string());
        }
    }
    if load_state == Some("not-found") {
        return Ok(SystemdUnitInspection {
            state: SystemdUnitState::Missing,
            control_group,
        });
    }
    if load_state != Some("loaded") {
        anyhow::bail!("prepared systemd unit has an unexpected load state");
    }
    let main_pid = pid.filter(|pid| *pid != 0);
    let state = match active_state {
        Some("inactive" | "failed") => SystemdUnitState::Inactive { main_pid },
        Some("active" | "activating" | "deactivating" | "reloading") => {
            SystemdUnitState::Active { main_pid }
        }
        _ => anyhow::bail!("prepared systemd unit has an unexpected active state"),
    };
    Ok(SystemdUnitInspection {
        state,
        control_group,
    })
}

pub fn stop_systemd_unit(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
) -> Result<()> {
    let systemctl = crate::agent_config::open_executable_authority(systemctl)?;
    validate_iq_systemd_unit(unit_name)?;
    let mut command = systemctl.command();
    crate::agent_config::harden_user_systemd_environment(&mut command)?;
    let output = command
        .args(["--user", "stop", "--no-block", "--", unit_name])
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "stop prepared systemd unit failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn validate_iq_systemd_unit(unit_name: &str) -> Result<()> {
    if let Some(cycle_id) = unit_name
        .strip_prefix("iq-agent-")
        .and_then(|name| name.strip_suffix(".service"))
    {
        return crate::control_domain::validate_systemd_unit_name(cycle_id, unit_name);
    }
    if let Some(cycle_id) = unit_name
        .strip_prefix("iq-agent-")
        .and_then(|name| name.strip_suffix(".scope"))
    {
        return crate::control_domain::validate_legacy_systemd_scope_name(cycle_id, unit_name);
    }
    anyhow::bail!("systemd unit name has no canonical IQ runner boundary")
}

pub fn stop_and_verify_systemd_unit(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
) -> Result<()> {
    let inspection = inspect_systemd_unit(systemctl, unit_name)?;
    if let Some(control_group) = inspection
        .control_group
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        kill_control_group(control_group)?;
    }
    stop_systemd_unit_if_active(systemctl, unit_name)?;
    verify_systemd_unit_stopped(systemctl, unit_name)
}

pub fn stop_exact_runner_service(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
    control_group: &str,
    pid: u32,
    process_start_ticks: u64,
) -> Result<()> {
    stop_exact_systemd_process(
        systemctl,
        unit_name,
        control_group,
        pid,
        process_start_ticks,
        true,
    )
}

pub fn stop_exact_legacy_runner_scope(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
    control_group: &str,
    pid: u32,
    process_start_ticks: u64,
) -> Result<()> {
    crate::control_domain::validate_legacy_systemd_scope_name(
        unit_name
            .strip_prefix("iq-agent-")
            .and_then(|name| name.strip_suffix(".scope"))
            .context("legacy runner scope has no cycle identity")?,
        unit_name,
    )?;
    stop_exact_systemd_process(
        systemctl,
        unit_name,
        control_group,
        pid,
        process_start_ticks,
        false,
    )
}

fn stop_exact_systemd_process(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
    control_group: &str,
    pid: u32,
    process_start_ticks: u64,
    require_main_pid: bool,
) -> Result<()> {
    let inspection = inspect_systemd_unit(systemctl, unit_name)?;
    match inspection.state {
        SystemdUnitState::Active { .. }
            if inspection.control_group.as_deref() != Some(control_group) =>
        {
            anyhow::bail!("durable runner cgroup differs from its active systemd unit");
        }
        SystemdUnitState::Inactive { .. }
            if inspection
                .control_group
                .as_deref()
                .is_some_and(|observed| !observed.is_empty() && observed != control_group) =>
        {
            anyhow::bail!("durable runner cgroup differs from its inactive systemd unit");
        }
        _ => {}
    }
    let members = systemd_control_group_members(control_group)?;
    let process_alive = exact_process_is_alive(pid, process_start_ticks)?;
    if process_alive && !members.contains(&pid) {
        anyhow::bail!("exact runner process is outside its durable systemd cgroup");
    }
    if require_main_pid && process_alive {
        match inspection.state {
            SystemdUnitState::Active {
                main_pid: Some(main_pid),
            } if main_pid == pid => {}
            _ => anyhow::bail!("exact runner PID differs from systemd MainPID authority"),
        }
    }
    kill_control_group(control_group)?;
    stop_systemd_unit_if_active(systemctl, unit_name)?;
    verify_systemd_unit_stopped(systemctl, unit_name)?;
    for _ in 0..100 {
        if systemd_control_group_members(control_group)?.is_empty() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!("durable runner cgroup retains live members after exact stop")
}

pub fn verify_live_runner_service_authority(
    systemctl: &crate::control_domain::ExecutableIdentity,
    authority: &crate::control_domain::RunnerServiceAuthority,
) -> Result<()> {
    authority.validate()?;
    verify_live_systemd_process(systemctl, authority, true)
}

pub fn verify_live_legacy_runner_scope_authority(
    systemctl: &crate::control_domain::ExecutableIdentity,
    authority: &crate::control_domain::LegacyRunnerScopeAuthority,
) -> Result<()> {
    authority.validate()?;
    verify_live_systemd_process(systemctl, authority, false)
}

trait SystemdProcessAuthority {
    fn unit_name(&self) -> &str;
    fn control_group(&self) -> &str;
    fn pid(&self) -> u32;
    fn process_start_ticks(&self) -> u64;
}

impl SystemdProcessAuthority for crate::control_domain::RunnerServiceAuthority {
    fn unit_name(&self) -> &str {
        &self.unit_name
    }
    fn control_group(&self) -> &str {
        &self.control_group
    }
    fn pid(&self) -> u32 {
        self.pid
    }
    fn process_start_ticks(&self) -> u64 {
        self.process_start_ticks
    }
}

impl SystemdProcessAuthority for crate::control_domain::LegacyRunnerScopeAuthority {
    fn unit_name(&self) -> &str {
        &self.unit_name
    }
    fn control_group(&self) -> &str {
        &self.control_group
    }
    fn pid(&self) -> u32 {
        self.pid
    }
    fn process_start_ticks(&self) -> u64 {
        self.process_start_ticks
    }
}

fn verify_live_systemd_process(
    systemctl: &crate::control_domain::ExecutableIdentity,
    authority: &impl SystemdProcessAuthority,
    require_main_pid: bool,
) -> Result<()> {
    let inspection = inspect_systemd_unit(systemctl, authority.unit_name())?;
    if !matches!(inspection.state, SystemdUnitState::Active { .. })
        || inspection.control_group.as_deref() != Some(authority.control_group())
    {
        anyhow::bail!("migration runner authority differs from the active systemd unit");
    }
    if require_main_pid
        && !matches!(inspection.state, SystemdUnitState::Active { main_pid: Some(pid) } if pid == authority.pid())
    {
        anyhow::bail!("migration runner PID differs from systemd MainPID authority");
    }
    if !exact_process_is_alive(authority.pid(), authority.process_start_ticks())? {
        anyhow::bail!("migration runner authority process is not alive");
    }
    let process_cgroups = fs::read_to_string(format!("/proc/{}/cgroup", authority.pid()))?;
    if !process_cgroups.lines().any(|line| {
        line.strip_prefix("0::")
            .is_some_and(|path| path == authority.control_group())
    }) || !systemd_control_group_members(authority.control_group())?.contains(&authority.pid())
    {
        anyhow::bail!("migration runner process is outside its exact systemd cgroup");
    }
    Ok(())
}

pub fn stop_systemd_unit_if_active(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
) -> Result<()> {
    if matches!(
        systemd_unit_state(systemctl, unit_name)?,
        SystemdUnitState::Active { .. }
    ) {
        if let Err(error) = stop_systemd_unit(systemctl, unit_name) {
            if matches!(
                systemd_unit_state(systemctl, unit_name)?,
                SystemdUnitState::Active { .. }
            ) {
                return Err(error);
            }
        }
    }
    Ok(())
}

pub fn verify_systemd_unit_stopped(
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
) -> Result<()> {
    for _ in 0..100 {
        let inspection = inspect_systemd_unit(systemctl, unit_name)?;
        let stopped = matches!(
            inspection.state,
            SystemdUnitState::Missing | SystemdUnitState::Inactive { main_pid: None }
        );
        if stopped && !systemd_control_group_has_live_members(inspection.control_group.as_deref())?
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!("runner systemd unit remains active after exact stop")
}

fn systemd_control_group_has_live_members(control_group: Option<&str>) -> Result<bool> {
    let Some(control_group) = control_group.filter(|path| !path.is_empty()) else {
        return Ok(false);
    };
    let relative = control_group
        .strip_prefix('/')
        .context("systemd control group is not absolute")?;
    let relative = Path::new(relative);
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        anyhow::bail!("systemd control group escapes the cgroup root");
    }
    Ok(!systemd_control_group_members(control_group)?.is_empty())
}

fn systemd_control_group_members(control_group: &str) -> Result<Vec<u32>> {
    let relative = control_group
        .strip_prefix('/')
        .context("systemd control group is not absolute")?;
    let relative = Path::new(relative);
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        anyhow::bail!("systemd control group escapes the cgroup root");
    }
    let members = match fs::read_to_string(
        Path::new("/sys/fs/cgroup")
            .join(relative)
            .join("cgroup.procs"),
    ) {
        Ok(members) => members,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).context("inspect systemd control-group members"),
    };
    members
        .lines()
        .filter(|member| !member.trim().is_empty())
        .map(|member| {
            member
                .parse::<u32>()
                .context("parse systemd control-group member")
        })
        .collect()
}

fn abort_unrecorded_spawn<F>(
    child: &mut std::process::Child,
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
    on_spawn_failed: F,
    cause: anyhow::Error,
) -> anyhow::Error
where
    F: FnOnce() -> Result<()>,
{
    let cleanup = (|| {
        stop_and_verify_systemd_unit(systemctl, unit_name)?;
        let _ = child.wait();
        on_spawn_failed().context("close failed runner spawn authority")
    })();
    match cleanup {
        Ok(()) => cause,
        Err(error) => error.context(format!(
            "runner launch also failed before cleanup: {cause:#}"
        )),
    }
}

fn terminate_runner_service(
    child: &mut std::process::Child,
    systemctl: &crate::control_domain::ExecutableIdentity,
    unit_name: &str,
    control_group: &str,
    pid: u32,
    process_start_ticks: u64,
) -> Result<()> {
    stop_exact_runner_service(
        systemctl,
        unit_name,
        control_group,
        pid,
        process_start_ticks,
    )?;
    let _ = child.wait();
    Ok(())
}

fn process_stat(pid: u32) -> Result<(u8, u64)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat
        .rfind(')')
        .context("process stat has no command terminator")?;
    let fields = stat[end + 2..].split_whitespace().collect::<Vec<_>>();
    let state = fields
        .first()
        .and_then(|value| value.as_bytes().first())
        .copied()
        .context("process stat has no state")?;
    let start_ticks = fields
        .get(19)
        .context("process stat has no start time")?
        .parse()
        .context("parse process start ticks")?;
    Ok((state, start_ticks))
}

pub fn process_start_ticks(pid: u32) -> Result<u64> {
    process_stat(pid).map(|(_, start_ticks)| start_ticks)
}

pub fn exact_process_is_alive(pid: u32, start_ticks: u64) -> Result<bool> {
    match process_stat(pid) {
        Ok((state, actual)) => Ok(actual == start_ticks && !matches!(state, b'Z' | b'X')),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(false)
        }
        Err(error) => Err(error).context("inspect exact process identity"),
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
    let sandbox_root = export
        .parent()
        .context("restart sandbox export has no parent")?;
    match fs::symlink_metadata(sandbox_root) {
        Ok(_) => remove_sandbox_export(&export)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect restart sandbox root"),
    }
    crate::agent_protocol::remove_protocol_cycle(retained_rift, cycle_id)?;
    Ok(())
}

pub fn cleanup_terminal_cycle_artifacts(
    workspace_root: &crate::sqlite::WorkspaceRootIdentity,
    artifacts: &crate::control_store::TerminalCycleArtifacts,
) -> Result<()> {
    let workspace = Path::new(&artifacts.workspace.path);
    if !workspace_root.path.is_absolute()
        || !workspace.is_absolute()
        || workspace.parent() != Some(workspace_root.path.as_path())
        || workspace.file_name() != Some(OsStr::new(&artifacts.item_id))
        || artifacts.workspace.source_rift_id != workspace_root.source_rift_id
    {
        anyhow::bail!("terminal cycle artifacts differ from the owned workspace root");
    }
    crate::control_domain::validate_cycle_id(&artifacts.cycle_id)?;
    let workspace_exists = match fs::symlink_metadata(workspace) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            anyhow::bail!("terminal cycle workspace is not a regular directory")
        }
        Ok(_) => {
            let marker = workspace.join(".rift");
            let actual = fs::read_to_string(&marker)
                .with_context(|| format!("read Rift identity marker {}", marker.display()))?;
            if actual.trim() != artifacts.workspace.rift_id {
                anyhow::bail!(
                    "terminal cycle workspace Rift identity differs from durable authority"
                );
            }
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error).context("inspect terminal cycle workspace"),
    };
    let sandbox = workspace_root
        .path
        .join(format!(".iq-agent-sandbox-{}", artifacts.cycle_id));
    match fs::symlink_metadata(&sandbox) {
        Ok(_) => remove_sandbox_export(&sandbox.join("export"))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect terminal cycle sandbox root"),
    }
    if workspace_exists {
        crate::agent_protocol::remove_protocol_cycle(workspace, &artifacts.cycle_id)?;
    }
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
    let mut child = crate::git_command::command_in(retained_rift)?
        .args(["apply", "--index", "--binary", "--whitespace=nowarn", "--"])
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
    let parent_path = sandbox_root
        .parent()
        .context("sandbox root has no owned parent")?;
    let root_name = sandbox_root
        .file_name()
        .context("sandbox root has no name")?;
    let root = crate::secure_fs::DirectoryHandle::open(sandbox_root, "sandbox root")?;
    let ownership = read_sandbox_ownership(root.directory())?;
    let parent_metadata = fs::metadata(parent_path)?;
    let root_metadata = root.directory().metadata()?;
    if parent_metadata.uid() != unsafe { libc::geteuid() }
        || parent_metadata.permissions().mode() & 0o022 != 0
    {
        anyhow::bail!("sandbox parent is not exclusively owned by the current user");
    }
    if ownership.version != 1
        || ownership.cycle_id
            != root_name
                .as_bytes()
                .strip_prefix(b".iq-agent-sandbox-")
                .and_then(|value| std::str::from_utf8(value).ok())
                .context("sandbox root has no exact cycle identity")?
        || (ownership.parent_device, ownership.parent_inode)
            != (parent_metadata.dev(), parent_metadata.ino())
        || (ownership.root_device, ownership.root_inode)
            != (root_metadata.dev(), root_metadata.ino())
    {
        anyhow::bail!("sandbox ownership manifest differs from live filesystem identity");
    }
    root.remove("sandbox root")
}

fn write_sandbox_ownership(root: &Path, cycle_id: &str) -> Result<()> {
    let parent = root.parent().context("sandbox root has no parent")?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    let root_metadata = fs::symlink_metadata(root)?;
    if parent_metadata.file_type().is_symlink()
        || !parent_metadata.is_dir()
        || root_metadata.file_type().is_symlink()
        || !root_metadata.is_dir()
    {
        anyhow::bail!("sandbox ownership requires real parent and root directories");
    }
    let ownership = SandboxOwnership {
        version: 1,
        cycle_id: cycle_id.to_string(),
        parent_device: parent_metadata.dev(),
        parent_inode: parent_metadata.ino(),
        root_device: root_metadata.dev(),
        root_inode: root_metadata.ino(),
    };
    let path = root.join(SANDBOX_OWNERSHIP_FILE);
    let mut manifest = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    serde_json::to_writer(&mut manifest, &ownership)?;
    manifest.write_all(b"\n")?;
    manifest.sync_all()?;
    File::open(root)?.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(any(debug_assertions, feature = "test-hooks"))]
#[doc(hidden)]
pub fn write_test_sandbox_ownership(root: &Path, cycle_id: &str) -> Result<()> {
    write_sandbox_ownership(root, cycle_id)
}

fn read_sandbox_ownership(root: &File) -> Result<SandboxOwnership> {
    let name = std::ffi::CString::new(SANDBOX_OWNERSHIP_FILE)?;
    let descriptor = unsafe {
        libc::openat(
            root.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).context("open sandbox ownership manifest");
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > 4096 {
        anyhow::bail!("sandbox ownership manifest is not a bounded regular file");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes).context("parse sandbox ownership manifest")
}

pub fn staged_tree_digest(repository: &Path) -> Result<String> {
    let output = git_output(repository, ["write-tree"])?;
    let tree = output.trim();
    if tree.is_empty() {
        anyhow::bail!("Git did not return a staged tree identity");
    }
    Ok(format!("{:x}", Sha256::digest(tree.as_bytes())))
}

pub(crate) fn service_read_operation(
    commands: &mut [crate::agent_config::AuthorizedCommand],
    _timeout: Duration,
    mut before_release: impl FnMut(usize) -> Result<()>,
) -> Result<Vec<std::process::Output>> {
    if commands.is_empty() {
        anyhow::bail!("read command operation is empty");
    }
    let mut outputs = Vec::with_capacity(commands.len());
    for (index, command) in commands.iter_mut().enumerate() {
        before_release(index).context("authorize bounded read command release")?;
        outputs.push(command.output()?);
    }
    Ok(outputs)
}

fn kill_control_group(control_group: &str) -> Result<()> {
    let relative = control_group
        .strip_prefix('/')
        .context("systemd control group is not absolute")?;
    let relative = Path::new(relative);
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        anyhow::bail!("systemd control group escapes the cgroup root");
    }
    let kill = Path::new("/sys/fs/cgroup")
        .join(relative)
        .join("cgroup.kill");
    match fs::write(&kill, b"1\n") {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("kill exact command control group"),
    }
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
    let git = crate::git_command::executable_identity()?;
    let mut command = vec![
        snapshot.sandbox.unshare.path.as_os_str().to_os_string(),
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
        snapshot.sandbox.bubblewrap.path.as_os_str().to_os_string(),
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
        OsString::from("--dir"),
        OsString::from("/iq-bin"),
        OsString::from("--ro-bind"),
        git.path.as_os_str().to_os_string(),
        OsString::from("/iq-git"),
        OsString::from("--ro-bind"),
        snapshot.executable.path.as_os_str().to_os_string(),
        OsString::from("/iq-runner"),
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
        OsString::from("--symlink"),
        OsString::from("/iq-git"),
        OsString::from("/iq-bin/git"),
        OsString::from("--setenv"),
        OsString::from("HOME"),
        OsString::from("/home/iq"),
        OsString::from("--setenv"),
        OsString::from("PATH"),
        OsString::from("/iq-bin:/usr/bin:/bin"),
        OsString::from("--setenv"),
        OsString::from("IQ_GIT_EXECUTABLE"),
        OsString::from("/iq-git"),
        OsString::from("--setenv"),
        OsString::from("GIT_CONFIG_NOSYSTEM"),
        OsString::from("1"),
        OsString::from("--setenv"),
        OsString::from("GIT_CONFIG_GLOBAL"),
        OsString::from("/dev/null"),
        OsString::from("--setenv"),
        OsString::from("GIT_NO_REPLACE_OBJECTS"),
        OsString::from("1"),
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
        "config".to_string(),
        crate::git_command::local_config_digest(repository)?,
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
    let refs = String::from_utf8(read_bounded(&export.join("refs"), 1024 * 1024)?)?;
    if before.get("refs").is_some_and(|expected| expected != &refs) {
        anyhow::bail!("agent changed Git refs");
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
    let output = crate::git_command::command_in(repository)?
        .args(args)
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
    let output = crate::git_command::command_in(repository)?
        .args(args)
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
    for (executable, name) in [
        (&sandbox.bubblewrap, "bwrap"),
        (&sandbox.unshare, "unshare"),
        (&sandbox.systemd_run, "systemd-run"),
        (&sandbox.systemctl, "systemctl"),
    ] {
        crate::agent_config::open_executable_authority(executable)
            .with_context(|| format!("open required sandbox helper {name}"))?;
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
