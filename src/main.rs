use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use iq::composition::{RepositoryInitOptions, RepositoryManager};
use iq::integrator::{
    verify_rift_workspace_config, workspace_status, HostSignoffPolicy, IntegrationPolicy,
    Integrator, IntegratorOptions, SignoffPolicy,
};
use iq::sqlite::{Attempt, EnqueueRequest, QueueItem, SqliteQueue, SqliteQueueReader};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "iq", version, about = "Durable repository integration queue")]
struct Cli {
    #[arg(long, global = true)]
    queue_db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Migrate {
        #[arg(long)]
        system_config: PathBuf,
    },
    Repo {
        #[command(subcommand)]
        command: RepoCommand,
    },
    DevWorkspace {
        #[command(subcommand)]
        command: DevWorkspaceCommand,
    },
    Submit {
        #[arg(long)]
        workspace: String,
        #[arg(long)]
        replace: Option<String>,
    },
    Cleanup {
        #[arg(long)]
        repo_key: Option<String>,
        #[arg(long)]
        workspace: Option<String>,
    },
    Integrate {
        #[arg(long)]
        system_config: PathBuf,
        #[arg(long, conflicts_with = "resume", required_unless_present = "resume")]
        next: bool,
        #[arg(long, conflicts_with = "next", required_unless_present = "next")]
        resume: Option<String>,
        #[arg(long)]
        repo_path: PathBuf,
        #[arg(long)]
        repo_key: Option<String>,
        #[arg(long, default_value = "main")]
        target: String,
        #[arg(long, default_value = "origin")]
        remote: String,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        owner: Option<String>,
    },
    Enqueue {
        #[arg(long)]
        repo_path: PathBuf,
        #[arg(long)]
        repo_key: Option<String>,
        #[arg(long)]
        source: String,
        #[arg(long, default_value = "main")]
        target: String,
        #[arg(long)]
        head: String,
        #[arg(long)]
        pr_url: Option<String>,
        #[arg(long)]
        producer: Option<String>,
        #[arg(long, default_value = "origin")]
        remote: String,
    },
    List,
    Inbox {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    Show {
        item: String,
        #[arg(long)]
        config: PathBuf,
    },
    Watch {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 0)]
        cursor: u64,
        #[arg(long, default_value_t = 1000)]
        limit: u32,
        #[arg(long)]
        json: bool,
    },
    Answer {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        external_id: String,
        #[arg(long)]
        request: String,
        #[arg(long)]
        effort: String,
        #[arg(long)]
        attempt: String,
        #[arg(long)]
        cycle: String,
        #[arg(long)]
        target_sha: String,
        #[arg(long)]
        source_sha: String,
        #[arg(long)]
        candidate_sha: Option<String>,
        #[arg(long)]
        answer: String,
    },
    Cancel {
        item: String,
    },
    Retry {
        item: String,
        #[arg(long)]
        config: PathBuf,
    },
    Notify {
        #[command(subcommand)]
        command: NotifyCommand,
        #[arg(long)]
        system_config: PathBuf,
    },
    Events {
        item: String,
    },
    Attempt {
        item: String,
    },
    Evidence {
        item: String,
        #[arg(long, value_enum, default_value_t = EvidencePhaseArg::All)]
        phase: EvidencePhaseArg,
    },
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    Daemon {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        system_config: PathBuf,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        ready_file: Option<PathBuf>,
        #[arg(long)]
        once: bool,
        #[arg(long, default_value_t = 5)]
        interval_seconds: u64,
    },
    Doctor {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        system_config: PathBuf,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    #[command(hide = true)]
    RemoteExec {
        #[arg(long)]
        repo_path: PathBuf,
        #[arg(long)]
        repo_key: String,
        #[arg(long, default_value = "main")]
        target: String,
        #[arg(long, default_value = "origin")]
        remote: String,
        #[arg(long)]
        workspace_root: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum RepoCommand {
    Init {
        #[arg(long, default_value = ".")]
        path: PathBuf,
        #[arg(long, default_value = "main")]
        target: String,
        #[arg(long, default_value = "origin")]
        remote: String,
        #[arg(long)]
        seed: Option<PathBuf>,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
    },
    List,
    Status {
        #[arg(long)]
        repo_key: String,
    },
}

#[derive(Subcommand, Debug)]
enum DevWorkspaceCommand {
    Create {
        #[arg(long)]
        repo_key: String,
        #[arg(long)]
        name: String,
    },
    List {
        #[arg(long)]
        repo_key: Option<String>,
    },
    Status {
        id: String,
    },
    Remove {
        id: String,
        /// Delete safe file residue after the exact Rift is absent.
        #[arg(long)]
        discard_residue: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCommand {
    /// Reconcile IQ-owned repositories without parsing IQ YAML in the host.
    Reconcile {
        #[arg(long)]
        current_config: PathBuf,
        #[arg(long)]
        desired_inventory: PathBuf,
        #[arg(long)]
        current_manager_state: PathBuf,
        #[arg(long)]
        staged_directory: PathBuf,
        #[arg(long)]
        reconcile_lock: PathBuf,
        #[arg(long)]
        bootstrap: bool,
        #[arg(long)]
        workspace_root: PathBuf,
    },
    /// Verify a staged generation against its manifest and current input CAS.
    VerifyGeneration {
        #[arg(long)]
        generation: PathBuf,
        #[arg(long)]
        current_config: PathBuf,
        #[arg(long)]
        current_manager_state: PathBuf,
        #[arg(long)]
        reconcile_lock: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum NotifyCommand {
    Dispatch,
    Redeliver { delivery_id: i64 },
}

#[derive(Parser, Debug)]
#[command(name = "iq")]
struct RemoteCli {
    #[command(subcommand)]
    command: RemoteCommand,
}

#[derive(Subcommand, Debug)]
enum RemoteCommand {
    Enqueue {
        #[arg(long)]
        source: String,
        #[arg(long)]
        head: String,
        #[arg(long)]
        pr_url: Option<String>,
        #[arg(long)]
        producer: Option<String>,
    },
    List,
    Events {
        item: String,
    },
    Attempt {
        item: String,
    },
    Evidence {
        item: String,
        #[arg(long, value_enum, default_value_t = EvidencePhaseArg::All)]
        phase: EvidencePhaseArg,
    },
    Retry {
        item: String,
    },
}

#[derive(Subcommand, Debug)]
enum WorkspaceCommand {
    Status {
        #[arg(long)]
        repo_path: PathBuf,
        #[arg(long)]
        repo_key: Option<String>,
        #[arg(long, default_value = "origin")]
        remote: String,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        owner: Option<String>,
    },
    Reset {
        #[arg(long)]
        system_config: PathBuf,
        #[arg(long)]
        repo_path: PathBuf,
        #[arg(long)]
        repo_key: Option<String>,
        #[arg(long, default_value = "origin")]
        remote: String,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        owner: Option<String>,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum EvidencePhaseArg {
    All,
    Validation,
    Signoff,
}

#[derive(Debug, Serialize)]
struct EvidenceOutput {
    attempt: Attempt,
    validation: Option<EvidenceFile>,
    signoff: Option<EvidenceFile>,
}

#[derive(Debug, Serialize)]
struct EvidenceFile {
    path: String,
    truncated: bool,
    content: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let db_path = match cli.queue_db {
        Some(path) => path,
        None => SqliteQueue::default_db_path()?,
    };
    match cli.command {
        Command::Migrate { system_config } => {
            let queue = SqliteQueue::migrate_v8(&db_path, &system_config)?;
            print_json(&json!({"schema_version": 9, "database": queue.path()}))?;
        }
        Command::Repo { command } => {
            let manager = RepositoryManager::new(SqliteQueue::open(&db_path)?);
            match command {
                RepoCommand::Init {
                    path,
                    target,
                    remote,
                    seed,
                    workspace_root,
                } => print_json(&manager.init(
                    &path,
                    RepositoryInitOptions {
                        target_branch: target,
                        remote,
                        seed_path: seed,
                        workspace_root,
                    },
                )?)?,
                RepoCommand::List => print_json(&manager.list()?)?,
                RepoCommand::Status { repo_key } => print_json(&manager.status(&repo_key)?)?,
            }
        }
        Command::DevWorkspace { command } => {
            let manager = RepositoryManager::new(SqliteQueue::open(&db_path)?);
            match command {
                DevWorkspaceCommand::Create { repo_key, name } => {
                    print_json(&manager.create_workspace(&repo_key, &name)?)?
                }
                DevWorkspaceCommand::List { repo_key } => {
                    print_json(&manager.workspaces(repo_key.as_deref())?)?
                }
                DevWorkspaceCommand::Status { id } => print_json(&manager.workspace_status(&id)?)?,
                DevWorkspaceCommand::Remove {
                    id,
                    discard_residue,
                } => {
                    let workspace = if discard_residue {
                        manager.discard_workspace_residue(&id)?
                    } else {
                        manager.remove_workspace(&id)?
                    };
                    print_json(&workspace)?;
                }
            }
        }
        Command::Submit { workspace, replace } => {
            let queue = SqliteQueue::open(&db_path)?;
            let manager = RepositoryManager::new(queue.clone());
            let (submission, item) = manager.submit(&workspace, replace.as_deref())?;
            iq::state_repository::reserve_full_issue(
                &iq::control_store::ControlStore::open(queue.path())?,
                &item.id,
            )?;
            print_json(&(submission, item))?;
        }
        Command::Cleanup {
            repo_key,
            workspace,
        } => {
            let manager = RepositoryManager::new(SqliteQueue::open(&db_path)?);
            if let Some(workspace) = workspace {
                print_json(&manager.remove_workspace(&workspace)?)?;
            } else {
                let repo_keys = match repo_key {
                    Some(repo_key) => vec![repo_key],
                    None => manager.list()?.into_iter().map(|repo| repo.key).collect(),
                };
                let mut cleaned = Vec::new();
                for repo_key in repo_keys {
                    cleaned.extend(manager.cleanup_repo(&repo_key)?);
                }
                print_json(&cleaned)?;
            }
        }
        Command::Integrate {
            next: _,
            system_config,
            resume,
            repo_path,
            repo_key,
            target,
            remote,
            workspace_root,
            owner,
        } => {
            let system = iq::agent_config::SystemConfig::load(&system_config)?;
            let integrator = Integrator::new(integrator_options_with_system_config(
                integrator_options(
                    db_path,
                    repo_path,
                    repo_key,
                    &target,
                    remote,
                    workspace_root,
                    owner,
                )?,
                &system,
            ))?;
            if let Some(item) = resume {
                print_json(&integrator.resume_item(&item)?)?;
            } else {
                print_json(&integrator.run_once()?)?;
            }
        }
        Command::Enqueue {
            repo_path,
            repo_key,
            source,
            target,
            head,
            pr_url,
            producer,
            remote,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            let repo_path = canonical_repo_path(&repo_path, "enqueue repository")?;
            let repo_key = match repo_key {
                Some(repo_key) => repo_key,
                None => default_repo_key(&repo_path, &target)?,
            };
            validate_branch_handoff(&repo_path, &remote, &source, &target, &head)?;
            let state_repository =
                iq::composition::load_project_control_only(&repo_path)?.state_repository;
            iq::state_repository::repository(&state_repository)?.verify()?;
            let item = queue.enqueue(EnqueueRequest {
                repo_key,
                repo_path: repo_path
                    .to_str()
                    .context("canonical enqueue repository path is not valid UTF-8")?
                    .to_string(),
                source_branch: source,
                target_branch: target,
                current_head_sha: head,
                pr_url,
                producer_metadata: json!({ "producer": producer }),
                state_repository,
            })?;
            iq::state_repository::reserve_full_issue(
                &iq::control_store::ControlStore::open(queue.path())?,
                &item.id,
            )?;
            print_json(&item)?;
        }
        Command::List => {
            let queue = SqliteQueueReader::open(&db_path)?;
            print_json(&queue.list_items()?)?;
        }
        Command::Inbox { config, limit } => {
            let system = iq::agent_config::SystemConfig::load(&config)?;
            print_json(&iq::control_api::request(
                &system.control_plane.unix_socket,
                &iq::control_api::ApiRequest::Inbox { limit },
                system.control_plane.max_response_bytes,
            )?)?;
        }
        Command::Show { item, config } => {
            let system = iq::agent_config::SystemConfig::load(&config)?;
            print_json(&iq::control_api::request(
                &system.control_plane.unix_socket,
                &iq::control_api::ApiRequest::Show { item_id: item },
                system.control_plane.max_response_bytes,
            )?)?;
        }
        Command::Watch {
            config,
            cursor,
            limit,
            json,
        } => {
            let system = iq::agent_config::SystemConfig::load(&config)?;
            iq::control_api::watch(
                &system.control_plane.unix_socket,
                cursor,
                limit,
                system.control_plane.max_response_bytes,
                |response| {
                    if json {
                        print_json(response)
                    } else {
                        print_json(&response.result)
                    }
                },
            )?;
        }
        Command::Answer {
            config,
            external_id,
            request,
            effort,
            attempt,
            cycle,
            target_sha,
            source_sha,
            candidate_sha,
            answer,
        } => {
            let system = iq::agent_config::SystemConfig::load(&config)?;
            print_json(&iq::control_api::request(
                &system.control_plane.unix_socket,
                &iq::control_api::ApiRequest::Answer {
                    answer: iq::control_store::AnswerCommand {
                        external_id,
                        request_id: request,
                        effort_id: effort,
                        attempt_id: attempt,
                        cycle_id: cycle,
                        target_sha,
                        source_sha,
                        candidate_sha,
                        answer,
                    },
                },
                system.control_plane.max_response_bytes,
            )?)?;
        }
        Command::Cancel { item } => {
            let queue = SqliteQueue::open(&db_path)?;
            let store = iq::control_store::ControlStore::open(queue.path())?;
            if let Some(effort) = store.effort_for_item(&item)? {
                let cancelled = store.cancel(&effort.id, "local_cli", "operator_cancelled")?;
                store.reconcile_cancelled_runner_terminations(false)?;
                print_json(&cancelled)?;
            } else {
                print_json(&queue.transition_item(&item, iq::core::QueueStatus::Cancelled)?)?;
            }
        }
        Command::Retry { item, config } => {
            let system = iq::agent_config::SystemConfig::load(&config)?;
            print_json(&iq::control_api::request(
                &system.control_plane.unix_socket,
                &iq::control_api::ApiRequest::Retry { item_id: item },
                system.control_plane.max_response_bytes,
            )?)?;
        }
        Command::Notify {
            command,
            system_config,
        } => {
            let system = iq::agent_config::SystemConfig::load(&system_config)?;
            let dispatcher =
                iq::notifications::NotificationDispatcher::new(&db_path, system.notifications);
            match command {
                NotifyCommand::Dispatch => {
                    print_json(&json!({"processed":dispatcher.dispatch_once()?}))?
                }
                NotifyCommand::Redeliver { delivery_id } => print_json(
                    &json!({"delivery_id":dispatcher.redeliver(delivery_id,"local_cli")?}),
                )?,
            }
        }
        Command::Events { item } => {
            let queue = SqliteQueueReader::open(&db_path)?;
            print_json(&queue.events(&item)?)?;
        }
        Command::Attempt { item } => {
            let queue = SqliteQueueReader::open(&db_path)?;
            let queued = queue.get_item(&item)?;
            let attempt_id = queued
                .current_attempt_id
                .context("item has no current integration attempt")?;
            print_json(&queue.get_attempt(&attempt_id)?)?;
        }
        Command::Evidence { item, phase } => {
            let queue = SqliteQueueReader::open(&db_path)?;
            let queued = queue.get_item(&item)?;
            print_json(&read_evidence(&queue, &queued, phase)?)?;
        }
        Command::Workspace { command } => match command {
            WorkspaceCommand::Status {
                repo_path,
                repo_key,
                remote: _,
                workspace_root: _,
                owner: _,
            } => {
                let repo_key = match repo_key {
                    Some(repo_key) => repo_key,
                    None => {
                        let repo_path = canonical_existing_repo_path(
                            &repo_path,
                            "workspace status repository",
                        )?;
                        default_repo_key(&repo_path, "main")?
                    }
                };
                let queue = SqliteQueueReader::open(&db_path)?;
                print_json(&workspace_status(&queue, &repo_key)?)?;
            }
            WorkspaceCommand::Reset {
                system_config,
                repo_path,
                repo_key,
                remote,
                workspace_root,
                owner,
            } => {
                let system = iq::agent_config::SystemConfig::load(&system_config)?;
                let integrator = Integrator::new(integrator_options_with_system_config(
                    integrator_options(
                        db_path,
                        repo_path,
                        repo_key,
                        "main",
                        remote,
                        workspace_root,
                        owner,
                    )?,
                    &system,
                ))?;
                print_json(&integrator.reset_workspaces()?)?;
            }
        },
        Command::Daemon {
            config,
            system_config,
            owner,
            ready_file,
            once,
            interval_seconds,
        } => {
            run_daemon_config(
                db_path,
                &config,
                &system_config,
                owner,
                ready_file,
                once,
                interval_seconds,
            )?;
        }
        Command::RemoteExec {
            repo_path,
            repo_key,
            target,
            remote,
            workspace_root,
        } => run_remote_exec(db_path, repo_path, repo_key, target, remote, workspace_root)?,
        Command::Doctor {
            config,
            system_config,
        } => run_doctor(&db_path, &config, &system_config)?,
        Command::Config { command } => match command {
            ConfigCommand::Reconcile {
                current_config,
                desired_inventory,
                current_manager_state,
                staged_directory,
                reconcile_lock,
                bootstrap,
                workspace_root,
            } => reconcile_config(
                &current_config,
                &desired_inventory,
                &current_manager_state,
                &staged_directory,
                &reconcile_lock,
                bootstrap,
                &workspace_root,
            )?,
            ConfigCommand::VerifyGeneration {
                generation,
                current_config,
                current_manager_state,
                reconcile_lock,
            } => verify_generation(
                &generation,
                &current_config,
                &current_manager_state,
                &reconcile_lock,
            )?,
        },
    }
    Ok(())
}

fn run_doctor(
    queue_db: &std::path::Path,
    config_path: &std::path::Path,
    system_config_path: &std::path::Path,
) -> Result<()> {
    let system_config = iq::agent_config::SystemConfig::load(system_config_path)?;
    let runner_snapshot = system_config.runner_snapshot(None)?;
    iq::agent_config::verify_executable(&runner_snapshot.executable)?;
    let notification_health = iq::notifications::NotificationDispatcher::new(
        queue_db,
        system_config.notifications.clone(),
    )
    .health();
    let config = read_daemon_config(config_path)?;
    let queue = SqliteQueue::open(queue_db)?;
    let repository_manager = RepositoryManager::new(queue.clone());
    let gh = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
    let mut results = Vec::new();
    for validated in config.repos {
        let ValidatedDaemonRepo {
            repo,
            canonical_repo_path,
            repo_key,
            target,
            remote,
            validation,
        } = validated;
        let registered = queue.repository_if_exists(&repo_key)?.is_some();
        if registered && (validation != ValidationConfig::None || repo.signoff.is_some()) {
            anyhow::bail!(
                "registered repository {repo_key} rejects daemon validation and signoff settings"
            );
        }
        let workspace_root = integrator_options_with_system_config(
            integrator_options(
                queue_db.to_path_buf(),
                canonical_repo_path.clone(),
                Some(repo_key.clone()),
                &target,
                remote.clone(),
                repo.workspace_root.clone(),
                Some("iq-doctor".into()),
            )?,
            &system_config,
        )
        .workspace_root;
        verify_rift_workspace_config(
            &canonical_repo_path,
            &workspace_root,
            &repo_key,
            None,
            queue_db,
        )?;
        let output = ProcessCommand::new("git")
            .args([
                "ls-remote",
                "--heads",
                remote.as_str(),
                &format!("refs/heads/{target}"),
            ])
            .current_dir(&canonical_repo_path)
            .output()
            .with_context(|| format!("query {remote}/{target}"))?;
        if !output.status.success() || output.stdout.is_empty() {
            anyhow::bail!(
                "cannot resolve {remote}/{target} from {}: {}",
                canonical_repo_path.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        if let Some(signoff) = &repo.signoff {
            let login_output = ProcessCommand::new(&gh)
                .args(["api", "user", "--jq", ".login"])
                .output()
                .with_context(|| format!("run {gh} api user"))?;
            if !login_output.status.success() {
                anyhow::bail!(
                    "gh authentication check failed: {}",
                    String::from_utf8_lossy(&login_output.stderr).trim()
                );
            }
            let login = String::from_utf8_lossy(&login_output.stdout)
                .trim()
                .to_string();
            if login != signoff.trusted_creator {
                anyhow::bail!(
                    "gh login {login} does not match trusted status creator {}",
                    signoff.trusted_creator
                );
            }
            let access = ProcessCommand::new(&gh)
                .args([
                    "repo",
                    "view",
                    signoff.repository.as_str(),
                    "--json",
                    "nameWithOwner",
                ])
                .output()
                .with_context(|| format!("run {gh} repo view {}", signoff.repository))?;
            if !access.status.success() {
                anyhow::bail!(
                    "gh user {login} cannot access {}: {}",
                    signoff.repository,
                    String::from_utf8_lossy(&access.stderr).trim()
                );
            }
        }
        let validation_report = if registered {
            let policy = repository_manager.inspect_local_policy(&repo_key)?;
            json!({
                "authority": "local_integration_checkout",
                "policy": policy.policy,
            })
        } else {
            match &validation {
                ValidationConfig::None => json!({
                    "authority": "none",
                    "policy": {"mode": "none"},
                }),
                ValidationConfig::Command { command } => json!({
                    "authority": "daemon",
                    "policy": {
                        "mode": "command",
                        "command": command,
                        "signoff_required": repo.signoff.is_some(),
                    },
                }),
            }
        };
        results.push(json!({
            "repo_key": repo_key,
            "repo_path": canonical_repo_path,
            "workspace_root": workspace_root,
            "target": target,
            "remote": remote,
            "validation": validation_report,
        }));
    }
    results.push(json!({
        "integration_agent": {
            "runner": runner_snapshot.kind,
            "executable": runner_snapshot.executable.path,
            "sandbox": runner_snapshot.sandbox,
            "cycle_limit": iq::control_domain::AUTOMATIC_CYCLE_LIMIT,
        },
        "control_socket": system_config.control_plane.unix_socket,
        "notifications": notification_health.iter().map(|health| json!({
            "backend": health.backend,
            "status": if health.available { "available" } else { "degraded" },
            "detail": health.detail,
        })).collect::<Vec<_>>(),
    }));
    print_json(&results)
}

fn run_remote_exec(
    db_path: PathBuf,
    repo_path: PathBuf,
    repo_key: String,
    target: String,
    remote: String,
    _workspace_root: PathBuf,
) -> Result<()> {
    let original = std::env::var("SSH_ORIGINAL_COMMAND")
        .context("remote-exec requires SSH_ORIGINAL_COMMAND")?;
    let args = shell_words::split(&original).context("parse SSH_ORIGINAL_COMMAND")?;
    let command = RemoteCli::try_parse_from(args).context("parse permitted remote IQ command")?;
    match command.command {
        RemoteCommand::Enqueue {
            source,
            head,
            pr_url,
            producer,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            let repo_path = canonical_repo_path(&repo_path, "remote enqueue repository")?;
            validate_branch_handoff(&repo_path, &remote, &source, &target, &head)?;
            let state_repository =
                iq::composition::load_project_control_only(&repo_path)?.state_repository;
            iq::state_repository::repository(&state_repository)?.verify()?;
            let item = queue.enqueue(EnqueueRequest {
                repo_key,
                repo_path: repo_path
                    .to_str()
                    .context("canonical remote enqueue repository path is not valid UTF-8")?
                    .to_string(),
                source_branch: source,
                target_branch: target,
                current_head_sha: head,
                pr_url,
                producer_metadata: json!({ "producer": producer }),
                state_repository,
            })?;
            iq::state_repository::reserve_full_issue(
                &iq::control_store::ControlStore::open(queue.path())?,
                &item.id,
            )?;
            print_json(&item)?;
        }
        RemoteCommand::List => {
            let queue = SqliteQueueReader::open(&db_path)?;
            let items = queue
                .list_items()?
                .into_iter()
                .filter(|item| item.repo_key == repo_key)
                .collect::<Vec<_>>();
            print_json(&items)?;
        }
        RemoteCommand::Events { item } => {
            let queue = SqliteQueueReader::open(&db_path)?;
            require_remote_item(&queue, &item, &repo_key)?;
            print_json(&queue.events(&item)?)?;
        }
        RemoteCommand::Attempt { item } => {
            let queue = SqliteQueueReader::open(&db_path)?;
            let queued = require_remote_item(&queue, &item, &repo_key)?;
            let attempt_id = queued
                .current_attempt_id
                .context("item has no current integration attempt")?;
            print_json(&queue.get_attempt(&attempt_id)?)?;
        }
        RemoteCommand::Evidence { item, phase } => {
            let queue = SqliteQueueReader::open(&db_path)?;
            let queued = require_remote_item(&queue, &item, &repo_key)?;
            print_json(&read_evidence(&queue, &queued, phase)?)?;
        }
        RemoteCommand::Retry { .. } => {
            anyhow::bail!("remote retry requires the authenticated Unix control API")
        }
    }
    Ok(())
}

fn require_remote_item(
    queue: &SqliteQueueReader,
    item_id: &str,
    repo_key: &str,
) -> Result<iq::sqlite::QueueItem> {
    let item = queue.get_item(item_id)?;
    require_remote_item_scope(&item, repo_key)?;
    Ok(item)
}

fn require_remote_item_scope(item: &QueueItem, repo_key: &str) -> Result<()> {
    if item.repo_key != repo_key {
        anyhow::bail!(
            "item {} belongs to repo queue {}, not {repo_key}",
            item.id,
            item.repo_key
        );
    }
    Ok(())
}

fn validate_branch_handoff(
    repo_path: &std::path::Path,
    remote: &str,
    branch: &str,
    target: &str,
    expected_head: &str,
) -> Result<()> {
    if branch == target {
        anyhow::bail!("source branch must not be target branch {target}");
    }
    if !matches!(expected_head.len(), 40 | 64)
        || !expected_head.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        anyhow::bail!("expected head must be a full hexadecimal Git object ID");
    }
    let ref_check = ProcessCommand::new("git")
        .args(["check-ref-format", "--branch", branch])
        .output()
        .context("validate source branch name")?;
    if !ref_check.status.success() {
        anyhow::bail!("invalid source branch name: {branch}");
    }
    let reference = format!("refs/heads/{branch}");
    let output = ProcessCommand::new("git")
        .args(["ls-remote", "--heads", remote, reference.as_str()])
        .current_dir(repo_path)
        .output()
        .with_context(|| format!("query {remote}/{branch} from {}", repo_path.display()))?;
    if !output.status.success() {
        anyhow::bail!(
            "failed to query {remote}/{branch}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let remote_head = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string();
    if remote_head.is_empty() {
        anyhow::bail!("remote branch {remote}/{branch} does not exist");
    }
    if remote_head != expected_head {
        anyhow::bail!("remote branch {remote}/{branch} is {remote_head}, expected {expected_head}");
    }
    Ok(())
}

fn integrator_options(
    queue_db: PathBuf,
    repo_path: PathBuf,
    repo_key: Option<String>,
    target: &str,
    remote: String,
    workspace_root: Option<PathBuf>,
    owner: Option<String>,
) -> Result<IntegratorOptions> {
    let repo_path = canonical_existing_repo_path(&repo_path, "integrator repository")?;
    let repo_key = match repo_key {
        Some(repo_key) => repo_key,
        None => default_repo_key(&repo_path, target)?,
    };
    let workspace_root = workspace_root.unwrap_or_else(|| {
        queue_db
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("workspaces")
            .join(workspace_scope(&repo_key))
    });
    Ok(IntegratorOptions {
        repo_key,
        repo_path,
        queue_db,
        owner_id: owner.unwrap_or_else(|| format!("iq-{}", std::process::id())),
        lease_ttl_seconds: 30,
        base_remote: remote,
        workspace_root,
        rift_database: None,
        system_config: iq::agent_config::SystemConfig {
            integration_agent: iq::agent_config::IntegrationAgentConfig {
                runner: iq::control_domain::RunnerKind::Opencode,
                executable: PathBuf::from("/invalid"),
                agent: "unconfigured".into(),
                model: "unconfigured".into(),
                cycle_timeout_seconds: 1,
                max_log_bytes: 1,
                max_result_bytes: 1,
                max_processes: 1,
                memory_bytes: 1,
                cpu_seconds: 1,
                writable_bytes: 1,
                open_files: 1,
                credential_env: "UNCONFIGURED".into(),
            },
            control_plane: iq::agent_config::ControlPlaneConfig {
                unix_socket: PathBuf::from("/invalid"),
                max_request_bytes: 1,
                max_free_text_bytes: 1,
                max_response_bytes: 1,
                max_concurrent_clients: 1,
                max_client_queue_bytes: 1,
                max_stream_backlog_events: 1,
                client_idle_seconds: 1,
            },
            notifications: Default::default(),
        },
    })
}

fn integrator_options_with_system_config(
    mut options: IntegratorOptions,
    system_config: &iq::agent_config::SystemConfig,
) -> IntegratorOptions {
    options.system_config = system_config.clone();
    options
}

fn workspace_scope(repo_key: &str) -> String {
    let hash = repo_key
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    format!("{hash:016x}")
}

fn default_repo_key(repo_path: &std::path::Path, target: &str) -> Result<String> {
    let path = repo_path
        .to_str()
        .context("canonical repository path is not valid UTF-8")?;
    Ok(format!("{path}::{target}"))
}

fn canonical_existing_repo_path(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve {label} {}", path.display()))?;
    require_utf8_path(&canonical, label)?;
    if !canonical.is_dir() {
        anyhow::bail!(
            "{label} path must be an existing directory: {}",
            path.display()
        );
    }
    Ok(canonical)
}

fn read_evidence(
    queue: &SqliteQueueReader,
    item: &QueueItem,
    phase: EvidencePhaseArg,
) -> Result<EvidenceOutput> {
    let attempt_id = item
        .current_attempt_id
        .as_deref()
        .context("item has no current integration attempt")?;
    let attempt = queue.get_attempt(attempt_id)?;
    let evidence_dir = attempt
        .validation_log_path
        .as_deref()
        .map(PathBuf::from)
        .map(|path| validated_evidence_dir(queue, item, &attempt, &path))
        .transpose()?;
    let validation = if matches!(phase, EvidencePhaseArg::All | EvidencePhaseArg::Validation) {
        attempt
            .validation_log_path
            .as_deref()
            .map(PathBuf::from)
            .map(|path| {
                read_evidence_file(
                    &path,
                    evidence_dir
                        .as_deref()
                        .context("attempt has no validated evidence directory")?,
                )
            })
            .transpose()?
    } else {
        None
    };
    let signoff = if matches!(phase, EvidencePhaseArg::All | EvidencePhaseArg::Signoff) {
        evidence_dir
            .as_deref()
            .map(|directory| read_optional_evidence_file(&directory.join("signoff.log"), directory))
            .transpose()?
            .flatten()
    } else {
        None
    };
    Ok(EvidenceOutput {
        attempt,
        validation,
        signoff,
    })
}

fn validated_evidence_dir(
    queue: &SqliteQueueReader,
    item: &QueueItem,
    attempt: &Attempt,
    path: &std::path::Path,
) -> Result<PathBuf> {
    let evidence_root = queue
        .path()
        .parent()
        .context("queue database has no evidence parent")?
        .join("evidence");
    let expected = evidence_root.join(&item.id).join(&attempt.id);
    let attempt_dir = path
        .parent()
        .context("evidence path has no attempt directory")?;
    let item_dir = attempt_dir
        .parent()
        .context("evidence path has no item directory")?;
    let actual_evidence_root = item_dir
        .parent()
        .context("evidence path has no evidence root")?;
    if attempt_dir != expected
        || attempt_dir.file_name() != Some(std::ffi::OsStr::new(&attempt.id))
        || item_dir.file_name() != Some(std::ffi::OsStr::new(&item.id))
        || actual_evidence_root != evidence_root
    {
        anyhow::bail!("attempt evidence path is outside its queue-owned evidence directory");
    }
    for component in [actual_evidence_root, item_dir, attempt_dir] {
        let metadata = std::fs::symlink_metadata(component)
            .with_context(|| format!("inspect evidence path {}", component.display()))?;
        if metadata.file_type().is_symlink() {
            anyhow::bail!("attempt evidence path contains a symlink");
        }
    }
    let canonical_expected_root = evidence_root
        .parent()
        .context("evidence root has no parent")?
        .canonicalize()
        .with_context(|| format!("resolve evidence parent {}", evidence_root.display()))?;
    let canonical_evidence_root = actual_evidence_root
        .canonicalize()
        .with_context(|| format!("resolve evidence root {}", actual_evidence_root.display()))?;
    if canonical_evidence_root.parent() != Some(canonical_expected_root.as_path()) {
        anyhow::bail!("attempt evidence root is outside queue-owned evidence storage");
    }
    let canonical_attempt = attempt_dir
        .canonicalize()
        .with_context(|| format!("resolve evidence directory {}", attempt_dir.display()))?;
    let canonical_path = path
        .canonicalize()
        .with_context(|| format!("resolve evidence file {}", path.display()))?;
    if canonical_path.parent() != Some(canonical_attempt.as_path()) {
        anyhow::bail!("attempt evidence file escapes its queue-owned evidence directory");
    }
    Ok(canonical_attempt)
}

fn read_optional_evidence_file(
    path: &std::path::Path,
    evidence_dir: &std::path::Path,
) -> Result<Option<EvidenceFile>> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => read_evidence_file(path, evidence_dir).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("inspect evidence file {}", path.display()))
        }
    }
}

fn read_evidence_file(
    path: &std::path::Path,
    evidence_dir: &std::path::Path,
) -> Result<EvidenceFile> {
    const MAX_EVIDENCE_BYTES: u64 = 128 * 1024;
    let canonical_path = path
        .canonicalize()
        .with_context(|| format!("resolve evidence file {}", path.display()))?;
    if canonical_path.parent() != Some(evidence_dir) {
        anyhow::bail!("attempt evidence file escapes its queue-owned evidence directory");
    }
    let directory_metadata = std::fs::symlink_metadata(evidence_dir)
        .with_context(|| format!("inspect evidence directory {}", evidence_dir.display()))?;
    if directory_metadata.file_type().is_symlink() || !directory_metadata.is_dir() {
        anyhow::bail!("evidence directory must be a real directory");
    }
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(evidence_dir)
        .with_context(|| format!("open evidence directory {}", evidence_dir.display()))?;
    let opened_directory = directory.metadata()?;
    if directory_metadata.dev() != opened_directory.dev()
        || directory_metadata.ino() != opened_directory.ino()
    {
        anyhow::bail!("evidence directory changed while opening");
    }
    let file_name = canonical_path
        .file_name()
        .context("evidence file has no file name")?;
    if file_name.as_bytes().contains(&b'/') {
        anyhow::bail!("invalid evidence file name");
    }
    let file_name = std::ffi::CString::new(file_name.as_bytes())?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("open evidence file {}", path.display()));
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    if !file.metadata()?.is_file() {
        anyhow::bail!("evidence file must be a regular file");
    }
    let length = file.metadata()?.len();
    let truncated = length > MAX_EVIDENCE_BYTES;
    let mut bytes = Vec::new();
    if truncated {
        let half = MAX_EVIDENCE_BYTES / 2;
        Read::by_ref(&mut file).take(half).read_to_end(&mut bytes)?;
        bytes.extend_from_slice(b"\n[IQ evidence middle truncated]\n");
        file.seek(SeekFrom::End(-(half as i64)))?;
        file.read_to_end(&mut bytes)?;
    } else {
        file.read_to_end(&mut bytes)?;
    }
    Ok(EvidenceFile {
        path: path.to_string_lossy().to_string(),
        truncated,
        content: String::from_utf8_lossy(&bytes).into_owned(),
    })
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonConfig {
    repos: Vec<DaemonRepoConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonRepoConfig {
    repo_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    repo_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace_root: Option<PathBuf>,
    validation: ValidationConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    signoff: Option<SignoffPolicy>,
    #[serde(default)]
    state_repository: iq::control_domain::StateRepositorySnapshot,
}

#[derive(Debug, Clone)]
struct ValidatedDaemonConfig {
    repos: Vec<ValidatedDaemonRepo>,
}

#[derive(Debug, Clone)]
struct ValidatedDaemonRepo {
    repo: DaemonRepoConfig,
    canonical_repo_path: PathBuf,
    repo_key: String,
    target: String,
    remote: String,
    validation: ValidationConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredInventory {
    manager_id: String,
    repositories: Vec<DesiredRepository>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredRepository {
    repo_path: PathBuf,
    target: String,
    validation: ValidationConfig,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ValidationConfig {
    None,
    Command { command: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagerState {
    manager_id: String,
    boundaries: Vec<ManagerBoundary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagerBoundary {
    repo_path: PathBuf,
    target: String,
    repo_key: String,
    ownership: ManagerOwnership,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ManagerOwnership {
    Adopted {
        original_validation: ValidationConfig,
        last_applied_validation: ValidationConfig,
    },
    Created {
        baseline: Box<RepositoryBaseline>,
        last_applied_validation: ValidationConfig,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RepositoryBaseline {
    repo_path: PathBuf,
    repo_key: String,
    target: String,
    remote: Option<String>,
    workspace_root: Option<PathBuf>,
    signoff: Option<SignoffPolicy>,
    state_repository: iq::control_domain::StateRepositorySnapshot,
}

#[derive(Debug, Serialize)]
struct ReconcileOutput {
    manager_id: String,
    repo_keys: Vec<String>,
    staged_directory: String,
    action: &'static str,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum InputSnapshotDigest {
    Present { sha256: String },
    Absent { sha256: String },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GeneratedFileDigest {
    path: String,
    sha256: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationManifest {
    version: u32,
    current_config: InputSnapshotDigest,
    current_manager_state: InputSnapshotDigest,
    files: Vec<GeneratedFileDigest>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Boundary {
    canonical_path: PathBuf,
    target: String,
}

impl Boundary {
    fn key(&self) -> String {
        format!("{}::{}", self.canonical_path.display(), self.target)
    }
}

fn reconcile_config(
    current_config_path: &Path,
    desired_inventory_path: &Path,
    current_manager_state_path: &Path,
    staged_directory: &Path,
    lock_path: &Path,
    bootstrap: bool,
    workspace_root: &Path,
) -> Result<()> {
    validate_reconcile_paths(
        current_config_path,
        desired_inventory_path,
        current_manager_state_path,
        staged_directory,
        lock_path,
    )?;
    let _lock = ReconcileLock::acquire(lock_path)?;

    let current_config_bytes = read_optional_bytes(current_config_path)?;
    let current_state_bytes = read_optional_bytes(current_manager_state_path)?;
    let inventory_bytes = std::fs::read(desired_inventory_path).with_context(|| {
        format!(
            "read desired app inventory {}",
            desired_inventory_path.display()
        )
    })?;
    let inventory: DesiredInventory =
        serde_json::from_slice(&inventory_bytes).with_context(|| {
            format!(
                "parse desired app inventory {}",
                desired_inventory_path.display()
            )
        })?;
    let manager_id = require_exact_nonblank(&inventory.manager_id, "desired inventory manager_id")?;

    let current_config = match current_config_bytes.as_deref() {
        Some(bytes) => parse_daemon_config(bytes, current_config_path)?,
        None => DaemonConfig { repos: Vec::new() },
    };

    let current_state = match current_state_bytes.as_deref() {
        Some(bytes) => serde_json::from_slice::<ManagerState>(bytes).with_context(|| {
            format!(
                "parse current manager state {}",
                current_manager_state_path.display()
            )
        })?,
        None if bootstrap => ManagerState {
            manager_id: manager_id.to_string(),
            boundaries: Vec::new(),
        },
        None => anyhow::bail!(
            "current manager state is missing; pass --bootstrap for first reconciliation"
        ),
    };
    validate_manager_state(&current_state, manager_id)?;

    let desired = normalize_desired_repositories(&inventory.repositories)?;
    let current = index_config_repositories(&current_config)?;
    let owned = index_manager_boundaries(&current_state)?;
    let workspace_root = if desired
        .iter()
        .any(|(boundary, _)| !current.contains_key(&boundary.key()))
    {
        Some(require_workspace_root(workspace_root)?)
    } else {
        None
    };

    for (boundary_key, owned_entry) in &owned {
        let current_repo = current.get(boundary_key).ok_or_else(|| {
            anyhow::anyhow!("owned manager boundary {boundary_key} is missing from current config")
        })?;
        if config_repo_key(current_repo, &config_boundary_for_matching(current_repo)?)?
            != owned_entry.repo_key
        {
            anyhow::bail!(
                "owned manager boundary {boundary_key} has repo_key {}, expected {}",
                config_repo_key(current_repo, &config_boundary_for_matching(current_repo)?)?,
                owned_entry.repo_key
            );
        }
    }

    let desired_by_key = desired
        .iter()
        .map(|(boundary, intent)| (boundary.key(), (boundary, intent)))
        .collect::<HashMap<_, _>>();
    let mut effective_repos = Vec::with_capacity(current_config.repos.len() + desired.len());
    let mut next_boundaries = Vec::with_capacity(desired.len());
    for repo in &current_config.repos {
        let boundary = config_boundary_for_matching(repo)?;
        let boundary_key = boundary.key();
        if let Some(managed) = owned.get(&boundary_key) {
            let desired_repo = desired_by_key.get(&boundary_key);
            match &managed.ownership {
                ManagerOwnership::Adopted {
                    original_validation,
                    last_applied_validation,
                } => {
                    let current_validation = validation_state_for_repo(repo)?;
                    if current_validation != *last_applied_validation {
                        anyhow::bail!("app-owned validation changed externally for {boundary_key}");
                    }
                    if let Some((_, intent)) = desired_repo {
                        let next_validation = validate_validation_config(intent)?;
                        effective_repos.push(apply_validation_state(
                            canonicalize_repo_path(repo.clone(), &boundary),
                            &next_validation,
                        )?);
                        next_boundaries.push(ManagerBoundary {
                            repo_path: boundary.canonical_path.clone(),
                            target: boundary.target.clone(),
                            repo_key: managed.repo_key.clone(),
                            ownership: ManagerOwnership::Adopted {
                                original_validation: original_validation.clone(),
                                last_applied_validation: next_validation,
                            },
                        });
                    } else {
                        // Adoption is reversible: restore original app-visible validation,
                        // retain all consumer-owned policy, then release ownership.
                        effective_repos.push(apply_validation_state(
                            canonicalize_repo_path(repo.clone(), &boundary),
                            original_validation,
                        )?);
                    }
                }
                ManagerOwnership::Created {
                    baseline,
                    last_applied_validation,
                } => {
                    if !matches_baseline(repo, baseline)? {
                        anyhow::bail!(
                            "manager-created boundary {boundary_key} policy changed externally; refusing destructive removal or overwrite"
                        );
                    }
                    let current_validation = validation_state_for_repo(repo)?;
                    if current_validation != *last_applied_validation {
                        anyhow::bail!("app-owned validation changed externally for {boundary_key}");
                    }
                    if let Some((_, intent)) = desired_repo {
                        let next_validation = validate_validation_config(intent)?;
                        effective_repos
                            .push(apply_validation_state(repo.clone(), &next_validation)?);
                        next_boundaries.push(ManagerBoundary {
                            repo_path: boundary.canonical_path.clone(),
                            target: boundary.target.clone(),
                            repo_key: managed.repo_key.clone(),
                            ownership: ManagerOwnership::Created {
                                baseline: baseline.clone(),
                                last_applied_validation: next_validation,
                            },
                        });
                    }
                }
            }
        } else if let Some((_, intent)) = desired_by_key.get(&boundary_key) {
            let original_validation = validation_state_for_repo(repo)?;
            let next_validation = validate_validation_config(intent)?;
            effective_repos.push(apply_validation_state(
                canonicalize_repo_path(repo.clone(), &boundary),
                &next_validation,
            )?);
            next_boundaries.push(ManagerBoundary {
                repo_path: boundary.canonical_path.clone(),
                target: boundary.target.clone(),
                repo_key: config_repo_key(repo, &boundary)?,
                ownership: ManagerOwnership::Adopted {
                    original_validation,
                    last_applied_validation: next_validation,
                },
            });
        } else {
            effective_repos.push(repo.clone());
        }
    }

    for (boundary, intent) in &desired {
        if current.contains_key(&boundary.key()) {
            continue;
        }
        let repo_key = default_repo_key(&boundary.canonical_path, &boundary.target)?;
        let repo = DaemonRepoConfig {
            repo_path: boundary.canonical_path.clone(),
            repo_key: Some(repo_key.clone()),
            target: Some(boundary.target.clone()),
            remote: Some("origin".into()),
            workspace_root: Some(stable_workspace_root(
                workspace_root
                    .as_deref()
                    .context("workspace root required for new repository boundary")?,
                boundary,
            )),
            validation: ValidationConfig::None,
            signoff: None,
            state_repository: iq::control_domain::StateRepositorySnapshot::Local,
        };
        let next_validation = validate_validation_config(intent)?;
        effective_repos.push(apply_validation_state(repo.clone(), &next_validation)?);
        next_boundaries.push(ManagerBoundary {
            repo_path: boundary.canonical_path.clone(),
            target: boundary.target.clone(),
            repo_key,
            ownership: ManagerOwnership::Created {
                baseline: Box::new(repository_baseline(&repo)?),
                last_applied_validation: next_validation,
            },
        });
    }

    let effective_config = DaemonConfig {
        repos: effective_repos,
    };
    let validated_effective = validate_daemon_config(&effective_config, false)?;
    let staged_config_bytes = serde_yaml::to_string(&effective_config)
        .context("serialize staged daemon config")?
        .into_bytes();

    validate_workspace_roots(&validated_effective)?;
    let staged_state = ManagerState {
        manager_id: manager_id.to_string(),
        boundaries: next_boundaries,
    };
    validate_manager_state(&staged_state, manager_id)?;
    let staged_state_bytes =
        serde_json::to_vec_pretty(&staged_state).context("serialize staged manager state")?;

    // Re-read source snapshots immediately before publishing. A host-side write while this
    // command was running must fail rather than silently replacing manual changes.
    if read_optional_bytes(current_config_path)?.as_deref() != current_config_bytes.as_deref()
        || read_optional_bytes(current_manager_state_path)?.as_deref()
            != current_state_bytes.as_deref()
    {
        anyhow::bail!("current config or manager state changed during reconciliation");
    }
    let action = if effective_config.repos.is_empty() {
        "stop"
    } else {
        "start"
    };
    let staged_action_bytes = format!("{action}\n").into_bytes();
    let manifest = GenerationManifest {
        version: 1,
        current_config: input_snapshot_digest(current_config_bytes.as_deref()),
        current_manager_state: input_snapshot_digest(current_state_bytes.as_deref()),
        files: vec![
            generated_file_digest("iq.yaml", &staged_config_bytes),
            generated_file_digest("iq-manager-state.json", &staged_state_bytes),
            generated_file_digest("action", &staged_action_bytes),
        ],
    };
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("serialize generation manifest")?;
    publish_generation(
        staged_directory,
        &staged_config_bytes,
        &staged_state_bytes,
        &staged_action_bytes,
        &manifest,
        &manifest_bytes,
    )?;
    print_json(&ReconcileOutput {
        manager_id: manager_id.to_string(),
        repo_keys: staged_state
            .boundaries
            .iter()
            .map(|entry| entry.repo_key.clone())
            .collect(),
        staged_directory: staged_directory.display().to_string(),
        action,
    })
}

fn input_snapshot_digest(bytes: Option<&[u8]>) -> InputSnapshotDigest {
    match bytes {
        Some(bytes) => InputSnapshotDigest::Present {
            sha256: sha256_hex(bytes),
        },
        None => InputSnapshotDigest::Absent {
            sha256: sha256_hex(b"iq-reconcile:absent-input:v1"),
        },
    }
}

fn generated_file_digest(path: &str, bytes: &[u8]) -> GeneratedFileDigest {
    GeneratedFileDigest {
        path: path.into(),
        sha256: sha256_hex(bytes),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_digest(&digest)
}

fn verify_generation(
    generation: &Path,
    current_config_path: &Path,
    current_manager_state_path: &Path,
    lock_path: &Path,
) -> Result<()> {
    validate_generation_verify_paths(
        generation,
        current_config_path,
        current_manager_state_path,
        lock_path,
    )?;
    let _lock = ReconcileLock::acquire(lock_path)?;
    if !generation.is_dir() {
        anyhow::bail!(
            "generation must be an existing directory: {}",
            generation.display()
        );
    }
    let manifest_bytes = std::fs::read(generation.join("manifest.json"))
        .with_context(|| format!("read generation manifest {}", generation.display()))?;
    let manifest: GenerationManifest =
        serde_json::from_slice(&manifest_bytes).context("parse generation manifest")?;
    validate_manifest(&manifest)?;
    verify_generation_files(generation, &manifest)?;
    let current_config = read_optional_bytes(current_config_path)?;
    if input_snapshot_digest(current_config.as_deref()) != manifest.current_config {
        anyhow::bail!("current config drifted from generation manifest base snapshot");
    }
    let current_manager_state = read_optional_bytes(current_manager_state_path)?;
    if input_snapshot_digest(current_manager_state.as_deref()) != manifest.current_manager_state {
        anyhow::bail!("current manager state drifted from generation manifest base snapshot");
    }
    print_json(&json!({"verified": true}))
}

fn normalize_desired_repositories(
    repositories: &[DesiredRepository],
) -> Result<Vec<(Boundary, ValidationConfig)>> {
    let mut boundaries = HashSet::new();
    let mut repo_keys = HashSet::new();
    repositories
        .iter()
        .map(|repository| {
            let path = canonical_repo_path(&repository.repo_path, "desired repository")?;
            let target = validate_target(&repository.target, "desired repository target")?;
            let boundary = Boundary {
                canonical_path: path,
                target: target.to_string(),
            };
            if !boundaries.insert(boundary.key()) {
                anyhow::bail!(
                    "desired inventory duplicates repository target {}",
                    boundary.key()
                );
            }
            let repo_key = default_repo_key(&boundary.canonical_path, &boundary.target)?;
            if !repo_keys.insert(repo_key.clone()) {
                anyhow::bail!("desired inventory duplicates repo_key {repo_key}");
            }
            Ok((
                boundary,
                validate_validation_config(&repository.validation)?,
            ))
        })
        .collect()
}

fn validate_validation_config(validation: &ValidationConfig) -> Result<ValidationConfig> {
    Ok(match validation {
        ValidationConfig::None => ValidationConfig::None,
        ValidationConfig::Command { command } => ValidationConfig::Command {
            command: require_exact_nonblank(command, "validation command")?.to_string(),
        },
    })
}

fn validation_state_for_repo(repo: &DaemonRepoConfig) -> Result<ValidationConfig> {
    validate_validation_config(&repo.validation)
}

fn apply_validation_state(
    mut repo: DaemonRepoConfig,
    state: &ValidationConfig,
) -> Result<DaemonRepoConfig> {
    repo.validation = validate_validation_config(state)?;
    Ok(repo)
}

fn canonicalize_repo_path(mut repo: DaemonRepoConfig, boundary: &Boundary) -> DaemonRepoConfig {
    repo.repo_path = boundary.canonical_path.clone();
    repo
}

fn config_repo_key(repo: &DaemonRepoConfig, boundary: &Boundary) -> Result<String> {
    match repo.repo_key.clone() {
        Some(repo_key) => Ok(repo_key),
        None => default_repo_key(&boundary.canonical_path, &boundary.target),
    }
}

fn repository_baseline(repo: &DaemonRepoConfig) -> Result<RepositoryBaseline> {
    Ok(RepositoryBaseline {
        repo_path: repo.repo_path.clone(),
        repo_key: match repo.repo_key.clone() {
            Some(repo_key) => repo_key,
            None => default_repo_key(&repo.repo_path, repo.target.as_deref().unwrap_or("main"))?,
        },
        target: repo.target.clone().unwrap_or_else(|| "main".into()),
        remote: repo.remote.clone(),
        workspace_root: repo.workspace_root.clone(),
        signoff: repo.signoff.clone(),
        state_repository: repo.state_repository.clone(),
    })
}

fn matches_baseline(repo: &DaemonRepoConfig, baseline: &RepositoryBaseline) -> Result<bool> {
    Ok(repo.repo_key.as_deref() == Some(baseline.repo_key.as_str())
        && repo.target.as_deref().unwrap_or("main") == baseline.target
        && repository_baseline(repo)? == *baseline)
}

fn validate_daemon_config(
    config: &DaemonConfig,
    require_nonempty: bool,
) -> Result<ValidatedDaemonConfig> {
    if require_nonempty && config.repos.is_empty() {
        anyhow::bail!("daemon config has no repos");
    }
    let mut repo_keys = HashSet::new();
    let mut boundaries = HashSet::new();
    let mut repos = Vec::with_capacity(config.repos.len());
    for repo in &config.repos {
        let canonical_repo_path = canonical_repo_path(&repo.repo_path, "configured repository")?;
        let target = configured_target(repo)?;
        let boundary = Boundary {
            canonical_path: canonical_repo_path.clone(),
            target: target.clone(),
        };
        let repo_key = match repo
            .repo_key
            .clone()
            .map(|key| require_exact_nonblank(&key, "repo_key").map(str::to_string))
            .transpose()?
        {
            Some(repo_key) => repo_key,
            None => default_repo_key(&canonical_repo_path, &target)?,
        };
        if repo_key.rsplit_once("::").map(|(_, scope)| scope) != Some(boundary.target.as_str()) {
            anyhow::bail!(
                "repo_key {repo_key} does not match configured target {}",
                boundary.target
            );
        }
        if !repo_keys.insert(repo_key.clone()) {
            anyhow::bail!("daemon config duplicates repo_key {repo_key}");
        }
        if !boundaries.insert(boundary.key()) {
            anyhow::bail!(
                "daemon config duplicates repository target {}",
                boundary.key()
            );
        }
        let remote = repo
            .remote
            .as_deref()
            .map(|value| require_exact_nonblank(value, "remote").map(str::to_string))
            .transpose()?
            .unwrap_or_else(|| "origin".into());
        if let Some(workspace_root) = &repo.workspace_root {
            if !workspace_root.is_absolute() {
                anyhow::bail!(
                    "workspace_root must be absolute: {}",
                    workspace_root.display()
                );
            }
            workspace_path_identity(workspace_root)?;
            if workspace_root.exists() && !workspace_root.is_dir() {
                anyhow::bail!(
                    "workspace_root must be a directory: {}",
                    workspace_root.display()
                );
            }
        }
        let validation = validate_validation_config(&repo.validation)?;
        if validation == ValidationConfig::None && repo.signoff.is_some() {
            anyhow::bail!("signoff requires validation mode command");
        }
        validate_signoff(repo.signoff.as_ref())?;
        repo.state_repository.clone().validate()?;
        repos.push(ValidatedDaemonRepo {
            repo: repo.clone(),
            canonical_repo_path,
            repo_key,
            target,
            remote,
            validation,
        });
    }
    Ok(ValidatedDaemonConfig { repos })
}

fn configured_target(repo: &DaemonRepoConfig) -> Result<String> {
    let target = repo.target.as_deref().unwrap_or("main");
    validate_target(target, "configured target")
}

fn validate_target(target: &str, label: &str) -> Result<String> {
    let target = require_exact_nonblank(target, label)?;
    let output = ProcessCommand::new("git")
        .args(["check-ref-format", "--branch", target])
        .output()
        .with_context(|| format!("validate {label} {target}"))?;
    if !output.status.success() {
        anyhow::bail!("{label} is not a valid Git branch name: {target}");
    }
    Ok(target.to_string())
}

fn validate_signoff(signoff: Option<&SignoffPolicy>) -> Result<()> {
    let Some(signoff) = signoff else {
        return Ok(());
    };
    require_exact_nonblank(&signoff.command, "signoff command")?;
    require_exact_nonblank(&signoff.repository, "signoff repository")?;
    require_exact_nonblank(&signoff.trusted_creator, "signoff trusted_creator")?;
    if signoff.required_contexts.is_empty() {
        anyhow::bail!("signoff required_contexts must not be empty");
    }
    for context in &signoff.required_contexts {
        require_exact_nonblank(context, "signoff required context")?;
    }
    Ok(())
}

fn index_config_repositories(config: &DaemonConfig) -> Result<HashMap<String, &DaemonRepoConfig>> {
    let mut indexed = HashMap::new();
    for repo in &config.repos {
        let boundary = config_boundary_for_matching(repo)?;
        if indexed.insert(boundary.key(), repo).is_some() {
            anyhow::bail!("daemon config contains duplicate repository boundary");
        }
    }
    Ok(indexed)
}

fn index_manager_boundaries(state: &ManagerState) -> Result<HashMap<String, ManagerBoundary>> {
    let mut indexed = HashMap::new();
    let mut repo_keys = HashSet::new();
    let mut workspace_roots = HashMap::<PathBuf, String>::new();
    for entry in &state.boundaries {
        let path = manager_state_repo_path(entry)?;
        let target = validate_target(&entry.target, "manager state target")?;
        let repo_key =
            require_exact_nonblank(&entry.repo_key, "manager state repo_key")?.to_string();
        if repo_key.rsplit_once("::").map(|(_, scope)| scope) != Some(target.as_str()) {
            anyhow::bail!("manager state repo_key {repo_key} does not match target {target}");
        }
        if !repo_keys.insert(repo_key.clone()) {
            anyhow::bail!("manager state duplicates repo_key {repo_key}");
        }
        let boundary = Boundary {
            canonical_path: path.clone(),
            target: target.clone(),
        };
        validate_manager_ownership(entry, &boundary, &repo_key)?;
        if let ManagerOwnership::Created { baseline, .. } = &entry.ownership {
            if let Some(workspace_root) = baseline.workspace_root.as_ref() {
                if !workspace_root.is_absolute() {
                    anyhow::bail!(
                        "manager-created workspace_root must be absolute: {}",
                        workspace_root.display()
                    );
                }
                if let Some(previous) = workspace_roots
                    .insert(workspace_path_identity(workspace_root)?, repo_key.clone())
                {
                    anyhow::bail!(
                        "manager boundaries {previous} and {repo_key} share workspace_root {}",
                        workspace_root.display()
                    );
                }
            }
        }
        if indexed
            .insert(
                boundary.key(),
                ManagerBoundary {
                    repo_path: path,
                    target: target.to_string(),
                    repo_key,
                    ownership: entry.ownership.clone(),
                },
            )
            .is_some()
        {
            anyhow::bail!(
                "manager state duplicates repository boundary {}",
                boundary.key()
            );
        }
    }
    Ok(indexed)
}

fn validate_manager_state(state: &ManagerState, expected_manager_id: &str) -> Result<()> {
    let manager_id = require_exact_nonblank(&state.manager_id, "manager state manager_id")?;
    if manager_id != expected_manager_id {
        anyhow::bail!(
            "manager mismatch: current manager state belongs to {manager_id}, desired inventory belongs to {expected_manager_id}"
        );
    }
    index_manager_boundaries(state)?;
    Ok(())
}

fn validate_manager_ownership(
    entry: &ManagerBoundary,
    boundary: &Boundary,
    repo_key: &str,
) -> Result<()> {
    let validate_validation = |state: &ValidationConfig, label: &str| -> Result<()> {
        if let ValidationConfig::Command { command } = state {
            require_exact_nonblank(command, label)?;
        }
        Ok(())
    };
    match &entry.ownership {
        ManagerOwnership::Adopted {
            original_validation,
            last_applied_validation,
        } => {
            validate_validation(original_validation, "original validation command")?;
            validate_validation(last_applied_validation, "last applied validation command")?;
        }
        ManagerOwnership::Created {
            baseline,
            last_applied_validation,
        } => {
            validate_validation(last_applied_validation, "last applied validation command")?;
            if baseline.repo_key != repo_key
                || baseline.repo_path != entry.repo_path
                || baseline.target != boundary.target
            {
                anyhow::bail!(
                    "manager-created baseline identity does not match boundary {repo_key}"
                );
            }
            require_exact_nonblank(&baseline.target, "manager-created baseline target")?;
        }
    }
    Ok(())
}

fn config_boundary_for_matching(repo: &DaemonRepoConfig) -> Result<Boundary> {
    let target = configured_target(repo)?;
    Ok(Boundary {
        canonical_path: canonical_repo_path_or_missing(&repo.repo_path, "configured repository")?,
        target,
    })
}

fn manager_state_repo_path(entry: &ManagerBoundary) -> Result<PathBuf> {
    match &entry.ownership {
        ManagerOwnership::Adopted { .. } => {
            canonical_repo_path(&entry.repo_path, "manager state repository")
        }
        ManagerOwnership::Created { .. } => {
            canonical_repo_path_or_missing(&entry.repo_path, "manager state repository")
        }
    }
}

fn canonical_repo_path(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        anyhow::bail!("{label} path must be absolute: {}", path.display());
    }
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve {label} {}", path.display()))?;
    require_utf8_path(&canonical, label)?;
    if !canonical.is_dir() {
        anyhow::bail!(
            "{label} path must be an existing directory: {}",
            path.display()
        );
    }
    Ok(canonical)
}

fn canonical_repo_path_or_missing(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        anyhow::bail!("{label} path must be absolute: {}", path.display());
    }
    match path.canonicalize() {
        Ok(canonical) if canonical.is_dir() => {
            require_utf8_path(&canonical, label)?;
            Ok(canonical)
        }
        Ok(_) => anyhow::bail!("{label} path must be a directory: {}", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            require_utf8_path(path, label)?;
            Ok(path.to_path_buf())
        }
        Err(error) => Err(error).with_context(|| format!("resolve {label} {}", path.display())),
    }
}

fn require_utf8_path(path: &Path, label: &str) -> Result<()> {
    if path.to_str().is_none() {
        anyhow::bail!("{label} canonical path is not valid UTF-8");
    }
    Ok(())
}

fn workspace_path_identity(path: &Path) -> Result<PathBuf> {
    reject_dot_aliases(path, "workspace root")?;
    if !path.is_absolute() {
        anyhow::bail!("workspace root must be absolute: {}", path.display());
    }
    if path.exists() {
        let canonical = path
            .canonicalize()
            .with_context(|| format!("resolve workspace root {}", path.display()));
        return canonical;
    }
    let mut missing = Vec::new();
    let mut cursor = path.to_path_buf();
    while !cursor.exists() {
        let name = cursor
            .file_name()
            .with_context(|| format!("workspace root has no file name {}", cursor.display()))?;
        missing.push(name.to_os_string());
        cursor = cursor
            .parent()
            .with_context(|| format!("workspace root has no existing ancestor {}", path.display()))?
            .to_path_buf();
    }
    let mut identity = cursor
        .canonicalize()
        .with_context(|| format!("resolve workspace root ancestor {}", cursor.display()))?;
    for component in missing.iter().rev() {
        identity.push(component);
    }
    Ok(identity)
}

fn reject_dot_aliases(path: &Path, label: &str) -> Result<()> {
    if path.components().any(|component| {
        matches!(
            component,
            std::path::Component::CurDir | std::path::Component::ParentDir
        )
    }) {
        anyhow::bail!(
            "{label} must not contain . or .. path components: {}",
            path.display()
        );
    }
    Ok(())
}

fn require_workspace_root(path: &Path) -> Result<PathBuf> {
    reject_dot_aliases(path, "workspace root")?;
    if !path.is_absolute() {
        anyhow::bail!("workspace root must be absolute: {}", path.display());
    }
    workspace_path_identity(path)?;
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve workspace root {}", path.display()))?;
    if !canonical.is_dir() {
        anyhow::bail!(
            "workspace root must be an existing directory: {}",
            path.display()
        );
    }
    Ok(canonical)
}

fn stable_workspace_root(root: &Path, boundary: &Boundary) -> PathBuf {
    let digest = Sha256::digest(boundary.key().as_bytes());
    root.join(format!("repo-{}", hex_digest(&digest)))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_workspace_roots(config: &ValidatedDaemonConfig) -> Result<()> {
    let mut owners = HashMap::<PathBuf, String>::new();
    for validated_repo in &config.repos {
        let key = validated_repo.repo_key.clone();
        let Some(workspace_root) = validated_repo.repo.workspace_root.as_ref() else {
            continue;
        };
        let workspace_root_identity = workspace_path_identity(workspace_root)?;
        if let Some(previous) = owners.insert(workspace_root_identity, key.clone()) {
            anyhow::bail!(
                "repository boundaries {previous} and {key} share workspace_root {}",
                workspace_root.display()
            );
        }
    }
    Ok(())
}

fn parse_daemon_config(bytes: &[u8], path: &Path) -> Result<DaemonConfig> {
    serde_yaml::from_slice(bytes).with_context(|| format!("parse daemon config {}", path.display()))
}

fn read_optional_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read input {}", path.display())),
    }
}

struct ReconcileLock {
    file: File,
}

impl ReconcileLock {
    fn acquire(path: &Path) -> Result<Self> {
        reject_lock_symlink(path)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("open reconciliation lock {}", path.display()))?;
        if !file.metadata()?.file_type().is_file() {
            anyhow::bail!(
                "reconciliation lock must be a regular file: {}",
                path.display()
            );
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("acquire reconciliation lock {}", path.display()));
        }
        Ok(Self { file })
    }
}

impl Drop for ReconcileLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

fn path_identity(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return path
            .canonicalize()
            .with_context(|| format!("resolve path identity {}", path.display()));
    }
    let parent = path
        .parent()
        .with_context(|| format!("path has no parent {}", path.display()))?
        .canonicalize()
        .with_context(|| format!("resolve path parent {}", path.display()))?;
    let name = path
        .file_name()
        .with_context(|| format!("path has no file name {}", path.display()))?;
    Ok(parent.join(name))
}

fn validate_reconcile_paths(
    current_config: &Path,
    desired_inventory: &Path,
    current_state: &Path,
    staged_directory: &Path,
    lock_path: &Path,
) -> Result<()> {
    reject_lock_symlink(lock_path)?;
    validate_path_collisions(&[
        ("current config", current_config),
        ("desired inventory", desired_inventory),
        ("current manager state", current_state),
        ("staged directory", staged_directory),
        ("reconciliation lock", lock_path),
    ])
}

fn validate_generation_verify_paths(
    generation: &Path,
    current_config: &Path,
    current_state: &Path,
    lock_path: &Path,
) -> Result<()> {
    reject_lock_symlink(lock_path)?;
    validate_path_collisions(&[
        ("generation", generation),
        ("current config", current_config),
        ("current manager state", current_state),
        ("reconciliation lock", lock_path),
    ])
}

fn validate_path_collisions(named: &[(&str, &Path)]) -> Result<()> {
    let mut identities = Vec::with_capacity(named.len());
    for (label, path) in named {
        identities.push((label, path_identity(path)?));
    }
    for (index, (left_label, left)) in identities.iter().enumerate() {
        for (right_label, right) in identities.iter().skip(index + 1) {
            if left == right {
                anyhow::bail!(
                    "{left_label} and {right_label} alias the same path {}",
                    left.display()
                );
            }
        }
    }
    Ok(())
}

fn reject_lock_symlink(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            anyhow::bail!(
                "reconciliation lock must not be a symlink: {}",
                path.display()
            )
        }
        Ok(metadata) if !metadata.file_type().is_file() => {
            anyhow::bail!(
                "reconciliation lock must be a regular file: {}",
                path.display()
            )
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect lock {}", path.display())),
    }
}

fn publish_generation(
    destination: &Path,
    config: &[u8],
    manager_state: &[u8],
    action: &[u8],
    manifest: &GenerationManifest,
    manifest_bytes: &[u8],
) -> Result<()> {
    if destination.exists() {
        return verify_existing_generation(destination, manifest);
    }
    let parent = destination
        .parent()
        .with_context(|| format!("staged directory has no parent {}", destination.display()))?;
    if !parent.is_dir() {
        anyhow::bail!(
            "staged directory parent is not a directory: {}",
            parent.display()
        );
    }
    let temporary = parent.join(format!(
        ".{}.tmp-{}",
        destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("generation"),
        Uuid::new_v4()
    ));
    std::fs::create_dir(&temporary)
        .with_context(|| format!("create generation directory {}", temporary.display()))?;
    let result = (|| -> Result<()> {
        write_generation_file(&temporary.join("iq.yaml"), config)?;
        write_generation_file(&temporary.join("iq-manager-state.json"), manager_state)?;
        write_generation_file(&temporary.join("action"), action)?;
        write_generation_file(&temporary.join("manifest.json"), manifest_bytes)?;
        File::open(&temporary)
            .with_context(|| format!("open generation directory {}", temporary.display()))?
            .sync_all()
            .with_context(|| format!("sync generation directory {}", temporary.display()))?;
        if destination.exists() {
            anyhow::bail!(
                "staged directory appeared during publication: {}",
                destination.display()
            );
        }
        std::fs::rename(&temporary, destination).with_context(|| {
            format!(
                "publish generation {} from {}",
                destination.display(),
                temporary.display()
            )
        })?;
        if let Err(error) = File::open(parent).and_then(|file| file.sync_all()) {
            eprintln!(
                "warning: generation published but parent durability sync failed for {}: {error}",
                parent.display()
            );
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&temporary);
    }
    result
}

fn verify_existing_generation(
    destination: &Path,
    expected_manifest: &GenerationManifest,
) -> Result<()> {
    validate_manifest(expected_manifest)?;
    if !destination.is_dir() {
        anyhow::bail!(
            "existing staged destination is not a directory: {}",
            destination.display()
        );
    }
    let expected_names = [
        "iq.yaml",
        "iq-manager-state.json",
        "action",
        "manifest.json",
    ];
    let actual_names = std::fs::read_dir(destination)
        .with_context(|| format!("read existing generation {}", destination.display()))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .with_context(|| format!("inspect existing generation {}", destination.display()))
        })
        .collect::<Result<Vec<_>>>()?;
    if actual_names.len() != expected_names.len()
        || expected_names.iter().any(|name| {
            !actual_names
                .iter()
                .any(|actual| actual == std::ffi::OsStr::new(name))
        })
    {
        anyhow::bail!("existing staged destination is not a complete generation");
    }
    let existing_manifest_bytes = std::fs::read(destination.join("manifest.json"))
        .context("read existing generation manifest")?;
    let existing_manifest: GenerationManifest = serde_json::from_slice(&existing_manifest_bytes)
        .context("parse existing generation manifest")?;
    if existing_manifest != *expected_manifest {
        anyhow::bail!(
            "existing staged generation manifest content does not match requested generation"
        );
    }
    verify_generation_files(destination, expected_manifest)
}

fn validate_manifest(manifest: &GenerationManifest) -> Result<()> {
    if manifest.version != 1 {
        anyhow::bail!(
            "unsupported generation manifest version {}",
            manifest.version
        );
    }
    let expected_paths = ["iq.yaml", "iq-manager-state.json", "action"];
    if manifest.files.len() != expected_paths.len()
        || expected_paths
            .iter()
            .any(|path| !manifest.files.iter().any(|file| file.path == *path))
    {
        anyhow::bail!("generation manifest has invalid generated file inventory");
    }
    let mut paths = HashSet::new();
    for file in &manifest.files {
        if !paths.insert(file.path.clone())
            || file.sha256.len() != 64
            || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            anyhow::bail!("generation manifest has invalid digest entry {}", file.path);
        }
    }
    for snapshot in [&manifest.current_config, &manifest.current_manager_state] {
        let digest = match snapshot {
            InputSnapshotDigest::Present { sha256 } | InputSnapshotDigest::Absent { sha256 } => {
                sha256
            }
        };
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            anyhow::bail!("generation manifest has invalid input snapshot digest");
        }
    }
    Ok(())
}

fn verify_generation_files(destination: &Path, manifest: &GenerationManifest) -> Result<()> {
    for file in &manifest.files {
        let bytes = std::fs::read(destination.join(&file.path))
            .with_context(|| format!("read existing generated file {}", file.path))?;
        if sha256_hex(&bytes) != file.sha256 {
            anyhow::bail!(
                "existing generated file digest does not match: {}",
                file.path
            );
        }
    }
    Ok(())
}

fn write_generation_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("create generation file {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write generation file {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync generation file {}", path.display()))
}

fn require_exact_nonblank<'a>(value: &'a str, field: &str) -> Result<&'a str> {
    if value.trim().is_empty() {
        anyhow::bail!("{field} must not be blank");
    }
    if value != value.trim() {
        anyhow::bail!("{field} must not have leading or trailing whitespace");
    }
    Ok(value)
}

fn run_daemon_config(
    db_path: PathBuf,
    config_path: &std::path::Path,
    system_config_path: &std::path::Path,
    owner: Option<String>,
    ready_file: Option<PathBuf>,
    once: bool,
    interval_seconds: u64,
) -> Result<()> {
    let system_config = iq::agent_config::SystemConfig::load(system_config_path)?;
    let config = read_daemon_config(config_path)?;
    let queue = SqliteQueue::open(&db_path)?;
    let control_store = iq::control_store::ControlStore::open(&db_path)?;
    control_store.reconcile_cancelled_runner_terminations(true)?;
    let notifications = iq::notifications::NotificationDispatcher::new(
        &db_path,
        system_config.notifications.clone(),
    );
    notifications.configure()?;
    let api = iq::control_api::ControlApiServer::bind(
        system_config.control_plane.clone(),
        control_store,
    )?;
    notifications.mark_started_unknown_after_restart()?;
    let startup_store = iq::control_store::ControlStore::open(&db_path)?;
    iq::state_repository::process_issue_reservation_outbox(&startup_store, 1000)?;
    if !once {
        std::thread::spawn(move || {
            if let Err(error) = api.serve() {
                eprintln!("IQ control API stopped: {error:#}");
            }
        });
    }
    let mut runners = Vec::new();
    for validated in config.repos {
        let ValidatedDaemonRepo {
            repo,
            canonical_repo_path,
            repo_key,
            target,
            remote,
            validation,
        } = validated;
        let registered = queue.repository_if_exists(&repo_key)?.is_some();
        if registered && (validation != ValidationConfig::None || repo.signoff.is_some()) {
            anyhow::bail!(
                "registered repository {repo_key} rejects daemon validation and signoff settings"
            );
        }
        let options = integrator_options_with_system_config(
            integrator_options(
                db_path.clone(),
                canonical_repo_path,
                Some(repo_key.clone()),
                &target,
                remote,
                repo.workspace_root,
                owner.clone(),
            )?,
            &system_config,
        );
        let policy = match validation {
            ValidationConfig::Command { command } => IntegrationPolicy::Validation {
                command,
                signoff: repo
                    .signoff
                    .map(HostSignoffPolicy::Required)
                    .unwrap_or(HostSignoffPolicy::None),
            },
            ValidationConfig::None => IntegrationPolicy::NoValidation,
        };
        let integrator = Integrator::new_with_policy(options, policy)?;
        runners.push((repo_key, integrator));
    }
    if let Some(path) = ready_file.as_deref() {
        write_ready_file(path)?;
    }
    loop {
        let mut results = Vec::new();
        for (repo_key, integrator) in &runners {
            let cycle = || -> Result<Option<QueueItem>> {
                let store = iq::control_store::ControlStore::open(&db_path)?;
                iq::state_repository::process_issue_reservation_outbox(&store, 1000)?;
                for effort in store.inbox(1000)? {
                    if let Err(error) =
                        iq::state_repository::ingest_answers(&store, &effort.item_id)
                    {
                        eprintln!(
                            "state repository answer ingestion failed for {}: {error:#}",
                            effort.item_id
                        );
                    }
                }
                for item_id in store.projection_items(1000)? {
                    if let Err(error) = iq::state_repository::project_item(&store, &item_id) {
                        eprintln!("state repository projection failed for {item_id}: {error:#}");
                    }
                }
                store.alert_exhausted_projection_debt(
                    system_config.notifications.projection_debt_alert_seconds,
                )?;
                let result = integrator.run_once()?;
                for item_id in store.projection_items(1000)? {
                    if let Err(error) = iq::state_repository::project_item(&store, &item_id) {
                        eprintln!("state repository projection failed for {item_id}: {error:#}");
                    }
                }
                while notifications.dispatch_once()? != 0 {}
                Ok(result)
            };
            match cycle() {
                Ok(result) => results.push(result),
                Err(error) if once => {
                    return Err(error).with_context(|| format!("run repo queue {repo_key}"));
                }
                Err(error) => {
                    eprintln!("repo queue {repo_key} cycle failed: {error:#}");
                    results.push(None);
                }
            }
        }
        if once {
            print_json(&results)?;
        }
        if let Some(path) = ready_file.as_deref() {
            write_ready_file(path)?;
        }
        if once {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(interval_seconds));
    }
    Ok(())
}

fn write_ready_file(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create daemon ready directory {}", parent.display()))?;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temporary, format!("{}\n", std::process::id()))
        .with_context(|| format!("write daemon ready file {}", temporary.display()))?;
    std::fs::rename(&temporary, path)
        .with_context(|| format!("publish daemon ready file {}", path.display()))
}

fn read_daemon_config(config_path: &std::path::Path) -> Result<ValidatedDaemonConfig> {
    let contents = std::fs::read_to_string(config_path)
        .with_context(|| format!("read daemon config {}", config_path.display()))?;
    let config: DaemonConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("parse daemon config {}", config_path.display()))?;
    validate_daemon_config(&config, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_validation_requires_explicit_none_or_command_mode() {
        let none = parse_daemon_config(
            b"repos:\n  - repo_path: /repo\n    validation:\n      mode: none\n",
            Path::new("iq.yaml"),
        )
        .unwrap();
        assert_eq!(none.repos[0].validation, ValidationConfig::None);

        let command = parse_daemon_config(
            b"repos:\n  - repo_path: /repo\n    validation:\n      mode: command\n      command: git diff --check\n",
            Path::new("iq.yaml"),
        )
        .unwrap();
        assert_eq!(
            command.repos[0].validation,
            ValidationConfig::Command {
                command: "git diff --check".into()
            }
        );

        for ambiguous in [
            b"repos:\n  - repo_path: /repo\n    validation:\n      mode: auto\n".as_slice(),
            b"repos:\n  - repo_path: /repo\n    validation_command: git diff --check\n".as_slice(),
            b"repos:\n  - repo_path: /repo\n".as_slice(),
        ] {
            assert!(parse_daemon_config(ambiguous, Path::new("iq.yaml")).is_err());
        }

        assert!(serde_json::from_slice::<DesiredInventory>(
            br#"{"manager_id":"manager","repositories":[{"repo_path":"/repo","target":"main","validation":{"mode":"auto"}}]}"#,
        )
        .is_err());
        assert!(serde_json::from_slice::<ManagerState>(
            br#"{"manager_id":"manager","boundaries":[{"repo_path":"/repo","target":"main","repo_key":"repo::main","ownership":{"kind":"adopted","original_validation":{"kind":"auto"},"last_applied_validation":{"kind":"auto"}}}]}"#,
        )
        .is_err());
    }
}
