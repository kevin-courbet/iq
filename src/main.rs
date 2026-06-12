use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use iq::core::{BlockedPhase, BlockedReason, QueueStatus};
use iq::integrator::{Integrator, IntegratorOptions};
use iq::issue_backends::{
    issue_adapter_for_provider, IssueBackendAdapter, IssueProvider, IssueSyncTarget,
    MarkdownIssueBackend,
};
use iq::sqlite::{EnqueueRequest, SqliteQueue};
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "iq", about = "Threadmill integration queue")]
struct Cli {
    #[arg(long, global = true)]
    queue_db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
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
    },
    List,
    Claim {
        #[arg(long)]
        repo_key: String,
    },
    Transition {
        item: String,
        status: StatusArg,
    },
    Block {
        item: String,
        phase: PhaseArg,
        reason: ReasonArg,
        #[arg(long)]
        message: String,
    },
    Answer {
        prompt: String,
        #[arg(long)]
        answer: String,
        #[arg(long, default_value = "user")]
        answered_by: String,
    },
    Requeue {
        item: String,
        #[arg(long)]
        head: String,
    },
    Retry {
        item: String,
    },
    Events {
        item: String,
    },
    Integrate {
        #[arg(long)]
        next: bool,
        #[arg(long)]
        resume: Option<String>,
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
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    Issue {
        #[command(subcommand)]
        command: IssueCommand,
    },
    Daemon {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        repo_path: Option<PathBuf>,
        #[arg(long)]
        repo_key: Option<String>,
        #[arg(long, default_value = "origin")]
        remote: String,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        once: bool,
        #[arg(long, default_value_t = 5)]
        interval_seconds: u64,
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
    AcceptCurrent {
        item: String,
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

#[derive(Subcommand, Debug)]
enum IssueCommand {
    Sync {
        item: String,
        #[arg(long)]
        provider: IssueProviderArg,
        #[arg(long)]
        repo: String,
        #[arg(long)]
        issue: Option<String>,
    },
    IngestAnswers {
        #[arg(long)]
        provider: IssueProviderArg,
        #[arg(long)]
        repo: String,
        #[arg(long)]
        issue: String,
        #[arg(long)]
        best_effort: bool,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum StatusArg {
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

#[derive(Copy, Clone, Debug, ValueEnum)]
enum PhaseArg {
    Merging,
    Validating,
    Integrating,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ReasonArg {
    NeedsUserInput,
    NeedsAgentFix,
    Infra,
    Dependency,
    Credentials,
    Provider,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum IssueProviderArg {
    Github,
    Gitlab,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let db_path = cli.queue_db.unwrap_or_else(SqliteQueue::default_db_path);
    match cli.command {
        Command::Enqueue {
            repo_path,
            repo_key,
            source,
            target,
            head,
            pr_url,
            producer,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            let repo_key = repo_key.unwrap_or_else(|| default_repo_key(&repo_path, &target));
            let item = queue.enqueue(EnqueueRequest {
                repo_key,
                repo_path: repo_path.to_string_lossy().to_string(),
                source_branch: source,
                target_branch: target,
                current_head_sha: head,
                pr_url,
                producer_metadata: json!({ "producer": producer }),
            })?;
            print_json(&item)?;
        }
        Command::List => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.list_items()?)?;
        }
        Command::Claim { repo_key } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.claim_next_ready(&repo_key)?)?;
        }
        Command::Transition { item, status } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.transition_item(&item, status.into())?)?;
        }
        Command::Block {
            item,
            phase,
            reason,
            message,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            let prompt_id = queue.block_item(&item, phase.into(), reason.into(), &message)?;
            print_json(&json!({ "prompt_id": prompt_id }))?;
        }
        Command::Answer {
            prompt,
            answer,
            answered_by,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.answer_prompt(&prompt, &answer, &answered_by)?)?;
        }
        Command::Requeue { item, head } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.requeue_agent_fix(&item, &head)?)?;
        }
        Command::Retry { item } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.retry_blocked(&item)?)?;
        }
        Command::Events { item } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.events(&item)?)?;
        }
        Command::Integrate {
            next,
            resume,
            repo_path,
            repo_key,
            remote,
            workspace_root,
            owner,
        } => {
            if !next && resume.is_none() {
                anyhow::bail!("use --next or --resume <item>");
            }
            let target = "main";
            let integrator = Integrator::new(integrator_options(
                db_path,
                repo_path,
                repo_key,
                target,
                remote,
                workspace_root,
                owner,
            ))?;
            if let Some(item_id) = resume {
                print_json(&integrator.resume_item(&item_id)?)?;
            } else {
                print_json(&integrator.run_once()?)?;
            }
        }
        Command::Workspace { command } => match command {
            WorkspaceCommand::Status {
                repo_path,
                repo_key,
                remote,
                workspace_root,
                owner,
            } => {
                let integrator = Integrator::new(integrator_options(
                    db_path,
                    repo_path,
                    repo_key,
                    "main",
                    remote,
                    workspace_root,
                    owner,
                ))?;
                print_json(&integrator.workspace_status()?)?;
            }
            WorkspaceCommand::AcceptCurrent {
                item,
                repo_path,
                repo_key,
                remote,
                workspace_root,
                owner,
            } => {
                let integrator = Integrator::new(integrator_options(
                    db_path,
                    repo_path,
                    repo_key,
                    "main",
                    remote,
                    workspace_root,
                    owner,
                ))?;
                print_json(&integrator.accept_current_workspace(&item)?)?;
            }
            WorkspaceCommand::Reset {
                repo_path,
                repo_key,
                remote,
                workspace_root,
                owner,
            } => {
                let integrator = Integrator::new(integrator_options(
                    db_path,
                    repo_path,
                    repo_key,
                    "main",
                    remote,
                    workspace_root,
                    owner,
                ))?;
                print_json(&integrator.reset_workspaces()?)?;
            }
        },
        Command::Issue { command } => match command {
            IssueCommand::Sync {
                item,
                provider,
                repo,
                issue,
            } => {
                let queue = SqliteQueue::open(&db_path)?;
                let item_row = queue.get_item(&item)?;
                let events = queue.events(&item)?;
                let prompts = queue.prompts_for_item(&item)?;
                let provider = IssueProvider::from(provider);
                let projection = MarkdownIssueBackend {
                    provider: provider.clone(),
                }
                .project_item(&item_row, &events, &prompts);
                let result = issue_adapter_for_provider(provider)?
                    .sync_projection(&IssueSyncTarget { repo, issue }, &projection)?;
                print_json(&result)?;
            }
            IssueCommand::IngestAnswers {
                provider,
                repo,
                issue,
                best_effort,
            } => {
                let queue = SqliteQueue::open(&db_path)?;
                let answers = issue_adapter_for_provider(provider.into())?.ingest_prompt_answers(
                    &IssueSyncTarget {
                        repo,
                        issue: Some(issue),
                    },
                )?;
                let mut applied = Vec::new();
                let mut had_error = false;
                for answer in answers {
                    match queue.answer_prompt(
                        &answer.prompt_id,
                        &answer.answer,
                        answer.answered_by.as_deref().unwrap_or("issue-comment"),
                    ) {
                        Ok(item) => applied.push(json!({"prompt_id": answer.prompt_id, "item_id": item.id, "status": item.status})),
                        Err(error) => {
                            had_error = true;
                            applied.push(json!({"prompt_id": answer.prompt_id, "error": error.to_string()}));
                        }
                    }
                }
                print_json(&applied)?;
                if had_error && !best_effort {
                    anyhow::bail!("failed to apply one or more issue prompt answers");
                }
            }
        },
        Command::Daemon {
            config,
            repo_path,
            repo_key,
            remote,
            workspace_root,
            owner,
            once,
            interval_seconds,
        } => {
            if let Some(config_path) = config {
                run_daemon_config(db_path, &config_path, owner, once, interval_seconds)?;
                return Ok(());
            }
            let repo_path = repo_path.ok_or_else(|| {
                anyhow::anyhow!("--repo-path is required unless --config is used")
            })?;
            let target = "main";
            let integrator = Integrator::new(integrator_options(
                db_path,
                repo_path,
                repo_key,
                target,
                remote,
                workspace_root,
                owner,
            ))?;
            loop {
                let result = integrator.run_once()?;
                print_json(&result)?;
                if once {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_secs(interval_seconds));
            }
        }
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
) -> IntegratorOptions {
    let repo_key = repo_key.unwrap_or_else(|| default_repo_key(&repo_path, target));
    let workspace_root = workspace_root.unwrap_or_else(|| repo_path.join(".iq-workspaces"));
    IntegratorOptions {
        repo_key,
        repo_path,
        queue_db,
        owner_id: owner.unwrap_or_else(|| format!("iq-{}", std::process::id())),
        lease_ttl_seconds: 30,
        base_remote: remote,
        workspace_root,
    }
}

fn default_repo_key(repo_path: &std::path::Path, target: &str) -> String {
    format!(
        "{}::{target}",
        repo_path
            .canonicalize()
            .unwrap_or_else(|_| repo_path.to_path_buf())
            .display()
    )
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Debug, Deserialize)]
struct DaemonConfig {
    repos: Vec<DaemonRepoConfig>,
}

#[derive(Debug, Deserialize)]
struct DaemonRepoConfig {
    repo_path: PathBuf,
    repo_key: Option<String>,
    target: Option<String>,
    remote: Option<String>,
    workspace_root: Option<PathBuf>,
}

fn run_daemon_config(
    db_path: PathBuf,
    config_path: &std::path::Path,
    owner: Option<String>,
    once: bool,
    interval_seconds: u64,
) -> Result<()> {
    let contents = std::fs::read_to_string(config_path)?;
    let config: DaemonConfig = serde_yaml::from_str(&contents)?;
    if config.repos.is_empty() {
        anyhow::bail!("daemon config has no repos");
    }
    let mut integrators = Vec::new();
    for repo in config.repos {
        let target = repo.target.unwrap_or_else(|| "main".into());
        integrators.push(Integrator::new(integrator_options(
            db_path.clone(),
            repo.repo_path,
            repo.repo_key,
            &target,
            repo.remote.unwrap_or_else(|| "origin".into()),
            repo.workspace_root,
            owner.clone(),
        ))?);
    }
    loop {
        let mut results = Vec::new();
        for integrator in &integrators {
            results.push(integrator.run_once()?);
        }
        print_json(&results)?;
        if once {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(interval_seconds));
    }
    Ok(())
}

impl From<StatusArg> for QueueStatus {
    fn from(value: StatusArg) -> Self {
        match value {
            StatusArg::Ready => QueueStatus::Ready,
            StatusArg::Merging => QueueStatus::Merging,
            StatusArg::Merged => QueueStatus::Merged,
            StatusArg::Validating => QueueStatus::Validating,
            StatusArg::Validated => QueueStatus::Validated,
            StatusArg::Integrating => QueueStatus::Integrating,
            StatusArg::Integrated => QueueStatus::Integrated,
            StatusArg::Blocked => QueueStatus::Blocked,
            StatusArg::Cancelled => QueueStatus::Cancelled,
        }
    }
}

impl From<PhaseArg> for BlockedPhase {
    fn from(value: PhaseArg) -> Self {
        match value {
            PhaseArg::Merging => BlockedPhase::Merging,
            PhaseArg::Validating => BlockedPhase::Validating,
            PhaseArg::Integrating => BlockedPhase::Integrating,
        }
    }
}

impl From<ReasonArg> for BlockedReason {
    fn from(value: ReasonArg) -> Self {
        match value {
            ReasonArg::NeedsUserInput => BlockedReason::NeedsUserInput,
            ReasonArg::NeedsAgentFix => BlockedReason::NeedsAgentFix,
            ReasonArg::Infra => BlockedReason::Infra,
            ReasonArg::Dependency => BlockedReason::Dependency,
            ReasonArg::Credentials => BlockedReason::Credentials,
            ReasonArg::Provider => BlockedReason::Provider,
        }
    }
}

impl From<IssueProviderArg> for IssueProvider {
    fn from(value: IssueProviderArg) -> Self {
        match value {
            IssueProviderArg::Github => IssueProvider::GitHub,
            IssueProviderArg::Gitlab => IssueProvider::GitLab,
        }
    }
}
