use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use iq::communication::{build_transports, CommunicationConfig, DecisionCommunicator};
use iq::integrator::{
    validation_command, IntegrationPolicy, Integrator, IntegratorOptions, SignoffPolicy,
};
use iq::issue_backends::{
    issue_adapter_for_provider, IssueBackendAdapter, IssueProvider, IssueSyncTarget,
    MarkdownIssueBackend,
};
use iq::sqlite::{Attempt, EnqueueRequest, QueueItem, SqliteQueue};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::Command as ProcessCommand;

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
    Answer {
        prompt: String,
        #[arg(long)]
        answer: String,
        #[arg(long, default_value = "user")]
        answered_by: String,
    },
    Cancel {
        item: String,
    },
    Requeue {
        item: String,
        #[arg(long)]
        head: String,
        #[arg(long, default_value = "origin")]
        remote: String,
    },
    Retry {
        item: String,
    },
    Events {
        item: String,
    },
    Attempt {
        item: String,
    },
    Evidence {
        item: String,
        #[arg(long)]
        workspace_root: PathBuf,
        #[arg(long, value_enum, default_value_t = EvidencePhaseArg::All)]
        phase: EvidencePhaseArg,
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
        config: PathBuf,
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
    Requeue {
        item: String,
        #[arg(long)]
        head: String,
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
            remote,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            let repo_key = repo_key.unwrap_or_else(|| default_repo_key(&repo_path, &target));
            validate_branch_handoff(&repo_path, &remote, &source, &target, &head)?;
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
        Command::Answer {
            prompt,
            answer,
            answered_by,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.answer_prompt(&prompt, &answer, &answered_by)?)?;
        }
        Command::Cancel { item } => {
            let queue = SqliteQueue::open(&db_path)?;
            print_json(&queue.transition_item(&item, iq::core::QueueStatus::Cancelled)?)?;
        }
        Command::Requeue { item, head, remote } => {
            let queue = SqliteQueue::open(&db_path)?;
            let queued = queue.get_item(&item)?;
            validate_branch_handoff(
                std::path::Path::new(&queued.repo_path),
                &remote,
                &queued.source_branch,
                &queued.target_branch,
                &head,
            )?;
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
        Command::Attempt { item } => {
            let queue = SqliteQueue::open(&db_path)?;
            let queued = queue.get_item(&item)?;
            let attempt_id = queued
                .current_attempt_id
                .context("item has no current integration attempt")?;
            print_json(&queue.get_attempt(&attempt_id)?)?;
        }
        Command::Evidence {
            item,
            workspace_root,
            phase,
        } => {
            let queue = SqliteQueue::open(&db_path)?;
            let queued = queue.get_item(&item)?;
            print_json(&read_evidence(&queue, &queued, &workspace_root, phase)?)?;
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
            owner,
            ready_file,
            once,
            interval_seconds,
        } => {
            run_daemon_config(db_path, &config, owner, ready_file, once, interval_seconds)?;
        }
        Command::RemoteExec {
            repo_path,
            repo_key,
            target,
            remote,
            workspace_root,
        } => run_remote_exec(db_path, repo_path, repo_key, target, remote, workspace_root)?,
        Command::Doctor { config } => run_doctor(&config)?,
    }
    Ok(())
}

fn run_doctor(config_path: &std::path::Path) -> Result<()> {
    let config = read_daemon_config(config_path)?;
    let gh = std::env::var("IQ_GITHUB_CLI").unwrap_or_else(|_| "gh".into());
    let mut results = Vec::new();
    for repo in config.repos {
        if !repo.repo_path.is_absolute() || !repo.repo_path.is_dir() {
            anyhow::bail!(
                "IQ repo path must be an existing absolute directory: {}",
                repo.repo_path.display()
            );
        }
        let target = repo.target.as_deref().unwrap_or("main");
        let remote = repo.remote.as_deref().unwrap_or("origin");
        let output = ProcessCommand::new("git")
            .args([
                "ls-remote",
                "--heads",
                remote,
                &format!("refs/heads/{target}"),
            ])
            .current_dir(&repo.repo_path)
            .output()
            .with_context(|| format!("query {remote}/{target}"))?;
        if !output.status.success() || output.stdout.is_empty() {
            anyhow::bail!(
                "cannot resolve {remote}/{target} from {}: {}",
                repo.repo_path.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let validation = match repo.validation_command.clone() {
            Some(command) if !command.trim().is_empty() => command,
            Some(_) => anyhow::bail!("validation_command must not be blank"),
            None => validation_command(&repo.repo_path)?
                .context("no validation command configured or derivable")?,
        };
        if let Some(signoff) = &repo.signoff {
            if signoff.command.trim().is_empty()
                || signoff.repository.trim().is_empty()
                || signoff
                    .required_contexts
                    .iter()
                    .all(|value| value.trim().is_empty())
                || signoff.trusted_creator.trim().is_empty()
            {
                anyhow::bail!(
                    "signoff policy requires command, repository, required_contexts, and trusted_creator"
                );
            }
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
        let communication_transport_count = if let Some(communication) = &repo.communication {
            let transports = build_transports(&communication.transports)?;
            for transport in &transports {
                transport.verify().with_context(|| {
                    format!("verify communication transport {}", transport.id())
                })?;
            }
            transports.len()
        } else {
            0
        };
        results.push(json!({
            "repo_key": repo.repo_key.unwrap_or_else(|| default_repo_key(&repo.repo_path, target)),
            "repo_path": repo.repo_path,
            "target": target,
            "remote": remote,
            "validation_command": validation,
            "signoff_required": repo.signoff.is_some(),
            "communication_transports": communication_transport_count,
        }));
    }
    print_json(&results)
}

fn run_remote_exec(
    db_path: PathBuf,
    repo_path: PathBuf,
    repo_key: String,
    target: String,
    remote: String,
    workspace_root: PathBuf,
) -> Result<()> {
    let original = std::env::var("SSH_ORIGINAL_COMMAND")
        .context("remote-exec requires SSH_ORIGINAL_COMMAND")?;
    let args = shell_words::split(&original).context("parse SSH_ORIGINAL_COMMAND")?;
    let command = RemoteCli::try_parse_from(args).context("parse permitted remote IQ command")?;
    let queue = SqliteQueue::open(&db_path)?;
    match command.command {
        RemoteCommand::Enqueue {
            source,
            head,
            pr_url,
            producer,
        } => {
            validate_branch_handoff(&repo_path, &remote, &source, &target, &head)?;
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
        RemoteCommand::List => {
            let items = queue
                .list_items()?
                .into_iter()
                .filter(|item| item.repo_key == repo_key)
                .collect::<Vec<_>>();
            print_json(&items)?;
        }
        RemoteCommand::Events { item } => {
            require_remote_item(&queue, &item, &repo_key)?;
            print_json(&queue.events(&item)?)?;
        }
        RemoteCommand::Attempt { item } => {
            let queued = require_remote_item(&queue, &item, &repo_key)?;
            let attempt_id = queued
                .current_attempt_id
                .context("item has no current integration attempt")?;
            print_json(&queue.get_attempt(&attempt_id)?)?;
        }
        RemoteCommand::Evidence { item, phase } => {
            let queued = require_remote_item(&queue, &item, &repo_key)?;
            print_json(&read_evidence(&queue, &queued, &workspace_root, phase)?)?;
        }
        RemoteCommand::Requeue { item, head } => {
            let queued = require_remote_item(&queue, &item, &repo_key)?;
            validate_branch_handoff(
                &repo_path,
                &remote,
                &queued.source_branch,
                &queued.target_branch,
                &head,
            )?;
            print_json(&queue.requeue_agent_fix(&item, &head)?)?;
        }
        RemoteCommand::Retry { item } => {
            require_remote_item(&queue, &item, &repo_key)?;
            print_json(&queue.retry_blocked(&item)?)?;
        }
    }
    Ok(())
}

fn require_remote_item(
    queue: &SqliteQueue,
    item_id: &str,
    repo_key: &str,
) -> Result<iq::sqlite::QueueItem> {
    let item = queue.get_item(item_id)?;
    if item.repo_key != repo_key {
        anyhow::bail!(
            "item {item_id} belongs to repo queue {}, not {repo_key}",
            item.repo_key
        );
    }
    Ok(item)
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

fn read_evidence(
    queue: &SqliteQueue,
    item: &QueueItem,
    workspace_root: &std::path::Path,
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
        .map(|path| validated_evidence_dir(workspace_root, item, &attempt, &path))
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
    workspace_root: &std::path::Path,
    item: &QueueItem,
    attempt: &Attempt,
    path: &std::path::Path,
) -> Result<PathBuf> {
    let expected = workspace_root
        .join(".evidence")
        .join(&item.id)
        .join(&attempt.id);
    let attempt_dir = path
        .parent()
        .context("evidence path has no attempt directory")?;
    let item_dir = attempt_dir
        .parent()
        .context("evidence path has no item directory")?;
    let evidence_root = item_dir
        .parent()
        .context("evidence path has no evidence root")?;
    if attempt_dir != expected
        || attempt_dir.file_name() != Some(std::ffi::OsStr::new(&attempt.id))
        || item_dir.file_name() != Some(std::ffi::OsStr::new(&item.id))
        || evidence_root.file_name() != Some(std::ffi::OsStr::new(".evidence"))
    {
        anyhow::bail!("attempt evidence path is outside its queue-owned evidence directory");
    }
    for component in [evidence_root, item_dir, attempt_dir] {
        let metadata = std::fs::symlink_metadata(component)
            .with_context(|| format!("inspect evidence path {}", component.display()))?;
        if metadata.file_type().is_symlink() {
            anyhow::bail!("attempt evidence path contains a symlink");
        }
    }
    let canonical_workspace = workspace_root
        .canonicalize()
        .with_context(|| format!("resolve workspace root {}", workspace_root.display()))?;
    let canonical_evidence_root = evidence_root
        .canonicalize()
        .with_context(|| format!("resolve evidence root {}", evidence_root.display()))?;
    if canonical_evidence_root.parent() != Some(canonical_workspace.as_path()) {
        anyhow::bail!("attempt evidence root is outside configured workspace root");
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
    match std::fs::metadata(path) {
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
    let mut file = std::fs::File::open(&canonical_path)
        .with_context(|| format!("open evidence file {}", path.display()))?;
    let length = file.metadata()?.len();
    let truncated = length > MAX_EVIDENCE_BYTES;
    let mut bytes = Vec::new();
    if truncated {
        let half = MAX_EVIDENCE_BYTES / 2;
        file.by_ref().take(half).read_to_end(&mut bytes)?;
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonConfig {
    repos: Vec<DaemonRepoConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonRepoConfig {
    repo_path: PathBuf,
    repo_key: Option<String>,
    target: Option<String>,
    remote: Option<String>,
    workspace_root: Option<PathBuf>,
    validation_command: Option<String>,
    signoff: Option<SignoffPolicy>,
    communication: Option<CommunicationConfig>,
}

fn run_daemon_config(
    db_path: PathBuf,
    config_path: &std::path::Path,
    owner: Option<String>,
    ready_file: Option<PathBuf>,
    once: bool,
    interval_seconds: u64,
) -> Result<()> {
    let config = read_daemon_config(config_path)?;
    let mut runners = Vec::new();
    for repo in config.repos {
        let target = repo.target.unwrap_or_else(|| "main".into());
        let validation_command = match repo.validation_command {
            Some(command) if !command.trim().is_empty() => command,
            Some(_) => anyhow::bail!("validation_command must not be blank"),
            None => validation_command(&repo.repo_path)?
                .context("daemon repository has no configured or derivable validation command")?,
        };
        let repo_key = repo
            .repo_key
            .clone()
            .unwrap_or_else(|| default_repo_key(&repo.repo_path, &target));
        let options = integrator_options(
            db_path.clone(),
            repo.repo_path,
            Some(repo_key.clone()),
            &target,
            repo.remote.unwrap_or_else(|| "origin".into()),
            repo.workspace_root,
            owner.clone(),
        );
        let integrator = Integrator::new_with_policy(
            options,
            IntegrationPolicy {
                validation_command: Some(validation_command),
                signoff: repo.signoff,
            },
        )?;
        let communicator = repo
            .communication
            .map(|communication| {
                DecisionCommunicator::new(
                    &db_path,
                    repo_key,
                    build_transports(&communication.transports)?,
                )
            })
            .transpose()?;
        runners.push((integrator, communicator));
    }
    for (_, communicator) in &runners {
        if let Some(communicator) = communicator {
            communicator.verify()?;
        }
    }
    if let Some(path) = ready_file.as_deref() {
        write_ready_file(path)?;
    }
    loop {
        let mut results = Vec::new();
        for (integrator, communicator) in &runners {
            let mut result = integrator.run_once()?;
            if let Some(communicator) = communicator {
                if let Some(cycle) = integrator.with_repo_lease(|| communicator.sync())? {
                    for error in cycle.errors {
                        eprintln!("{error}");
                    }
                    if cycle.applied_responses > 0 {
                        result = integrator.run_once()?;
                        if let Some(followup) =
                            integrator.with_repo_lease(|| communicator.sync())?
                        {
                            for error in followup.errors {
                                eprintln!("{error}");
                            }
                        }
                    }
                }
            }
            results.push(result);
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

fn read_daemon_config(config_path: &std::path::Path) -> Result<DaemonConfig> {
    let contents = std::fs::read_to_string(config_path)
        .with_context(|| format!("read daemon config {}", config_path.display()))?;
    let config: DaemonConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("parse daemon config {}", config_path.display()))?;
    if config.repos.is_empty() {
        anyhow::bail!("daemon config has no repos");
    }
    let mut repo_keys = HashSet::new();
    let mut boundaries = HashSet::new();
    for repo in &config.repos {
        let target = repo.target.as_deref().unwrap_or("main");
        let canonical = repo.repo_path.canonicalize().with_context(|| {
            format!("resolve configured repository {}", repo.repo_path.display())
        })?;
        let repo_key = repo
            .repo_key
            .clone()
            .unwrap_or_else(|| default_repo_key(&canonical, target));
        if repo_key.rsplit_once("::").map(|(_, scope)| scope) != Some(target) {
            anyhow::bail!("repo_key {repo_key} does not match configured target {target}");
        }
        if !repo_keys.insert(repo_key.clone()) {
            anyhow::bail!("daemon config duplicates repo_key {repo_key}");
        }
        let boundary = format!("{}::{target}", canonical.display());
        if !boundaries.insert(boundary.clone()) {
            anyhow::bail!("daemon config duplicates repository target {boundary}");
        }
    }
    Ok(config)
}

impl From<IssueProviderArg> for IssueProvider {
    fn from(value: IssueProviderArg) -> Self {
        match value {
            IssueProviderArg::Github => IssueProvider::GitHub,
            IssueProviderArg::Gitlab => IssueProvider::GitLab,
        }
    }
}
