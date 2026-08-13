use anyhow::{Context, Result};
use serde_json::json;

use crate::control_domain::{IntegrationEffortState, IssueVisibility, StateRepositorySnapshot};
use crate::control_store::{ControlStore, DurableEvent, IntegrationEffort};
use crate::issue_backends::{
    issue_adapter_for_provider, IssueProjection, IssueProvider, IssueRemoteAdapter, IssueSyncTarget,
};

pub trait StateRepository {
    fn verify(&self) -> Result<()>;
    fn project(
        &self,
        effort: &IntegrationEffort,
        events: &[DurableEvent],
        artifact: Option<&str>,
    ) -> Result<Option<RepositoryArtifact>>;
    fn close(&self, artifact: &RepositoryArtifact) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryArtifact {
    pub id: String,
    pub url: String,
}

pub struct LocalStateRepository;

impl StateRepository for LocalStateRepository {
    fn verify(&self) -> Result<()> {
        Ok(())
    }

    fn project(
        &self,
        _effort: &IntegrationEffort,
        _events: &[DurableEvent],
        _artifact: Option<&str>,
    ) -> Result<Option<RepositoryArtifact>> {
        Ok(None)
    }

    fn close(&self, _artifact: &RepositoryArtifact) -> Result<()> {
        anyhow::bail!("local state repository has no external artifact")
    }
}

pub struct IssueStateRepository {
    adapter: Box<dyn IssueRemoteAdapter>,
    repository: String,
    visibility: IssueVisibility,
}

impl StateRepository for IssueStateRepository {
    fn verify(&self) -> Result<()> {
        self.adapter.verify_destination(&self.repository)
    }

    fn project(
        &self,
        effort: &IntegrationEffort,
        events: &[DurableEvent],
        artifact: Option<&str>,
    ) -> Result<Option<RepositoryArtifact>> {
        let relevant = events
            .iter()
            .filter(|event| {
                self.visibility == IssueVisibility::Full
                    || event.alert
                    || matches!(
                        event.event_type.as_str(),
                        "answer_applied"
                            | "cycle_limit_retry_authorized"
                            | "infrastructure_retry_authorized"
                            | "provider_retry_authorized"
                            | "provider_reconciliation_resumed"
                            | "integrated"
                            | "cancelled"
                    )
            })
            .collect::<Vec<_>>();
        if self.visibility == IssueVisibility::Minimal
            && effort.state.blocker().is_none()
            && artifact.is_none()
        {
            return Ok(None);
        }
        if self.visibility == IssueVisibility::Minimal && relevant.is_empty() {
            return Ok(None);
        }
        let state = serde_json::to_string_pretty(&json!({
            "item_id": effort.item_id,
            "effort_id": effort.id,
            "attempt_id": effort.attempt_id,
            "target_sha": effort.target_sha,
            "source_sha": effort.source_sha,
            "failed_cycles": effort.failed_cycles,
            "state": effort.state,
        }))?;
        let comments = relevant
            .iter()
            .map(|event| {
                format!(
                    "<!-- iq:event:{} -->\n**{}**\n```json\n{}\n```",
                    event.id,
                    event.event_type,
                    serde_json::to_string_pretty(&event.payload).unwrap_or_else(|_| "null".into())
                )
            })
            .collect();
        let projection = IssueProjection {
            title: format!("IQ integration item {}", effort.item_id),
            labels: vec![
                "iq:queue".into(),
                format!("iq:status:{}", effort.state.name()),
            ],
            body: format!("<!-- iq:item:{} -->\n```json\n{state}\n```", effort.item_id),
            comments,
        };
        let result = self.adapter.sync_projection(
            &IssueSyncTarget {
                repo: self.repository.clone(),
                issue: artifact.map(str::to_string),
            },
            &projection,
        )?;
        Ok(Some(RepositoryArtifact {
            id: result.issue,
            url: result.url,
        }))
    }

    fn close(&self, artifact: &RepositoryArtifact) -> Result<()> {
        self.adapter.close(&IssueSyncTarget {
            repo: self.repository.clone(),
            issue: Some(artifact.id.clone()),
        })
    }
}

pub fn repository(snapshot: &StateRepositorySnapshot) -> Result<Box<dyn StateRepository>> {
    match snapshot {
        StateRepositorySnapshot::Local => Ok(Box::new(LocalStateRepository)),
        StateRepositorySnapshot::GithubIssue(issue) => Ok(Box::new(IssueStateRepository {
            adapter: issue_adapter_for_provider(IssueProvider::GitHub)?,
            repository: issue.repository.clone(),
            visibility: issue.visibility,
        })),
        StateRepositorySnapshot::GitlabIssue(issue) => Ok(Box::new(IssueStateRepository {
            adapter: issue_adapter_for_provider(IssueProvider::GitLab)?,
            repository: issue.repository.clone(),
            visibility: issue.visibility,
        })),
    }
}

pub fn project_item(store: &ControlStore, item_id: &str) -> Result<()> {
    let effort = store
        .effort_for_item(item_id)?
        .with_context(|| format!("item has no integration effort: {item_id}"))?;
    let stored = store.repository_artifact(&effort.id)?;
    let cursor = stored
        .as_ref()
        .map_or(0, |artifact| artifact.last_event_sequence);
    let events = store.effort_events_after(&effort.id, cursor, 10_000)?;
    let repository = repository(&effort.state_repository)?;
    let projection = (|| {
        repository.verify()?;
        let projected = repository.project(
            &effort,
            &events,
            stored
                .as_ref()
                .map(|artifact| artifact.artifact_id.as_str()),
        )?;
        if let Some(projected) = projected.as_ref() {
            let terminal = matches!(
                effort.state,
                IntegrationEffortState::Integrated(_) | IntegrationEffortState::Cancelled(_)
            );
            if terminal {
                repository.close(projected)?;
            }
        }
        Ok::<_, anyhow::Error>(projected)
    })();
    let projected = match projection {
        Ok(projected) => projected,
        Err(error) => {
            store.record_projection_debt(&effort.id, &error)?;
            return Err(error);
        }
    };
    if let Some(projected) = projected {
        let (provider, identity) = match &effort.state_repository {
            StateRepositorySnapshot::GithubIssue(issue) => ("github", issue.repository.as_str()),
            StateRepositorySnapshot::GitlabIssue(issue) => ("gitlab", issue.repository.as_str()),
            StateRepositorySnapshot::Local => unreachable!("local projection has no artifact"),
        };
        let closed = matches!(
            effort.state,
            IntegrationEffortState::Integrated(_) | IntegrationEffortState::Cancelled(_)
        );
        store.record_repository_projection(crate::control_store::RepositoryProjectionReceipt {
            effort_id: &effort.id,
            provider,
            repository: identity,
            artifact_id: &projected.id,
            artifact_url: &projected.url,
            last_event_sequence: events.last().map_or(cursor, |event| event.sequence),
            closed,
        })?;
    }
    Ok(())
}

pub fn reserve_full_issue(store: &ControlStore, item_id: &str) -> Result<()> {
    let snapshot = store.item_state_repository_binding(item_id)?;
    let (provider, issue, provider_name) = match &snapshot {
        StateRepositorySnapshot::GithubIssue(issue)
            if issue.visibility == IssueVisibility::Full =>
        {
            (IssueProvider::GitHub, issue, "github")
        }
        StateRepositorySnapshot::GitlabIssue(issue)
            if issue.visibility == IssueVisibility::Full =>
        {
            (IssueProvider::GitLab, issue, "gitlab")
        }
        _ => return Ok(()),
    };
    if store.item_repository_reservation(item_id)?.is_some() {
        return Ok(());
    }
    let adapter = issue_adapter_for_provider(provider)?;
    adapter.verify_destination(&issue.repository)?;
    let projection = IssueProjection {
        title: format!("IQ integration item {item_id}"),
        labels: vec!["iq:queue".into(), "iq:status:ready".into()],
        body: format!("<!-- iq:item:{item_id} -->\n```json\n{{\"item_id\":\"{item_id}\",\"state\":\"enqueued\"}}\n```"),
        comments: vec![format!("<!-- iq:event:enqueued-{item_id} -->\n**enqueued**")],
    };
    let artifact = adapter.sync_projection(
        &IssueSyncTarget {
            repo: issue.repository.clone(),
            issue: None,
        },
        &projection,
    )?;
    store.record_item_repository_reservation(
        item_id,
        provider_name,
        &issue.repository,
        &artifact.issue,
        &artifact.url,
    )
}

pub fn process_issue_reservation_outbox(store: &ControlStore, limit: u32) -> Result<usize> {
    let items = store.pending_issue_reservations(limit)?;
    let count = items.len();
    for item_id in items {
        reserve_full_issue(store, &item_id)?;
    }
    Ok(count)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderAnswer {
    version: u32,
    request_id: String,
    effort_id: String,
    attempt_id: String,
    cycle_id: String,
    target_sha: String,
    source_sha: String,
    candidate_sha: Option<String>,
    answer: String,
}

pub fn ingest_answers(
    store: &ControlStore,
    item_id: &str,
) -> Result<Vec<crate::control_store::AnswerDisposition>> {
    let effort = store
        .effort_for_item(item_id)?
        .with_context(|| format!("item has no integration effort: {item_id}"))?;
    let stored = store
        .repository_artifact(&effort.id)?
        .context("issue state repository has no projected artifact")?;
    let (provider, provider_name, repository_identity) = match &effort.state_repository {
        StateRepositorySnapshot::GithubIssue(issue) => {
            (IssueProvider::GitHub, "github", &issue.repository)
        }
        StateRepositorySnapshot::GitlabIssue(issue) => {
            (IssueProvider::GitLab, "gitlab", &issue.repository)
        }
        StateRepositorySnapshot::Local => return Ok(Vec::new()),
    };
    if stored.repository != *repository_identity {
        anyhow::bail!("stored issue artifact differs from effort repository snapshot");
    }
    let comments = issue_adapter_for_provider(provider)?.answer_comments(&IssueSyncTarget {
        repo: stored.repository.clone(),
        issue: Some(stored.artifact_id.clone()),
    })?;
    let mut dispositions = Vec::new();
    for comment in comments {
        if store.provider_comment_seen(
            provider_name,
            &stored.repository,
            &stored.artifact_id,
            &comment.id,
        )? {
            continue;
        }
        let Some(actor) = comment.actor else {
            store.record_provider_comment_receipt(
                &crate::control_store::ProviderCommentReceipt {
                    provider: provider_name,
                    repository: &stored.repository,
                    artifact_id: &stored.artifact_id,
                    comment_id: &comment.id,
                    effort_id: &effort.id,
                    actor: None,
                    body: &comment.body,
                    disposition: "malformed",
                },
            )?;
            dispositions.push(crate::control_store::AnswerDisposition::Malformed);
            continue;
        };
        let answer: ProviderAnswer = match serde_json::from_str(&comment.body) {
            Ok(answer) => answer,
            Err(_) => {
                store.record_provider_comment_receipt(
                    &crate::control_store::ProviderCommentReceipt {
                        provider: provider_name,
                        repository: &stored.repository,
                        artifact_id: &stored.artifact_id,
                        comment_id: &comment.id,
                        effort_id: &effort.id,
                        actor: Some(&actor),
                        body: &comment.body,
                        disposition: "malformed",
                    },
                )?;
                dispositions.push(crate::control_store::AnswerDisposition::Malformed);
                continue;
            }
        };
        if answer.version != 1 {
            store.record_provider_comment_receipt(
                &crate::control_store::ProviderCommentReceipt {
                    provider: provider_name,
                    repository: &stored.repository,
                    artifact_id: &stored.artifact_id,
                    comment_id: &comment.id,
                    effort_id: &effort.id,
                    actor: Some(&actor),
                    body: &comment.body,
                    disposition: "unknown",
                },
            )?;
            dispositions.push(crate::control_store::AnswerDisposition::Malformed);
            continue;
        }
        dispositions.push(store.answer_for_effort(
            &crate::control_store::AnswerCommand {
                external_id: crate::control_store::provider_comment_external_id(
                    provider_name,
                    &stored.repository,
                    &stored.artifact_id,
                    &comment.id,
                )?,
                request_id: answer.request_id,
                effort_id: answer.effort_id,
                attempt_id: answer.attempt_id,
                cycle_id: answer.cycle_id,
                target_sha: answer.target_sha,
                source_sha: answer.source_sha,
                candidate_sha: answer.candidate_sha,
                answer: answer.answer,
            },
            &effort.id,
            &crate::control_store::ResponderIdentity::Provider { actor },
            unsafe { libc::geteuid() },
        )?);
    }
    Ok(dispositions)
}
