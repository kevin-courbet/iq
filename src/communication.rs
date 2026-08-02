use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

use crate::core::QueueStatus;
use crate::issue_backends::{
    issue_adapter_for_provider, IssueBackendAdapter, IssueProvider, IssueRemoteAdapter,
    IssueSyncTarget, MarkdownIssueBackend,
};
use crate::sqlite::{
    CommunicationBinding, CommunicationResponseDisposition, Prompt, QueueEvent, QueueItem,
    SqliteQueue,
};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommunicationConfig {
    pub transports: Vec<TransportConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransportConfig {
    pub id: String,
    pub kind: String,
    pub settings: Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueTransportSettings {
    repository: String,
    allowed_responders: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionPublication {
    pub external_ref: Value,
    pub url: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionResponse {
    pub external_response_id: String,
    pub prompt_id: String,
    pub answer: String,
    pub actor: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DecisionResponseBatch {
    pub responses: Vec<DecisionResponse>,
    pub errors: Vec<String>,
}

pub trait DecisionTransport {
    fn id(&self) -> &str;
    fn kind(&self) -> &str;
    fn endpoint_fingerprint(&self) -> String;
    fn verify(&self) -> Result<()>;
    fn publish(
        &self,
        binding: &CommunicationBinding,
        item: &QueueItem,
        events: &[QueueEvent],
        prompts: &[Prompt],
        issue: Option<&str>,
    ) -> Result<DecisionPublication>;
    fn collect(&self, binding: &CommunicationBinding) -> Result<DecisionResponseBatch>;
    fn close(&self, binding: &CommunicationBinding) -> Result<()>;
    fn is_authorized(&self, actor: &str) -> bool;
}

struct IssueDecisionTransport {
    id: String,
    kind: String,
    provider: IssueProvider,
    repository: String,
    allowed_responders: HashSet<String>,
    adapter: Box<dyn IssueRemoteAdapter>,
}

impl DecisionTransport for IssueDecisionTransport {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn endpoint_fingerprint(&self) -> String {
        format!("{}:{}", self.kind, self.repository)
    }

    fn verify(&self) -> Result<()> {
        self.adapter.verify_destination(&self.repository)
    }

    fn publish(
        &self,
        binding: &CommunicationBinding,
        item: &QueueItem,
        events: &[QueueEvent],
        prompts: &[Prompt],
        issue: Option<&str>,
    ) -> Result<DecisionPublication> {
        let mut projection = MarkdownIssueBackend {
            provider: self.provider.clone(),
        }
        .project_item(item, events, prompts);
        projection.labels.clear();
        projection.body = format!("<!-- {} -->\n{}", binding.marker, projection.body);
        let result = self.adapter.sync_projection(
            &IssueSyncTarget {
                repo: self.repository.clone(),
                issue: issue.map(str::to_string),
            },
            &projection,
        )?;
        Ok(DecisionPublication {
            external_ref: json!({"issue": result.issue}),
            url: result.url,
        })
    }

    fn collect(&self, binding: &CommunicationBinding) -> Result<DecisionResponseBatch> {
        let issue = binding_issue(binding)?;
        let answers = self.adapter.ingest_prompt_answers(&IssueSyncTarget {
            repo: self.repository.clone(),
            issue: Some(issue.to_string()),
        })?;
        let mut batch = DecisionResponseBatch::default();
        for answer in answers {
            match (answer.external_response_id, answer.answered_by) {
                (Some(external_response_id), Some(actor)) => {
                    batch.responses.push(DecisionResponse {
                        external_response_id,
                        prompt_id: answer.prompt_id,
                        answer: answer.answer,
                        actor,
                    });
                }
                (None, _) => batch.errors.push(format!(
                    "prompt response {} is missing the provider's stable comment identity",
                    answer.prompt_id
                )),
                (_, None) => batch.errors.push(format!(
                    "prompt response {} is missing its provider actor identity",
                    answer.prompt_id
                )),
            }
        }
        Ok(batch)
    }

    fn close(&self, binding: &CommunicationBinding) -> Result<()> {
        let issue = binding_issue(binding)?;
        self.adapter.close(&IssueSyncTarget {
            repo: self.repository.clone(),
            issue: Some(issue.to_string()),
        })
    }

    fn is_authorized(&self, actor: &str) -> bool {
        self.allowed_responders
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(actor))
    }
}

fn binding_issue(binding: &CommunicationBinding) -> Result<&str> {
    binding
        .external_ref
        .as_ref()
        .and_then(|value| value.get("issue"))
        .and_then(Value::as_str)
        .context("communication binding has no issue reference")
}

pub fn build_transports(configs: &[TransportConfig]) -> Result<Vec<Box<dyn DecisionTransport>>> {
    let mut ids = HashSet::new();
    configs
        .iter()
        .map(|config| {
            let id = config.id.trim();
            if id.is_empty() || !ids.insert(id.to_string()) {
                anyhow::bail!("communication transport IDs must be unique and nonblank");
            }
            let settings: IssueTransportSettings = serde_json::from_value(config.settings.clone())
                .with_context(|| format!("parse {} settings for transport {id}", config.kind))?;
            if settings.repository.trim().is_empty() || settings.allowed_responders.is_empty() {
                anyhow::bail!(
                    "communication transport {id} requires repository and allowed_responders"
                );
            }
            if settings
                .allowed_responders
                .iter()
                .any(|actor| actor.trim().is_empty())
            {
                anyhow::bail!("communication transport {id} has a blank allowed responder");
            }
            let provider = match config.kind.as_str() {
                "github_issue" => IssueProvider::GitHub,
                "gitlab_issue" => IssueProvider::GitLab,
                other => anyhow::bail!("unsupported communication transport kind: {other}"),
            };
            Ok(Box::new(IssueDecisionTransport {
                id: id.to_string(),
                kind: config.kind.clone(),
                adapter: issue_adapter_for_provider(provider.clone())?,
                provider,
                repository: settings.repository,
                allowed_responders: settings.allowed_responders.into_iter().collect(),
            }) as Box<dyn DecisionTransport>)
        })
        .collect()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommunicationCycle {
    pub applied_responses: usize,
    pub errors: Vec<String>,
}

pub struct DecisionCommunicator {
    queue: SqliteQueue,
    repo_key: String,
    transports: Vec<Box<dyn DecisionTransport>>,
}

impl DecisionCommunicator {
    pub fn new(
        queue_db: &std::path::Path,
        repo_key: String,
        transports: Vec<Box<dyn DecisionTransport>>,
    ) -> Result<Self> {
        let queue = SqliteQueue::open(queue_db)?;
        let configured = transports
            .iter()
            .map(|transport| (transport.id(), transport))
            .collect::<HashMap<_, _>>();
        for binding in queue.communication_bindings(&repo_key)? {
            if binding.status == "closed" || binding.status == "retired" {
                continue;
            }
            let transport = configured
                .get(binding.transport_id.as_str())
                .with_context(|| {
                    format!(
                        "live communication binding {} requires removed transport {}",
                        binding.id, binding.transport_id
                    )
                })?;
            if binding.transport_kind != transport.kind()
                || binding.endpoint_fingerprint != transport.endpoint_fingerprint()
            {
                anyhow::bail!(
                    "communication transport {} changed identity while binding {} is live",
                    binding.transport_id,
                    binding.id
                );
            }
        }
        Ok(Self {
            queue,
            repo_key,
            transports,
        })
    }

    pub fn verify(&self) -> Result<()> {
        for transport in &self.transports {
            transport
                .verify()
                .with_context(|| format!("verify communication transport {}", transport.id()))?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<CommunicationCycle> {
        let items = self
            .queue
            .list_items()?
            .into_iter()
            .filter(|item| item.repo_key == self.repo_key)
            .map(|item| (item.id.clone(), item))
            .collect::<HashMap<_, _>>();
        let mut bindings = self
            .queue
            .communication_bindings(&self.repo_key)?
            .into_iter()
            .map(|binding| {
                (
                    (binding.item_id.clone(), binding.transport_id.clone()),
                    binding,
                )
            })
            .collect::<HashMap<_, _>>();
        let mut cycle = CommunicationCycle::default();

        for transport in &self.transports {
            for item in items.values() {
                let key = (item.id.clone(), transport.id().to_string());
                let prompts = self.queue.prompts_for_item(&item.id)?;
                let has_bounded_open_prompt = current_bounded_prompt(item, &prompts).is_some();
                let binding = if let Some(binding) = bindings.get(&key).cloned() {
                    binding
                } else if has_bounded_open_prompt {
                    let binding = self.queue.reserve_communication_binding(
                        &self.repo_key,
                        &item.id,
                        transport.id(),
                        transport.kind(),
                        &transport.endpoint_fingerprint(),
                    )?;
                    bindings.insert(key, binding.clone());
                    binding
                } else {
                    continue;
                };

                if let Err(error) =
                    self.sync_binding(transport.as_ref(), &binding, item, &prompts, &mut cycle)
                {
                    let message = format!(
                        "communication transport {} failed for item {}: {error:#}",
                        transport.id(),
                        item.id
                    );
                    self.queue
                        .record_communication_error(&binding.id, &message)?;
                    cycle.errors.push(message);
                }
            }
        }
        Ok(cycle)
    }

    fn sync_binding(
        &self,
        transport: &dyn DecisionTransport,
        binding: &CommunicationBinding,
        item: &QueueItem,
        prompts: &[Prompt],
        cycle: &mut CommunicationCycle,
    ) -> Result<()> {
        if binding.status == "closed" || binding.status == "retired" {
            return Ok(());
        }
        if binding.status == "pending_close" {
            transport.close(binding)?;
            self.queue
                .set_communication_binding_status(&binding.id, "closed")?;
            return Ok(());
        }
        if binding.status == "pending_create" && current_bounded_prompt(item, prompts).is_none() {
            self.queue.set_communication_binding_status(
                &binding.id,
                if is_terminal(item.status) {
                    "retired"
                } else {
                    "dormant"
                },
            )?;
            return Ok(());
        }
        if binding.status == "dormant" && is_terminal(item.status) {
            self.queue
                .set_communication_binding_status(&binding.id, "retired")?;
            return Ok(());
        }
        if binding.status == "dormant" && current_bounded_prompt(item, prompts).is_some() {
            self.queue
                .set_communication_binding_status(&binding.id, "pending_create")?;
        }

        let mut current = self
            .queue
            .communication_binding(&item.id, transport.id())?
            .context("communication binding disappeared during sync")?;
        if current.external_ref.is_none() {
            let events = self.queue.events(&item.id)?;
            let publication = transport.publish(&current, item, &events, prompts, None)?;
            self.queue.activate_communication_binding(
                &current.id,
                &publication.external_ref,
                &publication.url,
            )?;
            current = self
                .queue
                .communication_binding(&item.id, transport.id())?
                .context("communication binding disappeared after publication")?;
        }

        let responses = transport.collect(&current)?;
        if responses.errors.is_empty() {
            self.queue.clear_communication_error(&current.id)?;
        } else {
            let message = responses.errors.join("; ");
            self.queue
                .record_communication_error(&current.id, &message)?;
            cycle.errors.extend(responses.errors);
        }
        for response in responses.responses {
            let disposition = self.queue.apply_communication_response(
                &current.id,
                &response.external_response_id,
                &response.prompt_id,
                &response.answer,
                &response.actor,
                transport.is_authorized(&response.actor),
            )?;
            if disposition == CommunicationResponseDisposition::Applied {
                cycle.applied_responses += 1;
            }
        }

        let current_item = self.queue.get_item(&item.id)?;
        let current_events = self.queue.events(&item.id)?;
        let current_prompts = self.queue.prompts_for_item(&item.id)?;
        transport.publish(
            &current,
            &current_item,
            &current_events,
            &current_prompts,
            Some(binding_issue(&current)?),
        )?;
        if is_terminal(current_item.status) {
            self.queue
                .set_communication_binding_status(&current.id, "pending_close")?;
            transport.close(&current)?;
            self.queue
                .set_communication_binding_status(&current.id, "closed")?;
        }
        Ok(())
    }
}

fn current_bounded_prompt<'a>(item: &QueueItem, prompts: &'a [Prompt]) -> Option<&'a Prompt> {
    let current = item
        .validation_evidence
        .get("prompt_id")
        .and_then(Value::as_str)?;
    prompts.iter().find(|prompt| {
        prompt.id == current
            && prompt.status == "open"
            && prompt
                .options
                .iter()
                .any(|option| option.as_str() != "accept-current")
    })
}

fn is_terminal(status: QueueStatus) -> bool {
    matches!(status, QueueStatus::Integrated | QueueStatus::Cancelled)
}
