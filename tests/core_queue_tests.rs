use iq::core::{BlockedPhase, BlockedReason, BlockedState, QueueStatus, StateMachine};
use iq::issue_backends::{
    issue_adapter_for_provider, IssueBackendAdapter, IssueProvider, IssueSyncTarget,
    MarkdownIssueBackend,
};
use iq::providers::{provider_for_url, ProviderGate};
use iq::sqlite::{EnqueueRequest, SqliteQueue};
use rusqlite::{params, Connection};
use std::fs;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn state_machine_rejects_invalid_transition_and_resumes_by_block_reason() {
    let machine = StateMachine;

    assert!(machine
        .transition(QueueStatus::Ready, QueueStatus::Validated)
        .is_err());
    assert!(machine
        .transition(QueueStatus::Ready, QueueStatus::Merging)
        .is_ok());
    assert!(machine
        .transition(QueueStatus::Merging, QueueStatus::Blocked)
        .is_ok());
    assert!(machine
        .transition(QueueStatus::Blocked, QueueStatus::Merging)
        .is_err());
    assert!(machine
        .transition(QueueStatus::Blocked, QueueStatus::Ready)
        .is_err());
    assert_eq!(
        machine
            .resume_target(&BlockedState {
                phase: BlockedPhase::Merging,
                reason: BlockedReason::NeedsUserInput,
                prompt_id: Some("prompt-1".into()),
            })
            .unwrap(),
        QueueStatus::Merging
    );
    assert!(machine
        .resume_target(&BlockedState {
            phase: BlockedPhase::Merging,
            reason: BlockedReason::NeedsAgentFix,
            prompt_id: None,
        })
        .is_err());
}

#[test]
fn sqlite_enqueue_is_idempotent_but_rejects_active_head_changes() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();

    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    let repeated = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    let changed = queue.enqueue(EnqueueRequest {
        repo_key: "repo::main".into(),
        repo_path: "/repo".into(),
        source_branch: "agent/one".into(),
        target_branch: "main".into(),
        current_head_sha: "222".into(),
        pr_url: None,
        producer_metadata: serde_json::json!({"worker":"W001","attempt":2}),
    });

    assert_eq!(first.id, repeated.id);
    assert!(changed.is_err());
    assert_eq!(queue.get_item(&first.id).unwrap().current_head_sha, "111");
    assert_eq!(queue.list_items().unwrap().len(), 1);
}

#[test]
fn blocked_user_prompt_answer_resumes_phase_but_agent_fix_requires_requeue() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    let item = queue
        .transition_item(&item.id, QueueStatus::Merging)
        .unwrap();

    let prompt_id = queue
        .block_item(
            &item.id,
            BlockedPhase::Merging,
            BlockedReason::NeedsUserInput,
            "resolve conflict",
        )
        .unwrap();
    let generic_resume = queue.transition_item(&item.id, QueueStatus::Merging);
    assert!(generic_resume.is_err());
    assert_eq!(
        queue.get_item(&item.id).unwrap().status,
        QueueStatus::Blocked
    );
    assert!(queue.retry_blocked(&item.id).is_err());
    let resumed = queue
        .answer_prompt(&prompt_id, "use source", "user")
        .unwrap();
    assert_eq!(resumed.status, QueueStatus::Merging);
    let validating = queue
        .transition_item(&item.id, QueueStatus::Merged)
        .unwrap();
    let validating = queue
        .transition_item(&validating.id, QueueStatus::Validating)
        .unwrap();

    queue
        .block_item(
            &validating.id,
            BlockedPhase::Validating,
            BlockedReason::NeedsAgentFix,
            "validation failed",
        )
        .unwrap();
    let still_blocked = queue.answer_prompt(&prompt_id, "ignored duplicate", "user");
    assert!(still_blocked.is_err());
    let generic_requeue = queue.transition_item(&item.id, QueueStatus::Ready);
    assert!(generic_requeue.is_err());
    assert_eq!(
        queue.get_item(&item.id).unwrap().status,
        QueueStatus::Blocked
    );
    assert!(queue.retry_blocked(&item.id).is_err());
    let ready = queue.requeue_agent_fix(&item.id, "333").unwrap();
    assert_eq!(ready.status, QueueStatus::Ready);
    assert_eq!(ready.current_head_sha, "333");
}

#[test]
fn retrying_infrastructure_block_uses_typed_operation() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    queue
        .transition_item(&item.id, QueueStatus::Merging)
        .unwrap();
    queue
        .block_item(
            &item.id,
            BlockedPhase::Merging,
            BlockedReason::Infra,
            "workspace unavailable",
        )
        .unwrap();

    assert!(queue
        .transition_item(&item.id, QueueStatus::Merging)
        .is_err());
    let retrying = queue.retry_blocked(&item.id).unwrap();

    assert_eq!(retrying.status, QueueStatus::Merging);
    assert_eq!(retrying.blocked_reason, None);
    assert_eq!(retrying.blocked_phase, None);
}

#[test]
fn sqlite_read_fails_on_corrupt_persisted_json_fields() {
    let temp = tempdir().unwrap();
    let db = temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    queue
        .set_conflict_metadata(
            &item.id,
            &serde_json::json!({"files":["a.txt"]}),
            "target",
            "source",
        )
        .unwrap();
    let conn = Connection::open(&db).unwrap();

    conn.execute(
        "UPDATE queue_items SET validation_evidence_json=?1 WHERE id=?2",
        params!["{not-json", item.id],
    )
    .unwrap();
    let error = format!("{:#}", queue.get_item(&item.id).unwrap_err());
    assert!(error.contains("validation_evidence_json"), "{error}");

    conn.execute(
        "UPDATE queue_items SET validation_evidence_json='{}', producer_metadata_json=?1 WHERE id=?2",
        params!["{not-json", item.id],
    )
    .unwrap();
    let error = format!("{:#}", queue.get_item(&item.id).unwrap_err());
    assert!(error.contains("producer_metadata_json"), "{error}");

    conn.execute(
        "UPDATE queue_items SET producer_metadata_json='{}', conflict_json=?1 WHERE id=?2",
        params!["{not-json", item.id],
    )
    .unwrap();
    let error = format!("{:#}", queue.get_item(&item.id).unwrap_err());
    assert!(error.contains("conflict_json"), "{error}");
}

#[test]
fn sqlite_repo_lease_prevents_concurrent_integrators_until_expiry() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();

    assert!(queue
        .acquire_repo_lease("repo::main", "owner-a", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("repo::main", "owner-b", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("repo::main", "owner-a", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("expired::main", "owner-a", -1)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("expired::main", "owner-b", 60)
        .unwrap());
}

#[test]
fn sqlite_repo_lease_owner_check_rejects_stale_integrator() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();

    assert!(queue
        .acquire_repo_lease("repo::main", "owner-a", -1)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("repo::main", "owner-b", 60)
        .unwrap());

    assert!(!queue
        .ensure_repo_lease_owner("repo::main", "owner-a", 60)
        .unwrap());
    assert!(queue
        .ensure_repo_lease_owner("repo::main", "owner-b", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("repo::main", "owner-a", 60)
        .unwrap());

    assert!(queue
        .acquire_repo_lease("renewed::main", "owner-a", -1)
        .unwrap());
    assert!(!queue
        .ensure_repo_lease_owner("renewed::main", "owner-a", 60)
        .unwrap());
    assert!(queue
        .acquire_repo_lease("renewed::main", "owner-a", 60)
        .unwrap());
    assert!(!queue
        .acquire_repo_lease("renewed::main", "owner-b", 60)
        .unwrap());
}

#[test]
fn claim_next_ready_holds_queue_behind_oldest_active_item() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    let repo_key = "repo::main";

    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    let second = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: "/repo".into(),
            source_branch: "agent/two".into(),
            target_branch: "main".into(),
            current_head_sha: "222".into(),
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
        })
        .unwrap();

    let (claimed, _) = queue.claim_next_ready(repo_key).unwrap().unwrap();
    assert_eq!(claimed.id, first.id);
    queue
        .block_item(
            &first.id,
            BlockedPhase::Merging,
            BlockedReason::NeedsUserInput,
            "resolve first before later work starts",
        )
        .unwrap();

    assert!(queue.claim_next_ready(repo_key).unwrap().is_none());
    assert_eq!(
        queue.get_item(&second.id).unwrap().status,
        QueueStatus::Ready
    );
}

#[test]
fn issue_backend_projection_carries_item_state_events_and_prompts() {
    let temp = tempdir().unwrap();
    let queue = SqliteQueue::open(&temp.path().join("queues.db")).unwrap();
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: "repo::main".into(),
            repo_path: "/repo".into(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: "111".into(),
            pr_url: Some("https://github.com/org/repo/pull/7".into()),
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    queue
        .transition_item(&item.id, QueueStatus::Merging)
        .unwrap();
    let prompt_id = queue
        .block_item(
            &item.id,
            BlockedPhase::Merging,
            BlockedReason::NeedsUserInput,
            "resolve conflict",
        )
        .unwrap();
    let prompt = queue.get_prompt(&prompt_id).unwrap();
    let item = queue.get_item(&item.id).unwrap();
    let events = queue.events(&item.id).unwrap();

    let projection = MarkdownIssueBackend {
        provider: IssueProvider::GitHub,
    }
    .project_item(&item, &events, &[prompt]);

    assert!(projection.labels.contains(&"iq:status:blocked".into()));
    assert!(projection
        .labels
        .contains(&"iq:blocked:needs_user_input".into()));
    assert!(projection.body.contains("<!-- iq:item:"));
    assert!(projection.body.contains("agent/one"));
    assert!(projection
        .comments
        .iter()
        .any(|comment| comment.contains("<!-- iq:prompt:")));
    assert!(projection
        .comments
        .iter()
        .any(|comment| comment.contains("resolve conflict")));
}

#[test]
fn github_provider_adapter_maps_cli_checks_to_gate_state() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-gh");
    fs::write(
        &fake,
        "#!/bin/sh\nif [ \"$1 $2\" = \"pr view\" ]; then\n  printf '%s' '{\"headRefOid\":\"head1\",\"baseRefOid\":\"base1\",\"reviewDecision\":\"APPROVED\",\"mergeStateStatus\":\"CLEAN\",\"statusCheckRollup\":[{\"status\":\"COMPLETED\",\"conclusion\":\"SUCCESS\"}]}'\nelse\n  exit 0\nfi\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake);

    let provider = provider_for_url("https://github.com/org/repo/pull/7").unwrap();
    let snapshot = provider
        .snapshot("https://github.com/org/repo/pull/7")
        .unwrap();

    assert_eq!(snapshot.head_sha, "head1");
    assert_eq!(snapshot.base_sha, "base1");
    assert_eq!(snapshot.gate, ProviderGate::Pass);
    std::env::remove_var("IQ_GITHUB_CLI");
}

#[test]
fn github_issue_backend_syncs_projection_and_ingests_prompt_answers() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-gh");
    let log = temp.path().join("gh.log");
    fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> {log}
if [ "$1 $2" = "issue create" ]; then
  printf '%s' '{{"number":42,"url":"https://github.com/org/repo/issues/42"}}'
  exit 0
fi
if [ "$1 $2" = "issue view" ]; then
  printf '%s' '{{"comments":[{{"body":"Looks good\n\n```\niq answer prompt-1 use source\n```","author":{{"login":"octo"}}}}]}}'
  exit 0
fi
exit 0
"#,
            log = log.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake);

    let projection = iq::issue_backends::IssueProjection {
        title: "Integration queue: agent/one → main".into(),
        labels: vec!["iq:queue".into(), "iq:status:blocked".into()],
        body: "<!-- iq:item:item-1 -->\nbody".into(),
        comments: vec!["<!-- iq:prompt:prompt-1 -->\nPrompt".into()],
    };
    let adapter = issue_adapter_for_provider(IssueProvider::GitHub).unwrap();
    let synced = adapter
        .sync_projection(
            &IssueSyncTarget {
                repo: "org/repo".into(),
                issue: None,
            },
            &projection,
        )
        .unwrap();
    let answers = adapter
        .ingest_prompt_answers(&IssueSyncTarget {
            repo: "org/repo".into(),
            issue: Some(synced.issue),
        })
        .unwrap();

    let captured = fs::read_to_string(&log).unwrap();
    assert!(captured.contains("issue create"), "{captured}");
    assert!(captured.contains("issue comment 42"), "{captured}");
    assert_eq!(synced.url, "https://github.com/org/repo/issues/42");
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].prompt_id, "prompt-1");
    assert_eq!(answers[0].answer, "use source");
    assert_eq!(answers[0].answered_by.as_deref(), Some("octo"));
    std::env::remove_var("IQ_GITHUB_CLI");
}

#[test]
fn github_issue_backend_updates_managed_labels_and_skips_existing_marker_comments() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-gh");
    let log = temp.path().join("gh.log");
    fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> {log}
if [ "$1 $2" = "issue view" ]; then
  printf '%s' '{{"labels":[{{"name":"iq:queue"}},{{"name":"iq:status:ready"}},{{"name":"external"}}],"comments":[{{"body":"<!-- iq:prompt:prompt-1 -->\nold prompt","author":{{"login":"octo"}}}}]}}'
  exit 0
fi
exit 0
"#,
            log = log.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake);

    let projection = iq::issue_backends::IssueProjection {
        title: "Integration queue: agent/one → main".into(),
        labels: vec!["iq:queue".into(), "iq:status:blocked".into()],
        body: "<!-- iq:item:item-1 -->\nbody".into(),
        comments: vec![
            "<!-- iq:prompt:prompt-1 -->\nPrompt".into(),
            "<!-- iq:event:event-1 -->\nEvent".into(),
        ],
    };
    let adapter = issue_adapter_for_provider(IssueProvider::GitHub).unwrap();
    adapter
        .sync_projection(
            &IssueSyncTarget {
                repo: "org/repo".into(),
                issue: Some("42".into()),
            },
            &projection,
        )
        .unwrap();

    let captured = fs::read_to_string(&log).unwrap();
    assert!(captured.contains("issue view 42"), "{captured}");
    assert!(
        captured.contains("--add-label iq:status:blocked"),
        "{captured}"
    );
    assert!(
        captured.contains("--remove-label iq:status:ready"),
        "{captured}"
    );
    assert!(!captured.contains("--remove-label external"), "{captured}");
    assert!(
        captured.contains("issue comment 42 --repo org/repo --body <!-- iq:event:event-1 -->"),
        "{captured}"
    );
    assert!(
        !captured.contains("issue comment 42 --repo org/repo --body <!-- iq:prompt:prompt-1 -->"),
        "{captured}"
    );
    std::env::remove_var("IQ_GITHUB_CLI");
}

#[test]
fn gitlab_issue_backend_updates_managed_labels_and_skips_existing_marker_notes() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-glab");
    let log = temp.path().join("glab.log");
    fs::write(
        &fake,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> {log}
if [ "$1 $2" = "issue view" ]; then
  printf '%s' '{{"labels":["iq:queue","iq:status:blocked","team"],"comments":[{{"body":"<!-- iq:prompt:prompt-2 -->\nold prompt","author":{{"username":"maintainer"}}}}]}}'
  exit 0
fi
exit 0
"#,
            log = log.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITLAB_CLI", &fake);

    let projection = iq::issue_backends::IssueProjection {
        title: "Integration queue: agent/two → main".into(),
        labels: vec!["iq:queue".into(), "iq:status:validating".into()],
        body: "<!-- iq:item:item-2 -->\nbody".into(),
        comments: vec![
            "<!-- iq:prompt:prompt-2 -->\nPrompt".into(),
            "<!-- iq:event:event-2 -->\nEvent".into(),
        ],
    };
    let adapter = issue_adapter_for_provider(IssueProvider::GitLab).unwrap();
    adapter
        .sync_projection(
            &IssueSyncTarget {
                repo: "org/repo".into(),
                issue: Some("9".into()),
            },
            &projection,
        )
        .unwrap();

    let captured = fs::read_to_string(&log).unwrap();
    assert!(captured.contains("issue view 9"), "{captured}");
    assert!(
        captured.contains("--label iq:status:validating"),
        "{captured}"
    );
    assert!(
        captured.contains("--unlabel iq:status:blocked"),
        "{captured}"
    );
    assert!(!captured.contains("--unlabel team"), "{captured}");
    assert!(
        captured.contains("issue note 9 --repo org/repo --message <!-- iq:event:event-2 -->"),
        "{captured}"
    );
    assert!(
        !captured.contains("issue note 9 --repo org/repo --message <!-- iq:prompt:prompt-2 -->"),
        "{captured}"
    );
    std::env::remove_var("IQ_GITLAB_CLI");
}

#[test]
fn issue_answer_ingest_exits_nonzero_on_apply_failure_unless_best_effort() {
    let _guard = env_lock().lock().unwrap();
    let temp = tempdir().unwrap();
    let fake = temp.path().join("fake-gh");
    fs::write(
        &fake,
        r#"#!/bin/sh
if [ "$1 $2" = "issue view" ]; then
  printf '%s' '{"comments":[{"body":"iq answer missing-prompt use source","author":{"login":"octo"}}]}'
  exit 0
fi
exit 0
"#,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake);
    let db = temp.path().join("queues.db");

    let strict = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "issue",
            "ingest-answers",
            "--provider",
            "github",
            "--repo",
            "org/repo",
            "--issue",
            "42",
        ])
        .output()
        .unwrap();
    assert!(!strict.status.success());

    let best_effort = Command::new(env!("CARGO_BIN_EXE_iq"))
        .args([
            "--queue-db",
            db.to_str().unwrap(),
            "issue",
            "ingest-answers",
            "--provider",
            "github",
            "--repo",
            "org/repo",
            "--issue",
            "42",
            "--best-effort",
        ])
        .output()
        .unwrap();
    assert!(best_effort.status.success());
    std::env::remove_var("IQ_GITHUB_CLI");
}
