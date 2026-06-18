use iq::core::{BlockedPhase, BlockedReason, QueueStatus};
use iq::integrator::{git, git_output, validation_command, Integrator, IntegratorOptions};
use iq::sqlite::{EnqueueRequest, SqliteQueue};
use std::fs;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn direct_landing_integrates_only_after_remote_target_contains_landed_commit() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/one", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/one"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/one".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Integrated);
    let remote_main = git_output(&fixture.repo, ["rev-parse", "refs/remotes/origin/main"]).unwrap();
    assert_eq!(
        item.landed_commit_sha.as_deref(),
        Some(remote_main.as_str())
    );
    git(
        &fixture.repo,
        [
            "merge-base",
            "--is-ancestor",
            item.landed_commit_sha.as_ref().unwrap(),
            "refs/remotes/origin/main",
        ],
    )
    .unwrap();
}

#[test]
fn missing_validation_configuration_blocks_during_validating_phase() {
    let fixture = GitFixture::new(false);
    let source_head =
        fixture.create_source_branch("agent/no-validation", "feature.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/no-validation"],
    )
    .unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/no-validation".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Validating));
    assert_eq!(item.blocked_reason, Some(BlockedReason::NeedsUserInput));
}

#[test]
fn cargo_repo_without_threadmill_config_uses_cargo_test_default() {
    let fixture = GitFixture::new(false);
    fixture.create_cargo_project_on_main();
    let source_head =
        fixture.create_source_branch("agent/cargo-default", "feature.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/cargo-default"],
    )
    .unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/cargo-default".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Integrated);
    let attempt = queue
        .get_attempt(item.current_attempt_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(attempt.validation_command.as_deref(), Some("cargo test"));
}

#[test]
fn threadmill_config_validation_command_overrides_repo_default() {
    let fixture = GitFixture::new(false);
    fixture.create_cargo_project_on_main();
    fixture.set_validation_command("git diff --check");

    let command = validation_command(&fixture.repo).unwrap();

    assert_eq!(command.as_deref(), Some("git diff --check"));
}

#[test]
fn taskfile_validate_is_preferred_default_validation_command() {
    let temp = tempdir().unwrap();
    fs::write(
        temp.path().join("Taskfile.yml"),
        "version: '3'\ntasks:\n  validate:\n    cmds:\n      - cargo test\n",
    )
    .unwrap();
    fs::write(temp.path().join("Cargo.toml"), "[workspace]\n").unwrap();

    let command = validation_command(temp.path()).unwrap();

    assert_eq!(command.as_deref(), Some("task validate"));
}

#[test]
fn integrator_refuses_to_transition_after_lease_owner_changes() {
    let fixture = GitFixture::new(false);
    let db = fixture.temp.path().join("queues.db");
    fixture.set_validation_command("git diff --check");
    let source_head = fixture.create_source_branch("agent/stale-owner", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/stale-owner"]).unwrap();
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let enqueued = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/stale-owner".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W001"}),
        })
        .unwrap();
    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "owner-a".into(),
        lease_ttl_seconds: 1,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();
    let repo_key_for_steal = repo_key.to_string();
    let db_for_steal = db.clone();
    let stealer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        let conn = rusqlite::Connection::open(db_for_steal).unwrap();
        conn.execute(
            "UPDATE repo_leases SET owner_id='owner-b' WHERE repo_key=?1",
            [repo_key_for_steal],
        )
        .unwrap();
    });

    let result = integrator.run_once();
    stealer.join().unwrap();

    assert!(result.is_err());
    let item = queue.get_item(&enqueued.id).unwrap();
    assert!(matches!(
        item.status,
        QueueStatus::Merging | QueueStatus::Merged | QueueStatus::Validating
    ));
    assert_eq!(item.landed_commit_sha, None);
}

#[test]
fn source_branch_head_mismatch_blocks_without_integrating_moved_code() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/moved", "feature.txt", "accepted\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/moved"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/moved".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
        })
        .unwrap();
    fs::write(fixture.repo.join("moved.txt"), "not accepted\n").unwrap();
    git(&fixture.repo, ["add", "moved.txt"]).unwrap();
    git(&fixture.repo, ["commit", "-m", "move source branch"]).unwrap();
    let moved_head = git_output(&fixture.repo, ["rev-parse", "HEAD"]).unwrap();
    git(&fixture.repo, ["push", "origin", "agent/moved"]).unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Merging));
    assert_eq!(item.blocked_reason, Some(BlockedReason::NeedsAgentFix));
    let remote_main = git_output(&fixture.repo, ["rev-parse", "refs/remotes/origin/main"]).unwrap();
    assert_ne!(remote_main, moved_head);
    assert!(git(
        &fixture.repo,
        [
            "merge-base",
            "--is-ancestor",
            &moved_head,
            "refs/remotes/origin/main",
        ],
    )
    .is_err());
}

#[test]
fn answered_merge_conflict_resumes_same_attempt_and_integrates() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/conflict", "conflict.txt", "source\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/conflict"]).unwrap();
    fixture.commit_on_main("conflict.txt", "target\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/conflict".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();
    let blocked = integrator.run_once().unwrap().unwrap();
    assert_eq!(blocked.status, QueueStatus::Blocked);
    assert_eq!(blocked.blocked_phase, Some(BlockedPhase::Merging));
    assert_eq!(
        blocked.conflict.as_ref().unwrap()["files"][0],
        "conflict.txt"
    );
    let prompt_id = blocked.validation_evidence["prompt_id"].as_str().unwrap();
    let resumed = queue
        .answer_prompt(prompt_id, "use source", "user")
        .unwrap();
    assert_eq!(resumed.status, QueueStatus::Merging);

    let integrated = integrator.resume_item(&item.id).unwrap();

    assert_eq!(integrated.status, QueueStatus::Integrated);
    let remote_main = git_output(&fixture.repo, ["rev-parse", "refs/remotes/origin/main"]).unwrap();
    assert_eq!(
        integrated.landed_commit_sha.as_deref(),
        Some(remote_main.as_str())
    );
}

#[test]
fn merge_resume_without_current_answered_prompt_does_not_accept_workspace_resolution() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/missing-answer", "conflict.txt", "source\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/missing-answer"],
    )
    .unwrap();
    fixture.commit_on_main("conflict.txt", "target\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/missing-answer".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();
    let blocked = integrator.run_once().unwrap().unwrap();
    assert_eq!(blocked.status, QueueStatus::Blocked);
    let workspace = blocked
        .integration_workspace_path
        .as_ref()
        .map(std::path::PathBuf::from)
        .unwrap();
    fs::write(workspace.join("conflict.txt"), "source\n").unwrap();
    git(&workspace, ["add", "conflict.txt"]).unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE queue_items SET status='merging',blocked_phase=NULL,blocked_reason=NULL,blocked_message=NULL,prompt_id=NULL WHERE id=?1",
        [&blocked.id],
    )
    .unwrap();

    let result = integrator.resume_item(&item.id);

    assert!(result.is_err());
    let remote_contents = git_output(
        &fixture.repo,
        ["show", "refs/remotes/origin/main:conflict.txt"],
    )
    .unwrap();
    assert_eq!(remote_contents, "target");
}

#[test]
fn daemon_run_holds_later_ready_item_behind_oldest_blocked_item() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/conflict", "conflict.txt", "source\n");
    fixture.commit_on_main("conflict.txt", "target\n");
    let later_head = fixture.create_source_branch("agent/later", "later.txt", "later\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/conflict".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
        })
        .unwrap();
    let later = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/later".into(),
            target_branch: "main".into(),
            current_head_sha: later_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();
    let blocked = integrator.run_once().unwrap().unwrap();
    assert_eq!(blocked.id, first.id);
    assert_eq!(blocked.status, QueueStatus::Blocked);

    let held = integrator.run_once().unwrap().unwrap();

    assert_eq!(held.id, first.id);
    assert_eq!(held.status, QueueStatus::Blocked);
    assert_eq!(
        queue.get_item(&later.id).unwrap().status,
        QueueStatus::Ready
    );
}

#[test]
fn daemon_run_resumes_oldest_answered_item_before_claiming_later_ready_item() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/conflict", "conflict.txt", "source\n");
    fixture.commit_on_main("conflict.txt", "target\n");
    let later_head = fixture.create_source_branch("agent/later", "later.txt", "later\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let first = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/conflict".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W002"}),
        })
        .unwrap();
    let later = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/later".into(),
            target_branch: "main".into(),
            current_head_sha: later_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();
    let blocked = integrator.run_once().unwrap().unwrap();
    let prompt_id = blocked.validation_evidence["prompt_id"].as_str().unwrap();
    queue
        .answer_prompt(prompt_id, "use source", "user")
        .unwrap();

    let resumed = integrator.run_once().unwrap().unwrap();

    assert_eq!(resumed.id, first.id);
    assert_eq!(resumed.status, QueueStatus::Integrated);
    assert_eq!(
        queue.get_item(&later.id).unwrap().status,
        QueueStatus::Ready
    );
}

#[test]
fn pr_backed_conflict_resolution_pushes_source_branch_before_provider_merge() {
    let _guard = env_lock().lock().unwrap();
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/pr-conflict", "conflict.txt", "source\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/pr-conflict"]).unwrap();
    fixture.commit_on_main("conflict.txt", "target\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    let item = queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/pr-conflict".into(),
            target_branch: "main".into(),
            current_head_sha: source_head.clone(),
            pr_url: Some("https://github.com/org/repo/pull/7".into()),
            producer_metadata: serde_json::json!({"worker":"W003"}),
        })
        .unwrap();
    let fake_gh = fixture.temp.path().join("fake-gh");
    let remote = fixture.remote.clone();
    fs::write(
        &fake_gh,
        format!(
            r#"#!/bin/sh
if [ "$1 $2" = "pr view" ]; then
  head=$(git --git-dir={remote} rev-parse refs/heads/agent/pr-conflict)
  base=$(git --git-dir={remote} rev-parse refs/heads/main)
  printf '{{"headRefOid":"%s","baseRefOid":"%s","reviewDecision":"APPROVED","mergeStateStatus":"CLEAN","statusCheckRollup":[{{"status":"COMPLETED","conclusion":"SUCCESS"}}]}}' "$head" "$base"
  exit 0
fi
if [ "$1 $2" = "pr merge" ]; then
  git --git-dir={remote} update-ref refs/heads/main refs/heads/agent/pr-conflict
  exit 0
fi
exit 2
"#,
            remote = remote.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake_gh).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_gh, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake_gh);

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db.clone(),
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();
    let blocked = integrator.run_once().unwrap().unwrap();
    let prompt_id = blocked.validation_evidence["prompt_id"].as_str().unwrap();
    queue
        .answer_prompt(prompt_id, "use source", "user")
        .unwrap();

    let integrated = integrator.resume_item(&item.id).unwrap();

    assert_eq!(integrated.status, QueueStatus::Integrated);
    assert_ne!(integrated.current_head_sha, source_head);
    let source_remote = git_output(
        &fixture.repo,
        ["rev-parse", "refs/remotes/origin/agent/pr-conflict"],
    )
    .unwrap();
    assert_eq!(integrated.current_head_sha, source_remote);
    std::env::remove_var("IQ_GITHUB_CLI");
}

#[test]
fn target_moved_merge_conflict_blocks_with_conflict_metadata() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/late-conflict", "conflict.txt", "source\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/late-conflict"],
    )
    .unwrap();
    let moved_branch = "target/late-conflict";
    fixture.set_validation_command(&format!(
        "git --git-dir={} update-ref refs/heads/main $(git --git-dir={} rev-parse refs/heads/{moved_branch})",
        fixture.remote.display(),
        fixture.remote.display(),
    ));
    let moved_sha =
        fixture.create_unpublished_target_change(moved_branch, "conflict.txt", "target\n");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/late-conflict".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W004"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Merging));
    assert_eq!(item.blocked_reason, Some(BlockedReason::NeedsUserInput));
    assert_eq!(item.conflict.as_ref().unwrap()["files"][0], "conflict.txt");
    assert_eq!(item.target_sha.as_deref(), Some(moved_sha.as_str()));
}

#[test]
fn target_moved_missing_validation_config_blocks_for_user_input() {
    let fixture = GitFixture::new(true);
    let source_head =
        fixture.create_source_branch("agent/missing-revalidation", "feature.txt", "feature\n");
    git(
        &fixture.repo,
        ["push", "-u", "origin", "agent/missing-revalidation"],
    )
    .unwrap();
    let moved_branch = "target/no-validation";
    fixture.set_validation_command(&format!(
        "git --git-dir={} update-ref refs/heads/main $(git --git-dir={} rev-parse refs/heads/{moved_branch})",
        fixture.remote.display(),
        fixture.remote.display(),
    ));
    fixture.create_unpublished_target_deleting_validation(moved_branch);
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/missing-revalidation".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W005"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Validating));
    assert_eq!(item.blocked_reason, Some(BlockedReason::NeedsUserInput));
}

#[test]
fn direct_landing_fetch_failure_persists_integrating_block() {
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/fetch-fails", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/fetch-fails"]).unwrap();
    fixture.set_validation_command("git remote set-url origin /missing/iq-remote");
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/fetch-fails".into(),
            target_branch: "main".into(),
            current_head_sha: source_head,
            pr_url: None,
            producer_metadata: serde_json::json!({"worker":"W006"}),
        })
        .unwrap();

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Integrating));
    assert_eq!(item.blocked_reason, Some(BlockedReason::Infra));
}

#[test]
fn pr_provider_landing_blocks_when_provider_does_not_land_queued_head() {
    let _guard = env_lock().lock().unwrap();
    let fixture = GitFixture::new(true);
    let source_head = fixture.create_source_branch("agent/not-landed", "feature.txt", "feature\n");
    git(&fixture.repo, ["push", "-u", "origin", "agent/not-landed"]).unwrap();
    let db = fixture.temp.path().join("queues.db");
    let queue = SqliteQueue::open(&db).unwrap();
    let repo_key = "fixture::main";
    queue
        .enqueue(EnqueueRequest {
            repo_key: repo_key.into(),
            repo_path: fixture.repo.to_string_lossy().to_string(),
            source_branch: "agent/not-landed".into(),
            target_branch: "main".into(),
            current_head_sha: source_head.clone(),
            pr_url: Some("https://github.com/org/repo/pull/8".into()),
            producer_metadata: serde_json::json!({"worker":"W004"}),
        })
        .unwrap();
    let fake_gh = fixture.temp.path().join("fake-gh-noop");
    let remote = fixture.remote.clone();
    fs::write(
        &fake_gh,
        format!(
            r#"#!/bin/sh
if [ "$1 $2" = "pr view" ]; then
  head=$(git --git-dir={remote} rev-parse refs/heads/agent/not-landed)
  base=$(git --git-dir={remote} rev-parse refs/heads/main)
  printf '{{"headRefOid":"%s","baseRefOid":"%s","reviewDecision":"APPROVED","mergeStateStatus":"CLEAN","statusCheckRollup":[{{"status":"COMPLETED","conclusion":"SUCCESS"}}]}}' "$head" "$base"
  exit 0
fi
if [ "$1 $2" = "pr merge" ]; then
  exit 0
fi
exit 2
"#,
            remote = remote.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake_gh).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_gh, permissions).unwrap();
    }
    std::env::set_var("IQ_GITHUB_CLI", &fake_gh);

    let integrator = Integrator::new(IntegratorOptions {
        repo_key: repo_key.into(),
        repo_path: fixture.repo.clone(),
        queue_db: db,
        owner_id: "test-integrator".into(),
        lease_ttl_seconds: 30,
        base_remote: "origin".into(),
        workspace_root: fixture.temp.path().join("workspaces"),
    })
    .unwrap();

    let item = integrator.run_once().unwrap().unwrap();

    assert_eq!(item.status, QueueStatus::Blocked);
    assert_eq!(item.blocked_phase, Some(BlockedPhase::Integrating));
    assert_eq!(item.blocked_reason, Some(BlockedReason::Provider));
    assert_eq!(item.landed_commit_sha, None);
    assert!(git(
        &fixture.repo,
        [
            "merge-base",
            "--is-ancestor",
            source_head.as_str(),
            "refs/remotes/origin/main",
        ],
    )
    .is_err());
    std::env::remove_var("IQ_GITHUB_CLI");
}

struct GitFixture {
    temp: tempfile::TempDir,
    remote: std::path::PathBuf,
    repo: std::path::PathBuf,
}

impl GitFixture {
    fn new(include_validation: bool) -> Self {
        let temp = tempdir().unwrap();
        let remote = temp.path().join("remote.git");
        git(temp.path(), ["init", "--bare", remote.to_str().unwrap()]).unwrap();
        let repo = temp.path().join("repo");
        git(
            temp.path(),
            ["clone", remote.to_str().unwrap(), repo.to_str().unwrap()],
        )
        .unwrap();
        git(&repo, ["config", "user.email", "iq@example.test"]).unwrap();
        git(&repo, ["config", "user.name", "IQ Test"]).unwrap();
        let hooks = temp.path().join("empty-hooks");
        fs::create_dir(&hooks).unwrap();
        git(&repo, ["config", "core.hooksPath", hooks.to_str().unwrap()]).unwrap();
        git(&repo, ["checkout", "-b", "main"]).unwrap();
        fs::write(repo.join("README.md"), "base\n").unwrap();
        if include_validation {
            fs::write(
                repo.join(".threadmill.yml"),
                "integration:\n  validation:\n    command: git diff --check\n",
            )
            .unwrap();
        }
        git(&repo, ["add", "."]).unwrap();
        git(&repo, ["commit", "-m", "base"]).unwrap();
        git(&repo, ["push", "-u", "origin", "main"]).unwrap();
        Self { temp, remote, repo }
    }

    fn create_source_branch(&self, branch: &str, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "-b", branch, "main"]).unwrap();
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        git(&self.repo, ["add", path]).unwrap();
        git(&self.repo, ["commit", "-m", "feature"]).unwrap();
        let sha = git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap();
        git(
            &self.repo,
            ["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        )
        .unwrap();
        sha
    }

    fn set_validation_command(&self, command: &str) {
        git(&self.repo, ["checkout", "main"]).unwrap();
        fs::write(
            self.repo.join(".threadmill.yml"),
            format!("integration:\n  validation:\n    command: {command}\n"),
        )
        .unwrap();
        git(&self.repo, ["add", ".threadmill.yml"]).unwrap();
        git(&self.repo, ["commit", "-m", "validation command"]).unwrap();
        git(&self.repo, ["push", "origin", "main"]).unwrap();
    }

    fn create_cargo_project_on_main(&self) {
        git(&self.repo, ["checkout", "main"]).unwrap();
        fs::create_dir_all(self.repo.join("src")).unwrap();
        fs::write(
            self.repo.join("Cargo.toml"),
            "[package]\nname = \"iq-demo-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::write(
            self.repo.join("src/lib.rs"),
            "pub fn fixture_value() -> &'static str { \"iq-demo-fixture\" }\n",
        )
        .unwrap();
        git(&self.repo, ["add", "Cargo.toml", "src/lib.rs"]).unwrap();
        git(&self.repo, ["commit", "-m", "cargo project"]).unwrap();
        git(&self.repo, ["push", "origin", "main"]).unwrap();
    }

    fn create_unpublished_target_change(&self, branch: &str, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "-b", branch, "main"]).unwrap();
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        git(&self.repo, ["add", path]).unwrap();
        git(&self.repo, ["commit", "-m", "target moved"]).unwrap();
        let sha = git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap();
        git(
            &self.repo,
            ["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        )
        .unwrap();
        sha
    }

    fn create_unpublished_target_deleting_validation(&self, branch: &str) -> String {
        git(&self.repo, ["checkout", "-b", branch, "main"]).unwrap();
        fs::remove_file(self.repo.join(".threadmill.yml")).unwrap();
        git(&self.repo, ["add", ".threadmill.yml"]).unwrap();
        git(&self.repo, ["commit", "-m", "remove validation config"]).unwrap();
        let sha = git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap();
        git(
            &self.repo,
            ["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        )
        .unwrap();
        sha
    }

    fn commit_on_main(&self, path: &str, contents: &str) -> String {
        git(&self.repo, ["checkout", "main"]).unwrap();
        fs::write(self.repo.join(Path::new(path)), contents).unwrap();
        git(&self.repo, ["add", path]).unwrap();
        git(&self.repo, ["commit", "-m", "target change"]).unwrap();
        git(&self.repo, ["push", "origin", "main"]).unwrap();
        git_output(&self.repo, ["rev-parse", "HEAD"]).unwrap()
    }
}
