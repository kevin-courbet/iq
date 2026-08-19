use iq::issue_backends::{issue_adapter_for_provider, IssueProvider, IssueSyncTarget};
use iq::providers::provider_for_url;
use std::fs;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn provider_selection_rejects_malformed_supported_host_urls() {
    assert!(provider_for_url("https://github.com/org/repo").is_err());
    assert!(provider_for_url("https://gitlab.com/group/project/merge_requests/7").is_err());
    assert!(provider_for_url("ssh://github.com/org/repo/pull/7").is_err());
}

#[test]
// Controlled CLI scripts verify IQ's legacy adapter contract, not provider service behavior.
fn controlled_github_cli_contract_syncs_issue_projection() {
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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Github,
        &fake,
    )
    .unwrap();

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
    let captured = fs::read_to_string(&log).unwrap();
    assert!(captured.contains("issue create"), "{captured}");
    assert!(captured.contains("issue comment 42"), "{captured}");
    assert_eq!(synced.url, "https://github.com/org/repo/issues/42");
}

#[test]
fn controlled_github_cli_contract_updates_labels_and_skips_marker_comments() {
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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Github,
        &fake,
    )
    .unwrap();

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
}

#[test]
fn controlled_gitlab_cli_contract_updates_labels_and_skips_marker_notes() {
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
    let _provider_executable = iq::providers::inject_test_provider_executable(
        iq::repository_policy::Provider::Gitlab,
        &fake,
    )
    .unwrap();

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
}
