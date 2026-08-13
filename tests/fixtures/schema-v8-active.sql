PRAGMA foreign_keys=ON;

CREATE TABLE registered_remote_identities (
  repo_key TEXT PRIMARY KEY,
  integration_path BLOB NOT NULL UNIQUE,
  target_branch TEXT NOT NULL,
  remote_name TEXT NOT NULL,
  fetch_url TEXT NOT NULL,
  push_url TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE registered_repositories (
  repo_key TEXT PRIMARY KEY,
  integration_path BLOB NOT NULL UNIQUE,
  target_branch TEXT NOT NULL,
  remote TEXT NOT NULL,
  seed_path BLOB NOT NULL UNIQUE,
  seed_rift_id TEXT,
  seed_source_rift_id TEXT,
  workspace_root BLOB NOT NULL UNIQUE,
  checkout_reconciliation_json TEXT NOT NULL,
  seed_refresh_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE development_workspaces (
  id TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  name TEXT NOT NULL,
  path BLOB NOT NULL UNIQUE,
  rift_id TEXT,
  source_rift_id TEXT,
  branch TEXT NOT NULL UNIQUE,
  base_sha TEXT NOT NULL,
  status TEXT NOT NULL,
  cleanup_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(repo_key,name)
);

CREATE TABLE local_submissions (
  id TEXT PRIMARY KEY,
  queue_item_id TEXT NOT NULL,
  repo_key TEXT NOT NULL REFERENCES registered_repositories(repo_key),
  workspace_id TEXT NOT NULL REFERENCES development_workspaces(id),
  base_sha TEXT NOT NULL,
  commit_sha TEXT NOT NULL,
  private_ref TEXT NOT NULL UNIQUE,
  staging_ref TEXT NOT NULL UNIQUE,
  replaces_item_id TEXT,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE queue_items (
  id TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL,
  repo_path TEXT NOT NULL,
  source_branch TEXT NOT NULL,
  target_branch TEXT NOT NULL,
  pr_url TEXT,
  producer_metadata_json TEXT NOT NULL,
  validation_evidence_json TEXT NOT NULL,
  status TEXT NOT NULL,
  current_head_sha TEXT NOT NULL,
  current_attempt_id TEXT,
  blocked_phase TEXT,
  blocked_reason TEXT,
  blocked_message TEXT,
  retry_after TEXT,
  prompt_id TEXT,
  conflict_json TEXT,
  integration_workspace_path TEXT,
  integration_workspace_rift_id TEXT,
  integration_workspace_source_rift_id TEXT,
  integration_workspace_cleaned_at TEXT,
  target_sha TEXT,
  source_sha TEXT,
  landed_commit_sha TEXT,
  landing_state_json TEXT NOT NULL DEFAULT '{"state":"ready"}',
  source_kind TEXT NOT NULL DEFAULT 'remote_branch',
  source_ref TEXT,
  submission_id TEXT REFERENCES local_submissions(id),
  landing_policy TEXT NOT NULL DEFAULT 'direct',
  replacement_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX queue_items_active_identity
ON queue_items(repo_key,source_branch,target_branch)
WHERE status NOT IN ('integrated','cancelled');

CREATE TABLE integration_attempts (
  id TEXT PRIMARY KEY,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL,
  source_head_sha TEXT NOT NULL,
  target_base_sha TEXT,
  merge_commit_sha TEXT,
  validated_commit_sha TEXT,
  landed_commit_sha TEXT,
  validation_command TEXT,
  validation_exit_code INTEGER,
  validation_log_path TEXT,
  policy_snapshot_json TEXT,
  policy_digest TEXT,
  signoff_evidence_json TEXT,
  moved_base_json TEXT NOT NULL DEFAULT '{"state":"none"}',
  started_at TEXT NOT NULL,
  finished_at TEXT,
  result TEXT,
  UNIQUE(item_id,attempt_number)
);

CREATE TABLE queue_events (
  id TEXT PRIMARY KEY,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE prompts (
  id TEXT PRIMARY KEY,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  attempt_id TEXT,
  blocked_phase TEXT NOT NULL,
  status TEXT NOT NULL,
  question TEXT NOT NULL,
  options_json TEXT,
  allow_freeform INTEGER NOT NULL DEFAULT 1,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  answer TEXT,
  answered_by TEXT,
  answered_at TEXT
);

CREATE TABLE communication_bindings (
  id TEXT PRIMARY KEY,
  repo_key TEXT NOT NULL,
  item_id TEXT NOT NULL REFERENCES queue_items(id) ON DELETE CASCADE,
  transport_id TEXT NOT NULL,
  transport_kind TEXT NOT NULL,
  endpoint_fingerprint TEXT NOT NULL,
  marker TEXT NOT NULL UNIQUE,
  external_ref_json TEXT,
  external_url TEXT,
  status TEXT NOT NULL,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(item_id,transport_id)
);

CREATE TABLE communication_response_receipts (
  binding_id TEXT NOT NULL REFERENCES communication_bindings(id) ON DELETE CASCADE,
  external_response_id TEXT NOT NULL,
  prompt_id TEXT NOT NULL,
  answer TEXT NOT NULL,
  actor TEXT NOT NULL,
  disposition TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(binding_id,external_response_id)
);

CREATE TABLE repo_leases (
  repo_key TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TABLE queue_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

INSERT INTO queue_metadata VALUES
  ('workspace_schema_version','8'),
  ('database_id','fixture-v8-active');

CREATE TABLE workspace_roots (
  repo_key TEXT PRIMARY KEY,
  source_path TEXT NOT NULL,
  source_rift_id TEXT NOT NULL,
  workspace_root TEXT NOT NULL UNIQUE,
  registry_identity TEXT NOT NULL,
  generation INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE workspace_gc_debt (
  registry_identity TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
);
