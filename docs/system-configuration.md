# System Configuration

IQ reads strict YAML from an explicit absolute path. Unknown fields are errors.

Every command that can use Rift takes the global `--rift-executable <absolute-path>` option. IQ opens, verifies, and seals that exact executable during process startup. It never resolves Rift through `PATH`, and it rejects `IQ_RIFT_CLI`.

```yaml
integration_agent:
  runner: opencode
  executable: /usr/local/bin/opencode
  agent: iq-integration
  model: openai/gpt-5.6-sol
  cycle_timeout_seconds: 1800
  max_log_bytes: 1048576
  max_result_bytes: 262144
  max_processes: 64
  memory_bytes: 4294967296
  cpu_seconds: 1800
  writable_bytes: 8589934592
  open_files: 4096
  credential_env: OPENAI_API_KEY

control_plane:
  unix_socket: /home/user/.local/state/iq/control.sock
  max_request_bytes: 262144
  max_free_text_bytes: 16384
  max_response_bytes: 1048576
  max_concurrent_clients: 32
  max_client_queue_bytes: 1048576
  max_stream_backlog_events: 10000
  client_idle_seconds: 60

notifications:
  max_attempts: 5
  max_event_age_seconds: 86400
  projection_debt_alert_seconds: 900
  backends:
    - kind: wslg
      executable: /usr/bin/notify-send
    - kind: windows
      executable: /mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe
```

The `notifications` section is optional. When it is omitted, IQ enables no backends and uses the non-zero bounds shown above.

`credential_env` names the model credential that IQ reads from its environment. IQ passes it only to the sandboxed agent process. It does not put the value in arguments, protocol files, or logs.

The Linux runner mounts the normal runtime trees `/usr`, `/bin`, `/lib`, and `/lib64` read-only when they are present. No runtime command or path manifest is required.

When the current user's OpenCode configuration directory exists, IQ mounts it read-only at `/home/iq/.config/opencode`. Runtime state, cache, and temporary files remain inside the bounded sandbox.

`writable_bytes` bounds the tmpfs that backs the repository overlay, temporary files, protocol files, and exported result. IQ does not require Btrfs qgroups or a persistent filesystem quota.

The fixed automatic-cycle limit is 10 and is not a configuration field.

`iq doctor` verifies the exact runner executable identity, basic sandbox tools, state repository, socket path, and notification backend availability. An unavailable notification backend is degraded. It does not stop integration.

IQ resolves Git, provider, Rift, and sandbox executables to absolute paths. `IQ_GIT_EXECUTABLE`, `IQ_GIT_CLI`, `IQ_GITHUB_CLI`, and `IQ_GITLAB_CLI` are forbidden production overrides. Host executables and same-user host processes are trusted.

The agent runner uses one transient `iq-agent-<cycle-id>.service` per cycle. Other Git, provider, and Rift commands run directly. The default test suite excludes the long end-to-end integrator target. Run it explicitly with `cargo test --locked --features slow-tests --test integrator_tests`.

Each runner sandbox contains an ownership manifest for its parent and root filesystem identities. Cleanup requires an exclusively user-owned parent, verifies the live root against the manifest, and quarantines the exact directory under that parent before removal. Replacement or symlink roots fail closed.
