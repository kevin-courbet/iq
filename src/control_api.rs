use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use crate::agent_config::ControlPlaneConfig;
use crate::control_store::{AnswerCommand, ControlStore, ResponderIdentity};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApiRequest {
    Inbox { limit: u32 },
    Show { item_id: String },
    Answer { answer: AnswerCommand },
    Watch { cursor: u64, limit: u32 },
    Retry { item_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiEnvelope {
    pub version: u32,
    pub request: ApiRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiResponse {
    pub version: u32,
    pub ok: bool,
    pub result: serde_json::Value,
}

pub struct ControlApiServer {
    listener: UnixListener,
    config: ControlPlaneConfig,
    store: Arc<ControlStore>,
    lease: DaemonLease,
    database_lease: crate::control_store::DatabaseProcessLease,
}

impl ControlApiServer {
    pub fn bind(config: ControlPlaneConfig, store: ControlStore) -> Result<Self> {
        verify_socket_path(&config.unix_socket)?;
        let parent = config
            .unix_socket
            .parent()
            .context("control socket has no parent")?;
        if !parent.exists() {
            fs::create_dir(parent)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let parent_metadata = fs::symlink_metadata(parent)?;
        if parent_metadata.file_type().is_symlink()
            || !parent_metadata.is_dir()
            || parent_metadata.uid() != unsafe { libc::geteuid() }
            || parent_metadata.permissions().mode() & 0o777 != 0o700
        {
            anyhow::bail!("control socket parent must be an owned mode-0700 real directory");
        }
        let lease = DaemonLease::acquire(parent)?;
        let database_lease = crate::control_store::DatabaseProcessLease::acquire(store.path())?;
        remove_stale_socket(&config.unix_socket, &lease)?;
        let listener = UnixListener::bind(&config.unix_socket)?;
        fs::set_permissions(&config.unix_socket, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(false)?;
        Ok(Self {
            listener,
            config,
            store: Arc::new(store),
            lease,
            database_lease,
        })
    }

    pub fn serve(self) -> Result<()> {
        let _lease = self.lease;
        let _database_lease = self.database_lease;
        let permits = Arc::new(std::sync::Mutex::new(0_u32));
        for stream in self.listener.incoming() {
            let stream = stream?;
            let mut active = permits
                .lock()
                .map_err(|_| anyhow::anyhow!("API permit lock poisoned"))?;
            if *active >= self.config.max_concurrent_clients {
                drop(active);
                let mut stream = stream;
                write_response(
                    &mut stream,
                    &ApiResponse {
                        version: 1,
                        ok: false,
                        result: json!({"error":"too_many_clients"}),
                    },
                    self.config.max_response_bytes,
                )?;
                continue;
            }
            *active += 1;
            drop(active);
            let store = self.store.clone();
            let config = self.config.clone();
            let permits = permits.clone();
            thread::spawn(move || {
                let _ = handle_connection(stream, &config, &store);
                if let Ok(mut active) = permits.lock() {
                    *active = active.saturating_sub(1);
                }
            });
        }
        Ok(())
    }

    pub fn serve_one(self) -> Result<()> {
        let (stream, _) = self.listener.accept()?;
        handle_connection(stream, &self.config, &self.store)
    }
}

pub fn request(
    socket: &Path,
    request: &ApiRequest,
    max_response_bytes: u64,
) -> Result<ApiResponse> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("connect IQ control socket {}", socket.display()))?;
    let bytes = serde_json::to_vec(&ApiEnvelope {
        version: 1,
        request: request.clone(),
    })?;
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    stream.flush()?;
    let response = read_frame(&mut stream, max_response_bytes)?;
    serde_json::from_slice(&response).context("parse IQ control response")
}

pub fn watch(
    socket: &Path,
    cursor: u64,
    limit: u32,
    max_response_bytes: u64,
    mut receive: impl FnMut(&ApiResponse) -> Result<()>,
) -> Result<u64> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("connect IQ control socket {}", socket.display()))?;
    let bytes = serde_json::to_vec(&ApiEnvelope {
        version: 1,
        request: ApiRequest::Watch { cursor, limit },
    })?;
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    stream.flush()?;
    let mut last_cursor = cursor;
    loop {
        let response: ApiResponse =
            serde_json::from_slice(&read_frame(&mut stream, max_response_bytes)?)?;
        receive(&response)?;
        if let Some(value) = response
            .result
            .get("cursor")
            .and_then(serde_json::Value::as_u64)
        {
            last_cursor = value;
        }
        if !response.ok
            || response
                .result
                .get("kind")
                .and_then(serde_json::Value::as_str)
                == Some("disconnect")
        {
            return Ok(last_cursor);
        }
    }
}

fn handle_connection(
    mut stream: UnixStream,
    config: &ControlPlaneConfig,
    store: &ControlStore,
) -> Result<()> {
    let peer = peer_uid(&stream)?;
    let daemon_uid = unsafe { libc::geteuid() };
    if peer != daemon_uid {
        anyhow::bail!("control API peer UID is not the daemon UID");
    }
    stream.set_read_timeout(Some(Duration::from_secs(config.client_idle_seconds)))?;
    stream.set_write_timeout(Some(Duration::from_secs(config.client_idle_seconds)))?;
    let frame = read_frame(&mut stream, config.max_request_bytes)?;
    let envelope: ApiEnvelope =
        serde_json::from_slice(&frame).context("parse strict control API request")?;
    if envelope.version != 1 {
        anyhow::bail!("unsupported control API version");
    }
    if let ApiRequest::Watch { cursor, limit } = envelope.request {
        return stream_events(&mut stream, config, store, cursor, limit);
    }
    let result = match envelope.request {
        ApiRequest::Inbox { limit } => serde_json::to_value(store.inbox(limit)?)?,
        ApiRequest::Show { item_id } => serde_json::to_value(
            store
                .effort_for_item(&item_id)?
                .with_context(|| format!("item has no effort: {item_id}"))?,
        )?,
        ApiRequest::Answer { answer } => {
            if answer.answer.len() as u64 > config.max_free_text_bytes {
                anyhow::bail!("answer exceeds configured free-text bound");
            }
            serde_json::to_value(store.answer(
                &answer,
                &ResponderIdentity::LocalPeer { uid: peer },
                daemon_uid,
            )?)?
        }
        ApiRequest::Watch { .. } => unreachable!(),
        ApiRequest::Retry { item_id } => {
            let effort = store
                .effort_for_item(&item_id)?
                .with_context(|| format!("item has no effort: {item_id}"))?;
            serde_json::to_value(store.retry_blocked(
                &effort.id,
                &ResponderIdentity::LocalPeer { uid: peer },
                daemon_uid,
            )?)?
        }
    };
    write_response(
        &mut stream,
        &ApiResponse {
            version: 1,
            ok: true,
            result,
        },
        config.max_response_bytes.min(config.max_client_queue_bytes),
    )
}

fn stream_events(
    stream: &mut UnixStream,
    config: &ControlPlaneConfig,
    store: &ControlStore,
    cursor: u64,
    limit: u32,
) -> Result<()> {
    let maximum = u32::try_from(config.max_stream_backlog_events.min(10_000))?;
    if limit == 0 || limit > maximum {
        return write_response(
            stream,
            &ApiResponse {
                version: 1,
                ok: false,
                result: json!({"kind":"invalid_stream_limit","maximum":maximum,"cursor":cursor}),
            },
            config.max_response_bytes.min(config.max_client_queue_bytes),
        );
    }
    if let Some(oldest) = store.oldest_event_sequence()? {
        if cursor != 0 && cursor.saturating_add(1) < oldest {
            return write_response(
                stream,
                &ApiResponse {
                    version: 1,
                    ok: false,
                    result: json!({"kind":"cursor_expired","oldest_cursor":oldest.saturating_sub(1),"cursor":cursor}),
                },
                config.max_response_bytes.min(config.max_client_queue_bytes),
            );
        }
    }
    let message_bound =
        usize::try_from((config.max_client_queue_bytes / config.max_response_bytes.max(1)).max(1))?;
    let (sender, receiver) = mpsc::sync_channel::<Result<ApiResponse>>(message_bound);
    let backpressure = Arc::new(AtomicBool::new(false));
    let producer_backpressure = backpressure.clone();
    let producer_store = store.clone();
    let producer_max = config.max_client_queue_bytes.min(config.max_response_bytes);
    thread::spawn(move || {
        let mut selected_cursor = cursor;
        loop {
            let result = (|| -> Result<Option<ApiResponse>> {
                let events = producer_store.events_after(selected_cursor, limit)?;
                if events.is_empty() {
                    return Ok(None);
                }
                let next = events
                    .last()
                    .map_or(selected_cursor, |event| event.sequence);
                let response = ApiResponse {
                    version: 1,
                    ok: true,
                    result: json!({"kind":"events","cursor":next,"events":events}),
                };
                if serde_json::to_vec(&response)?.len() as u64 > producer_max {
                    producer_backpressure.store(true, Ordering::Release);
                    return Ok(None);
                }
                selected_cursor = next;
                Ok(Some(response))
            })();
            match result {
                Ok(Some(response)) => match sender.try_send(Ok(response)) {
                    Ok(()) => {}
                    Err(mpsc::TrySendError::Full(_)) => {
                        producer_backpressure.store(true, Ordering::Release);
                        break;
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => break,
                },
                Ok(None) if producer_backpressure.load(Ordering::Acquire) => break,
                Ok(None) => thread::sleep(Duration::from_millis(100)),
                Err(error) => {
                    let _ = sender.try_send(Err(error));
                    break;
                }
            }
        }
    });
    let idle = Duration::from_secs(config.client_idle_seconds);
    let mut sent_cursor = cursor;
    let mut last_activity = Instant::now();
    loop {
        if backpressure.load(Ordering::Acquire) {
            return write_response(
                stream,
                &ApiResponse {
                    version: 1,
                    ok: true,
                    result: json!({"kind":"disconnect","reason":"backpressure","cursor":sent_cursor}),
                },
                config.max_response_bytes.min(config.max_client_queue_bytes),
            );
        }
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(response)) => {
                write_response(stream, &response, config.max_response_bytes)?;
                sent_cursor = response.result["cursor"].as_u64().unwrap_or(sent_cursor);
                last_activity = Instant::now();
            }
            Ok(Err(error)) => return Err(error),
            Err(mpsc::RecvTimeoutError::Disconnected) if backpressure.load(Ordering::Acquire) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            Err(mpsc::RecvTimeoutError::Timeout) if last_activity.elapsed() >= idle => {
                return write_response(
                    stream,
                    &ApiResponse {
                        version: 1,
                        ok: true,
                        result: json!({"kind":"disconnect","reason":"idle","cursor":sent_cursor}),
                    },
                    config.max_response_bytes.min(config.max_client_queue_bytes),
                );
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

fn read_frame(stream: &mut UnixStream, max: u64) -> Result<Vec<u8>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as u64;
    if length == 0 || length > max {
        anyhow::bail!("control API frame exceeds configured bound");
    }
    let mut bytes = vec![0_u8; length as usize];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn write_response(stream: &mut UnixStream, response: &ApiResponse, max: u64) -> Result<()> {
    let bytes = serde_json::to_vec(response)?;
    if bytes.len() as u64 > max || bytes.len() > u32::MAX as usize {
        anyhow::bail!("control API response exceeds configured bound");
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    stream.flush()?;
    Ok(())
}

fn peer_uid(stream: &UnixStream) -> Result<u32> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error()).context("read Unix-socket peer credentials");
    }
    Ok(credentials.uid)
}

fn verify_socket_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        anyhow::bail!("control socket path must be absolute and normalized");
    }
    let mut current = PathBuf::new();
    for component in path
        .parent()
        .context("control socket has no parent")?
        .components()
    {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                anyhow::bail!(
                    "control socket path contains a symlink: {}",
                    current.display()
                )
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error).context("inspect control socket path"),
        }
    }
    Ok(())
}

struct DaemonLease {
    file: fs::File,
}

impl DaemonLease {
    fn acquire(parent: &Path) -> Result<Self> {
        let path = parent.join("daemon.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("acquire exclusive IQ daemon lease");
        }
        Ok(Self { file })
    }
}

impl Drop for DaemonLease {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

fn remove_stale_socket(path: &Path, _lease: &DaemonLease) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("inspect existing control socket"),
    };
    if !metadata.file_type().is_socket()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        anyhow::bail!("unexpected existing control socket path identity");
    }
    if UnixStream::connect(path).is_ok() {
        anyhow::bail!("a live IQ daemon owns the control socket");
    }
    fs::remove_file(path)?;
    Ok(())
}
