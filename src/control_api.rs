use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
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
}

pub struct DaemonLifetime {
    _daemon_lease: DaemonLease,
    _database_lease: crate::control_store::DatabaseProcessLease,
}

#[derive(Clone)]
struct ApiShutdown {
    state: Arc<Mutex<ApiShutdownState>>,
}

struct ApiShutdownState {
    requested: bool,
    next_stream_id: u64,
    streams: BTreeMap<u64, UnixStream>,
}

struct StreamRegistration {
    shutdown: ApiShutdown,
    stream_id: Option<u64>,
}

impl ApiShutdown {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ApiShutdownState {
                requested: false,
                next_stream_id: 0,
                streams: BTreeMap::new(),
            })),
        }
    }

    fn register(&self, stream: &UnixStream) -> Result<StreamRegistration> {
        let registered = stream.try_clone()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("control API shutdown lock poisoned"))?;
        if state.requested {
            drop(state);
            stream.shutdown(Shutdown::Both)?;
            drop(registered);
            return Ok(StreamRegistration {
                shutdown: self.clone(),
                stream_id: None,
            });
        }
        let stream_id = state.next_stream_id;
        state.next_stream_id = state
            .next_stream_id
            .checked_add(1)
            .context("control API stream identity exhausted")?;
        state.streams.insert(stream_id, registered);
        Ok(StreamRegistration {
            shutdown: self.clone(),
            stream_id: Some(stream_id),
        })
    }

    fn request(&self) -> Result<()> {
        let streams = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("control API shutdown lock poisoned"))?;
            state.requested = true;
            std::mem::take(&mut state.streams)
        };
        let mut first_error = None;
        for (_, stream) in streams {
            if let Err(error) = stream.shutdown(Shutdown::Both) {
                record_first_error(&mut first_error, error.into());
            }
        }
        finish_errors(first_error)
    }

    fn is_requested(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("control API shutdown lock poisoned"))?
            .requested)
    }

    fn unregister(&self, stream_id: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.streams.remove(&stream_id);
        }
    }
}

impl Drop for StreamRegistration {
    fn drop(&mut self) {
        if let Some(stream_id) = self.stream_id.take() {
            self.shutdown.unregister(stream_id);
        }
    }
}

struct ConnectionWorker {
    handle: thread::JoinHandle<Result<()>>,
}

struct ApiTaskTree {
    shutdown: ApiShutdown,
    workers: Vec<ConnectionWorker>,
}

impl ApiTaskTree {
    fn new() -> Self {
        Self {
            shutdown: ApiShutdown::new(),
            workers: Vec::new(),
        }
    }
}

impl Drop for ApiTaskTree {
    fn drop(&mut self) {
        let _ = self.shutdown.request();
        for worker in self.workers.drain(..) {
            let _ = worker.handle.join();
        }
    }
}

impl ControlApiServer {
    pub fn bind(config: ControlPlaneConfig, store: ControlStore) -> Result<(DaemonLifetime, Self)> {
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
        Ok((
            DaemonLifetime {
                _daemon_lease: lease,
                _database_lease: database_lease,
            },
            Self {
                listener,
                config,
                store: Arc::new(store),
            },
        ))
    }

    pub fn serve(self, shutdown: mpsc::Receiver<()>) -> Result<()> {
        self.serve_inner(shutdown, false)
    }

    #[doc(hidden)]
    #[cfg(debug_assertions)]
    pub fn serve_failure_for_test(self) -> Result<()> {
        let (_shutdown, receiver) = mpsc::channel();
        self.serve_inner(receiver, true)
    }

    fn serve_inner(self, shutdown: mpsc::Receiver<()>, fail_for_test: bool) -> Result<()> {
        self.listener.set_nonblocking(true)?;
        let permits = Arc::new(std::sync::Mutex::new(0_u32));
        let mut task_tree = ApiTaskTree::new();
        let mut first_error = None;
        loop {
            collect_finished_workers(&mut task_tree.workers, &mut first_error);
            if first_error.is_some() {
                break;
            }
            if let Err(error) = fail_control_api_serve_for_test(fail_for_test) {
                record_first_error(&mut first_error, error);
                break;
            }
            let stream = match self.listener.accept() {
                Ok((stream, _)) => stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    match shutdown.recv_timeout(Duration::from_millis(50)) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    }
                }
                Err(error) => {
                    record_first_error(&mut first_error, error.into());
                    break;
                }
            };
            let registration = match task_tree.shutdown.register(&stream) {
                Ok(registration) => registration,
                Err(error) => {
                    record_first_error(&mut first_error, error);
                    break;
                }
            };
            if let Err(error) = fail_control_api_serve_for_test(fail_for_test) {
                record_first_error(&mut first_error, error);
                break;
            }
            let mut active = match permits.lock() {
                Ok(active) => active,
                Err(_) => {
                    record_first_error(
                        &mut first_error,
                        anyhow::anyhow!("API permit lock poisoned"),
                    );
                    break;
                }
            };
            if *active >= self.config.max_concurrent_clients {
                drop(active);
                let mut stream = stream;
                if let Err(error) = write_response(
                    &mut stream,
                    &ApiResponse {
                        version: 1,
                        ok: false,
                        result: json!({"error":"too_many_clients"}),
                    },
                    self.config.max_response_bytes,
                ) {
                    record_first_error(&mut first_error, error);
                    break;
                }
                drop(registration);
                continue;
            }
            *active += 1;
            drop(active);
            let store = self.store.clone();
            let config = self.config.clone();
            let permits = permits.clone();
            let worker_shutdown = task_tree.shutdown.clone();
            task_tree.workers.push(ConnectionWorker {
                handle: thread::spawn(move || {
                    let result =
                        handle_connection(stream, &config, &store, &worker_shutdown, registration);
                    let result = match worker_shutdown.is_requested() {
                        Ok(true) => Ok(()),
                        Ok(false) => result,
                        Err(error) => result.and(Err(error)),
                    };
                    let permit_result = permits
                        .lock()
                        .map_err(|_| anyhow::anyhow!("API permit lock poisoned"))
                        .map(|mut active| *active = active.saturating_sub(1));
                    result.and(permit_result)
                }),
            });
        }
        if let Err(error) = task_tree.shutdown.request() {
            record_first_error(&mut first_error, error);
        }
        join_all_workers(&mut task_tree.workers, &mut first_error);
        finish_errors(first_error)
    }

    pub fn serve_one(self) -> Result<()> {
        let (stream, _) = self.listener.accept()?;
        let shutdown = ApiShutdown::new();
        let registration = shutdown.register(&stream)?;
        handle_connection(stream, &self.config, &self.store, &shutdown, registration)
    }
}

fn collect_finished_workers(
    workers: &mut Vec<ConnectionWorker>,
    first_error: &mut Option<anyhow::Error>,
) {
    let mut index = 0;
    while index < workers.len() {
        if workers[index].handle.is_finished() {
            let worker = workers.remove(index);
            record_join_result(
                first_error,
                worker.handle.join(),
                "IQ control API worker panicked",
            );
        } else {
            index += 1;
        }
    }
}

fn join_all_workers(workers: &mut Vec<ConnectionWorker>, first_error: &mut Option<anyhow::Error>) {
    for worker in workers.drain(..) {
        record_join_result(
            first_error,
            worker.handle.join(),
            "IQ control API worker panicked",
        );
    }
}

fn record_join_result(
    first_error: &mut Option<anyhow::Error>,
    result: std::thread::Result<Result<()>>,
    panic_message: &str,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => record_first_error(first_error, error),
        Err(_) => record_first_error(first_error, anyhow::anyhow!(panic_message.to_string())),
    }
}

fn record_first_error(first_error: &mut Option<anyhow::Error>, error: anyhow::Error) {
    if first_error.is_none() {
        *first_error = Some(error);
    }
}

fn finish_errors(first_error: Option<anyhow::Error>) -> Result<()> {
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn fail_control_api_serve_for_test(_fail: bool) -> Result<()> {
    #[cfg(debug_assertions)]
    if _fail {
        anyhow::bail!("simulated IQ control API failure");
    }
    #[cfg(debug_assertions)]
    if let Some(path) = std::env::var_os("IQ_TEST_CONTROL_API_FAILURE_TRIGGER") {
        match fs::symlink_metadata(path) {
            Ok(_) => anyhow::bail!("simulated IQ control API failure"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("inspect control API failure trigger"),
        }
    }
    Ok(())
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
    shutdown: &ApiShutdown,
    _registration: StreamRegistration,
) -> Result<()> {
    if shutdown.is_requested()? {
        return Ok(());
    }
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
        return stream_events(&mut stream, config, store, shutdown, cursor, limit);
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
    shutdown: &ApiShutdown,
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
    let (sender, receiver) = mpsc::sync_channel::<ApiResponse>(message_bound);
    let backpressure = Arc::new(AtomicBool::new(false));
    let producer_backpressure = backpressure.clone();
    let producer_store = store.clone();
    let producer_shutdown = shutdown.clone();
    let producer_max = config.max_client_queue_bytes.min(config.max_response_bytes);
    let (producer_stop, producer_stop_receiver) = mpsc::channel();
    let producer = thread::spawn(move || -> Result<()> {
        let mut selected_cursor = cursor;
        loop {
            if producer_shutdown.is_requested()? {
                return Ok(());
            }
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
                Ok(Some(response)) => match sender.try_send(response) {
                    Ok(()) => {}
                    Err(mpsc::TrySendError::Full(_)) => {
                        producer_backpressure.store(true, Ordering::Release);
                        return Ok(());
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => return Ok(()),
                },
                Ok(None) if producer_backpressure.load(Ordering::Acquire) => return Ok(()),
                Ok(None) => match producer_stop_receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                },
                Err(error) => return Err(error),
            }
        }
    });
    let idle = Duration::from_secs(config.client_idle_seconds);
    let mut sent_cursor = cursor;
    let mut last_activity = Instant::now();
    let stream_result = (|| -> Result<()> {
        loop {
            if shutdown.is_requested()? {
                return Ok(());
            }
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
                Ok(response) => {
                    write_response(stream, &response, config.max_response_bytes)?;
                    sent_cursor = response.result["cursor"].as_u64().unwrap_or(sent_cursor);
                    last_activity = Instant::now();
                }
                Err(mpsc::RecvTimeoutError::Disconnected)
                    if backpressure.load(Ordering::Acquire) => {}
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
    })();
    let _ = producer_stop.send(());
    drop(receiver);
    let producer_result = producer
        .join()
        .map_err(|_| anyhow::anyhow!("IQ control API event producer panicked"))?;
    stream_result.and(producer_result)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_registration_after_shutdown_closes_immediately() {
        let shutdown = ApiShutdown::new();
        shutdown.request().unwrap();
        let (stream, mut peer) = UnixStream::pair().unwrap();

        let _registration = shutdown.register(&stream).unwrap();

        peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(peer.read(&mut byte).unwrap(), 0);
    }
}
