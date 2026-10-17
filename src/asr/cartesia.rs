use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_util::sync::CancellationToken;

use super::{
    AsrAudioInput, AsrAudioPush, AsrDuplexLimits, AsrDuplexSession, AsrEvent, AsrEventStream,
    AsrInputError, AsrOpenError, AsrSession, AsrSessionConfig, LegacyDuplexFacade, StreamingAsr,
};
use crate::config::AsrConfig;
use crate::ids::Revision;
use crate::provider::OwnedProviderTasks;

const MESSAGE_BYTES: usize = 262_144;
const MAX_COMMANDS: usize = 64;
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Sink = futures::stream::SplitSink<Socket, Message>;
type Terminal = Result<(), AsrEvent>;

/// Cartesia Ink Whisper manual streaming with independent PCM and transcript handles.
/// FIFO flush attribution is an integration assumption to verify with the ignored paid contract.
pub struct CartesiaStreamingAsr {
    config: AsrConfig,
    authorization: tokio_tungstenite::tungstenite::http::HeaderValue,
}

impl CartesiaStreamingAsr {
    pub fn from_config(config: &AsrConfig) -> anyhow::Result<Self> {
        validate_config(config)?;
        let key = std::env::var(&config.api_key_env)
            .map_err(|_| anyhow::anyhow!("missing env {}", config.api_key_env))?;
        Self::with_api_key(config, key)
    }

    /// Constructs a host-owned factory without reading credential environment variables.
    pub fn with_api_key(config: &AsrConfig, api_key: String) -> anyhow::Result<Self> {
        validate_config(config)?;
        anyhow::ensure!(!api_key.trim().is_empty(), "Cartesia API key is empty");
        let mut authorization = format!("Bearer {api_key}")
            .parse::<tokio_tungstenite::tungstenite::http::HeaderValue>()
            .map_err(|_| anyhow::anyhow!("invalid Cartesia authorization header"))?;
        authorization.set_sensitive(true);
        Ok(Self {
            config: config.clone(),
            authorization,
        })
    }
}

fn validate_config(config: &AsrConfig) -> anyhow::Result<()> {
    let request = config
        .endpoint
        .as_str()
        .into_client_request()
        .map_err(|_| anyhow::anyhow!("invalid Cartesia ASR endpoint"))?;
    let uri = request.uri();
    anyhow::ensure!(
        matches!(uri.scheme_str(), Some("ws" | "wss"))
            && uri.authority().is_some_and(|a| !a.as_str().contains('@')),
        "invalid Cartesia ASR endpoint"
    );
    anyhow::ensure!(
        uri.scheme_str() == Some("wss")
            || matches!(uri.host(), Some("localhost" | "127.0.0.1" | "[::1]")),
        "Cartesia ASR requires WSS outside loopback"
    );
    anyhow::ensure!(
        uri.query().is_none(),
        "Cartesia ASR endpoint must not contain a query"
    );
    anyhow::ensure!(
        config.model == "ink-whisper",
        "Cartesia ASR requires ink-whisper"
    );
    anyhow::ensure!(
        config.manual_commit,
        "Cartesia ASR requires manual_commit: true"
    );
    language(&config.locale)?;
    anyhow::ensure!(
        config.cartesia.api_version == "2026-08-14",
        "unsupported Cartesia ASR API version"
    );
    for (name, millis) in [
        ("open_timeout_ms", config.open_timeout_ms),
        ("write_timeout_ms", config.write_timeout_ms),
        ("finalize_timeout_ms", config.cartesia.finalize_timeout_ms),
        ("finish_timeout_ms", config.cartesia.finish_timeout_ms),
    ] {
        anyhow::ensure!(
            millis > 0
                && millis <= u64::MAX / 1_000
                && Instant::now()
                    .checked_add(Duration::from_millis(millis))
                    .is_some(),
            "invalid Cartesia ASR {name}"
        );
    }
    anyhow::ensure!(
        config.input_buffer_ms > 0
            && config
                .input_buffer_ms
                .checked_mul(16)
                .is_some_and(|n| n > 0 && n <= u32::MAX as u64),
        "invalid Cartesia ASR input buffer"
    );
    anyhow::ensure!(
        config.chunk_ms > 0 && config.cartesia.max_transcript_bytes > 0,
        "Cartesia chunk/transcript limits must be positive"
    );
    Ok(())
}

fn language(locale: &str) -> anyhow::Result<&'static str> {
    match locale.trim().to_ascii_lowercase().as_str() {
        "ko" | "ko-kr" => Ok("ko"),
        _ => anyhow::bail!("Cartesia ASR currently supports Korean locale only"),
    }
}

#[async_trait]
impl StreamingAsr for CartesiaStreamingAsr {
    async fn open(&self, config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>> {
        let session = self
            .open_duplex(
                config,
                AsrDuplexLimits {
                    sample_budget: (self.config.input_buffer_ms * 16) as usize,
                    command_budget: MAX_COMMANDS,
                    write_timeout: Duration::from_millis(self.config.write_timeout_ms),
                },
                CancellationToken::new(),
            )
            .await?;
        Ok(Box::new(LegacyDuplexFacade::new(session)))
    }

    fn supports_duplex(&self) -> bool {
        true
    }

    async fn open_duplex(
        &self,
        config: AsrSessionConfig,
        limits: AsrDuplexLimits,
        cancellation: CancellationToken,
    ) -> Result<AsrDuplexSession, AsrOpenError> {
        if cancellation.is_cancelled()
            || config.sample_rate_hz != 16_000
            || !config.manual_commit
            || language(&config.locale).is_err()
            || limits.sample_budget == 0
            || limits.sample_budget > u32::MAX as usize
            || limits.command_budget == 0
            || limits.write_timeout.is_zero()
            || Instant::now().checked_add(limits.write_timeout).is_none()
        {
            return Err(AsrOpenError::Rejected(
                "Cartesia ASR requires 16 kHz Korean manual input and positive bounded limits"
                    .into(),
            ));
        }
        let endpoint = format!(
            "{}?model=ink-whisper&encoding=pcm_s16le&sample_rate=16000&language=ko",
            self.config.endpoint
        );
        let mut request = endpoint
            .into_client_request()
            .map_err(|_| AsrOpenError::Rejected("invalid Cartesia ASR endpoint".into()))?;
        request
            .headers_mut()
            .insert("Authorization", self.authorization.clone());
        request.headers_mut().insert(
            "Cartesia-Version",
            self.config
                .cartesia
                .api_version
                .parse()
                .expect("validated version"),
        );
        let mut ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
        ws_config.max_message_size = Some(MESSAGE_BYTES);
        ws_config.max_frame_size = Some(MESSAGE_BYTES);
        let connected = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(AsrOpenError::Rejected("Cartesia ASR open cancelled".into())),
            result = tokio::time::timeout(Duration::from_millis(self.config.open_timeout_ms), tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false)) => result,
        };
        let (socket, _) = connected
            .map_err(|_| AsrOpenError::TimedOut("Cartesia ASR open timed out".into()))?
            .map_err(open_error)?;
        let token = cancellation.child_token();
        let budget = limits.command_budget.min(MAX_COMMANDS);
        let (commands, command_rx) = mpsc::channel(budget);
        let (events_tx, events_rx) = mpsc::channel(16);
        let (terminal_tx, terminal_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = watch::channel(None);
        let (flush_tx, flush_rx) = watch::channel(None);
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let (close_tx, close_rx) = watch::channel(0u8);
        let settings = Settings {
            chunk_samples: (self.config.chunk_ms.min(100) * 16) as usize,
            write_timeout: limits.write_timeout,
            finalize_timeout: Duration::from_millis(self.config.cartesia.finalize_timeout_ms),
            max_transcript_bytes: self.config.cartesia.max_transcript_bytes,
        };
        let coordinator = Coordinator {
            pending,
            deadline: flush_tx,
        };
        let worker_token = token.clone();
        let worker = tokio::spawn(async move {
            let (mut sink, stream) = socket.split();
            let (pongs_tx, pongs_rx) = mpsc::channel(4);
            let outcome = {
                tokio::select! {
                    biased;
                    _ = worker_token.cancelled() => None,
                    _ = deadline_expired(finish_rx) => Some(Err(error(true, "Cartesia ASR finish timed out"))),
                    _ = deadline_expired(flush_rx) => Some(Err(error(true, "Cartesia ASR finalize timed out"))),
                    result = read_events(stream, events_tx, pongs_tx, &coordinator, close_rx, settings.max_transcript_bytes) => Some(result),
                    result = write_audio(&mut sink, command_rx, pongs_rx, &coordinator, close_tx, settings) => Some(Err(result.err().unwrap_or_else(|| error(true, "Cartesia ASR writer ended")))),
                }
            };
            if let Some(outcome) = outcome {
                let failed = outcome.is_err();
                let _ = terminal_tx.send(outcome);
                if failed {
                    worker_token.cancel();
                }
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), sink.close()).await;
        });
        Ok(AsrDuplexSession {
            input: Box::new(Input {
                commands: Some(commands),
                samples: Arc::new(Semaphore::new(limits.sample_budget)),
                slots: Arc::new(Semaphore::new(budget)),
                sample_budget: limits.sample_budget,
                token: token.clone(),
                finished: false,
                finish: finish_tx,
                finish_timeout: Duration::from_millis(self.config.cartesia.finish_timeout_ms),
            }),
            events: Box::new(Events {
                events: events_rx,
                terminal: Some(terminal_rx),
                ended: false,
                retained: VecDeque::new(),
                token: token.clone(),
            }),
            control: Box::new(OwnedProviderTasks {
                token,
                tasks: vec![worker],
            }),
        })
    }
}

fn open_error(failure: tokio_tungstenite::tungstenite::Error) -> AsrOpenError {
    if let tokio_tungstenite::tungstenite::Error::Http(response) = failure {
        let status = response.status().as_u16();
        let body = response
            .body()
            .as_ref()
            .and_then(|b| serde_json::from_slice::<Value>(b).ok());
        let code = body
            .as_ref()
            .and_then(|v| v.get("error_code"))
            .and_then(Value::as_str);
        let message = diagnostic(status, code);
        if retryable(status, code) {
            AsrOpenError::Transient(message)
        } else {
            AsrOpenError::Rejected(message)
        }
    } else {
        AsrOpenError::Transient("Cartesia ASR connection failed".into())
    }
}
fn retryable(status: u16, code: Option<&str>) -> bool {
    status >= 500
        || (status == 429 && !matches!(code, Some("quota_exceeded" | "plan_upgrade_required")))
}
fn diagnostic(status: u16, code: Option<&str>) -> String {
    let code = code
        .filter(|c| {
            !c.is_empty()
                && c.len() <= 64
                && c.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
        .unwrap_or("unknown");
    format!("Cartesia ASR error (status={status}, code={code})")
}
fn error(recoverable: bool, message: impl Into<String>) -> AsrEvent {
    AsrEvent::Error {
        recoverable,
        message: message.into(),
    }
}

struct Settings {
    chunk_samples: usize,
    write_timeout: Duration,
    finalize_timeout: Duration,
    max_transcript_bytes: usize,
}
enum Command {
    Audio {
        pcm: Vec<i16>,
        _samples: OwnedSemaphorePermit,
        _slot: OwnedSemaphorePermit,
    },
    Commit(OwnedSemaphorePermit),
}

struct Input {
    commands: Option<mpsc::Sender<Command>>,
    samples: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    sample_budget: usize,
    token: CancellationToken,
    finished: bool,
    finish: watch::Sender<Option<Instant>>,
    finish_timeout: Duration,
}
impl Drop for Input {
    fn drop(&mut self) {
        if !self.finished {
            self.token.cancel();
        }
    }
}
impl Input {
    fn check(&self) -> Result<(), AsrInputError> {
        if self.finished || self.token.is_cancelled() {
            Err(AsrInputError::Closed)
        } else {
            Ok(())
        }
    }
    fn send_audio(
        &self,
        pcm: Vec<i16>,
        samples: OwnedSemaphorePermit,
        slot: OwnedSemaphorePermit,
    ) -> Result<AsrAudioPush, AsrInputError> {
        match self
            .commands
            .as_ref()
            .ok_or(AsrInputError::Closed)?
            .try_send(Command::Audio {
                pcm,
                _samples: samples,
                _slot: slot,
            }) {
            Ok(()) => Ok(AsrAudioPush::Accepted),
            Err(mpsc::error::TrySendError::Full(Command::Audio { pcm, .. })) => {
                Ok(AsrAudioPush::Full(pcm))
            }
            Err(_) => Err(AsrInputError::Closed),
        }
    }
}
#[async_trait]
impl AsrAudioInput for Input {
    fn try_push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        self.check()?;
        if pcm.is_empty() {
            return Ok(AsrAudioPush::Accepted);
        }
        if pcm.len() > self.sample_budget {
            return Ok(AsrAudioPush::Full(pcm));
        }
        let Ok(samples) = self
            .samples
            .clone()
            .try_acquire_many_owned(pcm.len() as u32)
        else {
            return Ok(AsrAudioPush::Full(pcm));
        };
        let Ok(slot) = self.slots.clone().try_acquire_owned() else {
            return Ok(AsrAudioPush::Full(pcm));
        };
        self.send_audio(pcm, samples, slot)
    }
    async fn push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        self.check()?;
        if pcm.is_empty() {
            return Ok(AsrAudioPush::Accepted);
        }
        if pcm.len() > self.sample_budget {
            return Ok(AsrAudioPush::Full(pcm));
        }
        let acquire = async {
            let samples = self
                .samples
                .clone()
                .acquire_many_owned(pcm.len() as u32)
                .await
                .map_err(|_| AsrInputError::Closed)?;
            let slot = self
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| AsrInputError::Closed)?;
            Ok::<_, AsrInputError>((samples, slot))
        };
        let (samples, slot) = tokio::select! { biased; _ = self.token.cancelled() => return Err(AsrInputError::Cancelled), result = acquire => result?, };
        self.send_audio(pcm, samples, slot)
    }
    fn try_commit_utterance(&mut self) -> Result<(), AsrInputError> {
        self.check()?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| AsrInputError::Full)?;
        self.commands
            .as_ref()
            .ok_or(AsrInputError::Closed)?
            .try_send(Command::Commit(slot))
            .map_err(admission_error)
    }
    async fn commit_utterance(&mut self) -> Result<(), AsrInputError> {
        self.check()?;
        let slot = tokio::select! { biased; _ = self.token.cancelled() => return Err(AsrInputError::Cancelled), slot = self.slots.clone().acquire_owned() => slot.map_err(|_| AsrInputError::Closed)?, };
        self.commands
            .as_ref()
            .ok_or(AsrInputError::Closed)?
            .try_send(Command::Commit(slot))
            .map_err(admission_error)
    }
    fn finish_input(&mut self) -> Result<(), AsrInputError> {
        if self.finished {
            return Ok(());
        }
        self.check()?;
        self.finish
            .send_replace(Some(Instant::now() + self.finish_timeout));
        self.finished = true;
        self.commands.take();
        Ok(())
    }
}
fn admission_error<T>(error: mpsc::error::TrySendError<T>) -> AsrInputError {
    match error {
        mpsc::error::TrySendError::Full(_) => AsrInputError::Full,
        mpsc::error::TrySendError::Closed(_) => AsrInputError::Closed,
    }
}

struct Flush {
    id: u64,
    deadline: Instant,
    _slot: OwnedSemaphorePermit,
}
struct Coordinator {
    pending: Arc<Mutex<VecDeque<Flush>>>,
    deadline: watch::Sender<Option<Instant>>,
}
impl Coordinator {
    fn update(&self, pending: &VecDeque<Flush>) {
        self.deadline
            .send_replace(pending.iter().map(|f| f.deadline).min());
    }
    fn register(&self, id: u64, timeout: Duration, slot: OwnedSemaphorePermit) {
        let mut pending = self.pending.lock();
        pending.push_back(Flush {
            id,
            deadline: Instant::now() + timeout,
            _slot: slot,
        });
        self.update(&pending);
    }
    fn sent(&self, id: u64, timeout: Duration) {
        let mut pending = self.pending.lock();
        if let Some(flush) = pending.iter_mut().find(|flush| flush.id == id) {
            flush.deadline = Instant::now() + timeout;
        }
        self.update(&pending);
    }
    fn acknowledge(&self) -> Result<(), AsrEvent> {
        let mut pending = self.pending.lock();
        pending
            .pop_front()
            .ok_or_else(|| error(false, "Cartesia ASR unsolicited flush_done"))?;
        self.update(&pending);
        Ok(())
    }
}

async fn deadline_expired(mut deadline: watch::Receiver<Option<Instant>>) {
    loop {
        let current = *deadline.borrow_and_update();
        if let Some(current) = current {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(current) => return,
                changed = deadline.changed() => {
                                    if changed.is_err() { tokio::time::sleep_until(current).await; return; }
                                }
            }
        } else if deadline.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn send(sink: &mut Sink, message: Message, timeout: Duration) -> Result<(), AsrEvent> {
    tokio::time::timeout(timeout, sink.send(message))
        .await
        .map_err(|_| error(true, "Cartesia ASR write timed out"))?
        .map_err(|_| error(true, "Cartesia ASR write failed"))
}

async fn pace(
    sink: &mut Sink,
    pongs: &mut mpsc::Receiver<bytes::Bytes>,
    due: Instant,
    timeout: Duration,
) -> Result<(), AsrEvent> {
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(due) => return Ok(()),
            pong = pongs.recv(), if !pongs.is_closed() => {
                if let Some(pong) = pong { send(sink, Message::Pong(pong), timeout).await?; }
            }
        }
    }
}

async fn write_audio(
    sink: &mut Sink,
    mut commands: mpsc::Receiver<Command>,
    mut pongs: mpsc::Receiver<bytes::Bytes>,
    coordinator: &Coordinator,
    close: watch::Sender<u8>,
    settings: Settings,
) -> Result<(), AsrEvent> {
    let mut next_audio = Instant::now();
    let mut marker = 0u64;
    let mut finished = false;
    loop {
        tokio::select! {
            pong = pongs.recv(), if !pongs.is_closed() => {
                if let Some(pong) = pong { send(sink, Message::Pong(pong), settings.write_timeout).await?; }
            },
            command = commands.recv(), if !finished => match command {
                Some(Command::Audio { pcm, _samples, _slot }) => {
                    for chunk in pcm.chunks(settings.chunk_samples) {
                        pace(sink, &mut pongs, next_audio, settings.write_timeout).await?;
                        let duration = Duration::from_secs_f64(chunk.len() as f64 / 16_000.0);
                        if Instant::now().saturating_duration_since(next_audio) >= duration { next_audio = Instant::now(); }
                        let bytes: Vec<u8> = chunk.iter().flat_map(|s| s.to_le_bytes()).collect();
                        send(sink, Message::Binary(bytes.into()), settings.write_timeout).await?;
                        next_audio += duration;
                    }
                    drop((_samples, _slot));
                }
                Some(Command::Commit(slot)) => {
                    marker = marker.checked_add(1).ok_or_else(|| error(false, "Cartesia ASR marker overflow"))?;
                    coordinator.register(marker, settings.write_timeout, slot);
                    send(sink, Message::Text("finalize".into()), settings.write_timeout).await?;
                    coordinator.sent(marker, settings.finalize_timeout);
                }
                None => {
                    let mut changes = coordinator.deadline.subscribe();
                    while !coordinator.pending.lock().is_empty() {
                        tokio::select! {
                            changed = changes.changed() => { changed.map_err(|_| error(true, "Cartesia ASR flush coordinator closed"))?; }
                            pong = pongs.recv(), if !pongs.is_closed() => { if let Some(pong) = pong { send(sink, Message::Pong(pong), settings.write_timeout).await?; } }
                        }
                    }
                    close.send_replace(1);
                    send(sink, Message::Text("close".into()), settings.write_timeout).await?;
                    close.send_replace(2);
                    finished = true;
                }
            },
            else => std::future::pending::<()>().await,
        }
    }
}

#[derive(Default)]
struct Transcript {
    prefix: String,
    tail: String,
    last_snapshot: String,
    utterance: u64,
    revision: u64,
}
impl Transcript {
    fn partial(
        &mut self,
        text: &str,
        final_delta: bool,
        limit: usize,
    ) -> Result<Option<AsrEvent>, AsrEvent> {
        if text.len() > limit.saturating_sub(self.prefix.len()) {
            return Err(error(false, "Cartesia ASR transcript exceeds byte budget"));
        }
        if final_delta {
            self.prefix.push_str(text);
            self.tail.clear();
        } else {
            self.tail = text.to_owned();
        }
        let snapshot = format!("{}{}", self.prefix, self.tail);
        if snapshot == self.last_snapshot {
            return Ok(None);
        }
        self.last_snapshot.clone_from(&snapshot);
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| error(false, "Cartesia ASR revision overflow"))?;
        Ok(Some(AsrEvent::Partial {
            utterance_id: self.utterance,
            revision: Revision(self.revision),
            text: snapshot,
        }))
    }
    fn finish(&mut self) -> Result<AsrEvent, AsrEvent> {
        let event = AsrEvent::Final {
            utterance_id: self.utterance,
            text: std::mem::take(&mut self.prefix),
        };
        self.tail.clear();
        self.last_snapshot.clear();
        self.utterance = self
            .utterance
            .checked_add(1)
            .ok_or_else(|| error(false, "Cartesia ASR utterance overflow"))?;
        Ok(event)
    }
}

async fn emit(events: &mpsc::Sender<AsrEvent>, event: AsrEvent) -> Result<(), AsrEvent> {
    events
        .send(event)
        .await
        .map_err(|_| error(false, "Cartesia ASR output closed"))
}

async fn read_events(
    mut stream: futures::stream::SplitStream<Socket>,
    events: mpsc::Sender<AsrEvent>,
    pongs: mpsc::Sender<bytes::Bytes>,
    coordinator: &Coordinator,
    mut close: watch::Receiver<u8>,
    limit: usize,
) -> Terminal {
    let mut transcript = Transcript::default();
    let mut connection_id: Option<String> = None;
    while let Some(message) = stream.next().await {
        match message.map_err(|_| error(true, "Cartesia ASR receive failed"))? {
            Message::Ping(data) => {
                pongs
                    .send(data)
                    .await
                    .map_err(|_| error(true, "Cartesia ASR pong writer closed"))?;
            }
            Message::Pong(_) => {}
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text)
                    .map_err(|_| error(false, "malformed Cartesia ASR JSON"))?;
                let kind = value
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error(false, "missing Cartesia ASR event type"))?;
                if kind == "error" {
                    let status = value
                        .get("status_code")
                        .and_then(Value::as_u64)
                        .and_then(|s| u16::try_from(s).ok())
                        .ok_or_else(|| error(false, "invalid Cartesia ASR error status"))?;
                    let code = value.get("error_code").and_then(Value::as_str);
                    return Err(error(retryable(status, code), diagnostic(status, code)));
                }
                if !matches!(kind, "transcript" | "flush_done" | "done") {
                    continue;
                }
                let id = value
                    .get("request_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty() && id.len() <= 1024)
                    .ok_or_else(|| error(false, "invalid Cartesia ASR connection identity"))?;
                if let Some(expected) = &connection_id {
                    if expected != id {
                        return Err(error(false, "Cartesia ASR connection identity changed"));
                    }
                } else {
                    connection_id = Some(id.to_owned());
                }
                match kind {
                    "transcript" => {
                        let text = value
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(|| error(false, "missing Cartesia ASR transcript text"))?;
                        let final_delta = value
                            .get("is_final")
                            .and_then(Value::as_bool)
                            .ok_or_else(|| error(false, "missing Cartesia ASR is_final"))?;
                        if let Some(event) = transcript.partial(text, final_delta, limit)? {
                            emit(&events, event).await?;
                        }
                    }
                    "flush_done" => {
                        coordinator.acknowledge()?;
                        emit(&events, transcript.finish()?).await?;
                    }
                    "done" => {
                        if *close.borrow() == 0 || !coordinator.pending.lock().is_empty() {
                            return Err(error(false, "Cartesia ASR done before close/flush drain"));
                        }
                        while *close.borrow_and_update() != 2 {
                            close
                                .changed()
                                .await
                                .map_err(|_| error(true, "Cartesia ASR close write failed"))?;
                        }
                        if !transcript.prefix.is_empty() {
                            emit(&events, transcript.finish()?).await?;
                        }
                        return Ok(());
                    }
                    _ => unreachable!(),
                }
            }
            Message::Close(_) => break,
            _ => return Err(error(false, "unexpected Cartesia ASR binary response")),
        }
    }
    Err(error(true, "Cartesia ASR socket ended without done"))
}

struct Events {
    events: mpsc::Receiver<AsrEvent>,
    terminal: Option<oneshot::Receiver<Terminal>>,
    ended: bool,
    retained: VecDeque<AsrEvent>,
    token: CancellationToken,
}
impl Drop for Events {
    fn drop(&mut self) {
        self.token.cancel();
    }
}
impl Events {
    fn terminal(
        &mut self,
        result: Result<Terminal, oneshot::error::RecvError>,
    ) -> Option<AsrEvent> {
        self.terminal = None;
        match result {
            Ok(Ok(())) => None,
            other => {
                self.ended = true;
                self.events.close();
                let failed = matches!(other, Ok(Err(_)));
                while let Ok(event) = self.events.try_recv() {
                    if failed && matches!(event, AsrEvent::Final { .. }) {
                        self.retained.push_back(event);
                    }
                }
                if let Ok(Err(error)) = other {
                    self.retained.push_back(error);
                }
                self.retained.pop_front()
            }
        }
    }
}
#[async_trait]
impl AsrEventStream for Events {
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        if let Some(event) = self.retained.pop_front() {
            return Ok(Some(event));
        }
        if self.ended {
            return Ok(None);
        }
        if self.terminal.is_none() {
            return Ok(self.events.recv().await);
        }
        tokio::select! {
            biased;
            terminal = self.terminal.as_mut().expect("checked above") => {
                if let Some(error) = self.terminal(terminal) { return Ok(Some(error)); }
                if self.ended { return Ok(None); }
                Ok(self.events.recv().await)
            }
            event = self.events.recv() => {
                if event.is_some() { return Ok(event); }
                let terminal = self.terminal.as_mut().expect("terminal still present").await;
                Ok(self.terminal(terminal))
            }
        }
    }
}
