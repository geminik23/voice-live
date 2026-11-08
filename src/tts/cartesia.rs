use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_util::sync::CancellationToken;

use super::{
    AudioNormalizer, MAX_RAW_PCM_BYTES, StreamingTts, TextInputMode, TtsDuplexSession, TtsEvent,
    TtsInputError, TtsInputLimits, TtsOpenError, TtsRequest, TtsSessionOptions, TtsStream,
    TtsTextInput,
};
use crate::config::TtsConfig;
use crate::provider::OwnedProviderTasks;

const COMMAND_BUDGET: usize = 64;
const MESSAGE_BYTES: usize = 262_144;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Sink = futures::stream::SplitSink<Socket, Message>;

/// Native incremental Cartesia synthesis with one context and socket per response.
/// Endpoint behavior is covered offline; paid-provider suitability requires the ignored contract test.
pub struct CartesiaStreamingTts {
    config: TtsConfig,
    authorization: tokio_tungstenite::tungstenite::http::HeaderValue,
}

impl CartesiaStreamingTts {
    pub fn from_config(config: &TtsConfig) -> anyhow::Result<Self> {
        validate_config(config)?;
        let key = std::env::var(&config.api_key_env)
            .map_err(|_| anyhow::anyhow!("missing env {}", config.api_key_env))?;
        Self::with_api_key(config, key)
    }

    /// Constructs a host-owned factory without consulting credential environment variables.
    pub fn with_api_key(config: &TtsConfig, api_key: String) -> anyhow::Result<Self> {
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

fn validate_config(config: &TtsConfig) -> anyhow::Result<()> {
    let request = config
        .endpoint
        .as_str()
        .into_client_request()
        .map_err(|_| anyhow::anyhow!("invalid Cartesia TTS endpoint"))?;
    let uri = request.uri();
    anyhow::ensure!(
        matches!(uri.scheme_str(), Some("ws" | "wss"))
            && uri.authority().is_some_and(|a| !a.as_str().contains('@')),
        "invalid Cartesia TTS endpoint"
    );
    anyhow::ensure!(
        uri.scheme_str() == Some("wss")
            || matches!(uri.host(), Some("localhost" | "127.0.0.1" | "[::1]")),
        "Cartesia TTS requires WSS outside loopback"
    );
    // No query is needed for TTS; rejecting it also prevents encoded credential/version overrides.
    anyhow::ensure!(
        uri.query().is_none(),
        "Cartesia TTS endpoint must not contain a query"
    );
    anyhow::ensure!(
        config.cartesia.api_version == "2026-08-14",
        "unsupported Cartesia API version (expected 2026-08-14)"
    );
    anyhow::ensure!(
        matches!(
            config.model.as_str(),
            "sonic-3.6" | "sonic-3.5" | "sonic-3" | "sonic-latest"
        ),
        "unsupported Cartesia TTS model"
    );
    anyhow::ensure!(
        config.cartesia.max_buffer_delay_ms <= 5_000,
        "tts.cartesia.max_buffer_delay_ms must be between 0 and 5000"
    );
    validate_rate(config.sample_rate_hz)?;
    language(&config.language)?;
    anyhow::ensure!(
        config.open_timeout_ms > 0 && config.open_timeout_ms <= u64::MAX / 1_000,
        "invalid Cartesia open timeout"
    );
    anyhow::ensure!(
        config.request_timeout_ms > 0
            && config.request_timeout_ms <= u64::MAX / 1_000
            && config.max_input_bytes > 0,
        "invalid Cartesia request limits"
    );
    Ok(())
}

fn validate_rate(rate: u32) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(rate, 8_000 | 16_000 | 22_050 | 24_000 | 44_100 | 48_000),
        "unsupported Cartesia PCM sample rate"
    );
    Ok(())
}

fn language(value: &str) -> anyhow::Result<String> {
    crate::provider::cartesia_selector(value, false)
}

#[async_trait]
impl StreamingTts for CartesiaStreamingTts {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        super::synthesize_via_text_stream(
            self,
            request,
            TtsInputLimits {
                max_input_bytes: self.config.max_input_bytes,
                request_timeout: Duration::from_millis(self.config.request_timeout_ms),
            },
            cancellation,
        )
        .await
    }

    fn text_input_mode(&self) -> TextInputMode {
        TextInputMode::Incremental
    }
    fn supports_text_stream(&self) -> bool {
        true
    }

    async fn open_text_stream(
        &self,
        options: TtsSessionOptions,
        limits: TtsInputLimits,
        cancellation: CancellationToken,
    ) -> Result<TtsDuplexSession, TtsOpenError> {
        let reject = |message: String| TtsOpenError::Rejected(message);
        if cancellation.is_cancelled()
            || limits.max_input_bytes == 0
            || limits.request_timeout.is_zero()
            || Instant::now().checked_add(limits.request_timeout).is_none()
        {
            return Err(reject("cancelled or invalid Cartesia input limits".into()));
        }
        let language = language(&options.language).map_err(|e| reject(e.to_string()))?;
        let rate = options
            .preferred_sample_rate_hz
            .unwrap_or(self.config.sample_rate_hz);
        validate_rate(rate).map_err(|e| reject(e.to_string()))?;
        let voice = if options.voice.trim().is_empty() {
            self.config.resolved_voice()
        } else {
            options.voice
        };
        if voice.trim().is_empty() || voice.len() > 1_024 {
            return Err(reject(
                "Cartesia voice is required and must fit 1024 bytes".into(),
            ));
        }
        let mut request = self
            .config
            .endpoint
            .as_str()
            .into_client_request()
            .map_err(|_| reject("invalid Cartesia endpoint".into()))?;
        request
            .headers_mut()
            .insert("Authorization", self.authorization.clone());
        request.headers_mut().insert(
            "Cartesia-Version",
            self.config
                .cartesia
                .api_version
                .parse()
                .map_err(|_| reject("invalid Cartesia version header".into()))?,
        );
        let mut ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
        ws_config.max_message_size = Some(MESSAGE_BYTES);
        ws_config.max_frame_size = Some(MESSAGE_BYTES);
        let connected = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(reject("Cartesia open cancelled".into())),
            connected = tokio::time::timeout(Duration::from_millis(self.config.open_timeout_ms), tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false)) => connected,
        };
        let (socket, _) = connected
            .map_err(|_| TtsOpenError::TimedOut("Cartesia TTS open timed out".into()))?
            .map_err(open_error)?;
        let token = cancellation.child_token();
        let (commands_tx, commands_rx) = mpsc::channel(COMMAND_BUDGET);
        let (audio_tx, audio_rx) = mpsc::channel(16);
        let (terminal_tx, terminal_rx) = oneshot::channel();
        let (deadline_tx, deadline_rx) = watch::channel(None);
        let finished = Arc::new(AtomicBool::new(false));
        let context = format!("{}-{}", options.speech_id, uuid::Uuid::new_v4());
        let generation = json!({
            "model_id": self.config.model, "voice": {"id": voice}, "language": language,
            "context_id": context,
            "output_format": {"container": "raw", "encoding": "pcm_s16le", "sample_rate": rate},
            "max_buffer_delay_ms": self.config.cartesia.max_buffer_delay_ms,
        });
        let worker_token = token.clone();
        let (wire_finish_tx, wire_finish_rx) = watch::channel(0u8);
        let worker = tokio::spawn(async move {
            let (mut sink, stream) = socket.split();
            let (pongs_tx, pongs_rx) = mpsc::channel(4);
            let last_audio = Arc::new(parking_lot::Mutex::new(None));
            let outcome = {
                tokio::select! {
                    biased;
                    _ = worker_token.cancelled() => None,
                    _ = response_deadline(deadline_rx) => Some(error("Cartesia TTS request timed out")),
                    result = read_audio(stream, audio_tx, pongs_tx, &context, rate, wire_finish_rx, Arc::clone(&last_audio)) => Some(result.unwrap_or_else(|e| error(e.to_string()))),
                    result = write_text(&mut sink, commands_rx, pongs_rx, generation, last_audio, wire_finish_tx) => Some(error(result.err().map_or_else(|| "Cartesia writer ended unexpectedly".into(), |e| e.to_string()))),
                }
            };
            if let Some(outcome) = outcome {
                let _ = terminal_tx.send(outcome);
            }
            worker_token.cancel();
            let _ = tokio::time::timeout(Duration::from_millis(100), sink.close()).await;
        });
        Ok(TtsDuplexSession {
            input: Box::new(Input {
                commands: Some(commands_tx),
                permits: Arc::new(Semaphore::new(COMMAND_BUDGET)),
                token: token.clone(),
                deadline: deadline_tx,
                limits,
                bytes: 0,
                has_text: false,
                finished,
            }),
            events: Box::new(Events {
                audio: audio_rx,
                terminal: Some(terminal_rx),
                token: token.clone(),
            }),
            control: Box::new(OwnedProviderTasks {
                token,
                tasks: vec![worker],
            }),
        })
    }
}

fn open_error(failure: tokio_tungstenite::tungstenite::Error) -> TtsOpenError {
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
            TtsOpenError::Transient(message)
        } else {
            TtsOpenError::Rejected(message)
        }
    } else {
        TtsOpenError::Transient("Cartesia TTS connection failed".into())
    }
}

fn retryable(status: u16, code: Option<&str>) -> bool {
    status >= 500
        || (status == 429 && !matches!(code, Some("quota_exceeded" | "plan_upgrade_required")))
}

fn diagnostic(status: u16, code: Option<&str>) -> String {
    let code = code
        .filter(|c| {
            c.len() <= 64
                && !c.is_empty()
                && c.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
        .unwrap_or("unknown");
    format!("Cartesia TTS error (status={status}, code={code})")
}

fn error(message: impl Into<String>) -> TtsEvent {
    TtsEvent::Error {
        recoverable: false,
        message: message.into(),
    }
}

struct Command {
    text: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct Input {
    commands: Option<mpsc::Sender<Command>>,
    permits: Arc<Semaphore>,
    token: CancellationToken,
    deadline: watch::Sender<Option<Instant>>,
    limits: TtsInputLimits,
    bytes: usize,
    has_text: bool,
    finished: Arc<AtomicBool>,
}

impl Drop for Input {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Acquire) {
            self.token.cancel();
        }
    }
}

impl TtsTextInput for Input {
    fn push_text(&mut self, fragment: String) -> Result<(), TtsInputError> {
        if self.finished.load(Ordering::Acquire) {
            return Err(TtsInputError::Finished);
        }
        if self.token.is_cancelled() {
            return Err(TtsInputError::Closed);
        }
        if fragment.len() > self.limits.max_input_bytes.saturating_sub(self.bytes)
            || fragment.len() > MESSAGE_BYTES
            || serde_json::to_string(&fragment)
                .expect("string serialization")
                .len()
                > MESSAGE_BYTES - 4_096
        {
            return Err(TtsInputError::TooLarge(fragment));
        }
        if fragment.is_empty() {
            return Ok(());
        }
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| TtsInputError::Full(fragment.clone()))?;
        let bytes = fragment.len();
        let has_text = !fragment.trim().is_empty();
        let admitted_at = Instant::now();
        self.commands
            .as_ref()
            .ok_or(TtsInputError::Closed)?
            .try_send(Command {
                text: fragment,
                _permit: permit,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(command) => TtsInputError::Full(command.text),
                mpsc::error::TrySendError::Closed(_) => TtsInputError::Closed,
            })?;
        self.bytes += bytes;
        self.has_text |= has_text;
        if self.deadline.borrow().is_none() {
            self.deadline
                .send_replace(Some(admitted_at + self.limits.request_timeout));
        }
        Ok(())
    }

    fn finish_input(&mut self) -> Result<(), TtsInputError> {
        if self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.token.is_cancelled() {
            return Err(TtsInputError::Closed);
        }
        if !self.has_text {
            return Err(TtsInputError::EmptyInput);
        }
        // Closing the sole sender reserves finish independently of a saturated command queue.
        self.finished.store(true, Ordering::Release);
        self.commands.take();
        Ok(())
    }
}

async fn response_deadline(mut deadline: watch::Receiver<Option<Instant>>) {
    loop {
        let current = *deadline.borrow_and_update();
        if let Some(deadline) = current {
            tokio::time::sleep_until(deadline).await;
            return;
        }
        if deadline.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn write_text(
    sink: &mut Sink,
    mut commands: mpsc::Receiver<Command>,
    mut pongs: mpsc::Receiver<bytes::Bytes>,
    mut generation: Value,
    last_audio: Arc<parking_lot::Mutex<Option<Instant>>>,
    wire_finish: watch::Sender<u8>,
) -> anyhow::Result<()> {
    let mut finished = false;
    loop {
        let (message, command) = tokio::select! {
            pong = pongs.recv(), if !pongs.is_closed() => match pong {
                Some(data) => (Message::Pong(data), None),
                None => continue,
            },
            command = commands.recv(), if !finished => {
                anyhow::ensure!(last_audio.lock().is_none_or(|at| at.elapsed() < Duration::from_secs(1)), "Cartesia TTS context expired before continuation");
                let continuing = command.is_some();
                generation["transcript"] = Value::String(command.as_ref().map_or_else(String::new, |c| c.text.clone()));
                generation["continue"] = Value::Bool(continuing);
                finished = !continuing;
                let text = generation.to_string();
                anyhow::ensure!(text.len() <= MESSAGE_BYTES, "Cartesia TTS request exceeds frame budget");
                (Message::Text(text.into()), command)
            },
            else => std::future::pending::<(Message, Option<Command>)>().await,
        };
        let finishing = finished && matches!(message, Message::Text(_));
        if finishing {
            wire_finish.send_replace(1);
        }
        tokio::time::timeout(WRITE_TIMEOUT, sink.send(message))
            .await
            .map_err(|_| anyhow::anyhow!("Cartesia TTS write timed out"))?
            .map_err(|_| anyhow::anyhow!("Cartesia TTS write failed"))?;
        if finishing {
            wire_finish.send_replace(2);
        }
        drop(command);
    }
}

struct Events {
    audio: mpsc::Receiver<TtsEvent>,
    terminal: Option<oneshot::Receiver<TtsEvent>>,
    token: CancellationToken,
}

impl Drop for Events {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[async_trait]
impl TtsStream for Events {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.terminal.is_none() {
            return Ok(None);
        }
        match self.audio.recv().await {
            Some(event) => Ok(Some(event)),
            None => {
                let terminal = self.terminal.as_mut().expect("checked above").await;
                self.terminal = None;
                Ok(terminal.ok())
            }
        }
    }
}

async fn read_audio(
    mut stream: futures::stream::SplitStream<Socket>,
    audio: mpsc::Sender<TtsEvent>,
    pongs: mpsc::Sender<bytes::Bytes>,
    context: &str,
    rate: u32,
    mut wire_finish: watch::Receiver<u8>,
    last_audio: Arc<parking_lot::Mutex<Option<Instant>>>,
) -> anyhow::Result<TtsEvent> {
    let mut normalizer = AudioNormalizer::default();
    let mut sequence = 0u32;
    while let Some(message) = stream.next().await {
        match message.map_err(|_| anyhow::anyhow!("Cartesia TTS receive failed"))? {
            Message::Ping(data) => {
                pongs
                    .try_send(data)
                    .map_err(|_| anyhow::anyhow!("Cartesia TTS ping queue full"))?;
            }
            Message::Pong(_) => {}
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text)
                    .map_err(|_| anyhow::anyhow!("malformed Cartesia TTS JSON"))?;
                let kind = value
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("missing Cartesia TTS event type"))?;
                if kind == "error" {
                    if let Some(id) = value.get("context_id").filter(|id| !id.is_null()) {
                        anyhow::ensure!(
                            id.as_str() == Some(context),
                            "Cartesia TTS error context mismatch"
                        );
                    }
                    let status = status(&value)?;
                    let code = value.get("error_code").and_then(Value::as_str);
                    return Ok(TtsEvent::Error {
                        recoverable: retryable(status, code),
                        message: diagnostic(status, code),
                    });
                }
                if !matches!(
                    kind,
                    "chunk" | "done" | "flush_done" | "timestamps" | "phoneme_timestamps"
                ) {
                    continue;
                }
                anyhow::ensure!(
                    value.get("context_id").and_then(Value::as_str) == Some(context),
                    "Cartesia TTS context mismatch"
                );
                anyhow::ensure!(
                    (200..300).contains(&status(&value)?),
                    "Cartesia TTS non-success output status"
                );
                if !matches!(kind, "chunk" | "done") {
                    continue;
                }
                let done = value
                    .get("done")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| anyhow::anyhow!("missing Cartesia TTS done flag"))?;
                if done {
                    anyhow::ensure!(
                        *wire_finish.borrow() != 0,
                        "Cartesia TTS completed before wire finish"
                    );
                }
                if kind == "chunk" {
                    validate_audio_metadata(&value, rate)?;
                    let data = value
                        .get("data")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow::anyhow!("missing Cartesia TTS audio data"))?;
                    anyhow::ensure!(
                        data.len() <= MAX_RAW_PCM_BYTES.div_ceil(3) * 4,
                        "Cartesia encoded PCM exceeds byte budget"
                    );
                    let pcm = base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .map_err(|_| anyhow::anyhow!("invalid Cartesia TTS base64"))?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("Cartesia TTS sequence overflow"))?;
                    if !pcm.is_empty() {
                        *last_audio.lock() = Some(Instant::now());
                    }
                    for event in normalizer.normalize(TtsEvent::Audio {
                        response_id: context.into(),
                        sequence,
                        sample_rate: rate,
                        pcm_s16le: pcm.into(),
                    })? {
                        audio
                            .send(event)
                            .await
                            .map_err(|_| anyhow::anyhow!("Cartesia audio output closed"))?;
                    }
                }
                if done {
                    // A fast reply may arrive while the final write is completing, but never before it starts.
                    while *wire_finish.borrow_and_update() != 2 {
                        wire_finish
                            .changed()
                            .await
                            .map_err(|_| anyhow::anyhow!("Cartesia wire finish failed"))?;
                    }
                    let mut terminal = normalizer.normalize(TtsEvent::AudioDone {
                        response_id: context.into(),
                    })?;
                    return Ok(terminal.remove(0));
                }
                anyhow::ensure!(kind != "done", "Cartesia done event is not terminal");
            }
            Message::Close(_) => break,
            _ => anyhow::bail!("unexpected Cartesia TTS binary message"),
        }
    }
    anyhow::bail!("Cartesia TTS socket ended without terminal")
}

fn validate_audio_metadata(value: &Value, rate: u32) -> anyhow::Result<()> {
    let format = value.get("output_format");
    if let Some(format) = format {
        anyhow::ensure!(
            format.is_object(),
            "invalid Cartesia output format metadata"
        );
    }
    for metadata in std::iter::once(value).chain(format) {
        for (key, expected) in [
            ("sample_rate", json!(rate)),
            ("sample_rate_hz", json!(rate)),
            ("channels", json!(1)),
            ("encoding", json!("pcm_s16le")),
            ("container", json!("raw")),
        ] {
            if let Some(actual) = metadata.get(key) {
                anyhow::ensure!(
                    actual == &expected,
                    "Cartesia output format metadata mismatch"
                );
            }
        }
    }
    Ok(())
}

fn status(value: &Value) -> anyhow::Result<u16> {
    value
        .get("status_code")
        .and_then(Value::as_u64)
        .and_then(|s| u16::try_from(s).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid Cartesia TTS status code"))
}
