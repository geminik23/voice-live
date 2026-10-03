//! Deterministic duplex speech provider contracts.
//!
//! Every test drives the public traits, the runtime assembly, or the TTS
//! worker with test-local fakes: no cloud, no transport, virtual time.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use voice_live::asr::{
    AsrAudioInput, AsrAudioPush, AsrDuplexLimits, AsrDuplexSession, AsrEvent, AsrEventStream,
    AsrInputError, AsrOpenError, AsrSession, AsrSessionConfig, LegacyDuplexFacade, StreamingAsr,
};
use voice_live::provider::ProviderSessionControl;
use voice_live::session::tts_worker::{TtsCancellationRegistry, TtsWorkerSettings, run_tts_worker};
use voice_live::speech::{ClaimClass, SpeechAct};
use voice_live::tts::buffered::BufferedTtsAdapter;
use voice_live::tts::premade::PremadeTts;
use voice_live::tts::{
    StreamingTts, TextInputMode, TtsDuplexSession, TtsEvent, TtsInputError, TtsInputLimits,
    TtsOpenError, TtsRequest, TtsSessionOptions, TtsStream, TtsTextInput,
};
use voice_live::{
    ClientControlMessage, ClientInput, ServerMessage, SpeechId, SpeechProviders, VoiceRuntime,
    VoiceRuntimeConfig,
};

const INJECTED_CONFIG: &str = r#"
version: 1

asr:
  provider: inject
tts:
  provider: mock

turn_control:
  soft_silence_ms: 300
  hard_silence_ms: 700

observability:
  event_log: false

dev:
  allow_text_injection: true
"#;

fn asr_limits(sample_budget: usize) -> AsrDuplexLimits {
    AsrDuplexLimits {
        sample_budget,
        command_budget: 64,
        write_timeout: Duration::from_secs(2),
    }
}

type CallCounter = Arc<AtomicUsize>;
type PcmLog = Arc<Mutex<Vec<Vec<i16>>>>;

fn tts_options() -> TtsSessionOptions {
    TtsSessionOptions {
        speech_id: SpeechId::new(),
        speech_epoch: 1,
        language: "Korean".into(),
        voice: String::new(),
        claim_class: ClaimClass::Phatic,
        preferred_sample_rate_hz: Some(24_000),
    }
}

fn tts_limits(max_input_bytes: usize, request_timeout_ms: u64) -> TtsInputLimits {
    TtsInputLimits {
        max_input_bytes,
        request_timeout: Duration::from_millis(request_timeout_ms),
    }
}

async fn injected_runtime(
    config: &str,
    asr: Arc<dyn StreamingAsr>,
    tts: Arc<dyn StreamingTts>,
) -> anyhow::Result<Arc<VoiceRuntime>> {
    let config = VoiceRuntimeConfig::from_yaml_str(config)?;
    VoiceRuntime::build_with_providers(config, SpeechProviders { asr, tts }).await
}

async fn start_session(
    runtime: &Arc<VoiceRuntime>,
) -> (voice_live::GatewaySession, mpsc::Receiver<ServerMessage>) {
    let (client_tx, client_rx) = mpsc::channel::<ServerMessage>(256);
    let session = runtime.create_session(client_tx).await.expect("session");
    session
        .input_tx
        .send(ClientInput::Control(ClientControlMessage::SessionStart {
            input_sample_rate: 16_000,
            channels: 1,
        }))
        .await
        .expect("session_start delivers");
    (session, client_rx)
}

/// Waits for one JSON message kind; false on timeout or channel end.
async fn saw_message(
    client_rx: &mut mpsc::Receiver<ServerMessage>,
    kind: &str,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        while let Some(message) = client_rx.recv().await {
            if let ServerMessage::Json(value) = &message
                && value.get("type").and_then(|t| t.as_str()) == Some(kind)
            {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

/// Waits until the fake factory reports at least `expected` open attempts.
async fn wait_for_opens(opens: &CallCounter, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while opens.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("open attempts advanced in time");
}

/// Duplex ASR fake: records admitted PCM, keeps everything in flight (so a
/// small budget saturates deterministically), and can fail its first N opens
/// with a typed error.
struct ScriptedDuplexAsr {
    open_failures: usize,
    open_error: AsrOpenError,
    opens: Arc<AtomicUsize>,
    recorded: Arc<Mutex<Vec<Vec<i16>>>>,
}

impl ScriptedDuplexAsr {
    fn new(open_failures: usize, open_error: AsrOpenError) -> (Arc<Self>, CallCounter, PcmLog) {
        let opens = Arc::new(AtomicUsize::new(0));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                open_failures,
                open_error,
                opens: Arc::clone(&opens),
                recorded: Arc::clone(&recorded),
            }),
            opens,
            recorded,
        )
    }
}

#[async_trait]
impl StreamingAsr for ScriptedDuplexAsr {
    async fn open(&self, config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>> {
        let session = self
            .open_duplex(config, asr_limits(usize::MAX), CancellationToken::new())
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(Box::new(LegacyDuplexFacade::new(session)))
    }

    fn supports_duplex(&self) -> bool {
        true
    }

    async fn open_duplex(
        &self,
        _config: AsrSessionConfig,
        limits: AsrDuplexLimits,
        cancellation: CancellationToken,
    ) -> Result<AsrDuplexSession, AsrOpenError> {
        let attempt = self.opens.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= self.open_failures {
            return Err(self.open_error.clone());
        }

        if cancellation.is_cancelled() {
            return Err(AsrOpenError::Rejected("cancelled before open".into()));
        }

        let closed = Arc::new(AtomicBool::new(false));
        Ok(AsrDuplexSession {
            input: Box::new(BudgetedInput {
                recorded: Arc::clone(&self.recorded),
                in_flight: 0,
                budget: limits.sample_budget,
                finished: false,
            }),
            events: Box::new(QuietEvents {
                closed: Arc::clone(&closed),
                cancellation,
            }),
            control: Box::new(FlagControl { closed }),
        })
    }
}

struct BudgetedInput {
    recorded: Arc<Mutex<Vec<Vec<i16>>>>,
    in_flight: usize,
    budget: usize,
    finished: bool,
}

#[async_trait]
impl AsrAudioInput for BudgetedInput {
    fn try_push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        if self.finished {
            return Err(AsrInputError::Closed);
        }

        if self.in_flight.saturating_add(pcm.len()) > self.budget {
            return Ok(AsrAudioPush::Full(pcm));
        }

        self.in_flight += pcm.len();
        self.recorded.lock().push(pcm);
        Ok(AsrAudioPush::Accepted)
    }

    async fn push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        self.try_push_audio(pcm)
    }

    fn try_commit_utterance(&mut self) -> Result<(), AsrInputError> {
        if self.finished {
            return Err(AsrInputError::Closed);
        }
        Ok(())
    }

    async fn commit_utterance(&mut self) -> Result<(), AsrInputError> {
        self.try_commit_utterance()
    }

    fn finish_input(&mut self) -> Result<(), AsrInputError> {
        self.finished = true;
        Ok(())
    }
}

/// Parks while quiet: `Ok(None)` is reserved for an explicitly closed
/// session, exactly like a live quiet source.
struct QuietEvents {
    closed: Arc<AtomicBool>,
    cancellation: CancellationToken,
}

#[async_trait]
impl AsrEventStream for QuietEvents {
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        if self.closed.load(Ordering::SeqCst) || self.cancellation.is_cancelled() {
            return Ok(None);
        }
        std::future::pending().await
    }
}

struct FlagControl {
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl ProviderSessionControl for FlagControl {
    fn cancel(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Counting whole-text TTS. Reports as a legacy buffered provider, so the
/// assembly bridges it with the real `BufferedTtsAdapter`; `stall` freezes
/// output after the request starts, for deadline tests.
struct CountingTts {
    chunks: usize,
    stall: bool,
    synthesize_calls: Arc<AtomicUsize>,
    texts: Arc<Mutex<Vec<String>>>,
}

impl CountingTts {
    fn new(chunks: usize) -> (Arc<Self>, CallCounter, Arc<Mutex<Vec<String>>>) {
        let synthesize_calls = Arc::new(AtomicUsize::new(0));
        let texts = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                chunks,
                stall: false,
                synthesize_calls: Arc::clone(&synthesize_calls),
                texts: Arc::clone(&texts),
            }),
            synthesize_calls,
            texts,
        )
    }

    fn stalling() -> (Arc<Self>, CallCounter, Arc<Mutex<Vec<String>>>) {
        let synthesize_calls = Arc::new(AtomicUsize::new(0));
        let texts = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                chunks: 0,
                stall: true,
                synthesize_calls: Arc::clone(&synthesize_calls),
                texts: Arc::clone(&texts),
            }),
            synthesize_calls,
            texts,
        )
    }
}

#[async_trait]
impl StreamingTts for CountingTts {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        self.synthesize_calls.fetch_add(1, Ordering::SeqCst);
        self.texts.lock().push(request.text.clone());

        if self.stall {
            return Ok(Box::new(StalledTtsStream { cancellation }));
        }

        Ok(Box::new(ChunkedTtsStream {
            sample_rate: 24_000,
            remaining: self.chunks,
            sequence: 0,
            done_emitted: false,
            cancellation,
        }))
    }
}

struct ChunkedTtsStream {
    sample_rate: u32,
    remaining: usize,
    sequence: u32,
    done_emitted: bool,
    cancellation: CancellationToken,
}

#[async_trait]
impl TtsStream for ChunkedTtsStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.cancellation.is_cancelled() || self.done_emitted {
            return Ok(None);
        }

        if self.remaining == 0 {
            self.done_emitted = true;
            return Ok(Some(TtsEvent::AudioDone {
                response_id: String::new(),
            }));
        }

        self.remaining -= 1;
        self.sequence += 1;
        Ok(Some(TtsEvent::Audio {
            response_id: String::new(),
            sequence: self.sequence,
            sample_rate: self.sample_rate,
            pcm_s16le: bytes::Bytes::from(vec![0u8; 480]),
        }))
    }
}

/// Accepts a request and then never reports anything.
struct StalledTtsStream {
    cancellation: CancellationToken,
}

#[async_trait]
impl TtsStream for StalledTtsStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        self.cancellation.cancelled().await;
        Ok(None)
    }
}

/// Declares text streaming but refuses to open: the runtime must surface the
/// failure instead of quietly bridging to `synthesize`.
struct OpenFailingTts {
    opens: Arc<AtomicUsize>,
    synthesize_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl StreamingTts for OpenFailingTts {
    async fn synthesize(
        &self,
        _request: TtsRequest,
        _cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        self.synthesize_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(ChunkedTtsStream {
            sample_rate: 24_000,
            remaining: 1,
            sequence: 0,
            done_emitted: false,
            cancellation: CancellationToken::new(),
        }))
    }

    fn text_input_mode(&self) -> TextInputMode {
        TextInputMode::Buffered
    }

    fn supports_text_stream(&self) -> bool {
        true
    }

    async fn open_text_stream(
        &self,
        _options: TtsSessionOptions,
        _limits: TtsInputLimits,
        _cancellation: CancellationToken,
    ) -> Result<TtsDuplexSession, TtsOpenError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Err(TtsOpenError::Transient("injected open failure".into()))
    }
}

/// Declares `Incremental` input without stream support: a contract error the
/// assembly must refuse.
struct IncrementalNoStreamTts;

#[async_trait]
impl StreamingTts for IncrementalNoStreamTts {
    async fn synthesize(
        &self,
        _request: TtsRequest,
        _cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        unreachable!("the build must fail before any synthesis")
    }

    fn text_input_mode(&self) -> TextInputMode {
        TextInputMode::Incremental
    }

    fn supports_text_stream(&self) -> bool {
        false
    }
}

/// Native incremental fake: audio is emitted per accepted fragment, before
/// input finishes, which is what the `Incremental` declaration promises.
struct NativeIncrementalTts {
    fragments: Arc<Mutex<Vec<String>>>,
    options: Mutex<Vec<TtsSessionOptions>>,
}

#[derive(Default)]
struct NativeState {
    fragments: usize,
    finished: bool,
    emitted: usize,
    done_emitted: bool,
    bytes: usize,
    nonempty: bool,
    deadline: Option<tokio::time::Instant>,
    expired: bool,
}

impl NativeIncrementalTts {
    fn new() -> (Arc<Self>, Arc<Mutex<Vec<String>>>) {
        let fragments = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                fragments: Arc::clone(&fragments),
                options: Mutex::new(Vec::new()),
            }),
            fragments,
        )
    }
}

#[async_trait]
impl StreamingTts for NativeIncrementalTts {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        voice_live::tts::synthesize_via_text_stream(
            self,
            request,
            tts_limits(65_536, 30_000),
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
        self.options.lock().push(options);
        let cancellation = cancellation.child_token();
        let state = Arc::new(Mutex::new(NativeState::default()));
        let notify = Arc::new(Notify::new());
        let owner_state = state.clone();
        let owner_token = cancellation.clone();
        let owner_notify = notify.clone();
        let owner = tokio::spawn(async move {
            let deadline = loop {
                let deadline = owner_state.lock().deadline;
                if let Some(deadline) = deadline {
                    break deadline;
                }
                tokio::select! { _ = owner_token.cancelled() => return, _ = owner_notify.notified() => {} }
            };
            tokio::select! {
                _ = owner_token.cancelled() => {},
                _ = tokio::time::sleep_until(deadline) => {
                    owner_state.lock().expired = true;
                    owner_token.cancel();
                }
            }
        });

        Ok(TtsDuplexSession {
            input: Box::new(NativeInput {
                fragments: Arc::clone(&self.fragments),
                state: Arc::clone(&state),
                notify: Arc::clone(&notify),
                finished: false,
                limits,
                token: cancellation.clone(),
            }),
            events: Box::new(NativeEvents {
                state,
                notify,
                cancellation: cancellation.clone(),
            }),
            control: Box::new(NativeControl {
                token: cancellation,
                owner,
                closed: false,
            }),
        })
    }
}

struct NativeInput {
    fragments: Arc<Mutex<Vec<String>>>,
    state: Arc<Mutex<NativeState>>,
    notify: Arc<Notify>,
    finished: bool,
    limits: TtsInputLimits,
    token: CancellationToken,
}

impl Drop for NativeInput {
    fn drop(&mut self) {
        if !self.finished {
            self.token.cancel();
        }
    }
}

impl TtsTextInput for NativeInput {
    fn push_text(&mut self, fragment: String) -> Result<(), TtsInputError> {
        if self.finished {
            return Err(TtsInputError::Finished);
        }
        if self.token.is_cancelled() {
            return Err(TtsInputError::Closed);
        }
        let mut state = self.state.lock();
        if fragment.len() > self.limits.max_input_bytes.saturating_sub(state.bytes) {
            return Err(TtsInputError::TooLarge(fragment));
        }
        state.bytes += fragment.len();
        if !fragment.trim().is_empty() {
            state.nonempty = true;
            if state.deadline.is_none() {
                state.deadline = Some(tokio::time::Instant::now() + self.limits.request_timeout);
            }
        }
        state.fragments += 1;
        drop(state);
        self.fragments.lock().push(fragment);
        self.notify.notify_waiters();
        Ok(())
    }

    fn finish_input(&mut self) -> Result<(), TtsInputError> {
        if self.finished {
            return Ok(());
        }
        if self.token.is_cancelled() {
            return Err(TtsInputError::Closed);
        }
        {
            let mut state = self.state.lock();
            if !state.nonempty {
                return Err(TtsInputError::EmptyInput);
            }
            state.finished = true;
        }
        self.finished = true;
        self.notify.notify_waiters();
        Ok(())
    }
}

struct NativeEvents {
    state: Arc<Mutex<NativeState>>,
    notify: Arc<Notify>,
    cancellation: CancellationToken,
}

#[async_trait]
impl TtsStream for NativeEvents {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        loop {
            {
                let mut state = self.state.lock();
                if state.expired && !state.done_emitted {
                    state.done_emitted = true;
                    return Ok(Some(TtsEvent::Error {
                        recoverable: false,
                        message: "native request timed out".into(),
                    }));
                }
                if self.cancellation.is_cancelled() {
                    return Ok(None);
                }

                if state.emitted < state.fragments {
                    state.emitted += 1;
                    return Ok(Some(TtsEvent::Audio {
                        response_id: String::new(),
                        sequence: state.emitted as u32,
                        sample_rate: 24_000,
                        pcm_s16le: bytes::Bytes::from(vec![0u8; 480]),
                    }));
                }

                if state.finished && !state.done_emitted {
                    state.done_emitted = true;
                    return Ok(Some(TtsEvent::AudioDone {
                        response_id: String::new(),
                    }));
                }

                if state.done_emitted {
                    return Ok(None);
                }
            }

            tokio::select! {
                _ = self.notify.notified() => continue,
                _ = self.cancellation.cancelled() => continue,
            }
        }
    }
}

impl Drop for NativeEvents {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct NativeControl {
    token: CancellationToken,
    owner: tokio::task::JoinHandle<()>,
    closed: bool,
}
impl Drop for NativeControl {
    fn drop(&mut self) {
        self.token.cancel();
        self.owner.abort();
    }
}
#[async_trait]
impl ProviderSessionControl for NativeControl {
    fn cancel(&self) {
        self.token.cancel();
    }
    async fn close(&mut self) -> anyhow::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.token.cancel();
        if !self.owner.is_finished() {
            self.owner.abort();
        }
        let _ = (&mut self.owner).await;
        self.closed = true;
        Ok(())
    }
}

// Runtime assembly and injection.

#[tokio::test(start_paused = true)]
async fn injected_factories_serve_the_runtime_pipeline() {
    let (asr, opens, recorded) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let (tts, synthesize_calls, _) = CountingTts::new(2);

    let runtime = injected_runtime(INJECTED_CONFIG, asr, tts)
        .await
        .expect("injected providers build the runtime");
    let (session, mut client_rx) = start_session(&runtime).await;

    runtime.inject_user_text(session.session_id, "토요일 저녁 7시로 예약해줘");

    assert!(
        saw_message(&mut client_rx, "speech_done", Duration::from_secs(10)).await,
        "the pipeline must complete through the injected providers"
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    assert!(synthesize_calls.load(Ordering::SeqCst) >= 1);
    assert!(
        recorded.lock().is_empty(),
        "typed injection does not invent PCM frames"
    );
}

#[tokio::test(start_paused = true)]
async fn injection_is_routed_to_exactly_one_session() {
    let (asr, _, _) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let (tts, _, _) = CountingTts::new(1);

    let runtime = injected_runtime(INJECTED_CONFIG, asr, tts)
        .await
        .expect("runtime builds");
    let (session_a, mut client_rx_a) = start_session(&runtime).await;
    let (_session_b, mut client_rx_b) = start_session(&runtime).await;

    runtime.inject_user_text(session_a.session_id, "주말에 예약할게요");

    assert!(
        saw_message(&mut client_rx_a, "speech_done", Duration::from_secs(10)).await,
        "the injected session must complete"
    );
    assert!(
        !saw_message(&mut client_rx_b, "speech_started", Duration::from_secs(1)).await,
        "another client's session must stay silent"
    );
}

#[tokio::test(start_paused = true)]
async fn pushed_audio_reaches_the_injected_provider_input() {
    let (asr, opens, recorded) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let (tts, _, _) = CountingTts::new(1);

    let runtime = injected_runtime(INJECTED_CONFIG, asr, tts)
        .await
        .expect("runtime builds");
    let (session, _client_rx) = start_session(&runtime).await;

    // The connection opens asynchronously; frames pushed while connecting
    // are deliberately discarded, so wait for readiness first.
    wait_for_opens(&opens, 1).await;

    for sequence in 0..3u64 {
        session
            .input_tx
            .send(ClientInput::Audio(voice_live::media::AudioFrame {
                sequence,
                pcm: vec![100i16; 320],
            }))
            .await
            .expect("frame delivers");
    }

    // The media worker admits frames asynchronously; give it virtual time.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = recorded.lock();
    assert_eq!(
        received.len(),
        3,
        "every frame must be admitted to the provider input"
    );
    assert_eq!(received[0].len(), 320);
}

#[tokio::test(start_paused = true)]
async fn injected_provider_spec_probe_is_strict() {
    let bad_spec_config = r#"
version: 1
asr:
  provider: inject
tts:
  provider: mock
agents:
  task:
    spec: missing-spec-does-not-exist.yaml
dev:
  allow_text_injection: true
"#;

    let (asr, _, _) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let (tts, _, _) = CountingTts::new(1);
    let strict = injected_runtime(bad_spec_config, asr, tts).await;
    assert!(
        strict.is_err(),
        "an injected real provider must not fall back to the mock brain"
    );

    // The built-in path keeps its keyless development fallback.
    let config = VoiceRuntimeConfig::from_yaml_str(bad_spec_config).expect("config parses");
    let runtime = VoiceRuntime::build(config).await;
    assert!(
        runtime.is_ok(),
        "built-in development providers keep the mock fallback"
    );
}

#[tokio::test(start_paused = true)]
async fn fatal_asr_open_is_not_retried_but_typed_text_still_flows() {
    let (asr, opens, _) =
        ScriptedDuplexAsr::new(usize::MAX, AsrOpenError::Rejected("denied".into()));
    let (tts, _, _) = CountingTts::new(1);

    let runtime = injected_runtime(INJECTED_CONFIG, asr, tts)
        .await
        .expect("runtime builds");
    let (session, mut client_rx) = start_session(&runtime).await;

    // Injected finals are owned by the media session, not the provider
    // connection, so typed text flows even while the ASR is down.
    runtime.inject_user_text(session.session_id, "다른 날도 알아봐줘");

    assert!(
        saw_message(&mut client_rx, "speech_done", Duration::from_secs(10)).await,
        "typed text must not depend on the provider connection"
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "a rejected open must not be retried"
    );
}

#[tokio::test(start_paused = true)]
async fn transient_asr_open_retries_and_typed_text_flows_during_reconnect() {
    let (asr, opens, _) = ScriptedDuplexAsr::new(1, AsrOpenError::Transient("flaky".into()));
    let (tts, _, _) = CountingTts::new(1);

    let runtime = injected_runtime(INJECTED_CONFIG, asr, tts)
        .await
        .expect("runtime builds");
    let (session, mut client_rx) = start_session(&runtime).await;

    // The first open already failed; the runtime is in its retry window.
    wait_for_opens(&opens, 1).await;

    runtime.inject_user_text(session.session_id, "토요일 저녁 7시로 예약해줘");

    assert!(
        saw_message(&mut client_rx, "speech_done", Duration::from_secs(10)).await,
        "typed text must flow during the provider reconnect"
    );
    assert!(
        opens.load(Ordering::SeqCst) >= 2,
        "a transient open failure must actually be retried"
    );
}

#[tokio::test(start_paused = true)]
async fn input_overflow_cancels_the_connection_for_a_fresh_retry() {
    let overflow_config = r#"
version: 1
asr:
  provider: inject
  input_buffer_ms: 20
tts:
  provider: mock
turn_control:
  soft_silence_ms: 300
  hard_silence_ms: 700
observability:
  event_log: false
dev:
  allow_text_injection: true
"#;

    let (asr, opens, recorded) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let (tts, _, _) = CountingTts::new(1);

    let runtime = injected_runtime(overflow_config, asr, tts)
        .await
        .expect("runtime builds");
    let (session, _client_rx) = start_session(&runtime).await;

    wait_for_opens(&opens, 1).await;

    // A 20 ms frame is exactly the whole budget, so the second in-flight
    // frame saturates the queue.
    for sequence in 0..4u64 {
        session
            .input_tx
            .send(ClientInput::Audio(voice_live::media::AudioFrame {
                sequence,
                pcm: vec![0i16; 320],
            }))
            .await
            .expect("frame delivers");
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        runtime.metrics().counter("voice_asr_input_overflow_total") >= 1,
        "saturation must be reported as overflow"
    );
    assert!(
        opens.load(Ordering::SeqCst) >= 2,
        "a saturated connection is dropped and retried fresh"
    );
    assert_eq!(
        recorded.lock().len(),
        1,
        "only the pre-overflow frame was admitted; fresh reconnect never replays queued PCM"
    );
}

// Capability dispatch.

#[tokio::test(start_paused = true)]
async fn incremental_declaration_without_stream_support_fails_the_build() {
    let (asr, _, _) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let result = injected_runtime(INJECTED_CONFIG, asr, Arc::new(IncrementalNoStreamTts)).await;

    let error = match result {
        Ok(_) => panic!("the contract violation must fail the build"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("Incremental"),
        "the error must name the contract violation: {error}"
    );
}

#[tokio::test(start_paused = true)]
async fn streaming_open_failure_never_falls_back_to_legacy_synthesize() {
    let (asr, _, _) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let opens = Arc::new(AtomicUsize::new(0));
    let synthesize_calls = Arc::new(AtomicUsize::new(0));
    let tts: Arc<dyn StreamingTts> = Arc::new(OpenFailingTts {
        opens: Arc::clone(&opens),
        synthesize_calls: Arc::clone(&synthesize_calls),
    });

    let runtime = injected_runtime(INJECTED_CONFIG, asr, tts)
        .await
        .expect("runtime builds");
    let (session, mut client_rx) = start_session(&runtime).await;

    runtime.inject_user_text(session.session_id, "토요일 저녁 7시로 예약해줘");

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        !saw_message(&mut client_rx, "speech_started", Duration::from_secs(1)).await,
        "a failing open must not produce speech"
    );
    assert!(
        opens.load(Ordering::SeqCst) >= 1,
        "the streaming path must have been attempted"
    );
    assert_eq!(
        synthesize_calls.load(Ordering::SeqCst),
        0,
        "a declared streaming provider must never be bridged to synthesize"
    );
}

#[tokio::test(start_paused = true)]
async fn legacy_buffered_provider_is_bridged_by_the_assembly() {
    let (asr, _, _) = ScriptedDuplexAsr::new(0, AsrOpenError::Unsupported);
    let (tts, synthesize_calls, texts) = CountingTts::new(2);

    let runtime = injected_runtime(INJECTED_CONFIG, asr, tts)
        .await
        .expect("a buffered legacy provider builds through the bridge");
    let (session, mut client_rx) = start_session(&runtime).await;

    runtime.inject_user_text(session.session_id, "금요일 저녁에도 확인해줘");

    assert!(
        saw_message(&mut client_rx, "speech_done", Duration::from_secs(10)).await,
        "the bridged provider must complete the pipeline"
    );
    assert!(synthesize_calls.load(Ordering::SeqCst) >= 1);
    assert!(!texts.lock().is_empty());
}

// TTS session contracts.

#[tokio::test(start_paused = true)]
async fn buffered_adapter_waits_for_finish_before_the_inner_request() {
    let (inner, calls, texts) = CountingTts::new(2);
    let adapter = BufferedTtsAdapter::new(inner);

    let session = adapter
        .open_text_stream(
            tts_options(),
            tts_limits(65_536, 30_000),
            CancellationToken::new(),
        )
        .await
        .expect("the adapter opens");

    let mut input = session.input;
    let mut events = session.events;

    for fragment in ["확인", "했습니다", ". 두 곳이 가능합니다"] {
        input.push_text(fragment.into()).expect("fragment fits");
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no synthesis before finish"
    );

    input.finish_input().expect("finish admits");
    drop(input);

    let mut audio = 0;
    let mut done = false;
    while let Some(event) = events.next_event().await.expect("stream is healthy") {
        match event {
            TtsEvent::Audio { .. } => audio += 1,
            TtsEvent::AudioDone { .. } => {
                done = true;
                break;
            }
            TtsEvent::Error { .. } => panic!("a healthy provider must not fail"),
        }
    }

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(audio, 2);
    assert!(done);
    assert_eq!(
        texts.lock()[0],
        "확인했습니다. 두 곳이 가능합니다",
        "the fragments must be appended, not replaced"
    );
}

#[tokio::test(start_paused = true)]
async fn finish_semantics_and_input_budget_are_enforced() {
    let (inner, _, _) = CountingTts::new(1);
    let adapter = BufferedTtsAdapter::new(inner);

    let session = adapter
        .open_text_stream(
            tts_options(),
            tts_limits(10, 30_000),
            CancellationToken::new(),
        )
        .await
        .expect("the adapter opens");

    let mut input = session.input;

    // Empty input cannot finish.
    assert!(matches!(
        input.finish_input(),
        Err(TtsInputError::EmptyInput)
    ));

    input.push_text("0123456789".into()).expect("exactly fits");

    // Over-budget fragments come back with their ownership.
    match input.push_text("x".into()) {
        Err(TtsInputError::TooLarge(fragment)) => assert_eq!(fragment, "x"),
        other => panic!("expected TooLarge, got {other:?}"),
    }

    input.finish_input().expect("finish admits");
    input
        .finish_input()
        .expect("a second finish keeps the finished state");

    assert!(matches!(
        input.push_text("late".into()),
        Err(TtsInputError::Finished)
    ));
}

#[tokio::test(start_paused = true)]
async fn stalled_response_hits_the_request_deadline() {
    let (inner, calls, _) = CountingTts::stalling();
    let adapter = BufferedTtsAdapter::new(inner);

    let session = adapter
        .open_text_stream(
            tts_options(),
            tts_limits(65_536, 500),
            CancellationToken::new(),
        )
        .await
        .expect("the adapter opens");

    let mut input = session.input;
    let mut events = session.events;

    input.push_text("천천히 대답하는 공급자".into()).unwrap();
    input.finish_input().unwrap();
    drop(input);

    let event = tokio::time::timeout(Duration::from_secs(2), events.next_event())
        .await
        .expect("the deadline must fire")
        .expect("stream healthy")
        .expect("a terminal must arrive");

    assert!(
        matches!(event, TtsEvent::Error { .. }),
        "the deadline converts a stalled response into a failure: {event:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn native_incremental_audio_precedes_input_finish() {
    let (tts, fragments) = NativeIncrementalTts::new();

    let session = tts
        .open_text_stream(
            tts_options(),
            tts_limits(65_536, 30_000),
            CancellationToken::new(),
        )
        .await
        .expect("the native fake opens");

    let mut input = session.input;
    let mut events = session.events;

    input.push_text("안녕".into()).unwrap();
    input.push_text("하세요".into()).unwrap();

    // Audio must be observable before the input is finished.
    let before_finish = tokio::time::timeout(Duration::from_millis(500), events.next_event())
        .await
        .expect("audio must arrive without waiting for finish")
        .expect("stream healthy")
        .expect("audio event");

    assert!(matches!(before_finish, TtsEvent::Audio { .. }));

    input.finish_input().unwrap();
    drop(input);

    let mut done = false;
    while let Some(event) = events.next_event().await.expect("stream healthy") {
        if matches!(event, TtsEvent::AudioDone { .. }) {
            done = true;
            break;
        }
    }
    assert!(done, "the native session must terminate normally");

    let pushed = fragments.lock();
    assert_eq!(
        (*pushed).clone(),
        vec!["안녕".to_string(), "하세요".to_string()]
    );
}

#[tokio::test(start_paused = true)]
async fn cancelled_act_never_opens_the_provider() {
    let (inner, calls, _) = CountingTts::new(1);
    let tts: Arc<dyn StreamingTts> = BufferedTtsAdapter::new(inner);

    let (speech_tx, speech_rx) = mpsc::channel::<(SpeechAct, CancellationToken)>(4);
    let (event_tx, _event_rx) = mpsc::channel(64);
    let registry = Arc::new(TtsCancellationRegistry::default());

    let clock: voice_live::clock::ClockRef = Arc::new(voice_live::clock::TokioClock::new());
    let meta_factory: Arc<dyn voice_live::meta::MetaFactory> =
        Arc::new(voice_live::meta::ClockMetaFactory::new(clock));
    let shutdown = CancellationToken::new();

    let settings = TtsWorkerSettings {
        language: "Korean".into(),
        voice: String::new(),
        max_input_bytes: 65_536,
        request_timeout: Duration::from_secs(30),
        open_timeout: Duration::from_secs(10),
        preferred_sample_rate_hz: Some(24_000),
    };

    tokio::spawn(run_tts_worker(
        tts,
        speech_rx,
        event_tx,
        meta_factory,
        shutdown,
        registry,
        settings,
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    speech_tx
        .send((SpeechAct::phatic("네", 1, 10), cancelled))
        .await
        .expect("cancelled act queues");

    let live = CancellationToken::new();
    speech_tx
        .send((SpeechAct::phatic("네", 1, 10), live))
        .await
        .expect("live act queues");
    drop(speech_tx);

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the cancelled act must never reach the provider"
    );
}

#[tokio::test(start_paused = true)]
async fn premade_cache_hit_replays_without_calling_the_provider() {
    let (inner, calls, texts) = CountingTts::new(2);
    let premade = Arc::new(PremadeTts::with_settings(
        inner,
        vec!["네".into()],
        "Korean".into(),
        String::new(),
    ));

    premade.warmup().await;
    let calls_after_warmup = calls.load(Ordering::SeqCst);
    assert!(calls_after_warmup >= 1, "warmup must synthesize the phrase");

    let request = TtsRequest {
        speech_id: SpeechId::new(),
        speech_epoch: 1,
        text: "네".into(),
        language: "Korean".into(),
        voice: String::new(),
        claim_class: ClaimClass::Phatic,
    };

    let Some(mut stream) = premade
        .try_cached_synthesis(&request, CancellationToken::new())
        .await
    else {
        panic!("a warmed phatic clip must hit the cache");
    };

    let mut audio = 0;
    let mut done = false;
    while let Some(event) = stream.next_event().await.expect("cached stream healthy") {
        match event {
            TtsEvent::Audio { .. } => audio += 1,
            TtsEvent::AudioDone { .. } => {
                done = true;
                break;
            }
            TtsEvent::Error { .. } => panic!("a cached clip must replay cleanly"),
        }
    }

    assert!(audio >= 1);
    assert!(done);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        calls_after_warmup,
        "a cache hit must not call the provider again"
    );
    assert_eq!(
        texts.lock().len(),
        calls_after_warmup,
        "no hidden extra requests"
    );
}

#[tokio::test(start_paused = true)]
async fn buffered_deadline_survives_a_full_output_queue_without_polling() {
    let (inner, _, _) = CountingTts::new(100);
    let adapter = BufferedTtsAdapter::new(inner);
    let mut session = adapter
        .open_text_stream(
            tts_options(),
            tts_limits(1024, 100),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    session.input.push_text("test".into()).unwrap();
    session.input.finish_input().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(matches!(
        session.input.push_text("late".into()),
        Err(TtsInputError::Closed)
    ));
    let mut audio = 0;
    let mut failed = false;
    while let Some(event) = session.events.next_event().await.unwrap() {
        match event {
            TtsEvent::Audio { .. } => audio += 1,
            TtsEvent::Error { message, .. } => {
                assert!(message.contains("timed out"));
                failed = true;
            }
            TtsEvent::AudioDone { .. } => panic!("a full undrained output must expire"),
        }
    }
    assert!(failed);
    assert!(audio <= 16);
    session.control.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn buffered_idle_has_no_synthesis_deadline_and_drop_cancels_admission() {
    let (inner, calls, _) = CountingTts::new(1);
    let adapter = BufferedTtsAdapter::new(inner);
    let mut session = adapter
        .open_text_stream(
            tts_options(),
            tts_limits(1024, 100),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    session.input.push_text("waiting".into()).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    session.input.push_text(" for finish".into()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(session.events);
    assert!(matches!(
        session.input.finish_input(),
        Err(TtsInputError::Closed)
    ));
    session.control.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn native_deadline_is_absolute_and_owner_runs_without_output_polling() {
    let (tts, _) = NativeIncrementalTts::new();
    let mut session = tts
        .open_text_stream(
            tts_options(),
            tts_limits(1024, 100),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    session.input.push_text("first".into()).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    session.input.push_text("second".into()).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(matches!(
        session.input.push_text("too late".into()),
        Err(TtsInputError::Closed)
    ));
    assert!(matches!(
        session.events.next_event().await.unwrap(),
        Some(TtsEvent::Error { .. })
    ));
    assert!(session.events.next_event().await.unwrap().is_none());
    session.control.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn native_whole_text_convenience_retains_the_control_owner() {
    let (tts, texts) = NativeIncrementalTts::new();
    let mut stream = tts
        .synthesize(
            TtsRequest {
                speech_id: SpeechId::new(),
                speech_epoch: 1,
                text: "whole text".into(),
                language: "English".into(),
                voice: "test-voice".into(),
                claim_class: ClaimClass::Phatic,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(matches!(
        stream.next_event().await.unwrap(),
        Some(TtsEvent::Audio { .. })
    ));
    assert!(matches!(
        stream.next_event().await.unwrap(),
        Some(TtsEvent::AudioDone { .. })
    ));
    assert_eq!(*texts.lock(), vec!["whole text"]);
}

#[tokio::test(start_paused = true)]
async fn dropping_buffered_input_before_finish_never_starts_synthesis() {
    let (inner, calls, _) = CountingTts::new(1);
    let adapter = BufferedTtsAdapter::new(inner);
    let mut session = adapter
        .open_text_stream(
            tts_options(),
            tts_limits(1024, 100),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    session.input.push_text("unfinished".into()).unwrap();
    drop(session.input);
    assert!(session.events.next_event().await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    session.control.close().await.unwrap();
}

struct MalformedTts {
    bytes: usize,
    rate: u32,
}
#[async_trait]
impl StreamingTts for MalformedTts {
    async fn synthesize(
        &self,
        _: TtsRequest,
        _: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        Ok(Box::new(OneEvent(Some(TtsEvent::Audio {
            response_id: "response".into(),
            sequence: 1,
            sample_rate: self.rate,
            pcm_s16le: bytes::Bytes::from(vec![0; self.bytes]),
        }))))
    }
}
struct OneEvent(Option<TtsEvent>);
#[async_trait]
impl TtsStream for OneEvent {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        Ok(self.0.take())
    }
}

#[tokio::test(start_paused = true)]
async fn buffered_output_contract_rejects_zero_rate_odd_and_oversized_pcm() {
    for (bytes, rate) in [(2, 0), (3, 24_000), (65_538, 24_000)] {
        let adapter = BufferedTtsAdapter::new(Arc::new(MalformedTts { bytes, rate }));
        let mut session = adapter
            .open_text_stream(
                tts_options(),
                tts_limits(1024, 100),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        session.input.push_text("test".into()).unwrap();
        session.input.finish_input().unwrap();
        assert!(matches!(
            session.events.next_event().await.unwrap(),
            Some(TtsEvent::Error { .. })
        ));
        assert!(session.events.next_event().await.unwrap().is_none());
        session.control.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn premade_runtime_hit_completes_playback_without_a_provider_recall() {
    let (tts, calls, _) = CountingTts::new(1);
    let asr = Arc::new(voice_live::asr::fake::MockAsr::new(vec![
        voice_live::asr::fake::TimedAsrEvent {
            after_ms: 100,
            event: AsrEvent::Partial {
                utterance_id: 0,
                revision: voice_live::Revision(1),
                text: "그게".into(),
            },
        },
    ]));
    let config = VoiceRuntimeConfig::from_yaml_str(
        r#"
asr:
  provider: mock
tts:
  provider: mock
  premade:
    enabled: true
    phrases: ["네"]
turn_control:
  soft_silence_ms: 100
  hard_silence_ms: 5000
  backchannel:
    maximum_per_user_turn: 1
observability:
  event_log: false
"#,
    )
    .unwrap();
    let runtime = VoiceRuntime::build_with_providers(config, SpeechProviders { asr, tts })
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (session, mut client) = start_session(&runtime).await;
    for seq in 0..20 {
        session
            .input_tx
            .send(ClientInput::Audio(voice_live::media::AudioFrame {
                sequence: seq,
                pcm: vec![if seq < 5 { 20_000 } else { 0 }; 320],
            }))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut speech = None;
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(message) = client.recv().await {
            if let ServerMessage::Json(value) = message {
                if value["type"] == "speech_started" {
                    let id =
                        serde_json::from_value::<SpeechId>(value["speech_id"].clone()).unwrap();
                    speech = Some(id);
                }
                if value["type"] == "speech_done" {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    session
        .input_tx
        .send(ClientInput::Control(
            ClientControlMessage::PlaybackCompleted {
                speech_id: speech.expect("cached speech started"),
                speech_epoch: 1,
            },
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(runtime.metrics().counter("voice_tts_failed_total"), 0);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "runtime used the warmed clip rather than re-synthesizing"
    );
    runtime.close_session(session.session_id).await;
    assert_eq!(runtime.session_count(), 0);
}

struct ObservedNative {
    inner: Arc<NativeIncrementalTts>,
    opened: CallCounter,
    closed: CallCounter,
}
struct ObservedControl {
    inner: Box<dyn ProviderSessionControl>,
    closed: CallCounter,
}
#[async_trait]
impl ProviderSessionControl for ObservedControl {
    fn cancel(&self) {
        self.inner.cancel();
    }
    async fn close(&mut self) -> anyhow::Result<()> {
        self.inner.close().await?;
        self.closed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[async_trait]
impl StreamingTts for ObservedNative {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        self.inner.synthesize(request, cancellation).await
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
        token: CancellationToken,
    ) -> Result<TtsDuplexSession, TtsOpenError> {
        let mut session = self
            .inner
            .open_text_stream(options, limits, token.clone())
            .await?;
        session.events = Box::new(StalledTtsStream {
            cancellation: token,
        });
        session.control = Box::new(ObservedControl {
            inner: session.control,
            closed: self.closed.clone(),
        });
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(session)
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_an_active_tts_worker_closes_and_joins_its_provider() {
    let (inner, _) = NativeIncrementalTts::new();
    let opened = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(ObservedNative {
        inner,
        opened: opened.clone(),
        closed: closed.clone(),
    });
    let (tx, rx) = mpsc::channel(1);
    let (events, _event_rx) = mpsc::channel(16);
    let clock: voice_live::clock::ClockRef = Arc::new(voice_live::clock::TokioClock::new());
    let worker = tokio::spawn(run_tts_worker(
        provider,
        rx,
        events,
        Arc::new(voice_live::meta::ClockMetaFactory::new(clock)),
        CancellationToken::new(),
        Arc::new(TtsCancellationRegistry::default()),
        TtsWorkerSettings {
            language: "English".into(),
            voice: String::new(),
            max_input_bytes: 1024,
            request_timeout: Duration::from_secs(30),
            open_timeout: Duration::from_secs(1),
            preferred_sample_rate_hz: None,
        },
    ));
    let token = CancellationToken::new();
    tx.send((SpeechAct::phatic("text", 1, 10), token.clone()))
        .await
        .unwrap();
    wait_for_opens(&opened, 1).await;
    token.cancel();
    drop(tx);
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "active cancellation must run close, not just Drop"
    );
}

#[tokio::test(start_paused = true)]
async fn provisional_chunks_are_never_submitted_as_speech() {
    use voice_live::agent::CognitiveEvent;
    use voice_live::agent::mock::{FakeAgent, TimedCognitiveEvent};
    let agent = Arc::new(FakeAgent::with_script(
        vec![TimedCognitiveEvent {
            after_ms: 50,
            event: CognitiveEvent::Content {
                text: "provisional text".into(),
            },
        }],
        "authoritative reply",
    ));
    let (raw_tts, _, texts) = CountingTts::new(1);
    let tts = BufferedTtsAdapter::new(raw_tts);
    let config =
        Arc::new(VoiceRuntimeConfig::from_yaml_str("observability:\n  event_log: false").unwrap());
    let shutdown = CancellationToken::new();
    let (session, handle) = voice_live::VoiceSessionBuilder::new(
        config,
        Arc::new(voice_live::playback::UnacknowledgedPlaybackSink),
        tts,
        agent,
    )
    .with_shutdown(shutdown.clone())
    .build();
    let meta = session.meta_factory.new_meta();
    let worker = tokio::spawn(session.run());
    handle
        .emit(voice_live::VoiceEvent::UserTurnCommitted {
            meta,
            turn_id: voice_live::TurnId::new(),
            transcript_revision: voice_live::Revision(1),
            transcript: "user request".into(),
        })
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        *texts.lock(),
        vec!["authoritative reply"],
        "Content chunks are not immutable speech evidence"
    );
    shutdown.cancel();
    worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn native_premade_warmup_uses_the_runtime_voice_language_and_advisory_rate() {
    let (native, _) = NativeIncrementalTts::new();
    let cache = PremadeTts::with_rate(
        native.clone(),
        vec!["hello".into()],
        "English".into(),
        "voice-a".into(),
        Some(16_000),
    );
    cache.warmup().await;
    let options = native.options.lock().clone();
    assert_eq!(options.len(), 1);
    assert_eq!(options[0].language, "English");
    assert_eq!(options[0].voice, "voice-a");
    assert_eq!(options[0].preferred_sample_rate_hz, Some(16_000));
    drop(options);
    let request = TtsRequest {
        speech_id: SpeechId::new(),
        speech_epoch: 1,
        text: "hello".into(),
        language: "English".into(),
        voice: "voice-a".into(),
        claim_class: ClaimClass::Phatic,
    };
    let mut stream = cache
        .try_cached_synthesis(&request, CancellationToken::new())
        .await
        .unwrap();
    let Some(TtsEvent::Audio { sample_rate, .. }) = stream.next_event().await.unwrap() else {
        panic!("cached audio expected")
    };
    assert_eq!(
        sample_rate, 24_000,
        "actual clip rate remains authoritative even if preference differs"
    );
    assert_eq!(native.options.lock().len(), 1);
}
