use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::CognitiveAgent;
use crate::agent::mock::MockReservationAgent;
use crate::asr::fake::MockAsr;
use crate::asr::inject::InjectionQueue;
use crate::asr::together::TogetherNemotronAsr;
use crate::asr::{
    AsrAudioInput, AsrAudioPush, AsrDuplexLimits, AsrDuplexSession, AsrEvent, AsrEventStream,
    AsrInputError, AsrOpenError, StreamingAsr,
};
use crate::clock::TokioClock;
use crate::config::VoiceRuntimeConfig;
use crate::events::AsrSource;
use crate::events::VoiceEvent;
use crate::ids::Revision;
use crate::interaction::brain::{InteractionBrain, NoopInteractionBrain};
use crate::media::{AudioFrame, FrameSequenceTracker, PcmResampler, build_resampler};
use crate::meta::ClockMetaFactory;
use crate::metrics::Metrics;
use crate::playback::{ClientPlaybackSink, PlaybackCommand, PlaybackSink};
use crate::protocol::{ClientControlMessage, ServerMessage};
use crate::provider::ProviderSessionControl;
use crate::semantics::extractor::SlotExtractor;
use crate::session::VoiceSessionBuilder;
use crate::session::deep_worker::DeepWorker;
use crate::tts::buffered::BufferedTtsAdapter;
use crate::tts::fake::FakeTts;
use crate::tts::premade::PremadeTts;
use crate::tts::qwen::QwenRealtimeTts;
use crate::tts::{StreamingTts, TextInputMode};
use crate::vad::EnergyVad;

#[derive(Debug)]
pub enum ClientInput {
    Audio(AudioFrame),
    Control(ClientControlMessage),
}

pub struct GatewaySession {
    pub session_id: crate::ids::SessionId,
    pub input_tx: mpsc::Sender<ClientInput>,
}

struct SessionEntry {
    shutdown: CancellationToken,
    handles: Vec<tokio::task::JoinHandle<()>>,
    injection: Arc<InjectionQueue>,
}

impl Drop for SessionEntry {
    fn drop(&mut self) {
        self.shutdown.cancel();
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// The brains one session owns.
///
/// These are per session on purpose: the framework `RuntimeAgent` carries
/// conversation memory and serializes root turns, so sharing one across
/// WebSocket clients would leak transcripts between them and make concurrent
/// sessions block each other.
struct SessionBrains {
    task_agent: Arc<dyn CognitiveAgent>,
    speculative_agent: Option<Arc<dyn CognitiveAgent>>,
    interaction_brain: Arc<dyn InteractionBrain>,
    slot_extractor: Arc<dyn SlotExtractor>,
    deep_worker: Option<Arc<dyn DeepWorker>>,
}

pub struct VoiceRuntime {
    config: Arc<VoiceRuntimeConfig>,
    /// Source provider. `StreamingAsr` is a factory, so sharing it is safe;
    /// every session still gets its own `AsrSession` from `open`.
    asr: Arc<dyn StreamingAsr>,
    tts: Arc<dyn StreamingTts>,
    brains: BrainProvider,
    metrics: Arc<crate::metrics::Metrics>,
    sessions: Mutex<HashMap<crate::ids::SessionId, SessionEntry>>,
}

/// Externally supplied speech factories for
/// [`VoiceRuntime::build_with_providers`].
///
/// This carries only speech providers; agents, transport, and tool backends
/// keep their own wiring, so it never grows into a general dependency
/// container.
pub struct SpeechProviders {
    pub asr: Arc<dyn StreamingAsr>,
    pub tts: Arc<dyn StreamingTts>,
}

impl VoiceRuntime {
    pub async fn build(config: VoiceRuntimeConfig) -> anyhow::Result<Arc<Self>> {
        Self::build_from_arc(Arc::new(config)).await
    }

    pub async fn build_from_arc(config: Arc<VoiceRuntimeConfig>) -> anyhow::Result<Arc<Self>> {
        let asr: Arc<dyn StreamingAsr> = match config.asr.provider.as_str() {
            "together" => Arc::new(TogetherNemotronAsr::from_config(&config.asr)?),
            // `inject` is `mock` plus a text channel; the media worker drains
            // the queue itself so every provider can be driven by typed text.
            "inject" | "mock" => Arc::new(MockAsr::quiet()),
            other => {
                anyhow::bail!("unknown asr.provider '{other}' (expected together, inject, or mock)")
            }
        };

        let raw_tts: Arc<dyn StreamingTts> = match config.tts.provider.as_str() {
            "qwen" => Arc::new(QwenRealtimeTts::from_config(&config.tts)?),
            "mock" => Arc::new(FakeTts::new(
                config.tts.sample_rate_hz,
                config.tts.chunk_ms,
                3,
            )),
            other => anyhow::bail!("unknown tts.provider '{other}' (expected qwen or mock)"),
        };

        Self::assemble(config, asr, raw_tts, false).await
    }

    /// Builds a runtime around externally supplied speech providers.
    ///
    /// The factories are host-owned: `asr.provider`, `tts.provider`, the
    /// endpoint and model strings, and the API-key environment variables are
    /// not consulted for construction on this path. Locale, manual commit,
    /// audio timing, language, voice, and premade settings still apply,
    /// because they describe the session rather than the provider
    /// implementation. A provider without duplex support fails the build.
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use voice_live::{SpeechProviders, VoiceRuntime, VoiceRuntimeConfig};
    /// use voice_live::asr::fake::MockAsr;
    /// use voice_live::tts::fake::FakeTts;
    /// # async fn example() -> anyhow::Result<()> {
    /// let config = VoiceRuntimeConfig::from_yaml_str("version: 1")?;
    /// let runtime = VoiceRuntime::build_with_providers(config, SpeechProviders {
    ///     asr: Arc::new(MockAsr::quiet()),
    ///     tts: Arc::new(FakeTts::new(24_000, 60, 3)),
    /// }).await?;
    /// assert_eq!(runtime.session_count(), 0);
    /// # Ok(()) }
    /// ```
    pub async fn build_with_providers(
        config: VoiceRuntimeConfig,
        providers: SpeechProviders,
    ) -> anyhow::Result<Arc<Self>> {
        Self::build_with_providers_from_arc(Arc::new(config), providers).await
    }

    pub async fn build_with_providers_from_arc(
        config: Arc<VoiceRuntimeConfig>,
        providers: SpeechProviders,
    ) -> anyhow::Result<Arc<Self>> {
        Self::assemble(config, providers.asr, providers.tts, true).await
    }

    /// Common assembly for the built-in and injected provider paths.
    async fn assemble(
        config: Arc<VoiceRuntimeConfig>,
        asr: Arc<dyn StreamingAsr>,
        raw_tts: Arc<dyn StreamingTts>,
        injected: bool,
    ) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        let brains = BrainProvider::resolve(&config, injected).await?;
        if !asr.supports_duplex() {
            anyhow::bail!("the asr provider does not support duplex sessions");
        }

        // Capability dispatch, decided without opening any network session.
        // Wrapping twice is impossible: the adapter reports Buffered with
        // stream support, which the table passes through untouched.
        let tts: Arc<dyn StreamingTts> =
            match (raw_tts.text_input_mode(), raw_tts.supports_text_stream()) {
                (TextInputMode::Buffered, false) => BufferedTtsAdapter::new(raw_tts),
                (TextInputMode::Buffered, true) | (TextInputMode::Incremental, true) => raw_tts,
                (TextInputMode::Incremental, false) => {
                    anyhow::bail!(
                        "tts provider declares Incremental input without text-stream support"
                    );
                }
            };

        let tts: Arc<dyn StreamingTts> =
            if config.tts.premade.enabled && !config.tts.premade.phrases.is_empty() {
                let premade = Arc::new(PremadeTts::with_rate(
                    tts,
                    config.tts.premade.phrases.clone(),
                    config.tts.language.clone(),
                    config.tts.resolved_voice(),
                    Some(config.tts.sample_rate_hz),
                ));
                premade
                    .warmup_with_limits(
                        std::time::Duration::from_millis(config.tts.request_timeout_ms),
                        config.tts.max_input_bytes,
                    )
                    .await;
                premade
            } else {
                tts
            };

        Ok(Arc::new(Self {
            asr,
            tts,
            brains,
            metrics: Metrics::new(),
            sessions: Mutex::new(HashMap::new()),
            config,
        }))
    }

    pub fn config(&self) -> &VoiceRuntimeConfig {
        &self.config
    }

    pub fn metrics(&self) -> &crate::metrics::Metrics {
        &self.metrics
    }

    /// Promotes typed text to an ASR final for one specific session.
    ///
    /// Returns false when injection is disabled or the session is unknown, so
    /// the gateway can tell the client instead of dropping it silently.
    pub fn inject_user_text(&self, session_id: crate::ids::SessionId, text: &str) -> bool {
        if !self.config.dev.allow_text_injection {
            return false;
        }

        let queue = self
            .sessions
            .lock()
            .get(&session_id)
            .map(|entry| Arc::clone(&entry.injection));

        match queue {
            Some(queue) => {
                queue.push(text);
                true
            }
            None => false,
        }
    }

    pub async fn create_session(
        self: &Arc<Self>,
        client_tx: mpsc::Sender<ServerMessage>,
    ) -> anyhow::Result<GatewaySession> {
        let brains = self.brains.create().await?;
        let session_id = crate::ids::SessionId::new();
        let shutdown = CancellationToken::new();
        let clock: crate::clock::ClockRef = Arc::new(TokioClock::new());
        let meta_factory: Arc<dyn crate::meta::MetaFactory> =
            Arc::new(ClockMetaFactory::new(Arc::clone(&clock)));

        let (input_tx, input_rx) = mpsc::channel::<ClientInput>(256);

        let (trace_tx, trace_rx) = mpsc::channel::<VoiceEvent>(4096);
        if self.config.observability.event_log {
            crate::logging::EventLogWriter::spawn(
                trace_rx,
                self.config.observability.event_log_dir.clone(),
                session_id,
                self.config.observability.include_sensitive_payloads,
            );
        } else {
            drop(trace_rx);
        }

        let (playback_tx, playback_rx) = mpsc::channel::<PlaybackCommand>(256);
        let bridge = tokio::spawn(run_playback_bridge(
            playback_rx,
            client_tx,
            shutdown.clone(),
        ));

        let playback: Arc<dyn PlaybackSink> = Arc::new(ClientPlaybackSink::for_runtime(
            playback_tx,
            shutdown.clone(),
        ));

        let mut builder = VoiceSessionBuilder::new(
            Arc::clone(&self.config),
            playback,
            Arc::clone(&self.tts),
            brains.task_agent,
        )
        .with_session_id(session_id)
        .with_clock(clock)
        .with_interaction_brain(brains.interaction_brain)
        .with_slot_extractor(brains.slot_extractor)
        .with_metrics(Arc::clone(&self.metrics))
        .with_trace(trace_tx)
        .with_shutdown(shutdown.clone());

        if let Some(speculative_agent) = brains.speculative_agent {
            builder = builder.with_speculative_agent(speculative_agent);
        }

        if let Some(deep_worker) = brains.deep_worker {
            builder = builder.with_deep_worker(deep_worker);
        }

        let (session, session_handle_internal) = builder.build();

        let event_tx_for_media = session_handle_internal.event_tx.clone();

        let handle = tokio::spawn(async move {
            if let Err(error) = session.run().await {
                tracing::warn!(?error, "session supervisor ended");
            }
        });

        // Text injection is scoped to this session so a multi-client gateway
        // never routes one client's typed input into another's transcript.
        // The media worker drains the queue itself, so typed finals keep
        // flowing even while the provider connection is reconnecting.
        let injection = InjectionQueue::new();

        let media = tokio::spawn(run_media_worker(MediaWorkerArgs {
            input_rx,
            event_tx: event_tx_for_media,
            asr: Arc::clone(&self.asr),
            config: Arc::clone(&self.config),
            shutdown: shutdown.clone(),
            meta_factory,
            injection: Arc::clone(&injection),
            metrics: Arc::clone(&self.metrics),
        }));

        self.sessions.lock().insert(
            session_id,
            SessionEntry {
                shutdown,
                handles: vec![handle, media, bridge],
                injection,
            },
        );

        Ok(GatewaySession {
            session_id,
            input_tx,
        })
    }

    pub async fn close_session(&self, session_id: crate::ids::SessionId) {
        let entry = self.sessions.lock().remove(&session_id);
        if let Some(mut entry) = entry {
            entry.shutdown.cancel();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            for mut handle in entry.handles.drain(..) {
                if tokio::time::timeout_at(deadline, &mut handle)
                    .await
                    .is_err()
                {
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }
    }

    pub fn session_count(&self) -> usize {
        self.sessions.lock().len()
    }
}

/// Translates playback commands into gateway messages. One speech at a
/// time: a speech_started JSON precedes the first chunk of each speech and
/// a speech_done JSON follows the last one, so the client can ACK with the
/// right speech id.
async fn run_playback_bridge(
    mut playback_rx: mpsc::Receiver<PlaybackCommand>,
    client_tx: mpsc::Sender<ServerMessage>,
    shutdown: CancellationToken,
) {
    let mut current_speech: Option<crate::ids::SpeechId> = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,

            command = playback_rx.recv() => {
                let Some(command) = command else { break };

                let message = match command {
                    PlaybackCommand::Enqueue {
                        speech_id,
                        speech_epoch,
                        sequence,
                        sample_rate,
                        pcm,
                    } => {
                        if current_speech != Some(speech_id) {
                            current_speech = Some(speech_id);

                            let started = serde_json::json!({
                                "type": "speech_started",
                                "speech_id": speech_id.to_string(),
                                "speech_epoch": speech_epoch,
                                "sample_rate": sample_rate,
                            });

                            if !send_client(&client_tx, ServerMessage::Json(started), &shutdown).await {
                                break;
                            }
                        }

                        let mut payload =
                            Vec::with_capacity(8 + pcm.len());
                        payload.extend_from_slice(&(sequence as u64).to_le_bytes());
                        payload.extend_from_slice(&pcm);
                        ServerMessage::Binary(bytes::Bytes::from(payload))
                    }

                    PlaybackCommand::MarkDone { speech_id, .. } => {
                        current_speech = None;
                        ServerMessage::Json(serde_json::json!({
                            "type": "speech_done",
                            "speech_id": speech_id.to_string(),
                        }))
                    }

                    PlaybackCommand::Duck { gain, fade_ms } => {
                        ServerMessage::Json(serde_json::json!({
                            "type": "playback_duck",
                            "gain": gain,
                            "fade_ms": fade_ms,
                        }))
                    }

                    PlaybackCommand::Resume { gain, fade_ms } => {
                        ServerMessage::Json(serde_json::json!({
                            "type": "playback_resume",
                            "gain": gain,
                            "fade_ms": fade_ms,
                        }))
                    }

                    PlaybackCommand::Abort {
                        new_speech_epoch,
                        reason,
                    } => {
                        current_speech = None;
                        ServerMessage::Json(serde_json::json!({
                            "type": "playback_abort",
                            "new_speech_epoch": new_speech_epoch,
                            "reason": reason,
                        }))
                    }

                    PlaybackCommand::ClearAll => {
                        current_speech = None;
                        ServerMessage::Json(serde_json::json!({
                            "type": "playback_clear",
                        }))
                    }

                    PlaybackCommand::Shutdown => {
                        ServerMessage::Json(serde_json::json!({
                            "type": "session_stopped",
                        }))
                    }
                };

                if !send_client(&client_tx, message, &shutdown).await {
                    break;
                }
            }
        }
    }
}

async fn send_client(
    tx: &mpsc::Sender<ServerMessage>,
    message: ServerMessage,
    shutdown: &CancellationToken,
) -> bool {
    let sent = tokio::select! {
        _ = shutdown.cancelled() => return false,
        sent = tokio::time::timeout(std::time::Duration::from_secs(1), tx.send(message)) => sent,
    };
    if !matches!(sent, Ok(Ok(()))) {
        tracing::error!("client output stalled or closed; terminating session");
        shutdown.cancel();
        return false;
    }
    true
}

/// How this runtime builds brains for a session.
///
/// Resolved once at startup so the fail-fast and dev-fallback behaviour stays
/// at process start rather than surfacing on a client's first WebSocket frame.
enum BrainMode {
    /// Deterministic offline brains. Stateless, so construction is trivial.
    Mock,
    #[cfg(feature = "framework")]
    Framework {
        /// The demo booking backend. Shared on purpose: it models an external
        /// system of record, not per-conversation state.
        store: Arc<Mutex<crate::tools::reservation::ReservationStore>>,
    },
}

struct BrainProvider {
    /// Only the framework path rebuilds from config; the mock path is stateless.
    #[cfg_attr(not(feature = "framework"), allow(dead_code))]
    config: Arc<VoiceRuntimeConfig>,
    mode: BrainMode,
}

impl BrainProvider {
    async fn resolve(config: &Arc<VoiceRuntimeConfig>, injected: bool) -> anyhow::Result<Self> {
        #[cfg(not(feature = "framework"))]
        if injected && config.agents.task.spec.is_some() {
            anyhow::bail!("an injected task spec requires the framework feature");
        }
        #[cfg(feature = "framework")]
        if let Some(spec) = &config.agents.task.spec {
            // Probe the spec so a bad YAML or a missing LLM key is reported at
            // startup rather than on a client's first frame. Development
            // provider setups fall back to the mock brain instead, which is
            // what makes the keyless demo run. Injected providers are strict:
            // a real provider must not be mistaken for a dev setup because
            // the config still names built-in provider strings.
            match crate::agent::framework::probe_agent_spec(spec) {
                Ok(()) => {
                    return Ok(Self {
                        config: Arc::clone(config),
                        mode: BrainMode::Framework {
                            store: crate::tools::reservation::ReservationStore::demo(),
                        },
                    });
                }
                Err(error) if !injected && uses_only_development_providers(config) => {
                    tracing::warn!(%error, "agent framework setup failed; using mock task agent");
                }
                Err(error) => return Err(error),
            }
        }

        Ok(Self {
            config: Arc::clone(config),
            mode: BrainMode::Mock,
        })
    }

    async fn create(&self) -> anyhow::Result<SessionBrains> {
        match &self.mode {
            BrainMode::Mock => Ok(SessionBrains {
                task_agent: Arc::new(MockReservationAgent),
                speculative_agent: None,
                interaction_brain: Arc::new(NoopInteractionBrain),
                slot_extractor: Arc::new(crate::semantics::extractor::HeuristicKoreanSlotExtractor),
                deep_worker: None,
            }),

            #[cfg(feature = "framework")]
            BrainMode::Framework { store } => {
                let spec = self
                    .config
                    .agents
                    .task
                    .spec
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("missing task agent spec"))?;
                build_framework_brains(&self.config, spec, store).await
            }
        }
    }
}

#[cfg(feature = "framework")]
fn uses_only_development_providers(config: &VoiceRuntimeConfig) -> bool {
    matches!(config.asr.provider.as_str(), "mock" | "inject") && config.tts.provider == "mock"
}

#[cfg(feature = "framework")]
async fn build_framework_brains(
    config: &Arc<VoiceRuntimeConfig>,
    task_spec: &std::path::Path,
    reservation_store: &Arc<Mutex<crate::tools::reservation::ReservationStore>>,
) -> anyhow::Result<SessionBrains> {
    use crate::agent::framework::{
        AiAgentsCognitiveAgent, RuntimeDeepWorker, RuntimeInteractionBrain, RuntimeSlotExtractor,
        VoiceAgentHooks, build_runtime_agent, build_runtime_agent_with_memory,
        build_runtime_agent_with_tools, conversation_memory_for_spec,
    };
    use crate::tools::reservation::{speculative_tools, task_tools};

    // voice-live owns the task agent's conversation memory so each turn can
    // be rewritten to what the user heard, or rolled back. The same handle
    // goes to the builder and to the adapter.
    let memory = conversation_memory_for_spec(task_spec)?;
    let hooks = VoiceAgentHooks::new();
    let runtime = build_runtime_agent_with_memory(
        task_spec,
        Arc::clone(&hooks),
        task_tools(Arc::clone(reservation_store)),
        Some(Arc::clone(&memory)),
    )
    .await?;
    let task_agent: Arc<dyn CognitiveAgent> = Arc::new(AiAgentsCognitiveAgent::with_turn_memory(
        runtime,
        Arc::clone(&hooks),
        memory,
    ));

    // Root turns serialize per runtime. Speculative work therefore uses a
    // dedicated agent spec that exposes only read-only tools.
    let speculative_agent = if config.agents.speculative.enabled {
        let speculative_spec = config
            .agents
            .speculative
            .spec
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing speculative agent spec"))?;
        let speculative_hooks = VoiceAgentHooks::new();
        let speculative_runtime = build_runtime_agent_with_tools(
            speculative_spec,
            Arc::clone(&speculative_hooks),
            speculative_tools(Arc::clone(reservation_store)),
        )
        .await?;
        Some(Arc::new(AiAgentsCognitiveAgent::new(
            speculative_runtime,
            speculative_hooks,
        )) as Arc<dyn CognitiveAgent>)
    } else {
        None
    };

    // The controller spec backs both the floor brain and the slot extractor,
    // but they must not share a runtime: root turns serialize per runtime, and
    // a slot extraction running during a floor decision would deadlock the tick.
    let (interaction_brain, slot_extractor): (Arc<dyn InteractionBrain>, Arc<dyn SlotExtractor>) =
        match &config.agents.interaction.spec {
            Some(spec) if config.agents.interaction.enabled => {
                let brain_runtime = build_runtime_agent(spec, VoiceAgentHooks::new()).await;
                let extractor_runtime = build_runtime_agent(spec, VoiceAgentHooks::new()).await;

                let brain: Arc<dyn InteractionBrain> = match brain_runtime {
                    Ok(runtime) => Arc::new(RuntimeInteractionBrain::new(
                        runtime,
                        config.agents.interaction.timeout_ms,
                    )),
                    Err(error) => {
                        tracing::warn!(%error, "interaction brain unavailable; reflex policy only");
                        Arc::new(NoopInteractionBrain)
                    }
                };

                let extractor: Arc<dyn SlotExtractor> = match extractor_runtime {
                    Ok(runtime) => Arc::new(RuntimeSlotExtractor::new(runtime)),
                    Err(error) => {
                        tracing::warn!(%error, "slot extractor unavailable; using heuristics");
                        Arc::new(crate::semantics::extractor::HeuristicKoreanSlotExtractor)
                    }
                };

                (brain, extractor)
            }
            _ => (
                Arc::new(NoopInteractionBrain),
                Arc::new(crate::semantics::extractor::HeuristicKoreanSlotExtractor),
            ),
        };

    let deep_worker: Option<Arc<dyn DeepWorker>> = match &config.agents.deep.spec {
        Some(spec) if config.agents.deep.enabled => {
            match build_runtime_agent(spec, VoiceAgentHooks::new()).await {
                Ok(deep_runtime) => {
                    Some(Arc::new(RuntimeDeepWorker::new(deep_runtime)) as Arc<dyn DeepWorker>)
                }
                Err(error) => {
                    tracing::warn!(%error, "deep worker unavailable");
                    None
                }
            }
        }
        _ => None,
    };

    Ok(SessionBrains {
        task_agent,
        speculative_agent,
        interaction_brain,
        slot_extractor,
        deep_worker,
    })
}

const RECONNECT_BACKOFF_MIN_MS: u64 = 250;
const RECONNECT_BACKOFF_MAX_MS: u64 = 8_000;
const ASR_COMMAND_BUDGET: usize = 64;

/// Runtime-issued transcript identity, mapped per source.
///
/// Provider revisions and utterance ids restart with every connection, so
/// the media layer re-issues both from session-lifetime counters. Finals and
/// late partials at or below the sealed high-water mark are duplicates and
/// are discarded; a repeated string under a fresh id stays a legitimate new
/// utterance.
#[derive(Default)]
struct AsrIdentity {
    runtime_revision: u64,
    next_provider_id: u64,

    active_provider_id: Option<(u64, u64)>,
    provider_sealed: Option<u64>,
}

impl AsrIdentity {
    fn reset_generation(&mut self) {
        self.active_provider_id = None;
        self.provider_sealed = None;
    }

    fn runtime_id_for(&mut self, provider_id: u64) -> u64 {
        if let Some((active_id, runtime_id)) = self.active_provider_id
            && active_id == provider_id
        {
            return runtime_id;
        }
        let runtime_id = self.next_provider_id;
        self.next_provider_id += 1;
        self.active_provider_id = Some((provider_id, runtime_id));
        runtime_id
    }

    fn map_partial(&mut self, provider_id: u64, text: String) -> Option<(u64, Revision, String)> {
        if let Some(sealed) = self.provider_sealed
            && provider_id <= sealed
        {
            return None;
        }

        let runtime_id = self.runtime_id_for(provider_id);
        self.runtime_revision += 1;
        Some((runtime_id, Revision(self.runtime_revision), text))
    }

    fn map_final(&mut self, provider_id: u64, text: String) -> Option<(u64, String)> {
        if let Some(sealed) = self.provider_sealed
            && provider_id <= sealed
        {
            return None;
        }

        let runtime_id = self.runtime_id_for(provider_id);
        self.provider_sealed = Some(provider_id);
        self.active_provider_id = None;
        Some((runtime_id, text))
    }

    fn injection_final(&mut self, text: String) -> (u64, String) {
        let runtime_id = self.next_provider_id;
        self.next_provider_id += 1;
        (runtime_id, text)
    }
}

struct MediaState {
    resampler: Option<Box<dyn PcmResampler>>,
    sequence: FrameSequenceTracker,
    vad: EnergyVad,
    channels: u16,
    manual_commit: bool,
    identity: AsrIdentity,
}

/// One established duplex connection with its three handles.
struct AsrConnection {
    input: Box<dyn AsrAudioInput>,
    events: Box<dyn AsrEventStream>,
    control: Box<dyn ProviderSessionControl>,
}

/// Connection lifecycle. The open attempt runs in its own task so the media
/// loop keeps processing VAD and control while connecting.
enum AsrLinkState {
    Stopped,
    Connecting {
        attempt: tokio::task::JoinHandle<Result<AsrDuplexSession, AsrOpenError>>,
        token: CancellationToken,
    },
    Ready(AsrConnection),
    RetryWaiting {
        due: tokio::time::Instant,
    },
}

struct AsrLink {
    state: AsrLinkState,
    generation: u64,
    backoff_ms: u64,
    connected_once: bool,
    cleanup: tokio::task::JoinSet<()>,
}

impl AsrLink {
    fn new() -> Self {
        Self {
            state: AsrLinkState::Stopped,
            generation: 0,
            backoff_ms: RECONNECT_BACKOFF_MIN_MS,
            connected_once: false,
            cleanup: tokio::task::JoinSet::new(),
        }
    }

    fn start_connect(&mut self, asr: &Arc<dyn StreamingAsr>, config: &Arc<VoiceRuntimeConfig>) {
        self.generation += 1;
        let token = CancellationToken::new();
        let attempt = spawn_asr_open(asr, config, token.clone());
        self.state = AsrLinkState::Connecting { attempt, token };
    }

    async fn teardown(&mut self) {
        match std::mem::replace(&mut self.state, AsrLinkState::Stopped) {
            AsrLinkState::Stopped | AsrLinkState::RetryWaiting { .. } => {}
            AsrLinkState::Connecting { token, mut attempt } => {
                token.cancel();
                if let Ok(Ok(Ok(mut session))) =
                    tokio::time::timeout(std::time::Duration::from_secs(1), &mut attempt).await
                {
                    session.control.cancel();
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        session.control.close(),
                    )
                    .await;
                } else {
                    attempt.abort();
                }
            }
            AsrLinkState::Ready(mut connection) => {
                connection.control.cancel();
                self.cleanup.spawn(async move {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        connection.control.close(),
                    )
                    .await;
                });
            }
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        while !self.cleanup.is_empty() {
            if tokio::time::timeout_at(deadline, self.cleanup.join_next())
                .await
                .is_err()
            {
                self.cleanup.abort_all();
                while self.cleanup.join_next().await.is_some() {}
                break;
            }
        }
    }

    /// Cancels a ready connection and schedules a fresh reconnect. The queued
    /// audio dies with the connection; it is never replayed.
    async fn drop_connection(&mut self) {
        if let AsrLinkState::Ready(mut connection) =
            std::mem::replace(&mut self.state, AsrLinkState::Stopped)
        {
            connection.control.cancel();
            self.cleanup.spawn(async move {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    connection.control.close(),
                )
                .await;
            });
        }

        let backoff = self.backoff_ms;
        self.backoff_ms = (backoff * 2).min(RECONNECT_BACKOFF_MAX_MS);
        self.state = AsrLinkState::RetryWaiting {
            due: tokio::time::Instant::now() + std::time::Duration::from_millis(backoff),
        };
    }
}

impl Drop for AsrLink {
    fn drop(&mut self) {
        match &self.state {
            AsrLinkState::Connecting { attempt, token } => {
                token.cancel();
                attempt.abort();
            }
            AsrLinkState::Ready(connection) => connection.control.cancel(),
            _ => {}
        }
    }
}

enum LinkPoll {
    OpenAttempt(Result<AsrDuplexSession, AsrOpenError>),
    Event(AsrEvent),
    ConnectionClosed,
    ConnectionError(String),
    RetryDue,
}

/// Cancel safe: every arm either parks, awaits a join handle, or awaits a
/// contractually cancel-safe provider stream, so losing this future to the
/// worker's `select!` drops no transcript.
async fn poll_link(link: &mut AsrLink) -> LinkPoll {
    match &mut link.state {
        AsrLinkState::Stopped => std::future::pending::<LinkPoll>().await,
        AsrLinkState::Connecting { attempt, .. } => match attempt.await {
            Ok(result) => LinkPoll::OpenAttempt(result),
            Err(error) => LinkPoll::OpenAttempt(Err(AsrOpenError::Transient(format!(
                "asr open task failed: {error}"
            )))),
        },
        AsrLinkState::Ready(connection) => match connection.events.next_event().await {
            Ok(Some(event)) => LinkPoll::Event(event),
            // `Ok(None)` is a closed session, never "idle".
            Ok(None) => LinkPoll::ConnectionClosed,
            Err(error) => LinkPoll::ConnectionError(error.to_string()),
        },
        AsrLinkState::RetryWaiting { due } => {
            tokio::time::sleep_until(*due).await;
            LinkPoll::RetryDue
        }
    }
}

fn spawn_asr_open(
    asr: &Arc<dyn StreamingAsr>,
    config: &Arc<VoiceRuntimeConfig>,
    token: CancellationToken,
) -> tokio::task::JoinHandle<Result<AsrDuplexSession, AsrOpenError>> {
    let session_config =
        crate::asr::asr_session_config(&config.asr, config.audio.input_sample_rate_hz);
    let limits = asr_duplex_limits(config);
    let open_timeout = std::time::Duration::from_millis(config.asr.open_timeout_ms);
    let asr = Arc::clone(asr);

    tokio::spawn(async move {
        tokio::select! {
            _ = token.cancelled() => Err(AsrOpenError::Rejected("open cancelled".into())),
            _ = tokio::time::sleep(open_timeout) => {
                Err(AsrOpenError::TimedOut("asr open timed out".into()))
            }
            opened = asr.open_duplex(session_config, limits, token.clone()) => opened,
        }
    })
}

fn asr_duplex_limits(config: &VoiceRuntimeConfig) -> AsrDuplexLimits {
    let rate = config.audio.input_sample_rate_hz as u64;
    let sample_budget = rate.saturating_mul(config.asr.input_buffer_ms) / 1_000;
    AsrDuplexLimits {
        sample_budget: usize::try_from(sample_budget).expect("validated input sample budget"),
        command_budget: ASR_COMMAND_BUDGET,
        write_timeout: std::time::Duration::from_millis(config.asr.write_timeout_ms),
    }
}

/// Nonblocking event admission. A full supervisor queue means the session is
/// overloaded; failing the session beats parking the media loop forever or
/// silently dropping essential events.
fn emit(event_tx: &mpsc::Sender<VoiceEvent>, event: VoiceEvent) -> bool {
    if event_tx.try_send(event).is_ok() {
        return true;
    }
    tracing::error!("media event queue saturated; failing the session");
    false
}

/// Everything the media worker owns for one session.
struct MediaWorkerArgs {
    input_rx: mpsc::Receiver<ClientInput>,
    event_tx: mpsc::Sender<VoiceEvent>,
    asr: Arc<dyn StreamingAsr>,
    config: Arc<VoiceRuntimeConfig>,
    shutdown: CancellationToken,
    meta_factory: Arc<dyn crate::meta::MetaFactory>,
    injection: Arc<InjectionQueue>,
    metrics: Arc<Metrics>,
}

async fn run_media_worker(args: MediaWorkerArgs) {
    let MediaWorkerArgs {
        mut input_rx,
        event_tx,
        asr,
        config,
        shutdown,
        meta_factory,
        injection,
        metrics,
    } = args;
    let mut state = MediaState {
        resampler: None,
        sequence: FrameSequenceTracker::default(),
        vad: EnergyVad::new(config.vad.clone()),
        channels: 1,
        manual_commit: config.asr.manual_commit,
        identity: AsrIdentity::default(),
    };
    let mut link = AsrLink::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                link.teardown().await;
                break;
            }

            input = input_rx.recv() => {
                let Some(input) = input else {
                    link.teardown().await;
                    break;
                };
                match input {
                    ClientInput::Audio(frame) => {
                        if !handle_audio_frame(
                            &mut state,
                            &mut link,
                            frame,
                            &event_tx,
                            &meta_factory,
                            &metrics,
                        )
                        .await
                        {
                            shutdown.cancel();
                            link.teardown().await;
                            break;
                        }
                    }
                    ClientInput::Control(message) => {
                        match handle_control_message(
                            &mut state,
                            &mut link,
                            message,
                            &event_tx,
                            &asr,
                            &config,
                            &meta_factory,
                        )
                        .await
                        {
                            MediaFlow::Continue => {}
                            MediaFlow::Stop => {
                                link.teardown().await;
                                break;
                            }
                            MediaFlow::Overload => {
                                shutdown.cancel();
                                link.teardown().await;
                                break;
                            }
                        }
                    }
                }
            }

            _ = injection.wait() => {
                let mut alive = true;
                while let Some(text) = injection.pop() {
                    let (utterance_id, transcript) = state.identity.injection_final(text);
                    if !emit(
                        &event_tx,
                        VoiceEvent::AsrUtteranceFinal {
                            meta: meta_factory.new_meta(),
                            utterance_id,
                            source: AsrSource::Injection,
                            transcript,
                        },
                    ) {
                        alive = false;
                        break;
                    }
                }
                if !alive {
                    shutdown.cancel();
                    link.teardown().await;
                    break;
                }
            }

            poll = poll_link(&mut link) => {
                match poll {
                    LinkPoll::OpenAttempt(result) => {
                        if !handle_open_attempt(
                            result,
                            &mut link,
                            &event_tx,
                            &meta_factory,
                        ) {
                            shutdown.cancel();
                            link.teardown().await;
                            break;
                        }
                    }
                    LinkPoll::Event(event) => {
                        if !handle_asr_event(
                            event,
                            &mut state,
                            &mut link,
                            &event_tx,
                            &meta_factory,
                            &metrics,
                        )
                        .await
                        {
                            shutdown.cancel();
                            link.teardown().await;
                            break;
                        }
                    }
                    LinkPoll::ConnectionClosed => {
                        if !handle_connection_loss(
                            &mut state,
                            &mut link,
                            &event_tx,
                            &meta_factory,
                            &metrics,
                            "asr session closed".to_string(),
                        )
                        .await
                        {
                            shutdown.cancel();
                            link.teardown().await;
                            break;
                        }
                    }
                    LinkPoll::ConnectionError(message) => {
                        if !handle_connection_loss(
                            &mut state,
                            &mut link,
                            &event_tx,
                            &meta_factory,
                            &metrics,
                            message,
                        )
                        .await
                        {
                            shutdown.cancel();
                            link.teardown().await;
                            break;
                        }
                    }
                    LinkPoll::RetryDue => {
                        link.start_connect(&asr, &config);
                    }
                }
            }
        }
        while link.cleanup.try_join_next().is_some() {}
    }
    shutdown.cancel();
}

/// Outcome of admitting one control message.
enum MediaFlow {
    Continue,
    Stop,
    Overload,
}

fn handle_open_attempt(
    result: Result<AsrDuplexSession, AsrOpenError>,
    link: &mut AsrLink,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
) -> bool {
    match result {
        Ok(session) => {
            // Backoff resets only on an actually ready stream.
            link.backoff_ms = RECONNECT_BACKOFF_MIN_MS;
            link.connected_once = true;
            link.state = AsrLinkState::Ready(AsrConnection {
                input: session.input,
                events: session.events,
                control: session.control,
            });
        }
        Err(error) => {
            let retryable = error.retryable();
            if !emit(
                event_tx,
                VoiceEvent::ProviderError {
                    meta: meta_factory.new_meta(),
                    component: "asr".into(),
                    recoverable: retryable,
                    message: format!("asr open failed: {error}"),
                },
            ) {
                return false;
            }

            if retryable {
                let backoff = link.backoff_ms;
                link.backoff_ms = (backoff * 2).min(RECONNECT_BACKOFF_MAX_MS);
                link.state = AsrLinkState::RetryWaiting {
                    due: tokio::time::Instant::now() + std::time::Duration::from_millis(backoff),
                };
            } else {
                // Fatal provider failures stop the ASR source; they are not
                // retried until the next session start.
                link.state = AsrLinkState::Stopped;
            }
        }
    }

    true
}

async fn handle_asr_event(
    event: AsrEvent,
    state: &mut MediaState,
    link: &mut AsrLink,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
    metrics: &Arc<Metrics>,
) -> bool {
    match event {
        AsrEvent::Partial {
            utterance_id,
            revision: _,
            text,
        } => {
            let Some((utterance_id, revision, hypothesis)) =
                state.identity.map_partial(utterance_id, text)
            else {
                return true;
            };
            emit(
                event_tx,
                VoiceEvent::AsrPartial {
                    meta: meta_factory.new_meta(),
                    utterance_id,
                    revision,
                    hypothesis,
                },
            )
        }
        AsrEvent::Final { utterance_id, text } => {
            let Some((utterance_id, transcript)) = state.identity.map_final(utterance_id, text)
            else {
                return true;
            };
            emit(
                event_tx,
                VoiceEvent::AsrUtteranceFinal {
                    meta: meta_factory.new_meta(),
                    utterance_id,
                    source: AsrSource::Audio,
                    transcript,
                },
            )
        }
        AsrEvent::InjectedFinal { text, .. } => {
            let (utterance_id, transcript) = state.identity.injection_final(text);
            emit(
                event_tx,
                VoiceEvent::AsrUtteranceFinal {
                    meta: meta_factory.new_meta(),
                    utterance_id,
                    source: AsrSource::Injection,
                    transcript,
                },
            )
        }
        AsrEvent::Error {
            recoverable,
            message,
        } => {
            if !emit(
                event_tx,
                VoiceEvent::ProviderError {
                    meta: meta_factory.new_meta(),
                    component: "asr".into(),
                    recoverable,
                    message: message.clone(),
                },
            ) {
                return false;
            }

            if !recoverable {
                metrics.inc("voice_asr_fatal_total");
                if !emit(
                    event_tx,
                    VoiceEvent::AsrStreamReset {
                        meta: meta_factory.new_meta(),
                        generation: link.generation,
                    },
                ) {
                    return false;
                }
                state.identity.reset_generation();
                link.drop_connection().await;
                link.state = AsrLinkState::Stopped;
                return true;
            }

            handle_connection_loss(
                state,
                link,
                event_tx,
                meta_factory,
                metrics,
                format!("asr provider error: {message}"),
            )
            .await
        }
    }
}

/// A ready connection was lost. Derived work from its abandoned partials is
/// invalidated before anything new can arrive, then a fresh reconnect is
/// scheduled with backoff.
async fn handle_connection_loss(
    state: &mut MediaState,
    link: &mut AsrLink,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
    metrics: &Arc<Metrics>,
    reason: String,
) -> bool {
    metrics.inc("voice_asr_reconnect_total");

    if link.connected_once
        && !emit(
            event_tx,
            VoiceEvent::AsrStreamReset {
                meta: meta_factory.new_meta(),
                generation: link.generation,
            },
        )
    {
        return false;
    }

    if !emit(
        event_tx,
        VoiceEvent::ProviderError {
            meta: meta_factory.new_meta(),
            component: "asr".into(),
            recoverable: true,
            message: format!("reconnecting: {reason}"),
        },
    ) {
        return false;
    }

    state.identity.reset_generation();
    link.drop_connection().await;
    true
}

/// Outcome of admitting one audio frame to the provider connection.
enum AdmitOutcome {
    /// No live connection; the frame is discarded.
    NoConnection,
    Accepted,
    Overflow,
    ConnectionLost,
}

async fn handle_audio_frame(
    state: &mut MediaState,
    link: &mut AsrLink,
    frame: AudioFrame,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
    metrics: &Arc<Metrics>,
) -> bool {
    let status = state.sequence.accept(frame.sequence);
    if status == crate::media::FrameStatus::LateOrDuplicate {
        return true;
    }
    let now_us = meta_factory.new_meta().monotonic_us;

    if let crate::media::FrameStatus::Gap { missing } = status
        && missing > 25
        && !emit(
            event_tx,
            VoiceEvent::ProviderError {
                meta: meta_factory.new_meta(),
                component: "audio_input".into(),
                recoverable: true,
                message: format!("lost {missing} audio frames"),
            },
        )
    {
        return false;
    }

    let mut pcm = frame.pcm;
    if state.channels == 2 {
        pcm = pcm
            .chunks(2)
            .map(|pair| {
                let left = pair.first().copied().unwrap_or(0);
                let right = pair.get(1).copied().unwrap_or(0);
                (left / 2).saturating_add(right / 2)
            })
            .collect();
    }

    let resampled = match state.resampler.as_mut() {
        Some(resampler) => match resampler.process_i16(&pcm) {
            Ok(resampled) => resampled,
            Err(error) => {
                return emit(
                    event_tx,
                    VoiceEvent::ProviderError {
                        meta: meta_factory.new_meta(),
                        component: "audio_input".into(),
                        recoverable: true,
                        message: format!("resample failed: {error}"),
                    },
                );
            }
        },
        // No resampler means the client rate was rejected at session_start and
        // already reported. Dropping the audio is correct: forwarding it would
        // feed the provider samples at the wrong rate.
        None => return true,
    };

    if resampled.is_empty() {
        return true;
    }

    // Local VAD runs before any provider admission, so barge-in detection
    // never waits on the network.
    let transition = state.vad.process(&resampled, now_us);
    let speech_ended = matches!(transition, crate::vad::VadTransition::SpeechEnded { .. });

    let event = match transition {
        crate::vad::VadTransition::SpeechStarted { probability } => {
            Some(VoiceEvent::LocalSpeechStarted {
                meta: meta_factory.new_meta(),
                vad_probability: probability,
            })
        }
        crate::vad::VadTransition::SpeechEnded { duration_ms } => {
            Some(VoiceEvent::LocalSpeechEnded {
                meta: meta_factory.new_meta(),
                duration_ms,
            })
        }
        crate::vad::VadTransition::None => None,
    };

    if let Some(event) = event
        && !emit(event_tx, event)
    {
        return false;
    }

    if matches!(status, crate::media::FrameStatus::Gap { missing } if missing > 25) {
        return if matches!(link.state, AsrLinkState::Ready(_)) {
            handle_connection_loss(
                state,
                link,
                event_tx,
                meta_factory,
                metrics,
                "large client frame gap".into(),
            )
            .await
        } else {
            true
        };
    }
    let outcome = match &mut link.state {
        AsrLinkState::Ready(connection) => {
            match connection.input.try_push_audio(resampled) {
                Ok(AsrAudioPush::Accepted) => {
                    if speech_ended && state.manual_commit {
                        // The commit follows its audio in the same generation;
                        // a failed audio admission never commits onto a fresh
                        // connection.
                        match connection.input.try_commit_utterance() {
                            Ok(()) => AdmitOutcome::Accepted,
                            Err(AsrInputError::Full) => AdmitOutcome::Overflow,
                            Err(AsrInputError::Closed | AsrInputError::Cancelled) => {
                                AdmitOutcome::ConnectionLost
                            }
                        }
                    } else {
                        AdmitOutcome::Accepted
                    }
                }
                Ok(AsrAudioPush::Full(_)) => AdmitOutcome::Overflow,
                Err(AsrInputError::Full) => AdmitOutcome::Overflow,
                Err(AsrInputError::Closed | AsrInputError::Cancelled) => {
                    AdmitOutcome::ConnectionLost
                }
            }
        }
        _ => AdmitOutcome::NoConnection,
    };

    match outcome {
        // While disconnected the audio is discarded: the gap is reported as a
        // reconnect, never disguised as recognized speech.
        AdmitOutcome::NoConnection | AdmitOutcome::Accepted => true,
        AdmitOutcome::Overflow => {
            metrics.inc("voice_asr_input_overflow_total");
            handle_connection_loss(
                state,
                link,
                event_tx,
                meta_factory,
                metrics,
                "asr input queue overflow".to_string(),
            )
            .await
        }
        AdmitOutcome::ConnectionLost => {
            handle_connection_loss(
                state,
                link,
                event_tx,
                meta_factory,
                metrics,
                "asr connection lost during input".to_string(),
            )
            .await
        }
    }
}

async fn handle_control_message(
    state: &mut MediaState,
    link: &mut AsrLink,
    message: ClientControlMessage,
    event_tx: &mpsc::Sender<VoiceEvent>,
    asr: &Arc<dyn StreamingAsr>,
    config: &Arc<VoiceRuntimeConfig>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
) -> MediaFlow {
    match message {
        ClientControlMessage::SessionStart {
            input_sample_rate,
            channels,
        } => {
            if channels != 1 && channels != 2 {
                return if emit(
                    event_tx,
                    VoiceEvent::ProviderError {
                        meta: meta_factory.new_meta(),
                        component: "audio_input".into(),
                        recoverable: false,
                        message: "client audio must have one or two channels".into(),
                    },
                ) {
                    MediaFlow::Continue
                } else {
                    MediaFlow::Overload
                };
            }
            if !matches!(link.state, AsrLinkState::Stopped) {
                return MediaFlow::Continue;
            }
            state.channels = channels;

            match build_resampler(input_sample_rate, config.audio.input_sample_rate_hz) {
                Ok(resampler) => {
                    tracing::info!(
                        input_sample_rate,
                        target = config.audio.input_sample_rate_hz,
                        "client audio resampler ready"
                    );
                    state.resampler = Some(resampler);
                }
                Err(error) => {
                    // Never fall through to "no resampler": that would stream
                    // audio to the provider under the wrong sample rate label.
                    state.resampler = None;
                    if !emit(
                        event_tx,
                        VoiceEvent::ProviderError {
                            meta: meta_factory.new_meta(),
                            component: "audio_input".into(),
                            recoverable: false,
                            message: format!(
                                "cannot resample client audio {input_sample_rate} Hz -> {} Hz: {error}",
                                config.audio.input_sample_rate_hz
                            ),
                        },
                    ) {
                        return MediaFlow::Overload;
                    }
                    return MediaFlow::Stop;
                }
            }

            link.start_connect(asr, config);
            MediaFlow::Continue
        }

        ClientControlMessage::SessionStop => MediaFlow::Stop,

        ClientControlMessage::LocalVad { .. } => MediaFlow::Continue,

        ClientControlMessage::InjectUserText { .. } => {
            // Handled at the gateway layer through the runtime injector.
            MediaFlow::Continue
        }

        ClientControlMessage::PlaybackChunkCompleted {
            speech_id,
            played_samples,
            ..
        } => {
            if !emit(
                event_tx,
                VoiceEvent::PlaybackProgress {
                    meta: meta_factory.new_meta(),
                    speech_id,
                    played_samples,
                },
            ) {
                return MediaFlow::Overload;
            }
            MediaFlow::Continue
        }

        ClientControlMessage::PlaybackCompleted {
            speech_id,
            speech_epoch: _,
        } => {
            if !emit(
                event_tx,
                VoiceEvent::PlaybackCompleted {
                    meta: meta_factory.new_meta(),
                    speech_id,
                },
            ) {
                return MediaFlow::Overload;
            }
            MediaFlow::Continue
        }

        ClientControlMessage::PlaybackInterrupted {
            speech_id,
            played_samples,
            ..
        } => {
            if !emit(
                event_tx,
                VoiceEvent::PlaybackInterrupted {
                    meta: meta_factory.new_meta(),
                    speech_id,
                    played_samples,
                    reason: crate::interaction::InterruptionReason::UserTurn,
                },
            ) {
                return MediaFlow::Overload;
            }
            MediaFlow::Continue
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partials_map_to_monotonic_runtime_revisions() {
        let mut identity = AsrIdentity::default();

        let (first_id, first_revision, _) = identity.map_partial(0, "토".into()).unwrap();
        let (second_id, second_revision, _) = identity.map_partial(0, "토요".into()).unwrap();

        assert_eq!(first_id, second_id, "one utterance keeps one runtime id");
        assert!(
            second_revision > first_revision,
            "the runtime revision must advance even when the provider repeats one"
        );
    }

    #[test]
    fn finals_share_the_partials_id_and_seal_duplicates() {
        let mut identity = AsrIdentity::default();

        let (partial_id, _, _) = identity.map_partial(3, "토요일".into()).unwrap();
        let (final_id, _) = identity.map_final(3, "토요일 저녁".into()).unwrap();
        assert_eq!(partial_id, final_id);

        assert!(
            identity.map_final(3, "duplicate".into()).is_none(),
            "a duplicate final at or below the high-water mark is discarded"
        );
        assert!(
            identity.map_partial(3, "late".into()).is_none(),
            "a late partial of a sealed utterance is discarded"
        );

        let (next_id, _) = identity.map_final(4, "다른 발화".into()).unwrap();
        assert_eq!(
            next_id,
            final_id + 1,
            "a repeated string under a fresh id stays a new utterance"
        );
    }

    #[test]
    fn generation_reset_remaps_provider_ids_but_never_reuses_runtime_ids() {
        let mut identity = AsrIdentity::default();

        let (first_id, first_revision, _) = identity.map_partial(0, "첫".into()).unwrap();
        identity.map_final(0, "첫 발화".into()).unwrap();

        identity.reset_generation();

        // The provider restarts its ids at zero on a reconnect; the runtime
        // ids and the revision counter never regress.
        let (second_id, second_revision, _) = identity.map_partial(0, "둘".into()).unwrap();

        assert_ne!(first_id, second_id);
        assert!(second_revision > first_revision);
    }

    #[test]
    fn injection_ids_live_in_their_own_domain() {
        let mut identity = AsrIdentity::default();

        let (audio_id, _, _) = identity.map_partial(0, "음성".into()).unwrap();
        let (injection_id, _) = identity.injection_final("주말 예약".into());

        assert_ne!(audio_id, injection_id);
        assert!(injection_id > audio_id);
        let (next_audio, _, _) = identity.map_partial(1, "다음 음성".into()).unwrap();
        assert!(next_audio > injection_id);
    }
}
