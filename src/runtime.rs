use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::CognitiveAgent;
use crate::agent::mock::MockReservationAgent;
use crate::asr::fake::MockAsr;
use crate::asr::inject::{InjectableAsr, InjectionQueue};
use crate::asr::together::TogetherNemotronAsr;
use crate::asr::{AsrEvent, AsrSession, StreamingAsr};
use crate::clock::TokioClock;
use crate::config::VoiceRuntimeConfig;
use crate::events::VoiceEvent;
use crate::interaction::brain::{InteractionBrain, NoopInteractionBrain};
use crate::media::{AudioFrame, FrameSequenceTracker, PcmResampler, build_resampler};
use crate::meta::ClockMetaFactory;
use crate::metrics::Metrics;
use crate::playback::{ClientPlaybackSink, PlaybackCommand, PlaybackSink};
use crate::protocol::{ClientControlMessage, ServerMessage};
use crate::semantics::extractor::SlotExtractor;
use crate::session::VoiceSessionBuilder;
use crate::session::deep_worker::DeepWorker;
use crate::tts::StreamingTts;
use crate::tts::fake::FakeTts;
use crate::tts::premade::PremadeTts;
use crate::tts::qwen::QwenRealtimeTts;
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
    handle: tokio::task::JoinHandle<()>,
    injection: Arc<InjectionQueue>,
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

impl VoiceRuntime {
    pub async fn build(config: VoiceRuntimeConfig) -> anyhow::Result<Arc<Self>> {
        Self::build_from_arc(Arc::new(config)).await
    }

    pub async fn build_from_arc(config: Arc<VoiceRuntimeConfig>) -> anyhow::Result<Arc<Self>> {
        let asr: Arc<dyn StreamingAsr> = match config.asr.provider.as_str() {
            "together" => Arc::new(TogetherNemotronAsr::from_config(&config.asr)?),
            // `inject` is `mock` plus a text channel; the channel is attached
            // per session below so every provider can be driven by typed text.
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

        let tts: Arc<dyn StreamingTts> = if config.tts.premade.enabled
            && !config.tts.premade.phrases.is_empty()
        {
            let premade = Arc::new(PremadeTts::new(raw_tts, config.tts.premade.phrases.clone()));
            premade.warmup().await;
            premade
        } else {
            raw_tts
        };

        let brains = BrainProvider::resolve(&config).await?;

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
        tokio::spawn(run_playback_bridge(
            playback_rx,
            client_tx,
            shutdown.clone(),
        ));

        let playback: Arc<dyn PlaybackSink> = Arc::new(ClientPlaybackSink::new(playback_tx));

        let brains = self.brains.create().await?;

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
        let injection = InjectionQueue::new();
        let asr: Arc<dyn StreamingAsr> = if self.config.dev.allow_text_injection {
            Arc::new(InjectableAsr::new(
                Arc::clone(&self.asr),
                Arc::clone(&injection),
            ))
        } else {
            Arc::clone(&self.asr)
        };

        tokio::spawn(run_media_worker(
            input_rx,
            event_tx_for_media,
            asr,
            Arc::clone(&self.config),
            shutdown.clone(),
            meta_factory,
        ));

        self.sessions.lock().insert(
            session_id,
            SessionEntry {
                shutdown,
                handle,
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
        if let Some(entry) = entry {
            entry.shutdown.cancel();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), entry.handle).await;
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

                            if client_tx.send(ServerMessage::Json(started)).await.is_err() {
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

                if client_tx.send(message).await.is_err() {
                    break;
                }
            }
        }
    }
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
    async fn resolve(config: &Arc<VoiceRuntimeConfig>) -> anyhow::Result<Self> {
        #[cfg(feature = "framework")]
        if let Some(spec) = &config.agents.task.spec {
            // Probe the spec so a bad YAML or a missing LLM key is reported at
            // startup rather than on a client's first frame. Development
            // provider setups fall back to the mock brain instead, which is
            // what makes the keyless demo run.
            match crate::agent::framework::probe_agent_spec(spec) {
                Ok(()) => {
                    return Ok(Self {
                        config: Arc::clone(config),
                        mode: BrainMode::Framework {
                            store: crate::tools::reservation::ReservationStore::demo(),
                        },
                    });
                }
                Err(error) if uses_only_development_providers(config) => {
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

struct MediaState {
    asr_session: Option<Box<dyn AsrSession>>,
    resampler: Option<Box<dyn PcmResampler>>,
    sequence: FrameSequenceTracker,
    vad: EnergyVad,
    recent_frames: std::collections::VecDeque<Vec<i16>>,
    recent_samples: usize,
    channels: u16,
    reconnect_backoff_ms: u64,
    manual_commit: bool,
}

async fn run_media_worker(
    mut input_rx: mpsc::Receiver<ClientInput>,
    event_tx: mpsc::Sender<VoiceEvent>,
    asr: Arc<dyn StreamingAsr>,
    config: Arc<VoiceRuntimeConfig>,
    shutdown: CancellationToken,
    meta_factory: Arc<dyn crate::meta::MetaFactory>,
) {
    let mut state = MediaState {
        asr_session: None,
        resampler: None,
        sequence: FrameSequenceTracker::default(),
        vad: EnergyVad::new(config.vad.clone()),
        recent_frames: std::collections::VecDeque::new(),
        recent_samples: 0,
        channels: 1,
        reconnect_backoff_ms: RECONNECT_BACKOFF_MIN_MS,
        manual_commit: config.asr.manual_commit,
    };

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                if let Some(mut session) = state.asr_session.take() {
                    let _ = session.close().await;
                }
                break;
            }

            input = input_rx.recv() => {
                let Some(input) = input else { break };
                match input {
                    ClientInput::Audio(frame) => {
                        handle_audio_frame(&mut state, frame, &event_tx, &asr, &config, &meta_factory).await;
                    }
                    ClientInput::Control(message) => {
                        if handle_control_message(&mut state, message, &event_tx, &asr, &config, &meta_factory).await {
                            break;
                        }
                    }
                }
            }

            event = poll_asr_event(&mut state) => {
                match event {
                    PollResult::Event(asr_event) => {
                        forward_asr_event(asr_event, &event_tx, &meta_factory).await;
                    }
                    PollResult::NeedsReconnect(reason) => {
                        let _ = event_tx
                            .send(VoiceEvent::ProviderError {
                                meta: meta_factory.new_meta(),
                                component: "asr".into(),
                                recoverable: true,
                                message: format!("reconnecting: {reason}"),
                            })
                            .await;
                        reconnect_asr(&mut state, &asr, &config, &event_tx, &meta_factory).await;
                    }
                    PollResult::NoSession => {}
                }
            }
        }
    }
}

enum PollResult {
    Event(AsrEvent),
    NeedsReconnect(String),
    NoSession,
}

/// Cancel safe: every `AsrSession::next_event` implementation is required to be,
/// so losing this future to the worker's `select!` drops no transcript.
async fn poll_asr_event(state: &mut MediaState) -> PollResult {
    let Some(session) = state.asr_session.as_mut() else {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        return PollResult::NoSession;
    };

    match session.next_event().await {
        Ok(Some(event)) => PollResult::Event(event),
        // `Ok(None)` is a closed session, never "idle". Treating it as idle is
        // what previously let a dead socket spin forever without reconnecting.
        Ok(None) => PollResult::NeedsReconnect("asr session closed".into()),
        Err(error) => PollResult::NeedsReconnect(error.to_string()),
    }
}

async fn handle_audio_frame(
    state: &mut MediaState,
    frame: AudioFrame,
    event_tx: &mpsc::Sender<VoiceEvent>,
    asr: &Arc<dyn StreamingAsr>,
    config: &Arc<VoiceRuntimeConfig>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
) {
    let status = state.sequence.accept(frame.sequence);
    let now_us = meta_factory.new_meta().monotonic_us;

    if let crate::media::FrameStatus::Gap { missing } = status
        && missing > 25
    {
        let _ = event_tx
            .send(VoiceEvent::ProviderError {
                meta: meta_factory.new_meta(),
                component: "audio_input".into(),
                recoverable: true,
                message: format!("lost {missing} audio frames"),
            })
            .await;
        reconnect_asr(state, asr, config, event_tx, meta_factory).await;
        return;
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
                let _ = event_tx
                    .send(VoiceEvent::ProviderError {
                        meta: meta_factory.new_meta(),
                        component: "audio_input".into(),
                        recoverable: true,
                        message: format!("resample failed: {error}"),
                    })
                    .await;
                return;
            }
        },
        // No resampler means the client rate was rejected at session_start and
        // already reported. Dropping the audio is correct: forwarding it would
        // feed the provider samples at the wrong rate.
        None => return,
    };

    if resampled.is_empty() {
        return;
    }

    // Replay window for provider reconnects.
    state.recent_frames.push_back(resampled.clone());
    state.recent_samples += resampled.len();
    while state.recent_samples > 24_000 {
        match state.recent_frames.pop_front() {
            Some(removed) => state.recent_samples -= removed.len(),
            None => break,
        }
    }

    if let Some(session) = state.asr_session.as_mut()
        && session.push_audio(&resampled).await.is_err()
    {
        reconnect_asr(state, asr, config, event_tx, meta_factory).await;
    }

    let transition = state.vad.process(&resampled, now_us);

    let event = match transition {
        crate::vad::VadTransition::SpeechStarted { probability } => {
            Some(VoiceEvent::LocalSpeechStarted {
                meta: meta_factory.new_meta(),
                vad_probability: probability,
            })
        }
        crate::vad::VadTransition::SpeechEnded { duration_ms } => {
            // Server-side endpointing owns the utterance boundary when the
            // provider's own VAD is disabled, so ask for the final here.
            if state.manual_commit
                && let Some(session) = state.asr_session.as_mut()
                && let Err(error) = session.commit_audio().await
            {
                tracing::warn!(%error, "asr commit failed");
            }

            Some(VoiceEvent::LocalSpeechEnded {
                meta: meta_factory.new_meta(),
                duration_ms,
            })
        }
        crate::vad::VadTransition::None => None,
    };

    if let Some(event) = event {
        let _ = event_tx.send(event).await;
    }
}

async fn handle_control_message(
    state: &mut MediaState,
    message: ClientControlMessage,
    event_tx: &mpsc::Sender<VoiceEvent>,
    asr: &Arc<dyn StreamingAsr>,
    config: &Arc<VoiceRuntimeConfig>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
) -> bool {
    match message {
        ClientControlMessage::SessionStart {
            input_sample_rate,
            channels,
        } => {
            state.channels = channels.max(1);

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
                    let _ = event_tx
                        .send(VoiceEvent::ProviderError {
                            meta: meta_factory.new_meta(),
                            component: "audio_input".into(),
                            recoverable: false,
                            message: format!(
                                "cannot resample client audio {input_sample_rate} Hz -> {} Hz: {error}",
                                config.audio.input_sample_rate_hz
                            ),
                        })
                        .await;
                    return false;
                }
            }

            open_asr_session(state, asr, config, event_tx, meta_factory).await;
            false
        }

        ClientControlMessage::SessionStop => {
            if let Some(mut session) = state.asr_session.take() {
                let _ = session.close().await;
            }
            true
        }

        ClientControlMessage::LocalVad { .. } => false,

        ClientControlMessage::InjectUserText { .. } => {
            // Handled at the gateway layer through the runtime injector.
            false
        }

        ClientControlMessage::PlaybackChunkCompleted {
            speech_id,
            played_samples,
            ..
        } => {
            let _ = event_tx
                .send(VoiceEvent::PlaybackProgress {
                    meta: meta_factory.new_meta(),
                    speech_id,
                    played_samples,
                })
                .await;
            false
        }

        ClientControlMessage::PlaybackCompleted {
            speech_id,
            speech_epoch: _,
        } => {
            let _ = event_tx
                .send(VoiceEvent::PlaybackCompleted {
                    meta: meta_factory.new_meta(),
                    speech_id,
                })
                .await;
            false
        }

        ClientControlMessage::PlaybackInterrupted {
            speech_id,
            played_samples,
            ..
        } => {
            let _ = event_tx
                .send(VoiceEvent::PlaybackInterrupted {
                    meta: meta_factory.new_meta(),
                    speech_id,
                    played_samples,
                    reason: crate::interaction::InterruptionReason::UserTurn,
                })
                .await;
            false
        }
    }
}

async fn open_asr_session(
    state: &mut MediaState,
    asr: &Arc<dyn StreamingAsr>,
    config: &Arc<VoiceRuntimeConfig>,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
) {
    let session_config =
        crate::asr::asr_session_config(&config.asr, config.audio.input_sample_rate_hz);

    match asr.open(session_config).await {
        Ok(session) => {
            state.asr_session = Some(session);
            state.reconnect_backoff_ms = RECONNECT_BACKOFF_MIN_MS;

            // Replay the recent window so reconnects do not lose audio.
            for frame in &state.recent_frames {
                if let Some(session) = state.asr_session.as_mut() {
                    let _ = session.push_audio(frame).await;
                }
            }
        }
        Err(error) => {
            let _ = event_tx
                .send(VoiceEvent::ProviderError {
                    meta: meta_factory.new_meta(),
                    component: "asr".into(),
                    recoverable: true,
                    message: format!("asr open failed: {error}"),
                })
                .await;
        }
    }
}

async fn reconnect_asr(
    state: &mut MediaState,
    asr: &Arc<dyn StreamingAsr>,
    config: &Arc<VoiceRuntimeConfig>,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
) {
    if let Some(mut session) = state.asr_session.take() {
        let _ = session.close().await;
    }

    tokio::time::sleep(std::time::Duration::from_millis(state.reconnect_backoff_ms)).await;

    // Back off before the next attempt; a successful open resets this.
    state.reconnect_backoff_ms = (state.reconnect_backoff_ms * 2).min(RECONNECT_BACKOFF_MAX_MS);

    open_asr_session(state, asr, config, event_tx, meta_factory).await;
}

async fn forward_asr_event(
    event: AsrEvent,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
) {
    match event {
        AsrEvent::Partial {
            utterance_id,
            revision,
            text,
        } => {
            let _ = event_tx
                .send(VoiceEvent::AsrPartial {
                    meta: meta_factory.new_meta(),
                    utterance_id,
                    revision,
                    hypothesis: text,
                })
                .await;
        }
        AsrEvent::Final { utterance_id, text } => {
            let _ = event_tx
                .send(VoiceEvent::AsrUtteranceFinal {
                    meta: meta_factory.new_meta(),
                    utterance_id,
                    transcript: text,
                })
                .await;
        }
        AsrEvent::Error {
            recoverable,
            message,
        } => {
            let _ = event_tx
                .send(VoiceEvent::ProviderError {
                    meta: meta_factory.new_meta(),
                    component: "asr".into(),
                    recoverable,
                    message,
                })
                .await;
        }
    }
}
