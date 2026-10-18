use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::agent::{AgentTurnRequest, CognitiveAgent, TurnContext, TurnResolution};
use crate::clock::{ClockRef, TokioClock};
use crate::config::{ToolEffect, VoiceRuntimeConfig};
use crate::events::{AsrSource, DeepWorkResult, SessionSummary, SlotProvenance, VoiceEvent};
use crate::ids::Revision;

use crate::ids::{SpeechId, TaskId, TurnId};
use crate::interaction::brain::{InteractionBrain, NoopInteractionBrain};
use crate::interaction::policy::ReflexPolicy;
use crate::interaction::snapshot::InteractionSnapshot;
use crate::interaction::{
    InteractionAction, InteractionDecisionEnvelope, InterruptionReason, SemanticAssessment,
    SpeechState, UserSpeechState,
};
use crate::meta::{ClockMetaFactory, MetaFactory};
use crate::metrics::{Metrics, MetricsRef};
use crate::playback::{PlaybackCommand, PlaybackSink};
use crate::semantics::SlotName;
use crate::semantics::cue::{CueExtractor, SemanticCue};
use crate::semantics::extractor::SlotExtractor;
use crate::semantics::frame::{SemanticFrame, SlotUpdate};
use crate::semantics::partial_policy::{PartialControllerPolicy, TranscriptSnapshot};
use crate::session::deep_worker::DeepWorker;
use crate::session::task_runner::{ActiveTask, ActiveTaskKind, run_task_turn_with_timeout};
use crate::session::tts_worker::{TtsCancellationRegistry, TtsWorkerSettings, run_tts_worker};
use crate::session::view::SessionView;
use crate::speech::act::{ClaimClass, Interruptibility, SpeechAct};
use crate::speech::claim_gate::ClaimGate;
use crate::speech::ledger::{AudibleLedger, Heard, recent_assistant_text, render_heard_turn};
use crate::transcript::{TranscriptReconciler, TurnAssemblyBuffer};
use crate::tts::StreamingTts;

/// A main turn's reply while it is being spoken.
///
/// The turn is resolved into memory once every clause has settled: played in
/// full, interrupted part way, or dropped without playing.
struct TurnSpeech {
    final_text: String,
    /// Authorized clause acts, in reply order.
    clauses: Vec<(SpeechId, String)>,
}

struct SpeculativeResult {
    fingerprint: u64,
    summary: String,
    dependencies: Vec<String>,
    candidate_generation: u64,
    candidate_utterance_id: Option<u64>,
}

/// Synthesis outcome for one tracked act, kept separate from playback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SynthesisOutcome {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Playback lifecycle for one tracked act. `Stopping` means an Abort was
/// sent and the interrupted acknowledgement (or the stop deadline) is the
/// only thing left to settle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackLifecycle {
    NotStarted,
    Playing,
    Stopping,
    Settled,
}

/// The supervisor's private speech tracking: one entry for the current act
/// and at most one entry for an act that is stopping.
struct SpeechRecord {
    text: String,
    never: bool,
    sample_rate: u32,
    /// The main task whose reply contains this act, if any.
    owner: Option<TaskId>,
    synthesis: SynthesisOutcome,
    playback: PlaybackLifecycle,
    confirmed_samples: u64,
    stop_deadline_us: Option<u64>,
}

/// A slot's authoritative baseline before a candidate overwrote it.
struct SlotJournalEntry {
    previous: Option<crate::semantics::frame::SemanticValue<serde_json::Value>>,
    last_accepted: Option<Option<crate::semantics::frame::SemanticValue<serde_json::Value>>>,
    writer: crate::events::SlotProvenance,
}

/// A committed turn waiting behind a `Never`-protected act. The context is
/// the snapshot selected at commit time; uncommitted partials that arrive
/// while waiting never reshape it.
struct DeferredTurn {
    turn_id: TurnId,
    transcript: String,
    transcript_revision: Revision,
    context: TurnContext,
    dependencies: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
struct SemanticKey {
    candidate: String,
    heard_assistant: String,
    asked_question: bool,
    vad_active: bool,
    origin: Option<(AsrSource, u64)>,
    generation: u64,
    activity: u64,
    interaction_epoch: u64,
    thought_epoch: u64,
    speech_epoch: u64,
    frame_revision: Revision,
    floor: crate::interaction::FloorState,
    speech: SpeechState,
}

struct SemanticResult {
    key: SemanticKey,
    revision: Revision,
    utterance_id: Option<u64>,
    assessment: Option<SemanticAssessment>,
    host_updates: Option<Vec<SlotUpdate>>,
}

struct DeepCompletion {
    task_id: TaskId,
    thought_epoch: u64,
    result: Result<DeepWorkResult, String>,
}

struct SlotExtractionRequest {
    text: String,
    revision: Revision,
    provenance: SlotProvenance,
    host_generation: u64,
}

struct SlotExtractionResult {
    request: SlotExtractionRequest,
    sequence: u64,
    host_generation: u64,
    updates: Vec<SlotUpdate>,
}

pub struct VoiceSession {
    pub session_id: crate::ids::SessionId,
    pub state: SessionView,
    pub config: Arc<VoiceRuntimeConfig>,
    pub clock: ClockRef,
    pub meta_factory: Arc<dyn MetaFactory>,

    pub event_tx: mpsc::Sender<VoiceEvent>,
    event_rx: mpsc::Receiver<VoiceEvent>,
    pub trace_tx: Option<mpsc::Sender<VoiceEvent>>,

    pub playback: Arc<dyn PlaybackSink>,
    pub speech_tx: mpsc::Sender<(SpeechAct, CancellationToken)>,
    pub tts_registry: Arc<TtsCancellationRegistry>,

    pub task_agent: Arc<dyn CognitiveAgent>,
    pub speculative_agent: Option<Arc<dyn CognitiveAgent>>,
    pub interaction_brain: Arc<dyn InteractionBrain>,
    pub slot_extractor: Arc<dyn SlotExtractor>,
    pub deep_worker: Option<Arc<dyn DeepWorker>>,

    pub active_tasks: HashMap<TaskId, ActiveTask>,
    main_worker_done: HashMap<TaskId, watch::Receiver<bool>>,
    pub pending_speech: VecDeque<SpeechAct>,
    speech_records: HashMap<SpeechId, SpeechRecord>,
    /// Reply ownership of queued acts, so a failed synthesis can discard the
    /// remaining clauses of exactly the reply it belonged to.
    act_owner: HashMap<SpeechId, TaskId>,
    /// Monotonic time of the last playback signal for the active speech. The
    /// client owes us an ACK; if it never arrives the queue must not wedge.
    /// Synthesis chunks never refresh it: only real playback does.
    last_playback_signal_us: Option<u64>,

    pub transcript: TranscriptReconciler,
    pub turn_buffer: TurnAssemblyBuffer,
    pub audible_ledger: AudibleLedger,
    pub claim_gate: ClaimGate,
    pub cue_extractor: CueExtractor,
    pub text_analyzer: Arc<dyn crate::semantics::text::TextAnalyzer>,
    pub speech_renderer: Arc<dyn crate::speech::render::SpeechRenderer>,
    invocation_proofs: HashMap<TaskId, Vec<crate::speech::render::InvocationProof>>,
    proof_conflicts: HashSet<TaskId>,
    speech_questions: HashMap<SpeechId, crate::interaction::ExpectedAnswerType>,
    pub policy: ReflexPolicy,
    pub partial_policy: PartialControllerPolicy,

    last_interaction_run_us: Option<u64>,
    partial_last_snapshot: Option<TranscriptSnapshot>,
    semantic_mode: bool,
    semantic_slot_override: bool,
    semantic_activity: u64,
    semantic_origin: Option<(AsrSource, u64)>,
    semantic_stop_seen: Option<(u64, Option<(AsrSource, u64)>)>,
    semantic_overlap_pending: bool,
    semantic_overlap_base: Option<usize>,
    semantic_overlap_audio_max: Option<u64>,
    semantic_heard_anchor: Option<(String, bool)>,
    semantic_in_flight: bool,
    semantic_last_key: Option<SemanticKey>,
    semantic_observation: Option<(SemanticKey, SemanticAssessment)>,
    semantic_pending_updates: Option<(SemanticKey, Vec<SlotUpdate>, Option<SlotProvenance>)>,
    semantic_intent_baseline: Option<Option<crate::semantics::SemanticValue<String>>>,
    semantic_tx: mpsc::Sender<SemanticResult>,
    semantic_rx: mpsc::Receiver<SemanticResult>,
    semantic_stop_tx: mpsc::Sender<SemanticKey>,
    semantic_stop_rx: mpsc::Receiver<SemanticKey>,
    legacy_interaction_tx: mpsc::Sender<(InteractionDecisionEnvelope, u64)>,
    legacy_interaction_rx: mpsc::Receiver<(InteractionDecisionEnvelope, u64)>,
    slot_extraction_tx: mpsc::Sender<SlotExtractionResult>,
    slot_extraction_rx: mpsc::Receiver<SlotExtractionResult>,
    slot_extraction_running: Option<(u64, u64)>,
    slot_extraction_cancel: Option<CancellationToken>,
    slot_extraction_input: Option<(String, SlotProvenance, u64)>,
    legacy_final: Option<(u64, String)>,
    slot_extraction_pending: Option<SlotExtractionRequest>,
    slot_extraction_sequence: u64,
    slot_last_request: Option<(u64, u64, String, u64)>,
    slot_host_generation: u64,
    slot_host_writes: HashMap<SlotName, u64>,
    slot_host_overflow: u64,
    slot_extraction_order: HashMap<SlotName, (u64, u64)>,
    backchannel_count_this_turn: u32,
    last_backchannel_at_us: Option<u64>,
    speculative_results: Vec<SpeculativeResult>,
    deep_results: Vec<DeepWorkResult>,
    deep_deadlines: HashMap<TaskId, u64>,
    deep_tx: mpsc::Sender<DeepCompletion>,
    deep_rx: mpsc::Receiver<DeepCompletion>,
    turn_speech: HashMap<TaskId, TurnSpeech>,
    turn_started_at_us: Option<u64>,
    /// Increments on every ASR stream reset; results derived from older
    /// generations are discarded on arrival.
    candidate_generation: u64,
    /// Highest audio-source utterance whose candidates are closed (committed
    /// or superseded by an authoritative final).
    candidate_closed_watermark: Option<u64>,
    // Audio ids are session-monotonic; only a nonempty partial commit advances this boundary.
    consumed_audio_watermark: Option<u64>,
    suppressed_audio_watermark: Option<u64>,
    semantic_final: Option<(AsrSource, u64, String)>,
    slot_journal: HashMap<crate::semantics::frame::SlotName, SlotJournalEntry>,
    deferred_turn: Option<DeferredTurn>,
    workers: tokio::task::JoinSet<()>,

    pub shutdown: CancellationToken,
    pub metrics: MetricsRef,
}

#[derive(Clone)]
pub struct VoiceSessionHandle {
    pub session_id: crate::ids::SessionId,
    pub event_tx: mpsc::Sender<VoiceEvent>,
}

impl VoiceSessionHandle {
    pub async fn emit(&self, event: VoiceEvent) {
        let _ = self.event_tx.send(event).await;
    }

    pub fn try_emit(&self, event: VoiceEvent) -> bool {
        self.event_tx.try_send(event).is_ok()
    }

    /// Asks the session to run long work off the media loop.
    ///
    /// Nothing inside the media or cognitive loop can tell that a turn needs
    /// minutes rather than seconds, so this is deliberately host driven. The
    /// session owns epoch scoping: a result whose thought epoch has moved on is
    /// discarded instead of reaching the agent.
    pub async fn request_deep_work(
        &self,
        meta: crate::meta::EventMeta,
        objective: impl Into<String>,
        dependencies: Vec<String>,
    ) {
        self.emit(VoiceEvent::DeepWorkRequested {
            meta,
            objective: objective.into(),
            dependencies,
        })
        .await;
    }
}

pub struct VoiceSessionBuilder {
    pub session_id: Option<crate::ids::SessionId>,
    pub config: Arc<VoiceRuntimeConfig>,
    pub clock: ClockRef,
    pub meta_factory: Arc<dyn MetaFactory>,
    pub playback: Arc<dyn PlaybackSink>,
    pub tts: Arc<dyn StreamingTts>,
    pub task_agent: Arc<dyn CognitiveAgent>,
    pub speculative_agent: Option<Arc<dyn CognitiveAgent>>,
    pub interaction_brain: Arc<dyn InteractionBrain>,
    pub slot_extractor: Arc<dyn SlotExtractor>,
    pub deep_worker: Option<Arc<dyn DeepWorker>>,
    pub metrics: MetricsRef,
    pub trace_tx: Option<mpsc::Sender<VoiceEvent>>,
    pub shutdown: CancellationToken,
    text_analyzer: Option<Arc<dyn crate::semantics::text::TextAnalyzer>>,
    speech_renderer: Option<Arc<dyn crate::speech::render::SpeechRenderer>>,
    semantic_slot_override: bool,
}

impl VoiceSessionBuilder {
    pub fn new(
        config: Arc<VoiceRuntimeConfig>,
        playback: Arc<dyn PlaybackSink>,
        tts: Arc<dyn StreamingTts>,
        task_agent: Arc<dyn CognitiveAgent>,
    ) -> Self {
        let clock: ClockRef = Arc::new(TokioClock::new());
        let slot_extractor = crate::semantics::extractor::configured_fallback(&config.turn_control);
        Self {
            session_id: None,
            config,
            clock: Arc::clone(&clock),
            meta_factory: Arc::new(ClockMetaFactory::new(clock)),
            playback,
            tts,
            task_agent,
            speculative_agent: None,
            interaction_brain: Arc::new(NoopInteractionBrain),
            slot_extractor,
            deep_worker: None,
            metrics: Metrics::new(),
            trace_tx: None,
            shutdown: CancellationToken::new(),
            text_analyzer: None,
            speech_renderer: None,
            semantic_slot_override: false,
        }
    }

    pub fn with_text_analyzer(
        mut self,
        analyzer: Arc<dyn crate::semantics::text::TextAnalyzer>,
    ) -> Self {
        self.text_analyzer = Some(analyzer);
        self
    }

    pub fn with_speech_renderer(
        mut self,
        renderer: Arc<dyn crate::speech::render::SpeechRenderer>,
    ) -> Self {
        self.speech_renderer = Some(renderer);
        self
    }

    pub fn with_session_id(mut self, session_id: crate::ids::SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub fn with_clock(mut self, clock: ClockRef) -> Self {
        self.meta_factory = Arc::new(ClockMetaFactory::new(Arc::clone(&clock)));
        self.clock = clock;
        self
    }

    pub fn with_interaction_brain(mut self, brain: Arc<dyn InteractionBrain>) -> Self {
        self.interaction_brain = brain;
        self
    }

    pub fn with_speculative_agent(mut self, agent: Arc<dyn CognitiveAgent>) -> Self {
        self.speculative_agent = Some(agent);
        self
    }

    pub fn with_slot_extractor(mut self, extractor: Arc<dyn SlotExtractor>) -> Self {
        self.slot_extractor = extractor;
        self.semantic_slot_override = true;
        self
    }

    pub fn with_deep_worker(mut self, worker: Arc<dyn DeepWorker>) -> Self {
        self.deep_worker = Some(worker);
        self
    }

    pub fn with_metrics(mut self, metrics: MetricsRef) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_trace(mut self, trace_tx: mpsc::Sender<VoiceEvent>) -> Self {
        self.trace_tx = Some(trace_tx);
        self
    }

    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    pub fn build(self) -> (VoiceSession, VoiceSessionHandle) {
        self.try_build().expect("invalid session configuration")
    }

    pub fn try_build(self) -> Result<(VoiceSession, VoiceSessionHandle)> {
        self.config.validate()?;
        let session_id = self.session_id.unwrap_or_default();
        let (event_tx, event_rx) = mpsc::channel::<VoiceEvent>(1024);
        let (semantic_tx, semantic_rx) = mpsc::channel::<SemanticResult>(1);
        let (semantic_stop_tx, semantic_stop_rx) = mpsc::channel::<SemanticKey>(1);
        let (legacy_interaction_tx, legacy_interaction_rx) = mpsc::channel(64);
        let (slot_extraction_tx, slot_extraction_rx) = mpsc::channel(1);
        let (deep_tx, deep_rx) = mpsc::channel(64);
        let semantic_mode = self.text_analyzer.is_none()
            && (self.interaction_brain.supports_semantics()
                || (self.config.agents.interaction.enabled && self.interaction_brain.is_noop()));
        let (speech_tx, speech_rx) = mpsc::channel::<(SpeechAct, CancellationToken)>(64);
        let tts_registry = Arc::new(TtsCancellationRegistry::default());

        let cue_extractor = CueExtractor::from_config(&self.config.turn_control);
        let text_analyzer = self.text_analyzer.unwrap_or_else(|| {
            crate::semantics::text::ConfiguredTextAnalyzer::shared(&self.config.turn_control)
                .expect("validated text rules")
        });
        let policy = ReflexPolicy::with_analyzer(Arc::clone(&text_analyzer));
        let speech_renderer = self.speech_renderer.unwrap_or_else(|| {
            Arc::new(
                crate::speech::render::ConfiguredRenderer::new(&self.config.speech.outcomes)
                    .expect("validated outcome rules"),
            )
        });
        let asr_partials = self.config.asr.partials.clone();
        let interaction_interval = self.config.agents.interaction.minimum_interval_ms;
        let audible_capacity = self.config.speech.audible_history_max_clauses;

        let tts: Arc<dyn StreamingTts> = if self.tts.text_input_mode()
            == crate::tts::TextInputMode::Buffered
            && !self.tts.supports_text_stream()
        {
            crate::tts::buffered::BufferedTtsAdapter::new(self.tts)
        } else {
            self.tts
        };
        let worker_settings = TtsWorkerSettings {
            language: self.config.tts.language.clone(),
            voice: self.config.tts.resolved_voice(),
            max_input_bytes: self.config.tts.max_input_bytes,
            request_timeout: Duration::from_millis(self.config.tts.request_timeout_ms),
            open_timeout: Duration::from_millis(self.config.tts.open_timeout_ms),
            preferred_sample_rate_hz: Some(self.config.tts.sample_rate_hz),
        };

        let mut workers = tokio::task::JoinSet::new();
        workers.spawn(run_tts_worker(
            tts,
            speech_rx,
            event_tx.clone(),
            Arc::clone(&self.meta_factory),
            self.shutdown.clone(),
            Arc::clone(&tts_registry),
            worker_settings,
        ));

        let session = VoiceSession {
            session_id,
            state: SessionView::new(session_id),
            config: self.config,
            clock: self.clock,
            meta_factory: self.meta_factory,
            event_tx: event_tx.clone(),
            event_rx,
            trace_tx: self.trace_tx,
            playback: self.playback,
            speech_tx,
            tts_registry,
            task_agent: self.task_agent,
            speculative_agent: self.speculative_agent,
            interaction_brain: self.interaction_brain,
            slot_extractor: self.slot_extractor,
            deep_worker: self.deep_worker,
            active_tasks: HashMap::new(),
            main_worker_done: HashMap::new(),
            pending_speech: VecDeque::new(),
            speech_records: HashMap::new(),
            act_owner: HashMap::new(),
            last_playback_signal_us: None,
            transcript: TranscriptReconciler::new(asr_partials),
            turn_buffer: TurnAssemblyBuffer::default(),
            audible_ledger: AudibleLedger::with_capacity(audible_capacity),
            claim_gate: ClaimGate,
            cue_extractor,
            policy,
            text_analyzer,
            speech_renderer,
            invocation_proofs: HashMap::new(),
            proof_conflicts: HashSet::new(),
            speech_questions: HashMap::new(),
            partial_policy: PartialControllerPolicy::from_config(interaction_interval),
            last_interaction_run_us: None,
            partial_last_snapshot: None,
            semantic_mode,
            semantic_slot_override: self.semantic_slot_override,
            semantic_activity: 0,
            semantic_origin: None,
            semantic_stop_seen: None,
            semantic_overlap_pending: false,
            semantic_overlap_base: None,
            semantic_overlap_audio_max: None,
            semantic_heard_anchor: None,
            semantic_in_flight: false,
            semantic_last_key: None,
            semantic_observation: None,
            semantic_pending_updates: None,
            semantic_intent_baseline: None,
            semantic_tx,
            semantic_rx,
            semantic_stop_tx,
            semantic_stop_rx,
            legacy_interaction_tx,
            legacy_interaction_rx,
            slot_extraction_tx,
            slot_extraction_rx,
            slot_extraction_running: None,
            slot_extraction_cancel: None,
            slot_extraction_input: None,
            legacy_final: None,
            slot_extraction_pending: None,
            slot_extraction_sequence: 0,
            slot_last_request: None,
            slot_host_generation: 0,
            slot_host_writes: HashMap::new(),
            slot_host_overflow: 0,
            slot_extraction_order: HashMap::new(),
            backchannel_count_this_turn: 0,
            last_backchannel_at_us: None,
            speculative_results: Vec::new(),
            deep_results: Vec::new(),
            deep_deadlines: HashMap::new(),
            deep_tx,
            deep_rx,
            turn_speech: HashMap::new(),
            turn_started_at_us: None,
            candidate_generation: 0,
            candidate_closed_watermark: None,
            consumed_audio_watermark: None,
            suppressed_audio_watermark: None,
            semantic_final: None,
            slot_journal: HashMap::new(),
            deferred_turn: None,
            workers,
            shutdown: self.shutdown,
            metrics: self.metrics,
        };

        let handle = VoiceSessionHandle {
            session_id,
            event_tx,
        };

        Ok((session, handle))
    }
}

impl VoiceSession {
    pub async fn run(mut self) -> Result<()> {
        let mut control_tick = tokio::time::interval(Duration::from_millis(
            self.config.session.control_tick_ms.max(1),
        ));
        control_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        control_tick.tick().await;

        let shutdown = self.shutdown.clone();
        let result = loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break Ok(()),
                _ = control_tick.tick() => {
                    let result = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break Ok(()),
                        result = self.on_control_tick() => result,
                    };
                    if let Err(error) = result { break Err(error); }
                }
                legacy = self.legacy_interaction_rx.recv(), if !self.semantic_mode => {
                    if let Some((envelope, activity)) = legacy {
                        let result = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => break Ok(()),
                            result = self.apply_legacy_interaction(envelope, activity) => result,
                        };
                        if let Err(error) = result { break Err(error); }
                    }
                }
                stop = self.semantic_stop_rx.recv(), if self.semantic_mode => {
                    if let Some(key) = stop {
                        let result = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => break Ok(()),
                            result = self.apply_semantic_stop(&key) => result,
                        };
                        if let Err(error) = result { break Err(error); }
                    }
                }
                semantic = self.semantic_rx.recv(), if self.semantic_mode => {
                    if let Some(semantic) = semantic {
                        let result = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => break Ok(()),
                            result = self.apply_semantic_result(semantic) => result,
                        };
                        if let Err(error) = result { break Err(error); }
                    }
                }
                extracted = self.slot_extraction_rx.recv() => {
                    if let Some(extracted) = extracted {
                        let result = self.finish_slot_extraction(extracted).await;
                        if let Err(error) = result { break Err(error); }
                    }
                }
                completion = self.deep_rx.recv() => {
                    if let Some(completion) = completion {
                        self.finish_deep_work(completion, true).await;
                    }
                }
                event = self.event_rx.recv() => {
                    let Some(event) = event else { break Ok(()); };
                    let result = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break Ok(()),
                        result = self.handle_event(event) => result,
                    };
                    if let Err(error) = result { break Err(error); }
                }
            }
            self.maybe_spawn_semantic();
            while self.workers.try_join_next().is_some() {}
        };
        self.shutdown_active_work().await;
        result
    }

    fn monotonic_us(&self) -> u64 {
        self.clock.now_us()
    }

    fn new_meta(&self) -> crate::meta::EventMeta {
        self.meta_factory.new_meta()
    }

    async fn forward_trace(&self, event: &VoiceEvent) {
        if let Some(trace) = &self.trace_tx {
            let _ = trace.try_send(event.clone());
        }
    }

    fn mark_semantic_overlap(&mut self, source: AsrSource, utterance_id: u64) {
        if !self.semantic_mode
            || !(self.semantic_overlap_pending
                || self.state.floor == crate::interaction::FloorState::OverlapProbe)
        {
            return;
        }
        self.semantic_overlap_pending = true;
        if self.semantic_overlap_base.is_none() {
            self.semantic_overlap_base = Some(self.turn_buffer.final_count());
        }
        if source == AsrSource::Audio {
            self.semantic_overlap_audio_max = Some(
                self.semantic_overlap_audio_max
                    .map_or(utterance_id, |old| old.max(utterance_id)),
            );
        }
    }

    async fn handle_event(&mut self, event: VoiceEvent) -> Result<()> {
        self.forward_trace(&event).await;

        // Speech events are admitted by identity and lifecycle before any
        // projection or handler runs, so a late chunk, a duplicate terminal,
        // or an old acknowledgement can never touch the current state.
        let audio_id = match &event {
            VoiceEvent::AsrPartial { utterance_id, .. }
            | VoiceEvent::AsrUtteranceFinal {
                utterance_id,
                source: AsrSource::Audio,
                ..
            } => Some(*utterance_id),
            _ => None,
        };
        if audio_id.is_some_and(|id| {
            self.consumed_audio_watermark
                .is_some_and(|consumed| id <= consumed)
                || self
                    .suppressed_audio_watermark
                    .is_some_and(|suppressed| id <= suppressed)
        }) {
            return Ok(());
        }
        if !self.admit_speech_event(&event) {
            return Ok(());
        }

        if self.should_apply_projection(&event) {
            self.state.apply(&event);
        }

        match event {
            VoiceEvent::LocalSpeechStarted { .. } => {
                self.semantic_activity = self.semantic_activity.wrapping_add(1);
                if self.semantic_mode {
                    self.semantic_heard_anchor = Some((
                        self.heard_semantic_context(),
                        self.state.assistant_asked_question,
                    ));
                    let continuing_overlap =
                        self.semantic_overlap_pending && self.semantic_overlap_base.is_some();
                    self.semantic_overlap_pending = matches!(
                        self.state.speech,
                        SpeechState::Playing | SpeechState::Ducking
                    );
                    if !continuing_overlap || !self.semantic_overlap_pending {
                        self.semantic_overlap_base = self
                            .semantic_overlap_pending
                            .then(|| self.turn_buffer.final_count());
                        self.semantic_overlap_audio_max = None;
                    }
                    self.semantic_observation = None;
                }
                let previous_speaking = matches!(
                    self.state.speech,
                    SpeechState::Playing | SpeechState::Ducking
                );
                self.on_user_speech_started(previous_speaking).await?;
            }

            VoiceEvent::LocalSpeechEnded { .. } => {
                self.semantic_activity = self.semantic_activity.wrapping_add(1);
                if self.semantic_mode {
                    self.semantic_heard_anchor = Some((
                        self.heard_semantic_context(),
                        self.state.assistant_asked_question,
                    ));
                    self.semantic_observation = None;
                } else {
                    self.publish_interaction_snapshot().await;
                }
            }

            VoiceEvent::AsrPartial {
                utterance_id,
                revision,
                hypothesis,
                ..
            } => {
                if !self.semantic_mode
                    && !self.transcript.current_text().is_empty()
                    && (self.transcript.current_utterance() != utterance_id
                        || self.transcript.current_text() != hypothesis.trim())
                {
                    self.rollback_legacy_candidate(self.transcript.current_utterance());
                }
                if self.semantic_mode {
                    self.mark_semantic_overlap(AsrSource::Audio, utterance_id);
                    if !self.transcript.current_text().is_empty()
                        && (self.transcript.current_utterance() != utterance_id
                            || self.transcript.current_text() != hypothesis.trim())
                    {
                        self.rollback_semantic_candidate();
                    }
                    if self.semantic_overlap_pending
                        && self.state.floor != crate::interaction::FloorState::OverlapProbe
                        && self.transcript.current_text().is_empty()
                        && self.turn_buffer.is_empty()
                    {
                        self.state.last_user_audio_at_us = Some(self.monotonic_us());
                    }
                    self.semantic_origin = Some((AsrSource::Audio, utterance_id));
                    self.semantic_final = None;
                }
                self.on_asr_partial(utterance_id, revision, hypothesis)
                    .await?;
            }

            VoiceEvent::AsrUtteranceFinal {
                meta,
                utterance_id,
                source,
                transcript,
                ..
            } => {
                // Text injection has no mic activity, so the final itself
                // anchors the silence clock for the commit tick.
                self.mark_semantic_overlap(source, utterance_id);
                if source == AsrSource::Injection || self.state.last_user_audio_at_us.is_none() {
                    self.state.last_user_audio_at_us = Some(meta.monotonic_us);
                }
                if source == AsrSource::Injection {
                    if self.semantic_mode {
                        self.semantic_origin = Some((source, utterance_id));
                        self.semantic_heard_anchor = Some((
                            self.heard_semantic_context(),
                            self.state.assistant_asked_question,
                        ));
                        self.semantic_final = Some((source, utterance_id, transcript.clone()));
                    }
                    self.turn_buffer.push_final(transcript);
                    self.state.current_hypothesis = self.transcript.current_text().to_owned();
                    self.state.stable_prefix = self.transcript.stable_prefix().to_owned();
                    self.close_candidates_for(utterance_id, source);
                    if !self.semantic_mode {
                        self.publish_interaction_snapshot().await;
                    }
                    return Ok(());
                }
                let preserved_updates = if self.semantic_mode
                    && self.transcript.current_utterance() == utterance_id
                    && self.transcript.current_text() == transcript.trim()
                    && self
                        .semantic_pending_updates
                        .as_ref()
                        .is_some_and(|(key, _, _)| {
                            self.semantic_snapshot().is_some_and(|(current, _, _)| {
                                key.candidate == current.candidate
                                    && key.origin == Some((AsrSource::Audio, utterance_id))
                                    && key.generation == current.generation
                                    && key.interaction_epoch == current.interaction_epoch
                                    && key.thought_epoch == current.thought_epoch
                                    && key.speech_epoch == current.speech_epoch
                                    && key.frame_revision == current.frame_revision
                                    && key.heard_assistant == current.heard_assistant
                                    && key.asked_question == current.asked_question
                            })
                        }) {
                    self.semantic_pending_updates
                        .take()
                        .map(|(_, updates, _)| updates)
                } else {
                    None
                };
                if self.semantic_mode {
                    if !self.transcript.current_text().is_empty()
                        && (self.transcript.current_utterance() != utterance_id
                            || self.transcript.current_text() != transcript.trim())
                    {
                        self.rollback_semantic_candidate();
                    }
                    if self.semantic_overlap_pending
                        && self.state.floor != crate::interaction::FloorState::OverlapProbe
                        && self.transcript.current_text().is_empty()
                        && self.turn_buffer.is_empty()
                    {
                        self.state.last_user_audio_at_us = Some(meta.monotonic_us);
                    }
                    self.semantic_origin = Some((source, utterance_id));
                }
                if !self.semantic_mode
                    && !self.transcript.current_text().is_empty()
                    && (self.transcript.current_utterance() != utterance_id
                        || self.transcript.current_text() != transcript.trim())
                {
                    self.rollback_legacy_candidate(self.transcript.current_utterance());
                }
                let legacy_host_generation = if self.semantic_mode {
                    None
                } else {
                    self.slot_extraction_pending
                        .as_ref()
                        .filter(|request| request.provenance.source_utterance_id == utterance_id)
                        .map(|request| request.host_generation)
                        .or_else(|| {
                            self.slot_extraction_input
                                .as_ref()
                                .filter(|(_, provenance, _)| {
                                    provenance.source_utterance_id == utterance_id
                                })
                                .map(|(_, _, generation)| *generation)
                        })
                        .or_else(|| {
                            self.slot_last_request
                                .as_ref()
                                .filter(|(generation, id, _, _)| {
                                    *generation == self.candidate_generation && *id == utterance_id
                                })
                                .map(|(_, _, _, host_generation)| *host_generation)
                        })
                        .or_else(|| {
                            self.legacy_final.as_ref().map(|_| {
                                self.slot_extraction_input
                                    .as_ref()
                                    .map(|(_, _, generation)| *generation)
                                    .unwrap_or(self.slot_host_generation)
                            })
                        })
                };
                self.transcript.on_final(utterance_id, &transcript);
                let sealed = self.transcript.seal_current();
                if self.semantic_mode {
                    self.semantic_final = Some((source, utterance_id, sealed.clone()));
                }
                self.turn_buffer.push_final(sealed.clone());
                self.state.current_hypothesis.clear();
                self.state.stable_prefix.clear();
                if let Some((previous_id, _)) = self.legacy_final.take() {
                    self.cancel_slot_extraction();
                    self.rollback_unverified_slots(previous_id);
                    self.close_candidates_for(previous_id, AsrSource::Audio);
                }
                if let Some(host_generation) =
                    legacy_host_generation.filter(|_| !sealed.trim().is_empty())
                {
                    self.cancel_slot_extraction();
                    self.slot_extraction_pending = None;
                    let final_text = self.turn_buffer.render_candidate("");
                    self.legacy_final = Some((utterance_id, final_text.clone()));
                    self.start_slot_extraction(SlotExtractionRequest {
                        text: final_text,
                        revision: self.state.epochs.transcript,
                        provenance: SlotProvenance {
                            candidate_generation: self.candidate_generation,
                            source_utterance_id: utterance_id,
                            source,
                        },
                        host_generation,
                    });
                } else {
                    self.close_candidates_for(utterance_id, source);
                }
                if let Some(updates) = preserved_updates
                    && let Some((key, _, _)) = self.semantic_snapshot()
                {
                    self.semantic_pending_updates = Some((key, updates, None));
                }
                if !self.semantic_mode {
                    self.publish_interaction_snapshot().await;
                }
            }

            VoiceEvent::AsrStreamReset { .. } => {
                self.on_asr_stream_reset().await?;
            }

            VoiceEvent::SemanticFrameUpdated {
                updates,
                provenance,
                ..
            } => {
                self.apply_slot_updates(updates, provenance).await?;
            }

            VoiceEvent::InteractionDecision { envelope, .. } => {
                self.apply_floor_decision(envelope).await?;
            }

            VoiceEvent::UserTurnCommitted {
                turn_id,
                transcript,
                transcript_revision,
                ..
            } => {
                self.start_main_task(turn_id, transcript, transcript_revision)
                    .await?;
            }

            VoiceEvent::ToolStarted {
                task_id,
                thought_epoch,
                tool,
                ..
            } => {
                self.on_tool_started(task_id, thought_epoch, tool).await?;
            }

            VoiceEvent::ToolCompleted {
                task_id,
                thought_epoch,
                tool,
                success,
                output,
                ..
            } => {
                self.on_tool_completed(task_id, thought_epoch, tool, success, output)
                    .await?;
            }

            VoiceEvent::ToolExecuted {
                task_id,
                thought_epoch,
                tool,
                executed,
                ..
            } => {
                self.on_tool_executed(task_id, thought_epoch, tool, executed)
                    .await?;
            }

            VoiceEvent::InvocationProof {
                task_id,
                thought_epoch,
                proof,
                ..
            } => {
                if self.is_task_result_current(task_id, thought_epoch)
                    && self
                        .active_tasks
                        .get(&task_id)
                        .is_some_and(|t| t.kind == ActiveTaskKind::MainTurn)
                {
                    let records = self.invocation_proofs.entry(task_id).or_default();
                    if !proof.bounded()
                        || records.len() >= 128
                        || records.iter().any(|p| p.record_id == proof.record_id)
                    {
                        self.proof_conflicts.insert(task_id);
                    } else {
                        records.push(proof);
                    }
                }
            }

            VoiceEvent::AgentFinal {
                task_id,
                thought_epoch,
                text,
                ..
            } => {
                self.on_agent_final(task_id, thought_epoch, text).await?;
            }

            VoiceEvent::AgentFailed {
                task_id,
                thought_epoch,
                message,
                ..
            } => {
                self.on_agent_failed(task_id, thought_epoch, message)
                    .await?;
            }

            VoiceEvent::TtsAudioChunk {
                speech_id,
                speech_epoch,
                sequence,
                sample_rate,
                pcm,
                ..
            } => {
                self.on_tts_audio(speech_id, speech_epoch, sequence, sample_rate, pcm)
                    .await?;
            }

            VoiceEvent::TtsAudioDone {
                speech_id,
                speech_epoch,
                ..
            } => {
                self.on_tts_audio_done(speech_id, speech_epoch).await?;
            }

            VoiceEvent::TtsFailed {
                speech_id, message, ..
            } => {
                self.on_tts_failed(speech_id, message).await?;
            }

            VoiceEvent::PlaybackProgress {
                speech_id,
                played_samples,
                ..
            } => {
                if let Some(record) = self.speech_records.get_mut(&speech_id) {
                    record.confirmed_samples = record.confirmed_samples.max(played_samples);
                }
                if self.state.active_speech_id == Some(speech_id) {
                    self.last_playback_signal_us = Some(self.monotonic_us());
                }
                self.audible_ledger.mark_progress(speech_id, played_samples);
            }

            VoiceEvent::PlaybackCompleted { speech_id, .. } => {
                self.last_playback_signal_us = None;
                let question = self.speech_questions.remove(&speech_id);
                self.settle_speech_record(speech_id);
                self.audible_ledger.mark_completed(speech_id);
                if let Some(question) = question {
                    self.state.assistant_asked_question = true;
                    self.state.expected_answer_type = Some(question);
                }
                self.start_next_speech_if_possible().await?;
            }

            VoiceEvent::PlaybackInterrupted {
                speech_id,
                played_samples,
                reason,
                ..
            } => {
                if self.state.active_speech_id == Some(speech_id) {
                    self.last_playback_signal_us = None;
                }
                if let Some(record) = self.speech_records.get_mut(&speech_id) {
                    record.confirmed_samples = record.confirmed_samples.max(played_samples);
                }
                self.settle_speech_record(speech_id);
                self.audible_ledger
                    .mark_interrupted(speech_id, played_samples, reason);
                self.metrics.inc("voice_speech_interrupted_total");
            }

            VoiceEvent::DeepWorkRequested {
                objective,
                dependencies,
                ..
            } => {
                self.request_deep_work(objective, dependencies).await?;
            }

            VoiceEvent::DeepWorkResult { result, .. } => {
                self.finish_deep_work(
                    DeepCompletion {
                        task_id: result.task_id,
                        thought_epoch: result.thought_epoch,
                        result: Ok(result),
                    },
                    false,
                )
                .await;
            }

            VoiceEvent::ProviderError {
                component,
                recoverable,
                message,
                ..
            } => {
                tracing::warn!(
                    component = %component,
                    recoverable,
                    "provider error: {message}"
                );
                self.metrics.inc("voice_provider_error_total");
            }

            _ => {}
        }

        self.resolve_settled_turns();
        self.retire_question_metadata();
        self.maybe_release_deferred().await?;
        Ok(())
    }

    /// Admission for speech-lifecycle events. `true` means the event may run
    /// its projection and handler; `false` drops it after the trace. Events
    /// that are not speech-lifecycle events are always admitted.
    fn admit_speech_event(&self, event: &VoiceEvent) -> bool {
        let record = |speech_id: &SpeechId| self.speech_records.get(speech_id);

        match event {
            VoiceEvent::TtsAudioChunk {
                speech_id,
                speech_epoch,
                ..
            } => record(speech_id).is_some_and(|record| {
                record.synthesis == SynthesisOutcome::Running
                    && record.playback != PlaybackLifecycle::Settled
                    && *speech_epoch == self.state.epochs.speech
            }),

            VoiceEvent::TtsAudioDone {
                speech_id,
                speech_epoch,
                ..
            } => record(speech_id).is_some_and(|record| {
                record.synthesis == SynthesisOutcome::Running
                    && *speech_epoch == self.state.epochs.speech
            }),

            VoiceEvent::TtsFailed {
                speech_id,
                speech_epoch,
                ..
            } => record(speech_id).is_some_and(|record| {
                record.synthesis == SynthesisOutcome::Running
                    && *speech_epoch == self.state.epochs.speech
            }),

            VoiceEvent::PlaybackProgress { speech_id, .. } => {
                record(speech_id).is_some_and(|record| {
                    matches!(
                        record.playback,
                        PlaybackLifecycle::Playing | PlaybackLifecycle::Stopping
                    )
                })
            }

            VoiceEvent::PlaybackCompleted { speech_id, .. } => {
                record(speech_id).is_some_and(|record| {
                    record.synthesis == SynthesisOutcome::Succeeded
                        && record.playback == PlaybackLifecycle::Playing
                })
            }

            // An interrupted acknowledgement may legitimately arrive after an
            // epoch bump, so identity - not the epoch - decides here.
            VoiceEvent::PlaybackInterrupted { speech_id, .. } => record(speech_id)
                .is_some_and(|record| record.playback != PlaybackLifecycle::Settled),

            _ => true,
        }
    }

    fn settle_speech_record(&mut self, speech_id: SpeechId) {
        self.speech_questions.remove(&speech_id);
        if let Some(mut record) = self.speech_records.remove(&speech_id) {
            record.playback = PlaybackLifecycle::Settled;
        }
        self.act_owner.remove(&speech_id);
    }

    fn should_apply_projection(&self, event: &VoiceEvent) -> bool {
        match event {
            VoiceEvent::AgentStarted {
                task_id,
                thought_epoch,
                ..
            }
            | VoiceEvent::AgentChunk {
                task_id,
                thought_epoch,
                ..
            }
            | VoiceEvent::AgentFinal {
                task_id,
                thought_epoch,
                ..
            }
            | VoiceEvent::ToolStarted {
                task_id,
                thought_epoch,
                ..
            }
            | VoiceEvent::ToolCompleted {
                task_id,
                thought_epoch,
                ..
            }
            | VoiceEvent::ToolExecuted {
                task_id,
                thought_epoch,
                ..
            } => self.is_task_result_current(*task_id, *thought_epoch),
            VoiceEvent::AgentFailed {
                task_id,
                thought_epoch,
                ..
            } => {
                self.is_task_result_current(*task_id, *thought_epoch)
                    && self
                        .active_tasks
                        .get(task_id)
                        .is_some_and(|task| task.kind == ActiveTaskKind::MainTurn)
            }
            _ => true,
        }
    }

    // Barge-in handling.

    async fn on_user_speech_started(&mut self, agent_was_audible: bool) -> Result<()> {
        if agent_was_audible && self.config.turn_control.barge_in.duck_immediately {
            self.playback
                .command(PlaybackCommand::Duck {
                    gain: self.config.turn_control.barge_in.duck_gain,
                    fade_ms: self.config.turn_control.barge_in.duck_fade_ms,
                })
                .await;
            self.state.speech = SpeechState::Ducking;
        }

        self.metrics.inc("voice_user_speech_started_total");
        Ok(())
    }

    /// True when the in-flight act refuses mid-utterance interruption.
    ///
    /// Clause-granular acts already give `AfterClause` its meaning: the next
    /// clause simply never starts. `Never` is the only class that has to be
    /// honoured inside `abort_current_speech`.
    fn active_speech_is_uninterruptible(&self) -> bool {
        self.state.active_speech_id.is_some_and(|id| {
            self.speech_records
                .get(&id)
                .is_some_and(|record| record.never)
        })
    }

    /// A `Never` act currently holds the floor: an ordinary new turn defers
    /// behind it. Only a hard stop or a safety failure may end the act.
    fn current_act_is_never_protected(&self) -> bool {
        self.state.active_speech_id.is_some_and(|id| {
            self.speech_records.get(&id).is_some_and(|record| {
                record.never
                    && matches!(
                        record.synthesis,
                        SynthesisOutcome::Running | SynthesisOutcome::Succeeded
                    )
                    && matches!(
                        record.playback,
                        PlaybackLifecycle::NotStarted | PlaybackLifecycle::Playing
                    )
            })
        })
    }

    async fn abort_current_speech(&mut self, reason: InterruptionReason) -> Result<()> {
        // A hard stop is the user saying "stop" in as many words. It outranks
        // any act's own interruptibility: an utterance that cannot be stopped
        // by an explicit stop request is a trap, not a feature.
        if reason != InterruptionReason::HardStop && self.active_speech_is_uninterruptible() {
            // Drop everything queued behind it, but let the current act finish.
            self.pending_speech.clear();
            self.metrics.inc("voice_abort_deferred_total");
            return Ok(());
        }

        let new_epoch = self.state.epochs.invalidate_speech();
        // A new floor reality invalidates any interaction decision still in
        // flight against the old one.
        self.state.epochs.bump_interaction();

        let now_us = self.monotonic_us();
        let stop_ack_timeout_us = self.config.speech.playback_stop_ack_timeout_ms * 1_000;

        if let Some(speech_id) = self.state.active_speech_id.take() {
            self.tts_registry.cancel(&speech_id);
            // The cancelled synthesis emits no AudioDone, so the record's only
            // remaining duty is settling the ledger when the interrupted
            // acknowledgement (or the stop deadline) lands.
            match self.speech_records.get_mut(&speech_id) {
                Some(record) if record.playback != PlaybackLifecycle::NotStarted => {
                    record.synthesis = SynthesisOutcome::Cancelled;
                    record.never = false;
                    record.playback = PlaybackLifecycle::Stopping;
                    record.stop_deadline_us = Some(now_us.saturating_add(stop_ack_timeout_us));
                }
                Some(_) => {
                    // Nothing reached the browser, so nothing will ever
                    // acknowledge it.
                    self.settle_speech_record(speech_id);
                }
                None => {}
            }
            // `active_speech_id` is cleared so turn settlement does not wait
            // on an act that will never report again. If it had started
            // playing, the ledger still holds it in flight until the client's
            // interrupted ACK arrives with the real played length.
        }

        self.playback
            .command(PlaybackCommand::Abort {
                new_speech_epoch: new_epoch,
                reason: reason.clone(),
            })
            .await;

        // Hard stop acknowledgments survive the abort; everything else is
        // dropped because it was authored for the cancelled speech epoch.
        // Survivors are re-stamped, otherwise the epoch filter in
        // `start_next_speech_if_possible` would silently discard them anyway.
        self.pending_speech
            .retain(|act| act.priority >= crate::speech::act::PRIORITY_HARD_STOP_ACK);
        self.retag_pending_speech(new_epoch);

        if reason == InterruptionReason::HardStop
            && !self.config.speech.hard_stop_ack_text.trim().is_empty()
        {
            let ack = SpeechAct::phatic(
                self.config.speech.hard_stop_ack_text.clone(),
                new_epoch,
                crate::speech::act::PRIORITY_HARD_STOP_ACK,
            );
            self.enqueue_act_unchecked(ack);
        }

        self.state.speech = SpeechState::Aborted;
        self.state.floor = crate::interaction::FloorState::UserHolding;
        self.metrics.inc("voice_speech_aborted_total");
        Ok(())
    }

    async fn resume_after_backchannel(&mut self) -> Result<()> {
        self.state.epochs.bump_interaction();

        self.playback
            .command(PlaybackCommand::Resume {
                gain: 1.0,
                fade_ms: self.config.turn_control.barge_in.duck_fade_ms,
            })
            .await;

        if self.state.speech == SpeechState::Ducking {
            self.state.speech = SpeechState::Playing;
        }

        if !self.state.local_vad_active {
            self.state.floor = crate::interaction::FloorState::AgentHolding;
        }

        self.metrics.inc("voice_resume_after_backchannel_total");
        Ok(())
    }

    async fn apply_floor_decision(&mut self, envelope: InteractionDecisionEnvelope) -> Result<()> {
        if envelope.revision != self.state.epochs.transcript {
            return Ok(());
        }

        if envelope.interaction_epoch != self.state.epochs.interaction {
            return Ok(());
        }

        if self.state.local_vad_active
            && matches!(
                envelope.decision.action,
                InteractionAction::CommitUserTurn | InteractionAction::TakeFloor
            )
        {
            self.metrics.inc("voice_floor_decision_rejected_total");
            return Ok(());
        }

        match envelope.decision.action {
            InteractionAction::KeepListening | InteractionAction::HoldFloor => {
                self.metrics.inc("voice_floor_decision_total");
            }

            InteractionAction::DuckAgent => {
                self.playback
                    .command(PlaybackCommand::Duck {
                        gain: self.config.turn_control.barge_in.duck_gain,
                        fade_ms: self.config.turn_control.barge_in.duck_fade_ms,
                    })
                    .await;
                self.metrics.inc("voice_floor_decision_total");
            }

            InteractionAction::ResumeAgent => {
                // Discard both the partial and work derived from that backchannel.
                if !self.transcript.current_text().is_empty() {
                    let id = self.transcript.current_utterance();
                    self.rollback_semantic_candidate();
                    self.suppressed_audio_watermark = Some(
                        self.suppressed_audio_watermark
                            .map_or(id, |old| old.max(id)),
                    );
                    self.close_candidates_for(id, AsrSource::Audio);
                }
                self.slot_host_generation = self.slot_host_generation.saturating_add(1);
                self.slot_extraction_pending = None;
                self.slot_extraction_order.clear();
                self.state.current_hypothesis.clear();
                self.transcript.clear();
                self.resume_after_backchannel().await?;
            }

            InteractionAction::AbortSpeech => {
                let reason = match envelope.decision.user_state {
                    UserSpeechState::Correction => InterruptionReason::SemanticCorrection,
                    UserSpeechState::Answer => InterruptionReason::AnswerToQuestion,
                    _ => InterruptionReason::SemanticInterruption,
                };
                self.abort_current_speech(reason).await?;
            }

            InteractionAction::EmitBackchannel => {
                self.enqueue_backchannel().await?;
            }

            InteractionAction::CommitUserTurn | InteractionAction::TakeFloor => {
                self.commit_user_turn(&envelope.decision.reason_code)
                    .await?;
            }
        }

        Ok(())
    }

    // ASR partial and semantic processing.

    async fn on_asr_partial(
        &mut self,
        utterance_id: u64,
        revision: Revision,
        hypothesis: String,
    ) -> Result<()> {
        let now_us = self.monotonic_us();

        let agent_audible = matches!(
            self.state.speech,
            SpeechState::Playing | SpeechState::Ducking
        );

        let recent_assistant = recent_assistant_text(self.audible_ledger.history());

        if agent_audible
            && crate::echo::likely_self_echo(
                &hypothesis,
                &recent_assistant,
                true,
                &self.config.echo,
            )
        {
            // Self echo: never commit, never abort, keep ducking policy
            // available for the tick to resume.
            self.state.current_hypothesis.clear();
            self.metrics.inc("voice_self_echo_suppressed_total");
            return Ok(());
        }

        let stable_event = self
            .transcript
            .on_partial(utterance_id, revision, &hypothesis, now_us);

        if let Some(stable_prefix) = stable_event {
            let event = VoiceEvent::StableTranscriptChanged {
                meta: self.new_meta(),
                stable_prefix: stable_prefix.clone(),
            };
            self.forward_trace(&event).await;
            self.state.apply(&event);

            if !self.semantic_mode {
                self.spawn_slot_extraction(stable_prefix.clone(), revision);
                self.maybe_spawn_speculative(&stable_prefix);
            }
        }

        if self.semantic_mode {
            return Ok(());
        }

        let cues = self.text_analyzer.user(&hypothesis).cues;

        for cue in cues.clone() {
            let event = VoiceEvent::SemanticCue {
                meta: self.new_meta(),
                revision,
                cue: cue.clone(),
            };
            self.forward_trace(&event).await;

            match cue {
                SemanticCue::HardStop { .. } => {
                    if self.state.active_speech_id.is_some() {
                        self.abort_current_speech(InterruptionReason::HardStop)
                            .await?;
                    }
                }
                SemanticCue::Correction { .. } => {
                    // Corrections must react to the newest hypothesis, not the
                    // stable prefix which lags several revisions behind.
                    let hypothesis = self.transcript.current_text().to_string();
                    self.spawn_slot_extraction(
                        if hypothesis.trim().is_empty() {
                            self.state.current_hypothesis.clone()
                        } else {
                            hypothesis
                        },
                        revision,
                    );
                }
                SemanticCue::EntityCandidate {
                    slot,
                    value,
                    confidence,
                } => {
                    let update = SlotUpdate {
                        slot,
                        operation: crate::semantics::frame::SlotOperation::Set,
                        old_value: None,
                        new_value: value,
                        confidence,
                        source_revision: revision,
                    };
                    let provenance = SlotProvenance {
                        candidate_generation: self.candidate_generation,
                        source_utterance_id: utterance_id,
                        source: AsrSource::Audio,
                    };
                    self.apply_slot_updates_inner(vec![update], Some(provenance), false)
                        .await?;
                }
                _ => {}
            }
        }

        // A rewritten hypothesis needs its own extraction even if the stable prefix did not grow.
        let revised_request =
            self.slot_last_request
                .as_ref()
                .is_some_and(|(generation, id, text, _)| {
                    *generation == self.candidate_generation
                        && *id == utterance_id
                        && text != hypothesis.trim()
                });
        let different_running_candidate = self.slot_extraction_running.is_some()
            && !self
                .slot_extraction_input
                .as_ref()
                .is_some_and(|(text, provenance, _)| {
                    text == hypothesis.trim()
                        && provenance.candidate_generation == self.candidate_generation
                        && provenance.source_utterance_id == utterance_id
                });
        if (revised_request || different_running_candidate)
            && self.slot_extraction_pending.as_ref().is_none_or(|pending| {
                pending.text != hypothesis.trim()
                    || pending.provenance.candidate_generation != self.candidate_generation
                    || pending.provenance.source_utterance_id != utterance_id
            })
        {
            self.spawn_slot_extraction(hypothesis.trim().to_string(), revision);
        }

        // Event-driven interaction brain run under the partial controller
        // cadence. Urgent cues bypass the interval.
        if self.should_run_partial_controller(&cues, now_us) {
            self.spawn_interaction_brain().await;
        }

        Ok(())
    }

    fn should_run_partial_controller(&mut self, cues: &[SemanticCue], now_us: u64) -> bool {
        let current = TranscriptSnapshot {
            stable_prefix: self.transcript.stable_prefix().to_string(),
            hypothesis: self.transcript.current_text().to_string(),
        };

        let previous = self.partial_last_snapshot.take().unwrap_or_default();
        self.partial_last_snapshot = Some(current.clone());

        self.partial_policy.should_run(
            &previous,
            &current,
            cues,
            now_us,
            self.last_interaction_run_us,
        )
    }

    async fn spawn_interaction_brain(&mut self) {
        let now_us = self.monotonic_us();

        let interval = self.config.agents.interaction.minimum_interval_ms;
        if let Some(last) = self.last_interaction_run_us
            && now_us.saturating_sub(last) < interval.saturating_mul(1_000)
        {
            return;
        }

        self.last_interaction_run_us = Some(now_us);

        let snapshot = self.interaction_snapshot();
        let brain = Arc::clone(&self.interaction_brain);
        let tx = self.legacy_interaction_tx.clone();
        let activity = self.semantic_activity;

        let shutdown = self.shutdown.clone();
        let timeout = Duration::from_millis(self.config.agents.interaction.timeout_ms);
        self.workers.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = tokio::time::sleep(timeout) => {},
                _ = async {
            if let Some(envelope) = brain.decide(snapshot).await {
                let _ = tx.send((envelope, activity)).await;
            }
                } => {},
            }
        });
    }

    async fn apply_legacy_interaction(
        &mut self,
        envelope: InteractionDecisionEnvelope,
        activity: u64,
    ) -> Result<()> {
        if activity != self.semantic_activity {
            self.metrics.inc("voice_stale_interaction_result_total");
            return Ok(());
        }
        self.handle_event(VoiceEvent::InteractionDecision {
            meta: self.new_meta(),
            envelope,
        })
        .await
    }

    async fn publish_interaction_snapshot(&mut self) {
        if !matches!(
            self.state.floor,
            crate::interaction::FloorState::OverlapProbe
        ) {
            self.spawn_interaction_brain().await;
        }
    }

    fn interaction_snapshot(&self) -> InteractionSnapshot {
        let candidate = self
            .turn_buffer
            .render_candidate(self.transcript.current_text());
        InteractionSnapshot::from_view(&self.state, &candidate, self.monotonic_us())
    }

    fn heard_semantic_context(&self) -> String {
        let mut heard: Vec<String> = self
            .audible_ledger
            .history()
            .iter()
            .rev()
            .take(3)
            .map(|clause| clause.heard_text())
            .filter(|text| !text.is_empty())
            .collect();
        heard.reverse();
        if let Some(id) = self.audible_ledger.in_flight_id()
            && let Some(record) = self.speech_records.get(&id)
            && let Some(heard_state) = self.audible_ledger.heard_so_far(id)
        {
            let text = match heard_state {
                Heard::Full => record.text.clone(),
                Heard::Partial { approx_chars, .. } => {
                    record.text.chars().take(approx_chars).collect()
                }
                Heard::None => String::new(),
            };
            if !text.is_empty() {
                heard.push(text);
            }
        }
        heard
            .join(" ")
            .chars()
            .rev()
            .take(1_024)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    fn semantic_snapshot(&self) -> Option<(SemanticKey, InteractionSnapshot, Option<u64>)> {
        if !self.semantic_mode {
            return None;
        }
        let mut snapshot = self.interaction_snapshot();
        if snapshot.turn_candidate.trim().is_empty() || snapshot.turn_candidate.len() > 8_192 {
            return None;
        }
        if self.semantic_overlap_pending {
            snapshot.floor_state = crate::interaction::FloorState::OverlapProbe;
        }
        if let Some((heard, asked)) = &self.semantic_heard_anchor {
            snapshot.heard_assistant = heard.clone();
            snapshot.agent_asked_question = *asked;
        } else {
            snapshot.heard_assistant = self.heard_semantic_context();
        }
        let key = SemanticKey {
            candidate: snapshot.turn_candidate.clone(),
            heard_assistant: snapshot.heard_assistant.clone(),
            asked_question: snapshot.agent_asked_question,
            vad_active: self.state.local_vad_active,
            origin: self.semantic_origin,
            generation: self.candidate_generation,
            activity: self.semantic_activity,
            interaction_epoch: self.state.epochs.interaction,
            thought_epoch: self.state.epochs.thought,
            speech_epoch: self.state.epochs.speech,
            frame_revision: self.state.semantic_frame.revision,
            floor: snapshot.floor_state,
            speech: self.state.speech,
        };
        let utterance = (!self.transcript.current_text().is_empty())
            .then(|| self.transcript.current_utterance());
        Some((key, snapshot, utterance))
    }

    fn maybe_spawn_semantic(&mut self) {
        if self.semantic_in_flight {
            return;
        }
        let Some((key, snapshot, utterance_id)) = self.semantic_snapshot() else {
            return;
        };
        if self.semantic_last_key.as_ref() == Some(&key) {
            return;
        }
        let now = self.monotonic_us();
        if self.last_interaction_run_us.is_some_and(|last| {
            now.saturating_sub(last)
                < self
                    .config
                    .agents
                    .interaction
                    .minimum_interval_ms
                    .saturating_mul(1_000)
        }) {
            return;
        }
        self.last_interaction_run_us = Some(now);
        self.semantic_last_key = Some(key.clone());
        self.semantic_in_flight = true;
        let brain = Arc::clone(&self.interaction_brain);
        let extractor = self
            .semantic_slot_override
            .then(|| Arc::clone(&self.slot_extractor));
        let tx = self.semantic_tx.clone();
        let stop_tx = self.semantic_stop_tx.clone();
        let stop_key = key.clone();
        let shutdown = self.shutdown.clone();
        let timeout = Duration::from_millis(self.config.agents.interaction.timeout_ms);
        let metrics = Arc::clone(&self.metrics);
        self.workers.spawn(async move {
            let revision = snapshot.transcript_revision;
            let extraction_snapshot = snapshot.clone();
            let work = async {
                let semantic = async {
                    let assessment =
                        match tokio::time::timeout(timeout, brain.assess(snapshot)).await {
                            Ok(assessment) => assessment,
                            Err(_) => {
                                // Warn on the first timeout only; the counter keeps the rest visible.
                                if metrics.counter("voice_semantic_timeout_total") == 0 {
                                    tracing::warn!(
                                        timeout_ms = timeout.as_millis() as u64,
                                        "semantic interpretation timed out; using neutral timing"
                                    );
                                }
                                metrics.inc("voice_semantic_timeout_total");
                                None
                            }
                        };
                    if assessment
                        .as_ref()
                        .is_some_and(|value| value.valid() && value.stop)
                    {
                        let _ = stop_tx.send(stop_key).await;
                    }
                    assessment
                };
                let host = async {
                    match extractor {
                        Some(extractor) => tokio::time::timeout(
                            timeout,
                            extractor.extract(
                                &extraction_snapshot.semantic_frame,
                                &extraction_snapshot.turn_candidate,
                                revision,
                            ),
                        )
                        .await
                        .ok(),
                        None => None,
                    }
                };
                let (assessment, host_updates) = tokio::join!(semantic, host);
                let _ = tx
                    .send(SemanticResult {
                        key,
                        revision,
                        utterance_id,
                        assessment,
                        host_updates,
                    })
                    .await;
            };
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = work => {},
            }
        });
    }

    async fn apply_semantic_stop(&mut self, key: &SemanticKey) -> Result<()> {
        if !self.semantic_snapshot().is_some_and(|(current, _, _)| {
            current == *key
                || (key.vad_active
                    && !current.vad_active
                    && current.activity == key.activity.wrapping_add(1)
                    && current.candidate == key.candidate
                    && current.origin == key.origin
                    && current.generation == key.generation
                    && current.interaction_epoch == key.interaction_epoch
                    && current.thought_epoch == key.thought_epoch
                    && current.speech_epoch == key.speech_epoch
                    && current.frame_revision == key.frame_revision)
        }) {
            return Ok(());
        }
        let origin = (key.generation, key.origin);
        if self.semantic_stop_seen == Some(origin) {
            return Ok(());
        }
        let running: Vec<TaskId> = self
            .active_tasks
            .iter()
            .filter_map(|(id, task)| (task.kind == ActiveTaskKind::MainTurn).then_some(*id))
            .collect();
        if self.state.active_speech_id.is_some()
            || !running.is_empty()
            || self
                .pending_speech
                .iter()
                .any(|act| act.priority < crate::speech::act::PRIORITY_HARD_STOP_ACK)
        {
            self.semantic_stop_seen = Some(origin);
            for id in running {
                if let Some(mut task) = self.active_tasks.remove(&id) {
                    task.cancel();
                    self.resolve_turn(id, TurnResolution::Discard);
                }
                if self.state.active_task_id == Some(id) {
                    self.state.active_task_id = None;
                }
            }
            self.abort_current_speech(InterruptionReason::HardStop)
                .await?;
        }
        Ok(())
    }

    async fn apply_semantic_result(&mut self, result: SemanticResult) -> Result<()> {
        self.semantic_in_flight = false;
        let Some((current, _, _)) = self.semantic_snapshot() else {
            return Ok(());
        };
        if current != result.key {
            self.metrics.inc("voice_stale_semantic_result_total");
            return Ok(());
        }
        let assessment = result.assessment.filter(SemanticAssessment::valid);
        if assessment.is_none() && result.host_updates.is_none() {
            return Ok(());
        }
        if assessment.as_ref().is_some_and(|value| value.stop) {
            self.apply_semantic_stop(&result.key).await?;
            return Ok(());
        }
        let overlapping = result.key.floor == crate::interaction::FloorState::OverlapProbe;
        let proposed = match result.host_updates {
            Some(updates) => updates,
            None if self.semantic_slot_override => Vec::new(),
            None => assessment
                .iter()
                .flat_map(|assessment| assessment.updates.iter())
                .map(|update| SlotUpdate {
                    slot: update.slot.clone(),
                    operation: update.operation,
                    old_value: update.old_value.clone(),
                    new_value: update.new_value.clone(),
                    confidence: update.confidence,
                    source_revision: result.revision,
                })
                .collect(),
        };
        let mut names = HashSet::new();
        if proposed.len() > 32
            || proposed.iter().any(|update| {
                !names.insert(&update.slot)
                    || update.slot.trim().is_empty()
                    || update.slot.len() > 128
                    || !(0.0..=1.0).contains(&update.confidence)
                    || update.new_value.to_string().len() > 16_384
                    || (update.operation != crate::semantics::SlotOperation::Clear
                        && update.new_value.is_null())
            })
        {
            return Ok(());
        }
        let apply_overlap = !overlapping
            || assessment.as_ref().is_some_and(|value| {
                matches!(
                    value.state,
                    UserSpeechState::Interruption
                        | UserSpeechState::Correction
                        | UserSpeechState::Answer
                )
            });
        let mut updates = Vec::new();
        for mut update in proposed.into_iter().filter(|_| apply_overlap) {
            update.source_revision = result.revision;
            let previous = self.state.semantic_frame.slot_value(&update.slot).cloned();
            if update.operation == crate::semantics::SlotOperation::Clear {
                let Some(old) = previous else { continue };
                update.old_value = Some(old);
            } else if let Some(old) = previous {
                if old == update.new_value {
                    continue;
                }
                update.operation = crate::semantics::SlotOperation::Replace;
                update.old_value = Some(old);
            } else {
                update.operation = crate::semantics::SlotOperation::Set;
                update.old_value = None;
            }
            updates.push(update);
        }
        let provenance = result
            .utterance_id
            .map(|source_utterance_id| SlotProvenance {
                candidate_generation: result.key.generation,
                source_utterance_id,
                source: AsrSource::Audio,
            });
        self.semantic_pending_updates = None;
        let defer_updates = !updates.is_empty()
            && (overlapping
                || self
                    .active_tasks
                    .values()
                    .any(|task| task.kind == ActiveTaskKind::MainTurn));
        let deferred = if defer_updates {
            Some((updates, provenance))
        } else {
            if !updates.is_empty() {
                self.apply_slot_updates(updates, provenance).await?;
            }
            None
        };
        if let Some(assessment) = &assessment {
            if !overlapping && self.state.semantic_frame.negated != assessment.negated {
                self.state.semantic_frame.negated = assessment.negated;
                self.state.semantic_frame.revision = self.state.semantic_frame.revision.next();
            }
            if let Some(intent) = &assessment.intent
                && !overlapping
            {
                if result.utterance_id.is_some() && self.semantic_intent_baseline.is_none() {
                    self.semantic_intent_baseline = Some(self.state.semantic_frame.intent.clone());
                } else if result.utterance_id.is_none() {
                    self.semantic_intent_baseline = None;
                }
                self.state.semantic_frame.intent = Some(crate::semantics::SemanticValue {
                    value: intent.clone(),
                    confidence: assessment.confidence,
                    source_revision: result.revision,
                    status: crate::semantics::SemanticValueStatus::Tentative,
                });
            }
        }
        if let Some(assessment) = &assessment
            && !overlapping
            && let Some(intent) = &assessment.intent
        {
            let stable = self.transcript.stable_prefix().to_string();
            if !stable.is_empty() && self.transcript.current_text().starts_with(&stable) {
                self.maybe_spawn_speculative_intent(&stable, intent, assessment.confidence);
            }
        }
        if let Some((key, _, _)) = self.semantic_snapshot() {
            self.semantic_last_key = Some(key.clone());
            self.semantic_pending_updates =
                deferred.map(|(updates, provenance)| (key.clone(), updates, provenance));
            self.semantic_observation = assessment.map(|assessment| (key, assessment));
        }
        Ok(())
    }

    fn spawn_slot_extraction(&mut self, text: String, revision: Revision) {
        if text.trim().is_empty() {
            return;
        }
        let request = SlotExtractionRequest {
            text,
            revision,
            provenance: SlotProvenance {
                candidate_generation: self.candidate_generation,
                source_utterance_id: self.transcript.current_utterance(),
                source: AsrSource::Audio,
            },
            host_generation: self.slot_host_generation,
        };
        self.slot_last_request = Some((
            request.provenance.candidate_generation,
            request.provenance.source_utterance_id,
            request.text.clone(),
            request.host_generation,
        ));
        if self.slot_extraction_running.is_some() {
            self.slot_extraction_pending = Some(request);
        } else {
            self.start_slot_extraction(request);
        }
    }

    fn cancel_slot_extraction(&mut self) {
        if let Some(cancel) = self.slot_extraction_cancel.take() {
            cancel.cancel();
        }
        self.slot_extraction_running = None;
        self.slot_extraction_input = None;
    }

    fn start_slot_extraction(&mut self, request: SlotExtractionRequest) {
        if request.provenance.candidate_generation != self.candidate_generation
            || self
                .candidate_closed_watermark
                .is_some_and(|id| request.provenance.source_utterance_id <= id)
        {
            return;
        }
        self.slot_extraction_sequence = self.slot_extraction_sequence.saturating_add(1);
        let sequence = self.slot_extraction_sequence;
        let deadline = self.monotonic_us().saturating_add(
            self.config
                .agents
                .interaction
                .timeout_ms
                .saturating_mul(1_000),
        );
        self.slot_extraction_running = Some((sequence, deadline));
        self.slot_extraction_input = Some((
            request.text.clone(),
            request.provenance.clone(),
            request.host_generation,
        ));
        self.slot_last_request = Some((
            request.provenance.candidate_generation,
            request.provenance.source_utterance_id,
            request.text.clone(),
            request.host_generation,
        ));
        let cancellation = CancellationToken::new();
        self.slot_extraction_cancel = Some(cancellation.clone());
        let host_generation = request.host_generation;
        let extractor = Arc::clone(&self.slot_extractor);
        let mut frame = self.state.semantic_frame.clone();
        for (slot, entry) in &self.slot_journal {
            if entry.writer == request.provenance {
                match entry
                    .last_accepted
                    .clone()
                    .unwrap_or_else(|| entry.previous.clone())
                {
                    Some(value) => {
                        frame.slots.insert(slot.clone(), value);
                    }
                    None => {
                        frame.slots.remove(slot);
                    }
                }
            }
        }
        let tx = self.slot_extraction_tx.clone();
        let shutdown = self.shutdown.clone();
        let timeout = Duration::from_millis(self.config.agents.interaction.timeout_ms);
        self.workers.spawn(async move {
            let updates = tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = cancellation.cancelled() => return,
                result = tokio::time::timeout(timeout, extractor.extract(&frame, &request.text, request.revision)) => result.unwrap_or_default(),
            };
            let _ = tx.send(SlotExtractionResult { request, sequence, host_generation, updates }).await;
        });
    }

    async fn finish_slot_extraction(&mut self, result: SlotExtractionResult) -> Result<()> {
        if !self
            .slot_extraction_running
            .is_some_and(|(sequence, _)| sequence == result.sequence)
        {
            self.metrics.inc("voice_stale_slot_update_total");
            return Ok(());
        }
        self.slot_extraction_running = None;
        self.slot_extraction_cancel = None;
        self.slot_extraction_input = None;
        let provenance = result.request.provenance;
        let final_matches = self.legacy_final.as_ref().is_some_and(|(id, text)| {
            *id == provenance.source_utterance_id && text == &result.request.text
        });
        if (final_matches
            || (result.request.text.trim() == self.transcript.current_text().trim()
                && provenance.source_utterance_id == self.transcript.current_utterance()))
            && provenance.candidate_generation == self.candidate_generation
            && !self
                .candidate_closed_watermark
                .is_some_and(|id| provenance.source_utterance_id <= id)
        {
            let mut accepted = Vec::new();
            for mut update in result.updates {
                let order = (provenance.source_utterance_id, result.sequence);
                if self.slot_journal.get(&update.slot).is_some_and(|entry| {
                    entry.writer.source == AsrSource::Audio
                        && entry.writer.source_utterance_id > provenance.source_utterance_id
                }) || self.slot_host_overflow > result.host_generation
                    || self
                        .slot_host_writes
                        .get(&update.slot)
                        .is_some_and(|version| *version > result.host_generation)
                    || self
                        .slot_extraction_order
                        .get(&update.slot)
                        .is_some_and(|previous| *previous >= order)
                    || (!self.slot_extraction_order.contains_key(&update.slot)
                        && self.slot_extraction_order.len() >= 128)
                {
                    self.metrics.inc("voice_stale_slot_update_total");
                    continue;
                }
                self.slot_extraction_order
                    .insert(update.slot.clone(), order);
                update.source_revision = result.request.revision;
                accepted.push(update);
            }
            if !accepted.is_empty() {
                let event = VoiceEvent::SemanticFrameUpdated {
                    meta: self.new_meta(),
                    revision: result.request.revision,
                    updates: accepted.clone(),
                    provenance: Some(provenance.clone()),
                };
                self.forward_trace(&event).await;
                self.apply_slot_updates(accepted, Some(provenance.clone()))
                    .await?;
            }
        } else {
            self.metrics.inc("voice_stale_slot_update_total");
        }
        if final_matches {
            self.legacy_final = None;
            self.rollback_unverified_slots(provenance.source_utterance_id);
            self.close_candidates_for(provenance.source_utterance_id, AsrSource::Audio);
        }
        if let Some(pending) = self.slot_extraction_pending.take() {
            self.start_slot_extraction(pending);
        }
        Ok(())
    }

    /// Applies slot updates with their provenance.
    ///
    /// Candidate-derived work from an abandoned or closed generation is
    /// discarded before it can touch the frame. Candidate writes journal the
    /// authoritative baseline they overwrite, so a stream reset can restore
    /// it; host-issued writes retire the journal entry instead.
    async fn apply_slot_updates(
        &mut self,
        updates: Vec<SlotUpdate>,
        provenance: Option<SlotProvenance>,
    ) -> Result<()> {
        self.apply_slot_updates_inner(updates, provenance, true)
            .await
    }

    async fn apply_slot_updates_inner(
        &mut self,
        updates: Vec<SlotUpdate>,
        provenance: Option<SlotProvenance>,
        authoritative: bool,
    ) -> Result<()> {
        if provenance.is_none() && !updates.is_empty() {
            self.slot_host_generation = self.slot_host_generation.saturating_add(1);
            for update in &updates {
                if !self.slot_host_writes.contains_key(&update.slot)
                    && self.slot_host_writes.len() >= 128
                {
                    self.slot_host_overflow = self.slot_host_generation;
                    self.slot_host_writes.clear();
                }
                self.slot_host_writes
                    .insert(update.slot.clone(), self.slot_host_generation);
            }
        }
        if let Some(provenance) = &provenance
            && provenance.candidate_generation != self.candidate_generation
        {
            self.metrics.inc("voice_stale_slot_update_total");
            return Ok(());
        }

        if let Some(provenance) = &provenance
            && let Some(watermark) = self.candidate_closed_watermark
            && provenance.source == AsrSource::Audio
            && provenance.source_utterance_id <= watermark
        {
            self.metrics.inc("voice_stale_slot_update_total");
            return Ok(());
        }

        let mut changed_slots: Vec<SlotName> = Vec::new();

        for update in updates {
            let next = (update.operation != crate::semantics::frame::SlotOperation::Clear)
                .then(|| update.new_value.clone());
            let previous = self.state.semantic_frame.slots.get(&update.slot).cloned();
            let baseline = match &provenance {
                Some(provenance) => {
                    let journal =
                        self.slot_journal
                            .entry(update.slot.clone())
                            .or_insert_with(|| SlotJournalEntry {
                                previous: previous.clone(),
                                last_accepted: None,
                                writer: provenance.clone(),
                            });
                    journal.writer = provenance.clone();
                    journal
                        .last_accepted
                        .clone()
                        .unwrap_or_else(|| journal.previous.clone())
                        .map(|value| value.value)
                }
                None => self
                    .slot_journal
                    .remove(&update.slot)
                    .map(|journal| {
                        journal
                            .last_accepted
                            .unwrap_or(journal.previous)
                            .map(|value| value.value)
                    })
                    .unwrap_or_else(|| previous.map(|value| value.value)),
            };
            if authoritative && baseline != next {
                changed_slots.push(update.slot.clone());
            }
            let slot = update.slot.clone();
            self.state.semantic_frame.set_slot(update);
            if authoritative && provenance.is_some() {
                let accepted = self.state.semantic_frame.slots.get(&slot).cloned();
                if let Some(journal) = self.slot_journal.get_mut(&slot) {
                    journal.last_accepted = Some(accepted);
                }
            }
        }

        if !changed_slots.is_empty() {
            self.metrics
                .add("voice_correction_total", changed_slots.len() as u64);
            self.invalidate_tasks_for_slots(&changed_slots);
        }

        Ok(())
    }

    /// Closes the candidates of one utterance: its writes become
    /// authoritative, and later resets never roll them back.
    fn close_candidates_for(&mut self, source_utterance_id: u64, source: AsrSource) {
        if source == AsrSource::Injection {
            self.slot_journal.retain(|_, entry| {
                entry.writer.source != source
                    || entry.writer.source_utterance_id != source_utterance_id
            });
            return;
        }

        if self.semantic_mode {
            self.semantic_intent_baseline = None;
        }
        let watermark = self
            .candidate_closed_watermark
            .map(|current| current.max(source_utterance_id))
            .unwrap_or(source_utterance_id);
        self.candidate_closed_watermark = Some(watermark);
        self.slot_extraction_order
            .retain(|_, (id, _)| *id > watermark);
        if self
            .slot_extraction_pending
            .as_ref()
            .is_some_and(|request| request.provenance.source_utterance_id <= watermark)
        {
            self.slot_extraction_pending = None;
        }
        self.state.epochs.bump_interaction();
        for task in self.active_tasks.values_mut() {
            if task.kind == ActiveTaskKind::SpeculativeRead
                && task.candidate_generation == self.candidate_generation
                && task
                    .candidate_utterance_id
                    .is_some_and(|id| id <= watermark)
            {
                task.candidate_generation = u64::MAX;
            }
        }
        for result in &mut self.speculative_results {
            if result.candidate_generation == self.candidate_generation
                && result
                    .candidate_utterance_id
                    .is_some_and(|id| id <= watermark)
            {
                result.candidate_generation = u64::MAX;
            }
        }

        // Serial utterance ids: anything at or below the watermark is
        // closed, so only strictly newer candidates stay journaled.
        self.slot_journal.retain(|_, entry| {
            entry.writer.source != AsrSource::Audio || entry.writer.source_utterance_id > watermark
        });
    }

    /// An ASR generation restart voids the abandoned hypotheses and every
    /// kind of work derived from them. Authoritative finals, committed turns,
    /// and host-issued slot values are untouched.
    async fn on_asr_stream_reset(&mut self) -> Result<()> {
        if let Some((id, _)) = self.legacy_final.take() {
            self.cancel_slot_extraction();
            self.rollback_unverified_slots(id);
            self.close_candidates_for(id, AsrSource::Audio);
        }
        let preserved = if self.semantic_mode
            && self.transcript.current_text().is_empty()
            && self.semantic_final.is_some()
        {
            self.semantic_snapshot().and_then(|(key, _, _)| {
                let applicable = self
                    .semantic_observation
                    .as_ref()
                    .is_some_and(|(observed, _)| *observed == key)
                    || self
                        .semantic_pending_updates
                        .as_ref()
                        .is_some_and(|(observed, _, _)| *observed == key);
                applicable.then(|| {
                    (
                        key,
                        self.semantic_origin,
                        self.semantic_heard_anchor.clone(),
                        self.semantic_observation.clone(),
                        self.semantic_pending_updates.clone(),
                        self.state.semantic_frame.intent.clone(),
                        self.state.semantic_frame.negated,
                        self.semantic_overlap_pending,
                        self.semantic_overlap_base,
                        self.semantic_overlap_audio_max,
                        self.semantic_stop_seen,
                    )
                })
            })
        } else {
            None
        };
        self.transcript.clear();
        self.state.current_hypothesis.clear();
        self.state.stable_prefix.clear();
        self.state.epochs.bump_interaction();
        self.rollback_semantic_candidate();
        self.semantic_origin = None;
        self.semantic_stop_seen = None;
        self.semantic_overlap_pending = false;
        self.semantic_overlap_base = None;
        self.semantic_overlap_audio_max = None;
        self.semantic_heard_anchor = None;
        let abandoned = self.candidate_generation;
        self.candidate_generation += 1;
        self.slot_extraction_pending = None;
        self.slot_extraction_order.clear();
        self.slot_host_writes.clear();
        self.slot_host_overflow = 0;
        let cancelled: Vec<TaskId> = self
            .active_tasks
            .iter()
            .filter(|(_, task)| {
                task.kind == ActiveTaskKind::SpeculativeRead
                    && task.candidate_generation == abandoned
            })
            .map(|(id, _)| *id)
            .collect();
        for id in cancelled {
            if let Some(mut task) = self.active_tasks.remove(&id) {
                task.cancel();
            }
        }
        self.speculative_results
            .retain(|result| result.candidate_generation != abandoned);
        if let Some((
            old_key,
            origin,
            anchor,
            observation,
            pending,
            intent,
            negated,
            overlap,
            overlap_base,
            overlap_audio_max,
            stop_seen,
        )) = preserved
        {
            self.semantic_origin = origin;
            self.semantic_heard_anchor = anchor;
            self.semantic_overlap_pending = overlap;
            self.semantic_overlap_base = overlap_base;
            self.semantic_overlap_audio_max = overlap_audio_max;
            self.semantic_stop_seen = stop_seen;
            self.state.semantic_frame.intent = intent;
            self.state.semantic_frame.negated = negated;
            if let Some((key, _, _)) = self.semantic_snapshot()
                && key.candidate == old_key.candidate
                && key.origin == old_key.origin
            {
                self.semantic_observation = observation.map(|(_, value)| (key.clone(), value));
                self.semantic_pending_updates =
                    pending.map(|(_, updates, provenance)| (key.clone(), updates, provenance));
                self.semantic_last_key = Some(key);
            }
        }
        self.metrics.inc("voice_asr_reset_total");

        Ok(())
    }

    fn rollback_semantic_candidate(&mut self) {
        self.semantic_pending_updates = None;
        let utterance_id = self.transcript.current_utterance();
        let stale: Vec<TaskId> = self
            .active_tasks
            .iter()
            .filter_map(|(id, task)| {
                (task.kind == ActiveTaskKind::SpeculativeRead
                    && task.candidate_generation == self.candidate_generation
                    && task.candidate_utterance_id == Some(utterance_id))
                .then_some(*id)
            })
            .collect();
        for id in stale {
            if let Some(mut task) = self.active_tasks.remove(&id) {
                task.cancel();
            }
        }
        self.speculative_results.retain(|result| {
            result.candidate_generation != self.candidate_generation
                || result.candidate_utterance_id != Some(utterance_id)
        });
        self.rollback_abandoned_candidate_slots();
        if self.state.semantic_frame.negated {
            self.state.semantic_frame.negated = false;
            self.state.semantic_frame.revision = self.state.semantic_frame.revision.next();
        }
        if let Some(previous) = self.semantic_intent_baseline.take() {
            self.state.semantic_frame.intent = previous;
            self.state.semantic_frame.revision = self.state.semantic_frame.revision.next();
        }
    }

    fn rollback_legacy_candidate(&mut self, utterance_id: u64) {
        let stale: Vec<TaskId> = self
            .active_tasks
            .iter()
            .filter_map(|(id, task)| {
                (task.kind == ActiveTaskKind::SpeculativeRead
                    && task.candidate_generation == self.candidate_generation
                    && task.candidate_utterance_id == Some(utterance_id))
                .then_some(*id)
            })
            .collect();
        for id in stale {
            if let Some(mut task) = self.active_tasks.remove(&id) {
                task.cancel();
            }
        }
        self.speculative_results.retain(|result| {
            result.candidate_generation != self.candidate_generation
                || result.candidate_utterance_id != Some(utterance_id)
        });
        let slots: Vec<SlotName> = self
            .slot_journal
            .iter()
            .filter_map(|(slot, entry)| {
                (entry.writer.source == AsrSource::Audio
                    && entry.writer.candidate_generation == self.candidate_generation
                    && entry.writer.source_utterance_id == utterance_id)
                    .then_some(slot.clone())
            })
            .collect();
        for slot in slots {
            if let Some(entry) = self.slot_journal.remove(&slot) {
                match entry.previous {
                    Some(value) => {
                        self.state.semantic_frame.slots.insert(slot, value);
                    }
                    None => {
                        self.state.semantic_frame.slots.remove(&slot);
                    }
                }
                self.state.semantic_frame.revision = self.state.semantic_frame.revision.next();
            }
        }
    }

    fn rollback_unverified_slots(&mut self, utterance_id: u64) {
        let slots: Vec<SlotName> = self
            .slot_journal
            .iter()
            .filter_map(|(slot, entry)| {
                (entry.writer.source == AsrSource::Audio
                    && entry.writer.source_utterance_id == utterance_id)
                    .then_some(slot.clone())
            })
            .collect();
        for slot in slots {
            if let Some(entry) = self.slot_journal.remove(&slot) {
                match entry.last_accepted.unwrap_or(entry.previous) {
                    Some(value) => {
                        self.state.semantic_frame.slots.insert(slot, value);
                    }
                    None => {
                        self.state.semantic_frame.slots.remove(&slot);
                    }
                }
                self.state.semantic_frame.revision = self.state.semantic_frame.revision.next();
            }
        }
    }

    /// Restores exact metadata or absence without triggering unrelated correction policy.
    fn rollback_abandoned_candidate_slots(&mut self) {
        for (slot, entry) in std::mem::take(&mut self.slot_journal) {
            if entry.writer.source == AsrSource::Audio
                && entry.writer.candidate_generation == self.candidate_generation
            {
                match entry.previous {
                    Some(value) => {
                        self.state.semantic_frame.slots.insert(slot, value);
                    }
                    None => {
                        self.state.semantic_frame.slots.remove(&slot);
                    }
                }
                self.state.semantic_frame.revision = self.state.semantic_frame.revision.next();
                self.metrics.inc("voice_slot_rollback_total");
            }
        }
    }

    fn maybe_spawn_speculative(&mut self, stable_prefix: &str) {
        let cues = self.text_analyzer.user(stable_prefix).cues;
        let intent = cues.iter().find_map(|cue| match cue {
            SemanticCue::IntentCandidate { intent, confidence } => {
                Some((intent.clone(), *confidence))
            }
            _ => None,
        });
        if let Some((intent, confidence)) = intent {
            self.maybe_spawn_speculative_intent(stable_prefix, &intent, confidence);
        }
    }

    fn maybe_spawn_speculative_intent(
        &mut self,
        stable_prefix: &str,
        intent: &str,
        confidence: f32,
    ) {
        let Some(speculative_agent) = &self.speculative_agent else {
            return;
        };
        if !self.config.agents.speculative.enabled
            || confidence < self.config.agents.speculative.minimum_intent_confidence
        {
            return;
        }

        let active_speculative = self
            .active_tasks
            .values()
            .filter(|task| task.kind == ActiveTaskKind::SpeculativeRead)
            .count() as u32;

        if active_speculative >= self.config.agents.speculative.maximum_parallel_tasks {
            return;
        }

        let task_id = TaskId::new();
        let thought_epoch = self.state.epochs.thought;
        let dependencies: Vec<String> = self.state.semantic_frame.slots.keys().cloned().collect();
        let fingerprint = self
            .state
            .semantic_frame
            .dependency_fingerprint(&dependencies);

        let cancellation = CancellationToken::new();
        let request = AgentTurnRequest {
            task_id,
            thought_epoch,
            input: format!(
                "Speculative read-only prewarm for intent {intent}. Stable transcript: {stable_prefix}"
            ),
            context: TurnContext::default(),
            speculative: true,
            dependencies: dependencies.clone(),
        };

        let agent = Arc::clone(speculative_agent);
        let event_tx = self.event_tx.clone();
        let meta_factory = Arc::clone(&self.meta_factory);
        let child = cancellation.clone();
        let timeout = Duration::from_millis(self.config.agents.speculative.timeout_ms);

        self.workers.spawn(async move {
            run_task_turn_with_timeout(agent, request, child, event_tx, meta_factory, timeout)
                .await;
        });

        self.active_tasks.insert(
            task_id,
            ActiveTask {
                id: task_id,
                kind: ActiveTaskKind::SpeculativeRead,
                thought_epoch,
                dependencies,
                fingerprint,
                cancellation,
                handle: None,
                candidate_generation: self.candidate_generation,
                candidate_utterance_id: Some(self.transcript.current_utterance()),
            },
        );

        self.metrics.inc("voice_speculative_started_total");
    }

    fn invalidate_tasks_for_slots(&mut self, changed_slots: &[SlotName]) {
        let task_ids: Vec<TaskId> = self
            .active_tasks
            .iter()
            .filter_map(|(id, task)| {
                let intersects = task
                    .dependencies
                    .iter()
                    .any(|dependency| changed_slots.contains(dependency));

                if intersects { Some(*id) } else { None }
            })
            .collect();

        for task_id in task_ids {
            if let Some(mut task) = self.active_tasks.remove(&task_id) {
                self.deep_deadlines.remove(&task_id);
                task.cancel();
                if task.kind == ActiveTaskKind::MainTurn {
                    if self.state.active_task_id == Some(task_id) {
                        self.state.active_task_id = None;
                        self.state.task = crate::interaction::TaskState::Cancelled;
                    }
                    self.retire_task_progress(task_id);
                    self.resolve_turn(task_id, TurnResolution::Discard);
                }
            }
        }

        self.speculative_results.retain(|result| {
            !result
                .dependencies
                .iter()
                .any(|dependency| changed_slots.contains(dependency))
        });
        self.deep_results.retain(|result| {
            !result
                .dependencies
                .iter()
                .any(|dependency| changed_slots.contains(dependency))
        });

        self.state.epochs.invalidate_thought();
    }

    // User turn commit and main task processing.

    async fn commit_user_turn(&mut self, reason: &str) -> Result<()> {
        if self.legacy_final.is_some() {
            return Ok(());
        }
        let current_key = self.semantic_snapshot().map(|(key, _, _)| key);
        let pending = self
            .semantic_pending_updates
            .take()
            .filter(|(key, _, _)| Some(key) == current_key.as_ref());
        let sealed_source = self.transcript.current_utterance();
        let sealed = self.transcript.seal_current();
        let sealed_audio = !sealed.trim().is_empty();
        if sealed_audio {
            self.consumed_audio_watermark = Some(
                self.consumed_audio_watermark
                    .map_or(sealed_source, |id| id.max(sealed_source)),
            );
            self.turn_buffer.push_partial_seal(sealed);
        }

        let transcript = self.turn_buffer.take();
        self.semantic_final = None;
        self.semantic_overlap_pending = false;
        self.semantic_overlap_base = None;
        self.semantic_overlap_audio_max = None;
        self.semantic_origin = None;
        self.semantic_heard_anchor = None;

        if transcript.trim().is_empty() {
            return Ok(());
        }
        if let Some((_, updates, provenance)) = pending {
            self.settle_previous_turns().await?;
            self.apply_slot_updates(updates, provenance).await?;
        }

        // The committed turn promotes the candidates that fed it, even when
        // no authoritative final ever arrived: a later stream reset must not
        // roll back work the user already committed.
        if sealed_audio {
            self.close_candidates_for(sealed_source, AsrSource::Audio);
        }

        let turn_id = TurnId::new();
        let revision = self.state.epochs.transcript;
        let now_us = self.monotonic_us();

        if let Some(started) = self.turn_started_at_us {
            self.metrics.observe_ms(
                "voice_user_turn_duration_ms",
                now_us.saturating_sub(started) / 1_000,
            );
        }

        self.turn_started_at_us = None;
        self.semantic_intent_baseline = None;
        self.backchannel_count_this_turn = 0;
        // The floor has changed hands; older floor decisions are moot.
        self.state.epochs.bump_interaction();
        self.metrics.inc("voice_user_turn_committed_total");

        let event = VoiceEvent::UserTurnCommitted {
            meta: self.new_meta(),
            turn_id,
            transcript_revision: revision,
            transcript: transcript.clone(),
        };
        self.forward_trace(&event).await;
        self.state.apply(&event);
        self.start_main_task(turn_id, transcript, revision).await?;
        if self.semantic_mode {
            let changed =
                self.state.semantic_frame.negated || self.state.semantic_frame.intent.is_some();
            self.state.semantic_frame.negated = false;
            self.state.semantic_frame.intent = None;
            if changed {
                self.state.semantic_frame.revision = self.state.semantic_frame.revision.next();
            }
        }

        let _ = reason;
        Ok(())
    }

    async fn start_main_task(
        &mut self,
        turn_id: TurnId,
        transcript: String,
        transcript_revision: Revision,
    ) -> Result<()> {
        // The commit-time environment is selected here. A `Never`-protected
        // act defers this turn, and uncommitted partials arriving while it
        // waits must not reshape the context the turn eventually runs with.
        let context = render_turn_context(
            turn_id,
            transcript_revision,
            &self.state.semantic_frame,
            &self
                .speculative_results
                .iter()
                .map(|r| (r.fingerprint, r.summary.clone()))
                .collect::<Vec<_>>(),
            &self.deep_results,
            &self.config.tts.language,
        );
        let dependencies: Vec<String> = self.state.semantic_frame.slots.keys().cloned().collect();
        self.settle_previous_turns().await?;
        self.pending_speech.clear();
        self.act_owner.clear();
        if self.deferred_turn.take().is_some() {
            self.metrics.inc("voice_turn_deferred_superseded_total");
        }

        if self.current_act_is_never_protected() {
            self.deferred_turn = Some(DeferredTurn {
                turn_id,
                transcript,
                transcript_revision,
                context,
                dependencies,
            });
            self.metrics.inc("voice_turn_deferred_total");
            return Ok(());
        }

        self.spawn_main_task_now(transcript, context, dependencies)
            .await
    }

    async fn spawn_main_task_now(
        &mut self,
        transcript: String,
        context: TurnContext,
        dependencies: Vec<String>,
    ) -> Result<()> {
        let old_deep: Vec<TaskId> = self
            .active_tasks
            .iter()
            .filter_map(|(id, task)| (task.kind == ActiveTaskKind::DeepWorker).then_some(*id))
            .collect();
        for id in old_deep {
            if let Some(mut task) = self.active_tasks.remove(&id) {
                task.cancel();
            }
            self.deep_deadlines.remove(&id);
        }
        let task_id = TaskId::new();
        let thought_epoch = self.state.epochs.invalidate_thought();

        let cancellation = CancellationToken::new();
        let task_dependencies = dependencies.clone();
        let request = AgentTurnRequest {
            task_id,
            thought_epoch,
            // Only the committed words go in as the user message. Everything
            // else is turn environment, carried out of band so it never
            // accumulates in conversation memory.
            input: transcript,
            context,
            speculative: false,
            dependencies,
        };

        let agent = Arc::clone(&self.task_agent);
        let event_tx = self.event_tx.clone();
        let meta_factory = Arc::clone(&self.meta_factory);
        let child = cancellation.clone();
        let timeout = Duration::from_millis(self.config.agents.task.timeout_ms);
        let (done_tx, done_rx) = watch::channel(false);
        self.main_worker_done.insert(task_id, done_rx);

        self.workers.spawn(async move {
            run_task_turn_with_timeout(agent, request, child, event_tx, meta_factory, timeout)
                .await;
            let _ = done_tx.send(true);
        });

        self.active_tasks.insert(
            task_id,
            ActiveTask {
                id: task_id,
                kind: ActiveTaskKind::MainTurn,
                thought_epoch,
                dependencies: task_dependencies,
                fingerprint: 0,
                cancellation,
                handle: None,
                candidate_generation: self.candidate_generation,
                candidate_utterance_id: None,
            },
        );

        self.state.active_task_id = Some(task_id);
        self.state.task = crate::interaction::TaskState::Reasoning;

        self.metrics.inc("voice_task_started_total");
        Ok(())
    }

    /// Starts the deferred turn once nothing protects the floor anymore.
    /// Consuming the record once means a duplicated acknowledgement can
    /// never start it twice.
    async fn maybe_release_deferred(&mut self) -> Result<()> {
        if self.deferred_turn.is_none() {
            return Ok(());
        }

        if self.current_act_is_never_protected()
            || !self.speech_records.is_empty()
            || !self.turn_speech.is_empty()
        {
            return Ok(());
        }

        if self
            .active_tasks
            .values()
            .any(|task| task.kind == ActiveTaskKind::MainTurn)
        {
            return Ok(());
        }

        let deferred = self.deferred_turn.take().expect("checked above");
        self.metrics.inc("voice_turn_deferred_released_total");
        tracing::debug!(
            turn_id = %deferred.turn_id,
            revision = %deferred.transcript_revision,
            "releasing the turn deferred behind a protected act"
        );

        self.spawn_main_task_now(deferred.transcript, deferred.context, deferred.dependencies)
            .await
    }

    // Tool lifecycle processing.

    fn is_task_result_current(&self, task_id: TaskId, thought_epoch: u64) -> bool {
        match self.active_tasks.get(&task_id) {
            None => false,
            Some(task) => {
                if task.thought_epoch != thought_epoch {
                    return false;
                }

                match task.kind {
                    ActiveTaskKind::MainTurn => {
                        self.state.active_task_id == Some(task_id)
                            && !task.cancellation.is_cancelled()
                    }
                    ActiveTaskKind::SpeculativeRead | ActiveTaskKind::DeepWorker => {
                        !task.cancellation.is_cancelled()
                            && (task.kind != ActiveTaskKind::SpeculativeRead
                                || matches!(task.candidate_generation, u64::MAX)
                                || task.candidate_generation == self.candidate_generation)
                    }
                }
            }
        }
    }

    async fn on_tool_started(
        &mut self,
        task_id: TaskId,
        thought_epoch: u64,
        tool: String,
    ) -> Result<()> {
        if !self.is_task_result_current(task_id, thought_epoch) {
            return Ok(());
        }

        self.state.task = crate::interaction::TaskState::ExecutingTool;

        let evidence_id = format!("tool_started:{task_id}:{tool}");
        let Some(template) = self.config.speech.progress_templates.get(&tool) else {
            return Ok(());
        };

        if template.minimum_expected_latency_ms == 0 {
            return Ok(());
        }

        let act = SpeechAct {
            id: SpeechId::new(),
            speech_epoch: self.state.epochs.speech,
            class: ClaimClass::Process,
            text: template.text.clone(),
            priority: crate::speech::act::PRIORITY_PROGRESS,
            interruptibility: Interruptibility::Immediate,
            evidence: vec![crate::speech::act::EvidenceRef::new(
                evidence_id,
                crate::speech::act::EvidenceKind::ToolStarted,
            )],
            expires_at_monotonic_us: Some(self.monotonic_us() + template.expire_after_ms * 1_000),
        };

        self.authorize_and_enqueue(act, None).await?;
        Ok(())
    }

    async fn on_tool_completed(
        &mut self,
        task_id: TaskId,
        thought_epoch: u64,
        tool: String,
        success: bool,
        output: String,
    ) -> Result<()> {
        if !self.is_task_result_current(task_id, thought_epoch) {
            return Ok(());
        }

        self.state.task = crate::interaction::TaskState::Reasoning;
        let started = format!("tool_started:{task_id}:{tool}");
        self.state.evidence.remove(&started);
        self.pending_speech
            .retain(|act| !act.evidence.iter().any(|r| r.id == started));
        let _ = (success, output);
        self.maybe_commit_transaction_action(task_id, &tool);

        Ok(())
    }

    async fn on_tool_executed(
        &mut self,
        task_id: TaskId,
        thought_epoch: u64,
        tool: String,
        executed: bool,
    ) -> Result<()> {
        if !self.is_task_result_current(task_id, thought_epoch) {
            return Ok(());
        }

        if !executed {
            self.metrics.inc("voice_tool_not_executed_total");
        }

        self.maybe_commit_transaction_action(task_id, &tool);
        Ok(())
    }

    fn maybe_commit_transaction_action(&mut self, task_id: TaskId, tool: &str) {
        if self.config.tools.effects.get(tool) != Some(&ToolEffect::TransactionalWrite) {
            return;
        }

        let action_id = format!("action_commit:{task_id}:{tool}");
        if self.state.evidence.contains_key(&action_id) {
            return;
        }

        let completed_id = format!("tool_completed:{task_id}:{tool}");
        let executed_id = format!("tool_executed:{task_id}:{tool}");
        let completed = matches!(
            self.state.evidence.get(&completed_id),
            Some(crate::session::view::SessionEvidence::ToolCompleted { success: true, .. })
        );
        let executed = matches!(
            self.state.evidence.get(&executed_id),
            Some(crate::session::view::SessionEvidence::ToolExecuted { executed: true, .. })
        );

        if completed && executed {
            self.state.evidence.insert(
                action_id,
                crate::session::view::SessionEvidence::ActionCommit {
                    task_id,
                    tool: tool.to_string(),
                    success: true,
                },
            );
            self.metrics.inc("voice_action_commit_total");
        }
    }

    /// Drops progress lines still queued for a task that has ended. Their
    /// tool-started evidence can outlive the task, so the claim gate alone
    /// would still admit them after the reply or failure text.
    fn retire_task_progress(&mut self, task_id: TaskId) {
        let started = format!("tool_started:{task_id}:");
        self.pending_speech
            .retain(|act| !act.evidence.iter().any(|e| e.id.starts_with(&started)));
    }

    async fn on_strict_final(&mut self, task_id: TaskId, original: String) -> Result<()> {
        use crate::speech::render::RenderedReply;
        self.retire_task_progress(task_id);
        let proofs = self.invocation_proofs.remove(&task_id).unwrap_or_default();
        let conflict = self.proof_conflicts.remove(&task_id);
        let rendered = if conflict {
            Err(anyhow::anyhow!("conflicting invocation records"))
        } else if self.write_unconfirmed(task_id, &proofs) {
            // The reply may still say the write succeeded, in any language;
            // nothing in the turn can check it, so none of it is spoken.
            Err(anyhow::anyhow!("unconfirmed write in this turn"))
        } else {
            self.speech_renderer.render(&original, &proofs)
        };
        self.active_tasks.remove(&task_id);
        let mut spoken = Vec::new();
        let mut rejected = false;
        if let Ok(RenderedReply {
            text,
            class,
            question,
            records,
        }) = rendered
        {
            let valid_records = records
                .iter()
                .all(|id| proofs.iter().any(|p| p.record_id == *id && p.usable()));
            let requires_records = matches!(class, ClaimClass::Transactional | ClaimClass::Factual);
            if valid_records
                && (!requires_records || !records.is_empty())
                && !text.trim().is_empty()
                && text.len() <= 16_384
                && class != ClaimClass::Process
            {
                let clauses = split_clauses(&text);
                let count = clauses.len();
                self.turn_speech.insert(
                    task_id,
                    TurnSpeech {
                        final_text: original.clone(),
                        clauses: Vec::new(),
                    },
                );
                for (index, clause) in clauses.into_iter().enumerate() {
                    let id = SpeechId::new();
                    let evidence_id = format!("authorized:{id}");
                    self.state.evidence.insert(
                        evidence_id.clone(),
                        crate::session::view::SessionEvidence::AuthorizedSpeech {
                            task_id,
                            speech_id: id,
                            speech_epoch: self.state.epochs.speech,
                            text: clause.clone(),
                            class,
                        },
                    );
                    let act = SpeechAct {
                        id,
                        speech_epoch: self.state.epochs.speech,
                        class,
                        text: clause.clone(),
                        priority: crate::speech::act::PRIORITY_FINAL,
                        interruptibility: Interruptibility::AfterClause,
                        evidence: vec![crate::speech::EvidenceRef::new(
                            evidence_id,
                            crate::speech::EvidenceKind::AuthorizedSpeech,
                        )],
                        expires_at_monotonic_us: None,
                    };
                    if index + 1 == count
                        && let Some(question) = question
                    {
                        self.speech_questions.insert(id, question);
                    }
                    if self.authorize_and_enqueue(act, Some(task_id)).await? {
                        self.audible_ledger.hold(id);
                        spoken.push((id, clause));
                    }
                }
            } else {
                rejected = true;
            }
        } else {
            rejected = true;
        }
        self.turn_speech.insert(
            task_id,
            TurnSpeech {
                final_text: original,
                clauses: spoken,
            },
        );
        self.state.task = crate::interaction::TaskState::Completed;
        if rejected {
            // The turn keeps its tool history and records the reply as
            // undelivered; the user hears the configured failure text instead
            // of silence.
            self.metrics.inc("voice_claim_rejected_total");
            self.speak_failure_text().await?;
        }
        Ok(())
    }

    /// A tool declared as a write, or an undeclared tool that actually ran,
    /// may have written. If any such call in this turn failed, was never
    /// completed, or left an unusable record, the reply cannot be checked
    /// against what actually happened. A call to a tool that never ran (for
    /// example a name the model made up) cannot have written anything.
    fn write_unconfirmed(
        &self,
        task_id: TaskId,
        proofs: &[crate::speech::render::InvocationProof],
    ) -> bool {
        use crate::session::view::SessionEvidence;
        let ran = |tool: &str| {
            proofs.iter().any(|p| p.operation == tool)
                || self.state.evidence.values().any(|evidence| match evidence {
                    SessionEvidence::ToolStarted {
                        task_id: t,
                        tool: name,
                        ..
                    } => *t == task_id && name == tool,
                    SessionEvidence::ToolExecuted {
                        task_id: t,
                        tool: name,
                        executed,
                        ..
                    } => *t == task_id && name == tool && *executed,
                    _ => false,
                })
        };
        let may_write = |tool: &str| match self.config.tools.effects.get(tool) {
            Some(ToolEffect::PureRead) => false,
            Some(_) => true,
            None => ran(tool),
        };
        proofs
            .iter()
            .any(|p| may_write(&p.operation) && !p.usable())
            || self.state.evidence.values().any(|evidence| match evidence {
                SessionEvidence::ToolStarted {
                    task_id: t, tool, ..
                } => *t == task_id && may_write(tool),
                SessionEvidence::ToolCompleted {
                    task_id: t,
                    tool,
                    success,
                    ..
                } => *t == task_id && !*success && may_write(tool),
                _ => false,
            })
    }

    async fn speak_failure_text(&mut self) -> Result<()> {
        if self.config.speech.failure_text.trim().is_empty() {
            return Ok(());
        }
        let act = SpeechAct::phatic(
            self.config.speech.failure_text.clone(),
            self.state.epochs.speech,
            crate::speech::act::PRIORITY_CLARIFICATION,
        );
        self.authorize_and_enqueue(act, None).await?;
        Ok(())
    }

    // Agent final response processing.

    async fn on_agent_final(
        &mut self,
        task_id: TaskId,
        thought_epoch: u64,
        text: String,
    ) -> Result<()> {
        let kind = self.active_tasks.get(&task_id).map(|task| task.kind);

        match kind {
            Some(ActiveTaskKind::SpeculativeRead) => {
                if !self.is_task_result_current(task_id, thought_epoch) {
                    return Ok(());
                }
                let fingerprint = self
                    .active_tasks
                    .get(&task_id)
                    .map(|task| task.fingerprint)
                    .unwrap_or(0);
                let dependencies = self
                    .active_tasks
                    .get(&task_id)
                    .map(|task| task.dependencies.clone())
                    .unwrap_or_default();

                let candidate_generation = self
                    .active_tasks
                    .get(&task_id)
                    .map(|task| task.candidate_generation)
                    .unwrap_or(0);
                let candidate_utterance_id = self
                    .active_tasks
                    .get(&task_id)
                    .and_then(|task| task.candidate_utterance_id);
                self.active_tasks.remove(&task_id);
                self.speculative_results.push(SpeculativeResult {
                    fingerprint,
                    summary: text,
                    dependencies,
                    candidate_generation,
                    candidate_utterance_id,
                });
                self.metrics.inc("voice_speculative_completed_total");
                return Ok(());
            }
            Some(ActiveTaskKind::MainTurn) => {
                if !self.is_task_result_current(task_id, thought_epoch) {
                    self.metrics.inc("voice_task_stale_result_dropped_total");
                    return Ok(());
                }
            }
            Some(ActiveTaskKind::DeepWorker) | None => {
                return Ok(());
            }
        }

        self.on_strict_final(task_id, text).await
    }

    async fn on_agent_failed(
        &mut self,
        task_id: TaskId,
        thought_epoch: u64,
        message: String,
    ) -> Result<()> {
        if !self.is_task_result_current(task_id, thought_epoch) {
            self.metrics.inc("voice_task_stale_result_dropped_total");
            return Ok(());
        }

        let kind = self.active_tasks.get(&task_id).map(|task| task.kind);
        self.active_tasks.remove(&task_id);
        self.retire_task_progress(task_id);

        if kind == Some(ActiveTaskKind::SpeculativeRead) {
            self.metrics.inc("voice_speculative_failed_total");
            tracing::debug!("speculative agent failed: {message}");
            return Ok(());
        }

        // The failed turn may have left a user message with no reply. Roll it
        // back: the user is asked to repeat, and memory stays well paired.
        if kind == Some(ActiveTaskKind::MainTurn) {
            self.resolve_turn(task_id, TurnResolution::Discard);
        }

        self.speak_failure_text().await?;
        self.metrics.inc("voice_agent_failed_total");
        tracing::warn!("agent turn failed: {message}");
        Ok(())
    }

    // Speech queue and TTS flow.

    /// Returns whether the act was queued. A rejected act is dropped silently.
    async fn authorize_and_enqueue(
        &mut self,
        act: SpeechAct,
        owner: Option<TaskId>,
    ) -> Result<bool> {
        let now_us = self.monotonic_us();

        if let Err(rejection) = self.claim_gate.authorize(&act, &self.state, now_us) {
            self.metrics.inc("voice_claim_rejected_total");
            tracing::debug!(?rejection, "claim gate rejected speech");
            return Ok(false);
        }

        if let Some(expires_at) = act.expires_at_monotonic_us
            && now_us >= expires_at
        {
            self.metrics.inc("voice_speech_expired_total");
            return Ok(false);
        }

        if let Some(owner) = owner {
            self.act_owner.insert(act.id, owner);
        }

        self.enqueue_act_unchecked(act);
        self.start_next_speech_if_possible().await?;
        Ok(true)
    }

    fn enqueue_act_unchecked(&mut self, act: SpeechAct) {
        let position = self
            .pending_speech
            .iter()
            .position(|queued| queued.priority < act.priority)
            .unwrap_or(self.pending_speech.len());
        self.pending_speech.insert(position, act);
    }

    fn retag_pending_speech(&mut self, epoch: u64) {
        for act in &mut self.pending_speech {
            if let Some(reference) = act
                .evidence
                .iter()
                .find(|r| r.kind == crate::speech::EvidenceKind::AuthorizedSpeech)
                && let Some(crate::session::view::SessionEvidence::AuthorizedSpeech {
                    speech_id,
                    speech_epoch,
                    text,
                    class,
                    ..
                }) = self.state.evidence.get_mut(&reference.id)
                && *speech_id == act.id
                && *speech_epoch == act.speech_epoch
                && *text == act.text
                && *class == act.class
            {
                *speech_epoch = epoch;
            }
            act.speech_epoch = epoch;
        }
    }

    fn retire_question_metadata(&mut self) {
        self.speech_questions.retain(|id, _| {
            self.speech_records.contains_key(id)
                || self.pending_speech.iter().any(|act| act.id == *id)
        });
    }

    async fn start_next_speech_if_possible(&mut self) -> Result<()> {
        if self
            .speech_records
            .values()
            .any(|record| record.playback == PlaybackLifecycle::Stopping)
        {
            return Ok(());
        }
        if self.deferred_turn.is_some() {
            return Ok(());
        }
        if matches!(
            self.state.speech,
            SpeechState::Playing | SpeechState::Synthesizing | SpeechState::Ducking
        ) {
            return Ok(());
        }

        if self.state.local_vad_active {
            return Ok(());
        }

        while let Some(act) = self.pending_speech.pop_front() {
            if self
                .claim_gate
                .authorize(&act, &self.state, self.monotonic_us())
                .is_err()
            {
                self.act_owner.remove(&act.id);
                self.speech_questions.remove(&act.id);
                self.metrics.inc("voice_claim_rejected_total");
                continue;
            }
            if act.speech_epoch != self.state.epochs.speech {
                self.metrics.inc("voice_stale_speech_dropped_total");
                continue;
            }

            self.speech_records.insert(
                act.id,
                SpeechRecord {
                    text: act.text.clone(),
                    never: act.interruptibility == Interruptibility::Never,
                    sample_rate: 0,
                    owner: self.act_owner.get(&act.id).copied(),
                    synthesis: SynthesisOutcome::Running,
                    playback: PlaybackLifecycle::NotStarted,
                    confirmed_samples: 0,
                    stop_deadline_us: None,
                },
            );
            self.act_owner.remove(&act.id);
            self.state.active_speech_id = Some(act.id);

            // The playback watchdog is armed by the first real playback
            // evidence, never by synthesis admission.

            // The cancellation token is registered before queue admission,
            // so an abort can never miss the window while the act sits in
            // the queue.
            let cancellation = CancellationToken::new();
            self.tts_registry.insert(act.id, cancellation.clone());
            let act_id = act.id;

            if self.speech_tx.try_send((act, cancellation)).is_err() {
                self.tts_registry.remove(&act_id);
                self.speech_records.remove(&act_id);
                self.shutdown.cancel();
                anyhow::bail!("speech queue overloaded or closed");
            }

            self.state.speech = SpeechState::Synthesizing;
            return Ok(());
        }

        Ok(())
    }

    async fn on_tts_audio(
        &mut self,
        speech_id: SpeechId,
        speech_epoch: u64,
        sequence: u32,
        sample_rate: u32,
        pcm: bytes::Bytes,
    ) -> Result<()> {
        if pcm.is_empty() {
            return Ok(());
        }
        // Admission already verified identity before any state is changed.
        let first_audio = match self.speech_records.get_mut(&speech_id) {
            Some(record) if record.playback == PlaybackLifecycle::NotStarted => {
                record.playback = PlaybackLifecycle::Playing;
                record.sample_rate = sample_rate;
                true
            }
            Some(_) => false,
            None => return Ok(()),
        };

        // Only real playback evidence arms the watchdog; synthesis chunks
        // must not mask a client that stopped acknowledging.
        if first_audio {
            self.last_playback_signal_us = Some(self.monotonic_us());
        }

        if self.state.local_vad_active {
            // The user started speaking while we were synthesizing: keep the
            // audio but ducked, and never steal the floor.
            if first_audio {
                self.playback
                    .command(PlaybackCommand::Duck {
                        gain: self.config.turn_control.barge_in.duck_gain,
                        fade_ms: self.config.turn_control.barge_in.duck_fade_ms,
                    })
                    .await;
            }
            self.state.speech = SpeechState::Ducking;
        } else {
            if first_audio {
                self.state.speech = SpeechState::Playing;
            }
            if !matches!(
                self.state.floor,
                crate::interaction::FloorState::UserHolding
                    | crate::interaction::FloorState::OverlapProbe
            ) {
                self.state.floor = crate::interaction::FloorState::AgentHolding;
            }
        }

        if first_audio && let Some(record) = self.speech_records.get(&speech_id) {
            self.audible_ledger
                .speech_started(speech_id, &record.text, sample_rate);
        }
        self.audible_ledger
            .add_enqueued_samples(speech_id, (pcm.len() / 2) as u64);

        self.playback
            .command(PlaybackCommand::Enqueue {
                speech_id,
                speech_epoch,
                sequence,
                sample_rate,
                pcm,
            })
            .await;

        Ok(())
    }

    async fn on_tts_audio_done(&mut self, speech_id: SpeechId, speech_epoch: u64) -> Result<()> {
        // Admission verified the record, the epoch, and the running
        // synthesis: this is the first and only success terminal.
        if self
            .speech_records
            .get(&speech_id)
            .is_some_and(|record| record.playback == PlaybackLifecycle::NotStarted)
        {
            return self
                .on_tts_failed(speech_id, "tts completed without audio".into())
                .await;
        }
        if let Some(record) = self.speech_records.get_mut(&speech_id) {
            record.synthesis = SynthesisOutcome::Succeeded;
        }

        self.audible_ledger.mark_synthesis_done(speech_id);

        self.playback
            .command(PlaybackCommand::MarkDone {
                speech_id,
                speech_epoch,
            })
            .await;

        Ok(())
    }

    async fn on_tts_failed(&mut self, speech_id: SpeechId, message: String) -> Result<()> {
        let Some(record) = self.speech_records.get_mut(&speech_id) else {
            return Ok(());
        };

        // A duplicate failure terminal changes nothing.
        if record.synthesis != SynthesisOutcome::Running {
            return Ok(());
        }
        record.synthesis = SynthesisOutcome::Failed;
        let owner = record.owner;
        let reached_browser = record.playback != PlaybackLifecycle::NotStarted;

        self.metrics.inc("voice_tts_failed_total");
        tracing::warn!("tts failed: {message}");

        if reached_browser {
            // Some audio already reached the client: abort it and settle with
            // the interrupted acknowledgement, bounded by the stop deadline.
            // The epoch bump makes any late provider chunk stale.
            let new_epoch = self.state.epochs.invalidate_speech();
            let now_us = self.monotonic_us();

            let record = self
                .speech_records
                .get_mut(&speech_id)
                .expect("checked above");
            record.playback = PlaybackLifecycle::Stopping;
            record.stop_deadline_us = Some(
                now_us.saturating_add(self.config.speech.playback_stop_ack_timeout_ms * 1_000),
            );

            self.playback
                .command(PlaybackCommand::Abort {
                    new_speech_epoch: new_epoch,
                    reason: InterruptionReason::ProviderError,
                })
                .await;

            // Independent replies survive re-stamped; the failed reply's
            // remaining clauses do not.
            self.discard_reply_clauses(owner);
            self.retag_pending_speech(new_epoch);

            if self.state.active_speech_id == Some(speech_id) {
                self.state.active_speech_id = None;
                self.state.speech = SpeechState::Silent;
            }
        } else {
            // Nothing reached the browser: nothing to abort, so the act
            // settles as undelivered immediately.
            self.settle_speech_record(speech_id);
            self.discard_reply_clauses(owner);

            if self.state.active_speech_id == Some(speech_id) {
                self.state.active_speech_id = None;
                self.state.speech = SpeechState::Silent;
            }

            self.start_next_speech_if_possible().await?;
        }

        Ok(())
    }

    /// Removes the queued acts that belong to one reply, so a failed
    /// synthesis never leaves the rest of that reply speaking.
    fn discard_reply_clauses(&mut self, owner: Option<TaskId>) {
        let Some(owner) = owner else {
            return;
        };
        let Some(turn) = self.turn_speech.get(&owner) else {
            return;
        };

        let clause_ids: HashSet<SpeechId> = turn.clauses.iter().map(|(id, _)| *id).collect();
        let before = self.pending_speech.len();
        self.pending_speech
            .retain(|act| !clause_ids.contains(&act.id));
        if self.pending_speech.len() != before {
            self.metrics.inc("voice_reply_clauses_discarded_total");
        }
    }

    async fn enqueue_backchannel(&mut self) -> Result<()> {
        let config = &self.config.turn_control.backchannel;
        if !config.enabled || self.config.speech.backchannel_text.trim().is_empty() {
            return Ok(());
        }

        let now_us = self.monotonic_us();

        if self.backchannel_count_this_turn >= config.maximum_per_user_turn {
            return Ok(());
        }

        if let Some(last) = self.last_backchannel_at_us
            && now_us.saturating_sub(last) < config.minimum_interval_ms * 1_000
        {
            return Ok(());
        }

        self.backchannel_count_this_turn += 1;
        self.last_backchannel_at_us = Some(now_us);

        let act = SpeechAct::phatic(
            self.config.speech.backchannel_text.clone(),
            self.state.epochs.speech,
            crate::speech::act::PRIORITY_BACKCHANNEL,
        );
        self.authorize_and_enqueue(act, None).await?;
        self.metrics.inc("voice_backchannel_emitted_total");
        Ok(())
    }

    // Deterministic control tick.

    async fn discard_semantic_backchannel(&mut self, playing: bool) -> Result<()> {
        let final_event = self.semantic_final.take();
        if let Some(base) = self.semantic_overlap_base.take() {
            self.turn_buffer.discard_from(base);
        } else if let Some((_, _, text)) = &final_event {
            self.turn_buffer.discard_last_if(text);
        }
        let audio_id = self
            .semantic_overlap_audio_max
            .take()
            .or_else(|| {
                (!self.transcript.current_text().is_empty())
                    .then(|| self.transcript.current_utterance())
            })
            .or_else(|| {
                final_event
                    .as_ref()
                    .and_then(|(source, id, _)| (*source == AsrSource::Audio).then_some(*id))
            });
        if let Some(id) = audio_id {
            self.suppressed_audio_watermark = Some(
                self.suppressed_audio_watermark
                    .map_or(id, |old| old.max(id)),
            );
            self.close_candidates_for(id, AsrSource::Audio);
        }
        if let Some((AsrSource::Injection, id, _)) = final_event {
            self.close_candidates_for(id, AsrSource::Injection);
        }
        self.semantic_overlap_pending = false;
        self.state.current_hypothesis.clear();
        self.transcript.clear();
        if playing {
            self.resume_after_backchannel().await?;
        } else {
            self.state.floor = crate::interaction::FloorState::Idle;
        }
        Ok(())
    }

    async fn on_semantic_tick(&mut self, now_us: u64) -> Result<bool> {
        if self.state.local_vad_active {
            return Ok(false);
        }
        let candidate = self
            .turn_buffer
            .render_candidate(self.transcript.current_text());
        let Some(last_audio) = self.state.last_user_audio_at_us else {
            return Ok(false);
        };
        let silence_ms = now_us.saturating_sub(last_audio) / 1_000;
        let assessment = self.semantic_snapshot().and_then(|(key, _, _)| {
            self.semantic_observation
                .as_ref()
                .filter(|(observed, _)| *observed == key)
                .map(|(_, assessment)| assessment.clone())
        });
        let playing = matches!(
            self.state.speech,
            SpeechState::Ducking | SpeechState::Playing
        );
        if self.semantic_overlap_pending
            || self.state.floor == crate::interaction::FloorState::OverlapProbe
        {
            if silence_ms < self.config.turn_control.barge_in.confirm_ms {
                return Ok(false);
            }
            if candidate.trim().is_empty() {
                if playing && self.state.floor == crate::interaction::FloorState::OverlapProbe {
                    self.resume_after_backchannel().await?;
                }
                return Ok(false);
            }
            match assessment.as_ref().map(|value| value.state) {
                Some(UserSpeechState::Backchannel | UserSpeechState::NoiseOrEcho) => {
                    self.discard_semantic_backchannel(playing).await?;
                    return Ok(false);
                }
                Some(
                    UserSpeechState::Interruption
                    | UserSpeechState::Correction
                    | UserSpeechState::Answer,
                ) => {
                    self.semantic_overlap_pending = false;
                    self.semantic_overlap_base = None;
                    self.semantic_overlap_audio_max = None;
                    if playing {
                        let pending = self.semantic_pending_updates.take();
                        let reason = if assessment
                            .as_ref()
                            .is_some_and(|value| value.state == UserSpeechState::Correction)
                        {
                            InterruptionReason::SemanticCorrection
                        } else {
                            InterruptionReason::SemanticInterruption
                        };
                        self.abort_current_speech(reason).await?;
                        if let Some((key, _, _)) = self.semantic_snapshot() {
                            self.semantic_last_key = Some(key.clone());
                            self.semantic_pending_updates = pending
                                .map(|(_, updates, provenance)| (key.clone(), updates, provenance));
                            self.semantic_observation = assessment.map(|value| (key, value));
                        }
                        return Ok(false);
                    }
                }
                _ if silence_ms
                    >= self
                        .config
                        .agents
                        .interaction
                        .timeout_ms
                        .saturating_mul(2)
                        .max(self.config.turn_control.barge_in.confirm_ms) =>
                {
                    self.semantic_overlap_pending = false;
                    self.semantic_overlap_base = None;
                    self.semantic_overlap_audio_max = None;
                    if playing {
                        self.abort_current_speech(InterruptionReason::SemanticInterruption)
                            .await?;
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
        }
        if candidate.trim().is_empty() {
            return Ok(false);
        }
        if silence_ms >= self.config.turn_control.hard_silence_ms {
            self.commit_user_turn("hard_silence").await?;
            return Ok(true);
        }
        if let Some(assessment) = assessment {
            let threshold = if assessment.state == UserSpeechState::Answer
                && self.state.assistant_asked_question
            {
                self.config.turn_control.answer_min_silence_ms
            } else {
                self.config.turn_control.soft_silence_ms
            };
            let complete = matches!(
                assessment.state,
                UserSpeechState::Complete
                    | UserSpeechState::Answer
                    | UserSpeechState::Correction
                    | UserSpeechState::Interruption
            ) && assessment.complete.unwrap_or(matches!(
                assessment.state,
                UserSpeechState::Complete | UserSpeechState::Answer
            ));
            if silence_ms >= threshold && complete {
                self.commit_user_turn("semantic_complete").await?;
                return Ok(true);
            }
            if !complete
                && assessment.state == UserSpeechState::Incomplete
                && silence_ms >= self.config.turn_control.soft_silence_ms
            {
                self.enqueue_backchannel().await?;
            }
        }
        Ok(false)
    }

    async fn on_control_tick(&mut self) -> Result<()> {
        let now_us = self.monotonic_us();
        if self
            .slot_extraction_running
            .is_some_and(|(_, deadline)| now_us >= deadline)
        {
            self.cancel_slot_extraction();
            if let Some((id, _)) = self.legacy_final.take() {
                self.rollback_unverified_slots(id);
                self.close_candidates_for(id, AsrSource::Audio);
            }
            if let Some(pending) = self.slot_extraction_pending.take() {
                self.start_slot_extraction(pending);
            }
        }
        let expired: Vec<TaskId> = self
            .deep_deadlines
            .iter()
            .filter_map(|(id, deadline)| (now_us >= *deadline).then_some(*id))
            .collect();
        for id in expired {
            self.deep_deadlines.remove(&id);
            if let Some(mut task) = self.active_tasks.remove(&id) {
                task.cancel();
                self.metrics.inc("voice_deep_work_timeout_total");
            }
        }
        let candidate = self
            .turn_buffer
            .render_candidate(self.transcript.current_text());

        if self.turn_started_at_us.is_none()
            && (!self.transcript.current_text().is_empty() || !self.turn_buffer.is_empty())
        {
            self.turn_started_at_us = Some(now_us);
        }

        let recent_assistant = recent_assistant_text(self.audible_ledger.history());

        if self.semantic_mode {
            self.maybe_spawn_semantic();
            if self.on_semantic_tick(now_us).await? {
                return Ok(());
            }
        } else {
            if let Some(decision) = self.policy.decide_on_tick(
                &self.state,
                &candidate,
                now_us,
                &self.config.turn_control,
                &self.config.echo,
                &recent_assistant,
            ) {
                match decision.action {
                    InteractionAction::CommitUserTurn | InteractionAction::TakeFloor => {
                        if self.legacy_final.is_none() {
                            self.commit_user_turn(&decision.reason_code).await?;
                            return Ok(());
                        }
                    }
                    InteractionAction::KeepListening | InteractionAction::HoldFloor => {
                        self.metrics.inc("voice_floor_keep_listening_total");
                    }
                    _ => {
                        let envelope = InteractionDecisionEnvelope {
                            revision: self.state.epochs.transcript,
                            interaction_epoch: self.state.epochs.interaction,
                            decision,
                        };
                        self.apply_floor_decision(envelope).await?;
                    }
                }
            } else if !candidate.trim().is_empty() {
                let silence_ms = self.state.silence_ms(now_us);
                if silence_ms >= self.config.turn_control.soft_silence_ms {
                    self.spawn_interaction_brain().await;
                }
            }
            self.maybe_emit_backchannel(&candidate, now_us).await?;
        }
        self.recover_stalled_playback(now_us).await?;
        self.settle_expired_stops(now_us);
        self.start_next_speech_if_possible().await?;
        self.resolve_settled_turns();
        self.retire_question_metadata();
        self.maybe_release_deferred().await?;

        Ok(())
    }

    /// Releases the speech queue when a client stops acknowledging playback.
    ///
    /// `SpeechState` only leaves `Playing` on a client ACK. A disconnected or
    /// wedged client would otherwise pin the queue for the rest of the
    /// session. Only real playback is watched here: synthesis is bounded by
    /// the request deadline inside the TTS session, and a stopping act is
    /// bounded by its own stop deadline.
    async fn recover_stalled_playback(&mut self, now_us: u64) -> Result<()> {
        let timeout_ms = self.config.speech.playback_ack_timeout_ms;
        if timeout_ms == 0 {
            return Ok(());
        }

        let active_playing = self.state.active_speech_id.is_some_and(|speech_id| {
            self.speech_records
                .get(&speech_id)
                .is_some_and(|record| record.playback == PlaybackLifecycle::Playing)
        }) && matches!(
            self.state.speech,
            SpeechState::Playing | SpeechState::Ducking
        );

        if !active_playing {
            return Ok(());
        }

        let Some(last) = self.last_playback_signal_us else {
            return Ok(());
        };

        if now_us.saturating_sub(last) < timeout_ms * 1_000 {
            return Ok(());
        }

        tracing::warn!(
            timeout_ms,
            "no playback acknowledgement; releasing the speech queue"
        );

        // Releasing our own state is not enough. A merely slow client is still
        // playing, and the timed-out speech's late TTS chunks would still match
        // the current epoch, so the next act would overlap it. Bump the epoch
        // and tell the client to stop, exactly as a barge-in would.
        let new_epoch = self.state.epochs.invalidate_speech();

        if let Some(speech_id) = self.state.active_speech_id.take() {
            self.tts_registry.cancel(&speech_id);
            let confirmed = self
                .speech_records
                .get(&speech_id)
                .map(|record| record.confirmed_samples)
                .unwrap_or(0);
            self.settle_speech_record(speech_id);
            // Conservative: only the progress the client already confirmed
            // counts as heard.
            self.audible_ledger.mark_interrupted(
                speech_id,
                confirmed,
                InterruptionReason::ProviderError,
            );
        }

        self.playback
            .command(PlaybackCommand::Abort {
                new_speech_epoch: new_epoch,
                reason: InterruptionReason::ProviderError,
            })
            .await;

        // Anything queued was authored for the dead epoch; re-stamp it so the
        // timeout drops the stuck utterance without discarding pending speech.
        self.retag_pending_speech(new_epoch);

        self.last_playback_signal_us = None;
        self.state.active_speech_id = None;
        self.state.speech = SpeechState::Silent;

        // `PlaybackCompleted` is what normally hands the floor back, and it is
        // exactly the event that never arrived. Release it here too, otherwise
        // the floor stays `AgentHolding` for the rest of the session.
        if !self.state.local_vad_active
            && self.state.floor == crate::interaction::FloorState::AgentHolding
        {
            self.state.floor = crate::interaction::FloorState::Idle;
        }

        self.metrics.inc("voice_playback_ack_timeout_total");

        Ok(())
    }

    async fn maybe_emit_backchannel(&mut self, candidate: &str, now_us: u64) -> Result<()> {
        let config = &self.config.turn_control;
        if !config.backchannel.enabled {
            return Ok(());
        }

        let agent_silent = matches!(
            self.state.speech,
            SpeechState::Silent | SpeechState::Aborted
        );
        if !agent_silent {
            return Ok(());
        }

        // A backchannel belongs in the gap the user leaves mid-thought, so the
        // mic must be closed and the floor must be `UserPausing`. Requiring an
        // active VAD here as well made this branch unreachable: `UserPausing`
        // is only ever assigned when local speech ends.
        if self.state.local_vad_active {
            return Ok(());
        }

        if !matches!(
            self.state.floor,
            crate::interaction::FloorState::UserPausing
        ) {
            return Ok(());
        }

        if self.backchannel_count_this_turn >= config.backchannel.maximum_per_user_turn {
            return Ok(());
        }

        if let Some(last) = self.last_backchannel_at_us
            && now_us.saturating_sub(last) < config.backchannel.minimum_interval_ms * 1_000
        {
            return Ok(());
        }

        // Only inside the window where the reflex policy is still waiting:
        // before the soft threshold the pause is not yet meaningful, after the
        // hard threshold the turn is about to commit anyway.
        let silence_ms = self.state.silence_ms(now_us);
        if silence_ms < config.soft_silence_ms || silence_ms >= config.hard_silence_ms {
            return Ok(());
        }

        if self.policy.ends_incomplete(candidate, config) {
            self.enqueue_backchannel().await?;
        }

        Ok(())
    }

    // Turn memory resolution.
    //
    // The agent's conversation memory records a reply as soon as it is
    // generated. These methods report what actually happened to it, so the
    // agent can replace the reply with what was heard, or drop the turn.

    fn resolve_turn(&mut self, task_id: TaskId, resolution: TurnResolution) {
        self.invocation_proofs.remove(&task_id);
        self.proof_conflicts.remove(&task_id);
        self.state.evidence.retain(|_, evidence| !matches!(evidence, crate::session::view::SessionEvidence::AuthorizedSpeech { task_id: owner, .. } if *owner == task_id));
        match &resolution {
            TurnResolution::Discard => self.metrics.inc("voice_turn_discarded_total"),
            TurnResolution::Audible { final_text, heard } => {
                self.metrics.inc("voice_turn_resolved_total");
                if heard != final_text {
                    self.metrics.inc("voice_turn_heard_partially_total");
                }
            }
        }
        if let Some(mut done) = self.main_worker_done.remove(&task_id) {
            let agent = Arc::clone(&self.task_agent);
            self.workers.spawn(async move {
                if !*done.borrow() {
                    let _ = done.wait_for(|finished| *finished).await;
                }
                agent.resolve_turn(task_id, resolution);
            });
        } else {
            self.task_agent.resolve_turn(task_id, resolution);
        }
    }

    /// Whether one clause of a reply can no longer change what the user heard.
    fn clause_settled(&self, speech_id: SpeechId) -> bool {
        if self.audible_ledger.is_settled(speech_id) {
            return true;
        }

        let queued = self.pending_speech.iter().any(|act| act.id == speech_id);
        let active = self.state.active_speech_id == Some(speech_id);
        let playing = self.audible_ledger.in_flight_id() == Some(speech_id);

        !(queued || active || playing)
    }

    /// Resolves a reply with what was heard and releases its held clauses.
    fn resolve_heard_turn(&mut self, task_id: TaskId, turn: TurnSpeech) {
        let heard = self.heard_rendering(&turn);
        for (id, _) in &turn.clauses {
            self.audible_ledger.release(*id);
        }
        self.resolve_turn(
            task_id,
            TurnResolution::Audible {
                final_text: turn.final_text,
                heard,
            },
        );
    }

    fn heard_rendering(&self, turn: &TurnSpeech) -> String {
        let clauses: Vec<(&str, Heard)> = turn
            .clauses
            .iter()
            .map(|(id, text)| {
                let heard = self.audible_ledger.heard_so_far(*id).unwrap_or(Heard::None);
                (text.as_str(), heard)
            })
            .collect();
        render_heard_turn(&clauses)
    }

    /// Conservative fast settlement for acts already waiting on an
    /// interrupted acknowledgement: the confirmed progress is all they get.
    fn settle_all_stopping_now(&mut self) {
        let stopping: Vec<SpeechId> = self
            .speech_records
            .iter()
            .filter(|(_, record)| record.playback == PlaybackLifecycle::Stopping)
            .map(|(id, _)| *id)
            .collect();

        for speech_id in stopping {
            let confirmed = self
                .speech_records
                .get(&speech_id)
                .map(|record| record.confirmed_samples)
                .unwrap_or(0);
            self.settle_speech_record(speech_id);
            self.audible_ledger.mark_interrupted(
                speech_id,
                confirmed,
                InterruptionReason::UserTurn,
            );
        }
    }

    /// Settles stopping records whose interrupted acknowledgement never came.
    fn settle_expired_stops(&mut self, now_us: u64) {
        let expired: Vec<SpeechId> = self
            .speech_records
            .iter()
            .filter(|(_, record)| {
                record.playback == PlaybackLifecycle::Stopping
                    && record
                        .stop_deadline_us
                        .is_some_and(|deadline| now_us >= deadline)
            })
            .map(|(id, _)| *id)
            .collect();

        for speech_id in expired {
            let confirmed = self
                .speech_records
                .get(&speech_id)
                .map(|record| record.confirmed_samples)
                .unwrap_or(0);
            self.settle_speech_record(speech_id);
            self.audible_ledger.mark_interrupted(
                speech_id,
                confirmed,
                InterruptionReason::ProviderError,
            );
            self.metrics.inc("voice_stop_ack_timeout_total");
        }
    }

    /// Resolves every reply whose clauses have all settled.
    fn resolve_settled_turns(&mut self) {
        let settled: Vec<TaskId> = self
            .turn_speech
            .iter()
            .filter(|(_, turn)| turn.clauses.iter().all(|(id, _)| self.clause_settled(*id)))
            .map(|(task_id, _)| *task_id)
            .collect();

        for task_id in settled {
            if let Some(turn) = self.turn_speech.remove(&task_id) {
                self.resolve_heard_turn(task_id, turn);
            }
        }
    }

    /// Settles everything from earlier turns before a new one starts.
    ///
    /// A new committed turn means the conversation has moved on. A previous
    /// turn still reasoning is cancelled and discarded. A previous reply still
    /// being spoken is stopped and resolved with what was heard so far, because
    /// the new turn's context must reflect what the user heard before they
    /// spoke again, not what might have played afterwards.
    async fn settle_previous_turns(&mut self) -> Result<()> {
        let running: Vec<TaskId> = self
            .active_tasks
            .iter()
            .filter(|(_, task)| task.kind == ActiveTaskKind::MainTurn)
            .map(|(task_id, _)| *task_id)
            .collect();

        for task_id in running {
            if let Some(mut task) = self.active_tasks.remove(&task_id) {
                task.cancel();
                self.metrics.inc("voice_task_superseded_total");
                self.resolve_turn(task_id, TurnResolution::Discard);
            }
        }

        let protected_owner = self
            .state
            .active_speech_id
            .filter(|_| self.current_act_is_never_protected())
            .and_then(|id| self.speech_records.get(&id).and_then(|record| record.owner));
        // Ownerless phatic/process speech follows the same commit boundary as replies.
        if self.state.active_speech_id.is_some() && !self.current_act_is_never_protected() {
            // Aborting moves the floor to the user, but at this point the turn
            // commit has already handed it back; keep that.
            let floor = self.state.floor;
            self.abort_current_speech(InterruptionReason::UserTurn)
                .await?;
            self.state.floor = floor;
        }

        self.settle_all_stopping_now();
        self.pending_speech.clear();
        let unresolved: Vec<TaskId> = self
            .turn_speech
            .keys()
            .copied()
            .filter(|id| Some(*id) != protected_owner)
            .collect();
        for task_id in unresolved {
            if let Some(turn) = self.turn_speech.remove(&task_id) {
                self.resolve_heard_turn(task_id, turn);
            }
        }

        Ok(())
    }

    // Deep worker integration.

    pub async fn request_deep_work(
        &mut self,
        objective: impl Into<String>,
        dependencies: Vec<SlotName>,
    ) -> Result<()> {
        let Some(deep_worker) = &self.deep_worker else {
            return Ok(());
        };

        let task_id = TaskId::new();
        let thought_epoch = self.state.epochs.thought;

        let request = crate::session::deep_worker::DeepWorkRequest {
            task_id,
            thought_epoch,
            objective: objective.into(),
            committed_context: serde_json::to_value(
                self.state
                    .committed_user_turns
                    .last()
                    .map(|turn| turn.transcript.clone())
                    .unwrap_or_default(),
            )
            .unwrap_or(serde_json::Value::Null),
            dependencies: dependencies.clone(),
        };

        let worker = Arc::clone(deep_worker);
        let tx = self.deep_tx.clone();
        let timeout = Duration::from_millis(self.config.agents.deep.timeout_ms);
        let cancellation = CancellationToken::new();
        let child = cancellation.clone();
        let deadline = self
            .monotonic_us()
            .saturating_add(self.config.agents.deep.timeout_ms.saturating_mul(1_000));
        self.deep_deadlines.insert(task_id, deadline);
        self.active_tasks.insert(
            task_id,
            ActiveTask {
                id: task_id,
                kind: ActiveTaskKind::DeepWorker,
                thought_epoch,
                dependencies,
                fingerprint: 0,
                cancellation,
                handle: None,
                candidate_generation: self.candidate_generation,
                candidate_utterance_id: None,
            },
        );

        let shutdown = self.shutdown.clone();
        self.workers.spawn(async move {
            let result = tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = child.cancelled() => return,
                result = tokio::time::timeout(timeout, worker.run(request)) => match result {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(_) => Err(format!("timed out after {}ms", timeout.as_millis())),
                },
            };
            let _ = tx
                .send(DeepCompletion {
                    task_id,
                    thought_epoch,
                    result,
                })
                .await;
        });

        self.metrics.inc("voice_deep_work_started_total");
        Ok(())
    }

    async fn finish_deep_work(&mut self, completion: DeepCompletion, forward: bool) {
        let current = self
            .active_tasks
            .get(&completion.task_id)
            .is_some_and(|task| {
                task.kind == ActiveTaskKind::DeepWorker
                    && task.thought_epoch == completion.thought_epoch
                    && !task.cancellation.is_cancelled()
            });
        if !current {
            self.metrics.inc("voice_deep_stale_dropped_total");
            return;
        }
        let mut task = self
            .active_tasks
            .remove(&completion.task_id)
            .expect("checked above");
        task.cancel();
        self.deep_deadlines.remove(&completion.task_id);
        match completion.result {
            Ok(mut result)
                if result.task_id == completion.task_id
                    && result.thought_epoch == completion.thought_epoch =>
            {
                result.dependencies = task.dependencies;
                if forward {
                    let event = VoiceEvent::DeepWorkResult {
                        meta: self.new_meta(),
                        result: result.clone(),
                    };
                    self.forward_trace(&event).await;
                }
                self.deep_results.push(result);
            }
            Ok(_) => self.metrics.inc("voice_deep_stale_dropped_total"),
            Err(error) => {
                self.metrics.inc("voice_deep_work_failed_total");
                tracing::warn!("deep work failed: {error}");
            }
        }
    }

    // Session shutdown.

    async fn shutdown_active_work(&mut self) {
        // Main turns resolve exactly once: a running turn discards here, and
        // a reply that already received its Final resolves with the
        // confirmed ledger outcome below.
        let tasks: Vec<(TaskId, ActiveTask)> = self.active_tasks.drain().collect();
        for (task_id, mut task) in tasks {
            task.cancel();
            if task.kind == ActiveTaskKind::MainTurn {
                self.resolve_turn(task_id, TurnResolution::Discard);
            }
        }

        let records: Vec<(SpeechId, SpeechRecord)> = self.speech_records.drain().collect();
        for (speech_id, record) in records {
            self.tts_registry.cancel(&speech_id);
            if record.playback != PlaybackLifecycle::NotStarted {
                self.audible_ledger.mark_interrupted(
                    speech_id,
                    record.confirmed_samples,
                    InterruptionReason::ProviderError,
                );
            }
        }
        self.pending_speech.clear();
        self.act_owner.clear();
        self.deferred_turn = None;
        let turns: Vec<(TaskId, TurnSpeech)> = self.turn_speech.drain().collect();
        for (task_id, turn) in turns {
            self.resolve_heard_turn(task_id, turn);
        }
        // Tokens for acts queued but never dequeued must not outlive the
        // session either.
        self.tts_registry.cancel_all();

        self.shutdown.cancel();
        let _ = tokio::time::timeout(
            Duration::from_millis(1_000),
            self.playback.command(PlaybackCommand::Shutdown),
        )
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while !self.workers.is_empty() {
            if tokio::time::timeout_at(deadline, self.workers.join_next())
                .await
                .is_err()
            {
                self.workers.abort_all();
                while self.workers.join_next().await.is_some() {}
                break;
            }
        }

        let summary = SessionSummary {
            session_id: self.session_id,
            floor: self.state.floor.as_str().to_string(),
            task: self.state.task.as_str().to_string(),
            speech: self.state.speech.as_str().to_string(),
            transcript_revision: self.state.epochs.transcript.0,
            thought_epoch: self.state.epochs.thought,
            committed_turns: self.state.committed_user_turns.len(),
            audible_clauses: self.audible_ledger.history().len(),
            active_task_ids: Vec::new(),
            pending_speech: self.pending_speech.len(),
            committed_transcripts: self
                .state
                .committed_user_turns
                .iter()
                .map(|turn| turn.transcript.clone())
                .collect(),
            audible_texts: self
                .audible_ledger
                .history()
                .iter()
                .filter(|clause| clause.is_complete())
                .map(|clause| clause.text.clone())
                .collect(),
        };

        let event = VoiceEvent::SessionClosed {
            meta: self.new_meta(),
            summary,
        };
        self.forward_trace(&event).await;
        // Shutdown must never hang waiting on its own event queue.
        let _ = self.event_tx.try_send(event);
    }
}

fn split_clauses(text: &str) -> Vec<String> {
    let normalized = text.trim();
    if normalized.is_empty() {
        return Vec::new();
    }

    let mut clauses = Vec::new();
    let mut current = String::new();

    for ch in normalized.chars() {
        current.push(ch);
        let is_clause_end = matches!(ch, '.' | '!' | '?' | '。' | '！' | '？');
        if is_clause_end {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                clauses.push(trimmed);
            }
            current.clear();
        }
    }

    let remainder = current.trim().to_string();
    if !remainder.is_empty() {
        clauses.push(remainder);
    }

    if clauses.is_empty() {
        clauses.push(normalized.to_string());
    }

    clauses
}

/// Renders the turn environment that reaches the model through the system
/// prompt rather than the user message.
///
/// None of this is conversation: it describes the state the turn runs in, and
/// is replaced on every turn. What the user heard is not here at all; it lives
/// in the agent's memory, where resolved turns hold only audible text.
fn render_turn_context(
    turn_id: TurnId,
    revision: Revision,
    semantic_frame: &SemanticFrame,
    speculative_hints: &[(u64, String)],
    deep_results: &[DeepWorkResult],
    speech_language: &str,
) -> TurnContext {
    let idempotency_key = format!("voice-turn-{turn_id}");
    let frame_json = serde_json::to_string(semantic_frame).unwrap_or_default();

    let dependencies: Vec<String> = semantic_frame.slots.keys().cloned().collect();
    let fingerprint = semantic_frame.dependency_fingerprint(&dependencies);

    // The synthesis language is configuration, and the reply must be in it to
    // be pronounced correctly. The runtime does not detect languages; with no
    // configured language the line is omitted rather than defaulted.
    let mut brief = format!(
        "transcript_revision: {revision}\n\
         idempotency_key: {idempotency_key}\n"
    );
    if !speech_language.trim().is_empty() {
        brief.push_str(&format!("speech_language: {speech_language}\n"));
    }
    brief.push_str(&format!("semantic_frame: {frame_json}"));

    let matching_speculation: Vec<&str> = speculative_hints
        .iter()
        .filter(|(fp, _)| *fp == fingerprint)
        .map(|(_, summary)| summary.as_str())
        .collect();
    if !matching_speculation.is_empty() {
        brief.push_str("\nspeculative_results:\n");
        brief.push_str(&matching_speculation.join("\n"));
    }

    if !deep_results.is_empty() {
        let summaries: Vec<&str> = deep_results.iter().map(|r| r.summary.as_str()).collect();
        brief.push_str("\ndeep_worker_results:\n");
        brief.push_str(&summaries.join("\n"));
    }

    TurnContext {
        idempotency_key,
        brief,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestSemanticBrain {
        results: parking_lot::Mutex<VecDeque<SemanticAssessment>>,
        snapshots: parking_lot::Mutex<Vec<InteractionSnapshot>>,
    }

    #[async_trait::async_trait]
    impl InteractionBrain for TestSemanticBrain {
        async fn decide(&self, _: InteractionSnapshot) -> Option<InteractionDecisionEnvelope> {
            None
        }

        fn supports_semantics(&self) -> bool {
            true
        }

        async fn assess(&self, snapshot: InteractionSnapshot) -> Option<SemanticAssessment> {
            self.snapshots.lock().push(snapshot);
            self.results.lock().pop_front()
        }
    }

    struct BlockingSemanticBrain {
        calls: std::sync::atomic::AtomicUsize,
        release: tokio::sync::Notify,
        snapshots: parking_lot::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl InteractionBrain for BlockingSemanticBrain {
        async fn decide(&self, _: InteractionSnapshot) -> Option<InteractionDecisionEnvelope> {
            None
        }
        fn supports_semantics(&self) -> bool {
            true
        }
        async fn assess(&self, snapshot: InteractionSnapshot) -> Option<SemanticAssessment> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.snapshots.lock().push(snapshot.turn_candidate);
            if call == 0 {
                self.release.notified().await;
            }
            Some(assessment(UserSpeechState::Complete, Some(true)))
        }
    }

    struct BlockingHostExtractor;

    #[async_trait::async_trait]
    impl SlotExtractor for BlockingHostExtractor {
        async fn extract(&self, _: &SemanticFrame, _: &str, _: Revision) -> Vec<SlotUpdate> {
            std::future::pending().await
        }
    }

    struct TestHostExtractor;

    #[async_trait::async_trait]
    impl SlotExtractor for TestHostExtractor {
        async fn extract(&self, _: &SemanticFrame, _: &str, revision: Revision) -> Vec<SlotUpdate> {
            vec![SlotUpdate {
                slot: "date".into(),
                operation: crate::semantics::SlotOperation::Set,
                old_value: None,
                new_value: serde_json::json!("Saturday"),
                confidence: 0.9,
                source_revision: revision,
            }]
        }
    }

    fn semantic_brain(results: Vec<SemanticAssessment>) -> Arc<TestSemanticBrain> {
        Arc::new(TestSemanticBrain {
            results: parking_lot::Mutex::new(results.into()),
            snapshots: parking_lot::Mutex::new(Vec::new()),
        })
    }

    fn assessment(state: UserSpeechState, complete: Option<bool>) -> SemanticAssessment {
        SemanticAssessment {
            state,
            complete,
            stop: false,
            negated: false,
            intent: None,
            updates: Vec::new(),
            confidence: 0.9,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_assessment_updates_and_commits_injected_final() {
        let (mut session, _, _) = harness();
        let mut result = assessment(UserSpeechState::Complete, Some(true));
        result.intent = Some("reservation".into());
        result.updates.push(crate::interaction::SemanticUpdate {
            slot: "party_size".into(),
            operation: crate::semantics::SlotOperation::Set,
            old_value: None,
            new_value: serde_json::json!(4),
            confidence: 0.9,
        });
        let brain = semantic_brain(vec![result]);
        session.interaction_brain = brain.clone();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: "four people please".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("party_size"),
            Some(&serde_json::json!(4))
        );
        assert_eq!(
            session
                .state
                .semantic_frame
                .intent
                .as_ref()
                .map(|value| value.value.as_str()),
            Some("reservation")
        );
        tokio::time::advance(Duration::from_millis(600)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(session.state.committed_user_turns.len(), 1);
        assert_eq!(
            session.state.committed_user_turns[0].transcript,
            "four people please"
        );
        assert_eq!(brain.snapshots.lock().len(), 1);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_single_flight_uses_latest_pending_candidate() {
        use std::sync::atomic::Ordering;
        let (mut session, _, _) = harness();
        let brain = Arc::new(BlockingSemanticBrain {
            calls: std::sync::atomic::AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
            snapshots: parking_lot::Mutex::new(Vec::new()),
        });
        session.semantic_mode = true;
        session.interaction_brain = brain.clone();
        session.semantic_slot_override = false;
        for (revision, text) in [
            (1, "Friday"),
            (2, "Friday evening"),
            (3, "Saturday evening"),
        ] {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: text.into(),
                })
                .await
                .unwrap();
            session.maybe_spawn_semantic();
            tokio::task::yield_now().await;
        }
        assert_eq!(brain.calls.load(Ordering::SeqCst), 1);
        brain.release.notify_one();
        let first = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(first).await.unwrap();
        assert!(session.semantic_observation.is_none());
        tokio::time::advance(Duration::from_millis(300)).await;
        session.maybe_spawn_semantic();
        let latest = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(latest).await.unwrap();
        assert!(session.semantic_observation.is_some());
        assert_eq!(
            brain.snapshots.lock().as_slice(),
            ["Friday", "Saturday evening"]
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_timeout_is_counted_instead_of_disappearing() {
        let (mut session, _, _) = harness();
        // Never released, so every request runs into the interaction timeout.
        let brain = Arc::new(BlockingSemanticBrain {
            calls: std::sync::atomic::AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
            snapshots: parking_lot::Mutex::new(Vec::new()),
        });
        session.semantic_mode = true;
        session.interaction_brain = brain;
        session.semantic_slot_override = false;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        assert!(result.assessment.is_none());
        assert_eq!(session.metrics.counter("voice_semantic_timeout_total"), 1);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_host_slot_override_works_when_model_is_unavailable() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = true;
        session.interaction_brain = Arc::new(NoopInteractionBrain);
        session.slot_extractor = Arc::new(TestHostExtractor);
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: "request".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        assert!(session.semantic_observation.is_none());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_result_rejects_changed_text_but_accepts_identical_revision() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        let (old, _, utterance) = session.semantic_snapshot().unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(2),
                hypothesis: "Saturday".into(),
            })
            .await
            .unwrap();
        let mut stale = assessment(UserSpeechState::Complete, Some(true));
        stale.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Set,
            old_value: None,
            new_value: serde_json::json!("Friday"),
            confidence: 0.9,
        });
        session
            .apply_semantic_result(SemanticResult {
                key: old,
                revision: Revision(1),
                utterance_id: utterance,
                assessment: Some(stale),
                host_updates: None,
            })
            .await
            .unwrap();
        assert!(session.state.semantic_frame.slot_value("date").is_none());
        assert!(session.semantic_observation.is_none());
        let (same, _, utterance) = session.semantic_snapshot().unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(3),
                hypothesis: "Saturday".into(),
            })
            .await
            .unwrap();
        session
            .apply_semantic_result(SemanticResult {
                key: same,
                revision: Revision(2),
                utterance_id: utterance,
                assessment: Some(assessment(UserSpeechState::Incomplete, Some(false))),
                host_updates: None,
            })
            .await
            .unwrap();
        assert!(session.semantic_observation.is_some());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_correction_waits_for_commit_before_superseding_main_turn() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "earlier request".into(), Revision(1))
            .await
            .unwrap();
        let old_task = session.state.active_task_id.unwrap();
        let cancellation = session.active_tasks[&old_task].cancellation.clone();
        let old_epoch = session.state.epochs.thought;
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(2),
                hypothesis: "Saturday instead".into(),
            })
            .await
            .unwrap();
        session.state.last_user_audio_at_us = Some(session.monotonic_us());
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut correction = assessment(UserSpeechState::Correction, Some(true));
        correction.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Replace,
            old_value: Some(serde_json::json!("Friday")),
            new_value: serde_json::json!("Saturday"),
            confidence: 0.9,
        });
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(2),
                utterance_id,
                assessment: Some(correction),
                host_updates: None,
            })
            .await
            .unwrap();
        assert!(!cancellation.is_cancelled());
        assert_eq!(session.state.epochs.thought, old_epoch);
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Friday"))
        );
        tokio::time::advance(Duration::from_millis(600)).await;
        session.on_control_tick().await.unwrap();
        assert!(cancellation.is_cancelled());
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        assert_eq!(session.state.committed_user_turns.len(), 1);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_final_correction_survives_audio_reset() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "earlier request".into(), Revision(1))
            .await
            .unwrap();
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "Saturday instead".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut correction = assessment(UserSpeechState::Correction, Some(true));
        correction.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Replace,
            old_value: Some(serde_json::json!("Friday")),
            new_value: serde_json::json!("Saturday"),
            confidence: 0.9,
        });
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(correction),
                host_updates: None,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrStreamReset {
                meta: session.new_meta(),
                generation: 1,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1_200)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        assert_eq!(
            session.state.committed_user_turns[0].transcript,
            "Saturday instead"
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_pending_correction_survives_matching_final_and_timeout() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "earlier request".into(), Revision(1))
            .await
            .unwrap();
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(2),
                hypothesis: "Saturday".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut correction = assessment(UserSpeechState::Correction, Some(true));
        correction.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Replace,
            old_value: Some(serde_json::json!("Friday")),
            new_value: serde_json::json!("Saturday"),
            confidence: 0.9,
        });
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(2),
                utterance_id,
                assessment: Some(correction),
                host_updates: None,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "Saturday".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(2),
                utterance_id,
                assessment: None,
                host_updates: None,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1_200)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        assert_eq!(session.state.committed_user_turns[0].transcript, "Saturday");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_host_update_rejects_in_flight_result() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        session
            .apply_slot_updates(vec![slot("host value")], None)
            .await
            .unwrap();
        let mut old = assessment(UserSpeechState::Complete, Some(true));
        old.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Set,
            old_value: None,
            new_value: serde_json::json!("Friday"),
            confidence: 0.9,
        });
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(old),
                host_updates: None,
            })
            .await
            .unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("host value"))
        );
        assert!(session.semantic_observation.is_none());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_final_only_audio_is_analyzed_without_a_partial() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        let mut result = assessment(UserSpeechState::Complete, Some(true));
        result.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Set,
            old_value: None,
            new_value: serde_json::json!("Saturday"),
            confidence: 0.9,
        });
        session.interaction_brain = semantic_brain(vec![result]);
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "Saturday please".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        tokio::time::advance(Duration::from_millis(600)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(session.state.committed_user_turns.len(), 1);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_result_from_before_speech_end_is_discarded() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(assessment(UserSpeechState::Complete, Some(true))),
                host_updates: None,
            })
            .await
            .unwrap();
        assert!(session.semantic_observation.is_none());
        assert!(session.state.committed_user_turns.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_backchannel_final_is_not_recommitted_by_late_audio() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session.interaction_brain =
            semantic_brain(vec![assessment(UserSpeechState::Backchannel, Some(true))]);
        track(&mut session, false, None);
        session.state.floor = crate::interaction::FloorState::OverlapProbe;
        session.state.last_user_audio_at_us = Some(session.monotonic_us());
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "yes".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.turn_buffer.is_empty());
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "yes".into(),
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1_500)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.state.committed_user_turns.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_multiple_audio_finals_are_discarded_as_one_backchannel() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session.interaction_brain =
            semantic_brain(vec![assessment(UserSpeechState::Backchannel, Some(true))]);
        track(&mut session, false, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "yes".into(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 1,
                source: AsrSource::Audio,
                transcript: "uh huh".into(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.turn_buffer.is_empty());
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "yes".into(),
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1_200)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.state.committed_user_turns.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_injected_backchannel_is_not_committed() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session.interaction_brain =
            semantic_brain(vec![assessment(UserSpeechState::Backchannel, Some(true))]);
        track(&mut session, false, None);
        session.state.floor = crate::interaction::FloorState::OverlapProbe;
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: "yes".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.turn_buffer.is_empty());
        tokio::time::advance(Duration::from_millis(1_500)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.state.committed_user_turns.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_heard_context_is_stable_while_the_user_speaks() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        let id = track(&mut session, false, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "stop".into(),
            })
            .await
            .unwrap();
        let (before, _, _) = session.semantic_snapshot().unwrap();
        session.audible_ledger.mark_progress(id, 12_000);
        let (after, _, _) = session.semantic_snapshot().unwrap();
        assert!(
            before == after,
            "playback progress must not invalidate an in-flight stop"
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_overlap_waits_for_a_fresh_assessment_after_old_work() {
        let (mut session, _, sink) = harness();
        session.semantic_mode = true;
        track(&mut session, false, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "yes".into(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(900)).await;
        session
            .on_semantic_tick(session.monotonic_us())
            .await
            .unwrap();
        assert!(
            !sink
                .0
                .lock()
                .iter()
                .any(|command| matches!(command, PlaybackCommand::Abort { .. }))
        );
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(assessment(UserSpeechState::Backchannel, Some(true))),
                host_updates: None,
            })
            .await
            .unwrap();
        session
            .on_semantic_tick(session.monotonic_us())
            .await
            .unwrap();
        assert!(
            sink.0
                .lock()
                .iter()
                .any(|command| matches!(command, PlaybackCommand::Resume { .. }))
        );
        assert!(
            !sink
                .0
                .lock()
                .iter()
                .any(|command| matches!(command, PlaybackCommand::Abort { .. }))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_heard_context_is_anchored_at_speech_end() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        let id = track(&mut session, false, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "yes".into(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        let (before, _, _) = session.semantic_snapshot().unwrap();
        session.audible_ledger.mark_progress(id, 12_000);
        let (after, _, _) = session.semantic_snapshot().unwrap();
        assert!(
            before == after,
            "later playback must not invalidate the speech-end context"
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_changed_final_rolls_back_partial_slots_before_fallback() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut old = assessment(UserSpeechState::Incomplete, Some(false));
        old.intent = Some("reservation".into());
        old.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Set,
            old_value: None,
            new_value: serde_json::json!("Friday"),
            confidence: 0.9,
        });
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(old),
                host_updates: None,
            })
            .await
            .unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Friday"))
        );
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "Saturday".into(),
            })
            .await
            .unwrap();
        assert!(session.state.semantic_frame.slot_value("date").is_none());
        assert!(session.state.semantic_frame.intent.is_none());
        tokio::time::advance(Duration::from_millis(1_200)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(session.state.committed_user_turns[0].transcript, "Saturday");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_long_input_still_commits_on_hard_silence() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        let text = "x".repeat(8_193);
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: text.clone(),
            })
            .await
            .unwrap();
        assert!(session.semantic_snapshot().is_none());
        tokio::time::advance(Duration::from_millis(1_200)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(session.state.committed_user_turns[0].transcript, text);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_empty_overlap_resumes_after_silence() {
        let (mut session, _, sink) = harness();
        session.semantic_mode = true;
        track(&mut session, false, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        assert!(
            sink.0
                .lock()
                .iter()
                .any(|command| matches!(command, PlaybackCommand::Resume { .. }))
        );
        assert!(
            !sink
                .0
                .lock()
                .iter()
                .any(|command| matches!(command, PlaybackCommand::Abort { .. }))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_new_utterance_with_same_text_rejects_old_result() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "yes".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 1,
                revision: Revision(2),
                hypothesis: "yes".into(),
            })
            .await
            .unwrap();
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(assessment(UserSpeechState::Complete, Some(true))),
                host_updates: None,
            })
            .await
            .unwrap();
        assert!(session.semantic_observation.is_none());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_rewritten_stable_prefix_is_not_used_for_speculation() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.speculative_agent = Some(Arc::new(crate::agent::mock::FakeAgent::finalizing(
            "old result",
        )));
        Arc::make_mut(&mut session.config)
            .agents
            .speculative
            .enabled = true;
        let mut partials = session.config.asr.partials.clone();
        partials.stable_after_ms = 0;
        partials.stable_revisions = 2;
        session.transcript = TranscriptReconciler::new(partials);
        for (revision, text) in [(1, "Friday"), (2, "Friday"), (3, "Saturday")] {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: text.into(),
                })
                .await
                .unwrap();
        }
        assert_eq!(session.transcript.stable_prefix(), "Friday");
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut result = assessment(UserSpeechState::Incomplete, Some(false));
        result.intent = Some("search".into());
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(3),
                utterance_id,
                assessment: Some(result),
                host_updates: None,
            })
            .await
            .unwrap();
        assert!(
            !session
                .active_tasks
                .values()
                .any(|task| task.kind == ActiveTaskKind::SpeculativeRead)
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_revised_partial_cancels_speculation_from_old_text() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session.speculative_agent = Some(Arc::new(crate::agent::mock::FakeAgent::finalizing(
            "old result",
        )));
        Arc::make_mut(&mut session.config)
            .agents
            .speculative
            .enabled = true;
        let mut partials = session.config.asr.partials.clone();
        partials.stable_after_ms = 0;
        partials.stable_revisions = 2;
        session.transcript = TranscriptReconciler::new(partials);
        for revision in [1, 2] {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: "Friday".into(),
                })
                .await
                .unwrap();
        }
        assert_eq!(session.transcript.stable_prefix(), "Friday");
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut result = assessment(UserSpeechState::Incomplete, Some(false));
        result.intent = Some("search".into());
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(2),
                utterance_id,
                assessment: Some(result),
                host_updates: None,
            })
            .await
            .unwrap();
        let (id, token) = session
            .active_tasks
            .iter()
            .find(|(_, task)| task.kind == ActiveTaskKind::SpeculativeRead)
            .map(|(id, task)| (*id, task.cancellation.clone()))
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(3),
                hypothesis: "Saturday".into(),
            })
            .await
            .unwrap();
        assert!(token.is_cancelled());
        assert!(!session.active_tasks.contains_key(&id));
        assert!(session.state.semantic_frame.intent.is_none());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_stop_cancels_a_reasoning_turn_before_it_speaks() {
        let (mut session, recorder, sink) = harness();
        session
            .start_main_task(TurnId::new(), "earlier request".into(), Revision(1))
            .await
            .unwrap();
        let old_task = session.state.active_task_id.unwrap();
        let cancellation = session.active_tasks[&old_task].cancellation.clone();
        session.semantic_mode = true;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(2),
                hypothesis: "stop".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut stop = assessment(UserSpeechState::Interruption, Some(true));
        stop.stop = true;
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(2),
                utterance_id,
                assessment: Some(stop),
                host_updates: None,
            })
            .await
            .unwrap();
        assert!(cancellation.is_cancelled());
        assert!(!session.active_tasks.contains_key(&old_task));
        assert!(sink.0.lock().iter().any(|command| matches!(
            command,
            PlaybackCommand::Abort {
                reason: InterruptionReason::HardStop,
                ..
            }
        )));
        session.shutdown_active_work().await;
        assert_eq!(
            recorder
                .resolutions()
                .iter()
                .filter(|(id, _)| *id == old_task)
                .count(),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_stop_survives_speech_end_while_host_extractor_is_blocked() {
        let (mut session, _, sink) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = true;
        session.slot_extractor = Arc::new(BlockingHostExtractor);
        let mut stop = assessment(UserSpeechState::Interruption, Some(true));
        stop.stop = true;
        session.interaction_brain = semantic_brain(vec![stop]);
        track(&mut session, true, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "stop".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let key = session.semantic_stop_rx.recv().await.unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        session.apply_semantic_stop(&key).await.unwrap();
        assert!(session.semantic_in_flight);
        assert!(sink.0.lock().iter().any(|command| matches!(
            command,
            PlaybackCommand::Abort {
                reason: InterruptionReason::HardStop,
                ..
            }
        )));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_stop_does_not_wait_for_host_extractor() {
        let (mut session, _, sink) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = true;
        session.slot_extractor = Arc::new(BlockingHostExtractor);
        let mut stop = assessment(UserSpeechState::Interruption, Some(true));
        stop.stop = true;
        session.interaction_brain = semantic_brain(vec![stop]);
        track(&mut session, true, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "stop".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let key = session.semantic_stop_rx.recv().await.unwrap();
        session.apply_semantic_stop(&key).await.unwrap();
        assert!(session.semantic_in_flight);
        assert!(sink.0.lock().iter().any(|command| matches!(
            command,
            PlaybackCommand::Abort {
                reason: InterruptionReason::HardStop,
                ..
            }
        )));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_repeated_stop_does_not_enqueue_duplicate_acknowledgements() {
        let (mut session, _, sink) = harness();
        session.semantic_mode = true;
        track(&mut session, true, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "stop".into(),
            })
            .await
            .unwrap();
        for _ in 0..2 {
            let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
            let mut stop = assessment(UserSpeechState::Interruption, Some(true));
            stop.stop = true;
            session
                .apply_semantic_result(SemanticResult {
                    key,
                    revision: Revision(1),
                    utterance_id,
                    assessment: Some(stop),
                    host_updates: None,
                })
                .await
                .unwrap();
        }
        assert_eq!(
            sink.0
                .lock()
                .iter()
                .filter(|command| matches!(command, PlaybackCommand::Abort { .. }))
                .count(),
            1
        );
        assert_eq!(
            session
                .pending_speech
                .iter()
                .filter(|act| act.priority == crate::speech::act::PRIORITY_HARD_STOP_ACK)
                .count(),
            1
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_stop_wins_over_invalid_host_updates() {
        let (mut session, _, sink) = harness();
        session.semantic_mode = true;
        track(&mut session, true, None);
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "stop".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut stop = assessment(UserSpeechState::Interruption, Some(true));
        stop.stop = true;
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(stop),
                host_updates: Some(vec![slot("A"), slot("B")]),
            })
            .await
            .unwrap();
        assert!(sink.0.lock().iter().any(|command| matches!(
            command,
            PlaybackCommand::Abort {
                reason: InterruptionReason::HardStop,
                ..
            }
        )));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_overlap_correction_survives_abort_and_fallback() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        session.semantic_mode = true;
        track(&mut session, false, None);
        session.state.floor = crate::interaction::FloorState::OverlapProbe;
        session.state.last_user_audio_at_us = Some(session.monotonic_us());
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "Saturday instead".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut correction = assessment(UserSpeechState::Correction, Some(true));
        correction.updates.push(crate::interaction::SemanticUpdate {
            slot: "date".into(),
            operation: crate::semantics::SlotOperation::Replace,
            old_value: Some(serde_json::json!("Friday")),
            new_value: serde_json::json!("Saturday"),
            confidence: 0.9,
        });
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(1),
                utterance_id,
                assessment: Some(correction),
                host_updates: None,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        assert_eq!(
            session.state.committed_user_turns[0].transcript,
            "Saturday instead"
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_late_audio_after_empty_overlap_is_not_committed_as_backchannel() {
        let (mut session, _, sink) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session.interaction_brain =
            semantic_brain(vec![assessment(UserSpeechState::Backchannel, Some(true))]);
        track(&mut session, false, None);
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        assert!(
            sink.0
                .lock()
                .iter()
                .any(|command| matches!(command, PlaybackCommand::Resume { .. }))
        );
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "yes".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.turn_buffer.is_empty());
        assert!(session.state.committed_user_turns.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_snapshot_contains_heard_text_only() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        let heard_id = SpeechId::new();
        session
            .audible_ledger
            .speech_started(heard_id, "Which day?", 24_000);
        session.audible_ledger.mark_completed(heard_id);
        let live_id = track(&mut session, false, None);
        session.audible_ledger.mark_progress(live_id, 0);
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        let (_, snapshot, _) = session.semantic_snapshot().unwrap();
        assert!(snapshot.heard_assistant.contains("Which day?"));
        assert!(!snapshot.heard_assistant.contains("추가 확인을 진행합니다"));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_injection_survives_unrelated_audio_reset() {
        let (mut session, _, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: "do not book Friday".into(),
            })
            .await
            .unwrap();
        let (key, _, utterance_id) = session.semantic_snapshot().unwrap();
        let mut result = assessment(UserSpeechState::Complete, Some(true));
        result.negated = true;
        result.intent = Some("reservation".into());
        session
            .apply_semantic_result(SemanticResult {
                key,
                revision: Revision(0),
                utterance_id,
                assessment: Some(result),
                host_updates: None,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrStreamReset {
                meta: session.new_meta(),
                generation: 1,
            })
            .await
            .unwrap();
        assert!(session.state.semantic_frame.negated);
        assert_eq!(
            session
                .state
                .semantic_frame
                .intent
                .as_ref()
                .map(|v| v.value.as_str()),
            Some("reservation")
        );
        assert!(session.semantic_observation.is_some());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_negation_reaches_only_the_current_turn_context() {
        let (mut session, recorder, _) = harness();
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        let mut result = assessment(UserSpeechState::Complete, Some(true));
        result.negated = true;
        session.interaction_brain = semantic_brain(vec![result]);
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: "not Friday".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        tokio::time::advance(Duration::from_millis(600)).await;
        session.on_control_tick().await.unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let requests = recorder.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].context.brief.contains("\"negated\":true"));
        assert!(!session.state.semantic_frame.negated);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn semantic_incomplete_waits_but_hard_silence_still_recovers() {
        let (mut session, _, _) = harness();
        session.interaction_brain =
            semantic_brain(vec![assessment(UserSpeechState::Incomplete, Some(false))]);
        session.semantic_mode = true;
        session.semantic_slot_override = false;
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: "I was thinking".into(),
            })
            .await
            .unwrap();
        session.maybe_spawn_semantic();
        let result = session.semantic_rx.recv().await.unwrap();
        session.apply_semantic_result(result).await.unwrap();
        tokio::time::advance(Duration::from_millis(600)).await;
        session.on_control_tick().await.unwrap();
        assert!(session.state.committed_user_turns.is_empty());
        tokio::time::advance(Duration::from_millis(600)).await;
        session.on_control_tick().await.unwrap();
        assert_eq!(session.state.committed_user_turns.len(), 1);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn interaction_commit_does_not_consume_active_speech() {
        let (mut session, _, _) = harness();
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        let envelope = InteractionDecisionEnvelope {
            revision: session.state.epochs.transcript,
            interaction_epoch: session.state.epochs.interaction,
            decision: crate::interaction::InteractionDecision::new(
                UserSpeechState::Complete,
                InteractionAction::CommitUserTurn,
                0.9,
                "semantic_test",
            ),
        };
        session.apply_floor_decision(envelope).await.unwrap();
        assert!(session.state.committed_user_turns.is_empty());
        assert_eq!(session.transcript.current_text(), "Friday");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn interaction_decision_from_before_speech_end_is_stale() {
        let (mut session, _, _) = harness();
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "Friday".into(),
            })
            .await
            .unwrap();
        let envelope = InteractionDecisionEnvelope {
            revision: session.state.epochs.transcript,
            interaction_epoch: session.state.epochs.interaction,
            decision: crate::interaction::InteractionDecision::new(
                UserSpeechState::Complete,
                InteractionAction::CommitUserTurn,
                0.9,
                "semantic_test",
            ),
        };
        let activity = session.semantic_activity;
        session
            .handle_event(VoiceEvent::LocalSpeechEnded {
                meta: session.new_meta(),
                duration_ms: 100,
            })
            .await
            .unwrap();
        session
            .apply_legacy_interaction(envelope, activity)
            .await
            .unwrap();
        assert!(session.state.committed_user_turns.is_empty());
        assert_eq!(session.transcript.current_text(), "Friday");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn older_slot_result_cannot_overwrite_a_newer_candidate_value() {
        let (mut session, _, _) = harness();
        let provenance = SlotProvenance {
            candidate_generation: session.candidate_generation,
            source_utterance_id: 0,
            source: AsrSource::Audio,
        };
        let update = |revision, value: &str| SlotUpdate {
            slot: "date".into(),
            operation: crate::semantics::frame::SlotOperation::Set,
            old_value: None,
            new_value: serde_json::json!(value),
            confidence: 0.9,
            source_revision: Revision(revision),
        };
        finish_extraction(&mut session, provenance.clone(), 2, update(2, "Saturday")).await;
        finish_extraction(&mut session, provenance, 1, update(1, "Friday")).await;
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn older_utterance_cannot_overwrite_a_newer_candidate_value() {
        let (mut session, _, _) = harness();
        let older = SlotProvenance {
            candidate_generation: session.candidate_generation,
            source_utterance_id: 0,
            source: AsrSource::Audio,
        };
        let mut newer = older.clone();
        newer.source_utterance_id = 1;
        let update = |revision, value: &str| SlotUpdate {
            slot: "date".into(),
            operation: crate::semantics::frame::SlotOperation::Set,
            old_value: None,
            new_value: serde_json::json!(value),
            confidence: 0.9,
            source_revision: Revision(revision),
        };
        finish_extraction(&mut session, newer, 2, update(2, "Saturday")).await;
        finish_extraction(&mut session, older, 1, update(1, "Friday")).await;
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn older_slot_result_cannot_restore_a_cleared_candidate_value() {
        let (mut session, _, _) = harness();
        let provenance = SlotProvenance {
            candidate_generation: session.candidate_generation,
            source_utterance_id: 0,
            source: AsrSource::Audio,
        };
        let update = |revision, operation, value| SlotUpdate {
            slot: "date".into(),
            operation,
            old_value: None,
            new_value: value,
            confidence: 0.9,
            source_revision: Revision(revision),
        };
        finish_extraction(
            &mut session,
            provenance.clone(),
            2,
            update(
                2,
                crate::semantics::frame::SlotOperation::Clear,
                serde_json::Value::Null,
            ),
        )
        .await;
        finish_extraction(
            &mut session,
            provenance,
            1,
            update(
                1,
                crate::semantics::frame::SlotOperation::Set,
                serde_json::json!("Friday"),
            ),
        )
        .await;
        assert_eq!(session.state.semantic_frame.slot_value("date"), None);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn clearing_a_slot_invalidates_its_dependent_task() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        let date_task = TaskId::new();
        let other_task = TaskId::new();
        let date_cancellation = CancellationToken::new();
        let other_cancellation = CancellationToken::new();
        for (id, dependency, cancellation) in [
            (date_task, "date", date_cancellation.clone()),
            (other_task, "user_id", other_cancellation.clone()),
        ] {
            session.active_tasks.insert(
                id,
                ActiveTask {
                    id,
                    kind: ActiveTaskKind::SpeculativeRead,
                    thought_epoch: session.state.epochs.thought,
                    dependencies: vec![dependency.into()],
                    fingerprint: 0,
                    cancellation,
                    handle: None,
                    candidate_generation: session.candidate_generation,
                    candidate_utterance_id: Some(0),
                },
            );
        }
        let mut clear = slot("unused");
        clear.operation = crate::semantics::frame::SlotOperation::Clear;
        clear.new_value = serde_json::Value::Null;
        let provenance = candidate(&session, 0);
        session
            .apply_slot_updates(vec![clear], Some(provenance))
            .await
            .unwrap();
        assert_eq!(session.state.semantic_frame.slot_value("date"), None);
        assert!(date_cancellation.is_cancelled());
        assert!(!other_cancellation.is_cancelled());
        assert!(!session.active_tasks.contains_key(&date_task));
        assert!(session.active_tasks.contains_key(&other_task));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn extraction_order_distinguishes_requests_with_the_same_revision() {
        let (mut session, _, _) = harness();
        let provenance = candidate(&session, 0);
        finish_extraction(&mut session, provenance.clone(), 2, slot("latest")).await;
        finish_extraction(&mut session, provenance, 1, slot("older")).await;
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("latest"))
        );
        session.shutdown_active_work().await;
    }

    struct DelayedSlotExtractor {
        calls: parking_lot::Mutex<Vec<String>>,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl SlotExtractor for DelayedSlotExtractor {
        async fn extract(
            &self,
            _: &SemanticFrame,
            text: &str,
            revision: Revision,
        ) -> Vec<SlotUpdate> {
            let first = {
                let mut calls = self.calls.lock();
                calls.push(text.to_string());
                calls.len() == 1
            };
            if first {
                self.release.notified().await;
            }
            let mut update = slot(text);
            update.source_revision = revision;
            vec![update]
        }
    }

    #[tokio::test(start_paused = true)]
    async fn legacy_partial_extraction_coalesces_the_latest_request() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor.clone();
        for (revision, hypothesis) in [(1, "아니 first"), (2, "아니 middle"), (3, "아니 latest")]
        {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: hypothesis.into(),
                })
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;
        assert_eq!(extractor.calls.lock().as_slice(), &["아니 first"]);
        extractor.release.notify_one();
        let first = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(first).await.unwrap();
        let latest = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(latest).await.unwrap();
        assert_eq!(
            extractor.calls.lock().as_slice(),
            &["아니 first", "아니 latest"]
        );
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("아니 latest"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_legacy_extraction_cannot_consume_its_successor() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor.clone();
        session
            .handle_event(VoiceEvent::LocalSpeechStarted {
                meta: session.new_meta(),
                vad_probability: 1.0,
            })
            .await
            .unwrap();
        for (revision, hypothesis) in [(1, "아니 first"), (2, "아니 latest")] {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: hypothesis.into(),
                })
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(
            session.config.agents.interaction.timeout_ms + 1,
        ))
        .await;
        session.on_control_tick().await.unwrap();
        let expected = session.slot_extraction_running.unwrap().0;
        loop {
            let result = tokio::time::timeout(
                Duration::from_millis(100),
                session.slot_extraction_rx.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            let is_latest = result.sequence == expected;
            session.finish_slot_extraction(result).await.unwrap();
            if is_latest {
                break;
            }
        }
        assert_eq!(
            extractor.calls.lock().as_slice(),
            &["아니 first", "아니 latest"]
        );
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("아니 latest"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn host_write_blocks_a_previously_started_legacy_extraction() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor.clone();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "아니 old".into(),
            })
            .await
            .unwrap();
        tokio::task::yield_now().await;
        session
            .handle_event(VoiceEvent::SemanticFrameUpdated {
                meta: session.new_meta(),
                revision: Revision(1),
                updates: vec![slot("host")],
                provenance: None,
            })
            .await
            .unwrap();
        extractor.release.notify_one();
        let old = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(old).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("host"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn revised_partial_cannot_be_overwritten_by_an_old_legacy_extraction() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor.clone();
        for (revision, hypothesis) in [(1, "아니 금요일"), (2, "토요일")] {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: hypothesis.into(),
                })
                .await
                .unwrap();
        }
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("토요일"))
        );
        extractor.release.notify_one();
        let old = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(old).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("토요일"))
        );
        let latest = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(latest).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("토요일"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn retired_deep_result_cannot_be_restored_through_the_public_event() {
        let (mut session, _, _) = harness();
        let result = DeepWorkResult {
            task_id: TaskId::new(),
            thought_epoch: session.state.epochs.thought,
            summary: "retired".into(),
            dependencies: vec!["date".into()],
        };
        session
            .handle_event(VoiceEvent::DeepWorkResult {
                meta: session.new_meta(),
                result: result.clone(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::DeepWorkResult {
                meta: session.new_meta(),
                result,
            })
            .await
            .unwrap();
        assert!(session.deep_results.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn final_replaces_an_inflight_partial_extraction_before_commit() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor.clone();
        for (revision, text) in [(1, "아니 금요일"), (2, "아니 금요일 말고 토요일")]
        {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: text.into(),
                })
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "아니 금요일 말고 토요일".into(),
            })
            .await
            .unwrap();
        session.commit_user_turn("test").await.unwrap();
        assert!(session.state.committed_user_turns.is_empty());
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            session.slot_extraction_rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        session.finish_slot_extraction(result).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("아니 금요일 말고 토요일"))
        );
        session.commit_user_turn("test").await.unwrap();
        assert_eq!(session.state.committed_user_turns.len(), 1);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn successive_legacy_finals_are_extracted_as_the_assembled_turn() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor;
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "아니 first".into(),
            })
            .await
            .unwrap();
        tokio::task::yield_now().await;
        for (id, text) in [(0, "아니 first"), (1, "second")] {
            session
                .handle_event(VoiceEvent::AsrUtteranceFinal {
                    meta: session.new_meta(),
                    utterance_id: id,
                    source: AsrSource::Audio,
                    transcript: text.into(),
                })
                .await
                .unwrap();
        }
        let expected = session.slot_extraction_running.unwrap().0;
        loop {
            let result = tokio::time::timeout(
                Duration::from_millis(100),
                session.slot_extraction_rx.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            let latest = result.sequence == expected;
            session.finish_slot_extraction(result).await.unwrap();
            if latest {
                break;
            }
        }
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("아니 first second"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn final_extractor_compares_against_the_authoritative_value_not_its_cue() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("금요일")], None)
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "토요일".into(),
            })
            .await
            .unwrap();
        session.spawn_slot_extraction("토요일".into(), Revision(1));
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "토요일".into(),
            })
            .await
            .unwrap();
        let expected = session.slot_extraction_running.unwrap().0;
        loop {
            let result = tokio::time::timeout(
                Duration::from_millis(100),
                session.slot_extraction_rx.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            let latest = result.sequence == expected;
            session.finish_slot_extraction(result).await.unwrap();
            if latest {
                break;
            }
        }
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("토요일"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn legacy_final_restores_last_accepted_value_not_a_later_cue() {
        let (mut session, _, _) = harness();
        let provenance = candidate(&session, 0);
        session
            .apply_slot_updates(vec![slot("Saturday")], Some(provenance.clone()))
            .await
            .unwrap();
        session
            .apply_slot_updates_inner(vec![slot("Friday")], Some(provenance), false)
            .await
            .unwrap();
        session.rollback_unverified_slots(0);
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("Saturday"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reset_with_same_text_queues_a_new_legacy_extraction() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor.clone();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "same".into(),
            })
            .await
            .unwrap();
        session.spawn_slot_extraction("same".into(), Revision(1));
        tokio::task::yield_now().await;
        session.on_asr_stream_reset().await.unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 1,
                revision: Revision(2),
                hypothesis: "same".into(),
            })
            .await
            .unwrap();
        assert!(session.slot_extraction_pending.is_some());
        extractor.release.notify_one();
        let old = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(old).await.unwrap();
        let latest = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(latest).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("same"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn unrelated_host_write_does_not_discard_pending_extraction() {
        let (mut session, _, _) = harness();
        let extractor = Arc::new(DelayedSlotExtractor {
            calls: parking_lot::Mutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
        });
        session.slot_extractor = extractor.clone();
        for (revision, text) in [(1, "아니 first"), (2, "아니 latest")] {
            session
                .handle_event(VoiceEvent::AsrPartial {
                    meta: session.new_meta(),
                    utterance_id: 0,
                    revision: Revision(revision),
                    hypothesis: text.into(),
                })
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;
        let mut host = slot("profile");
        host.slot = "user_id".into();
        session
            .handle_event(VoiceEvent::SemanticFrameUpdated {
                meta: session.new_meta(),
                revision: Revision(2),
                updates: vec![host],
                provenance: None,
            })
            .await
            .unwrap();
        extractor.release.notify_one();
        let old = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(old).await.unwrap();
        let latest = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(latest).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("아니 latest"))
        );
        assert_eq!(
            session.state.semantic_frame.slot_value("user_id"),
            Some(&serde_json::json!("profile"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn provisional_cue_does_not_cancel_a_main_turn_when_verified_value_is_unchanged() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("토요일")], None)
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "request".into(), Revision(1))
            .await
            .unwrap();
        let task = session.state.active_task_id.unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "아니 금요일 말고 토요일".into(),
            })
            .await
            .unwrap();
        assert!(session.active_tasks.contains_key(&task));
        let completion = session.slot_extraction_rx.recv().await.unwrap();
        session.finish_slot_extraction(completion).await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("토요일"))
        );
        assert!(session.active_tasks.contains_key(&task));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn setting_a_previously_absent_slot_retires_dependent_deep_work() {
        let (mut session, _, _) = harness();
        session.deep_worker = Some(Arc::new(crate::session::deep_worker::FakeDeepWorker {
            summary: "old".into(),
            delay_ms: 500,
        }));
        session
            .request_deep_work("date", vec!["date".into()])
            .await
            .unwrap();
        let id = *session
            .active_tasks
            .iter()
            .find(|(_, task)| task.kind == ActiveTaskKind::DeepWorker)
            .unwrap()
            .0;
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        assert!(!session.active_tasks.contains_key(&id));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn turn_resolution_waits_for_the_worker_to_finish_registering_memory() {
        let (mut session, recorder, _) = harness();
        let task = TaskId::new();
        let (done_tx, done_rx) = watch::channel(false);
        session.main_worker_done.insert(task, done_rx);
        session.resolve_turn(task, TurnResolution::Discard);
        tokio::task::yield_now().await;
        assert!(recorder.resolutions().is_empty());
        done_tx.send(true).unwrap();
        tokio::task::yield_now().await;
        assert_eq!(recorder.resolutions().len(), 1);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn registered_public_deep_result_is_consumed_once_with_request_dependencies() {
        let (mut session, _, _) = harness();
        session.deep_worker = Some(Arc::new(crate::session::deep_worker::FakeDeepWorker {
            summary: "worker".into(),
            delay_ms: 10_000,
        }));
        session
            .request_deep_work("profile", vec!["user_id".into()])
            .await
            .unwrap();
        let (id, task) = session
            .active_tasks
            .iter()
            .find(|(_, task)| task.kind == ActiveTaskKind::DeepWorker)
            .unwrap();
        let result = DeepWorkResult {
            task_id: *id,
            thought_epoch: task.thought_epoch,
            summary: "host".into(),
            dependencies: vec!["forged".into()],
        };
        session
            .handle_event(VoiceEvent::DeepWorkResult {
                meta: session.new_meta(),
                result: result.clone(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::DeepWorkResult {
                meta: session.new_meta(),
                result,
            })
            .await
            .unwrap();
        assert_eq!(session.deep_results.len(), 1);
        assert_eq!(session.deep_results[0].dependencies, ["user_id"]);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn unrelated_main_turn_accepts_its_original_epoch_after_a_correction() {
        let (mut session, recorder, _) = harness();
        // A read-only lookup, so the reply is not held back as an unconfirmed write.
        Arc::make_mut(&mut session.config)
            .tools
            .effects
            .insert("profile".into(), ToolEffect::PureRead);
        let mut profile = slot("user");
        profile.slot = "user_id".into();
        session
            .apply_slot_updates(vec![profile], None)
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "profile request".into(), Revision(1))
            .await
            .unwrap();
        let task = session.state.active_task_id.unwrap();
        let epoch = session.active_tasks[&task].thought_epoch;
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        let mut clear = slot("unused");
        clear.operation = crate::semantics::frame::SlotOperation::Clear;
        clear.new_value = serde_json::Value::Null;
        session.apply_slot_updates(vec![clear], None).await.unwrap();
        assert!(session.state.epochs.thought > epoch);
        assert!(session.is_task_result_current(task, epoch));
        session
            .handle_event(VoiceEvent::ToolStarted {
                meta: session.new_meta(),
                task_id: task,
                thought_epoch: epoch,
                tool: "profile".into(),
                args: serde_json::Value::Null,
            })
            .await
            .unwrap();
        assert!(
            session
                .state
                .evidence
                .contains_key(&format!("tool_started:{task}:profile"))
        );
        session
            .handle_event(VoiceEvent::AgentFinal {
                meta: session.new_meta(),
                task_id: task,
                thought_epoch: epoch,
                text: "Profile found.".into(),
            })
            .await
            .unwrap();
        assert!(session.turn_speech.contains_key(&task));
        session.shutdown_active_work().await;
        assert_eq!(
            recorder
                .resolutions()
                .iter()
                .filter(|(id, _)| *id == task)
                .count(),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn related_main_turn_is_cancelled_once_and_wrong_epoch_cannot_consume_it() {
        let (mut session, recorder, _) = harness();
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "date request".into(), Revision(1))
            .await
            .unwrap();
        let task = session.state.active_task_id.unwrap();
        let epoch = session.active_tasks[&task].thought_epoch;
        session
            .on_agent_failed(task, epoch + 1, "wrong epoch".into())
            .await
            .unwrap();
        assert!(session.active_tasks.contains_key(&task));
        let mut clear = slot("unused");
        clear.operation = crate::semantics::frame::SlotOperation::Clear;
        clear.new_value = serde_json::Value::Null;
        session.apply_slot_updates(vec![clear], None).await.unwrap();
        assert!(!session.active_tasks.contains_key(&task));
        assert_eq!(session.state.active_task_id, None);
        session
            .on_agent_final(task, epoch, "late".into())
            .await
            .unwrap();
        session
            .on_agent_failed(task, epoch, "late".into())
            .await
            .unwrap();
        session.shutdown_active_work().await;
        assert_eq!(
            recorder
                .resolutions()
                .iter()
                .filter(|(id, _)| *id == task)
                .count(),
            1
        );
        assert!(matches!(
            &recorder.resolutions()[0].1,
            TurnResolution::Discard
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn correction_retires_only_related_deep_work_and_stored_results() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("Friday")], None)
            .await
            .unwrap();
        session.deep_worker = Some(Arc::new(crate::session::deep_worker::FakeDeepWorker {
            summary: "deep result".into(),
            delay_ms: 10,
        }));
        session
            .request_deep_work("date", vec!["date".into()])
            .await
            .unwrap();
        session
            .request_deep_work("profile", vec!["user_id".into()])
            .await
            .unwrap();
        let date_id = *session
            .active_tasks
            .iter()
            .find(|(_, t)| t.kind == ActiveTaskKind::DeepWorker && t.dependencies == ["date"])
            .unwrap()
            .0;
        let other_id = *session
            .active_tasks
            .iter()
            .find(|(_, t)| t.kind == ActiveTaskKind::DeepWorker && t.dependencies == ["user_id"])
            .unwrap()
            .0;
        let old_epoch = session.state.epochs.thought;
        session.deep_results.push(DeepWorkResult {
            task_id: TaskId::new(),
            thought_epoch: old_epoch,
            summary: "old date".into(),
            dependencies: vec!["date".into()],
        });
        let mut clear = slot("unused");
        clear.operation = crate::semantics::frame::SlotOperation::Clear;
        clear.new_value = serde_json::Value::Null;
        session.apply_slot_updates(vec![clear], None).await.unwrap();
        assert!(!session.active_tasks.contains_key(&date_id));
        assert!(session.active_tasks.contains_key(&other_id));
        assert!(session.deep_results.is_empty());
        assert!(session.state.epochs.thought > old_epoch);
        tokio::task::yield_now().await;
        let completion = session.deep_rx.recv().await.unwrap();
        session.finish_deep_work(completion, true).await;
        assert_eq!(session.deep_results.len(), 1);
        assert_eq!(session.deep_results[0].task_id, other_id);
        assert_eq!(session.deep_results[0].dependencies, ["user_id"]);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn strict_survivors_keep_authorization_and_questions_require_playback() {
        let (mut session, _, _) = harness();
        session.speech_renderer =
            Arc::new(crate::speech::render::ConfiguredRenderer::new(&Default::default()).unwrap());
        let (tx, mut rx) = mpsc::channel(8);
        session.speech_tx = tx;
        let owner = TaskId::new();
        session
            .on_strict_final(
                owner,
                r#"{"text":"First. Continue?","question":"yes_no"}"#.into(),
            )
            .await
            .unwrap();
        let _first = rx.try_recv().unwrap();
        assert!(!session.state.assistant_asked_question);
        assert_eq!(session.pending_speech.len(), 1);
        session.state.epochs.speech += 1;
        session.retag_pending_speech(session.state.epochs.speech);
        session.state.speech = SpeechState::Silent;
        session.start_next_speech_if_possible().await.unwrap();
        let (last, _) = rx.try_recv().unwrap();
        assert_eq!(last.text, "Continue?");
        session
            .on_tts_audio(
                last.id,
                last.speech_epoch,
                1,
                24000,
                bytes::Bytes::from_static(&[0, 0, 0, 0]),
            )
            .await
            .unwrap();
        session
            .on_tts_audio_done(last.id, last.speech_epoch)
            .await
            .unwrap();
        assert!(!session.state.assistant_asked_question);
        session
            .handle_event(VoiceEvent::PlaybackCompleted {
                meta: session.new_meta(),
                speech_id: last.id,
            })
            .await
            .unwrap();
        assert!(session.state.assistant_asked_question);
        assert!(session.speech_questions.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn queued_progress_is_retired_on_completion_and_rechecked_after_expiry() {
        let (mut session, _, _) = harness();
        Arc::make_mut(&mut session.config)
            .speech
            .progress_templates
            .insert(
                "lookup".into(),
                crate::config::ProgressTemplate {
                    text: "Working.".into(),
                    minimum_expected_latency_ms: 1,
                    expire_after_ms: 1,
                },
            );
        session
            .start_main_task(TurnId::new(), "request".into(), Revision(1))
            .await
            .unwrap();
        let task = session.state.active_task_id.unwrap();
        let epoch = session.state.epochs.thought;
        let (tx, mut rx) = mpsc::channel(8);
        session.speech_tx = tx;
        session.state.speech = SpeechState::Playing;
        session
            .handle_event(VoiceEvent::ToolStarted {
                meta: session.new_meta(),
                task_id: task,
                thought_epoch: epoch,
                tool: "lookup".into(),
                args: serde_json::Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(session.pending_speech.len(), 1);
        session
            .handle_event(VoiceEvent::ToolCompleted {
                meta: session.new_meta(),
                task_id: task,
                thought_epoch: epoch,
                tool: "lookup".into(),
                success: true,
                output: String::new(),
            })
            .await
            .unwrap();
        assert!(session.pending_speech.is_empty());
        session.state.speech = SpeechState::Playing;
        session
            .handle_event(VoiceEvent::ToolStarted {
                meta: session.new_meta(),
                task_id: task,
                thought_epoch: epoch,
                tool: "lookup".into(),
                args: serde_json::Value::Null,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(2)).await;
        session.state.speech = SpeechState::Silent;
        session.start_next_speech_if_possible().await.unwrap();
        assert!(session.pending_speech.is_empty());
        assert!(rx.try_recv().is_err());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn queued_progress_is_not_spoken_after_the_task_ends() {
        for fail in [true, false] {
            let (mut session, _, _) = harness();
            let config = Arc::make_mut(&mut session.config);
            config.speech.failure_text = "Failed.".into();
            config.speech.progress_templates.insert(
                "lookup".into(),
                crate::config::ProgressTemplate {
                    text: "Working.".into(),
                    minimum_expected_latency_ms: 1,
                    expire_after_ms: 60_000,
                },
            );
            session
                .start_main_task(TurnId::new(), "request".into(), Revision(1))
                .await
                .unwrap();
            let task = session.state.active_task_id.unwrap();
            let epoch = session.state.epochs.thought;
            let (tx, mut rx) = mpsc::channel(8);
            session.speech_tx = tx;
            session.state.speech = SpeechState::Playing;
            session
                .handle_event(VoiceEvent::ToolStarted {
                    meta: session.new_meta(),
                    task_id: task,
                    thought_epoch: epoch,
                    tool: "lookup".into(),
                    args: serde_json::Value::Null,
                })
                .await
                .unwrap();
            assert_eq!(session.pending_speech.len(), 1);
            let end = if fail {
                VoiceEvent::AgentFailed {
                    meta: session.new_meta(),
                    task_id: task,
                    thought_epoch: epoch,
                    message: "timed out".into(),
                }
            } else {
                VoiceEvent::AgentFinal {
                    meta: session.new_meta(),
                    task_id: task,
                    thought_epoch: epoch,
                    text: "Done.".into(),
                }
            };
            session.handle_event(end).await.unwrap();

            // Playback frees up long before the progress line would expire.
            let mut spoken = Vec::new();
            for _ in 0..4 {
                session.state.speech = SpeechState::Silent;
                session.start_next_speech_if_possible().await.unwrap();
                while let Ok((act, _)) = rx.try_recv() {
                    spoken.push(act.text);
                }
            }
            assert!(
                !spoken.iter().any(|text| text == "Working."),
                "fail={fail}: {spoken:?}"
            );
            session.shutdown_active_work().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn partial_commit_rejects_late_audio_before_projection() {
        let (mut session, _, _) = harness();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "첫 요청입니다".into(),
            })
            .await
            .unwrap();
        session.commit_user_turn("test").await.unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 1,
                revision: Revision(2),
                hypothesis: "다음 요청입니다".into(),
            })
            .await
            .unwrap();
        let revision = session.state.epochs.transcript;
        for event in [
            VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(99),
                hypothesis: "늦은 수정".into(),
            },
            VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "첫 요청의 늦은 결과".into(),
            },
        ] {
            session.handle_event(event).await.unwrap();
            assert_eq!(session.state.epochs.transcript, revision);
            assert_eq!(session.transcript.current_text(), "다음 요청입니다");
            assert_eq!(session.state.current_hypothesis, "다음 요청입니다");
            assert!(session.turn_buffer.is_empty());
        }
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn final_first_commit_does_not_consume_empty_successor() {
        let (mut session, _, _) = harness();
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Audio,
                transcript: "같은 요청입니다".into(),
            })
            .await
            .unwrap();
        session.commit_user_turn("test").await.unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 1,
                revision: Revision(2),
                hypothesis: "같은 요청입니다".into(),
            })
            .await
            .unwrap();
        assert_eq!(session.transcript.current_text(), "같은 요청입니다");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn injection_only_commit_does_not_consume_audio_identity() {
        let (mut session, _, _) = harness();
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 10,
                source: AsrSource::Injection,
                transcript: "입력한 요청입니다".into(),
            })
            .await
            .unwrap();
        session.commit_user_turn("test").await.unwrap();
        assert!(session.consumed_audio_watermark.is_none());
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "음성 요청입니다".into(),
            })
            .await
            .unwrap();
        assert_eq!(session.transcript.current_text(), "음성 요청입니다");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn consumed_audio_guard_survives_reset_but_does_not_filter_injection() {
        let (mut session, _, _) = harness();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "같은 요청입니다".into(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 10,
                source: AsrSource::Injection,
                transcript: "추가 입력".into(),
            })
            .await
            .unwrap();
        session.commit_user_turn("test").await.unwrap();
        assert_eq!(session.consumed_audio_watermark, Some(0));
        session
            .handle_event(VoiceEvent::AsrStreamReset {
                meta: session.new_meta(),
                generation: 2,
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrPartial {
                meta: session.new_meta(),
                utterance_id: 1,
                revision: Revision(2),
                hypothesis: "같은 요청입니다".into(),
            })
            .await
            .unwrap();
        session
            .handle_event(VoiceEvent::AsrUtteranceFinal {
                meta: session.new_meta(),
                utterance_id: 0,
                source: AsrSource::Injection,
                transcript: "독립 입력".into(),
            })
            .await
            .unwrap();
        assert_eq!(session.transcript.current_text(), "같은 요청입니다");
        assert_eq!(session.turn_buffer.render_candidate(""), "독립 입력");
        session.shutdown_active_work().await;
    }

    #[test]
    fn clauses_split_on_sentence_boundaries() {
        assert_eq!(
            split_clauses("확인했습니다. 두 곳이 가능합니다."),
            vec!["확인했습니다.", "두 곳이 가능합니다."]
        );
        // No terminator still yields one speakable clause.
        assert_eq!(split_clauses("네 알겠어요"), vec!["네 알겠어요"]);
    }

    #[derive(Default)]
    struct RecordedPlayback(parking_lot::Mutex<Vec<PlaybackCommand>>);

    #[async_trait::async_trait]
    impl PlaybackSink for RecordedPlayback {
        async fn command(&self, command: PlaybackCommand) {
            self.0.lock().push(command);
        }
    }

    fn harness() -> (
        VoiceSession,
        Arc<crate::agent::mock::AgentRecorder>,
        Arc<RecordedPlayback>,
    ) {
        let agent = crate::agent::mock::FakeAgent::finalizing("완료했습니다.");
        let recorder = agent.recorder();
        let sink = Arc::new(RecordedPlayback::default());
        let tts = crate::tts::buffered::BufferedTtsAdapter::new(Arc::new(
            crate::tts::fake::FakeTts::new(24_000, 60, 2),
        ));
        let mut config =
            VoiceRuntimeConfig::from_yaml_str("observability:\n  event_log: false").unwrap();
        config.turn_control = crate::config::TurnControlConfig::korean_example();
        config.speech.hard_stop_ack_text = "네, 멈췄어요.".into();
        let config = Arc::new(config);
        let (session, _) =
            VoiceSessionBuilder::new(config, sink.clone(), tts, Arc::new(agent)).build();
        session.clock.now_us();
        (session, recorder, sink)
    }

    fn slot(value: &str) -> SlotUpdate {
        SlotUpdate {
            slot: "date".into(),
            operation: crate::semantics::frame::SlotOperation::Set,
            old_value: None,
            new_value: value.into(),
            confidence: 0.73,
            source_revision: Revision(7),
        }
    }

    async fn finish_extraction(
        session: &mut VoiceSession,
        provenance: SlotProvenance,
        sequence: u64,
        update: SlotUpdate,
    ) {
        let revision = update.source_revision;
        if session.transcript.current_text().is_empty() {
            session.transcript.on_partial(
                provenance.source_utterance_id,
                revision,
                "candidate",
                session.monotonic_us(),
            );
        }
        session.slot_extraction_running = Some((sequence, u64::MAX));
        session
            .finish_slot_extraction(SlotExtractionResult {
                request: SlotExtractionRequest {
                    text: "candidate".into(),
                    revision,
                    provenance,
                    host_generation: session.slot_host_generation,
                },
                sequence,
                host_generation: session.slot_host_generation,
                updates: vec![update],
            })
            .await
            .unwrap();
    }

    fn candidate(session: &VoiceSession, id: u64) -> SlotProvenance {
        SlotProvenance {
            candidate_generation: session.candidate_generation,
            source_utterance_id: id,
            source: AsrSource::Audio,
        }
    }

    fn track(session: &mut VoiceSession, never: bool, owner: Option<TaskId>) -> SpeechId {
        let id = SpeechId::new();
        let text = "첫 번째 안내를 전달하는 동안 추가 확인을 진행합니다.".to_string();
        session.speech_records.insert(
            id,
            SpeechRecord {
                text: text.clone(),
                never,
                sample_rate: 24_000,
                owner,
                synthesis: SynthesisOutcome::Running,
                playback: PlaybackLifecycle::Playing,
                confirmed_samples: 2_400,
                stop_deadline_us: None,
            },
        );
        session.state.active_speech_id = Some(id);
        session.state.speech = SpeechState::Playing;
        session.audible_ledger.speech_started(id, &text, 24_000);
        session.audible_ledger.add_enqueued_samples(id, 24_000);
        session.audible_ledger.mark_progress(id, 2_400);
        if let Some(owner) = owner {
            session.turn_speech.insert(
                owner,
                TurnSpeech {
                    final_text: text.clone(),
                    clauses: vec![(id, text)],
                },
            );
        }
        id
    }

    struct BlockedPlayback;
    #[async_trait::async_trait]
    impl PlaybackSink for BlockedPlayback {
        async fn command(&self, _: PlaybackCommand) {
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_escapes_an_external_sink_stalled_inside_an_event_handler() {
        let (mut session, recorder, _) = harness();
        session.playback = Arc::new(BlockedPlayback);
        track(&mut session, false, Some(TaskId::new()));
        let shutdown = session.shutdown.clone();
        let events = session.event_tx.clone();
        let meta = session.new_meta();
        let worker = tokio::spawn(session.run());
        events
            .send(VoiceEvent::AsrPartial {
                meta,
                utterance_id: 0,
                revision: Revision(1),
                hypothesis: "그만".into(),
            })
            .await
            .unwrap();
        tokio::task::yield_now().await;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(recorder.resolutions().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn hard_stop_overrides_never_before_the_first_audio() {
        let (mut session, _, sink) = harness();
        let id = track(&mut session, true, None);
        session.speech_records.get_mut(&id).unwrap().playback = PlaybackLifecycle::NotStarted;
        session.audible_ledger = AudibleLedger::default();
        session.state.speech = SpeechState::Synthesizing;
        session
            .on_asr_partial(0, Revision(1), "그만".into())
            .await
            .unwrap();
        assert!(!session.speech_records.contains_key(&id));
        assert!(sink.0.lock().iter().any(|command| matches!(
            command,
            PlaybackCommand::Abort {
                reason: InterruptionReason::HardStop,
                ..
            }
        )));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn changing_candidate_writer_does_not_promote_a_tentative_baseline() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("authoritative")], None)
            .await
            .unwrap();
        for (id, text) in [(0, "candidate A"), (1, "candidate B")] {
            let provenance = candidate(&session, id);
            session
                .apply_slot_updates(vec![slot(text)], Some(provenance))
                .await
                .unwrap();
        }
        session.on_asr_stream_reset().await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("authoritative"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn speculative_promotion_matches_the_closed_utterance_only() {
        let (mut session, _, _) = harness();
        let first = TaskId::new();
        let second = TaskId::new();
        for (task_id, utterance_id) in [(first, 0), (second, 1)] {
            session.active_tasks.insert(
                task_id,
                ActiveTask {
                    id: task_id,
                    kind: ActiveTaskKind::SpeculativeRead,
                    thought_epoch: 1,
                    dependencies: Vec::new(),
                    fingerprint: 0,
                    cancellation: CancellationToken::new(),
                    handle: None,
                    candidate_generation: 0,
                    candidate_utterance_id: Some(utterance_id),
                },
            );
        }
        session.close_candidates_for(0, AsrSource::Audio);
        assert_eq!(session.active_tasks[&first].candidate_generation, u64::MAX);
        assert_eq!(session.active_tasks[&second].candidate_generation, 0);
        session.on_asr_stream_reset().await.unwrap();
        assert!(session.active_tasks.contains_key(&first));
        assert!(!session.active_tasks.contains_key(&second));
        session
            .on_agent_final(second, 1, "abandoned result".into())
            .await
            .unwrap();
        assert!(session.speculative_results.is_empty());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_commit_while_stopping_supersedes_the_deferred_record() {
        let (mut session, recorder, _) = harness();
        let id = track(&mut session, true, None);
        session
            .start_main_task(TurnId::new(), "obsolete".into(), Revision(1))
            .await
            .unwrap();
        session.on_tts_failed(id, "failure".into()).await.unwrap();
        session
            .start_main_task(TurnId::new(), "latest".into(), Revision(2))
            .await
            .unwrap();
        assert!(session.deferred_turn.is_none());
        tokio::task::yield_now().await;
        assert_eq!(recorder.requests().len(), 1);
        assert_eq!(recorder.requests()[0].input, "latest");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn failed_speech_admission_retains_exactly_one_turn_resolution_owner() {
        let (mut session, recorder, _) = harness();
        session
            .start_main_task(TurnId::new(), "request".into(), Revision(1))
            .await
            .unwrap();
        let id = session.state.active_task_id.unwrap();
        let epoch = session.state.epochs.thought;
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        session.speech_tx = tx;
        session.state.evidence.insert(
            format!("agent_final:{id}"),
            crate::session::view::SessionEvidence::AgentFinal {
                task_id: id,
                text: "allowed reply".into(),
            },
        );
        assert!(
            session
                .on_agent_final(id, epoch, "allowed reply".into())
                .await
                .is_err()
        );
        session.shutdown_active_work().await;
        assert_eq!(recorder.resolutions().len(), 1);
        assert!(
            matches!(&recorder.resolutions()[0].1, TurnResolution::Audible { heard, .. } if heard == crate::speech::ledger::MARKER_UNDELIVERED)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reset_restores_original_candidate_baseline_with_metadata() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("A")], None)
            .await
            .unwrap();
        session.state.semantic_frame.commit_pending();
        let baseline = serde_json::to_value(&session.state.semantic_frame.slots["date"]).unwrap();
        let provenance = candidate(&session, 0);
        session
            .apply_slot_updates(vec![slot("B")], Some(provenance.clone()))
            .await
            .unwrap();
        session
            .apply_slot_updates(vec![slot("D")], Some(provenance))
            .await
            .unwrap();
        let thought = session.state.epochs.thought;
        session.on_asr_stream_reset().await.unwrap();
        assert_eq!(
            serde_json::to_value(&session.state.semantic_frame.slots["date"]).unwrap(),
            baseline
        );
        assert_eq!(session.state.epochs.thought, thought);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn host_interleaving_retires_old_journal_and_clear_restores_absence() {
        let (mut session, _, _) = harness();
        session
            .apply_slot_updates(vec![slot("A")], None)
            .await
            .unwrap();
        let provenance = candidate(&session, 0);
        session
            .apply_slot_updates(vec![slot("B")], Some(provenance.clone()))
            .await
            .unwrap();
        session
            .apply_slot_updates(vec![slot("C")], None)
            .await
            .unwrap();
        let baseline = serde_json::to_value(&session.state.semantic_frame.slots["date"]).unwrap();
        let mut clear = slot("ignored");
        clear.operation = crate::semantics::frame::SlotOperation::Clear;
        session
            .apply_slot_updates(vec![clear], Some(provenance))
            .await
            .unwrap();
        session.on_asr_stream_reset().await.unwrap();
        assert_eq!(
            serde_json::to_value(&session.state.semantic_frame.slots["date"]).unwrap(),
            baseline
        );
        let provenance = candidate(&session, 1);
        let mut update = slot("temp");
        update.slot = "new-slot".into();
        session
            .apply_slot_updates(vec![update], Some(provenance))
            .await
            .unwrap();
        session.on_asr_stream_reset().await.unwrap();
        assert!(!session.state.semantic_frame.slots.contains_key("new-slot"));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn closing_partial_candidate_preserves_it_on_reset_and_rejects_late_extraction() {
        let (mut session, _, _) = harness();
        let provenance = candidate(&session, 0);
        session
            .apply_slot_updates(vec![slot("committed")], Some(provenance.clone()))
            .await
            .unwrap();
        session.close_candidates_for(0, AsrSource::Audio);
        session
            .apply_slot_updates(vec![slot("late")], Some(provenance))
            .await
            .unwrap();
        session.on_asr_stream_reset().await.unwrap();
        assert_eq!(
            session.state.semantic_frame.slot_value("date"),
            Some(&serde_json::json!("committed"))
        );
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn injection_final_does_not_close_an_audio_candidate() {
        let (mut session, _, _) = harness();
        let provenance = candidate(&session, 0);
        session
            .apply_slot_updates(vec![slot("tentative")], Some(provenance))
            .await
            .unwrap();
        session.close_candidates_for(
            crate::ids::INJECTION_UTTERANCE_ID_BASE,
            AsrSource::Injection,
        );
        session.on_asr_stream_reset().await.unwrap();
        assert!(session.state.semantic_frame.slot_value("date").is_none());
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn partial_failure_waits_for_ack_before_next_speech_and_resolves_once() {
        let (mut session, recorder, sink) = harness();
        let owner = TaskId::new();
        let id = track(&mut session, false, Some(owner));
        let dropped = SpeechAct::phatic("폐기할 후속 문장", session.state.epochs.speech, 60);
        session
            .turn_speech
            .get_mut(&owner)
            .unwrap()
            .clauses
            .push((dropped.id, dropped.text.clone()));
        session.pending_speech.push_back(dropped);
        let independent = SpeechAct::phatic("독립 안내", session.state.epochs.speech, 10);
        let next = independent.id;
        session.pending_speech.push_back(independent);
        session
            .on_tts_failed(id, "partial synthesis error".into())
            .await
            .unwrap();
        session.start_next_speech_if_possible().await.unwrap();
        assert!(session.state.active_speech_id.is_none());
        assert_eq!(session.audible_ledger.in_flight_id(), Some(id));
        assert_eq!(session.pending_speech.len(), 1);
        assert!(
            sink.0
                .lock()
                .iter()
                .any(|command| matches!(command, PlaybackCommand::Abort { .. }))
        );
        let ack = VoiceEvent::PlaybackInterrupted {
            meta: session.new_meta(),
            speech_id: id,
            played_samples: 2_400,
            reason: InterruptionReason::ProviderError,
        };
        session.handle_event(ack.clone()).await.unwrap();
        session.handle_event(ack).await.unwrap();
        assert_eq!(recorder.resolutions().len(), 1);
        assert!(matches!(
            session.audible_ledger.history()[0].heard,
            Heard::Partial { .. }
        ));
        session.start_next_speech_if_possible().await.unwrap();
        assert_eq!(session.state.active_speech_id, Some(next));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn new_turn_fast_settles_ownerless_and_stopping_speech() {
        let (mut session, _, _) = harness();
        let id = track(&mut session, false, None);
        session
            .start_main_task(TurnId::new(), "새 요청".into(), Revision(3))
            .await
            .unwrap();
        assert!(session.speech_records.is_empty());
        assert!(session.audible_ledger.in_flight_id().is_none());
        let history = session.audible_ledger.history().len();
        session
            .handle_event(VoiceEvent::PlaybackInterrupted {
                meta: session.new_meta(),
                speech_id: id,
                played_samples: 99_999,
                reason: InterruptionReason::UserTurn,
            })
            .await
            .unwrap();
        assert_eq!(session.audible_ledger.history().len(), history);
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn never_remains_protected_after_audio_done_and_releases_commit_snapshot_once() {
        let (mut session, recorder, _) = harness();
        let owner = TaskId::new();
        let id = track(&mut session, true, Some(owner));
        session
            .on_tts_audio_done(id, session.state.epochs.speech)
            .await
            .unwrap();
        session
            .apply_slot_updates(vec![slot("commit")], None)
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "최신 요청".into(), Revision(3))
            .await
            .unwrap();
        session
            .apply_slot_updates(vec![slot("later")], None)
            .await
            .unwrap();
        assert!(session.deferred_turn.is_some());
        assert!(recorder.resolutions().is_empty());
        session.maybe_release_deferred().await.unwrap();
        assert!(recorder.requests().is_empty());
        let ack = VoiceEvent::PlaybackCompleted {
            meta: session.new_meta(),
            speech_id: id,
        };
        session.handle_event(ack.clone()).await.unwrap();
        session.handle_event(ack).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(recorder.resolutions().len(), 1);
        let requests = recorder.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].context.brief.contains("commit"));
        assert!(!requests[0].context.brief.contains("later"));
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_latest_commit_releases_on_watchdog_without_extra_event() {
        let (mut session, recorder, _) = harness();
        let id = track(&mut session, true, None);
        session
            .start_main_task(TurnId::new(), "old".into(), Revision(2))
            .await
            .unwrap();
        session
            .start_main_task(TurnId::new(), "latest".into(), Revision(3))
            .await
            .unwrap();
        session.last_playback_signal_us = Some(0);
        tokio::time::advance(Duration::from_secs(16)).await;
        session.on_control_tick().await.unwrap();
        tokio::task::yield_now().await;
        assert!(!session.speech_records.contains_key(&id));
        let requests = recorder.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].input, "latest");
        session.shutdown_active_work().await;
    }

    #[tokio::test(start_paused = true)]
    async fn old_progress_cannot_refresh_a_successor_watchdog() {
        let (mut session, _, _) = harness();
        let old = track(&mut session, false, None);
        session.settle_previous_turns().await.unwrap();
        let successor = track(&mut session, false, None);
        session.last_playback_signal_us = Some(123);
        session
            .handle_event(VoiceEvent::PlaybackProgress {
                meta: session.new_meta(),
                speech_id: old,
                played_samples: 48_000,
            })
            .await
            .unwrap();
        assert_eq!(session.last_playback_signal_us, Some(123));
        assert_eq!(session.state.active_speech_id, Some(successor));
        session.shutdown_active_work().await;
    }

    #[test]
    fn brief_names_the_speech_language_only_when_configured() {
        let frame = SemanticFrame::default();
        let render = |language: &str| {
            render_turn_context(TurnId::new(), Revision(1), &frame, &[], &[], language).brief
        };
        assert!(render("English").contains("speech_language: English\n"));
        let unset = render("");
        assert!(!unset.contains("speech_language"));
        assert!(unset.contains("idempotency_key: ") && unset.contains("semantic_frame: "));
    }
    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_owned_workers_and_resolves_a_running_turn_once() {
        let (mut session, recorder, _) = harness();
        session
            .start_main_task(TurnId::new(), "요청".into(), Revision(1))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        session.shutdown_active_work().await;
        session.shutdown_active_work().await;
        assert!(session.workers.is_empty());
        assert!(session.active_tasks.is_empty());
        assert_eq!(recorder.resolutions().len(), 1);
    }
}
