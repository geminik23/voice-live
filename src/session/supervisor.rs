use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
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
    InteractionAction, InteractionDecisionEnvelope, InterruptionReason, SpeechState,
    UserSpeechState,
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
    pub policy: ReflexPolicy,
    pub partial_policy: PartialControllerPolicy,

    last_interaction_run_us: Option<u64>,
    partial_last_snapshot: Option<TranscriptSnapshot>,
    backchannel_count_this_turn: u32,
    last_backchannel_at_us: Option<u64>,
    speculative_results: Vec<SpeculativeResult>,
    deep_results: Vec<DeepWorkResult>,
    turn_speech: HashMap<TaskId, TurnSpeech>,
    turn_started_at_us: Option<u64>,
    /// Increments on every ASR stream reset; results derived from older
    /// generations are discarded on arrival.
    candidate_generation: u64,
    /// Highest audio-source utterance whose candidates are closed (committed
    /// or superseded by an authoritative final).
    candidate_closed_watermark: Option<u64>,
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
}

impl VoiceSessionBuilder {
    pub fn new(
        config: Arc<VoiceRuntimeConfig>,
        playback: Arc<dyn PlaybackSink>,
        tts: Arc<dyn StreamingTts>,
        task_agent: Arc<dyn CognitiveAgent>,
    ) -> Self {
        let clock: ClockRef = Arc::new(TokioClock::new());
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
            slot_extractor: Arc::new(crate::semantics::extractor::HeuristicKoreanSlotExtractor),
            deep_worker: None,
            metrics: Metrics::new(),
            trace_tx: None,
            shutdown: CancellationToken::new(),
        }
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
        let session_id = self.session_id.unwrap_or_default();
        let (event_tx, event_rx) = mpsc::channel::<VoiceEvent>(1024);
        let (speech_tx, speech_rx) = mpsc::channel::<(SpeechAct, CancellationToken)>(64);
        let tts_registry = Arc::new(TtsCancellationRegistry::default());

        let cue_extractor = CueExtractor::from_config(&self.config.turn_control);
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
            pending_speech: VecDeque::new(),
            speech_records: HashMap::new(),
            act_owner: HashMap::new(),
            last_playback_signal_us: None,
            transcript: TranscriptReconciler::new(asr_partials),
            turn_buffer: TurnAssemblyBuffer::default(),
            audible_ledger: AudibleLedger::with_capacity(audible_capacity),
            claim_gate: ClaimGate,
            cue_extractor,
            policy: ReflexPolicy::new(),
            partial_policy: PartialControllerPolicy::from_config(interaction_interval),
            last_interaction_run_us: None,
            partial_last_snapshot: None,
            backchannel_count_this_turn: 0,
            last_backchannel_at_us: None,
            speculative_results: Vec::new(),
            deep_results: Vec::new(),
            turn_speech: HashMap::new(),
            turn_started_at_us: None,
            candidate_generation: 0,
            candidate_closed_watermark: None,
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

        (session, handle)
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

    async fn handle_event(&mut self, event: VoiceEvent) -> Result<()> {
        self.forward_trace(&event).await;

        // Speech events are admitted by identity and lifecycle before any
        // projection or handler runs, so a late chunk, a duplicate terminal,
        // or an old acknowledgement can never touch the current state.
        if !self.admit_speech_event(&event) {
            return Ok(());
        }

        if self.should_apply_projection(&event) {
            self.state.apply(&event);
        }

        match event {
            VoiceEvent::LocalSpeechStarted { .. } => {
                let previous_speaking = matches!(
                    self.state.speech,
                    SpeechState::Playing | SpeechState::Ducking
                );
                self.on_user_speech_started(previous_speaking).await?;
            }

            VoiceEvent::LocalSpeechEnded { .. } => {
                self.publish_interaction_snapshot().await;
            }

            VoiceEvent::AsrPartial {
                utterance_id,
                revision,
                hypothesis,
                ..
            } => {
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
                if self.state.last_user_audio_at_us.is_none() {
                    self.state.last_user_audio_at_us = Some(meta.monotonic_us);
                }
                if source == AsrSource::Injection {
                    self.turn_buffer.push_final(transcript);
                    self.state.current_hypothesis = self.transcript.current_text().to_owned();
                    self.state.stable_prefix = self.transcript.stable_prefix().to_owned();
                    self.close_candidates_for(utterance_id, source);
                    self.publish_interaction_snapshot().await;
                    return Ok(());
                }
                self.transcript.on_final(utterance_id, &transcript);
                let sealed = self.transcript.seal_current();
                self.turn_buffer.push_final(sealed);
                self.state.current_hypothesis.clear();
                self.state.stable_prefix.clear();
                self.close_candidates_for(utterance_id, source);
                self.publish_interaction_snapshot().await;
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
                self.settle_speech_record(speech_id);
                self.audible_ledger.mark_completed(speech_id);
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
                self.apply_deep_work_result(result);
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
        for act in &mut self.pending_speech {
            act.speech_epoch = new_epoch;
        }

        if reason == InterruptionReason::HardStop {
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
                // A backchannel is not a user turn: discard the partial so a
                // later silence cannot commit it.
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

            self.spawn_slot_extraction(stable_prefix.clone(), revision);
            self.maybe_spawn_speculative(&stable_prefix);
        }

        let cues = self.cue_extractor.extract(&hypothesis);

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
                    self.apply_slot_updates(vec![update], Some(provenance))
                        .await?;
                }
                _ => {}
            }
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
            && now_us.saturating_sub(last) < interval * 1_000
        {
            return;
        }

        self.last_interaction_run_us = Some(now_us);

        let snapshot = self.interaction_snapshot();
        let brain = Arc::clone(&self.interaction_brain);
        let event_tx = self.event_tx.clone();
        let meta_factory = Arc::clone(&self.meta_factory);

        let shutdown = self.shutdown.clone();
        let timeout = Duration::from_millis(self.config.agents.interaction.timeout_ms);
        self.workers.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = tokio::time::sleep(timeout) => {},
                _ = async {
            if let Some(envelope) = brain.decide(snapshot).await {
                let _ = event_tx
                    .send(VoiceEvent::InteractionDecision {
                        meta: meta_factory.new_meta(),
                        envelope,
                    })
                    .await;
            }
                } => {},
            }
        });
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

    fn spawn_slot_extraction(&mut self, stable_prefix: String, revision: Revision) {
        if stable_prefix.trim().is_empty() {
            return;
        }

        let extractor = Arc::clone(&self.slot_extractor);
        let previous_frame = self.state.semantic_frame.clone();
        let event_tx = self.event_tx.clone();
        let meta_factory = Arc::clone(&self.meta_factory);
        let provenance = SlotProvenance {
            candidate_generation: self.candidate_generation,
            source_utterance_id: self.transcript.current_utterance(),
            source: AsrSource::Audio,
        };

        let shutdown = self.shutdown.clone();
        let timeout = Duration::from_millis(self.config.agents.interaction.timeout_ms);
        self.workers.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = tokio::time::sleep(timeout) => {},
                _ = async {
            let updates = extractor
                .extract(&previous_frame, &stable_prefix, revision)
                .await;
            if updates.is_empty() {
                return;
            }

            let _ = event_tx
                .send(VoiceEvent::SemanticFrameUpdated {
                    meta: meta_factory.new_meta(),
                    revision,
                    updates,
                    provenance: Some(provenance),
                })
                .await;
                } => {},
            }
        });
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
            let is_correction = update.operation == crate::semantics::frame::SlotOperation::Replace
                || update.old_value.is_some();

            if is_correction {
                changed_slots.push(update.slot.clone());
            }

            match &provenance {
                Some(provenance) => {
                    let previous = self.state.semantic_frame.slots.get(&update.slot).cloned();
                    let journal =
                        self.slot_journal
                            .entry(update.slot.clone())
                            .or_insert_with(|| SlotJournalEntry {
                                previous: previous.clone(),
                                writer: provenance.clone(),
                            });
                    if journal.writer != *provenance {
                        journal.writer = provenance.clone();
                    }
                }
                None => {
                    self.slot_journal.remove(&update.slot);
                }
            }

            self.state.semantic_frame.set_slot(update);
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

        let watermark = self
            .candidate_closed_watermark
            .map(|current| current.max(source_utterance_id))
            .unwrap_or(source_utterance_id);
        self.candidate_closed_watermark = Some(watermark);
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
        self.transcript.clear();
        self.state.current_hypothesis.clear();
        self.state.stable_prefix.clear();
        self.state.epochs.bump_interaction();
        self.rollback_abandoned_candidate_slots();
        let abandoned = self.candidate_generation;
        self.candidate_generation += 1;
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
        self.metrics.inc("voice_asr_reset_total");

        Ok(())
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
        let Some(speculative_agent) = &self.speculative_agent else {
            return;
        };

        if !self.config.agents.speculative.enabled {
            return;
        }

        let cues = self.cue_extractor.extract(stable_prefix);
        let intent = cues
            .iter()
            .find_map(|cue| match cue {
                SemanticCue::IntentCandidate { intent, confidence } => {
                    Some((intent.clone(), *confidence))
                }
                _ => None,
            })
            .filter(|(_, confidence)| {
                *confidence >= self.config.agents.speculative.minimum_intent_confidence
            });

        let Some((intent, _)) = intent else {
            return;
        };

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
                task.cancel();
                if task.kind == ActiveTaskKind::MainTurn {
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

        self.state.epochs.invalidate_thought();
    }

    // User turn commit and main task processing.

    async fn commit_user_turn(&mut self, reason: &str) -> Result<()> {
        let sealed_source = self.transcript.current_utterance();
        let sealed = self.transcript.seal_current();
        if !sealed.trim().is_empty() {
            self.turn_buffer.push_partial_seal(sealed);
        }

        let transcript = self.turn_buffer.take();

        if transcript.trim().is_empty() {
            return Ok(());
        }

        // The committed turn promotes the candidates that fed it, even when
        // no authoritative final ever arrived: a later stream reset must not
        // roll back work the user already committed.
        self.close_candidates_for(sealed_source, AsrSource::Audio);

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
        let task_id = TaskId::new();
        let thought_epoch = self.state.epochs.invalidate_thought();

        let cancellation = CancellationToken::new();
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

        self.workers.spawn(async move {
            run_task_turn_with_timeout(agent, request, child, event_tx, meta_factory, timeout)
                .await;
        });

        self.active_tasks.insert(
            task_id,
            ActiveTask {
                id: task_id,
                kind: ActiveTaskKind::MainTurn,
                thought_epoch,
                dependencies: Vec::new(),
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
                            && self.state.epochs.thought == thought_epoch
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
                    self.active_tasks.remove(&task_id);
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
                    self.active_tasks.remove(&task_id);
                    self.metrics.inc("voice_task_stale_result_dropped_total");
                    // Never spoken, so it must not survive in memory either.
                    self.resolve_turn(task_id, TurnResolution::Discard);
                    return Ok(());
                }
            }
            Some(ActiveTaskKind::DeepWorker) | None => {
                return Ok(());
            }
        }

        self.active_tasks.remove(&task_id);
        self.state.task = crate::interaction::TaskState::Finalizing;

        let transactional = is_transactional_claim(&text);

        let evidence: Vec<crate::speech::act::EvidenceRef> = if transactional {
            let prefix = format!("action_commit:{task_id}:");
            self.state
                .evidence
                .keys()
                .filter(|id| id.starts_with(&prefix))
                .map(|id| {
                    crate::speech::act::EvidenceRef::new(
                        id.clone(),
                        crate::speech::act::EvidenceKind::ActionCommit,
                    )
                })
                .collect()
        } else {
            vec![crate::speech::act::EvidenceRef::new(
                format!("agent_final:{task_id}"),
                crate::speech::act::EvidenceKind::AgentFinal,
            )]
        };

        self.turn_speech.insert(
            task_id,
            TurnSpeech {
                final_text: text.clone(),
                clauses: Vec::new(),
            },
        );
        let mut spoken = Vec::new();
        for clause in split_clauses(&text) {
            let class = if transactional {
                ClaimClass::Transactional
            } else {
                ClaimClass::Factual
            };

            let act = SpeechAct {
                id: SpeechId::new(),
                speech_epoch: self.state.epochs.speech,
                class,
                text: clause.clone(),
                priority: crate::speech::act::PRIORITY_FINAL,
                interruptibility: Interruptibility::AfterClause,
                evidence: evidence.clone(),
                expires_at_monotonic_us: None,
            };

            let id = act.id;
            self.turn_speech
                .get_mut(&task_id)
                .expect("reply owner registered")
                .clauses
                .push((id, clause.clone()));
            if self.authorize_and_enqueue(act, Some(task_id)).await? {
                spoken.push((id, clause));
            }
        }

        // Tracked until every clause settles. A reply whose clauses were all
        // rejected by the claim gate settles immediately as undelivered, so
        // an unverified claim never stands in memory as if the user heard it.
        self.turn_speech.insert(
            task_id,
            TurnSpeech {
                final_text: text.clone(),
                clauses: spoken,
            },
        );

        if let Some(answer_type) = expected_answer_type(&text) {
            self.state.assistant_asked_question = true;
            self.state.expected_answer_type = Some(answer_type);
        }

        self.state.task = crate::interaction::TaskState::Completed;
        Ok(())
    }

    async fn on_agent_failed(
        &mut self,
        task_id: TaskId,
        thought_epoch: u64,
        message: String,
    ) -> Result<()> {
        if !self.is_task_result_current(task_id, thought_epoch) {
            self.metrics.inc("voice_task_stale_result_dropped_total");
            // A failed stale main turn may still have written its user message.
            if self
                .active_tasks
                .get(&task_id)
                .is_some_and(|task| task.kind == ActiveTaskKind::MainTurn)
            {
                self.active_tasks.remove(&task_id);
                self.resolve_turn(task_id, TurnResolution::Discard);
            } else {
                self.active_tasks.remove(&task_id);
            }
            return Ok(());
        }

        let kind = self.active_tasks.get(&task_id).map(|task| task.kind);
        self.active_tasks.remove(&task_id);

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

        let act = SpeechAct::phatic(
            "죄송합니다. 응답을 준비하지 못했어요. 다시 말씀해 주세요.",
            self.state.epochs.speech,
            crate::speech::act::PRIORITY_CLARIFICATION,
        );
        self.authorize_and_enqueue(act, None).await?;

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
            for act in &mut self.pending_speech {
                act.speech_epoch = new_epoch;
            }

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
        if !config.enabled {
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
            "네",
            self.state.epochs.speech,
            crate::speech::act::PRIORITY_BACKCHANNEL,
        );
        self.authorize_and_enqueue(act, None).await?;
        self.metrics.inc("voice_backchannel_emitted_total");
        Ok(())
    }

    // Deterministic control tick.

    async fn on_control_tick(&mut self) -> Result<()> {
        let now_us = self.monotonic_us();
        let candidate = self
            .turn_buffer
            .render_candidate(self.transcript.current_text());

        if self.turn_started_at_us.is_none()
            && (!self.transcript.current_text().is_empty() || !self.turn_buffer.is_empty())
        {
            self.turn_started_at_us = Some(now_us);
        }

        let recent_assistant = recent_assistant_text(self.audible_ledger.history());

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
                    self.commit_user_turn(&decision.reason_code).await?;
                    return Ok(());
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
        self.recover_stalled_playback(now_us).await?;
        self.settle_expired_stops(now_us);
        self.start_next_speech_if_possible().await?;
        self.resolve_settled_turns();
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
        for act in &mut self.pending_speech {
            act.speech_epoch = new_epoch;
        }

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

    fn resolve_turn(&self, task_id: TaskId, resolution: TurnResolution) {
        match &resolution {
            TurnResolution::Discard => self.metrics.inc("voice_turn_discarded_total"),
            TurnResolution::Audible { final_text, heard } => {
                self.metrics.inc("voice_turn_resolved_total");
                if heard != final_text {
                    self.metrics.inc("voice_turn_heard_partially_total");
                }
            }
        }
        self.task_agent.resolve_turn(task_id, resolution);
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
                let heard = self.heard_rendering(&turn);
                self.resolve_turn(
                    task_id,
                    TurnResolution::Audible {
                        final_text: turn.final_text,
                        heard,
                    },
                );
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
                let heard = self.heard_rendering(&turn);
                self.resolve_turn(
                    task_id,
                    TurnResolution::Audible {
                        final_text: turn.final_text,
                        heard,
                    },
                );
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
        let event_tx = self.event_tx.clone();
        let meta_factory = Arc::clone(&self.meta_factory);
        let timeout = Duration::from_millis(self.config.agents.deep.timeout_ms);

        let shutdown = self.shutdown.clone();
        self.workers.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = async {
            match tokio::time::timeout(timeout, worker.run(request)).await {
                Ok(Ok(result)) => {
                    let _ = event_tx
                        .send(VoiceEvent::DeepWorkResult {
                            meta: meta_factory.new_meta(),
                            result,
                        })
                        .await;
                }
                Ok(Err(error)) => {
                    tracing::warn!("deep work failed: {error}");
                }
                Err(_) => {
                    tracing::warn!("deep work timed out after {}ms", timeout.as_millis());
                }
            }
                } => {},
            }
        });

        self.metrics.inc("voice_deep_work_started_total");
        Ok(())
    }

    fn apply_deep_work_result(&mut self, result: DeepWorkResult) {
        if result.thought_epoch != self.state.epochs.thought {
            self.metrics.inc("voice_deep_stale_dropped_total");
            return;
        }

        self.deep_results.push(result);
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

        // Replies that already finished generating resolve exactly once with
        // the confirmed ledger outcome; no late client acknowledgement is
        // waited for here.
        let turns: Vec<(TaskId, TurnSpeech)> = self.turn_speech.drain().collect();
        for (task_id, turn) in turns {
            let heard = self.heard_rendering(&turn);
            self.resolve_turn(
                task_id,
                TurnResolution::Audible {
                    final_text: turn.final_text,
                    heard,
                },
            );
        }
    }
}

/// Classifies the answer an assistant question invites.
///
/// The interaction brain uses this to disambiguate short replies: "네" after a
/// yes/no question is an answer, not a backchannel.
fn expected_answer_type(text: &str) -> Option<crate::interaction::ExpectedAnswerType> {
    use crate::interaction::ExpectedAnswerType;

    let asks_question = text.contains('?')
        || text.contains("할까요")
        || text.contains("맞나요")
        || text.contains("드릴까요")
        || text.contains("하시겠어요")
        || text.contains("어떠세요")
        || text.contains("인가요");

    if !asks_question {
        return None;
    }

    if text.contains("몇 명") || text.contains("몇명") || text.contains("몇 시") {
        return Some(ExpectedAnswerType::Number);
    }

    if text.contains("어느") || text.contains("어떤") || text.contains("중에") {
        return Some(ExpectedAnswerType::Choice);
    }

    if text.contains("맞나요")
        || text.contains("할까요")
        || text.contains("드릴까요")
        || text.contains("하시겠어요")
    {
        return Some(ExpectedAnswerType::YesNo);
    }

    Some(ExpectedAnswerType::FreeText)
}

fn is_transactional_claim(text: &str) -> bool {
    let patterns = [
        "예약이 완료",
        "예약되었습니다",
        "취소가 완료",
        "취소되었습니다",
        "결제가 완료",
        "결제되었습니다",
        "처리되었습니다",
        "등록되었습니다",
    ];
    patterns.iter().any(|pattern| text.contains(pattern))
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
) -> TurnContext {
    let idempotency_key = format!("voice-turn-{turn_id}");
    let frame_json = serde_json::to_string(semantic_frame).unwrap_or_default();

    let dependencies: Vec<String> = semantic_frame.slots.keys().cloned().collect();
    let fingerprint = semantic_frame.dependency_fingerprint(&dependencies);

    let mut brief = format!(
        "transcript_revision: {revision}\n\
         idempotency_key: {idempotency_key}\n\
         semantic_frame: {frame_json}"
    );

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

    #[test]
    fn expected_answer_type_classifies_assistant_questions() {
        use crate::interaction::ExpectedAnswerType;

        assert_eq!(
            expected_answer_type("토요일 7시로 예약할까요?"),
            Some(ExpectedAnswerType::YesNo)
        );
        assert_eq!(
            expected_answer_type("몇 명이서 가시나요?"),
            Some(ExpectedAnswerType::Number)
        );
        assert_eq!(
            expected_answer_type("강남과 홍대 중에 어디가 좋으세요?"),
            Some(ExpectedAnswerType::Choice)
        );
        assert_eq!(expected_answer_type("두 곳이 가능합니다."), None);
    }

    #[test]
    fn transactional_claims_are_detected() {
        assert!(is_transactional_claim("예약이 완료되었습니다."));
        assert!(!is_transactional_claim("토요일 7시에 두 곳이 가능합니다."));
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
        let config = Arc::new(
            VoiceRuntimeConfig::from_yaml_str("observability:\n  event_log: false").unwrap(),
        );
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
            matches!(&recorder.resolutions()[0].1, TurnResolution::Audible { heard, .. } if heard == "[응답이 전달되지 않음]")
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
