use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::{AgentTurnRequest, CognitiveAgent, TurnContext, TurnResolution};
use crate::clock::{ClockRef, TokioClock};
use crate::config::{ToolEffect, VoiceRuntimeConfig};
use crate::events::{DeepWorkResult, SessionSummary, VoiceEvent};
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
use crate::session::tts_worker::{TtsCancellationRegistry, run_tts_worker};
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
    pub speech_tx: mpsc::Sender<SpeechAct>,
    pub tts_registry: Arc<TtsCancellationRegistry>,

    pub task_agent: Arc<dyn CognitiveAgent>,
    pub speculative_agent: Option<Arc<dyn CognitiveAgent>>,
    pub interaction_brain: Arc<dyn InteractionBrain>,
    pub slot_extractor: Arc<dyn SlotExtractor>,
    pub deep_worker: Option<Arc<dyn DeepWorker>>,

    pub active_tasks: HashMap<TaskId, ActiveTask>,
    pub pending_speech: VecDeque<SpeechAct>,
    pub speech_texts: HashMap<SpeechId, (String, u32)>,
    /// Acts that declared `Interruptibility::Never`, tracked separately so the
    /// abort path can honour them without carrying the whole act around.
    uninterruptible_speech: HashSet<SpeechId>,
    /// Monotonic time of the last playback signal for the active speech. The
    /// client owes us an ACK; if it never arrives the queue must not wedge.
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
        let (speech_tx, speech_rx) = mpsc::channel::<SpeechAct>(64);
        let tts_registry = Arc::new(TtsCancellationRegistry::default());

        let cue_extractor = CueExtractor::from_config(&self.config.turn_control);
        let asr_partials = self.config.asr.partials.clone();
        let interaction_interval = self.config.agents.interaction.minimum_interval_ms;
        let audible_capacity = self.config.speech.audible_history_max_clauses;

        tokio::spawn(run_tts_worker(
            self.tts,
            speech_rx,
            event_tx.clone(),
            Arc::clone(&self.meta_factory),
            self.shutdown.clone(),
            Arc::clone(&tts_registry),
            self.config.tts.resolved_voice(),
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
            speech_texts: HashMap::new(),
            uninterruptible_speech: HashSet::new(),
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

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    self.shutdown_active_work().await;
                    break;
                }

                _ = control_tick.tick() => {
                    self.on_control_tick().await?;
                }

                event = self.event_rx.recv() => {
                    let Some(event) = event else {
                        break;
                    };
                    self.handle_event(event).await?;
                }
            }
        }

        Ok(())
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
                transcript,
                ..
            } => {
                // Text injection has no mic activity, so the final itself
                // anchors the silence clock for the commit tick.
                if self.state.last_user_audio_at_us.is_none() {
                    self.state.last_user_audio_at_us = Some(meta.monotonic_us);
                }
                self.transcript.on_final(utterance_id, &transcript);
                let sealed = self.transcript.seal_current();
                self.turn_buffer.push_final(sealed);
                self.state.current_hypothesis.clear();
                self.state.stable_prefix.clear();
                self.publish_interaction_snapshot().await;
            }

            VoiceEvent::SemanticFrameUpdated { updates, .. } => {
                self.apply_slot_updates(updates).await?;
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
                self.last_playback_signal_us = Some(self.monotonic_us());
                self.audible_ledger.mark_progress(speech_id, played_samples);
            }

            VoiceEvent::PlaybackCompleted { speech_id, .. } => {
                self.last_playback_signal_us = None;
                self.uninterruptible_speech.remove(&speech_id);
                self.audible_ledger.mark_completed(speech_id);
                self.start_next_speech_if_possible().await?;
            }

            VoiceEvent::PlaybackInterrupted {
                speech_id,
                played_samples,
                reason,
                ..
            } => {
                self.last_playback_signal_us = None;
                self.uninterruptible_speech.remove(&speech_id);
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
        Ok(())
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
        self.state
            .active_speech_id
            .is_some_and(|id| self.uninterruptible_speech.contains(&id))
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

        if let Some(speech_id) = self.state.active_speech_id.take() {
            self.tts_registry.cancel(&speech_id);
            // The cancelled synthesis emits no AudioDone, so nothing else will
            // ever retire this entry.
            self.speech_texts.remove(&speech_id);
            self.uninterruptible_speech.remove(&speech_id);
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
                    if agent_audible {
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
                    self.apply_slot_updates(vec![update]).await?;
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

        tokio::spawn(async move {
            if let Some(envelope) = brain.decide(snapshot).await {
                let _ = event_tx
                    .send(VoiceEvent::InteractionDecision {
                        meta: meta_factory.new_meta(),
                        envelope,
                    })
                    .await;
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

        tokio::spawn(async move {
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
                })
                .await;
        });
    }

    async fn apply_slot_updates(&mut self, updates: Vec<SlotUpdate>) -> Result<()> {
        let mut changed_slots: Vec<SlotName> = Vec::new();

        for update in updates {
            let is_correction = update.operation == crate::semantics::frame::SlotOperation::Replace
                || update.old_value.is_some();

            if is_correction {
                changed_slots.push(update.slot.clone());
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

        let handle = tokio::spawn(async move {
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
                handle: Some(handle),
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
        let sealed = self.transcript.seal_current();
        if !sealed.trim().is_empty() {
            self.turn_buffer.push_partial_seal(sealed);
        }

        let transcript = self.turn_buffer.take();

        if transcript.trim().is_empty() {
            return Ok(());
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
        self.backchannel_count_this_turn = 0;
        // The floor has changed hands; older floor decisions are moot.
        self.state.epochs.bump_interaction();
        self.metrics.inc("voice_user_turn_committed_total");

        self.event_tx
            .send(VoiceEvent::UserTurnCommitted {
                meta: self.new_meta(),
                turn_id,
                transcript_revision: revision,
                transcript,
            })
            .await?;

        let _ = reason;
        Ok(())
    }

    async fn start_main_task(
        &mut self,
        turn_id: TurnId,
        transcript: String,
        transcript_revision: Revision,
    ) -> Result<()> {
        // The previous turn must be settled before this one can start: the
        // agent will not admit a new turn while an earlier one is unresolved.
        self.settle_previous_turns().await?;

        let task_id = TaskId::new();
        let thought_epoch = self.state.epochs.invalidate_thought();

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
            dependencies: self.state.semantic_frame.slots.keys().cloned().collect(),
        };

        let agent = Arc::clone(&self.task_agent);
        let event_tx = self.event_tx.clone();
        let meta_factory = Arc::clone(&self.meta_factory);
        let child = cancellation.clone();
        let timeout = Duration::from_millis(self.config.agents.task.timeout_ms);

        let handle = tokio::spawn(async move {
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
                handle: Some(handle),
            },
        );

        self.state.active_task_id = Some(task_id);
        self.state.task = crate::interaction::TaskState::Reasoning;

        self.metrics.inc("voice_task_started_total");
        Ok(())
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

        self.authorize_and_enqueue(act).await?;
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

                self.active_tasks.remove(&task_id);
                self.speculative_results.push(SpeculativeResult {
                    fingerprint,
                    summary: text,
                    dependencies,
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
            if self.authorize_and_enqueue(act).await? {
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
        self.authorize_and_enqueue(act).await?;

        self.metrics.inc("voice_agent_failed_total");
        tracing::warn!("agent turn failed: {message}");
        Ok(())
    }

    // Speech queue and TTS flow.

    /// Returns whether the act was queued. A rejected act is dropped silently.
    async fn authorize_and_enqueue(&mut self, act: SpeechAct) -> Result<bool> {
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

            self.speech_texts.insert(act.id, (act.text.clone(), 0));
            self.state.active_speech_id = Some(act.id);
            self.last_playback_signal_us = Some(self.monotonic_us());

            if act.interruptibility == Interruptibility::Never {
                self.uninterruptible_speech.insert(act.id);
            }

            if self.speech_tx.send(act).await.is_err() {
                return Ok(());
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
        if speech_epoch != self.state.epochs.speech {
            self.metrics.inc("voice_stale_tts_chunk_dropped_total");
            return Ok(());
        }

        self.last_playback_signal_us = Some(self.monotonic_us());

        let first_audio = !matches!(
            self.state.speech,
            SpeechState::Playing | SpeechState::Ducking
        );

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

        let first_audio_text = self.speech_texts.get_mut(&speech_id).map(|(text, rate)| {
            *rate = sample_rate;
            text.clone()
        });

        if first_audio && let Some(text) = first_audio_text {
            self.audible_ledger
                .speech_started(speech_id, &text, sample_rate);
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
        if speech_epoch != self.state.epochs.speech {
            return Ok(());
        }

        self.speech_texts.remove(&speech_id);
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
        // A failed synthesis must never wedge the queue: reset the speech
        // state so the next act can start.
        self.speech_texts.remove(&speech_id);
        self.uninterruptible_speech.remove(&speech_id);
        self.tts_registry.cancel(&speech_id);

        if self.state.active_speech_id == Some(speech_id) {
            self.state.active_speech_id = None;
            self.state.speech = SpeechState::Silent;
            self.last_playback_signal_us = None;
        }

        self.metrics.inc("voice_tts_failed_total");
        tracing::warn!("tts failed: {message}");

        self.start_next_speech_if_possible().await?;
        Ok(())
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
        self.authorize_and_enqueue(act).await?;
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
        self.start_next_speech_if_possible().await?;
        self.resolve_settled_turns();

        Ok(())
    }

    /// Releases the speech queue when a client stops acknowledging playback.
    ///
    /// `SpeechState` only leaves `Playing` on a client ACK. A disconnected or
    /// wedged client would otherwise pin the queue for the rest of the session.
    async fn recover_stalled_playback(&mut self, now_us: u64) -> Result<()> {
        let timeout_ms = self.config.speech.playback_ack_timeout_ms;
        if timeout_ms == 0 {
            return Ok(());
        }

        if !matches!(
            self.state.speech,
            SpeechState::Synthesizing | SpeechState::Playing | SpeechState::Ducking
        ) {
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

        if let Some(speech_id) = self.state.active_speech_id {
            self.tts_registry.cancel(&speech_id);
            self.speech_texts.remove(&speech_id);
            self.uninterruptible_speech.remove(&speech_id);
            self.audible_ledger
                .mark_interrupted(speech_id, 0, InterruptionReason::ProviderError);
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

        let still_speaking = self
            .turn_speech
            .values()
            .any(|turn| turn.clauses.iter().any(|(id, _)| !self.clause_settled(*id)));

        if still_speaking {
            // Aborting moves the floor to the user, but at this point the turn
            // commit has already handed it back; keep that.
            let floor = self.state.floor;
            self.abort_current_speech(InterruptionReason::UserTurn)
                .await?;
            self.state.floor = floor;
        }

        let unresolved: Vec<TaskId> = self.turn_speech.keys().copied().collect();
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

        tokio::spawn(async move {
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
        for (_, mut task) in self.active_tasks.drain() {
            task.cancel();
        }

        for (speech_id, _) in self.speech_texts.drain() {
            self.tts_registry.cancel(&speech_id);
        }

        self.playback.command(PlaybackCommand::Shutdown).await;

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
        let _ = self.event_tx.send(event).await;
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
}
