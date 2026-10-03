use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::mock::FakeAgent;
use crate::clock::{ClockRef, TokioClock};
use crate::config::VoiceRuntimeConfig;
use crate::events::VoiceEvent;
use crate::ids::{Revision, SpeechId, TaskId};
use crate::interaction::SpeechState;
use crate::meta::ClockMetaFactory;
use crate::playback::{PlaybackCommand, PlaybackSink, SimulatedPlaybackSink};
use crate::session::VoiceSessionBuilder;
use crate::session::task_runner::{ActiveTask, ActiveTaskKind};
use crate::tts::fake::FakeTts;

/// Scenario DSL per the implementation doc: an initial state, a timed event
/// timeline replayed in virtual time, and expectations evaluated against the
/// recorded commands, events, and final session summary.
#[derive(Debug, Clone, Deserialize)]
pub struct Scenario {
    pub name: String,

    #[serde(default)]
    pub initial: ScenarioInitial,

    pub timeline: Vec<TimedScenarioEvent>,

    #[serde(default)]
    pub expect: Vec<Expect>,

    #[serde(default = "default_settle_ms")]
    pub settle_ms: u64,

    #[serde(default = "default_agent_final")]
    pub agent_final: String,

    #[serde(default)]
    pub agent_script: Vec<TimedAgentScriptEvent>,

    #[serde(default)]
    pub config: Option<VoiceRuntimeConfig>,

    /// Chunks the fake TTS emits per act. `0` makes synthesis fail, which is
    /// how the TTS-failure recovery path is exercised.
    #[serde(default = "default_tts_chunks")]
    pub tts_chunks_per_request: usize,

    /// When false the playback sink never acknowledges, standing in for a
    /// client that vanished mid-utterance.
    #[serde(default = "default_true")]
    pub playback_acks: bool,

    /// Installs a deep worker returning this summary.
    #[serde(default)]
    pub deep_work_summary: Option<String>,

    #[serde(default = "default_deep_work_delay_ms")]
    pub deep_work_delay_ms: u64,
}

fn default_tts_chunks() -> usize {
    3
}

fn default_deep_work_delay_ms() -> u64 {
    200
}

fn default_settle_ms() -> u64 {
    2_500
}

fn default_agent_final() -> String {
    "확인했습니다. 토요일 저녁 7시 기준으로 두 곳이 가능합니다.".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimedAgentScriptEvent {
    pub after_ms: u64,
    #[serde(flatten)]
    pub event: AgentScriptEvent,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentScriptEvent {
    ToolStarted {
        tool: String,
    },
    ToolExecuted {
        tool: String,
        #[serde(default = "default_true")]
        executed: bool,
    },
    ToolCompleted {
        tool: String,
        #[serde(default = "default_true")]
        success: bool,
        #[serde(default)]
        output: Option<String>,
    },
    Final {
        text: String,
    },
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ScenarioInitial {
    pub assistant_speaking: bool,
    pub assistant_asked_question: bool,
    #[serde(default)]
    pub assistant_recent_text: Option<String>,
    #[serde(default)]
    pub semantic_frame: HashMap<String, String>,
    #[serde(default)]
    pub active_tasks: Vec<ScenarioTask>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioTask {
    pub id: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimedScenarioEvent {
    pub at_ms: u64,
    pub event: ScenarioEvent,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScenarioEvent {
    LocalSpeechStarted {
        #[serde(default = "default_vad_probability")]
        vad_probability: f32,
    },
    LocalSpeechEnded,
    AsrPartial {
        revision: u64,
        text: String,
        #[serde(default)]
        utterance_id: Option<u64>,
    },
    AsrFinal {
        text: String,
        #[serde(default)]
        utterance_id: Option<u64>,
    },
    AsrStreamReset {
        generation: u64,
    },
    InteractionDecision {
        user_state: String,
        action: String,
        confidence: f32,
    },
    ToolStarted {
        tool: String,
    },
    ToolCompleted {
        tool: String,
        #[serde(default = "default_true")]
        success: bool,
        #[serde(default)]
        output: Option<String>,
    },
    ToolExecuted {
        tool: String,
        #[serde(default = "default_true")]
        executed: bool,
    },
    AgentFinal {
        text: String,
    },
    DeepWorkRequested {
        objective: String,
        #[serde(default)]
        dependencies: Vec<String>,
    },
    DeepResult {
        summary: String,
    },
}

fn default_vad_probability() -> f32 {
    0.9
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Expect {
    Command {
        command: String,
    },
    CommandWithin {
        command: String,
        after_event: Option<String>,
        within_ms: u64,
    },
    NeverCommand {
        command: String,
    },
    Event {
        event: String,
    },
    NeverEvent {
        event: String,
    },
    UserTurnCommitted {
        contains: Option<String>,
    },
    CommittedTranscriptEquals {
        text: String,
    },
    NoUserTurnCommitted,
    CommittedTurnCount {
        exactly: usize,
    },
    TaskCancelled {
        id: String,
    },
    TaskNotCancelled {
        id: String,
    },
    ThoughtEpochIncremented,
    AudibleContains {
        text: String,
    },
    AudibleExcludes {
        text: String,
    },
    MetricAtLeast {
        name: String,
        value: u64,
    },
    MetricAtMost {
        name: String,
        value: u64,
    },
    /// Passes if any resolution reported to the agent matches every given
    /// predicate. `heard_*` predicates apply to audible resolutions only.
    TurnResolved {
        resolution: ResolutionKind,
        #[serde(default)]
        heard_equals: Option<String>,
        #[serde(default)]
        heard_contains: Option<String>,
        #[serde(default)]
        heard_excludes: Option<String>,
        /// The reply was heard in full, so memory keeps it unchanged.
        #[serde(default)]
        heard_equals_final: Option<bool>,
    },
    TurnResolutionCount {
        exactly: usize,
    },
    /// The user message of every main turn equals this text.
    AgentInputEquals {
        text: String,
    },
    /// The turn context of every main turn contains this text.
    AgentContextContains {
        text: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionKind {
    Audible,
    Discard,
}

#[derive(Debug, Clone)]
pub struct ScenarioReport {
    pub name: String,
    pub passed: bool,
    pub failures: Vec<String>,
    pub commands: Vec<RecordedCommand>,
    pub events: Vec<RecordedEvent>,
    pub summary: Option<crate::events::SessionSummary>,
}

#[derive(Debug, Clone)]
pub struct RecordedCommand {
    pub kind: String,
    pub at_us: u64,
}

#[derive(Debug, Clone)]
pub struct RecordedEvent {
    pub kind: String,
    pub at_us: u64,
}

struct RecordingPlaybackSink {
    inner: Arc<dyn PlaybackSink>,
    clock: ClockRef,
    recorder: mpsc::UnboundedSender<RecordedCommand>,
}

#[async_trait::async_trait]
impl PlaybackSink for RecordingPlaybackSink {
    async fn command(&self, command: PlaybackCommand) {
        let _ = self.recorder.send(RecordedCommand {
            kind: command.kind().to_string(),
            at_us: self.clock.now_us(),
        });
        self.inner.command(command).await;
    }
}

pub struct ScenarioRunner;

impl ScenarioRunner {
    pub fn load_yaml(raw: &str) -> anyhow::Result<Scenario> {
        Ok(serde_yaml::from_str(raw)?)
    }

    pub async fn run(scenario: &Scenario) -> ScenarioReport {
        let config = Arc::new(VoiceRuntimeConfig::default());
        Self::run_with_config(scenario, config).await
    }

    pub async fn run_with_config(
        scenario: &Scenario,
        base_config: Arc<VoiceRuntimeConfig>,
    ) -> ScenarioReport {
        let config = Arc::new(match &scenario.config {
            Some(overlay) => {
                let mut merged = (*base_config).clone();
                merged.session = overlay.session.clone();
                merged.audio = overlay.audio.clone();
                merged.asr = overlay.asr.clone();
                merged.tts = overlay.tts.clone();
                merged.turn_control = overlay.turn_control.clone();
                merged.agents = overlay.agents.clone();
                merged.speech = overlay.speech.clone();
                merged.tools = overlay.tools.clone();
                merged.vad = overlay.vad.clone();
                merged.echo = overlay.echo.clone();
                merged.observability = overlay.observability.clone();
                merged.dev = overlay.dev.clone();
                merged
            }
            None => (*base_config).clone(),
        });
        let base_config = config;
        let clock: crate::clock::ClockRef = Arc::new(TokioClock::new());
        let meta_factory: Arc<dyn crate::meta::MetaFactory> =
            Arc::new(ClockMetaFactory::new(Arc::clone(&clock)));

        let (sim_tx, mut sim_rx) = mpsc::channel::<VoiceEvent>(4096);
        let (trace_tx, mut trace_rx) = mpsc::channel::<VoiceEvent>(4096);
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<RecordedCommand>();

        let inner_sink: Arc<dyn PlaybackSink> = if scenario.playback_acks {
            Arc::new(SimulatedPlaybackSink::spawn(
                sim_tx,
                Arc::clone(&meta_factory),
            ))
        } else {
            drop(sim_tx);
            Arc::new(crate::playback::UnacknowledgedPlaybackSink)
        };

        let playback: Arc<dyn PlaybackSink> = Arc::new(RecordingPlaybackSink {
            inner: inner_sink,
            clock: Arc::clone(&clock),
            recorder: command_tx,
        });

        // The scenario harness wires TTS the same way the runtime assembly
        // does: a buffered whole-text fake goes through the duplex bridge.
        let raw_tts: Arc<dyn crate::tts::StreamingTts> =
            Arc::new(FakeTts::new(24_000, 60, scenario.tts_chunks_per_request));
        let tts = crate::tts::buffered::BufferedTtsAdapter::new(raw_tts);
        let fake = if scenario.agent_script.is_empty() {
            FakeAgent::finalizing(scenario.agent_final.clone())
        } else {
            let script: Vec<crate::agent::mock::TimedCognitiveEvent> = scenario
                .agent_script
                .iter()
                .map(|timed| crate::agent::mock::TimedCognitiveEvent {
                    after_ms: timed.after_ms,
                    event: map_agent_script_event(&timed.event),
                })
                .collect();
            FakeAgent::scripted_only(script)
        };
        let recorder = fake.recorder();
        let agent: Arc<dyn crate::agent::CognitiveAgent> = Arc::new(fake);

        let metrics = crate::metrics::Metrics::new();

        let shutdown = CancellationToken::new();
        let mut builder = VoiceSessionBuilder::new(base_config, playback, tts, agent)
            .with_clock(Arc::clone(&clock))
            .with_trace(trace_tx)
            .with_metrics(Arc::clone(&metrics))
            .with_shutdown(shutdown.clone());

        if let Some(summary) = &scenario.deep_work_summary {
            builder =
                builder.with_deep_worker(Arc::new(crate::session::deep_worker::FakeDeepWorker {
                    summary: summary.clone(),
                    delay_ms: scenario.deep_work_delay_ms,
                }));
        }

        let (mut session, handle) = builder.build();

        // Playback ACKs from the simulator flow into the session channel.
        let session_event_tx = handle.event_tx.clone();
        tokio::spawn(async move {
            while let Some(event) = sim_rx.recv().await {
                if session_event_tx.send(event).await.is_err() {
                    break;
                }
            }
        });

        // Initial state.
        if scenario.initial.assistant_speaking {
            session.state.speech = SpeechState::Playing;
            let speech_id = SpeechId::new();
            session.state.active_speech_id = Some(speech_id);
            session.state.floor = crate::interaction::FloorState::AgentHolding;
        }

        if scenario.initial.assistant_asked_question {
            session.state.assistant_asked_question = true;
        }

        if let Some(recent_text) = &scenario.initial.assistant_recent_text {
            let speech_id = SpeechId::new();
            session
                .audible_ledger
                .speech_started(speech_id, recent_text, 24_000);
            session.audible_ledger.mark_progress(speech_id, 24_000);
            session.audible_ledger.mark_completed(speech_id);
        }

        for (slot, value) in &scenario.initial.semantic_frame {
            session
                .state
                .semantic_frame
                .set_slot(crate::semantics::frame::SlotUpdate {
                    slot: slot.clone(),
                    operation: crate::semantics::frame::SlotOperation::Set,
                    old_value: None,
                    new_value: serde_json::Value::String(value.clone()),
                    confidence: 0.95,
                    source_revision: Revision(1),
                });
        }

        let mut task_tokens: HashMap<String, (TaskId, CancellationToken)> = HashMap::new();
        let mut thought_epoch_start = session.state.epochs.thought;

        for scenario_task in &scenario.initial.active_tasks {
            let task_id = TaskId::new();
            let cancellation = CancellationToken::new();
            task_tokens.insert(scenario_task.id.clone(), (task_id, cancellation.clone()));
            session.active_tasks.insert(
                task_id,
                ActiveTask {
                    id: task_id,
                    kind: ActiveTaskKind::SpeculativeRead,
                    thought_epoch: session.state.epochs.thought,
                    dependencies: scenario_task.dependencies.clone(),
                    fingerprint: 0,
                    cancellation,
                    handle: None,
                    candidate_generation: 0,
                    candidate_utterance_id: None,
                },
            );
        }

        thought_epoch_start = thought_epoch_start.max(1);

        let session_task = tokio::spawn(async move { session.run().await });

        // Feed the timeline in virtual time.
        let start = tokio::time::Instant::now();
        let mut timeline = scenario.timeline.clone();
        timeline.sort_by_key(|timed| timed.at_ms);
        let mut utterance_source = ScenarioUtteranceSource::default();

        for timed in timeline {
            let target = start + Duration::from_millis(timed.at_ms);
            tokio::time::sleep_until(target).await;

            let event = map_scenario_event(&timed.event, &meta_factory, &mut utterance_source);
            if let Some(event) = event {
                let _ = handle.try_emit(event);
            }
        }

        tokio::time::sleep(Duration::from_millis(scenario.settle_ms)).await;

        // Capture task cancellation state before shutdown, which legitimately
        // cancels every remaining task.
        let task_states: HashMap<String, bool> = task_tokens
            .iter()
            .map(|(id, (_, token))| (id.clone(), token.is_cancelled()))
            .collect();

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), session_task).await;

        // Drain.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut commands = Vec::new();
        while let Ok(record) = command_rx.try_recv() {
            commands.push(record);
        }

        let mut events = Vec::new();
        let mut summary = None;
        while let Ok(event) = trace_rx.try_recv() {
            if let VoiceEvent::SessionClosed { summary: s, .. } = &event {
                summary = Some(s.clone())
            }
            events.push(RecordedEvent {
                kind: event.kind().to_string(),
                at_us: event_meta_us(&event),
            });
        }

        let failures = evaluate(
            scenario,
            &Observed {
                commands: &commands,
                events: &events,
                summary: &summary,
                task_states: &task_states,
                thought_epoch_start,
                metrics: &metrics,
                recorder: &recorder,
            },
        );

        ScenarioReport {
            name: scenario.name.clone(),
            passed: failures.is_empty(),
            failures,
            commands,
            events,
            summary,
        }
    }
}

fn event_meta_us(event: &VoiceEvent) -> u64 {
    match event {
        VoiceEvent::LocalSpeechStarted { meta, .. } => meta.monotonic_us,
        VoiceEvent::LocalSpeechEnded { meta, .. } => meta.monotonic_us,
        VoiceEvent::AsrPartial { meta, .. } => meta.monotonic_us,
        VoiceEvent::StableTranscriptChanged { meta, .. } => meta.monotonic_us,
        VoiceEvent::AsrUtteranceFinal { meta, .. } => meta.monotonic_us,
        VoiceEvent::AsrStreamReset { meta, .. } => meta.monotonic_us,
        VoiceEvent::SemanticCue { meta, .. } => meta.monotonic_us,
        VoiceEvent::SemanticFrameUpdated { meta, .. } => meta.monotonic_us,
        VoiceEvent::InteractionDecision { meta, .. } => meta.monotonic_us,
        VoiceEvent::UserTurnCommitted { meta, .. } => meta.monotonic_us,
        VoiceEvent::AgentStarted { meta, .. } => meta.monotonic_us,
        VoiceEvent::AgentChunk { meta, .. } => meta.monotonic_us,
        VoiceEvent::AgentFinal { meta, .. } => meta.monotonic_us,
        VoiceEvent::AgentFailed { meta, .. } => meta.monotonic_us,
        VoiceEvent::ToolStarted { meta, .. } => meta.monotonic_us,
        VoiceEvent::ToolCompleted { meta, .. } => meta.monotonic_us,
        VoiceEvent::ToolExecuted { meta, .. } => meta.monotonic_us,
        VoiceEvent::TtsAudioChunk { meta, .. } => meta.monotonic_us,
        VoiceEvent::TtsAudioDone { meta, .. } => meta.monotonic_us,
        VoiceEvent::TtsFailed { meta, .. } => meta.monotonic_us,
        VoiceEvent::PlaybackProgress { meta, .. } => meta.monotonic_us,
        VoiceEvent::PlaybackCompleted { meta, .. } => meta.monotonic_us,
        VoiceEvent::PlaybackInterrupted { meta, .. } => meta.monotonic_us,
        VoiceEvent::DeepWorkRequested { meta, .. } => meta.monotonic_us,
        VoiceEvent::DeepWorkResult { meta, .. } => meta.monotonic_us,
        VoiceEvent::ProviderError { meta, .. } => meta.monotonic_us,
        VoiceEvent::SessionClosed { meta, .. } => meta.monotonic_us,
    }
}

/// Tracks recognizer identity independently of a VAD end, including late finals.
/// Explicit DSL ids override automatic assignment for identity regressions.
#[derive(Default)]
struct ScenarioUtteranceSource {
    current: u64,
    boundary_pending: bool,
    seen_asr: bool,
}

fn map_scenario_event(
    event: &ScenarioEvent,
    meta_factory: &Arc<dyn crate::meta::MetaFactory>,
    utterance_source: &mut ScenarioUtteranceSource,
) -> Option<VoiceEvent> {
    let meta = meta_factory.new_meta();

    if matches!(
        event,
        ScenarioEvent::AsrPartial { .. } | ScenarioEvent::AsrFinal { .. }
    ) {
        utterance_source.seen_asr = true;
    }
    Some(match event {
        ScenarioEvent::LocalSpeechStarted { vad_probability } => {
            if utterance_source.seen_asr {
                utterance_source.boundary_pending = true;
                utterance_source.seen_asr = false;
            }
            VoiceEvent::LocalSpeechStarted {
                meta,
                vad_probability: *vad_probability,
            }
        }
        ScenarioEvent::LocalSpeechEnded => VoiceEvent::LocalSpeechEnded {
            meta,
            duration_ms: 400,
        },
        ScenarioEvent::AsrPartial {
            revision,
            text,
            utterance_id,
        } => {
            if utterance_source.boundary_pending {
                utterance_source.current += 1;
                utterance_source.boundary_pending = false;
            }
            VoiceEvent::AsrPartial {
                meta,
                utterance_id: utterance_id.unwrap_or(utterance_source.current),
                revision: Revision(*revision),
                hypothesis: text.clone(),
            }
        }
        ScenarioEvent::AsrFinal { text, utterance_id } => {
            if utterance_source.boundary_pending {
                utterance_source.current += 1;
                utterance_source.boundary_pending = false;
            }
            let utterance_id = utterance_id.unwrap_or(utterance_source.current);
            utterance_source.boundary_pending = true;
            VoiceEvent::AsrUtteranceFinal {
                meta,
                utterance_id,
                source: crate::events::AsrSource::Audio,
                transcript: text.clone(),
            }
        }
        ScenarioEvent::AsrStreamReset { generation } => VoiceEvent::AsrStreamReset {
            meta,
            generation: *generation,
        },
        ScenarioEvent::InteractionDecision {
            user_state,
            action,
            confidence,
        } => VoiceEvent::InteractionDecision {
            meta,
            envelope: crate::interaction::InteractionDecisionEnvelope {
                revision: Revision(1),
                interaction_epoch: 1,
                decision: crate::interaction::InteractionDecision::new(
                    parse_user_state(user_state),
                    parse_action(action),
                    *confidence,
                    "scenario",
                ),
            },
        },
        ScenarioEvent::ToolStarted { tool } => {
            let task_id = TaskId::new();
            VoiceEvent::ToolStarted {
                meta,
                task_id,
                thought_epoch: 1,
                tool: tool.clone(),
                args: serde_json::Value::Null,
            }
        }
        ScenarioEvent::ToolCompleted {
            tool,
            success,
            output,
        } => {
            let task_id = TaskId::new();
            VoiceEvent::ToolCompleted {
                meta,
                task_id,
                thought_epoch: 1,
                tool: tool.clone(),
                success: *success,
                output: output.clone().unwrap_or_default(),
            }
        }
        ScenarioEvent::ToolExecuted { tool, executed } => {
            let task_id = TaskId::new();
            VoiceEvent::ToolExecuted {
                meta,
                task_id,
                thought_epoch: 1,
                tool: tool.clone(),
                call_id: format!("scenario-{tool}"),
                executed: *executed,
            }
        }
        ScenarioEvent::AgentFinal { text } => {
            let task_id = TaskId::new();
            VoiceEvent::AgentFinal {
                meta,
                task_id,
                thought_epoch: 1,
                text: text.clone(),
            }
        }
        ScenarioEvent::DeepWorkRequested {
            objective,
            dependencies,
        } => VoiceEvent::DeepWorkRequested {
            meta,
            objective: objective.clone(),
            dependencies: dependencies.clone(),
        },
        ScenarioEvent::DeepResult { summary } => VoiceEvent::DeepWorkResult {
            meta,
            result: crate::events::DeepWorkResult {
                task_id: TaskId::new(),
                thought_epoch: 1,
                summary: summary.clone(),
                dependencies: Vec::new(),
            },
        },
    })
}

fn map_agent_script_event(event: &AgentScriptEvent) -> crate::agent::CognitiveEvent {
    match event {
        AgentScriptEvent::ToolStarted { tool } => crate::agent::CognitiveEvent::ToolStarted {
            tool: tool.clone(),
            args: serde_json::Value::Null,
        },
        AgentScriptEvent::ToolExecuted { tool, executed } => {
            crate::agent::CognitiveEvent::ToolExecuted {
                tool: tool.clone(),
                call_id: format!("script-{tool}"),
                executed: *executed,
            }
        }
        AgentScriptEvent::ToolCompleted {
            tool,
            success,
            output,
        } => crate::agent::CognitiveEvent::ToolCompleted {
            tool: tool.clone(),
            success: *success,
            output: output.clone().unwrap_or_default(),
        },
        AgentScriptEvent::Final { text } => {
            crate::agent::CognitiveEvent::Final { text: text.clone() }
        }
    }
}

fn parse_user_state(raw: &str) -> crate::interaction::UserSpeechState {
    match raw.trim().to_lowercase().as_str() {
        "incomplete" => crate::interaction::UserSpeechState::Incomplete,
        "complete" => crate::interaction::UserSpeechState::Complete,
        "backchannel" => crate::interaction::UserSpeechState::Backchannel,
        "interruption" => crate::interaction::UserSpeechState::Interruption,
        "correction" => crate::interaction::UserSpeechState::Correction,
        "answer" => crate::interaction::UserSpeechState::Answer,
        "side_speech" => crate::interaction::UserSpeechState::SideSpeech,
        "noise_or_echo" | "noise" | "echo" => crate::interaction::UserSpeechState::NoiseOrEcho,
        _ => crate::interaction::UserSpeechState::Uncertain,
    }
}

fn parse_action(raw: &str) -> crate::interaction::InteractionAction {
    match raw.trim().to_lowercase().as_str() {
        "keep_listening" => crate::interaction::InteractionAction::KeepListening,
        "hold_floor" | "hold" => crate::interaction::InteractionAction::HoldFloor,
        "duck" => crate::interaction::InteractionAction::DuckAgent,
        "resume" | "resume_agent" => crate::interaction::InteractionAction::ResumeAgent,
        "emit_backchannel" => crate::interaction::InteractionAction::EmitBackchannel,
        "take_floor" => crate::interaction::InteractionAction::TakeFloor,
        "abort_speech" => crate::interaction::InteractionAction::AbortSpeech,
        "commit_turn" | "commit_user_turn" => crate::interaction::InteractionAction::CommitUserTurn,
        _ => crate::interaction::InteractionAction::KeepListening,
    }
}

/// Everything the runner recorded, for evaluating expectations.
struct Observed<'a> {
    commands: &'a [RecordedCommand],
    events: &'a [RecordedEvent],
    summary: &'a Option<crate::events::SessionSummary>,
    task_states: &'a HashMap<String, bool>,
    thought_epoch_start: u64,
    metrics: &'a crate::metrics::Metrics,
    recorder: &'a crate::agent::mock::AgentRecorder,
}

fn evaluate(scenario: &Scenario, observed: &Observed<'_>) -> Vec<String> {
    let Observed {
        commands,
        events,
        summary,
        task_states,
        thought_epoch_start,
        metrics,
        recorder,
    } = *observed;

    let mut failures = Vec::new();

    for expect in &scenario.expect {
        match expect {
            Expect::Command { command } => {
                if !commands.iter().any(|record| record.kind == *command) {
                    failures.push(format!("expected command '{command}' was never issued"));
                }
            }

            Expect::CommandWithin {
                command,
                after_event,
                within_ms,
            } => {
                let anchor_us = after_event
                    .as_ref()
                    .and_then(|name| events.iter().find(|event| event.kind == *name))
                    .map(|event| event.at_us)
                    .unwrap_or(0);

                let matched = commands.iter().any(|record| {
                    record.kind == *command
                        && record.at_us >= anchor_us
                        && record.at_us.saturating_sub(anchor_us) <= within_ms * 1_000
                });

                if !matched {
                    failures.push(format!(
                        "expected command '{command}' within {within_ms}ms of '{:?}'",
                        after_event.clone().unwrap_or_else(|| "start".into())
                    ));
                }
            }

            Expect::NeverCommand { command } => {
                if commands.iter().any(|record| record.kind == *command) {
                    failures.push(format!("forbidden command '{command}' was issued"));
                }
            }

            Expect::Event { event } => {
                if !events.iter().any(|record| record.kind == *event) {
                    failures.push(format!("expected event '{event}' never occurred"));
                }
            }

            Expect::NeverEvent { event } => {
                if events.iter().any(|record| record.kind == *event) {
                    failures.push(format!("forbidden event '{event}' occurred"));
                }
            }

            Expect::UserTurnCommitted { contains } => {
                let Some(summary) = summary else {
                    failures.push("session closed without a summary".into());
                    continue;
                };

                let matched = summary.committed_transcripts.iter().any(|transcript| {
                    contains
                        .as_ref()
                        .map(|needle| transcript.contains(needle))
                        .unwrap_or(true)
                });

                if !matched {
                    failures.push(format!(
                        "no committed turn contained '{:?}'",
                        contains.clone().unwrap_or_default()
                    ));
                }
            }

            Expect::CommittedTranscriptEquals { text } => {
                let Some(summary) = summary else {
                    failures.push("session closed without a summary".into());
                    continue;
                };

                if summary.committed_transcripts != [text.clone()] {
                    failures.push(format!(
                        "committed transcripts were {:?}, expected exactly {:?}",
                        summary.committed_transcripts,
                        [text]
                    ));
                }
            }

            Expect::NoUserTurnCommitted => {
                if let Some(summary) = summary
                    && !summary.committed_transcripts.is_empty()
                {
                    failures.push(format!(
                        "unexpected committed turns: {:?}",
                        summary.committed_transcripts
                    ));
                }
            }

            Expect::CommittedTurnCount { exactly } => {
                let Some(summary) = summary else {
                    failures.push("session closed without a summary".into());
                    continue;
                };
                if summary.committed_transcripts.len() != *exactly {
                    failures.push(format!(
                        "committed turn count was {}, expected {exactly}",
                        summary.committed_transcripts.len()
                    ));
                }
            }

            Expect::TaskCancelled { id } => {
                let cancelled = task_states.get(id).copied().unwrap_or(false);
                if !cancelled {
                    failures.push(format!("task '{id}' was not cancelled"));
                }
            }

            Expect::TaskNotCancelled { id } => {
                let cancelled = task_states.get(id).copied().unwrap_or(true);
                if cancelled {
                    failures.push(format!("task '{id}' was cancelled"));
                }
            }

            Expect::ThoughtEpochIncremented => {
                let Some(summary) = summary else {
                    failures.push("session closed without a summary".into());
                    continue;
                };
                if summary.thought_epoch <= thought_epoch_start {
                    failures.push(format!(
                        "thought epoch not incremented: {} <= {thought_epoch_start}",
                        summary.thought_epoch
                    ));
                }
            }

            Expect::AudibleContains { text } => {
                let Some(summary) = summary else {
                    failures.push("session closed without a summary".into());
                    continue;
                };
                if !summary
                    .audible_texts
                    .iter()
                    .any(|spoken| spoken.contains(text))
                {
                    failures.push(format!("audible history lacks '{text}'"));
                }
            }

            Expect::AudibleExcludes { text } => {
                let Some(summary) = summary else {
                    failures.push("session closed without a summary".into());
                    continue;
                };
                if summary
                    .audible_texts
                    .iter()
                    .any(|spoken| spoken.contains(text))
                {
                    failures.push(format!("audible history contains '{text}' but must not"));
                }
            }

            Expect::MetricAtLeast { name, value } => {
                let observed = metrics.counter(name);
                if observed < *value {
                    failures.push(format!(
                        "metric '{name}' was {observed}, expected at least {value}"
                    ));
                }
            }

            Expect::MetricAtMost { name, value } => {
                let observed = metrics.counter(name);
                if observed > *value {
                    failures.push(format!(
                        "metric '{name}' was {observed}, expected at most {value}"
                    ));
                }
            }

            Expect::TurnResolved {
                resolution,
                heard_equals,
                heard_contains,
                heard_excludes,
                heard_equals_final,
            } => {
                use crate::agent::TurnResolution;

                let resolutions = recorder.resolutions();
                let matched = resolutions.iter().any(|(_, observed)| match observed {
                    TurnResolution::Discard => *resolution == ResolutionKind::Discard,
                    TurnResolution::Audible { final_text, heard } => {
                        *resolution == ResolutionKind::Audible
                            && heard_equals.as_ref().is_none_or(|t| heard == t)
                            && heard_contains.as_ref().is_none_or(|t| heard.contains(t))
                            && heard_excludes.as_ref().is_none_or(|t| !heard.contains(t))
                            && heard_equals_final.is_none_or(|want| (heard == final_text) == want)
                    }
                });

                if !matched {
                    failures.push(format!(
                        "no {resolution:?} resolution matched; observed {resolutions:?}"
                    ));
                }
            }

            Expect::TurnResolutionCount { exactly } => {
                let observed = recorder.resolutions().len();
                if observed != *exactly {
                    failures.push(format!(
                        "{observed} turn resolutions, expected {exactly}: {:?}",
                        recorder.resolutions()
                    ));
                }
            }

            Expect::AgentInputEquals { text } => {
                let main: Vec<_> = recorder
                    .requests()
                    .into_iter()
                    .filter(|request| !request.speculative)
                    .collect();
                if main.is_empty() {
                    failures.push("no main turn reached the agent".into());
                }
                for request in main {
                    if request.input != *text {
                        failures.push(format!(
                            "agent input was {:?}, expected {text:?}",
                            request.input
                        ));
                    }
                }
            }

            Expect::AgentContextContains { text } => {
                let main: Vec<_> = recorder
                    .requests()
                    .into_iter()
                    .filter(|request| !request.speculative)
                    .collect();
                if main.is_empty() {
                    failures.push("no main turn reached the agent".into());
                }
                for request in main {
                    if !request.context.brief.contains(text.as_str()) {
                        failures.push(format!(
                            "turn context {:?} lacks {text:?}",
                            request.context.brief
                        ));
                    }
                }
            }
        }
    }

    failures
}
