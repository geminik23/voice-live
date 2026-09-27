use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{OwnedMutexGuard, mpsc, watch};

use ai_agents::agent::{
    Agent, AgentBuilder, AgentResponse, AgentStreamEvent, RuntimeAgent, StreamChunk,
};
use ai_agents::hooks::AgentHooks;
use ai_agents::llm::Role;
use ai_agents::memory::{InMemoryStore, Memory, MemoryConfig, MemorySnapshot};
use ai_agents::tools::{Tool, ToolExecutionRecord, ToolResult};

use crate::agent::{
    AgentTurnRequest, AgentTurnStream, ChannelAgentTurnStream, CognitiveAgent, CognitiveEvent,
    TurnContext, TurnResolution,
};
use crate::ids::Revision;
use crate::ids::TaskId;
use crate::interaction::brain::InteractionBrain;
use crate::interaction::snapshot::InteractionSnapshot;
use crate::interaction::{
    InteractionAction, InteractionDecision, InteractionDecisionEnvelope, UserSpeechState,
};
use crate::semantics::frame::SemanticFrame;
use crate::semantics::{SlotOperation, SlotUpdate};

/// Hooks bridge that forwards authoritative tool execution evidence into the
/// active turn's event channel. A runtime serializes root turns, so a single
/// bridge slot per agent instance is safe.
pub struct VoiceAgentHooks {
    bridge: Mutex<HooksBridge>,
}

#[derive(Default)]
struct HooksBridge {
    tx: Option<mpsc::Sender<CognitiveEvent>>,
    task_id: Option<TaskId>,
    thought_epoch: u64,
}

impl VoiceAgentHooks {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            bridge: Mutex::new(HooksBridge::default()),
        })
    }

    fn snapshot(&self) -> (Option<mpsc::Sender<CognitiveEvent>>, Option<TaskId>, u64) {
        let bridge = self.bridge.lock();
        (bridge.tx.clone(), bridge.task_id, bridge.thought_epoch)
    }

    /// Clears the bridge only if it still belongs to `task_id`.
    ///
    /// A cancelled turn's spawned task can finish *after* the next turn has
    /// already installed its own sender. An unconditional reset would then
    /// silence the live turn's `ToolExecuted` evidence, and a transactional
    /// claim with no execution record is rejected by the claim gate: a
    /// reservation that really happened would never be spoken.
    fn release(&self, task_id: TaskId) {
        let mut bridge = self.bridge.lock();
        if bridge.task_id == Some(task_id) {
            *bridge = HooksBridge::default();
        }
    }
}

impl Default for VoiceAgentHooks {
    fn default() -> Self {
        Self {
            bridge: Mutex::new(HooksBridge::default()),
        }
    }
}

#[async_trait]
impl AgentHooks for VoiceAgentHooks {
    async fn on_tool_start(&self, tool: &str, args: &Value) {
        let (tx, _, _) = self.snapshot();
        if let Some(tx) = tx {
            let _ = tx
                .send(CognitiveEvent::ToolStarted {
                    tool: tool.to_string(),
                    args: args.clone(),
                })
                .await;
        }
    }

    async fn on_tool_complete(&self, _tool: &str, _result: &ToolResult, _duration_ms: u64) {}

    async fn on_tool_execution_record(&self, record: &ToolExecutionRecord) {
        let (tx, _, _) = self.snapshot();
        if let Some(tx) = tx {
            let _ = tx
                .send(CognitiveEvent::ToolExecuted {
                    tool: record.canonical_id.clone(),
                    call_id: record.call_id.clone(),
                    executed: record.executed,
                })
                .await;
        }
    }

    async fn on_response(&self, _response: &AgentResponse) {}
}

/// Runtime context key the adapter sets before every turn. Agent specs render
/// it as `{{ context.voice.brief }}` and `{{ context.voice.idempotency_key }}`.
///
/// Specs declare it `required: true`, which ai-agents checks at the start of
/// every turn: a key that was never set fails the turn before any model call.
/// The check is presence only, so a stale value from an earlier turn would
/// pass. The adapter therefore sets a fresh value before every root turn.
pub const VOICE_CONTEXT_KEY: &str = "voice";

/// How long a turn may take to finish tearing down before memory is left as
/// generated rather than edited while it might still be written.
const TURN_FINISH_GRACE: Duration = Duration::from_secs(10);

/// How long a new turn waits for the previous one to be resolved before it
/// abandons the resolution. Resolutions are issued on every terminal path, so
/// this only fires on a bug; it exists so a bug degrades to "memory as
/// generated" instead of a session that can never speak again.
const RESOLUTION_GRACE: Duration = Duration::from_secs(10);

/// Host-owned conversation memory with per-turn savepoints.
///
/// The framework writes the user message and the generated reply before a
/// single sample plays. This keeps a snapshot from before each turn so the turn
/// can be rolled back, and rewrites the stored reply to what was heard.
///
/// Every edit obeys three rules. It never happens during an active turn: a
/// turn holds `gate` from admission until it is resolved, and edits wait for
/// the framework stream to be dropped. It only ever restores an exact earlier
/// snapshot or rewrites the reply's text, so a native tool exchange is never
/// split. And there is one writer: resolutions for one agent run one at a time
/// under the same gate.
struct TurnMemory {
    memory: Arc<dyn Memory>,
    gate: Arc<tokio::sync::Mutex<()>>,
    pending: Mutex<HashMap<TaskId, PendingTurn>>,
}

struct PendingTurn {
    savepoint: MemorySnapshot,
    finished: watch::Receiver<bool>,
    gate: OwnedMutexGuard<()>,
}

impl TurnMemory {
    async fn admit(&self) -> anyhow::Result<(OwnedMutexGuard<()>, MemorySnapshot)> {
        let gate = match tokio::time::timeout(RESOLUTION_GRACE, Arc::clone(&self.gate).lock_owned())
            .await
        {
            Ok(gate) => gate,
            Err(_) => {
                tracing::warn!("previous turn was never resolved; keeping its memory as generated");
                self.abandon_unresolved().await;
                Arc::clone(&self.gate).lock_owned().await
            }
        };

        let savepoint = self
            .memory
            .snapshot()
            .await
            .map_err(|error| anyhow::anyhow!("memory snapshot failed: {error}"))?;

        Ok((gate, savepoint))
    }

    async fn abandon_unresolved(&self) {
        let stale: Vec<PendingTurn> = self.pending.lock().drain().map(|(_, turn)| turn).collect();
        for mut turn in stale {
            wait_finished(&mut turn.finished).await;
            drop(turn.gate);
        }
    }
}

/// True once the turn's framework stream has been dropped. A dropped sender
/// counts: the forwarder task has ended, so the stream is gone with it.
async fn wait_finished(finished: &mut watch::Receiver<bool>) -> bool {
    if *finished.borrow() {
        return true;
    }
    matches!(
        tokio::time::timeout(TURN_FINISH_GRACE, finished.wait_for(|done| *done)).await,
        Ok(Ok(_)) | Ok(Err(_))
    )
}

/// What applying a resolution did to memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionOutcome {
    /// The reply was heard in full; memory already holds it.
    Unchanged,
    /// The stored reply was replaced by what was heard.
    Rewritten,
    /// Memory was restored to its state before the turn.
    RolledBack,
    /// The reply could not be located, so memory was left as generated.
    FinalNotFound,
}

/// Writes one resolution into memory. Must only run between turns.
///
/// The reply is located by exact content, searching backwards and stopping at
/// the turn's user message. Position alone is not enough: when a state
/// transition regenerates the reply, the framework stores the stale
/// pre-transition text as an assistant message first.
pub async fn apply_resolution(
    memory: &dyn Memory,
    savepoint: MemorySnapshot,
    resolution: &TurnResolution,
) -> anyhow::Result<ResolutionOutcome> {
    match resolution {
        TurnResolution::Discard => {
            memory
                .restore(savepoint)
                .await
                .map_err(|error| anyhow::anyhow!("memory rollback failed: {error}"))?;
            Ok(ResolutionOutcome::RolledBack)
        }

        TurnResolution::Audible { final_text, heard } => {
            if heard == final_text {
                return Ok(ResolutionOutcome::Unchanged);
            }

            let mut snapshot = memory
                .snapshot()
                .await
                .map_err(|error| anyhow::anyhow!("memory snapshot failed: {error}"))?;

            let mut target = None;
            for (index, message) in snapshot.messages.iter().enumerate().rev() {
                match message.role {
                    Role::User => break,
                    Role::Assistant if message.content == *final_text => {
                        target = Some(index);
                        break;
                    }
                    _ => {}
                }
            }

            let Some(index) = target else {
                return Ok(ResolutionOutcome::FinalNotFound);
            };

            snapshot.messages[index].content = heard.clone();
            memory
                .restore(snapshot)
                .await
                .map_err(|error| anyhow::anyhow!("memory rewrite failed: {error}"))?;
            Ok(ResolutionOutcome::Rewritten)
        }
    }
}

/// Builds the conversation memory voice-live owns for an agent spec.
///
/// `AgentBuilder::memory` replaces the spec's `memory:` block outright, so its
/// `max_messages` is read here to keep the YAML meaningful. Only `in-memory`
/// is honoured; anything else is reported and replaced.
pub fn conversation_memory_for_spec(spec: &std::path::Path) -> anyhow::Result<Arc<dyn Memory>> {
    let raw = std::fs::read_to_string(spec)?;
    let value: serde_yaml::Value = serde_yaml::from_str(&raw)?;
    let config: MemoryConfig = match value.get("memory") {
        Some(memory) => serde_yaml::from_value(memory.clone())?,
        None => MemoryConfig::default(),
    };

    if config.memory_type != "in-memory" {
        tracing::warn!(
            spec = %spec.display(),
            memory_type = %config.memory_type,
            "voice-live owns conversation memory and supports only in-memory; ignoring the spec's memory type"
        );
    }

    Ok(Arc::new(InMemoryStore::new(config.max_messages)))
}

/// CognitiveAgent adapter over an ai-agents RuntimeAgent. The root-turn gate
/// serializes turns per runtime; parallel brains use separate instances.
pub struct AiAgentsCognitiveAgent {
    runtime: Arc<RuntimeAgent>,
    hooks: Arc<VoiceAgentHooks>,
    turn_memory: Option<Arc<TurnMemory>>,
}

impl AiAgentsCognitiveAgent {
    /// Memory is left entirely to the framework. For side agents whose output
    /// never reaches the user, such as speculative reads.
    pub fn new(runtime: Arc<RuntimeAgent>, hooks: Arc<VoiceAgentHooks>) -> Self {
        Self {
            runtime,
            hooks,
            turn_memory: None,
        }
    }

    /// voice-live owns conversation memory. `memory` must be the same handle
    /// given to `AgentBuilder::memory`. Turns are serialized, savepointed, and
    /// settled through [`CognitiveAgent::resolve_turn`].
    pub fn with_turn_memory(
        runtime: Arc<RuntimeAgent>,
        hooks: Arc<VoiceAgentHooks>,
        memory: Arc<dyn Memory>,
    ) -> Self {
        Self {
            runtime,
            hooks,
            turn_memory: Some(Arc::new(TurnMemory {
                memory,
                gate: Arc::new(tokio::sync::Mutex::new(())),
                pending: Mutex::new(HashMap::new()),
            })),
        }
    }

    pub fn runtime(&self) -> Arc<RuntimeAgent> {
        Arc::clone(&self.runtime)
    }

    fn set_turn_context(&self, context: &TurnContext) -> anyhow::Result<()> {
        self.runtime
            .context_manager()
            .set(
                VOICE_CONTEXT_KEY,
                serde_json::json!({
                    "brief": context.brief,
                    "idempotency_key": context.idempotency_key,
                }),
            )
            .map_err(|error| anyhow::anyhow!("setting turn context failed: {error}"))
    }
}

/// Answers "could this spec build at all?" without building it.
///
/// Used once at startup to decide fail-fast versus mock fallback. It stops
/// after LLM configuration on purpose: that is where a missing API key shows
/// up, and going further would run `auto_configure_mcp`/`auto_configure_spawner`
/// for a stack that is immediately dropped, which can leave spawned MCP
/// processes behind.
pub fn probe_agent_spec(spec: &std::path::Path) -> anyhow::Result<()> {
    AgentBuilder::from_yaml_file(spec)?.auto_configure_llms()?;
    Ok(())
}

pub async fn build_runtime_agent(
    spec: &std::path::Path,
    hooks: Arc<VoiceAgentHooks>,
) -> anyhow::Result<Arc<RuntimeAgent>> {
    build_runtime_agent_with_tools(spec, hooks, Vec::new()).await
}

pub async fn build_runtime_agent_with_tools(
    spec: &std::path::Path,
    hooks: Arc<VoiceAgentHooks>,
    tools: Vec<Arc<dyn Tool>>,
) -> anyhow::Result<Arc<RuntimeAgent>> {
    build_runtime_agent_with_memory(spec, hooks, tools, None).await
}

/// As [`build_runtime_agent_with_tools`], optionally with host-owned memory.
/// Passing `Some` replaces the spec's `memory:` block entirely.
pub async fn build_runtime_agent_with_memory(
    spec: &std::path::Path,
    hooks: Arc<VoiceAgentHooks>,
    tools: Vec<Arc<dyn Tool>>,
    memory: Option<Arc<dyn Memory>>,
) -> anyhow::Result<Arc<RuntimeAgent>> {
    let mut builder = AgentBuilder::from_yaml_file(spec)?
        .auto_configure_llms()?
        .auto_configure_features()?
        .auto_configure_mcp()
        .await?
        .auto_configure_spawner()
        .await?
        .hooks(hooks as Arc<dyn AgentHooks>);

    for tool in tools {
        builder = builder.tool(tool);
    }

    if let Some(memory) = memory {
        builder = builder.memory(memory);
    }

    Ok(Arc::new(builder.build()?))
}

#[async_trait]
impl CognitiveAgent for AiAgentsCognitiveAgent {
    async fn start_turn(
        &self,
        request: AgentTurnRequest,
    ) -> anyhow::Result<Box<dyn AgentTurnStream>> {
        // Wait for the previous turn to be fully resolved before touching
        // memory or context: neither the snapshot below nor the context write
        // acquires the framework's root gate.
        let admission = match &self.turn_memory {
            Some(turn_memory) => Some(turn_memory.admit().await?),
            None => None,
        };

        if request.context != TurnContext::default() {
            self.set_turn_context(&request.context)?;
        }

        let (tx, rx) = mpsc::channel::<CognitiveEvent>(256);
        let (finished_tx, finished_rx) = watch::channel(false);

        {
            let mut bridge = self.hooks.bridge.lock();
            bridge.tx = Some(tx.clone());
            bridge.task_id = Some(request.task_id);
            bridge.thought_epoch = request.thought_epoch;
        }

        let task_id = request.task_id;

        if let (Some(turn_memory), Some((gate, savepoint))) = (&self.turn_memory, admission) {
            turn_memory.pending.lock().insert(
                task_id,
                PendingTurn {
                    savepoint,
                    finished: finished_rx,
                    gate,
                },
            );
        }

        let runtime = Arc::clone(&self.runtime);
        let hooks = Arc::clone(&self.hooks);
        let input = request.input;

        tokio::spawn(async move {
            let stream = runtime.chat_stream_events(&input).await;

            let mut stream = match stream {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = tx
                        .send(CognitiveEvent::Failed {
                            message: error.to_string(),
                        })
                        .await;
                    hooks.release(task_id);
                    let _ = finished_tx.send(true);
                    return;
                }
            };

            let mut saw_final = false;
            let mut failure: Option<String> = None;

            loop {
                // If the consumer is gone, drop the framework stream now rather
                // than waiting for its next event: dropping it is what ends the
                // root turn, and memory cannot be settled until that happens.
                let event = tokio::select! {
                    biased;
                    _ = tx.closed() => break,
                    event = stream.next() => event,
                };
                let Some(event) = event else { break };

                match event {
                    AgentStreamEvent::Chunk(chunk) => {
                        let outgoing = match chunk {
                            StreamChunk::Content { text } => Some(CognitiveEvent::Content { text }),
                            // ToolStarted is forwarded by the hook with its
                            // complete argument object, so this chunk is a duplicate.
                            StreamChunk::ToolCallStart { .. } => None,
                            StreamChunk::ToolResult {
                                name,
                                output,
                                success,
                                ..
                            } => Some(CognitiveEvent::ToolCompleted {
                                tool: name,
                                success,
                                output,
                            }),
                            StreamChunk::StateTransition { from, to } => {
                                Some(CognitiveEvent::StateTransition { from, to })
                            }
                            StreamChunk::Error { message } => {
                                failure = Some(message);
                                None
                            }
                            _ => None,
                        };

                        if let Some(event) = outgoing
                            && tx.send(event).await.is_err()
                        {
                            break;
                        }
                    }
                    AgentStreamEvent::Final(response) => {
                        saw_final = true;
                        let _ = tx
                            .send(CognitiveEvent::Final {
                                text: response.content,
                            })
                            .await;
                        break;
                    }

                    _ => {}
                }
            }

            if !saw_final {
                let message = failure.unwrap_or_else(|| "stream ended without final".to_string());
                let _ = tx.send(CognitiveEvent::Failed { message }).await;
            }

            // Order matters: the stream must be gone (its drop guard ends the
            // root turn) before anyone is told the turn is finished.
            drop(stream);
            hooks.release(task_id);
            let _ = finished_tx.send(true);
        });

        Ok(Box::new(ChannelAgentTurnStream::new(rx)))
    }

    fn resolve_turn(&self, task_id: TaskId, resolution: TurnResolution) {
        let Some(turn_memory) = &self.turn_memory else {
            return;
        };

        let Some(turn) = turn_memory.pending.lock().remove(&task_id) else {
            tracing::debug!(%task_id, "no pending turn to resolve");
            return;
        };

        let memory = Arc::clone(&turn_memory.memory);

        tokio::spawn(async move {
            let PendingTurn {
                savepoint,
                mut finished,
                gate,
            } = turn;

            if !wait_finished(&mut finished).await {
                tracing::warn!(
                    %task_id,
                    "turn did not finish tearing down; leaving memory as generated"
                );
                drop(gate);
                return;
            }

            match apply_resolution(&*memory, savepoint, &resolution).await {
                Ok(ResolutionOutcome::FinalNotFound) => tracing::warn!(
                    %task_id,
                    "reply not found in memory; leaving memory as generated"
                ),
                Ok(outcome) => tracing::debug!(%task_id, ?outcome, "turn memory resolved"),
                Err(error) => tracing::warn!(%task_id, %error, "turn memory resolution failed"),
            }

            // Releasing the gate admits the next turn, which snapshots the
            // memory written here.
            drop(gate);
        });
    }
}

/// Interaction brain backed by a fast controller RuntimeAgent. Output is JSON
/// and validated by the host before it becomes a decision.
pub struct RuntimeInteractionBrain {
    runtime: Arc<RuntimeAgent>,
    timeout: Duration,
}

impl RuntimeInteractionBrain {
    pub fn new(runtime: Arc<RuntimeAgent>, timeout_ms: u64) -> Self {
        Self {
            runtime,
            timeout: Duration::from_millis(timeout_ms),
        }
    }
}

#[async_trait]
impl InteractionBrain for RuntimeInteractionBrain {
    async fn decide(&self, snapshot: InteractionSnapshot) -> Option<InteractionDecisionEnvelope> {
        let prompt = format!(
            "Classify the current Korean user speech state and choose one action.\n\
             Return JSON only: {{\"user_state\": \"...\", \"action\": \"...\", \"confidence\": 0.0}}\n\
             user_state: incomplete, complete, backchannel, interruption, correction, answer, side_speech, noise_or_echo, uncertain\n\
             action: keep_listening, hold_floor, duck, resume, emit_backchannel, take_floor, abort_speech, commit_turn\n\n\
             Session state:\n{}",
            serde_json::to_string_pretty(&snapshot).ok()?
        );

        let response = tokio::time::timeout(self.timeout, self.runtime.chat(&prompt))
            .await
            .ok()?
            .ok()?;

        let parsed: serde_json::Value = serde_json::from_str(response.content.trim()).ok()?;
        let user_state = parse_user_state(parsed.get("user_state")?.as_str()?)?;
        let action = parse_action(parsed.get("action")?.as_str()?)?;
        let confidence = parsed
            .get("confidence")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.5) as f32;

        Some(InteractionDecisionEnvelope {
            revision: snapshot.transcript_revision,
            interaction_epoch: snapshot.interaction_epoch,
            decision: InteractionDecision::new(user_state, action, confidence, "llm_controller"),
        })
    }
}

fn parse_user_state(raw: &str) -> Option<UserSpeechState> {
    match raw.trim().to_lowercase().as_str() {
        "incomplete" => Some(UserSpeechState::Incomplete),
        "complete" => Some(UserSpeechState::Complete),
        "backchannel" => Some(UserSpeechState::Backchannel),
        "interruption" => Some(UserSpeechState::Interruption),
        "correction" => Some(UserSpeechState::Correction),
        "answer" => Some(UserSpeechState::Answer),
        "side_speech" => Some(UserSpeechState::SideSpeech),
        "noise_or_echo" | "noise" | "echo" => Some(UserSpeechState::NoiseOrEcho),
        _ => None,
    }
}

fn parse_action(raw: &str) -> Option<InteractionAction> {
    match raw.trim().to_lowercase().as_str() {
        "keep_listening" => Some(InteractionAction::KeepListening),
        "hold_floor" | "hold" => Some(InteractionAction::HoldFloor),
        "duck" => Some(InteractionAction::DuckAgent),
        "resume" | "resume_agent" => Some(InteractionAction::ResumeAgent),
        "emit_backchannel" => Some(InteractionAction::EmitBackchannel),
        "take_floor" => Some(InteractionAction::TakeFloor),
        "abort_speech" => Some(InteractionAction::AbortSpeech),
        "commit_turn" | "commit_user_turn" => Some(InteractionAction::CommitUserTurn),
        _ => None,
    }
}

/// Stateless slot micro-agent: extracts slot updates from the stable
/// transcript prefix without tools or durable memory.
pub struct RuntimeSlotExtractor {
    runtime: Arc<RuntimeAgent>,
}

impl RuntimeSlotExtractor {
    pub fn new(runtime: Arc<RuntimeAgent>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl crate::semantics::SlotExtractor for RuntimeSlotExtractor {
    async fn extract(
        &self,
        previous_frame: &SemanticFrame,
        stable_transcript: &str,
        source_revision: Revision,
    ) -> Vec<SlotUpdate> {
        let prompt = format!(
            "Extract slot updates from Korean speech. Return JSON only:\n\
             {{\"updates\": [{{\"slot\": \"date|time|party_size|...\", \"operation\": \"set|replace|clear\", \
             \"old_value\": <optional>, \"new_value\": <value>, \"confidence\": 0.0}}]}}\n\n\
             Previous frame:\n{}\n\nStable transcript:\n{}",
            serde_json::to_string(&previous_frame.slots).unwrap_or_default(),
            stable_transcript
        );

        let Ok(Ok(response)) =
            tokio::time::timeout(Duration::from_millis(1_200), self.runtime.chat(&prompt)).await
        else {
            return Vec::new();
        };

        let Ok(value) = serde_json::from_str::<Value>(response.content.trim()) else {
            return Vec::new();
        };

        let Some(updates) = value.get("updates").and_then(|u| u.as_array()) else {
            return Vec::new();
        };

        updates
            .iter()
            .filter_map(|update| {
                let slot = update.get("slot")?.as_str()?.to_string();
                let operation = match update.get("operation")?.as_str()? {
                    "replace" => SlotOperation::Replace,
                    "clear" => SlotOperation::Clear,
                    _ => SlotOperation::Set,
                };
                let new_value = update.get("new_value")?.clone();
                let old_value = update.get("old_value").cloned();
                let confidence = update
                    .get("confidence")
                    .and_then(|c| c.as_f64())
                    .unwrap_or(0.8) as f32;

                Some(SlotUpdate {
                    slot,
                    operation,
                    old_value,
                    new_value,
                    confidence,
                    source_revision,
                })
            })
            .collect()
    }
}

/// Deep worker backed by a separate RuntimeAgent so long work never blocks
/// the media or interaction planes.
pub struct RuntimeDeepWorker {
    runtime: Arc<RuntimeAgent>,
}

impl RuntimeDeepWorker {
    pub fn new(runtime: Arc<RuntimeAgent>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl crate::session::deep_worker::DeepWorker for RuntimeDeepWorker {
    async fn run(
        &self,
        request: crate::session::deep_worker::DeepWorkRequest,
    ) -> anyhow::Result<crate::events::DeepWorkResult> {
        let input = format!(
            "<objective>{}</objective>\n<committed_context>{}</committed_context>",
            request.objective,
            serde_json::to_string(&request.committed_context).unwrap_or_default()
        );

        let response = self.runtime.chat(&input).await?;

        Ok(crate::events::DeepWorkResult {
            task_id: request.task_id,
            thought_epoch: request.thought_epoch,
            summary: response.content,
            dependencies: request.dependencies,
        })
    }
}
