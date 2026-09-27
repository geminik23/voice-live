use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::ids::TaskId;

/// Per-turn environment that is **not** part of the conversation.
///
/// It reaches the model through the system prompt, which the framework
/// rebuilds on every call and never stores, so none of it accumulates in
/// conversation memory. Only the committed transcript travels as the user
/// message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnContext {
    pub idempotency_key: String,
    /// Pre-rendered text for the system prompt. Rendered on our side rather
    /// than left to the template engine, whose map formatting is not stable
    /// and whose undefined values render silently as empty.
    pub brief: String,
}

#[derive(Debug, Clone)]
pub struct AgentTurnRequest {
    pub task_id: TaskId,
    pub thought_epoch: u64,
    /// The committed user turn, and nothing else.
    pub input: String,
    pub context: TurnContext,
    pub speculative: bool,
    pub dependencies: Vec<String>,
}

/// How a finished main turn is written back to conversation memory.
///
/// Framework memory records the generated reply before a single sample is
/// played. The supervisor knows what was actually heard and reports it here
/// once the turn's speech has settled, so memory ends up holding audible
/// truth rather than generated text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnResolution {
    /// Keep the turn. `final_text` is exactly what the agent produced;
    /// `heard` replaces it in memory.
    Audible { final_text: String, heard: String },
    /// Remove the turn entirely: user message, tool exchanges and reply.
    Discard,
}

#[derive(Debug, Clone)]
pub enum CognitiveEvent {
    Started,
    Content {
        text: String,
    },
    ToolStarted {
        tool: String,
        args: Value,
    },
    ToolCompleted {
        tool: String,
        success: bool,
        output: String,
    },
    ToolExecuted {
        tool: String,
        call_id: String,
        executed: bool,
    },
    StateTransition {
        from: Option<String>,
        to: String,
    },
    Final {
        text: String,
    },
    Failed {
        message: String,
    },
}

/// Provider-agnostic turn abstraction so the deterministic runtime never
/// depends on the concrete agent framework in tests.
#[async_trait]
pub trait CognitiveAgent: Send + Sync {
    async fn start_turn(
        &self,
        request: AgentTurnRequest,
    ) -> anyhow::Result<Box<dyn AgentTurnStream>>;

    /// Settles a main turn's conversation memory.
    ///
    /// Called at most once per main turn, from the supervisor task, so it
    /// must not block: an implementation that needs to wait for the turn to
    /// finish tearing down does so on its own task. Stateless agents keep the
    /// default no-op.
    fn resolve_turn(&self, _task_id: TaskId, _resolution: TurnResolution) {}
}

#[async_trait]
pub trait AgentTurnStream: Send {
    async fn next_event(&mut self) -> anyhow::Result<Option<CognitiveEvent>>;
}

/// Channel-backed stream shared by the framework adapter and fakes. Dropping
/// it aborts the spawned consumer, which releases the framework root-turn
/// gate through stream drop.
pub struct ChannelAgentTurnStream {
    receiver: mpsc::Receiver<CognitiveEvent>,
}

impl ChannelAgentTurnStream {
    pub fn new(receiver: mpsc::Receiver<CognitiveEvent>) -> Self {
        Self { receiver }
    }
}

#[async_trait]
impl AgentTurnStream for ChannelAgentTurnStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<CognitiveEvent>> {
        Ok(self.receiver.recv().await)
    }
}

pub mod mock;

#[cfg(feature = "framework")]
pub mod framework;
