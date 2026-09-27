use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FloorState {
    #[default]
    Idle,
    UserHolding,
    UserPausing,
    AgentHolding,
    OverlapProbe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    #[default]
    None,
    Committed,
    Reasoning,
    ExecutingTool,
    Finalizing,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechState {
    #[default]
    Silent,
    Planning,
    Synthesizing,
    Playing,
    Ducking,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UserSpeechState {
    #[default]
    Uncertain,
    Incomplete,
    Complete,
    Backchannel,
    Interruption,
    Correction,
    Answer,
    SideSpeech,
    NoiseOrEcho,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionAction {
    KeepListening,
    HoldFloor,
    DuckAgent,
    ResumeAgent,
    EmitBackchannel,
    AbortSpeech,
    CommitUserTurn,
    TakeFloor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractionDecision {
    pub user_state: UserSpeechState,
    pub action: InteractionAction,
    pub confidence: f32,
    pub reason_code: String,
}

impl InteractionDecision {
    pub fn new(
        user_state: UserSpeechState,
        action: InteractionAction,
        confidence: f32,
        reason_code: impl Into<String>,
    ) -> Self {
        Self {
            user_state,
            action,
            confidence,
            reason_code: reason_code.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractionDecisionEnvelope {
    pub revision: crate::ids::Revision,
    pub interaction_epoch: u64,
    pub decision: InteractionDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionReason {
    HardStop,
    SemanticInterruption,
    SemanticCorrection,
    AnswerToQuestion,
    UserTurn,
    ProviderError,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedAnswerType {
    #[default]
    YesNo,
    Choice,
    FreeText,
    Number,
}

impl FloorState {
    pub fn as_str(&self) -> &'static str {
        match self {
            FloorState::Idle => "idle",
            FloorState::UserHolding => "user_holding",
            FloorState::UserPausing => "user_pausing",
            FloorState::AgentHolding => "agent_holding",
            FloorState::OverlapProbe => "overlap_probe",
        }
    }
}

impl TaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::None => "none",
            TaskState::Committed => "committed",
            TaskState::Reasoning => "reasoning",
            TaskState::ExecutingTool => "executing_tool",
            TaskState::Finalizing => "finalizing",
            TaskState::Completed => "completed",
            TaskState::Cancelled => "cancelled",
        }
    }
}

impl SpeechState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpeechState::Silent => "silent",
            SpeechState::Planning => "planning",
            SpeechState::Synthesizing => "synthesizing",
            SpeechState::Playing => "playing",
            SpeechState::Ducking => "ducking",
            SpeechState::Aborted => "aborted",
        }
    }
}
