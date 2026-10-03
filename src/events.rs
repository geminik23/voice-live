use bytes::Bytes;
use serde_json::Value;

use crate::ids::{Revision, SpeechId, TaskId, TurnId};
use crate::interaction::{InteractionDecisionEnvelope, InterruptionReason};
use crate::meta::EventMeta;
use crate::semantics::SemanticCue;

/// Origin of a transcript, independent of provider or runtime numeric ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AsrSource {
    Audio,
    Injection,
}

/// Which candidate a derived slot update came from. `None` provenance marks
/// a host-issued update, which never rolls back on a stream reset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotProvenance {
    pub candidate_generation: u64,
    pub source_utterance_id: u64,
    pub source: AsrSource,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum VoiceEvent {
    LocalSpeechStarted {
        meta: EventMeta,
        vad_probability: f32,
    },

    LocalSpeechEnded {
        meta: EventMeta,
        duration_ms: u64,
    },

    AsrPartial {
        meta: EventMeta,
        utterance_id: u64,
        revision: Revision,
        hypothesis: String,
    },

    StableTranscriptChanged {
        meta: EventMeta,
        stable_prefix: String,
    },

    AsrUtteranceFinal {
        meta: EventMeta,
        utterance_id: u64,
        source: AsrSource,
        transcript: String,
    },

    /// The ASR connection generation restarted, so hypotheses and work
    /// derived from abandoned partials are invalid. Authoritative finals and
    /// committed turns are unaffected. Library internal; no browser wire
    /// change.
    AsrStreamReset {
        meta: EventMeta,
        generation: u64,
    },

    SemanticCue {
        meta: EventMeta,
        revision: Revision,
        cue: SemanticCue,
    },

    SemanticFrameUpdated {
        meta: EventMeta,
        revision: Revision,
        updates: Vec<crate::semantics::frame::SlotUpdate>,
        provenance: Option<SlotProvenance>,
    },

    InteractionDecision {
        meta: EventMeta,
        envelope: InteractionDecisionEnvelope,
    },

    UserTurnCommitted {
        meta: EventMeta,
        turn_id: TurnId,
        transcript_revision: Revision,
        transcript: String,
    },

    AgentStarted {
        meta: EventMeta,
        task_id: TaskId,
        thought_epoch: u64,
    },

    AgentChunk {
        meta: EventMeta,
        task_id: TaskId,
        thought_epoch: u64,
        text: String,
    },

    AgentFinal {
        meta: EventMeta,
        task_id: TaskId,
        thought_epoch: u64,
        text: String,
    },

    AgentFailed {
        meta: EventMeta,
        task_id: TaskId,
        thought_epoch: u64,
        message: String,
    },

    ToolStarted {
        meta: EventMeta,
        task_id: TaskId,
        thought_epoch: u64,
        tool: String,
        args: Value,
    },

    ToolCompleted {
        meta: EventMeta,
        task_id: TaskId,
        thought_epoch: u64,
        tool: String,
        success: bool,
        output: String,
    },

    ToolExecuted {
        meta: EventMeta,
        task_id: TaskId,
        thought_epoch: u64,
        tool: String,
        call_id: String,
        executed: bool,
    },

    TtsAudioChunk {
        meta: EventMeta,
        speech_id: SpeechId,
        speech_epoch: u64,
        sequence: u32,
        sample_rate: u32,
        pcm: Bytes,
    },

    TtsAudioDone {
        meta: EventMeta,
        speech_id: SpeechId,
        speech_epoch: u64,
    },

    TtsFailed {
        meta: EventMeta,
        speech_id: SpeechId,
        speech_epoch: u64,
        recoverable: bool,
        message: String,
    },

    PlaybackProgress {
        meta: EventMeta,
        speech_id: SpeechId,
        played_samples: u64,
    },

    PlaybackCompleted {
        meta: EventMeta,
        speech_id: SpeechId,
    },

    PlaybackInterrupted {
        meta: EventMeta,
        speech_id: SpeechId,
        played_samples: u64,
        reason: InterruptionReason,
    },

    /// Host-issued request for long-running work.
    ///
    /// Deep work has no authoritative trigger inside the media loop: nothing in
    /// the agent stream says "this will take a while". It is therefore driven
    /// from outside via `VoiceSessionHandle::request_deep_work`, and the
    /// supervisor only owns epoch scoping and result injection.
    DeepWorkRequested {
        meta: EventMeta,
        objective: String,
        dependencies: Vec<String>,
    },

    DeepWorkResult {
        meta: EventMeta,
        result: DeepWorkResult,
    },

    ProviderError {
        meta: EventMeta,
        component: String,
        recoverable: bool,
        message: String,
    },

    SessionClosed {
        meta: EventMeta,
        summary: SessionSummary,
    },
}

#[derive(Debug, Clone)]
pub struct DeepWorkResult {
    pub task_id: TaskId,
    pub thought_epoch: u64,
    pub summary: String,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionSummary {
    pub session_id: crate::ids::SessionId,
    pub floor: String,
    pub task: String,
    pub speech: String,
    pub transcript_revision: u64,
    pub thought_epoch: u64,
    pub committed_turns: usize,
    pub audible_clauses: usize,
    pub active_task_ids: Vec<TaskId>,
    pub pending_speech: usize,
    pub committed_transcripts: Vec<String>,
    pub audible_texts: Vec<String>,
}

impl VoiceEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            VoiceEvent::LocalSpeechStarted { .. } => "local_speech_started",
            VoiceEvent::LocalSpeechEnded { .. } => "local_speech_ended",
            VoiceEvent::AsrPartial { .. } => "asr_partial",
            VoiceEvent::StableTranscriptChanged { .. } => "stable_transcript_changed",
            VoiceEvent::AsrUtteranceFinal { .. } => "asr_utterance_final",
            VoiceEvent::AsrStreamReset { .. } => "asr_stream_reset",
            VoiceEvent::SemanticCue { .. } => "semantic_cue",
            VoiceEvent::SemanticFrameUpdated { .. } => "semantic_frame_updated",
            VoiceEvent::InteractionDecision { .. } => "interaction_decision",
            VoiceEvent::UserTurnCommitted { .. } => "user_turn_committed",
            VoiceEvent::AgentStarted { .. } => "agent_started",
            VoiceEvent::AgentChunk { .. } => "agent_chunk",
            VoiceEvent::AgentFinal { .. } => "agent_final",
            VoiceEvent::AgentFailed { .. } => "agent_failed",
            VoiceEvent::ToolStarted { .. } => "tool_started",
            VoiceEvent::ToolCompleted { .. } => "tool_completed",
            VoiceEvent::ToolExecuted { .. } => "tool_executed",
            VoiceEvent::TtsAudioChunk { .. } => "tts_audio_chunk",
            VoiceEvent::TtsAudioDone { .. } => "tts_audio_done",
            VoiceEvent::TtsFailed { .. } => "tts_failed",
            VoiceEvent::PlaybackProgress { .. } => "playback_progress",
            VoiceEvent::PlaybackCompleted { .. } => "playback_completed",
            VoiceEvent::PlaybackInterrupted { .. } => "playback_interrupted",
            VoiceEvent::DeepWorkRequested { .. } => "deep_work_requested",
            VoiceEvent::DeepWorkResult { .. } => "deep_work_result",
            VoiceEvent::ProviderError { .. } => "provider_error",
            VoiceEvent::SessionClosed { .. } => "session_closed",
        }
    }
}
