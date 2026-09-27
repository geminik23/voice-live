use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use crate::epochs::Epochs;
use crate::events::VoiceEvent;
use crate::ids::{Revision, SessionId, SpeechId, TaskId, TurnId};
use crate::interaction::{ExpectedAnswerType, FloorState, SpeechState, TaskState};
use crate::semantics::SemanticFrame;

#[derive(Debug, Clone, Serialize)]
pub struct SessionView {
    pub session_id: SessionId,
    pub floor: FloorState,
    pub task: TaskState,
    pub speech: SpeechState,
    pub epochs: Epochs,

    pub local_vad_active: bool,
    pub user_speech_started_at_us: Option<u64>,
    pub last_user_audio_at_us: Option<u64>,

    pub current_hypothesis: String,
    pub stable_prefix: String,
    pub committed_user_turns: Vec<CommittedUserTurn>,

    pub assistant_asked_question: bool,
    pub expected_answer_type: Option<ExpectedAnswerType>,

    pub active_task_id: Option<TaskId>,
    pub active_speech_id: Option<SpeechId>,

    pub semantic_frame: SemanticFrame,
    pub evidence: HashMap<String, SessionEvidence>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommittedUserTurn {
    pub turn_id: TurnId,
    pub revision: Revision,
    pub transcript: String,
    pub committed_at_us: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionEvidence {
    ToolStarted {
        task_id: TaskId,
        tool: String,
        args: Value,
    },
    ToolCompleted {
        task_id: TaskId,
        tool: String,
        success: bool,
        output: String,
    },
    ToolExecuted {
        task_id: TaskId,
        tool: String,
        call_id: String,
        executed: bool,
    },
    AgentFinal {
        task_id: TaskId,
        text: String,
    },
    ActionCommit {
        task_id: TaskId,
        tool: String,
        success: bool,
    },
}

impl SessionView {
    pub fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            floor: FloorState::Idle,
            task: TaskState::None,
            speech: SpeechState::Silent,
            epochs: Epochs::initial(),
            local_vad_active: false,
            user_speech_started_at_us: None,
            last_user_audio_at_us: None,
            current_hypothesis: String::new(),
            stable_prefix: String::new(),
            committed_user_turns: Vec::new(),
            assistant_asked_question: false,
            expected_answer_type: None,
            active_task_id: None,
            active_speech_id: None,
            semantic_frame: SemanticFrame::default(),
            evidence: HashMap::new(),
        }
    }

    pub fn silence_ms(&self, now_us: u64) -> u64 {
        match self.last_user_audio_at_us {
            Some(last) => now_us.saturating_sub(last) / 1_000,
            None => 0,
        }
    }

    /// Pure projection: no I/O, no time sources, no policy decisions.
    pub fn apply(&mut self, event: &VoiceEvent) {
        match event {
            VoiceEvent::LocalSpeechStarted { meta, .. } => {
                self.local_vad_active = true;
                self.user_speech_started_at_us = Some(meta.monotonic_us);
                self.last_user_audio_at_us = Some(meta.monotonic_us);

                if self.speech == SpeechState::Playing || self.speech == SpeechState::Ducking {
                    self.floor = FloorState::OverlapProbe;
                } else {
                    self.floor = FloorState::UserHolding;
                }
            }

            VoiceEvent::LocalSpeechEnded { meta, .. } => {
                self.local_vad_active = false;
                self.last_user_audio_at_us = Some(meta.monotonic_us);

                if self.floor != FloorState::OverlapProbe {
                    self.floor = FloorState::UserPausing;
                }
            }

            VoiceEvent::AsrPartial {
                revision,
                hypothesis,
                ..
            } => {
                self.epochs.transcript = *revision;
                self.current_hypothesis = hypothesis.clone();
            }

            VoiceEvent::StableTranscriptChanged { stable_prefix, .. } => {
                self.stable_prefix = stable_prefix.clone();
            }

            VoiceEvent::AsrUtteranceFinal { transcript, .. } => {
                self.current_hypothesis = transcript.clone();
                self.stable_prefix = transcript.clone();
            }

            VoiceEvent::UserTurnCommitted {
                meta,
                turn_id,
                transcript_revision,
                transcript,
            } => {
                self.committed_user_turns.push(CommittedUserTurn {
                    turn_id: *turn_id,
                    revision: *transcript_revision,
                    transcript: transcript.clone(),
                    committed_at_us: meta.monotonic_us,
                });
                self.task = TaskState::Committed;
                self.floor = FloorState::Idle;
                self.current_hypothesis.clear();
                self.stable_prefix.clear();
                self.semantic_frame.commit_pending();
            }

            VoiceEvent::ToolStarted {
                task_id,
                thought_epoch,
                tool,
                args,
                ..
            } => {
                self.task = TaskState::ExecutingTool;
                let _ = thought_epoch;
                let id = format!("tool_started:{task_id}:{tool}");
                self.evidence.insert(
                    id,
                    SessionEvidence::ToolStarted {
                        task_id: *task_id,
                        tool: tool.clone(),
                        args: args.clone(),
                    },
                );
            }

            VoiceEvent::ToolCompleted {
                task_id,
                tool,
                success,
                output,
                ..
            } => {
                self.task = TaskState::Reasoning;
                let id = format!("tool_completed:{task_id}:{tool}");
                self.evidence.insert(
                    id,
                    SessionEvidence::ToolCompleted {
                        task_id: *task_id,
                        tool: tool.clone(),
                        success: *success,
                        output: output.clone(),
                    },
                );
            }

            VoiceEvent::ToolExecuted {
                task_id,
                tool,
                call_id,
                executed,
                ..
            } => {
                let id = format!("tool_executed:{task_id}:{tool}");
                self.evidence.insert(
                    id,
                    SessionEvidence::ToolExecuted {
                        task_id: *task_id,
                        tool: tool.clone(),
                        call_id: call_id.clone(),
                        executed: *executed,
                    },
                );
            }

            VoiceEvent::AgentFinal { task_id, text, .. } => {
                self.task = TaskState::Finalizing;
                let id = format!("agent_final:{task_id}");
                self.evidence.insert(
                    id,
                    SessionEvidence::AgentFinal {
                        task_id: *task_id,
                        text: text.clone(),
                    },
                );
            }

            VoiceEvent::AgentFailed { .. } => {
                self.task = TaskState::Cancelled;
            }

            VoiceEvent::PlaybackCompleted { speech_id, .. } => {
                if self.active_speech_id == Some(*speech_id) {
                    self.active_speech_id = None;
                    if self.speech == SpeechState::Playing || self.speech == SpeechState::Ducking {
                        self.speech = SpeechState::Silent;
                    }
                    if !self.local_vad_active && self.floor == FloorState::AgentHolding {
                        self.floor = FloorState::Idle;
                    }
                    self.assistant_asked_question = false;
                    self.expected_answer_type = None;
                }
            }

            VoiceEvent::PlaybackInterrupted { speech_id, .. }
                if self.active_speech_id == Some(*speech_id) =>
            {
                self.active_speech_id = None;
                self.speech = SpeechState::Aborted;
            }

            _ => {}
        }
    }
}
