use serde::Serialize;

use crate::ids::Revision;
use crate::interaction::{ExpectedAnswerType, FloorState, SpeechState, TaskState};
use crate::semantics::SemanticFrame;
use crate::session::view::SessionView;

/// Read-only view of the session handed to the interaction brain. It carries
/// transcript state, floor state, and the dialog expectations the brain needs
/// to disambiguate short acknowledgements.
#[derive(Debug, Clone, Serialize)]
pub struct InteractionSnapshot {
    pub transcript_revision: Revision,
    pub interaction_epoch: u64,

    pub latest_partial: String,
    pub stable_prefix: String,
    pub turn_candidate: String,

    pub floor_state: FloorState,
    pub task_state: TaskState,
    pub speech_state: SpeechState,

    pub agent_is_speaking: bool,
    pub agent_asked_question: bool,
    pub expected_answer_type: Option<ExpectedAnswerType>,

    pub silence_ms: u64,
    pub semantic_frame: SemanticFrame,
}

impl InteractionSnapshot {
    pub fn from_view(view: &SessionView, turn_candidate: &str, now_us: u64) -> Self {
        Self {
            transcript_revision: view.epochs.transcript,
            interaction_epoch: view.epochs.interaction,
            latest_partial: view.current_hypothesis.clone(),
            stable_prefix: view.stable_prefix.clone(),
            turn_candidate: turn_candidate.to_string(),
            floor_state: view.floor,
            task_state: view.task,
            speech_state: view.speech,
            agent_is_speaking: matches!(
                view.speech,
                SpeechState::Playing | SpeechState::Ducking | SpeechState::Synthesizing
            ),
            agent_asked_question: view.assistant_asked_question,
            expected_answer_type: view.expected_answer_type,
            silence_ms: view.silence_ms(now_us),
            semantic_frame: view.semantic_frame.clone(),
        }
    }
}

impl Default for InteractionSnapshot {
    fn default() -> Self {
        Self {
            transcript_revision: Revision::default(),
            interaction_epoch: 1,
            latest_partial: String::new(),
            stable_prefix: String::new(),
            turn_candidate: String::new(),
            floor_state: FloorState::Idle,
            task_state: TaskState::None,
            speech_state: SpeechState::Silent,
            agent_is_speaking: false,
            agent_asked_question: false,
            expected_answer_type: None,
            silence_ms: 0,
            semantic_frame: SemanticFrame::default(),
        }
    }
}
