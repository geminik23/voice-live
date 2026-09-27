use serde::{Deserialize, Serialize};

use crate::ids::SpeechId;

pub const PRIORITY_HARD_STOP_ACK: u8 = 100;
pub const PRIORITY_CLARIFICATION: u8 = 80;
pub const PRIORITY_FINAL: u8 = 60;
pub const PRIORITY_PROGRESS: u8 = 30;
pub const PRIORITY_BACKCHANNEL: u8 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimClass {
    Phatic,
    Process,
    Factual,
    Transactional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Interruptibility {
    Immediate,
    AfterClause,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    ToolStarted,
    ToolCompleted,
    ToolExecuted,
    AgentFinal,
    ActionCommit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceRef {
    pub id: String,
    pub kind: EvidenceKind,
}

impl EvidenceRef {
    pub fn new(id: impl Into<String>, kind: EvidenceKind) -> Self {
        Self {
            id: id.into(),
            kind,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeechAct {
    pub id: SpeechId,
    pub speech_epoch: u64,
    pub class: ClaimClass,
    pub text: String,
    pub priority: u8,
    pub interruptibility: Interruptibility,
    pub evidence: Vec<EvidenceRef>,
    pub expires_at_monotonic_us: Option<u64>,
}

impl SpeechAct {
    pub fn phatic(text: impl Into<String>, speech_epoch: u64, priority: u8) -> Self {
        Self {
            id: SpeechId::new(),
            speech_epoch,
            class: ClaimClass::Phatic,
            text: text.into(),
            priority,
            interruptibility: Interruptibility::Immediate,
            evidence: Vec::new(),
            expires_at_monotonic_us: None,
        }
    }
}
