use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::interaction::UserSpeechState;
use crate::semantics::SlotOperation;

/// Advisory interpretation; execution and speech authorization remain with the session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAssessment {
    pub state: UserSpeechState,
    #[serde(default)]
    pub complete: Option<bool>,
    #[serde(default)]
    pub stop: bool,
    #[serde(default)]
    pub negated: bool,
    #[serde(default)]
    pub intent: Option<String>,
    #[serde(default)]
    pub updates: Vec<SemanticUpdate>,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticUpdate {
    pub slot: String,
    pub operation: SlotOperation,
    #[serde(default)]
    pub old_value: Option<Value>,
    #[serde(default)]
    pub new_value: Value,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
}

fn default_confidence() -> f32 {
    0.5
}

impl SemanticAssessment {
    pub fn parse(raw: &str) -> Option<Self> {
        if raw.len() > 16_384 {
            return None;
        }
        let assessment: Self = serde_json::from_str(raw.trim()).ok()?;
        assessment.valid().then_some(assessment)
    }

    pub fn valid(&self) -> bool {
        let mut slots = std::collections::HashSet::new();
        if !(0.0..=1.0).contains(&self.confidence)
            || self.updates.len() > 32
            || self
                .intent
                .as_ref()
                .is_some_and(|intent| intent.trim().is_empty() || intent.len() > 128)
            || (self.stop
                && matches!(
                    self.state,
                    UserSpeechState::Backchannel | UserSpeechState::NoiseOrEcho
                ))
            || (self.complete == Some(true) && self.state == UserSpeechState::Incomplete)
            || (self.state == UserSpeechState::Uncertain
                && (self.stop
                    || self.negated
                    || self.intent.is_some()
                    || !self.updates.is_empty()
                    || self.complete == Some(true)))
            || self.updates.iter().any(|update| {
                !slots.insert(&update.slot)
                    || update.slot.trim().is_empty()
                    || update.slot.len() > 128
                    || !(0.0..=1.0).contains(&update.confidence)
                    || (update.operation != SlotOperation::Clear && update.new_value.is_null())
            })
        {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_operations_and_accepts_neutral_results() {
        let invalid = r#"{"state":"correction","updates":[{"slot":"date","operation":"delete","new_value":"Friday"}]}"#;
        assert!(SemanticAssessment::parse(invalid).is_none());
        let valid = r#"{"state":"uncertain","updates":[]}"#;
        assert_eq!(
            SemanticAssessment::parse(valid).unwrap().state,
            UserSpeechState::Uncertain
        );
        let contradictory = r#"{"state":"incomplete","complete":true}"#;
        assert!(SemanticAssessment::parse(contradictory).is_none());
        let uncertain_update = r#"{"state":"uncertain","updates":[{"slot":"date","operation":"set","new_value":"Friday"}]}"#;
        assert!(SemanticAssessment::parse(uncertain_update).is_none());
        let duplicate = r#"{"state":"correction","updates":[{"slot":"date","operation":"set","new_value":"A"},{"slot":"date","operation":"set","new_value":"B"}]}"#;
        assert!(SemanticAssessment::parse(duplicate).is_none());
        assert!(SemanticAssessment::parse(&"x".repeat(16_385)).is_none());
    }
}
