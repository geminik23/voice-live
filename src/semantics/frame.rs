use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::Revision;

pub type SlotName = String;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SemanticFrame {
    pub intent: Option<SemanticValue<String>>,
    pub slots: HashMap<SlotName, SemanticValue<Value>>,
    /// Hash of slot names this frame was last resolved against, used to
    /// invalidate dependency-scoped speculative work.
    pub revision: Revision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticValue<T> {
    pub value: T,
    pub confidence: f32,
    pub source_revision: Revision,
    pub status: SemanticValueStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticValueStatus {
    Tentative,
    Stable,
    Committed,
    Revised,
    Invalidated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotUpdate {
    pub slot: SlotName,
    pub operation: SlotOperation,
    pub old_value: Option<Value>,
    pub new_value: Value,
    pub confidence: f32,
    pub source_revision: Revision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotOperation {
    Set,
    Replace,
    Clear,
}

impl SemanticFrame {
    /// Applies one slot update.
    ///
    /// `Clear` removes the slot: an extractor that reports a deletion must not
    /// have it silently turned into an assignment.
    pub fn set_slot(&mut self, update: SlotUpdate) {
        if update.operation == SlotOperation::Clear {
            self.slots.remove(&update.slot);
            self.revision = self.revision.next();
            return;
        }

        let previous_status = self.slots.get(&update.slot).map(|v| v.status);
        let status = match previous_status {
            Some(SemanticValueStatus::Stable | SemanticValueStatus::Committed) => {
                SemanticValueStatus::Revised
            }
            _ => SemanticValueStatus::Tentative,
        };

        self.slots.insert(
            update.slot.clone(),
            SemanticValue {
                value: update.new_value.clone(),
                confidence: update.confidence,
                source_revision: update.source_revision,
                status,
            },
        );

        self.revision = self.revision.next();
    }

    pub fn commit_pending(&mut self) {
        for value in self.slots.values_mut() {
            if value.status == SemanticValueStatus::Tentative {
                value.status = SemanticValueStatus::Committed;
            }
        }
    }

    pub fn slot_value(&self, slot: &str) -> Option<&Value> {
        self.slots.get(slot).map(|v| &v.value)
    }

    /// Sorted (slot, value) pairs for dependency fingerprints.
    #[cfg(test)]
    fn test_update(slot: &str, operation: SlotOperation, value: Value) -> SlotUpdate {
        SlotUpdate {
            slot: slot.into(),
            operation,
            old_value: None,
            new_value: value,
            confidence: 0.9,
            source_revision: Revision(1),
        }
    }

    pub fn dependency_fingerprint(&self, dependencies: &[SlotName]) -> u64 {
        let mut key = String::new();
        let mut names = dependencies.to_vec();
        names.sort();
        for name in names {
            let value = self
                .slots
                .get(&name)
                .map(|v| v.value.to_string())
                .unwrap_or_default();
            key.push_str(&name);
            key.push('=');
            key.push_str(&value);
            key.push(';');
        }

        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in key.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_removes_the_slot_instead_of_assigning_to_it() {
        let mut frame = SemanticFrame::default();
        frame.set_slot(SemanticFrame::test_update(
            "date",
            SlotOperation::Set,
            Value::String("토요일".into()),
        ));
        assert!(frame.slot_value("date").is_some());

        frame.set_slot(SemanticFrame::test_update(
            "date",
            SlotOperation::Clear,
            Value::Null,
        ));

        assert!(
            frame.slot_value("date").is_none(),
            "a cleared slot must not remain in the frame"
        );
    }

    #[test]
    fn revising_a_committed_slot_marks_it_revised() {
        let mut frame = SemanticFrame::default();
        frame.set_slot(SemanticFrame::test_update(
            "time",
            SlotOperation::Set,
            Value::String("19:00".into()),
        ));
        frame.commit_pending();
        frame.set_slot(SemanticFrame::test_update(
            "time",
            SlotOperation::Set,
            Value::String("20:00".into()),
        ));

        assert_eq!(frame.slots["time"].status, SemanticValueStatus::Revised);
    }
}
