use std::collections::VecDeque;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::interaction::snapshot::InteractionSnapshot;
use crate::interaction::{
    InteractionAction, InteractionDecision, InteractionDecisionEnvelope, UserSpeechState,
};

/// Second-opinion brain for cases the deterministic policy cannot prove.
/// Results are always validated against the policy before being applied.
#[async_trait]
pub trait InteractionBrain: Send + Sync {
    async fn decide(&self, snapshot: InteractionSnapshot) -> Option<InteractionDecisionEnvelope>;
}

/// Production fallback when no interaction agent is configured: the
/// deterministic policy alone owns every decision.
pub struct NoopInteractionBrain;

#[async_trait]
impl InteractionBrain for NoopInteractionBrain {
    async fn decide(&self, _snapshot: InteractionSnapshot) -> Option<InteractionDecisionEnvelope> {
        None
    }
}

/// Test brain that replays scripted decisions in order.
pub struct ScriptedInteractionBrain {
    queue: Mutex<VecDeque<InteractionDecisionEnvelope>>,
}

impl ScriptedInteractionBrain {
    pub fn new(decisions: Vec<InteractionDecisionEnvelope>) -> Self {
        Self {
            queue: Mutex::new(decisions.into()),
        }
    }

    pub fn push(&self, envelope: InteractionDecisionEnvelope) {
        self.queue.lock().push_back(envelope);
    }
}

#[async_trait]
impl InteractionBrain for ScriptedInteractionBrain {
    async fn decide(&self, snapshot: InteractionSnapshot) -> Option<InteractionDecisionEnvelope> {
        let mut queue = self.queue.lock();
        let mut envelope = queue.pop_front()?;

        // Re-stamp the envelope so staleness checks compare against the
        // snapshot the supervisor actually queried.
        envelope.revision = snapshot.transcript_revision;
        envelope.interaction_epoch = snapshot.interaction_epoch;
        Some(envelope)
    }
}

pub fn envelope_for_snapshot(
    snapshot: &InteractionSnapshot,
    user_state: UserSpeechState,
    action: InteractionAction,
    confidence: f32,
    reason: impl Into<String>,
) -> InteractionDecisionEnvelope {
    InteractionDecisionEnvelope {
        revision: snapshot.transcript_revision,
        interaction_epoch: snapshot.interaction_epoch,
        decision: InteractionDecision::new(user_state, action, confidence, reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Revision;

    #[tokio::test]
    async fn scripted_brain_restamps_envelopes() {
        let envelope = InteractionDecisionEnvelope {
            revision: Revision(1),
            interaction_epoch: 1,
            decision: InteractionDecision::new(
                UserSpeechState::Backchannel,
                InteractionAction::ResumeAgent,
                0.9,
                "test",
            ),
        };

        let brain = ScriptedInteractionBrain::new(vec![envelope]);
        let snapshot = InteractionSnapshot {
            transcript_revision: Revision(42),
            interaction_epoch: 7,
            ..InteractionSnapshot::default()
        };

        let result = brain.decide(snapshot).await.unwrap();
        assert_eq!(result.revision, Revision(42));
        assert_eq!(result.interaction_epoch, 7);
    }

    #[tokio::test]
    async fn noop_brain_returns_none() {
        let brain = NoopInteractionBrain;
        assert!(brain.decide(InteractionSnapshot::default()).await.is_none());
    }
}
