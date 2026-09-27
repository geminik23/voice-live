use thiserror::Error;

use crate::session::view::{SessionEvidence, SessionView};
use crate::speech::{ClaimClass, EvidenceKind, EvidenceRef, SpeechAct};

#[derive(Debug, Error)]
pub enum ClaimRejection {
    #[error("process claim requires a tool lifecycle evidence id")]
    ProcessWithoutToolStarted,
    #[error("factual claim requires an agent final or successful tool evidence")]
    FactualWithoutAuthoritativeEvidence,
    #[error("transactional claim requires an executed, committed side effect")]
    TransactionalWithoutActionCommit,
    #[error("evidence {0} is missing from the session")]
    MissingEvidence(String),
    #[error("act expired at {0}")]
    Expired(u64),
}

/// Speech claims are verified before synthesis. Nothing is spoken without
/// matching evidence, so "thinking out loud" can never become a lie.
#[derive(Debug, Default)]
pub struct ClaimGate;

impl ClaimGate {
    pub fn authorize(
        &self,
        act: &SpeechAct,
        view: &SessionView,
        now_us: u64,
    ) -> Result<(), ClaimRejection> {
        if let Some(expires_at) = act.expires_at_monotonic_us
            && now_us >= expires_at
        {
            return Err(ClaimRejection::Expired(expires_at));
        }

        match act.class {
            ClaimClass::Phatic => Ok(()),
            ClaimClass::Process => {
                let tool_refs: Vec<&EvidenceRef> = act
                    .evidence
                    .iter()
                    .filter(|r| matches!(r.kind, EvidenceKind::ToolStarted))
                    .collect();

                if tool_refs.is_empty() {
                    return Err(ClaimRejection::ProcessWithoutToolStarted);
                }

                for reference in tool_refs {
                    if !view.evidence.contains_key(&reference.id) {
                        return Err(ClaimRejection::MissingEvidence(reference.id.clone()));
                    }
                }

                Ok(())
            }
            ClaimClass::Factual => {
                let valid = act.evidence.iter().any(|r| match r.kind {
                    EvidenceKind::AgentFinal => matches!(
                        view.evidence.get(&r.id),
                        Some(SessionEvidence::AgentFinal { .. })
                    ),
                    EvidenceKind::ToolCompleted => matches!(
                        view.evidence.get(&r.id),
                        Some(SessionEvidence::ToolCompleted { success: true, .. })
                    ),
                    _ => false,
                });
                if valid {
                    Ok(())
                } else {
                    Err(ClaimRejection::FactualWithoutAuthoritativeEvidence)
                }
            }
            ClaimClass::Transactional => {
                let committed = act.evidence.iter().any(|r| {
                    matches!(r.kind, EvidenceKind::ActionCommit)
                        && matches!(
                            view.evidence.get(&r.id),
                            Some(SessionEvidence::ActionCommit { success: true, .. })
                        )
                });
                if committed {
                    Ok(())
                } else {
                    Err(ClaimRejection::TransactionalWithoutActionCommit)
                }
            }
        }
    }
}

pub fn action_commit_evidence(task_id: &crate::ids::TaskId, tool: &str) -> EvidenceRef {
    EvidenceRef::new(
        format!("action_commit:{task_id}:{tool}"),
        EvidenceKind::ActionCommit,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{SessionId, TaskId};
    use crate::session::view::SessionView;
    use crate::speech::{ClaimClass, EvidenceKind, Interruptibility, SpeechAct};
    use std::collections::HashMap;

    fn act(class: ClaimClass, evidence: Vec<EvidenceRef>) -> SpeechAct {
        SpeechAct {
            id: crate::ids::SpeechId::new(),
            speech_epoch: 1,
            class,
            text: "테스트".into(),
            priority: 50,
            interruptibility: Interruptibility::Immediate,
            evidence,
            expires_at_monotonic_us: None,
        }
    }

    #[test]
    fn phatic_needs_no_evidence() {
        let gate = ClaimGate;
        let view = SessionView::new(SessionId::new());

        assert!(
            gate.authorize(&act(ClaimClass::Phatic, vec![]), &view, 0)
                .is_ok()
        );
    }

    #[test]
    fn process_requires_tool_started() {
        let gate = ClaimGate;
        let mut view = SessionView::new(SessionId::new());
        let task_id = TaskId::new();

        view.evidence.insert(
            format!("tool_started:{task_id}:search"),
            SessionEvidence::ToolStarted {
                task_id,
                tool: "search".into(),
                args: serde_json::Value::Null,
            },
        );

        let missing = act(
            ClaimClass::Process,
            vec![EvidenceRef::new(
                "tool_started:none:search",
                EvidenceKind::ToolStarted,
            )],
        );
        assert!(matches!(
            gate.authorize(&missing, &view, 0),
            Err(ClaimRejection::MissingEvidence(_))
        ));

        let valid = act(
            ClaimClass::Process,
            vec![EvidenceRef::new(
                format!("tool_started:{task_id}:search"),
                EvidenceKind::ToolStarted,
            )],
        );
        assert!(gate.authorize(&valid, &view, 0).is_ok());
    }

    #[test]
    fn transactional_requires_action_commit() {
        let gate = ClaimGate;
        let mut view = SessionView::new(SessionId::new());
        let task_id = TaskId::new();

        let mut evidence = HashMap::new();
        evidence.insert(
            format!("action_commit:{task_id}:reserve"),
            SessionEvidence::ActionCommit {
                task_id,
                tool: "reserve".into(),
                success: true,
            },
        );
        view.evidence = evidence;

        let ok = act(
            ClaimClass::Transactional,
            vec![action_commit_evidence(&task_id, "reserve")],
        );
        assert!(gate.authorize(&ok, &view, 0).is_ok());

        let tool_only = act(
            ClaimClass::Transactional,
            vec![EvidenceRef::new(
                format!("tool_started:{task_id}:reserve"),
                EvidenceKind::ToolStarted,
            )],
        );
        assert!(matches!(
            gate.authorize(&tool_only, &view, 0),
            Err(ClaimRejection::TransactionalWithoutActionCommit)
        ));
    }

    #[test]
    fn expired_acts_are_dropped() {
        let gate = ClaimGate;
        let view = SessionView::new(SessionId::new());

        let mut expired = act(ClaimClass::Phatic, vec![]);
        expired.expires_at_monotonic_us = Some(100);

        assert!(matches!(
            gate.authorize(&expired, &view, 500),
            Err(ClaimRejection::Expired(100))
        ));
    }
}
