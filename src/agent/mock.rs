use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use parking_lot::Mutex;

use super::{
    AgentTurnRequest, AgentTurnStream, ChannelAgentTurnStream, CognitiveAgent, CognitiveEvent,
    TurnResolution,
};
use crate::ids::TaskId;

/// What a fake agent saw and was told, for assertions.
///
/// Scenarios cannot see framework memory, so they assert on the calls the
/// supervisor makes instead: what went in as the user message, what went in
/// as turn context, and how each turn was resolved.
#[derive(Default)]
pub struct AgentRecorder {
    requests: Mutex<Vec<AgentTurnRequest>>,
    resolutions: Mutex<Vec<(TaskId, TurnResolution)>>,
}

impl AgentRecorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn requests(&self) -> Vec<AgentTurnRequest> {
        self.requests.lock().clone()
    }

    pub fn resolutions(&self) -> Vec<(TaskId, TurnResolution)> {
        self.resolutions.lock().clone()
    }
}

/// Scripted agent for tests and the mock demo mode. Each turn replays the
/// configured event script under virtual time, then ends with the configured
/// terminal event.
pub struct FakeAgent {
    script: Vec<TimedCognitiveEvent>,
    terminal: TerminalEvent,
    recorder: Arc<AgentRecorder>,
}

enum TerminalEvent {
    Final { text: String },
    Failed { message: String },
    None,
}

#[derive(Clone)]
pub struct TimedCognitiveEvent {
    pub after_ms: u64,
    pub event: CognitiveEvent,
}

impl FakeAgent {
    pub fn finalizing(text: impl Into<String>) -> Self {
        Self {
            script: Vec::new(),
            terminal: TerminalEvent::Final { text: text.into() },
            recorder: AgentRecorder::new(),
        }
    }

    pub fn failing(message: impl Into<String>) -> Self {
        Self {
            script: Vec::new(),
            terminal: TerminalEvent::Failed {
                message: message.into(),
            },
            recorder: AgentRecorder::new(),
        }
    }

    pub fn with_script(script: Vec<TimedCognitiveEvent>, final_text: impl Into<String>) -> Self {
        Self {
            script,
            terminal: TerminalEvent::Final {
                text: final_text.into(),
            },
            recorder: AgentRecorder::new(),
        }
    }

    /// Script carries its own terminal event, so no extra final is appended.
    pub fn scripted_only(script: Vec<TimedCognitiveEvent>) -> Self {
        Self {
            script,
            terminal: TerminalEvent::None,
            recorder: AgentRecorder::new(),
        }
    }

    pub fn into_arc(self) -> Arc<dyn CognitiveAgent> {
        Arc::new(self)
    }

    pub fn recorder(&self) -> Arc<AgentRecorder> {
        Arc::clone(&self.recorder)
    }
}

#[async_trait]
impl CognitiveAgent for FakeAgent {
    async fn start_turn(
        &self,
        request: AgentTurnRequest,
    ) -> anyhow::Result<Box<dyn AgentTurnStream>> {
        self.recorder.requests.lock().push(request);
        let (tx, rx) = mpsc::channel::<CognitiveEvent>(64);

        let script = self.script.clone();
        let terminal = match &self.terminal {
            TerminalEvent::Final { text } => FinalClone::Final { text: text.clone() },
            TerminalEvent::Failed { message } => FinalClone::Failed {
                message: message.clone(),
            },
            TerminalEvent::None => FinalClone::None,
        };

        tokio::spawn(async move {
            for timed in script {
                tokio::time::sleep(Duration::from_millis(timed.after_ms)).await;
                if tx.send(timed.event).await.is_err() {
                    return;
                }
            }

            match terminal {
                FinalClone::Final { text } => {
                    let _ = tx.send(CognitiveEvent::Final { text }).await;
                }
                FinalClone::Failed { message } => {
                    let _ = tx.send(CognitiveEvent::Failed { message }).await;
                }
                FinalClone::None => {}
            }
        });

        Ok(Box::new(ChannelAgentTurnStream::new(rx)))
    }

    fn resolve_turn(&self, task_id: TaskId, resolution: TurnResolution) {
        self.recorder.resolutions.lock().push((task_id, resolution));
    }
}

enum FinalClone {
    Final { text: String },
    Failed { message: String },
    None,
}
