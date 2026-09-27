use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::agent::{AgentTurnRequest, CognitiveAgent, CognitiveEvent};
use crate::events::VoiceEvent;
use crate::ids::TaskId;
use crate::meta::MetaFactory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveTaskKind {
    MainTurn,
    SpeculativeRead,
    DeepWorker,
}

pub struct ActiveTask {
    pub id: TaskId,
    pub kind: ActiveTaskKind,
    pub thought_epoch: u64,
    pub dependencies: Vec<String>,
    pub fingerprint: u64,
    pub cancellation: CancellationToken,
    pub handle: Option<JoinHandle<()>>,
}

impl ActiveTask {
    pub fn cancel(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Consumes one agent turn and forwards lifecycle events into the session
/// channel. Dropping the stream aborts the underlying framework turn and
/// releases its root gate.
pub async fn run_task_turn_with_timeout(
    agent: Arc<dyn CognitiveAgent>,
    request: AgentTurnRequest,
    cancellation: CancellationToken,
    event_tx: tokio::sync::mpsc::Sender<VoiceEvent>,
    meta_factory: Arc<dyn MetaFactory>,
    timeout: std::time::Duration,
) {
    let task_id = request.task_id;
    let thought_epoch = request.thought_epoch;
    let timeout_tx = event_tx.clone();
    let timeout_meta = Arc::clone(&meta_factory);

    if tokio::time::timeout(
        timeout,
        run_task_turn(agent, request, cancellation, event_tx, meta_factory),
    )
    .await
    .is_err()
    {
        let _ = timeout_tx
            .send(VoiceEvent::AgentFailed {
                meta: timeout_meta.new_meta(),
                task_id,
                thought_epoch,
                message: format!("agent turn timed out after {}ms", timeout.as_millis()),
            })
            .await;
    }
}

pub async fn run_task_turn(
    agent: Arc<dyn CognitiveAgent>,
    request: AgentTurnRequest,
    cancellation: CancellationToken,
    event_tx: tokio::sync::mpsc::Sender<VoiceEvent>,
    meta_factory: Arc<dyn MetaFactory>,
) {
    if cancellation.is_cancelled() {
        return;
    }

    let task_id = request.task_id;
    let thought_epoch = request.thought_epoch;

    let stream = match agent.start_turn(request).await {
        Ok(stream) => stream,
        Err(error) => {
            let _ = event_tx
                .send(VoiceEvent::AgentFailed {
                    meta: meta_factory.new_meta(),
                    task_id,
                    thought_epoch,
                    message: error.to_string(),
                })
                .await;
            return;
        }
    };

    let mut stream = stream;

    let _ = event_tx
        .send(VoiceEvent::AgentStarted {
            meta: meta_factory.new_meta(),
            task_id,
            thought_epoch,
        })
        .await;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                return;
            }

            event = stream.next_event() => {
                let event = match event {
                    Ok(Some(event)) => event,
                    Ok(None) => {
                        let _ = event_tx
                            .send(VoiceEvent::AgentFailed {
                                meta: meta_factory.new_meta(),
                                task_id,
                                thought_epoch,
                                message: "stream ended without final".into(),
                            })
                            .await;
                        return;
                    }
                    Err(error) => {
                        let _ = event_tx
                            .send(VoiceEvent::AgentFailed {
                                meta: meta_factory.new_meta(),
                                task_id,
                                thought_epoch,
                                message: error.to_string(),
                            })
                            .await;
                        return;
                    }
                };

                let forwarded = match event {
                    CognitiveEvent::Started => None,
                    CognitiveEvent::Content { text } => Some(VoiceEvent::AgentChunk {
                        meta: meta_factory.new_meta(),
                        task_id,
                        thought_epoch,
                        text,
                    }),
                    CognitiveEvent::ToolStarted { tool, args } => Some(VoiceEvent::ToolStarted {
                        meta: meta_factory.new_meta(),
                        task_id,
                        thought_epoch,
                        tool,
                        args,
                    }),
                    CognitiveEvent::ToolCompleted {
                        tool,
                        success,
                        output,
                    } => Some(VoiceEvent::ToolCompleted {
                        meta: meta_factory.new_meta(),
                        task_id,
                        thought_epoch,
                        tool,
                        success,
                        output,
                    }),
                    CognitiveEvent::ToolExecuted {
                        tool,
                        call_id,
                        executed,
                    } => Some(VoiceEvent::ToolExecuted {
                        meta: meta_factory.new_meta(),
                        task_id,
                        thought_epoch,
                        tool,
                        call_id,
                        executed,
                    }),
                    CognitiveEvent::StateTransition { .. } => None,
                    CognitiveEvent::Final { text } => Some(VoiceEvent::AgentFinal {
                        meta: meta_factory.new_meta(),
                        task_id,
                        thought_epoch,
                        text,
                    }),
                    CognitiveEvent::Failed { message } => Some(VoiceEvent::AgentFailed {
                        meta: meta_factory.new_meta(),
                        task_id,
                        thought_epoch,
                        message,
                    }),
                };

                match forwarded {
                    Some(event) => {
                        let is_terminal = matches!(
                            event,
                            VoiceEvent::AgentFinal { .. } | VoiceEvent::AgentFailed { .. }
                        );

                        if event_tx.send(event).await.is_err() {
                            return;
                        }

                        if is_terminal {
                            return;
                        }
                    }
                    None => continue,
                }
            }
        }
    }
}
