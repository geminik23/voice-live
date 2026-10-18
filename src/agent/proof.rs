use crate::agent::CognitiveEvent;
use crate::ids::TaskId;
use crate::speech::render::InvocationProof;
use ai_agents::hooks::AgentHooks;
use ai_agents::tools::{Tool, ToolExecutionContext, ToolExecutionRecord, ToolResult};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
struct Origin {
    task_id: TaskId,
    thought_epoch: u64,
    tx: mpsc::Sender<CognitiveEvent>,
}

#[derive(Hash, PartialEq, Eq)]
struct ExecutionKey {
    call_id: String,
    operation: String,
    source: String,
    started_at: String,
}
impl ExecutionKey {
    fn new(
        call: &str,
        operation: &str,
        source: &impl serde::Serialize,
        started_at: &chrono::DateTime<chrono::Utc>,
    ) -> Option<Self> {
        let source = serde_json::to_string(source).ok()?;
        if call.is_empty()
            || call.len() > 256
            || operation.is_empty()
            || operation.len() > 256
            || source.len() > 1024
        {
            return None;
        }
        Some(Self {
            call_id: call.into(),
            operation: operation.into(),
            source,
            started_at: started_at.to_rfc3339(),
        })
    }
}

/// Bridges executor records using immutable turn scope and request identity, never current-turn state.
#[derive(Default)]
pub struct VoiceAgentHooks {
    origins: Mutex<HashMap<String, Origin>>,
    calls: Mutex<HashMap<ExecutionKey, Option<String>>>,
}
impl VoiceAgentHooks {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub(crate) fn admit(
        &self,
        task_id: TaskId,
        thought_epoch: u64,
        tx: mpsc::Sender<CognitiveEvent>,
    ) -> anyhow::Result<String> {
        let mut origins = self.origins.lock();
        anyhow::ensure!(origins.len() < 128, "too many unresolved proof origins");
        let token = format!("voice-turn-{}", uuid::Uuid::new_v4());
        origins.insert(
            token.clone(),
            Origin {
                task_id,
                thought_epoch,
                tx,
            },
        );
        Ok(token)
    }
    pub(crate) fn release(&self, task_id: TaskId) {
        let mut origins = self.origins.lock();
        origins.retain(|_, origin| origin.task_id != task_id);
        self.calls.lock().retain(|_, token| {
            token
                .as_ref()
                .is_some_and(|token| origins.contains_key(token))
        });
    }
    /// Wraps a host tool without changing its policy, safety classification, or arguments.
    pub fn bind_tool(self: &Arc<Self>, tool: Arc<dyn Tool>) -> Arc<dyn Tool> {
        Arc::new(BoundTool {
            inner: tool,
            hooks: Arc::clone(self),
        })
    }
}

async fn emit(origin: &Origin, proof: InvocationProof) {
    let _ = origin
        .tx
        .send(CognitiveEvent::InvocationProof {
            task_id: origin.task_id,
            thought_epoch: origin.thought_epoch,
            proof,
        })
        .await;
}
fn invalid_observation() -> InvocationProof {
    InvocationProof {
        record_id: uuid::Uuid::new_v4().to_string(),
        call_id: String::new(),
        operation: String::new(),
        source: Value::Null,
        arguments: Value::Null,
        output: String::new(),
        executed: false,
        success: false,
        timed_out: false,
        cancelled: false,
        output_truncated: true,
    }
}

struct BoundTool {
    inner: Arc<dyn Tool>,
    hooks: Arc<VoiceAgentHooks>,
}
#[async_trait]
impl Tool for BoundTool {
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn description(&self) -> &str {
        self.inner.description()
    }
    fn input_schema(&self) -> Value {
        self.inner.input_schema()
    }
    fn safety_metadata(&self) -> ai_agents::tools::ToolSafetyMetadata {
        self.inner.safety_metadata()
    }
    fn policy_bindings(&self) -> ai_agents::tools::ToolPolicyBindings {
        self.inner.policy_bindings()
    }
    fn classify_call(&self, args: &Value) -> ai_agents::tools::ToolCallClassification {
        self.inner.classify_call(args)
    }
    async fn execute(&self, args: Value, ctx: ToolExecutionContext) -> ToolResult {
        let token = ctx.actor.sender_agent_id.as_ref();
        let origin = token.and_then(|token| self.hooks.origins.lock().get(token).cloned());
        if let (Some(origin), Some(token)) = (&origin, token) {
            let valid = if let Some(key) = ExecutionKey::new(
                &ctx.call_id,
                &ctx.canonical_id,
                &ctx.source,
                &ctx.started_at,
            ) {
                let mut calls = self.hooks.calls.lock();
                if let Some(existing) = calls.get_mut(&key) {
                    if existing.as_ref() != Some(token) {
                        *existing = None;
                    }
                    existing.is_some()
                } else if calls.len() < 128 {
                    calls.insert(key, Some(token.clone()));
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if !valid {
                emit(origin, invalid_observation()).await;
            }
            if ctx.canonical_id.len() <= 256 {
                let args = if args.to_string().len() <= 16384 {
                    args.clone()
                } else {
                    Value::Null
                };
                let _ = origin
                    .tx
                    .send(CognitiveEvent::ToolStarted {
                        tool: ctx.canonical_id.clone(),
                        args,
                    })
                    .await;
            }
        }
        self.inner.execute(args, ctx).await
    }
}

#[cfg(test)]
#[path = "proof_tests.rs"]
mod tests;

#[async_trait]
impl AgentHooks for VoiceAgentHooks {
    async fn on_tool_execution_record(&self, record: &ToolExecutionRecord) {
        let Some(key) = ExecutionKey::new(
            &record.call_id,
            &record.canonical_id,
            &record.source,
            &record.started_at,
        ) else {
            return;
        };
        let token = self.calls.lock().get(&key).cloned().flatten();
        let origin = token.and_then(|token| self.origins.lock().get(&token).cloned());
        let Some(origin) = origin else {
            return;
        };
        let oversized_args = record.executed_arguments.to_string().len() > 16_384;
        let oversized_output = record.output.len() > 16_384;
        let proof = InvocationProof {
            record_id: uuid::Uuid::new_v4().to_string(),
            call_id: record.call_id.clone(),
            operation: record.canonical_id.clone(),
            source: serde_json::to_value(&record.source).unwrap_or(Value::Null),
            arguments: if oversized_args {
                Value::Null
            } else {
                record.executed_arguments.clone()
            },
            output: if oversized_output {
                String::new()
            } else {
                record.output.clone()
            },
            executed: record.executed,
            success: record.success,
            timed_out: record.timed_out,
            cancelled: record.cancelled,
            output_truncated: record.output_truncated || oversized_output || oversized_args,
        };
        emit(&origin, proof).await;
        let _ = origin
            .tx
            .send(CognitiveEvent::ToolCompleted {
                tool: record.canonical_id.clone(),
                success: record.success,
                output: String::new(),
            })
            .await;
        let _ = origin
            .tx
            .send(CognitiveEvent::ToolExecuted {
                tool: record.canonical_id.clone(),
                call_id: record.call_id.clone(),
                executed: record.executed,
            })
            .await;
    }
}
