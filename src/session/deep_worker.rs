use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use crate::events::DeepWorkResult;
use crate::ids::TaskId;

#[derive(Debug, Clone, Serialize)]
pub struct DeepWorkRequest {
    pub task_id: TaskId,
    pub thought_epoch: u64,
    pub objective: String,
    pub committed_context: Value,
    pub dependencies: Vec<String>,
}

#[async_trait]
pub trait DeepWorker: Send + Sync {
    async fn run(&self, request: DeepWorkRequest) -> anyhow::Result<DeepWorkResult>;
}

/// Deep worker that returns a fixed summary after a fixed delay. Used by the
/// virtual-time scenarios so the host-driven deep work path stays covered.
pub struct FakeDeepWorker {
    pub summary: String,
    pub delay_ms: u64,
}

#[async_trait]
impl DeepWorker for FakeDeepWorker {
    async fn run(&self, request: DeepWorkRequest) -> anyhow::Result<DeepWorkResult> {
        tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;

        Ok(DeepWorkResult {
            task_id: request.task_id,
            thought_epoch: request.thought_epoch,
            summary: self.summary.clone(),
            dependencies: request.dependencies,
        })
    }
}
