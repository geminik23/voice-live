//! Minimal lifecycle contract shared by provider sessions.

use async_trait::async_trait;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Bounded-session lifecycle handle owned by the runtime or a host.
///
/// voice-live cannot force cleanup of arbitrary host tasks, so this is a
/// contract implementations must follow rather than a guarantee the trait
/// can enforce. The runtime verifies its own adapters and fakes against it.
#[async_trait]
pub trait ProviderSessionControl: Send {
    /// Requests immediate cancellation.
    ///
    /// Must not perform I/O and must not await: it only signals.
    fn cancel(&self);

    /// Closes provider-owned workers, joining them within a bounded time.
    ///
    /// Idempotent: closing an already-closed session succeeds immediately
    /// and must never hang.
    async fn close(&mut self) -> anyhow::Result<()>;
}

pub(crate) struct OwnedProviderTasks {
    pub token: CancellationToken,
    pub tasks: Vec<JoinHandle<()>>,
}

impl Drop for OwnedProviderTasks {
    fn drop(&mut self) {
        self.token.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[async_trait]
impl ProviderSessionControl for OwnedProviderTasks {
    fn cancel(&self) {
        self.token.cancel();
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.token.cancel();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(1_000);
        while let Some(task) = self.tasks.last_mut() {
            if tokio::time::timeout_at(deadline, &mut *task).await.is_err() {
                tracing::warn!("provider worker exceeded cleanup deadline; aborting");
                task.abort();
                let _ = task.await;
            }
            self.tasks.pop();
        }
        Ok(())
    }
}
