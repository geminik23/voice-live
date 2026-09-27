use async_trait::async_trait;

pub use crate::config::AsrConfig;
use crate::ids::Revision;

#[derive(Debug, Clone)]
pub enum AsrEvent {
    /// A revised hypothesis for `utterance_id`.
    ///
    /// `revision` must be strictly increasing for the lifetime of the session,
    /// across utterance boundaries. The supervisor uses it as the transcript
    /// epoch, and the reconciler uses it to age the stable-prefix window, so an
    /// adapter whose provider does not supply one must synthesize a monotonic
    /// counter rather than repeating a constant.
    Partial {
        utterance_id: u64,
        revision: Revision,
        text: String,
    },
    Final {
        utterance_id: u64,
        text: String,
    },
    Error {
        recoverable: bool,
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct AsrSessionConfig {
    pub sample_rate_hz: u32,
    pub locale: String,
    pub manual_commit: bool,
}

#[async_trait]
pub trait StreamingAsr: Send + Sync {
    async fn open(&self, config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>>;
}

#[async_trait]
pub trait AsrSession: Send {
    async fn push_audio(&mut self, pcm: &[i16]) -> anyhow::Result<()>;

    /// Requests an early final for the current utterance when the provider
    /// supports manual commit.
    async fn commit_audio(&mut self) -> anyhow::Result<()>;

    /// Awaits the next event.
    ///
    /// `Ok(None)` means the session is **closed** and the caller should
    /// reconnect. A source that simply has nothing to report right now must
    /// park until it does; returning `Ok(None)` for "idle" makes a dead socket
    /// indistinguishable from a quiet one.
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>>;

    async fn close(&mut self) -> anyhow::Result<()>;
}

pub fn asr_session_config(config: &AsrConfig, sample_rate_hz: u32) -> AsrSessionConfig {
    AsrSessionConfig {
        sample_rate_hz,
        locale: config.locale.clone(),
        manual_commit: config.manual_commit,
    }
}

pub mod fake;
pub mod inject;
pub mod together;
