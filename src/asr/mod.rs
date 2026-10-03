use std::time::Duration;

use async_trait::async_trait;

pub use crate::config::AsrConfig;
use crate::ids::Revision;
use crate::provider::ProviderSessionControl;
use tokio_util::sync::CancellationToken;

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
    /// Typed final from the injection source, separate from recognizer utterance identity.
    InjectedFinal {
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

    /// Whether this factory can serve the runtime duplex path.
    fn supports_duplex(&self) -> bool {
        false
    }

    /// Opens a session with independent PCM input and event output.
    ///
    /// Success means the stream is ready for use, not merely that a worker
    /// was spawned. Legacy implementations keep the default refusal, and a
    /// duplex session must never wrap a legacy serial one.
    async fn open_duplex(
        &self,
        config: AsrSessionConfig,
        limits: AsrDuplexLimits,
        cancellation: CancellationToken,
    ) -> Result<AsrDuplexSession, AsrOpenError> {
        let _ = (config, limits, cancellation);
        Err(AsrOpenError::Unsupported)
    }
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

/// Typed duplex open errors, so retry policy never string-matches messages.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum AsrOpenError {
    /// The factory does not implement duplex sessions.
    #[error("duplex asr sessions are unsupported")]
    Unsupported,
    /// The provider rejected the configuration or request. Not retried.
    #[error("asr open rejected: {0}")]
    Rejected(String),
    /// A transient transport failure. Retried with backoff.
    #[error("asr open failed transiently: {0}")]
    Transient(String),
    /// The open exceeded its deadline. Retried with backoff.
    #[error("asr open timed out: {0}")]
    TimedOut(String),
}

impl AsrOpenError {
    /// Whether the runtime may schedule another attempt after this error.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Transient(_) | Self::TimedOut(_))
    }
}

/// Bounded budgets the runtime passes to [`StreamingAsr::open_duplex`].
#[derive(Debug, Clone, Copy)]
pub struct AsrDuplexLimits {
    /// Maximum queued normalized mono PCM16 samples awaiting the writer.
    pub sample_budget: usize,
    /// Maximum queued audio/commit/finish commands awaiting the writer.
    pub command_budget: usize,
    /// Per-write deadline for provider socket writes.
    pub write_timeout: Duration,
}

/// Ownership-aware result of a nonblocking audio admission.
#[derive(Debug)]
pub enum AsrAudioPush {
    /// The bounded input queue took ownership of the samples.
    Accepted,
    /// The queue is saturated, so the samples are handed back.
    Full(Vec<i16>),
}

/// Typed input admission failures.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum AsrInputError {
    /// The input is finished or the session is closed.
    #[error("asr input closed")]
    Closed,
    /// The command queue is saturated.
    #[error("asr input queue full")]
    Full,
    /// The session was cancelled or torn down.
    #[error("asr session cancelled")]
    Cancelled,
}

/// The independent PCM input side of a duplex ASR session.
///
/// One producer drives the handle so audio and commits stay FIFO. The
/// nonblocking admissions exist for the runtime media loop; the async
/// conveniences park for queue space and are for standalone callers.
#[async_trait]
pub trait AsrAudioInput: Send {
    /// Nonblocking admission for the media loop. Enqueue is the success
    /// point; `Full` returns ownership of the samples.
    fn try_push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError>;

    /// Parks until the queue has space. Dropping the future before enqueue
    /// abandons the send.
    async fn push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError>;

    /// Nonblocking commit admission. The commit follows previously admitted
    /// audio in FIFO order.
    fn try_commit_utterance(&mut self) -> Result<(), AsrInputError>;

    /// Parks until the command queue accepts the commit.
    async fn commit_utterance(&mut self) -> Result<(), AsrInputError>;

    /// Ends PCM input; queued audio still drains. Idempotent, so a second
    /// call keeps the same finished state instead of failing.
    fn finish_input(&mut self) -> Result<(), AsrInputError>;
}

/// Cancel-safe event receiver of a duplex ASR session.
#[async_trait]
pub trait AsrEventStream: Send {
    /// `Ok(None)` means the session closed, never "idle".
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>>;
}

/// The three handles of a duplex ASR session.
pub struct AsrDuplexSession {
    pub input: Box<dyn AsrAudioInput>,
    pub events: Box<dyn AsrEventStream>,
    pub control: Box<dyn ProviderSessionControl>,
}

/// Legacy serial facade over one duplex session.
///
/// Built-in providers implement `open` on top of `open_duplex` through this
/// facade; the reverse, wrapping a serial session and calling it duplex, is
/// forbidden.
pub struct LegacyDuplexFacade {
    input: Box<dyn AsrAudioInput>,
    events: Box<dyn AsrEventStream>,
    control: Box<dyn ProviderSessionControl>,
}

impl LegacyDuplexFacade {
    pub fn new(session: AsrDuplexSession) -> Self {
        Self {
            input: session.input,
            events: session.events,
            control: session.control,
        }
    }
}

#[async_trait]
impl AsrSession for LegacyDuplexFacade {
    async fn push_audio(&mut self, pcm: &[i16]) -> anyhow::Result<()> {
        match self.input.push_audio(pcm.to_vec()).await {
            Ok(AsrAudioPush::Accepted) => Ok(()),
            Ok(AsrAudioPush::Full(_)) => anyhow::bail!("audio frame exceeds ASR admission budget"),
            Err(error) => Err(anyhow::anyhow!(error)),
        }
    }

    async fn commit_audio(&mut self) -> anyhow::Result<()> {
        self.input
            .commit_utterance()
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        self.events.next_event().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.control.close().await
    }
}

pub mod fake;
pub mod inject;
pub mod together;
