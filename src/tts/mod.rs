use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::ids::SpeechId;
use crate::provider::ProviderSessionControl;
use crate::speech::ClaimClass;

#[derive(Debug, Clone)]
pub struct TtsRequest {
    pub speech_id: SpeechId,
    pub speech_epoch: u64,
    pub text: String,
    pub language: String,
    pub voice: String,
    pub claim_class: ClaimClass,
}

#[derive(Debug, Clone)]
pub enum TtsEvent {
    Audio {
        response_id: String,
        sequence: u32,
        sample_rate: u32,
        pcm_s16le: Bytes,
    },
    AudioDone {
        response_id: String,
    },
    Error {
        recoverable: bool,
        message: String,
    },
}

/// How a provider accepts synthesis text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextInputMode {
    /// The provider synthesizes once the complete text has been submitted.
    Buffered,
    /// The provider synthesizes incrementally while text fragments arrive.
    Incremental,
}

/// Typed duplex open errors, so retry policy never string-matches messages.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum TtsOpenError {
    /// The factory does not implement text streaming sessions.
    #[error("tts text streaming is unsupported")]
    Unsupported,
    /// The provider rejected the options. Not retried.
    #[error("tts open rejected: {0}")]
    Rejected(String),
    /// A transient transport failure.
    #[error("tts open failed transiently: {0}")]
    Transient(String),
    /// The open exceeded its deadline.
    #[error("tts open timed out: {0}")]
    TimedOut(String),
}

/// Session options for one duplex synthesis response. The input text itself
/// travels through [`TtsTextInput`], not through these options.
#[derive(Debug, Clone)]
pub struct TtsSessionOptions {
    pub speech_id: SpeechId,
    pub speech_epoch: u64,
    pub language: String,
    pub voice: String,
    pub claim_class: ClaimClass,
    /// Advisory only. The emitted [`TtsEvent::Audio`] sample rate stays
    /// authoritative for the ledger and the browser.
    pub preferred_sample_rate_hz: Option<u32>,
}

/// Bounded budgets the runtime passes to [`StreamingTts::open_text_stream`].
#[derive(Debug, Clone, Copy)]
pub struct TtsInputLimits {
    /// Maximum accumulated input bytes for one response.
    pub max_input_bytes: usize,
    /// Deadline for one response after its first accepted nonempty input,
    /// or after a buffered request is admitted.
    pub request_timeout: Duration,
}

/// Typed text admission failures.
#[derive(Debug, thiserror::Error)]
pub enum TtsInputError {
    /// The accumulated text exceeded the byte budget; the fragment returns.
    #[error("tts input exceeded the byte budget")]
    TooLarge(String),
    /// A native input queue is saturated; the unaccepted fragment is returned.
    #[error("tts input queue full")]
    Full(String),
    /// `finish_input` arrived with no accumulated text.
    #[error("tts input is empty")]
    EmptyInput,
    /// Input already finished.
    #[error("tts input finished")]
    Finished,
    /// The session is closed or cancelled.
    #[error("tts input closed")]
    Closed,
}

/// The independent text input side of a duplex TTS session.
///
/// Fragments append in FIFO order and enqueue is the success point, so both
/// admissions are trivially cancel safe.
pub trait TtsTextInput: Send {
    /// Appends a text fragment.
    fn push_text(&mut self, fragment: String) -> Result<(), TtsInputError>;

    /// Ends text input; audio output drains afterwards. A second call after
    /// a successful finish keeps the finished state instead of failing.
    fn finish_input(&mut self) -> Result<(), TtsInputError>;
}

/// The three handles of a duplex TTS session.
pub struct TtsDuplexSession {
    pub input: Box<dyn TtsTextInput>,
    pub events: Box<dyn TtsStream>,
    pub control: Box<dyn ProviderSessionControl>,
}

#[async_trait]
pub trait StreamingTts: Send + Sync {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>>;

    /// The provider's text input contract. Defaults to buffered.
    fn text_input_mode(&self) -> TextInputMode {
        TextInputMode::Buffered
    }

    /// Whether [`StreamingTts::open_text_stream`] is supported.
    fn supports_text_stream(&self) -> bool {
        false
    }

    /// Opens a session with independent text input and audio output.
    ///
    /// Success means the session is ready to accept text. Legacy whole-text
    /// implementations keep the default refusal.
    async fn open_text_stream(
        &self,
        options: TtsSessionOptions,
        limits: TtsInputLimits,
        cancellation: CancellationToken,
    ) -> Result<TtsDuplexSession, TtsOpenError> {
        let _ = (options, limits, cancellation);
        Err(TtsOpenError::Unsupported)
    }

    /// Cache lookup for complete authorized requests. The default is no
    /// cache; a miss returns `None` and the caller proceeds on the normal
    /// duplex path. A failed lookup is a miss, never an error.
    async fn try_cached_synthesis(
        &self,
        request: &TtsRequest,
        cancellation: CancellationToken,
    ) -> Option<Box<dyn TtsStream>> {
        let _ = (request, cancellation);
        None
    }
}

#[async_trait]
pub trait TtsStream: Send {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>>;
}

/// Adapts a native text session to the existing whole-text output API.
/// The returned stream retains its control owner until it is dropped.
pub async fn synthesize_via_text_stream(
    provider: &dyn StreamingTts,
    request: TtsRequest,
    limits: TtsInputLimits,
    cancellation: CancellationToken,
) -> anyhow::Result<Box<dyn TtsStream>> {
    let options = TtsSessionOptions {
        speech_id: request.speech_id,
        speech_epoch: request.speech_epoch,
        language: request.language,
        voice: request.voice,
        claim_class: request.claim_class,
        preferred_sample_rate_hz: None,
    };
    let mut session = provider
        .open_text_stream(options, limits, cancellation)
        .await?;
    session.input.push_text(request.text)?;
    session.input.finish_input()?;
    Ok(Box::new(OwnedTtsStream::new(
        session.events,
        session.control,
    )))
}

struct OwnedTtsStream {
    events: Box<dyn TtsStream>,
    control: Box<dyn ProviderSessionControl>,
    pending: Option<TtsEvent>,
    closed: bool,
}

impl OwnedTtsStream {
    fn new(events: Box<dyn TtsStream>, control: Box<dyn ProviderSessionControl>) -> Self {
        Self {
            events,
            control,
            pending: None,
            closed: false,
        }
    }
}

impl Drop for OwnedTtsStream {
    fn drop(&mut self) {
        self.control.cancel();
    }
}

#[async_trait]
impl TtsStream for OwnedTtsStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.closed {
            return Ok(self.pending.take());
        }
        if self.pending.is_none() {
            let event = self.events.next_event().await?;
            if matches!(event, Some(TtsEvent::Audio { .. })) {
                return Ok(event);
            }
            self.pending = event;
        }
        // Save the terminal before awaiting cleanup so a cancelled poll cannot lose it.
        let _ = tokio::time::timeout(Duration::from_millis(1_500), self.control.close()).await;
        self.closed = true;
        Ok(self.pending.take())
    }
}

pub(crate) const MAX_RAW_PCM_BYTES: usize = 65_536;
pub(crate) const MAX_PCM_BYTES: usize = 8_192;

#[derive(Default)]
pub(crate) struct AudioNormalizer {
    response: Option<String>,
    rate: Option<u32>,
    provider_sequence: Option<u32>,
    sequence: u32,
    pub has_audio: bool,
}

impl AudioNormalizer {
    fn identity(&mut self, id: &str) -> anyhow::Result<()> {
        if let Some(response) = &self.response {
            anyhow::ensure!(response == id, "tts response identity changed");
        } else {
            self.response = Some(id.to_owned());
        }
        Ok(())
    }

    pub fn normalize(&mut self, event: TtsEvent) -> anyhow::Result<Vec<TtsEvent>> {
        match event {
            TtsEvent::Audio {
                response_id,
                sequence,
                sample_rate,
                pcm_s16le,
            } => {
                self.identity(&response_id)?;
                anyhow::ensure!(sample_rate > 0, "tts sample rate is zero");
                anyhow::ensure!(
                    pcm_s16le.len().is_multiple_of(2),
                    "tts PCM has odd byte length"
                );
                anyhow::ensure!(
                    pcm_s16le.len() <= MAX_RAW_PCM_BYTES,
                    "tts PCM exceeds raw chunk budget"
                );
                anyhow::ensure!(
                    self.rate.is_none_or(|rate| rate == sample_rate),
                    "tts sample rate changed"
                );
                anyhow::ensure!(
                    self.provider_sequence
                        .is_none_or(|previous| sequence > previous),
                    "tts sequence duplicated or reordered"
                );
                self.rate = Some(sample_rate);
                self.provider_sequence = Some(sequence);
                let mut events = Vec::new();
                for offset in (0..pcm_s16le.len()).step_by(MAX_PCM_BYTES) {
                    self.has_audio = true;
                    self.sequence = self
                        .sequence
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("tts sequence overflow"))?;
                    events.push(TtsEvent::Audio {
                        response_id: response_id.clone(),
                        sequence: self.sequence,
                        sample_rate,
                        pcm_s16le: pcm_s16le
                            .slice(offset..(offset + MAX_PCM_BYTES).min(pcm_s16le.len())),
                    });
                }
                Ok(events)
            }
            TtsEvent::AudioDone { response_id } => {
                self.identity(&response_id)?;
                anyhow::ensure!(self.has_audio, "tts produced no audio");
                Ok(vec![TtsEvent::AudioDone { response_id }])
            }
            error @ TtsEvent::Error { .. } => Ok(vec![error]),
        }
    }
}

pub mod buffered;
pub mod fake;
pub mod premade;
pub mod qwen;

#[cfg(test)]
mod tests {
    use super::*;

    fn audio(id: &str, sequence: u32, rate: u32, bytes: usize) -> TtsEvent {
        TtsEvent::Audio {
            response_id: id.into(),
            sequence,
            sample_rate: rate,
            pcm_s16le: Bytes::from(vec![0; bytes]),
        }
    }

    #[test]
    fn normalization_uses_actual_rate_and_bounded_monotonic_chunks() {
        let mut normalizer = AudioNormalizer::default();
        let chunks = normalizer
            .normalize(audio("response", 7, 16_000, 20_000))
            .unwrap();
        assert_eq!(chunks.len(), 3);
        for (index, event) in chunks.into_iter().enumerate() {
            let TtsEvent::Audio {
                sequence,
                sample_rate,
                pcm_s16le,
                ..
            } = event
            else {
                panic!("audio expected")
            };
            assert_eq!(sequence, index as u32 + 1);
            assert_eq!(sample_rate, 16_000);
            assert!(pcm_s16le.len() <= MAX_PCM_BYTES);
        }
    }

    #[test]
    fn identity_rate_and_order_changes_are_contract_errors() {
        for event in [
            audio("other", 2, 24_000, 2),
            audio("response", 2, 16_000, 2),
            audio("response", 1, 24_000, 2),
        ] {
            let mut normalizer = AudioNormalizer::default();
            normalizer
                .normalize(audio("response", 1, 24_000, 2))
                .unwrap();
            assert!(normalizer.normalize(event).is_err());
        }
    }

    #[test]
    fn empty_audio_never_establishes_a_successful_response() {
        let mut normalizer = AudioNormalizer::default();
        assert!(
            normalizer
                .normalize(audio("response", 1, 24_000, 0))
                .unwrap()
                .is_empty()
        );
        assert!(
            normalizer
                .normalize(TtsEvent::AudioDone {
                    response_id: "response".into()
                })
                .is_err()
        );
    }
}
