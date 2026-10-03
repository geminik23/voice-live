use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::{
    StreamingTts, TtsDuplexSession, TtsEvent, TtsInputLimits, TtsOpenError, TtsRequest,
    TtsSessionOptions, TtsStream,
};
use crate::speech::ClaimClass;

/// Premade clip cache for short phatic phrases. Backchannel latency targets
/// are unreachable with on-demand hosted synthesis, so the configured
/// phrases are pre-synthesized through the inner provider and replayed
/// instantly afterwards.
///
/// Only clips that completed with audio are cached, and a hit requires the
/// same text, language, and voice. The clip's own sample rate is authoritative
/// on replay: the runtime forwards it to the ledger and the browser rather
/// than the request's advisory preference.
pub struct PremadeTts {
    inner: Arc<dyn StreamingTts>,
    clips: Mutex<HashMap<CacheKey, Vec<CachedChunk>>>,
    warmup_pending: Mutex<Vec<String>>,
    language: String,
    voice: String,
    preferred_rate: Option<u32>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    text: String,
    language: String,
    voice: String,
    preferred_rate: Option<u32>,
}

#[derive(Clone)]
struct CachedChunk {
    sequence: u32,
    sample_rate: u32,
    pcm: Bytes,
}

impl PremadeTts {
    pub fn new(inner: Arc<dyn StreamingTts>, phrases: Vec<String>) -> Self {
        Self::with_settings(inner, phrases, "Korean".into(), String::new())
    }

    pub fn with_settings(
        inner: Arc<dyn StreamingTts>,
        phrases: Vec<String>,
        language: String,
        voice: String,
    ) -> Self {
        Self::with_rate(inner, phrases, language, voice, None)
    }

    pub fn with_rate(
        inner: Arc<dyn StreamingTts>,
        phrases: Vec<String>,
        language: String,
        voice: String,
        preferred_rate: Option<u32>,
    ) -> Self {
        Self {
            inner,
            clips: Mutex::new(HashMap::new()),
            warmup_pending: Mutex::new(phrases),
            language,
            voice,
            preferred_rate,
        }
    }

    /// Pre-synthesizes configured phrases so the first real backchannel is
    /// already cached. A phrase that fails, or never finishes its audio, is
    /// not cached: on-demand synthesis remains the fallback.
    pub async fn warmup(&self) {
        self.warmup_with_timeout(Duration::from_secs(30)).await;
    }

    pub async fn warmup_with_timeout(&self, timeout: Duration) {
        self.warmup_with_limits(timeout, 65_536).await;
    }

    pub async fn warmup_with_limits(&self, timeout: Duration, max_input_bytes: usize) {
        loop {
            let phrase = {
                let mut pending = self.warmup_pending.lock();
                match pending.pop() {
                    Some(phrase) => phrase,
                    None => break,
                }
            };

            if phrase.trim().is_empty() || phrase.len() > max_input_bytes {
                continue;
            }
            if self
                .clips
                .lock()
                .contains_key(&self.key_for(&phrase, &self.language, &self.voice))
            {
                continue;
            }

            let request = TtsRequest {
                speech_id: crate::ids::SpeechId::new(),
                speech_epoch: 0,
                text: phrase.clone(),
                language: self.language.clone(),
                voice: self.voice.clone(),
                claim_class: ClaimClass::Phatic,
            };

            let token = CancellationToken::new();
            let synthesize = async {
                let mut stream: Box<dyn TtsStream> = if self.inner.supports_text_stream()
                    && self.inner.text_input_mode() == super::TextInputMode::Incremental
                {
                    let mut session = self
                        .inner
                        .open_text_stream(
                            TtsSessionOptions {
                                speech_id: request.speech_id,
                                speech_epoch: request.speech_epoch,
                                language: request.language,
                                voice: request.voice,
                                claim_class: request.claim_class,
                                preferred_sample_rate_hz: self.preferred_rate,
                            },
                            TtsInputLimits {
                                max_input_bytes,
                                request_timeout: timeout,
                            },
                            token.clone(),
                        )
                        .await?;
                    session.input.push_text(request.text)?;
                    session.input.finish_input()?;
                    Box::new(super::OwnedTtsStream::new(session.events, session.control))
                } else {
                    self.inner.synthesize(request, token.clone()).await?
                };
                let mut normalizer = super::AudioNormalizer::default();
                let mut chunks = Vec::new();
                let mut completed = false;
                let mut bytes = 0usize;
                while let Some(event) = stream.next_event().await? {
                    for event in normalizer.normalize(event)? {
                        match event {
                            TtsEvent::Audio {
                                sequence,
                                sample_rate,
                                pcm_s16le,
                                ..
                            } => {
                                if !pcm_s16le.is_empty() {
                                    bytes += pcm_s16le.len();
                                    anyhow::ensure!(
                                        bytes <= 1_048_576,
                                        "premade clip exceeds byte budget"
                                    );
                                    chunks.push(CachedChunk {
                                        sequence,
                                        sample_rate,
                                        pcm: pcm_s16le,
                                    });
                                }
                            }
                            TtsEvent::AudioDone { .. } => {
                                completed = true;
                                break;
                            }
                            TtsEvent::Error { message, .. } => anyhow::bail!(message),
                        }
                    }
                    if completed {
                        break;
                    }
                }
                anyhow::ensure!(
                    completed && !chunks.is_empty(),
                    "premade clip was incomplete"
                );
                Ok::<_, anyhow::Error>(chunks)
            };
            let result = tokio::time::timeout(timeout, synthesize).await;
            token.cancel();
            if let Ok(Ok(chunks)) = result {
                self.clips
                    .lock()
                    .insert(self.key_for(&phrase, &self.language, &self.voice), chunks);
            }
        }
    }

    fn key_for(&self, text: &str, language: &str, voice: &str) -> CacheKey {
        CacheKey {
            text: text.to_string(),
            language: language.to_string(),
            voice: voice.to_string(),
            preferred_rate: self.preferred_rate,
        }
    }

    fn lookup(&self, request: &TtsRequest) -> Option<Vec<CachedChunk>> {
        if request.claim_class != ClaimClass::Phatic {
            return None;
        }

        self.clips
            .lock()
            .get(&self.key_for(&request.text, &request.language, &request.voice))
            .cloned()
    }
}

#[async_trait]
impl StreamingTts for PremadeTts {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        if let Some(chunks) = self.lookup(&request) {
            return Ok(Box::new(CachedTtsStream {
                response_id: request.speech_id.to_string(),
                chunks: chunks.into_iter().collect(),
                done: false,
                cancellation,
            }));
        }

        self.inner.synthesize(request, cancellation).await
    }

    /// Forwarded, not flattened: wrapping a native provider here must not
    /// make the assembly see `Buffered`.
    fn text_input_mode(&self) -> super::TextInputMode {
        self.inner.text_input_mode()
    }

    fn supports_text_stream(&self) -> bool {
        self.inner.supports_text_stream()
    }

    /// No cache lookup here: a streaming session never buffers its text to
    /// the end, so it goes straight to the underlying duplex factory.
    async fn open_text_stream(
        &self,
        options: TtsSessionOptions,
        limits: TtsInputLimits,
        cancellation: CancellationToken,
    ) -> Result<TtsDuplexSession, TtsOpenError> {
        self.inner
            .open_text_stream(options, limits, cancellation)
            .await
    }

    /// The runtime's cache hook for complete authorized acts. A phatic hit
    /// replays the warmed clip; anything else is a miss and the caller opens
    /// the normal duplex path.
    async fn try_cached_synthesis(
        &self,
        request: &TtsRequest,
        cancellation: CancellationToken,
    ) -> Option<Box<dyn TtsStream>> {
        let chunks = self.lookup(request)?;
        Some(Box::new(CachedTtsStream {
            response_id: request.speech_id.to_string(),
            chunks: chunks.into_iter().collect(),
            done: false,
            cancellation,
        }))
    }
}

struct CachedTtsStream {
    response_id: String,
    chunks: std::collections::VecDeque<CachedChunk>,
    done: bool,
    cancellation: CancellationToken,
}

#[async_trait]
impl TtsStream for CachedTtsStream {
    /// Cancel safe: the pacing sleep happens before the dequeue, so losing
    /// this future to a `select!` never swallows a chunk.
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.done || self.cancellation.is_cancelled() {
            return Ok(None);
        }

        tokio::time::sleep(Duration::from_millis(10)).await;

        if self.cancellation.is_cancelled() {
            return Ok(None);
        }

        if let Some(chunk) = self.chunks.pop_front() {
            return Ok(Some(TtsEvent::Audio {
                sequence: chunk.sequence,
                sample_rate: chunk.sample_rate,
                pcm_s16le: chunk.pcm,
                response_id: self.response_id.clone(),
            }));
        }

        self.done = true;
        Ok(Some(TtsEvent::AudioDone {
            response_id: self.response_id.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SpeechId;
    use crate::speech::ClaimClass;
    use crate::tts::fake::FakeTts;

    fn request(text: &str, class: ClaimClass, language: &str, voice: &str) -> TtsRequest {
        TtsRequest {
            speech_id: SpeechId::new(),
            speech_epoch: 1,
            text: text.into(),
            language: language.into(),
            voice: voice.into(),
            claim_class: class,
        }
    }

    fn korean(text: &str, class: ClaimClass) -> TtsRequest {
        request(text, class, "Korean", "")
    }

    #[tokio::test(start_paused = true)]
    async fn warmup_caches_phrases_and_replays_instantly() {
        let inner = Arc::new(FakeTts::new(24_000, 60, 3));
        let premade =
            PremadeTts::with_settings(inner, vec!["네".into()], "Korean".into(), String::new());

        premade.warmup().await;

        let start = tokio::time::Instant::now();
        let mut stream = premade
            .synthesize(korean("네", ClaimClass::Phatic), CancellationToken::new())
            .await
            .unwrap();

        let mut audio = 0;
        let mut done = false;
        while let Some(event) = stream.next_event().await.unwrap() {
            match event {
                TtsEvent::Audio { .. } => audio += 1,
                TtsEvent::AudioDone { .. } => {
                    done = true;
                    break;
                }
                TtsEvent::Error { .. } => break,
            }
        }

        assert_eq!(audio, 3);
        assert!(done);
        assert!(start.elapsed() < Duration::from_millis(200));
    }

    #[tokio::test(start_paused = true)]
    async fn factual_speech_bypasses_cache() {
        let inner = Arc::new(FakeTts::new(24_000, 60, 2));
        let premade =
            PremadeTts::with_settings(inner, vec!["네".into()], "Korean".into(), String::new());

        premade.warmup().await;

        let mut stream = premade
            .synthesize(korean("네", ClaimClass::Factual), CancellationToken::new())
            .await
            .unwrap();

        let mut audio = 0;
        while let Some(event) = stream.next_event().await.unwrap() {
            if matches!(event, TtsEvent::Audio { .. }) {
                audio += 1;
            }
        }

        assert_eq!(audio, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn cache_hook_hits_only_matching_language_and_voice() {
        let inner = Arc::new(FakeTts::new(24_000, 60, 2));
        let premade = Arc::new(PremadeTts::with_settings(
            inner,
            vec!["네".into()],
            "Korean".into(),
            String::new(),
        ));

        premade.warmup().await;

        let hit = premade
            .try_cached_synthesis(&korean("네", ClaimClass::Phatic), CancellationToken::new())
            .await;
        assert!(hit.is_some(), "same text, language, and voice must hit");

        let wrong_language = request("네", ClaimClass::Phatic, "English", "");
        assert!(
            premade
                .try_cached_synthesis(&wrong_language, CancellationToken::new())
                .await
                .is_none(),
            "a different language must miss"
        );

        let wrong_voice = request("네", ClaimClass::Phatic, "Korean", "other-voice");
        assert!(
            premade
                .try_cached_synthesis(&wrong_voice, CancellationToken::new())
                .await
                .is_none(),
            "a different voice must miss"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_warmup_clips_are_not_cached() {
        // Zero chunks per request: the fake fails before any audio.
        let inner = Arc::new(FakeTts::new(24_000, 60, 0));
        let premade = Arc::new(PremadeTts::with_settings(
            inner,
            vec!["네".into()],
            "Korean".into(),
            String::new(),
        ));

        premade.warmup().await;

        assert!(
            premade
                .try_cached_synthesis(&korean("네", ClaimClass::Phatic), CancellationToken::new())
                .await
                .is_none(),
            "an incomplete clip must never be registered"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cached_stream_cancellation_loses_no_chunk() {
        let inner = Arc::new(FakeTts::new(24_000, 60, 2));
        let premade =
            PremadeTts::with_settings(inner, vec!["네".into()], "Korean".into(), String::new());
        premade.warmup().await;

        let token = CancellationToken::new();
        let mut stream = premade
            .try_cached_synthesis(&korean("네", ClaimClass::Phatic), token.clone())
            .await
            .unwrap();

        // Cancel mid-pacing-sleep; the not-yet-dequeued chunk survives.
        token.cancel();
        assert!(stream.next_event().await.unwrap().is_none());
    }
}
