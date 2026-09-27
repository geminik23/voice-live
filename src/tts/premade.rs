use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::{StreamingTts, TtsEvent, TtsRequest, TtsStream};
use crate::speech::ClaimClass;

/// Premade clip cache for short phatic phrases. Backchannel latency targets
/// are unreachable with on-demand hosted synthesis, so the configured
/// phrases are pre-synthesized through the inner provider and replayed
/// instantly afterwards.
pub struct PremadeTts {
    inner: Arc<dyn StreamingTts>,
    clips: Mutex<HashMap<String, Vec<CachedChunk>>>,
    warmup_pending: Mutex<Vec<String>>,
}

#[derive(Clone)]
struct CachedChunk {
    sequence: u32,
    sample_rate: u32,
    pcm: Bytes,
}

impl PremadeTts {
    pub fn new(inner: Arc<dyn StreamingTts>, phrases: Vec<String>) -> Self {
        Self {
            inner,
            clips: Mutex::new(HashMap::new()),
            warmup_pending: Mutex::new(phrases),
        }
    }

    /// Pre-synthesizes configured phrases so the first real backchannel is
    /// already cached. Failures are silent: the fallback path synthesizes on
    /// demand anyway.
    pub async fn warmup(&self) {
        loop {
            let phrase = {
                let mut pending = self.warmup_pending.lock();
                match pending.pop() {
                    Some(phrase) => phrase,
                    None => break,
                }
            };

            if self.clips.lock().contains_key(&phrase) {
                continue;
            }

            let request = TtsRequest {
                speech_id: crate::ids::SpeechId::new(),
                speech_epoch: 0,
                text: phrase.clone(),
                language: "Korean".into(),
                voice: String::new(),
                claim_class: ClaimClass::Phatic,
            };

            let Ok(mut stream) = self
                .inner
                .synthesize(request, CancellationToken::new())
                .await
            else {
                continue;
            };

            let mut chunks = Vec::new();
            while let Ok(Some(event)) = stream.next_event().await {
                match event {
                    TtsEvent::Audio {
                        sequence,
                        sample_rate,
                        pcm_s16le,
                        ..
                    } => chunks.push(CachedChunk {
                        sequence,
                        sample_rate,
                        pcm: pcm_s16le,
                    }),
                    TtsEvent::AudioDone { .. } => break,
                    TtsEvent::Error { .. } => break,
                }
            }

            if !chunks.is_empty() {
                self.clips.lock().insert(phrase, chunks);
            }
        }
    }

    fn lookup(&self, request: &TtsRequest) -> Option<Vec<CachedChunk>> {
        if request.claim_class != ClaimClass::Phatic {
            return None;
        }

        self.clips.lock().get(&request.text).cloned()
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
                request,
                chunks: chunks.into_iter().collect(),
                done: false,
            }));
        }

        self.inner.synthesize(request, cancellation).await
    }
}

struct CachedTtsStream {
    request: TtsRequest,
    chunks: std::collections::VecDeque<CachedChunk>,
    done: bool,
}

#[async_trait]
impl TtsStream for CachedTtsStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.done {
            return Ok(None);
        }

        if let Some(chunk) = self.chunks.pop_front() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            return Ok(Some(TtsEvent::Audio {
                response_id: self.request.speech_id.to_string(),
                sequence: chunk.sequence,
                sample_rate: chunk.sample_rate,
                pcm_s16le: chunk.pcm,
            }));
        }

        self.done = true;
        Ok(Some(TtsEvent::AudioDone {
            response_id: self.request.speech_id.to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SpeechId;
    use crate::speech::ClaimClass;
    use crate::tts::fake::FakeTts;

    fn request(text: &str, class: ClaimClass) -> TtsRequest {
        TtsRequest {
            speech_id: SpeechId::new(),
            speech_epoch: 1,
            text: text.into(),
            language: "Korean".into(),
            voice: String::new(),
            claim_class: class,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn warmup_caches_phrases_and_replays_instantly() {
        let inner = Arc::new(FakeTts::new(24_000, 60, 3));
        let premade = PremadeTts::new(inner, vec!["네".into()]);

        premade.warmup().await;

        let start = tokio::time::Instant::now();
        let mut stream = premade
            .synthesize(request("네", ClaimClass::Phatic), CancellationToken::new())
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
        let premade = PremadeTts::new(inner, vec!["네".into()]);

        premade.warmup().await;

        let mut stream = premade
            .synthesize(request("네", ClaimClass::Factual), CancellationToken::new())
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
}
