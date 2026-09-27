use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{StreamingTts, TtsEvent, TtsRequest, TtsStream};

/// Deterministic fake TTS: emits configured chunks of silence at a fixed
/// cadence, then a single AudioDone, then terminates with None.
pub struct FakeTts {
    pub sample_rate: u32,
    pub chunk_ms: u64,
    pub chunks_per_request: usize,
    pub silence: bool,
}

impl FakeTts {
    pub fn new(sample_rate: u32, chunk_ms: u64, chunks_per_request: usize) -> Self {
        Self {
            sample_rate,
            chunk_ms,
            chunks_per_request,
            silence: true,
        }
    }

    fn chunk_bytes(&self) -> usize {
        (self.sample_rate as usize * self.chunk_ms as usize / 1_000) * 2
    }
}

#[async_trait]
impl StreamingTts for FakeTts {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        Ok(Box::new(FakeTtsStream {
            request,
            cancellation,
            sample_rate: self.sample_rate,
            chunk_ms: self.chunk_ms,
            remaining: self.chunks_per_request,
            sequence: 0,
            chunk_bytes: self.chunk_bytes(),
            done_emitted: false,
        }))
    }
}

pub struct FakeTtsStream {
    request: TtsRequest,
    cancellation: CancellationToken,
    sample_rate: u32,
    chunk_ms: u64,
    remaining: usize,
    sequence: u32,
    chunk_bytes: usize,
    done_emitted: bool,
}

#[async_trait]
impl TtsStream for FakeTtsStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.cancellation.is_cancelled() || self.done_emitted {
            return Ok(None);
        }

        if self.remaining == 0 {
            if self.sequence == 0 {
                // Nothing was synthesized at all; fail rather than fake a
                // successful done.
                self.done_emitted = true;
                return Ok(Some(TtsEvent::Error {
                    recoverable: true,
                    message: "fake tts produced no audio".into(),
                }));
            }

            self.done_emitted = true;
            let response_id = self.request.speech_id.to_string();
            return Ok(Some(TtsEvent::AudioDone { response_id }));
        }

        tokio::time::sleep(Duration::from_millis(self.chunk_ms)).await;

        if self.cancellation.is_cancelled() {
            return Ok(None);
        }

        self.remaining -= 1;
        self.sequence += 1;

        let pcm = Bytes::from(vec![0u8; self.chunk_bytes]);
        Ok(Some(TtsEvent::Audio {
            response_id: self.request.speech_id.to_string(),
            sequence: self.sequence,
            sample_rate: self.sample_rate,
            pcm_s16le: pcm,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SpeechId;
    use crate::speech::ClaimClass;

    fn request() -> TtsRequest {
        TtsRequest {
            speech_id: SpeechId::new(),
            speech_epoch: 1,
            text: "테스트".into(),
            language: "Korean".into(),
            voice: String::new(),
            claim_class: ClaimClass::Phatic,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn emits_chunks_then_done_then_none() {
        let tts = FakeTts::new(24_000, 60, 2);
        let mut stream = tts
            .synthesize(request(), CancellationToken::new())
            .await
            .unwrap();

        assert!(matches!(
            stream.next_event().await.unwrap(),
            Some(TtsEvent::Audio { .. })
        ));
        assert!(matches!(
            stream.next_event().await.unwrap(),
            Some(TtsEvent::Audio { .. })
        ));
        assert!(matches!(
            stream.next_event().await.unwrap(),
            Some(TtsEvent::AudioDone { .. })
        ));
        assert!(stream.next_event().await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_terminates_stream() {
        let tts = FakeTts::new(24_000, 60, 5);
        let token = CancellationToken::new();
        let mut stream = tts.synthesize(request(), token.clone()).await.unwrap();

        token.cancel();
        assert!(stream.next_event().await.unwrap().is_none());
    }
}
