use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::ids::SpeechId;
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

#[async_trait]
pub trait StreamingTts: Send + Sync {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>>;
}

#[async_trait]
pub trait TtsStream: Send {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>>;
}

pub mod fake;
pub mod premade;
pub mod qwen;
