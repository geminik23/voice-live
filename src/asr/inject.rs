use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use super::{AsrEvent, AsrSession, AsrSessionConfig, StreamingAsr};

/// One session's pending injected utterances.
///
/// Injection is a development affordance: typed text is promoted to an ASR
/// final so the whole commit -> agent -> claim gate -> TTS -> playback path
/// runs without a paid speech provider. The queue is owned per session so a
/// multi-session gateway routes each injection to the client that sent it.
#[derive(Debug, Default)]
pub struct InjectionQueue {
    pending: Mutex<VecDeque<String>>,
}

impl InjectionQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn push(&self, text: impl Into<String>) {
        let text = text.into();
        if !text.trim().is_empty() {
            self.pending.lock().push_back(text);
        }
    }

    fn pop(&self) -> Option<String> {
        self.pending.lock().pop_front()
    }
}

/// Decorates any streaming ASR source with a text-injection channel.
///
/// The inner provider keeps working normally; injected text is merged into the
/// same event stream as synthetic finals. This is what makes the mock demo
/// usable: `MockAsr::quiet()` alone never emits anything.
pub struct InjectableAsr {
    inner: Arc<dyn StreamingAsr>,
    queue: Arc<InjectionQueue>,
}

impl InjectableAsr {
    pub fn new(inner: Arc<dyn StreamingAsr>, queue: Arc<InjectionQueue>) -> Self {
        Self { inner, queue }
    }
}

#[async_trait]
impl StreamingAsr for InjectableAsr {
    async fn open(&self, config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>> {
        Ok(Box::new(InjectableAsrSession {
            inner: self.inner.open(config).await?,
            queue: Arc::clone(&self.queue),
            // Injected finals must not collide with the inner provider's
            // utterance ids, which start at zero and count up.
            next_utterance: u64::MAX / 2,
        }))
    }
}

struct InjectableAsrSession {
    inner: Box<dyn AsrSession>,
    queue: Arc<InjectionQueue>,
    next_utterance: u64,
}

#[async_trait]
impl AsrSession for InjectableAsrSession {
    async fn push_audio(&mut self, pcm: &[i16]) -> anyhow::Result<()> {
        self.inner.push_audio(pcm).await
    }

    async fn commit_audio(&mut self) -> anyhow::Result<()> {
        self.inner.commit_audio().await
    }

    /// Cancel safe: the injection branch only removes an item once it is about
    /// to be returned, and the inner branch inherits the inner session's own
    /// cancel safety. Losing this future to a `select!` therefore drops no
    /// event.
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        loop {
            if let Some(text) = self.queue.pop() {
                self.next_utterance += 1;
                return Ok(Some(AsrEvent::Final {
                    utterance_id: self.next_utterance,
                    text,
                }));
            }

            tokio::select! {
                biased;

                event = self.inner.next_event() => return event,

                _ = tokio::time::sleep(Duration::from_millis(20)) => continue,
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asr::fake::MockAsr;

    fn session_config() -> AsrSessionConfig {
        AsrSessionConfig {
            sample_rate_hz: 16_000,
            locale: "ko-KR".into(),
            manual_commit: false,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn injected_text_becomes_a_final_over_a_silent_provider() {
        let queue = InjectionQueue::new();
        let asr = InjectableAsr::new(Arc::new(MockAsr::quiet()), Arc::clone(&queue));

        let mut session = asr.open(session_config()).await.unwrap();
        queue.push("토요일 저녁 7시로 예약해줘");

        let event = session.next_event().await.unwrap().unwrap();

        match event {
            AsrEvent::Final { text, .. } => assert_eq!(text, "토요일 저녁 7시로 예약해줘"),
            other => panic!("expected a final, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn inner_provider_events_still_flow_through() {
        let queue = InjectionQueue::new();
        let asr = InjectableAsr::new(Arc::new(MockAsr::from_text("안녕하세요")), queue);

        let mut session = asr.open(session_config()).await.unwrap();
        let event = session.next_event().await.unwrap().unwrap();

        assert!(matches!(event, AsrEvent::Partial { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn blank_injections_are_ignored() {
        let queue = InjectionQueue::new();
        queue.push("   ");
        assert!(queue.pop().is_none());
    }
}
