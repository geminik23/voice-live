use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use super::{AsrEvent, AsrSession, AsrSessionConfig, StreamingAsr};
use crate::ids::Revision;

#[derive(Debug, Clone)]
pub struct TimedAsrEvent {
    pub after_ms: u64,
    pub event: AsrEvent,
}

/// Scripted provider used by virtual-time tests and the mock demo mode.
pub struct ScriptedAsrSession {
    events: Mutex<VecDeque<TimedAsrEvent>>,
    closed: bool,
}

pub struct MockAsr {
    events: Vec<TimedAsrEvent>,
}

impl MockAsr {
    pub fn new(events: Vec<TimedAsrEvent>) -> Self {
        Self { events }
    }

    pub fn from_text(text: &str) -> Self {
        let events = vec![
            TimedAsrEvent {
                after_ms: 0,
                event: AsrEvent::Partial {
                    utterance_id: 0,
                    revision: Revision(1),
                    text: text.to_string(),
                },
            },
            TimedAsrEvent {
                after_ms: 100,
                event: AsrEvent::Final {
                    utterance_id: 0,
                    text: text.to_string(),
                },
            },
        ];
        Self::new(events)
    }

    pub fn quiet() -> Self {
        Self::new(Vec::new())
    }
}

#[async_trait]
impl StreamingAsr for MockAsr {
    async fn open(&self, _config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>> {
        Ok(Box::new(ScriptedAsrSession {
            events: Mutex::new(self.events.iter().cloned().collect()),
            closed: false,
        }))
    }
}

#[async_trait]
impl AsrSession for ScriptedAsrSession {
    async fn push_audio(&mut self, _pcm: &[i16]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn commit_audio(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Cancel safe: the delay is awaited before the event is removed from the
    /// script, so losing this future to a `select!` never swallows an event.
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        let delay_ms = {
            let queue = self.events.lock();
            queue.front().map(|timed| timed.after_ms)
        };

        let Some(delay_ms) = delay_ms else {
            // The script is exhausted, not the session. Parking here keeps
            // `Ok(None)` reserved for a genuinely closed session so the media
            // worker can treat it as a reconnect signal.
            if self.closed {
                return Ok(None);
            }
            return std::future::pending().await;
        };

        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        Ok(self.events.lock().pop_front().map(|timed| timed.event))
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.events.lock().clear();
        self.closed = true;
        Ok(())
    }
}
