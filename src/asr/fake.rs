use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::{
    AsrAudioInput, AsrAudioPush, AsrDuplexLimits, AsrDuplexSession, AsrEvent, AsrEventStream,
    AsrInputError, AsrOpenError, AsrSession, AsrSessionConfig, LegacyDuplexFacade, StreamingAsr,
};
use crate::ids::Revision;
use crate::provider::ProviderSessionControl;

#[derive(Debug, Clone)]
pub struct TimedAsrEvent {
    pub after_ms: u64,
    pub event: AsrEvent,
}

/// Records PCM that a mock session accepted, so duplex integration tests can
/// prove audio actually reached the provider input.
#[derive(Default)]
pub struct PcmRecorder {
    pushes: Mutex<Vec<Vec<i16>>>,
}

impl PcmRecorder {
    pub fn received(&self) -> Vec<Vec<i16>> {
        self.pushes.lock().clone()
    }

    fn record(&self, pcm: &[i16]) {
        self.pushes.lock().push(pcm.to_vec());
    }
}

/// Legacy scripted session type retained for source compatibility.
pub type ScriptedAsrSession = LegacyDuplexFacade;

/// Scripted provider used by virtual-time tests and the mock demo mode.
pub struct MockAsr {
    events: Vec<TimedAsrEvent>,
    recorder: Option<Arc<PcmRecorder>>,
}

impl MockAsr {
    pub fn new(events: Vec<TimedAsrEvent>) -> Self {
        Self {
            events,
            recorder: None,
        }
    }

    /// A factory that also exposes what each opened session received.
    pub fn recording(events: Vec<TimedAsrEvent>) -> (Self, Arc<PcmRecorder>) {
        let recorder = Arc::new(PcmRecorder::default());
        (
            Self {
                events,
                recorder: Some(Arc::clone(&recorder)),
            },
            recorder,
        )
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

/// One scheduled event with its absolute virtual-time deadline.
///
/// The deadline is fixed when the session opens so a cancelled poll re-polls
/// against the same instant; re-sleeping the relative delay every poll would
/// postpone a scripted event forever under a periodic select wrapper.
#[derive(Debug, Clone)]
struct ScheduledEvent {
    due: tokio::time::Instant,
    event: AsrEvent,
}

struct MockSessionState {
    events: Mutex<VecDeque<ScheduledEvent>>,
    closed: Mutex<bool>,
    recorder: Option<Arc<PcmRecorder>>,
    cancellation: CancellationToken,
}

#[async_trait]
impl StreamingAsr for MockAsr {
    async fn open(&self, config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>> {
        let duplex = self
            .open_duplex(
                config,
                AsrDuplexLimits {
                    sample_budget: usize::MAX,
                    command_budget: 64,
                    write_timeout: Duration::from_secs(2),
                },
                CancellationToken::new(),
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;

        Ok(Box::new(LegacyDuplexFacade::new(duplex)))
    }

    fn supports_duplex(&self) -> bool {
        true
    }

    async fn open_duplex(
        &self,
        _config: AsrSessionConfig,
        _limits: AsrDuplexLimits,
        cancellation: CancellationToken,
    ) -> Result<AsrDuplexSession, AsrOpenError> {
        if cancellation.is_cancelled() {
            return Err(AsrOpenError::Rejected("cancelled before open".into()));
        }

        let mut due = tokio::time::Instant::now();
        let mut scheduled = VecDeque::with_capacity(self.events.len());
        for timed in &self.events {
            due += Duration::from_millis(timed.after_ms);
            scheduled.push_back(ScheduledEvent {
                due,
                event: timed.event.clone(),
            });
        }

        let state = Arc::new(MockSessionState {
            events: Mutex::new(scheduled),
            closed: Mutex::new(false),
            recorder: self.recorder.clone(),
            cancellation: cancellation.child_token(),
        });

        Ok(AsrDuplexSession {
            input: Box::new(MockAudioInput {
                state: Arc::clone(&state),
                finished: false,
            }),
            events: Box::new(MockEventStream {
                state: Arc::clone(&state),
                cancellation: state.cancellation.clone(),
            }),
            control: Box::new(MockControl { state }),
        })
    }
}

struct MockAudioInput {
    state: Arc<MockSessionState>,
    finished: bool,
}

#[async_trait]
impl AsrAudioInput for MockAudioInput {
    fn try_push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        if self.finished || self.state.cancellation.is_cancelled() {
            return Err(AsrInputError::Closed);
        }
        if let Some(recorder) = &self.state.recorder {
            recorder.record(&pcm);
        }
        Ok(AsrAudioPush::Accepted)
    }

    async fn push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        self.try_push_audio(pcm)
    }

    fn try_commit_utterance(&mut self) -> Result<(), AsrInputError> {
        if self.finished || self.state.cancellation.is_cancelled() {
            return Err(AsrInputError::Closed);
        }
        Ok(())
    }

    async fn commit_utterance(&mut self) -> Result<(), AsrInputError> {
        self.try_commit_utterance()
    }

    fn finish_input(&mut self) -> Result<(), AsrInputError> {
        self.finished = true;
        Ok(())
    }
}

impl Drop for MockAudioInput {
    fn drop(&mut self) {
        if !self.finished {
            self.state.cancellation.cancel();
        }
    }
}

struct MockEventStream {
    state: Arc<MockSessionState>,
    cancellation: CancellationToken,
}

impl Drop for MockEventStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[async_trait]
impl AsrEventStream for MockEventStream {
    /// Cancel safe: the deadline is absolute and the event is removed only
    /// once it is about to be returned.
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        loop {
            if self.cancellation.is_cancelled() || *self.state.closed.lock() {
                return Ok(None);
            }

            let Some(due) = self.state.events.lock().front().map(|event| event.due) else {
                // The script is exhausted, not the session. Parking here keeps
                // `Ok(None)` reserved for a genuinely closed session so the
                // media worker can treat it as a reconnect signal.
                self.cancellation.cancelled().await;
                return Ok(None);
            };

            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Ok(None),
                _ = tokio::time::sleep_until(due) => {},
            }

            let popped = self.state.events.lock().pop_front();
            if let Some(scheduled) = popped {
                return Ok(Some(scheduled.event));
            }
        }
    }
}

struct MockControl {
    state: Arc<MockSessionState>,
}

impl Drop for MockControl {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[async_trait]
impl ProviderSessionControl for MockControl {
    fn cancel(&self) {
        self.state.cancellation.cancel();
        *self.state.closed.lock() = true;
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.cancel();
        *self.state.closed.lock() = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_config() -> AsrSessionConfig {
        AsrSessionConfig {
            sample_rate_hz: 16_000,
            locale: "ko-KR".into(),
            manual_commit: false,
        }
    }

    fn limits() -> AsrDuplexLimits {
        AsrDuplexLimits {
            sample_budget: 1_600,
            command_budget: 64,
            write_timeout: Duration::from_secs(2),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn scripted_events_keep_absolute_deadlines_across_repolls() {
        let asr = MockAsr::new(vec![TimedAsrEvent {
            after_ms: 100,
            event: AsrEvent::Partial {
                utterance_id: 0,
                revision: Revision(1),
                text: "토".into(),
            },
        }]);
        let mut session = asr
            .open_duplex(session_config(), limits(), CancellationToken::new())
            .await
            .unwrap();

        // Drop two polls mid-sleep, as a select wrapper would. The deadline
        // must stay absolute, so the event still fires 100 ms after the open
        // instead of re-sleeping the full delay on every cancelled poll.
        let start = tokio::time::Instant::now();
        for _ in 0..2 {
            let _ =
                tokio::time::timeout(Duration::from_millis(20), session.events.next_event()).await;
        }

        let event = session.events.next_event().await.unwrap().unwrap();
        assert!(matches!(event, AsrEvent::Partial { .. }));
        assert_eq!(start.elapsed(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_session_parks_instead_of_closing() {
        let asr = MockAsr::quiet();
        let mut session = asr
            .open_duplex(session_config(), limits(), CancellationToken::new())
            .await
            .unwrap();

        let poll = tokio::time::timeout(Duration::from_millis(50), session.events.next_event());
        assert!(poll.await.is_err(), "idle must park, not close");
    }

    #[tokio::test(start_paused = true)]
    async fn input_records_pcm_and_finish_is_idempotent() {
        let (asr, recorder) = MockAsr::recording(Vec::new());
        let mut session = asr
            .open_duplex(session_config(), limits(), CancellationToken::new())
            .await
            .unwrap();

        session.input.try_push_audio(vec![1, 2, 3]).unwrap();
        session.input.finish_input().unwrap();
        session.input.finish_input().unwrap();

        assert!(matches!(
            session.input.try_push_audio(vec![4]),
            Err(AsrInputError::Closed)
        ));

        let received = recorder.received();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], vec![1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn legacy_open_still_yields_scripted_events() {
        let asr = MockAsr::from_text("안녕하세요");
        let mut session = asr.open(session_config()).await.unwrap();

        assert!(matches!(
            session.next_event().await.unwrap().unwrap(),
            AsrEvent::Partial { .. }
        ));
        assert!(matches!(
            session.next_event().await.unwrap().unwrap(),
            AsrEvent::Final { .. }
        ));
    }
}
