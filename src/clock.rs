use std::sync::Arc;

pub type ClockRef = Arc<dyn MonotonicClock>;

/// Monotonic microsecond clock. Production uses tokio time so paused
/// virtual-time tests advance every runtime timeout and cadence.
pub trait MonotonicClock: Send + Sync {
    fn now_us(&self) -> u64;
}

pub struct TokioClock {
    base: std::sync::OnceLock<tokio::time::Instant>,
}

impl TokioClock {
    pub fn new() -> Self {
        Self {
            base: std::sync::OnceLock::new(),
        }
    }

    fn base(&self) -> tokio::time::Instant {
        *self.base.get_or_init(tokio::time::Instant::now)
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for TokioClock {
    fn now_us(&self) -> u64 {
        let elapsed = tokio::time::Instant::now().saturating_duration_since(self.base());
        elapsed.as_micros() as u64
    }
}

/// Manual clock for pure unit tests that never touch tokio timers.
#[derive(Default)]
pub struct ManualClock(parking_lot::Mutex<u64>);

impl ManualClock {
    pub fn advance_ms(&self, ms: u64) {
        let mut now = self.0.lock();
        *now += ms * 1_000;
    }
}

impl MonotonicClock for ManualClock {
    fn now_us(&self) -> u64 {
        *self.0.lock()
    }
}

pub fn default_clock() -> ClockRef {
    Arc::new(TokioClock::new())
}
