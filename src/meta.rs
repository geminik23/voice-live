use crate::clock::ClockRef;

#[derive(Debug, Clone)]
pub struct EventMeta {
    pub monotonic_us: u64,
    pub wall_clock: chrono::DateTime<chrono::Utc>,
}

pub trait MetaFactory: Send + Sync {
    fn new_meta(&self) -> EventMeta;
}

pub struct ClockMetaFactory {
    clock: ClockRef,
}

impl ClockMetaFactory {
    pub fn new(clock: ClockRef) -> Self {
        Self { clock }
    }
}

impl MetaFactory for ClockMetaFactory {
    fn new_meta(&self) -> EventMeta {
        EventMeta {
            monotonic_us: self.clock.now_us(),
            wall_clock: chrono::Utc::now(),
        }
    }
}
