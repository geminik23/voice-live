use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

/// Lightweight metrics registry matching the documented metric names.
/// Counters are atomic; latency series keep the last 1024 samples per key
/// for P50/P95 reporting without external dependencies.
#[derive(Default)]
pub struct Metrics {
    counters: Mutex<HashMap<String, u64>>,
    latencies: Mutex<HashMap<String, Vec<u64>>>,
}

pub type MetricsRef = Arc<Metrics>;

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc(&self, name: &str) {
        *self.counters.lock().entry(name.to_string()).or_insert(0) += 1;
    }

    pub fn add(&self, name: &str, value: u64) {
        *self.counters.lock().entry(name.to_string()).or_insert(0) += value;
    }

    pub fn counter(&self, name: &str) -> u64 {
        self.counters.lock().get(name).copied().unwrap_or(0)
    }

    pub fn observe_ms(&self, name: &str, ms: u64) {
        let mut latencies = self.latencies.lock();
        let series = latencies.entry(name.to_string()).or_default();
        series.push(ms);
        if series.len() > 1024 {
            series.remove(0);
        }
    }

    pub fn percentile(&self, name: &str, p: f32) -> Option<u64> {
        let latencies = self.latencies.lock();
        let series = latencies.get(name)?;

        let mut sorted = series.clone();
        sorted.sort_unstable();
        if sorted.is_empty() {
            return None;
        }

        let index = ((p.clamp(0.0, 1.0)) * (sorted.len() - 1) as f32).round() as usize;
        Some(sorted[index])
    }

    pub fn mean(&self, name: &str) -> Option<u64> {
        let latencies = self.latencies.lock();
        let series = latencies.get(name)?;
        if series.is_empty() {
            return None;
        }
        Some(series.iter().sum::<u64>() / series.len() as u64)
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let latencies = self.latencies.lock();

        MetricsSnapshot {
            counters: self.counters.lock().clone(),
            latencies: latencies
                .iter()
                .map(|(name, series)| {
                    let mut sorted = series.clone();
                    sorted.sort_unstable();

                    let percentile = |pct: f32| -> Option<u64> {
                        if sorted.is_empty() {
                            return None;
                        }
                        let index =
                            ((pct.clamp(0.0, 1.0)) * (sorted.len() - 1) as f32).round() as usize;
                        Some(sorted[index])
                    };

                    let mean = if sorted.is_empty() {
                        None
                    } else {
                        Some(sorted.iter().sum::<u64>() / sorted.len() as u64)
                    };

                    (
                        name.clone(),
                        LatencySnapshot {
                            count: sorted.len(),
                            mean,
                            p50: percentile(0.50),
                            p95: percentile(0.95),
                            p99: percentile(0.99),
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub counters: HashMap<String, u64>,
    pub latencies: HashMap<String, LatencySnapshot>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LatencySnapshot {
    pub count: usize,
    pub mean: Option<u64>,
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_computes_latency_without_reentrant_locking() {
        let metrics = Metrics::default();
        metrics.observe_ms("voice_test_latency_ms", 10);
        metrics.observe_ms("voice_test_latency_ms", 20);
        metrics.observe_ms("voice_test_latency_ms", 30);

        let snapshot = metrics.snapshot();
        let latency = snapshot.latencies.get("voice_test_latency_ms").unwrap();

        assert_eq!(latency.count, 3);
        assert_eq!(latency.mean, Some(20));
        assert_eq!(latency.p50, Some(20));
        assert_eq!(latency.p95, Some(30));
    }
}
