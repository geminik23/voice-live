use crate::semantics::SemanticCue;

#[derive(Debug, Clone, Default)]
pub struct TranscriptSnapshot {
    pub stable_prefix: String,
    pub hypothesis: String,
}

/// Event-driven gate for partial controller runs. Urgent cues run
/// immediately; semantic deltas run at a bounded cadence; anything else waits
/// for a maximum flush interval. The injected clock keeps this testable in
/// virtual time.
pub struct PartialControllerPolicy {
    minimum_interval_ms: u64,
    maximum_flush_ms: u64,
    minimum_stable_delta_chars: usize,
}

impl PartialControllerPolicy {
    pub fn new(minimum_interval_ms: u64, maximum_flush_ms: u64) -> Self {
        Self {
            minimum_interval_ms,
            maximum_flush_ms,
            minimum_stable_delta_chars: 3,
        }
    }

    pub fn from_config(interaction_interval_ms: u64) -> Self {
        Self::new(interaction_interval_ms, interaction_interval_ms * 2 + 100)
    }

    pub fn should_run(
        &self,
        previous: &TranscriptSnapshot,
        current: &TranscriptSnapshot,
        cues: &[SemanticCue],
        now_us: u64,
        last_run_at_us: Option<u64>,
    ) -> bool {
        let urgent = cues.iter().any(|cue| {
            matches!(
                cue,
                SemanticCue::HardStop { .. }
                    | SemanticCue::Correction { .. }
                    | SemanticCue::Negation { .. }
            )
        });

        if urgent {
            return true;
        }

        let stable_delta = stable_delta_chars(&previous.stable_prefix, &current.stable_prefix);
        let since_last = last_run_at_us.map(|at| now_us.saturating_sub(at));

        match since_last {
            None => true,
            Some(elapsed_us) => {
                let interval_ok = elapsed_us >= self.minimum_interval_ms * 1_000;
                let flush_due = elapsed_us >= self.maximum_flush_ms * 1_000;
                let user_active = !current.hypothesis.trim().is_empty();

                (stable_delta >= self.minimum_stable_delta_chars && interval_ok)
                    || (flush_due && user_active)
            }
        }
    }
}

fn stable_delta_chars(previous: &str, current: &str) -> usize {
    let previous_chars: Vec<char> = previous.chars().collect();
    let current_chars: Vec<char> = current.chars().collect();

    if current_chars.len() < previous_chars.len() {
        // Prefix rewrote; treat the full current prefix as the delta.
        return current_chars.len();
    }

    current_chars.len() - previous_chars.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(stable: &str, hypothesis: &str) -> TranscriptSnapshot {
        TranscriptSnapshot {
            stable_prefix: stable.into(),
            hypothesis: hypothesis.into(),
        }
    }

    #[test]
    fn urgent_cues_bypass_intervals() {
        let policy = PartialControllerPolicy::new(250, 600);
        let prev = snapshot("토요일", "토요일 저");
        let cur = snapshot("토요일 저녁", "토요일 저녁 7");

        assert!(!policy.should_run(&prev, &cur, &[], 1_000, Some(999)));

        let cue = SemanticCue::HardStop {
            phrase: "잠깐".into(),
        };
        assert!(policy.should_run(&prev, &cur, &[cue], 1_000, Some(999)));
    }

    #[test]
    fn semantic_delta_respects_interval() {
        let policy = PartialControllerPolicy::new(250, 600);
        let prev = snapshot("", "토요일");
        let cur = snapshot("토요일 저녁", "토요일 저녁 7시");

        // First run always goes through.
        assert!(policy.should_run(&prev, &cur, &[], 10_000, None));

        // Within the interval: suppressed.
        assert!(!policy.should_run(&prev, &cur, &[], 100_000, Some(10_000)));

        // Past the interval with enough delta: allowed.
        assert!(policy.should_run(&prev, &cur, &[], 300_000, Some(10_000)));
    }

    #[test]
    fn maximum_flush_fires_while_user_talks() {
        let policy = PartialControllerPolicy::new(250, 600);
        let prev = snapshot("토요일 저녁", "토요일 저녁 7");
        let cur = prev.clone();

        assert!(policy.should_run(&prev, &cur, &[], 700_000, Some(0)));
    }
}
