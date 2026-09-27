use std::collections::VecDeque;

use crate::config::AsrPartialsConfig;
use crate::ids::Revision;

/// Streaming ASR partials are revisioned replacements, never appends. The
/// reconciler tracks the stable prefix that has survived N consecutive
/// revisions for at least the configured duration.
#[derive(Debug)]
pub struct TranscriptReconciler {
    config: AsrPartialsConfig,

    current_utterance: u64,
    current_revision: Revision,
    current: String,

    recent: VecDeque<(Revision, String)>,
    revision_times: VecDeque<(Revision, u64)>,

    stable_prefix: String,
}

impl TranscriptReconciler {
    pub fn new(config: AsrPartialsConfig) -> Self {
        Self {
            config,
            current_utterance: 0,
            current_revision: Revision::default(),
            current: String::new(),
            recent: VecDeque::new(),
            revision_times: VecDeque::new(),
            stable_prefix: String::new(),
        }
    }

    pub fn current_text(&self) -> &str {
        &self.current
    }

    pub fn stable_prefix(&self) -> &str {
        &self.stable_prefix
    }

    pub fn current_utterance(&self) -> u64 {
        self.current_utterance
    }

    pub fn current_revision(&self) -> Revision {
        self.current_revision
    }

    /// Returns Some(new stable prefix) when the stable prefix advanced.
    pub fn on_partial(
        &mut self,
        utterance_id: u64,
        revision: Revision,
        hypothesis: &str,
        now_us: u64,
    ) -> Option<String> {
        if utterance_id != self.current_utterance {
            self.start_utterance(utterance_id);
        }

        self.current_revision = revision;
        self.current = hypothesis.trim().to_string();
        self.recent.push_back((revision, self.current.clone()));
        self.revision_times.push_back((revision, now_us));

        // Revision 1 within an utterance is never stable by definition, so the
        // retention window of 3 keeps the last three hypotheses only.
        while self.recent.len() > 3 {
            self.recent.pop_front();
        }
        while self.revision_times.len() > 32 {
            self.revision_times.pop_front();
        }

        if !self.config.enabled {
            return None;
        }

        let candidate = self.stable_candidate(now_us);

        if candidate.len() > self.stable_prefix.len()
            && self.current.starts_with(candidate.as_str())
        {
            self.stable_prefix = candidate.clone();
            Some(self.stable_prefix.clone())
        } else {
            None
        }
    }

    fn stable_candidate(&self, now_us: u64) -> String {
        let needed = self.config.stable_revisions.max(1) as usize;

        if self.recent.len() < needed {
            return String::new();
        }

        let window: Vec<&str> = self
            .recent
            .iter()
            .rev()
            .take(needed)
            .map(|(_, text)| text.as_str())
            .collect();

        let oldest_included_revision = self
            .recent
            .iter()
            .rev()
            .nth(needed - 1)
            .map(|(revision, _)| *revision);

        let Some(oldest) = oldest_included_revision else {
            return String::new();
        };

        let oldest_seen_us = self
            .revision_times
            .iter()
            .find(|(revision, _)| *revision == oldest)
            .map(|(_, us)| *us);

        let Some(oldest_seen_us) = oldest_seen_us else {
            return String::new();
        };

        if now_us.saturating_sub(oldest_seen_us) < self.config.stable_after_ms * 1_000 {
            return String::new();
        }

        common_prefix(&window)
    }

    pub fn on_final(&mut self, utterance_id: u64, transcript: &str) {
        if utterance_id != self.current_utterance {
            self.start_utterance(utterance_id);
        }

        self.current = transcript.trim().to_string();
        self.stable_prefix = self.current.clone();
    }

    pub fn seal_current(&mut self) -> String {
        let sealed = if self.current.is_empty() {
            self.stable_prefix.clone()
        } else {
            self.current.clone()
        };

        self.start_utterance(self.current_utterance + 1);
        sealed
    }

    pub fn clear(&mut self) {
        self.start_utterance(self.current_utterance + 1);
    }

    fn start_utterance(&mut self, utterance_id: u64) {
        self.current_utterance = utterance_id;
        self.current_revision = Revision::default();
        self.current.clear();
        self.stable_prefix.clear();
        self.recent.clear();
        self.revision_times.clear();
    }
}

fn common_prefix(texts: &[&str]) -> String {
    let Some(first) = texts.first() else {
        return String::new();
    };

    let mut end = 0;
    'outer: for (index, ch) in first.char_indices() {
        for text in texts.iter().skip(1) {
            let candidate = text.get(index..index + ch.len_utf8());
            if candidate != Some(&first[index..index + ch.len_utf8()]) {
                break 'outer;
            }
        }
        end = index + ch.len_utf8();
    }

    first[..end].to_string()
}

/// Accumulates finalized ASR output plus the current stable hypothesis into
/// the candidate text for the next user turn commit.
#[derive(Debug, Default)]
pub struct TurnAssemblyBuffer {
    committed_finals: Vec<String>,
}

impl TurnAssemblyBuffer {
    pub fn push_final(&mut self, transcript: impl Into<String>) {
        let transcript = transcript.into();
        if !transcript.trim().is_empty() {
            self.committed_finals.push(transcript.trim().to_string());
        }
    }

    pub fn push_partial_seal(&mut self, sealed: impl Into<String>) {
        self.push_final(sealed);
    }

    pub fn render_candidate(&self, current_hypothesis: &str) -> String {
        let mut parts = self.committed_finals.clone();
        let trimmed = current_hypothesis.trim();
        if !trimmed.is_empty() {
            parts.push(trimmed.to_string());
        }
        parts.join(" ")
    }

    pub fn is_empty(&self) -> bool {
        self.committed_finals.is_empty()
    }

    pub fn take(&mut self) -> String {
        let joined = self.committed_finals.join(" ");
        self.committed_finals.clear();
        joined
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partials() -> AsrPartialsConfig {
        AsrPartialsConfig {
            enabled: true,
            stable_after_ms: 0,
            stable_revisions: 2,
        }
    }

    #[test]
    fn partial_replaces_do_not_append() {
        let mut reconciler = TranscriptReconciler::new(partials());

        reconciler.on_partial(0, Revision(10), "금요일 저녁", 0);
        reconciler.on_partial(0, Revision(11), "금요일 저녁 일곱", 1_000);
        reconciler.on_partial(0, Revision(12), "금요일 말고 토요일 저녁 일곱", 2_000);

        assert_eq!(reconciler.current_text(), "금요일 말고 토요일 저녁 일곱");
    }

    #[test]
    fn stable_prefix_does_not_retreat_on_rewrite() {
        let mut reconciler = TranscriptReconciler::new(partials());

        reconciler.on_partial(0, Revision(1), "금요일 저녁", 0);
        assert!(
            reconciler
                .on_partial(0, Revision(2), "금요일 저녁 일곱", 1_000)
                .is_some()
        );
        let stable = reconciler.stable_prefix().to_string();

        reconciler.on_partial(0, Revision(3), "아니 금요일 말고 토요일", 2_000);

        assert!(reconciler.stable_prefix().starts_with(&stable));
    }

    #[test]
    fn stable_prefix_requires_revision_count() {
        let mut reconciler = TranscriptReconciler::new(partials());

        assert!(
            reconciler
                .on_partial(0, Revision(1), "안녕하세요", 0)
                .is_none()
        );
        assert!(
            reconciler
                .on_partial(0, Revision(2), "안녕하세요", 1)
                .is_some()
        );
        assert_eq!(reconciler.stable_prefix(), "안녕하세요");
    }

    #[test]
    fn stable_prefix_is_korean_char_safe() {
        let mut reconciler = TranscriptReconciler::new(partials());

        reconciler.on_partial(0, Revision(1), "내일 오후", 0);
        reconciler.on_partial(0, Revision(2), "내일 오후 여섯", 1_000);

        assert_eq!(reconciler.stable_prefix(), "내일 오후");
    }

    #[test]
    fn final_after_partial_resets_current() {
        let mut reconciler = TranscriptReconciler::new(partials());

        reconciler.on_partial(0, Revision(1), "토요일", 0);
        reconciler.on_final(0, "토요일 저녁 7시 예약해줘");

        assert_eq!(reconciler.current_text(), "토요일 저녁 7시 예약해줘");

        reconciler.on_partial(1, Revision(1), "감사합니다", 10_000);
        assert_eq!(reconciler.current_text(), "감사합니다");
        assert_eq!(reconciler.current_utterance(), 1);
    }

    #[test]
    fn turn_buffer_joins_finals() {
        let mut buffer = TurnAssemblyBuffer::default();
        buffer.push_final("금요일 저녁 7시로");
        buffer.push_partial_seal("강남에서 4명 예약해줘");

        assert_eq!(
            buffer.render_candidate(""),
            "금요일 저녁 7시로 강남에서 4명 예약해줘"
        );
        assert_eq!(buffer.take(), "금요일 저녁 7시로 강남에서 4명 예약해줘");
        assert!(buffer.is_empty());
    }
}
