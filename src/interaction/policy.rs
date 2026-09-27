use crate::config::TurnControlConfig;
use crate::interaction::{InteractionAction, InteractionDecision, UserSpeechState};
use crate::session::view::SessionView;

/// Deterministic reflex controller. It answers every case it can prove from
/// text and timing alone; uncertain cases return None and escalate to the
/// interaction brain.
#[derive(Debug, Default)]
pub struct ReflexPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapOutcome {
    Backchannel,
    Interruption,
    Correction,
    Answer,
    Noise,
    Unknown,
}

impl ReflexPolicy {
    pub fn new() -> Self {
        Self
    }

    pub fn is_backchannel_word(&self, text: &str, config: &TurnControlConfig) -> bool {
        let normalized = normalize_words(text);
        if normalized.chars().count() > 6 {
            return false;
        }

        config
            .backchannel_words
            .iter()
            .any(|word| normalize_words(word) == normalized)
    }

    pub fn is_hard_stop(&self, text: &str, config: &TurnControlConfig) -> bool {
        let normalized = normalize_words(text);
        config
            .hard_stop_phrases
            .iter()
            .any(|phrase| normalized.contains(&normalize_words(phrase)))
    }

    pub fn is_correction(&self, text: &str) -> bool {
        let normalized = normalize_words(text);
        normalized.contains("말고")
            || normalized.contains("아니")
            || normalized.contains("바꿔")
            || normalized.contains("정정")
            || normalized.contains("다시")
    }

    pub fn ends_incomplete(&self, text: &str, config: &TurnControlConfig) -> bool {
        let raw = text.trim_end();
        let normalized = normalize_words(raw);
        config.incomplete_endings.iter().any(|ending| {
            let normalized_ending = normalize_words(ending);
            if normalized_ending.is_empty() {
                // Punctuation-only endings such as "..." match on the raw
                // tail. Matching an empty pattern would classify every
                // utterance as incomplete.
                !ending.trim().is_empty() && raw.ends_with(ending.trim())
            } else {
                normalized.ends_with(&normalized_ending)
            }
        })
    }

    pub fn ends_complete(&self, text: &str, config: &TurnControlConfig) -> bool {
        let raw = text.trim_end();
        let normalized = normalize_words(raw);
        config.complete_endings.iter().any(|ending| {
            let normalized_ending = normalize_words(ending);
            if normalized_ending.is_empty() {
                !ending.trim().is_empty() && raw.ends_with(ending.trim())
            } else {
                normalized.ends_with(&normalized_ending)
            }
        })
    }

    /// Classifies overlapping user speech while the assistant is speaking.
    ///
    /// This is the single classifier for the overlap probe; `decide_on_tick`
    /// turns the outcome into an action rather than re-deriving it.
    pub fn classify_overlap(
        &self,
        hypothesis: &str,
        recent_assistant_text: &str,
        config: &TurnControlConfig,
        echo: &crate::config::EchoConfig,
    ) -> OverlapOutcome {
        if self.is_backchannel_word(hypothesis, config) {
            return OverlapOutcome::Backchannel;
        }

        if self.is_hard_stop(hypothesis, config) {
            return OverlapOutcome::Interruption;
        }

        if self.is_correction(hypothesis) {
            return OverlapOutcome::Correction;
        }

        if crate::echo::likely_self_echo(hypothesis, recent_assistant_text, true, echo) {
            return OverlapOutcome::Noise;
        }

        if normalize_words(hypothesis).is_empty() {
            return OverlapOutcome::Noise;
        }

        OverlapOutcome::Unknown
    }

    /// Called from the control tick when the mic is idle.
    ///
    /// `recent_assistant_text` is what the user actually heard, used only to
    /// recognise self-echo during an overlap probe.
    pub fn decide_on_tick(
        &self,
        view: &SessionView,
        candidate: &str,
        now_us: u64,
        config: &TurnControlConfig,
        echo: &crate::config::EchoConfig,
        recent_assistant_text: &str,
    ) -> Option<InteractionDecision> {
        if view.local_vad_active {
            return Some(InteractionDecision::new(
                UserSpeechState::Incomplete,
                InteractionAction::KeepListening,
                1.0,
                "user_still_speaking",
            ));
        }

        let last_audio_us = view.last_user_audio_at_us?;

        let silence_ms = now_us.saturating_sub(last_audio_us) / 1_000;
        let hypothesis = view.current_hypothesis.trim();

        if hypothesis.is_empty() && candidate.trim().is_empty() {
            return None;
        }

        // Overlap probe: assistant ducked, user stopped, classify now.
        if view.floor == crate::interaction::FloorState::OverlapProbe
            && (view.speech == crate::interaction::SpeechState::Ducking
                || view.speech == crate::interaction::SpeechState::Playing)
        {
            if silence_ms >= config.barge_in.confirm_ms {
                if hypothesis.is_empty() {
                    return Some(InteractionDecision::new(
                        UserSpeechState::NoiseOrEcho,
                        InteractionAction::ResumeAgent,
                        0.6,
                        "empty_overlap_resume",
                    ));
                }

                return Some(
                    match self.classify_overlap(hypothesis, recent_assistant_text, config, echo) {
                        OverlapOutcome::Backchannel => InteractionDecision::new(
                            UserSpeechState::Backchannel,
                            InteractionAction::ResumeAgent,
                            0.9,
                            "backchannel_after_overlap",
                        ),
                        OverlapOutcome::Noise => InteractionDecision::new(
                            UserSpeechState::NoiseOrEcho,
                            InteractionAction::ResumeAgent,
                            0.7,
                            "self_echo_during_overlap",
                        ),
                        OverlapOutcome::Correction => InteractionDecision::new(
                            UserSpeechState::Correction,
                            InteractionAction::AbortSpeech,
                            0.9,
                            "correction_during_overlap",
                        ),
                        OverlapOutcome::Interruption | OverlapOutcome::Answer => {
                            InteractionDecision::new(
                                UserSpeechState::Interruption,
                                InteractionAction::AbortSpeech,
                                0.85,
                                "interruption_during_overlap",
                            )
                        }
                        OverlapOutcome::Unknown => InteractionDecision::new(
                            UserSpeechState::Interruption,
                            InteractionAction::AbortSpeech,
                            0.7,
                            "unclassified_overlap_aborts",
                        ),
                    },
                );
            }
            return None;
        }

        if silence_ms >= config.hard_silence_ms {
            return Some(InteractionDecision::new(
                UserSpeechState::Complete,
                InteractionAction::CommitUserTurn,
                0.9,
                "hard_silence",
            ));
        }

        // A question we asked makes short utterances authoritative answers,
        // faster than the general soft-silence gate.
        if view.assistant_asked_question
            && self.is_backchannel_word(hypothesis, config)
            && silence_ms >= config.answer_min_silence_ms
        {
            return Some(InteractionDecision::new(
                UserSpeechState::Answer,
                InteractionAction::CommitUserTurn,
                0.95,
                "answer_to_question",
            ));
        }

        if silence_ms < config.soft_silence_ms {
            return None;
        }

        let basis = if hypothesis.is_empty() {
            candidate
        } else {
            hypothesis
        };

        if self.ends_incomplete(basis, config) {
            return Some(InteractionDecision::new(
                UserSpeechState::Incomplete,
                InteractionAction::KeepListening,
                0.9,
                "incomplete_connective_ending",
            ));
        }

        if self.ends_complete(basis, config) && config.require_semantic_completion {
            return Some(InteractionDecision::new(
                UserSpeechState::Complete,
                InteractionAction::CommitUserTurn,
                0.85,
                "complete_ending_with_soft_silence",
            ));
        }

        None
    }
}

pub fn normalize_words(text: &str) -> String {
    text.chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ')
        .flat_map(|c| c.to_lowercase())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interaction::{FloorState, InteractionAction, SpeechState, UserSpeechState};

    fn config() -> TurnControlConfig {
        TurnControlConfig::default()
    }

    fn echo() -> crate::config::EchoConfig {
        crate::config::EchoConfig::default()
    }

    fn view_with_silence(silence_ms: u64, now_us: u64) -> SessionView {
        let mut view = SessionView::new(crate::ids::SessionId::new());
        view.last_user_audio_at_us = Some(now_us - silence_ms * 1_000);
        view
    }

    #[test]
    fn backchannel_word_after_explanation() {
        let policy = ReflexPolicy::new();
        let cfg = config();

        assert!(policy.is_backchannel_word("네", &cfg));
        assert!(policy.is_backchannel_word("아 그렇군요", &cfg));
        assert!(!policy.is_backchannel_word("토요일로 해주세요", &cfg));
    }

    #[test]
    fn hard_stop_is_detected() {
        let policy = ReflexPolicy::new();
        let cfg = config();

        assert!(policy.is_hard_stop("잠깐, 예약하지 마", &cfg));
        assert!(policy.is_hard_stop("취소해", &cfg));
        assert!(!policy.is_hard_stop("예약해줘", &cfg));
    }

    #[test]
    fn punctuation_only_endings_do_not_make_everything_incomplete() {
        let policy = ReflexPolicy::new();
        let cfg = config();

        // The "..." pattern must not reduce to an always-true suffix match.
        assert!(!policy.ends_incomplete("토요일 저녁 7시로 예약해줘", &cfg));
        assert!(policy.ends_complete("토요일 저녁 7시로 예약해줘", &cfg));
        assert!(policy.ends_incomplete("저는 이번 주말에...", &cfg));
    }

    #[test]
    fn korean_endings_control_commit() {
        let policy = ReflexPolicy::new();
        let cfg = config();

        assert!(policy.ends_incomplete("저는 이번 주말에 가볼 만한 곳을 찾고 있는데", &cfg));
        assert!(policy.ends_complete("토요일 저녁 7시로 예약해 주세요", &cfg));
        assert!(policy.ends_incomplete("그게...", &cfg));
    }

    #[test]
    fn question_short_answer_commits() {
        let policy = ReflexPolicy::new();
        let cfg = config();
        let now = 100_000_000;

        let mut view = view_with_silence(cfg.answer_min_silence_ms, now);
        view.assistant_asked_question = true;
        view.current_hypothesis = "네".into();

        let decision = policy
            .decide_on_tick(&view, "네", now, &cfg, &echo(), "")
            .expect("decision");

        assert_eq!(decision.action, InteractionAction::CommitUserTurn);
        assert_eq!(decision.user_state, UserSpeechState::Answer);
    }

    #[test]
    fn incomplete_ending_waits_past_soft_silence() {
        let policy = ReflexPolicy::new();
        let cfg = config();
        let now = 100_000_000;

        let mut view = view_with_silence(cfg.soft_silence_ms + 50, now);
        view.current_hypothesis = "찾고 있는데".into();

        let decision = policy
            .decide_on_tick(&view, "찾고 있는데", now, &cfg, &echo(), "")
            .unwrap();
        assert_eq!(decision.action, InteractionAction::KeepListening);
    }

    #[test]
    fn hard_silence_commits_anyway() {
        let policy = ReflexPolicy::new();
        let cfg = config();
        let now = 100_000_000;

        let mut view = view_with_silence(cfg.hard_silence_ms, now);
        view.current_hypothesis = "그러니까".into();

        let decision = policy
            .decide_on_tick(&view, "그러니까", now, &cfg, &echo(), "")
            .expect("hard silence must commit");

        assert_eq!(decision.action, InteractionAction::CommitUserTurn);
    }

    #[test]
    fn overlap_backchannel_resumes() {
        let policy = ReflexPolicy::new();
        let cfg = config();
        let now = 100_000_000;

        let mut view = view_with_silence(cfg.barge_in.confirm_ms, now);
        view.floor = FloorState::OverlapProbe;
        view.speech = SpeechState::Ducking;
        view.current_hypothesis = "네".into();

        let decision = policy
            .decide_on_tick(&view, "네", now, &cfg, &echo(), "")
            .unwrap();
        assert_eq!(decision.action, InteractionAction::ResumeAgent);
        assert_eq!(decision.user_state, UserSpeechState::Backchannel);
    }

    #[test]
    fn overlap_interruption_aborts() {
        let policy = ReflexPolicy::new();
        let cfg = config();
        let now = 100_000_000;

        let mut view = view_with_silence(cfg.barge_in.confirm_ms, now);
        view.floor = FloorState::OverlapProbe;
        view.speech = SpeechState::Ducking;
        view.current_hypothesis = "강남 말고 홍대로".into();

        let decision = policy
            .decide_on_tick(&view, "강남 말고 홍대로", now, &cfg, &echo(), "")
            .unwrap();
        assert_eq!(decision.action, InteractionAction::AbortSpeech);
    }
}
