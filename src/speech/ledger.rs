use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::ids::SpeechId;
use crate::interaction::InterruptionReason;

/// How much of a clause the user actually heard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Heard {
    Full,
    /// Playback stopped part way. `approx_chars` estimates the heard prefix
    /// from played versus synthesized samples; it is an estimate, not a
    /// word-aligned cut.
    Partial {
        played_ms: u64,
        approx_chars: usize,
    },
    None,
}

#[derive(Debug, Clone, Serialize)]
pub struct AudibleClause {
    pub speech_id: SpeechId,
    /// The full clause text. Whether the user heard it, or only a prefix of
    /// it, is `heard`'s business; use [`AudibleClause::heard_text`] for
    /// anything that should reflect what reached the user.
    pub text: String,
    pub heard: Heard,
    pub played_ms: u64,
    pub interruption_reason: Option<InterruptionReason>,
}

impl AudibleClause {
    pub fn is_complete(&self) -> bool {
        self.heard == Heard::Full
    }

    /// The text the user actually heard: the whole clause, an estimated
    /// prefix, or nothing.
    pub fn heard_text(&self) -> String {
        heard_prefix(&self.text, self.heard)
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct AudibleContext {
    pub completed: Vec<AudibleClause>,
    pub interrupted_count: usize,
}

impl AudibleContext {
    pub fn render_text(&self) -> String {
        self.completed
            .iter()
            .map(|clause| clause.text.clone())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Debug)]
struct InFlight {
    speech_id: SpeechId,
    text: String,
    sample_rate: u32,
    played_samples: u64,
}

/// Clauses kept for self-echo detection and the session summary. Durable
/// conversational truth lives in the agent's memory; this is a recent window.
pub const DEFAULT_AUDIBLE_HISTORY_MAX_CLAUSES: usize = 64;

/// Audible Truth ledger: what playback actually confirmed.
///
/// Interrupted clauses keep their text alongside a [`Heard::Partial`] estimate
/// so the agent can know what it was saying when it was cut off. Anything that
/// exposes "what the user heard" goes through [`AudibleClause::heard_text`],
/// never the raw `text`.
#[derive(Debug)]
pub struct AudibleLedger {
    in_flight: Option<InFlight>,
    history: Vec<AudibleClause>,
    interrupted_count: usize,
    max_clauses: usize,
    /// Samples handed to playback per speech, for partial-heard estimates.
    enqueued_samples: HashMap<SpeechId, u64>,
    /// Speeches whose synthesis has finished, so `enqueued_samples` is their
    /// true length rather than a lower bound.
    synthesis_done: HashSet<SpeechId>,
}

impl Default for AudibleLedger {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_AUDIBLE_HISTORY_MAX_CLAUSES)
    }
}

impl AudibleLedger {
    pub fn with_capacity(max_clauses: usize) -> Self {
        Self {
            in_flight: None,
            history: Vec::new(),
            interrupted_count: 0,
            max_clauses: max_clauses.max(1),
            enqueued_samples: HashMap::new(),
            synthesis_done: HashSet::new(),
        }
    }

    pub fn speech_started(&mut self, speech_id: SpeechId, text: &str, sample_rate: u32) {
        self.in_flight = Some(InFlight {
            speech_id,
            text: text.trim().to_string(),
            sample_rate,
            played_samples: 0,
        });
    }

    /// Records synthesized audio handed to playback, for partial estimates.
    pub fn add_enqueued_samples(&mut self, speech_id: SpeechId, samples: u64) {
        *self.enqueued_samples.entry(speech_id).or_insert(0) += samples;
    }

    /// Synthesis for this speech is complete; its enqueued length is final.
    pub fn mark_synthesis_done(&mut self, speech_id: SpeechId) {
        self.synthesis_done.insert(speech_id);
    }

    fn total_samples(&self, speech_id: SpeechId) -> TotalSamples {
        let enqueued = self.enqueued_samples.get(&speech_id).copied().unwrap_or(0);
        if self.synthesis_done.contains(&speech_id) {
            TotalSamples::Exact(enqueued)
        } else {
            TotalSamples::AtLeast(enqueued)
        }
    }

    fn forget_speech(&mut self, speech_id: SpeechId) {
        self.enqueued_samples.remove(&speech_id);
        self.synthesis_done.remove(&speech_id);
    }

    pub fn mark_progress(&mut self, speech_id: SpeechId, played_samples: u64) {
        if let Some(in_flight) = &mut self.in_flight
            && in_flight.speech_id == speech_id
        {
            in_flight.played_samples = in_flight.played_samples.max(played_samples);
        }
    }

    pub fn mark_completed(&mut self, speech_id: SpeechId) {
        if let Some(in_flight) = self.in_flight.take() {
            if in_flight.speech_id == speech_id {
                self.forget_speech(speech_id);
                self.push(AudibleClause {
                    speech_id,
                    text: in_flight.text,
                    heard: Heard::Full,
                    played_ms: samples_to_ms(in_flight.played_samples, in_flight.sample_rate),
                    interruption_reason: None,
                });
            } else {
                self.in_flight = Some(in_flight);
            }
        }
    }

    pub fn mark_interrupted(
        &mut self,
        speech_id: SpeechId,
        played_samples: u64,
        reason: InterruptionReason,
    ) {
        self.interrupted_count += 1;

        if let Some(in_flight) = self.in_flight.take() {
            if in_flight.speech_id == speech_id {
                let played = played_samples.max(in_flight.played_samples);
                let total = self.total_samples(speech_id);
                self.forget_speech(speech_id);
                let heard = partial_heard(&in_flight.text, played, total, in_flight.sample_rate);
                self.push(AudibleClause {
                    speech_id,
                    text: in_flight.text,
                    heard,
                    played_ms: samples_to_ms(played, in_flight.sample_rate),
                    interruption_reason: Some(reason),
                });
            } else {
                self.in_flight = Some(in_flight);
            }
        }
    }

    fn push(&mut self, clause: AudibleClause) {
        self.history.push(clause);
        if self.history.len() > self.max_clauses {
            let excess = self.history.len() - self.max_clauses;
            self.history.drain(..excess);
        }
    }

    pub fn history(&self) -> &[AudibleClause] {
        &self.history
    }

    /// What is known *right now* about one speech act: its settled record,
    /// or, if it is still playing, a partial estimate from progress so far.
    /// `None` means the ledger has never seen it play.
    pub fn heard_so_far(&self, speech_id: SpeechId) -> Option<Heard> {
        if let Some(clause) = self.history.iter().rev().find(|c| c.speech_id == speech_id) {
            return Some(clause.heard);
        }

        let in_flight = self
            .in_flight
            .as_ref()
            .filter(|f| f.speech_id == speech_id)?;
        Some(partial_heard(
            &in_flight.text,
            in_flight.played_samples,
            self.total_samples(speech_id),
            in_flight.sample_rate,
        ))
    }

    pub fn is_settled(&self, speech_id: SpeechId) -> bool {
        self.history.iter().any(|c| c.speech_id == speech_id)
    }

    pub fn in_flight_id(&self) -> Option<SpeechId> {
        self.in_flight.as_ref().map(|f| f.speech_id)
    }

    pub fn audible_context(&self) -> AudibleContext {
        AudibleContext {
            completed: self
                .history
                .iter()
                .filter(|clause| clause.is_complete())
                .cloned()
                .collect(),
            interrupted_count: self.interrupted_count,
        }
    }

    pub fn current_text(&self) -> &str {
        match &self.in_flight {
            Some(in_flight) => &in_flight.text,
            None => "",
        }
    }
}

fn samples_to_ms(samples: u64, sample_rate: u32) -> u64 {
    if sample_rate == 0 {
        return 0;
    }
    samples * 1_000 / sample_rate as u64
}

/// Length of a clause's audio as far as the ledger knows it.
#[derive(Debug, Clone, Copy)]
enum TotalSamples {
    /// Synthesis finished; this is the whole clause.
    Exact(u64),
    /// Synthesis was still running; the clause is at least this long.
    AtLeast(u64),
}

/// Conservative speaking-time estimate per character, used only when
/// synthesis had not finished. Deliberately slow: over-estimating the clause
/// length under-attributes what was heard, and claiming the user heard words
/// they did not is the error this ledger exists to prevent.
const EST_MS_PER_CHAR: u64 = 150;

fn partial_heard(text: &str, played: u64, total: TotalSamples, sample_rate: u32) -> Heard {
    if played == 0 {
        return Heard::None;
    }

    let chars = text.chars().count() as u64;
    let total = match total {
        TotalSamples::Exact(samples) => samples.max(played),
        // Playback can catch up with a streaming synthesis, at which point
        // played == enqueued and a naive ratio says "heard everything". Fall
        // back to an estimate of the full clause length instead.
        TotalSamples::AtLeast(samples) => {
            let estimated = chars * EST_MS_PER_CHAR * sample_rate as u64 / 1_000;
            samples.max(played).max(estimated)
        }
    };
    let approx_chars = (chars * played / total.max(1)).min(chars) as usize;

    Heard::Partial {
        played_ms: samples_to_ms(played, sample_rate),
        approx_chars,
    }
}

fn heard_prefix(text: &str, heard: Heard) -> String {
    match heard {
        Heard::Full => text.to_string(),
        Heard::Partial { approx_chars, .. } => text.chars().take(approx_chars).collect(),
        Heard::None => String::new(),
    }
}

/// Marker appended where the user cut the assistant off mid-clause.
pub const MARKER_INTERRUPTED: &str = "… [중단됨]";
/// Marker appended when later clauses of a reply never played.
pub const MARKER_REST_UNDELIVERED: &str = "[중단됨: 이후 내용은 전달되지 않음]";
/// Stored in place of a reply that never reached the user at all.
pub const MARKER_UNDELIVERED: &str = "[응답이 전달되지 않음]";

/// Renders one reply, clause by clause, into what the user actually heard.
///
/// This is the text that replaces the generated reply in conversation memory.
/// It stops at the first clause that did not play in full, because nothing
/// after an interruption reached the user either.
pub fn render_heard_turn(clauses: &[(&str, Heard)]) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut cut = false;

    for (text, heard) in clauses {
        match heard {
            Heard::Full => out.push(text.trim().to_string()),
            Heard::Partial { .. } => {
                let prefix = heard_prefix(text, *heard);
                let prefix = prefix.trim_end();
                if prefix.is_empty() {
                    cut = true;
                } else {
                    out.push(format!("{prefix}{MARKER_INTERRUPTED}"));
                }
                return finish(out, cut);
            }
            Heard::None => {
                cut = true;
                return finish(out, cut);
            }
        }
    }

    finish(out, cut)
}

fn finish(mut out: Vec<String>, cut: bool) -> String {
    if out.is_empty() {
        return MARKER_UNDELIVERED.to_string();
    }
    if cut {
        out.push(MARKER_REST_UNDELIVERED.to_string());
    }
    out.join(" ")
}

/// The last few clauses the user heard, for self-echo detection.
pub fn recent_assistant_text(history: &[AudibleClause]) -> String {
    history
        .iter()
        .rev()
        .take(3)
        .map(AudibleClause::heard_text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SpeechId;
    use crate::interaction::InterruptionReason;

    #[test]
    fn completed_clauses_enter_history() {
        let mut ledger = AudibleLedger::default();
        let speech_id = SpeechId::new();

        ledger.speech_started(speech_id, "토요일 저녁 7시로 확인해볼게요.", 24_000);
        ledger.mark_progress(speech_id, 24_000);
        ledger.mark_completed(speech_id);

        let context = ledger.audible_context();
        assert_eq!(context.completed.len(), 1);
        assert_eq!(context.completed[0].played_ms, 1_000);
        assert!(context.render_text().contains("토요일"));
    }

    /// Interrupted clauses stay out of the "completed" view, but keep their
    /// text so the heard prefix can be recovered.
    #[test]
    fn interrupted_clause_is_partial_not_complete() {
        let mut ledger = AudibleLedger::default();
        let speech_id = SpeechId::new();
        let text = "예약이 완료되었습니다. 창가 자리도 문의해볼게요.";

        ledger.speech_started(speech_id, text, 24_000);
        ledger.add_enqueued_samples(speech_id, 24_000);
        ledger.mark_synthesis_done(speech_id);
        ledger.mark_progress(speech_id, 12_000);
        ledger.mark_interrupted(speech_id, 12_000, InterruptionReason::HardStop);

        let context = ledger.audible_context();
        assert_eq!(context.completed.len(), 0);
        assert_eq!(context.interrupted_count, 1);

        let clause = &ledger.history()[0];
        assert!(!clause.is_complete());
        let Heard::Partial { approx_chars, .. } = clause.heard else {
            panic!("expected a partial, got {:?}", clause.heard);
        };
        assert_eq!(approx_chars, text.chars().count() / 2);
        assert_eq!(clause.heard_text().chars().count(), approx_chars);
    }

    #[test]
    fn interruption_before_any_playback_is_heard_none() {
        let mut ledger = AudibleLedger::default();
        let speech_id = SpeechId::new();

        ledger.speech_started(speech_id, "두 곳이 가능합니다.", 24_000);
        ledger.mark_interrupted(speech_id, 0, InterruptionReason::HardStop);

        assert_eq!(ledger.history()[0].heard, Heard::None);
        assert_eq!(ledger.history()[0].heard_text(), "");
    }

    /// With synthesis finished, the enqueued length is exact.
    #[test]
    fn exact_total_gives_a_proportional_prefix() {
        let text = "토요일 저녁 7시에 가능합니다"; // 16 chars
        let heard = partial_heard(text, 12_000, TotalSamples::Exact(24_000), 24_000);
        let Heard::Partial { approx_chars, .. } = heard else {
            panic!("expected a partial");
        };
        assert_eq!(approx_chars, 8);
    }

    /// Playback caught up with a still-running synthesis: played == enqueued.
    /// A naive ratio would claim the whole clause was heard.
    #[test]
    fn streaming_synthesis_does_not_over_attribute() {
        let text = "토요일 저녁 7시에 강남 테이블이 가능합니다"; // 24 chars
        let heard = partial_heard(text, 4_800, TotalSamples::AtLeast(4_800), 24_000);
        let Heard::Partial { approx_chars, .. } = heard else {
            panic!("expected a partial");
        };
        assert!(
            approx_chars < text.chars().count() / 2,
            "200 ms of a ~3.6 s clause must not read as most of it: {approx_chars}"
        );
    }

    #[test]
    fn partial_estimate_never_exceeds_the_clause() {
        let text = "짧은 문장";
        let heard = partial_heard(text, 48_000, TotalSamples::Exact(12_000), 24_000);
        let Heard::Partial { approx_chars, .. } = heard else {
            panic!("expected a partial");
        };
        assert_eq!(approx_chars, text.chars().count());
    }

    #[test]
    fn progress_is_monotonic() {
        let mut ledger = AudibleLedger::default();
        let speech_id = SpeechId::new();

        ledger.speech_started(speech_id, "텍스트", 16_000);
        ledger.mark_progress(speech_id, 16_000);
        ledger.mark_progress(speech_id, 4_000);
        ledger.mark_completed(speech_id);

        assert_eq!(ledger.history()[0].played_ms, 1_000);
    }

    #[test]
    fn history_is_capped() {
        let mut ledger = AudibleLedger::with_capacity(3);
        let mut ids = Vec::new();

        for i in 0..5 {
            let id = SpeechId::new();
            ids.push(id);
            ledger.speech_started(id, &format!("문장 {i}"), 24_000);
            ledger.mark_completed(id);
        }

        assert_eq!(ledger.history().len(), 3);
        assert_eq!(
            ledger.history()[0].speech_id,
            ids[2],
            "oldest clauses go first"
        );
    }

    #[test]
    fn heard_so_far_sees_settled_and_in_flight_acts() {
        let mut ledger = AudibleLedger::default();
        let done = SpeechId::new();
        let playing = SpeechId::new();
        let never = SpeechId::new();

        ledger.speech_started(done, "첫 문장.", 24_000);
        ledger.mark_completed(done);

        ledger.speech_started(playing, "두 번째 문장입니다.", 24_000);
        ledger.add_enqueued_samples(playing, 24_000);
        ledger.mark_progress(playing, 6_000);

        assert_eq!(ledger.heard_so_far(done), Some(Heard::Full));
        assert!(matches!(
            ledger.heard_so_far(playing),
            Some(Heard::Partial { .. })
        ));
        assert_eq!(ledger.heard_so_far(never), None);
    }

    #[test]
    fn full_reply_renders_verbatim() {
        let rendered = render_heard_turn(&[
            ("확인했습니다.", Heard::Full),
            ("두 곳이 가능합니다.", Heard::Full),
        ]);
        assert_eq!(rendered, "확인했습니다. 두 곳이 가능합니다.");
    }

    #[test]
    fn interrupted_reply_keeps_only_the_heard_prefix() {
        let rendered = render_heard_turn(&[
            ("확인했습니다.", Heard::Full),
            (
                "토요일 7시에 강남 테이블이 가능합니다.",
                Heard::Partial {
                    played_ms: 600,
                    approx_chars: 6,
                },
            ),
            ("홍대 키친도 가능합니다.", Heard::Full),
        ]);

        assert_eq!(rendered, "확인했습니다. 토요일 7시… [중단됨]");
        assert!(
            !rendered.contains("홍대"),
            "nothing after an interruption was heard"
        );
    }

    #[test]
    fn unplayed_tail_is_marked() {
        let rendered = render_heard_turn(&[
            ("확인했습니다.", Heard::Full),
            ("두 곳이 가능합니다.", Heard::None),
        ]);
        assert_eq!(rendered, format!("확인했습니다. {MARKER_REST_UNDELIVERED}"));
    }

    #[test]
    fn nothing_heard_is_marked_undelivered() {
        assert_eq!(
            render_heard_turn(&[("예약이 완료되었습니다.", Heard::None)]),
            MARKER_UNDELIVERED
        );
        assert_eq!(render_heard_turn(&[]), MARKER_UNDELIVERED);
    }
}
