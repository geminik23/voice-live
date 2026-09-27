use async_trait::async_trait;

use crate::ids::Revision;
use crate::semantics::frame::{SemanticFrame, SlotOperation, SlotUpdate};

#[async_trait]
pub trait SlotExtractor: Send + Sync {
    async fn extract(
        &self,
        previous_frame: &SemanticFrame,
        stable_transcript: &str,
        source_revision: Revision,
    ) -> Vec<SlotUpdate>;
}

/// Deterministic Korean slot extractor for dates, times, party size, and
/// replacement corrections, all without any model call.
pub struct HeuristicKoreanSlotExtractor;

#[async_trait]
impl SlotExtractor for HeuristicKoreanSlotExtractor {
    async fn extract(
        &self,
        previous_frame: &SemanticFrame,
        stable_transcript: &str,
        source_revision: Revision,
    ) -> Vec<SlotUpdate> {
        let mut updates = Vec::new();

        // Corrections come first and are reported as `Replace`, which is what
        // drives dependency-scoped task invalidation upstream.
        for (slot, old, new) in detect_corrections(stable_transcript) {
            updates.push(SlotUpdate {
                slot,
                operation: SlotOperation::Replace,
                old_value: Some(old),
                new_value: new,
                confidence: 0.9,
                source_revision,
            });
        }

        let corrected: Vec<String> = updates.iter().map(|update| update.slot.clone()).collect();

        let mut push_if_new = |slot: &str, value: serde_json::Value| {
            if corrected.iter().any(|name| name == slot) {
                return;
            }
            // Compare the values themselves. Comparing `Value::to_string()`
            // against a bare string never matches, because the former is JSON
            // encoded and keeps its quotes.
            if previous_frame.slot_value(slot) == Some(&value) {
                return;
            }
            updates.push(SlotUpdate {
                slot: slot.to_string(),
                operation: SlotOperation::Set,
                old_value: None,
                new_value: value,
                confidence: 0.85,
                source_revision,
            });
        };

        if let Some(party_size) = detect_party_size(stable_transcript) {
            push_if_new("party_size", serde_json::json!(party_size));
        }

        if let Some(time) = detect_time(stable_transcript) {
            push_if_new("time", serde_json::Value::String(time));
        }

        if let Some(date) = detect_date(stable_transcript) {
            push_if_new("date", serde_json::Value::String(date));
        }

        updates
    }
}

/// `X 말고 Y` corrections across every slot the heuristics understand.
///
/// Limiting this to dates left "4명 말고 6명" and "7시 말고 8시" unable to
/// invalidate dependent speculative work.
fn detect_corrections(text: &str) -> Vec<(String, serde_json::Value, serde_json::Value)> {
    let mut corrections = Vec::new();

    for (slot, pattern) in correction_patterns() {
        let Some(captures) = pattern.captures(text) else {
            continue;
        };
        let (Some(old), Some(new)) = (captures.get(1), captures.get(2)) else {
            continue;
        };

        let (old, new) = match slot {
            "party_size" => (
                serde_json::json!(parse_leading_number(old.as_str())),
                serde_json::json!(parse_leading_number(new.as_str())),
            ),
            _ => (
                serde_json::Value::String(normalize_spacing(old.as_str())),
                serde_json::Value::String(normalize_spacing(new.as_str())),
            ),
        };

        if old != new {
            corrections.push((slot.to_string(), old, new));
        }
    }

    corrections
}

fn correction_patterns() -> Vec<(&'static str, regex::Regex)> {
    const DAY: &str =
        r"오늘|내일|모레|글피|주말|평일|월요일|화요일|수요일|목요일|금요일|토요일|일요일";
    const CLOCK: &str = r"(?:오전|오후|아침|저녁|밤)?\s*\d{1,2}\s*시(?:\s*반)?";
    const PARTY: &str = r"\d{1,2}\s*(?:명|사람|인분)";

    vec![
        (
            "date",
            regex::Regex::new(&format!(r"({DAY})\s*말고\s*({DAY})")).expect("date correction"),
        ),
        (
            "time",
            regex::Regex::new(&format!(r"({CLOCK})\s*말고\s*({CLOCK})")).expect("time correction"),
        ),
        (
            "party_size",
            regex::Regex::new(&format!(r"({PARTY})\s*말고\s*({PARTY})")).expect("party correction"),
        ),
    ]
}

fn parse_leading_number(text: &str) -> u32 {
    text.chars()
        .take_while(|c| c.is_ascii_digit() || c.is_whitespace())
        .filter(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

fn normalize_spacing(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn detect_party_size(text: &str) -> Option<u32> {
    let re = regex::Regex::new(r"(\d{1,2})\s*(명|사람|인분)").ok()?;
    let captures = re.captures(text)?;
    captures.get(1)?.as_str().parse().ok()
}

fn detect_time(text: &str) -> Option<String> {
    let re = regex::Regex::new(r"(오전|오후|아침|저녁|밤)?\s*(\d{1,2})\s*시(\s*반)?|\d{1,2}:\d{2}")
        .ok()?;
    let captures = re.captures(text)?;
    Some(normalize_spacing(captures.get(0)?.as_str()))
}

fn detect_date(text: &str) -> Option<String> {
    let re = regex::Regex::new(
        r"(오늘|내일|모레|주말|평일|이번\s*주말|다음\s*주말|월요일|화요일|수요일|목요일|금요일|토요일|일요일)",
    )
    .ok()?;
    let captures = re.captures(text)?;
    Some(captures.get(0)?.as_str().replace(' ', ""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Revision;

    #[tokio::test]
    async fn detects_party_size_and_date() {
        let extractor = HeuristicKoreanSlotExtractor;
        let frame = SemanticFrame::default();

        let updates = extractor
            .extract(&frame, "토요일 저녁에 4명 예약해줘", Revision(10))
            .await;

        assert!(
            updates
                .iter()
                .any(|u| u.slot == "party_size" && u.new_value == serde_json::json!(4))
        );
        assert!(
            updates
                .iter()
                .any(|u| u.slot == "date" && u.new_value == serde_json::json!("토요일"))
        );
    }

    #[tokio::test]
    async fn detects_correction_replace() {
        let extractor = HeuristicKoreanSlotExtractor;
        let frame = SemanticFrame::default();

        let updates = extractor
            .extract(&frame, "아니 금요일 말고 토요일로 해주세요", Revision(12))
            .await;

        let correction = updates
            .iter()
            .find(|u| u.operation == SlotOperation::Replace)
            .expect("correction update");

        assert_eq!(correction.slot, "date");
        assert_eq!(correction.old_value, Some(serde_json::json!("금요일")));
        assert_eq!(correction.new_value, serde_json::json!("토요일"));
    }

    /// Only `Replace` updates invalidate dependent speculative work, so a party
    /// size correction has to produce one just like a date correction does.
    #[tokio::test]
    async fn detects_party_size_and_time_corrections() {
        let extractor = HeuristicKoreanSlotExtractor;
        let frame = SemanticFrame::default();

        let party = extractor
            .extract(&frame, "4명 말고 6명으로 해주세요", Revision(20))
            .await;
        let correction = party
            .iter()
            .find(|u| u.slot == "party_size" && u.operation == SlotOperation::Replace)
            .expect("party size correction");
        assert_eq!(correction.old_value, Some(serde_json::json!(4)));
        assert_eq!(correction.new_value, serde_json::json!(6));

        let time = extractor
            .extract(&frame, "7시 말고 8시로 해주세요", Revision(21))
            .await;
        let correction = time
            .iter()
            .find(|u| u.slot == "time" && u.operation == SlotOperation::Replace)
            .expect("time correction");
        assert_eq!(correction.old_value, Some(serde_json::json!("7시")));
        assert_eq!(correction.new_value, serde_json::json!("8시"));
    }

    /// The previous comparison went through `Value::to_string()`, which keeps
    /// JSON quotes, so an unchanged date re-emitted an update on every partial.
    #[tokio::test]
    async fn unchanged_slots_do_not_re_emit_updates() {
        let extractor = HeuristicKoreanSlotExtractor;
        let mut frame = SemanticFrame::default();

        let first = extractor
            .extract(&frame, "토요일 저녁 7시에 4명", Revision(1))
            .await;
        assert!(!first.is_empty());

        for update in first {
            frame.set_slot(update);
        }

        let second = extractor
            .extract(&frame, "토요일 저녁 7시에 4명", Revision(2))
            .await;

        assert!(
            second.is_empty(),
            "identical transcript must not produce updates: {second:?}"
        );
    }
}
