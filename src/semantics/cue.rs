use regex::RegexSet;
use serde::{Deserialize, Serialize};

use crate::config::TurnControlConfig;

/// Normalized semantic events extracted from partial transcripts before any
/// LLM sees them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SemanticCue {
    HardStop {
        phrase: String,
    },
    Correction {
        target_slot: Option<String>,
    },
    Negation {
        target: Option<String>,
    },
    EntityCandidate {
        slot: String,
        value: serde_json::Value,
        confidence: f32,
    },
    IntentCandidate {
        intent: String,
        confidence: f32,
    },
    CommitCue,
}

pub struct CueExtractor {
    hard_stop: RegexSet,
    correction: RegexSet,
    negation: RegexSet,
    intent: Vec<(String, RegexSet)>,
    entity_patterns: Vec<(&'static str, regex::Regex)>,
}

impl CueExtractor {
    pub fn from_config(config: &TurnControlConfig) -> Self {
        let hard_stop = RegexSet::new(&config.hard_stop_phrases).unwrap_or_default();

        let correction = RegexSet::new([
            r"말고",
            r"아니(요|라)?\s",
            r"바꿔",
            r"변경(해|해줘|해 주세요)",
            r"다시\s*(해|말씀|정정|설정)",
            r"정정",
        ])
        .unwrap_or_default();

        let negation = RegexSet::new([
            r"하지\s*마",
            r"하지마",
            r"싫어",
            r"필요\s*없",
            r"없애",
            r"안\s*(돼|되|하세요|해)",
            r"되지\s*않",
        ])
        .unwrap_or_default();

        let intent = vec![
            (
                "reservation".into(),
                RegexSet::new([r"예약", r"잡아(줘|주세요|주실)", r"booking"]).unwrap(),
            ),
            (
                "search".into(),
                RegexSet::new([r"찾아(줘|주세요|주실)", r"검색", r"검색해", r"찾아줄래"]).unwrap(),
            ),
            (
                "cancellation".into(),
                RegexSet::new([r"취소(해|해줘|해 주세요|할게)?", r"끊어(줘|주세요)"]).unwrap(),
            ),
        ];

        let entity_patterns = vec![
            (
                "party_size",
                regex::Regex::new(r"(\d{1,2})\s*(명|사람|인분)").unwrap(),
            ),
            (
                "time",
                regex::Regex::new(
                    r"(오전|오후|아침|저녁|밤)?\s*(\d{1,2})\s*시(\s*반)?|\d{1,2}:\d{2}",
                )
                .unwrap(),
            ),
            (
                "date",
                regex::Regex::new(
                    r"(오늘|내일|모레|글피|주말|평일|이번\s*주말|다음\s*주말|(월|화|수|목|금|토|일)요일|\d{1,2}\s*일)",
                )
                .unwrap(),
            ),
        ];

        Self {
            hard_stop,
            correction,
            negation,
            intent,
            entity_patterns,
        }
    }

    pub fn extract(&self, text: &str) -> Vec<SemanticCue> {
        let mut cues = Vec::new();

        if self.hard_stop.is_match(text) {
            cues.push(SemanticCue::HardStop {
                phrase: text.to_string(),
            });
        }

        if self.correction.is_match(text) {
            cues.push(SemanticCue::Correction { target_slot: None });
        }

        if self.negation.is_match(text) {
            cues.push(SemanticCue::Negation { target: None });
        }

        for (slot, pattern) in &self.entity_patterns {
            if let Some(captures) = pattern.captures(text) {
                let raw = captures
                    .get(0)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default();
                let normalized = normalize_entity(slot, &raw);
                cues.push(SemanticCue::EntityCandidate {
                    slot: slot.to_string(),
                    value: serde_json::Value::String(normalized),
                    confidence: 0.7,
                });
            }
        }

        let mut best_intent: Option<(String, f32)> = None;
        for (intent, set) in &self.intent {
            if set.is_match(text) {
                let confidence = 0.75;
                if best_intent
                    .as_ref()
                    .map(|(_, c)| confidence > *c)
                    .unwrap_or(true)
                {
                    best_intent = Some((intent.clone(), confidence));
                }
            }
        }

        if let Some((intent, confidence)) = best_intent {
            cues.push(SemanticCue::IntentCandidate { intent, confidence });
        }

        cues
    }

    pub fn is_hard_stop(&self, text: &str) -> bool {
        self.hard_stop.is_match(text)
    }

    pub fn is_correction(&self, text: &str) -> bool {
        self.correction.is_match(text)
    }
}

fn normalize_entity(slot: &str, raw: &str) -> String {
    match slot {
        "party_size" => raw
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse::<u32>()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| raw.to_string()),
        _ => raw.split_whitespace().collect::<Vec<_>>().join(" "),
    }
}
