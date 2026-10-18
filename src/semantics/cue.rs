use regex::RegexSet;
use serde::{Deserialize, Serialize};

use crate::config::TurnControlConfig;

/// Advisory semantic cues; they never authorize speech or side effects.
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
    hard_stop: Vec<String>,
    correction: RegexSet,
    negation: RegexSet,
    intent: Vec<(String, RegexSet)>,
    entity_patterns: Vec<(String, regex::Regex)>,
}

impl CueExtractor {
    pub fn from_config(config: &TurnControlConfig) -> Self {
        Self::try_from_config(config).expect("validated text rules")
    }
    pub fn try_from_config(config: &TurnControlConfig) -> anyhow::Result<Self> {
        super::text::validate_rules(config)?;
        Ok(Self {
            hard_stop: config
                .hard_stop_phrases
                .iter()
                .map(|p| p.to_lowercase())
                .collect(),
            correction: RegexSet::new(&config.text_rules.correction)?,
            negation: RegexSet::new(&config.text_rules.negation)?,
            intent: config
                .text_rules
                .intents
                .iter()
                .map(|(name, patterns)| Ok((name.clone(), RegexSet::new(patterns)?)))
                .collect::<anyhow::Result<_>>()?,
            entity_patterns: config
                .text_rules
                .entities
                .iter()
                .map(|(name, pattern)| Ok((name.clone(), regex::Regex::new(pattern)?)))
                .collect::<anyhow::Result<_>>()?,
        })
    }
    pub fn extract(&self, text: &str) -> Vec<SemanticCue> {
        let mut cues = Vec::new();
        if self.is_hard_stop(text) {
            cues.push(SemanticCue::HardStop {
                phrase: text.to_owned(),
            });
        }
        if self.is_correction(text) {
            cues.push(SemanticCue::Correction { target_slot: None });
        }
        if self.negation.is_match(text) {
            cues.push(SemanticCue::Negation { target: None });
        }
        for (slot, pattern) in &self.entity_patterns {
            if let Some(found) = pattern.find(text) {
                cues.push(SemanticCue::EntityCandidate {
                    slot: slot.clone(),
                    value: serde_json::Value::String(found.as_str().to_owned()),
                    confidence: 0.7,
                });
            }
        }
        if let Some((name, _)) = self.intent.iter().find(|(_, set)| set.is_match(text)) {
            cues.push(SemanticCue::IntentCandidate {
                intent: name.clone(),
                confidence: 0.75,
            });
        }
        cues
    }
    pub fn is_hard_stop(&self, text: &str) -> bool {
        let text = text.to_lowercase();
        self.hard_stop.iter().any(|p| text.contains(p))
    }
    pub fn is_correction(&self, text: &str) -> bool {
        self.correction.is_match(text)
    }
}
