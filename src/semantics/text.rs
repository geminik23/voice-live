use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::cue::{CueExtractor, SemanticCue};
use crate::config::TurnControlConfig;

/// Application-owned advisory rules, compiled once at session construction.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TextRules {
    pub correction: Vec<String>,
    pub negation: Vec<String>,
    pub intents: BTreeMap<String, Vec<String>>,
    pub entities: BTreeMap<String, String>,
}

#[derive(Default)]
pub struct UserTextAnalysis {
    pub cues: Vec<SemanticCue>,
    pub backchannel: bool,
    pub complete: bool,
    pub incomplete: bool,
}

/// Pure, synchronous interpretation; authorization, clocks and state remain in the runtime.
pub trait TextAnalyzer: Send + Sync {
    fn user(&self, text: &str) -> UserTextAnalysis;
}

pub struct ConfiguredTextAnalyzer {
    config: TurnControlConfig,
    cues: CueExtractor,
}

impl ConfiguredTextAnalyzer {
    pub fn new(config: &TurnControlConfig) -> anyhow::Result<Self> {
        validate_rules(config)?;
        Ok(Self {
            config: config.clone(),
            cues: CueExtractor::try_from_config(config)?,
        })
    }
    pub fn shared(config: &TurnControlConfig) -> anyhow::Result<Arc<dyn TextAnalyzer>> {
        Ok(Arc::new(Self::new(config)?))
    }
}

pub fn validate_rules(config: &TurnControlConfig) -> anyhow::Result<()> {
    let rules = &config.text_rules;
    let regexes: Vec<&str> = rules
        .correction
        .iter()
        .chain(&rules.negation)
        .chain(rules.intents.values().flatten())
        .map(String::as_str)
        .chain(rules.entities.values().map(String::as_str))
        .collect();
    let literals: Vec<&str> = config
        .hard_stop_phrases
        .iter()
        .chain(&config.backchannel_words)
        .chain(&config.complete_endings)
        .chain(&config.incomplete_endings)
        .map(String::as_str)
        .collect();
    anyhow::ensure!(
        regexes.len() + literals.len() <= 256,
        "too many text rules (maximum 256)"
    );
    for pattern in regexes {
        anyhow::ensure!(
            !pattern.is_empty() && pattern.len() <= 1024,
            "text regex must contain 1 to 1024 bytes"
        );
        let re = regex::RegexBuilder::new(pattern)
            .size_limit(1 << 20)
            .build()?;
        let syntax = regex_syntax::Parser::new().parse(pattern)?;
        anyhow::ensure!(
            syntax.properties().minimum_len().is_none_or(|len| len > 0) && !re.is_match(""),
            "text regex must consume text, not only assertions"
        );
    }
    for literal in literals {
        anyhow::ensure!(
            !literal.trim().is_empty() && literal.len() <= 1024,
            "text literal must contain 1 to 1024 bytes"
        );
    }
    Ok(())
}

fn folded(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn backchannel_key(text: &str) -> String {
    folded(
        &text
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .collect::<String>(),
    )
}

fn ending(text: &str, patterns: &[String]) -> bool {
    let raw = text.trim_end();
    let text = folded(raw.trim_end_matches(['.', '?', '!', '。', '？', '！']));
    patterns.iter().any(|p| {
        if p.chars().all(|c| c.is_ascii_punctuation()) {
            return raw.ends_with(p);
        }
        text.ends_with(&folded(p))
    })
}

impl TextAnalyzer for ConfiguredTextAnalyzer {
    fn user(&self, text: &str) -> UserTextAnalysis {
        UserTextAnalysis {
            cues: self.cues.extract(text),
            backchannel: {
                let key = backchannel_key(text);
                !key.is_empty()
                    && self
                        .config
                        .backchannel_words
                        .iter()
                        .any(|word| backchannel_key(word) == key)
            },
            complete: ending(text, &self.config.complete_endings),
            incomplete: self
                .config
                .incomplete_endings
                .iter()
                .any(|p| text.trim_end().ends_with(p))
                || ending(text, &self.config.incomplete_endings),
        }
    }
}
