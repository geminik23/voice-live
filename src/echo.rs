use crate::config::EchoConfig;

/// Text similarity guard against assistant self-echo reaching ASR. This is
/// evidence, never a verdict: the supervisor combines it with playback state
/// and mic onset timing.
pub fn normalize_transcript(text: &str) -> String {
    text.chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ')
        .flat_map(|c| c.to_lowercase())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Sorensen-Dice over character bigrams. Korean substrings match well without
/// tokenization.
pub fn normalized_similarity(a: &str, b: &str) -> f32 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let bigrams_a = bigrams(a);
    let bigrams_b = bigrams(b);

    if bigrams_a.is_empty() || bigrams_b.is_empty() {
        let shorter = a.chars().count().min(b.chars().count());
        let longer = a.chars().count().max(b.chars().count());
        return if longer == 0 {
            0.0
        } else {
            shorter as f32 / longer as f32
        };
    }

    let mut matches = 0usize;
    let mut remaining = bigrams_b.clone();
    for bigram in &bigrams_a {
        if let Some(position) = remaining.iter().position(|candidate| candidate == bigram) {
            remaining.remove(position);
            matches += 1;
        }
    }

    (2 * matches) as f32 / (bigrams_a.len() + bigrams_b.len()) as f32
}

fn bigrams(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() < 2 {
        return Vec::new();
    }

    (0..chars.len() - 1)
        .map(|i| chars[i..i + 2].iter().collect())
        .collect()
}

pub fn likely_self_echo(
    inbound: &str,
    recent_assistant_text: &str,
    assistant_playing: bool,
    config: &EchoConfig,
) -> bool {
    if !config.enabled || !assistant_playing {
        return false;
    }

    let inbound = normalize_transcript(inbound);
    let outbound = normalize_transcript(recent_assistant_text);

    if inbound.chars().count() < config.min_chars {
        return false;
    }

    normalized_similarity(&inbound, &outbound) >= config.similarity_threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> EchoConfig {
        EchoConfig::default()
    }

    #[test]
    fn echo_is_detected() {
        let spoken = "토요일 저녁 일곱 시로 확인해볼게요.";
        let inbound = "토요일 저녁 일곱 시로 확인해볼게요";

        assert!(likely_self_echo(inbound, spoken, true, &config()));
        assert!(!likely_self_echo(inbound, spoken, false, &config()));
    }

    #[test]
    fn distinct_user_speech_is_not_echo() {
        let spoken = "토요일 저녁 일곱 시로 확인해볼게요.";
        let inbound = "아니 금요일 말고 토요일로 해주세요";

        assert!(!likely_self_echo(inbound, spoken, true, &config()));
    }

    #[test]
    fn short_backchannel_is_never_echo() {
        assert!(!likely_self_echo(
            "네",
            "네, 확인해볼게요.",
            true,
            &config()
        ));
    }

    #[test]
    fn similarity_is_symmetric_and_ordered() {
        let a = "금요일 말고 토요일";
        let b = "금요일 말고 토요일";

        assert!((normalized_similarity(a, b) - 1.0).abs() < f32::EPSILON);
        assert!(normalized_similarity(a, "완전히 다른 문장") < 0.4);
    }
}
