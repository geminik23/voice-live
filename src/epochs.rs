use crate::ids::Revision;

/// Epochs partition stale asynchronous results. Any result tagged with an
/// older epoch than the session's current value is discarded before it can
/// reach memory, speech, or tool state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Epochs {
    pub transcript: Revision,
    pub interaction: u64,
    pub thought: u64,
    pub speech: u64,
}

impl Epochs {
    pub fn initial() -> Self {
        Self {
            transcript: Revision(1),
            interaction: 1,
            thought: 1,
            speech: 1,
        }
    }

    pub fn invalidate_thought(&mut self) -> u64 {
        self.thought += 1;
        self.thought
    }

    pub fn invalidate_speech(&mut self) -> u64 {
        self.speech += 1;
        self.speech
    }

    pub fn bump_interaction(&mut self) -> u64 {
        self.interaction += 1;
        self.interaction
    }

    pub fn bump_transcript(&mut self) -> Revision {
        self.transcript = self.transcript.next();
        self.transcript
    }
}
