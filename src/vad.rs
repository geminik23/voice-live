use crate::config::VadConfig;

/// Server-side energy VAD with adaptive noise floor and hysteresis. It drives
/// immediate duck and mic-state tracking; provider endpointing runs beside it.
#[derive(Debug)]
pub struct EnergyVad {
    config: VadConfig,
    noise_floor: f32,
    onset_count: u32,
    offset_count: u32,
    speaking: bool,
    speech_started_at_us: Option<u64>,
    last_voice_us: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VadTransition {
    None,
    SpeechStarted { probability: f32 },
    SpeechEnded { duration_ms: u64 },
}

impl EnergyVad {
    pub fn new(config: VadConfig) -> Self {
        Self {
            config,
            // The floor starts low and only adapts during non-speech so a
            // loud first frame can never be mistaken for ambient noise.
            noise_floor: 100.0,
            onset_count: 0,
            offset_count: 0,
            speaking: false,
            speech_started_at_us: None,
            last_voice_us: None,
        }
    }

    pub fn is_speaking(&self) -> bool {
        self.speaking
    }

    pub fn last_voice_us(&self) -> Option<u64> {
        self.last_voice_us
    }

    pub fn process(&mut self, pcm: &[i16], now_us: u64) -> VadTransition {
        let rms = frame_rms(pcm);

        let onset_threshold = self.noise_floor * self.config.onset_ratio;
        let offset_threshold = self.noise_floor * self.config.offset_ratio;

        if !self.speaking {
            // Adapt the floor only while idle so speech never raises it.
            if rms < self.noise_floor {
                self.noise_floor = 0.9 * self.noise_floor + 0.1 * (rms + 1.0);
            } else if rms < onset_threshold {
                self.noise_floor = 0.99 * self.noise_floor + 0.01 * rms;
            }

            if rms >= onset_threshold {
                self.onset_count += 1;
                self.offset_count = 0;
            } else {
                self.onset_count = 0;
            }

            if self.onset_count >= self.config.onset_frames.max(1) {
                self.speaking = true;
                self.speech_started_at_us = Some(now_us);
                self.last_voice_us = Some(now_us);
                self.onset_count = 0;
                let probability =
                    ((rms / onset_threshold - 1.0).clamp(0.0, 1.0) * 0.5 + 0.5).clamp(0.0, 1.0);
                return VadTransition::SpeechStarted { probability };
            }
        } else {
            if rms >= offset_threshold {
                self.offset_count = 0;
                self.last_voice_us = Some(now_us);
            } else {
                self.offset_count += 1;
            }

            if self.offset_count >= self.config.offset_frames.max(1) {
                self.speaking = false;
                self.offset_count = 0;
                let duration_ms = self
                    .speech_started_at_us
                    .map(|started| now_us.saturating_sub(started) / 1_000)
                    .unwrap_or(0);
                return VadTransition::SpeechEnded { duration_ms };
            }
        }

        VadTransition::None
    }
}

fn frame_rms(pcm: &[i16]) -> f32 {
    if pcm.is_empty() {
        return 0.0;
    }

    let sum: f64 = pcm
        .iter()
        .map(|s| {
            let sample = *s as f64;
            sample * sample
        })
        .sum();

    (sum / pcm.len() as f64).sqrt() as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vad() -> EnergyVad {
        EnergyVad::new(VadConfig::default())
    }

    fn quiet(n: usize) -> Vec<i16> {
        vec![10; n]
    }

    fn loud(n: usize) -> Vec<i16> {
        vec![3_000; n]
    }

    #[test]
    fn onset_requires_consecutive_frames() {
        let mut vad = vad();
        let frame = loud(320);

        assert_eq!(vad.process(&frame, 0), VadTransition::None);
        assert_eq!(vad.process(&frame, 20_000), VadTransition::None);
        assert!(matches!(
            vad.process(&frame, 40_000),
            VadTransition::SpeechStarted { .. }
        ));
    }

    #[test]
    fn offset_requires_longer_hysteresis() {
        let mut vad = vad();
        let frame = loud(320);
        let silence = quiet(320);

        for i in 0..3 {
            vad.process(&frame, i * 20_000);
        }

        assert!(vad.is_speaking());

        let mut ended = false;
        for i in 0..12 {
            if let VadTransition::SpeechEnded { duration_ms } =
                vad.process(&silence, (i + 4) * 20_000)
            {
                assert!(duration_ms > 0);
                ended = true;
            }
        }

        assert!(ended);
        assert!(!vad.is_speaking());
    }
}
