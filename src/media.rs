use anyhow::Result;

/// Incoming PCM frame with transport sequence metadata.
#[derive(Debug, Clone)]
pub struct AudioFrame {
    pub sequence: u64,
    pub pcm: Vec<i16>,
}

/// Binary wire format: 8-byte little-endian u64 sequence followed by
/// little-endian i16 samples.
pub fn encode_client_audio(sequence: u64, pcm: &[i16]) -> bytes::Bytes {
    let mut buffer = Vec::with_capacity(8 + pcm.len() * 2);
    buffer.extend_from_slice(&sequence.to_le_bytes());
    for sample in pcm {
        buffer.extend_from_slice(&sample.to_le_bytes());
    }
    bytes::Bytes::from(buffer)
}

pub fn decode_client_audio(bytes: &[u8]) -> Result<AudioFrame> {
    if bytes.len() < 8 {
        anyhow::bail!("audio frame shorter than header");
    }

    let sequence = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let sample_bytes = &bytes[8..];
    if !sample_bytes.len().is_multiple_of(2) {
        anyhow::bail!("audio payload has odd length");
    }

    let pcm = sample_bytes
        .chunks_exact(2)
        .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();

    Ok(AudioFrame { sequence, pcm })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameStatus {
    Accepted,
    Gap { missing: u64 },
    LateOrDuplicate,
}

#[derive(Debug, Default)]
pub struct FrameSequenceTracker {
    expected: Option<u64>,
    lost_frames: u64,
    reordered_frames: u64,
}

impl FrameSequenceTracker {
    pub fn accept(&mut self, sequence: u64) -> FrameStatus {
        match self.expected {
            None => {
                self.expected = Some(sequence + 1);
                FrameStatus::Accepted
            }
            Some(expected) if sequence == expected => {
                self.expected = Some(sequence + 1);
                FrameStatus::Accepted
            }
            Some(expected) if sequence > expected => {
                let gap = sequence - expected;
                self.lost_frames += gap;
                self.expected = Some(sequence + 1);
                FrameStatus::Gap { missing: gap }
            }
            Some(_) => {
                self.reordered_frames += 1;
                FrameStatus::LateOrDuplicate
            }
        }
    }

    pub fn lost_frames(&self) -> u64 {
        self.lost_frames
    }

    pub fn reordered_frames(&self) -> u64 {
        self.reordered_frames
    }
}

pub trait PcmResampler: Send {
    fn process_i16(&mut self, input: &[i16]) -> Result<Vec<i16>>;

    fn input_rate(&self) -> u32;

    fn output_rate(&self) -> u32;
}

/// Windowed-sinc low-pass sized for a decimation or interpolation ratio.
fn lowpass_kernel(taps: usize, cutoff: f32) -> Vec<f32> {
    let mut kernel = Vec::with_capacity(taps);
    let center = (taps - 1) as f32 / 2.0;
    let mut sum = 0.0f32;

    for tap in 0..taps {
        let x = tap as f32 - center;
        let sinc = if x.abs() < f32::EPSILON {
            2.0 * cutoff
        } else {
            (2.0 * cutoff * std::f32::consts::PI * x).sin() / (std::f32::consts::PI * x)
        };
        let window =
            0.54 - 0.46 * (2.0 * std::f32::consts::PI * tap as f32 / (taps - 1) as f32).cos();
        let value = sinc * window;
        sum += value;
        kernel.push(value);
    }

    for value in &mut kernel {
        *value /= sum;
    }

    kernel
}

fn clamp_to_i16(value: f32) -> i16 {
    value.clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

/// Picks the right resampler for a rate pair.
///
/// Integer ratios (the common 48 kHz -> 16 kHz browser case) use the
/// decimating path; everything else falls back to band-limited linear
/// interpolation. Constructing a resampler never silently fails: a caller that
/// gets an error must surface it rather than passing audio through at the wrong
/// rate.
pub fn build_resampler(input_rate: u32, output_rate: u32) -> Result<Box<dyn PcmResampler>> {
    if input_rate == 0 || output_rate == 0 {
        anyhow::bail!("sample rates must be non-zero: {input_rate} -> {output_rate}");
    }

    if input_rate.is_multiple_of(output_rate) {
        Ok(Box::new(DecimatingResampler::new(input_rate, output_rate)?))
    } else {
        Ok(Box::new(LinearResampler::new(input_rate, output_rate)?))
    }
}

/// Fixed-ratio decimating resampler with an anti-alias windowed-sinc kernel.
/// The demo path only needs 48 kHz to 16 kHz (ratio 1/3).
pub struct DecimatingResampler {
    input_rate: u32,
    output_rate: u32,
    kernel: Vec<f32>,
    ratio: usize,
    remainder: Vec<i16>,
    /// Absolute index (in input samples) of `remainder[0]`. Decimation phase is
    /// a property of the stream, not of the chunk it arrived in; deriving it
    /// from a per-call index makes the phase jump at every chunk boundary.
    consumed: u64,
}

impl DecimatingResampler {
    pub fn new(input_rate: u32, output_rate: u32) -> Result<Self> {
        if !input_rate.is_multiple_of(output_rate) {
            anyhow::bail!("only integer decimation is supported: {input_rate} -> {output_rate}");
        }

        let ratio = (input_rate / output_rate) as usize;
        let taps = 33;
        let cutoff = output_rate as f32 / (2.0 * input_rate as f32);

        Ok(Self {
            input_rate,
            output_rate,
            kernel: lowpass_kernel(taps, cutoff),
            ratio,
            remainder: Vec::new(),
            consumed: 0,
        })
    }
}

impl PcmResampler for DecimatingResampler {
    fn input_rate(&self) -> u32 {
        self.input_rate
    }

    fn output_rate(&self) -> u32 {
        self.output_rate
    }

    fn process_i16(&mut self, input: &[i16]) -> Result<Vec<i16>> {
        if self.ratio == 1 {
            return Ok(input.to_vec());
        }

        let mut buffer = std::mem::take(&mut self.remainder);
        buffer.extend_from_slice(input);

        let taps = self.kernel.len();
        let ratio = self.ratio as u64;
        let mut output = Vec::with_capacity(buffer.len() / self.ratio + 1);

        let mut position = 0usize;
        while position + taps <= buffer.len() {
            // Absolute index of the kernel's centre tap.
            let centre = self.consumed + (position + taps / 2) as u64;
            if centre.is_multiple_of(ratio) {
                let mut acc = 0.0f32;
                for (tap, coefficient) in self.kernel.iter().enumerate() {
                    acc += buffer[position + tap] as f32 * coefficient;
                }
                output.push(clamp_to_i16(acc));
            }
            position += 1;
        }

        self.consumed += position as u64;
        self.remainder = buffer.split_off(position.min(buffer.len()));

        Ok(output)
    }
}

/// Band-limited linear resampler for non-integer rate pairs such as
/// 44.1 kHz -> 16 kHz. The input is low-passed at the output Nyquist first, so
/// interpolation does not fold energy back into the passband.
pub struct LinearResampler {
    input_rate: u32,
    output_rate: u32,
    kernel: Vec<f32>,
    history: Vec<f32>,
    /// Last filtered sample of the previous chunk. Interpolation needs a
    /// left-hand neighbour that may live in the previous chunk, so it is
    /// carried over rather than the phase being reset.
    last_filtered: Option<f32>,
    /// Fractional read position into the filtered stream, in input samples.
    position: f64,
    step: f64,
}

impl LinearResampler {
    pub fn new(input_rate: u32, output_rate: u32) -> Result<Self> {
        if input_rate == 0 || output_rate == 0 {
            anyhow::bail!("sample rates must be non-zero: {input_rate} -> {output_rate}");
        }

        // Only band-limit when downsampling; upsampling needs no extra filter.
        let cutoff = if output_rate < input_rate {
            output_rate as f32 / (2.0 * input_rate as f32)
        } else {
            0.5
        };

        Ok(Self {
            input_rate,
            output_rate,
            kernel: lowpass_kernel(33, cutoff),
            history: Vec::new(),
            last_filtered: None,
            position: 0.0,
            step: input_rate as f64 / output_rate as f64,
        })
    }
}

impl PcmResampler for LinearResampler {
    fn input_rate(&self) -> u32 {
        self.input_rate
    }

    fn output_rate(&self) -> u32 {
        self.output_rate
    }

    fn process_i16(&mut self, input: &[i16]) -> Result<Vec<i16>> {
        let taps = self.kernel.len();

        // Low-pass into a filtered buffer that keeps `taps - 1` samples of
        // history so successive chunks join without a discontinuity.
        let mut raw = std::mem::take(&mut self.history);
        raw.extend(input.iter().map(|sample| *sample as f32));

        if raw.len() < taps {
            self.history = raw;
            return Ok(Vec::new());
        }

        let mut filtered = Vec::with_capacity(raw.len() - taps + 2);

        // Index 0 is the previous chunk's final sample, so a read position that
        // fell between the two chunks still has both neighbours. The position
        // was already rebased onto that sample at the end of the last call, so
        // it needs no further shift here.
        if let Some(previous) = self.last_filtered {
            filtered.push(previous);
        }

        for window in raw.windows(taps) {
            let mut acc = 0.0f32;
            for (tap, coefficient) in self.kernel.iter().enumerate() {
                acc += window[tap] * coefficient;
            }
            filtered.push(acc);
        }

        self.history = raw[raw.len() - (taps - 1)..].to_vec();

        let mut output = Vec::new();
        while self.position + 1.0 < filtered.len() as f64 {
            let index = self.position.floor() as usize;
            let fraction = (self.position - index as f64) as f32;
            let sample = filtered[index] + (filtered[index + 1] - filtered[index]) * fraction;
            output.push(clamp_to_i16(sample));
            self.position += self.step;
        }

        // The final sample becomes index 0 of the next chunk, so rebase the
        // position onto it. Clamping a negative position to zero instead would
        // discard up to one sample of phase per chunk and drift the output.
        self.last_filtered = filtered.last().copied();
        self.position -= (filtered.len() - 1) as f64;

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_tracker_reports_gaps_and_reorders() {
        let mut tracker = FrameSequenceTracker::default();

        assert_eq!(tracker.accept(1), FrameStatus::Accepted);
        assert_eq!(tracker.accept(2), FrameStatus::Accepted);
        assert_eq!(tracker.accept(5), FrameStatus::Gap { missing: 2 });
        assert_eq!(tracker.accept(3), FrameStatus::LateOrDuplicate);

        assert_eq!(tracker.lost_frames(), 2);
        assert_eq!(tracker.reordered_frames(), 1);
    }

    #[test]
    fn frame_codec_round_trips() {
        let pcm = vec![100, -200, 30_000];
        let encoded = encode_client_audio(7, &pcm);
        let frame = decode_client_audio(&encoded).unwrap();

        assert_eq!(frame.sequence, 7);
        assert_eq!(frame.pcm, pcm);
    }

    #[test]
    fn decimator_reduces_sample_count() {
        let mut resampler = DecimatingResampler::new(48_000, 16_000).unwrap();
        let input: Vec<i16> = (0..4_800).map(|i| (i as f32 * 0.1) as i16).collect();

        let output = resampler.process_i16(&input).unwrap();

        let expected = (input.len() / 3) as f32;
        assert!(
            (output.len() as f32 - expected).abs() <= 12.0,
            "output len {} expected near {expected}",
            output.len()
        );
    }

    /// Feeding one buffer must produce the same samples as feeding the same
    /// audio in 20 ms chunks. A per-call phase would desynchronise the two.
    #[test]
    fn decimator_phase_is_continuous_across_chunks() {
        let input: Vec<i16> = (0..9_600)
            .map(|i| ((i as f32 / 12.0).sin() * 8_000.0) as i16)
            .collect();

        let mut whole = DecimatingResampler::new(48_000, 16_000).unwrap();
        let single = whole.process_i16(&input).unwrap();

        let mut chunked = DecimatingResampler::new(48_000, 16_000).unwrap();
        let mut streamed = Vec::new();
        for chunk in input.chunks(960) {
            streamed.extend(chunked.process_i16(chunk).unwrap());
        }

        assert_eq!(
            single, streamed,
            "chunked decimation must match single-shot decimation"
        );
    }

    fn resample_seconds(seconds: usize) -> usize {
        let mut resampler = LinearResampler::new(44_100, 16_000).unwrap();
        let input: Vec<i16> = (0..44_100 * seconds)
            .map(|i| ((i as f32 / 20.0).sin() * 6_000.0) as i16)
            .collect();

        input
            .chunks(882)
            .map(|chunk| resampler.process_i16(chunk).unwrap().len())
            .sum()
    }

    #[test]
    fn linear_resampler_handles_non_integer_ratios() {
        let mut resampler = LinearResampler::new(44_100, 16_000).unwrap();
        let input: Vec<i16> = (0..44_100)
            .map(|i| ((i as f32 / 20.0).sin() * 6_000.0) as i16)
            .collect();

        let mut output = Vec::new();
        for chunk in input.chunks(882) {
            output.extend(resampler.process_i16(chunk).unwrap());
        }

        // One second in, one second out, less the fixed 33-tap filter warmup.
        assert!(
            (output.len() as i64 - 16_000).abs() <= 16,
            "expected roughly 16000 samples, got {}",
            output.len()
        );
        assert!(
            output.iter().any(|sample| sample.abs() > 1_000),
            "resampled audio must not be silence"
        );
    }

    /// The only shortfall allowed is the one-time filter warmup, so it must be
    /// the *same* for one second of audio and for ten.
    ///
    /// Rebasing the read position onto the carried-over sample is what makes
    /// this hold. Clamping a negative position to zero, or shifting it twice,
    /// loses a fraction of a sample per chunk, which compounds with length.
    #[test]
    fn linear_resampler_does_not_drift_over_a_long_stream() {
        let short_deficit = 16_000 - resample_seconds(1) as i64;
        let long_deficit = 160_000 - resample_seconds(10) as i64;

        assert_eq!(
            short_deficit, long_deficit,
            "shortfall must be a constant warmup, not proportional to length"
        );
    }

    #[test]
    fn build_resampler_picks_a_strategy_and_never_fails_silently() {
        assert_eq!(
            build_resampler(48_000, 16_000).unwrap().output_rate(),
            16_000
        );
        assert_eq!(
            build_resampler(44_100, 16_000).unwrap().output_rate(),
            16_000
        );
        assert!(build_resampler(0, 16_000).is_err());
    }
}
