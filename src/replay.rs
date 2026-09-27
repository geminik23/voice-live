use std::path::Path;
use std::time::Duration;

use crate::media::AudioFrame;

pub struct WavFrameSource {
    samples: Vec<i16>,
    sample_rate: u32,
    frame_samples: usize,
    cursor: usize,
    sequence: u64,
}

impl WavFrameSource {
    pub fn open(path: impl AsRef<Path>, frame_ms: u64) -> anyhow::Result<Self> {
        let mut reader = hound::WavReader::open(path)?;
        let spec = reader.spec();
        if spec.bits_per_sample != 16 || spec.sample_format != hound::SampleFormat::Int {
            anyhow::bail!("replay WAV must use signed 16-bit PCM");
        }
        if spec.channels == 0 {
            anyhow::bail!("replay WAV must contain at least one channel");
        }

        let interleaved: Vec<i16> = reader.samples::<i16>().collect::<Result<_, _>>()?;
        let samples = if spec.channels == 1 {
            interleaved
        } else {
            interleaved
                .chunks(spec.channels as usize)
                .map(|frame| {
                    let sum: i32 = frame.iter().map(|sample| *sample as i32).sum();
                    (sum / frame.len() as i32) as i16
                })
                .collect()
        };

        let frame_samples = (spec.sample_rate as u64 * frame_ms / 1_000) as usize;
        if frame_samples == 0 {
            anyhow::bail!("frame_ms is too small for the WAV sample rate");
        }

        Ok(Self {
            samples,
            sample_rate: spec.sample_rate,
            frame_samples,
            cursor: 0,
            sequence: 0,
        })
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn next_frame(&mut self) -> Option<TimedAudioFrame> {
        if self.cursor >= self.samples.len() {
            return None;
        }

        let end = (self.cursor + self.frame_samples).min(self.samples.len());
        let offset_ms = self.cursor as u64 * 1_000 / self.sample_rate as u64;
        self.sequence += 1;
        let frame = AudioFrame {
            sequence: self.sequence,
            pcm: self.samples[self.cursor..end].to_vec(),
        };
        self.cursor = end;

        Some(TimedAudioFrame { offset_ms, frame })
    }
}

pub struct TimedAudioFrame {
    pub offset_ms: u64,
    pub frame: AudioFrame,
}

pub async fn replay_wav_realtime(
    source: &mut WavFrameSource,
    tx: tokio::sync::mpsc::Sender<AudioFrame>,
) -> anyhow::Result<()> {
    let start = tokio::time::Instant::now();

    while let Some(frame) = source.next_frame() {
        tokio::time::sleep_until(start + Duration::from_millis(frame.offset_ms)).await;
        tx.send(frame.frame)
            .await
            .map_err(|_| anyhow::anyhow!("audio replay receiver closed"))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_source_splits_stereo_into_mono_frames() {
        let path = std::env::temp_dir().join(format!("replay-{}.wav", uuid::Uuid::new_v4()));
        let spec = hound::WavSpec {
            channels: 2,
            sample_rate: 16_000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut writer = hound::WavWriter::create(&path, spec).unwrap();
        for index in 0..640 {
            writer.write_sample(index as i16).unwrap();
            writer
                .write_sample((index as i16).saturating_neg())
                .unwrap();
        }
        writer.finalize().unwrap();

        let mut source = WavFrameSource::open(&path, 20).unwrap();
        let first = source.next_frame().unwrap();
        let second = source.next_frame().unwrap();

        assert_eq!(source.sample_rate(), 16_000);
        assert_eq!(first.frame.pcm.len(), 320);
        assert_eq!(first.offset_ms, 0);
        assert_eq!(second.offset_ms, 20);
        assert!(first.frame.pcm.iter().all(|sample| *sample == 0));
        assert!(source.next_frame().is_none());

        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn replay_obeys_frame_timestamps() {
        let path = std::env::temp_dir().join(format!("replay-{}.wav", uuid::Uuid::new_v4()));
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: 16_000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut writer = hound::WavWriter::create(&path, spec).unwrap();
        for _ in 0..640 {
            writer.write_sample(0_i16).unwrap();
        }
        writer.finalize().unwrap();

        let mut source = WavFrameSource::open(&path, 20).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let start = tokio::time::Instant::now();
        replay_wav_realtime(&mut source, tx).await.unwrap();

        assert_eq!(rx.recv().await.unwrap().sequence, 1);
        assert_eq!(rx.recv().await.unwrap().sequence, 2);
        assert!(start.elapsed() >= Duration::from_millis(20));

        std::fs::remove_file(path).unwrap();
    }
}
