//! Paid provider contracts; ignored by default and bounded across open, I/O, and cleanup.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use voice_live::asr::together::TogetherNemotronAsr;
use voice_live::asr::{AsrDuplexLimits, AsrEvent, AsrSessionConfig, StreamingAsr};
use voice_live::speech::ClaimClass;
use voice_live::tts::buffered::BufferedTtsAdapter;
use voice_live::tts::qwen::QwenRealtimeTts;
use voice_live::tts::{StreamingTts, TtsEvent, TtsInputLimits, TtsSessionOptions};

#[tokio::test]
#[ignore = "requires paid Together API key and consented WAV fixture"]
async fn together_nemotron_korean_contract() {
    let fixture_path = std::env::var("VOICE_KOREAN_FIXTURE")
        .expect("set VOICE_KOREAN_FIXTURE to a consented mono PCM16 WAV");
    let mut reader = hound::WavReader::open(fixture_path).expect("fixture opens");
    let spec = reader.spec();
    assert_eq!(
        (spec.channels, spec.sample_rate, spec.bits_per_sample),
        (1, 16_000, 16)
    );
    assert_eq!(spec.sample_format, hound::SampleFormat::Int);
    let fixture: Vec<i16> = reader
        .samples::<i16>()
        .collect::<Result<_, _>>()
        .expect("PCM samples");
    assert!(!fixture.is_empty());
    let asr = TogetherNemotronAsr::from_config(&voice_live::config::AsrConfig::default())
        .expect("ASR credentials");
    let token = CancellationToken::new();
    let mut session = tokio::time::timeout(
        Duration::from_secs(15),
        asr.open_duplex(
            AsrSessionConfig {
                sample_rate_hz: 16_000,
                locale: "ko-KR".into(),
                manual_commit: true,
            },
            AsrDuplexLimits {
                sample_budget: 16_000,
                command_budget: 64,
                write_timeout: Duration::from_secs(2),
            },
            token.clone(),
        ),
    )
    .await
    .expect("bounded ASR open")
    .expect("ready ASR session");
    let feed = async {
        for frame in fixture.chunks(320) {
            let admitted = session.input.push_audio(frame.to_vec()).await?;
            anyhow::ensure!(
                matches!(admitted, voice_live::asr::AsrAudioPush::Accepted),
                "fixture frame rejected"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        session.input.commit_utterance().await?;
        Ok::<_, anyhow::Error>(())
    };
    let drain = async {
        while let Some(event) = session.events.next_event().await? {
            match event {
                AsrEvent::Final { text, .. } => {
                    anyhow::ensure!(!text.trim().is_empty(), "empty final");
                    return Ok(());
                }
                AsrEvent::Partial { text, .. } => {
                    anyhow::ensure!(!text.trim().is_empty(), "empty partial")
                }
                AsrEvent::InjectedFinal { .. } => {
                    anyhow::bail!("provider contract cannot use injected text")
                }
                AsrEvent::Error { message, .. } => anyhow::bail!(message),
            }
        }
        anyhow::bail!("ASR closed without a final")
    };
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::try_join!(feed, drain)
    })
    .await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), session.control.close())
        .await
        .expect("bounded ASR cleanup")
        .expect("ASR cleanup");
    result
        .expect("bounded ASR contract")
        .expect("fixture must reach an authoritative final");
}

#[tokio::test]
#[ignore = "requires paid Qwen TTS API key and voice"]
async fn qwen_tts_korean_streaming_contract() {
    let config = voice_live::config::TtsConfig::default();
    let tts = BufferedTtsAdapter::new(Arc::new(
        QwenRealtimeTts::from_config(&config).expect("TTS credentials"),
    ));
    let token = CancellationToken::new();
    let mut session = tokio::time::timeout(
        Duration::from_secs(15),
        tts.open_text_stream(
            TtsSessionOptions {
                speech_id: voice_live::SpeechId::new(),
                speech_epoch: 1,
                language: "Korean".into(),
                voice: std::env::var("QWEN_VOICE_ID").expect("QWEN_VOICE_ID"),
                claim_class: ClaimClass::Factual,
                preferred_sample_rate_hz: Some(24_000),
            },
            TtsInputLimits {
                max_input_bytes: 65_536,
                request_timeout: Duration::from_secs(30),
            },
            token.clone(),
        ),
    )
    .await
    .expect("bounded TTS open")
    .expect("TTS buffered session");
    session
        .input
        .push_text("토요일 저녁 일곱 시로 확인해볼게요.".into())
        .expect("input");
    session.input.finish_input().expect("finish");
    let result = tokio::time::timeout(Duration::from_secs(35), async {
        let mut bytes = 0usize;
        let mut rate = None;
        while let Some(event) = session.events.next_event().await? {
            match event {
                TtsEvent::Audio {
                    sample_rate,
                    pcm_s16le,
                    ..
                } => {
                    anyhow::ensure!(
                        sample_rate > 0 && pcm_s16le.len().is_multiple_of(2),
                        "invalid PCM"
                    );
                    anyhow::ensure!(rate.is_none_or(|rate| rate == sample_rate), "rate changed");
                    rate = Some(sample_rate);
                    bytes += pcm_s16le.len();
                }
                TtsEvent::AudioDone { .. } => {
                    anyhow::ensure!(bytes > 0, "no audio");
                    return Ok(());
                }
                TtsEvent::Error { message, .. } => anyhow::bail!(message),
            }
        }
        anyhow::bail!("TTS closed without terminal")
    })
    .await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), session.control.close())
        .await
        .expect("bounded TTS cleanup")
        .expect("TTS cleanup");
    result
        .expect("bounded TTS contract")
        .expect("audio and terminal required");
}
