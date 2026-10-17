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
#[ignore = "requires paid Cartesia key and consented Korean WAV; streams the fixture twice"]
async fn cartesia_stt_korean_streaming_contract() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let path = std::env::var("VOICE_KOREAN_FIXTURE").expect("consented Korean PCM16 WAV required");
    let mut reader = hound::WavReader::open(path).expect("fixture opens");
    let spec = reader.spec();
    assert_eq!(
        (spec.channels, spec.sample_rate, spec.bits_per_sample),
        (1, 16000, 16)
    );
    assert_eq!(spec.sample_format, hound::SampleFormat::Int);
    let pcm: Vec<i16> = reader
        .samples::<i16>()
        .collect::<Result<_, _>>()
        .expect("PCM16 fixture");
    assert!(
        !pcm.is_empty() && pcm.len() <= 16000 * 15,
        "use a consented fixture of at most 15 seconds"
    );
    let config = voice_live::VoiceRuntimeConfig::from_file(
        "full-duplex-demo/configs/voice-runtime.cartesia.yaml",
    )
    .unwrap();
    let asr = voice_live::asr::cartesia::CartesiaStreamingAsr::from_config(&config.asr)
        .expect("Cartesia credentials");
    let token = CancellationToken::new();
    let mut session = tokio::time::timeout(
        Duration::from_secs(15),
        asr.open_duplex(
            AsrSessionConfig {
                sample_rate_hz: 16000,
                locale: "ko-KR".into(),
                manual_commit: true,
            },
            AsrDuplexLimits {
                sample_budget: 16000,
                command_budget: 64,
                write_timeout: Duration::from_secs(2),
            },
            token.clone(),
        ),
    )
    .await
    .expect("bounded open")
    .expect("Cartesia ASR ready");
    let first_finalize = Arc::new(AtomicBool::new(false));
    let finalized = first_finalize.clone();
    let started = tokio::time::Instant::now();
    let feed = async {
        let mut pacing = tokio::time::interval(Duration::from_millis(20));
        pacing.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        for _ in 0..2 {
            for frame in pcm.chunks(320) {
                pacing.tick().await;
                anyhow::ensure!(
                    matches!(
                        session.input.push_audio(frame.to_vec()).await?,
                        voice_live::asr::AsrAudioPush::Accepted
                    ),
                    "PCM admission failed"
                );
            }
            session.input.commit_utterance().await?;
            finalized.store(true, Ordering::Release);
            for _ in 0..10 {
                pacing.tick().await;
                anyhow::ensure!(
                    matches!(
                        session.input.push_audio(vec![0; 320]).await?,
                        voice_live::asr::AsrAudioPush::Accepted
                    ),
                    "silence admission failed"
                );
            }
        }
        session.input.finish_input()?;
        Ok::<_, anyhow::Error>(())
    };
    let drain = async {
        let mut finals = Vec::new();
        let mut partials = 0;
        let mut first_latency = None;
        let mut during_speech = false;
        while let Some(event) = session.events.next_event().await? {
            match event {
                AsrEvent::Partial { text, .. } => {
                    if !text.trim().is_empty() {
                        partials += 1;
                        first_latency.get_or_insert(started.elapsed().as_millis());
                        during_speech |= !first_finalize.load(Ordering::Acquire);
                    }
                }
                AsrEvent::Final { utterance_id, text } => {
                    anyhow::ensure!(
                        utterance_id == finals.len() as u64 && !text.trim().is_empty(),
                        "invalid or empty flush attribution"
                    );
                    finals.push(text);
                }
                AsrEvent::Error { message, .. } => anyhow::bail!(message),
                AsrEvent::InjectedFinal { .. } => {
                    anyhow::bail!("injection is not a provider contract")
                }
            }
        }
        anyhow::ensure!(
            finals.len() == 2,
            "expected two complete fixture utterances"
        );
        anyhow::ensure!(
            partials > 0 && during_speech,
            "no nonempty partial before first finalize admission; streaming UX not established"
        );
        eprintln!(
            "Cartesia STT: partials={partials}, first_partial_ms={first_latency:?}, finals={}, total_ms={}",
            finals.len(),
            started.elapsed().as_millis()
        );
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::try_join!(feed, drain)
    })
    .await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), session.control.close())
        .await
        .expect("bounded cleanup")
        .expect("cleanup");
    result
        .expect("bounded STT contract")
        .expect("streaming partials, two flushes, and done required");
}

#[tokio::test]
#[ignore = "requires paid Cartesia API key and consented Korean voice; sends text externally"]
async fn cartesia_tts_korean_streaming_contract() {
    let config = voice_live::VoiceRuntimeConfig::from_file(
        "full-duplex-demo/configs/voice-runtime.cartesia-tts.yaml",
    )
    .unwrap();
    let tts = voice_live::tts::cartesia::CartesiaStreamingTts::from_config(&config.tts)
        .expect("Cartesia credentials");
    let token = CancellationToken::new();
    let mut session = tokio::time::timeout(
        Duration::from_secs(15),
        tts.open_text_stream(
            TtsSessionOptions {
                speech_id: voice_live::SpeechId::new(),
                speech_epoch: 1,
                language: "ko".into(),
                voice: config.tts.resolved_voice(),
                claim_class: ClaimClass::Phatic,
                preferred_sample_rate_hz: Some(24000),
            },
            TtsInputLimits {
                max_input_bytes: 65536,
                request_timeout: Duration::from_secs(30),
            },
            token.clone(),
        ),
    )
    .await
    .expect("bounded open")
    .expect("Cartesia text session");
    let result = tokio::time::timeout(Duration::from_secs(35), async {
        let admitted_at = tokio::time::Instant::now();
        session
            .input
            .push_text("안녕하세요. 오늘의 예약 시간을 확인하고 있습니다. ".into())?;
        let first = session
            .events
            .next_event()
            .await?
            .ok_or_else(|| anyhow::anyhow!("closed before first audio"))?;
        let TtsEvent::Audio {
            sample_rate,
            pcm_s16le,
            ..
        } = first
        else {
            anyhow::bail!("audio before finish required: {first:?}");
        };
        anyhow::ensure!(
            sample_rate == 24000 && !pcm_s16le.is_empty() && pcm_s16le.len().is_multiple_of(2),
            "invalid first PCM"
        );
        eprintln!(
            "Cartesia first audio before finish: {} ms",
            admitted_at.elapsed().as_millis()
        );
        session
            .input
            .push_text("토요일 저녁 일곱 시로 확인해볼게요.".into())?;
        session.input.finish_input()?;
        while let Some(event) = session.events.next_event().await? {
            match event {
                TtsEvent::Audio {
                    sample_rate,
                    pcm_s16le,
                    ..
                } => anyhow::ensure!(
                    sample_rate == 24000 && pcm_s16le.len().is_multiple_of(2),
                    "invalid PCM"
                ),
                TtsEvent::AudioDone { .. } => return Ok::<_, anyhow::Error>(()),
                TtsEvent::Error { message, .. } => anyhow::bail!(message),
            }
        }
        anyhow::bail!("Cartesia closed without terminal")
    })
    .await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), session.control.close())
        .await
        .expect("bounded cleanup")
        .expect("cleanup");
    result
        .expect("bounded Cartesia contract")
        .expect("streaming audio and success terminal");
}

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
