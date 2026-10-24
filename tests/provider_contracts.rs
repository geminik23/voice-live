//! Paid provider contracts; ignored by default and bounded across open, I/O, and cleanup.
#![cfg(all(feature = "cartesia", feature = "together", feature = "qwen"))]

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use voice_live::asr::together::TogetherNemotronAsr;
use voice_live::asr::{AsrDuplexLimits, AsrEvent, AsrSessionConfig, StreamingAsr};
use voice_live::speech::ClaimClass;
use voice_live::tts::buffered::BufferedTtsAdapter;
use voice_live::tts::qwen::QwenRealtimeTts;
use voice_live::tts::{StreamingTts, TtsEvent, TtsInputLimits, TtsSessionOptions};

const ENGLISH_FIRST: &str = "This is a streaming speech test. ";
const ENGLISH_SECOND: &str = "Please check the appointment for Monday at ten.";

fn english_artifact(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("temp/cartesia-validation")
        .join(name)
}

#[tokio::test]
#[ignore = "paid Cartesia English TTS; writes synthetic PCM16 WAV under git-ignored temp"]
async fn cartesia_tts_english_streaming_contract() {
    let mut config = voice_live::VoiceRuntimeConfig::from_file(
        "examples/browser/configs/ko/voice-runtime.cartesia.yaml",
    )
    .unwrap()
    .tts;
    config.language = "en".into();
    config.sample_rate_hz = 16000;
    let tts = voice_live::tts::cartesia::CartesiaStreamingTts::from_config(&config)
        .expect("Cartesia credentials");
    std::fs::create_dir_all(english_artifact("")).unwrap();
    let token = CancellationToken::new();
    let mut session = tokio::time::timeout(
        Duration::from_secs(15),
        tts.open_text_stream(
            TtsSessionOptions {
                speech_id: voice_live::SpeechId::new(),
                speech_epoch: 1,
                language: "English".into(),
                voice: config.resolved_voice(),
                claim_class: ClaimClass::Phatic,
                preferred_sample_rate_hz: Some(16000),
            },
            TtsInputLimits {
                max_input_bytes: 65536,
                request_timeout: Duration::from_secs(30),
            },
            token.clone(),
        ),
    )
    .await
    .expect("bounded TTS open")
    .expect("English TTS ready");
    let result = tokio::time::timeout(Duration::from_secs(35), async {
        let started = tokio::time::Instant::now();
        session.input.push_text(ENGLISH_FIRST.into())?;
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
            if let TtsEvent::Error { message, .. } = first {
                anyhow::bail!(message);
            }
            anyhow::bail!("audio before finish required");
        };
        anyhow::ensure!(
            sample_rate == 16000 && !pcm_s16le.is_empty() && pcm_s16le.len().is_multiple_of(2),
            "invalid first PCM"
        );
        let first_audio_ms = started.elapsed().as_millis();
        let mut pcm = pcm_s16le.to_vec();
        let mut chunks = 1;
        session.input.push_text(ENGLISH_SECOND.into())?;
        session.input.finish_input()?;
        while let Some(event) = session.events.next_event().await? {
            match event {
                TtsEvent::Audio {
                    sample_rate,
                    pcm_s16le,
                    ..
                } => {
                    anyhow::ensure!(
                        sample_rate == 16000 && pcm_s16le.len().is_multiple_of(2),
                        "invalid PCM"
                    );
                    anyhow::ensure!(
                        pcm_s16le.len() <= (16000 * 2 * 15usize).saturating_sub(pcm.len()),
                        "generated fixture exceeds 15 seconds"
                    );
                    pcm.extend_from_slice(&pcm_s16le);
                    chunks += 1;
                }
                TtsEvent::AudioDone { .. } => {
                    return Ok::<_, anyhow::Error>((
                        pcm,
                        chunks,
                        first_audio_ms,
                        started.elapsed().as_millis(),
                    ));
                }
                TtsEvent::Error { message, .. } => anyhow::bail!(message),
                _ => unreachable!("unexpected event variant"),
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
    let (pcm, chunks, first_audio_ms, total_ms) = result
        .expect("bounded TTS contract")
        .expect("English TTS audio before finish and normal done");
    let mut wav = hound::WavWriter::create(
        english_artifact("english-tts-16k.wav"),
        hound::WavSpec {
            channels: 1,
            sample_rate: 16000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        },
    )
    .unwrap();
    for sample in pcm.as_chunks::<2>().0 {
        wav.write_sample(i16::from_le_bytes(*sample)).unwrap();
    }
    wav.finalize().unwrap();
    let report = serde_json::json!({"status":"passed","language":"en","model":config.model,"api_version":config.cartesia.api_version,"sample_rate":16000,"channels":1,"bits_per_sample":16,"text":format!("{ENGLISH_FIRST}{ENGLISH_SECOND}"),"chunks":chunks,"pcm_bytes":pcm.len(),"audio_duration_ms":pcm.len()/32,"first_audio_before_finish":true,"first_audio_ms":first_audio_ms,"total_ms":total_ms});
    std::fs::write(
        english_artifact("tts-english.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("English TTS: {report}");
}

#[tokio::test]
#[ignore = "paid Cartesia English STT; streams the WAV generated by the English TTS contract twice"]
async fn cartesia_stt_english_streaming_contract() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let mut wav = hound::WavReader::open(english_artifact("english-tts-16k.wav"))
        .expect("run cartesia_tts_english_streaming_contract first");
    let spec = wav.spec();
    assert_eq!(
        (spec.channels, spec.sample_rate, spec.bits_per_sample),
        (1, 16000, 16)
    );
    assert_eq!(spec.sample_format, hound::SampleFormat::Int);
    assert!(wav.duration() > 0 && wav.duration() <= 16000 * 15);
    let pcm: Vec<i16> = wav.samples::<i16>().collect::<Result<_, _>>().unwrap();
    let mut config = voice_live::VoiceRuntimeConfig::from_file(
        "examples/browser/configs/ko/voice-runtime.cartesia.yaml",
    )
    .unwrap()
    .asr;
    config.locale = "en-US".into();
    let asr = voice_live::asr::cartesia::CartesiaStreamingAsr::from_config(&config)
        .expect("Cartesia credentials");
    let token = CancellationToken::new();
    let mut session = tokio::time::timeout(
        Duration::from_secs(15),
        asr.open_duplex(
            AsrSessionConfig {
                sample_rate_hz: 16000,
                locale: "en-US".into(),
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
    .expect("bounded STT open")
    .expect("English STT ready");
    let finalized = Arc::new(AtomicBool::new(false));
    let feed_finalized = finalized.clone();
    let started = tokio::time::Instant::now();
    let feed = async {
        let mut pace = tokio::time::interval(Duration::from_millis(20));
        pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        for _ in 0..2 {
            for frame in pcm.chunks(320) {
                pace.tick().await;
                anyhow::ensure!(
                    matches!(
                        session.input.push_audio(frame.to_vec()).await?,
                        voice_live::asr::AsrAudioPush::Accepted
                    ),
                    "PCM admission failed"
                );
            }
            session.input.commit_utterance().await?;
            feed_finalized.store(true, Ordering::Release);
            for _ in 0..10 {
                pace.tick().await;
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
        let mut events = Vec::new();
        let mut partials = 0;
        let mut first_partial_ms = None;
        let mut before_finalize = false;
        while let Some(event) = session.events.next_event().await? {
            match event {
                AsrEvent::Partial {
                    utterance_id, text, ..
                } => {
                    if !text.trim().is_empty() {
                        partials += 1;
                        first_partial_ms.get_or_insert(started.elapsed().as_millis());
                        before_finalize |= !finalized.load(Ordering::Acquire);
                        if events.len() < 128 {
                            events.push(serde_json::json!({"kind":"partial","utterance_id":utterance_id,"at_ms":started.elapsed().as_millis(),"text":text}));
                        }
                    }
                }
                AsrEvent::Final { utterance_id, text } => {
                    if events.len() < 128 {
                        events.push(serde_json::json!({"kind":"final","utterance_id":utterance_id,"at_ms":started.elapsed().as_millis(),"text":text}));
                    }
                    finals.push((utterance_id, text));
                }
                AsrEvent::Error { message, .. } => anyhow::bail!(message),
                AsrEvent::InjectedFinal { .. } => {
                    anyhow::bail!("injection cannot satisfy a provider contract")
                }
                _ => unreachable!("unexpected event variant"),
            }
        }
        Ok::<_, anyhow::Error>((finals, events, partials, first_partial_ms, before_finalize))
    };
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::try_join!(feed, drain)
    })
    .await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), session.control.close())
        .await
        .expect("bounded STT cleanup")
        .expect("STT cleanup");
    let ((), (finals, events, partials, first_partial_ms, before_finalize)) = result
        .expect("bounded STT contract")
        .expect("two English utterances and done required");
    let ordered = finals.len() == 2
        && finals
            .iter()
            .enumerate()
            .all(|(index, (id, text))| *id == index as u64 && !text.trim().is_empty());
    let content_matches = ordered
        && finals.iter().all(|(_, text)| {
            let lower = text.to_ascii_lowercase();
            lower.contains("streaming")
                && lower.contains("monday")
                && (lower.contains("ten") || lower.contains("10"))
        });
    let passed = ordered && content_matches && partials > 0 && before_finalize;
    let report = serde_json::json!({"status":if passed {"passed"} else {"failed"},"language":"en","model":config.model,"api_version":config.cartesia.api_version,"fixture_duration_ms":pcm.len()/16,"repetitions":2,"partials":partials,"first_partial_ms":first_partial_ms,"partial_before_first_finalize":before_finalize,"ordered_nonempty_finals":ordered,"content_matches":content_matches,"finals":finals,"events":events,"total_ms":started.elapsed().as_millis()});
    std::fs::write(
        english_artifact("stt-english.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("English STT: {report}");
    assert!(ordered, "expected two nonempty ordered utterance finals");
    assert!(
        content_matches,
        "recognized English must retain streaming, Monday, and ten/10"
    );
    assert!(
        partials > 0 && before_finalize,
        "no nonempty partial before first finalize admission; streaming UX not established"
    );
}

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
        "examples/browser/configs/ko/voice-runtime.cartesia.yaml",
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
                _ => unreachable!("unexpected event variant"),
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
        "examples/browser/configs/ko/voice-runtime.cartesia-tts.yaml",
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
                _ => unreachable!("unexpected event variant"),
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
                _ => unreachable!("unexpected event variant"),
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
                _ => unreachable!("unexpected event variant"),
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
