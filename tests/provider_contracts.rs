//! Paid provider contract tests. These are ignored by default and validate
//! the real Together ASR and Qwen TTS wire adapters when credentials exist.
//! Run with: cargo test --test provider_contracts --
//! --ignored

use tokio_util::sync::CancellationToken;
use voice_live::asr::together::TogetherNemotronAsr;
use voice_live::asr::{AsrConfig, AsrEvent, AsrSessionConfig, StreamingAsr};
use voice_live::ids::SpeechId;
use voice_live::speech::ClaimClass;
use voice_live::tts::qwen::QwenRealtimeTts;
use voice_live::tts::{StreamingTts, TtsEvent, TtsRequest};

fn asr_config() -> AsrConfig {
    AsrConfig::default()
}

#[tokio::test]
#[ignore = "requires paid Together API key"]
async fn together_nemotron_korean_contract() {
    let key_present = std::env::var("TOGETHER_API_KEY").is_ok();
    assert!(key_present, "TOGETHER_API_KEY must be set");

    let asr = TogetherNemotronAsr::from_config(&asr_config()).unwrap();
    let config = AsrSessionConfig {
        sample_rate_hz: 16_000,
        locale: "ko-KR".into(),
        manual_commit: false,
    };

    let mut session = asr.open(config).await.expect("asr session opens");

    let fixture_path = std::env::var("VOICE_KOREAN_FIXTURE")
        .expect("VOICE_KOREAN_FIXTURE must point to a Korean 16 kHz PCM WAV");
    let mut reader = hound::WavReader::open(fixture_path).expect("fixture opens");
    let spec = reader.spec();
    assert_eq!(spec.channels, 1, "fixture must be mono");
    assert_eq!(spec.sample_rate, 16_000, "fixture must be 16 kHz");
    assert_eq!(spec.bits_per_sample, 16, "fixture must be signed PCM16");
    let fixture: Vec<i16> = reader
        .samples::<i16>()
        .collect::<Result<_, _>>()
        .expect("fixture samples decode");

    for frame in fixture.chunks(320) {
        session.push_audio(frame).await.expect("push audio");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    session.commit_audio().await.expect("commit");

    let start = std::time::Instant::now();
    let mut saw_event = false;

    while start.elapsed() < std::time::Duration::from_secs(20) {
        match session.next_event().await.expect("asr event") {
            Some(AsrEvent::Partial { text, .. }) => {
                saw_event = true;
                assert!(!text.trim().is_empty(), "partial text must be non-empty");
            }
            Some(AsrEvent::Final { text, .. }) => {
                saw_event = true;
                assert!(!text.trim().is_empty(), "final text must be non-empty");
                break;
            }
            Some(AsrEvent::Error { message, .. }) => {
                panic!("asr provider error: {message}");
            }
            None => break,
        }
    }

    assert!(saw_event, "expected at least one transcript event");
    session.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires paid Qwen TTS API key"]
async fn qwen_tts_korean_streaming_contract() {
    let key_present = std::env::var("DASHSCOPE_API_KEY").is_ok();
    assert!(key_present, "DASHSCOPE_API_KEY must be set");

    let tts = QwenRealtimeTts::from_config(&voice_live::config::TtsConfig::default())
        .expect("tts builds");

    let request = TtsRequest {
        speech_id: SpeechId::new(),
        speech_epoch: 1,
        text: "토요일 저녁 일곱 시로 확인해볼게요.".into(),
        language: "Korean".into(),
        voice: std::env::var("QWEN_VOICE_ID").unwrap_or_default(),
        claim_class: ClaimClass::Factual,
    };

    let mut stream = tts
        .synthesize(request, CancellationToken::new())
        .await
        .expect("tts session opens");

    let start = std::time::Instant::now();
    let mut bytes = 0usize;
    let mut saw_done = false;

    while start.elapsed() < std::time::Duration::from_secs(20) {
        match stream.next_event().await.expect("tts event") {
            Some(TtsEvent::Audio { pcm_s16le, .. }) => bytes += pcm_s16le.len(),
            Some(TtsEvent::AudioDone { .. }) => {
                saw_done = true;
                break;
            }
            Some(TtsEvent::Error { message, .. }) => panic!("tts provider error: {message}"),
            None => break,
        }
    }

    assert!(bytes > 0, "expected audio bytes");
    assert!(saw_done, "expected audio done event");
}
