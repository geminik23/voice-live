//! Drives `VoiceRuntime` through its channels with no transport attached.
//! This is the seam the gateway sits on, so it must hold without axum.

use std::time::Duration;

use voice_live::{
    ClientControlMessage, ClientInput, ServerMessage, VoiceRuntime, VoiceRuntimeConfig,
};

const CONFIG: &str = r#"
version: 1

asr:
  provider: inject
tts:
  provider: mock

turn_control:
  soft_silence_ms: 300
  hard_silence_ms: 700

observability:
  event_log: false

dev:
  allow_text_injection: true
"#;

/// The runtime is driven through its gateway session channels directly, with
/// no transport attached. This is the seam the axum gateway sits on.
#[tokio::test]
async fn runtime_pipeline_without_gateway() {
    let config = VoiceRuntimeConfig::from_yaml_str(CONFIG).expect("config parses");
    let runtime = VoiceRuntime::build(config).await.expect("runtime builds");

    let (client_tx, mut client_rx) = tokio::sync::mpsc::channel::<ServerMessage>(256);
    let session = runtime.create_session(client_tx).await.expect("session");

    session
        .input_tx
        .send(ClientInput::Control(ClientControlMessage::SessionStart {
            input_sample_rate: 48000,
            channels: 1,
        }))
        .await
        .expect("session_start delivers");

    runtime.inject_user_text(session.session_id, "토요일 저녁 7시로 예약해줘");

    let mut saw_speech_started = false;
    let mut audio_frames = 0usize;
    let mut saw_speech_done = false;

    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(message) = client_rx.recv().await {
            match message {
                ServerMessage::Json(value) => {
                    let kind = value.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    if kind == "speech_started" {
                        saw_speech_started = true;
                    }
                    if kind == "speech_done" {
                        saw_speech_done = true;
                        break;
                    }
                }
                ServerMessage::Binary(_) => audio_frames += 1,
            }
        }
    })
    .await
    .expect("pipeline must finish within the timeout");

    assert!(saw_speech_started, "client must receive speech_started");
    assert!(audio_frames > 0, "client must receive audio chunks");
    assert!(saw_speech_done, "client must receive speech_done");
}
