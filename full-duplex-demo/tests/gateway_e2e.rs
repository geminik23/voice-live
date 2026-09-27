//! End-to-end gateway test: a real in-process axum server and a real
//! WebSocket client. This lives with the gateway it exercises.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use voice_live::{VoiceRuntime, VoiceRuntimeConfig};

#[path = "../src/gateway.rs"]
mod gateway;
use gateway::{AppState, build_router};

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

#[tokio::test]
async fn gateway_full_pipeline_over_websocket() {
    let config = VoiceRuntimeConfig::from_yaml_str(CONFIG).expect("config parses");
    let runtime = VoiceRuntime::build(config).await.expect("runtime builds");

    let app = build_router(AppState::new(
        Arc::clone(&runtime),
        std::env::temp_dir().join("voice-gateway-e2e-web"),
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server runs");
    });

    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/api/voice/ws"))
        .await
        .expect("websocket connects");

    let (mut sink, mut stream) = socket.split();

    let session_start = serde_json::json!({
        "type": "session_start",
        "input_sample_rate": 48000,
        "channels": 1,
    });
    sink.send(Message::Text(session_start.to_string().into()))
        .await
        .expect("session_start sends");

    let inject = serde_json::json!({
        "type": "inject_user_text",
        "text": "토요일 저녁 7시로 예약해줘",
    });
    sink.send(Message::Text(inject.to_string().into()))
        .await
        .expect("injection sends");

    let mut saw_speech_started = false;
    let mut audio_frames = 0usize;
    let mut saw_speech_done = false;

    let deadline = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(message) = stream.next().await {
            match message.expect("message") {
                Message::Text(text) => {
                    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                    match value.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                        "speech_started" => saw_speech_started = true,
                        "speech_done" => {
                            saw_speech_done = true;
                            break;
                        }
                        _ => {}
                    }
                }
                Message::Binary(_) => audio_frames += 1,
                _ => {}
            }
        }
    })
    .await;

    assert!(deadline.is_ok(), "pipeline must finish within the timeout");
    assert!(saw_speech_started, "client must receive speech_started");
    assert!(audio_frames > 0, "client must receive audio chunks");
    assert!(saw_speech_done, "client must receive speech_done");

    server.abort();

    let summaries = runtime.metrics().snapshot();
    assert!(
        summaries
            .counters
            .get("voice_user_turn_committed_total")
            .copied()
            .unwrap_or(0)
            >= 1,
        "runtime must have committed the injected turn: {:?}",
        summaries.counters
    );
}
