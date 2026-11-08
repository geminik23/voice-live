//! The smallest end-to-end use of voice-live, with no API keys and no transport.
//!
//! It builds a runtime with development providers (text injection instead of speech recognition, silent audio instead of speech synthesis, and a fixed task reply), opens one session, and plays the client's part over the session channels: it starts the session, says one line, and acknowledges playback the way a browser would.
//!
//! Run it with `cargo run --example quickstart`. A real host replaces the providers in the config, supplies its own tools through `RuntimePolicies`, and bridges the channels to a transport such as a WebSocket (see `examples/browser`).

use std::time::Duration;

use voice_live::{
    ClientControlMessage, ClientInput, ServerMessage, SpeechId, VoiceRuntime, VoiceRuntimeConfig,
};

const CONFIG: &str = r#"
version: 1
asr:
  provider: inject
tts:
  provider: mock
speech:
  mock_reply: "Tell me the date, time, and party size."
observability:
  event_log: false
dev:
  allow_text_injection: true
"#;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = VoiceRuntime::build(VoiceRuntimeConfig::from_yaml_str(CONFIG)?).await?;

    // The runtime writes to `client_tx`; the host reads `client_rx` and forwards it to its client.
    let (client_tx, mut client_rx) = tokio::sync::mpsc::channel::<ServerMessage>(256);
    let session = runtime.create_session(client_tx).await?;

    session
        .input_tx
        .send(ClientInput::Control(ClientControlMessage::SessionStart {
            input_sample_rate: 16_000,
            channels: 1,
        }))
        .await?;

    println!("user: I'd like to book a table.");
    runtime.inject_user_text(session.session_id, "I'd like to book a table.");

    let mut audio_bytes = 0;
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(message) = client_rx.recv().await {
            match message {
                ServerMessage::Binary(bytes) => audio_bytes += bytes.len(),
                ServerMessage::Json(value) => {
                    let kind = value["type"].as_str().unwrap_or_default().to_string();
                    println!("server: {kind}");
                    if kind == "speech_done" {
                        // A client reports playback so the runtime knows the reply was heard.
                        let speech_id: SpeechId =
                            serde_json::from_value(value["speech_id"].clone())?;
                        let speech_epoch = value["speech_epoch"].as_u64().unwrap_or_default();
                        session
                            .input_tx
                            .send(ClientInput::Control(
                                ClientControlMessage::PlaybackCompleted {
                                    speech_id,
                                    speech_epoch,
                                },
                            ))
                            .await?;
                        break;
                    }
                }
            }
        }
        anyhow::Ok(())
    })
    .await??;

    println!("received {audio_bytes} bytes of reply audio (silent with the mock provider)");
    runtime.close_session(session.session_id).await;
    Ok(())
}
