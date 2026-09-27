//! axum WebSocket gateway.
//!
//! This is the transport adapter, and it lives in the demo on purpose. The
//! runtime exposes a pair of channels (`ServerMessage` out, `ClientInput` in)
//! and knows nothing about HTTP; swapping this file for a WebRTC bridge would
//! not touch the library.

use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use voice_live::media::decode_client_audio;
use voice_live::{ClientControlMessage, ClientInput, ServerMessage, VoiceRuntime};

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<VoiceRuntime>,
    /// Directory served at `/`. Resolved by the caller (normally from the
    /// config file's location) so the demo does not depend on the process
    /// working directory.
    pub web_dir: PathBuf,
}

impl AppState {
    pub fn new(runtime: Arc<VoiceRuntime>, web_dir: impl Into<PathBuf>) -> Self {
        Self {
            runtime,
            web_dir: web_dir.into(),
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    let web_dir = state.web_dir.clone();

    Router::new()
        .route("/api/voice/ws", get(voice_ws_handler))
        .fallback_service(tower_http::services::ServeDir::new(web_dir))
        .with_state(state)
}

async fn voice_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(error) = serve_voice_socket(socket, state.runtime).await {
            tracing::warn!(?error, "voice socket ended");
        }
    })
}

async fn serve_voice_socket(socket: WebSocket, runtime: Arc<VoiceRuntime>) -> anyhow::Result<()> {
    let (mut ws_sink, mut ws_stream) = socket.split();

    let (client_tx, mut client_rx) = mpsc::channel::<ServerMessage>(256);
    let client_tx_for_errors = client_tx.clone();

    let writer = tokio::spawn(async move {
        while let Some(message) = client_rx.recv().await {
            let ws_message = match message {
                ServerMessage::Json(value) => {
                    Message::Text(serde_json::to_string(&value).unwrap_or_default().into())
                }
                ServerMessage::Binary(bytes) => Message::Binary(bytes),
            };

            if ws_sink.send(ws_message).await.is_err() {
                break;
            }
        }
    });

    let session = runtime.create_session(client_tx).await?;
    let input_tx = session.input_tx.clone();
    let session_id = session.session_id;

    let mut result: anyhow::Result<()> = Ok(());

    while let Some(message) = ws_stream.next().await {
        match message {
            Ok(Message::Text(text)) => {
                if let Ok(control) = serde_json::from_str::<ClientControlMessage>(&text) {
                    if let ClientControlMessage::InjectUserText { text } = &control {
                        if !runtime.inject_user_text(session_id, text) {
                            tracing::warn!("text injection rejected");
                            let _ = client_tx_for_errors
                                .send(ServerMessage::Json(serde_json::json!({
                                    "type": "injection_rejected",
                                    "reason": "dev.allow_text_injection is disabled",
                                })))
                                .await;
                        }
                        continue;
                    }

                    if input_tx.send(ClientInput::Control(control)).await.is_err() {
                        break;
                    }
                }
            }
            Ok(Message::Binary(bytes)) => match decode_client_audio(&bytes) {
                Ok(frame) => {
                    if input_tx.send(ClientInput::Audio(frame)).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, "invalid audio frame");
                }
            },
            Ok(Message::Ping(_)) => {}
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(error) => {
                result = Err(anyhow::anyhow!("socket error: {error}"));
                break;
            }
        }
    }

    runtime.close_session(session_id).await;
    writer.abort();

    result
}
