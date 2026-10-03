use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_util::sync::CancellationToken;

use super::{StreamingTts, TtsEvent, TtsRequest, TtsStream};
use crate::config::TtsConfig;

/// Qwen realtime TTS adapter over the DashScope realtime WebSocket.
///
/// The wire shape follows the documented realtime response lifecycle (session
/// create, response create with input text, streamed audio deltas in base64,
/// response done). Field names must be validated with the ignored provider
/// contract test before paid use.
pub struct QwenRealtimeTts {
    config: TtsConfig,
    api_key: String,
}

impl QwenRealtimeTts {
    pub fn from_config(config: &TtsConfig) -> anyhow::Result<Self> {
        let api_key = std::env::var(&config.api_key_env)
            .map_err(|_| anyhow::anyhow!("missing env {}", config.api_key_env))?;
        Ok(Self {
            config: config.clone(),
            api_key,
        })
    }
}

#[async_trait]
impl StreamingTts for QwenRealtimeTts {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        let setup = async {
            let mut http_request = self.config.endpoint.as_str().into_client_request()?;
            http_request
                .headers_mut()
                .insert("Authorization", format!("Bearer {}", self.api_key).parse()?);
            http_request
                .headers_mut()
                .insert("X-DashScope-DataInspection", "false".parse()?);

            let (socket, _) = tokio::select! {
                _ = cancellation.cancelled() => {
                    anyhow::bail!("tts connect cancelled");
                }
                connected = tokio_tungstenite::connect_async_with_config(http_request, Some(socket_config()), false) => {
                    connected.map_err(|e| anyhow::anyhow!("tts connect failed: {e}"))?
                }
            };

            // The request carries the resolved voice (config value, else the
            // `tts.voice_env` environment variable); the config field alone is
            // usually blank because voice ids are account scoped.
            let voice = if request.voice.trim().is_empty() {
                self.config.voice.clone()
            } else {
                request.voice.clone()
            };

            let (mut sink, mut stream) = socket.split();

            let session_create = serde_json::json!({
                "type": "session.create",
                "session": {
                    "model": self.config.model,
                    "voice": voice,
                    "output_audio_format": "pcm16",
                    "sample_rate_hz": self.config.sample_rate_hz,
                }
            });

            sink.send(Message::Text(session_create.to_string().into()))
                .await
                .map_err(|e| anyhow::anyhow!("tts session create failed: {e}"))?;

            // Drain until session.created so the first response starts on a warm
            // session. Cancellation is checked from setup onward; errors surface
            // as a synthesis failure rather than hanging the open.
            let mut ready = false;
            while let Some(message) = tokio::select! {
                _ = cancellation.cancelled() => {
                    anyhow::bail!("tts setup cancelled");
                }
                message = stream.next() => message,
            } {
                match message {
                    Ok(Message::Text(text)) => {
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            if value.get("type").and_then(|t| t.as_str()) == Some("session.created")
                            {
                                ready = true;
                                break;
                            }
                            if value.get("type").and_then(|t| t.as_str()) == Some("error") {
                                let message = value
                                    .get("message")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("tts session error");
                                anyhow::bail!("tts session error: {message}");
                            }
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => anyhow::bail!("tts session closed early"),
                    _ => {}
                }
            }

            anyhow::ensure!(ready, "tts session ended before readiness");
            let response_create = serde_json::json!({
                "type": "response.create",
                "response": {
                    "input_text": request.text,
                    "language": request.language,
                    "voice": voice,
                }
            });

            sink.send(Message::Text(response_create.to_string().into()))
                .await
                .map_err(|e| anyhow::anyhow!("tts response create failed: {e}"))?;

            Ok(Box::new(QwenTtsStream {
                sink,
                stream,
                response_id: request.speech_id.to_string(),
                sample_rate: self.config.sample_rate_hz,
                sequence: 0,
                finished: false,
                cancellation: cancellation.clone(),
            }) as Box<dyn TtsStream>)
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => anyhow::bail!("tts setup cancelled"),
            result = tokio::time::timeout(Duration::from_millis(self.config.open_timeout_ms), setup) => result.map_err(|_| anyhow::anyhow!("tts setup timed out"))?,
        }
    }
}

fn socket_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    config.max_message_size = Some(262_144);
    config.max_frame_size = Some(262_144);
    config
}

pub struct QwenTtsStream {
    sink: futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    stream: futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    response_id: String,
    sample_rate: u32,
    sequence: u32,
    finished: bool,
    cancellation: CancellationToken,
}

#[async_trait]
impl TtsStream for QwenTtsStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    // The documented realtime client events expose no direct
                    // response.cancel, so cancellation drops the socket; the
                    // speech epoch drop keeps audible output correct anyway.
                    self.finished = true;
                    return Ok(None);
                }

                message = self.stream.next() => {
                    let Some(message) = message else {
                        self.finished = true;
                        return Ok(Some(TtsEvent::Error {
                            recoverable: true,
                            message: "tts socket closed".into(),
                        }));
                    };

                    match message {
                        Ok(Message::Text(text)) => {
                            let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
                            else {
                                continue;
                            };

                            let kind = value.get("type").and_then(|t| t.as_str()).unwrap_or("");

                            match kind {
                                "response.audio.delta" | "audio.delta" | "tts_audio.delta" => {
                                    let delta = value
                                        .get("delta")
                                        .or_else(|| value.get("audio"))
                                        .and_then(|d| d.as_str())
                                        .unwrap_or_default();
                                    if delta.is_empty() {
                                        continue;
                                    }

                                    anyhow::ensure!(delta.len() <= 87_384, "encoded tts audio exceeds byte budget");
                                    let pcm = base64::engine::general_purpose::STANDARD
                                        .decode(delta)
                                        .map_err(|e| anyhow::anyhow!("tts audio decode failed: {e}"))?;

                                    self.sequence += 1;
                                    return Ok(Some(TtsEvent::Audio {
                                        response_id: self.response_id.clone(),
                                        sequence: self.sequence,
                                        sample_rate: self.sample_rate,
                                        pcm_s16le: bytes::Bytes::from(pcm),
                                    }));
                                }

                                "response.done" | "response.completed" | "tts_audio.done" => {
                                    self.finished = true;
                                    // Return the terminal before cleanup; dropping the response closes its socket.
                                    return Ok(Some(TtsEvent::AudioDone {
                                        response_id: self.response_id.clone(),
                                    }));
                                }

                                "error" => {
                                    self.finished = true;
                                    let message = value
                                        .get("message")
                                        .and_then(|m| m.as_str())
                                        .unwrap_or("tts provider error");
                                    return Ok(Some(TtsEvent::Error {
                                        recoverable: true,
                                        message: message.to_string(),
                                    }));
                                }

                                _ => continue,
                            }
                        }

                        Ok(Message::Binary(bytes)) => {
                            self.sequence += 1;
                            return Ok(Some(TtsEvent::Audio {
                                response_id: self.response_id.clone(),
                                sequence: self.sequence,
                                sample_rate: self.sample_rate,
                                pcm_s16le: bytes,
                            }));
                        }

                        Ok(Message::Ping(data)) => {
                            tokio::select! {
                                _ = self.cancellation.cancelled() => { self.finished = true; return Ok(None); }
                                result = tokio::time::timeout(Duration::from_millis(1_000), self.sink.send(Message::Pong(data))) => {
                                    result.map_err(|_| anyhow::anyhow!("tts pong timed out"))??;
                                }
                            }
                        }

                        Ok(Message::Close(_)) | Err(_) => {
                            self.finished = true;
                            return Ok(Some(TtsEvent::Error {
                                recoverable: true,
                                message: "tts socket closed".into(),
                            }));
                        }

                        Ok(_) => {}
                    }
                }
            }
        }
    }
}
