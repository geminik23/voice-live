use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_util::sync::CancellationToken;

use super::{
    AsrAudioInput, AsrAudioPush, AsrDuplexLimits, AsrDuplexSession, AsrEvent, AsrEventStream,
    AsrInputError, AsrOpenError, AsrSession, AsrSessionConfig, LegacyDuplexFacade, StreamingAsr,
};
use crate::config::AsrConfig;
use crate::ids::Revision;
use crate::provider::OwnedProviderTasks;

/// Together transcription adapter using the documented realtime JSON/base64 protocol.
/// Live model, locale, and account compatibility still require the ignored paid contract test.
pub struct TogetherNemotronAsr {
    config: AsrConfig,
    api_key: String,
}

impl TogetherNemotronAsr {
    pub fn from_config(config: &AsrConfig) -> anyhow::Result<Self> {
        let api_key = std::env::var(&config.api_key_env)
            .map_err(|_| anyhow::anyhow!("missing env {}", config.api_key_env))?;
        Ok(Self {
            config: config.clone(),
            api_key,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }
}

enum Command {
    Audio(Vec<i16>, OwnedSemaphorePermit),
    Commit,
    Finish,
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[async_trait]
impl StreamingAsr for TogetherNemotronAsr {
    async fn open(&self, config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>> {
        let session = self
            .open_duplex(
                config,
                AsrDuplexLimits {
                    sample_budget: 16_000,
                    command_budget: 64,
                    write_timeout: Duration::from_millis(self.config.write_timeout_ms),
                },
                CancellationToken::new(),
            )
            .await?;
        Ok(Box::new(LegacyDuplexFacade::new(session)))
    }

    fn supports_duplex(&self) -> bool {
        true
    }

    async fn open_duplex(
        &self,
        config: AsrSessionConfig,
        limits: AsrDuplexLimits,
        cancellation: CancellationToken,
    ) -> Result<AsrDuplexSession, AsrOpenError> {
        if config.sample_rate_hz != 16_000
            || limits.sample_budget == 0
            || limits.sample_budget > u32::MAX as usize
            || limits.command_budget == 0
            || limits.write_timeout.is_zero()
        {
            return Err(AsrOpenError::Rejected(
                "Together requires 16 kHz mono PCM and positive bounded limits".into(),
            ));
        }
        let token = cancellation.child_token();
        let connect = async {
            let separator = if self.config.endpoint.contains('?') {
                '&'
            } else {
                '?'
            };
            let url = format!(
                "{}{separator}model={}&input_audio_format=pcm_s16le_16000&language={}&turn_detection={}",
                self.config.endpoint,
                percent_encode(&self.config.model),
                percent_encode(&config.locale),
                if config.manual_commit {
                    "none"
                } else {
                    "server_vad"
                }
            );
            let mut request = url
                .into_client_request()
                .map_err(|error| AsrOpenError::Rejected(error.to_string()))?;
            request.headers_mut().insert(
                "Authorization",
                format!("Bearer {}", self.api_key)
                    .parse()
                    .map_err(|_| AsrOpenError::Rejected("invalid authorization header".into()))?,
            );
            request
                .headers_mut()
                .insert("OpenAI-Beta", "realtime=v1".parse().expect("static header"));
            let mut ws_config =
                tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
            ws_config.max_message_size = Some(262_144);
            ws_config.max_frame_size = Some(262_144);
            let (mut socket, _) =
                tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false)
                    .await
                    .map_err(classify_connect_error)?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let value: serde_json::Value = serde_json::from_str(&text)
                            .map_err(|error| AsrOpenError::Rejected(error.to_string()))?;
                        match value["type"].as_str() {
                            Some("session.created") => return Ok(socket),
                            Some(
                                "error" | "conversation.item.input_audio_transcription.failed",
                            ) => return Err(AsrOpenError::Rejected(value.to_string())),
                            _ => {
                                return Err(AsrOpenError::Rejected(
                                    "unexpected event before session readiness".into(),
                                ));
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        socket
                            .send(Message::Pong(data))
                            .await
                            .map_err(classify_connect_error)?;
                    }
                    _ => {
                        return Err(AsrOpenError::Transient(
                            "socket closed before readiness".into(),
                        ));
                    }
                }
            }
        };
        let socket = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(AsrOpenError::Rejected("open cancelled".into())),
            result = tokio::time::timeout(Duration::from_millis(self.config.open_timeout_ms), connect) => result.map_err(|_| AsrOpenError::TimedOut("ASR readiness timed out".into()))??,
        };
        let (sink, stream) = socket.split();
        let (commands, command_rx) = mpsc::channel(limits.command_budget);
        let (events, event_rx) = mpsc::channel(64);
        let (pongs, pong_rx) = mpsc::channel(1);
        let writer = tokio::spawn(writer(
            sink,
            command_rx,
            pong_rx,
            events.clone(),
            token.clone(),
            limits.write_timeout,
        ));
        let reader = tokio::spawn(reader(stream, events, pongs, token.clone()));
        Ok(AsrDuplexSession {
            input: Box::new(Input {
                commands,
                samples: Arc::new(Semaphore::new(limits.sample_budget)),
                budget: limits.sample_budget,
                finished: false,
                token: token.clone(),
            }),
            events: Box::new(Events {
                events: event_rx,
                token: token.clone(),
            }),
            control: Box::new(OwnedProviderTasks {
                token,
                tasks: vec![writer, reader],
            }),
        })
    }
}

fn classify_connect_error(error: tokio_tungstenite::tungstenite::Error) -> AsrOpenError {
    if let tokio_tungstenite::tungstenite::Error::Http(response) = &error {
        return match response.status().as_u16() {
            400..=499 if response.status().as_u16() != 429 => {
                AsrOpenError::Rejected(format!("asr handshake rejected: {}", response.status()))
            }
            _ => AsrOpenError::Transient(format!("asr handshake failed: {}", response.status())),
        };
    }
    AsrOpenError::Transient(error.to_string())
}

fn audio_message(pcm: &[i16]) -> Message {
    let bytes: Vec<u8> = pcm.iter().flat_map(|sample| sample.to_le_bytes()).collect();
    Message::Text(serde_json::json!({"type": "input_audio_buffer.append", "audio": base64::engine::general_purpose::STANDARD.encode(bytes)}).to_string().into())
}

async fn writer(
    mut sink: futures::stream::SplitSink<Socket, Message>,
    mut commands: mpsc::Receiver<Command>,
    mut pongs: mpsc::Receiver<bytes::Bytes>,
    events: mpsc::Sender<AsrEvent>,
    token: CancellationToken,
    timeout: Duration,
) {
    let mut input_finished = false;
    loop {
        let (message, permit, finished) = tokio::select! {
            biased;
            _ = token.cancelled() => return,
            pong = pongs.recv(), if !pongs.is_closed() => match pong {
                Some(pong) => (Message::Pong(pong), None, false),
                None => continue,
            },
            command = commands.recv(), if !input_finished => match command {
                Some(Command::Audio(pcm, permit)) => (audio_message(&pcm), Some(permit), false),
                Some(Command::Commit) => (Message::Text(r#"{"type":"input_audio_buffer.commit"}"#.into()), None, false),
                Some(Command::Finish) => (Message::Text(r#"{"type":"input_audio_buffer.commit"}"#.into()), None, true),
                None => return,
            },
        };
        let result = tokio::select! {
            biased;
            _ = token.cancelled() => return,
            result = tokio::time::timeout(timeout, sink.send(message)) => result,
        };
        drop(permit);
        if !matches!(result, Ok(Ok(()))) {
            let _ = events.try_send(AsrEvent::Error {
                recoverable: true,
                message: "ASR write failed or timed out".into(),
            });
            token.cancel();
            return;
        }
        if finished {
            input_finished = true;
        }
    }
}

async fn reader(
    mut stream: futures::stream::SplitStream<Socket>,
    events: mpsc::Sender<AsrEvent>,
    pongs: mpsc::Sender<bytes::Bytes>,
    token: CancellationToken,
) {
    let mut state = ParseState::default();
    loop {
        let message = tokio::select! {
            biased;
            _ = token.cancelled() => return,
            message = stream.next() => message,
        };
        let event = match message {
            Some(Ok(Message::Text(text))) => parse_event(&text, &mut state),
            Some(Ok(Message::Ping(data))) => {
                if pongs.try_send(data).is_err() {
                    token.cancel();
                    return;
                }
                None
            }
            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => Some(AsrEvent::Error {
                recoverable: true,
                message: "ASR socket closed".into(),
            }),
            _ => None,
        };
        if let Some(event) = event {
            let terminal = matches!(event, AsrEvent::Error { .. });
            tokio::select! {
                biased;
                _ = token.cancelled() => return,
                sent = events.send(event) => if sent.is_err() { token.cancel(); return; },
            }
            if terminal {
                token.cancel();
                return;
            }
        }
    }
}

struct Input {
    commands: mpsc::Sender<Command>,
    samples: Arc<Semaphore>,
    budget: usize,
    finished: bool,
    token: CancellationToken,
}

impl Drop for Input {
    fn drop(&mut self) {
        if !self.finished {
            self.token.cancel();
        }
    }
}

#[async_trait]
impl AsrAudioInput for Input {
    fn try_push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        if self.finished || self.token.is_cancelled() {
            return Err(AsrInputError::Closed);
        }
        if pcm.len() > self.budget {
            return Ok(AsrAudioPush::Full(pcm));
        }
        let permit = match self
            .samples
            .clone()
            .try_acquire_many_owned(pcm.len() as u32)
        {
            Ok(permit) => permit,
            Err(_) => return Ok(AsrAudioPush::Full(pcm)),
        };
        match self.commands.try_send(Command::Audio(pcm, permit)) {
            Ok(()) => Ok(AsrAudioPush::Accepted),
            Err(mpsc::error::TrySendError::Full(Command::Audio(pcm, _))) => {
                Ok(AsrAudioPush::Full(pcm))
            }
            Err(_) => Err(AsrInputError::Closed),
        }
    }

    async fn push_audio(&mut self, pcm: Vec<i16>) -> Result<AsrAudioPush, AsrInputError> {
        if self.finished || self.token.is_cancelled() {
            return Err(AsrInputError::Closed);
        }
        if pcm.len() > self.budget {
            return Ok(AsrAudioPush::Full(pcm));
        }
        let permit = tokio::select! {
            biased;
            _ = self.token.cancelled() => return Err(AsrInputError::Cancelled),
            permit = self.samples.clone().acquire_many_owned(pcm.len() as u32) => permit.map_err(|_| AsrInputError::Closed)?,
        };
        tokio::select! {
            biased;
            _ = self.token.cancelled() => Err(AsrInputError::Cancelled),
            sent = self.commands.send(Command::Audio(pcm, permit)) => sent.map(|()| AsrAudioPush::Accepted).map_err(|_| AsrInputError::Closed),
        }
    }

    fn try_commit_utterance(&mut self) -> Result<(), AsrInputError> {
        if self.finished || self.token.is_cancelled() {
            return Err(AsrInputError::Closed);
        }
        self.commands
            .try_send(Command::Commit)
            .map_err(admission_error)
    }

    async fn commit_utterance(&mut self) -> Result<(), AsrInputError> {
        if self.finished || self.token.is_cancelled() {
            return Err(AsrInputError::Closed);
        }
        tokio::select! {
            biased;
            _ = self.token.cancelled() => Err(AsrInputError::Cancelled),
            sent = self.commands.send(Command::Commit) => sent.map_err(|_| AsrInputError::Closed),
        }
    }

    fn finish_input(&mut self) -> Result<(), AsrInputError> {
        if self.finished {
            return Ok(());
        }
        if self.token.is_cancelled() {
            return Err(AsrInputError::Closed);
        }
        self.commands
            .try_send(Command::Finish)
            .map_err(admission_error)?;
        self.finished = true;
        Ok(())
    }
}

fn admission_error<T>(error: mpsc::error::TrySendError<T>) -> AsrInputError {
    match error {
        mpsc::error::TrySendError::Full(_) => AsrInputError::Full,
        mpsc::error::TrySendError::Closed(_) => AsrInputError::Closed,
    }
}

struct Events {
    events: mpsc::Receiver<AsrEvent>,
    token: CancellationToken,
}
impl Drop for Events {
    fn drop(&mut self) {
        self.token.cancel();
    }
}
#[async_trait]
impl AsrEventStream for Events {
    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        tokio::select! {
            biased;
            event = self.events.recv() => Ok(event),
            _ = self.token.cancelled() => Ok(self.events.try_recv().ok()),
        }
    }
}

#[derive(Default)]
struct ParseState {
    utterance_id: u64,
    revision: u64,
}
fn parse_event(text: &str, state: &mut ParseState) -> Option<AsrEvent> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    match value["type"].as_str()? {
        "conversation.item.input_audio_transcription.delta" => {
            state.revision += 1;
            Some(AsrEvent::Partial {
                utterance_id: state.utterance_id,
                revision: Revision(state.revision),
                text: value["delta"].as_str()?.into(),
            })
        }
        "conversation.item.input_audio_transcription.completed" => {
            let utterance_id = state.utterance_id;
            state.utterance_id += 1;
            Some(AsrEvent::Final {
                utterance_id,
                text: value["transcript"].as_str()?.into(),
            })
        }
        "error" | "conversation.item.input_audio_transcription.failed" => Some(AsrEvent::Error {
            recoverable: false,
            message: value["error"]["message"]
                .as_str()
                .or_else(|| value["message"].as_str())
                .unwrap_or("unclassified ASR provider rejection")
                .into(),
        }),
        _ => None,
    }
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
                (byte as char).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn documented_delta_is_a_snapshot_with_session_monotonic_revisions() {
        let mut state = ParseState::default();
        let first = parse_event(
            r#"{"type":"conversation.item.input_audio_transcription.delta","delta":"토"}"#,
            &mut state,
        )
        .unwrap();
        let second = parse_event(
            r#"{"type":"conversation.item.input_audio_transcription.delta","delta":"토요일"}"#,
            &mut state,
        )
        .unwrap();
        let (
            AsrEvent::Partial { revision: a, .. },
            AsrEvent::Partial {
                revision: b, text, ..
            },
        ) = (first, second)
        else {
            panic!("partials expected")
        };
        assert!(b > a);
        assert_eq!(text, "토요일");
    }
    #[test]
    fn final_closes_the_partials_utterance() {
        let mut state = ParseState::default();
        let final_event = parse_event(r#"{"type":"conversation.item.input_audio_transcription.completed","transcript":"토요일"}"#, &mut state).unwrap();
        let AsrEvent::Final { utterance_id, .. } = final_event else {
            panic!("final expected")
        };
        assert_eq!(utterance_id, 0);
        assert_eq!(state.utterance_id, 1);
    }
    #[test]
    fn audio_is_json_base64_pcm_and_queries_are_encoded() {
        let Message::Text(text) = audio_message(&[1, -1]) else {
            panic!("JSON expected")
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["type"], "input_audio_buffer.append");
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(value["audio"].as_str().unwrap())
                .unwrap(),
            vec![1, 0, 255, 255]
        );
        assert_eq!(
            percent_encode("nvidia/model&other"),
            "nvidia%2Fmodel%26other"
        );
    }
    #[tokio::test]
    async fn open_waits_for_documented_ready_and_uses_json_pcm() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            connected_tx.send(()).unwrap();
            ready_rx.await.unwrap();
            socket
                .send(Message::Text(r#"{"type":"session.created"}"#.into()))
                .await
                .unwrap();
            let Message::Text(audio) = socket.next().await.unwrap().unwrap() else {
                panic!("JSON audio expected")
            };
            let value: serde_json::Value = serde_json::from_str(&audio).unwrap();
            assert_eq!(value["type"], "input_audio_buffer.append");
            socket.send(Message::Text(r#"{"type":"conversation.item.input_audio_transcription.delta","delta":"토요일"}"#.into())).await.unwrap();
            let Message::Text(commit) = socket.next().await.unwrap().unwrap() else {
                panic!("commit expected")
            };
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&commit).unwrap()["type"],
                "input_audio_buffer.commit"
            );
            socket
                .send(Message::Ping(bytes::Bytes::from_static(b"ping")))
                .await
                .unwrap();
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Pong(_)
            ));
            socket.send(Message::Text(r#"{"type":"conversation.item.input_audio_transcription.completed","transcript":"토요일"}"#.into())).await.unwrap();
        });
        let config = AsrConfig {
            endpoint: format!("ws://{address}/realtime"),
            ..AsrConfig::default()
        };
        let provider = TogetherNemotronAsr {
            config,
            api_key: "test-key".into(),
        };
        let open = tokio::spawn(async move {
            provider
                .open_duplex(
                    AsrSessionConfig {
                        sample_rate_hz: 16_000,
                        locale: "ko-KR".into(),
                        manual_commit: true,
                    },
                    AsrDuplexLimits {
                        sample_budget: 320,
                        command_budget: 64,
                        write_timeout: Duration::from_secs(1),
                    },
                    CancellationToken::new(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), connected_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !open.is_finished(),
            "transport upgrade alone is not readiness"
        );
        ready_tx.send(()).unwrap();
        let mut session = tokio::time::timeout(Duration::from_secs(2), open)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        session.input.push_audio(vec![1, -1]).await.unwrap();
        session.input.finish_input().unwrap();
        drop(session.input);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), session.events.next_event())
                .await
                .unwrap()
                .unwrap(),
            Some(AsrEvent::Partial { .. })
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), session.events.next_event())
                .await
                .unwrap()
                .unwrap(),
            Some(AsrEvent::Final { .. })
        ));
        session.control.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn unsupported_rate_is_rejected_before_network_open() {
        let provider = TogetherNemotronAsr {
            config: AsrConfig::default(),
            api_key: "test".into(),
        };
        let result = provider
            .open_duplex(
                AsrSessionConfig {
                    sample_rate_hz: 48_000,
                    locale: "ko-KR".into(),
                    manual_commit: false,
                },
                AsrDuplexLimits {
                    sample_budget: 320,
                    command_budget: 64,
                    write_timeout: Duration::from_secs(1),
                },
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(result, Err(AsrOpenError::Rejected(_))));
    }

    #[test]
    fn authentication_is_rejected_without_string_matching() {
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(401)
            .body(None)
            .unwrap();
        assert!(matches!(
            classify_connect_error(tokio_tungstenite::tungstenite::Error::Http(response)),
            AsrOpenError::Rejected(_)
        ));
    }
}
