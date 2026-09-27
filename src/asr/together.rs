use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::{AsrEvent, AsrSession, AsrSessionConfig, StreamingAsr};
use crate::config::AsrConfig;
use crate::ids::Revision;

/// Together realtime streaming ASR adapter.
///
/// The wire shape follows the documented realtime event model (session
/// create, binary PCM input, manual commit, partial and final transcript
/// events). Exact field names must be validated with the ignored provider
/// contract test before paid use; the trait boundary keeps this adapter
/// swappable without touching the runtime.
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

enum WorkerCommand {
    Push(Vec<u8>),
    Commit,
    Close,
}

struct TogetherAsrSession {
    command_tx: mpsc::Sender<WorkerCommand>,
    event_rx: mpsc::Receiver<AsrEvent>,
}

#[async_trait]
impl StreamingAsr for TogetherNemotronAsr {
    async fn open(&self, session_config: AsrSessionConfig) -> anyhow::Result<Box<dyn AsrSession>> {
        let (command_tx, command_rx) = mpsc::channel::<WorkerCommand>(64);
        let (event_tx, event_rx) = mpsc::channel::<AsrEvent>(64);

        tokio::spawn(run_socket(
            self.config.clone(),
            self.api_key.clone(),
            session_config,
            command_rx,
            event_tx,
        ));

        Ok(Box::new(TogetherAsrSession {
            command_tx,
            event_rx,
        }))
    }
}

#[async_trait]
impl AsrSession for TogetherAsrSession {
    async fn push_audio(&mut self, pcm: &[i16]) -> anyhow::Result<()> {
        let mut bytes = Vec::with_capacity(pcm.len() * 2);
        for sample in pcm {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        self.command_tx
            .send(WorkerCommand::Push(bytes))
            .await
            .map_err(|_| anyhow::anyhow!("asr worker closed"))
    }

    async fn commit_audio(&mut self) -> anyhow::Result<()> {
        self.command_tx
            .send(WorkerCommand::Commit)
            .await
            .map_err(|_| anyhow::anyhow!("asr worker closed"))
    }

    async fn next_event(&mut self) -> anyhow::Result<Option<AsrEvent>> {
        match self.event_rx.recv().await {
            Some(event) => Ok(Some(event)),
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        let _ = self.command_tx.send(WorkerCommand::Close).await;
        Ok(())
    }
}

async fn run_socket(
    config: AsrConfig,
    api_key: String,
    session_config: AsrSessionConfig,
    mut command_rx: mpsc::Receiver<WorkerCommand>,
    event_tx: mpsc::Sender<AsrEvent>,
) {
    let url = format!(
        "{}?model={}",
        config.endpoint,
        urlencoding_lite(&config.model)
    );

    let request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("X-Model", &config.model)
        .body(())
        .map_err(|e| anyhow::anyhow!("asr request build failed: {e}"));

    let request = match request {
        Ok(request) => request,
        Err(_) => return,
    };

    let (socket, _) = match tokio_tungstenite::connect_async(request).await {
        Ok(value) => value,
        Err(error) => {
            let _ = event_tx
                .send(AsrEvent::Error {
                    recoverable: true,
                    message: error.to_string(),
                })
                .await;
            return;
        }
    };

    let (mut sink, mut stream) = socket.split();

    let session_create = serde_json::json!({
        "type": "session.create",
        "session": {
            "model": config.model,
            "language": session_config.locale,
            "input_audio_format": "pcm16",
            "sample_rate_hz": session_config.sample_rate_hz,
            "vad": if session_config.manual_commit { "none" } else { "server" },
            "partial_transcripts": true,
        }
    });

    if sink
        .send(Message::Text(session_create.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    // The provider is not guaranteed to supply a revision. `AsrEvent::Partial`
    // requires a session-monotonic one, so the adapter owns the counter.
    let mut state = ParseState::default();

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(WorkerCommand::Push(bytes)) => {
                        if sink.send(Message::Binary(bytes.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(WorkerCommand::Commit) => {
                        let commit = serde_json::json!({"type": "input_audio.commit"});
                        if sink.send(Message::Text(commit.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Some(WorkerCommand::Close) | None => {
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                }
            }

            message = stream.next() => {
                let Some(message) = message else { break };
                match message {
                    Ok(Message::Text(text)) => {
                        if let Some(event) = parse_event(&text, &mut state)
                            && event_tx.send(event).await.is_err() {
                                break;
                            }
                    }
                    Ok(Message::Binary(_)) => {}
                    Ok(Message::Ping(data)) => {
                        let _ = sink.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        }
    }

    let _ = event_tx
        .send(AsrEvent::Error {
            recoverable: true,
            message: "asr socket closed".into(),
        })
        .await;
}

/// Per-socket parsing state. Both counters are session scoped so a provider
/// that restarts revisions per utterance, or omits them entirely, still yields
/// the strictly increasing revision `AsrEvent::Partial` requires.
#[derive(Debug, Default)]
struct ParseState {
    utterance_id: u64,
    revision: u64,
}

impl ParseState {
    /// Monotonic across the session: a provider revision is only adopted when
    /// it moves the counter forward.
    fn next_revision(&mut self, provider_revision: Option<u64>) -> Revision {
        self.revision = match provider_revision {
            Some(revision) if revision > self.revision => revision,
            _ => self.revision + 1,
        };
        Revision(self.revision)
    }
}

fn parse_event(text: &str, state: &mut ParseState) -> Option<AsrEvent> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let kind = value.get("type")?.as_str()?.to_string();

    match kind.as_str() {
        "transcript.partial" | "partial" => {
            let text = value
                .get("text")
                .or_else(|| value.get("delta"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let revision = state.next_revision(value.get("revision").and_then(|v| v.as_u64()));
            Some(AsrEvent::Partial {
                utterance_id: state.utterance_id,
                revision,
                text,
            })
        }
        "transcript.final" | "final" | "utterance.final" => {
            let text = value
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            // The final closes the utterance it shares an id with; only then
            // does the next utterance begin.
            let utterance_id = state.utterance_id;
            state.utterance_id += 1;
            Some(AsrEvent::Final { utterance_id, text })
        }
        "error" => {
            let message = value
                .get("message")
                .or_else(|| value.get("error"))
                .and_then(|v| v.as_str())
                .unwrap_or("asr provider error")
                .to_string();
            Some(AsrEvent::Error {
                recoverable: true,
                message,
            })
        }
        _ => None,
    }
}

fn urlencoding_lite(value: &str) -> String {
    value.replace('/', "%2F")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revisions_are_session_monotonic_without_provider_support() {
        let mut state = ParseState::default();

        let first = parse_event(r#"{"type":"partial","text":"토"}"#, &mut state).unwrap();
        let second = parse_event(r#"{"type":"partial","text":"토요"}"#, &mut state).unwrap();

        let (AsrEvent::Partial { revision: a, .. }, AsrEvent::Partial { revision: b, .. }) =
            (first, second)
        else {
            panic!("expected partials");
        };

        assert!(b > a, "revision must advance: {b:?} <= {a:?}");
    }

    #[test]
    fn revision_never_regresses_when_the_provider_restarts_it() {
        let mut state = ParseState::default();

        parse_event(r#"{"type":"partial","revision":9,"text":"토"}"#, &mut state).unwrap();
        let next = parse_event(
            r#"{"type":"partial","revision":1,"text":"토요"}"#,
            &mut state,
        )
        .unwrap();

        let AsrEvent::Partial { revision, .. } = next else {
            panic!("expected a partial");
        };
        assert_eq!(revision, Revision(10));
    }

    #[test]
    fn a_final_shares_its_utterance_id_with_its_own_partials() {
        let mut state = ParseState::default();

        let partial = parse_event(r#"{"type":"partial","text":"토요일"}"#, &mut state).unwrap();
        let final_event =
            parse_event(r#"{"type":"final","text":"토요일 저녁"}"#, &mut state).unwrap();
        let next_partial = parse_event(r#"{"type":"partial","text":"7시"}"#, &mut state).unwrap();

        let AsrEvent::Partial {
            utterance_id: first,
            ..
        } = partial
        else {
            panic!("expected a partial");
        };
        let AsrEvent::Final {
            utterance_id: sealed,
            ..
        } = final_event
        else {
            panic!("expected a final");
        };
        let AsrEvent::Partial {
            utterance_id: next, ..
        } = next_partial
        else {
            panic!("expected a partial");
        };

        assert_eq!(
            first, sealed,
            "the final closes the utterance it belongs to"
        );
        assert_eq!(next, sealed + 1, "the next utterance gets a fresh id");
    }
}
