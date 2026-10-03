use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{
    AudioNormalizer, StreamingTts, TextInputMode, TtsDuplexSession, TtsEvent, TtsInputError,
    TtsInputLimits, TtsOpenError, TtsRequest, TtsSessionOptions, TtsStream, TtsTextInput,
};
use crate::provider::OwnedProviderTasks;

/// Bridges a whole-text provider without claiming native incremental synthesis.
pub struct BufferedTtsAdapter {
    inner: Arc<dyn StreamingTts>,
}

impl BufferedTtsAdapter {
    pub fn new(inner: Arc<dyn StreamingTts>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl StreamingTts for BufferedTtsAdapter {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        self.inner.synthesize(request, cancellation).await
    }

    fn text_input_mode(&self) -> TextInputMode {
        TextInputMode::Buffered
    }

    fn supports_text_stream(&self) -> bool {
        true
    }

    async fn open_text_stream(
        &self,
        options: TtsSessionOptions,
        limits: TtsInputLimits,
        cancellation: CancellationToken,
    ) -> Result<TtsDuplexSession, TtsOpenError> {
        if cancellation.is_cancelled()
            || limits.max_input_bytes == 0
            || limits.request_timeout.is_zero()
        {
            return Err(TtsOpenError::Rejected(
                "cancelled or invalid input limits".into(),
            ));
        }
        let token = cancellation.child_token();
        let (input_tx, input_rx) = oneshot::channel();
        let (audio_tx, audio_rx) = mpsc::channel(16);
        let (terminal_tx, terminal_rx) = oneshot::channel();
        let inner = Arc::clone(&self.inner);
        let worker_token = token.clone();
        let worker = tokio::spawn(async move {
            let admitted = tokio::select! {
                biased;
                _ = worker_token.cancelled() => return,
                admitted = input_rx => match admitted {
                    Ok(admitted) => admitted,
                    Err(_) => return,
                },
            };
            let (text, deadline) = admitted;
            let request = TtsRequest {
                speech_id: options.speech_id,
                speech_epoch: options.speech_epoch,
                text,
                language: options.language,
                voice: options.voice,
                claim_class: options.claim_class,
            };
            let outcome = tokio::select! {
                biased;
                _ = worker_token.cancelled() => None,
                _ = tokio::time::sleep_until(deadline) => Some(error("tts request timed out")),
                result = synthesize_and_forward(inner, request, worker_token.clone(), audio_tx) => Some(result),
            };
            // The separate terminal slot survives a full audio queue and never blocks cleanup.
            if let Some(outcome) = outcome {
                let _ = terminal_tx.send(outcome);
            }
            worker_token.cancel();
        });
        Ok(TtsDuplexSession {
            input: Box::new(BufferedInput {
                text: String::new(),
                finished: false,
                sender: Some(input_tx),
                token: token.clone(),
                limits,
            }),
            events: Box::new(BufferedEvents {
                audio: audio_rx,
                terminal: Some(terminal_rx),
                token: token.clone(),
            }),
            control: Box::new(OwnedProviderTasks {
                token,
                tasks: vec![worker],
            }),
        })
    }
}

struct BufferedInput {
    text: String,
    finished: bool,
    sender: Option<oneshot::Sender<(String, tokio::time::Instant)>>,
    token: CancellationToken,
    limits: TtsInputLimits,
}

impl Drop for BufferedInput {
    fn drop(&mut self) {
        if !self.finished {
            self.token.cancel();
        }
    }
}

impl TtsTextInput for BufferedInput {
    fn push_text(&mut self, fragment: String) -> Result<(), TtsInputError> {
        if self.token.is_cancelled() {
            return Err(TtsInputError::Closed);
        }
        if self.finished {
            return Err(TtsInputError::Finished);
        }
        if fragment.len() > self.limits.max_input_bytes.saturating_sub(self.text.len()) {
            return Err(TtsInputError::TooLarge(fragment));
        }
        self.text.push_str(&fragment);
        Ok(())
    }

    fn finish_input(&mut self) -> Result<(), TtsInputError> {
        if self.finished {
            return Ok(());
        }
        if self.token.is_cancelled() {
            return Err(TtsInputError::Closed);
        }
        if self.text.trim().is_empty() {
            return Err(TtsInputError::EmptyInput);
        }
        let deadline = tokio::time::Instant::now() + self.limits.request_timeout;
        let sender = self.sender.take().ok_or(TtsInputError::Closed)?;
        sender
            .send((std::mem::take(&mut self.text), deadline))
            .map_err(|_| TtsInputError::Closed)?;
        self.finished = true;
        Ok(())
    }
}

struct BufferedEvents {
    audio: mpsc::Receiver<TtsEvent>,
    terminal: Option<oneshot::Receiver<TtsEvent>>,
    token: CancellationToken,
}

impl Drop for BufferedEvents {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[async_trait]
impl TtsStream for BufferedEvents {
    async fn next_event(&mut self) -> anyhow::Result<Option<TtsEvent>> {
        if self.terminal.is_none() {
            return Ok(None);
        }
        // All admitted audio precedes the terminal, even when expiry happened without output polling.
        match self.audio.recv().await {
            Some(audio) => Ok(Some(audio)),
            None => {
                let terminal = self.terminal.as_mut().expect("checked above").await;
                self.terminal = None;
                Ok(terminal.ok())
            }
        }
    }
}

fn error(message: impl Into<String>) -> TtsEvent {
    TtsEvent::Error {
        recoverable: false,
        message: message.into(),
    }
}

async fn synthesize_and_forward(
    inner: Arc<dyn StreamingTts>,
    request: TtsRequest,
    token: CancellationToken,
    audio: mpsc::Sender<TtsEvent>,
) -> TtsEvent {
    let mut stream = match inner.synthesize(request, token).await {
        Ok(stream) => stream,
        Err(failure) => return error(failure.to_string()),
    };
    let mut normalizer = AudioNormalizer::default();
    loop {
        let event = match stream.next_event().await {
            Ok(Some(event)) => event,
            Ok(None) => return error("tts stream ended before a terminal"),
            Err(failure) => return error(failure.to_string()),
        };
        let events = match normalizer.normalize(event) {
            Ok(events) => events,
            Err(failure) => return error(failure.to_string()),
        };
        for event in events {
            if matches!(event, TtsEvent::AudioDone { .. } | TtsEvent::Error { .. }) {
                return event;
            }
            if audio.send(event).await.is_err() {
                return error("tts output closed");
            }
        }
    }
}
