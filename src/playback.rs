use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::events::VoiceEvent;
use crate::ids::SpeechId;
use crate::interaction::InterruptionReason;
use crate::meta::MetaFactory;

#[derive(Debug, Clone)]
pub enum PlaybackCommand {
    Enqueue {
        speech_id: SpeechId,
        speech_epoch: u64,
        sequence: u32,
        sample_rate: u32,
        pcm: Bytes,
    },
    MarkDone {
        speech_id: SpeechId,
        speech_epoch: u64,
    },
    Duck {
        gain: f32,
        fade_ms: u32,
    },
    Resume {
        gain: f32,
        fade_ms: u32,
    },
    Abort {
        new_speech_epoch: u64,
        reason: InterruptionReason,
    },
    ClearAll,
    Shutdown,
}

impl PlaybackCommand {
    pub fn kind(&self) -> &'static str {
        match self {
            PlaybackCommand::Enqueue { .. } => "enqueue",
            PlaybackCommand::MarkDone { .. } => "mark_done",
            PlaybackCommand::Duck { .. } => "duck",
            PlaybackCommand::Resume { .. } => "resume",
            PlaybackCommand::Abort { .. } => "abort",
            PlaybackCommand::ClearAll => "clear_all",
            PlaybackCommand::Shutdown => "shutdown",
        }
    }
}

#[async_trait]
pub trait PlaybackSink: Send + Sync {
    async fn command(&self, command: PlaybackCommand);
}

/// Forwards playback commands to the WebSocket gateway writer.
#[derive(Clone)]
pub struct ClientPlaybackSink {
    tx: mpsc::Sender<PlaybackCommand>,
    shutdown: Option<tokio_util::sync::CancellationToken>,
}

impl ClientPlaybackSink {
    pub fn new(tx: mpsc::Sender<PlaybackCommand>) -> Self {
        Self { tx, shutdown: None }
    }

    /// Uses nonblocking runtime admission; overload terminates this session.
    pub fn for_runtime(
        tx: mpsc::Sender<PlaybackCommand>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            tx,
            shutdown: Some(shutdown),
        }
    }
}

#[async_trait]
impl PlaybackSink for ClientPlaybackSink {
    async fn command(&self, command: PlaybackCommand) {
        if let Some(shutdown) = &self.shutdown {
            if self.tx.try_send(command).is_err() {
                tracing::error!("playback queue overloaded or closed; terminating session");
                shutdown.cancel();
            }
        } else {
            let _ = self.tx.send(command).await;
        }
    }
}

/// A sink that accepts commands and never acknowledges them, standing in for a
/// client that disconnected mid-utterance.
pub struct UnacknowledgedPlaybackSink;

#[async_trait]
impl PlaybackSink for UnacknowledgedPlaybackSink {
    async fn command(&self, _command: PlaybackCommand) {}
}

/// Deterministic playout simulator for virtual-time tests. It drains
/// commands, advances playback with tokio timers, and reports progress,
/// completion, and interruption back into the session event channel exactly
/// like the browser client does.
pub struct SimulatedPlaybackSink {
    tx: mpsc::Sender<PlaybackCommand>,
}

impl SimulatedPlaybackSink {
    pub fn spawn(event_tx: mpsc::Sender<VoiceEvent>, meta_factory: Arc<dyn MetaFactory>) -> Self {
        let (command_tx, command_rx) = mpsc::channel::<PlaybackCommand>(256);
        tokio::spawn(run_simulator(command_rx, event_tx, meta_factory));
        Self { tx: command_tx }
    }
}

#[async_trait]
impl PlaybackSink for SimulatedPlaybackSink {
    async fn command(&self, command: PlaybackCommand) {
        let _ = self.tx.send(command).await;
    }
}

struct SimState {
    speech_id: Option<SpeechId>,
    speech_epoch: u64,
    sample_rate: u32,
    played_samples: u64,
    pending: VecDeque<(tokio::time::Instant, u64)>,
    done: bool,
    timeline_end: Option<tokio::time::Instant>,
}

impl SimState {
    fn new() -> Self {
        Self {
            speech_id: None,
            speech_epoch: 0,
            sample_rate: 24_000,
            played_samples: 0,
            pending: VecDeque::new(),
            done: false,
            timeline_end: None,
        }
    }
}

async fn run_simulator(
    mut command_rx: mpsc::Receiver<PlaybackCommand>,
    event_tx: mpsc::Sender<VoiceEvent>,
    meta_factory: Arc<dyn MetaFactory>,
) {
    let mut state = SimState::new();

    loop {
        let next_end = state.pending.front().map(|(end, _)| *end);

        tokio::select! {
            command = command_rx.recv() => {
                let Some(command) = command else { break };
                match command {
                    PlaybackCommand::Enqueue {
                        speech_id,
                        speech_epoch,
                        sample_rate,
                        pcm,
                        ..
                    } => {
                        if state.speech_id != Some(speech_id) {
                            // A new speech takes over the playout timeline.
                            state.pending.clear();
                            state.timeline_end = None;
                            state.played_samples = 0;
                            state.speech_id = Some(speech_id);
                            state.speech_epoch = speech_epoch;
                            state.sample_rate = sample_rate;
                        } else if state.speech_epoch != speech_epoch {
                            continue;
                        }

                        let now = tokio::time::Instant::now();
                        let start = state
                            .timeline_end
                            .map(|end| end.max(now))
                            .unwrap_or(now);
                        let samples = (pcm.len() / 2) as u64;
                        let ms = if sample_rate == 0 {
                            0
                        } else {
                            samples * 1_000 / sample_rate as u64
                        };
                        let end = start + Duration::from_millis(ms);

                        state.pending.push_back((end, samples));
                        state.timeline_end = Some(end);
                    }

                    PlaybackCommand::MarkDone { speech_epoch, .. } => {
                        if state.speech_epoch == speech_epoch {
                            state.done = true;
                            if state.pending.is_empty() {
                                finish_playback(&mut state, &event_tx, &meta_factory).await;
                            }
                        }
                    }

                    PlaybackCommand::Duck { .. } | PlaybackCommand::Resume { .. } => {}

                    PlaybackCommand::Abort {
                        new_speech_epoch,
                        reason,
                    } => {
                        if let Some(speech_id) = state.speech_id.take() {
                            let _ = event_tx
                                .send(VoiceEvent::PlaybackInterrupted {
                                    meta: meta_factory.new_meta(),
                                    speech_id,
                                    played_samples: state.played_samples,
                                    reason,
                                })
                                .await;
                        }

                        state.pending.clear();
                        state.timeline_end = None;
                        state.played_samples = 0;
                        state.done = false;
                        state.speech_epoch = new_speech_epoch;
                    }

                    PlaybackCommand::ClearAll => {
                        state.pending.clear();
                        state.timeline_end = None;
                        state.played_samples = 0;
                        state.done = false;
                        state.speech_id = None;
                    }

                    PlaybackCommand::Shutdown => break,
                }
            }

            _ = async {
                match next_end {
                    Some(end) => tokio::time::sleep_until(end).await,
                    None => std::future::pending::<()>().await,
                }
            }, if !state.pending.is_empty() => {
                let now = tokio::time::Instant::now();

                while let Some((end, samples)) = state.pending.front() {
                    if *end <= now {
                        state.played_samples += *samples;
                        state.pending.pop_front();
                    } else {
                        break;
                    }
                }

                if let Some(speech_id) = state.speech_id {
                    let _ = event_tx
                        .send(VoiceEvent::PlaybackProgress {
                            meta: meta_factory.new_meta(),
                            speech_id,
                            played_samples: state.played_samples,
                        })
                        .await;
                }

                if state.pending.is_empty() && state.done {
                    finish_playback(&mut state, &event_tx, &meta_factory).await;
                }
            }
        }
    }
}

async fn finish_playback(
    state: &mut SimState,
    event_tx: &mpsc::Sender<VoiceEvent>,
    meta_factory: &Arc<dyn MetaFactory>,
) {
    state.done = false;
    if let Some(speech_id) = state.speech_id.take() {
        let _ = event_tx
            .send(VoiceEvent::PlaybackCompleted {
                meta: meta_factory.new_meta(),
                speech_id,
            })
            .await;
    }
}
