//! Wire protocol between a client and the voice runtime.
//!
//! These types describe *what* is exchanged, never *how* it is carried. The
//! runtime hands out an [`mpsc::Sender<ServerMessage>`] and an
//! [`mpsc::Receiver<ClientInput>`]; whatever bridges those channels to a
//! socket — WebSocket today, WebRTC later — lives outside this crate.
//!
//! [`mpsc::Sender<ServerMessage>`]: tokio::sync::mpsc::Sender
//! [`mpsc::Receiver<ClientInput>`]: tokio::sync::mpsc::Receiver

use serde::Deserialize;

use crate::ids::SpeechId;

/// Something to deliver to the client.
///
/// `Binary` frames carry `[u64 LE sequence][i16 LE PCM]`, the same framing the
/// client uses for microphone audio. See [`crate::media::encode_client_audio`].
#[derive(Debug)]
pub enum ServerMessage {
    Json(serde_json::Value),
    Binary(bytes::Bytes),
}

/// A control message from the client.
///
/// Playback acknowledgements are load bearing rather than advisory: the
/// audible-truth ledger uses `played_samples` to decide what the user actually
/// heard, so a client that under-reports corrupts the agent's context.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientControlMessage {
    /// Sent once before the first audio frame.
    ///
    /// `input_sample_rate` must be the capture device's *real* rate. Browsers
    /// may ignore a requested rate, and the runtime picks its resampler from
    /// this value.
    SessionStart {
        input_sample_rate: u32,
        channels: u16,
    },
    /// Advisory only; the server's own energy VAD owns the decision.
    LocalVad {
        state: String,
        probability: Option<f32>,
    },
    PlaybackChunkCompleted {
        speech_id: SpeechId,
        speech_epoch: u64,
        sequence: u32,
        played_samples: u64,
    },
    PlaybackCompleted {
        speech_id: SpeechId,
        speech_epoch: u64,
    },
    PlaybackInterrupted {
        speech_id: SpeechId,
        speech_epoch: u64,
        played_samples: u64,
    },
    SessionStop,
    /// Development affordance, gated by `dev.allow_text_injection`.
    InjectUserText {
        text: String,
    },
}
