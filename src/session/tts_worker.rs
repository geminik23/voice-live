use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::events::VoiceEvent;
use crate::meta::MetaFactory;
use crate::speech::SpeechAct;
use crate::tts::{StreamingTts, TtsEvent, TtsRequest};

#[derive(Default)]
pub struct TtsCancellationRegistry {
    tokens: Mutex<HashMap<crate::ids::SpeechId, CancellationToken>>,
}

impl TtsCancellationRegistry {
    pub fn insert(&self, speech_id: crate::ids::SpeechId, token: CancellationToken) {
        self.tokens.lock().insert(speech_id, token);
    }

    pub fn remove(&self, speech_id: &crate::ids::SpeechId) {
        self.tokens.lock().remove(speech_id);
    }

    pub fn cancel(&self, speech_id: &crate::ids::SpeechId) {
        if let Some(token) = self.tokens.lock().get(speech_id) {
            token.cancel();
        }
    }
}

/// Synthesizes queued speech acts into the event stream. All results flow
/// back as events; the worker never touches session state directly.
pub async fn run_tts_worker(
    tts: Arc<dyn StreamingTts>,
    mut speech_rx: mpsc::Receiver<SpeechAct>,
    event_tx: mpsc::Sender<VoiceEvent>,
    meta_factory: Arc<dyn MetaFactory>,
    shutdown: CancellationToken,
    registry: Arc<TtsCancellationRegistry>,
    voice: String,
) {
    loop {
        let act = tokio::select! {
            _ = shutdown.cancelled() => None,
            act = speech_rx.recv() => act,
        };

        let Some(act) = act else {
            break;
        };

        let cancellation = CancellationToken::new();
        registry.insert(act.id, cancellation.clone());

        let request = TtsRequest {
            speech_id: act.id,
            speech_epoch: act.speech_epoch,
            text: act.text.clone(),
            language: "Korean".to_string(),
            voice: voice.clone(),
            claim_class: act.class,
        };

        match tts.synthesize(request, cancellation.clone()).await {
            Ok(mut stream) => loop {
                if cancellation.is_cancelled() {
                    break;
                }

                match stream.next_event().await {
                    Ok(Some(TtsEvent::Audio {
                        sequence,
                        sample_rate,
                        pcm_s16le,
                        ..
                    })) => {
                        let chunk = VoiceEvent::TtsAudioChunk {
                            meta: meta_factory.new_meta(),
                            speech_id: act.id,
                            speech_epoch: act.speech_epoch,
                            sequence,
                            sample_rate,
                            pcm: pcm_s16le,
                        };

                        if event_tx.send(chunk).await.is_err() {
                            break;
                        }
                    }
                    Ok(Some(TtsEvent::AudioDone { .. })) => {
                        let done = VoiceEvent::TtsAudioDone {
                            meta: meta_factory.new_meta(),
                            speech_id: act.id,
                            speech_epoch: act.speech_epoch,
                        };
                        let _ = event_tx.send(done).await;
                        break;
                    }
                    Ok(Some(TtsEvent::Error {
                        recoverable,
                        message,
                    })) => {
                        let failed = VoiceEvent::TtsFailed {
                            meta: meta_factory.new_meta(),
                            speech_id: act.id,
                            speech_epoch: act.speech_epoch,
                            recoverable,
                            message,
                        };
                        let _ = event_tx.send(failed).await;
                        break;
                    }
                    // A cancelled stream ends early by design; that is an
                    // abort, not a synthesis failure, and must not be counted
                    // as one on every barge-in.
                    Ok(None) if cancellation.is_cancelled() => break,
                    Ok(None) => {
                        let failed = VoiceEvent::TtsFailed {
                            meta: meta_factory.new_meta(),
                            speech_id: act.id,
                            speech_epoch: act.speech_epoch,
                            recoverable: true,
                            message: "tts stream ended unexpectedly".into(),
                        };
                        let _ = event_tx.send(failed).await;
                        break;
                    }
                    Err(error) => {
                        let failed = VoiceEvent::TtsFailed {
                            meta: meta_factory.new_meta(),
                            speech_id: act.id,
                            speech_epoch: act.speech_epoch,
                            recoverable: true,
                            message: error.to_string(),
                        };
                        let _ = event_tx.send(failed).await;
                        break;
                    }
                }
            },
            Err(error) => {
                let failed = VoiceEvent::TtsFailed {
                    meta: meta_factory.new_meta(),
                    speech_id: act.id,
                    speech_epoch: act.speech_epoch,
                    recoverable: true,
                    message: error.to_string(),
                };
                let _ = event_tx.send(failed).await;
            }
        }

        registry.remove(&act.id);
    }
}
